/* 
Purpose:
   - Load Amazon product metadata from a JSONL staging table (products_raw_stg) into the target table (products_raw) in PostgreSQL.
Key problems handled:
   - Some lines in the source JSONL are NOT valid JSON (e.g., embedded HTML/CSS with unescaped quotes).
     We use safe_jsonb(text) to return NULL instead of failing the whole load.
   - The staging batch may contain duplicate ASINs.
     We de-duplicate with DISTINCT ON (asin) before INSERT...ON CONFLICT, to avoid:
     "ON CONFLICT DO UPDATE command cannot affect row a second time"
*/

BEGIN;

-- Ensure staging table exists (no-op if it already exists)
CREATE TABLE IF NOT EXISTS public.products_raw_stg (
  line TEXT NOT NULL
);

-- Create/replace a safe JSON parser: returns NULL for invalid JSON instead of raising an error.
CREATE OR REPLACE FUNCTION public.safe_jsonb(t text)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
  RETURN t::jsonb;
EXCEPTION WHEN others THEN
  RETURN NULL;
END;
$$;

-- Load into target table:
/*
- parse JSON safely
- keep only valid JSON rows with non-empty ASIN
- de-duplicate per ASIN within this batch (choose the "largest" record by line length)
- upsert into products_raw
*/
WITH parsed AS (
  SELECT public.safe_jsonb(line) AS j, line
  FROM public.products_raw_stg
),
good AS (
  SELECT
    j,
    (j->>'asin') AS asin,
    length(line) AS line_len
  FROM parsed
  WHERE j IS NOT NULL
    AND (j->>'asin') IS NOT NULL
    AND (j->>'asin') <> ''
),
dedup AS (
  -- Pick exactly one row per ASIN (the "largest" JSON line is usually the most complete record)
  SELECT DISTINCT ON (asin)
    j
  FROM good
  ORDER BY asin, line_len DESC
)
INSERT INTO public.products_raw (
  asin, title, brand, main_cat,
  category, description, feature, rank,
  also_buy, also_view, similar_item,
  tech1, tech2, fit,
  date_raw, price_raw,
  image_url, image_url_high_res,
  ingest_dt
)
SELECT
  j->>'asin'          AS asin,
  j->>'title'         AS title,
  j->>'brand'         AS brand,
  j->>'main_cat'      AS main_cat,
  j->'category'       AS category,
  j->'description'    AS description,
  j->'feature'        AS feature,
  j->'rank'           AS rank,
  j->'also_buy'       AS also_buy,
  j->'also_view'      AS also_view,
  j->>'similar_item'  AS similar_item,
  j->>'tech1'         AS tech1,
  j->>'tech2'         AS tech2,
  j->>'fit'           AS fit,
  j->>'date'          AS date_raw,
  j->>'price'         AS price_raw,
  j->'imageURL'       AS image_url,
  j->'imageURLHighRes' AS image_url_high_res,
  CURRENT_DATE        AS ingest_dt
FROM dedup
ON CONFLICT (asin) DO UPDATE
SET
  title = EXCLUDED.title,
  brand = EXCLUDED.brand,
  main_cat = EXCLUDED.main_cat,
  category = EXCLUDED.category,
  description = EXCLUDED.description,
  feature = EXCLUDED.feature,
  rank = EXCLUDED.rank,
  also_buy = EXCLUDED.also_buy,
  also_view = EXCLUDED.also_view,
  similar_item = EXCLUDED.similar_item,
  tech1 = EXCLUDED.tech1,
  tech2 = EXCLUDED.tech2,
  fit = EXCLUDED.fit,
  date_raw = EXCLUDED.date_raw,
  price_raw = EXCLUDED.price_raw,
  image_url = EXCLUDED.image_url,
  image_url_high_res = EXCLUDED.image_url_high_res,
  ingest_dt = EXCLUDED.ingest_dt,
  updated_at = now()
;

-- Post-load verification (counts in the target table)
SELECT COUNT(*) AS products_raw_rows FROM public.products_raw;
SELECT COUNT(DISTINCT asin) AS products_raw_distinct_asin FROM public.products_raw;

-- Clean up staging only after a successful load
TRUNCATE public.products_raw_stg;

COMMIT;