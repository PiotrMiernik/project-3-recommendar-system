CREATE TABLE IF NOT EXISTS public.products_raw (
  asin              TEXT PRIMARY KEY,
  title             TEXT,
  brand             TEXT,
  main_cat          TEXT,
  category          JSONB,
  description       JSONB,
  feature           JSONB,
  rank              JSONB,
  also_buy          JSONB,
  also_view         JSONB,
  similar_item      TEXT,
  tech1             TEXT,
  tech2             TEXT,
  fit               TEXT,
  date_raw          TEXT,
  price_raw         TEXT,
  image_url         JSONB,
  image_url_high_res JSONB,
  ingest_dt         DATE NOT NULL DEFAULT CURRENT_DATE,
  updated_at        TIMESTAMP NOT NULL DEFAULT now()
);

