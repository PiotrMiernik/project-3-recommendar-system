-- Count records in ML-ready feature tables
SELECT 'product_features' AS table_name, COUNT(*) AS row_count
FROM mlready.product_features

UNION ALL

SELECT 'user_features' AS table_name, COUNT(*) AS row_count
FROM mlready.user_features

UNION ALL

SELECT 'global_stats' AS table_name, COUNT(*) AS row_count
FROM mlready.global_stats

UNION ALL

SELECT 'user_product_interactions' AS table_name, COUNT(*) AS row_count
FROM mlready.user_product_interactions;

-- Check nulls in key columns
SELECT
    COUNT(*) AS total_rows,
    SUM(CASE WHEN asin IS NULL THEN 1 ELSE 0 END) AS null_asin_count
FROM mlready.product_features;