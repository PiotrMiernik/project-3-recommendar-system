-- Check number of generated review embeddings
SELECT
    COUNT(*) AS embeddings_count,
    COUNT(DISTINCT review_id) AS distinct_reviews,
    COUNT(DISTINCT asin) AS distinct_products,
    COUNT(DISTINCT model_version) AS model_versions
FROM vector.review_embeddings;

-- Check embedding vector dimension
SELECT
    vector_dims(embedding) AS embedding_dimension,
    COUNT(*) AS row_count
FROM vector.review_embeddings
GROUP BY vector_dims(embedding);

-- Preview embeddings metadata
SELECT
    review_id,
    asin,
    model_version,
    text_hash,
    created_at
FROM vector.review_embeddings
LIMIT 10;