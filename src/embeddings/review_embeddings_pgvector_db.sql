-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Create target schema
CREATE SCHEMA IF NOT EXISTS vector;

-- Create table for review embeddings
CREATE TABLE IF NOT EXISTS vector.review_embeddings (
    review_id TEXT PRIMARY KEY,
    asin TEXT,
    reviewer_id TEXT,
    review_timestamp TIMESTAMP,
    text_hash TEXT NOT NULL,
    model_version TEXT NOT NULL,
    embedding VECTOR(384) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Helpful index for filtering by product
CREATE INDEX IF NOT EXISTS idx_review_embeddings_asin
    ON vector.review_embeddings (asin);

-- Helpful index for filtering by reviewer
CREATE INDEX IF NOT EXISTS idx_review_embeddings_reviewer_id
    ON vector.review_embeddings (reviewer_id);

-- Helpful index for incremental checks / lineage
CREATE INDEX IF NOT EXISTS idx_review_embeddings_model_version
    ON vector.review_embeddings (model_version);

CREATE INDEX IF NOT EXISTS idx_review_embeddings_text_hash
    ON vector.review_embeddings (text_hash);