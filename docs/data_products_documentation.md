# Data Products Documentation

This document describes the final data products produced by the recommender system pipeline.

The final outputs include:

- ML-ready Iceberg tables stored in Amazon S3 and queryable through Athena
- Review embeddings stored in PostgreSQL with pgvector

---

# 1. ML-ready Tables

## 1.1 `mlready.product_features`

Product-level feature table used for recommendation and downstream ML workloads.

| Column                     | Type    | Unit       | Description                                                |
|----------------------------|---------|------------|------------------------------------------------------------|
| `asin`                     | string  | N/A        | Amazon product identifier.                                 |
| `title`                    | string  | N/A        | Product title.                                             |
| `brand`                    | string  | N/A        | Product brand name.                                        |
| `price`                    | double  | USD        | Product price.                                             |
| `has_brand`                | boolean | N/A        | Indicates whether the product has a non-empty brand value. |
| `has_price`                | boolean | N/A        | Indicates whether the product has a valid price.           |
| `title_length`             | integer | characters | Length of the product title.                               |
| `description_total_length` | integer | characters | Total length of the product description text.              |
| `main_category`            | string  | N/A        | Main product category.                                     |
| `feature_snapshot_date`    | date    | N/A        | Date when the feature snapshot was generated.              |

---

## 1.2 `mlready.user_features`

User-level aggregated behavioral feature table.

| Column                    | Type      | Unit          | Description                                                |
|---------------------------|-----------|---------------|------------------------------------------------------------|
| `reviewer_id`             | string    | N/A           | Unique reviewer/user identifier.                           |
| `reviews_count`           | integer   | reviews       | Total number of reviews written by the user.               |
| `distinct_products_count` | integer   | products      | Number of distinct products reviewed by the user.          |
| `avg_rating`              | double    | rating points | Average rating given by the user.                          |
| `min_rating`              | integer   | rating points | Minimum rating given by the user.                          |
| `max_rating`              | integer   | rating points | Maximum rating given by the user.                          |
| `verified_reviews_ratio`  | double    | ratio 0–1     | Share of reviews marked as verified purchases.             |
| `avg_vote_count`          | double    | votes         | Average number of helpful votes received by user's reviews.|
| `avg_review_text_length`  | double    | characters    | Average length of user's review text.                      |
| `first_review_timestamp`  | timestamp | N/A           | Timestamp of the user's first review.                      |
| `last_review_timestamp`   | timestamp | N/A           | Timestamp of the user's latest review.                     |
| `active_days_span`        | integer   | days          | Number of days between the user's first and latest review. |
| `feature_snapshot_date`   | date      | N/A           | Date when the feature snapshot was generated.              |

---

## 1.3 `mlready.global_stats`

Product-level review statistics table.

| Column                    | Type      | Unit          | Description                                              |
|---------------------------|-----------|---------------|----------------------------------------------------------|
| `asin`                    | string    | N/A           | Amazon product identifier.                               |
| `reviews_count`           | integer   | reviews       | Total number of reviews for the product.                 |
| `distinct_reviewers_count`| integer   | users         | Number of distinct reviewers for the product.            |
| `avg_rating`              | double    | rating points | Average product rating.                                  |
| `rating_stddev`           | double    | rating points | Standard deviation of product ratings.                   |
| `verified_reviews_ratio`  | double    | ratio 0–1     | Share of product reviews marked as verified purchases.   |
| `avg_vote_count`          | double    | votes         | Average number of helpful votes for product reviews.     |
| `avg_review_text_length`  | double    | characters    | Average review text length for the product.              |
| `positive_reviews_ratio`  | double    | ratio 0–1     | Share of positive reviews, usually based on high ratings.|
| `negative_reviews_ratio`  | double    | ratio 0–1     | Share of negative reviews, usually based on low ratings. |
| `first_review_timestamp`  | timestamp | N/A           | Timestamp of the first review for the product.           |
| `last_review_timestamp`   | timestamp | N/A           | Timestamp of the latest review for the product.          |
| `feature_snapshot_date`   | date      | N/A           | Date when the feature snapshot was generated.            |

---

## 1.4 `mlready.user_product_interactions`

Interaction-level table joining user behavior, product metadata and product review statistics.

| Column                           | Type      | Unit          | Description                                                         |
|----------------------------------|-----------|---------------|---------------------------------------------------------------------|
| `review_id`                      | string    | N/A           | Unique review identifier.                                           |
| `reviewer_id`                    | string    | N/A           | Unique reviewer/user identifier.                                    |
| `asin`                           | string    | N/A           | Amazon product identifier.                                          |
| `rating`                         | double    | rating points | Rating given by the user to the product.                            |
| `vote_count`                     | integer   | votes         | Number of helpful votes received by the review.                     |
| `verified`                       | boolean   | N/A           | Indicates whether the review is from a verified purchase.           |
| `review_text_length`             | integer   | characters    | Length of the review text.                                          |
| `summary_length`                 | integer   | characters    | Length of the review summary.                                       |
| `review_timestamp`               | timestamp | N/A           | Timestamp of the review.                                            |
| `review_date`                    | date      | N/A           | Date of the review.                                                 |
| `product_main_category`          | string    | N/A           | Main category of the reviewed product.                              |
| `product_brand`                  | string    | N/A           | Brand of the reviewed product.                                      |
| `product_price`                  | double    | USD           | Product price.                                                      |
| `product_has_description`        | boolean   | N/A           | Indicates whether the product has a description.                    |
| `product_title_length`           | integer   | characters    | Length of the product title.                                        |
| `product_description_length`     | integer   | characters    | Length of the product description.                                  |
| `user_reviews_count`             | integer   | reviews       | Total number of reviews written by the user.                        |
| `user_avg_rating`                | double    | rating points | Average rating given by the user.                                   |
| `user_verified_reviews_ratio`    | double    | ratio 0–1     | Share of user's reviews marked as verified purchases.               |
| `user_active_days_span`          | integer   | days          | Number of days between user's first and latest review.              |
| `product_reviews_count`          | integer   | reviews       | Total number of reviews for the product.                            |
| `product_avg_rating`             | double    | rating points | Average rating of the product.                                      |
| `product_positive_reviews_ratio` | double    | ratio 0–1     | Share of positive reviews for the product.                          |
| `product_negative_reviews_ratio` | double    | ratio 0–1     | Share of negative reviews for the product.                          |
| `label`                          | double    | rating points | Target label for recommendation modeling, based on the user rating. |
| `feature_snapshot_date`          | date      | N/A           | Date when the feature snapshot was generated.                       |

---

# 2. Vector Store

## 2.1 `vector.review_embeddings`

PostgreSQL pgvector table storing semantic embeddings generated from review text.

Each row represents one review embedding together with metadata required for retrieval, filtering and incremental updates.

| Column             | Type        | Unit              | Description                                                              |
|--------------------|-------------|-------------------|--------------------------------------------------------------------------|
| `review_id`        | string      | N/A               | Unique review identifier. Primary business key for the embedding record. |
| `asin`             | string      | N/A               | Amazon product identifier related to the review.                         |
| `reviewer_id`      | string      | N/A               | Unique reviewer/user identifier.                                         |
| `review_timestamp` | timestamp   | N/A               | Timestamp of the original review.                                        |
| `text_hash`        | string      | N/A               | Hash of the input text used to detect changed review content.            |
| `model_version`    | string      | N/A               | Name/version of the embedding model used to generate the vector.         |
| `embedding`        | vector(384) | vector dimensions | Semantic embedding generated from review text using `all-MiniLM-L6-v2`.  |
| `created_at`       | timestamp   | N/A               | Timestamp when the embedding record was created in pgvector.             |
| `updated_at`       | timestamp   | N/A               | Timestamp when the embedding record was last updated.                    |

---

# 3. Usage Notes

ML-ready tables are designed for downstream recommendation modeling, analytics and feature exploration.

The `vector.review_embeddings` table is designed for semantic retrieval and similarity search workflows using PostgreSQL with pgvector.

The embedding dimension is 384 because the project uses the `all-MiniLM-L6-v2` Sentence Transformers model.