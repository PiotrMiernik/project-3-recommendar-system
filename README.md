# Project 3 – Recommender System Data Platform


![CI](https://github.com/PiotrMiernik/project-3-recommender-system/actions/workflows/ci.yml/badge.svg)

This project is an end-to-end recommendation system data platform built on AWS using modern data engineering and MLOps-oriented practices.

The platform ingests raw product metadata and user reviews, processes them through a layered data architecture, generates ML-ready feature tables, and creates vector embeddings for downstream recommendation and retrieval use cases.

The project focuses on scalable batch data processing, feature engineering, vector search preparation, and production-oriented orchestration using MongoDB, Apache Airflow, AWS EMR Serverless (Spark), Docker (AWD ECR), Apache Iceberg, and pgvector.

---

# Project Overview

The main goal of the project is to simulate a production-oriented recommendation system data platform that:

- Collects product metadata and user review data from multiple data sources
- Simulates real production data sources using self-created PostgreSQL and MongoDB databases populated with Amazon review datasets
- Loads raw data into a scalable S3-based data lake
- Transforms and validates data through multiple processing layers
- Builds ML-ready feature tables for downstream recommendation models
- Generates text embeddings from review data
- Stores vector embeddings in PostgreSQL with pgvector support
- Orchestrates the full workflow with Apache Airflow
- Uses lightweight CI pipelines and automated validation checks

The project simulates a realistic recommendation system backend architecture that could support downstream machine learning, recommendation, analytics, and semantic search workloads.

---

# Key Features

- Layered data architecture: Raw → Staging → ML-ready
- Batch-oriented scalable processing using PySpark on AWS EMR Serverless
- Iceberg-based feature store for ML-ready datasets
- Vector embeddings pipeline using Sentence Transformers
- pgvector integration for vector similarity search
- Apache Airflow orchestration
- Docker-based dependency isolation for embedding generation with AWS ECR
- Data validation using Great Expectations and SQL validation checks
- Lightweight CI pipeline with automated unit tests
- Infrastructure-as-Code approach using Terraform

---

# Repository Structure

```text
project-3-recommender-system/

├── LICENSE
├── README.md
├── config                               # Environment configuration
│
├── dags                                 # Apache Airflow DAG definitions
│   └── recommender_system_pipeline_prod.py
│
├── data                                 # Local raw datasets used to populate source databases
│   ├── Toys_and_Games_load.json
│   └── meta_Toys_and_Games_load.jsonl
│
├── docker                               # Docker configuration for embeddings runtime
│   └── embeddings
│       └── Dockerfile
│
├── docs                                 # Documentation and architecture diagrams
│   └── architecture.png
│
├── requirements-ci.txt                  # Lightweight CI dependencies
├── requirements-embeddings.txt          # Dependencies for embeddings pipeline
├── requirements.txt                     # Main project dependencies
│
├── src                                  # Main application source code
│   ├── common                           # Shared utilities and helper modules
│   │   ├── config.py
│   │   ├── embedding_utils.py
│   │   ├── logging.py
│   │   ├── manifest.py
│   │   ├── s3_utils.py
│   │   └── spark_utils.py
│   │
│   ├── data_product_check               # Final SQL-based data product validation checks
│   │   ├── mlready_tables_chcek.sql
│   │   └── pgvector_embeddings_check.sql
│   │
│   ├── data_sources                     # Source database setup scripts
│   │   ├── mongo
│   │   │   ├── eda_stats.mongodb.js
│   │   │   ├── indexing.mongodb.js
│   │   │   └── setup_recommender.mongodb.js
│   │   │
│   │   └── postgresql
│   │       ├── load_recommender.sql
│   │       ├── setup_recommender.sql
│   │       └── test.sql
│   │
│   ├── data_validation                  # Great Expectations and validation scripts
│   │   ├── gx_results
│   │   ├── validate_mlready_product_features.py
│   │   ├── validate_mlready_product_review_stats.py
│   │   ├── validate_mlready_user_features.py
│   │   ├── validate_mlready_user_product_interactions.py
│   │   ├── validate_review_embeddings.py
│   │   ├── validate_staging_products.py
│   │   └── validate_staging_reviews.py
│   │
│   ├── embeddings                       # Embeddings generation and pgvector loading
│   │   ├── review_embeddings_pgvector_db.sql
│   │   ├── step1_prepare_and_filter_reviews.py
│   │   ├── step2_generate_review_embeddings.py
│   │   └── step3_load_reviews_embeddings_to_pgvector.py
│   │
│   ├── ingestion                        # Raw ingestion into S3 data lake
│   │   ├── ingest_products_to_s3_raw.py
│   │   └── ingest_reviews_to_s3_raw.py
│   │
│   └── transformations                  # Spark transformations and feature engineering
│       ├── build_mlready_product_features.py
│       ├── build_mlready_product_review_stats.py
│       ├── build_mlready_user_features.py
│       ├── build_mlready_user_product_interactions.py
│       ├── transform_raw_products_to_staging.py
│       └── transform_raw_reviews_to_staging.py
│
├── src_package.zip                      # Packaged source code uploaded to S3 for EMR jobs
│
├── terraform                            # Infrastructure-as-Code configuration
│   ├── README.md
│   ├── ecr.tf
│   ├── emr.tf
│   ├── glue.tf
│   ├── iam.tf
│   ├── outputs.tf
│   ├── providers.tf
│   ├── rds.tf
│   ├── s3.tf
│   ├── terraform.tfvars
│   ├── terraform.tfvars.example
│   └── variables.tf
│
├── tests                                # Smoke and unit tests
│   ├── test_smoke.py
│   └── unit
│       └── test_text_cleaning.py
│
└── .github/workflows                    # GitHub Actions CI workflows
    └── ci.yml

---

# Architecture Overview

The platform uses a layered architecture designed for scalable batch processing and ML feature preparation.

---

# Data Sources

The project simulates production-like source systems by manually creating and loading databases using Amazon review datasets:

MongoDB Atlas – user reviews dataset
AWS RDS PostgreSQL – product metadata dataset

The raw source data originates from Amazon product review datasets and is loaded into dedicated MongoDB and PostgreSQL databases before ingestion into the pipeline.

---

# Storage Layers

Raw Layer
    Stores raw JSONL GZ files in Amazon S3
    Append-only ingestion approach
    Includes ingestion manifests
    Simulates production-style immutable raw storage
Staging Layer
    Cleaned and normalized Parquet datasets
    Built using PySpark transformations on EMR Serverless
    Standardized schemas and data types
ML-ready Layer
    Iceberg tables containing feature-engineered datasets
    Optimized for downstream ML and analytics workloads
    Stored in S3 and managed through Glue Catalog

---

# Data Pipeline

The orchestration layer is implemented with Apache Airflow.

The pipeline executes sequentially to reduce EMR Serverless resource contention and simplify orchestration stability.

Main workflow stages:

Raw ingestion from MongoDB and PostgreSQL
Raw → Staging transformations
Staging data quality validation
ML-ready feature engineering
ML-ready data quality validation
Product reviews preparation and filtering
Prepared product reviews quality validation
Embedding generation
Loading vectors into pgvector

---
# ML-ready Feature Store

The ML-ready layer consists of four Iceberg tables:

Table	                    Purpose
product_features	        Product metadata and engineered product-level features
user_features	            Aggregated user-level behavioral features
product_review_stats	    Product review statistics and quality metrics
user_product_interactions	User-product interaction dataset

These datasets are designed for downstream recommendation models and analytical workloads.

---

# Embeddings Pipeline

The embeddings pipeline transforms review text into vector representations suitable for semantic search and recommendation systems.

Main Processing Steps
    Clean review text
    Decode HTML entities
    Normalize whitespace
    Build unified text input from summary + review text
    Generate embeddings using Sentence Transformers
    Normalize vectors
    Store vectors in PostgreSQL with pgvector

Incremental Processing
The pipeline supports incremental processing using:
    review_id
    text_hash
    model_version
This prevents unnecessary re-generation of existing embeddings.

---

# Technologies Used

    Python 3.11+
    Apache Airflow
    Apache Spark / PySpark
    AWS EMR Serverless
    AWS S3
    AWS RDS PostgreSQL
    AWS ECR
    MongoDB Atlas
    PostgreSQL pgvector
    Apache Iceberg
    Docker
    Great Expectations
    Terraform
    GitHub Actions
    Sentence Transformers

---

# CI/CD Workflows

File	                    Purpose
.github/workflows/ci.yml	Runs lightweight smoke and unit tests

The CI pipeline automatically validates the project on pushes to the dev branch and pull requests to main.

---

# Testing and Validation Strategy

The project includes multiple validation layers.

Code Validation
    Smoke tests for critical project modules
    Unit tests for embedding text preprocessing logic
Data Validation
    Great Expectations validation for staging and ML-ready datasets
    SQL-based data product validation checks
Final Data Product Checks
Final SQL checks verify:
    Existence of ML-ready Iceberg tables
    Row counts and key column quality
    Pgvector embeddings integrity
    Embedding vector dimensions

---

# Key Design Decisions and Improvements

Several architectural changes were introduced during development:

    Switched from parallel to sequential Airflow execution due to EMR Serverless resource contention
    Introduced Docker-based embeddings generation after dependency conflicts in EMR environments
    Implemented incremental embeddings logic using text_hash and model_version
    Simplified CI to lightweight unit testing instead of full pipeline execution
    Used Iceberg tables for scalable ML-ready feature storage
    Separated tabular ML-ready features from vector embeddings storage
    Added SQL-based downstream data product validation checks

---

# Output

The final outputs of the project are:
    ML-ready Iceberg feature tables stored in S3
    Review embeddings stored in PostgreSQL pgvector
    Query-ready datasets for downstream ML systems
    Validated data products ready for analytics and recommendation workloads

---

# Future Improvements

Potential future improvements:
    Real-time streaming ingestion with Kafka
    Online feature store
    ANN vector indexing optimization
    Automated model training pipeline
    Kubernetes-based orchestration
    Full deployment automation pipeline

---

# License

This project is licensed under the terms of the LICENSE file.
Created by Piotr Miernik – 2026