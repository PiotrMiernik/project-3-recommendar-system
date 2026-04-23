# This module is responsible for loading configuration from the .env file
# and providing it as a unified settings dictionary for the entire project.

import os
from pathlib import Path

def get_required_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {var_name}")
    return value

def load_settings() -> dict:
    from dotenv import load_dotenv

    project_root = Path(__file__).resolve().parents[2]
    env_path = project_root / "config" / ".env"

    load_dotenv(env_path)

    settings = {
        "aws_region": get_required_env("AWS_REGION"),
        "s3_bucket": get_required_env("S3_BUCKET"),
        "s3_raw_prefix": os.getenv("S3_RAW_PREFIX", "raw/"),
        "s3_staging_prefix": os.getenv("S3_STAGING_PREFIX", "staging/"),
        "s3_mlready_prefix": os.getenv("S3_MLREADY_PREFIX", "mlready/"),
        "mongodb_uri": get_required_env("MONGODB_URI"),
        "mongodb_db": get_required_env("MONGODB_DB"),
        "mongodb_collection_reviews": get_required_env("MONGODB_COLLECTION_REVIEWS"),
        "rds_host": get_required_env("RDS_HOST"),
        "rds_port": int(os.getenv("RDS_PORT", "5432")),
        "rds_dbname": get_required_env("RDS_DBNAME"),
        "rds_user": get_required_env("RDS_USER"),
        "rds_password": get_required_env("RDS_PASSWORD"),
        "chunk_size": int(os.getenv("CHUNK_SIZE", "5000")),
    }
    return settings

def load_emr_transform_settings() -> dict:
    return {
        "aws_region": os.getenv("AWS_REGION"),
        "s3_bucket": os.getenv("S3_BUCKET"),
        "s3_raw_prefix": os.getenv("S3_RAW_PREFIX", "raw/"),
        "s3_staging_prefix": os.getenv("S3_STAGING_PREFIX", "staging/"),
        "s3_mlready_prefix": os.getenv("S3_MLREADY_PREFIX", "mlready/"),
        "embedding_model_version": os.getenv("EMBEDDING_MODEL_VERSION", "all-MiniLM-L6-v2"),
    }

def get_iceberg_settings() -> dict:
    """
    Centralized naming and identification for Iceberg tables in the Gold layer.
    """
    catalog = "glue_catalog"
    db = "mlready"
    
    return {
        "catalog_name": catalog,
        "database_name": db,
        "table_products": f"{catalog}.{db}.product_features",
        "table_users": f"{catalog}.{db}.user_features",
        "table_interactions": f"{catalog}.{db}.user_product_interactions",
        "table_stats": f"{catalog}.{db}.global_stats"
    } 