# This module is responsible for loading configuration from the .env file
# and providing it as a unified settings dictionary for the entire project.

import os
from pathlib import Path

from dotenv import load_dotenv

# Retrieves a required environment variable.
# Raises an error if the variable is missing or empty.
def get_required_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if value is None or value.strip() == "":
        raise ValueError(f"Missing required environment variable: {var_name}")
    return value

# Loads environment variables from config/.env and returns all settings
# as a dictionary used across ingestion, Airflow, and future pipelines.
def load_settings() -> dict:
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
        "embedding_model_version": os.getenv(
            "EMBEDDING_MODEL_VERSION", "all-MiniLM-L6-v2"
        ),
        "chunk_size": int(os.getenv("CHUNK_SIZE", "5000")),
    }

    return settings