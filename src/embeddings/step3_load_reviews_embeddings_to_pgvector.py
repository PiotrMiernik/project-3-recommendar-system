import os
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import boto3
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

from src.common.logging import get_logger

logger = get_logger(__name__)

SCHEMA_NAME = "vector"
TABLE_NAME = "review_embeddings"
VECTOR_DIMENSION = 384


def load_settings() -> Dict[str, Any]:
    """
    Load settings from config/.env.

    This script is intended to run locally, so it loads both:
    - S3 settings
    - PostgreSQL / pgvector settings
    """
    project_root = Path(__file__).resolve().parents[2]
    env_path = project_root / "config" / ".env"
    load_dotenv(env_path)

    return {
        "aws_region": os.getenv("AWS_REGION"),
        "s3_bucket": os.getenv("S3_BUCKET"),
        "s3_staging_prefix": os.getenv("S3_STAGING_PREFIX", "staging/"),
        "rds_host": os.getenv("RDS_HOST"),
        "rds_port": int(os.getenv("RDS_PORT", "5432")),
        "rds_dbname": os.getenv("RDS_DBNAME"),
        "rds_user": os.getenv("RDS_USER"),
        "rds_password": os.getenv("RDS_PASSWORD"),
        "chunk_size": int(os.getenv("CHUNK_SIZE", "500")),
    }


def build_input_prefix(settings: Dict[str, Any], ingest_dt: Optional[str] = None) -> str:
    """
    Build the S3 prefix for generated review embeddings.

    If ingest_dt is provided, load only one partition.
    """
    base_prefix = f"{settings['s3_staging_prefix']}review_embeddings/"

    if ingest_dt:
        return f"{base_prefix}ingest_dt={ingest_dt}/"

    return base_prefix


def list_parquet_keys(
    s3_client,
    bucket: str,
    prefix: str,
) -> List[str]:
    """
    List all parquet files under the given S3 prefix.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    parquet_keys: List[str] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                parquet_keys.append(key)

    return parquet_keys


def validate_required_columns(df: pd.DataFrame) -> None:
    """
    Validate that the input DataFrame contains all required columns.
    """
    required = {
        "review_id",
        "asin",
        "reviewer_id",
        "review_timestamp",
        "text_hash",
        "model_version",
        "embedding",
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")


def vector_to_pgvector_literal(vector: Any) -> str:
    """
    Convert a Python list / array-like embedding into pgvector text format.

    Example:
    [0.1, 0.2, 0.3]
    """
    if vector is None:
        raise ValueError("Embedding cannot be None.")

    vector_list = list(vector)

    if len(vector_list) != VECTOR_DIMENSION:
        raise ValueError(
            f"Invalid embedding dimension: expected {VECTOR_DIMENSION}, got {len(vector_list)}"
        )

    return "[" + ",".join(str(float(x)) for x in vector_list) + "]"


def normalize_timestamp(value: Any) -> Optional[Any]:
    """
    Convert pandas timestamp values into Python datetime objects.
    """
    if pd.isna(value):
        return None

    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()

    return value


def prepare_records(df: pd.DataFrame) -> List[tuple]:
    """
    Convert the DataFrame into records ready for PostgreSQL upsert.
    """
    records: List[tuple] = []

    for row in df.itertuples(index=False):
        records.append(
            (
                row.review_id,
                row.asin,
                row.reviewer_id,
                normalize_timestamp(row.review_timestamp),
                row.text_hash,
                row.model_version,
                vector_to_pgvector_literal(row.embedding),
            )
        )

    return records


def get_db_connection(settings: Dict[str, Any]):
    """
    Create and return a PostgreSQL connection.
    """
    return psycopg2.connect(
        host=settings["rds_host"],
        port=settings["rds_port"],
        dbname=settings["rds_dbname"],
        user=settings["rds_user"],
        password=settings["rds_password"],
    )


def chunk_records(records: List[tuple], chunk_size: int) -> Iterable[List[tuple]]:
    """
    Split records into chunks for batch upserts.
    """
    for i in range(0, len(records), chunk_size):
        yield records[i:i + chunk_size]


def upsert_records(conn, records: List[tuple], chunk_size: int) -> None:
    """
    Upsert records into vector.review_embeddings.

    Update happens only when:
    - text_hash changed, or
    - model_version changed
    """
    sql = f"""
        INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (
            review_id,
            asin,
            reviewer_id,
            review_timestamp,
            text_hash,
            model_version,
            embedding
        )
        VALUES %s
        ON CONFLICT (review_id)
        DO UPDATE SET
            asin = EXCLUDED.asin,
            reviewer_id = EXCLUDED.reviewer_id,
            review_timestamp = EXCLUDED.review_timestamp,
            text_hash = EXCLUDED.text_hash,
            model_version = EXCLUDED.model_version,
            embedding = EXCLUDED.embedding,
            updated_at = CURRENT_TIMESTAMP
        WHERE
            {TABLE_NAME}.text_hash IS DISTINCT FROM EXCLUDED.text_hash
            OR {TABLE_NAME}.model_version IS DISTINCT FROM EXCLUDED.model_version
    """

    with conn.cursor() as cursor:
        for batch in chunk_records(records, chunk_size):
            execute_values(
                cursor,
                sql,
                batch,
                template="(%s, %s, %s, %s, %s, %s, %s::vector)",
                page_size=len(batch),
            )
        conn.commit()


def run_load_review_embeddings_to_pgvector(ingest_dt: Optional[str] = None) -> None:
    """
    Main entrypoint for loading review embeddings from S3 to pgvector.

    This version processes one Parquet file at a time to reduce memory usage.
    """
    settings = load_settings()

    s3_client = boto3.client("s3", region_name=settings["aws_region"])
    bucket = settings["s3_bucket"]
    prefix = build_input_prefix(settings, ingest_dt=ingest_dt)

    logger.info(f"Listing parquet files under: s3://{bucket}/{prefix}")
    parquet_keys = list_parquet_keys(s3_client, bucket, prefix)

    if not parquet_keys:
        raise ValueError(f"No parquet files found under s3://{bucket}/{prefix}")

    logger.info(f"Found {len(parquet_keys)} parquet files to load.")

    conn = None
    total_loaded = 0

    try:
        conn = get_db_connection(settings)

        for key in parquet_keys:
            logger.info(f"Processing parquet file: s3://{bucket}/{key}")

            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp_file:
                s3_client.download_file(bucket, key, tmp_file.name)

                df = pd.read_parquet(tmp_file.name)

                if df.empty:
                    logger.warning(f"Skipping empty parquet file: s3://{bucket}/{key}")
                    continue

                validate_required_columns(df)

                records = prepare_records(df)

                if not records:
                    logger.warning(f"No records prepared from file: s3://{bucket}/{key}")
                    continue

                upsert_records(
                    conn=conn,
                    records=records,
                    chunk_size=settings["chunk_size"],
                )

                total_loaded += len(records)

                logger.info(
                    f"Loaded {len(records)} records from {key}. "
                    f"Total loaded so far: {total_loaded}"
                )

                del df
                del records

        logger.info(
            f"Review embeddings loaded to pgvector successfully. "
            f"Total records processed: {total_loaded}"
        )

    except Exception as exc:
        if conn is not None:
            conn.rollback()
        logger.exception(f"Loading review embeddings to pgvector failed: {exc}")
        raise

    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ingest-dt",
        required=False,
        help="Optional ingest_dt partition to load, for example: 2026-05-05",
    )
    args = parser.parse_args()

    run_load_review_embeddings_to_pgvector(ingest_dt=args.ingest_dt)