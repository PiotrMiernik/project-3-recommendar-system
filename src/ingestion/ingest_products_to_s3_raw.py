# This script extracts product data from PostgreSQL table products_raw
# and uploads it to Amazon S3 Raw layer in JSONL.GZ format.

from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import RealDictCursor

from common.config import load_settings
from common.logging import get_logger
from common.manifest import build_manifest
from common.s3_utils import (
    build_manifest_key,
    build_part_key,
    build_raw_prefix,
    upload_json_to_s3,
    upload_jsonl_gz_to_s3,
)


def fetch_products_in_batches(connection, chunk_size: int):
    """
    Fetch product records from PostgreSQL in batches using a server-side cursor.

    Args:
        connection: psycopg2 database connection.
        chunk_size (int): Number of records fetched per batch.

    Yields:
        list[dict]: Batch of product records as dictionaries.
    """
    # Use a named cursor (server-side) to avoid loading all data into memory
    # Named cursors are managed by the server, which is better for large datasets
    with connection.cursor(name="products_raw_cursor", cursor_factory=RealDictCursor) as cursor:
        cursor.itersize = chunk_size
        cursor.execute("SELECT * FROM products_raw")

        # Fetch data in chunks
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            yield [dict(row) for row in rows]


def main() -> None:
    """
    Extract product data from PostgreSQL and upload it to S3 Raw layer.
    The data is written in chunked JSONL.GZ files, followed by creation
    and upload of a manifest file describing the ingestion run.

    Returns:
        None
    """
    # Load configuration from .env file
    settings = load_settings()

    # Initialize logger for this ingestion job
    logger = get_logger("ingest_products")

    # Define ingestion date (used for S3 partitioning) and extraction timestamp
    ingest_dt = datetime.now().strftime("%Y-%m-%d")
    extract_ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    bucket = settings["s3_bucket"]
    region = settings["aws_region"]
    chunk_size = settings["chunk_size"]

    logger.info("Starting PostgreSQL to S3 Raw ingestion for products")

    # Create PostgreSQL connection using credentials from config
    connection = psycopg2.connect(
        host=settings["rds_host"],
        port=settings["rds_port"],
        dbname=settings["rds_dbname"],
        user=settings["rds_user"],
        password=settings["rds_password"],
    )

    try:
        # 1. Get total number of records in data source before starting
        with connection.cursor() as temp_cursor:
            temp_cursor.execute("SELECT COUNT(*) FROM products_raw")
            total_in_db = temp_cursor.fetchone()[0]
        
        logger.info(f"Total rows found in Postgres table: {total_in_db}")

        uploaded_file_keys = []
        total_records = 0

        # 2. Iterate over batches fetched from PostgreSQL
        for part_number, batch in enumerate(fetch_products_in_batches(connection, chunk_size)):
            # Build S3 key for current chunk
            part_key = build_part_key("products", ingest_dt, part_number)

            logger.info(
                f"Uploading batch {part_number} with {len(batch)} records to s3://{bucket}/{part_key}"
            )

            # Upload batch as compressed JSON Lines file
            upload_jsonl_gz_to_s3(
                bucket=bucket,
                key=part_key,
                records=batch,
                region=region,
            )

            # Track uploaded files and total number of records
            uploaded_file_keys.append(part_key)
            total_records += len(batch)

        # 3. Build manifest describing the ingestion run
        manifest = build_manifest(
            source_name="products",
            ingest_dt=ingest_dt,
            extract_ts_utc=extract_ts_utc,
            bucket=bucket,
            prefix=build_raw_prefix("products", ingest_dt),
            file_keys=uploaded_file_keys,
            record_count=total_records,
            mode="one_shot",
            incremental_enabled=False,
            cursor_field=None,
            watermark_in=None,
            watermark_out=None,
        )

        # Build S3 key for manifest file
        manifest_key = build_manifest_key("products", ingest_dt)

        logger.info(f"Uploading manifest to s3://{bucket}/{manifest_key}")

        # Upload manifest JSON to S3
        upload_json_to_s3(
            bucket=bucket,
            key=manifest_key,
            payload=manifest,
            region=region,
        )

        logger.info(
            f"Finished products ingestion successfully. Uploaded {total_records} records "
            f"into {len(uploaded_file_keys)} file(s)."
        )

        # 4. Final data integrity check
        if total_records == total_in_db:
            logger.info(f"SUCCESS: Data integrity check passed ({total_records}/{total_in_db})")
        else:
            logger.warning(
                f"WARNING: Data mismatch! Source has {total_in_db} records, "
                f"but only {total_records} were uploaded."
            )

    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}")
        raise

    finally:
        # Ensure database connection is always closed
        connection.close()
        logger.info("PostgreSQL connection closed")


if __name__ == "__main__":
    # Entry point for script execution
    main()