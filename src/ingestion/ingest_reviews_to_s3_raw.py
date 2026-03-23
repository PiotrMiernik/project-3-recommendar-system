# This script extracts review data from MongoDB collection reviews_raw
# and uploads it to Amazon S3 Raw layer in JSONL.GZ format.

from datetime import datetime, timezone

from pymongo import MongoClient

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


def fetch_reviews_in_batches(collection, chunk_size: int):
    """
    Fetch review documents from MongoDB in batches using a cursor.

    Args:
        collection: MongoDB collection object.
        chunk_size (int): Number of documents fetched per batch.

    Yields:
        list[dict]: Batch of review documents as dictionaries.
    """
    # Create a cursor to iterate through the full collection in batches
    cursor = collection.find({}, no_cursor_timeout=True).batch_size(chunk_size)

    batch = []

    try:
        for document in cursor:
            # Convert MongoDB ObjectId and other non-JSON-native values to strings later during serialization
            batch.append(document)

            if len(batch) == chunk_size:
                yield batch
                batch = []

        if batch:
            yield batch

    finally:
        # Ensure cursor is always closed
        cursor.close()


def main() -> None:
    """
    Extract review data from MongoDB and upload it to S3 Raw layer.
    The data is written in chunked JSONL.GZ files, followed by creation
    and upload of a manifest file describing the ingestion run.

    Returns:
        None
    """
    # Load configuration from .env file
    settings = load_settings()

    # Initialize logger for this ingestion job
    logger = get_logger("ingest_reviews")

    # Define ingestion date (used for S3 partitioning) and extraction timestamp
    ingest_dt = datetime.now().strftime("%Y-%m-%d")
    extract_ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    bucket = settings["s3_bucket"]
    region = settings["aws_region"]
    chunk_size = settings["chunk_size"]

    logger.info("Starting MongoDB to S3 Raw ingestion for reviews")

    # Create MongoDB client and connect to the source collection
    client = MongoClient(settings["mongodb_uri"])
    db = client[settings["mongodb_db"]]
    collection = db[settings["mongodb_collection_reviews"]]

    uploaded_file_keys = []
    total_records = 0

    try:
        # Iterate over batches fetched from MongoDB
        for part_number, batch in enumerate(fetch_reviews_in_batches(collection, chunk_size)):
            # Build S3 key for current chunk
            part_key = build_part_key("reviews", ingest_dt, part_number)

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

        # Build manifest describing the ingestion run
        manifest = build_manifest(
            source_name="reviews",
            ingest_dt=ingest_dt,
            extract_ts_utc=extract_ts_utc,
            bucket=bucket,
            prefix=build_raw_prefix("reviews", ingest_dt),
            file_keys=uploaded_file_keys,
            record_count=total_records,
            mode="one_shot",
            incremental_enabled=False,
            cursor_field="unixReviewTime",
            watermark_in=None,
            watermark_out=None,
        )

        # Build S3 key for manifest file
        manifest_key = build_manifest_key("reviews", ingest_dt)

        logger.info(f"Uploading manifest to s3://{bucket}/{manifest_key}")

        # Upload manifest JSON to S3
        upload_json_to_s3(
            bucket=bucket,
            key=manifest_key,
            payload=manifest,
            region=region,
        )

        logger.info(
            f"Finished reviews ingestion successfully. Uploaded {total_records} records "
            f"into {len(uploaded_file_keys)} file(s)."
        )

    finally:
        # Ensure MongoDB client is always closed
        client.close()
        logger.info("MongoDB connection closed")


if __name__ == "__main__":
    # Entry point for script execution
    main()