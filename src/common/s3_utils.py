# This module provides helper functions for working with Amazon S3,
# including building S3 paths, uploading data, and reading JSON objects.

import gzip
import json
from io import BytesIO

import boto3
from botocore.exceptions import ClientError

# Creates and returns a boto3 S3 client, optionally for a specific region.
def get_s3_client(region: str | None = None):
    if region:
        return boto3.client("s3", region_name=region)
    return boto3.client("s3")

# Builds the base S3 prefix for raw data for a given entity and ingestion date.
def build_raw_prefix(entity: str, ingest_dt: str) -> str:
    return f"raw/{entity}/ingest_dt={ingest_dt}/"

# Builds the S3 key for a single data chunk (part file).
def build_part_key(entity: str, ingest_dt: str, part_number: int) -> str:
    return f"{build_raw_prefix(entity, ingest_dt)}part-{part_number:05d}.jsonl.gz"

# Builds the S3 key for the manifest file corresponding to a given ingestion run.
def build_manifest_key(entity: str, ingest_dt: str) -> str:
    return f"{build_raw_prefix(entity, ingest_dt)}_manifest.json"

# Builds the S3 key for storing pipeline state (e.g., watermark) for a given entity.
def build_state_key(entity: str) -> str:
    return f"raw/_state/{entity}.json"

# Converts (serializes) a list of records into JSON Lines format, compresses it with gzip,
# and uploads the result to S3.
def upload_jsonl_gz_to_s3(
    bucket: str,
    key: str,
    records: list[dict],
    region: str | None = None,
) -> None:
    buffer = BytesIO()

    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz_file:
        for record in records:
            line = json.dumps(record, ensure_ascii=False, default=str) + "\n"
            gz_file.write(line.encode("utf-8"))

    buffer.seek(0)

    s3_client = get_s3_client(region)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/json",
        ContentEncoding="gzip",
    )

# Serializes a dictionary to JSON and uploads it to S3 (used for manifest or state files).
def upload_json_to_s3(
    bucket: str,
    key: str,
    payload: dict,
    region: str | None = None,
) -> None:
    body = json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8")

    s3_client = get_s3_client(region)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )

# Reads a JSON object from S3 and returns it as a dictionary.
# Returns None if the object does not exist.
def read_json_from_s3(
    bucket: str,
    key: str,
    region: str | None = None,
) -> dict | None:
    s3_client = get_s3_client(region)

    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        if error_code in {"NoSuchKey", "404"}:
            return None
        raise