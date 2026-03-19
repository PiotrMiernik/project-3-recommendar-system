# This module provides helper functions for building ingestion manifests,
# which describe what data was written to S3 during a specific run (data ingestion run metadata).

def build_manifest(
    source_name: str,
    ingest_dt: str,
    extract_ts_utc: str,
    bucket: str,
    prefix: str,
    file_keys: list[str],
    record_count: int,
    mode: str = "one_shot",
    incremental_enabled: bool = False,
    cursor_field: str | None = None,
    watermark_in: str | int | None = None,
    watermark_out: str | int | None = None,
) -> dict:
# Builds a manifest dictionary describing a single ingestion run,
# including source, S3 location, file list, record counts, and incremental metadata.
    return {
        "manifest_version": "1.0",
        "source": {
            "name": source_name,
        },
        "run": {
            "ingest_dt": ingest_dt,
            "extract_ts_utc": extract_ts_utc,
            "mode": mode,
        },
        "s3": {
            "bucket": bucket,
            "prefix": prefix,
            "format": "jsonl.gz",
            "files": file_keys,
        },
        "counts": {
            "records": record_count,
            "files": len(file_keys),
        },
        "incremental": {
            "enabled": incremental_enabled,
            "cursor_field": cursor_field,
            "watermark_in": watermark_in,
            "watermark_out": watermark_out,
        },
    }