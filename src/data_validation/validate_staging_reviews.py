import json
from pathlib import Path

import great_expectations as gx
import pandas as pd

from src.common.config import load_settings
from src.common.logging import get_logger


logger = get_logger(__name__)

ENTITY = "reviews"


# S3 path resolution
def build_partition_path(settings: dict, ingest_dt: str) -> str:
    """
    Build S3 path for a specific staging partition.
    Example:
    s3://bucket/staging/reviews/ingest_dt=YYYY-MM-DD/
    """
    bucket = settings["s3_bucket"]
    prefix = settings["s3_staging_prefix"]  # kept as-is to match current config.py
    return f"s3://{bucket}/{prefix}{ENTITY}/ingest_dt={ingest_dt}/"


# Data loading
def read_staging_partition(partition_path: str, aws_region: str) -> pd.DataFrame:
    """
    Load Parquet data from S3 staging into a Pandas DataFrame.
    Uses pyarrow + s3fs under the hood.
    """
    logger.info(f"Reading staging data from: {partition_path}")

    df = pd.read_parquet(
        partition_path,
        engine="pyarrow",
        storage_options={"client_kwargs": {"region_name": aws_region}},
    )

    if df.empty:
        raise ValueError(f"Staging partition is empty: {partition_path}")

    return df


# Great Expectations setup
def build_batch(df: pd.DataFrame):
    """
    Create a Great Expectations batch from a Pandas DataFrame.
    """
    context = gx.get_context()

    data_source = context.data_sources.add_pandas("staging_reviews_ds")
    data_asset = data_source.add_dataframe_asset(name="staging_reviews_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        "staging_reviews_batch"
    )

    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    return batch


# Expectations
def build_expectations():
    """
    Define all expectations for staging_reviews.
    """
    return [
        # asin must not be null -> each review must be linked to a product
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="asin",
            severity="critical",
        ),

        # reviewer_id must not be null -> each review must be linked to a user
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="reviewer_id",
            severity="critical",
        ),

        # review_id must be unique -> confirms deduplication worked correctly
        gx.expectations.ExpectColumnValuesToBeUnique(
            column="review_id",
            severity="critical",
        ),

        # rating must be within the expected review scale
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="rating",
            min_value=1.0,
            max_value=5.0,
            severity="critical",
        ),

        # review_time must not be null -> confirms date parsing worked correctly
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="review_time",
            severity="critical",
        ),

        # review_text should be string-like -> useful for downstream NLP and embeddings
        gx.expectations.ExpectColumnValuesToBeInTypeList(
            column="review_text",
            type_list=["str", "string"],
            severity="warning",
        ),
    ]


# Validation helpers
def normalize_result(result) -> dict:
    """
    Normalize a GX result object to dictionary format.
    Handles multiple GX return types.
    """
    if hasattr(result, "to_json_dict"):
        return result.to_json_dict()

    if isinstance(result, dict):
        return result

    if hasattr(result, "model_dump"):
        return result.model_dump()

    return {"raw_result": str(result)}


def extract_success(result_dict: dict) -> bool:
    """
    Extract the success flag from a GX result dictionary.
    """
    return bool(result_dict.get("success", False))


def run_validation(df: pd.DataFrame):
    """
    Execute all expectations against the dataset.
    Returns overall success flag and detailed results.
    """
    batch = build_batch(df)
    expectations = build_expectations()

    results = []

    for expectation in expectations:
        result = batch.validate(expectation)
        result_dict = normalize_result(result)

        results.append(result_dict)

        expectation_type = (
            result_dict.get("expectation_config", {}).get("type")
            or result_dict.get("expectation_type")
            or type(expectation).__name__
        )

        success = extract_success(result_dict)

        result_details = result_dict.get("result", {})
        unexpected_count = result_details.get("unexpected_count")
        unexpected_percent = result_details.get("unexpected_percent")
        partial_unexpected = result_details.get("partial_unexpected_list")

        logger.info(
            f"{expectation_type} -> success={success} | "
            f"unexpected_count={unexpected_count} | "
            f"unexpected_percent={unexpected_percent}"
        )

        if not success and partial_unexpected:
            logger.warning(
                f"{expectation_type} sample unexpected values: {partial_unexpected[:5]}"
            )

    overall_success = all(extract_success(r) for r in results)

    return overall_success, results


# Save results
def save_results(
    output_dir: str,
    ingest_dt: str,
    partition_path: str,
    success: bool,
    results: list,
):
    """
    Save validation results to a local JSON file.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    output_path = Path(output_dir) / f"validate_staging_reviews_{ingest_dt}.json"

    payload = {
        "entity": ENTITY,
        "ingest_dt": ingest_dt,
        "partition_path": partition_path,
        "success": success,
        "expectations": results,
    }

    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )

    return output_path


# Airflow entrypoint
def run_staging_reviews_validation(
    ingest_dt: str,
    output_dir: str = "gx_results",
) -> None:
    """
    Main entrypoint for Airflow.
    Runs Great Expectations validation for a single staging reviews partition.
    """
    settings = load_settings()

    logger.info("Starting validation for staging_reviews")
    logger.info(f"Target ingest_dt: {ingest_dt}")

    partition_path = build_partition_path(settings, ingest_dt)

    try:
        df = read_staging_partition(partition_path, settings["aws_region"])

        logger.info(f"Rows: {len(df)}")
        logger.info(f"Columns: {list(df.columns)}")

        success, results = run_validation(df)

        result_path = save_results(
            output_dir=output_dir,
            ingest_dt=ingest_dt,
            partition_path=partition_path,
            success=success,
            results=results,
        )

        logger.info(f"Validation results saved: {result_path}")
        logger.info(f"Validation success: {success}")

        if not success:
            raise ValueError("Validation failed for staging_reviews")

    except Exception as exc:
        logger.exception(f"Validation failed for staging_reviews: {exc}")
        raise