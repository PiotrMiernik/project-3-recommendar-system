import json
from pathlib import Path

import great_expectations as gx
from great_expectations.dataset import SparkDFDataset
from pyspark.sql import SparkSession

from src.common.config import load_emr_transform_settings, get_iceberg_settings
from src.common.logging import get_logger


logger = get_logger(__name__)

ENTITY = "user_product_interactions"
iceberg_cfg = get_iceberg_settings()


# Spark / Iceberg setup
def create_spark_session(app_name: str) -> SparkSession:
    """
    Create a local Spark session with Iceberg + Glue catalog support.
    """
    settings = load_emr_transform_settings()
    catalog_name = iceberg_cfg["catalog_name"]
    warehouse_path = f"s3://{settings['s3_bucket']}/{settings['s3_mlready_prefix']}"

    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)
        .getOrCreate()
    )


# Data loading
def read_mlready_table(spark: SparkSession, table_name: str):
    """
    Load the mlready Iceberg table into a Spark DataFrame.
    """
    logger.info(f"Reading mlready table: {table_name}")

    df = spark.read.table(table_name)

    if df.rdd.isEmpty():
        raise ValueError(f"ML-ready table is empty: {table_name}")

    return df


# Great Expectations setup
def build_batch(df):
    """
    Create a Great Expectations batch from a Spark DataFrame.
    """
    return SparkDFDataset(df)


# Expectations
def build_expectations():
    """
    Define all expectations for mlready.user_product_interactions.
    """
    return [
        # Primary/business key checks
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="review_id",
            severity="critical",
        ),
        gx.expectations.ExpectColumnValuesToBeUnique(
            column="review_id",
            severity="critical",
        ),

        # Required identifier columns
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="reviewer_id",
            severity="critical",
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="asin",
            severity="critical",
        ),

        # Structural checks for required columns
        gx.expectations.ExpectColumnToExist(column="rating"),
        gx.expectations.ExpectColumnToExist(column="vote_count"),
        gx.expectations.ExpectColumnToExist(column="verified"),
        gx.expectations.ExpectColumnToExist(column="review_text_length"),
        gx.expectations.ExpectColumnToExist(column="summary_length"),
        gx.expectations.ExpectColumnToExist(column="review_timestamp"),
        gx.expectations.ExpectColumnToExist(column="review_date"),
        gx.expectations.ExpectColumnToExist(column="product_main_category"),
        gx.expectations.ExpectColumnToExist(column="product_brand"),
        gx.expectations.ExpectColumnToExist(column="product_price"),
        gx.expectations.ExpectColumnToExist(column="product_has_description"),
        gx.expectations.ExpectColumnToExist(column="product_title_length"),
        gx.expectations.ExpectColumnToExist(column="product_description_length"),
        gx.expectations.ExpectColumnToExist(column="user_reviews_count"),
        gx.expectations.ExpectColumnToExist(column="user_avg_rating"),
        gx.expectations.ExpectColumnToExist(column="user_verified_reviews_ratio"),
        gx.expectations.ExpectColumnToExist(column="user_active_days_span"),
        gx.expectations.ExpectColumnToExist(column="product_reviews_count"),
        gx.expectations.ExpectColumnToExist(column="product_avg_rating"),
        gx.expectations.ExpectColumnToExist(column="product_positive_reviews_ratio"),
        gx.expectations.ExpectColumnToExist(column="product_negative_reviews_ratio"),
        gx.expectations.ExpectColumnToExist(column="label"),
        gx.expectations.ExpectColumnToExist(column="feature_snapshot_date"),

        # Critical completeness checks
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="review_timestamp",
            severity="critical",
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="review_date",
            severity="critical",
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="feature_snapshot_date",
            severity="critical",
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="label",
            severity="critical",
        ),

        # Rating / label range checks
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="rating",
            min_value=1,
            max_value=5,
            severity="critical",
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="label",
            min_value=1,
            max_value=5,
            severity="critical",
        ),

        # Interaction-level numeric checks
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="vote_count",
            min_value=0,
            severity="critical",
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="review_text_length",
            min_value=0,
            severity="critical",
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="summary_length",
            min_value=0,
            severity="critical",
        ),

        # Product feature checks
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="product_title_length",
            min_value=0,
            severity="critical",
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="product_description_length",
            min_value=0,
            severity="critical",
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="product_price",
            min_value=0,
            mostly=0.95,
            severity="warning",
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="product_has_description",
            value_set=[True, False],
            severity="critical",
        ),

        # User feature checks
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="user_reviews_count",
            min_value=1,
            severity="critical",
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="user_avg_rating",
            min_value=1,
            max_value=5,
            severity="critical",
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="user_verified_reviews_ratio",
            min_value=0,
            max_value=1,
            severity="critical",
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="user_active_days_span",
            min_value=0,
            severity="critical",
        ),

        # Product stats checks
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="product_reviews_count",
            min_value=1,
            severity="critical",
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="product_avg_rating",
            min_value=1,
            max_value=5,
            severity="critical",
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="product_positive_reviews_ratio",
            min_value=0,
            max_value=1,
            severity="critical",
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="product_negative_reviews_ratio",
            min_value=0,
            max_value=1,
            severity="critical",
        ),

        # Boolean column domain check
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="verified",
            value_set=[True, False],
            severity="critical",
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


def run_validation(df):
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
    execution_date: str,
    table_name: str,
    success: bool,
    results: list,
):
    """
    Save validation results to a local JSON file.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    output_path = Path(output_dir) / f"validate_mlready_{ENTITY}_{execution_date}.json"

    payload = {
        "entity": ENTITY,
        "execution_date": execution_date,
        "table_name": table_name,
        "success": success,
        "expectations": results,
    }

    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )

    return output_path


# Airflow entrypoint
def run_mlready_user_product_interactions_validation(
    execution_date: str,
    output_dir: str = "src/data_validation/gx_results",
) -> None:
    """
    Main entrypoint for Airflow.
    Runs Great Expectations validation for mlready.user_product_interactions.
    """
    table_name = iceberg_cfg["table_interactions"]
    spark = create_spark_session("validate-mlready-user-product-interactions")

    logger.info("Starting validation for mlready.user_product_interactions")
    logger.info(f"Target execution_date: {execution_date}")
    logger.info(f"Target table: {table_name}")

    try:
        df = read_mlready_table(spark, table_name)

        logger.info(f"Rows: {df.count()}")
        logger.info(f"Columns: {list(df.columns)}")

        success, results = run_validation(df)

        result_path = save_results(
            output_dir=output_dir,
            execution_date=execution_date,
            table_name=table_name,
            success=success,
            results=results,
        )

        logger.info(f"Validation results saved: {result_path}")
        logger.info(f"Validation success: {success}")

        if not success:
            raise ValueError("Validation failed for mlready.user_product_interactions")

    except Exception as exc:
        logger.exception(f"Validation failed for mlready.user_product_interactions: {exc}")
        raise

    finally:
        spark.stop()