"""
Validation script for mlready.product_features using Great Expectations.

This script:
- reads the Iceberg table through Spark
- runs lightweight data quality checks
- saves validation results to a local JSON file in the repository

It is designed to be executed locally, not on EMR.
"""
import json
from pathlib import Path

import great_expectations as gx
from pyspark.sql import SparkSession

from src.common.config import load_emr_transform_settings, get_iceberg_settings
from src.common.logging import get_logger


logger = get_logger(__name__)

ENTITY = "product_features"
iceberg_cfg = get_iceberg_settings()


# Spark / Iceberg setup
def create_spark_session(app_name: str) -> SparkSession:
    """
    Create a local Spark session with Iceberg runtime and Glue catalog support.
    """
    settings = load_emr_transform_settings()
    catalog_name = iceberg_cfg["catalog_name"]
    warehouse_path = f"s3://{settings['s3_bucket']}/{settings['s3_mlready_prefix']}"

    iceberg_version = "1.10.1"
    dependencies = (
        f"org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:{iceberg_version},"
        f"org.apache.iceberg:iceberg-aws-bundle:{iceberg_version}"
    )

    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", dependencies)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{catalog_name}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            f"spark.sql.catalog.{catalog_name}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config(
            f"spark.sql.catalog.{catalog_name}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", warehouse_path)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
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
    context = gx.get_context()

    data_source = context.data_sources.add_spark(f"mlready_{ENTITY}_ds")
    data_asset = data_source.add_dataframe_asset(name=f"mlready_{ENTITY}_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        f"mlready_{ENTITY}_batch"
    )

    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    return batch


# Expectations
def build_expectations():
    """
    Define all expectations for mlready.product_features.
    Expectations are aligned with the current build_mlready_product_features output schema.
    """
    return [
        # Primary/business key checks
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="asin",
            severity="critical",
        ),
        gx.expectations.ExpectColumnValuesToBeUnique(
            column="asin",
            severity="critical",
        ),

        # Structural checks for required columns
        gx.expectations.ExpectColumnToExist(column="title"),
        gx.expectations.ExpectColumnToExist(column="brand"),
        gx.expectations.ExpectColumnToExist(column="main_category"),
        gx.expectations.ExpectColumnToExist(column="price"),
        gx.expectations.ExpectColumnToExist(column="has_brand"),
        gx.expectations.ExpectColumnToExist(column="has_price"),
        gx.expectations.ExpectColumnToExist(column="title_length"),
        gx.expectations.ExpectColumnToExist(column="description_total_length"),
        gx.expectations.ExpectColumnToExist(column="feature_snapshot_date"),

        # Snapshot date must be present
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="feature_snapshot_date",
            severity="critical",
        ),

        # Numeric feature columns must be non-negative
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="title_length",
            min_value=0,
            severity="critical",
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="description_total_length",
            min_value=0,
            severity="critical",
        ),

        # Price may be null, but if present should be non-negative
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="price",
            min_value=0,
            mostly=0.95,
            severity="warning",
        ),

        # Boolean columns should only contain True / False
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="has_brand",
            value_set=[True, False],
            severity="critical",
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="has_price",
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
def run_mlready_product_features_validation(
    execution_date: str,
    output_dir: str = "src/data_validation/gx_results",
) -> None:
    """
    Main entrypoint for Airflow.
    Runs Great Expectations validation for mlready.product_features.
    """
    table_name = iceberg_cfg["table_products"]
    spark = create_spark_session("validate-mlready-product-features")

    logger.info("Starting validation for mlready.product_features")
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
            raise ValueError("Validation failed for mlready.product_features")

    except Exception as exc:
        logger.exception(f"Validation failed for mlready.product_features: {exc}")
        raise

    finally:
        spark.stop()