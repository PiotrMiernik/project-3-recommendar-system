from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.config import load_settings
from src.common.logging import get_logger
from src.common.s3_utils import read_json_from_s3


logger = get_logger(__name__)

SOURCE_SYSTEM = "postgres"
ENTITY = "products"

FINAL_COLUMNS = [
    "asin",
    "title",
    "brand",
    "main_category",
    "categories",
    "description",
    "features",
    "also_buy",
    "also_view",
    "rank_raw",
    "price",
    "similar_item",
    "date_raw",
    "ingest_dt",
    "updated_at",
    "source_system",
]


# Spark session
def create_spark() -> SparkSession:
    """
    Create and return a Spark session for the transformation job.
    """
    return (
        SparkSession.builder.appName("transform-products-raw-to-staging")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


# Manifest handling
def load_manifest(settings: dict, manifest_key: str) -> dict:
    """
    Load ingestion manifest from S3.
    The manifest defines which raw files belong to the current ingestion run.
    """
    manifest = read_json_from_s3(
        bucket=settings["s3_bucket"],
        key=manifest_key,
        region=settings["aws_region"],
    )

    if manifest is None:
        raise ValueError(f"Manifest not found: {manifest_key}")

    return manifest


def extract_input_paths(settings: dict, manifest: dict) -> list[str]:
    """
    Resolve full S3 input paths from manifest file entries.
    """
    bucket = settings["s3_bucket"]
    files = manifest["s3"]["files"]

    if not files:
        raise ValueError("Manifest contains no input files.")

    return [f"s3://{bucket}/{key}" for key in files]


def extract_ingest_dt(manifest: dict) -> str:
    """
    Extract ingest date from manifest metadata.
    """
    ingest_dt = manifest["run"]["ingest_dt"]

    if not ingest_dt:
        raise ValueError("Manifest does not contain ingest_dt.")

    return ingest_dt


# Technical validation
def validate_required_columns(df: DataFrame) -> None:
    """
    Validate that the raw dataset contains the full expected input contract.
    This is a technical schema validation, not a business-quality check.
    """
    required = {
        "asin",
        "title",
        "brand",
        "main_cat",
        "category",
        "description",
        "feature",
        "rank",
        "also_buy",
        "also_view",
        "similar_item",
        "date_raw",
        "price_raw",
        "ingest_dt",
        "updated_at",
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")


# Column normalization helpers
def normalize_string(col):
    """
    Trim string values and convert empty strings to null.
    """
    return F.when(F.trim(col) == "", None).otherwise(F.trim(col))


def ensure_array(col):
    """
    Ensure a column is represented as array<string>.
    If the value is null, return an empty array.
    If the value is scalar, wrap it into a single-element array.
    """
    return (
        F.when(col.isNull(), F.array().cast("array<string>"))
        .when(col.cast("array<string>").isNotNull(), col.cast("array<string>"))
        .otherwise(F.array(col.cast("string")))
    )


def parse_price(col):
    """
    Extract numeric price from raw price string.
    Invalid or non-numeric values are converted to null.
    """
    value = F.regexp_extract(col, r"(\d+(?:\.\d+)?)", 1)
    return F.when(value == "", None).otherwise(value.cast("double"))


def rename_columns(df: DataFrame) -> DataFrame:
    """
    Standardize raw column names to the staging naming convention.
    """
    return (
        df.withColumnRenamed("main_cat", "main_category")
        .withColumnRenamed("category", "categories")
        .withColumnRenamed("feature", "features")
        .withColumnRenamed("rank", "rank_raw")
    )


# Core transformation
def transform(df: DataFrame, ingest_dt: str) -> DataFrame:
    """
    Transform raw products data into staging schema.
    Includes:
    - column renaming
    - type normalization
    - technical validation after casting
    - deduplication by asin
    """
    df = rename_columns(df)

    # Normalize selected string columns
    for col_name in ["asin", "title", "brand", "main_category", "similar_item", "date_raw"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, normalize_string(F.col(col_name)))

    # Normalize selected array-like columns
    for col_name in ["categories", "description", "features", "rank_raw", "also_buy", "also_view"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, ensure_array(F.col(col_name)))

    # Cast and enrich technical metadata
    df = (
        df.withColumn("price", parse_price(F.col("price_raw")))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("ingest_dt", F.to_date(F.lit(ingest_dt)))
        .withColumn("source_system", F.lit(SOURCE_SYSTEM))
    )

    # Validate critical fields after transformation
    invalid_asin_count = df.filter(F.col("asin").isNull()).count()
    if invalid_asin_count > 0:
        raise ValueError(f"Found {invalid_asin_count} records with null asin after normalization.")

    invalid_updated_at_count = df.filter(F.col("updated_at").isNull()).count()
    if invalid_updated_at_count > 0:
        raise ValueError(
            f"Found {invalid_updated_at_count} records with null updated_at after parsing."
        )

    invalid_ingest_dt_count = df.filter(F.col("ingest_dt").isNull()).count()
    if invalid_ingest_dt_count > 0:
        raise ValueError("ingest_dt could not be parsed.")

    # Keep only the latest version of each product
    window = Window.partitionBy("asin").orderBy(F.col("updated_at").desc_nulls_last())

    df = (
        df.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn", "price_raw")
    )

    # Enforce final staging schema and column order
    df = df.select(*FINAL_COLUMNS)

    return df


# Output writing
def write_output(df: DataFrame, settings: dict) -> None:
    """
    Write transformed data to S3 staging as Parquet,
    partitioned by ingest_dt.
    """
    bucket = settings["s3_bucket"]
    prefix = settings["s3_silver_prefix"]  # kept as-is to match current config.py

    output_path = f"s3://{bucket}/{prefix}{ENTITY}"
    logger.info(f"Writing output to: {output_path}")

    (
        df.write
        .mode("overwrite")
        .partitionBy("ingest_dt")
        .parquet(output_path)
    )


# Airflow entrypoint
def run_products_transformation(manifest_key: str) -> None:
    """
    Main entrypoint for Airflow.
    Runs the full raw -> staging transformation for products
    based on a specific ingestion manifest.
    """
    settings = load_settings()
    spark = create_spark()

    try:
        logger.info("Starting raw -> staging transformation for products")
        logger.info(f"Manifest key: {manifest_key}")

        manifest = load_manifest(settings, manifest_key)
        input_paths = extract_input_paths(settings, manifest)
        ingest_dt = extract_ingest_dt(manifest)

        logger.info(f"Resolved ingest_dt: {ingest_dt}")
        logger.info(f"Input files count: {len(input_paths)}")

        raw_df = spark.read.json(input_paths)

        validate_required_columns(raw_df)

        input_count = raw_df.count()
        if input_count == 0:
            raise ValueError("Input DataFrame is empty.")

        logger.info(f"Input records count: {input_count}")

        # Persist the transformed DataFrame because it is reused for count() and write()
        staging_df = transform(raw_df, ingest_dt).persist(StorageLevel.MEMORY_AND_DISK)

        output_count = staging_df.count()
        if output_count == 0:
            raise ValueError("Output DataFrame is empty after transformation.")

        logger.info(f"Output records count: {output_count}")

        write_output(staging_df, settings)

        logger.info("Products transformation finished successfully.")

    except Exception as exc:
        logger.exception(f"Products transformation failed: {exc}")
        raise
    finally:
        try:
            if "staging_df" in locals():
                staging_df.unpersist()
        finally:
            spark.stop()