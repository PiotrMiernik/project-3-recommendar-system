from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.config import load_settings
from src.common.logging import get_logger
from src.common.s3_utils import read_json_from_s3


logger = get_logger(__name__)

SOURCE_SYSTEM = "mongodb"
ENTITY = "reviews"

FINAL_COLUMNS = [
    "review_id",
    "asin",
    "reviewer_id",
    "rating",
    "vote_count",
    "review_text",
    "summary",
    "review_time",
    "review_timestamp",
    "verified",
    "style",
    "ingest_dt",
    "source_system",
]


# Spark session
def create_spark() -> SparkSession:
    """
    Create and return a Spark session for the transformation job.
    """
    return (
        SparkSession.builder.appName("transform-reviews-raw-to-staging")
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
        "_id",
        "overall",
        "verified",
        "reviewTime",
        "reviewerID",
        "asin",
        "reviewText",
        "summary",
        "unixReviewTime",
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


def parse_vote(col):
    """
    Parse vote count from raw string representation.
    Handles values like '12' or '1,234'.
    Empty values are converted to null.
    """
    cleaned = F.regexp_replace(F.coalesce(col.cast("string"), F.lit("")), ",", "")
    return F.when(cleaned == "", None).otherwise(cleaned.cast("int"))


def normalize_style(col):
    """
    Ensure the style column is represented as map<string, string>.
    If null, return an empty map.
    """
    return (
        F.when(col.isNull(), F.create_map().cast(T.MapType(T.StringType(), T.StringType())))
        .otherwise(col.cast(T.MapType(T.StringType(), T.StringType())))
    )


def build_review_id() -> F.Column:
    """
    Build a deterministic technical review_id from business-relevant fields.
    This avoids dependency on MongoDB internal _id in downstream layers.
    """
    return F.sha2(
        F.concat_ws(
            "||",
            F.coalesce(F.col("asin"), F.lit("")),
            F.coalesce(F.col("reviewer_id"), F.lit("")),
            F.coalesce(F.col("unix_review_time").cast("string"), F.lit("")),
            F.coalesce(F.col("rating").cast("string"), F.lit("")),
        ),
        256,
    )


def rename_columns(df: DataFrame) -> DataFrame:
    """
    Standardize raw column names to the staging naming convention.
    """
    rename_map = {
        "overall": "rating",
        "reviewerID": "reviewer_id",
        "reviewText": "review_text",
        "reviewTime": "review_time_raw",
        "unixReviewTime": "unix_review_time",
    }

    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    return df


# Core transformation
def transform(df: DataFrame, ingest_dt: str) -> DataFrame:
    """
    Transform raw reviews data into staging schema.
    Includes:
    - column renaming
    - type normalization
    - deterministic review_id generation
    - technical validation after casting
    - deduplication by review_id
    """
    df = rename_columns(df)

    # Normalize selected string columns
    for col_name in ["asin", "reviewer_id", "review_text", "summary"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, normalize_string(F.col(col_name)))

    # Cast and enrich technical metadata
    df = (
        df.withColumn("rating", F.col("rating").cast("double"))
        .withColumn(
            "vote_count",
            parse_vote(F.col("vote")) if "vote" in df.columns else F.lit(None).cast("int"),
        )
        .withColumn("review_time", F.to_date(F.col("review_time_raw"), "MM dd, yyyy"))
        .withColumn(
            "review_timestamp",
            F.to_timestamp(F.from_unixtime(F.col("unix_review_time").cast("long")))
        )
        .withColumn(
            "verified",
            F.when(F.col("verified").isNull(), None).otherwise(F.col("verified").cast("boolean"))
        )
        .withColumn(
            "style",
            normalize_style(F.col("style"))
            if "style" in df.columns
            else F.create_map().cast(T.MapType(T.StringType(), T.StringType())),
        )
        .withColumn("ingest_dt", F.to_date(F.lit(ingest_dt)))
        .withColumn("source_system", F.lit(SOURCE_SYSTEM))
    )

    # Build deterministic technical key for deduplication and downstream joins
    df = df.withColumn("review_id", build_review_id())

    # Validate critical fields after transformation
    invalid_asin_count = df.filter(F.col("asin").isNull()).count()
    if invalid_asin_count > 0:
        raise ValueError(f"Found {invalid_asin_count} records with null asin after normalization.")

    invalid_reviewer_id_count = df.filter(F.col("reviewer_id").isNull()).count()
    if invalid_reviewer_id_count > 0:
        raise ValueError(
            f"Found {invalid_reviewer_id_count} records with null reviewer_id after normalization."
        )

    invalid_rating_count = df.filter(F.col("rating").isNull()).count()
    if invalid_rating_count > 0:
        raise ValueError(f"Found {invalid_rating_count} records with null rating after casting.")

    invalid_review_time_count = df.filter(F.col("review_time").isNull()).count()
    if invalid_review_time_count > 0:
        raise ValueError(
            f"Found {invalid_review_time_count} records with null review_time after parsing."
        )

    invalid_review_timestamp_count = df.filter(F.col("review_timestamp").isNull()).count()
    if invalid_review_timestamp_count > 0:
        raise ValueError(
            f"Found {invalid_review_timestamp_count} records with null review_timestamp after parsing."
        )

    invalid_ingest_dt_count = df.filter(F.col("ingest_dt").isNull()).count()
    if invalid_ingest_dt_count > 0:
        raise ValueError("ingest_dt could not be parsed.")

    invalid_review_id_count = df.filter(F.col("review_id").isNull()).count()
    if invalid_review_id_count > 0:
        raise ValueError(f"Found {invalid_review_id_count} records with null review_id.")

    # Keep only the latest version of each review_id
    window = Window.partitionBy("review_id").orderBy(
        F.col("review_timestamp").desc_nulls_last(),
        F.col("vote_count").desc_nulls_last(),
    )

    df = (
        df.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn", "_id", "vote", "review_time_raw", "unix_review_time", "reviewerName")
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
def run_reviews_transformation(manifest_key: str) -> None:
    """
    Main entrypoint for Airflow.
    Runs the full raw -> staging transformation for reviews
    based on a specific ingestion manifest.
    """
    settings = load_settings()
    spark = create_spark()

    try:
        logger.info("Starting raw -> staging transformation for reviews")
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

        logger.info("Reviews transformation finished successfully.")

    except Exception as exc:
        logger.exception(f"Reviews transformation failed: {exc}")
        raise
    finally:
        try:
            if "staging_df" in locals():
                staging_df.unpersist()
        finally:
            spark.stop()


# Script entrypoint for EMR Serverless
if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        raise ValueError(
            "Expected exactly one argument: manifest_key. "
            "Usage: spark-submit transform_raw_reviews_to_staging.py <manifest_key>"
        )

    manifest_key = sys.argv[1]
    run_reviews_transformation(manifest_key=manifest_key)