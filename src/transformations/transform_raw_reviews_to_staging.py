from typing import List, Optional, Dict, Any
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T 

from src.common.config import load_emr_transform_settings
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
def load_manifest(settings: Dict[str, Any], manifest_key: str) -> Dict[str, Any]:
    """
    Load ingestion manifest from S3.
    """
    manifest = read_json_from_s3(
        bucket=settings["s3_bucket"],
        key=manifest_key,
        region=settings["aws_region"],
    )

    if manifest is None:
        raise ValueError(f"Manifest not found: {manifest_key}")

    return manifest

# CHANGED: list[str] -> List[str] for Python 3.9 compatibility
def extract_input_paths(settings: Dict[str, Any], manifest: Dict[str, Any]) -> List[str]:
    """
    Resolve full S3 input paths from manifest file entries.
    """
    bucket = settings["s3_bucket"]
    files = manifest["s3"]["files"]

    if not files:
        raise ValueError("Manifest contains no input files.")

    return [f"s3://{bucket}/{key}" for key in files]

def extract_ingest_dt(manifest: Dict[str, Any]) -> str:
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
    """
    required = {
        "_id", "overall", "verified", "reviewTime",
        "reviewerID", "asin", "reviewText", "summary", "unixReviewTime",
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

# Column normalization helpers
def normalize_string(col: F.Column) -> F.Column:
    """
    Trim string values and convert empty strings to null.
    """
    return F.when(F.trim(col) == "", None).otherwise(F.trim(col))

def parse_vote(col: F.Column) -> F.Column:
    """
    Parse vote count from raw string representation.
    """
    cleaned = F.regexp_replace(F.coalesce(col.cast("string"), F.lit("")), ",", "")
    return F.when(cleaned == "", None).otherwise(cleaned.cast("int"))

def normalize_style(col: F.Column) -> F.Column:
    """
    Ensure the style column is represented as map<string, string>.
    """
    # Defensive casting to ensure downstream compatibility
    return (
        F.when(col.isNull(), F.create_map().cast(T.MapType(T.StringType(), T.StringType())))
        .otherwise(col.cast(T.MapType(T.StringType(), T.StringType())))
    )

def build_review_id() -> F.Column:
    """
    Build a deterministic technical review_id.
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
    Standardize raw column names.
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
    """
    df = rename_columns(df)

    # Normalize string columns
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

    df = df.withColumn("review_id", build_review_id())

    # Final cleanup and deduplication
    window = Window.partitionBy("review_id").orderBy(
        F.col("review_timestamp").desc_nulls_last(),
        F.col("vote_count").desc_nulls_last(),
    )

    df = (
        df.withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn", "_id", "vote", "review_time_raw", "unix_review_time", "reviewerName")
    )

    return df.select(*FINAL_COLUMNS)

# Output writing
def write_output(df: DataFrame, settings: Dict[str, Any]) -> None:
    """
    Write transformed data to S3.
    """
    bucket = settings["s3_bucket"]
    prefix = settings["s3_staging_prefix"]
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
    settings = load_emr_transform_settings()
    spark = create_spark()

    try:
        manifest = load_manifest(settings, manifest_key)
        input_paths = extract_input_paths(settings, manifest)
        ingest_dt = extract_ingest_dt(manifest)

        raw_df = spark.read.json(input_paths)
        validate_required_columns(raw_df)

        staging_df = transform(raw_df, ingest_dt).persist(StorageLevel.MEMORY_AND_DISK)
        
        if staging_df.count() == 0:
            raise ValueError("Output DataFrame is empty after transformation.")

        write_output(staging_df, settings)
        logger.info("Reviews transformation finished successfully.")

    except Exception as exc:
        logger.exception(f"Reviews transformation failed: {exc}")
        raise
    finally:
        if "staging_df" in locals():
            staging_df.unpersist()
        spark.stop()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        sys.exit(1)

    manifest_key = sys.argv[1]
    run_reviews_transformation(manifest_key=manifest_key)