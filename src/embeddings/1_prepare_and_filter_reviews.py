from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.config import load_emr_transform_settings
from src.common.logging import get_logger
from src.common.embedding_utils import build_text_for_embedding

logger = get_logger(__name__)

INPUT_ENTITY = "reviews"
OUTPUT_ENTITY = "reviews_for_embedding"

FINAL_COLUMNS = [
    "review_id",
    "asin",
    "reviewer_id",
    "review_timestamp",
    "ingest_dt",
    "text_for_embedding",
    "text_hash",
    "model_version",
]


def create_spark() -> SparkSession:
    """
    Create and return a Spark session for the prepare/filter job.
    """
    return (
        SparkSession.builder.appName("prepare-and-filter-reviews-for-embeddings")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def build_input_path(settings: dict) -> str:
    """
    Build the S3 input path for staging reviews.
    """
    return f"s3://{settings['s3_bucket']}/{settings['s3_staging_prefix']}{INPUT_ENTITY}"


def build_output_path(settings: dict) -> str:
    """
    Build the S3 output path for the intermediate dataset used
    by the embeddings generation job.
    """
    return f"s3://{settings['s3_bucket']}/{settings['s3_staging_prefix']}{OUTPUT_ENTITY}"


def validate_required_columns(df: DataFrame) -> None:
    """
    Validate that the staging dataset contains all required columns
    for the embeddings preparation step.
    """
    required = {
        "review_id",
        "asin",
        "reviewer_id",
        "summary",
        "review_text",
        "review_timestamp",
        "ingest_dt",
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")


def transform(df: DataFrame, model_version: str) -> DataFrame:
    """
    Prepare review records for embedding generation.

    Steps:
    - Build text_for_embedding from summary + review_text
    - Filter out empty records
    - Add text_hash for incremental logic
    - Add model_version for model lineage
    - Keep only columns required by the next step
    """
    build_text_udf = F.udf(build_text_for_embedding, T.StringType())

    df = (
        df.withColumn(
            "text_for_embedding",
            build_text_udf(F.col("summary"), F.col("review_text"))
        )
        .filter(F.col("text_for_embedding").isNotNull())
        .withColumn("text_hash", F.sha2(F.col("text_for_embedding"), 256))
        .withColumn("model_version", F.lit(model_version))
    )

    return df.select(*FINAL_COLUMNS)


def write_output(df: DataFrame, output_path: str) -> None:
    """
    Write the prepared dataset to S3 in Parquet format,
    partitioned by ingest_dt.
    """
    logger.info(f"Writing output to: {output_path}")

    (
        df.write
        .mode("overwrite")
        .partitionBy("ingest_dt")
        .parquet(output_path)
    )


def run_prepare_and_filter_reviews() -> None:
    """
    Main entrypoint for the EMR job.
    """
    settings = load_emr_transform_settings()
    spark = create_spark()

    input_path = build_input_path(settings)
    output_path = build_output_path(settings)
    model_version = settings["embedding_model_version"]

    try:
        logger.info(f"Reading staging reviews from: {input_path}")
        staging_df = spark.read.parquet(input_path)

        validate_required_columns(staging_df)

        prepared_df = transform(
            df=staging_df,
            model_version=model_version,
        ).persist(StorageLevel.MEMORY_AND_DISK)

        if prepared_df.count() == 0:
            raise ValueError("Output DataFrame is empty after preparation.")

        write_output(prepared_df, output_path)
        logger.info("Prepare and filter reviews job finished successfully.")

    except Exception as exc:
        logger.exception(f"Prepare and filter reviews job failed: {exc}")
        raise
    finally:
        if "prepared_df" in locals():
            prepared_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    run_prepare_and_filter_reviews()