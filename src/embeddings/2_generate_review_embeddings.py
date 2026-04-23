from typing import Iterator, List

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import types as T

from src.common.config import load_emr_transform_settings
from src.common.embedding_utils import (
    generate_embeddings,
    load_embedding_model,
    normalize_embeddings,
)
from src.common.logging import get_logger

logger = get_logger(__name__)

INPUT_ENTITY = "reviews_for_embedding"
OUTPUT_ENTITY = "review_embeddings"
DEFAULT_BATCH_SIZE = 32

FINAL_COLUMNS = [
    "review_id",
    "asin",
    "reviewer_id",
    "review_timestamp",
    "ingest_dt",
    "text_hash",
    "model_version",
    "embedding",
]


def create_spark() -> SparkSession:
    """
    Create and return a Spark session for the embeddings generation job.
    """
    return (
        SparkSession.builder.appName("generate-review-embeddings")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def build_input_path(settings: dict) -> str:
    """
    Build the S3 input path for prepared review texts.
    """
    return f"s3://{settings['s3_bucket']}/{settings['s3_staging_prefix']}{INPUT_ENTITY}"


def build_output_path(settings: dict) -> str:
    """
    Build the S3 output path for generated review embeddings.
    """
    return f"s3://{settings['s3_bucket']}/{settings['s3_staging_prefix']}{OUTPUT_ENTITY}"


def validate_required_columns(df: DataFrame) -> None:
    """
    Validate that the input dataset contains all columns required
    for embeddings generation.
    """
    required = {
        "review_id",
        "asin",
        "reviewer_id",
        "review_timestamp",
        "ingest_dt",
        "text_for_embedding",
        "text_hash",
        "model_version",
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")


def process_batch(batch: List[Row], model, batch_size: int) -> Iterator[Row]:
    """
    Generate normalized embeddings for one batch of rows.
    """
    texts = [row["text_for_embedding"] for row in batch]

    embeddings = generate_embeddings(
        texts=texts,
        model=model,
        batch_size=batch_size,
    )
    normalized_embeddings = normalize_embeddings(embeddings)

    for row, embedding in zip(batch, normalized_embeddings):
        yield Row(
            review_id=row["review_id"],
            asin=row["asin"],
            reviewer_id=row["reviewer_id"],
            review_timestamp=row["review_timestamp"],
            ingest_dt=row["ingest_dt"],
            text_hash=row["text_hash"],
            model_version=row["model_version"],
            embedding=[float(value) for value in embedding],
        )


def generate_partition_embeddings(
    rows: Iterator[Row],
    model_name: str,
    batch_size: int,
) -> Iterator[Row]:
    """
    Generate embeddings inside a Spark partition.

    This function:
    - loads the model once per partition,
    - processes partition rows in batches,
    - returns one output row per input review.
    """
    model = load_embedding_model(model_name)
    batch: List[Row] = []

    for row in rows:
        batch.append(row)

        if len(batch) >= batch_size:
            yield from process_batch(batch, model, batch_size)
            batch = []

    if batch:
        yield from process_batch(batch, model, batch_size)


def transform(
    df: DataFrame,
    spark: SparkSession,
    model_name: str,
    batch_size: int,
) -> DataFrame:
    """
    Generate embeddings for prepared review texts and return
    the final Spark DataFrame.
    """
    output_schema = T.StructType([
        T.StructField("review_id", T.StringType(), False),
        T.StructField("asin", T.StringType(), True),
        T.StructField("reviewer_id", T.StringType(), True),
        T.StructField("review_timestamp", T.TimestampType(), True),
        T.StructField("ingest_dt", T.DateType(), True),
        T.StructField("text_hash", T.StringType(), False),
        T.StructField("model_version", T.StringType(), False),
        T.StructField("embedding", T.ArrayType(T.FloatType()), False),
    ])

    embeddings_rdd = df.rdd.mapPartitions(
        lambda rows: generate_partition_embeddings(
            rows=rows,
            model_name=model_name,
            batch_size=batch_size,
        )
    )

    embeddings_df = spark.createDataFrame(embeddings_rdd, schema=output_schema)

    return embeddings_df.select(*FINAL_COLUMNS)


def write_output(df: DataFrame, output_path: str) -> None:
    """
    Write generated embeddings to S3 in Parquet format,
    partitioned by ingest_dt.
    """
    logger.info(f"Writing output to: {output_path}")

    (
        df.write
        .mode("overwrite")
        .partitionBy("ingest_dt")
        .parquet(output_path)
    )


def run_generate_review_embeddings() -> None:
    """
    Main entrypoint for the EMR job.
    """
    settings = load_emr_transform_settings()
    spark = create_spark()

    input_path = build_input_path(settings)
    output_path = build_output_path(settings)
    model_name = settings["embedding_model_version"]
    batch_size = int(settings.get("embedding_batch_size", DEFAULT_BATCH_SIZE))

    try:
        logger.info(f"Reading prepared review texts from: {input_path}")
        input_df = spark.read.parquet(input_path)

        validate_required_columns(input_df)

        embeddings_df = transform(
            df=input_df,
            spark=spark,
            model_name=model_name,
            batch_size=batch_size,
        ).persist(StorageLevel.MEMORY_AND_DISK)

        if embeddings_df.count() == 0:
            raise ValueError("Output DataFrame is empty after embeddings generation.")

        write_output(embeddings_df, output_path)
        logger.info("Generate review embeddings job finished successfully.")

    except Exception as exc:
        logger.exception(f"Generate review embeddings job failed: {exc}")
        raise
    finally:
        if "embeddings_df" in locals():
            embeddings_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    run_generate_review_embeddings()