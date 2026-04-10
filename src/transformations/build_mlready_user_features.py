"""
This script aggregates user review history from staging into a Gold feature table.
It calculates behavioral metrics such as review frequency, average ratings, and 
activity span to create a comprehensive profile for each user.
"""

import argparse
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import load_emr_transform_settings, get_iceberg_settings
from src.common.logging import get_logger
from src.common.spark_utils import column_exists

logger = get_logger(__name__)
iceberg_cfg = get_iceberg_settings()

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build user features for Gold layer.")
    parser.add_argument("--input-path", required=True, help="S3 path to staging reviews")
    parser.add_argument("--execution-date", required=True, help="Processing date (YYYY-MM-DD)")
    return parser.parse_args()

def create_spark_session(app_name: str) -> SparkSession:
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
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

def build_user_features(df: DataFrame, execution_date: str) -> DataFrame:
    """
    Aggregates review data to create a behavioral profile for each user.
    Ensures one row per reviewer_id with all mandatory metrics.
    """
    
    # 1. Main aggregation
    user_features = df.groupBy("reviewer_id").agg(
        F.count("asin").alias("reviews_count"),
        F.countDistinct("asin").alias("distinct_products_count"),
        F.avg("overall").alias("avg_rating"),
        F.min("overall").alias("min_rating"),
        F.max("overall").alias("max_rating"),
        # Verified review ratio (average of boolean cast to int)
        F.avg(F.col("verified").cast("int")).alias("verified_reviews_ratio"),
        # Average votes (handling potential nulls in 'vote' column)
        F.avg(F.coalesce(F.col("vote").cast("double"), F.lit(0.0))).alias("avg_vote_count"),
        # Text metrics
        F.avg(F.length(F.coalesce(F.col("reviewText"), F.lit("")))).alias("avg_review_text_length"),
        # Temporal features
        F.min("unixReviewTime").alias("first_review_timestamp"),
        F.max("unixReviewTime").alias("last_review_timestamp")
    )

    # 2. Add derived features and metadata with strict type casting
    return (
        user_features
        .withColumn(
            "active_days_span", 
            ((F.col("last_review_timestamp") - F.col("first_review_timestamp")) / 86400).cast("int")
        )
        .withColumn("feature_snapshot_date", F.to_date(F.lit(execution_date)))
        .select(
            F.col("reviewer_id").cast("string"),
            F.col("reviews_count").cast("long"),
            F.col("distinct_products_count").cast("long"),
            F.col("avg_rating").cast("double"),
            F.col("min_rating").cast("int"),
            F.col("max_rating").cast("int"),
            F.col("verified_reviews_ratio").cast("double"),
            F.col("avg_vote_count").cast("double"),
            F.col("avg_review_text_length").cast("double"),
            # Converting unix timestamp to actual Timestamp type
            F.from_unixtime(F.col("first_review_timestamp")).cast("timestamp").alias("first_review_timestamp"),
            F.from_unixtime(F.col("last_review_timestamp")).cast("timestamp").alias("last_review_timestamp"),
            F.col("active_days_span"),
            "feature_snapshot_date"
        )
    )

def main():
    args = parse_args()
    spark = create_spark_session("gold-users-transformation")
    
    try:
        logger.info(f"Processing user features for {args.execution_date}")
        input_df = spark.read.parquet(args.input_path)
        
        # Simple deduplication for user-item pairs
        input_df = input_df.dropDuplicates(["reviewer_id", "asin", "unixReviewTime"])
        
        final_df = build_user_features(input_df, args.execution_date)

        target_table = iceberg_cfg["table_users"]
        logger.info(f"Writing to Iceberg: {target_table}")
        final_df.writeTo(target_table).using("iceberg").createOrReplace()
        
    except Exception as e:
        logger.error(f"Failed to build user features: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()