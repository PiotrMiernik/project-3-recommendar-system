"""
This script aggregates user review history from staging into a Gold feature table (Iceberg).
It calculates behavioral metrics such as review frequency, average ratings, and 
activity span to create a comprehensive profile for each user.
"""

import argparse
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import load_emr_transform_settings, get_iceberg_settings
from src.common.logging import get_logger

logger = get_logger(__name__)
iceberg_cfg = get_iceberg_settings()

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build user features for Gold layer.")
    parser.add_argument("--input-path", required=True, help="S3 path to staging reviews")
    parser.add_argument("--execution-date", required=True, help="Processing date (YYYY-MM-DD)")
    return parser.parse_args()

def create_spark_session(app_name: str) -> SparkSession:
    """
    Initializes Spark Session with Iceberg extensions and Glue Catalog configuration.
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
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

def build_user_features(df: DataFrame, execution_date: str) -> DataFrame:
    """
    Aggregates review data to create a behavioral profile for each user.
    Uses column names standardized in the Staging layer.
    """
    
    # 1. Core aggregation using Staging schema columns
    # Note: 'rating' and 'vote_count' are used instead of 'overall' and 'vote'
    user_features = df.groupBy("reviewer_id").agg(
        F.count("asin").alias("reviews_count"),
        F.countDistinct("asin").alias("distinct_products_count"),
        F.avg("rating").alias("avg_rating"),
        F.min("rating").alias("min_rating"),
        F.max("rating").alias("max_rating"),
        F.avg(F.col("verified").cast("int")).alias("verified_reviews_ratio"),
        # Staging already provides vote_count as an integer
        F.avg(F.coalesce(F.col("vote_count"), F.lit(0))).alias("avg_vote_count"),
        # Use review_text (standardized name) to calculate text length metrics
        F.avg(F.length(F.coalesce(F.col("review_text"), F.lit("")))).alias("avg_review_text_length"),
        F.min("review_timestamp").alias("first_review_timestamp"),
        F.max("review_timestamp").alias("last_review_timestamp")
    )

    # 2. Feature engineering and technical metadata
    # Calculated 'active_days_span' by converting timestamps to unix seconds (long)
    return (
        user_features
        .withColumn(
            "active_days_span", 
            ((F.col("last_review_timestamp").cast("long") - 
              F.col("first_review_timestamp").cast("long")) / 86400).cast("int")
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
            # Timestamps are already objects, no need for from_unixtime conversion
            F.col("first_review_timestamp"),
            F.col("last_review_timestamp"),
            F.col("active_days_span"),
            "feature_snapshot_date"
        )
    )

def main():
    args = parse_args()
    spark = create_spark_session("gold-users-transformation")
    
    try:
        logger.info(f"Starting user features build for execution_date: {args.execution_date}")
        
        # Load data from Staging (Parquet)
        input_df = spark.read.parquet(args.input_path)
        
        # Build features
        final_df = build_user_features(input_df, args.execution_date)

        target_table = iceberg_cfg["table_users"]
        logger.info(f"Writing features to Iceberg table: {target_table}")
        
        # Atomic write using Iceberg's createOrReplace (ACID compliant)
        final_df.writeTo(target_table).using("iceberg").createOrReplace()
        
        logger.info("User features build completed successfully.")

    except Exception as e:
        logger.error(f"Critical error during user features transformation: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()