"""
This script aggregates user reviews to calculate product-level statistics.
It generates metrics such as rating distribution (positive/negative ratio), 
standard deviation of ratings, and engagement metrics for each ASIN.
"""

import argparse
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Internal project imports
from src.common.config import load_emr_transform_settings, get_iceberg_settings
from src.common.logging import get_logger
from src.common.spark_utils import column_exists

logger = get_logger(__name__)
iceberg_cfg = get_iceberg_settings()

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build product review stats for Gold layer.")
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

def build_product_review_stats(df: DataFrame, execution_date: str) -> DataFrame:
    """
    Aggregates reviews by ASIN to compute statistical features.
    """
    # Logic for positive (4-5) and negative (1-2) reviews
    is_positive_col = F.when(F.col("overall") >= 4, 1).otherwise(0)
    is_negative_col = F.when(F.col("overall") <= 2, 1).otherwise(0)

    stats_df = df.groupBy("asin").agg(
        F.count("reviewer_id").alias("reviews_count"),
        F.countDistinct("reviewer_id").alias("distinct_reviewers_count"),
        F.avg("overall").alias("avg_rating"),
        F.stddev("overall").alias("rating_stddev"),
        F.avg(F.col("verified").cast("int")).alias("verified_reviews_ratio"),
        F.avg(F.coalesce(F.col("vote").cast("double"), F.lit(0.0))).alias("avg_vote_count"),
        F.avg(F.length(F.coalesce(F.col("reviewText"), F.lit("")))).alias("avg_review_text_length"),
        F.avg(is_positive_col).alias("positive_reviews_ratio"),
        F.avg(is_negative_col).alias("negative_reviews_ratio"),
        F.min("unixReviewTime").alias("first_review_timestamp"),
        F.max("unixReviewTime").alias("last_review_timestamp")
    )

    # Final selection with explicit casting
    return (
        stats_df
        .withColumn("feature_snapshot_date", F.to_date(F.lit(execution_date)))
        .select(
            F.col("asin").cast("string"),
            F.col("reviews_count").cast("long"),
            F.col("distinct_reviewers_count").cast("long"),
            F.col("avg_rating").cast("double"),
            # stddev can return null if only 1 review exists - we handle it
            F.coalesce(F.col("rating_stddev"), F.lit(0.0)).cast("double").alias("rating_stddev"),
            F.col("verified_reviews_ratio").cast("double"),
            F.col("avg_vote_count").cast("double"),
            F.col("avg_review_text_length").cast("double"),
            F.col("positive_reviews_ratio").cast("double"),
            F.col("negative_reviews_ratio").cast("double"),
            F.from_unixtime(F.col("first_review_timestamp")).cast("timestamp").alias("first_review_timestamp"),
            F.from_unixtime(F.col("last_review_timestamp")).cast("timestamp").alias("last_review_timestamp"),
            "feature_snapshot_date"
        )
    )

def main():
    args = parse_args()
    spark = create_spark_session("gold-product-review-stats")
    
    try:
        logger.info(f"Aggregating product review stats for {args.execution_date}")
        input_df = spark.read.parquet(args.input_path)
        
        # Initial cleanup: deduplicate the same review from the same user on the same product
        input_df = input_df.dropDuplicates(["reviewer_id", "asin", "unixReviewTime"])
        
        final_df = build_product_review_stats(input_df, args.execution_date)

        target_table = iceberg_cfg["table_stats"]
        logger.info(f"Writing to Iceberg: {target_table}")
        
        final_df.writeTo(target_table).using("iceberg").createOrReplace()
        
        logger.info("Product review stats update completed.")
    except Exception as e:
        logger.error(f"Job failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()