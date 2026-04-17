"""
This script creates the final Master Feature Table for ML training.
It joins user interactions (reviews) from staging with enriched product 
and user features stored in Iceberg tables.
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
    parser = argparse.ArgumentParser(description="Build Master Feature Table (Interactions).")
    parser.add_argument("--execution-date", required=True, help="Processing date (YYYY-MM-DD)")
    return parser.parse_args()

def create_spark_session(app_name: str) -> SparkSession:
    """
    Initializes Spark with Iceberg and Glue Catalog support.
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

def build_master_table(interactions_df: DataFrame, 
                       products_df: DataFrame, 
                       users_df: DataFrame, 
                       stats_df: DataFrame,
                       execution_date: str) -> DataFrame:
    """
    Enriches interactions with existing features only.
    """
    # 1. Enrich with Product Metadata
    enriched_df = interactions_df.join(
        products_df.select(
            F.col("asin").alias("p_asin"),
            F.col("main_category").alias("product_main_category"), 
            F.col("brand").alias("product_brand"),
            F.col("price").alias("product_price"),
            # Zamiast has_description, używamy logicznej flagi z długości opisu
            F.when(F.col("description_total_length") > 0, True).otherwise(False).alias("product_has_description"),
            F.col("title_length").alias("product_title_length"),
            F.col("description_total_length").alias("product_description_length")
        ),
        interactions_df.asin == F.col("p_asin"),
        "left"
    ).drop("p_asin")

    # 2. Enrich with User Profiles
    enriched_df = enriched_df.join(
        users_df.select(
            F.col("reviewer_id").alias("u_reviewer_id"),
            F.col("reviews_count").alias("user_reviews_count"),
            F.col("avg_rating").alias("user_avg_rating"),
            F.col("verified_reviews_ratio").alias("user_verified_reviews_ratio"),
            F.col("active_days_span").alias("user_active_days_span")
        ),
        enriched_df.reviewer_id == F.col("u_reviewer_id"),
        "left"
    ).drop("u_reviewer_id")

    # 3. Enrich with Product Stats
    enriched_df = enriched_df.join(
        stats_df.select(
            F.col("asin").alias("s_asin"),
            F.col("reviews_count").alias("product_reviews_count"),
            F.col("avg_rating").alias("product_avg_rating"),
            F.col("positive_reviews_ratio").alias("product_positive_reviews_ratio"),
            F.col("negative_reviews_ratio").alias("product_negative_reviews_ratio")
        ),
        enriched_df.asin == F.col("s_asin"),
        "left"
    ).drop("s_asin")

    # 4. Final selection
    return (
        enriched_df
        .withColumn("review_text_length", F.length(F.coalesce(F.col("review_text"), F.lit(""))))
        .withColumn("summary_length", F.length(F.coalesce(F.col("summary"), F.lit(""))))
        .withColumn("review_date", F.to_date(F.col("review_timestamp")))
        .withColumn("label", F.col("rating").cast("double"))
        .withColumn("feature_snapshot_date", F.to_date(F.lit(execution_date)))
        .select(
            "review_id", "reviewer_id", "asin",
            "rating", "vote_count", "verified", "review_text_length", "summary_length", "review_timestamp", "review_date",
            "product_main_category", "product_brand", "product_price", "product_has_description", 
            "product_title_length", "product_description_length",
            "user_reviews_count", "user_avg_rating", "user_verified_reviews_ratio", "user_active_days_span",
            "product_reviews_count", "product_avg_rating", "product_positive_reviews_ratio", "product_negative_reviews_ratio",
            "label", "feature_snapshot_date"
        )
    )

def main():
    args = parse_args()
    spark = create_spark_session("gold-master-interactions")
    
    try:
        logger.info(f"Loading reference Iceberg tables for enrichment...")
        products_df = spark.table(iceberg_cfg["table_products"])
        users_df = spark.table(iceberg_cfg["table_users"])
        stats_df = spark.table(iceberg_cfg["table_stats"])
        
        # Interactions are loaded from the specific staging partition (Silver layer)
        settings = load_emr_transform_settings()
        input_path = f"s3://{settings['s3_bucket']}/{settings['s3_staging_prefix']}reviews/ingest_dt={args.execution_date}"
        
        logger.info(f"Loading staging interactions from: {input_path}")
        interactions_raw_df = spark.read.parquet(input_path)
        
        # Deduplication based on the standardized review_id
        interactions_clean = interactions_raw_df.dropDuplicates(["review_id"])
        
        # Execution of the Master join logic
        final_df = build_master_table(
            interactions_clean, products_df, users_df, stats_df, args.execution_date
        )

        target_table = iceberg_cfg["table_interactions"]
        logger.info(f"Writing Master Feature Table to Iceberg: {target_table}")
        
        # Perform ACID compliant write
        final_df.writeTo(target_table).using("iceberg").createOrReplace()
        
        logger.info("Master Feature Table build completed successfully.")
        
    except Exception as e:
        logger.error(f"Critical failure in Master Table build: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()