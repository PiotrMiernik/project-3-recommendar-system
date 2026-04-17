"""
This script transforms staging product metadata into an ML-ready Gold table.
It handles deduplication, price extraction, and computes product-level features 
used for generating recommendations.
"""

import argparse
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common.config import load_emr_transform_settings, get_iceberg_settings
from src.common.logging import get_logger
from src.common.spark_utils import get_dtype, extract_price, deduplicate_by_key

logger = get_logger(__name__)
iceberg_cfg = get_iceberg_settings()

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build product features for Gold layer.")
    parser.add_argument("--input-path", required=True, help="S3 path to staging products")
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

def build_product_features(df: DataFrame, execution_date: str) -> DataFrame:
    price_col = extract_price(df)
    desc_type = get_dtype(df, "description")

    return (
        df
        .withColumn("price", price_col)
        .withColumn("has_brand", F.when(F.col("brand").isNotNull() & (F.trim(F.col("brand")) != ""), True).otherwise(False))
        .withColumn("has_price", F.col("price").isNotNull() & (F.col("price") > 0))
        .withColumn("title_length", F.length(F.coalesce(F.col("title"), F.lit(""))))
        .withColumn("description_total_length", 
            F.aggregate(F.coalesce(F.col("description"), F.array()), F.lit(0), lambda acc, x: acc + F.length(F.coalesce(x, F.lit(""))))
            if isinstance(desc_type, T.ArrayType) else F.lit(0))
        .withColumn("feature_snapshot_date", F.to_date(F.lit(execution_date)))
        .select(
            "asin", "title", "brand", "price", "has_brand", "has_price", 
            "title_length", "description_total_length", "main_category", "feature_snapshot_date"
        )
    )

def main():
    args = parse_args()
    spark = create_spark_session("gold-products-transformation")
    
    try:
        logger.info(f"Processing product features for {args.execution_date}")
        input_df = spark.read.parquet(args.input_path)
        deduped_df = deduplicate_by_key(input_df, key_col="asin")
        final_df = build_product_features(deduped_df, args.execution_date)

        target_table = iceberg_cfg["table_products"]
        logger.info(f"Writing to Iceberg: {target_table}")
        final_df.writeTo(target_table).using("iceberg").createOrReplace()
        
    except Exception as e:
        logger.error(f"Failed to build product features: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()