import argparse
import sys
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Internal project imports
from src.common.config import load_emr_transform_settings
from src.common.logging import get_logger
from src.common.spark_utils import column_exists, get_dtype, extract_price, deduplicate_by_key

logger = get_logger(__name__)

# Iceberg Table Configuration
CATALOG_NAME = "glue_catalog"
DATABASE_NAME = "mlready"
TABLE_NAME = "mlready_product_features"
FULL_TABLE_IDENTIFIER = f"{CATALOG_NAME}.{DATABASE_NAME}.{TABLE_NAME}"

def parse_args() -> argparse.Namespace:
    """Handles CLI arguments using argparse (safer than sys.argv)."""
    parser = argparse.ArgumentParser(description="Build product features for ML-ready layer.")
    parser.add_argument("--input-path", required=True, help="S3 path to staging products")
    parser.add_argument("--execution-date", required=True, help="Processing date (YYYY-MM-DD)")
    return parser.parse_args()

def create_spark_session(app_name: str) -> SparkSession:
    """Creates Spark session with Iceberg & Glue Catalog config."""
    settings = load_emr_transform_settings()
    # Path where Iceberg will store data files
    warehouse_path = f"s3://{settings['s3_bucket']}/mlready/"

    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", warehouse_path)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )

def build_product_features(df: DataFrame, execution_date: str) -> DataFrame:
    """Core feature engineering logic."""
    price_col = extract_price(df)
    desc_type = get_dtype(df, "description")

    return (
        df
        .withColumn("price", price_col)
        .withColumn("has_brand", F.when(F.col("brand").isNotNull() & (F.trim(F.col("brand")) != ""), True).otherwise(False))
        .withColumn("has_price", F.col("price").isNotNull() & (F.col("price") > 0))
        .withColumn("has_description", 
            F.when(F.col("description").isNotNull() & (F.size("description") > 0), True).otherwise(False) 
            if isinstance(desc_type, T.ArrayType) else F.lit(False))
        .withColumn("title_length", F.length(F.coalesce(F.col("title"), F.lit(0))))
        .withColumn("description_total_length", 
            F.aggregate(F.coalesce(F.col("description"), F.array()), F.lit(0), lambda acc, x: acc + F.length(F.coalesce(x, F.lit(""))))
            if isinstance(desc_type, T.ArrayType) else F.lit(0))
        .withColumn("features_count", F.size("feature") if isinstance(get_dtype(df, "feature"), T.ArrayType) else F.lit(0))
        .withColumn("categories_count", F.size("category") if isinstance(get_dtype(df, "category"), T.ArrayType) else F.lit(0))
        .withColumn("feature_snapshot_date", F.to_date(F.lit(execution_date)))
        .select(
            "asin", "title", "brand", "main_cat", "price", "has_brand", "has_price", 
            "has_description", "title_length", "description_total_length", 
            "features_count", "categories_count", "feature_snapshot_date"
        )
    )

def main():
    args = parse_args()
    spark = create_spark_session("gold-product-features")
    
    try:
        logger.info(f"Processing data for execution date: {args.execution_date}")
        input_df = spark.read.parquet(args.input_path)
        
        # Using the helper from spark_utils
        deduped_df = deduplicate_by_key(input_df, key_col="asin")
        
        final_df = build_product_features(deduped_df, args.execution_date)

        # Write result to Iceberg table managed by Glue
        final_df.writeTo(FULL_TABLE_IDENTIFIER).using("iceberg").createOrReplace()
        
        logger.info(f"Successfully updated Iceberg table: {FULL_TABLE_IDENTIFIER}")
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()