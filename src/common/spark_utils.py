from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

def column_exists(df: DataFrame, col_name: str) -> bool:
    """Checks if a column exists in the DataFrame."""
    return col_name in df.columns

def get_dtype(df: DataFrame, col_name: str):
    """Returns the data type of a specific column."""
    for field in df.schema.fields:
        if field.name == col_name:
            return field.dataType
    return None

def extract_price(df: DataFrame) -> F.Column:
    """
    Logic to extract price from 'price' or 'price_raw'.
    Handles currency symbols and formatting.
    """
    if column_exists(df, "price"):
        return F.col("price").cast("double")
    
    if column_exists(df, "price_raw"):
        return F.regexp_replace(
            F.regexp_extract(F.col("price_raw").cast("string"), r"(\d+(?:\.\d+)?)", 1),
            ",", ""
        ).cast("double")
    
    return F.lit(None).cast("double")

def deduplicate_by_key(df: DataFrame, key_col: str, order_col: str = "updated_at") -> DataFrame:
    """
    Deduplicates a DataFrame based on a key (e.g., ASIN).
    Prioritizes records with the highest value in order_col.
    """
    if not column_exists(df, key_col):
        raise ValueError(f"Key column '{key_col}' is missing for deduplication!")
    
    if column_exists(df, order_col):
        window_spec = Window.partitionBy(key_col).orderBy(F.col(order_col).desc_nulls_last())
        return (
            df.withColumn("row_num", F.row_number().over(window_spec))
            .filter(F.col("row_num") == 1)
            .drop("row_num")
        )
    
    return df.dropDuplicates([key_col])