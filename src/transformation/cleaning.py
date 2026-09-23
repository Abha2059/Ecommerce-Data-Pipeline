"""
PySpark Data Cleaning & Standardization Module
Defines explicit schemas, normalizes datatypes, standardizes strings, and eliminates duplicates.
"""

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.utils.logger_config import get_logger

logger = get_logger("transformation.cleaning")

# Explicit PySpark Schemas
RAW_SALES_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("order_date", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity", StringType(), True),
    StructField("unit_price", StringType(), True),
    StructField("discount", StringType(), True),
    StructField("region", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
])

PRODUCT_CATALOG_SCHEMA = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), False),
    StructField("stock", IntegerType(), True),
    StructField("source_system", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
])

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), False),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
])


def standardize_column_names(df: DataFrame) -> DataFrame:
    """Standardizes column headers to lowercase snake_case."""
    for col_name in df.columns:
        cleaned = col_name.strip().lower().replace(" ", "_").replace("-", "_")
        if cleaned != col_name:
            df = df.withColumnRenamed(col_name, cleaned)
    return df


def clean_sales_dataframe(df: DataFrame) -> DataFrame:
    """
    Cleans raw sales PySpark DataFrame:
    - Trims string columns
    - Parses date formats with fallback (supports yyyy-MM-dd and yyyy/MM/dd)
    - Formats region to Title Case
    - Fills default values for optional columns
    - Deduplicates order line items by (order_id, product_id)
    - Casts numeric fields to appropriate types
    """
    logger.info("Initiating PySpark sales cleaning and standardization")
    initial_count = df.count()

    # 1. Standardize column names
    df = standardize_column_names(df)

    # 2. Trim string columns
    string_cols: List[str] = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in string_cols:
        df = df.withColumn(c, F.trim(F.col(c)))

    # 3. Standardize Date to DateType (yyyy-MM-dd)
    df = df.withColumn(
        "normalized_date_str",
        F.regexp_replace(F.col("order_date"), "/", "-")
    )
    df = df.withColumn(
        "order_date",
        F.coalesce(
            F.to_date(F.col("normalized_date_str"), "yyyy-MM-dd"),
            F.to_date(F.col("order_date"))
        )
    ).drop("normalized_date_str")

    # 4. Fill optional defaults
    df = df.withColumn(
        "payment_method",
        F.when(
            (F.col("payment_method").isNull()) | (F.col("payment_method") == ""),
            F.lit("Unknown")
        ).otherwise(F.col("payment_method"))
    )

    df = df.withColumn(
        "order_status",
        F.when(
            (F.col("order_status").isNull()) | (F.col("order_status") == ""),
            F.lit("Completed")
        ).otherwise(F.initcap(F.col("order_status")))
    )

    # 5. Normalize Region (Title Cased, clean multiple spaces)
    df = df.withColumn(
        "region",
        F.when(
            (F.col("region").isNull()) | (F.col("region") == ""),
            F.lit("Unknown")
        ).otherwise(F.initcap(F.regexp_replace(F.col("region"), r"\s+", " ")))
    )

    # 6. Type conversions
    df = (
        df
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price", F.round(F.col("unit_price").cast(DecimalType(10, 2)), 2))
        .withColumn(
            "discount",
            F.coalesce(F.round(F.col("discount").cast(DecimalType(5, 2)), 2), F.lit(0.00))
        )
    )

    # 7. Deduplicate identical order line items: (order_id, product_id)
    df = df.dropDuplicates(["order_id", "product_id"])
    cleaned_count = df.count()
    deduped_diff = initial_count - cleaned_count
    if deduped_diff > 0:
        logger.info(f"Removed {deduped_diff} duplicate order line records. Clean records: {cleaned_count}")

    return df
