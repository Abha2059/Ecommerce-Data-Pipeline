"""
PySpark Financial Calculations & Aggregations Module
Calculates revenue metrics (gross_amount, discount_amount, net_amount) and computes analytical rollups.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from src.utils.logger_config import get_logger

logger = get_logger("transformation.aggregations")


def calculate_sales_financials(df: DataFrame) -> DataFrame:
    """
    Computes financial metrics for each order line:
    - gross_amount = round(quantity * unit_price, 2)
    - discount_amount = round(gross_amount * (discount / 100.0), 2)
    - net_amount = round(gross_amount - discount_amount, 2)
    """
    logger.info("Computing revenue and financial metrics (gross_amount, discount_amount, net_amount)")

    # 1. Gross amount = quantity * unit_price
    df = df.withColumn(
        "gross_amount",
        F.round(
            (F.col("quantity") * F.col("unit_price")).cast(DecimalType(12, 2)),
            2
        )
    )

    # 2. Discount amount = gross_amount * (discount / 100.0)
    df = df.withColumn(
        "discount_amount",
        F.round(
            (F.col("gross_amount") * (F.col("discount") / F.lit(100.0))).cast(DecimalType(12, 2)),
            2
        )
    )

    # 3. Net amount = gross_amount - discount_amount
    df = df.withColumn(
        "net_amount",
        F.round(
            (F.col("gross_amount") - F.col("discount_amount")).cast(DecimalType(12, 2)),
            2
        )
    )

    return df


def compute_daily_summary(df: DataFrame) -> DataFrame:
    """Computes daily roll-up aggregations for fast validation and reporting."""
    return (
        df.groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_items"),
            F.round(F.sum("gross_amount"), 2).alias("gross_revenue"),
            F.round(F.sum("discount_amount"), 2).alias("total_discounts"),
            F.round(F.sum("net_amount"), 2).alias("net_revenue"),
            F.countDistinct("customer_id").alias("unique_customers")
        )
        .orderBy(F.col("order_date").desc())
    )
