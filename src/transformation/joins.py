"""
PySpark Relational Joins Module
Performs optimized broadcast joins across sales transactions, product catalog, and customer dimensions.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger_config import get_logger

logger = get_logger("transformation.joins")


def enrich_sales_with_catalog_and_customers(
    sales_df: DataFrame,
    products_df: DataFrame,
    customers_df: DataFrame
) -> DataFrame:
    """
    Performs dimensional joins:
    - sales_df LEFT JOIN products_df ON product_id (broadcast)
    - sales_df LEFT JOIN customers_df ON customer_id (broadcast)
    
    Returns an enriched DataFrame containing customer and product attributes.
    """
    logger.info("Enriching sales data via broadcast joins with product catalog and customer master")

    # Rename metadata columns in dimensions to avoid collisions with sales metadata
    prod_prepared = (
        products_df
        .select(
            F.col("product_id").alias("p_product_id"),
            F.col("product_name"),
            F.col("category").alias("product_category"),
            F.col("brand").alias("product_brand"),
            F.col("price").alias("catalog_price"),
            F.col("stock").alias("catalog_stock")
        )
    )

    cust_prepared = (
        customers_df
        .select(
            F.col("customer_id").alias("c_customer_id"),
            F.col("customer_name"),
            F.col("city").alias("customer_city"),
            F.col("state").alias("customer_state"),
            F.col("country").alias("customer_country")
        )
    )

    # Broadcast join sales with products
    enriched_df = sales_df.join(
        F.broadcast(prod_prepared),
        sales_df["product_id"] == prod_prepared["p_product_id"],
        how="left"
    ).drop("p_product_id")

    # Broadcast join with customers
    enriched_df = enriched_df.join(
        F.broadcast(cust_prepared),
        enriched_df["customer_id"] == cust_prepared["c_customer_id"],
        how="left"
    ).drop("c_customer_id")

    logger.info(f"Enrichment completed. Total enriched rows: {enriched_df.count()}")
    return enriched_df
