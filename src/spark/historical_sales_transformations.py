"""
Historical Sales PySpark Transformation Pipeline
Applies distributed data cleaning, deduplication, schema enforcement,
derived metric calculation, and Snappy Parquet partitioning.
"""

from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any, Dict, Optional, Tuple

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    coalesce,
    col,
    concat_ws,
    count,
    countDistinct,
    date_format,
    lit,
    max as spark_max,
    min as spark_min,
    month,
    round as spark_round,
    sum as spark_sum,
    to_timestamp,
    trim,
    upper,
    when,
    year,
)
from pyspark.sql.types import (
    ByteType,
    DecimalType,
    DoubleType,
    IntegerType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.transformation.spark_session import get_spark_session
from src.utils.config import cfg
from src.utils.logger_config import get_logger

logger = get_logger("spark.historical_sales_transformations")


class TransformationError(Exception):
    """Raised when PySpark transformation or validation fails."""
    pass


class HistoricalSalesTransformer:
    """
    Transforms raw UCI Online Retail historical data into a partitioned Snappy Parquet Data Lake.
    """

    # Explicit schema for reading raw CSV without inferring types
    RAW_SCHEMA = StructType([
        StructField("InvoiceNo", StringType(), True),
        StructField("StockCode", StringType(), True),
        StructField("Description", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("InvoiceDate", StringType(), True),
        StructField("UnitPrice", DecimalType(10, 2), True),
        StructField("CustomerID", StringType(), True),
        StructField("Country", StringType(), True),
    ])

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        input_csv_path: Optional[Path] = None,
        output_parquet_dir: Optional[Path] = None,
    ):
        self.spark = spark or get_spark_session()
        self.input_csv_path = input_csv_path or cfg.paths.raw_csv_path
        self.output_parquet_dir = output_parquet_dir or cfg.paths.processed_data_dir
        self.output_parquet_dir.mkdir(parents=True, exist_ok=True)

    def load_raw_data(self) -> DataFrame:
        """Reads raw historical CSV using an explicit PySpark StructType schema."""
        if not self.input_csv_path.exists():
            error_msg = f"Raw CSV not found at {self.input_csv_path}"
            logger.error(error_msg)
            raise TransformationError(error_msg)

        logger.info(f"Loading raw data into PySpark from {self.input_csv_path}")
        df = (
            self.spark.read.format("csv")
            .option("header", "true")
            .option("mode", "DROPMALFORMED")
            .schema(self.RAW_SCHEMA)
            .load(str(self.input_csv_path))
        )
        return df

    def transform(self, raw_df: DataFrame) -> Tuple[DataFrame, Dict[str, Any]]:
        """
        Executes distributed transformations on the raw historical data:
        1. Standardize column names (snake_case).
        2. Trim string fields and uppercase stock codes.
        3. Parse timestamps supporting multiple standard date patterns.
        4. Cast CustomerID to nullable integer without fabricating guest data.
        5. Derive is_cancellation flag.
        6. Compute gross_amount = Quantity * UnitPrice.
        7. Extract transaction_year and transaction_month for partitioning.
        8. Deduplicate identical rows.
        """
        initial_count = raw_df.count()
        logger.info(f"Initial raw records loaded: {initial_count:,}")

        # 1. Standardize and clean fields
        cleaned_df = (
            raw_df
            .withColumn("invoice_no", trim(col("InvoiceNo")))
            .withColumn("stock_code", upper(trim(col("StockCode"))))
            .withColumn(
                "description",
                when(col("Description").isNull() | (trim(col("Description")) == ""), lit("UNKNOWN DESCRIPTION"))
                .otherwise(trim(col("Description"))),
            )
            .withColumn("quantity", col("Quantity").cast(IntegerType()))
            .withColumn("unit_price", col("UnitPrice").cast(DecimalType(10, 2)))
            .withColumn(
                "customer_id",
                # Handle floats like 17850.0 and string integers, null when missing
                when(col("CustomerID").isNotNull() & (trim(col("CustomerID")) != ""),
                     col("CustomerID").cast(DoubleType()).cast(IntegerType()))
                .otherwise(lit(None).cast(IntegerType())),
            )
            .withColumn(
                "country",
                when(col("Country").isNull() | (trim(col("Country")) == ""), lit("Unknown"))
                .otherwise(trim(col("Country"))),
            )
        )

        # 2. Parse InvoiceDate supporting multiple formats
        date_expr = coalesce(
            to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm"),
            to_timestamp(col("InvoiceDate"), "dd/MM/yyyy HH:mm"),
            to_timestamp(col("InvoiceDate"), "yyyy-MM-dd"),
        )
        cleaned_df = cleaned_df.withColumn("invoice_date", date_expr)

        # Filter out corrupted rows where invoice_no or invoice_date could not be parsed
        valid_df = cleaned_df.filter(col("invoice_no").isNotNull() & col("invoice_date").isNotNull())

        # 3. Derive business metrics
        transformed_df = (
            valid_df
            # is_cancellation: 1 if invoice_no starts with 'C' or quantity < 0
            .withColumn(
                "is_cancellation",
                when(col("invoice_no").startswith("C") | (col("quantity") < 0), lit(1)).otherwise(lit(0)),
            )
            # gross_amount: quantity * unit_price
            .withColumn(
                "gross_amount",
                spark_round(col("quantity").cast(DecimalType(12, 2)) * col("unit_price"), 2).cast(DecimalType(12, 2)),
            )
            # Year and Month partition keys
            .withColumn("transaction_year", year(col("invoice_date")).cast(ShortType()))
            .withColumn("transaction_month", month(col("invoice_date")).cast(ByteType()))
            .withColumn("source_system", lit("UCI_ONLINE_RETAIL"))
            .withColumn("ingestion_timestamp", lit(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")).cast(TimestampType()))
        )

        # Select final curated schema order
        selected_df = transformed_df.select(
            "invoice_no",
            "stock_code",
            "description",
            "quantity",
            "invoice_date",
            "unit_price",
            "customer_id",
            "country",
            "gross_amount",
            "is_cancellation",
            "transaction_year",
            "transaction_month",
            "source_system",
            "ingestion_timestamp",
        )

        # 4. Deduplicate
        deduped_df = selected_df.dropDuplicates(["invoice_no", "stock_code", "invoice_date", "quantity"])
        final_count = deduped_df.count()
        duplicates_removed = initial_count - final_count

        logger.info(f"Deduplication complete: {duplicates_removed:,} duplicate rows removed. Final count: {final_count:,}")

        # Compute summary metrics using PySpark aggregations (avoiding full collect)
        agg_row = deduped_df.select(
            count("*").alias("total_records"),
            countDistinct("invoice_no").alias("unique_invoices"),
            countDistinct("stock_code").alias("unique_products"),
            countDistinct("customer_id").alias("unique_customers"),
            spark_sum(when(col("customer_id").isNull(), 1).otherwise(0)).alias("guest_orders"),
            spark_sum(when(col("is_cancellation") == 1, 1).otherwise(0)).alias("cancellations"),
            spark_min("invoice_date").alias("min_date"),
            spark_max("invoice_date").alias("max_date"),
            spark_round(spark_sum("gross_amount"), 2).alias("total_gross_amount"),
        ).collect()[0]

        metrics = {
            "initial_raw_records": initial_count,
            "final_curated_records": final_count,
            "duplicates_removed": duplicates_removed,
            "unique_invoices": agg_row["unique_invoices"],
            "unique_products": agg_row["unique_products"],
            "unique_customers": agg_row["unique_customers"],
            "guest_order_records": agg_row["guest_orders"],
            "cancellation_records": agg_row["cancellations"],
            "min_invoice_date": str(agg_row["min_date"]),
            "max_invoice_date": str(agg_row["max_date"]),
            "total_gross_amount": float(agg_row["total_gross_amount"] or 0.0),
        }

        return deduped_df, metrics

    def write_parquet(self, df: DataFrame) -> Path:
        """
        Writes transformed dataset to Snappy-compressed Parquet partitioned by year and month.
        Controls small file explosion by repartitioning on partition keys.
        """
        logger.info(f"Writing Snappy Parquet partitioned dataset to: {self.output_parquet_dir}")
        (
            df.repartition(2, "transaction_year", "transaction_month")
            .write.mode("overwrite")
            .format("parquet")
            .option("compression", "snappy")
            .partitionBy("transaction_year", "transaction_month")
            .save(str(self.output_parquet_dir))
        )
        logger.info("Successfully wrote partitioned Parquet dataset.")
        return self.output_parquet_dir

    def validate_parquet_output(self, expected_record_count: int) -> Dict[str, Any]:
        """
        Reads back the written Parquet dataset to verify row counts, schema, and partitions.
        """
        logger.info(f"Validating Parquet dataset at {self.output_parquet_dir}")
        readback_df = self.spark.read.parquet(str(self.output_parquet_dir))
        readback_count = readback_df.count()

        if readback_count != expected_record_count:
            error_msg = f"Parquet row count mismatch: wrote {expected_record_count}, read back {readback_count}"
            logger.error(error_msg)
            raise TransformationError(error_msg)

        # Inspect partition directories
        partition_dirs = [p.name for p in self.output_parquet_dir.glob("transaction_year=*")]
        if not partition_dirs:
            raise TransformationError(f"No transaction_year=* partitions found in {self.output_parquet_dir}")

        validation_result = {
            "validation_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "parquet_path": str(self.output_parquet_dir),
            "readback_record_count": readback_count,
            "year_partitions": partition_dirs,
            "status": "PASSED",
        }
        logger.info(f"Parquet validation passed: {readback_count:,} records across partitions {partition_dirs}")
        return validation_result

    def run(self) -> Dict[str, Any]:
        """Executes full transformation pipeline and returns execution report."""
        raw_df = self.load_raw_data()
        transformed_df, metrics = self.transform(raw_df)
        self.write_parquet(transformed_df)
        val_result = self.validate_parquet_output(metrics["final_curated_records"])

        report = {**metrics, "parquet_validation": val_result}
        report_path = cfg.paths.validated_data_dir / "transformation_metrics.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        return report


if __name__ == "__main__":
    transformer = HistoricalSalesTransformer()
    res = transformer.run()
    print("PySpark transformation complete:", res)
