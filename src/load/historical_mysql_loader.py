"""
Historical MySQL Warehouse Loader
Manages transactional and idempotent loading of historical e-commerce data
into fact_sales, dim_products, dim_customers, and etl_pipeline_logs.
"""

from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
from typing import Any, Dict, List, Optional

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import pyarrow.dataset as ds
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from src.utils.config import DatabaseConfig, cfg
from src.utils.logger_config import get_logger

logger = get_logger("load.historical_mysql_loader")


class LoaderError(Exception):
    """Raised when data warehouse loading or validation fails."""
    pass


class HistoricalWarehouseLoader:
    """
    Loads partitioned historical Parquet transactions and derived dimensions into MySQL 8.0.
    Ensures idempotency, transactional consistency, and row-count verification.
    """

    def __init__(
        self,
        parquet_dir: Optional[Path] = None,
        db_config: Optional[DatabaseConfig] = None,
    ):
        self.parquet_dir = parquet_dir or cfg.paths.processed_data_dir
        self.config = db_config or cfg.db
        self._engine: Optional[Engine] = None

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            logger.info(f"Connecting to MySQL database '{self.config.database}' at {self.config.host}:{self.config.port}")
            self._engine = create_engine(
                self.config.connection_url,
                pool_size=self.config.pool_size,
                pool_pre_ping=True,
            )
        return self._engine

    def load_dimensions(self) -> Dict[str, int]:
        """
        Derives and idempotently loads dim_products and dim_customers from the curated Parquet dataset.
        """
        logger.info("Extracting and loading dimension tables from Parquet dataset...")
        dataset = ds.dataset(str(self.parquet_dir), format="parquet", partitioning="hive")

        # 1. Product Dimension
        logger.info("Deriving dim_products from transaction line items...")
        prod_table = dataset.to_table(columns=["stock_code", "description", "unit_price", "invoice_date", "quantity"])
        prod_df = prod_table.to_pandas()

        prod_agg = prod_df.groupby("stock_code").agg(
            description=("description", "last"),
            latest_unit_price=("unit_price", "last"),
            first_sold_at=("invoice_date", "min"),
            last_sold_at=("invoice_date", "max"),
            total_units_sold=("quantity", "sum"),
        ).reset_index()

        prod_recs = prod_agg.to_dict(orient="records")
        for r in prod_recs:
            for k, v in r.items():
                if pd.isna(v):
                    r[k] = None

        upsert_prod = text("""
            INSERT INTO dim_products (
                stock_code, description, latest_unit_price, first_sold_at, last_sold_at, total_units_sold
            ) VALUES (
                :stock_code, :description, :latest_unit_price, :first_sold_at, :last_sold_at, :total_units_sold
            )
            ON DUPLICATE KEY UPDATE
                description = VALUES(description),
                latest_unit_price = VALUES(latest_unit_price),
                last_sold_at = VALUES(last_sold_at),
                total_units_sold = VALUES(total_units_sold)
        """)

        # 2. Customer Dimension (only real registered customers with non-null CustomerID)
        logger.info("Deriving dim_customers from registered customer transactions...")
        cust_table = dataset.to_table(columns=["customer_id", "country", "invoice_no", "gross_amount", "invoice_date"])
        cust_df = cust_table.to_pandas()
        registered_cust = cust_df[cust_df["customer_id"].notna()].copy()
        registered_cust["customer_id"] = registered_cust["customer_id"].astype(int)

        cust_agg = registered_cust.groupby("customer_id").agg(
            country=("country", "first"),
            total_orders=("invoice_no", "nunique"),
            total_spend=("gross_amount", "sum"),
            first_order_date=("invoice_date", "min"),
            last_order_date=("invoice_date", "max"),
        ).reset_index()

        cust_recs = cust_agg.to_dict(orient="records")
        for r in cust_recs:
            for k, v in r.items():
                if pd.isna(v):
                    r[k] = None

        upsert_cust = text("""
            INSERT INTO dim_customers (
                customer_id, country, total_orders, total_spend, first_order_date, last_order_date
            ) VALUES (
                :customer_id, :country, :total_orders, :total_spend, :first_order_date, :last_order_date
            )
            ON DUPLICATE KEY UPDATE
                country = VALUES(country),
                total_orders = VALUES(total_orders),
                total_spend = VALUES(total_spend),
                last_order_date = VALUES(last_order_date)
        """)

        with self.engine.begin() as conn:
            # Batch insert products
            batch_size = 1000
            for i in range(0, len(prod_recs), batch_size):
                conn.execute(upsert_prod, prod_recs[i : i + batch_size])
            logger.info(f"Loaded {len(prod_recs):,} products into dim_products")

            # Batch insert customers
            for i in range(0, len(cust_recs), batch_size):
                conn.execute(upsert_cust, cust_recs[i : i + batch_size])
            logger.info(f"Loaded {len(cust_recs):,} registered customers into dim_customers")

        return {"dim_products_loaded": len(prod_recs), "dim_customers_loaded": len(cust_recs)}

    def load_fact_sales(self, batch_size: int = 10000, max_records: Optional[int] = None) -> int:
        """
        Loads fact sales transactions in memory-efficient streaming batches.
        Uses INSERT IGNORE for idempotency on composite key.
        """
        logger.info(f"Loading fact_sales from Parquet dataset in batches of {batch_size:,}...")
        dataset = ds.dataset(str(self.parquet_dir), format="parquet", partitioning="hive")

        insert_query = text("""
            INSERT IGNORE INTO fact_sales (
                invoice_no, stock_code, description, quantity, invoice_date, unit_price,
                customer_id, country, gross_amount, is_cancellation, transaction_year,
                transaction_month, source_system, ingestion_timestamp
            ) VALUES (
                :invoice_no, :stock_code, :description, :quantity, :invoice_date, :unit_price,
                :customer_id, :country, :gross_amount, :is_cancellation, :transaction_year,
                :transaction_month, :source_system, :ingestion_timestamp
            )
        """)

        total_inserted = 0
        scanner = dataset.scanner(batch_size=batch_size)

        with self.engine.begin() as conn:
            for record_batch in scanner.to_batches():
                chunk_df = record_batch.to_pandas()
                recs = chunk_df.to_dict(orient="records")

                for r in recs:
                    for k, v in r.items():
                        if pd.isna(v):
                            r[k] = None

                conn.execute(insert_query, recs)
                total_inserted += len(recs)
                logger.info(f"Loaded batch: {total_inserted:,} cumulative fact_sales rows processed")

                if max_records and total_inserted >= max_records:
                    logger.info(f"Reached specified load ceiling: {max_records:,} rows")
                    break

        logger.info(f"Finished loading fact_sales: {total_inserted:,} total records handled.")
        return total_inserted

    def validate_warehouse_counts(self) -> Dict[str, int]:
        """Validates row counts in all warehouse tables."""
        with self.engine.connect() as conn:
            fact_count = conn.execute(text("SELECT COUNT(*) FROM fact_sales")).scalar() or 0
            prod_count = conn.execute(text("SELECT COUNT(*) FROM dim_products")).scalar() or 0
            cust_count = conn.execute(text("SELECT COUNT(*) FROM dim_customers")).scalar() or 0

        logger.info(f"Warehouse validation counts -> fact_sales: {fact_count:,}, dim_products: {prod_count:,}, dim_customers: {cust_count:,}")
        return {
            "fact_sales_count": fact_count,
            "dim_products_count": prod_count,
            "dim_customers_count": cust_count,
        }

    def log_pipeline_run(
        self,
        run_id: str,
        pipeline_name: str,
        start_time: datetime,
        end_time: Optional[datetime],
        status: str,
        records_processed: int = 0,
        records_rejected: int = 0,
        error_message: Optional[str] = None,
        metrics_json: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Records pipeline run telemetry into etl_pipeline_logs."""
        log_query = text("""
            INSERT INTO etl_pipeline_logs (
                run_id, pipeline_name, start_time, end_time, status,
                records_processed, records_rejected, error_message, metrics_json
            ) VALUES (
                :run_id, :pipeline_name, :start_time, :end_time, :status,
                :records_processed, :records_rejected, :error_message, :metrics_json
            )
            ON DUPLICATE KEY UPDATE
                end_time = VALUES(end_time),
                status = VALUES(status),
                records_processed = VALUES(records_processed),
                records_rejected = VALUES(records_rejected),
                error_message = VALUES(error_message),
                metrics_json = VALUES(metrics_json)
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(
                    log_query,
                    {
                        "run_id": run_id,
                        "pipeline_name": pipeline_name,
                        "start_time": start_time,
                        "end_time": end_time,
                        "status": status,
                        "records_processed": records_processed,
                        "records_rejected": records_rejected,
                        "error_message": error_message,
                        "metrics_json": json.dumps(metrics_json) if metrics_json else None,
                    },
                )
            logger.info(f"Logged pipeline run {run_id} status: {status}")
        except Exception as e:
            logger.error(f"Failed to record pipeline log: {e}")

    def run(self, max_fact_records: Optional[int] = None) -> Dict[str, Any]:
        """Runs the entire warehouse loading sequence."""
        dim_results = self.load_dimensions()
        fact_rows = self.load_fact_sales(max_records=max_fact_records)
        counts = self.validate_warehouse_counts()
        return {**dim_results, "fact_rows_inserted": fact_rows, **counts}

    def close(self):
        if self._engine:
            self._engine.dispose()
            self._engine = None


if __name__ == "__main__":
    loader = HistoricalWarehouseLoader()
    res = loader.run()
    print("Warehouse load complete:", res)
    loader.close()
