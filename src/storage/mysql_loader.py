"""
MySQL Idempotent Data Loader & Pipeline Auditor
Loads curated sales facts and dimension records using atomic transactions
and MySQL UPSERT (INSERT ... ON DUPLICATE KEY UPDATE).
"""

from datetime import datetime, timezone
import time
from typing import Any, Dict, List, Optional
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from src.utils.logger_config import get_logger
from src.utils.config import DatabaseConfig, cfg

logger = get_logger("storage.mysql_loader")


class MySQLLoaderError(Exception):
    """Raised when MySQL loading or transaction fails."""
    pass


class MySQLLoader:
    """Manages transactional, idempotent loads into MySQL data warehouse."""

    def __init__(self, db_config: Optional[DatabaseConfig] = None):
        self.config = db_config or cfg.db
        self._engine: Optional[Engine] = None

    @property
    def engine(self) -> Engine:
        """Lazy initialization of MySQL engine."""
        if self._engine is None:
            self._engine = create_engine(
                self.config.connection_url,
                pool_size=self.config.pool_size,
                pool_pre_ping=True
            )
        return self._engine

    def load_fact_sales(self, df: pd.DataFrame, batch_size: int = 1000) -> int:
        """
        Idempotently inserts or updates sales records in fact_sales.
        
        Args:
            df: Pandas DataFrame containing curated sales facts.
            batch_size: Number of records per chunk.
            
        Returns:
            Count of rows processed.
        """
        if df.empty:
            logger.warning("Empty DataFrame passed to load_fact_sales. Skipping.")
            return 0

        logger.info(f"Loading {len(df)} records into fact_sales (idempotent UPSERT)")
        start_time = time.time()

        upsert_query = text("""
            INSERT INTO fact_sales (
                order_id, order_date, customer_id, product_id, quantity, unit_price,
                discount, gross_amount, net_amount, region, payment_method,
                order_status, source_system, ingestion_timestamp
            ) VALUES (
                :order_id, :order_date, :customer_id, :product_id, :quantity, :unit_price,
                :discount, :gross_amount, :net_amount, :region, :payment_method,
                :order_status, :source_system, :ingestion_timestamp
            )
            ON DUPLICATE KEY UPDATE
                order_date = VALUES(order_date),
                customer_id = VALUES(customer_id),
                quantity = VALUES(quantity),
                unit_price = VALUES(unit_price),
                discount = VALUES(discount),
                gross_amount = VALUES(gross_amount),
                net_amount = VALUES(net_amount),
                region = VALUES(region),
                payment_method = VALUES(payment_method),
                order_status = VALUES(order_status),
                source_system = VALUES(source_system),
                ingestion_timestamp = VALUES(ingestion_timestamp)
        """)

        # Clean NaN/None to Python None for clean SQL NULL insertion
        records: List[Dict[str, Any]] = df.to_dict(orient="records")
        for r in records:
            for k, v in r.items():
                if pd.isna(v):
                    r[k] = None

        total_loaded = 0
        try:
            with self.engine.begin() as conn:
                for i in range(0, len(records), batch_size):
                    chunk = records[i : i + batch_size]
                    conn.execute(upsert_query, chunk)
                    total_loaded += len(chunk)
                    logger.debug(f"Loaded batch {i // batch_size + 1}: {len(chunk)} rows")

            duration = round(time.time() - start_time, 2)
            logger.info(f"Successfully UPSERT-loaded {total_loaded} records into fact_sales in {duration}s")
            return total_loaded

        except SQLAlchemyError as e:
            error_msg = f"Database transaction failed during fact_sales loading: {str(e)}"
            logger.error(error_msg)
            raise MySQLLoaderError(error_msg) from e

    def sync_dimensions(self, products_df: pd.DataFrame, customers_df: pd.DataFrame) -> None:
        """
        Synchronizes dim_products and dim_customers with latest master data.
        """
        logger.info("Synchronizing dim_products and dim_customers dimensions")

        upsert_prod = text("""
            INSERT INTO dim_products (product_id, product_name, category, brand, price, stock)
            VALUES (:product_id, :product_name, :category, :brand, :price, :stock)
            ON DUPLICATE KEY UPDATE
                product_name = VALUES(product_name),
                category = VALUES(category),
                brand = VALUES(brand),
                price = VALUES(price),
                stock = VALUES(stock)
        """)

        upsert_cust = text("""
            INSERT INTO dim_customers (customer_id, customer_name, city, state, country)
            VALUES (:customer_id, :customer_name, :city, :state, :country)
            ON DUPLICATE KEY UPDATE
                customer_name = VALUES(customer_name),
                city = VALUES(city),
                state = VALUES(state),
                country = VALUES(country)
        """)

        try:
            with self.engine.begin() as conn:
                if not products_df.empty:
                    p_records = products_df[[
                        "product_id", "product_name", "category", "brand", "price", "stock"
                    ]].to_dict(orient="records")
                    conn.execute(upsert_prod, p_records)
                    logger.info(f"Synchronized {len(p_records)} records to dim_products")

                if not customers_df.empty:
                    c_records = customers_df[[
                        "customer_id", "customer_name", "city", "state", "country"
                    ]].to_dict(orient="records")
                    conn.execute(upsert_cust, c_records)
                    logger.info(f"Synchronized {len(c_records)} records to dim_customers")

        except SQLAlchemyError as e:
            err = f"Failed synchronizing dimension tables: {str(e)}"
            logger.error(err)
            raise MySQLLoaderError(err) from e

    def log_pipeline_run(
        self,
        run_id: str,
        pipeline_name: str,
        start_time: datetime,
        end_time: Optional[datetime],
        status: str,
        records_processed: int = 0,
        records_rejected: int = 0,
        error_message: Optional[str] = None
    ) -> None:
        """Records pipeline telemetry into etl_pipeline_logs."""
        log_query = text("""
            INSERT INTO etl_pipeline_logs (
                run_id, pipeline_name, start_time, end_time, status,
                records_processed, records_rejected, error_message
            ) VALUES (
                :run_id, :pipeline_name, :start_time, :end_time, :status,
                :records_processed, :records_rejected, :error_message
            )
            ON DUPLICATE KEY UPDATE
                end_time = VALUES(end_time),
                status = VALUES(status),
                records_processed = VALUES(records_processed),
                records_rejected = VALUES(records_rejected),
                error_message = VALUES(error_message)
        """)

        try:
            with self.engine.begin() as conn:
                conn.execute(log_query, {
                    "run_id": run_id,
                    "pipeline_name": pipeline_name,
                    "start_time": start_time,
                    "end_time": end_time,
                    "status": status,
                    "records_processed": records_processed,
                    "records_rejected": records_rejected,
                    "error_message": error_message
                })
            logger.info(f"Updated pipeline audit log: run_id={run_id}, status={status}")
        except Exception as e:
            logger.error(f"Failed to record pipeline audit log to database: {str(e)}")

    def close(self):
        """Safely releases engine and connection pool."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            logger.info("MySQL loader connection pool disposed safely")
