"""
MySQL Data Warehouse Loader
Manages transactional and idempotent upsert loading into dimensional and aggregate tables.
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

logger = get_logger("load.mysql_warehouse_loader")


class MySQLWarehouseLoader:
    """Manages transactional and idempotent loads into MySQL dimensional and aggregate tables."""

    def __init__(self, db_config: Optional[DatabaseConfig] = None):
        self.config = db_config or cfg.db
        self._engine: Optional[Engine] = None

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(
                self.config.connection_url,
                pool_size=self.config.pool_size,
                pool_pre_ping=True
            )
        return self._engine

    def sync_dimensions(
        self,
        customers_df: Optional[pd.DataFrame] = None,
        products_df: Optional[pd.DataFrame] = None,
        stores_df: Optional[pd.DataFrame] = None
    ) -> None:
        """Synchronizes dim_customers, dim_products, and dim_stores using UPSERT."""
        logger.info("Synchronizing data warehouse dimensions (customers, products, stores)")

        upsert_cust = text("""
            INSERT INTO dim_customers (customer_id, customer_name, customer_email, city, state, customer_segment)
            VALUES (:customer_id, :customer_name, :customer_email, :city, :state, :customer_segment)
            ON DUPLICATE KEY UPDATE
                customer_name = VALUES(customer_name),
                customer_email = VALUES(customer_email),
                city = VALUES(city),
                state = VALUES(state),
                customer_segment = VALUES(customer_segment)
        """)

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

        upsert_store = text("""
            INSERT INTO dim_stores (store_id, store_name, city, state)
            VALUES (:store_id, :store_name, :city, :state)
            ON DUPLICATE KEY UPDATE
                store_name = VALUES(store_name),
                city = VALUES(city),
                state = VALUES(state)
        """)

        with self.engine.begin() as conn:
            if customers_df is not None and not customers_df.empty:
                c_cols = ["customer_id", "customer_name", "customer_email", "city", "state", "customer_segment"]
                existing_cols = [c for c in c_cols if c in customers_df.columns]
                c_recs = customers_df[existing_cols].to_dict(orient="records")
                conn.execute(upsert_cust, c_recs)
                logger.info(f"Upserted {len(c_recs)} records to dim_customers")

            if products_df is not None and not products_df.empty:
                p_df = products_df.copy()
                if "unit_price" in p_df.columns and "price" not in p_df.columns:
                    p_df["price"] = p_df["unit_price"]
                p_cols = ["product_id", "product_name", "category", "brand", "price", "stock"]
                existing_p = [c for c in p_cols if c in p_df.columns]
                p_recs = p_df[existing_p].to_dict(orient="records")
                conn.execute(upsert_prod, p_recs)
                logger.info(f"Upserted {len(p_recs)} records to dim_products")

            if stores_df is not None and not stores_df.empty:
                s_cols = ["store_id", "store_name", "city", "state"]
                existing_s = [c for c in s_cols if c in stores_df.columns]
                s_recs = stores_df[existing_s].to_dict(orient="records")
                conn.execute(upsert_store, s_recs)
                logger.info(f"Upserted {len(s_recs)} records to dim_stores")

    def load_daily_aggregates(self, agg_df: pd.DataFrame) -> int:
        """
        Loads aggregated daily metrics into agg_daily_sales.
        Allows fast dashboard analytics without scanning millions of fact rows.
        """
        if agg_df.empty:
            logger.warning("Empty aggregate DataFrame provided for warehouse loading.")
            return 0

        logger.info(f"Loading {len(agg_df)} summary records into agg_daily_sales (UPSERT)")
        start = time.time()

        upsert_agg = text("""
            INSERT INTO agg_daily_sales (
                sale_date, store_id, category, total_orders, total_items_sold,
                gross_revenue, total_discount, net_revenue
            ) VALUES (
                :sale_date, :store_id, :category, :total_orders, :total_items_sold,
                :gross_revenue, :total_discount, :net_revenue
            )
            ON DUPLICATE KEY UPDATE
                total_orders = VALUES(total_orders),
                total_items_sold = VALUES(total_items_sold),
                gross_revenue = VALUES(gross_revenue),
                total_discount = VALUES(total_discount),
                net_revenue = VALUES(net_revenue)
        """)

        records = agg_df.to_dict(orient="records")
        with self.engine.begin() as conn:
            conn.execute(upsert_agg, records)

        duration = round(time.time() - start, 2)
        logger.info(f"Loaded {len(records)} aggregate rows into agg_daily_sales in {duration}s")
        return len(records)

    def load_fact_sales_sample(self, fact_df: pd.DataFrame, max_rows: int = 50000) -> int:
        """
        Loads fact sales transactions up to max_rows using batch UPSERT.
        """
        if fact_df.empty:
            return 0

        sample_df = fact_df.head(max_rows)
        logger.info(f"Loading {len(sample_df)} records into fact_sales (idempotent UPSERT)")

        upsert_query = text("""
            INSERT INTO fact_sales (
                order_id, order_date, customer_id, product_id, store_id, quantity, unit_price,
                discount, gross_amount, net_amount, payment_method,
                order_status, source_system, ingestion_timestamp
            ) VALUES (
                :order_id, :order_date, :customer_id, :product_id, :store_id, :quantity, :unit_price,
                :discount, :gross_amount, :net_amount, :payment_method,
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
                payment_method = VALUES(payment_method),
                order_status = VALUES(order_status),
                source_system = VALUES(source_system),
                ingestion_timestamp = VALUES(ingestion_timestamp)
        """)

        records = sample_df.to_dict(orient="records")
        for r in records:
            for k, v in r.items():
                if pd.isna(v):
                    r[k] = None

        batch_size = 1000
        with self.engine.begin() as conn:
            for i in range(0, len(records), batch_size):
                chunk = records[i : i + batch_size]
                conn.execute(upsert_query, chunk)

        logger.info(f"Successfully loaded {len(records)} fact_sales records into MySQL")
        return len(records)

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
        except Exception as e:
            logger.error(f"Failed to record audit log: {str(e)}")

    def close(self):
        if self._engine:
            self._engine.dispose()
            self._engine = None
