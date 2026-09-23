"""
MySQL Master Data Extractor
Extracts dimension and master entity tables (customers, products, stores) from MySQL with connection pooling.
"""

from datetime import datetime, timezone
from pathlib import Path
import time
from typing import Optional
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from src.utils.logger_config import get_logger
from src.utils.config import DatabaseConfig, cfg

logger = get_logger("extract.mysql_extractor")


class MySQLExtractor:
    """Extracts master relational data from MySQL using SQLAlchemy."""

    def __init__(self, db_config: Optional[DatabaseConfig] = None):
        self.config = db_config or cfg.db
        self._engine: Optional[Engine] = None

    @property
    def engine(self) -> Engine:
        """Lazy initialization of database engine with connection pooling."""
        if self._engine is None:
            logger.info(
                f"Initializing MySQL connection pool to {self.config.user}@{self.config.host}:{self.config.port}/{self.config.database}"
            )
            self._engine = create_engine(
                self.config.connection_url,
                pool_size=self.config.pool_size,
                pool_recycle=3600,
                pool_pre_ping=True
            )
        return self._engine

    def test_connection(self) -> bool:
        """Verifies database availability."""
        try:
            with self.engine.connect() as conn:
                res = conn.execute(text("SELECT 1")).scalar()
                return res == 1
        except SQLAlchemyError as e:
            logger.error(f"MySQL connection check failed: {str(e)}")
            return False

    def sync_master_data_from_files_if_needed(self, sample_dir: Optional[Path] = None):
        """
        Populates MySQL master tables from generated master CSVs if tables are empty.
        Ensures foreign keys always reconcile.
        """
        dir_path = sample_dir or cfg.base_dir / "data" / "sample"
        cust_csv = dir_path / "master_customers.csv"
        prod_csv = dir_path / "master_products.csv"
        store_csv = dir_path / "master_stores.csv"

        if not (cust_csv.exists() and prod_csv.exists() and store_csv.exists()):
            return

        with self.engine.connect() as conn:
            cust_count = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar()
            if cust_count and cust_count > 50:
                logger.info(f"MySQL master tables already populated ({cust_count} customers).")
                return

        logger.info("Seeding MySQL master tables with generated synthetic dimensions...")
        cust_df = pd.read_csv(cust_csv)
        prod_df = pd.read_csv(prod_csv)
        store_df = pd.read_csv(store_csv)

        with self.engine.begin() as conn:
            cust_df.to_sql("customers", con=conn, if_exists="append", index=False)
            cust_df.to_sql("dim_customers", con=conn, if_exists="append", index=False)

            # Map unit_price -> price
            if "unit_price" in prod_df.columns and "price" not in prod_df.columns:
                prod_df["price"] = prod_df["unit_price"]
            p_cols = ["product_id", "product_name", "category", "brand", "price", "stock"]
            prod_df[p_cols].to_sql("products", con=conn, if_exists="append", index=False)
            prod_df[p_cols].to_sql("dim_products", con=conn, if_exists="append", index=False)

            store_df.to_sql("stores", con=conn, if_exists="append", index=False)
            store_df.to_sql("dim_stores", con=conn, if_exists="append", index=False)

        logger.info(f"Seeded {len(cust_df)} customers, {len(prod_df)} products, and {len(store_df)} stores into MySQL.")

    def read_table(self, table_name: str) -> pd.DataFrame:
        """Reads table into a DataFrame with lineage metadata."""
        logger.info(f"Extracting table '{table_name}' from MySQL")
        start = time.time()
        with self.engine.connect() as conn:
            df = pd.read_sql_table(table_name, con=conn)
        elapsed = round(time.time() - start, 2)
        logger.info(f"Extracted {len(df)} records from '{table_name}' in {elapsed}s")
        df["source_system"] = "MYSQL_MASTER"
        df["ingestion_timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        return df

    def read_customers(self) -> pd.DataFrame:
        return self.read_table("customers")

    def read_products(self) -> pd.DataFrame:
        return self.read_table("products")

    def read_stores(self) -> pd.DataFrame:
        return self.read_table("stores")

    def close(self):
        if self._engine:
            self._engine.dispose()
            self._engine = None
