"""
MySQL Master Data Reader (Source C)
Connects to MySQL to extract master customers, products, and retail stores.
"""

from datetime import datetime, timezone
import time
from typing import Optional
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from src.utils.logger_config import get_logger
from src.utils.config import DatabaseConfig, cfg

logger = get_logger("ingestion.mysql_reader")


class MySQLExtractionError(Exception):
    """Custom exception for MySQL extraction failures."""
    pass


class MySQLReader:
    """Extracts master relational data from MySQL using SQLAlchemy."""

    def __init__(self, db_config: Optional[DatabaseConfig] = None):
        self.config = db_config or cfg.db
        self._engine: Optional[Engine] = None

    @property
    def engine(self) -> Engine:
        """Lazy initialization of database engine with connection pooling."""
        if self._engine is None:
            # Mask host and user in logs
            logger.info(f"Establishing MySQL connection pool to {self.config.user}@{self.config.host}:{self.config.port}/{self.config.database}")
            self._engine = create_engine(
                self.config.connection_url,
                pool_size=self.config.pool_size,
                pool_recycle=3600,
                pool_pre_ping=True
            )
        return self._engine

    def test_connection(self) -> bool:
        """Validates that MySQL server is reachable and database exists."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).scalar()
                is_valid = result == 1
                logger.info("MySQL connection verified successfully")
                return is_valid
        except SQLAlchemyError as e:
            logger.error(f"MySQL connection verification failed: {str(e)}")
            return False

    def read_table(self, table_name: str) -> pd.DataFrame:
        """Generic table reader with lineage metadata enrichment."""
        logger.info(f"Extracting table '{table_name}' from MySQL database '{self.config.database}'")
        start_time = time.time()
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql_table(table_name, con=conn)
                elapsed = round(time.time() - start_time, 2)
                logger.info(f"Extracted {len(df)} records from '{table_name}' in {elapsed}s")

                df["source_system"] = "MYSQL_MASTER"
                df["ingestion_timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                return df
        except Exception as e:
            error_msg = f"Failed extracting table '{table_name}': {str(e)}"
            logger.error(error_msg)
            raise MySQLExtractionError(error_msg) from e

    def read_customers(self) -> pd.DataFrame:
        """Extracts master customers table."""
        return self.read_table("customers")

    def read_products(self) -> pd.DataFrame:
        """Extracts master products table."""
        return self.read_table("products")

    def read_stores(self) -> pd.DataFrame:
        """Extracts master retail stores table."""
        return self.read_table("stores")

    def close(self):
        """Safely disposes database connection pool."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            logger.info("MySQL connection pool closed safely")
