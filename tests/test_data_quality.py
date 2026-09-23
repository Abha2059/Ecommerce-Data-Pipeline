"""
Data Quality & Integrity Test Suite
Tests schema validity, deduplication, date parsing, numerical derivations,
Parquet partitioning, MySQL warehouse loading, and absence of synthetic artifacts.
"""

from decimal import Decimal
import json
from pathlib import Path
import sys
import pytest

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import pyarrow.dataset as ds
from sqlalchemy import create_engine, text

from src.extract.historical_extractor import HistoricalDataExtractor
from src.load.historical_mysql_loader import HistoricalWarehouseLoader
from src.spark.historical_sales_transformations import HistoricalSalesTransformer
from src.transformation.spark_session import get_spark_session
from src.utils.config import cfg
from src.validation.raw_validator import RawDataValidator


@pytest.fixture(scope="session")
def spark_session():
    """Provides a shared local PySpark session for unit tests."""
    spark = get_spark_session()
    yield spark


@pytest.fixture(scope="session")
def db_engine():
    """Provides a shared SQLAlchemy engine connected to local MySQL ecommerce_dw."""
    engine = create_engine(cfg.db.connection_url)
    yield engine
    engine.dispose()


class TestRawDataIngestionQuality:
    """Verifies raw download and provenance metadata."""

    def test_raw_file_presence_and_metadata(self):
        """Verifies raw CSV and metadata file existence and authenticity."""
        assert cfg.paths.raw_csv_path.exists(), f"Raw CSV missing at {cfg.paths.raw_csv_path}"
        assert cfg.paths.metadata_path.exists(), f"Metadata JSON missing at {cfg.paths.metadata_path}"

        with open(cfg.paths.metadata_path, "r", encoding="utf-8") as f:
            meta = json.load(f)

        assert meta["source_name"] == "UCI Machine Learning Repository - Online Retail"
        assert meta["record_count"] == 541909
        assert meta["column_count"] == 8
        assert "InvoiceNo" in meta["columns"]
        assert "StockCode" in meta["columns"]
        assert meta["csv_file_bytes"] > 40 * 1024 * 1024  # > 40MB
        assert len(meta["sha256_csv"]) == 64

    def test_raw_columns_and_non_empty(self):
        """Verifies raw columns match the verified UCI schema."""
        sample_df = pd.read_csv(cfg.paths.raw_csv_path, nrows=10)
        expected_cols = [
            "InvoiceNo",
            "StockCode",
            "Description",
            "Quantity",
            "InvoiceDate",
            "UnitPrice",
            "CustomerID",
            "Country",
        ]
        assert list(sample_df.columns) == expected_cols


class TestPySparkTransformations:
    """Tests PySpark data transformations, derivations, and schema rules."""

    def test_transformation_derivation_logic(self, spark_session):
        """Validates cancellation flagging, gross amount calculation, and date partitioning."""
        sample_data = [
            ("536365", "85123A", "WHITE HANGING HEART", 6, "2010-12-01 08:26:00", Decimal("2.55"), "17850.0", "United Kingdom"),
            ("C536379", "D", "Discount", -1, "2010-12-01 09:41:00", Decimal("27.50"), "17560.0", "United Kingdom"),
            ("536380", "22865", "HAND WARMER", 12, "2011-05-15 11:30:00", Decimal("1.25"), None, "France"),
        ]
        columns = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
        raw_df = spark_session.createDataFrame(sample_data, columns)

        transformer = HistoricalSalesTransformer(spark=spark_session)
        transformed_df, metrics = transformer.transform(raw_df)

        rows = {r["invoice_no"]: r for r in transformed_df.collect()}

        # 1. Normal order: is_cancellation=0, gross_amount = 6 * 2.55 = 15.30
        r1 = rows["536365"]
        assert r1["is_cancellation"] == 0
        assert r1["gross_amount"] == Decimal("15.30")
        assert r1["transaction_year"] == 2010
        assert r1["transaction_month"] == 12
        assert r1["customer_id"] == 17850

        # 2. Cancellation: starts with C and negative quantity -> is_cancellation=1
        r2 = rows["C536379"]
        assert r2["is_cancellation"] == 1
        assert r2["gross_amount"] == Decimal("-27.50")

        # 3. Guest order: customer_id is None
        r3 = rows["536380"]
        assert r3["customer_id"] is None
        assert r3["is_cancellation"] == 0
        assert r3["gross_amount"] == Decimal("15.00")
        assert r3["transaction_year"] == 2011
        assert r3["transaction_month"] == 5

    def test_deduplication(self, spark_session):
        """Validates that identical duplicate transactions are removed."""
        sample_data = [
            ("536365", "85123A", "ITEM", 6, "2010-12-01 08:26:00", Decimal("2.55"), "17850", "United Kingdom"),
            ("536365", "85123A", "ITEM", 6, "2010-12-01 08:26:00", Decimal("2.55"), "17850", "United Kingdom"),  # Duplicate
        ]
        columns = ["InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"]
        raw_df = spark_session.createDataFrame(sample_data, columns)

        transformer = HistoricalSalesTransformer(spark=spark_session)
        transformed_df, metrics = transformer.transform(raw_df)

        assert transformed_df.count() == 1
        assert metrics["duplicates_removed"] == 1


class TestParquetStorageQuality:
    """Verifies curated Snappy Parquet partitioning and schema."""

    def test_parquet_directory_and_partitions(self):
        """Verifies partitioned Parquet files exist on disk."""
        assert cfg.paths.processed_data_dir.exists()
        year_dirs = list(cfg.paths.processed_data_dir.glob("transaction_year=*"))
        assert len(year_dirs) >= 2, "Expected at least 2010 and 2011 year partitions"

        month_dirs = list(cfg.paths.processed_data_dir.glob("transaction_year=*/transaction_month=*"))
        assert len(month_dirs) >= 12, "Expected partitioned months across the historical period"

    def test_parquet_record_counts(self):
        """Verifies row count in Parquet equals 536,480 (541,909 raw minus 5,429 duplicates)."""
        dataset = ds.dataset(str(cfg.paths.processed_data_dir), format="parquet", partitioning="hive")
        assert dataset.count_rows() == 536480


class TestDataWarehouseIntegrity:
    """Verifies MySQL 8.0 tables, constraints, and referential integrity."""

    def test_fact_sales_row_count(self, db_engine):
        """Verifies fact_sales table contains loaded historical records."""
        with db_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM fact_sales")).scalar()
            assert count == 536480, f"Expected 536,480 fact rows, got {count}"

    def test_dim_products_row_count_and_validity(self, db_engine):
        """Verifies dim_products is populated with non-zero products."""
        with db_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM dim_products")).scalar()
            assert count == 3958, f"Expected 3,958 distinct products, got {count}"

            # Verify no null stock_code
            nulls = conn.execute(text("SELECT COUNT(*) FROM dim_products WHERE stock_code IS NULL")).scalar()
            assert nulls == 0

    def test_dim_customers_validity(self, db_engine):
        """Verifies dim_customers contains only valid registered customers."""
        with db_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM dim_customers")).scalar()
            assert count == 4372, f"Expected 4,372 distinct registered customers, got {count}"

            # Verify no non-positive customer_id
            invalid_ids = conn.execute(text("SELECT COUNT(*) FROM dim_customers WHERE customer_id <= 0")).scalar()
            assert invalid_ids == 0

    def test_customer_referential_consistency(self, db_engine):
        """Verifies that non-null customer_id values in fact_sales exist in dim_customers."""
        with db_engine.connect() as conn:
            orphan_count = conn.execute(text("""
                SELECT COUNT(DISTINCT f.customer_id)
                FROM fact_sales f
                LEFT JOIN dim_customers c ON f.customer_id = c.customer_id
                WHERE f.customer_id IS NOT NULL AND c.customer_id IS NULL
            """)).scalar()
            assert orphan_count == 0, f"Found {orphan_count} orphan customer IDs in fact_sales"

    def test_zero_synthetic_data_columns(self, db_engine):
        """Asserts that no legacy synthetic columns exist in the fact_sales table."""
        with db_engine.connect() as conn:
            cols = [
                row[0]
                for row in conn.execute(text("DESCRIBE fact_sales")).fetchall()
            ]
            # Ensure fake fields are not in fact_sales
            assert "discount" not in cols
            assert "store_id" not in cols
            assert "payment_method" not in cols
            assert "order_status" not in cols
            # Ensure genuine fields exist
            assert "invoice_no" in cols
            assert "stock_code" in cols
            assert "is_cancellation" in cols
            assert "gross_amount" in cols
            assert "unit_price" in cols
