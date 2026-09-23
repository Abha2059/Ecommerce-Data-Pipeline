"""
Historical E-Commerce Data Engineering Pipeline CLI Runner
Executes extraction, validation, PySpark transformations, Snappy Parquet partitioning,
and MySQL 8.0 Data Warehouse loading end-to-end with execution telemetry.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.extract.historical_extractor import HistoricalDataExtractor
from src.load.historical_mysql_loader import HistoricalWarehouseLoader
from src.spark.historical_sales_transformations import HistoricalSalesTransformer
from src.utils.config import cfg
from src.utils.logger_config import get_logger
from src.validation.raw_validator import RawDataValidator

logger = get_logger("run_historical_pipeline")


def run_pipeline(
    force_extract: bool = False,
    skip_spark: bool = False,
    skip_load: bool = False,
    max_fact_records: int = None,
):
    """Executes the complete historical data pipeline."""
    run_id = f"RUN_HISTORICAL_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    start_time = datetime.now(timezone.utc)
    t0 = time.time()

    print("=" * 80)
    print("HISTORICAL E-COMMERCE DATA ENGINEERING PIPELINE")
    print("Source: UCI Machine Learning Repository - Online Retail Dataset (2010 - 2011)")
    print(f"Run ID: {run_id}")
    print("=" * 80)

    loader = HistoricalWarehouseLoader()

    try:
        # Stage 1: Extraction & Raw Preservation
        print("\n[STAGE 1/5] Extracting & Preserving Verified Historical Data...")
        extractor = HistoricalDataExtractor()
        extract_meta = extractor.extract(force=force_extract)
        print(f"  -> Source: {extract_meta['source_name']}")
        print(f"  -> Historical Period: {extract_meta['historical_period']}")
        print(f"  -> License: {extract_meta['license']}")
        print(f"  -> Preserved Raw Records: {extract_meta['record_count']:,}")
        print(f"  -> Raw CSV Size: {extract_meta['csv_file_bytes'] / (1024*1024):.2f} MB")
        print(f"  -> SHA-256: {extract_meta['sha256_csv'][:16]}...")

        # Stage 2: Raw Validation
        print("\n[STAGE 2/5] Running Raw Schema & Integrity Validation...")
        validator = RawDataValidator()
        raw_report = validator.validate()
        print(f"  -> Status: {raw_report['status']}")
        print(f"  -> Columns Verified: {len(raw_report['columns_present'])}")
        print(f"  -> Missing Customer IDs (Guest Orders): {raw_report['null_percentages']['CustomerID']}%")

        # Stage 3: PySpark Distributed Transformations & Parquet Output
        if not skip_spark:
            print("\n[STAGE 3/5] Transforming Data with PySpark & Partitioning Snappy Parquet...")
            transformer = HistoricalSalesTransformer()
            spark_metrics = transformer.run()
            print(f"  -> Initial Raw Records: {spark_metrics['initial_raw_records']:,}")
            print(f"  -> Duplicates Removed: {spark_metrics['duplicates_removed']:,}")
            print(f"  -> Final Curated Records: {spark_metrics['final_curated_records']:,}")
            print(f"  -> Unique Invoices: {spark_metrics['unique_invoices']:,}")
            print(f"  -> Unique Products: {spark_metrics['unique_products']:,}")
            print(f"  -> Registered Customers: {spark_metrics['unique_customers']:,}")
            print(f"  -> Guest Order Records: {spark_metrics['guest_order_records']:,}")
            print(f"  -> Cancellations Flagged: {spark_metrics['cancellation_records']:,}")
            print(f"  -> Date Range: {spark_metrics['min_invoice_date']} to {spark_metrics['max_invoice_date']}")
            print(f"  -> Total Gross Revenue: £{spark_metrics['total_gross_amount']:,.2f}")
            print(f"  -> Parquet Partitions: {spark_metrics['parquet_validation']['year_partitions']}")
        else:
            print("\n[STAGE 3/5] Skipping PySpark transformation (--skip-spark specified)")

        # Stage 4: MySQL 8.0 Warehouse Loading
        if not skip_load:
            print("\n[STAGE 4/5] Loading Historical Data into MySQL 8.0 Data Warehouse...")
            load_results = loader.run(max_fact_records=max_fact_records)
            print(f"  -> dim_products Rows: {load_results['dim_products_count']:,}")
            print(f"  -> dim_customers Rows: {load_results['dim_customers_count']:,}")
            print(f"  -> fact_sales Rows: {load_results['fact_sales_count']:,}")
        else:
            print("\n[STAGE 4/5] Skipping MySQL warehouse load (--skip-load specified)")

        # Stage 5: Logging Pipeline Status & Telemetry
        print("\n[STAGE 5/5] Logging Telemetry to etl_pipeline_logs...")
        end_time = datetime.now(timezone.utc)
        elapsed = round(time.time() - t0, 2)
        counts = loader.validate_warehouse_counts()

        loader.log_pipeline_run(
            run_id=run_id,
            pipeline_name="historical_ecommerce_pipeline",
            start_time=start_time,
            end_time=end_time,
            status="COMPLETED",
            records_processed=counts["fact_sales_count"],
            records_rejected=0,
            error_message=None,
            metrics_json={
                "elapsed_seconds": elapsed,
                "warehouse_counts": counts,
            },
        )

        print("\n" + "=" * 80)
        print("PIPELINE EXECUTION SUMMARY")
        print("=" * 80)
        print(f"Total Execution Time: {elapsed:.2f} seconds")
        print(f"fact_sales loaded:   {counts['fact_sales_count']:,} records")
        print(f"dim_products loaded: {counts['dim_products_count']:,} products")
        print(f"dim_customers loaded: {counts['dim_customers_count']:,} registered customers")
        print(f"Status: COMPLETED (Run ID: {run_id})")
        print("=" * 80)

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        end_time = datetime.now(timezone.utc)
        loader.log_pipeline_run(
            run_id=run_id,
            pipeline_name="historical_ecommerce_pipeline",
            start_time=start_time,
            end_time=end_time,
            status="FAILED",
            records_processed=0,
            records_rejected=0,
            error_message=str(e),
        )
        print(f"\n[ERROR] Pipeline failed: {e}")
        sys.exit(1)
    finally:
        loader.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Historical E-Commerce Data Engineering Pipeline")
    parser.add_argument("--force-extract", action="store_true", help="Force re-download and re-extraction of raw data")
    parser.add_argument("--skip-spark", action="store_true", help="Skip PySpark transformation and Parquet writing")
    parser.add_argument("--skip-load", action="store_true", help="Skip MySQL warehouse loading")
    parser.add_argument("--max-fact-records", type=int, default=None, help="Limit maximum fact_sales records loaded into MySQL")
    args = parser.parse_args()

    run_pipeline(
        force_extract=args.force_extract,
        skip_spark=args.skip_spark,
        skip_load=args.skip_load,
        max_fact_records=args.max_fact_records,
    )
