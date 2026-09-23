"""
Historical E-Commerce Data Engineering Pipeline DAG
Orchestrates end-to-end ingestion, validation, PySpark transformation,
Snappy Parquet partitioning, MySQL warehouse loading, and data quality checks.
"""

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ECOMMERCE_ROOT = Path("/Users/abhaykumar/Documents/Ecommerce-Data-Pipeline").resolve()
for p in [str(PROJECT_ROOT), str(ECOMMERCE_ROOT)]:
    if p not in sys.path:
        sys.path.insert(0, p)

# Safe import for Airflow environments
try:
    from airflow import DAG  # type: ignore
    try:
        from airflow.providers.standard.operators.python import PythonOperator  # type: ignore
    except ImportError:
        from airflow.operators.python import PythonOperator  # type: ignore
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    # Lightweight stub for local syntax validation outside Airflow
    class DAG:
        def __init__(self, *args, **kwargs):
            self.tasks = []
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    class PythonOperator:
        def __init__(self, task_id, python_callable, op_kwargs=None, **kwargs):
            self.task_id = task_id
            self.python_callable = python_callable
            self.op_kwargs = op_kwargs or {}
        def __rshift__(self, other):
            return other

from src.extract.historical_extractor import HistoricalDataExtractor
from src.load.historical_mysql_loader import HistoricalWarehouseLoader
from src.spark.historical_sales_transformations import HistoricalSalesTransformer
from src.utils.config import cfg
from src.utils.logger_config import get_logger
from src.validation.raw_validator import RawDataValidator

logger = get_logger("dags.historical_ecommerce_pipeline")

# =====================================================================
# DAG Task Callables
# =====================================================================

def task_validate_configuration(**context):
    """Task 1: Validates database connectivity, required paths, and environment settings."""
    logger.info("Validating pipeline configuration and environment...")
    assert cfg.paths.raw_data_dir.exists(), "Raw data directory missing"
    assert cfg.paths.processed_data_dir.parent.exists(), "Processed data root missing"
    assert cfg.db.host, "MySQL host not configured"
    
    # Test DB engine connectivity
    loader = HistoricalWarehouseLoader()
    with loader.engine.connect() as conn:
        logger.info("MySQL connection established successfully.")
    loader.close()
    return {"status": "SUCCESS", "db_host": cfg.db.host, "db_name": cfg.db.database}


def task_extract_historical_data(**context):
    """Task 2: Downloads verified UCI Online Retail archive and preserves raw files."""
    logger.info("Starting historical data extraction...")
    extractor = HistoricalDataExtractor()
    meta = extractor.extract(force=False)
    logger.info(f"Extraction completed: {meta['record_count']:,} records preserved at {meta['csv_file_bytes']:,} bytes")
    return meta


def task_validate_raw_data(**context):
    """Task 3: Validates raw CSV size, header schema, record counts, and null distributions."""
    logger.info("Validating raw data integrity...")
    validator = RawDataValidator()
    report = validator.validate()
    logger.info(f"Raw data validation result: {report['status']} ({report['total_records']:,} rows)")
    return report


def task_inspect_source_schema(**context):
    """Task 4: Inspects and logs source schema characteristics and column datatypes."""
    logger.info("Inspecting source schema and recording data dictionary...")
    with open(cfg.paths.metadata_path, "r", encoding="utf-8") as f:
        meta = json.load(f)
    logger.info(f"Source: {meta['source_name']}, Period: {meta['historical_period']}, Columns: {meta['columns']}")
    return meta["columns"]


def task_process_data_with_pyspark(**context):
    """Task 5: Executes PySpark cleaning, deduplication, timestamp parsing, and derivations."""
    logger.info("Processing historical dataset with PySpark...")
    transformer = HistoricalSalesTransformer()
    raw_df = transformer.load_raw_data()
    deduped_df, metrics = transformer.transform(raw_df)
    logger.info(f"PySpark transformation finished: {metrics['final_curated_records']:,} curated records")
    return metrics


def task_write_parquet_output(**context):
    """Task 6: Writes transformed data into Snappy-compressed Parquet partitioned by year and month."""
    logger.info("Writing Snappy Parquet partitioned dataset...")
    transformer = HistoricalSalesTransformer()
    raw_df = transformer.load_raw_data()
    deduped_df, _ = transformer.transform(raw_df)
    out_dir = transformer.write_parquet(deduped_df)
    logger.info(f"Partitioned Parquet written to: {out_dir}")
    return str(out_dir)


def task_validate_parquet_output(**context):
    """Task 7: Validates output Parquet schema, partition directories, and record counts."""
    logger.info("Validating Parquet data lake integrity...")
    transformer = HistoricalSalesTransformer()
    # Read metrics to get expected count
    metrics_path = cfg.paths.validated_data_dir / "transformation_metrics.json"
    with open(metrics_path, "r", encoding="utf-8") as f:
        metrics = json.load(f)
    result = transformer.validate_parquet_output(metrics["final_curated_records"])
    logger.info(f"Parquet validation passed: {result['readback_record_count']:,} records")
    return result


def task_load_mysql_tables(**context):
    """Task 8: Loads dim_products, dim_customers, and fact_sales into MySQL 8.0."""
    logger.info("Loading processed historical data into MySQL 8.0 Data Warehouse...")
    loader = HistoricalWarehouseLoader()
    result = loader.run()
    loader.close()
    logger.info(f"Warehouse load complete: {result}")
    return result


def task_run_data_quality_checks(**context):
    """Task 9: Runs comprehensive post-load data quality assertions."""
    logger.info("Executing warehouse data quality checks...")
    loader = HistoricalWarehouseLoader()
    counts = loader.validate_warehouse_counts()
    loader.close()
    assert counts["fact_sales_count"] > 0, "fact_sales is empty"
    assert counts["dim_products_count"] > 0, "dim_products is empty"
    assert counts["dim_customers_count"] > 0, "dim_customers is empty"
    logger.info(f"Data quality checks passed: fact_sales={counts['fact_sales_count']:,}")
    return {"status": "PASSED", "counts": counts}


def task_log_pipeline_status(**context):
    """Task 10: Records complete pipeline telemetry into etl_pipeline_logs."""
    logger.info("Logging final pipeline run status...")
    run_id = f"RUN_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    loader = HistoricalWarehouseLoader()
    counts = loader.validate_warehouse_counts()
    loader.log_pipeline_run(
        run_id=run_id,
        pipeline_name="historical_ecommerce_pipeline",
        start_time=datetime.now(timezone.utc),
        end_time=datetime.now(timezone.utc),
        status="COMPLETED",
        records_processed=counts["fact_sales_count"],
        records_rejected=0,
        error_message=None,
        metrics_json={"warehouse_counts": counts},
    )
    loader.close()
    logger.info(f"Pipeline run {run_id} successfully logged.")
    return {"run_id": run_id, "status": "COMPLETED"}


# =====================================================================
# DAG Definition
# =====================================================================

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="historical_ecommerce_pipeline",
    default_args=default_args,
    description="End-to-end historical e-commerce data engineering pipeline (UCI Online Retail)",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "historical", "pyspark", "parquet", "mysql"],
) as dag:

    t1_validate_config = PythonOperator(
        task_id="validate_configuration",
        python_callable=task_validate_configuration,
    )

    t2_extract_data = PythonOperator(
        task_id="extract_historical_data",
        python_callable=task_extract_historical_data,
    )

    t3_validate_raw = PythonOperator(
        task_id="validate_raw_data",
        python_callable=task_validate_raw_data,
    )

    t4_inspect_schema = PythonOperator(
        task_id="inspect_source_schema",
        python_callable=task_inspect_source_schema,
    )

    t5_spark_transform = PythonOperator(
        task_id="process_data_with_pyspark",
        python_callable=task_process_data_with_pyspark,
    )

    t6_write_parquet = PythonOperator(
        task_id="write_parquet_output",
        python_callable=task_write_parquet_output,
    )

    t7_validate_parquet = PythonOperator(
        task_id="validate_parquet_output",
        python_callable=task_validate_parquet_output,
    )

    t8_load_mysql = PythonOperator(
        task_id="load_mysql_tables",
        python_callable=task_load_mysql_tables,
    )

    t9_quality_checks = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=task_run_data_quality_checks,
    )

    t10_log_status = PythonOperator(
        task_id="log_pipeline_status",
        python_callable=task_log_pipeline_status,
    )

    # Linear and robust dependency graph
    (
        t1_validate_config
        >> t2_extract_data
        >> t3_validate_raw
        >> t4_inspect_schema
        >> t5_spark_transform
        >> t6_write_parquet
        >> t7_validate_parquet
        >> t8_load_mysql
        >> t9_quality_checks
        >> t10_log_status
    )
