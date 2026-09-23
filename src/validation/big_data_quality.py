"""
Big Data Quality Audit Engine
Executes distributed validation rules over PySpark DataFrames without converting to Pandas,
producing an auditable data quality report.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any, Dict, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger_config import get_logger
from src.utils.config import cfg

logger = get_logger("validation.big_data_quality")


@dataclass
class QualityMetric:
    check_name: str
    status: str  # "PASSED" or "FAILED"
    failed_records: int
    threshold: int = 0
    details: str = ""


@dataclass
class BigDataQualityReport:
    input_record_count: int
    output_record_count: int
    duplicate_record_count: int
    invalid_record_count: int
    null_value_count: int
    processing_duration_seconds: float
    output_parquet_location: str
    execution_timestamp: str
    status: str  # "PASSED", "WARNING", "FAILED"
    checks: List[QualityMetric] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class BigDataQualityAuditor:
    """Performs distributed data quality audits on large sales datasets."""

    def __init__(self, reports_dir: Optional[Path] = None):
        self.reports_dir = Path(reports_dir or cfg.paths.quarantine_data_dir)
        self.reports_dir.mkdir(parents=True, exist_ok=True)

    def audit_transformed_sales(
        self,
        raw_df: DataFrame,
        curated_df: DataFrame,
        parquet_location: str,
        start_time: float
    ) -> BigDataQualityReport:
        """
        Runs comprehensive data quality checks on the curated PySpark DataFrame.
        """
        logger.info("Executing comprehensive Big Data Quality audit")
        checks: List[QualityMetric] = []

        raw_count = raw_df.count()
        curated_count = curated_df.count()

        # 1. Duplicate transaction ID check
        dup_count = curated_df.groupBy("transaction_id").count().filter(F.col("count") > 1).count()
        checks.append(QualityMetric(
            check_name="duplicate_transaction_ids",
            status="PASSED" if dup_count == 0 else "FAILED",
            failed_records=dup_count,
            details=f"Found {dup_count} duplicate transaction_ids"
        ))

        # 2. Null customer IDs
        null_cust = curated_df.filter(F.col("customer_id").isNull() | (F.col("customer_id") == "")).count()
        checks.append(QualityMetric(
            check_name="null_customer_ids",
            status="PASSED" if null_cust == 0 else "FAILED",
            failed_records=null_cust,
            details=f"Found {null_cust} null customer_ids"
        ))

        # 3. Null product IDs
        null_prod = curated_df.filter(F.col("product_id").isNull() | (F.col("product_id") == "")).count()
        checks.append(QualityMetric(
            check_name="null_product_ids",
            status="PASSED" if null_prod == 0 else "FAILED",
            failed_records=null_prod,
            details=f"Found {null_prod} null product_ids"
        ))

        # 4. Null store IDs
        null_store = curated_df.filter(F.col("store_id").isNull() | (F.col("store_id") == "")).count()
        checks.append(QualityMetric(
            check_name="null_store_ids",
            status="PASSED" if null_store == 0 else "FAILED",
            failed_records=null_store,
            details=f"Found {null_store} null store_ids"
        ))

        # 5. Non-positive quantities
        bad_qty = curated_df.filter(F.col("quantity") <= 0).count()
        checks.append(QualityMetric(
            check_name="negative_or_zero_quantities",
            status="PASSED" if bad_qty == 0 else "FAILED",
            failed_records=bad_qty,
            details=f"Found {bad_qty} non-positive quantities"
        ))

        # 6. Negative prices
        bad_price = curated_df.filter(F.col("unit_price") < 0).count()
        checks.append(QualityMetric(
            check_name="negative_unit_prices",
            status="PASSED" if bad_price == 0 else "FAILED",
            failed_records=bad_price,
            details=f"Found {bad_price} negative unit prices"
        ))

        # 7. Invalid dates
        bad_dates = curated_df.filter(F.col("transaction_date").isNull()).count()
        checks.append(QualityMetric(
            check_name="invalid_transaction_dates",
            status="PASSED" if bad_dates == 0 else "FAILED",
            failed_records=bad_dates,
            details=f"Found {bad_dates} invalid transaction dates"
        ))

        # 8. Negative net amounts
        bad_net = curated_df.filter(F.col("net_amount") < 0).count()
        checks.append(QualityMetric(
            check_name="negative_net_amounts",
            status="PASSED" if bad_net == 0 else "FAILED",
            failed_records=bad_net,
            details=f"Found {bad_net} negative net revenue amounts"
        ))

        # 9. Referential Integrity (Customer and Product name attached via join)
        unmatched_cust = curated_df.filter(F.col("customer_name").isNull()).count()
        checks.append(QualityMetric(
            check_name="referential_integrity_customers",
            status="PASSED" if unmatched_cust == 0 else "WARNING",
            failed_records=unmatched_cust,
            details=f"Found {unmatched_cust} orphan customer references"
        ))

        total_nulls = null_cust + null_prod + null_store + bad_dates
        total_invalids = bad_qty + bad_price + bad_net
        total_failed_checks = sum(1 for c in checks if c.status == "FAILED")

        overall_status = "PASSED"
        if total_failed_checks > 0:
            overall_status = "FAILED"
        elif any(c.status == "WARNING" for c in checks):
            overall_status = "WARNING"

        duration = round(time.time() - start_time, 2)
        report = BigDataQualityReport(
            input_record_count=raw_count,
            output_record_count=curated_count,
            duplicate_record_count=dup_count,
            invalid_record_count=total_invalids,
            null_value_count=total_nulls,
            processing_duration_seconds=duration,
            output_parquet_location=parquet_location,
            execution_timestamp=datetime.now(timezone.utc).isoformat(),
            status=overall_status,
            checks=checks
        )

        # Save report to JSON
        timestamp_slug = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        report_path = self.reports_dir / f"quality_report_{timestamp_slug}.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report.to_dict(), f, indent=2)

        logger.info(
            f"Quality Audit Summary: Status={overall_status} | Inputs={raw_count:,} | Outputs={curated_count:,} | "
            f"Duplicates={dup_count} | Nulls={total_nulls} | Invalids={total_invalids} | Report={report_path.name}"
        )
        return report
