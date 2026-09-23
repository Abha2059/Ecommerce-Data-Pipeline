"""
Raw Data Validation Module
Inspects raw historical CSV for expected schema, size, record counts, and null distributions.
"""

from datetime import datetime, timezone
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd

from src.utils.logger_config import get_logger
from src.utils.config import cfg

logger = get_logger("validation.raw_validator")


class RawValidationError(Exception):
    """Raised when raw data fails integrity validation."""
    pass


class RawDataValidator:
    """Validates raw CSV downloaded from verified historical source."""

    EXPECTED_COLUMNS = [
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "UnitPrice",
        "CustomerID",
        "Country",
    ]

    def __init__(
        self,
        raw_csv_path: Optional[Path] = None,
        report_output_path: Optional[Path] = None,
    ):
        self.raw_csv_path = raw_csv_path or cfg.paths.raw_csv_path
        self.report_output_path = (
            report_output_path
            or cfg.paths.validated_data_dir / "raw_validation_report.json"
        )
        self.report_output_path.parent.mkdir(parents=True, exist_ok=True)

    def validate(self) -> Dict[str, Any]:
        """
        Executes schema and integrity validation on the raw file.
        """
        logger.info(f"Validating raw historical file: {self.raw_csv_path}")

        if not self.raw_csv_path.exists():
            error_msg = f"Raw CSV file does not exist at {self.raw_csv_path}"
            logger.error(error_msg)
            raise RawValidationError(error_msg)

        file_size_bytes = self.raw_csv_path.stat().st_size
        file_size_mb = round(file_size_bytes / (1024 * 1024), 2)
        logger.info(f"Raw CSV file size: {file_size_mb} MB ({file_size_bytes:,} bytes)")

        if file_size_bytes < 10 * 1024 * 1024:  # Must be at least 10MB
            error_msg = f"Raw CSV file is unexpectedly small ({file_size_mb} MB)"
            logger.error(error_msg)
            raise RawValidationError(error_msg)

        # Inspect headers and first rows
        sample_df = pd.read_csv(self.raw_csv_path, nrows=5)
        actual_columns = list(sample_df.columns)

        missing_cols = [c for c in self.EXPECTED_COLUMNS if c not in actual_columns]
        if missing_cols:
            error_msg = f"Raw CSV missing required columns: {missing_cols}. Found: {actual_columns}"
            logger.error(error_msg)
            raise RawValidationError(error_msg)

        logger.info(f"Verified all 8 required columns: {self.EXPECTED_COLUMNS}")

        # Scan full counts and null distributions efficiently
        total_rows = 0
        null_counts: Dict[str, int] = {col: 0 for col in self.EXPECTED_COLUMNS}
        
        # Stream in 100,000-row chunks to prevent memory spikes
        for chunk in pd.read_csv(self.raw_csv_path, chunksize=100000, dtype=str):
            total_rows += len(chunk)
            for col in self.EXPECTED_COLUMNS:
                null_counts[col] += int(chunk[col].isna().sum())

        logger.info(f"Total raw records counted: {total_rows:,}")
        if total_rows < 500000:
            error_msg = f"Unexpectedly low record count in historical source: {total_rows:,}"
            logger.error(error_msg)
            raise RawValidationError(error_msg)

        null_percentages = {
            col: round((null_counts[col] / total_rows) * 100, 2)
            for col in self.EXPECTED_COLUMNS
        }

        report = {
            "validation_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "file_path": str(self.raw_csv_path),
            "file_size_bytes": file_size_bytes,
            "file_size_mb": file_size_mb,
            "total_records": total_rows,
            "columns_present": actual_columns,
            "null_counts": null_counts,
            "null_percentages": null_percentages,
            "status": "PASSED",
        }

        with open(self.report_output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        logger.info(f"Raw validation passed. Report written to {self.report_output_path}")
        return report


if __name__ == "__main__":
    validator = RawDataValidator()
    res = validator.validate()
    print("Validation passed:", res["status"], f"({res['total_records']} rows)")
