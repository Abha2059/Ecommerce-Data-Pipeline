"""
CSV Sales Data Ingestion Module (Source A)
Ingests raw CSV batches, validates basic file readability, and attaches audit metadata.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union
import pandas as pd

from src.utils.logger_config import get_logger
from src.utils.config import cfg

logger = get_logger("ingestion.csv_reader")


class CSVIngestionError(Exception):
    """Raised when CSV reading or file validation fails."""
    pass


class CSVSalesReader:
    """Ingests raw sales CSV files and injects lineage metadata."""

    def __init__(self, raw_data_dir: Optional[Union[str, Path]] = None):
        self.raw_data_dir = Path(raw_data_dir) if raw_data_dir else cfg.paths.raw_data_dir

    def read_csv(self, file_path: Union[str, Path]) -> pd.DataFrame:
        """
        Reads a specific CSV file and enriches each record with ingestion metadata.
        
        Args:
            file_path: Absolute or relative path to CSV file.
            
        Returns:
            pd.DataFrame containing raw rows + lineage metadata columns.
        """
        path = Path(file_path)
        if not path.is_absolute():
            path = self.raw_data_dir / path

        logger.info(f"Initiating extraction from CSV: {path}")

        if not path.exists():
            error_msg = f"Target CSV file does not exist: {path}"
            logger.error(error_msg)
            raise CSVIngestionError(error_msg)

        if path.stat().st_size == 0:
            error_msg = f"Target CSV file is empty (0 bytes): {path}"
            logger.error(error_msg)
            raise CSVIngestionError(error_msg)

        try:
            # Read CSV with string types initially to avoid premature corrupt conversions
            df = pd.read_csv(path, dtype=str, keep_default_na=False)
            initial_count = len(df)
            logger.info(f"Successfully read {initial_count} raw records from {path.name}")

            # Inject audit and lineage metadata
            ingestion_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            df["source_system"] = "CSV_SALES"
            df["source_file"] = path.name
            df["ingestion_timestamp"] = ingestion_time

            return df

        except Exception as e:
            error_msg = f"Failed to parse CSV file {path}: {str(e)}"
            logger.error(error_msg)
            raise CSVIngestionError(error_msg) from e

    def find_latest_batch(self, pattern: str = "*.csv") -> Optional[Path]:
        """Finds the most recently modified CSV in the raw directory."""
        csv_files = list(self.raw_data_dir.glob(pattern))
        if not csv_files:
            logger.warning(f"No CSV files found matching pattern '{pattern}' in {self.raw_data_dir}")
            return None
        latest = max(csv_files, key=lambda p: p.stat().st_mtime)
        logger.info(f"Identified latest CSV batch: {latest.name}")
        return latest
