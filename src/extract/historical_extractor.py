"""
Historical Data Extraction Module
Downloads and preserves the verified UCI Machine Learning Repository Online Retail Dataset.
Records source URL, download timestamp, cryptographic hash, and schema metadata.
"""

from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional
import zipfile

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

import sys

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger_config import get_logger
from src.utils.config import cfg

logger = get_logger("extract.historical_extractor")


class ExtractionError(Exception):
    """Raised when data extraction or validation fails."""
    pass


class HistoricalDataExtractor:
    """
    Extracts real historical sales data from the UCI Machine Learning Repository.
    Preserves raw files without alterations and logs audit metadata.
    """

    def __init__(
        self,
        source_url: Optional[str] = None,
        output_dir: Optional[Path] = None,
        timeout: Optional[int] = None,
    ):
        self.source_url = source_url or cfg.source.source_url
        self.output_dir = output_dir or cfg.paths.raw_data_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.timeout = timeout or cfg.source.timeout_seconds
        self.session = self._create_resilient_session()

        self.zip_path = self.output_dir / "online_retail_raw.zip"
        self.xlsx_path = self.output_dir / "online_retail_raw.xlsx"
        self.csv_path = self.output_dir / "online_retail_raw.csv"
        self.metadata_path = self.output_dir / "source_metadata.json"

    def _create_resilient_session(self) -> requests.Session:
        """Configures requests Session with exponential backoff retries."""
        session = requests.Session()
        retries = Retry(
            total=cfg.source.max_retries,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (DataEngineeringPipeline/1.0; HistoricalAnalysis)"
        })
        return session

    @staticmethod
    def _compute_sha256(file_path: Path) -> str:
        """Calculates SHA-256 cryptographic hash of a file."""
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

    def download_raw_archive(self, force: bool = False) -> Path:
        """
        Downloads the raw ZIP archive from the verified source URL.
        """
        if self.zip_path.exists() and not force:
            logger.info(f"Raw archive already present at: {self.zip_path} (skipping download)")
            return self.zip_path

        logger.info(f"Downloading historical dataset archive from: {self.source_url}")
        try:
            with self.session.get(self.source_url, stream=True, timeout=self.timeout) as resp:
                resp.raise_for_status()
                total_bytes = 0
                with open(self.zip_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1048576):  # 1MB chunks
                        if chunk:
                            f.write(chunk)
                            total_bytes += len(chunk)
            
            logger.info(f"Successfully downloaded {total_bytes:,} bytes to {self.zip_path}")
            return self.zip_path

        except requests.exceptions.RequestException as e:
            error_msg = f"Failed to download archive from {self.source_url}: {e}"
            logger.error(error_msg)
            raise ExtractionError(error_msg) from e

    def extract_and_convert_to_csv(self, force: bool = False) -> Path:
        """
        Extracts the Excel file from the ZIP archive and converts it to pure CSV
        to allow fast distributed reading by PySpark.
        """
        if self.csv_path.exists() and not force:
            logger.info(f"Raw CSV already exists at: {self.csv_path} (skipping extraction)")
            return self.csv_path

        archive_path = self.download_raw_archive(force=force)

        if not zipfile.is_zipfile(archive_path):
            raise ExtractionError(f"Downloaded file {archive_path} is not a valid zip archive.")

        logger.info("Unpacking Excel workbook from ZIP archive...")
        with zipfile.ZipFile(archive_path, "r") as zf:
            namelist = zf.namelist()
            xlsx_files = [f for f in namelist if f.endswith(".xlsx")]
            if not xlsx_files:
                raise ExtractionError(f"No .xlsx file found in zip archive: {namelist}")
            
            target_file = xlsx_files[0]
            with zf.open(target_file) as src_file, open(self.xlsx_path, "wb") as dst_file:
                dst_file.write(src_file.read())

        logger.info(f"Preserved raw Excel file at: {self.xlsx_path}")
        logger.info("Reading Excel sheet and writing pristine raw CSV...")
        df = pd.read_excel(self.xlsx_path, engine="openpyxl")
        
        # Save raw CSV
        df.to_csv(self.csv_path, index=False, encoding="utf-8")
        logger.info(f"Successfully created raw CSV with {len(df):,} records at {self.csv_path}")

        # Record provenance metadata
        self._record_metadata(df)
        return self.csv_path

    def _record_metadata(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Saves lineage and verification metadata."""
        metadata = {
            "source_name": cfg.source.source_name,
            "source_url": self.source_url,
            "download_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "historical_period": "2010-12-01 to 2011-12-09",
            "license": "Creative Commons Attribution 4.0 International (CC BY 4.0)",
            "record_count": int(len(df)),
            "column_count": int(len(df.columns)),
            "columns": list(df.columns),
            "zip_file_bytes": self.zip_path.stat().st_size if self.zip_path.exists() else 0,
            "xlsx_file_bytes": self.xlsx_path.stat().st_size if self.xlsx_path.exists() else 0,
            "csv_file_bytes": self.csv_path.stat().st_size if self.csv_path.exists() else 0,
            "sha256_csv": self._compute_sha256(self.csv_path),
        }

        with open(self.metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

        logger.info(f"Saved source metadata to {self.metadata_path}")
        return metadata

    def extract(self, force: bool = False) -> Dict[str, Any]:
        """Runs the extraction pipeline and returns metadata summary."""
        csv_file = self.extract_and_convert_to_csv(force=force)
        with open(self.metadata_path, "r", encoding="utf-8") as f:
            return json.load(f)


if __name__ == "__main__":
    extractor = HistoricalDataExtractor()
    meta = extractor.extract(force=False)
    print(f"Extraction complete: {meta['record_count']} records, CSV size: {meta['csv_file_bytes']} bytes")
