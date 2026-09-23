"""
Centralized Configuration Loader
Loads YAML / environment settings for the Historical E-Commerce Pipeline.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from dotenv import load_dotenv

# Ensure .env is loaded
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent.parent


@dataclass
class DatabaseConfig:
    host: str = "localhost"
    port: int = 3306
    database: str = "ecommerce_dw"
    user: str = "root"
    password: str = ""
    pool_size: int = 5

    @property
    def connection_url(self) -> str:
        """Returns SQLAlchemy-compatible connection URL."""
        if self.password:
            return f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        return f"mysql+mysqlconnector://{self.user}@{self.host}:{self.port}/{self.database}"


@dataclass
class HistoricalSourceConfig:
    source_name: str = "UCI Machine Learning Repository - Online Retail"
    source_url: str = (
        "https://archive.ics.uci.edu/static/public/352/online+retail.zip"
    )
    timeout_seconds: int = 60
    max_retries: int = 3
    expected_row_count_approx: int = 541909
    expected_columns: List[str] = field(
        default_factory=lambda: [
            "InvoiceNo",
            "StockCode",
            "Description",
            "Quantity",
            "InvoiceDate",
            "UnitPrice",
            "CustomerID",
            "Country",
        ]
    )


@dataclass
class PathsConfig:
    raw_data_dir: Path = BASE_DIR / "data" / "raw"
    validated_data_dir: Path = BASE_DIR / "data" / "validated"
    processed_data_dir: Path = BASE_DIR / "data" / "processed" / "historical_sales"
    quarantine_data_dir: Path = BASE_DIR / "data" / "quarantine"

    @property
    def raw_csv_path(self) -> Path:
        return self.raw_data_dir / "online_retail_raw.csv"

    @property
    def raw_zip_path(self) -> Path:
        return self.raw_data_dir / "online_retail_raw.zip"

    @property
    def metadata_path(self) -> Path:
        return self.raw_data_dir / "source_metadata.json"


@dataclass
class SparkConfig:
    app_name: str = "HistoricalEcommercePipeline"
    master: str = "local[*]"
    log_level: str = "WARN"


class PipelineConfig:
    """Master configuration holder for the pipeline."""

    def __init__(self, config_path: Optional[str] = None):
        self.base_dir = BASE_DIR
        self.config_path = (
            Path(config_path) if config_path else BASE_DIR / "config" / "config.yaml"
        )
        self._raw_yaml: Dict[str, Any] = self._load_yaml()

        self.db = self._init_database()
        self.source = self._init_source()
        self.paths = self._init_paths()
        self.spark = self._init_spark()

    def _load_yaml(self) -> Dict[str, Any]:
        if self.config_path.exists():
            with open(self.config_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        return {}

    def _init_database(self) -> DatabaseConfig:
        yaml_db = self._raw_yaml.get("database", {})
        return DatabaseConfig(
            host=os.getenv("MYSQL_HOST", yaml_db.get("host", "localhost")),
            port=int(os.getenv("MYSQL_PORT", yaml_db.get("port", 3306))),
            database=os.getenv("MYSQL_DATABASE", yaml_db.get("database", "ecommerce_dw")),
            user=os.getenv("MYSQL_USER", yaml_db.get("user", "root")),
            password=os.getenv("MYSQL_PASSWORD", yaml_db.get("password", "")),
            pool_size=int(yaml_db.get("pool_size", 5)),
        )

    def _init_source(self) -> HistoricalSourceConfig:
        yaml_source = self._raw_yaml.get("source", {})
        return HistoricalSourceConfig(
            source_url=os.getenv(
                "HISTORICAL_SOURCE_URL",
                yaml_source.get(
                    "source_url",
                    "https://archive.ics.uci.edu/static/public/352/online+retail.zip",
                ),
            ),
            timeout_seconds=int(yaml_source.get("timeout_seconds", 60)),
            max_retries=int(yaml_source.get("max_retries", 3)),
        )

    def _init_paths(self) -> PathsConfig:
        yaml_paths = self._raw_yaml.get("paths", {})
        raw = BASE_DIR / yaml_paths.get("raw_data_dir", "data/raw")
        val = BASE_DIR / yaml_paths.get("validated_data_dir", "data/validated")
        proc = (
            BASE_DIR
            / yaml_paths.get(
                "processed_data_dir", "data/processed/historical_sales"
            )
        )
        quar = BASE_DIR / yaml_paths.get("quarantine_data_dir", "data/quarantine")

        raw.mkdir(parents=True, exist_ok=True)
        val.mkdir(parents=True, exist_ok=True)
        proc.mkdir(parents=True, exist_ok=True)
        quar.mkdir(parents=True, exist_ok=True)

        return PathsConfig(
            raw_data_dir=raw,
            validated_data_dir=val,
            processed_data_dir=proc,
            quarantine_data_dir=quar,
        )

    def _init_spark(self) -> SparkConfig:
        yaml_spark = self._raw_yaml.get("spark", {})
        return SparkConfig(
            app_name=yaml_spark.get("app_name", "HistoricalEcommercePipeline"),
            master=os.getenv("SPARK_MASTER", yaml_spark.get("master", "local[*]")),
            log_level=yaml_spark.get("log_level", "WARN"),
        )


# Global singleton instance
cfg = PipelineConfig()
