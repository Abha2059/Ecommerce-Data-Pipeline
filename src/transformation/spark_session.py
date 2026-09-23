"""
PySpark Session Factory
Creates an optimized local SparkSession configured for pipeline workloads.
"""

import sys
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger_config import get_logger
from src.utils.config import cfg

logger = get_logger("transformation.spark_session")


def get_spark_session(app_name: Optional[str] = None, master: Optional[str] = None) -> SparkSession:
    """
    Initializes and returns a configured SparkSession singleton.
    Configured for local execution on macOS / Docker containers.
    """
    name = app_name or cfg.spark.app_name
    spark_master = master or cfg.spark.master

    logger.info(f"Initializing PySpark session '{name}' with master '{spark_master}'")

    spark = (
        SparkSession.builder
        .appName(name)
        .master(spark_master)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(cfg.spark.log_level)
    logger.info(f"PySpark session active: version {spark.version}")
    return spark
