"""
Parquet Curated Storage Engine
Writes curated sales data partitioned by order_date (e.g. order_date=YYYY-MM-DD)
using Snappy compression, with post-write read-back verification.
"""

from pathlib import Path
from typing import Dict, Optional, Union
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger_config import get_logger
from src.utils.config import cfg

logger = get_logger("storage.parquet_writer")


class ParquetStorageError(Exception):
    """Raised when Parquet write or verification fails."""
    pass


class ParquetWriter:
    """Manages writing and verifying partitioned Parquet datasets."""

    def __init__(self, output_dir: Optional[Union[str, Path]] = None):
        self.output_dir = Path(output_dir or cfg.paths.processed_data_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write_spark_dataframe(
        self,
        df: DataFrame,
        mode: str = "overwrite",
        partition_by: str = "order_date"
    ) -> Dict[str, Union[int, str]]:
        """
        Writes a PySpark DataFrame to partitioned Parquet files and verifies data integrity.
        
        Args:
            df: PySpark DataFrame to write.
            mode: "overwrite" or "append".
            partition_by: Column to partition by (default: 'order_date').
            
        Returns:
            Dict containing write summary and record count.
        """
        target_path = str(self.output_dir)
        expected_count = df.count()
        logger.info(
            f"Writing {expected_count} records to Parquet at '{target_path}', "
            f"partitioned by '{partition_by}', mode='{mode}'"
        )

        try:
            # Enable dynamic partition overwrite so writing one date partition does not wipe other dates
            df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            (
                df.write
                .mode(mode)
                .partitionBy(partition_by)
                .option("compression", "snappy")
                .parquet(target_path)
            )
            logger.info("Parquet write completed successfully. Commencing read-back verification.")

            # Verification: Read back using Spark
            spark = df.sparkSession
            verified_df = spark.read.parquet(target_path)
            actual_count = verified_df.count()

            logger.info(f"Verification successful: Read back {actual_count} records from '{target_path}'")
            return {
                "target_path": target_path,
                "expected_records": expected_count,
                "verified_records": actual_count,
                "status": "SUCCESS"
            }

        except Exception as e:
            err = f"Failed writing or verifying Parquet dataset at '{target_path}': {str(e)}"
            logger.error(err)
            raise ParquetStorageError(err) from e

    def read_parquet(self, partition_value: Optional[str] = None) -> pd.DataFrame:
        """
        Reads partitioned Parquet into a Pandas DataFrame using PyArrow.
        
        Args:
            partition_value: Optional date string 'YYYY-MM-DD' to read specific partition.
        """
        path = self.output_dir
        if partition_value:
            path = self.output_dir / f"order_date={partition_value}"

        if not path.exists():
            logger.warning(f"Requested Parquet path does not exist: {path}")
            return pd.DataFrame()

        df = pd.read_parquet(path)
        logger.info(f"Loaded {len(df)} records from Parquet dataset '{path}' into Pandas")
        return df
