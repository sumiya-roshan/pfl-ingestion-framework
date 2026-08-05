"""
Writes the extracted DataFrame to a raw landing path (S3 / Databricks Volume).

The landing path is sourced from  config_source_system.landing_volume_path.
This writer is only invoked when landing_volume_path is non-null; the
orchestrator skips it otherwise.

Output path structure
----------------------
  <landing_volume_path> / <source_object_name> / ingest_date=<YYYY-MM-DD> /

This creates date-partitioned Hive-style folders for easy lifecycle management
and replay of specific dates.

Supported formats (driven by ingestion_config.file_format):
  parquet (default) | delta | csv | json
"""
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class S3RawWriter:

    def write(
        self,
        df: DataFrame,
        landing_volume_path: str,
        source_object_name: str,
        file_format: str = "parquet",
        mode: str = "append",
    ) -> str:
        """
        Write *df* to the landing path and return the full target path written to.

        Parameters
        ----------
        df                  : Spark DataFrame to write
        landing_volume_path : root path from config_source_system (S3 or Volume)
        source_object_name  : ingestion_config.source_object_name (used as sub-folder)
        file_format         : output format — parquet | delta | csv | json
        mode                : Spark write mode (default: append)
        """
        ingest_date = datetime.utcnow().strftime("%Y-%m-%d")
        target_path = (
            f"{landing_volume_path.rstrip('/')}/"
            f"{source_object_name}/"
            f"ingest_date={ingest_date}"
        )

        df_out = df.withColumn("_ingested_at", F.current_timestamp())

        (
            df_out.write.format(file_format)
            .mode(mode)
            .save(target_path)
        )
        return target_path
