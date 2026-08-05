"""
Writes the extracted DataFrame to a raw landing path (S3 / Databricks Volume).

The landing path is provided externally (widget / job parameter) rather than
read from config_source_system, so a single job can target a specific bucket.

Output path structure
---------------------
  <landing_volume_path> / <source_name> / <source_schema> / <source_object_name> / ingest_date=<YYYY-MM-DD> /

  - landing_volume_path : base path from job widget (e.g. s3://pfl-raw/landing)
  - source_name         : human-readable source system name → separates systems in the same bucket
  - source_schema       : DB schema / SFTP sub-folder
  - source_object_name  : table / file name

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
        source_name: str,
        source_schema: Optional[str],
        source_object_name: str,
        file_format: str = "parquet",
        mode: str = "append",
    ) -> str:
        """
        Write *df* to the landing path and return the full target path.

        Parameters
        ----------
        landing_volume_path : base path from job widget (S3 or Volume)
        source_name         : config_source_system.source_name (sub-folder)
        source_schema       : ingestion_config.source_schema (sub-folder; can be None)
        source_object_name  : ingestion_config.source_object_name (leaf folder)
        file_format         : output format
        mode                : Spark write mode (default: append)
        """
        ingest_date = datetime.utcnow().strftime("%Y-%m-%d")
        schema_part = f"{source_schema}/" if source_schema else ""

        target_path = (
            f"{landing_volume_path.rstrip('/')}/"
            f"{source_name}/"
            f"{schema_part}"
            f"{source_object_name}/"
            f"ingest_date={ingest_date}"
        )

        df_out = df.withColumn("_ingested_at", F.current_timestamp())
        df_out.write.format(file_format).mode(mode).save(target_path)
        return target_path
