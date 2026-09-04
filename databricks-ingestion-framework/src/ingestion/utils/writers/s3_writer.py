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

from databricks.sdk.runtime import dbutils
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class S3RawWriter:
    def write(
        self,
        df: DataFrame,
        landing_volume_path: str,
        source_name: str,
        source_schema: str | None,
        source_object_name: str,
        file_format: str = "parquet",
        mode: str = "append",
        file_prefix: str | None = None,
        file_timestamp: datetime | None = None,
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

        schema_part = f"{source_schema}/" if source_schema else ""
        print(type(file_timestamp),file_timestamp)
        target_path = (
            f"{landing_volume_path.rstrip('/')}/"
            f"{source_name}/"
            f"{schema_part}"
            f"{source_object_name}/"
            f"{file_timestamp.strftime('%Y')}/"
            f"{file_timestamp.strftime('%b')}/"
            f"{file_timestamp.strftime('%d')}"
        )

        df_out = df.withColumn("_ingested_at", F.current_timestamp())
        df_out.write.format(file_format).mode(mode).save(target_path)

        file_timestamp = file_timestamp.strftime("%Y_%m_%d_%H_%M_%S")

        final_filename = self._build_filename(
            source_object_name, file_timestamp, file_format, file_prefix
        )
        self._rename_output_file(target_path, final_filename)

        return target_path

    @staticmethod
    def _build_filename(
        source_object_name: str,
        timestamp: str,
        file_format: str,
        filename_prefix: str | None,
    ) -> str:
        base = f"{source_object_name}_{timestamp}.{file_format}"
        return f"{filename_prefix}_{base}" if filename_prefix else base

    @staticmethod
    def _rename_output_file(target_path: str, final_filename: str) -> None:
        """
        Rename Spark's auto-generated part-* file to final_filename, and
        clean up the _SUCCESS / .crc sidecar files. Uses dbutils.fs (via the
        Connect-safe SDK shim) so it works under Spark Connect / serverless,
        where sparkContext._jvm is unavailable.
        """
        files = dbutils.fs.ls(target_path)

        part_files = [f for f in files if f.name.startswith("part-")]
        if not part_files:
            raise FileNotFoundError(
                f"No part-* file found under {target_path} to rename"
            )

        part_file = part_files[0].path
        dbutils.fs.mv(part_file, f"{target_path}/{final_filename}")

        # cleanup: _SUCCESS, _committed_*, _started_*, .crc files
        for f in dbutils.fs.ls(target_path):
            if f.name.startswith("_") or f.name.endswith(".crc"):
                dbutils.fs.rm(f.path, True)
