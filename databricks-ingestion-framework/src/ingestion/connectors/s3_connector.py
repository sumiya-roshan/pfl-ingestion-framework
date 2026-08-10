"""
S3 connector — reads delimited (CSV-style) files directly from an S3 bucket
via Spark's native s3a filesystem support.

Config sources
--------------
  source_system  (config_source_system — SAME table every other connector
                  uses, untouched by S3 support)
    .secret_scope / .secret_key_credentials
                               → optional AWS keys, JSON {"username": "<access_key>",
                                 "password": "<secret_key>"}. Leave both unset when the
                                 workspace already has bucket access via an instance
                                 profile / Unity Catalog external location — in that
                                 case this connector sets no explicit credentials.

  ingest_obj  (s3_config_master, via S3ConfigManager — REPLACES ingestion_config
              for S3 only; one row per report/object)
    .s3_source_bucket_name    → source_bucket_name
    .s3_external_path         → external_path — object key / prefix under the bucket
                                 (or a full s3://... URI, used verbatim)
    .s3_column_delimiter      → column_delimiter (default ',')
    .s3_first_row_header      → first_row_header (default True)

Source path resolution
-----------------------
  s3://<source_bucket_name>/<external_path>
  (external_path is used verbatim if it already starts with s3:// or s3a://)

Load semantics
--------------
This connector is file-batch oriented, like SFTP: there is no column to push
an incremental predicate down to on the S3 side, so extract() always returns
watermark=None. load_type / write_mode on the ingestion object (derived from
key_column) still control how the bronze writer applies the result —
append for plain loads, merge on key_column when one is configured.
"""
from typing import Optional, Tuple

from pyspark.sql import DataFrame

from .base_connector import BaseConnector


class S3Connector(BaseConnector):

    # ── Private helpers ───────────────────────────────────────────────────────

    def _resolve_source_path(self) -> str:
        io = self.ingest_obj

        external_path = (io.s3_external_path or "").strip()
        if external_path.startswith("s3://") or external_path.startswith("s3a://"):
            return external_path

        bucket = io.s3_source_bucket_name
        if not bucket:
            raise ValueError(
                f"s3_config_master.source_bucket_name is not set for "
                f"report '{io.source_object_name}' (config_master_id={io.ingestion_object_id})"
            )
        return f"s3://{bucket.strip('/')}/{external_path.lstrip('/')}"

    def _get_credentials(self) -> Optional[Tuple[str, str]]:
        ss = self.source_system
        if not ss.secret_scope or not ss.secret_key_credentials:
            return None
        return self.secrets.get_credentials(ss.secret_scope, ss.secret_key_credentials)

    def _apply_credentials(self) -> None:
        """
        Sets fs.s3a access/secret keys on the Spark session's Hadoop
        configuration when explicit credentials are configured. When
        secret_scope/secret_key_credentials are unset, relies on the
        cluster's instance profile or a Unity Catalog external
        location/storage credential already granting bucket access.
        """
        credentials = self._get_credentials()
        if not credentials:
            return

        access_key, secret_key = credentials
        hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.access.key", access_key)
        hadoop_conf.set("fs.s3a.secret.key", secret_key)

    # ── Public extract ────────────────────────────────────────────────────────

    def extract(self, watermark_start: Optional[str]) -> Tuple[DataFrame, Optional[str]]:
        self._apply_credentials()
        path = self._resolve_source_path()

        io = self.ingest_obj
        delimiter = io.s3_column_delimiter or ","
        header = io.s3_first_row_header if io.s3_first_row_header is not None else True

        df = (
            self.spark.read
            .option("header", str(bool(header)).lower())
            .option("delimiter", delimiter)
            .option("inferSchema", "true")
            .csv(path)
        )

        # File-batch source: no column-based watermark to push down.
        return df, None
