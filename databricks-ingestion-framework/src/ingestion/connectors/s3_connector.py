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
import csv
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import boto3
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

    def _is_serverless(self) -> bool:
        """
        Serverless compute runs the driver on Spark Connect, which has no
        local SparkContext — hadoopConfiguration() (used for s3a credentials
        below) isn't reachable there, and serverless also blocks setting
        spark.hadoop.* at runtime outright. extract() therefore routes
        serverless through boto3 end-to-end instead of spark.read.
        """
        return "connect" in type(self.spark).__module__

    def _get_credentials(self) -> Optional[Tuple[str, str]]:
        ss = self.source_system
        if not ss.secret_scope or not ss.secret_key_credentials:
            return None
        return self.secrets.get_credentials(ss.secret_scope, ss.secret_key_credentials)

    def _apply_credentials(self) -> None:
        """
        Sets fs.s3a access/secret keys on the Spark session's Hadoop
        configuration when explicit credentials are configured. Classic
        cluster only — see _is_serverless. When secret_scope/
        secret_key_credentials are unset, relies on the cluster's instance
        profile or a Unity Catalog external location/storage credential
        already granting bucket access.
        """
        credentials = self._get_credentials()
        if not credentials:
            return

        access_key, secret_key = credentials
        hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.access.key", access_key)
        hadoop_conf.set("fs.s3a.secret.key", secret_key)

    @staticmethod
    def _parse_s3_uri(uri: str) -> Tuple[str, str]:
        parsed = urlparse(uri)
        return parsed.netloc, parsed.path.lstrip("/")

    @staticmethod
    def _list_object_keys(s3_client, bucket: str, key: str) -> List[str]:
        """external_path may name a single object or a prefix/folder — try
        the exact key first, then fall back to listing under it as a prefix."""
        if not key.endswith("/"):
            try:
                s3_client.head_object(Bucket=bucket, Key=key)
                return [key]
            except s3_client.exceptions.ClientError:
                pass

        prefix = key if key.endswith("/") else f"{key}/"
        keys = []
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith("/") and obj["Size"] > 0:
                    keys.append(obj["Key"])

        if not keys:
            raise ValueError(f"No objects found under s3://{bucket}/{key}")
        return keys

    def _extract_classic(self) -> DataFrame:
        io = self.ingest_obj

        self._apply_credentials()
        path = self._resolve_source_path()

        delimiter = io.s3_column_delimiter or ","
        header = io.s3_first_row_header if io.s3_first_row_header is not None else True

        return (
            self.spark.read
            .option("header", str(bool(header)).lower())
            .option("delimiter", delimiter)
            .option("inferSchema", "true")
            .csv(path)
        )

    # TODO: hardcoded for testing — replace with a config-driven credential
    # name (source_system or s3_config_master) once the vending path is confirmed.
    _SERVICE_CREDENTIAL_NAME = "benny_credential"

    def _extract_serverless(self) -> DataFrame:
        """
        Reads via boto3 end-to-end (list/get/parse) since spark.read's s3a
        path isn't usable under Spark Connect. Rows land as string columns —
        there's no boto3-side equivalent of Spark's inferSchema — and only
        the final createDataFrame call touches Spark.

        Credentials come from a Unity Catalog Service Credential, vended via
        dbutils.credentials.getServiceCredentialsProvider — serverless has no
        instance profile / ambient credential chain for boto3 to fall back on.
        """
        io = self.ingest_obj
        bucket, key = self._parse_s3_uri(self._resolve_source_path())
        delimiter = io.s3_column_delimiter or ","
        header = io.s3_first_row_header if io.s3_first_row_header is not None else True

        session = boto3.Session()
        session._session._credentials = self.secrets.get_service_credentials_provider(
            self._SERVICE_CREDENTIAL_NAME
        )
        s3_client = session.client("s3")

        columns: Optional[List[str]] = None
        rows: List[List[str]] = []
        for object_key in self._list_object_keys(s3_client, bucket, key):
            body = s3_client.get_object(Bucket=bucket, Key=object_key)["Body"].read()
            for i, row in enumerate(csv.reader(body.decode("utf-8").splitlines(), delimiter=delimiter)):
                if header and i == 0:
                    columns = columns or row
                    continue
                rows.append(row)

        if columns is None:
            columns = [f"_c{i}" for i in range(len(rows[0]))] if rows else []

        return self.spark.createDataFrame(rows, columns)

    # ── Public extract ────────────────────────────────────────────────────────

    def extract(self, watermark_start: Optional[str]) -> Tuple[DataFrame, Optional[str]]:
        df = self._extract_serverless() if self._is_serverless() else self._extract_classic()

        # File-batch source: no column-based watermark to push down.
        return df, None
