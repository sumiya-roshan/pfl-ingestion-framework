"""
S3 connector — reads delimited (CSV-style) files from S3 using boto3.

Why boto3 and not spark.read.csv(s3://...)?
--------------------------------------------
On Unity Catalog-enabled clusters (Single User or Shared access mode),
Databricks intercepts ALL cloud storage paths (s3://, s3a://, abfss://)
at Spark's query *analysis* phase via ResolveWithCredential before any
filesystem code runs. This means fs.s3a.access.key / fs.s3a.secret.key
set on the Hadoop configuration are never reached — UC checks External
Location permissions first and raises PERMISSION_DENIED.

boto3 is the AWS Python SDK and bypasses Spark's filesystem layer entirely.
We fetch the file content through the AWS SDK, parse it with pandas, and
then convert the resulting DataFrame to a Spark DataFrame. This works on
both classic and Unity Catalog-enabled clusters without needing External
Locations or Storage Credentials configured in Unity Catalog.

Credentials
-----------
  source_system.secret_scope / .secret_key_credentials
      → secret stored in a Databricks secret scope as JSON:
        {"username": "<AWS_ACCESS_KEY_ID>", "password": "<AWS_SECRET_ACCESS_KEY>"}
      → when BOTH are set, boto3 client is created with explicit creds.
      → when EITHER is unset, boto3 falls back to the cluster's IAM instance
        profile (works on clusters that have an attached instance profile with
        S3 read access).

Config fields from ingest_obj (resolved from s3_config_master)
--------------------------------------------------------------
  s3_source_bucket_name  → S3 bucket name (without s3:// prefix)
  s3_external_path       → object key / prefix inside the bucket
                           (or full s3[a]:// URI — bucket+key are extracted)
  s3_column_delimiter    → column delimiter character (default ',')
  s3_first_row_header    → bool, whether first row is a header (default True)

Load semantics
--------------
File-batch oriented: extract() always returns watermark=None.
load_type / write_mode from the config row control how the bronze writer
applies the result (overwrite for FULL, append or merge otherwise).
"""
import io
import re
from typing import Optional, Tuple

import pandas as pd
from pyspark.sql import DataFrame

from .base_connector import BaseConnector


class S3Connector(BaseConnector):

    # ── Private helpers ───────────────────────────────────────────────────────

    def _parse_bucket_and_key(self) -> Tuple[str, str]:
        """
        Return (bucket, key) from the config fields.

        Handles three formats in s3_external_path:
          1. Full URI:  s3://my-bucket/path/to/file.csv
          2. Full URI:  s3a://my-bucket/path/to/file.csv
          3. Key only:  path/to/file.csv  (bucket = s3_source_bucket_name)
        """
        io_cfg = self.ingest_obj
        external_path = (io_cfg.s3_external_path or "").strip()

        uri_match = re.match(r"s3a?://([^/]+)/?(.*)", external_path)
        if uri_match:
            bucket = uri_match.group(1)
            key    = uri_match.group(2)
        else:
            bucket = (io_cfg.s3_source_bucket_name or "").strip("/")
            key    = external_path.lstrip("/")

        if not bucket:
            raise ValueError(
                f"source_bucket_name is not set for "
                f"report '{io_cfg.source_object_name}' (config_id={io_cfg.config_id})"
            )
        if not key:
            raise ValueError(
                f"s3_external_path / external_path is not set for "
                f"report '{io_cfg.source_object_name}' (config_id={io_cfg.config_id})"
            )
        return bucket, key

    def _get_boto3_client(self):
        """
        Build a boto3 S3 client.

        Uses explicit credentials from the Databricks secret scope when both
        secret_scope and secret_key_credentials are set on the source system row.
        Falls back to the cluster's IAM instance profile otherwise.
        """
        import boto3

        ss = self.source_system
        if ss.secret_scope and ss.secret_key_credentials:
            access_key, secret_key = self.secrets.get_credentials(
                ss.secret_scope, ss.secret_key_credentials
            )
            return boto3.client(
                "s3",
                aws_access_key_id     = access_key,
                aws_secret_access_key = secret_key,
            )
        else:
            # Rely on IAM instance profile attached to the cluster
            return boto3.client("s3")

    # ── Public extract ────────────────────────────────────────────────────────

    def extract(self, watermark_start: Optional[str]) -> Tuple[DataFrame, Optional[str]]:
        """
        Download the S3 object via boto3, parse with pandas, return as Spark DF.

        Steps:
          1. Resolve bucket + key from config fields.
          2. Build a boto3 client (explicit creds or IAM profile).
          3. Stream the object body into memory.
          4. Parse with pandas.read_csv().
          5. Convert to Spark DataFrame via spark.createDataFrame().

        Returns (df, None) — watermark is always None for file-batch sources.
        """
        bucket, key = self._parse_bucket_and_key()
        client      = self._get_boto3_client()

        io_cfg    = self.ingest_obj
        delimiter = io_cfg.s3_column_delimiter or ","
        header    = io_cfg.s3_first_row_header if io_cfg.s3_first_row_header is not None else True

        # Stream the S3 object body into an in-memory buffer
        response = client.get_object(Bucket=bucket, Key=key)
        body     = response["Body"].read()

        pandas_df = pd.read_csv(
            io.BytesIO(body),
            sep           = delimiter,
            header        = 0 if header else None,
        )

        # Convert all column names to strings (pandas may infer int column names
        # when header=False, which Spark does not accept)
        pandas_df.columns = [str(c) for c in pandas_df.columns]

        spark_df = self.spark.createDataFrame(pandas_df)

        # File-batch source: no column-based watermark to push down.
        return spark_df, None