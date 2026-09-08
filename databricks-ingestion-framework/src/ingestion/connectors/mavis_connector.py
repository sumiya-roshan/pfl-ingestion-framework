"""
MavisApiConnector — replaces the 5 ADF activities in PL_LSQ_Mavis_Raw_To_Silver
that handle data movement from the Mavis REST API to ADLS.

Lives in ingestion/connectors/ alongside jdbc_connector, sftp_connector, etc.
Unlike the other connectors it is NOT registered in factory.py because it uses
a different configuration object (MavisTableConfig instead of IngestionTaskConfig)
and requires dbutils for ADLS file operations. It is instantiated directly by
MavisOrchestrator.

  ADF activity                  →  Method here
  ─────────────────────────────────────────────────────────────────────────────
  WB_Get_Request_ID             →  _trigger_export()
  Until_Status_Succeeded        →  _poll_until_ready()
    └─ WB_Check_Status          →      _check_status()
    └─ SV_Get_Status / Wait     →      (set variable + sleep 60s)
  WB_Get_Url                    →  _get_download_url()
  CP_Get_Binary_Zip_File        →  _download_and_store_zip()
  CP_Get_Unzipped_File          →  _unzip_to_csv()

Public API
──────────
  connector = MavisApiConnector(spark, dbutils, table, trigger_time_utc, raw_sa_name)
  df, zip_abfss, csv_abfss = connector.extract()

Returns
───────
  df          : Spark DataFrame read from the unzipped CSV in ADLS
  zip_abfss   : abfss:// path where the raw ZIP was stored (for audit)
  csv_abfss   : abfss:// path where the unzipped CSV was stored (for Silver input)
"""

from __future__ import annotations

import io
import os
import tempfile
import time
import zipfile
from datetime import datetime, timezone, timedelta

import requests

from api_sources.lsq_mavis.config import MavisTableConfig

# IST offset: UTC+5:30
_IST_OFFSET = timedelta(hours=5, minutes=30)

# How long to wait between status polls (mirrors ADF Wait activity: 60s)
_POLL_INTERVAL_SEC = 60

# Overall poll timeout — mirrors ADF Until timeout of 12 hours
_POLL_TIMEOUT_SEC = 12 * 3600

# Mavis export status suffix that indicates success
_SUCCESS_SUFFIX = "_Success"


def _utc_to_ist(dt_utc: datetime) -> datetime:
    """Convert a UTC datetime to IST (UTC+5:30), timezone-naive output."""
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    return (dt_utc + _IST_OFFSET).replace(tzinfo=None)


def _build_raw_paths(table: MavisTableConfig, trigger_time_utc: datetime) -> tuple[str, str]:
    """
    Derive the ADLS relative paths for the ZIP and CSV files.

    Mirrors ADF path expressions (all date parts computed from trigger_time in IST):
      ZIP: {Raw_Folder_Path}/zip/{yyyy}/{MMM}/{dd}/{Raw_File_Name}_{yyyy_MM_dd_HH_mm_ss}.zip
      CSV: {Raw_Folder_Path}/unzip/{yyyy}/{MMM}/{dd}/{Raw_File_Name}_{yyyy_MM_dd_HH_mm_ss}.csv
    """
    ist = _utc_to_ist(trigger_time_utc)
    year      = ist.strftime("%Y")
    month     = ist.strftime("%b")   # 'Jan', 'Feb', … — matches ADF 'MMM'
    day       = ist.strftime("%d")
    timestamp = ist.strftime("%Y_%m_%d_%H_%M_%S")

    base = f"{table.raw_folder_path.rstrip('/')}"
    file = f"{table.raw_file_name}_{timestamp}"

    zip_rel = f"{base}/zip/{year}/{month}/{day}/{file}.zip"
    csv_rel = f"{base}/unzip/{year}/{month}/{day}/{file}.csv"
    return zip_rel, csv_rel


def _build_s3_path(bucket: str, container: str, relative_path: str) -> str:
    """Build a fully-qualified s3:// URI for the raw landing path."""
    return f"s3://{bucket}/{container}/{relative_path.lstrip('/')}"


class MavisApiConnector:
    """
    Handles the full Mavis API → raw ZIP → unzipped CSV → Spark DataFrame pipeline
    for a single config row (one table).

    Not a BaseConnector subclass — instantiated directly by MavisOrchestrator
    rather than through the connector factory.
    """

    def __init__(
        self,
        spark,
        dbutils,
        table: MavisTableConfig,
        trigger_time_utc: datetime,
        s3_bucket_name: str,
    ):
        self.spark            = spark
        self.dbutils          = dbutils
        self.table            = table
        self.trigger_time_utc = trigger_time_utc
        self.s3_bucket_name   = s3_bucket_name

        # Derived once — reused across all steps
        self._zip_rel, self._csv_rel = _build_raw_paths(table, trigger_time_utc)
        self._zip_s3 = _build_s3_path(s3_bucket_name, table.raw_container_name, self._zip_rel)
        self._csv_s3 = _build_s3_path(s3_bucket_name, table.raw_container_name, self._csv_rel)

        self._headers = {
            "Content-Type": "application/json",
            "x-api-key":    table.x_api_key,
        }

    # ── Public entry point ────────────────────────────────────────────────────

    def extract(self) -> tuple:
        """
        Full extract flow: trigger export → poll → download URL → ZIP → CSV → DataFrame.

        Returns
        -------
        (df, zip_abfss, csv_abfss)
          df         : Spark DataFrame read from the unzipped CSV
          zip_abfss  : abfss:// path of the stored ZIP (for audit)
          csv_abfss  : abfss:// path of the stored CSV (for Silver input)
        """
        # Step 1 — trigger the async Mavis export job, get RequestId
        request_id = self._trigger_export()
        print(f"[Mavis] config_id={self.table.config_id} — RequestId={request_id}")

        # Step 2 — poll until the export job completes on the Mavis side
        self._poll_until_ready(request_id)
        print(f"[Mavis] config_id={self.table.config_id} — export ready, fetching download URL")

        # Step 3 — get the pre-signed download URL for the ZIP
        file_url = self._get_download_url(request_id)
        print(f"[Mavis] config_id={self.table.config_id} — FileURL obtained")

        # Step 4 — download the ZIP and stream it to disk and S3
        local_zip_path = self._download_and_store_zip(file_url)
        print(f"[Mavis] config_id={self.table.config_id} — ZIP stored at {self._zip_s3}")

        # Step 5 — unzip via disk stream, write CSV to S3, read back as DataFrame
        df = self._unzip_to_csv_and_read(local_zip_path)
        print(f"[Mavis] config_id={self.table.config_id} — CSV stored at {self._csv_s3}")

        return df, self._zip_s3, self._csv_s3

    # ── Step 1: WB_Get_Request_ID ─────────────────────────────────────────────

    def _trigger_export(self) -> str:
        """
        POST to Mavis /rows/export to initiate an async export.
        Returns the RequestId string.

        ADF equivalent: WB_Get_Request_ID
          URL  : {Prod_API}{Database_ID}/{Table_ID}/rows/export?orgcode={Org_Code}
          Body : Source_Filter (with from_date / to_date substituted for Incremental)
        """
        t    = self.table
        url  = f"{t.prod_api.rstrip('/')}/{t.database_id}/{t.table_id}/rows/export?orgcode={t.org_code}"
        body = self._build_export_body()

        response = requests.post(url, headers=self._headers, data=body, timeout=120)
        response.raise_for_status()
        data = response.json()

        request_id = data.get("Data", {}).get("RequestId")
        if not request_id:
            raise ValueError(
                f"Mavis export response did not contain Data.RequestId. "
                f"Response: {data}"
            )
        return str(request_id)

    def _build_export_body(self) -> str:
        """
        Build the POST body for /rows/export.

        ADF expression:
          Incremental: replace 'from_date' → To_Date, 'to_date' → triggerTime in Source_Filter
          Full:        use Source_Filter as-is
        The ADF " → ' substitution was an ADF-expression artefact; not needed in Python.
        """
        t    = self.table
        body = t.source_filter or "{}"

        if t.load_type.upper() == "INCREMENTAL":
            # Format dates to match ADF formatDateTime(...,'yyyy-MM-dd HH:mm:ss')
            ist          = _utc_to_ist(self.trigger_time_utc)
            to_date_fmt  = t.to_date or ist.strftime("%Y-%m-%d %H:%M:%S")
            trigger_fmt  = ist.strftime("%Y-%m-%d %H:%M:%S")
            body = body.replace("from_date", to_date_fmt)
            body = body.replace("to_date",   trigger_fmt)

        return body

    # ── Step 2: Until_Status_Succeeded ────────────────────────────────────────

    def _poll_until_ready(self, request_id: str) -> None:
        """
        Poll the Mavis request-history endpoint every 60 s until the job status
        contains '_Success'. Raises on timeout or on a terminal failure status.

        ADF equivalent: Until_Status_Succeeded loop
          URL       : https://poonawalla-mavis-rest.leadsquared.com/api/{Database_ID}/tables/{Table_ID}/requesthistory?orgcode={Org_Code}
          Body      : {"Parameter": {"RequestId": "<RequestId>"}}
          Condition : status contains '_Success'
          Wait      : 60 seconds per iteration
        """
        t        = self.table
        url      = (
            f"https://poonawalla-mavis-rest.leadsquared.com/api/"
            f"{t.database_id}/tables/{t.table_id}/requesthistory?orgcode={t.org_code}"
        )
        body     = {"Parameter": {"RequestId": request_id}}
        deadline = time.monotonic() + _POLL_TIMEOUT_SEC
        iteration = 0

        while True:
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"[Mavis] Timed out after {_POLL_TIMEOUT_SEC // 3600}h waiting for "
                    f"export RequestId={request_id} (config_id={t.config_id}) to complete."
                )

            status = self._check_status(url, body)
            iteration += 1
            print(
                f"[Mavis] config_id={t.config_id} — poll #{iteration} "
                f"RequestId={request_id} status='{status}'"
            )

            if _SUCCESS_SUFFIX in status:
                return

            if "fail" in status.lower() or "error" in status.lower():
                raise RuntimeError(
                    f"[Mavis] Export failed for RequestId={request_id} "
                    f"(config_id={t.config_id}). Status='{status}'"
                )

            time.sleep(_POLL_INTERVAL_SEC)

    def _check_status(self, url: str, body: dict) -> str:
        """
        POST to the requesthistory endpoint and return the current status string.

        ADF equivalent: WB_Check_Status (inside Until loop)
          Returns: Data.Requests[0].Status
        """
        response = requests.post(url, headers=self._headers, json=body, timeout=60)
        response.raise_for_status()
        data = response.json()
        try:
            return str(data["Data"]["Requests"][0]["Status"])
        except (KeyError, IndexError, TypeError) as exc:
            raise ValueError(
                f"Unexpected requesthistory response structure: {data}"
            ) from exc

    # ── Step 3: WB_Get_Url ────────────────────────────────────────────────────

    def _get_download_url(self, request_id: str) -> str:
        """
        POST to Mavis /request/download to obtain the pre-signed FileURL.

        ADF equivalent: WB_Get_Url (retry 50×, 30s interval)
          URL  : {Prod_API}{Database_ID}/{Table_ID}/request/download?orgcode={Org_Code}
          Body : {"RequestID": "<RequestId>", "FileType": "ExportedFile"}
        """
        t    = self.table
        url  = f"{t.prod_api.rstrip('/')}/{t.database_id}/{t.table_id}/request/download?orgcode={t.org_code}"
        body = {"RequestID": request_id, "FileType": "ExportedFile"}

        max_retries    = 50
        retry_interval = 30
        last_exc: Exception | None = None

        for attempt in range(1, max_retries + 1):
            try:
                response = requests.post(url, headers=self._headers, json=body, timeout=120)
                response.raise_for_status()
                data     = response.json()
                file_url = data.get("Data", {}).get("FileURL")
                if not file_url:
                    raise ValueError(
                        f"Mavis download response did not contain Data.FileURL. "
                        f"Response: {data}"
                    )
                return str(file_url)
            except Exception as exc:
                last_exc = exc
                print(
                    f"[Mavis] config_id={t.config_id} — _get_download_url attempt "
                    f"{attempt}/{max_retries} failed: {exc}"
                )
                if attempt < max_retries:
                    time.sleep(retry_interval)

        raise RuntimeError(
            f"[Mavis] Failed to get download URL after {max_retries} attempts "
            f"for RequestId={request_id} (config_id={t.config_id}). "
            f"Last error: {last_exc}"
        )

    # ── Step 4: CP_Get_Binary_Zip_File ────────────────────────────────────────

    def _download_and_store_zip(self, file_url: str) -> str:
        """
        HTTP GET the FileURL, stream the ZIP to a local temp file, copy to S3.
        Returns the local temp file path of the downloaded ZIP.

        ADF equivalent: CP_Get_Binary_Zip_File
        """
        response  = requests.get(file_url, stream=True, timeout=600)
        response.raise_for_status()

        # Create a temp file but DO NOT delete=True so we can reuse it in the next step
        tmp_fd, tmp_zip_path = tempfile.mkstemp(suffix=".zip")
        os.close(tmp_fd)

        # Stream chunks directly to disk (driver storage), bypassing memory
        with open(tmp_zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)

        # Copy from local driver temp → S3 using dbutils.fs.cp
        self.dbutils.fs.cp(f"file://{tmp_zip_path}", self._zip_s3)

        return tmp_zip_path

    # ── Step 5: CP_Get_Unzipped_File → Spark DataFrame ───────────────────────

    def _unzip_to_csv_and_read(self, local_zip_path: str):
        """
        Unzip via disk stream, write the first CSV entry to S3, read back as Spark DataFrame.
        """
        tmp_fd, tmp_csv_path = tempfile.mkstemp(suffix=".csv")
        os.close(tmp_fd)

        try:
            # Stream unzipping from disk to disk (no RAM blowout)
            import shutil
            with zipfile.ZipFile(local_zip_path) as zf:
                csv_members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                if not csv_members:
                    csv_members = zf.namelist()
                if not csv_members:
                    raise ValueError(f"[Mavis] ZIP for config_id={self.table.config_id} is empty.")
                
                with zf.open(csv_members[0]) as source_file:
                    with open(tmp_csv_path, 'wb') as target_file:
                        shutil.copyfileobj(source_file, target_file, length=8 * 1024 * 1024)

            self.dbutils.fs.cp(f"file://{tmp_csv_path}", self._csv_s3)
        finally:
            if tmp_csv_path and os.path.exists(tmp_csv_path):
                os.unlink(tmp_csv_path)
            # We can now safely delete the local ZIP file
            if local_zip_path and os.path.exists(local_zip_path):
                os.unlink(local_zip_path)

        # Read the CSV from S3 into a Spark DataFrame
        df = (
            self.spark.read
            .option("header",      "true")
            .option("inferSchema", "true")
            .option("multiLine",   "true")
            .option("escape",      '"')
            .csv(self._csv_s3)
        )
        return df
