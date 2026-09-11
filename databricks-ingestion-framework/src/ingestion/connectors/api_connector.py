"""
LSQ Mavis (LeadSquared Mavis DB export API) connector.

Talks to the Mavis export API and lands its files in S3. It does NOT own config
lookups or step sequencing — see ingestion.utils.config_manager and
ingestion.lsq_mavis.mavis_api_extractor.

Flow per task:
  1. start_export(task)                    -> RequestId
  2. poll_until_ready(task, request_id)    loop until the status is "done"
  3. get_download_url(task, request_id)    -> FileURL
  4. download_and_extract_to_s3(task, url)  download the zip to the task's raw
     landing path, unzip into a sibling `extracted/` folder, return the paths.

The extracted files are picked up by the normal S3 ingestion config downstream;
this module does not load them into Spark.

The x-api-key header comes from the task's own Api_Key column (never logged).
Any failure raises; the extractor tags it with the step + config_id.
"""

from __future__ import annotations

import json
import time
import os
import tempfile
import requests
import zipfile
import shutil
from pyspark.dbutils import DBUtils
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field


def _fmt_dt(value) -> str:
    """Normalise a date value to the ADF ``yyyy-MM-dd HH:mm:ss`` string."""
    return str(value or "").strip().replace("T", " ").split(".")[0][:19]


@dataclass
class MavisApiConfig:
    """
    HTTP / polling settings for the Mavis export API.
    Endpoints mirror the ADF pipeline.
    
    The actual base URL (prod_api) and api_key come from the individual
    task configuration row.
    """

    prod_api: str | None = None
    start_export_path: str = (
        "{database_id}/{table_id}/rows/export?orgcode={org_code}"
    )
    status_path: str = (
        "{database_id}/tables/{table_id}/requesthistory?orgcode={org_code}"
    )
    download_url_path: str = (
        "{database_id}/{table_id}/request/download?orgcode={org_code}"
    )

    request_timeout_seconds: int = 60
    poll_interval_seconds: int = 30      # ADF: 30s between status checks

    # Per-step retry FLOORS (ADF activity retry counts). The effective count is
    # ``max(source_sys.retry_count, <floor>)`` — the single config value can't
    # express six different ADF values, so the two outliers stay here.
    start_export_retries: int = 0        # ADF: Web Get RequestId  -> 0
    poll_retries: int = 0               # ADF: Web Get Status     -> 0
    download_url_retries: int = 50       # ADF: Web Get URL        -> 50
    copy_retries: int = 1               # ADF: Copy zip / unzip   -> 1
    silver_retries: int = 2            # ADF: Silver notebook    -> 2

    done_statuses: list[str] = field(
        default_factory=lambda: ["completed", "success", "succeeded", "done"]
    )
    failed_statuses: list[str] = field(
        default_factory=lambda: ["failed", "error", "cancelled", "canceled"]
    )


class MavisApiExportConnector:
    """The four export steps for a single ``MavisIngestionTaskConfig``."""

    def __init__(self, spark, api_config: MavisApiConfig | None = None):
        self.spark = spark
        self.api = api_config or MavisApiConfig()

    # ── Export steps ───────────────────────────────────────────────────────

    def start_export(self, task) -> str:
        """POST to start the export; return the RequestId."""
        resp = self._post(
            self._build_url(self.api.start_export_path, task),
            task,
            self._build_start_body(task),
        )
        request_id = resp.get("RequestId") or resp.get("RequestID")
        if not request_id:
            raise RuntimeError(
                f"config_id={task.config_id}: start_export response has no RequestId"
            )
        print(f"[Mavis] config_id={task.config_id} export started, id={request_id}")
        return str(request_id)

    def poll_until_ready(self, task, request_id: str, query_timeout: int) -> str:
        """
        POST every ``poll_interval_seconds`` until the status is done / failed.

        ``query_timeout`` is the wall-clock budget in seconds (ADF: the Until
        loop's 12h timeout, from ``config_source_system.query_timeout``).
        """
        url = self._build_url(self.api.status_path, task)
        done = {s.lower() for s in self.api.done_statuses}
        failed = {s.lower() for s in self.api.failed_statuses}

        deadline = time.monotonic() + query_timeout
        attempt = 0
        while time.monotonic() < deadline:
            attempt += 1
            resp = self._post(url, task, {"Parameter": {"RequestId": request_id}})
            status = self._extract_status(resp)
            print(f"[Mavis] config_id={task.config_id} poll {attempt}: '{status}'")
            if status.lower() in done:
                return status
            if status.lower() in failed:
                raise RuntimeError(
                    f"config_id={task.config_id}: export failed with status '{status}'"
                )
            time.sleep(self.api.poll_interval_seconds)

        raise RuntimeError(
            f"config_id={task.config_id}: export not ready after {attempt} polls "
            f"({query_timeout}s query_timeout budget)"
        )

    def get_download_url(self, task, request_id: str) -> str:
        """POST for the exported file's download URL."""
        resp = self._post(
            self._build_url(self.api.download_url_path, task),
            task,
            {"RequestID": request_id, "FileType": "ExportedFile"},
        )
        file_url = resp.get("FileURL") or resp.get("FileUrl")
        if not file_url:
            raise RuntimeError(
                f"config_id={task.config_id}: get_download_url response has no FileURL"
            )
        return str(file_url)

    def download_and_extract_to_s3(
        self,
        task,
        download_url: str,
        source_sys=None,
        trigger_time=None,
        query_timeout: int | None = None,
    ) -> dict:
        """
        Hit the download URL to stream the ZIP file to a temporary Volume path,
        upload that ZIP to the S3 raw landing path, then stream-extract the
        inner CSV and upload it to the S3 unzip path.

        ``source_sys``   provides ``landing_volume_path`` (final S3 bucket root)
                         and ``temp_volume_path`` (Unity Catalog Volume used for
                         staging — avoids local-disk limits on shared/serverless
                         clusters).
        ``trigger_time`` is the fixed run instant (naive UTC — the task's
                         ``sink_batch_started_date``); drives the IST-dated path
                         exactly like ADF's ``triggerTime``. Falls back to "now".
        ``query_timeout``(seconds) is the streaming-download read timeout.

        Path convention (matches ADF):
          bucket    = source_sys.landing_volume_path  (already includes container)
          folder    = task.target_table               (sink / silver table name)
          file_name = {target_schema}_{target_table}

          s3_zip  : {bucket}/{folder}/zip/{yyyy}/{MMM}/{dd}/{file_name}_{ts}.zip
          s3_csv  : {bucket}/{folder}/unzip/{yyyy}/{MMM}/{dd}/{file_name}_{ts}.csv

        Temp staging (same inner structure under the Volume root):
          tmp_zip : {temp_volume_path}/{folder}/zip/{yyyy}/{MMM}/{dd}/{file_name}_{ts}.zip
          tmp_csv : {temp_volume_path}/{folder}/unzip/{yyyy}/{MMM}/{dd}/{file_name}_{ts}.csv

        Returns ``s3_zip_path`` / ``s3_csv_path`` plus copy metrics
        (``data_read_bytes``, ``data_written_bytes``, ``copy_duration_sec``,
        ``throughput_mb_per_sec``).
        """
        # ── resolve landing bucket from source_sys ───────────────────────────
        landing = (
            getattr(source_sys, "landing_volume_path", None)
            or getattr(task, "s3_raw_landing_path", None)
            or ""
        ).rstrip("/")
        if not landing:
            raise RuntimeError(
                f"config_id={task.config_id}: landing_volume_path is not set "
                f"on the source system config"
            )

        temp_root = (
            getattr(source_sys, "temp_volume_path", None) or ""
        ).rstrip("/")
        if not temp_root:
            raise RuntimeError(
                f"config_id={task.config_id}: temp_volume_path is not set "
                f"on the source system config"
            )

        # 1. Hit the download URL
        resp = requests.get(
            download_url,
            stream=True,
            timeout=query_timeout or self.api.request_timeout_seconds,
        )
        resp.raise_for_status()

        # 2. Build IST-dated path components from the fixed trigger_time
        base = trigger_time or datetime.now(timezone.utc)
        ist  = base.replace(tzinfo=None) + timedelta(hours=5, minutes=30)
        year  = ist.strftime("%Y")
        month = ist.strftime("%b")
        day   = ist.strftime("%d")
        ts    = ist.strftime("%Y_%m_%d_%H_%M_%S")

        # folder = sink table name; file_name = schema_table (matches ADF convention)
        folder    = (task.sink_table_name or "export").strip("/")
        file_name = f"{task.sink_schema_name}_{task.sink_table_name}" if task.sink_schema_name else folder

        # inner path shared by both the final S3 destination and the staging Volume
        inner_zip  = f"{folder}/zip/{year}/{month}/{day}/{file_name}_{ts}.zip"
        inner_csv  = f"{folder}/unzip/{year}/{month}/{day}/{file_name}_{ts}.csv"

        s3_zip_path  = f"{landing}/{inner_zip}"
        s3_csv_path  = f"{landing}/{inner_csv}"
        tmp_zip_path = f"{temp_root}/{inner_zip}"
        tmp_csv_path = f"{temp_root}/{inner_csv}"

        # 3. Create the folder structure inside the Volume before writing
        os.makedirs(os.path.dirname(tmp_zip_path), exist_ok=True)
        os.makedirs(os.path.dirname(tmp_csv_path), exist_ok=True)

        started = time.monotonic()
        try:
            # 4. Stream download → Volume ZIP file (8 MB chunks, never loads whole file into RAM)
            with open(tmp_zip_path, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                    fh.write(chunk)

            dbutils = DBUtils(self.spark)

            # 5. Upload ZIP from Volume → final S3 path
            dbutils.fs.cp(tmp_zip_path, s3_zip_path)
            print(f"[Mavis] config_id={task.config_id} uploaded ZIP -> {s3_zip_path}")

            # 6. Disk-to-disk unzip inside the Volume to prevent MemoryError
            with zipfile.ZipFile(tmp_zip_path) as zf:
                csv_members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                if not csv_members:
                    csv_members = zf.namelist()
                if not csv_members:
                    raise ValueError(
                        f"[Mavis] ZIP for config_id={task.config_id} is empty."
                    )

                with zf.open(csv_members[0]) as source_file:
                    with open(tmp_csv_path, "wb") as target_file:
                        shutil.copyfileobj(source_file, target_file, length=8 * 1024 * 1024)

            # 7. Upload CSV from Volume → final S3 path
            dbutils.fs.cp(tmp_csv_path, s3_csv_path)
            print(f"[Mavis] config_id={task.config_id} uploaded CSV -> {s3_csv_path}")

            zip_bytes = os.path.getsize(tmp_zip_path)
            csv_bytes = os.path.getsize(tmp_csv_path)
            duration  = round(time.monotonic() - started, 2)
            return {
                "s3_zip_path": s3_zip_path,
                "s3_csv_path": s3_csv_path,
                "data_read_bytes": zip_bytes,
                "data_written_bytes": csv_bytes,
                "copy_duration_sec": duration,
                "throughput_mb_per_sec": (
                    round(zip_bytes / 1e6 / duration, 2) if duration > 0 else None
                ),
            }

        finally:
            pass
            # ⚠️ TESTING ONLY — cleanup commented out so temp files stay in Volume for inspection
            # if os.path.exists(tmp_zip_path):
            #     os.unlink(tmp_zip_path)
            # if os.path.exists(tmp_csv_path):
            #     os.unlink(tmp_csv_path)


    # ── Helpers ────────────────────────────────────────────────────────────

    @staticmethod
    def _build_start_body(task) -> str:
        """
        start_export body = the configured ``Source_Filter`` (a JSON string),
        matching the ADF logic.

        FULL load  : Source_Filter sent as-is.
        INCREMENTAL: the literal tokens in Source_Filter are replaced ADF-style —
          ``from_date`` -> the configured ``To_Date`` (previous window end)
          ``to_date``   -> the batch trigger time (sink_batch_started_date)
        both formatted ``yyyy-MM-dd HH:mm:ss``.
        """
        source_filter = task.source_filter
        if not source_filter:
            raise RuntimeError(
                f"config_id={task.config_id}: Source_Filter is not set on the config row"
            )
        if task.effective_load_type == "INCREMENTAL":
            source_filter = source_filter.replace(
                "from_date", _fmt_dt(task.to_date)
            ).replace("to_date", _fmt_dt(task.sink_batch_started_date))
        return source_filter

    def _build_url(self, path_template: str, task) -> str:
        """
        ``{prod_api}`` + the ADF path template with ``{database_id}`` /
        ``{table_id}`` / ``{org_code}`` substituted from the task row.

        The base URL is the task's own ``prod_api`` (the ADF ``Prod_API`` column
        on the Mavis config row); it falls back to ``MavisApiConfig.prod_api``
        when the column is blank.
        """
        path = (
            path_template.replace("{database_id}", str(task.database_id or ""))
            .replace("{table_id}", str(task.table_id or ""))
            .replace("{org_code}", str(task.org_code or ""))
        )
        base = getattr(task, "prod_api", None) or self.api.prod_api
        if not base:
            raise RuntimeError(
                f"config_id={task.config_id}: Prod_API is missing on the config row"
            )
        return f"{base.rstrip('/')}/{path.lstrip('/')}"

    def _post(self, url: str, task, body: dict | str) -> dict:
        """POST ``body`` as JSON with the task's x-api-key; return the JSON object.
        ``body`` may be a dict or an already-serialised JSON string (Source_Filter).
        """
        import requests

        if not task.prod_api_key:
            raise RuntimeError(
                f"config_id={task.config_id}: Api_Key is missing on the config row"
            )
        resp = requests.post(
            url,
            headers={"x-api-key": task.prod_api_key, "Content-Type": "application/json"},
            data=body if isinstance(body, str) else json.dumps(body),
            timeout=self.api.request_timeout_seconds,
        )
        resp.raise_for_status()
        parsed = resp.json()
        return parsed if isinstance(parsed, dict) else {"data": parsed}

    @staticmethod
    def _extract_status(resp: dict) -> str:
        """
        Pull the export status out of the requesthistory response. Handles a
        flat ``{"Status": ...}`` object and a list of history records (newest
        first assumed) under ``data`` / ``RequestHistory`` / ``Records``.
        """
        if resp.get("Status") or resp.get("status"):
            return str(resp.get("Status") or resp.get("status")).strip()
        records = (
            resp.get("data")
            or resp.get("RequestHistory")
            or resp.get("Records")
            or []
        )
        if isinstance(records, list) and records and isinstance(records[0], dict):
            rec = records[0]
            return str(rec.get("Status") or rec.get("status") or "").strip()
        return ""
