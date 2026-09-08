"""
LSQ Mavis (LeadSquared Mavis DB export API) connector.

Talks to the Mavis export API and lands its files in S3. It does NOT own config
lookups or step sequencing — see ingestion.utils.config_manager and
ingestion.utils.mavis_api_extractor.

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
import zipfile
from dataclasses import dataclass, field


def _fmt_dt(value) -> str:
    """Normalise a date value to the ADF ``yyyy-MM-dd HH:mm:ss`` string."""
    return str(value or "").strip().replace("T", " ").split(".")[0][:19]


@dataclass
class MavisApiConfig:
    """
    HTTP / polling settings for the Mavis export API.

    Endpoints mirror the ADF pipeline. ``prod_api`` is the ADF ``Prod_API``
    parameter (base URL, e.g. ``https://<host>/v2/...``); the three path
    templates are concatenated onto it with the per-task ids substituted:

      start_export   {prod_api}{database_id}/{table_id}/rows/export?orgcode={org_code}
      status         {prod_api}{database_id}/tables/{table_id}/requesthistory?orgcode={org_code}
      download_url   {prod_api}{database_id}/{table_id}/request/download?orgcode={org_code}
    """

    prod_api: str = "https://mavis-api.leadsquared.com/"
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
    poll_interval_seconds: int = 15
    poll_max_attempts: int = 120

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

    def poll_until_ready(self, task, request_id: str) -> str:
        """POST every ``poll_interval_seconds`` until the status is done / failed."""
        url = self._build_url(self.api.status_path, task)
        done = {s.lower() for s in self.api.done_statuses}
        failed = {s.lower() for s in self.api.failed_statuses}

        attempt = 0
        while attempt < self.api.poll_max_attempts:
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
            f"config_id={task.config_id}: export not ready after {attempt} polls"
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

    def download_and_extract_to_s3(self, task, download_url: str) -> list[str]:
        """
        Download the export zip to the task's raw landing path, unzip it into a
        sibling ``extracted/`` folder, and return the extracted file paths.

        ``s3_raw_landing_path`` must be a writable path (Unity Catalog Volume or
        DBFS mount), not a bare ``s3://`` URI.
        """
        import requests

        landing = (task.s3_raw_landing_path or "").rstrip("/")
        if not landing:
            raise RuntimeError(
                f"config_id={task.config_id}: s3_raw_landing_path is not set"
            )

        resp = requests.get(download_url, timeout=self.api.request_timeout_seconds)
        resp.raise_for_status()

        zip_path = f"{landing}/export.zip"
        with open(zip_path, "wb") as fh:
            fh.write(resp.content)
        print(f"[Mavis] config_id={task.config_id} zip -> {zip_path}")

        extracted_dir = f"{landing}/extracted"
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(extracted_dir)
            files = [
                f"{extracted_dir}/{n}" for n in zf.namelist() if not n.endswith("/")
            ]
        print(f"[Mavis] config_id={task.config_id} extracted {len(files)} file(s)")
        return files

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
        """
        path = (
            path_template.replace("{database_id}", str(task.database_id or ""))
            .replace("{table_id}", str(task.table_id or ""))
            .replace("{org_code}", str(task.org_code or ""))
        )
        return f"{self.api.prod_api.rstrip('/')}/{path.lstrip('/')}"

    def _post(self, url: str, task, body: dict | str) -> dict:
        """POST ``body`` as JSON with the task's x-api-key; return the JSON object.
        ``body`` may be a dict or an already-serialised JSON string (Source_Filter).
        """
        import requests

        if not task.api_key:
            raise RuntimeError(
                f"config_id={task.config_id}: Api_Key is missing on the config row"
            )
        resp = requests.post(
            url,
            headers={"x-api-key": task.api_key, "Content-Type": "application/json"},
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
