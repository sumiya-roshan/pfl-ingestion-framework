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


@dataclass
class MavisApiConfig:
    """HTTP / polling settings for the Mavis export API."""

    api_base_url: str = "https://mavis-api.leadsquared.com"
    start_export_path: str = "/api/v1/export/start"
    status_path: str = "/api/v1/export/status"
    download_url_path: str = "/api/v1/export/download"

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
            resp = self._post(url, task, {"RequestId": request_id})
            status = str(resp.get("Status") or resp.get("status") or "").strip()
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
    def _build_start_body(task) -> dict:
        """start_export body. INCREMENTAL adds FromDate / ToDate from the config row."""
        body = {
            "OrgCode": task.org_code,
            "DatabaseId": task.database_id,
            "TableId": task.table_id,
        }
        if task.effective_load_type == "INCREMENTAL":
            body["FromDate"] = task.from_date
            body["ToDate"] = task.to_date
        return body

    def _build_url(self, path: str, task) -> str:
        """
        Per-task endpoint URL. ``{org_code}`` / ``{database_id}`` / ``{table_id}``
        placeholders in the configured path are substituted; a path with no
        placeholders gets the three appended as query params.
        """
        base = self.api.api_base_url.rstrip("/")
        if "{" in path:
            path = (
                path.replace("{org_code}", str(task.org_code or ""))
                .replace("{database_id}", str(task.database_id or ""))
                .replace("{table_id}", str(task.table_id or ""))
            )
            return f"{base}/{path.lstrip('/')}"
        return (
            f"{base}/{path.lstrip('/')}?orgCode={task.org_code}"
            f"&databaseId={task.database_id}&tableId={task.table_id}"
        )

    def _post(self, url: str, task, body: dict) -> dict:
        """POST ``body`` as JSON with the task's x-api-key; return the JSON object."""
        import requests

        if not task.api_key:
            raise RuntimeError(
                f"config_id={task.config_id}: Api_Key is missing on the config row"
            )
        resp = requests.post(
            url,
            headers={"x-api-key": task.api_key, "Content-Type": "application/json"},
            data=json.dumps(body),
            timeout=self.api.request_timeout_seconds,
        )
        resp.raise_for_status()
        parsed = resp.json()
        return parsed if isinstance(parsed, dict) else {"data": parsed}
