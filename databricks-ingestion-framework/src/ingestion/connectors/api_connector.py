"""
LSQ Mavis (LeadSquared Mavis DB export API) connector — HTTP + S3 plumbing.

This module owns everything about *talking to the Mavis export API and moving
its files*, split into discrete steps. It does NOT own orchestration or config
lookups:

  * config-table reads/writes           → ingestion.utils.config_manager
                                          (MavisConfigManager, MavisIngestionTaskConfig)
  * step sequencing + status writeback   → ingestion.utils.mavis_api_extractor
                                          (MavisApiExtractor.run)

Steps implemented here (all POST, all keyed by the task's own Api_Key column —
never dbutils.secrets, and the raw key value is never logged):

  1. start_export(task)                    kick off the export, parse RequestId.
     INCREMENTAL tasks send a from/to window
     (silver_last_sink_date - lookback_hours → run time); FULL omits it.
  2. poll_until_ready(task, request_id)     POST loop with backoff until the
     status is in the configurable "done" set; raises immediately on a
     "failed" status, logging attempt / status / elapsed / config_id.
  3. get_download_url(task, request_id)     body {"RequestID", "FileType":
     "ExportedFile"} → parse FileURL.
  4/5. download_and_extract_to_s3(task, url) stream the (zip) archive straight
     into the task's raw S3 prefix (no DBFS/local landing kept), then unzip via
     a temp file and stream every member to a sibling "extracted" prefix.
     Local temp files are always cleaned up. retry_count / retry_interval from
     the shared config_source_system row guard transient failures.
  6. load_to_target(task, extracted_paths)  auto-detect each file's format from
     its extension (_infer_format), read into Spark, write to
     target_catalog.target_schema.target_table per write_mode.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import time
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from ..utils.config_manager import (
    MavisIngestionTaskConfig,
    SourceSystemConfig,
    _DictSerializable,
)
from ..utils.retry import retry_on_failure


@dataclass
class MavisApiConfig(_DictSerializable):
    """
    HTTP / polling behaviour for the Mavis export API. Endpoint paths and the
    done/failed status vocabularies are configurable so nothing about the remote
    contract is hardcoded into the flow.
    """

    api_base_url: str = "https://mavis-api.leadsquared.com"
    start_export_path: str = "/api/v1/export/start"
    status_path: str = "/api/v1/export/status"
    download_url_path: str = "/api/v1/export/download"

    request_timeout_seconds: int = 60

    poll_interval_seconds: int = 15
    poll_backoff_factor: float = 1.5
    poll_max_interval_seconds: int = 120
    poll_max_attempts: int = 120

    # Status strings (compared case-insensitively) that end the poll loop.
    done_statuses: list[str] = field(
        default_factory=lambda: ["completed", "success", "succeeded", "done"]
    )
    failed_statuses: list[str] = field(
        default_factory=lambda: ["failed", "error", "cancelled", "canceled"]
    )

    # Sibling S3 prefixes under the task's raw landing path.
    raw_prefix: str = "raw"
    extracted_prefix: str = "extracted"


class MavisApiExportError(RuntimeError):
    """Raised for any failure in the Mavis export flow; message is config_id-tagged."""


class MavisApiExportConnector:
    """
    Discrete steps of the Mavis export for a single ``MavisIngestionTaskConfig``.
    Instance methods use ``self.spark``; pure transforms are ``@staticmethod``.
    Orchestration (calling these in order + status writeback) lives in
    ``MavisApiExtractor``.

    ``source_system`` is the shared ``config_source_system`` row — its
    ``retry_count`` / ``retry_interval`` drive the download/upload retries, the
    same way every other connector uses them.
    """

    def __init__(
        self,
        spark,
        source_system: SourceSystemConfig,
        api_config: MavisApiConfig | None = None,
        boto3_session=None,
    ):
        self.spark = spark
        self.source_system = source_system
        self.api = api_config or MavisApiConfig()
        self._boto3_session = boto3_session

    @property
    def _retry_count(self) -> int:
        return int(self.source_system.retry_count or 0)

    @property
    def _retry_interval(self) -> int:
        return int(self.source_system.retry_interval or 0)

    # ── Step 1 — START EXPORT ───────────────────────────────────────────────

    def start_export(self, task: MavisIngestionTaskConfig) -> str:
        """POST to start the export; return the RequestId."""
        url = self._build_url(self.api.start_export_path, task)
        body = self._build_start_body(task, self._now_utc())

        resp = self._post(url, task, body, step="start_export")
        request_id = (
            resp.get("RequestId")
            or resp.get("RequestID")
            or resp.get("requestId")
            or resp.get("request_id")
        )
        if not request_id:
            raise MavisApiExportError(
                f"config_id={task.config_id}: start_export response has no "
                f"RequestId (keys: {list(resp.keys())})"
            )
        print(
            f"[Mavis] config_id={task.config_id} started export, "
            f"RequestId={request_id}"
        )
        return str(request_id)

    @staticmethod
    def _build_start_body(task: MavisIngestionTaskConfig, run_time: datetime) -> dict:
        """
        Pure transform: request body for the start call. INCREMENTAL tasks get a
        from/to window (silver_last_sink_date - lookback_hours → run_time); FULL
        tasks omit date filters entirely.
        """
        body: dict = {
            "OrgCode": task.org_code,
            "DatabaseId": task.database_id,
            "TableId": task.table_id,
        }
        if task.effective_load_type == "INCREMENTAL":
            from_dt = MavisApiExportConnector._incremental_from(task)
            body["FromDate"] = from_dt.strftime("%Y-%m-%d %H:%M:%S")
            body["ToDate"] = run_time.strftime("%Y-%m-%d %H:%M:%S")
        return body

    @staticmethod
    def _incremental_from(task: MavisIngestionTaskConfig) -> datetime:
        base = task.silver_last_sink_date
        if not base:
            raise MavisApiExportError(
                f"config_id={task.config_id}: INCREMENTAL load needs "
                f"silver_last_sink_date but it is empty"
            )
        raw = str(base).strip().replace("Z", "+00:00").replace("T", " ").split(".")[0]
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")  # noqa: DTZ007
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt - timedelta(hours=task.lookback_hours or 0)

    # ── Step 2 — POLL STATUS ───────────────────────────────────────────────

    def poll_until_ready(self, task: MavisIngestionTaskConfig, request_id: str) -> str:
        """
        POST-loop with backoff until the reported status is in
        ``api.done_statuses``. Raises immediately on ``api.failed_statuses`` or
        when the attempt/timeout budget is exhausted.
        """
        url = self._build_url(self.api.status_path, task)
        done = {s.lower() for s in self.api.done_statuses}
        failed = {s.lower() for s in self.api.failed_statuses}

        interval = float(self.api.poll_interval_seconds)
        started = time.monotonic()

        for attempt in range(1, self.api.poll_max_attempts + 1):
            resp = self._post(
                url,
                task,
                {"RequestId": request_id, "RequestID": request_id},
                step="poll_until_ready",
            )
            status = str(resp.get("Status") or resp.get("status") or "").strip()
            elapsed = time.monotonic() - started
            print(
                f"[Mavis] config_id={task.config_id} poll attempt {attempt} "
                f"status='{status}' elapsed={elapsed:.1f}s"
            )

            if status.lower() in done:
                return status
            if status.lower() in failed:
                raise MavisApiExportError(
                    f"config_id={task.config_id}: export reported failed status "
                    f"'{status}' after {elapsed:.1f}s"
                )

            time.sleep(interval)
            interval = min(
                interval * self.api.poll_backoff_factor,
                float(self.api.poll_max_interval_seconds),
            )

        raise MavisApiExportError(
            f"config_id={task.config_id}: export not ready after "
            f"{self.api.poll_max_attempts} poll attempts"
        )

    # ── Step 3 — GET DOWNLOAD URL ──────────────────────────────────────────

    def get_download_url(self, task: MavisIngestionTaskConfig, request_id: str) -> str:
        url = self._build_url(self.api.download_url_path, task)
        resp = self._post(
            url,
            task,
            {"RequestID": request_id, "FileType": "ExportedFile"},
            step="get_download_url",
        )
        file_url = (
            resp.get("FileURL")
            or resp.get("FileUrl")
            or resp.get("fileUrl")
            or resp.get("file_url")
        )
        if not file_url:
            raise MavisApiExportError(
                f"config_id={task.config_id}: get_download_url response has no "
                f"FileURL (keys: {list(resp.keys())})"
            )
        return str(file_url)

    # ── Steps 4 & 5 — DOWNLOAD TO S3 + UNZIP ───────────────────────────────

    def download_and_extract_to_s3(
        self, task: MavisIngestionTaskConfig, download_url: str
    ) -> list[str]:
        """
        Stream the export archive straight into the task's raw S3 prefix (no
        DBFS/local landing kept), then unzip via a temp file and stream each
        member to a sibling ``extracted`` prefix. Returns the extracted S3 URIs.
        Transient failures are retried per source_system.retry_count/retry_interval.
        """
        import requests  # local import: keeps module importable without requests

        s3 = self._s3_client()
        raw_bucket, raw_key_prefix = self._split_s3_uri(self._raw_base_uri(task))

        archive_name = self._archive_name(download_url)
        raw_key = f"{raw_key_prefix.rstrip('/')}/{self.api.raw_prefix}/{archive_name}"
        extracted_key_prefix = (
            f"{raw_key_prefix.rstrip('/')}/{self.api.extracted_prefix}"
        )

        def _download_to_s3() -> None:
            with requests.get(
                download_url, stream=True, timeout=self.api.request_timeout_seconds
            ) as r:
                r.raise_for_status()
                r.raw.decode_content = True
                s3.upload_fileobj(r.raw, raw_bucket, raw_key)

        retry_on_failure(
            _download_to_s3,
            max_retries=self._retry_count,
            retry_interval=self._retry_interval,
            description=(
                f"download config_id={task.config_id} → s3://{raw_bucket}/{raw_key}"
            ),
        )
        raw_uri = f"s3://{raw_bucket}/{raw_key}"
        print(f"[Mavis] config_id={task.config_id} archive landed at {raw_uri}")

        # Unzip: pull the archive back down to local temp, extract, push members up.
        tmp_dir = tempfile.mkdtemp(prefix=f"mavis_{task.config_id}_")
        extracted_uris: list[str] = []
        try:
            local_zip = os.path.join(tmp_dir, archive_name)

            def _fetch_archive() -> None:
                s3.download_file(raw_bucket, raw_key, local_zip)

            retry_on_failure(
                _fetch_archive,
                max_retries=self._retry_count,
                retry_interval=self._retry_interval,
                description=f"refetch archive config_id={task.config_id}",
            )

            if not zipfile.is_zipfile(local_zip):
                # Not a zip — treat the downloaded file itself as the single member.
                member_key = f"{extracted_key_prefix}/{archive_name}"
                s3.upload_file(local_zip, raw_bucket, member_key)
                return [f"s3://{raw_bucket}/{member_key}"]

            extract_dir = os.path.join(tmp_dir, "unzipped")
            os.makedirs(extract_dir, exist_ok=True)
            with zipfile.ZipFile(local_zip) as zf:
                members = [m for m in zf.namelist() if not m.endswith("/")]
                zf.extractall(extract_dir)

            for member in members:
                local_member = os.path.join(extract_dir, member)
                member_key = f"{extracted_key_prefix}/{member.lstrip('/')}"

                def _upload_member(lm=local_member, mk=member_key) -> None:
                    s3.upload_file(lm, raw_bucket, mk)

                retry_on_failure(
                    _upload_member,
                    max_retries=self._retry_count,
                    retry_interval=self._retry_interval,
                    description=f"upload member {member} config_id={task.config_id}",
                )
                extracted_uris.append(f"s3://{raw_bucket}/{member_key}")

            print(
                f"[Mavis] config_id={task.config_id} extracted "
                f"{len(extracted_uris)} file(s) → "
                f"s3://{raw_bucket}/{extracted_key_prefix}/"
            )
            return extracted_uris
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    # ── Step 6 — LOAD TO SPARK / WRITE TO TARGET ───────────────────────────

    def load_to_target(
        self, task: MavisIngestionTaskConfig, extracted_s3_paths: list[str]
    ) -> int:
        """
        Read every extracted file (format inferred from its extension) into a
        single Spark DataFrame and write it to the task's target table per
        write_mode. Returns the row count written.
        """
        if not extracted_s3_paths:
            raise MavisApiExportError(
                f"config_id={task.config_id}: no extracted files to load"
            )

        combined = None
        for path in extracted_s3_paths:
            fmt = self._infer_format(path)
            df = self._read_one(path, fmt)
            combined = (
                df
                if combined is None
                else combined.unionByName(df, allowMissingColumns=True)
            )

        row_count = combined.count()
        (
            combined.write.format("delta")
            .mode(task.effective_write_mode)
            .option("mergeSchema", "true")
            .saveAsTable(task.full_target_table)
        )
        print(
            f"[Mavis] config_id={task.config_id} wrote {row_count} row(s) to "
            f"{task.full_target_table} (mode={task.effective_write_mode})"
        )
        return row_count

    def _read_one(self, path: str, fmt: str):
        spark_path = path.replace("s3://", "s3a://", 1)
        reader = self.spark.read
        if fmt == "csv":
            return (
                reader.option("header", "true")
                .option("inferSchema", "true")
                .csv(spark_path)
            )
        if fmt == "json":
            return reader.option("multiLine", "true").json(spark_path)
        if fmt == "parquet":
            return reader.parquet(spark_path)
        raise MavisApiExportError(f"unhandled inferred format '{fmt}' for {path}")

    @staticmethod
    def _infer_format(file_path: str) -> str:
        """
        Map a file extension to a Spark reader format. Raises for anything
        unrecognised rather than guessing.
        """
        ext = os.path.splitext(file_path.split("?")[0])[1].lower().lstrip(".")
        mapping = {
            "csv": "csv",
            "txt": "csv",
            "tsv": "csv",
            "json": "json",
            "ndjson": "json",
            "jsonl": "json",
            "parquet": "parquet",
            "pq": "parquet",
        }
        if ext not in mapping:
            raise MavisApiExportError(
                f"cannot infer Spark format for extension '.{ext}' ({file_path})"
            )
        return mapping[ext]

    # ── HTTP plumbing ──────────────────────────────────────────────────────

    def _post(
        self, url: str, task: MavisIngestionTaskConfig, body: dict, step: str
    ) -> dict:
        """
        POST ``body`` as JSON with the ``x-api-key`` header taken from the
        task's own Api_Key column. Never logs the key. Returns the parsed JSON
        body. All errors are re-raised as config_id-tagged
        ``MavisApiExportError``.
        """
        import requests

        if not task.api_key:
            raise MavisApiExportError(
                f"config_id={task.config_id}: Api_Key is missing on the config row"
            )
        headers = {
            "x-api-key": task.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        try:
            resp = requests.post(
                url,
                headers=headers,
                data=json.dumps(body),
                timeout=self.api.request_timeout_seconds,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise MavisApiExportError(
                f"config_id={task.config_id}: HTTP call failed at '{step}' "
                f"(POST {url}): {exc}"
            ) from exc

        try:
            parsed = resp.json()
        except ValueError as exc:
            raise MavisApiExportError(
                f"config_id={task.config_id}: non-JSON response at '{step}' "
                f"(POST {url}): {resp.text[:200]!r}"
            ) from exc

        if not isinstance(parsed, dict):
            return {"data": parsed}
        return parsed

    def _build_url(self, path: str, task: MavisIngestionTaskConfig) -> str:
        """
        Build a per-task endpoint URL from org_code / database_id / table_id.
        Placeholders in the configured path are substituted; otherwise the ids
        are appended as query params so no Mavis-specific routing is hardcoded.
        """
        base = self.api.api_base_url.rstrip("/")
        filled = (
            path.replace("{org_code}", str(task.org_code or ""))
            .replace("{database_id}", str(task.database_id or ""))
            .replace("{table_id}", str(task.table_id or ""))
        )
        url = f"{base}/{filled.lstrip('/')}"
        if "{" not in path and "org_code" not in path:
            sep = "&" if "?" in url else "?"
            url = (
                f"{url}{sep}orgCode={task.org_code}"
                f"&databaseId={task.database_id}&tableId={task.table_id}"
            )
        return url

    # ── S3 plumbing ────────────────────────────────────────────────────────

    def _s3_client(self):
        import boto3

        session = self._boto3_session or boto3.session.Session()
        return session.client("s3")

    def _raw_base_uri(self, task: MavisIngestionTaskConfig) -> str:
        if task.s3_raw_landing_path:
            return task.s3_raw_landing_path
        raise MavisApiExportError(
            f"config_id={task.config_id}: no raw landing path configured "
            f"(expected s3_raw_landing_path / raw_landing_path column)"
        )

    @staticmethod
    def _split_s3_uri(uri: str) -> tuple[str, str]:
        cleaned = uri.strip()
        for scheme in ("s3://", "s3a://", "s3n://"):
            if cleaned.startswith(scheme):
                cleaned = cleaned[len(scheme):]
                break
        else:
            raise MavisApiExportError(f"not an S3 URI: {uri!r}")
        bucket, _, key = cleaned.partition("/")
        return bucket, key

    @staticmethod
    def _archive_name(download_url: str) -> str:
        return os.path.basename(download_url.split("?")[0]) or "mavis_export.zip"

    @staticmethod
    def _now_utc() -> datetime:
        return datetime.now(timezone.utc)
