"""
LSQ Mavis export orchestrator — the ADF "ForEach body" for one task.

Runs the export steps in order for a single ``MavisIngestionTaskConfig`` and
writes Status back to its config-table row via the ordinary ``ConfigManager``:

  1. Status = AUDIT_STATUS_INPROGRESS
  2. start_export            -> RequestId
  3. poll_until_ready        (12h wall-clock budget from query_timeout)
  4. get_download_url        -> FileURL
  5. download_and_extract_to_s3
  6. Status = AUDIT_STATUS_SUCCESS   (on failure: AUDIT_STATUS_FAILED, re-raised)
  7. trigger the Silver notebook   (logged only — never fails the export)

Owned here, mirroring ``IngestionOrchestrator`` for the connector path:

* **Audit** — one INPROGRESS row per task at the start, closed SUCCESS / FAILED
  at the end. Every ADF-derived column (Delta_Layer, SourceSchema/Table,
  TargetSchema/Table, business_date, trigger_time, Frequency, byte / throughput
  / duration metrics) is reproduced from the fixed ``sink_batch_started_date``
  (ADF ``triggerTime``) and the connector's real copy metrics.
* **Retry** — per ADF activity. ``config_source_system`` gives one
  ``retry_count`` / ``retry_interval`` / ``query_timeout``; the two ADF retry
  outliers (get_download_url = 50, silver = 2) are floors in ``MavisApiConfig``.
* **Silver** — ``dbutils.notebook.run`` of the Mavis silver notebook, with all
  parameters derived here.

Entry point: ``src/ingestion/lsq_mavis/api_export_main.py``.

    extractor = MavisApiExtractor(spark, config_mgr, audit_table=AUDIT_TABLE,
                                  environment=environment, dbutils=dbutils,
                                  silver_notebook_path=..., silver_notebook_timeout=...)
    results = extractor.run_all(tasks, source_sys, pipeline_name,
                                job_context, config_master_id)
    # results: {config_id: ("SUCCESS", [zip, csv]) | ("FAILED", "<error>")}

``run_all`` fans the tasks out one worker per distinct ``config_id``; ``run``
does one task. The caller owns the summary print + ``dbutils.notebook.exit``.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from pyspark.dbutils import DBUtils

from ..connectors.api_connector import MavisApiConfig, MavisApiExportConnector
from ..utils.audit import AuditLogger
from ..utils.config_manager import (
    AUDIT_STATUS_FAILED,
    AUDIT_STATUS_INPROGRESS,
    AUDIT_STATUS_SUCCESS,
    ConfigManager,
    MavisIngestionTaskConfig,
)
from ..utils.logger import get_logger
from ..utils.retry import retry_on_failure

_IST = timedelta(hours=5, minutes=30)
_DEFAULT_QUERY_TIMEOUT = 12 * 3600  # 43200s — fallback when query_timeout is blank


def _parse_bsd(value) -> datetime:
    """
    ``sink_batch_started_date`` -> naive UTC datetime (the ADF ``triggerTime``).
    ``get_tasks.py`` stamps it as UTC ``'YYYY-MM-DD HH:MM:SS'``; it arrives as a
    ``datetime`` (standalone) or an ISO-ish string (job mode via taskValues).
    Falls back to "now" for the ``'1'`` sentinel / blank / unparseable.
    """
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    text = str(value).strip() if value is not None else ""
    if text and text != "1":
        try:
            return datetime.fromisoformat(
                text.replace("T", " ").rstrip("Z").split("+")[0].strip()
            )
        except ValueError:
            pass
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _hms_to_seconds(value) -> int | None:
    """``'HH:mm:ss'`` (e.g. ``'12:00:00'``) -> whole seconds. Blank / ``'0'`` /
    malformed -> ``None`` (caller applies its own default)."""
    if not value:
        return None
    parts = str(value).strip().split(":")
    if len(parts) != 3:
        return None
    try:
        h, m, s = (int(p) for p in parts)
    except ValueError:
        return None
    return (h * 3600 + m * 60 + s) or None


class MavisApiExtractor:
    """End-to-end runner for one Mavis export task."""

    # api_export_main.py runs tasks concurrently (ThreadPoolExecutor); serialise
    # the child-config Status writes to the shared Delta table. AuditLogger
    # already serialises its own writes with an internal lock.
    _status_lock = threading.Lock()

    def __init__(
        self,
        spark,
        config_mgr: ConfigManager,
        audit_table: str,
        environment: str = "dev",
        department_id: int = 0,
        api_config: MavisApiConfig | None = None,
        connector: MavisApiExportConnector | None = None,
        dbutils=None,
        silver_notebook_path: str | None = None,
        silver_notebook_timeout: int | None = None,
    ):
        self.spark = spark
        self.config_mgr = config_mgr
        self.connector = connector or MavisApiExportConnector(spark, api_config)
        self.audit = AuditLogger(
            spark, audit_table=audit_table, department_id=department_id
        )
        self.logger = get_logger(environment=environment)
        self._dbutils = dbutils
        self.silver_notebook_path = silver_notebook_path
        self.silver_notebook_timeout = silver_notebook_timeout

    # ── retry policy ────────────────────────────────────────────────────────

    def _retry_plan(self, source_sys) -> dict:
        """
        One ``source_sys.retry_count`` / ``retry_interval`` / ``query_timeout``
        -> per-step retry counts (each floored at the ADF value), a uniform
        interval, and the wall-clock timeout budget (seconds).
        """
        base = int(getattr(source_sys, "retry_count", 0) or 0)
        interval = int(getattr(source_sys, "retry_interval", 0) or 30)
        query_timeout = (
            _hms_to_seconds(getattr(source_sys, "query_timeout", None))
            or _DEFAULT_QUERY_TIMEOUT
        )
        api = self.connector.api
        return {
            "interval": interval,
            "query_timeout": query_timeout,
            "start_export": max(base, api.start_export_retries),
            "poll_until_ready": max(base, api.poll_retries),
            "get_download_url": max(base, api.download_url_retries),
            "download_and_extract_to_s3": max(base, api.copy_retries),
            "silver": max(base, api.silver_retries),
        }

    # ── main entry ─────────────────────────────────────────────────────────

    def run(
        self,
        task: MavisIngestionTaskConfig,
        source_sys,
        pipeline_name: str,
        job_context: dict | None = None,
        config_master_id: int | None = None,
    ) -> list[str]:
        """
        Run every step for ``task``; return ``[s3_zip_path, s3_csv_path]``. On
        any failure the config row is flagged Status='Failed', the audit row is
        closed FAILED, and the exception is re-raised for the caller's summary.
        """
        fqn = task.child_table_fqn
        ctx = dict(job_context or {})

        # Fixed run instant — ADF triggerTime == sink_batch_started_date (UTC).
        trigger_time = _parse_bsd(task.sink_batch_started_date)
        trigger_time_ist = trigger_time + _IST
        ts = trigger_time_ist.strftime("%Y_%m_%d_%H_%M_%S")
        folder = (task.raw_folder_path or "").strip("/")

        # ADF-manner audit derivations (concat + IST formatDateTime).
        derived = dict(
            delta_layer="Raw",
            frequency="Daily",
            trigger_time=trigger_time_ist,
            business_date=trigger_time.date(),
            source_schema=(
                f"mavis/{folder}/zip/"
                f"{trigger_time_ist:%Y}/{trigger_time_ist:%b}/{trigger_time_ist:%d}"
            ),
            source_table=f"{task.raw_file_name}_{ts}.zip",
            target_schema=(
                f"mavis/{folder}/unzip/"
                f"{trigger_time_ist:%Y}/{trigger_time_ist:%b}/{trigger_time_ist:%d}"
            ),
            target_table=f"{task.raw_file_name}_{ts}.csv",
        )

        audit_run = self.audit.start_run(
            task=task,
            source_sys=source_sys,
            job_context=ctx,
            pipeline_name=pipeline_name,
            config_master_id=config_master_id,
            **derived,
        )

        plan = self._retry_plan(source_sys)
        current_step = ["start_export"]

        def _step(name, fn):
            current_step[0] = name
            return retry_on_failure(
                fn,
                max_retries=plan[name],
                retry_interval=plan["interval"],
                logger=self.logger,
                description=f"{name} config_id={task.config_id}",
            )

        try:
            self._update_status(fqn, task.config_id, AUDIT_STATUS_INPROGRESS)

            request_id = _step(
                "start_export", lambda: self.connector.start_export(task)
            )
            _step(
                "poll_until_ready",
                lambda: self.connector.poll_until_ready(
                    task, request_id, query_timeout=plan["query_timeout"]
                ),
            )
            download_url = _step(
                "get_download_url",
                lambda: self.connector.get_download_url(task, request_id),
            )
            paths = _step(
                "download_and_extract_to_s3",
                lambda: self.connector.download_and_extract_to_s3(
                    task,
                    download_url,
                    trigger_time=trigger_time,
                    query_timeout=plan["query_timeout"],
                ),
            )

            self._update_status(fqn, task.config_id, AUDIT_STATUS_SUCCESS)
            self.audit.complete_run(
                audit_run,
                AUDIT_STATUS_SUCCESS,
                rows_read=1,
                rows_copied=1,
                data_read_bytes=paths.get("data_read_bytes", 0),
                data_written_bytes=paths.get("data_written_bytes", 0),
                throughput_mb_per_sec=paths.get("throughput_mb_per_sec"),
                copy_duration_sec=paths.get("copy_duration_sec"),
            )
            self.logger.info(
                f"[MavisApiExtractor] config_id={task.config_id} SUCCESS "
                f"(ZIP: {paths['s3_zip_path']}, CSV: {paths['s3_csv_path']})"
            )

            self._trigger_silver(task, trigger_time_ist, plan)

            return [paths["s3_zip_path"], paths["s3_csv_path"]]
        except Exception as exc:
            step = current_step[0]
            self.logger.exception(
                f"[MavisApiExtractor] config_id={task.config_id} FAILED at "
                f"'{step}': {exc}"
            )
            try:
                self._update_status(fqn, task.config_id, AUDIT_STATUS_FAILED)
            except Exception as update_exc:  # best effort
                self.logger.error(
                    f"[MavisApiExtractor] config_id={task.config_id} could not "
                    f"write Failed status: {update_exc}"
                )
            try:
                self.audit.fail_run(
                    audit_run,
                    error_code=f"MAVIS_{step.upper()}",
                    error_message=str(exc),
                )
            except Exception as audit_exc:  # best effort
                self.logger.error(
                    f"[MavisApiExtractor] config_id={task.config_id} could not "
                    f"write FAILED audit row: {audit_exc}"
                )
            raise

    # ── parallel fan-out ──────────────────────────────────────────────────

    def run_all(
        self,
        tasks,
        source_sys,
        pipeline_name: str,
        job_context: dict | None = None,
        config_master_id: int | None = None,
    ) -> dict[int, tuple[str, object]]:
        """
        Run ``run()`` for every task in parallel — one worker per distinct
        ``config_id`` (each Mavis export is independent). Never raises for a
        single-task failure; the caller drives the summary + notebook exit.

        Returns ``{config_id: (status, detail)}`` — ``("SUCCESS", [zip, csv])``
        or ``("FAILED", "<error string>")``.
        """
        if not tasks:
            return {}

        def _one(task):
            self.logger.info(
                f"[api_export] config_id={task.config_id} START "
                f"({task.source_object_name})"
            )
            try:
                paths = self.run(
                    task, source_sys, pipeline_name, job_context, config_master_id
                )
                self.logger.info(f"[api_export] config_id={task.config_id} SUCCESS")
                return task.config_id, "SUCCESS", paths
            except Exception as exc:  # run() already wrote Status=FAILED + audit
                self.logger.error(
                    f"[api_export] config_id={task.config_id} FAILED: {exc}"
                )
                return task.config_id, "FAILED", str(exc)

        max_workers = len({t.config_id for t in tasks})
        self.logger.info(
            f"[api_export] running {len(tasks)} export task(s) "
            f"(max_workers={max_workers})"
        )

        results: dict[int, tuple[str, object]] = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_cid = {
                executor.submit(_one, task): task.config_id for task in tasks
            }
            for future in as_completed(future_to_cid):
                cid = future_to_cid[future]
                try:
                    config_id, status, detail = future.result()
                    results[config_id] = (status, detail)
                except Exception as exc:  # defensive — _one shouldn't raise
                    results[cid] = ("FAILED", str(exc))
                    self.logger.error(
                        f"[api_export] config_id={cid} FAILED (worker error): {exc}"
                    )
        return results

    # ── silver ─────────────────────────────────────────────────────────────

    def _trigger_silver(self, task, trigger_time_ist, plan) -> None:
        """
        Run the Mavis silver notebook for this task via ``dbutils.notebook.run``.
        Retried ``plan['silver']`` times. A Silver failure is logged and
        swallowed — it never flips the export to FAILED (matches
        ``IngestionOrchestrator._trigger_silver``).
        """
        if not self.silver_notebook_path:
            self.logger.info(
                f"[MavisApiExtractor] config_id={task.config_id} — silver skipped "
                f"(no notebook path)"
            )
            return

        dbutils = self._dbutils or DBUtils(self.spark)
        timeout = self.silver_notebook_timeout or plan["query_timeout"]
        params = {
            "config_id": str(task.config_id),
            "load_type": task.load_type or "",
            "raw_sa_name": task.s3_raw_landing_path or "",
            "containerName": task.raw_container_name or "",
            "raw_folder_path": task.raw_folder_path or "",
            "raw_file_name": task.raw_file_name or "",
            # IST — the silver notebook subtracts 5:30 to get UTC.
            "triggerTime": trigger_time_ist.strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "sink_schema_name": task.target_schema or "",
            "sink_table_name": task.target_table or "",
            "deltaColumn": task.delta_column or "",
            "key_column": task.key_column or "",
        }
        try:
            exit_value = retry_on_failure(
                lambda: dbutils.notebook.run(
                    self.silver_notebook_path, timeout, params
                ),
                max_retries=plan["silver"],
                retry_interval=plan["interval"],
                logger=self.logger,
                description=f"silver config_id={task.config_id}",
            )
            self.logger.info(
                f"[MavisApiExtractor] config_id={task.config_id} silver SUCCESS "
                f"-> {exit_value}"
            )
        except Exception as exc:
            self.logger.exception(
                f"[MavisApiExtractor] config_id={task.config_id} silver FAILED: {exc}"
            )

    # ── helpers ────────────────────────────────────────────────────────────

    def _update_status(self, child_table_fqn, config_id, status) -> None:
        with self._status_lock:
            self.config_mgr.update_status(child_table_fqn, config_id, status)

            
