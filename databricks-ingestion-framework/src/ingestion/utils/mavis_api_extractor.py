"""
LSQ Mavis export orchestrator — the ADF "ForEach body" for one task.

Runs the export steps in order for a single ``MavisIngestionTaskConfig`` and
writes Status back to its config-table row via the ordinary ``ConfigManager``:

  1. Status = AUDIT_STATUS_INPROGRESS
  2. start_export            -> RequestId
  3. poll_until_ready
  4. get_download_url        -> FileURL
  5. download_and_extract_to_s3
  6. Status = AUDIT_STATUS_SUCCESS   (on failure: AUDIT_STATUS_FAILED, re-raised)

Status values reuse the shared AUDIT_STATUS_* vocabulary — no separate Mavis
audit status. The extracted files are loaded to their target table by the
normal downstream S3 ingestion config, not here.

Audit + logging are owned here (same shape as ``IngestionOrchestrator`` for the
connector path): the extractor builds its own ``AuditLogger`` / logger from the
audit table + environment, writes one INPROGRESS audit row per task at the
start, and closes it SUCCESS / FAILED at the end. ``src/main/main.py`` only
constructs the extractor and calls ``run(task, ...)`` per task.

    extractor = MavisApiExtractor(spark, config_mgr, audit_table=AUDIT_TABLE,
                                  environment=environment)
    for task in tasks:            # tasks are MavisIngestionTaskConfig
        extractor.run(task, source_sys, pipeline_name, job_context, config_master_id)
"""

from __future__ import annotations

import threading

from ..connectors.api_connector import MavisApiConfig, MavisApiExportConnector
from .audit import AuditLogger
from .config_manager import (
    AUDIT_STATUS_FAILED,
    AUDIT_STATUS_INPROGRESS,
    AUDIT_STATUS_SUCCESS,
    ConfigManager,
    MavisIngestionTaskConfig,
)
from .logger import get_logger


class MavisApiExtractor:
    """End-to-end runner for one Mavis export task."""

    # main.py runs tasks concurrently (one thread per config_id); serialise the
    # child-config Status writes to the shared Delta table. AuditLogger already
    # serialises its own writes with an internal lock.
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
    ):
        self.spark = spark
        self.config_mgr = config_mgr
        self.connector = connector or MavisApiExportConnector(spark, api_config)
        self.audit = AuditLogger(
            spark, audit_table=audit_table, department_id=department_id
        )
        self.logger = get_logger(environment=environment)

    def run(
        self,
        task: MavisIngestionTaskConfig,
        source_sys,
        pipeline_name: str,
        job_context: dict | None = None,
        config_master_id: int | None = None,
    ) -> list[str]:
        """
        Run every step for ``task``; return the extracted S3 file paths. On any
        failure the row is flagged Status='Failed', the audit row closed FAILED,
        and the exception re-raised.
        """
        fqn = task.child_table_fqn
        step = "start_export"

        audit_run = self.audit.start_run(
            task=task,
            source_sys=source_sys,
            job_context=dict(job_context or {}),
            pipeline_name=pipeline_name,
            config_master_id=config_master_id,
        )

        try:
            self._update_status(fqn, task.config_id, AUDIT_STATUS_INPROGRESS)

            request_id = self.connector.start_export(task)

            step = "poll_until_ready"
            self.connector.poll_until_ready(task, request_id)

            step = "get_download_url"
            download_url = self.connector.get_download_url(task, request_id)

            step = "download_and_extract_to_s3"
            paths = self.connector.download_and_extract_to_s3(task, download_url)

            self._update_status(fqn, task.config_id, AUDIT_STATUS_SUCCESS)
            self.audit.complete_run(audit_run, AUDIT_STATUS_SUCCESS)

            self.logger.info(
                f"[MavisApiExtractor] config_id={task.config_id} SUCCESS "
                f"(ZIP: {paths['s3_zip_path']}, CSV: {paths['s3_csv_path']})"
            )
            return [paths["s3_zip_path"], paths["s3_csv_path"]]
        except Exception as exc:
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

    def _update_status(self, child_table_fqn, config_id, status) -> None:
        with self._status_lock:
            self.config_mgr.update_status(child_table_fqn, config_id, status)
