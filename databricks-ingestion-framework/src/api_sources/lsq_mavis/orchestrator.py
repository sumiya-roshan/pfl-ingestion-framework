"""
MavisOrchestrator — per-table run lifecycle for LSQ Mavis ingestion.

Lives in api_sources/lsq_mavis/ alongside config.py, get_tasks.py, and main.py.
Uses absolute imports since it is outside the ingestion package.

Replaces the per-pipeline-instance logic in PL_LSQ_Mavis_Raw_To_Silver:
  - MavisApiConnector   (ingestion/connectors/mavis_connector.py)
  - AuditLogger         (ingestion/utils/audit.py)
  - SilverProcessor     (silver/silver_processor.py)
  - DependencyLogger    (ingestion/utils/dependency_logger.py)
  - GraphMailNotifier   (ingestion/utils/email_notifier.py)

Per-table flow
──────────────
  1. dep.start_table()             → insert dependency row
  2. audit.start_run()             → insert INPROGRESS row
  3. connector.extract()           → trigger API export, poll, download ZIP, unzip CSV, DataFrame
  4. dep.mark_source_to_raw_end()  → stamp raw end time
  5. silver_processor.trigger()    → run Silver notebook (inline, coupled)
  6. dep.mark_dependency_resolved()
  7. audit.complete_run()          → SUCCESS
  8. notifier.send_email()         → success / failure email

Never re-raises — catches all exceptions and returns a result dict.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from silver.silver_processor import SilverProcessor

from ingestion.connectors.mavis_connector import MavisApiConnector
from ingestion.utils.audit import AuditLogger
from ingestion.utils.config_manager import (
    AUDIT_STATUS_FAILED,
    AUDIT_STATUS_SUCCESS,
)
from ingestion.utils.dependency_logger import DependencyLogger
from ingestion.utils.email_notifier import GraphMailNotifier
from ingestion.utils.logger import get_logger
from api_sources.lsq_mavis.config import MavisTableConfig


class MavisOrchestrator:
    """
    Orchestrates a single Mavis table ingestion end-to-end.

    Parameters
    ----------
    spark                  : active SparkSession
    dbutils                : Databricks dbutils
    audit_table            : FQN of tb_audit_log (shared with all other pipelines)
    dependency_table       : FQN of dependency_master_config
    pipeline_name          : job-level pipeline name written to audit rows
    environment            : 'dev' | 'uat' | 'prod'
    raw_sa_name            : ADLS Gen2 storage account name (e.g. 'pflrawsa')
    silver_notebook_path   : workspace path to the Mavis Silver notebook
    silver_notebook_timeout: max seconds to wait for the Silver notebook
    """

    def __init__(
        self,
        spark,
        dbutils,
        audit_table: str,
        dependency_table: str,
        pipeline_name: str,
        environment: str = "prod",
        s3_bucket_name: str = "",
        silver_notebook_path: str | None = None,
        silver_notebook_timeout: int = 3600,
    ):
        self.spark         = spark
        self.dbutils       = dbutils
        self.pipeline_name = pipeline_name
        self.environment   = environment
        self.s3_bucket_name = s3_bucket_name

        self.audit      = AuditLogger(spark, audit_table=audit_table)
        self.dependency = DependencyLogger(spark, dependency_table=dependency_table)
        self.notifier   = GraphMailNotifier(dbutils=dbutils, logger=get_logger(environment))
        self.logger     = get_logger(environment)

        self.silver_processor = (
            SilverProcessor(dbutils, silver_notebook_path, silver_notebook_timeout)
            if silver_notebook_path
            else None
        )

    # ── Public API ────────────────────────────────────────────────────────────

    def run(
        self,
        table: MavisTableConfig,
        trigger_time_utc: datetime,
        job_context: dict,
        sink_batch_started_date: datetime,
        config_table_fqn: str,
    ) -> dict:
        """
        Execute one Mavis table ingestion end-to-end. Never re-raises.

        Parameters
        ----------
        table                   : MavisTableConfig for this config row
        trigger_time_utc        : batch trigger time in UTC
        job_context             : dict with job_run_id, job_id, trigger_type, etc.
        sink_batch_started_date : batch start datetime (for audit business_date)
        config_table_fqn        : FQN of tb_mavis_db_ingestion_config (for status updates)

        Returns
        -------
        dict with keys: config_id, status, rows_read, error, silver_result
        """
        run_start     = datetime.now(timezone.utc)
        pipeline_name = table.pipeline_name or self.pipeline_name
        silver_result = None

        # ── 1. Dependency row ──────────────────────────────────────────────────
        dep_run = self.dependency.start_table(
            config_master_id    = table.config_master_id,
            source_system_id    = table.config_id,   # no config_source_system row for Mavis
            config_id           = table.config_id,
            table_name          = table.sink_table_name,
            pipeline_name       = pipeline_name,
            job_run_id          = job_context.get("job_run_id"),
            business_date       = sink_batch_started_date.date(),
            pipeline_start_time = job_context.get("pipeline_start_time") or run_start,
        )

        # ── 2. Audit: INPROGRESS ───────────────────────────────────────────────
        audit_run = self.audit.start_run(
            task             = _AuditTaskAdapter(table),
            source_sys       = _AuditSourceAdapter(table),
            job_context      = job_context,
            pipeline_name    = pipeline_name,
            config_master_id = table.config_master_id,
            business_date    = sink_batch_started_date.date(),
        )
        run_id = audit_run["job_run_id"]

        try:
            # ── 3. Extract: API → ZIP → CSV → DataFrame ────────────────────────
            self.logger.info(
                f"[Mavis] START config_id={table.config_id} "
                f"table={table.sink_table_name} load_type={table.load_type}"
            )
            connector = MavisApiConnector(
                spark            = self.spark,
                dbutils          = self.dbutils,
                table            = table,
                trigger_time_utc = trigger_time_utc,
                s3_bucket_name   = self.s3_bucket_name,
            )

            extract_start = time.time()
            df, zip_s3, csv_s3 = connector.extract()
            rows_read         = df.count()
            copy_duration_sec = round(time.time() - extract_start, 2)

            self.logger.info(
                f"[Mavis] config_id={table.config_id} — extracted {rows_read:,} rows"
            )

            # ── 4. Stamp raw end time ──────────────────────────────────────────
            self.dependency.mark_source_to_raw_end(dep_run)
            self._update_config_status(config_table_fqn, table.config_id, "In Progress")

            # ── 5. Silver (inline, coupled) ────────────────────────────────────
            if self.silver_processor:
                self.dependency.mark_raw_to_silver_start(dep_run)
                silver_result = self._trigger_silver(table, csv_s3)
                self.dependency.mark_raw_to_silver_end(dep_run)

                if silver_result and silver_result.get("status") == "FAILED":
                    self._update_config_status(config_table_fqn, table.config_id, "Failed")
                    self.notifier.send_email(
                        subject    = f"[FAILURE] SILVER — LSQ_Mavis.{table.sink_table_name} (config_id={table.config_id})",
                        body       = (
                            f"Stage failed: SILVER\n"
                            f"Source: LSQ_Mavis\n"
                            f"Table: {table.sink_table_name}\n"
                            f"Config ID: {table.config_id}\n"
                            f"Run ID: {run_id}\n\n"
                            f"Error:\n{silver_result.get('error') or 'Unknown Silver failure'}"
                        ),
                        recipients = table.recipient_list,
                        config_id  = table.config_id,
                    )
                else:
                    self.dependency.mark_dependency_resolved(dep_run)
                    self._stamp_silver_last_sink_date(config_table_fqn, table.config_id)

            # ── 6. Audit: SUCCESS ──────────────────────────────────────────────
            self.audit.complete_run(
                audit_run         = audit_run,
                status            = AUDIT_STATUS_SUCCESS,
                rows_read         = rows_read,
                rows_copied       = rows_read,
                copy_duration_sec = copy_duration_sec,
            )
            self._update_config_status(config_table_fqn, table.config_id, "Succeeded")

            duration = round(time.time() - run_start.timestamp(), 2)
            self.logger.info(
                f"[Mavis] SUCCESS config_id={table.config_id} "
                f"table={table.sink_table_name} rows={rows_read} duration={duration}s"
            )

            # ── 7. Success email ───────────────────────────────────────────────
            self.notifier.send_email(
                subject    = f"[SUCCESS] LSQ_Mavis.{table.sink_table_name} (config_id={table.config_id})",
                body       = (
                    f"Source: LSQ_Mavis\n"
                    f"Table: {table.sink_table_name} ({table.table_description or ''})\n"
                    f"Config ID: {table.config_id}\n"
                    f"Run ID: {run_id}\n"
                    f"Target: {table.full_target_table}\n"
                    f"Rows read: {rows_read:,}\n"
                ),
                recipients = table.recipient_list,
                config_id  = table.config_id,
            )

            return {
                "config_id":     table.config_id,
                "status":        AUDIT_STATUS_SUCCESS,
                "rows_read":     rows_read,
                "error":         None,
                "silver_result": silver_result,
            }

        except Exception as exc:
            error_msg  = str(exc)
            error_code = type(exc).__name__
            self.logger.exception(
                f"[Mavis] FAILED config_id={table.config_id} "
                f"table={table.sink_table_name}: {error_msg}"
            )
            try:
                self.audit.fail_run(
                    audit_run     = audit_run,
                    error_code    = error_code,
                    error_message = error_msg,
                )
            except Exception as audit_exc:
                self.logger.error(
                    f"[Mavis] Could not write FAILED audit row for "
                    f"config_id={table.config_id}: {audit_exc}"
                )
            self._update_config_status(config_table_fqn, table.config_id, "Failed")
            self.notifier.send_email(
                subject    = f"[FAILURE] LSQ_Mavis.{table.sink_table_name} (config_id={table.config_id})",
                body       = (
                    f"Source: LSQ_Mavis\n"
                    f"Table: {table.sink_table_name}\n"
                    f"Config ID: {table.config_id}\n"
                    f"Run ID: {run_id}\n\n"
                    f"Error:\n{error_msg}"
                ),
                recipients = table.recipient_list,
                config_id  = table.config_id,
            )
            return {
                "config_id": table.config_id,
                "status":    AUDIT_STATUS_FAILED,
                "rows_read": 0,
                "error":     error_msg,
            }

    # ── Silver trigger ────────────────────────────────────────────────────────

    def _trigger_silver(self, table: MavisTableConfig, csv_s3: str) -> dict:
        """
        Trigger the Mavis Silver notebook inline (synchronously, on this thread).
        Never raises — caught and returned as a result dict.
        """
        target = table.full_target_table
        try:
            return self.silver_processor.trigger(
                config_id          = table.config_id,
                source_system_id   = table.config_id,
                landing_path       = csv_s3,
                file_format        = "csv",
                silver_catalog     = table.target_catalog,
                silver_schema      = table.target_schema,
                silver_table       = table.sink_table_name,
                source_schema      = table.raw_container_name,
                source_object_name = table.sink_table_name,
                load_type          = table.load_type,
                primary_key_cols   = table.key_column or "",
            )
        except Exception as exc:
            self.logger.exception(
                f"[Mavis SILVER] Trigger failed for config_id={table.config_id}"
            )
            return {
                "config_id":  table.config_id,
                "target":     target,
                "status":     "FAILED",
                "exit_value": None,
                "error":      str(exc),
            }

    # ── Config table helpers ──────────────────────────────────────────────────

    def _update_config_status(
        self, config_table_fqn: str, config_id: int, status: str
    ) -> None:
        """Best-effort status update on tb_mavis_db_ingestion_config."""
        try:
            self.spark.sql(f"""
                UPDATE {config_table_fqn}
                SET Status = '{status}'
                WHERE Config_ID = {config_id}
            """)
        except Exception as exc:
            self.logger.warning(
                f"[Mavis] Could not update Status='{status}' for config_id={config_id}: {exc}"
            )

    def _stamp_silver_last_sink_date(
        self, config_table_fqn: str, config_id: int
    ) -> None:
        """Best-effort stamp of Silver_Last_Sink_Date = current_timestamp()."""
        try:
            self.spark.sql(f"""
                UPDATE {config_table_fqn}
                SET Silver_Last_Sink_Date = current_timestamp()
                WHERE Config_ID = {config_id}
            """)
        except Exception as exc:
            self.logger.warning(
                f"[Mavis] Could not stamp Silver_Last_Sink_Date for config_id={config_id}: {exc}"
            )


# ── Thin adapter shims ────────────────────────────────────────────────────────
# AuditLogger and DependencyLogger were designed for IngestionTaskConfig /
# SourceSystemConfig. These lightweight adapters expose the same attribute
# surface so we don't need to duplicate or modify the shared utils.

class _AuditTaskAdapter:
    """Exposes MavisTableConfig as the duck-type surface AuditLogger needs."""
    def __init__(self, table: MavisTableConfig):       self._t = table
    @property
    def config_id(self):                               return self._t.config_id
    @property
    def effective_delta_layer(self):                   return "SILVER"
    @property
    def load_type(self):                               return self._t.load_type
    @property
    def source_schema(self):                           return self._t.raw_container_name
    @property
    def source_object_name(self):                      return self._t.sink_table_name
    @property
    def target_schema(self):                           return self._t.target_schema
    @property
    def target_table(self):                            return self._t.sink_table_name
    @property
    def frequency(self):                               return "Daily"


class _AuditSourceAdapter:
    """Exposes MavisTableConfig as the duck-type surface AuditLogger needs for source_sys."""
    def __init__(self, table: MavisTableConfig):       self._t = table
    @property
    def source_name(self):                             return self._t.source_name
