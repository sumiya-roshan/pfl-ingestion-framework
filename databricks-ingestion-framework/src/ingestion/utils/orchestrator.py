"""
Main driver: given an ingestion_object_id, resolves config, extracts data,
writes to landing path (if provided) and/or bronze Delta table, and records
the run in data_pipeline_execution_master.

Key behaviours
--------------
- delta_layer   : read from ingestion_config.delta_layer (not a widget)
- pipeline_name : read from ingestion_config.pipeline_name (auto-detected from
                  Databricks Job context in the calling notebook)
- landing_volume_path : passed as a parameter from the job widget — NOT from
                  config_source_system (removed)
- Fault tolerance: run() catches all exceptions, records FAILED in audit, and
                  returns a result dict — it never re-raises. The calling
                  notebook decides whether to raise after collecting all results.
"""
from datetime import datetime
from typing import Optional

from .config_manager import IngestionTaskConfig, SourceSystemConfig
from .watermark import resolve_watermark
from .audit import AuditLogger
from ..connectors.factory import get_connector
from .writers.s3_writer import S3RawWriter
from .writers.bronze_writer import BronzeWriter
from .logger import get_logger
from .secrets import SecretResolver


class IngestionOrchestrator:
    """
    Orchestrates a single ingestion run end-to-end.

    """

    def __init__(
        self,
        spark,
        dbutils,
        pipeline_name,
        audit_table: str            = "migration_x_catalog.pfl_x_schema.tb_audit_log",
        environment = "dev",
        job_context =None
    ):
        self.spark         = spark
        self.pipeline_name = pipeline_name
        self.environment   = environment
        self.job_context = job_context or {}
        self.audit = AuditLogger( spark=spark,audit_table=audit_table,)
        self.secrets = SecretResolver(dbutils)
        self.s3_writer = S3RawWriter()
        self.bronze_writer = BronzeWriter(spark)
        self.logger        = get_logger(environment=environment)

    # ── Public API ────────────────────────────────────────────────────────────

    def run(
        self,
        source_sys: SourceSystemConfig,
        task: IngestionTaskConfig,
        landing_volume_path: Optional[str] = None,
        job_context=None,
        task_start_time=None,
    ):
        """
        Execute a single ingestion object end-to-end.

        Never re-raises — catches all exceptions and returns a result dict.
        Callers should check result["status"] == "FAILED" and act accordingly.

        Returns
        -------
        dict with keys: config_id, status, rows_read, error
        """
        ingest_obj = task
        ctx = job_context or self.job_context
        start_time = task_start_time or datetime.utcnow()

        # pipeline_name and delta_layer come from config table, with fallbacks
        pipeline_name = ingest_obj.pipeline_name or self.pipeline_name
        delta_layer   = ingest_obj.effective_delta_layer   # property: config → fallback 'BRONZE'

        watermark_start = resolve_watermark(self.spark, self.logger, ingest_obj)

        run_id = self.audit.start_run(
            ingest_obj, source_sys, pipeline_name, delta_layer, trigger_type, trigger_id, business_date
        )

        self.logger.info(
            f" START config_id={ingest_obj.config_id} "
            f"source='{source_sys.source_name}' object='{ingest_obj.source_object_name}' "
            f"load_type={ingest_obj.load_type} delta_layer={delta_layer} "
            f"watermark_start={watermark_start}"
        )

        try:
            connector = get_connector(self.spark, source_sys, ingest_obj, self.secrets)
            df, watermark_end = connector.extract(watermark_start)
            rows_read = df.count()

            rows_copied  = 0
            rows_deleted = 0

            # ── Landing / raw write (optional) ────────────────────────────────
            if landing_volume_path:
                fmt = ingest_obj.file_format or "parquet"
                landing_path = self.s3_writer.write(
                    df,
                    landing_volume_path = landing_volume_path,
                    source_name         = source_sys.source_name,
                    source_schema       = ingest_obj.source_schema,
                    source_object_name  = ingest_obj.source_object_name,
                    file_format         = fmt,
                )
                self.logger.info(
                    f" Landing write → {landing_path} ({rows_read} rows, format={fmt})"
                )

            # ── Bronze Delta write ─────────────────────────────────────────────
            bronze_start = datetime.utcnow()
            target_table = self.bronze_writer.write(
                df,
                catalog               = ingest_obj.target_catalog,
                schema                = ingest_obj.target_schema,
                table                 = ingest_obj.target_table,
                write_mode            = ingest_obj.write_mode,
                merge_keys            = ingest_obj.primary_key_list,
                schema_evolution_mode = ingest_obj.schema_evolution_mode,
                partition_column      = ingest_obj.partition_column,
            )
            copy_duration = (
                datetime.utcnow() - bronze_start
            ).total_seconds()

            rows_copied = rows_read
            # ---------------------------------------------------------
            # 5. SUCCESS audit
            # ---------------------------------------------------------

            self.audit.log_execution(
                source_sys=source_sys,
                job_context=ctx,
                pipeline_name=(self.pipeline_name
                ),
                start_time=start_time,
                end_time=datetime.utcnow(),
                rows_read=rows_read,
                rows_copied=rows_copied,
                copy_duration_sec=copy_duration,
                status="SUCCESS")
            
            self.logger.info(
                f" Bronze write → {target_table} ({rows_read} rows, mode={ingest_obj.write_mode})"
            )

            self.logger.info(f" SUCCESS — {rows_read} records processed.")


            return {
                "config_id": ingest_obj.config_id,
                "status":   "SUCCESS",
                "rows_read": rows_read,
                "rows_copied": rows_copied,
                "error":    None,
            }

        except Exception as exc:
            # ---------------------------------------------------------
            # FAILED audit
            # ---------------------------------------------------------

            self.audit.log_execution(
                source_sys=source_sys,
                job_context=ctx,
                pipeline_name=(self.pipeline_name
                ),
                start_time=start_time,
                end_time=datetime.utcnow(),
                status="FAILED",
                error_code=type(exc).__name__,
                error_message=str(exc),
            )
            self.logger.error(
                f" FAILED config_id={ingest_obj.config_id}: {exc}",
                exc_info=True,
            )
            try:
                self.audit.fail_run(error_message=exc)
            except Exception as audit_exc:
                self.logger.error(f" Could not write FAILED status to audit: {audit_exc}")
            return {
                "config_id": ingest_obj.config_id,
                "status":   "FAILED",
                "rows_read": 0,
                 "rows_copied": 0,
                "error": str(exc),
                "error_code": type(exc).__name__,
            }
