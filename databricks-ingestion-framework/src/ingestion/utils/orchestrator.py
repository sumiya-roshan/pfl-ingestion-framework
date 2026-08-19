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
from datetime import date
from typing import Optional

from .config_manager import (
    AUDIT_STATUS_FAILED,
    AUDIT_STATUS_SUCCESS,
    ConfigManager,
    IngestionTaskConfig,
    SourceSystemConfig,
)
from .watermark import resolve_watermark
from .audit import AuditLogger
from ..connectors.factory import get_connector
from .writers.s3_writer import S3RawWriter
from .writers.bronze_writer import BronzeWriter
from .logger import get_logger
from .secrets import SecretResolver
from .retry import retry_on_failure


class IngestionOrchestrator:
    """
    Orchestrates a single ingestion run end-to-end.

    Parameters
    ----------
    spark         : active SparkSession
    dbutils       : Databricks dbutils; None in unit tests
    audit_table   : FQN of data_pipeline_execution_master
    pipeline_name : fallback name when config table pipeline_name is NULL
    environment   : 'dev' | 'uat' | 'prod' — controls log level
    department_id : written to audit table department_id NOT NULL column
    """

    def __init__(
        self,
        spark,
        dbutils=None,
        audit_table: str   = "migration_x_catalog.pfl_x_schema.data_pipeline_execution_master",
        pipeline_name: str = "ingestion_framework",
        environment: str   = "dev",
        department_id: int = 0,
    ):
        self.spark         = spark
        self.pipeline_name = pipeline_name
        self.environment   = environment

        self.audit         = AuditLogger(spark, audit_table=audit_table, department_id=department_id)
        self.config_manager = ConfigManager(spark)
        self.secrets       = SecretResolver(dbutils)
        self.s3_writer     = S3RawWriter()
        self.bronze_writer = BronzeWriter(spark)
        self.logger        = get_logger(environment=environment)

    # ── Public API ────────────────────────────────────────────────────────────

    def run(
        self,
        source_sys: SourceSystemConfig,
        task: IngestionTaskConfig,
        config_master_id: Optional[int]    = None,
        landing_volume_path: Optional[str] = None,
        trigger_id: Optional[str]          = None,
        # trigger_type: Optional[str]        = None,
        business_date: Optional[date]      = None,
        job_context: Optional[dict]        = None,
    ) -> dict:
        """
        Execute a single ingestion object end-to-end.

        Never re-raises — catches all exceptions and returns a result dict.
        Callers should check result["status"] == "FAILED" and act accordingly.

        Parameters
        ----------
        source_sys          : Pre-fetched SourceSystemConfig
        task                : Pre-fetched IngestionTaskConfig
        config_master_id    : The config_master routing table ID (widget value from main.py);
                              written to the audit table's config_master_id column
        landing_volume_path : base S3/Volume path for raw landing write (widget value);
                              if None/empty, landing write is skipped
        trigger_id          : Databricks job run ID (for audit traceability)
        trigger_type        : 'SCHEDULED' | 'MANUAL' | 'EVENT'
        business_date       : override for business_date audit column (defaults to today UTC)

        Returns
        -------
        """
        from datetime import datetime
        run_start_time = datetime.utcnow()
        ingest_obj = task

        # pipeline_name and delta_layer come from config table, with fallbacks
        pipeline_name = ingest_obj.pipeline_name or self.pipeline_name
        delta_layer   = ingest_obj.effective_delta_layer   # property: config → fallback 'BRONZE'

        # Start audit: insert the INPROGRESS record before any ingestion work.
        audit_context = dict(job_context or {})
        # audit_context.setdefault("trigger_type", trigger_type)
        audit_context.setdefault("trigger_id", trigger_id)
        audit_run = self.audit.start_run(
            task=ingest_obj,
            source_sys=source_sys,
            job_context=audit_context,
            pipeline_name=pipeline_name,
            config_master_id=config_master_id,
            business_date=business_date,
        )
        run_id = audit_run["job_run_id"]

        try:
            watermark_start = resolve_watermark(self.spark, self.logger, ingest_obj)
            self.logger.info(
                f"[{run_id}] START config_id={ingest_obj.config_id} "
                f"source='{source_sys.source_name}' object='{ingest_obj.source_object_name}' "
                f"load_type={ingest_obj.load_type} delta_layer={delta_layer} "
                f"watermark_start={watermark_start}"
            )

            connector = get_connector(self.spark, source_sys, ingest_obj, self.secrets)
            df, watermark_end = retry_on_failure(
                lambda: connector.extract(watermark_start),
                max_retries    = int(source_sys.retry_count or 0),
                retry_interval = int(source_sys.retry_interval or 0),
                logger         = self.logger,
                description    = f"[{run_id}] extract config_id={ingest_obj.config_id} object='{ingest_obj.source_object_name}'",
            )
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
                    f"[{run_id}] Landing write → {landing_path} ({rows_read} rows, format={fmt})"
                )

            import time
            write_start_time = time.time()

            # ── Bronze Delta write ─────────────────────────────────────────────
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
            rows_copied = rows_read
            copy_duration_sec = round(time.time() - write_start_time, 2)

            self.logger.info(
                f"[{run_id}] Bronze write → {target_table} ({rows_read} rows, mode={ingest_obj.write_mode})"
            )

            # Retrieve metrics from Delta table history
            data_read_bytes = 0
            data_written_bytes = 0
            try:
                history_df = self.spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1")
                history_row = history_df.select("operationMetrics").collect()[0]
                metrics = history_row["operationMetrics"] if history_row["operationMetrics"] else {}
                
                # Extract bytes written
                bytes_written = (
                    metrics.get("numOutputBytes") or 
                    metrics.get("numTargetBytesAdded") or 
                    metrics.get("numTargetBytesWritten")
                )
                if bytes_written:
                    data_written_bytes = int(bytes_written)
                
                # Extract bytes read
                bytes_read = (
                    metrics.get("numTargetBytesRead") or 
                    metrics.get("numSourceBytesRead") or 
                    metrics.get("numReadBytes")
                )
                if bytes_read:
                    data_read_bytes = int(bytes_read)
            except Exception as e:
                self.logger.warning(f"[{run_id}] Could not retrieve Delta execution metrics: {e}")

            # Calculate throughput (MB/sec)
            throughput_mb_per_sec = None
            if copy_duration_sec > 0:
                throughput_mb_per_sec = round((data_written_bytes / (1024.0 * 1024.0)) / copy_duration_sec, 2)

            # End audit: update the INPROGRESS record with SUCCESS and end time.
            self.audit.complete_run(
                audit_run             = audit_run,
                status                = AUDIT_STATUS_SUCCESS,
                rows_read             = rows_read,
                rows_copied           = rows_copied,
                rows_deleted          = rows_deleted,
                data_read_bytes       = data_read_bytes,
                data_written_bytes    = data_written_bytes,
                throughput_mb_per_sec = throughput_mb_per_sec,
                copy_duration_sec     = copy_duration_sec,
            )
# trigger_silver [TO DO]
            if config_master_id is not None:
                try:
                    self.config_manager.update_sink_metadata(
                        config_master_id=config_master_id,
                        ingest_obj=ingest_obj,
                        sink_batch_started_date=run_start_time,
                        rownum=rows_copied,
                        data_size=data_written_bytes,
                    )
                    self.logger.info(f"[{run_id}] Sink metadata updated for config_id={ingest_obj.config_id}")
                except Exception as sink_exc:
                    self.logger.warning(f"[{run_id}] Could not update sink metadata: {sink_exc}")

            self.logger.info(f"[{run_id}] SUCCESS — {rows_read} records processed.")
            return {
                "config_id": ingest_obj.config_id,
                "run_id":   run_id,
                "status":   AUDIT_STATUS_SUCCESS,
                "rows_read": rows_read,
                "error_code": None,
                "error":    None,
            }

        except Exception as exc:
            # Never re-raise — record FAILED in audit and return error dict.
            # The fan-out driver (run_all_sources.py) collects results and
            # raises a summary exception if any table failed.
            error_msg = str(exc)
            # if hasattr(exc, "getErrorClass"):
            #     error_code_dtl = exc.getErrorClass()
            error_code  = type(exc).__name__
            self.logger.error(
                f"[{run_id}] FAILED config_id={ingest_obj.config_id}: {error_msg}",
                exc_info=True,
            )
            try:
                self.audit.fail_run(audit_run=audit_run, error_code = error_code,error_message=error_msg)
            except Exception as audit_exc:
                self.logger.error(f"[{run_id}] Could not write FAILED status to audit: {audit_exc}")
            return {
                "config_id": ingest_obj.config_id,
                "run_id":   run_id,
                "status":   AUDIT_STATUS_FAILED,
                "rows_read": 0,
                "error_code": error_code,
                "error":    error_msg,
            }
