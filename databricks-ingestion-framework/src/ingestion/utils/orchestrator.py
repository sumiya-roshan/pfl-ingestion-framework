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
        self.secrets       = SecretResolver(dbutils)
        self.s3_writer     = S3RawWriter()
        self.bronze_writer = BronzeWriter(spark)
        self.logger        = get_logger(environment=environment)

    # ── Public API ────────────────────────────────────────────────────────────

    def run(
        self,
        source_sys: SourceSystemConfig,
        task: IngestionTaskConfig,
        landing_volume_path: Optional[str] = None,
        trigger_id: Optional[str]          = None,
        trigger_type: str                  = "MANUAL",
        business_date: Optional[date]      = None,
    ) -> dict:
        """
        Execute a single ingestion object end-to-end.

        Never re-raises — catches all exceptions and returns a result dict.
        Callers should check result["status"] == "FAILED" and act accordingly.

        Parameters
        ----------
        source_sys          : Pre-fetched SourceSystemConfig
        task                : Pre-fetched IngestionTaskConfig
        landing_volume_path : base S3/Volume path for raw landing write (widget value);
                              if None/empty, landing write is skipped
        trigger_id          : Databricks job run ID (for audit traceability)
        trigger_type        : 'SCHEDULED' | 'MANUAL' | 'EVENT'
        business_date       : override for business_date audit column (defaults to today UTC)

        Returns
        -------
        dict with keys: config_id, run_id, status, rows_read, error
        """
        ingest_obj = task

        # pipeline_name and delta_layer come from config table, with fallbacks
        pipeline_name = ingest_obj.pipeline_name or self.pipeline_name
        delta_layer   = ingest_obj.effective_delta_layer   # property: config → fallback 'BRONZE'

        watermark_start = resolve_watermark(self.spark, self.logger, ingest_obj)

        run_id = self.audit.start_run(
            ingest_obj, source_sys, pipeline_name, delta_layer, trigger_type, trigger_id, business_date
        )

        self.logger.info(
            f"[{run_id}] START config_id={ingest_obj.config_id} "
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
                    f"[{run_id}] Landing write → {landing_path} ({rows_read} rows, format={fmt})"
                )

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
            self.logger.info(
                f"[{run_id}] Bronze write → {target_table} ({rows_read} rows, mode={ingest_obj.write_mode})"
            )

            self.audit.complete_run(
                run_id       = run_id,
                status       = "SUCCESS",
                rows_read    = rows_read,
                rows_copied  = rows_copied,
                rows_deleted = rows_deleted,
            )
            self.logger.info(f"[{run_id}] SUCCESS — {rows_read} records processed.")
            return {
                "config_id": ingest_obj.config_id,
                "run_id":   run_id,
                "status":   "SUCCESS",
                "rows_read": rows_read,
                "error":    None,
            }

        except Exception as exc:
            # Never re-raise — record FAILED in audit and return error dict.
            # The fan-out driver (run_all_sources.py) collects results and
            # raises a summary exception if any table failed.
            error_msg = str(exc)
            self.logger.error(
                f"[{run_id}] FAILED config_id={ingest_obj.config_id}: {error_msg}",
                exc_info=True,
            )
            try:
                self.audit.fail_run(run_id=run_id, error_message=error_msg)
            except Exception as audit_exc:
                self.logger.error(f"[{run_id}] Could not write FAILED status to audit: {audit_exc}")
            return {
                "config_id": ingest_obj.config_id,
                "run_id":   run_id,
                "status":   "FAILED",
                "rows_read": 0,
                "error":    error_msg,
            }
