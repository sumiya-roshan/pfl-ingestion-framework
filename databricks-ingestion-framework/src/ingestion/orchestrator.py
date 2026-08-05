"""
Main driver: given an ingestion_object_id, resolves config from the two
pre-existing config tables, extracts data via the appropriate connector,
writes to the landing path (if configured) and/or the bronze Delta table,
and records the run in data_pipeline_execution_master.

This is what notebooks/01_run_ingestion.py calls.

Watermark resolution for INCREMENTAL loads
------------------------------------------
Because data_pipeline_execution_master has no watermark columns, the last
watermark is derived by querying max(incremental_column) directly from the
target Delta table.  This approach:
  - requires no additional state table
  - is always consistent with the data that was actually written
  - works correctly after a manual backfill / table repair
"""
from datetime import date
from typing import Optional

from .config_manager import ConfigManager, IngestionObjectConfig, SourceSystemConfig
from .config_manager import SOURCE_SYSTEM_TABLE, INGESTION_CONFIG_TABLE, AUDIT_TABLE
from .audit import AuditLogger
from .connectors.factory import get_connector
from .writers.s3_writer import S3RawWriter
from .writers.bronze_writer import BronzeWriter
from .utils.logger import get_logger
from .utils.secrets import SecretResolver

logger = get_logger()


class IngestionOrchestrator:
    """
    Orchestrates a single ingestion run end-to-end.

    Parameters
    ----------
    spark                  : active SparkSession
    dbutils                : Databricks dbutils (for secret resolution); None in tests
    source_system_table    : FQN of config_source_system (default from config_manager)
    ingestion_config_table : FQN of ingestion_config (default from config_manager)
    audit_table            : FQN of data_pipeline_execution_master
    pipeline_name          : name logged to the audit table (defaults to 'ingestion_framework')
    delta_layer            : layer label logged to the audit table (default: 'BRONZE')
    department_id          : written to audit table department_id NOT NULL column (default: 0)
    """

    def __init__(
        self,
        spark,
        dbutils=None,
        source_system_table: str = SOURCE_SYSTEM_TABLE,
        ingestion_config_table: str = INGESTION_CONFIG_TABLE,
        audit_table: str = AUDIT_TABLE,
        pipeline_name: str = "ingestion_framework",
        delta_layer: str = "BRONZE",
        department_id: int = 0,
    ):
        self.spark         = spark
        self.pipeline_name = pipeline_name
        self.delta_layer   = delta_layer

        self.config_mgr    = ConfigManager(
            spark,
            source_system_table=source_system_table,
            ingestion_config_table=ingestion_config_table,
        )
        self.audit         = AuditLogger(spark, audit_table=audit_table, department_id=department_id)
        self.secrets       = SecretResolver(dbutils)
        self.s3_writer     = S3RawWriter()
        self.bronze_writer = BronzeWriter(spark)


    # ── Public API ────────────────────────────────────────────────────────────

    def run_by_source_type(
        self,
        source_type: str,
        job_run_id: Optional[str] = None,
        business_date: Optional[date] = None,
    ):

        # Fetch all ingestion objects for the source type
        ingestion_objects = self.config_mgr.get_ingestion_objects(source_type)

        if not ingestion_objects:
            raise Exception(f"No ingestion objects found for source type: {source_type}")

        results = []

        for obj in ingestion_objects:

            ingestion_object_id = obj.ingestion_object_id

            logger.info(
                f"Starting ingestion for object {ingestion_object_id}"
            )

            try:

                result = self.run(
                    ingestion_object_id=ingestion_object_id,
                    job_run_id=job_run_id,
                    business_date=business_date,
                )

                results.append(result)

            except Exception as e:

                logger.exception(
                    f"Failed ingestion_object_id={ingestion_object_id}"
                )

                results.append(
                    {
                        "ingestion_object_id": ingestion_object_id,
                        "status": "FAILED",
                        "error": str(e),
                    }
                )

        return results


    def run(
        self,
        ingestion_object_id: int,
        job_run_id: Optional[str] = None,
        business_date: Optional[date] = None,
    ) -> dict:
        """
        Execute a single ingestion object end-to-end.

        Parameters
        ----------
        ingestion_object_id : PK of the row in ingestion_config to execute
        job_run_id          : Databricks job run ID (for traceability in logs)
        business_date       : override for the business_date audit column
                              (defaults to today UTC)

        Returns
        -------
        dict with keys: run_id, status, rows_read
        """
        ingest_obj  = self.config_mgr.get_ingestion_object(ingestion_object_id)
        source_sys  = self.config_mgr.get_source_system(ingest_obj.source_system_id)

        watermark_start = self._resolve_watermark(ingest_obj)

        run_id = self.audit.start_run(
            ingestion_object_id = ingestion_object_id,
            source_name         = source_sys.source_name,
            pipeline_name       = self.pipeline_name,
            load_type           = ingest_obj.load_type,
            source_schema       = ingest_obj.source_schema,
            source_table        = ingest_obj.source_object_name,
            target_schema       = ingest_obj.target_schema,
            target_table        = ingest_obj.target_table,
            delta_layer         = self.delta_layer,
            trigger_id          = job_run_id,
            trigger_name        = job_run_id,
            business_date       = business_date,
        )

        logger.info(
            f"[{run_id}] START ingestion_object_id={ingestion_object_id} "
            f"source='{source_sys.source_name}' "
            f"object='{ingest_obj.source_object_name}' "
            f"load_type={ingest_obj.load_type} "
            f"watermark_start={watermark_start}"
        )

        try:
            connector = get_connector(self.spark, source_sys, ingest_obj, self.secrets)
            df, watermark_end = connector.extract(watermark_start)
            df.cache()
            rows_read = df.count()

            rows_copied  = 0
            rows_deleted = 0

            # ── Landing / raw write (optional) ────────────────────────────────
            if source_sys.landing_volume_path:
                fmt = ingest_obj.file_format or "parquet"
                landing_path = self.s3_writer.write(
                    df,
                    landing_volume_path = source_sys.landing_volume_path,
                    source_object_name  = ingest_obj.source_object_name,
                    file_format         = fmt,
                )
                logger.info(
                    f"[{run_id}] Landing write → {landing_path} "
                    f"({rows_read} rows, format={fmt})"
                )

            # ── Bronze Delta write ─────────────────────────────────────────────
            target_table = self.bronze_writer.write(
                df,
                catalog              = ingest_obj.target_catalog,
                schema               = ingest_obj.target_schema,
                table                = ingest_obj.target_table,
                write_mode           = ingest_obj.write_mode,
                merge_keys           = ingest_obj.primary_key_list,
                schema_evolution_mode= ingest_obj.schema_evolution_mode,
                partition_column     = ingest_obj.partition_column,
            )
            rows_copied = rows_read
            logger.info(
                f"[{run_id}] Bronze write → {target_table} "
                f"({rows_read} rows, mode={ingest_obj.write_mode})"
            )

            self.audit.complete_run(
                run_id       = run_id,
                status       = "SUCCESS",
                rows_read    = rows_read,
                rows_copied  = rows_copied,
                rows_deleted = rows_deleted,
            )
            logger.info(f"[{run_id}] SUCCESS — {rows_read} records processed.")
            df.unpersist()
            return {"run_id": run_id, "status": "SUCCESS", "rows_read": rows_read}

        except Exception as exc:
            logger.exception(f"[{run_id}] FAILED: {exc}")
            self.audit.fail_run(run_id=run_id, error_message=str(exc))
            raise

    # ── Watermark resolution ──────────────────────────────────────────────────

    def _resolve_watermark(self, ingest_obj: IngestionObjectConfig) -> Optional[str]:
        """
        For INCREMENTAL loads, derive the starting watermark by querying
        max(incremental_column) from the target Delta table.

        Returns None when:
          - load_type is FULL
          - no incremental_column is set
          - the target table does not yet exist (first run)
          - the target table is empty
        """
        if ingest_obj.load_type != "INCREMENTAL" or not ingest_obj.incremental_column:
            return None

        full_table = ingest_obj.full_target_table
        if not self.spark.catalog.tableExists(full_table):
            logger.info(
                f"Watermark: target table {full_table} does not exist yet — "
                f"using incremental_end_value='{ingest_obj.incremental_end_value}'"
            )
            return ingest_obj.incremental_end_value

        col = ingest_obj.incremental_column
        try:
            row = (
                self.spark.table(full_table)
                .selectExpr(f"MAX(`{col}`) AS max_val")
                .collect()
            )
            if row and row[0]["max_val"] is not None:
                wm = str(row[0]["max_val"])
                logger.info(
                    f"Watermark: resolved max({col})='{wm}' from {full_table}"
                )
                return wm
        except Exception as exc:
            logger.warning(
                f"Watermark: could not read max({col}) from {full_table}: {exc}. "
                f"Falling back to incremental_end_value='{ingest_obj.incremental_end_value}'"
            )
        return ingest_obj.incremental_end_value
