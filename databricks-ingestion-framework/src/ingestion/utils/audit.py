"""
Writes execution records to data_pipeline_execution_master Delta table.

Each pipeline run produces:
  - An INSERT at the start  (status = RUNNING)
  - An UPDATE at the end    (status = SUCCESS | FAILED)

Watermark tracking for INCREMENTAL loads is not stored here; the orchestrator
derives the last watermark directly from the target Delta table (max of
incremental_column), which is more robust and requires no schema changes here.

Note on department_id
---------------------
The pre-existing table requires department_id INT NOT NULL.  Because department
is not a pipeline-level concept in this framework, the constant
DEFAULT_DEPARTMENT_ID (0) is written for every run.  Override it by passing
department_id to AuditLogger.__init__.
"""
import threading
import uuid
from datetime import datetime, date
from typing import Optional
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DateType,
    TimestampType, DecimalType, LongType, DoubleType,
)
from .config_manager import AUDIT_TABLE

DEFAULT_DEPARTMENT_ID = 0


class AuditLogger:

    def __init__(
        self,
        spark,
        audit_table: str = AUDIT_TABLE,
        department_id: int = DEFAULT_DEPARTMENT_ID,
    ):
        self.spark         = spark
        self.table         = audit_table
        self.department_id = department_id
        # Serialise concurrent audit writes from parallel ingestion threads.
        # Bronze writes (to different tables) remain fully parallel — only the
        # single shared audit table needs this guard.
        self._write_lock   = threading.Lock()

    # ── Run lifecycle methods ─────────────────────────────────────────────────

    def start_run(
        self,
        ingest_obj,
        source_sys,
        pipeline_name: str,
        delta_layer: str = "BRONZE",
        trigger_type: Optional[str] = "MANUAL",
        trigger_id: Optional[str] = None,
        business_date: Optional[date] = None,
        frequency: Optional[str] = None,
    ) -> str:
        """
        Inserts a RUNNING row into data_pipeline_execution_master.
        Returns the run_id (UUID string) that must be passed to complete_run().

        Accepts ingest_obj (IngestionObjectConfig) and source_sys (SourceSystemConfig)
        directly — no manual field mapping required in the caller.
        """
        run_id     = str(uuid.uuid4())
        now        = datetime.utcnow()
        biz_date   = business_date if business_date is not None else now.date()
        obj_id_int = int(ingest_obj.ingestion_object_id)


        schema = StructType([
            StructField("config_master_id",       IntegerType(),        True),
            StructField("table_id",               IntegerType(),        True),
            StructField("delta_layer",            StringType(),         True),
            StructField("source_name",            StringType(),         True),
            StructField("pipeline_name",          StringType(),         True),
            StructField("load_type",              StringType(),         True),
            StructField("frequency",              StringType(),         True),
            StructField("business_date",          DateType(),           True),
            StructField("run_id",                 StringType(),         True),
            StructField("trigger_type",           StringType(),         True),
            StructField("trigger_id",             StringType(),         True),
            StructField("trigger_name",           StringType(),         True),
            StructField("trigger_time",           TimestampType(),      True),
            StructField("end_time",               TimestampType(),      True),
            StructField("execution_duration_sec", DecimalType(10, 2),   True),
            StructField("source_schema",          StringType(),         True),
            StructField("source_table",           StringType(),         True),
            StructField("target_schema",          StringType(),         True),
            StructField("target_table",           StringType(),         True),
            StructField("rows_read",              LongType(),           True),
            StructField("rows_copied",            LongType(),           True),
            StructField("rows_deleted",           LongType(),           True),
            StructField("total_cost",             DoubleType(),         True),   
            StructField("department_id",          IntegerType(),        True),
            StructField("status",                 StringType(),         True),
        ])

        row = [(
            obj_id_int,                      # config_master_id
            obj_id_int,                      # table_id
            delta_layer,
            source_sys.source_name,
            pipeline_name,
            ingest_obj.load_type,
            frequency,
            biz_date,
            run_id,
            trigger_type,
            trigger_id,
            None,
            now,                             # trigger_time
            None,                            # end_time (set on complete)
            None,                            # execution_duration_sec (set on complete)
            ingest_obj.source_schema,
            ingest_obj.source_object_name,
            ingest_obj.target_schema,
            ingest_obj.target_table,
            0,                               # rows_read
            0,                               # rows_copied
            0,                               # rows_deleted
            0.0,                            # total_cost
            int(self.department_id),
            "RUNNING",
        )]

        df = self.spark.createDataFrame(row, schema=schema)
        with self._write_lock:
            df.writeTo(self.table).using("delta").append()
        return run_id

    def complete_run(
        self,
        run_id: str,
        status: str,
        rows_read: int = 0,
        rows_copied: int = 0,
        rows_deleted: int = 0,
    ) -> None:
        """
        Updates the RUNNING row to SUCCESS or FAILED and stamps end metrics.
        """
        self.spark.sql(f"""
            UPDATE {self.table}
            SET end_time               = current_timestamp(),
                execution_duration_sec = CAST(
                    (unix_timestamp(current_timestamp()) - unix_timestamp(trigger_time))
                    AS DECIMAL(10, 2)),
                status       = '{status}',
                rows_read    = {int(rows_read)},
                rows_copied  = {int(rows_copied)},
                rows_deleted = {int(rows_deleted)}
            WHERE run_id = '{run_id}'
        """)

    def fail_run(self, run_id: str, error_message: str) -> None:
        """Marks a run FAILED (error_message is logged by orchestrator)."""
        self.complete_run(run_id=run_id, status="FAILED")

    # ── Query helpers ─────────────────────────────────────────────────────────

    def get_last_run_status(self, ingestion_object_id: int) -> Optional[str]:
        """
        Returns the status of the most recent run for the given
        ingestion_object_id, or None if no runs exist.
        """
        rows = (
            self.spark.table(self.table)
            .filter(f"table_id = {int(ingestion_object_id)}")
            .orderBy("trigger_time", ascending=False)
            .limit(1)
            .select("status")
            .collect()
        )
        return rows[0]["status"] if rows else None

    def get_pipeline_failures(self, pipeline_name: str, run_id_prefix: Optional[str] = None):
        """
        Returns all FAILED rows for a given pipeline_name in the audit table.
        Useful for the fan-out driver to report failures at the end of a run.
        """
        df = (
            self.spark.table(self.table)
            .filter(f"pipeline_name = '{pipeline_name}' AND status = 'FAILED'")
        )
        if run_id_prefix:
            df = df.filter(f"run_id LIKE '{run_id_prefix}%'")
        return df
