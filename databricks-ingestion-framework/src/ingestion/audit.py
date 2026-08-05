"""
Writes execution records to data_pipeline_execution_master Delta table.

Each pipeline run produces:
  - An INSERT at the start  (status = RUNNING)
  - An UPDATE at the end    (status = SUCCESS | FAILED)

Watermark tracking for INCREMENTAL loads is no longer stored here; the
orchestrator derives the last watermark directly from the target Delta table
(max of incremental_column), which is more robust and doesn't require schema
changes to the audit table.

Note on department_id
---------------------
The pre-existing table requires department_id INT NOT NULL.  Because department
is not a pipeline-level concept in this framework, the constant
DEFAULT_DEPARTMENT_ID (0) is written for every run.  Override it by passing
department_id to AuditLogger.__init__ if your organisation maps pipelines to
departments via a separate dimension.
"""
import uuid
from datetime import datetime, date
from typing import Optional

from .config_manager import AUDIT_TABLE

# department_id is NOT NULL in the pre-existing audit table.
# Set to 0 as a neutral constant; override via AuditLogger(department_id=...).
DEFAULT_DEPARTMENT_ID = 0


class AuditLogger:

    def __init__(
        self,
        spark,
        audit_table: str = AUDIT_TABLE,
        department_id: int = DEFAULT_DEPARTMENT_ID,
    ):
        self.spark = spark
        self.table = audit_table
        self.department_id = department_id

    # ── Run lifecycle methods ─────────────────────────────────────────────────

    def start_run(
        self,
        ingestion_object_id: int,
        source_name: str,
        pipeline_name: str,
        load_type: str,
        source_schema: Optional[str],
        source_table: Optional[str],
        target_schema: str,
        target_table: str,
        delta_layer: str = "BRONZE",
        trigger_type: Optional[str] = "MANUAL",
        trigger_id: Optional[str] = None,
        trigger_name: Optional[str] = None,
        business_date: Optional[date] = None,
        frequency: Optional[str] = None,
    ) -> str:
        """
        Inserts a RUNNING row into data_pipeline_execution_master.
        Returns the run_id (UUID string) that must be passed to complete_run().
        """
        run_id = str(uuid.uuid4())
        now = datetime.utcnow()
        biz_date = business_date if business_date is not None else now.date()
        obj_id_int = int(ingestion_object_id)

        row = [(
            obj_id_int,             # config_master_id  (reuses ingestion_object_id)
            obj_id_int,             # table_id
            delta_layer,
            source_name,
            pipeline_name,
            load_type,
            frequency,              # nullable STRING
            biz_date,               # DATE NOT NULL
            run_id,
            trigger_type,
            trigger_id,
            trigger_name,
            now,                    # trigger_time  NOT NULL
            None,                   # end_time      (set on complete)
            None,                   # execution_duration_sec (set on complete)
            source_schema,
            source_table,
            target_schema,
            target_table,
            0,                      # rows_read
            0,                      # rows_copied
            0,                      # rows_deleted
            None,                   # total_cost
            int(self.department_id),
            "RUNNING",              # status  NOT NULL
        )]

        cols = [
            "config_master_id", "table_id", "delta_layer", "source_name",
            "pipeline_name", "load_type", "frequency", "business_date", "run_id",
            "trigger_type", "trigger_id", "trigger_name", "trigger_time",
            "end_time", "execution_duration_sec",
            "source_schema", "source_table", "target_schema", "target_table",
            "rows_read", "rows_copied", "rows_deleted", "total_cost",
            "department_id", "status",
        ]

        df = self.spark.createDataFrame(row, cols)
        df.write.format("delta").mode("append").saveAsTable(self.table)
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
        execution_duration_sec is computed from trigger_time inside Spark SQL
        so we do not need to pass clock values from Python.
        """
        self.spark.sql(f"""
            UPDATE {self.table}
            SET end_time                = current_timestamp(),
                execution_duration_sec  = CAST(
                    (unix_timestamp(current_timestamp()) - unix_timestamp(trigger_time))
                    AS DECIMAL(10, 2)),
                status                  = '{status}',
                rows_read               = {int(rows_read)},
                rows_copied             = {int(rows_copied)},
                rows_deleted            = {int(rows_deleted)}
            WHERE run_id = '{run_id}'
        """)

    def fail_run(self, run_id: str, error_message: str) -> None:
        """
        Convenience wrapper: marks a run FAILED.
        The error_message is logged to the Python logger by the orchestrator
        (the pre-existing audit table has no error_message column).
        """
        self.complete_run(run_id=run_id, status="FAILED")

    # ── Watermark helpers ─────────────────────────────────────────────────────

    def get_last_run_status(self, ingestion_object_id: int) -> Optional[str]:
        """
        Returns the status of the most recent (by trigger_time) run for the
        given ingestion_object_id, or None if no runs exist.
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
