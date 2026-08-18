"""Audit lifecycle logging for the deployed execution-master Delta table."""
from datetime import datetime
from decimal import Decimal
import threading
from typing import Any, Dict, Optional

from pyspark.sql.types import (
    DateType, DecimalType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType,
)

from .config_manager import AUDIT_STATUS_FAILED, AUDIT_STATUS_INPROGRESS, AUDIT_STATUS_SUCCESS, AUDIT_STATUS_SKIPPED


class AuditLogger:
    """Creates one audit row at start and updates it when the task finishes."""

    def __init__(self, spark, audit_table: str, department_id: int = 0):
        self.spark = spark
        self.table = audit_table
        self.department_id = department_id
        # main.py can run tasks concurrently; serialise writes to the shared
        # Delta audit table while leaving ingestion work parallel.
        self._write_lock = threading.Lock()

    def start_run(
        self,
        task,
        source_sys,
        job_context: Optional[Dict[str, Any]],
        pipeline_name: str,
        config_master_id: Optional[int] = None,
        business_date=None,
    ) -> Dict[str, Any]:
        """Insert an INPROGRESS row and capture the current start time."""
        ctx = job_context or {}
        start_time = datetime.utcnow()
        table_id = int(task.config_id)
        job_run_id = ctx.get("job_run_id")
        if not job_run_id:
            raise ValueError("job_run_id must be provided in job_context and not be empty")
        job_run_id = str(job_run_id)

        row = [(
            int(config_master_id) if config_master_id is not None else table_id,
            table_id,
            int(self.department_id),
            self._required_string(task.effective_delta_layer),
            self._required_string(source_sys.source_name),
            self._required_string(pipeline_name),
            self._required_string(task.load_type),
            getattr(task, "frequency", None),
            business_date if business_date is not None else start_time.date(),
            self._required_string(ctx.get("job_id"), "MANUAL"),
            job_run_id,
            ctx.get("trigger_type"),
            ctx.get("trigger_id"),
            ctx.get("trigger_name"),
            start_time,
            None,
            None,
            task.source_schema,
            task.source_object_name,
            self._required_string(task.target_schema),
            self._required_string(task.target_table),
            0, 0, 0, 0, 0, 0,
            None, None,
            self._required_string(ctx.get("notebook_name"), "UNKNOWN"),
            self._required_string(ctx.get("databricks_url"), "UNKNOWN"),
            "INGESTION",
            AUDIT_STATUS_INPROGRESS,
            None,
            None,
        )]

        with self._write_lock:
            self.spark.createDataFrame(row, schema=self._schema()).writeTo(self.table).using("delta").append()
        return {"job_run_id": job_run_id, "table_id": table_id}

    def complete_run(
        self,
        audit_run: Dict[str, Any],
        status: str,
        rows_read: int = 0,
        rows_copied: int = 0,
        rows_deleted: int = 0,
        data_read_bytes: int = 0,
        data_written_bytes: int = 0,
        throughput_mb_per_sec: Optional[float] = None,
        copy_duration_sec: Optional[float] = None,
        error_code: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """Set end time and final SUCCESS or FAILED status for an audit row."""
        if status not in (AUDIT_STATUS_SUCCESS, AUDIT_STATUS_FAILED):
            raise ValueError(f"Final audit status must be SUCCESS or FAILED, got {status!r}")

        with self._write_lock:
            self.spark.sql(f"""
                UPDATE {self.table}
                SET end_time               = current_timestamp(),
                    execution_duration_sec = CAST(
                        unix_timestamp(current_timestamp()) - unix_timestamp(trigger_time)
                        AS DECIMAL(10, 2)),
                    status                  = {self._sql_literal(status)},
                    rows_read               = {int(rows_read or 0)},
                    rows_copied             = {int(rows_copied or 0)},
                    rows_deleted            = {int(rows_deleted or 0)},
                    data_read_bytes         = {int(data_read_bytes or 0)},
                    data_written_bytes      = {int(data_written_bytes or 0)},
                    throughput_mb_per_sec   = {self._sql_numeric(throughput_mb_per_sec)},
                    copy_duration_sec       = {self._sql_numeric(copy_duration_sec)},
                    error_code              = {self._sql_literal(error_code)},
                    error_message           = {self._sql_literal(error_message)}
                WHERE job_run_id = {self._sql_literal(audit_run['job_run_id'])}
                  AND table_id = {int(audit_run['table_id'])}
                  AND status = {self._sql_literal(AUDIT_STATUS_INPROGRESS)}
            """)

    def fail_run(self, audit_run: Dict[str, Any],error_code: str, error_message: str) -> None:
        """Mark the active audit record as FAILED."""
        self.complete_run(audit_run, AUDIT_STATUS_FAILED,error_code= error_code, error_message=error_message)

    def log_skipped_row(
        self,
        task,
        source_sys,
        job_context: Optional[Dict[str, Any]],
        pipeline_name: str,
        config_master_id: Optional[int] = None,
        reason: Optional[str] = None,
        business_date=None,
    ) -> None:
        """
        Insert a single, complete SKIPPED audit record for a table that was
        excluded by the source_lookup task because it had 0 rows in the source.

        Unlike start_run + complete_run, this writes the terminal record
        immediately (start and end timestamps are both now).

        Parameters
        ----------
        task             : IngestionTaskConfig for the skipped table
        source_sys       : SourceSystemConfig
        job_context      : job/run context dict (same format as start_run)
        pipeline_name    : pipeline name string
        config_master_id : config_master routing ID
        reason           : human-readable reason written to error_message column
        business_date    : override for business_date (defaults to UTC today)
        """
        ctx        = job_context or {}
        now        = datetime.utcnow()
        table_id   = int(task.config_id)
        job_run_id = str(ctx.get("job_run_id") or "MANUAL")

        row = [(
            int(config_master_id) if config_master_id is not None else table_id,
            table_id,
            int(self.department_id),
            self._required_string(task.effective_delta_layer),
            self._required_string(source_sys.source_name),
            self._required_string(pipeline_name),
            self._required_string(task.load_type),
            getattr(task, "frequency", None),
            business_date if business_date is not None else now.date(),
            self._required_string(ctx.get("job_id"), "MANUAL"),
            job_run_id,
            ctx.get("trigger_type"),
            ctx.get("trigger_id"),
            ctx.get("trigger_name"),
            now,           # trigger_time
            now,           # end_time  (same — skipped immediately)
            Decimal("0"),  # execution_duration_sec
            task.source_schema,
            task.source_object_name,
            self._required_string(task.target_schema),
            self._required_string(task.target_table),
            0, 0, 0, 0, 0, 0,  # rows_* and byte metrics
            None, None,         # throughput, copy_duration
            self._required_string(ctx.get("notebook_name"), "UNKNOWN"),
            self._required_string(ctx.get("databricks_url"), "UNKNOWN"),
            "LOOKUP",           # operation_performed
            AUDIT_STATUS_SKIPPED,
            "SOURCE_LOOKUP_ZERO_ROWS",  # error_code
            reason or "Source lookup returned 0 rows — table excluded from ingestion.",
        )]

        with self._write_lock:
            self.spark.createDataFrame(row, schema=self._schema()).writeTo(self.table).using("delta").append()

    @staticmethod
    def _required_string(value: Any, default: str = "UNKNOWN") -> str:
        return str(value) if value is not None else default

    @staticmethod
    def _sql_literal(value: Any) -> str:
        return "NULL" if value is None else "'" + str(value).replace("'", "''") + "'"

    @staticmethod
    def _sql_numeric(value: Any) -> str:
        return "NULL" if value is None else str(value)

    @staticmethod
    def _schema() -> StructType:
        return StructType([
            StructField("config_master_id", IntegerType(), True),
            StructField("table_id", IntegerType(), True),
            StructField("department_id", IntegerType(), True),
            StructField("delta_layer", StringType(), True),
            StructField("source_name", StringType(), True),
            StructField("pipeline_name", StringType(), True),
            StructField("load_type", StringType(), True),
            StructField("frequency", StringType(), True),
            StructField("business_date", DateType(), True),
            StructField("job_id", StringType(), True),
            StructField("job_run_id", StringType(), True),
            StructField("trigger_type", StringType(), True),
            StructField("trigger_id", StringType(), True),
            StructField("trigger_name", StringType(), True),
            StructField("trigger_time", TimestampType(), True),
            StructField("end_time", TimestampType(), True),
            StructField("execution_duration_sec", DecimalType(10, 2), True),
            StructField("source_schema", StringType(), True),
            StructField("source_table", StringType(), True),
            StructField("target_schema", StringType(), True),
            StructField("target_table", StringType(), True),
            StructField("rows_read", LongType(), True),
            StructField("rows_copied", LongType(), True),
            StructField("rows_deleted", LongType(), True),
            StructField("rows_affected", LongType(), True),
            StructField("data_read_bytes", LongType(), True),
            StructField("data_written_bytes", LongType(), True),
            StructField("throughput_mb_per_sec", DecimalType(10, 2), True),
            StructField("copy_duration_sec", DecimalType(10, 2), True),
            StructField("databricks_notebook_name", StringType(), True),
            StructField("databricks_url", StringType(), True),
            StructField("operation_performed", StringType(), True),
            StructField("status", StringType(), True),
            StructField("error_code", StringType(), True),
            StructField("error_message", StringType(), True),
        ])
