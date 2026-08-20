"""
Tracks per-table, per-run stage timing (Source→Raw, Raw→Silver) plus the
job-level pipeline_start_time/pipeline_end_time and the SLA-driven
dependency_resolve_time used by downstream dependents.

pipeline_start_time/pipeline_end_time are job-level — the SAME value across
every table in a given job_run_id (captured once in main.py, at job start
and job end). source_to_raw_*/raw_to_silver_* are per-table.

A row is inserted per table at that table's start via start_table() — before
AuditLogger.start_run() is called for that table — then updated per-stage as
that table progresses. pipeline_end_time and dependency_resolve_time are not
known until every table in the job has finished, so they're stamped onto
every row for the job in one bulk UPDATE via complete_job(), called once at
the very end of main.py.
"""
import threading
from datetime import datetime
from typing import Any, Dict, Optional

from pyspark.sql.types import (
    BooleanType, DateType, IntegerType, StringType,
    StructField, StructType, TimestampType,
)


class DependencyLogger:

    def __init__(self, spark, dependency_table: str):
        self.spark = spark
        self.table = dependency_table
        # main.py runs tables concurrently; serialise writes to the shared
        # Delta dependency table while leaving ingestion work parallel.
        self._write_lock = threading.Lock()

    def start_table(
        self,
        config_master_id: int,
        source_system_id: int,
        config_id: int,
        table_name: str,
        pipeline_name: str,
        job_run_id: str,
        business_date,
        pipeline_start_time: datetime,
    ) -> Dict[str, Any]:
        """Insert the row for one table, called at the start of that table's run."""
        row = [(
            int(config_master_id),
            int(source_system_id),
            int(config_id),
            str(table_name),
            str(pipeline_name),
            True,
            str(job_run_id),
            business_date,
            pipeline_start_time,
            None,                       # pipeline_end_time — set later by complete_job()
            datetime.utcnow(),          # source_to_raw_start_time
            None,                       # source_to_raw_end_time
            None,                       # raw_to_silver_start_time
            None,                       # raw_to_silver_end_time
            None,                       # dependency_resolve_time
        )]
        with self._write_lock:
            self.spark.createDataFrame(row, schema=self._schema()).writeTo(self.table).using("delta").append()
        return {"job_run_id": str(job_run_id), "config_id": int(config_id)}

    def mark_source_to_raw_end(self, dep_run: Dict[str, Any]) -> None:
        self._touch(dep_run, "source_to_raw_end_time")

    def mark_raw_to_silver_start(self, dep_run: Dict[str, Any]) -> None:
        self._touch(dep_run, "raw_to_silver_start_time")

    def mark_raw_to_silver_end(self, dep_run: Dict[str, Any]) -> None:
        self._touch(dep_run, "raw_to_silver_end_time")

    def complete_job(self, job_run_id: str) -> None:
        """
        Bulk-stamps pipeline_end_time and dependency_resolve_time onto every
        row for this job_run_id at once. Called once, after every table in
        the job has finished — not per table, since pipeline_end_time isn't
        known until then.

        dependency_resolve_time = LEAST(pipeline_end_time, pipeline_start_time + 24h)
        i.e. if the job is still running 24h after it started, the dependency
        is considered resolved at that boundary rather than the actual
        (later) completion time.
        """
        with self._write_lock:
            self.spark.sql(f"""
                UPDATE {self.table}
                SET pipeline_end_time       = current_timestamp(),
                    dependency_resolve_time = LEAST(
                        current_timestamp(),
                        pipeline_start_time + INTERVAL 24 HOURS
                    )
                WHERE job_run_id = {self._sql_literal(job_run_id)}
            """)

    def _touch(self, dep_run: Dict[str, Any], column: str) -> None:
        with self._write_lock:
            self.spark.sql(f"""
                UPDATE {self.table}
                SET {column} = current_timestamp()
                WHERE job_run_id = {self._sql_literal(dep_run['job_run_id'])}
                  AND config_id  = {int(dep_run['config_id'])}
            """)

    @staticmethod
    def _sql_literal(value: Any) -> str:
        return "NULL" if value is None else "'" + str(value).replace("'", "''") + "'"

    @staticmethod
    def _schema() -> StructType:
        return StructType([
            StructField("config_master_id",       IntegerType(), True),
            StructField("source_system_id",       IntegerType(), True),
            StructField("config_id",              IntegerType(), True),
            StructField("table_name",             StringType(), True),
            StructField("pipeline_name",          StringType(), True),
            StructField("is_active",              BooleanType(), True),
            StructField("job_run_id",             StringType(), True),
            StructField("business_date",          DateType(), True),
            StructField("pipeline_start_time",    TimestampType(), True),
            StructField("pipeline_end_time",      TimestampType(), True),
            StructField("source_to_raw_start_time", TimestampType(), True),
            StructField("source_to_raw_end_time",   TimestampType(), True),
            StructField("raw_to_silver_start_time", TimestampType(), True),
            StructField("raw_to_silver_end_time",   TimestampType(), True),
            StructField("dependency_resolve_time",  TimestampType(), True),
        ])
