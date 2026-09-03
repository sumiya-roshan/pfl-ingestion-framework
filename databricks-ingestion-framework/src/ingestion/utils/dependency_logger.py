"""
Tracks per-table, per-run stage timing (Source→Raw, Raw→Silver) plus the
job-level pipeline_start_time/pipeline_end_time.

pipeline_start_time/pipeline_end_time are job-level — the SAME value across
every table in a given job_run_id (captured once in main.py, at job start
and job end). source_to_raw_*/raw_to_silver_* are per-table.

dependency_resolve_time is the signal downstream teams poll: NULL means
this table's Silver isn't done yet for today; once it has a timestamp, the
table is safe to consume. It's set to the SAME timestamp as
raw_to_silver_end_time, at the moment that table's Silver run finishes — not
a job-level value, and not capped/derived from anything else. A table still
running (or not yet reached) simply stays NULL until then.

(config_id, source_system_id) is the unique key for this table — ONE row per
table, refreshed (not appended) on every run. config_id alone isn't enough:
each child config table (rdbms_ingestion_config, s3_config_master, ...) has
its own independent Config_ID sequence, so the same config_id can legitimately
refer to different tables under different source systems. start_table() is an
upsert (MERGE): a table that's already run before gets its existing row
refreshed — stage timestamps reset for the new run — rather than a new row
appended; a table running for the first time gets inserted. It's called
before AuditLogger.start_run() is called for that table, then updated
per-stage as that table progresses. pipeline_end_time is job-level and isn't
known until every table in the job has finished, so it's stamped onto every
row touched by this job_run_id in one bulk UPDATE via complete_job(), called
once at the very end of main.py.
"""

import threading
from datetime import datetime
from typing import Any

from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
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
    ) -> dict[str, Any]:
        """
        Upsert the row for one table, called at the start of that table's
        run. (config_id, source_system_id) is the unique key — a table
        that's already run before gets its existing row refreshed (stage
        timestamps reset for the new run) rather than a new row appended;
        a table running for the first time gets inserted.
        """
        row = [
            (
                int(config_master_id),
                int(source_system_id),
                int(config_id),
                str(table_name),
                str(pipeline_name),
                str(job_run_id),
                business_date,
                pipeline_start_time,
            )
        ]
        source_schema = StructType(
            [
                StructField("config_master_id", IntegerType(), True),
                StructField("source_system_id", IntegerType(), True),
                StructField("config_id", IntegerType(), True),
                StructField("table_name", StringType(), True),
                StructField("pipeline_name", StringType(), True),
                StructField("job_run_id", StringType(), True),
                StructField("business_date", DateType(), True),
                StructField("pipeline_start_time", TimestampType(), True),
            ]
        )
        source_view = f"__dep_start_source_{config_id}_{source_system_id}"

        with self._write_lock:
            self.spark.createDataFrame(
                row, schema=source_schema
            ).createOrReplaceTempView(source_view)
            self.spark.sql(f"""
                MERGE INTO {self.table} AS target
                USING {source_view} AS source
                ON  target.config_id        = source.config_id
                AND target.source_system_id = source.source_system_id
                WHEN MATCHED THEN UPDATE SET
                    target.config_master_id        = source.config_master_id,
                    target.table_name              = source.table_name,
                    target.pipeline_name           = source.pipeline_name,
                    target.is_active               = true,
                    target.job_run_id              = source.job_run_id,
                    target.business_date           = source.business_date,
                    target.pipeline_start_time     = source.pipeline_start_time,
                    target.pipeline_end_time       = NULL,
                    target.source_to_raw_start_time = current_timestamp(),
                    target.source_to_raw_end_time   = NULL,
                    target.raw_to_silver_start_time = NULL,
                    target.raw_to_silver_end_time   = NULL,
                    target.dependency_resolve_time  = NULL
                WHEN NOT MATCHED THEN INSERT (
                    config_master_id, source_system_id, config_id, table_name,
                    pipeline_name, is_active, job_run_id, business_date,
                    pipeline_start_time, pipeline_end_time,
                    source_to_raw_start_time, source_to_raw_end_time,
                    raw_to_silver_start_time, raw_to_silver_end_time,
                    dependency_resolve_time
                )
                VALUES (
                    source.config_master_id, source.source_system_id, source.config_id, source.table_name,
                    source.pipeline_name, true, source.job_run_id, source.business_date,
                    source.pipeline_start_time, NULL,
                    current_timestamp(), NULL,
                    NULL, NULL,
                    NULL
                )
            """)
            self.spark.catalog.dropTempView(source_view)
        return {
            "job_run_id": str(job_run_id),
            "config_id": int(config_id),
            "source_system_id": int(source_system_id),
        }

    def mark_source_to_raw_end(self, dep_run: dict[str, Any]) -> None:
        self._touch(dep_run, "source_to_raw_end_time")

    def mark_raw_to_silver_start(self, dep_run: dict[str, Any]) -> None:
        self._touch(dep_run, "raw_to_silver_start_time")

    def mark_raw_to_silver_end(self, dep_run: dict[str, Any]) -> None:
        """
        Marks this table's Silver run as finished — and, in the same UPDATE,
        stamps dependency_resolve_time with that same timestamp. That's the
        signal downstream teams poll: NULL = not ready yet, a timestamp =
        today's data for this table is safe to consume.
        """
        with self._write_lock:
            self.spark.sql(f"""
                UPDATE {self.table}
                SET raw_to_silver_end_time  = current_timestamp(),
                    dependency_resolve_time = current_timestamp()
                WHERE config_id        = {int(dep_run["config_id"])}
                  AND source_system_id = {int(dep_run["source_system_id"])}
            """)

    def complete_job(self, job_run_id: str) -> None:
        """
        Bulk-stamps pipeline_end_time onto every row for this job_run_id at
        once. Called once, after every table in the job has finished — not
        per table, since pipeline_end_time isn't known until then. Does not
        touch dependency_resolve_time — that's set per-table, independently,
        in mark_raw_to_silver_end().
        """
        with self._write_lock:
            self.spark.sql(f"""
                UPDATE {self.table}
                SET pipeline_end_time = current_timestamp()
                WHERE job_run_id = {self._sql_literal(job_run_id)}
            """)

    def _touch(self, dep_run: dict[str, Any], column: str) -> None:
        # (config_id, source_system_id) is the unique key.
        with self._write_lock:
            self.spark.sql(f"""
                UPDATE {self.table}
                SET {column} = current_timestamp()
                WHERE config_id        = {int(dep_run["config_id"])}
                  AND source_system_id = {int(dep_run["source_system_id"])}
            """)

    @staticmethod
    def _sql_literal(value: Any) -> str:
        return "NULL" if value is None else "'" + str(value).replace("'", "''") + "'"
