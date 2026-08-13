from decimal import Decimal

from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class AuditLogger:
    """Writes ingestion audit records using the deployed audit-table schema."""

    def __init__(self, spark, audit_table):
        self.spark = spark
        self.table = audit_table

    def log_execution(
        self,
        task,
        source_sys,
        job_context,
        pipeline_name,
        start_time,
        end_time,
        status,
        rows_read=0,
        rows_copied=0,
        rows_deleted=0,
        **_unused,
    ):
        """Append one record without requiring Delta schema migration.

        The managed audit table has a fixed, ACL-protected schema.  Keep this
        DataFrame aligned to that schema; operational-only fields are not
        persisted until the table is intentionally altered by its owner.
        """
        ctx = job_context or {}

        def required_string(value, default="UNKNOWN"):
            return str(value) if value is not None else default

        execution_duration_sec = (end_time - start_time).total_seconds()
        row = [(
            int(task.config_master_id),
            int(task.config_id),
            required_string(task.effective_delta_layer),
            required_string(source_sys.source_name),
            required_string(pipeline_name, "manual_run"),
            required_string(task.load_type),
            task.frequency,
            start_time.date(),
            required_string(ctx.get("job_run_id"), "MANUAL"),
            ctx.get("trigger_type"),
            ctx.get("trigger_id"),
            ctx.get("trigger_name"),
            start_time,
            end_time,
            Decimal(str(execution_duration_sec)),
            task.source_schema,
            task.source_object_name,
            required_string(task.target_schema),
            required_string(task.target_table),
            int(rows_read or 0),
            int(rows_copied or 0),
            int(rows_deleted or 0),
            None,  # total_cost is not calculated by this ingestion workflow.
            None,  # department_id is not available in task configuration.
            required_string(status),
        )]

        schema = StructType([
            StructField("config_master_id", IntegerType(), True),
            StructField("table_id", IntegerType(), True),
            StructField("delta_layer", StringType(), True),
            StructField("source_name", StringType(), True),
            StructField("pipeline_name", StringType(), True),
            StructField("load_type", StringType(), True),
            StructField("frequency", StringType(), True),
            StructField("business_date", DateType(), True),
            StructField("run_id", StringType(), True),
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
            StructField("total_cost", DoubleType(), True),
            StructField("department_id", IntegerType(), True),
            StructField("status", StringType(), True),
        ])

        self.spark.createDataFrame(row, schema=schema).writeTo(self.table).using("delta").append()
