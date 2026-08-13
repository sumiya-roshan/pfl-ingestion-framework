from decimal import Decimal

from pyspark.sql.types import DateType, DecimalType, IntegerType, LongType, StringType, StructField, StructType, TimestampType


class AuditLogger:
    """Writes audit entries using the deployed, ACL-protected table schema."""

    def __init__(self, spark, audit_table):
        self.spark = spark
        self.table = audit_table

    def log_execution(
        self, task, source_sys, job_context, pipeline_name, start_time, end_time,
        status, rows_read=0, rows_copied=0, rows_deleted=0, rows_affected=0,
        data_read_bytes=0, data_written_bytes=0, throughput_mb_per_sec=None,
        copy_duration_sec=None, operation_performed=None, error_code=None,
        error_message=None,
    ):
        """Append one audit entry without triggering Delta schema migration."""
        ctx = job_context or {}

        def required_string(value, default="UNKNOWN"):
            return str(value) if value is not None else default

        execution_duration_sec = (end_time - start_time).total_seconds()
        row = [(
            # department_id is not part of the ingestion configuration. The
            # target table requires a value, so use 0 as the neutral sentinel.
            int(task.config_master_id), int(task.config_id), 0,
            required_string(task.effective_delta_layer), required_string(source_sys.source_name),
            required_string(pipeline_name, "manual_run"), required_string(task.load_type),
            task.frequency, start_time.date(),
            required_string(ctx.get("job_id"), "MANUAL"),
            required_string(ctx.get("job_run_id"), "MANUAL"),
            ctx.get("trigger_type"), ctx.get("trigger_id"), ctx.get("trigger_name"),
            start_time, end_time, Decimal(str(execution_duration_sec)),
            task.source_schema, task.source_object_name,
            required_string(task.target_schema), required_string(task.target_table),
            int(rows_read or 0), int(rows_copied or 0), int(rows_deleted or 0),
            int(rows_affected or 0), int(data_read_bytes or 0), int(data_written_bytes or 0),
            Decimal(str(throughput_mb_per_sec)) if throughput_mb_per_sec is not None else None,
            Decimal(str(copy_duration_sec)) if copy_duration_sec is not None else None,
            ctx.get("notebook_name"), ctx.get("databricks_url"), operation_performed,
            required_string(status), error_code, error_message,
        )]

        schema = StructType([
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

        self.spark.createDataFrame(row, schema=schema).writeTo(self.table).using("delta").append()
