from datetime import datetime
from decimal import Decimal
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DateType,
    TimestampType,
    LongType,
    DecimalType,
)


class AuditLogger:

    def __init__(
        self,
        spark,
        audit_table,
    ):
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
        rows_affected=0,
        data_read_bytes=0,
        data_written_bytes=0,
        throughput_mb_per_sec=None,
        copy_duration_sec=None,
        operation_performed=None,
        error_code=None,
        error_message=None,
    ):

        ctx = job_context or {}

        # -------------------------------------------------------------
        # Databricks execution information
        # -------------------------------------------------------------

        job_id = ctx.get("job_id")
        job_run_id = ctx.get("job_run_id")
        trigger_type = ctx.get("trigger_type")
        trigger_id = ctx.get("trigger_id")
        trigger_name = ctx.get("trigger_name")

        # -------------------------------------------------------------
        # Execution duration
        # -------------------------------------------------------------

        execution_duration_sec = (
            end_time - start_time
        ).total_seconds()

        # -------------------------------------------------------------
        # Create audit row
        # -------------------------------------------------------------

        row = [(
            int(task.config_master_id),
            int(task.config_id),

            task.effective_delta_layer,
            source_sys.source_name,
            pipeline_name,
            task.load_type,
            task.frequency,

            start_time.date(),

            job_id,
            job_run_id,

            trigger_type,
            trigger_id,
            trigger_name,

            start_time,
            end_time,
            Decimal(str(execution_duration_sec)),

            task.source_schema,
            task.source_object_name,

            task.target_schema,
            task.target_table,

            int(rows_read or 0),
            int(rows_copied or 0),
            int(rows_deleted or 0),
            int(rows_affected or 0),

            int(data_read_bytes or 0),
            int(data_written_bytes or 0),

            (
                Decimal(str(throughput_mb_per_sec))
                if throughput_mb_per_sec is not None
                else None
            ),

            (
                Decimal(str(copy_duration_sec))
                if copy_duration_sec is not None
                else None
            ),

            ctx.get("notebook_name"),
            ctx.get("databricks_url"),

            operation_performed,

            status,

            error_code,
            error_message,
        )]

        schema = StructType([

            StructField(
                "config_master_id",
                IntegerType(),
                False
            ),

            StructField(
                "table_id",
                IntegerType(),
                False
            ),

            StructField(
                "delta_layer",
                StringType(),
                False
            ),

            StructField(
                "source_name",
                StringType(),
                False
            ),

            StructField(
                "pipeline_name",
                StringType(),
                False
            ),

            StructField(
                "load_type",
                StringType(),
                False
            ),

            StructField(
                "frequency",
                StringType(),
                True
            ),

            StructField(
                "business_date",
                DateType(),
                False
            ),

            StructField(
                "job_id",
                StringType(),
                False
            ),

            StructField(
                "job_run_id",
                StringType(),
                False
            ),

            StructField(
                "trigger_type",
                StringType(),
                True
            ),

            StructField(
                "trigger_id",
                StringType(),
                True
            ),

            StructField(
                "trigger_name",
                StringType(),
                True
            ),

            StructField(
                "trigger_time",
                TimestampType(),
                False
            ),

            StructField(
                "end_time",
                TimestampType(),
                True
            ),

            StructField(
                "execution_duration_sec",
                DecimalType(10, 2),
                True
            ),

            StructField(
                "source_schema",
                StringType(),
                True
            ),

            StructField(
                "source_table",
                StringType(),
                True
            ),

            StructField(
                "target_schema",
                StringType(),
                False
            ),

            StructField(
                "target_table",
                StringType(),
                False
            ),

            StructField(
                "rows_read",
                LongType(),
                True
            ),

            StructField(
                "rows_copied",
                LongType(),
                True
            ),

            StructField(
                "rows_deleted",
                LongType(),
                True
            ),

            StructField(
                "rows_affected",
                LongType(),
                True
            ),

            StructField(
                "data_read_bytes",
                LongType(),
                True
            ),

            StructField(
                "data_written_bytes",
                LongType(),
                True
            ),

            StructField(
                "throughput_mb_per_sec",
                DecimalType(10, 2),
                True
            ),

            StructField(
                "copy_duration_sec",
                DecimalType(10, 2),
                True
            ),

            StructField(
                "databricks_notebook_name",
                StringType(),
                True
            ),

            StructField(
                "databricks_url",
                StringType(),
                True
            ),

            StructField(
                "operation_performed",
                StringType(),
                True
            ),

            StructField(
                "status",
                StringType(),
                False
            ),

            StructField(
                "error_code",
                StringType(),
                True
            ),

            StructField(
                "error_message",
                StringType(),
                True
            ),
        ])

        df = self.spark.createDataFrame(
            row,
            schema=schema
        )

        df.writeTo(
            self.table
        ).using("delta").append()
