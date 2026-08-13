from datetime import datetime
from decimal import Decimal
from typing import Optional, Dict, Any


class AuditLogger:

    def __init__(
        self,
        spark,
        audit_table: str
    ):
        self.spark = spark
        self.audit_table = audit_table

    # -------------------------------------------------------------------------
    # Databricks Runtime Context
    # -------------------------------------------------------------------------

    def _get_databricks_context(self) -> Dict[str, Any]:
        """
        Reads execution metadata from the current Databricks Job/Task context.

        """

        job_id = (
            self.spark.sparkContext
            .getLocalProperty("spark.databricks.job.id")
        )

        job_run_id = (
            self.spark.sparkContext
            .getLocalProperty("spark.databricks.job.runId")
        )

        task_key = (
            self.spark.sparkContext
            .getLocalProperty("spark.databricks.job.taskKey")
        )

        return {
            "job_id": job_id,
            "job_run_id": job_run_id,
            "task_key": task_key
        }

    # -------------------------------------------------------------------------
    # Notebook / Databricks Information
    # -------------------------------------------------------------------------

    def _get_notebook_context(self) -> Dict[str, Any]:
        """
        Gets notebook/task information from Databricks notebook context.
        """

        try:
            dbutils = self.spark._jvm.com.databricks.dbutils_v1.DBUtilsHolder.dbutils()

            notebook_context = (
                dbutils.notebook()
                .getContext()
            )

            notebook_path = (
                notebook_context
                .notebookPath()
                .getOrElse(None)
            )

            browser_host = (
                notebook_context
                .browserHostName()
                .getOrElse(None)
            )

            return {
                "notebook_name": notebook_path,
                "databricks_url": (
                    f"https://{browser_host}"
                    if browser_host
                    else None
                )
            }

        except Exception:
            return {
                "notebook_name": None,
                "databricks_url": None
            }

    # -------------------------------------------------------------------------
    # Trigger Information
    # -------------------------------------------------------------------------

    def _get_trigger_context(self) -> Dict[str, Any]:
        """
        Gets trigger information when exposed by the Databricks runtime.

        Values remain NULL when the runtime does not expose them.
        """

        return {
            "trigger_type": self.spark.sparkContext.getLocalProperty(
                "spark.databricks.job.trigger.type"
            ),
            "trigger_id": self.spark.sparkContext.getLocalProperty(
                "spark.databricks.job.trigger.id"
            ),
            "trigger_name": self.spark.sparkContext.getLocalProperty(
                "spark.databricks.job.trigger.name"
            )
        }

    # -------------------------------------------------------------------------
    # Write Audit Record
    # -------------------------------------------------------------------------

    def log_execution(
        self,
        *,
        config_master_id: int,
        table_id: int,
        department_id: int,
        delta_layer: str,
        source_name: str,
        pipeline_name: str,
        load_type: str,
        frequency: Optional[str],
        business_date,
        source_schema: Optional[str],
        source_table: Optional[str],
        target_schema: str,
        target_table: str,

        trigger_time: datetime,
        end_time: datetime,

        rows_read: int = 0,
        rows_copied: int = 0,
        rows_deleted: int = 0,
        rows_affected: int = 0,

        data_read_bytes: int = 0,
        data_written_bytes: int = 0,

        throughput_mb_per_sec: Optional[float] = None,
        copy_duration_sec: Optional[float] = None,

        operation_performed: Optional[str] = None,

        status: str = "SUCCESS",

        error_code: Optional[str] = None,
        error_message: Optional[str] = None
    ) -> None:

        # ---------------------------------------------------------------------
        # Validate status
        # ---------------------------------------------------------------------

        status = status.upper()

        if status not in ("SUCCESS", "FAILED"):
            raise ValueError(
                "status must be either SUCCESS or FAILED"
            )

        # ---------------------------------------------------------------------
        # Get Databricks execution information
        # ---------------------------------------------------------------------

        dbx_context = self._get_databricks_context()
        notebook_context = self._get_notebook_context()
        trigger_context = self._get_trigger_context()

        job_id = dbx_context.get("job_id")
        job_run_id = dbx_context.get("job_run_id")

        if not job_run_id:
            raise RuntimeError(
                "Databricks Job Run ID could not be determined from the "
                "current execution context."
            )

        # ---------------------------------------------------------------------
        # Calculate duration
        # ---------------------------------------------------------------------

        execution_duration_sec = (
            end_time - trigger_time
        ).total_seconds()

        # ---------------------------------------------------------------------
        # Clean error information
        # ---------------------------------------------------------------------

        if status == "SUCCESS":
            error_code = None
            error_message = None

        # ---------------------------------------------------------------------
        # Calculate throughput if not supplied
        # ---------------------------------------------------------------------

        if (
            throughput_mb_per_sec is None
            and execution_duration_sec > 0
            and data_read_bytes is not None
        ):
            throughput_mb_per_sec = (
                data_read_bytes / (1024 * 1024)
            ) / execution_duration_sec

        # ---------------------------------------------------------------------
        # Create one audit record
        # ---------------------------------------------------------------------

        data = [(
            config_master_id,
            table_id,
            department_id,
            delta_layer,
            source_name,
            pipeline_name,
            load_type,
            frequency,

            business_date,

            job_id,
            job_run_id,

            trigger_context.get("trigger_type"),
            trigger_context.get("trigger_id"),
            trigger_context.get("trigger_name"),

            trigger_time,
            end_time,

            Decimal(str(round(execution_duration_sec, 2))),

            source_schema,
            source_table,

            target_schema,
            target_table,

            rows_read,
            rows_copied,
            rows_deleted,
            rows_affected,

            data_read_bytes,
            data_written_bytes,

            (
                Decimal(str(round(throughput_mb_per_sec, 2)))
                if throughput_mb_per_sec is not None
                else None
            ),

            (
                Decimal(str(round(copy_duration_sec, 2)))
                if copy_duration_sec is not None
                else None
            ),

            notebook_context.get("notebook_name"),
            notebook_context.get("databricks_url"),

            operation_performed,

            status,

            error_code,
            error_message
        )]

        # ---------------------------------------------------------------------
        # Explicit schema
        # ---------------------------------------------------------------------

        schema = """
            config_master_id INT,
            table_id INT,
            department_id INT,
            delta_layer STRING,
            source_name STRING,
            pipeline_name STRING,
            load_type STRING,
            frequency STRING,

            business_date DATE,

            job_id STRING,
            job_run_id STRING,

            trigger_type STRING,
            trigger_id STRING,
            trigger_name STRING,

            trigger_time TIMESTAMP,
            end_time TIMESTAMP,

            execution_duration_sec DECIMAL(10,2),

            source_schema STRING,
            source_table STRING,

            target_schema STRING,
            target_table STRING,

            rows_read BIGINT,
            rows_copied BIGINT,
            rows_deleted BIGINT,
            rows_affected BIGINT,

            data_read_bytes BIGINT,
            data_written_bytes BIGINT,

            throughput_mb_per_sec DECIMAL(10,2),
            copy_duration_sec DECIMAL(10,2),

            databricks_notebook_name STRING,
            databricks_url STRING,

            operation_performed STRING,

            status STRING,

            error_code STRING,
            error_message STRING
        """

        # ---------------------------------------------------------------------
        # Insert audit record
        # ---------------------------------------------------------------------

        audit_df = self.spark.createDataFrame(
            data,
            schema=schema
        )

        audit_df.writeTo(
            self.audit_table
        ).using("delta").append()