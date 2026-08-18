"""
SilverProcessor — triggers the Silver transformation notebook for a specific
table using dbutils.notebook.run().

Called directly from IngestionOrchestrator right after a table's Bronze write
and audit SUCCESS, on that table's own thread pool. All context the Silver
notebook needs (Bronze table location, source info, load type, keys, and
everything AuditLogger needs to write its own SILVER audit row) is passed in
as widget parameters — the notebook does not re-query config_master/
ingestion_config or the audit table to look any of it up again.

dbutils is passed in from the calling notebook since it is a notebook-level
object and cannot be imported as a module.
"""
import logging

log = logging.getLogger("ingestion_framework")


class SilverProcessor:
    """
    Runs the Silver transformation notebook for one table via
    dbutils.notebook.run().

    Parameters
    ----------
    dbutils              : Databricks dbutils object (passed from the calling notebook)
    silver_notebook_path : Workspace path to the Silver transformation notebook
                           e.g. /Workspace/Shared/pfl-ingestion-framework/src/silver/silver_transform
    timeout_seconds      : Max seconds to wait for the Silver notebook to finish (default 3600)
    """

    def __init__(self, dbutils, silver_notebook_path: str, timeout_seconds: int = 3600):
        self.dbutils               = dbutils
        self.silver_notebook_path  = silver_notebook_path
        self.timeout_seconds       = timeout_seconds

    def trigger(
        self,
        config_id: int,
        bronze_table: str,
        source_schema: str,
        source_object_name: str,
        load_type: str,
        primary_key_cols: str,
        source_name: str,
        pipeline_name: str,
        target_schema: str,
        target_table: str,
        audit_table: str,
        department_id: int,
        job_run_id: str,
        config_master_id: int = None,
        frequency: str = "",
    ) -> dict:
        """
        Runs the Silver notebook synchronously for one table and returns a result dict.

        The Silver notebook is expected to accept these widget parameters:
          - config_id           : int    (identifies the ingestion config row)
          - bronze_table        : str    (fully-qualified Bronze table just written,
                                          catalog.schema.table)
          - source_schema       : str
          - source_object_name  : str
          - load_type           : str
          - primary_key_cols    : str    (comma-separated, may be empty)
          - source_name         : str    (source system name, for its own audit row)
          - pipeline_name       : str
          - target_schema       : str    (currently Bronze's target_schema, reused —
                                          Silver has no real output location yet)
          - target_table        : str    (currently Bronze's target_table, reused)
          - audit_table         : str    (FQN of data_pipeline_execution_master)
          - department_id       : int
          - job_run_id          : str    (root job run ID — same one Bronze's audit row
                                          used, so Bronze and Silver rows correlate)
          - config_master_id    : int    (may be blank)
          - frequency           : str    (may be blank)

        Returns dict with keys: config_id, bronze_table, status, exit_value, error
        """
        log.info(
            f"[SILVER] Running Silver notebook for config_id={config_id} "
            f"bronze_table='{bronze_table}'"
        )

        exit_value = self.dbutils.notebook.run(
            self.silver_notebook_path,
            self.timeout_seconds,
            {
                "config_id":          str(config_id),
                "bronze_table":       bronze_table,
                "source_schema":      source_schema or "",
                "source_object_name": source_object_name or "",
                "load_type":          load_type or "",
                "primary_key_cols":   primary_key_cols or "",
                "source_name":        source_name or "",
                "pipeline_name":      pipeline_name or "",
                "target_schema":      target_schema or "",
                "target_table":       target_table or "",
                "audit_table":        audit_table or "",
                "department_id":      str(department_id or 0),
                "job_run_id":         job_run_id or "",
                "config_master_id":   str(config_master_id) if config_master_id is not None else "",
                "frequency":          frequency or "",
            },
        )

        log.info(
            f"[SILVER] Silver notebook finished for bronze_table='{bronze_table}' "
            f"exit_value='{exit_value}'"
        )
        return {
            "config_id":    config_id,
            "bronze_table": bronze_table,
            "status":       "SUCCESS",
            "exit_value":   exit_value,
            "error":        None,
        }
