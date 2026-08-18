"""
SilverProcessor — triggers the Silver transformation notebook for a specific
table using dbutils.notebook.run().

Called directly from IngestionOrchestrator right after a table's Bronze write
and audit SUCCESS, on that table's own thread pool. All context the Silver
notebook needs (Bronze table location, source info, load type, keys) is
passed in as widget parameters — the notebook does not re-query
config_master/ingestion_config or the audit table to look it up again.

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
        primary_key_cols: str = "",
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
