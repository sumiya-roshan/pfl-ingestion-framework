"""
SilverProcessor — triggers the Silver transformation notebook for a specific
table using dbutils.notebook.run().

Called directly from IngestionOrchestrator right after a table's S3 landing
write, on that table's own thread pool. Silver reads from the S3 landing path
(not the Bronze Delta table) and writes into `{target_schema}_silver`. All
context the Silver notebook needs is passed in as widget parameters — the
notebook does not re-query config_master/ingestion_config or the audit table
to look any of it up again.

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
        source_system_id: int,
        landing_path: str,
        file_format: str,
        silver_catalog: str,
        silver_schema: str,
        silver_table: str,
        source_schema: str,
        source_object_name: str,
        load_type: str,
        primary_key_cols: str = "",
    ) -> dict:
        """
        Runs the Silver notebook synchronously for one table and returns a result dict.

        The Silver notebook is expected to accept these widget parameters:
          - config_id           : int    (identifies the ingestion config row)
          - source_system_id    : int    (with config_id, uniquely identifies this table —
                                           config_id alone isn't unique across different
                                           child config tables)
          - landing_path        : str    (S3 path the Bronze/landing write just wrote to)
          - file_format         : str    (parquet | delta | csv | json — how to read landing_path)
          - silver_catalog      : str
          - silver_schema       : str    (Bronze's target_schema + '_silver')
          - silver_table        : str    (same table name as Bronze's target_table)
          - source_schema       : str
          - source_object_name  : str
          - load_type           : str
          - primary_key_cols    : str    (comma-separated, may be empty)

        Returns dict with keys: config_id, target, status, exit_value, error
        """
        target = f"{silver_catalog}.{silver_schema}.{silver_table}"
        log.info(
            f"[SILVER] Running Silver notebook for config_id={config_id} "
            f"landing_path='{landing_path}' → {target}"
        )

        exit_value = self.dbutils.notebook.run(
            self.silver_notebook_path,
            self.timeout_seconds,
            {
                "config_id":          str(config_id),
                "source_system_id":   str(source_system_id),
                "landing_path":       landing_path,
                "file_format":        file_format or "parquet",
                "silver_catalog":     silver_catalog or "",
                "silver_schema":      silver_schema or "",
                "silver_table":       silver_table or "",
                "source_schema":      source_schema or "",
                "source_object_name": source_object_name or "",
                "load_type":          load_type or "",
                "primary_key_cols":   primary_key_cols or "",
            },
        )

        log.info(
            f"[SILVER] Silver notebook finished for target='{target}' "
            f"exit_value='{exit_value}'"
        )
        return {
            "config_id":  config_id,
            "target":     target,
            "status":     "SUCCESS",
            "exit_value": exit_value,
            "error":      None,
        }
