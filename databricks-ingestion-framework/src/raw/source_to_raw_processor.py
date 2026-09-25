"""
SourceToRawProcessor — triggers the generic Source→Raw extract notebook for
a specific table using dbutils.notebook.run(), the same pattern
SilverProcessor already uses for the Silver stage.

Called directly from IngestionOrchestrator in place of the inline
get_connector()/connector.extract()/S3RawWriter.write() calls it used to
make itself. Exists purely so this stage shows up as its own step in the
Databricks Jobs "Notebook Workflows" UI, not to change what it does.

dbutils is passed in from the calling notebook since it is a notebook-level
object and cannot be imported as a module.
"""

import json
import logging

log = logging.getLogger("ingestion_framework")


class SourceToRawProcessor:
    """
    Runs the generic Source→Raw notebook for one table via
    dbutils.notebook.run().

    Parameters
    ----------
    dbutils                 : Databricks dbutils object (passed from the calling notebook)
    source_to_raw_notebook_path : Workspace path to src/raw/source_to_raw
    timeout_seconds         : Max seconds to wait for the notebook to finish (default 3600)
    """

    def __init__(self, dbutils, source_to_raw_notebook_path: str, timeout_seconds: int = 3600):
        self.dbutils = dbutils
        self.source_to_raw_notebook_path = source_to_raw_notebook_path
        self.timeout_seconds = timeout_seconds

    def trigger(
        self,
        source_sys,
        ingest_obj,
        raw_bucket_path: str,
        file_timestamp,
        run_id: str | None = None,
    ) -> dict:
        """
        Runs the Source→Raw notebook synchronously for one table and returns
        a result dict.

        source_sys/ingest_obj are serialised via their own to_dict() — the
        notebook reconstructs them with SourceSystemConfig.from_dict()/
        IngestionTaskConfig.from_dict() and calls get_connector() itself,
        exactly what orchestrator.py used to do inline.

        Returns dict with keys: status, rows_read, landing_path, error.
        Never raises — a dbutils.notebook.run() failure (crash, timeout) is
        caught and returned as status=FAILED, same contract as a notebook
        that ran but reported its own failure via the exit-value JSON.
        """
        log.info(
            f"[SOURCE_TO_RAW] Running Source→Raw notebook for "
            f"config_id={ingest_obj.config_id} object='{ingest_obj.source_object_name}'"
        )

        try:
            exit_value = self.dbutils.notebook.run(
                self.source_to_raw_notebook_path,
                self.timeout_seconds,
                {
                    "source_sys_json": json.dumps(source_sys.to_dict()),
                    "ingest_obj_json": json.dumps(ingest_obj.to_dict()),
                    "raw_bucket_path": raw_bucket_path or "",
                    "file_timestamp": file_timestamp.isoformat() if file_timestamp else "",
                    "run_id": str(run_id or ""),
                },
            )
        except Exception as exc:
            log.exception(
                f"[SOURCE_TO_RAW] Trigger failed for config_id={ingest_obj.config_id}"
            )
            return {
                "status": "FAILED",
                "rows_read": 0,
                "landing_path": None,
                "error": str(exc),
            }

        try:
            outcome = json.loads(exit_value) if exit_value else {}
        except (TypeError, ValueError):
            outcome = {}

        if not isinstance(outcome, dict) or str(outcome.get("status", "")).lower() != "success":
            error = (
                outcome.get("error")
                if isinstance(outcome, dict) and outcome.get("error")
                else "Source→Raw notebook reported failure or gave no exit value."
            )
            log.error(
                f"[SOURCE_TO_RAW] config_id={ingest_obj.config_id} FAILED: {error}"
            )
            return {
                "status": "FAILED",
                "rows_read": 0,
                "landing_path": None,
                "error": error,
            }

        log.info(
            f"[SOURCE_TO_RAW] config_id={ingest_obj.config_id} SUCCESS — "
            f"{outcome.get('rows_read')} rows → {outcome.get('landing_path')}"
        )
        return {
            "status": "SUCCESS",
            "rows_read": int(outcome.get("rows_read") or 0),
            "landing_path": outcome.get("landing_path"),
            "error": None,
        }
