"""
Lentra ingestion orchestrator — the equivalent of IngestionOrchestrator (for
JDBC/S3/Mongo/federated sources) or MavisApiExtractor (for the LSQ Mavis
export API) for the Lentra source shape.

Unlike those two, the actual connection/processing logic for Lentra lives in
a client-provided Databricks notebook outside this package — this class
doesn't drive a Python connector or API client directly. LentraLoader's job
is to trigger that notebook correctly with the right parameters (everything
LentraIngestionTaskConfig carries), flip the config row's Status around it
(In Progress -> Success/Failed), and hand back a result dict — the same
"reset -> run -> status writeback" shape MavisApiExtractor uses, just with a
notebook run in place of the API calls.

Entry point: src/main/main.py branches to this class when the discovered
tasks are LentraIngestionTaskConfig objects (see ConfigManager.get_active_tasks
/ ConfigManager.is_lentra_shaped) instead of building an
IngestionOrchestrator.
"""

from __future__ import annotations

from .config_manager import (
    AUDIT_STATUS_FAILED,
    AUDIT_STATUS_INPROGRESS,
    AUDIT_STATUS_SUCCESS,
    ConfigManager,
    LentraIngestionTaskConfig,
)


class LentraLoader:
    """End-to-end runner for one Lentra ingestion task."""

    def __init__(
        self,
        spark,
        dbutils,
        config_mgr: ConfigManager,
        client_notebook_path: str,
        raw_sa_name: str,
        run_id: str,
        client_notebook_timeout: int = 3600,
        aws_region: str = "us-east-1",
    ):
        self.spark = spark
        self.dbutils = dbutils
        self.config_mgr = config_mgr
        self.client_notebook_path = client_notebook_path
        self.raw_sa_name = raw_sa_name
        self.run_id = run_id
        self.client_notebook_timeout = client_notebook_timeout
        self.aws_region = aws_region

    @staticmethod
    def build_params(
        task: LentraIngestionTaskConfig,
        raw_sa_name: str,
        run_id: str,
        aws_region: str = "us-east-1",
    ) -> dict[str, str]:
        """
        Builds the exact parameter dict the client notebook receives —
        exposed as a shared static method (not just inlined in run()) so
        other callers needing the same values (e.g. main.py publishing them
        as taskValues for a downstream job task, such as a classify/DMS-extract
        notebook wired directly in the job graph for specific sources) can't
        drift from what the client notebook actually gets. All values are
        strings, since dbutils.notebook.run() parameters must be strings.
        """
        return {
            "raw_sa_name": raw_sa_name or "",
            "source_name": task.source_name or "",
            "run_id": str(run_id or ""),
            "aws_region": aws_region or "",
            "Config_ID": str(task.config_id),
            "Config_Master_ID": str(task.source_config_master_id or ""),
            "Report_Name": task.report_name or "",
            "Load_Type": task.load_type or "",
            "Compute_Policy_ID": task.compute_policy_id or "",
            "Cluster_Option": task.cluster_option or "",
            "Worker_No": task.worker_no or "",
            "Source_Bucket_Name": task.source_bucket_name or "",
            "External_Path": task.external_path or "",
            "Raw_Sink_Container_Name": task.raw_sink_container_name or "",
            "Raw_Sink_File_Path": task.raw_sink_file_path or "",
            "Silver_Sink_Schema_Name": task.silver_sink_schema_name or "",
            "Silver_Sink_table_Name": task.silver_sink_table_name or "",
            "Access_Key_ID": task.access_key_id or "",
            "Secret_Access_Key": task.secret_access_key or "",
        }

    def run(self, task: LentraIngestionTaskConfig) -> dict:
        """
        Trigger the client-provided notebook for one Lentra config row, then
        flip Status to Success/Failed based on the outcome. Never raises —
        catches everything and returns a result dict, matching the
        fault-tolerant style the rest of this framework uses (main.py collects
        results from every task regardless of individual failures).
        """
        fqn = task.child_table_fqn
        config_id = task.config_id

        try:
            self.config_mgr.update_status(fqn, config_id, AUDIT_STATUS_INPROGRESS)

            params = self.build_params(task, self.raw_sa_name, self.run_id, self.aws_region)

            print(
                f"[LentraLoader] config_id={config_id} ({task.report_name}) "
                f"triggering {self.client_notebook_path}"
            )
            exit_value = self.dbutils.notebook.run(
                self.client_notebook_path, self.client_notebook_timeout, params
            )

            self.config_mgr.update_status(fqn, config_id, AUDIT_STATUS_SUCCESS)
            print(f"[LentraLoader] config_id={config_id} SUCCESS — {exit_value}")
            return {
                "config_id": config_id,
                "report_name": task.report_name,
                "status": AUDIT_STATUS_SUCCESS,
                "exit_value": exit_value,
                "error": None,
            }
        except Exception as exc:
            print(f"[LentraLoader] config_id={config_id} FAILED: {exc}")
            try:
                self.config_mgr.update_status(fqn, config_id, AUDIT_STATUS_FAILED)
            except Exception as update_exc:  # best effort
                print(
                    f"[LentraLoader] config_id={config_id} could not write "
                    f"Failed status: {update_exc}"
                )
            return {
                "config_id": config_id,
                "report_name": task.report_name,
                "status": AUDIT_STATUS_FAILED,
                "exit_value": None,
                "error": str(exc),
            }
