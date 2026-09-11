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

DMS-master branch: for the two sources in DMS_API_RESPONSE_FILES_SOURCES,
after the client notebook succeeds, a second ("classify") notebook is
triggered with the same params. The two downstream jobs (api_extract /
dms_extract) are NOT triggered from here — the client wants them visible as
real tasks in the job graph, not hidden inside notebook logs. main.py
publishes a `dms_master` taskValue instead, and a Condition task + two Run
Job tasks wired directly in the Databricks job handle the branching (see
main.py's Lentra branch for what gets published).

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

# Sources that need the DMS-master branch (classify notebook here, plus
# api_extract/dms_extract as visible job-graph tasks downstream — see module
# docstring). Exact list, not a pattern match — only these two ever trigger it.
DMS_API_RESPONSE_FILES_SOURCES = [
    "lentra_dealer_dms_api_response_files_hdr",
    "lentra_cd_dms_api_response_files_hdr",
]


class LentraLoader:
    """End-to-end runner for one Lentra ingestion task."""

    def __init__(
        self,
        spark,
        dbutils,
        config_mgr: ConfigManager,
        load_notebook_path: str,
        raw_sa_name: str,
        run_id: str,
        notebook_timeout: int = 3600,
        classify_notebook_path: str | None = None,
    ):
        self.spark = spark
        self.dbutils = dbutils
        self.config_mgr = config_mgr
        self.load_notebook_path = load_notebook_path
        self.raw_sa_name = raw_sa_name
        self.run_id = run_id
        self.notebook_timeout = notebook_timeout
        self.classify_notebook_path = classify_notebook_path

    @staticmethod
    def build_params(
        task: LentraIngestionTaskConfig,
        raw_sa_name: str,
        run_id: str,
    ) -> dict[str, str]:
        """
        Builds the exact parameter dict the client notebook (and, for DMS-
        master sources, the classify notebook) receives — exposed as a
        shared static method so callers needing the same values (e.g.
        main.py publishing them as taskValues for the downstream
        api_extract/dms_extract Run Job tasks) can't drift from what these
        actually get. All values are strings, since dbutils.notebook.run()
        parameters and Jobs API job_parameters must be strings.
        """
        return {
            "raw_sa_name": raw_sa_name or "",
            "source_name": task.source_name or "",
            "run_id": str(run_id or ""),
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

        DMS-master branch (see module docstring) lives inside this same try
        block, so a classify-notebook failure is treated identically to a
        load-notebook failure: Status -> Failed, overall result FAILED.
        Triggering api_extract/dms_extract themselves is NOT done here — see
        module docstring.
        """
        fqn = task.child_table_fqn
        config_id = task.config_id

        try:
            self.config_mgr.update_status(fqn, config_id, AUDIT_STATUS_INPROGRESS)

            params = self.build_params(task, self.raw_sa_name, self.run_id)

            print(
                f"[LentraLoader] config_id={config_id} ({task.report_name}) "
                f"triggering {self.load_notebook_path}"
            )
            exit_value = self.dbutils.notebook.run(
                self.load_notebook_path, self.notebook_timeout, params
            )

            classify_exit_value = None

            if task.source_name in DMS_API_RESPONSE_FILES_SOURCES:
                if not self.classify_notebook_path:
                    raise ValueError(
                        f"source_name={task.source_name!r} is a DMS-master source "
                        f"but classify_notebook_path was not configured."
                    )
                print(
                    f"[LentraLoader] config_id={config_id} ({task.report_name}) "
                    f"is a DMS-master source — triggering {self.classify_notebook_path}"
                )
                classify_exit_value = self.dbutils.notebook.run(
                    self.classify_notebook_path, self.notebook_timeout, params
                )

            self.config_mgr.update_status(fqn, config_id, AUDIT_STATUS_SUCCESS)
            print(f"[LentraLoader] config_id={config_id} SUCCESS — {exit_value}")
            return {
                "config_id": config_id,
                "report_name": task.report_name,
                "status": AUDIT_STATUS_SUCCESS,
                "exit_value": exit_value,
                "classify_exit_value": classify_exit_value,
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
                "classify_exit_value": None,
                "error": str(exc),
            }
