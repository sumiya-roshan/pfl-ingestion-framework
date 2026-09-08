"""
LSQ Mavis export orchestrator — the ADF "ForEach body" for one task.

Runs the export steps in order for a single ``MavisIngestionTaskConfig`` and
writes Status back to its config-table row via the ordinary ``ConfigManager``:

  1. Status = AUDIT_STATUS_INPROGRESS
  2. start_export            -> RequestId
  3. poll_until_ready
  4. get_download_url        -> FileURL
  5. download_and_extract_to_s3
  6. Status = AUDIT_STATUS_SUCCESS   (on failure: AUDIT_STATUS_FAILED, re-raised)

Status values reuse the shared AUDIT_STATUS_* vocabulary — no separate Mavis
audit status. The extracted files are loaded to their target table by the
normal downstream S3 ingestion config, not here.

The get_tasks notebook does the Stage-1 batch reset, then:

    source_sys, tasks = config_mgr.get_active_tasks(
        config_master_id=..., source_system_id=..., batch_start_date=...,
    )
    extractor = MavisApiExtractor(spark, config_mgr)
    for task in tasks:
        extractor.run(task)
"""

from __future__ import annotations

from ..connectors.api_connector import MavisApiConfig, MavisApiExportConnector
from .config_manager import (
    AUDIT_STATUS_FAILED,
    AUDIT_STATUS_INPROGRESS,
    AUDIT_STATUS_SUCCESS,
    ConfigManager,
    MavisIngestionTaskConfig,
)


class MavisApiExtractor:
    """End-to-end runner for one Mavis export task."""

    def __init__(
        self,
        spark,
        config_mgr: ConfigManager,
        api_config: MavisApiConfig | None = None,
        connector: MavisApiExportConnector | None = None,
    ):
        self.spark = spark
        self.config_mgr = config_mgr
        self.connector = connector or MavisApiExportConnector(spark, api_config)

    def run(self, task: MavisIngestionTaskConfig) -> list[str]:
        """
        Run every step for ``task``; return the extracted S3 file paths. On any
        failure the row is flagged Status='Failed' and the exception re-raised.
        """
        fqn = task.child_table_fqn
        step = "start_export"
        try:
            self.config_mgr.update_status(fqn, task.config_id, AUDIT_STATUS_INPROGRESS)

            request_id = self.connector.start_export(task)

            step = "poll_until_ready"
            self.connector.poll_until_ready(task, request_id)

            step = "get_download_url"
            download_url = self.connector.get_download_url(task, request_id)

            step = "download_and_extract_to_s3"
            files = self.connector.download_and_extract_to_s3(task, download_url)

            self.config_mgr.update_status(fqn, task.config_id, AUDIT_STATUS_SUCCESS)
            print(
                f"[MavisApiExtractor] config_id={task.config_id} SUCCESS "
                f"({len(files)} file(s))"
            )
            return files
        except Exception as exc:
            print(
                f"[MavisApiExtractor] config_id={task.config_id} FAILED at "
                f"'{step}': {exc}"
            )
            try:
                self.config_mgr.update_status(fqn, task.config_id, AUDIT_STATUS_FAILED)
            except Exception as update_exc:  # best effort
                print(
                    f"[MavisApiExtractor] config_id={task.config_id} could not "
                    f"write Failed status: {update_exc}"
                )
            raise
