"""
LSQ Mavis export orchestrator — the ADF "ForEach body" for one task.

Wires together the two halves of the Mavis flow:

  * ingestion.connectors.api_connector.MavisApiExportConnector
        the discrete HTTP + S3 steps (start / poll / download-url /
        download+unzip / load-to-target)
  * ingestion.utils.config_manager.ConfigManager
        config-table status writeback — the SAME ConfigManager used everywhere
        else (update_status / increment_execution_count), no Mavis subclass.

``MavisApiExtractor.run(task)`` executes every step end to end for a single
``MavisIngestionTaskConfig``:

  1. Status = AUDIT_STATUS_INPROGRESS
  2. start_export            → RequestId
  3. poll_until_ready
  4. get_download_url        → FileURL
  5. download_and_extract_to_s3
  6. load_to_target          → row count → target table
  7. On success: Status = AUDIT_STATUS_SUCCESS, Day_Execution_Count += 1
     On failure at any step: Status = AUDIT_STATUS_FAILED, with the failing step
     name, config_id and exception logged, then ``MavisApiExportError`` re-raised.

Status values reuse the shared AUDIT_STATUS_* vocabulary — no separate Mavis
audit status.

Task list production: the get_tasks notebook does the Stage-1 batch reset, then
calls the ordinary ``ConfigManager.get_active_tasks`` (which returns
``MavisIngestionTaskConfig`` objects for the LSQ_Mavis source), then loops:

    source_sys, tasks = config_mgr.get_active_tasks(
        config_master_id=..., source_system_id=..., batch_start_date=...,
    )
    extractor = MavisApiExtractor(spark, config_mgr, source_sys)
    for task in tasks:
        extractor.run(task)
"""

from __future__ import annotations

from ..connectors.api_connector import (
    MavisApiConfig,
    MavisApiExportConnector,
    MavisApiExportError,
)
from .config_manager import (
    AUDIT_STATUS_FAILED,
    AUDIT_STATUS_INPROGRESS,
    AUDIT_STATUS_SUCCESS,
    ConfigManager,
    MavisIngestionTaskConfig,
    SourceSystemConfig,
)


class MavisApiExtractor:
    """
    End-to-end runner for a single Mavis export task. Holds ``self.spark`` and
    delegates the per-step work to a ``MavisApiExportConnector``.

    ``source_system`` is the shared config_source_system row returned alongside
    the tasks by ``ConfigManager.get_active_tasks`` — passed straight through to
    the connector for its retry policy.
    """

    def __init__(
        self,
        spark,
        config_mgr: ConfigManager,
        source_system: SourceSystemConfig,
        api_config: MavisApiConfig | None = None,
        connector: MavisApiExportConnector | None = None,
        boto3_session=None,
    ):
        self.spark = spark
        self.config_mgr = config_mgr
        self.source_system = source_system
        self.api_config = api_config or MavisApiConfig()
        self.connector = connector or MavisApiExportConnector(
            spark,
            source_system,
            api_config=self.api_config,
            boto3_session=boto3_session,
        )

    def run(self, task: MavisIngestionTaskConfig) -> list[str]:
        """
        Execute all steps end to end for ``task``. Returns the list of extracted
        S3 file paths. Raises ``MavisApiExportError`` (config_id + step tagged)
        on any failure, after flagging the row Status='Failed'.
        """
        fqn = task.child_table_fqn
        step = "start"
        try:
            self.config_mgr.update_status(fqn, task.config_id, AUDIT_STATUS_INPROGRESS)

            step = "start_export"
            request_id = self.connector.start_export(task)

            step = "poll_until_ready"
            self.connector.poll_until_ready(task, request_id)

            step = "get_download_url"
            download_url = self.connector.get_download_url(task, request_id)

            step = "download_and_extract_to_s3"
            extracted_paths = self.connector.download_and_extract_to_s3(
                task, download_url
            )

            step = "load_to_target"
            rows_written = self.connector.load_to_target(task, extracted_paths)

            step = "status_update"
            self.config_mgr.update_status(fqn, task.config_id, AUDIT_STATUS_SUCCESS)
            self.config_mgr.increment_execution_count(fqn, task.config_id)
            print(
                f"[MavisApiExtractor] config_id={task.config_id} SUCCESS "
                f"({rows_written} row(s) → {task.full_target_table})"
            )
            return extracted_paths
        except Exception as exc:
            print(
                f"[MavisApiExtractor] config_id={task.config_id} FAILED at "
                f"step '{step}': {exc}"
            )
            try:
                self.config_mgr.update_status(fqn, task.config_id, AUDIT_STATUS_FAILED)
            except Exception as update_exc:  # best effort
                print(
                    f"[MavisApiExtractor] config_id={task.config_id} could not "
                    f"write Failed status: {update_exc}"
                )
            raise MavisApiExportError(
                f"config_id={task.config_id} failed at step '{step}': {exc}"
            ) from exc
