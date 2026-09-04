"""
Main driver: given an ingestion_object_id, resolves config, extracts data,
writes to landing path (if provided) and/or bronze Delta table, and records
the run in data_pipeline_execution_master.

Key behaviours
--------------
- delta_layer   : read from ingestion_config.delta_layer (not a widget)
- pipeline_name : read from ingestion_config.pipeline_name (auto-detected from
                  Databricks Job context in the calling notebook)
- landing_volume_path : passed as a parameter from the job widget — NOT from
                  config_source_system (removed)
- Fault tolerance: run() catches all exceptions, records FAILED in audit, and
                  returns a result dict — it never re-raises. The calling
                  notebook decides whether to raise after collecting all results.
- Silver trigger : coupled — runs inline, synchronously, right after the S3
                  landing write, on the same thread. The Bronze Delta write
                  below does not start until Silver has finished for that
                  table. No separate thread pool; each table's run() call
                  simply takes longer when Silver is enabled.
"""
import threading
import time
from datetime import date, datetime, timezone

from lookup.lookup_executor import LookupExecutor
from lookup.lookup_query_builder import build_lookup_query
from silver.silver_processor import SilverProcessor

from ..connectors.factory import get_connector
from ..connectors.federated_connector import FederatedConnector
from ..connectors.jdbc_connector import JdbcConnector
from .audit import AuditLogger
from .config_manager import (
    AUDIT_STATUS_FAILED,
    AUDIT_STATUS_SKIPPED,
    AUDIT_STATUS_SUCCESS,
    ConfigManager,
    IngestionTaskConfig,
    SourceSystemConfig,
)
from .dependency_logger import DependencyLogger
from .email_notifier import GraphMailNotifier
from .logger import get_logger
from .retry import retry_on_failure
from .secrets import SecretResolver
from .watermark import resolve_watermark
from .writers.bronze_writer import BronzeWriter
from .writers.s3_writer import S3RawWriter


class IngestionOrchestrator:
    """
    Orchestrates a single ingestion run end-to-end.

    Parameters
    ----------
    spark         : active SparkSession
    dbutils       : Databricks dbutils; None in unit tests
    audit_table   : FQN of data_pipeline_execution_master
    pipeline_name : fallback name when config table pipeline_name is NULL
    environment   : 'dev' | 'uat' | 'prod' — controls log level
    department_id : written to audit table department_id NOT NULL column
    """

    def __init__(
        self,
        spark,
        dbutils,
        audit_table: str,
        dependency_table: str ,
        pipeline_name: str,
        environment: str   = "dev",
        department_id: int = 0,
        silver_notebook_path: str | None = None,
        silver_notebook_timeout: int        = 3600,
        config_mgr: ConfigManager | None = None,
    ):
        self.spark         = spark
        self.dbutils       = dbutils
        self.pipeline_name = pipeline_name
        self.environment   = environment
        self.config_mgr    = config_mgr

        self.audit         = AuditLogger(spark, audit_table=audit_table, department_id=department_id)
        self.config_manager = ConfigManager(spark)
        self.dependency    = DependencyLogger(spark, dependency_table=dependency_table)
        self.secrets       = SecretResolver(dbutils)
        self.s3_writer     = S3RawWriter()
        self.bronze_writer = BronzeWriter(spark)
        self.logger        = get_logger(environment=environment)
        self.lookup_executor = LookupExecutor(spark, self.secrets, self.logger)
        self.notifier      = GraphMailNotifier(dbutils=dbutils, logger=self.logger)

        # Silver trigger: runs inline, coupled to the landing write — a table's
        # Bronze Delta write does not start until that table's Silver run has
        # finished. Disabled when no notebook path is given.
        self.silver_processor = (
            SilverProcessor(dbutils, silver_notebook_path, silver_notebook_timeout)
            if silver_notebook_path else None
        )
        # if self.silver_processor:
        #     print(f"[SILVER] ready (coupled, inline) — notebook='{silver_notebook_path}'")

    # ── Public API ────────────────────────────────────────────────────────────

    def run(
        self,
        source_sys: SourceSystemConfig,
        ingest_obj: IngestionTaskConfig,
        config_master_id: int | None    = None,
        landing_volume_path: str | None = None,
        trigger_id: str | None          = None,
        # trigger_type: Optional[str]        = None,
        business_date: date | None      = None,
        job_context: dict | None        = None,
        sink_batch_started_date: datetime | None = None,
    ) -> dict:
        """
        Execute a single ingestion object end-to-end.

        Never re-raises — catches all exceptions and returns a result dict.
        Callers should check result["status"] == "FAILED" and act accordingly.

        Parameters
        ----------
        source_sys          : Pre-fetched SourceSystemConfig
        task                : Pre-fetched IngestionTaskConfig
        config_master_id    : The config_master routing table ID (widget value from main.py);
                              written to the audit table's config_master_id column
        landing_volume_path : base S3/Volume path for raw landing write (widget value);
                              if None/empty, landing write is skipped
        trigger_id          : Databricks job run ID (for audit traceability)
        trigger_type        : 'SCHEDULED' | 'MANUAL' | 'EVENT'
        business_date       : override for business_date audit column (defaults to today UTC)

        Returns
        -------
        """
        
        run_start_time = datetime.now(timezone.utc)
        # pipeline_name and delta_layer come from config table, with fallbacks
        pipeline_name = ingest_obj.pipeline_name or self.pipeline_name
        delta_layer   = ingest_obj.effective_delta_layer   # property: config → fallback 'BRONZE'

        # ── Presence check — runs immediately before extraction, per table ─────
        # Zero rows in the source → skip extraction entirely and log SKIPPED.
        # Lookup errors are fail-safe (count=-1, included=True) and fall through
        # to normal extraction below.
        # Only applies to JDBC/RDBMS sources (config_master_id = 1) — other
        # source types skip the lookup and are always included.
        if config_master_id == 1:
            lookup_result = self.lookup_executor.check_presence(source_sys, ingest_obj)
        else:
            lookup_result = {"count": 1, "included": True, "error": None, "resolved_query": None}

        if lookup_result["count"] == 0:
            self.logger.info(
                f"[Lookup] config_id={ingest_obj.config_id} ({ingest_obj.source_object_name}) "
                f"— 0 rows in source. Skipping extraction."
            )
            #check the dependency table also
            skip_context = dict(job_context or {})
            skip_context.setdefault("trigger_id", trigger_id)
            try:
                self.audit.log_skipped_row(
                    task              = ingest_obj,
                    source_sys        = source_sys,
                    job_context       = skip_context,
                    pipeline_name     = pipeline_name,
                    config_master_id  = config_master_id,
                    reason            = f"Source lookup returned 0 rows. Query: {lookup_result['resolved_query']}",
                    business_date     = business_date,
                )
            except Exception as audit_exc:
                self.logger.error(
                    f"config_id={ingest_obj.config_id} — failed to write SKIPPED audit row: {audit_exc}"
                )
            self._set_config_status(ingest_obj, "Skipped")
            return {
                "config_id": ingest_obj.config_id,
                "run_id":   None,
                "status":   AUDIT_STATUS_SKIPPED,
                "rows_read": 0,
                "error_code": None,
                "error":    None,
            }

        # Lookup itself failed (count == -1) — do not attempt extraction; mark
        # the config row Failed so the batch state reflects it.
        if lookup_result["count"] == -1:
            self.logger.warning(
                f"[Lookup] config_id={ingest_obj.config_id} ({ingest_obj.source_object_name}) "
                f"— lookup failed: {lookup_result['error']}. Marking Failed, skipping extraction."
            )
            self._set_config_status(ingest_obj, "Failed", run_id=None)
            return {
                "config_id": ingest_obj.config_id,
                "run_id":   None,
                "status":   AUDIT_STATUS_FAILED,
                "rows_read": 0,
                "error_code": "LookupFailed",
                "error":    lookup_result["error"],
            }

        # Start audit: insert the INPROGRESS record before any ingestion work.
        audit_context = dict(job_context or {})
        # audit_context.setdefault("trigger_type", trigger_type)
        audit_context.setdefault("trigger_id", trigger_id)

        # Dependency row: inserted BEFORE the audit row, per requirement.
        # pipeline_start_time is job-level (same value for every table in this
        # job_run_id) — set once in main.py and threaded through job_context.
        dep_run = self.dependency.start_table(
            config_master_id    = config_master_id if config_master_id is not None else ingest_obj.config_id,
            source_system_id    = source_sys.source_id,
            config_id           = ingest_obj.config_id,
            table_name          = ingest_obj.source_object_name,
            pipeline_name       = pipeline_name,
            job_run_id          = audit_context.get("job_run_id"),
            business_date       = business_date or run_start_time.date(),
            pipeline_start_time = audit_context.get("pipeline_start_time") or datetime.now(timezone.utc),
        )

        # Start audit: insert the INPROGRESS record before any ingestion work.
        audit_run = self.audit.start_run(
            task=ingest_obj,
            source_sys=source_sys,
            job_context=audit_context,
            pipeline_name=pipeline_name,
            config_master_id=config_master_id,
            business_date=business_date,
        )
        run_id = audit_run["job_run_id"]

        try:
            watermark_start = resolve_watermark(ingest_obj)
            if ingest_obj.load_type.upper() == "INCREMENTAL" and watermark_start:
                self.logger.debug(f"[{run_id}] Incremental watermark = {watermark_start}")
            stage = "EXTRACT"
            # watermark_start = resolve_watermark(self.spark, self.logger, ingest_obj)
            self.logger.info(
                f"[{run_id}] START config_id={ingest_obj.config_id} "
                f"source='{source_sys.source_name}' object='{ingest_obj.source_object_name}' "
                f"load_type={ingest_obj.load_type} delta_layer={delta_layer} "
                f"watermark_start={watermark_start}"
            )

            connector = get_connector(self.spark, source_sys, ingest_obj, self.secrets)
            df, _watermark_end = retry_on_failure(
                lambda: connector.extract(watermark_start),
                max_retries    = int(source_sys.retry_count or 0),
                retry_interval = int(source_sys.retry_interval or 0),
                logger         = self.logger,
                description    = f"[{run_id}] extract config_id={ingest_obj.config_id} object='{ingest_obj.source_object_name}'",
            )
            rows_read = df.count()

            # Staging_Flag=1 → pull ALL primary-key rows from source (unfiltered)
            # and land them in s3. Same flat rewrite as the lookup probe, just
            # without the row-limit clause (row_limit=False).
            if ingest_obj.staging_flag == 1 and isinstance(connector, (JdbcConnector, FederatedConnector)):
                if not landing_volume_path:
                    raise ValueError(
                        f"Staging_Flag=1 for config_id={ingest_obj.config_id}, "
                        "but landing_volume_path is not configured."
                    )

                key_col = ", ".join(ingest_obj.primary_key_list) if ingest_obj.primary_key_list else "*"
                is_federated = isinstance(connector, FederatedConnector)

                base_sql = (
                    connector._base_sql(connector._foreign_catalog())
                    if is_federated
                    else connector._base_sql()
                )

                key_query = build_lookup_query(base_sql, key_col, "full", row_limit=False)
                self.logger.info(f"[{run_id}] PK staging query → {key_query}")

                if is_federated:
                    pk_df = retry_on_failure(
                        lambda: self.spark.sql(key_query),
                        max_retries    = int(source_sys.retry_count or 0),
                        retry_interval = int(source_sys.retry_interval or 0),
                        logger         = self.logger,
                        description    = f"[{run_id}] primary-key extract config_id={ingest_obj.config_id} object='{ingest_obj.source_object_name}'",
                    )
                else:
                    pk_df = retry_on_failure(
                        lambda: (
                            self.spark.read.format("jdbc")
                            .options(**connector._read_options())
                            .option("query", key_query)
                            .load()
                        ),
                        max_retries    = int(source_sys.retry_count or 0),
                        retry_interval = int(source_sys.retry_interval or 0),
                        logger         = self.logger,
                        description    = f"[{run_id}] primary-key extract config_id={ingest_obj.config_id} object='{ingest_obj.source_object_name}'",
                    )

                pk_rows = pk_df.count()
                fmt = ingest_obj.file_format or "parquet"

                pk_path = self.s3_writer.write(
                    pk_df,
                    landing_volume_path = landing_volume_path,
                    source_name         = source_sys.source_name,
                    source_schema       = ingest_obj.source_schema,
                    source_object_name  = ingest_obj.source_object_name,
                    file_format         = fmt,
                    file_prefix         = 'all_key',
                    file_timestamp      = sink_batch_started_date,
                )

                self.logger.info(
                    f"[{run_id}] PK staging write → {pk_path} ({pk_rows} rows)"
                )

            rows_copied  = 0
            rows_deleted = 0

            # ── Landing / raw write (optional) ────────────────────────────────
            # landing_path/fmt/silver_result stay in scope past the if-block
            # (None/default when landing write is skipped).
            landing_path  = None
            silver_result = None
            fmt = ingest_obj.file_format or "parquet"
            write_start_time = time.time()
            if landing_volume_path:
                stage = "RAW_WRITE"
                landing_path = self.s3_writer.write(
                    df,
                    landing_volume_path = landing_volume_path,
                    source_name         = source_sys.source_name,
                    source_schema       = ingest_obj.source_schema,
                    source_object_name  = ingest_obj.source_object_name,
                    file_format         = fmt,
                    file_timestamp      = sink_batch_started_date or run_start_time,
                )
                self.logger.info(
                    f"[{run_id}] Landing write → {landing_path} ({rows_read} rows, format={fmt})"
                )
                self.dependency.mark_source_to_raw_end(dep_run)
                stage = "SILVER"

                # ── Trigger Silver, coupled — right after the S3 write, on this
                # same thread. The Bronze Delta write below does not start until
                # this finishes. _trigger_silver() never raises (catches its own
                # exceptions and returns a FAILED result dict), so this can't
                # abort the Bronze write on its own.
                if self.silver_processor:
                    silver_schema = f"{ingest_obj.target_schema}_silver"
                    print(
                        f"[SILVER] {threading.current_thread().name} running Silver (coupled) for "
                        f"config_id={ingest_obj.config_id} landing_path='{landing_path}' "
                        f"→ {ingest_obj.target_catalog}.{silver_schema}.{ingest_obj.target_table}"
                    )
                    self.dependency.mark_raw_to_silver_start(dep_run)
                    silver_result = self._trigger_silver(
                        config_id           = ingest_obj.config_id,
                        source_system_id    = source_sys.source_id,
                        landing_path        = landing_path,
                        file_format         = fmt,
                        silver_catalog      = ingest_obj.target_catalog,
                        silver_schema       = silver_schema,
                        silver_table        = ingest_obj.target_table,
                        source_schema       = ingest_obj.source_schema,
                        source_object_name  = ingest_obj.source_object_name,
                        load_type           = ingest_obj.load_type,
                        primary_key_cols    = ingest_obj.primary_key_cols,
                    )
                    self.dependency.mark_raw_to_silver_end(dep_run)

                    # _trigger_silver() never raises — a Silver failure comes
                    # back here as a FAILED result dict, not an exception, so
                    # it has to be checked explicitly or it would silently
                    # never trigger a notification (or fail the table at all).
                    if silver_result and silver_result.get("status") == "FAILED":
                        self._set_config_status(ingest_obj, "Failed", run_id=run_id)
                        self.notifier.send_email(
                            subject=f"[FAILURE] SILVER — {source_sys.source_name}.{ingest_obj.source_object_name} (config_id={ingest_obj.config_id})",
                            body=(
                                f"Stage failed: SILVER\n"
                                f"Source: {source_sys.source_name}\n"
                                f"Table: {ingest_obj.source_object_name}\n"
                                f"Config ID: {ingest_obj.config_id}\n"
                                f"Run ID: {run_id}\n\n"
                                f"Error:\n{silver_result.get('error') or 'Unknown Silver failure'}"
                            ),
                            recipients=ingest_obj.recipient_list,
                            config_id=ingest_obj.config_id,
                        )

                    # Stamp Silver_Last_Sink_Date in the child config table this
                    # task came from. Best-effort — a bookkeeping failure here
                    # must not fail the table's actual ingestion.
                    if self.config_mgr and ingest_obj.child_table_fqn:
                        try:
                            self.config_mgr.update_silver_last_sink_date(
                                child_table_fqn=ingest_obj.child_table_fqn,
                                config_id=ingest_obj.config_id,
                            )
                        except Exception as sink_exc:
                            self.logger.warning(
                                f"[{run_id}] Could not update Silver_Last_Sink_Date "
                                f"for config_id={ingest_obj.config_id}: {sink_exc}"
                            )

            # Raw layer never ran (no landing_volume_path) — nothing was
            # written, so don't report SUCCESS. Mark the config row Skipped.
            if not landing_path:
                self.logger.warning(
                    f"[{run_id}] config_id={ingest_obj.config_id} — raw layer did not run "
                    f"(no landing_volume_path). Marking Skipped."
                )
                self._set_config_status(ingest_obj, "Skipped", run_id=run_id)
                try:
                    self.audit.fail_run(
                        audit_run=audit_run,
                        error_code="RawSkipped",
                        error_message="Raw layer did not run (no landing_volume_path configured).",
                    )
                except Exception as audit_exc:
                    self.logger.error(f"[{run_id}] Could not write audit row: {audit_exc}")
                return {
                    "config_id": ingest_obj.config_id,
                    "run_id":   run_id,
                    "status":   AUDIT_STATUS_SKIPPED,
                    "rows_read": rows_read,
                    "error_code": None,
                    "error":    None,
                }

            target_table = f"{ingest_obj.target_catalog}.{ingest_obj.target_schema}.{ingest_obj.target_table}"
            rows_copied = rows_read
            copy_duration_sec = round(time.time() - write_start_time, 2)

            if rows_read == 0:
                self.logger.warning(f"[{run_id}] Source returned zero records for {ingest_obj.source_object_name}")
            self.logger.info(f"[{run_id}] {rows_read:,} records written to {target_table}")

            # Retrieve metrics from Delta table history
            data_read_bytes = 0
            data_written_bytes = 0
            try:
                history_df = self.spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1")
                history_row = history_df.select("operationMetrics").collect()[0]
                metrics = history_row["operationMetrics"] if history_row["operationMetrics"] else {}

                # Extract bytes written
                bytes_written = (
                    metrics.get("numOutputBytes") or
                    metrics.get("numTargetBytesAdded") or
                    metrics.get("numTargetBytesWritten")
                )
                if bytes_written:
                    data_written_bytes = int(bytes_written)

                # Extract bytes read
                bytes_read = (
                    metrics.get("numTargetBytesRead") or
                    metrics.get("numSourceBytesRead") or
                    metrics.get("numReadBytes")
                )
                if bytes_read:
                    data_read_bytes = int(bytes_read)
            except Exception as e:
                self.logger.warning(f"[{run_id}] Could not retrieve Delta execution metrics: {e}")

            # Calculate throughput (MB/sec)
            throughput_mb_per_sec = None
            if copy_duration_sec > 0:
                throughput_mb_per_sec = round((data_written_bytes / (1024.0 * 1024.0)) / copy_duration_sec, 2)

            # End audit: update the INPROGRESS record with SUCCESS and end time.
            self.audit.complete_run(
                audit_run             = audit_run,
                status                = AUDIT_STATUS_SUCCESS,
                rows_read             = rows_read,
                rows_copied           = rows_copied,
                rows_deleted          = rows_deleted,
                data_read_bytes       = data_read_bytes,
                data_written_bytes    = data_written_bytes,
                throughput_mb_per_sec = throughput_mb_per_sec,
                copy_duration_sec     = copy_duration_sec,
            )
            if config_master_id is not None:
                try:
                    self.config_manager.update_sink_metadata(
                        config_master_id=config_master_id,
                        ingest_obj=ingest_obj,
                        sink_batch_started_date=sink_batch_started_date,
                        rownum=rows_copied,
                        data_size=data_written_bytes,
                    )
                    self.logger.info(f"[{run_id}] Sink metadata updated for config_id={ingest_obj.config_id}")
                except Exception as sink_exc:
                    self.logger.warning(f"[{run_id}] Could not update sink metadata: {sink_exc}")

            total_duration_sec = round(time.time() - run_start_time.timestamp(), 2)
            if total_duration_sec >= 60:
                mins = int(total_duration_sec // 60)
                secs = int(total_duration_sec % 60)
                duration_str = f"{mins}m {secs}s"
            else:
                duration_str = f"{total_duration_sec}s"
            self.logger.info(f"[{run_id}] Pipeline completed in {duration_str}")
            self.logger.info(f"[{run_id}] SUCCESS — {rows_read} records processed.")

            self.notifier.send_email(
                subject=f"[SUCCESS] {source_sys.source_name}.{ingest_obj.source_object_name} (config_id={ingest_obj.config_id})",
                body=(
                    f"Source: {source_sys.source_name}\n"
                    f"Table: {ingest_obj.source_object_name}\n"
                    f"Config ID: {ingest_obj.config_id}\n"
                    f"Run ID: {run_id}\n"
                    f"Target: {target_table}\n"
                    f"Rows read: {rows_read}\n"
                    f"Rows copied: {rows_copied}\n"
                ),
                recipients=ingest_obj.recipient_list,
                config_id=ingest_obj.config_id,
            )

            if self.silver_processor and not landing_path:
                print(
                    f"[SILVER] Skipped for config_id={ingest_obj.config_id} — "
                    f"no landing_volume_path was set, nothing for Silver to read."
                )

            return {
                "config_id": ingest_obj.config_id,
                "run_id":   run_id,
                "status":   AUDIT_STATUS_SUCCESS,
                "rows_read": rows_read,
                "error_code": None,
                "error":    None,
                "silver_result": silver_result,
            }

        except Exception as exc:
            # Never re-raise — record FAILED in audit and return error dict.
            # The fan-out driver (run_all_sources.py) collects results and
            # raises a summary exception if any table failed.
            error_msg = str(exc)
            # if hasattr(exc, "getErrorClass"):
            #     error_code_dtl = exc.getErrorClass()
            error_code  = type(exc).__name__
            self.logger.exception(
                f"[{run_id}] Failed to write {ingest_obj.target_table} table due to error: {error_msg}",
            )
            try:
                self.audit.fail_run(audit_run=audit_run, error_code = error_code,error_message=error_msg)
            except Exception as audit_exc:
                self.logger.error(f"[{run_id}] Could not write FAILED status to audit: {audit_exc}")
            self._set_config_status(ingest_obj, "Failed", run_id=run_id)
            self.notifier.send_email(
                subject=f"[FAILURE] {stage} — {source_sys.source_name}.{ingest_obj.source_object_name} (config_id={ingest_obj.config_id})",
                body=(
                    f"Stage failed: {stage}\n"
                    f"Source: {source_sys.source_name}\n"
                    f"Table: {ingest_obj.source_object_name}\n"
                    f"Config ID: {ingest_obj.config_id}\n"
                    f"Run ID: {run_id}\n\n"
                    f"Error:\n{error_msg}"
                ),
                recipients=ingest_obj.recipient_list,
                config_id=ingest_obj.config_id,
            )
            return {
                "config_id": ingest_obj.config_id,
                "run_id":   run_id,
                "status":   AUDIT_STATUS_FAILED,
                "rows_read": 0,
                "error_code": error_code,
                "error":    error_msg,
            }

    def _set_config_status(self, ingest_obj, status: str, run_id=None) -> None:
        """
        Best-effort write of Status (e.g. 'Failed' / 'Skipped') to the child
        config table row for this task. A bookkeeping failure here must never
        fail the run, so it is logged and swallowed.
        """
        if not (self.config_mgr and ingest_obj.child_table_fqn):
            return
        try:
            self.config_mgr.update_status(
                ingest_obj.child_table_fqn, ingest_obj.config_id, status
            )
        except Exception as status_exc:
            prefix = f"[{run_id}] " if run_id else ""
            self.logger.warning(
                f"{prefix}config_id={ingest_obj.config_id} — could not set "
                f"Status={status} in config: {status_exc}"
            )

    # ── Silver trigger ───────────────────────────────────────────────────────

    def _trigger_silver(
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
        primary_key_cols: str,
    ) -> dict:
        """Runs inline on the calling (Bronze) thread — coupled with the landing
        write, not a separate pool. Never raises — caught and returned as a
        result dict, so a Silver failure can't abort the caller's Bronze write."""
        thread_name = threading.current_thread().name
        target = f"{silver_catalog}.{silver_schema}.{silver_table}"
        print(f"[SILVER] {thread_name} started for config_id={config_id} landing_path='{landing_path}' → {target}")
        try:
            result = self.silver_processor.trigger(
                config_id           = config_id,
                source_system_id    = source_system_id,
                landing_path        = landing_path,
                file_format         = file_format,
                silver_catalog      = silver_catalog,
                silver_schema       = silver_schema,
                silver_table        = silver_table,
                source_schema       = source_schema,
                source_object_name  = source_object_name,
                load_type           = load_type,
                primary_key_cols    = primary_key_cols,
            )
        except Exception as exc:
            self.logger.exception(
                f"[SILVER] Trigger failed for config_id={config_id} landing_path='{landing_path}'",
            )
            return {
                "config_id":    config_id,
                "target":       target,
                "status":       "FAILED",
                "exit_value":   None,
                "error":        str(exc),
            }

        print(f"[SILVER] {thread_name} finished for config_id={config_id} status={result['status']}")
        return result
