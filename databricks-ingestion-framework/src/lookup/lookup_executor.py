"""
LookupExecutor — Source row-count pre-check before ingestion.

Connects to each source table using the same connector infrastructure as the
main ingestion pipeline and runs a COUNT query to determine whether data is
present. Tables with zero rows are excluded from the ingestion run and logged
to the audit table as SKIPPED.

DRY Design
----------
This class instantiates the actual connector classes (JdbcConnector, MongoConnector)
using the ingestion framework's Connector Factory. It reuses their connection, URI,
and option resolution methods directly, eliminating duplicate credential, host,
or database/collection mapping logic.

Query template (pipeline-level)
--------------------------------
A single lookup_query_template is configured per pipeline in the
pipeline_lookup_config table. The template is applied to every table in that
pipeline at runtime by substituting placeholders:

    {schema}             → task.source_schema  (or empty string if NULL)
    {source_schema}      → same as {schema}
    {table}              → task.source_object_name
    {source_object_name} → same as {table}

Threading model
---------------
All lookup queries run concurrently inside a ThreadPoolExecutor (same pattern
as main.py). The caller controls max_workers.
"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

# Import the existing connectors and factory
from ingestion.connectors.factory import get_connector
from ingestion.connectors.jdbc_connector import JdbcConnector
from ingestion.connectors.mongo_connector import MongoConnector


class LookupExecutor:
    """
    Runs COUNT queries against a source system for a list of ingestion tasks
    and returns a result dict per task indicating whether data is present.

    Instantiates the connector class for each task using get_connector() and
    utilizes their internal helpers to fetch connection parameters.

    Parameters
    ----------
    spark                 : active SparkSession
    secrets               : SecretResolver instance
    logger                : logging.Logger (from ingestion.utils.logger.get_logger)
    lookup_query_template : pipeline-level COUNT template from pipeline_lookup_config.
                            If None, auto-generates SELECT COUNT(*) FROM {schema}.{table}
    """

    def __init__(
        self,
        spark,
        secrets,
        logger,
        lookup_query_template: Optional[str] = None,
    ):
        self.spark                 = spark
        self.secrets               = secrets
        self.logger                = logger
        self.lookup_query_template = lookup_query_template  # may be None → auto-gen
        self._lock                 = threading.Lock()

    # ── Public API ────────────────────────────────────────────────────────────

    def run_all(
        self,
        source_sys,
        tasks: List[Any],
        max_workers: int = 4,
    ) -> List[Dict]:
        """
        Run lookup queries for all tasks concurrently.

        The pipeline-level lookup_query_template is applied to each task by
        substituting {schema} / {table} placeholders with the task's actual
        source_schema and source_object_name values.

        Parameters
        ----------
        source_sys  : SourceSystemConfig
        tasks       : list of IngestionTaskConfig
        max_workers : thread pool size

        Returns
        -------
        list of dicts, one per task:
            {
                "config_id":          int,
                "source_object_name": str,
                "resolved_query":     str,   # the actual query that ran
                "count":              int,   # 0 = no data, >0 = has data, -1 = error
                "included":           bool,
                "error":              str | None,
            }
        """
        results = []
        self.logger.info(
            f"[LookupExecutor] Starting lookup for {len(tasks)} task(s) on source "
            f"'{source_sys.source_name}' (max_workers={max_workers}). "
            f"Template: {(self.lookup_query_template or 'auto-generate')!r}"
        )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {
                executor.submit(self._lookup_one, source_sys, task): task
                for task in tasks
            }
            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    result = future.result()
                except Exception as exc:
                    # Outer safety net — _lookup_one is already fail-safe.
                    self.logger.warning(
                        f"[LookupExecutor] Unexpected executor error for "
                        f"config_id={task.config_id} ({task.source_object_name}): {exc}. "
                        f"Including table (fail-safe)."
                    )
                    result = self._make_failsafe_result(task, error=str(exc))
                results.append(result)

        return results

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _lookup_one(self, source_sys, task) -> Dict:
        """
        Run a single COUNT lookup for one ingestion task.
        Dispatches to JDBC, MongoDB, or file-based strategy.
        Always returns a result dict — never raises (fail-safe).
        """
        config_id    = task.config_id
        object_name  = task.source_object_name

        # Resolve the per-table query from the pipeline-level template
        resolved_query = self._resolve_query_for_task(task)

        try:
            # Instantiate the actual connector using the factory
            connector = get_connector(self.spark, source_sys, task, self.secrets)

            if isinstance(connector, JdbcConnector):
                count = self._lookup_jdbc(connector, resolved_query)
            elif isinstance(connector, MongoConnector):
                count = self._lookup_mongo(connector)
            else:
                # File-based (S3, SFTP, etc.) — always included
                self.logger.info(
                    f"[LookupExecutor] config_id={config_id} ({object_name}) "
                    f"— non-queryable source ({type(connector).__name__}), always included."
                )
                count = 1

            included = count > 0
            self.logger.info(
                f"[LookupExecutor] config_id={config_id} ({object_name}) → "
                f"count={count}  included={included}"
            )
            return {
                "config_id":          config_id,
                "source_object_name": object_name,
                "resolved_query":     resolved_query,
                "count":              count,
                "included":           included,
                "error":              None,
            }

        except Exception as exc:
            self.logger.warning(
                f"[LookupExecutor] Lookup FAILED for config_id={config_id} "
                f"({object_name}): {exc}. Including table (fail-safe)."
            )
            return self._make_failsafe_result(
                task, resolved_query=resolved_query, error=str(exc)
            )

    # ── Strategy: JDBC ───────────────────────────────────────────────────────

    def _lookup_jdbc(self, connector: JdbcConnector, resolved_query: str) -> int:
        """
        Run the resolved COUNT query via JDBC and return the integer result.
        Reuses the connector's solved connection options (driver, credentials, url).
        """
        # Call the connector's helper to resolve JDBC configuration parameters
        options = connector._read_options()
        dbtable = f"({resolved_query}) _lkp_count"

        df = (
            self.spark.read.format("jdbc")
            .options(**options)
            .option("dbtable",  dbtable)
            .load()
        )
        row = df.collect()
        if not row:
            return 0
        count_value = row[0][0]
        return int(count_value) if count_value is not None else 0

    # ── Strategy: MongoDB ────────────────────────────────────────────────────

    def _lookup_mongo(self, connector: MongoConnector) -> int:
        """
        Count documents in the MongoDB collection using PyMongo.
        Reuses the connector's solved URI, database, and collection properties.
        """
        try:
            import pymongo  # noqa: PLC0415
        except ImportError:
            self.logger.warning(
                "[LookupExecutor] pymongo not available — including MongoDB "
                f"table '{connector.ingest_obj.source_object_name}' by default."
            )
            return 1

        uri        = connector._build_connection_uri()
        database   = connector._resolve_database()
        collection = connector._resolve_collection()

        client = pymongo.MongoClient(uri)
        try:
            return int(client[database][collection].count_documents({}))
        finally:
            client.close()

    # ── Template resolution ───────────────────────────────────────────────────

    def _resolve_query_for_task(self, task) -> str:
        """
        Apply the pipeline-level lookup_query_template to a specific task by
        substituting the {schema} / {table} placeholders.

        Placeholder support (all case-insensitive):
            {schema}             → task.source_schema  (empty string if NULL)
            {source_schema}      → same
            {table}              → task.source_object_name
            {source_object_name} → same

        If the template is None, auto-generates:
            SELECT COUNT(*) FROM {schema_prefix}{table}
        where schema_prefix is "{schema}." when source_schema is not NULL.
        """
        schema = task.source_schema or ""
        table  = task.source_object_name or ""

        if not self.lookup_query_template:
            schema_prefix = f"{schema}." if schema else ""
            return f"SELECT COUNT(*) FROM {schema_prefix}{table}"

        resolved = self.lookup_query_template
        for placeholder, value in [
            ("{source_schema}",      schema),
            ("{schema}",             schema),
            ("{source_object_name}", table),
            ("{table}",              table),
        ]:
            resolved = resolved.replace(placeholder, value)
            resolved = resolved.replace(placeholder.upper(), value)
            resolved = resolved.replace(placeholder.lower(), value)

        return resolved

    # ── Fallback result ───────────────────────────────────────────────────────

    @staticmethod
    def _make_failsafe_result(
        task,
        resolved_query: Optional[str] = None,
        error: Optional[str] = None,
    ) -> Dict:
        """Return a fail-safe result dict (included=True) for a task that errored."""
        return {
            "config_id":          task.config_id,
            "source_object_name": task.source_object_name,
            "resolved_query":     resolved_query or "N/A",
            "count":              -1,
            "included":           True,
            "error":              error,
        }
