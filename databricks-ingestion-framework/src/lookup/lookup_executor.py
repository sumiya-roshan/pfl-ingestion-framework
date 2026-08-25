"""
LookupExecutor — Source row-presence pre-check, run inline per table
immediately before extraction (see IngestionOrchestrator.run()).

Connects to a source table using the same connector infrastructure as the
main ingestion pipeline and runs a lightweight probe query to determine
whether data is present. Tables with zero rows are excluded from ingestion
and logged to the audit table as SKIPPED — without ever calling extract().

DRY Design
----------
This class instantiates the actual connector classes (JdbcConnector, MongoConnector)
using the ingestion framework's Connector Factory. It reuses their connection, URI,
and option resolution methods directly, eliminating duplicate credential, host,
or database/collection mapping logic.

Query template
--------------
Each task must carry its own probe query in task.lookup_query, sourced from
rdbms_ingestion_config.Lookup_Query_Template. There is no pipeline-level
fallback and no auto-generation — a task with no template configured raises
ValueError (caught by check_presence()'s retry/fail-safe handling like any
other lookup error).

Placeholders substituted into the template:

    {schema}             → task.source_schema  (or empty string if NULL)
    {source_schema}      → same as {schema}
    {table}              → task.source_object_name
    {source_object_name} → same as {table}
    {key_column}          → task.primary_key_cols (or '1' if NULL)
    {key_columns}         → same as {key_column}

Threading model
---------------
IngestionOrchestrator.run() calls check_presence() for one task at a time,
immediately followed by extraction for that same task if data is present.
Concurrency across tables comes from the caller's own ThreadPoolExecutor
(main.py) running run() for multiple tasks in parallel — there is no
separate lookup-only fan-out phase.
"""
from __future__ import annotations

from typing import Dict

# Import the existing connectors and factory
from ingestion.connectors.factory import get_connector
from ingestion.connectors.jdbc_connector import JdbcConnector
from ingestion.connectors.mongo_connector import MongoConnector


class LookupExecutor:
    """
    Runs a presence-check query against a source system for one ingestion
    task at a time and returns a result dict indicating whether data exists.

    Instantiates the connector class for the task using get_connector() and
    utilizes its internal helpers to fetch connection parameters.

    Parameters
    ----------
    spark   : active SparkSession
    secrets : SecretResolver instance
    logger  : logging.Logger (from ingestion.utils.logger.get_logger)
    """

    def __init__(self, spark, secrets, logger):
        self.spark   = spark
        self.secrets = secrets
        self.logger  = logger

    # ── Public API ────────────────────────────────────────────────────────────

    def check_presence(self, source_sys, task) -> Dict:
        """
        Run a presence check for one ingestion task — the entry point used by
        IngestionOrchestrator.run() to gate extraction. Dispatches to the JDBC,
        MongoDB, or file-based strategy. Retries on the source system's
        retry_count/retry_interval. Always returns a result dict — never raises
        (a lookup error is fail-safe: the table is included, not excluded).
        """
        config_id      = task.config_id
        object_name    = task.source_object_name
        resolved_query = None

        max_retries    = int(getattr(source_sys, "retry_count", 0) or 0)
        retry_interval = int(getattr(source_sys, "retry_interval", 0) or 0)

        attempt = 0
        while True:
            try:
                connector = get_connector(self.spark, source_sys, task, self.secrets)

                if isinstance(connector, JdbcConnector):
                    resolved_query = self._resolve_query_for_task(task)
                    count = self._lookup_jdbc(connector, resolved_query)
                elif isinstance(connector, MongoConnector):
                    # find_one() probes the collection directly — no query template needed.
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
                if attempt >= max_retries:
                    self.logger.warning(
                        f"[LookupExecutor] Lookup FAILED for config_id={config_id} "
                        f"({object_name}) after {attempt} retries: {exc}. Including table (fail-safe)."
                    )
                    return {
                        "config_id":          config_id,
                        "source_object_name": object_name,
                        "resolved_query":     resolved_query or "N/A",
                        "count":              -1,
                        "included":           True,
                        "error":              str(exc),
                    }

                attempt += 1
                self.logger.warning(
                    f"[LookupExecutor] Lookup attempt {attempt}/{max_retries} FAILED for config_id={config_id} "
                    f"({object_name}): {exc}. Retrying in {retry_interval}s..."
                )
                if retry_interval > 0:
                    import time
                    time.sleep(retry_interval)

    # ── Strategy: JDBC ───────────────────────────────────────────────────────

    def _lookup_jdbc(self, connector: JdbcConnector, resolved_query: str) -> int:
        """
        Run the resolved query via JDBC and return 1 if any row is collected (data exists),
        or 0 if no row is collected.
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
        return 1

    # ── Strategy: MongoDB ────────────────────────────────────────────────────

    def _lookup_mongo(self, connector: MongoConnector) -> int:
        """
        Check if the MongoDB collection contains any documents using find_one().
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
            doc = client[database][collection].find_one()
            return 1 if doc is not None else 0
        finally:
            client.close()

    # ── Template resolution ───────────────────────────────────────────────────

    def _resolve_query_for_task(self, task) -> str:
        """
        Resolve the probe query for a specific task by substituting the
        {schema} / {table} / {key_column} placeholders into task.lookup_query
        (rdbms_ingestion_config.Lookup_Query_Template).

        Placeholder support (all case-insensitive):
            {schema}             → task.source_schema  (empty string if NULL)
            {source_schema}      → same
            {table}              → task.source_object_name
            {source_object_name} → same
            {key_column}         → task.primary_key_cols (or '1' if NULL)
            {key_columns}        → same

        Raises ValueError if task.lookup_query is not set — there is no
        pipeline-level fallback and no auto-generation.
        """
        template = getattr(task, "lookup_query", None)
        if not template:
            raise ValueError(
                f"No Lookup_Query_Template configured for config_id={task.config_id} "
                f"({task.source_object_name}) in rdbms_ingestion_config."
            )

        schema = task.source_schema or ""
        table  = task.source_object_name or ""
        key_cols = task.primary_key_cols or "1"

        resolved = template
        for placeholder, value in [
            ("{source_schema}",      schema),
            ("{schema}",             schema),
            ("{source_object_name}", table),
            ("{table}",              table),
            ("{key_column}",         key_cols),
            ("{key_columns}",        key_cols),
        ]:
            resolved = resolved.replace(placeholder, value)
            resolved = resolved.replace(placeholder.upper(), value)
            resolved = resolved.replace(placeholder.lower(), value)

        return resolved
