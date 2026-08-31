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

Query source
------------
For JDBC sources the probe query is assembled in one place —
_build_jdbc_probe_query() — from the task's Source_Query, with the pure SQL
rewrite delegated to lookup.lookup_query_builder.build_lookup_query. There is
no separate Lookup_Query_Template column. FULL load probes
"SELECT <key> ... LIMIT 1" unfiltered; INCREMENTAL load adds a
Silver_Last_Sink_Date/Lookback_Hours watermark predicate.

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

from ingestion.utils.watermark import resolve_watermark
from lookup.lookup_query_builder import build_lookup_query, detect_pattern_type


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

        from ingestion.connectors.factory import get_connector
        from ingestion.connectors.jdbc_connector import JdbcConnector
        from ingestion.connectors.mongo_connector import MongoConnector

        attempt = 0
        while True:
            try:
                connector = get_connector(self.spark, source_sys, task, self.secrets)

                if isinstance(connector, JdbcConnector):
                    resolved_query = self._build_jdbc_probe_query(source_sys, task)
                    self.logger.debug(f"[LookupExecutor] Lookup query generated: {resolved_query}")
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
                    f"Retry attempt {attempt} of {max_retries} for [LookupExecutor] Lookup for config_id={config_id} "
                    f"({object_name}) due to error: {exc}. Retrying in {retry_interval}s..."
                )
                if retry_interval > 0:
                    import time
                    time.sleep(retry_interval)

    # ── Probe query construction ────────────────────────────────────────────

    @staticmethod
    def _build_jdbc_probe_query(source_sys, task) -> str:
        """
        The single place the JDBC row-presence query is constructed.

        Reads the task's Source_Query (custom_query, or a plain
        ``SELECT * FROM schema.table``) and hands it to
        lookup_query_builder.build_lookup_query, which does the pure SQL
        rewrite:

          FULL        → SELECT <key> FROM <src> <row-limit>
          INCREMENTAL → + Delta_Column_1 (OR Delta_Column_2) >= cutoff,
                        cutoff = Silver_Last_Sink_Date - Lookback_Hours
          trigger_time templated Source_Query → special_trigger_time pattern
        """
        if task.custom_query:
            source_query = task.custom_query
        else:
            schema_prefix = f"{task.source_schema}." if task.source_schema else ""
            source_query = f"SELECT * FROM {schema_prefix}{task.source_object_name}"

        incremental = task.load_type == "INCREMENTAL"
        dialect = (
            "postgres"
            if (source_sys.source_type or "").upper() in ("POSTGRES", "MYSQL")
            else "oracle"
        )

        return build_lookup_query(
            source_query=source_query,
            key_col=", ".join(task.primary_key_list) if task.primary_key_list else "1",
            load_type="incremental" if incremental else "full",
            cutoff=resolve_watermark(task) if incremental else None,
            delta_col=task.incremental_column,
            delta_col_2=task.delta_column_2,
            dialect=dialect,
            pattern_type=detect_pattern_type(source_query),
        )

    # ── Strategy: JDBC ───────────────────────────────────────────────────────

    def _lookup_jdbc(self, connector, resolved_query: str) -> int:
        """
        Run the resolved query via JDBC and return 1 if any row is collected (data exists),
        or 0 if no row is collected.
        Reuses the connector's solved connection options (driver, credentials, url).
        """
        # Reuse the connector's resolved JDBC config (driver, credentials, url).
        # resolved_query is a flat SELECT (see _build_jdbc_probe_query) — passed
        # via the Spark JDBC 'query' option, so no subquery wrapper is needed.
        options = connector._read_options()

        df = (
            self.spark.read.format("jdbc")
            .options(**options)
            .option("query", resolved_query)
            .load()
        )
        row = df.collect()
        if not row:
            return 0
        return 1

    # ── Strategy: MongoDB ────────────────────────────────────────────────────

    def _lookup_mongo(self, connector) -> int:
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

