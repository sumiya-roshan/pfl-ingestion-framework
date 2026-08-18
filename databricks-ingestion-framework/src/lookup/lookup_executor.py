"""
LookupExecutor — Source row-count pre-check before ingestion.

Connects to each source table using the same connector infrastructure as the
main ingestion pipeline and runs a COUNT query to determine whether data is
present. Tables with zero rows are excluded from the ingestion run and logged
to the audit table as SKIPPED.

Query template (pipeline-level)
--------------------------------
A single lookup_query_template is configured per pipeline in the
pipeline_lookup_config table. The template is applied to every table in that
pipeline at runtime by substituting placeholders:

    {schema}             → task.source_schema  (or empty string if NULL)
    {source_schema}      → same as {schema}
    {table}              → task.source_object_name
    {source_object_name} → same as {table}

Example templates:
    NULL                 → auto-generates: SELECT COUNT(*) FROM {schema}.{table}
    "SELECT COUNT(*) FROM {schema}.{table} WHERE load_date = CURRENT_DATE"
    "SELECT COUNT(*) FROM {table}"   (no schema prefix)

Threading model
---------------
All lookup queries run concurrently inside a ThreadPoolExecutor (same pattern
as main.py). The caller controls max_workers.

Supported source types
----------------------
- JDBC (RDBMS: POSTGRES, MYSQL, ORACLE, MSSQL, etc.)
    Runs the resolved query via spark.read.jdbc(...).count().
    Uses the same JDBC URL + credential resolution as JdbcConnector.
- MongoDB
    Uses PyMongo collection.count_documents({}) — no Spark overhead.
    Falls back to count=1 (included) if PyMongo is unavailable.
- S3 / SFTP
    Row-count pre-check is not meaningful for file-based sources.
    Always returns count=1 (always included).

Fail-safe behaviour
-------------------
If a lookup query throws ANY exception, the table is treated as having data
(count=1, included=True) and a warning is logged. Ingestion will attempt the
table as usual — if the source is genuinely broken, the ingestion task will
record FAILED in the audit table.
"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

# ── Built-in JDBC driver class map (mirrors JdbcConnector) ───────────────────
_DEFAULT_DRIVER: dict = {
    "POSTGRES":   "org.postgresql.Driver",
    "POSTGRESQL": "org.postgresql.Driver",
    "PG":         "org.postgresql.Driver",
    "MYSQL":      "com.mysql.cj.jdbc.Driver",
    "ORACLE":     "oracle.jdbc.OracleDriver",
    "MSSQL":      "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "SQLSERVER":  "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

# ── File-based source types — always included, no meaningful COUNT check ──────
_FILE_BASED_SOURCES = {"S3", "SFTP"}

# ── MongoDB source types ──────────────────────────────────────────────────────
_MONGO_SOURCES = {"MONGODB", "MONGO"}


def _build_jdbc_url(source_sys) -> str:
    """Mirrors JdbcConnector._resolve_jdbc_url()."""
    if source_sys.connection_uri:
        return source_sys.connection_uri
    st   = source_sys.source_type.upper()
    host = source_sys.host or ""
    port = source_sys.port or 0
    db   = source_sys.database_name or ""
    if st in ("POSTGRES", "POSTGRESQL", "PG"):
        return f"jdbc:postgresql://{host}:{port}/{db}"
    if st == "MYSQL":
        return f"jdbc:mysql://{host}:{port}/{db}"
    if st == "ORACLE":
        return f"jdbc:oracle:thin:@//{host}:{port}/{db}"
    if st in ("MSSQL", "SQLSERVER"):
        return f"jdbc:sqlserver://{host}:{port};databaseName={db}"
    raise ValueError(
        f"[LookupExecutor] No built-in JDBC URL for source_type='{source_sys.source_type}'. "
        f"Set connection_uri in config_source_system."
    )


def _resolve_driver(source_sys) -> str:
    """Mirrors JdbcConnector._resolve_driver()."""
    if source_sys.driver_class:
        return source_sys.driver_class
    driver = _DEFAULT_DRIVER.get(source_sys.source_type.upper())
    if driver is None:
        raise ValueError(
            f"[LookupExecutor] No built-in JDBC driver for source_type='{source_sys.source_type}'. "
            f"Set driver_class in config_source_system."
        )
    return driver


class LookupExecutor:
    """
    Runs COUNT queries against a source system for a list of ingestion tasks
    and returns a result dict per task indicating whether data is present.

    A single lookup_query_template is resolved at the pipeline level and then
    applied per-table by substituting {schema} and {table} placeholders.

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
            f"Template: {self.lookup_query_template!r or 'auto-generate'}"
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
        source_type  = source_sys.source_type.upper()
        config_id    = task.config_id
        object_name  = task.source_object_name

        # Resolve the per-table query from the pipeline-level template
        resolved_query = self._resolve_query_for_task(task)

        self.logger.info(
            f"[LookupExecutor] config_id={config_id} ({object_name}) — "
            f"source_type={source_type}  query={resolved_query!r}"
        )

        try:
            if source_type in _FILE_BASED_SOURCES:
                count = self._lookup_file_based(source_sys, task)
            elif source_type in _MONGO_SOURCES:
                count = self._lookup_mongo(source_sys, task)
            else:
                # Default: all JDBC-based sources
                count = self._lookup_jdbc(source_sys, resolved_query)

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

    def _lookup_jdbc(self, source_sys, resolved_query: str) -> int:
        """
        Run the resolved COUNT query via JDBC and return the integer result.
        Wraps the query as a subquery alias so JDBC driver accepts any SELECT.
        """
        username, password = self.secrets.get_credentials(
            source_sys.secret_scope,
            source_sys.secret_key_credentials,
        )
        url    = _build_jdbc_url(source_sys)
        driver = _resolve_driver(source_sys)

        dbtable = f"({resolved_query}) _lkp_count"

        df = (
            self.spark.read.format("jdbc")
            .option("url",      url)
            .option("user",     username)
            .option("password", password)
            .option("driver",   driver)
            .option("dbtable",  dbtable)
            .load()
        )
        row = df.collect()
        if not row:
            return 0
        # COUNT(*) → single row, single column. Grab the first value.
        count_value = row[0][0]
        return int(count_value) if count_value is not None else 0

    # ── Strategy: MongoDB ────────────────────────────────────────────────────

    def _lookup_mongo(self, source_sys, task) -> int:
        """
        Count documents in the MongoDB collection using PyMongo.
        Falls back to count=1 (included) if pymongo is unavailable.
        """
        try:
            import pymongo  # noqa: PLC0415
        except ImportError:
            self.logger.warning(
                "[LookupExecutor] pymongo not available — including MongoDB "
                f"table '{task.source_object_name}' by default."
            )
            return 1

        username, password = self.secrets.get_credentials(
            source_sys.secret_scope,
            source_sys.secret_key_credentials,
        )

        uri = source_sys.connection_uri
        if not uri:
            host = source_sys.host or "localhost"
            port = source_sys.port or 27017
            if username and password:
                uri = f"mongodb://{username}:{password}@{host}:{port}"
            else:
                uri = f"mongodb://{host}:{port}"

        replica_set = source_sys.nosql_replica_set
        if replica_set:
            sep = "&" if "?" in uri else "?"
            uri = f"{uri}{sep}replicaSet={replica_set}"

        client = pymongo.MongoClient(uri)
        try:
            db_name    = source_sys.database_name or source_sys.nosql_collection_name
            collection = source_sys.nosql_collection_name or task.source_object_name
            return int(client[db_name][collection].count_documents({}))
        finally:
            client.close()

    # ── Strategy: File-based (S3 / SFTP) ────────────────────────────────────

    def _lookup_file_based(self, source_sys, task) -> int:  # noqa: ARG002
        """
        File-based sources are always included — no meaningful pre-check.
        The ingestion task handles missing files gracefully.
        """
        self.logger.info(
            f"[LookupExecutor] config_id={task.config_id} ({task.source_object_name}) "
            f"— file-based source ({source_sys.source_type}), always included."
        )
        return 1

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
            # Auto-generate: include schema prefix only when schema is set
            schema_prefix = f"{schema}." if schema else ""
            return f"SELECT COUNT(*) FROM {schema_prefix}{table}"

        # Replace all supported placeholder variants (case-insensitive)
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
            "count":              -1,   # -1 = unknown due to error
            "included":           True, # fail-safe: always include on error
            "error":              error,
        }
