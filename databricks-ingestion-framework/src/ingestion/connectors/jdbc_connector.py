"""
Generic JDBC connector for RDBMS sources: POSTGRES, MYSQL, ORACLE, MSSQL, etc.

Connection URL resolution priority
-----------------------------------
1. config_source_system.connection_uri      — use verbatim if set (full override)
2. config_source_system.driver_class        — explicit JDBC driver class (overrides built-in map)
3. Built-in URL + driver templates keyed on source_type

Credential resolution
---------------------
config_source_system.secret_key_credentials  →  Databricks Secret  →
    JSON {"username": "<val>", "password": "<val>"}

Incremental extraction
----------------------
- load_type = INCREMENTAL  →  predicate pushed into the source query via a
  subquery wrapper so it works both with custom_query and schema.table reads.
- source_filter from ingestion_config is ANDed into the same subquery.
"""
from typing import Optional, Tuple

from pyspark.sql import DataFrame

from .base_connector import BaseConnector


def _parse_timeout_to_seconds(value: Optional[str]) -> Optional[int]:
    """
    Parses pipeline_lookup_config.query_timeout ('HH:mm:ss', e.g. '12:00:00'),
    carried per-task as IngestionTaskConfig.query_timeout, into whole seconds
    for the JDBC 'queryTimeout' option. Returns None for unset/blank/zero
    values (no timeout applied).
    """
    if not value:
        return None
    value = str(value).strip()
    if not value:
        return None
    parts = value.split(":")
    if len(parts) != 3:
        raise ValueError(
            f"Invalid query_timeout '{value}': expected 'HH:mm:ss' format."
        )
    hours, minutes, seconds = (int(p) for p in parts)
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds or None

# ── Built-in JDBC driver class map ───────────────────────────────────────────
# Entries here are used only when config_source_system.driver_class is NULL.
# To support a new RDBMS without code changes, set driver_class in the config
# table and the correct JDBC URL in connection_uri (or set host/port/db).
_DEFAULT_DRIVER: dict = {
    "POSTGRES": "org.postgresql.Driver",
    "MYSQL":    "com.mysql.cj.jdbc.Driver",
    "ORACLE":   "oracle.jdbc.OracleDriver",
    "MSSQL":    "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

# ── Built-in URL templates ────────────────────────────────────────────────────
# Used only when connection_uri is NULL in config_source_system.
def _build_url(source_type: str, host: str, port: int, database_name: str, extra_params: dict) -> str:
    st = source_type.upper()
    if st == "POSTGRES":
        return f"jdbc:postgresql://{host}:{port}/{database_name}"
    if st == "MYSQL":
        return f"jdbc:mysql://{host}:{port}/{database_name}"
    if st == "ORACLE":
        # Oracle supports both SID and service-name connect descriptors.
        # extra_params can carry 'oracle_connect_type' = 'sid' | 'service' (default: service)
        connect_type = extra_params.get("oracle_connect_type", "service")
        if connect_type == "sid":
            return f"jdbc:oracle:thin:@{host}:{port}:{database_name}"
        return f"jdbc:oracle:thin:@//{host}:{port}/{database_name}"
    if st == "MSSQL":
        return f"jdbc:sqlserver://{host}:{port};databaseName={database_name}"
    raise ValueError(
        f"No built-in JDBC URL template for source_type='{source_type}'. "
        f"Set connection_uri directly in config_source_system."
    )


class JdbcConnector(BaseConnector):

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _resolve_jdbc_url(self) -> str:
        ss = self.source_system
        return _build_url(
            source_type=ss.source_type,
            host=ss.host,
            port=ss.port or 0,
            database_name=ss.database_name or "",
            extra_params={},                                # no extra_params in new schema
        )

    def _resolve_driver(self) -> str:
        ss = self.source_system
        if ss.driver_class:
            return ss.driver_class                          # explicit config wins
        driver = _DEFAULT_DRIVER.get(ss.source_type.upper())
        if driver is None:
            raise ValueError(
                f"No built-in JDBC driver for source_type='{ss.source_type}'. "
                f"Set driver_class in config_source_system."
            )
        return driver

    def _read_options(self) -> dict:
        ss = self.source_system
        username, password = self.secrets.get_credentials(
            ss.secret_scope, ss.secret_key_credentials
        )
        opts = {
            "url":      self._resolve_jdbc_url(),
            "user":     username,
            "password": password,
            "driver":   self._resolve_driver(),
        }
        # Parallel-read tuning: optionally set via source_filter JSON extras
        # or extend ingestion_config with dedicated columns in a future iteration.
        if self.ingest_obj.data_read_size:
            opts["fetchsize"] = str(self.ingest_obj.data_read_size)

        # Source query timeout (pipeline_lookup_config.query_timeout, 'HH:mm:ss',
        # carried per-task as ingest_obj.query_timeout). Maps to
        # java.sql.Statement.setQueryTimeout(): the JDBC driver asks the source
        # DB to cancel the running statement once exceeded, rather than merely
        # abandoning the client-side wait.
        timeout_sec = _parse_timeout_to_seconds(self.ingest_obj.query_timeout)
        if timeout_sec:
            opts["queryTimeout"] = str(timeout_sec)
        return opts

    def _build_source_query(self, watermark_start: Optional[str]) -> str:
        """
        Constructs the SQL subquery sent to the JDBC driver as dbtable.

        Priority:
          1. custom_query  (verbatim user-supplied SQL)
          2. source_schema.source_object_name  (standard table reference)

        Then injects:
          - incremental predicate (load_type = INCREMENTAL)
          - source_filter         (additional static predicate from config)
        """
        io = self.ingest_obj

        if io.custom_query:
            base_sql = io.custom_query
        else:
            schema_prefix = f"{io.source_schema}." if io.source_schema else ""
            base_sql = f"SELECT * FROM {schema_prefix}{io.source_object_name}"

        predicates = []

        if io.load_type == "INCREMENTAL" and io.incremental_column:
            wm = watermark_start if watermark_start is not None else io.incremental_end_value
            if wm is not None:
                predicates.append(f"{io.incremental_column} > '{wm}'")

        if io.source_filter:
            predicates.append(f"({io.source_filter})")

        if predicates:
            where_clause = " AND ".join(predicates)
            # Wrap as subquery so predicates apply consistently to both
            # plain tables and custom queries without altering base_sql.
            base_sql = f"SELECT * FROM ({base_sql}) _src_wrapped WHERE {where_clause}"

        return f"({base_sql}) _src"

    # ── Public extract ────────────────────────────────────────────────────────

    def extract(self, watermark_start: Optional[str]) -> Tuple[DataFrame, Optional[str]]:
        import time
        retries = 3
        delay = 5
        last_exception = None

        options  = self._read_options()
        dbtable  = self._build_source_query(watermark_start)

        for attempt in range(retries):
            try:
                df = (
                    self.spark.read.format("jdbc")
                    .options(**options)
                    .option("dbtable", dbtable)
                    .load()
                )

                max_watermark: Optional[str] = None
                io = self.ingest_obj
                if io.load_type == "INCREMENTAL" and io.incremental_column:
                    # Spark performs the actual database read action here. 
                    # Wrapping this ensures we catch transient connection dropouts.
                    max_row = df.agg({io.incremental_column: "max"}).collect()
                    if max_row and max_row[0][0] is not None:
                        max_watermark = str(max_row[0][0])

                return df, max_watermark
            except Exception as exc:
                last_exception = exc
                if attempt < retries - 1:
                    time.sleep(delay)

        raise ConnectionError(
            f"Failed to execute JDBC extraction after {retries} attempts due to transient error: {last_exception}"
        )
