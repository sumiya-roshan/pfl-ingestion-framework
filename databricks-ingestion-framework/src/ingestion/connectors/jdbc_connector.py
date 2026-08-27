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
import json
from pathlib import Path
from typing import Optional, Tuple

from pyspark.sql import DataFrame

from .base_connector import BaseConnector

# JSON map of config_id (str, the per-table row identifier — NOT
# config_master_id, which is shared by every pipeline of a given source type)
# -> {"pipeline_name": ..., "table_name": ..., "lookup_query": "<template>"}
# for tables whose incremental lookup can't be expressed generically (e.g. a
# join against a header table where the delta columns live on the joined
# side). pipeline_name/table_name are documentation only — the lookup key is
# config_id. See JdbcConnector._special_probe_query().
_SPECIAL_LOOKUP_CONFIG_PATH = (
    Path(__file__).resolve().parents[3] / "config" / "special_lookup_queries.json"
)

# Loaded lazily on first use and kept for the life of the process — set
# _special_lookup_queries_cache back to None (e.g. in a test) to force a re-read.
_special_lookup_queries_cache: Optional[dict] = None


def _load_special_lookup_queries() -> dict:
    """
    Loads _SPECIAL_LOOKUP_CONFIG_PATH once per process. Missing file → {}
    (every pipeline falls back to the generic build_probe_query() logic).
    Malformed JSON raises a clear configuration error rather than silently
    falling back, since a typo there should not go unnoticed.
    """
    global _special_lookup_queries_cache
    if _special_lookup_queries_cache is not None:
        return _special_lookup_queries_cache

    if not _SPECIAL_LOOKUP_CONFIG_PATH.exists():
        _special_lookup_queries_cache = {}
        return _special_lookup_queries_cache

    try:
        with open(_SPECIAL_LOOKUP_CONFIG_PATH, "r") as f:
            _special_lookup_queries_cache = json.load(f)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid JSON in special lookup config '{_SPECIAL_LOOKUP_CONFIG_PATH}': {exc}"
        ) from exc

    return _special_lookup_queries_cache


def _parse_timeout_to_seconds(value: Optional[str]) -> Optional[int]:
    """
    Parses config_source_system.query_timeout ('HH:mm:ss', e.g. '12:00:00')
    into whole seconds for the JDBC 'queryTimeout' option. Returns None for
    unset/blank/zero values (no timeout applied).
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

    def _read_options(self) -> dict:
        ss = self.source_system
        username, password = self.secrets.get_credentials(
            ss.secret_scope, ss.secret_key_credentials
        )

        url = _build_url(
            source_type=ss.source_type,
            host=ss.host,
            port=ss.port or 0,
            database_name=ss.database_name or "",
            extra_params={},                                # no extra_params in new schema
        )

        driver = ss.driver_class                            # explicit config wins
        if not driver:
            driver = _DEFAULT_DRIVER.get(ss.source_type.upper())
            if driver is None:
                raise ValueError(
                    f"No built-in JDBC driver for source_type='{ss.source_type}'. "
                    f"Set driver_class in config_source_system."
                )

        opts = {
            "url":      url,
            "user":     username,
            "password": password,
            "driver":   driver,
        }
        # Parallel-read tuning: optionally set via source_filter JSON extras
        # or extend ingestion_config with dedicated columns in a future iteration.
        if self.ingest_obj.data_read_size:
            opts["fetchsize"] = str(self.ingest_obj.data_read_size)

        # Source query timeout (config_source_system.query_timeout, 'HH:mm:ss').
        # Maps to java.sql.Statement.setQueryTimeout(): the JDBC driver asks the
        # source DB to cancel the running statement once exceeded, rather than
        # merely abandoning the client-side wait.
        timeout_sec = _parse_timeout_to_seconds(ss.query_timeout)
        if timeout_sec:
            opts["queryTimeout"] = str(timeout_sec)
        return opts

    def _base_sql(self) -> str:
        """
        Source_Query (custom_query) verbatim, or a plain schema.table SELECT *
        when no custom query is configured. Never mutated — always wrapped by
        _wrap_query() so the original text is untouched.
        """
        io = self.ingest_obj
        if io.custom_query:
            return io.custom_query
        schema_prefix = f"{io.source_schema}." if io.source_schema else ""
        return f"SELECT * FROM {schema_prefix}{io.source_object_name}"

    def _wrap_query(
        self,
        base_sql: str,
        predicates: list,
        select_cols: str = "*",
        row_limit: Optional[int] = None,
    ) -> str:
        """
        Wraps base_sql as a subquery with a single outer WHERE ANDing all
        predicates together. Because the predicate always lands on the outer
        wrapper, any WHERE clause already inside base_sql (schema.table has
        none, but a custom_query might) stays inside its own subquery and is
        never touched or duplicated.
        """
        where_clause = f" WHERE {' AND '.join(predicates)}" if predicates else ""
        sql = f"SELECT {select_cols} FROM ({base_sql}) _src_wrapped{where_clause}"

        if row_limit:
            source_type = self.source_system.source_type.upper()
            if source_type == "MSSQL":
                sql = sql.replace("SELECT ", f"SELECT TOP {row_limit} ", 1)
            elif source_type == "ORACLE":
                sql += f" FETCH FIRST {row_limit} ROWS ONLY"
            else:  # POSTGRES, MYSQL
                sql += f" LIMIT {row_limit}"

        return f"({sql}) _src"

    def _build_source_query(self, watermark_start: Optional[str]) -> str:
        """
        Constructs the SQL subquery sent to the JDBC driver as dbtable.

        Priority:
          1. custom_query  (verbatim user-supplied SQL)
          2. source_schema.source_object_name  (standard table reference)

        Then injects:
          - incremental predicate (load_type = INCREMENTAL): Delta_Column_1 > watermark,
            ORed with Delta_Column_2 > watermark when configured
          - source_filter         (additional static predicate from config)
        """
        io = self.ingest_obj
        predicates = []

        if io.load_type == "INCREMENTAL" and io.incremental_column:
            wm = watermark_start if watermark_start is not None else io.incremental_end_value
            if wm is not None:
                predicate = f"{io.incremental_column} > '{wm}'"
                if io.delta_column_2:
                    predicate = f"({predicate} OR {io.delta_column_2} > '{wm}')"
                predicates.append(predicate)

        if io.source_filter:
            predicates.append(f"({io.source_filter})")

        return self._wrap_query(self._base_sql(), predicates)

    def build_probe_query(self) -> str:
        """
        existence-check query derived from Source_Query, projecting the
        primary key columns (falls back to '1' if none configured), limited
        to 1 row (dialect-mapped). Used by LookupExecutor instead of a
        separate Lookup_Query_Template column.

        FULL load: always the generic unfiltered form.

        INCREMENTAL load: first checks special_lookup_queries.json for a
        config_id-specific template — for the rare table whose join structure
        can't be filtered generically (delta columns live on a joined table).
        If no entry matches, falls back to the generic predicate:
        Delta_Column_1 (OR Delta_Column_2) >= Silver_Last_Sink_Date - Lookback_Hours.
        """
        io = self.ingest_obj
        select_cols = ", ".join(io.primary_key_list) if io.primary_key_list else "1"
        predicates = []

        if io.load_type == "INCREMENTAL":
            from ..utils.watermark import resolve_watermark
            cutoff = resolve_watermark(io)

            special_entry = _load_special_lookup_queries().get(str(io.config_id))
            if special_entry is not None:
                template = special_entry.get("lookup_query")
                if not template:
                    raise ValueError(
                        f"special_lookup_queries.json entry for config_id="
                        f"{io.config_id} is missing 'lookup_query'."
                    )
                # custom_query is never parsed here — the template is used
                # verbatim aside from these two placeholder substitutions.
                query = template.replace("{key_column}", select_cols).replace(
                    "{lookback_timestamp}", cutoff or ""
                )
                return f"({query}) _src"

            if io.incremental_column and cutoff is not None:
                predicate = f"{io.incremental_column} >= '{cutoff}'"
                if io.delta_column_2:
                    predicate = f"({predicate} OR {io.delta_column_2} >= '{cutoff}')"
                predicates.append(predicate)

        return self._wrap_query(self._base_sql(), predicates, select_cols=select_cols, row_limit=1)

    def build_key_query(self) -> str:
        """
        SELECT <primary_key_cols> ... derived from Source_Query. Independent
        of load_type — always unfiltered (no incremental/lookback predicate),
        matching the ADF key-extraction behavior.
        """
        io = self.ingest_obj
        key_cols = ", ".join(io.primary_key_list) if io.primary_key_list else "*"
        return self._wrap_query(self._base_sql(), [], select_cols=key_cols)

    # ── Public extract ────────────────────────────────────────────────────────

    def extract(self, watermark_start: Optional[str]) -> Tuple[DataFrame, Optional[str]]:
        """
        Retries are handled by the caller (orchestrator.py wraps this whole
        call in retry_on_failure(max_retries=source_sys.retry_count,
        retry_interval=source_sys.retry_interval)) — no retry loop here, so
        there's a single configurable retry policy instead of two nested ones.
        """
        options = self._read_options()
        dbtable = self._build_source_query(watermark_start)

        df = (
            self.spark.read.format("jdbc")
            .options(**options)
            .option("dbtable", dbtable)
            .load()
        )

        max_watermark: Optional[str] = None
        io = self.ingest_obj
        if io.load_type == "INCREMENTAL" and io.incremental_column:
            max_row = df.agg({io.incremental_column: "max"}).collect()
            if max_row and max_row[0][0] is not None:
                max_watermark = str(max_row[0][0])

        return df, max_watermark
