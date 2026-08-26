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

# JSON map of config_master_id (str) -> {"lookup_query": "<template>"} for
# pipelines whose incremental lookup can't be expressed generically (e.g. a
# join against a header table where the delta columns live on the joined
# side). See JdbcConnector._special_probe_query().
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

    def _lookback_cutoff(self) -> Optional[str]:
        """
        Silver_Last_Sink_Date - Lookback_Hours, formatted 'YYYY-MM-DD HH:MM:SS'.
        Returns None for FULL load or when silver_last_sink_date isn't set yet
        (first run). Shared by _lookback_predicates() and the special-case
        template substitution in _special_probe_query() — both must derive
        the cutoff the same way.
        """
        io = self.ingest_obj
        if io.load_type != "INCREMENTAL" or not io.silver_last_sink_date:
            return None

        from datetime import datetime, timedelta

        last_sink_str = str(io.silver_last_sink_date).replace("T", " ").split(".")[0]
        last_sink = datetime.strptime(last_sink_str, "%Y-%m-%d %H:%M:%S")
        cutoff = last_sink - timedelta(hours=int(io.lookback_hours or 3))
        return cutoff.strftime("%Y-%m-%d %H:%M:%S")

    def _lookback_predicates(self) -> list:
        """
        Watermark predicate for the generic lookup probe:
        incremental_column (OR delta_column_2) >= Silver_Last_Sink_Date - Lookback_Hours.

        Returns [] for FULL load, or when there's no incremental column or no
        cutoff yet (first run — probe unfiltered).
        """
        io = self.ingest_obj
        cutoff_str = self._lookback_cutoff()
        if not io.incremental_column or cutoff_str is None:
            return []

        predicate = f"{io.incremental_column} >= '{cutoff_str}'"
        if io.delta_column_2:
            predicate = f"({predicate} OR {io.delta_column_2} >= '{cutoff_str}')"
        return [predicate]

    def _special_probe_query(self, select_cols: str) -> Optional[str]:
        """
        Looks up self.ingest_obj.config_master_id in special_lookup_queries.json.
        If a matching entry exists, substitutes {key_column}/{lookback_timestamp}
        into its 'lookup_query' template and returns the finished, wrapped
        dbtable expression. Returns None when there's no matching entry, so
        build_probe_query() falls back to the generic logic unchanged.

        custom_query is never parsed or modified here — the template is used
        verbatim aside from the two placeholder substitutions.
        """
        config_master_id = self.ingest_obj.config_master_id
        if config_master_id is None:
            return None

        entry = _load_special_lookup_queries().get(str(config_master_id))
        if entry is None:
            return None

        template = entry.get("lookup_query")
        if not template:
            raise ValueError(
                f"special_lookup_queries.json entry for config_master_id="
                f"{config_master_id} is missing 'lookup_query'."
            )

        cutoff_str = self._lookback_cutoff() or ""
        query = template.replace("{key_column}", select_cols).replace(
            "{lookback_timestamp}", cutoff_str
        )
        return f"({query}) _src"

    def build_probe_query(self) -> str:
        """
        SELECT <key_cols> ... LIMIT 1 (dialect-mapped) — cheap existence check
        derived from Source_Query, projecting the primary key columns instead
        of '*' (falls back to '1' if no key column is configured).

        FULL load: unfiltered, always generic. INCREMENTAL load: first checks
        special_lookup_queries.json for a config_master_id-specific template
        (see _special_probe_query()); if none matches, filters by
        _lookback_predicates() (Delta_Column_1/2 vs. Silver_Last_Sink_Date -
        Lookback_Hours) same as before. Used by LookupExecutor instead of a
        separate Lookup_Query_Template column.
        """
        io = self.ingest_obj
        select_cols = ", ".join(io.primary_key_list) if io.primary_key_list else "1"

        if io.load_type == "INCREMENTAL":
            special_query = self._special_probe_query(select_cols)
            if special_query is not None:
                return special_query

        return self._wrap_query(
            self._base_sql(), self._lookback_predicates(), select_cols=select_cols, row_limit=1
        )

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
