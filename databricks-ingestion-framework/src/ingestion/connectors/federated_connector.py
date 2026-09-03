"""
Lakehouse Federation connector — reads from a Unity Catalog *foreign catalog*
(e.g. a Postgres database registered via CREATE CONNECTION + CREATE FOREIGN
CATALOG) instead of opening a JDBC connection itself.

Why this is different from JdbcConnector
-----------------------------------------
JdbcConnector opens its own `spark.read.format("jdbc")` connection using
host/port/driver_class/secret_key_credentials pulled from config_source_system.

FederatedConnector does none of that. The network path and credentials are
already owned by the Unity Catalog "Connection" + "Foreign Catalog" objects
that were created once in Databricks (Catalog Explorer → External Data →
Connections, or CREATE CONNECTION ... TYPE POSTGRESQL / CREATE FOREIGN
CATALOG ... USING CONNECTION ...). Once that foreign catalog exists, its
tables behave like ordinary three-level-namespace Spark tables
(<foreign_catalog>.<schema>.<table>) — Unity Catalog grants control who can
read them, and Databricks pushes filters/predicates down to Postgres
automatically. So extract() here is just a spark.sql() SELECT.

Deliberately minimal — same three methods as JdbcConnector
------------------------------------------------------------
_base_sql() / _build_source_query() / extract(), same shape and naming as
JdbcConnector. No _wrap_query() helper (inlined directly in
_build_source_query, matching JdbcConnector), no build_probe_query() (so
LookupExecutor treats federated sources as non-queryable/always-included,
same as any other non-JDBC/Mongo source — see lookup_executor.py), no
_validate_connection() connection-name safety check. config_source_system
.uc_connection_name is consequently unused by this connector.

Field mapping from config tables
---------------------------------
config_source_system
  .database_name       → Unity Catalog *foreign catalog* name (the catalog
                          created via CREATE FOREIGN CATALOG for this Postgres
                          source).
  .host/.port/.driver_class/.secret_scope/.secret_key_credentials/
  .uc_connection_name   → NOT used by this connector (the UC Connection
                          object already owns network/auth; uc_connection_name
                          is unused — no validation is performed against it).
                          Fine to leave NULL for FEDERATED_POSTGRES rows.

ingestion_config
  .source_schema       → schema inside the foreign catalog (e.g. 'public')
  .source_object_name  → table name inside that schema
  .custom_query         → raw SQL verbatim (SELECT * FROM <catalog>.<schema>.<table> when unset)
  .source_filter        → extra static predicate, ANDed into the WHERE clause
  .load_type / .incremental_column / .delta_column_2 → same incremental
    predicate convention as JdbcConnector, applied as a normal SQL WHERE
    clause (Databricks pushes it down to Postgres where possible).

Registering a new source
--------------------------
Insert a config_source_system row with:
  source_type    = 'POSTGRES_FEDERATED'   (or 'FEDERATED')
  ingest_method  = 'FEDERATED'
  database_name  = '<the foreign catalog name you created in Unity Catalog>'
No host/port/credentials/driver_class needed — see above.
"""

from pyspark.sql import DataFrame

from .base_connector import BaseConnector


class FederatedConnector(BaseConnector):
    # ── Internal helpers ─────────────────────────────────────────────────────

    def _foreign_catalog(self) -> str:
        """
        Resolves the Unity Catalog foreign catalog name from
        config_source_system.database_name. The Federation equivalent of
        JdbcConnector._read_options() — this is what stands in for
        "connection info" here, since there's no host/port/driver to
        assemble.
        """
        ss = self.source_system
        if ss.database_name:
            return ss.database_name
        raise ValueError(
            f"No foreign catalog configured for source_id={ss.source_id} "
            f"(source_name='{ss.source_name}'). Set config_source_system.database_name "
            f"to the Unity Catalog foreign catalog name."
        )

    def _base_sql(self, catalog: str) -> str:
        """
        SELECT * when no custom query is configured.
        """
        io = self.ingest_obj
        if io.custom_query:
            return io.custom_query
        schema = io.source_schema or "public"
        return f"SELECT * FROM {catalog}.{schema}.{io.source_object_name}"

    def _build_source_query(self, catalog: str, watermark_start: str | None) -> str:
        """
        Constructs the SQL sent to spark.sql()
          - incremental (load_type = INCREMENTAL): incremental_column > watermark,
            OR id with delta_column_2 when configured
          - source_filter (additional static predicate from config)
        """
        io = self.ingest_obj
        predicates = []

        if io.load_type == "INCREMENTAL" and io.incremental_column:
            wm = (
                watermark_start
                if watermark_start is not None
                else getattr(io, "incremental_end_value", None)
            )
            if wm is not None:
                predicate = f"{io.incremental_column} > '{wm}'"
                if io.delta_column_2:
                    predicate = f"({predicate} OR {io.delta_column_2} > '{wm}')"
                predicates.append(predicate)

        if io.source_filter:
            predicates.append(f"({io.source_filter})")

        where_clause = f" WHERE {' AND '.join(predicates)}" if predicates else ""
        return f"SELECT * FROM ({self._base_sql(catalog)}) _src_wrapped{where_clause}"

    # ── Public extract ───────────────────────────────────────────────────────

    def extract(self, watermark_start: str | None) -> tuple[DataFrame, str | None]:
        """
        Retries are handled by the caller (orchestrator.py wraps this whole
        call in retry_on_failure(max_retries=source_sys.retry_count,
        retry_interval=source_sys.retry_interval)) — no retry loop here, so
        there's a single configurable retry policy instead of two nested
        ones. Matches JdbcConnector.extract()'s docstring/contract exactly.
        """
        catalog = self._foreign_catalog()
        query = self._build_source_query(catalog, watermark_start)
        df = self.spark.sql(query)

        max_watermark: str | None = None
        io = self.ingest_obj
        if io.load_type == "INCREMENTAL" and io.incremental_column:
            max_row = df.agg({io.incremental_column: "max"}).collect()
            if max_row and max_row[0][0] is not None:
                max_watermark = str(max_row[0][0])

        return df, max_watermark
