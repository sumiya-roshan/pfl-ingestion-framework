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

Field mapping from config tables
---------------------------------
config_source_system
  .database_name       → Unity Catalog *foreign catalog* name (the catalog
                          created via CREATE FOREIGN CATALOG for this Postgres
                          source), used when extra_params has no override.
  .uc_connection_name   → Unity Catalog *Connection* name (created via
                          CREATE CONNECTION ... TYPE POSTGRESQL) that the
                          foreign catalog above is expected to be built on.
                          Optional — when set, extract() verifies the
                          foreign catalog is actually backed by this
                          connection before querying it (see
                          _validate_connection()), catching a
                          misconfigured/repointed catalog early instead of
                          silently reading from the wrong server. When
                          unset, this check is skipped.
  .extra_params         → optional JSON override, e.g. {"foreign_catalog": "postgres_catalog"}
  .host/.port/.driver_class/.secret_scope/.secret_key_credentials → NOT used
                          by this connector (the UC Connection object already
                          owns those). Fine to leave NULL for FEDERATED_POSTGRES rows.

ingestion_config
  .source_schema       → schema inside the foreign catalog (e.g. 'public')
  .source_object_name  → table name inside that schema
  .custom_query         → raw SQL verbatim (optional '{catalog}' placeholder
                          substitution, so the same query can be reused across
                          environments where the foreign catalog name differs)
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
import json
from typing import Optional, Tuple

from pyspark.sql import DataFrame

from .base_connector import BaseConnector


class FederatedConnector(BaseConnector):

    # ── Internal helpers ──────────────────────────────────────────────────

    def _foreign_catalog(self) -> str:
        """
        Resolves the Unity Catalog foreign catalog name.

        Priority:
          1. config_source_system.extra_params  JSON {"foreign_catalog": "<name>"}
          2. config_source_system.database_name
        """
        ss = self.source_system

        if ss.extra_params:
            try:
                extra = json.loads(ss.extra_params)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"config_source_system.extra_params for source_id={ss.source_id} "
                    f"is not valid JSON: {exc}"
                ) from exc
            catalog = extra.get("foreign_catalog")
            if catalog:
                return catalog

        if ss.database_name:
            return ss.database_name

        raise ValueError(
            f"No foreign catalog configured for source_id={ss.source_id} "
            f"(source_name='{ss.source_name}'). Set config_source_system.database_name "
            f"to the Unity Catalog foreign catalog name, or set "
            f"extra_params='{{\"foreign_catalog\": \"<name>\"}}'."
        )

    def _validate_connection(self, catalog: str) -> None:
        """
        Best-effort check that the foreign catalog is actually backed by
        config_source_system.uc_connection_name, via DESCRIBE CATALOG
        EXTENDED (which surfaces a 'Connection Name' property for foreign
        catalogs). Skipped entirely when uc_connection_name is unset.

        - Catalog doesn't exist / DESCRIBE fails → raise (nothing to read).
        - Connection Name property found but doesn't match → raise (this is
          the whole point: catches a catalog silently repointed at a
          different server).
        - Property not found (older DBR / different catalog type) → log a
          warning and continue rather than fail a run over a property Databricks
          didn't return, since the property's exact name/availability isn't
          guaranteed across versions.
        """
        expected = self.source_system.uc_connection_name
        if not expected:
            return

        try:
            rows = self.spark.sql(f"DESCRIBE CATALOG EXTENDED `{catalog}`").collect()
        except Exception as exc:
            raise ValueError(
                f"Could not DESCRIBE CATALOG EXTENDED `{catalog}` while validating "
                f"uc_connection_name='{expected}' for source_id="
                f"{self.source_system.source_id}: {exc}"
            ) from exc

        actual = None
        for row in rows:
            info_name = str(row["info_name"] or "").strip().lower()
            if "connection" in info_name:
                actual = row["info_value"]
                break

        if actual is None:
            print(
                f"[FederatedConnector] Warning: could not find a 'Connection Name' "
                f"property on catalog '{catalog}' to validate against "
                f"uc_connection_name='{expected}'. Skipping check."
            )
            return

        if str(actual).strip() != str(expected).strip():
            raise ValueError(
                f"Foreign catalog '{catalog}' is backed by connection "
                f"'{actual}', but config_source_system.uc_connection_name="
                f"'{expected}' for source_id={self.source_system.source_id} "
                f"expected a different connection. Refusing to query — this "
                f"usually means the catalog was repointed at a different "
                f"server, or the config row is stale."
            )

    def _base_sql(self, catalog: str) -> str:
        """
        Source_Query (custom_query) verbatim (with an optional '{catalog}'
        placeholder substitution), or a plain three-level-namespace
        SELECT * when no custom query is configured. Never mutated —
        always wrapped by _wrap_query() so the original text is untouched.
        """
        io = self.ingest_obj
        if io.custom_query:
            return io.custom_query.replace("{catalog}", catalog)
        schema = io.source_schema or "public"
        return f"SELECT * FROM {catalog}.{schema}.{io.source_object_name}"

    def _wrap_query(self, base_sql: str, predicates: list) -> str:
        """
        Wraps base_sql as a subquery with a single outer WHERE ANDing all
        predicates together, mirroring JdbcConnector._wrap_query() so any
        WHERE clause already inside a custom_query stays inside its own
        subquery and is never touched or duplicated.
        """
        where_clause = f" WHERE {' AND '.join(predicates)}" if predicates else ""
        return f"SELECT * FROM ({base_sql}) _src_wrapped{where_clause}"

    def _build_query(self, catalog: str, watermark_start: Optional[str]) -> str:
        """
        Constructs the SQL sent to spark.sql().

        Injects:
          - incremental predicate (load_type = INCREMENTAL): incremental_column > watermark,
            ORed with delta_column_2 when configured
          - source_filter  (additional static predicate from config)
        """
        io = self.ingest_obj
        predicates = []

        if io.load_type == "INCREMENTAL" and io.incremental_column:
            wm = watermark_start if watermark_start is not None else getattr(
                io, "incremental_end_value", None
            )
            if wm is not None:
                predicate = f"{io.incremental_column} > '{wm}'"
                if io.delta_column_2:
                    predicate = f"({predicate} OR {io.delta_column_2} > '{wm}')"
                predicates.append(predicate)

        if io.source_filter:
            predicates.append(f"({io.source_filter})")

        return self._wrap_query(self._base_sql(catalog), predicates)

    # ── Public extract ────────────────────────────────────────────────────

    def extract(self, watermark_start: Optional[str]) -> Tuple[DataFrame, Optional[str]]:
        """
        Retries are handled by the caller (orchestrator.py wraps this whole
        call in retry_on_failure(...)) — no retry loop here, matching every
        other connector.
        """
        catalog = self._foreign_catalog()
        self._validate_connection(catalog)
        query = self._build_query(catalog, watermark_start)
        print('query:', query)
        df = self.spark.sql(query)
        print(df.head(20))
        max_watermark: Optional[str] = None
        io = self.ingest_obj
        if io.load_type == "INCREMENTAL" and io.incremental_column:
            max_row = df.agg({io.incremental_column: "max"}).collect()
            if max_row and max_row[0][0] is not None:
                max_watermark = str(max_row[0][0])

        return df, max_watermark