"""
FederatedConnector — reads from a Databricks Lakehouse Federation
(Unity Catalog foreign catalog) instead of via JDBC.

The foreign catalog is created once in Databricks UI:
  Unity Catalog → External Data → Connections → Create connection
  Then: Catalog → Create foreign catalog on top of that connection.

The catalog name (e.g. 'pg_test_rds_catalog') is stored in
config_source_system.federated_catalog_name.

No drivers, no credentials in code — auth is managed by Unity Catalog.

Supports:
  - FULL and INCREMENTAL loads
  - custom_query (plain Spark SQL referencing the foreign catalog)
  - source_filter pushdown
  - dual delta column (Delta_Column_1 OR Delta_Column_2) predicate
  - build_key_query()   for Staging_Flag = 1 PK staging
  - build_probe_query() for LookupExecutor COUNT(*) presence check
"""
from typing import Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .base_connector import BaseConnector
from ..utils.watermark import resolve_watermark


class FederatedConnector(BaseConnector):
    """
    Reads data from a Unity Catalog foreign catalog (Lakehouse Federation).
    Uses spark.table() / spark.sql() — no JDBC driver or credentials needed.
    """

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _federated_catalog(self) -> str:
        """
        Returns the foreign catalog name from config_source_system.
        Raises clearly if not configured.
        """
        cat = self.source_system.federated_catalog_name
        if not cat:
            raise ValueError(
                f"source_system '{self.source_system.source_name}' has no "
                f"federated_catalog_name configured in config_source_system. "
                f"Set it to the Unity Catalog foreign catalog name "
                f"(e.g. 'pg_test_rds_catalog')."
            )
        return cat

    def _base_table_ref(self) -> str:
        """
        Fully-qualified foreign table reference:
          {federated_catalog}.{source_schema}.{source_object_name}
        """
        io  = self.ingest_obj
        cat = self._federated_catalog()
        schema_prefix = f"{io.source_schema}." if io.source_schema else ""
        return f"{cat}.{schema_prefix}{io.source_object_name}"

    def _base_df(self) -> DataFrame:
        """
        Base DataFrame — either from custom_query (spark.sql) or
        spark.table() on the foreign catalog reference.
        """
        io = self.ingest_obj
        if io.custom_query:
            return self.spark.sql(io.custom_query)
        return self.spark.table(self._base_table_ref())

    def _apply_filters(
        self,
        df: DataFrame,
        watermark_start: Optional[str] = None,
    ) -> DataFrame:
        """
        Applies incremental and static filters to the DataFrame.

        Order:
          1. Incremental predicate  (INCREMENTAL load + watermark_start)
          2. source_filter          (static SQL string from config)
        """
        io = self.ingest_obj

        if io.load_type == "INCREMENTAL" and io.incremental_column and watermark_start:
            predicate = F.col(io.incremental_column) > watermark_start
            if io.delta_column_2:
                predicate = predicate | (F.col(io.delta_column_2) > watermark_start)
            df = df.filter(predicate)

        if io.source_filter:
            df = df.filter(io.source_filter)

        return df

    # ── Lookup / probe helpers (used by LookupExecutor) ───────────────────────

    def build_probe_query(self) -> str:
        """
        Returns a Spark SQL COUNT(*) string for the LookupExecutor presence
        check. For INCREMENTAL loads the lookback watermark predicate is
        included so the count reflects the incremental window only.
        """
        io        = self.ingest_obj
        table_ref = self._base_table_ref()
        predicates = []

        if io.load_type == "INCREMENTAL" and io.incremental_column:
            cutoff = resolve_watermark(io)
            if cutoff is not None:
                pred = f"{io.incremental_column} >= '{cutoff}'"
                if io.delta_column_2:
                    pred = f"({pred} OR {io.delta_column_2} >= '{cutoff}')"
                predicates.append(pred)

        where_clause = f" WHERE {' AND '.join(predicates)}" if predicates else ""

        if io.custom_query:
            return f"SELECT COUNT(*) FROM ({io.custom_query}) _src{where_clause}"
        return f"SELECT COUNT(*) FROM {table_ref}{where_clause}"

    def build_key_query(self) -> str:
        """
        Returns a Spark SQL string selecting only primary key columns —
        used for Staging_Flag = 1 PK staging. Always unfiltered (no
        incremental predicate), matching the ADF key-extraction behaviour.
        """
        io        = self.ingest_obj
        key_cols  = ", ".join(io.primary_key_list) if io.primary_key_list else "*"
        table_ref = self._base_table_ref()

        if io.custom_query:
            return f"SELECT {key_cols} FROM ({io.custom_query}) _src"
        return f"SELECT {key_cols} FROM {table_ref}"

    # ── Public extract ────────────────────────────────────────────────────────

    def extract(self, watermark_start: Optional[str]) -> Tuple[DataFrame, Optional[str]]:
        """
        Reads data from the foreign catalog and returns (DataFrame, max_watermark).

        FULL       : reads the whole table (+ source_filter if set).
        INCREMENTAL: applies Delta_Column_1 (OR Delta_Column_2) > watermark_start,
                     then source_filter.

        Retries are handled by the caller (orchestrator.py wraps this in
        retry_on_failure) — no retry loop here.
        """
        df = self._base_df()
        df = self._apply_filters(df, watermark_start)

        # Capture max watermark for incremental metadata update
        max_watermark: Optional[str] = None
        io = self.ingest_obj
        if io.load_type == "INCREMENTAL" and io.incremental_column:
            max_row = df.agg({io.incremental_column: "max"}).collect()
            if max_row and max_row[0][0] is not None:
                max_watermark = str(max_row[0][0])

        return df, max_watermark
