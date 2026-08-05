"""
Writes the extracted DataFrame into a Databricks bronze Delta table.

Supported write modes (driven by ingestion_config.write_mode)
--------------------------------------------------------------
  append    — default; adds new rows; mergeSchema honours schema evolution
  overwrite — full table replace; respects schema_evolution_mode
  merge     — upsert via DeltaTable.merge; requires primary_key_cols to be set

Schema evolution (driven by ingestion_config.schema_evolution_mode)
-------------------------------------------------------------------
  none      / NULL  — no schema evolution; fails if schema changes
  merge             — mergeSchema=true   (append / overwrite modes)
  overwrite         — overwriteSchema=true (overwrite mode only)

Partitioning (driven by ingestion_config.partition_column)
----------------------------------------------------------
  When set, the target table is written with .partitionBy(partition_column).
  Ignored for merge mode (Delta handles partition pruning automatically).
"""
from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable


class BronzeWriter:

    def __init__(self, spark):
        self.spark = spark

    def write(
        self,
        df: DataFrame,
        catalog: str,
        schema: str,
        table: str,
        write_mode: str = "append",
        merge_keys: Optional[List[str]] = None,
        schema_evolution_mode: Optional[str] = None,
        partition_column: Optional[str] = None,
    ) -> str:
        """
        Write *df* to the target Delta table and return the fully-qualified name.

        Parameters
        ----------
        df                   : source DataFrame (already extracted & transformed)
        catalog              : Unity Catalog catalog name
        schema               : schema / database name
        table                : table name
        write_mode           : 'append' | 'overwrite' | 'merge'
        merge_keys           : list of column names used as merge keys (merge mode only)
        schema_evolution_mode: 'none' | 'merge' | 'overwrite' | None
        partition_column     : column to partition by on first write (append/overwrite)
        """
        full_table_name = f"{catalog}.{schema}.{table}"
        evo = (schema_evolution_mode or "none").lower()

        # Stamp ingestion metadata
        df_out = df.withColumn("_ingested_at", F.current_timestamp())

        # Ensure the target schema exists
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

        if write_mode == "overwrite":
            self._write_overwrite(df_out, full_table_name, evo, partition_column)

        elif write_mode == "merge":
            if not merge_keys:
                raise ValueError(
                    f"primary_key_cols must be set in ingestion_config for "
                    f"write_mode='merge' (table: {full_table_name})"
                )
            self._write_merge(df_out, full_table_name, merge_keys)

        else:  # append (default)
            self._write_append(df_out, full_table_name, evo, partition_column)

        return full_table_name

    # ── Write mode implementations ────────────────────────────────────────────

    def _write_append(
        self,
        df: DataFrame,
        full_table_name: str,
        evo: str,
        partition_column: Optional[str],
    ) -> None:
        writer = df.write.format("delta").mode("append")
        if evo in ("merge", "overwrite"):
            writer = writer.option("mergeSchema", "true")
        if partition_column:
            writer = writer.partitionBy(partition_column)
        writer.saveAsTable(full_table_name)

    def _write_overwrite(
        self,
        df: DataFrame,
        full_table_name: str,
        evo: str,
        partition_column: Optional[str],
    ) -> None:
        writer = df.write.format("delta").mode("overwrite")
        if evo == "overwrite":
            writer = writer.option("overwriteSchema", "true")
        elif evo == "merge":
            writer = writer.option("mergeSchema", "true")
        if partition_column:
            writer = writer.partitionBy(partition_column)
        writer.saveAsTable(full_table_name)

    def _write_merge(
        self,
        df: DataFrame,
        full_table_name: str,
        merge_keys: List[str],
    ) -> None:
        if not self.spark.catalog.tableExists(full_table_name):
            # First run: create the table via a plain write
            df.write.format("delta").saveAsTable(full_table_name)
        else:
            target = DeltaTable.forName(self.spark, full_table_name)
            merge_condition = " AND ".join(
                [f"target.{k} = source.{k}" for k in merge_keys]
            )
            (
                target.alias("target")
                .merge(df.alias("source"), merge_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
