"""
Writes the extracted DataFrame into a Databricks bronze Delta table.

Supported write modes (driven by ingestion_config.write_mode):
  append    — adds new rows; mergeSchema honours schema evolution
  overwrite — full table replace; respects schema_evolution_mode
  merge     — upsert via DeltaTable.merge; requires primary_key_cols

Schema evolution (driven by ingestion_config.schema_evolution_mode):
  none / NULL  — fails if schema changes
  merge        — mergeSchema=true
  overwrite    — overwriteSchema=true (overwrite mode only)

Serverless compatibility:
  Uses DataFrameWriterV2 (writeTo) API throughout — no saveAsTable/PERSIST TABLE.
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
        full_table_name = f"{catalog}.{schema}.{table}"
        evo = (schema_evolution_mode or "none").lower()

        df_out = df.withColumn("_ingested_at", F.current_timestamp())
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

        if write_mode == "overwrite":
            self._write_overwrite(df_out, full_table_name, evo, partition_column)
        elif write_mode == "merge":
            if not merge_keys:
                raise ValueError(
                    f"primary_key_cols must be set for write_mode='merge' "
                    f"(table: {full_table_name})"
                )
            self._write_merge(df_out, full_table_name, merge_keys)
        else:
            self._write_append(df_out, full_table_name, evo, partition_column)

        return full_table_name

    # ── Write mode implementations ────────────────────────────────────────────

    def _write_append(self, df, full_table_name, evo, partition_column):
        if not self.spark.catalog.tableExists(full_table_name):
            writer = df.writeTo(full_table_name).using("delta")
            if evo in ("merge", "overwrite"):
                writer = writer.option("mergeSchema", "true")
            if partition_column:
                writer = writer.partitionedBy(partition_column)
            writer.create()
        else:
            writer = df.writeTo(full_table_name)
            if evo in ("merge", "overwrite"):
                writer = writer.option("mergeSchema", "true")
            writer.append()

    def _write_overwrite(self, df, full_table_name, evo, partition_column):
        writer = df.writeTo(full_table_name).using("delta")
        if evo == "overwrite":
            writer = writer.option("overwriteSchema", "true")
        elif evo == "merge":
            writer = writer.option("mergeSchema", "true")
        if partition_column:
            writer = writer.partitionedBy(partition_column)
        writer.createOrReplace()

    def _write_merge(self, df, full_table_name, merge_keys):
        if not self.spark.catalog.tableExists(full_table_name):
            df.writeTo(full_table_name).using("delta").create()
        else:
            target = DeltaTable.forName(self.spark, full_table_name)
            condition = " AND ".join(
                [f"target.{k} = source.{k}" for k in merge_keys]
            )
            (
                target.alias("target")
                .merge(df.alias("source"), condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
