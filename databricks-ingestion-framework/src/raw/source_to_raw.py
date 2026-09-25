# Databricks notebook source
# MAGIC %md
# MAGIC # Source → Raw — Generic Extract Notebook
# MAGIC
# MAGIC Triggered by `IngestionOrchestrator` via `dbutils.notebook.run()` — one
# MAGIC call per table, synchronous, on that table's own thread, same pattern
# MAGIC the Silver trigger already uses. Existing purely so this stage shows up
# MAGIC as its own step in the Databricks Jobs "Notebook Workflows" UI instead
# MAGIC of being buried inside `orchestrator.run()`'s own execution.
# MAGIC
# MAGIC Handles every source type (JDBC/RDBMS, NoSQL, S3, Federated) through
# MAGIC the same `get_connector()` factory `orchestrator.py` used to call
# MAGIC inline — nothing about connector selection or the extract/write logic
# MAGIC itself changes, only where it physically runs.
# MAGIC
# MAGIC Staging_Flag=1 primary-key-only extraction (the `all_key_...` file) is
# MAGIC NOT handled here — that stays inline in `orchestrator.py`, unchanged,
# MAGIC and builds its own connector separately.

# COMMAND ----------

import sys

sys.path.append("..")

import json
import time
from datetime import datetime, timezone

from ingestion.connectors.factory import get_connector
from ingestion.utils.config_manager import IngestionTaskConfig, SourceSystemConfig
from ingestion.utils.retry import retry_on_failure
from ingestion.utils.secrets import SecretResolver
from ingestion.utils.watermark import resolve_watermark
from ingestion.utils.writers.s3_writer import S3RawWriter

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets
# MAGIC
# MAGIC `source_sys_json` carries the source connection info `get_connector()`
# MAGIC needs (host, port, driver_class, secret_scope, ...) as one JSON blob —
# MAGIC not broken out field-by-field, since it's plumbing rather than anything
# MAGIC useful to see at a glance in the Run Parameters panel.
# MAGIC
# MAGIC Every `IngestionTaskConfig` field (the per-table config — config_id,
# MAGIC source_schema, load_type, target_catalog/schema/table, ...) is its own
# MAGIC widget instead, so each one is individually visible there, the same way
# MAGIC the Silver notebook's parameters are.

# COMMAND ----------

dbutils.widgets.text("source_sys_json", "", "JSON — SourceSystemConfig.to_dict()")

# One widget per IngestionTaskConfig field, name-for-name.
dbutils.widgets.text("config_id", "", "IngestionTaskConfig.config_id")
dbutils.widgets.text("source_schema", "", "IngestionTaskConfig.source_schema")
dbutils.widgets.text("source_object_name", "", "IngestionTaskConfig.source_object_name")
dbutils.widgets.text("custom_query", "", "IngestionTaskConfig.custom_query")
dbutils.widgets.text("load_type", "", "IngestionTaskConfig.load_type")
dbutils.widgets.text("incremental_column", "", "IngestionTaskConfig.incremental_column")
dbutils.widgets.text("primary_key_cols", "", "IngestionTaskConfig.primary_key_cols")
dbutils.widgets.text("target_catalog", "", "IngestionTaskConfig.target_catalog")
dbutils.widgets.text("target_schema", "", "IngestionTaskConfig.target_schema")
dbutils.widgets.text("target_table", "", "IngestionTaskConfig.target_table")
dbutils.widgets.text("pipeline_name", "", "IngestionTaskConfig.pipeline_name")
dbutils.widgets.text("delta_layer", "", "IngestionTaskConfig.delta_layer")
dbutils.widgets.text("data_read_size", "", "IngestionTaskConfig.data_read_size")
dbutils.widgets.text("file_format", "", "IngestionTaskConfig.file_format")
dbutils.widgets.text("write_mode", "", "IngestionTaskConfig.write_mode")
dbutils.widgets.text("priority", "", "IngestionTaskConfig.priority")
dbutils.widgets.text("batch_id", "", "IngestionTaskConfig.batch_id")
dbutils.widgets.text("s3_source_bucket_name", "", "IngestionTaskConfig.s3_source_bucket_name")
dbutils.widgets.text("s3_external_path", "", "IngestionTaskConfig.s3_external_path")
dbutils.widgets.text("s3_column_delimiter", "", "IngestionTaskConfig.s3_column_delimiter")
dbutils.widgets.text("s3_first_row_header", "", "IngestionTaskConfig.s3_first_row_header")
dbutils.widgets.text("s3_raw_sink_bucket_name", "", "IngestionTaskConfig.s3_raw_sink_bucket_name")
dbutils.widgets.text("s3_raw_sink_file_path", "", "IngestionTaskConfig.s3_raw_sink_file_path")
dbutils.widgets.text("schema_evolution_mode", "", "IngestionTaskConfig.schema_evolution_mode")
dbutils.widgets.text("partition_column", "", "IngestionTaskConfig.partition_column")
dbutils.widgets.text("source_filter", "", "IngestionTaskConfig.source_filter")
dbutils.widgets.text("staging_flag", "", "IngestionTaskConfig.staging_flag")
dbutils.widgets.text("config_master_id", "", "IngestionTaskConfig.config_master_id")
dbutils.widgets.text("silver_last_sink_date", "", "IngestionTaskConfig.silver_last_sink_date")
dbutils.widgets.text("delta_column_2", "", "IngestionTaskConfig.delta_column_2")
dbutils.widgets.text("lookback_hours", "", "IngestionTaskConfig.lookback_hours")
dbutils.widgets.text("child_table_fqn", "", "IngestionTaskConfig.child_table_fqn")
dbutils.widgets.text("recipients", "", "IngestionTaskConfig.recipients")

dbutils.widgets.text("landing_volume_path", "", "Base S3/Volume path for the raw landing write")
dbutils.widgets.text("file_timestamp", "", "ISO datetime — the batch's sink_batch_started_date, used to build the dated landing folder")
dbutils.widgets.text("run_id", "", "Job run ID — for retry/log message tagging only")

# COMMAND ----------

def _str(name):
    """None if the widget is empty, otherwise its raw string value."""
    v = dbutils.widgets.get(name)
    return v if v else None


def _int(name):
    v = dbutils.widgets.get(name)
    return int(v) if v else None


def _bool(name):
    v = dbutils.widgets.get(name)
    if not v:
        return None
    return v.strip().lower() in ("true", "1", "yes")


source_sys_json      = dbutils.widgets.get("source_sys_json")
landing_volume_path  = _str("landing_volume_path")
file_timestamp_raw   = _str("file_timestamp")
run_id               = dbutils.widgets.get("run_id") or ""

if not source_sys_json:
    dbutils.notebook.exit(json.dumps({
        "status": "FAILED",
        "error": "source_sys_json widget is required.",
    }))
if not landing_volume_path:
    dbutils.notebook.exit(json.dumps({
        "status": "FAILED",
        "error": "landing_volume_path widget is required.",
    }))

source_sys = SourceSystemConfig.from_dict(json.loads(source_sys_json))

# config_id/source_object_name/load_type/target_catalog/target_schema/
# target_table/pipeline_name/write_mode/priority/batch_id have no default on
# IngestionTaskConfig (required fields) — every one of them is sent by
# SourceToRawProcessor.trigger() (driven off ingest_obj.to_dict(), so it
# can't leave one out), so _str()/_int() returning None here would mean a
# genuinely missing widget, not an expected-empty one.
ingest_obj = IngestionTaskConfig(
    config_id              = _int("config_id"),
    source_schema           = _str("source_schema"),
    source_object_name      = _str("source_object_name"),
    custom_query             = _str("custom_query"),
    load_type                = _str("load_type"),
    incremental_column       = _str("incremental_column"),
    primary_key_cols         = _str("primary_key_cols"),
    target_catalog           = _str("target_catalog"),
    target_schema            = _str("target_schema"),
    target_table             = _str("target_table"),
    pipeline_name            = _str("pipeline_name"),
    delta_layer              = _str("delta_layer"),
    data_read_size           = _int("data_read_size"),
    file_format               = _str("file_format"),
    write_mode                = _str("write_mode"),
    priority                  = _int("priority"),
    batch_id                  = _int("batch_id"),
    s3_source_bucket_name     = _str("s3_source_bucket_name"),
    s3_external_path          = _str("s3_external_path"),
    s3_column_delimiter       = _str("s3_column_delimiter"),
    s3_first_row_header       = _bool("s3_first_row_header"),
    s3_raw_sink_bucket_name   = _str("s3_raw_sink_bucket_name"),
    s3_raw_sink_file_path     = _str("s3_raw_sink_file_path"),
    schema_evolution_mode     = _str("schema_evolution_mode"),
    partition_column          = _str("partition_column"),
    source_filter             = _str("source_filter"),
    staging_flag              = _int("staging_flag"),
    config_master_id          = _int("config_master_id"),
    silver_last_sink_date     = _str("silver_last_sink_date"),
    delta_column_2            = _str("delta_column_2"),
    lookback_hours            = _int("lookback_hours"),
    child_table_fqn           = _str("child_table_fqn"),
    recipients                = _str("recipients"),
)

# file_timestamp drives the dated landing folder (S3RawWriter appends
# YYYY/Mon/DD) — parsed the same way the client Silver notebook's trigger_time
# is, just without that one's [:-2]/7-digit-fraction quirk since we control
# both ends of this format ourselves.
file_timestamp = (
    datetime.fromisoformat(file_timestamp_raw.strip().replace("T", " "))
    if file_timestamp_raw
    else datetime.now(timezone.utc)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract + write

# COMMAND ----------

def _dir_size_bytes(path: str) -> int:
    """
    Recursively sums file sizes under a Volume/S3 path via dbutils.fs.ls —
    kept off sparkContext._jvm / df._jdf on purpose since those are
    unavailable under Spark Connect / serverless (see S3RawWriter's own
    dbutils.fs-based rename for the same reasoning).
    """
    total = 0
    for f in dbutils.fs.ls(path):
        total += _dir_size_bytes(f.path) if f.isDir() else f.size
    return total


secrets                  = SecretResolver(dbutils)
error_msg                = "No error"
rows_read                = 0
landing_path             = None
data_read                = 0
data_written             = 0
throughput               = None
copy_duration_in_seconds = 0.0

try:
    copy_start_time = time.time()
    watermark_start = resolve_watermark(ingest_obj)
    print(
        f"[SOURCE_TO_RAW] config_id={ingest_obj.config_id} "
        f"source='{source_sys.source_name}' object='{ingest_obj.source_object_name}' "
        f"load_type={ingest_obj.load_type} watermark_start={watermark_start}"
    )

    connector = get_connector(spark, source_sys, ingest_obj, secrets)
    df, _watermark_end = retry_on_failure(
        lambda: connector.extract(watermark_start),
        max_retries    = int(source_sys.retry_count or 0),
        retry_interval = int(source_sys.retry_interval or 0),
        logger         = None,
        description    = f"[{run_id}] extract config_id={ingest_obj.config_id} object='{ingest_obj.source_object_name}'",
    )
    rows_read = df.count()

    fmt = ingest_obj.file_format or "parquet"
    landing_path = S3RawWriter().write(
        df,
        landing_volume_path = landing_volume_path,
        source_name         = source_sys.source_name,
        source_schema       = ingest_obj.source_schema,
        source_object_name  = ingest_obj.source_object_name,
        file_format         = fmt,
        file_timestamp      = file_timestamp,
    )
    copy_duration_in_seconds = round(time.time() - copy_start_time, 2)

    # Straight extract→write with no transform in between, so the source
    # byte count and the landing byte count are the same data — there's no
    # per-connector "bytes read" figure to pull separately (JDBC/API/S3
    # connectors all hand back a materialized DataFrame, not a byte count).
    data_written = _dir_size_bytes(landing_path)
    data_read = data_written
    throughput = (
        round((data_written / (1024.0 * 1024.0)) / copy_duration_in_seconds, 2)
        if copy_duration_in_seconds > 0
        else None
    )

    print(
        f"[SOURCE_TO_RAW] config_id={ingest_obj.config_id} "
        f"Landing write → {landing_path} ({rows_read} rows, format={fmt}, "
        f"{data_written} bytes, {copy_duration_in_seconds}s, {throughput} MB/s)"
    )
except Exception as e:
    error_msg = str(e)
    print(f"[SOURCE_TO_RAW] config_id={ingest_obj.config_id} FAILED: {error_msg}")

# COMMAND ----------

if error_msg == "No error":
    dbutils.notebook.exit(json.dumps({
        "status":                   "SUCCESS",
        "rows_read":                rows_read,
        "landing_path":             landing_path,
        "data_read":                data_read,
        "data_written":             data_written,
        "throughput":               throughput,
        "copy_duration_in_seconds": copy_duration_in_seconds,
        "error":                    None,
    }))
else:
    dbutils.notebook.exit(json.dumps({
        "status":                   "FAILED",
        "rows_read":                0,
        "landing_path":             None,
        "data_read":                0,
        "data_written":             0,
        "throughput":               None,
        "copy_duration_in_seconds": copy_duration_in_seconds,
        "error":                    error_msg,
    }))
