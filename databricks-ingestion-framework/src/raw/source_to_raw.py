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
# MAGIC `source_sys_json`/`ingest_obj_json` carry everything `get_connector()`
# MAGIC and `connector.extract()` need (credentials, host, query, load type,
# MAGIC watermark columns, ...) — both already have `to_dict()`/`from_dict()`,
# MAGIC so this reuses that instead of flattening 20+ fields into individual
# MAGIC widgets the way the Silver notebook does.

# COMMAND ----------

dbutils.widgets.text("source_sys_json", "", "JSON — SourceSystemConfig.to_dict()")
dbutils.widgets.text("ingest_obj_json", "", "JSON — IngestionTaskConfig.to_dict()")
dbutils.widgets.text("raw_bucket_path", "", "Base S3/Volume path for the raw landing write")
dbutils.widgets.text("file_timestamp", "", "ISO datetime — the batch's sink_batch_started_date, used to build the dated landing folder")
dbutils.widgets.text("run_id", "", "Job run ID — for retry/log message tagging only")

# COMMAND ----------

source_sys_json    = dbutils.widgets.get("source_sys_json")
ingest_obj_json    = dbutils.widgets.get("ingest_obj_json")
raw_bucket_path    = dbutils.widgets.get("raw_bucket_path") or None
file_timestamp_raw = dbutils.widgets.get("file_timestamp") or None
run_id             = dbutils.widgets.get("run_id") or ""

if not source_sys_json or not ingest_obj_json:
    dbutils.notebook.exit(json.dumps({
        "status": "FAILED",
        "error": "source_sys_json and ingest_obj_json widgets are both required.",
    }))
if not raw_bucket_path:
    dbutils.notebook.exit(json.dumps({
        "status": "FAILED",
        "error": "raw_bucket_path widget is required.",
    }))

source_sys = SourceSystemConfig.from_dict(json.loads(source_sys_json))
ingest_obj = IngestionTaskConfig.from_dict(json.loads(ingest_obj_json))

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

secrets      = SecretResolver(dbutils)
error_msg    = "No error"
rows_read    = 0
landing_path = None

try:
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
        raw_bucket_path     = raw_bucket_path,
        source_name         = source_sys.source_name,
        source_schema       = ingest_obj.source_schema,
        source_object_name  = ingest_obj.source_object_name,
        file_format         = fmt,
        file_timestamp      = file_timestamp,
    )
    print(
        f"[SOURCE_TO_RAW] config_id={ingest_obj.config_id} "
        f"Landing write → {landing_path} ({rows_read} rows, format={fmt})"
    )
except Exception as e:
    error_msg = str(e)
    print(f"[SOURCE_TO_RAW] config_id={ingest_obj.config_id} FAILED: {error_msg}")

# COMMAND ----------

if error_msg == "No error":
    dbutils.notebook.exit(json.dumps({
        "status":       "SUCCESS",
        "rows_read":    rows_read,
        "landing_path": landing_path,
        "error":        None,
    }))
else:
    dbutils.notebook.exit(json.dumps({
        "status":       "FAILED",
        "rows_read":    0,
        "landing_path": None,
        "error":        error_msg,
    }))
