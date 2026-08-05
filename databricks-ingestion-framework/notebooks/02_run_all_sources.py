# Databricks notebook source
# MAGIC %md
# MAGIC # Run Ingestion — Fan-out Driver
# MAGIC
# MAGIC Reads all enabled ingestion_object_ids from `ingestion_config` and
# MAGIC launches `01_run_ingestion` for each one using a thread pool.
# MAGIC
# MAGIC **For production workloads**, prefer the Databricks Jobs "for-each" task
# MAGIC (see `jobs/`) over this notebook — it gives per-object retries, concurrency
# MAGIC control, and full visibility in the Jobs UI.

# COMMAND ----------

import sys
sys.path.append("../src")

from concurrent.futures import ThreadPoolExecutor, as_completed
from ingestion.config_manager import (
    ConfigManager,
    SOURCE_SYSTEM_TABLE,
    INGESTION_CONFIG_TABLE,
    AUDIT_TABLE,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("source_type_filter",     "",                     "Filter by source_type (blank = all)")
dbutils.widgets.text("max_parallel",           "4",                    "Max parallel notebook runs")
dbutils.widgets.text("pipeline_name",          "ingestion_framework",  "Pipeline Name")
dbutils.widgets.text("delta_layer",            "BRONZE",               "Delta Layer")
dbutils.widgets.text("trigger_type",           "SCHEDULED",            "Trigger Type")
dbutils.widgets.text("source_system_table",    SOURCE_SYSTEM_TABLE,    "Source System Table (override)")
dbutils.widgets.text("ingestion_config_table", INGESTION_CONFIG_TABLE, "Ingestion Config Table (override)")
dbutils.widgets.text("audit_table",            AUDIT_TABLE,            "Audit Table (override)")

# COMMAND ----------

# source_type_filter     = dbutils.widgets.get("source_type_filter")     or None
max_parallel           = int(dbutils.widgets.get("max_parallel"))
pipeline_name          = dbutils.widgets.get("pipeline_name")          or "ingestion_framework"
delta_layer            = dbutils.widgets.get("delta_layer")            or "BRONZE"
trigger_type           = dbutils.widgets.get("trigger_type")           or "SCHEDULED"
source_system_table    = dbutils.widgets.get("source_system_table")    or SOURCE_SYSTEM_TABLE
ingestion_config_table = dbutils.widgets.get("ingestion_config_table") or INGESTION_CONFIG_TABLE
audit_table            = dbutils.widgets.get("audit_table")            or AUDIT_TABLE

# COMMAND ----------

config_mgr = ConfigManager(
    spark,
    source_system_table    = source_system_table,
    ingestion_config_table = ingestion_config_table,
)
object_ids = config_mgr.get_active_ingestion_objects()
print(f"Found {len(object_ids)} enabled ingestion object(s): {object_ids}")

# COMMAND ----------

def run_one(obj_id: int):
    try:
        result = dbutils.notebook.run(
            "01_run_ingestion",
            timeout_seconds = 3600,
            arguments = {
                "ingestion_object_id":    str(obj_id),
                "pipeline_name":          pipeline_name,
                "delta_layer":            delta_layer,
                "trigger_type":           trigger_type,
                "source_system_table":    source_system_table,
                "ingestion_config_table": ingestion_config_table,
                "audit_table":            audit_table,
            },
        )
        return obj_id, "SUCCESS", result
    except Exception as exc:
        return obj_id, "FAILED", str(exc)


results = []
with ThreadPoolExecutor(max_workers=max_parallel) as pool:
    futures = {pool.submit(run_one, oid): oid for oid in object_ids}
    for future in as_completed(futures):
        results.append(future.result())

# COMMAND ----------

for obj_id, status, detail in results:
    print(f"ingestion_object_id={obj_id}: {status} — {detail}")

failed = [r for r in results if r[1] == "FAILED"]
if failed:
    raise Exception(
        f"{len(failed)} ingestion object(s) failed: {[f[0] for f in failed]}"
    )
