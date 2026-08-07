# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion — Source System Entry Point
# MAGIC
# MAGIC Runs ALL ingestion objects belonging to a given source system in parallel.
# MAGIC
# MAGIC **Input:** one of `source_system_id`, `source_name`, or `source_type` (or a combination).
# MAGIC The notebook discovers every ingestion object under that source from
# MAGIC `ingestion_config` and processes them concurrently.
# MAGIC
# MAGIC **Fault tolerance:** a failure on one table does NOT stop the others.
# MAGIC All objects are attempted; a summary is printed at the end. The notebook
# MAGIC raises a final exception only if at least one table failed, so the
# MAGIC Databricks Job task correctly shows FAILED.
# MAGIC
# MAGIC **From config table (not widgets):**
# MAGIC - `delta_layer`   → `ingestion_config.delta_layer` per object
# MAGIC - `pipeline_name` → auto-detected from Databricks Job name

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install paramiko --quiet

# COMMAND ----------

import sys
import json
sys.path.append("..")   

from concurrent.futures import ThreadPoolExecutor, as_completed
from ingestion.utils.config_manager import (
    ConfigManager,
    SOURCE_SYSTEM_TABLE,
    INGESTION_CONFIG_TABLE,
    AUDIT_TABLE,
)
from ingestion.utils.orchestrator import IngestionOrchestrator

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("source_system_id",       "",                     "Source System ID (int, blank = all)")
dbutils.widgets.text("source_name",            "",                     "Source Name (e.g. PG_TEST_RDS, blank = all)")
dbutils.widgets.text("source_type",            "",                     "Source Type (e.g. POSTGRES, blank = all)")
dbutils.widgets.text("pipeline_name",          "",                     "Pipeline Name — filters ingestion_config.pipeline_name (blank = auto-detect from job)")
dbutils.widgets.text("landing_volume_path",    "",                     "Landing Volume Base Path (blank = skip landing write)")
dbutils.widgets.text("environment",            "dev",                  "Environment: dev | uat | prod")
dbutils.widgets.text("max_parallel",           "4",                    "Max parallel ingestion runs")
dbutils.widgets.text("trigger_type",           "SCHEDULED",            "Trigger Type: SCHEDULED | MANUAL | EVENT")
dbutils.widgets.text("source_system_table",    SOURCE_SYSTEM_TABLE,    "Source System Table (override)")
dbutils.widgets.text("ingestion_config_table", INGESTION_CONFIG_TABLE, "Ingestion Config Table (override)")
dbutils.widgets.text("audit_table",            AUDIT_TABLE,            "Audit Table (override)")

# COMMAND ----------

source_system_id_raw   = dbutils.widgets.get("source_system_id")       or None
source_name            = dbutils.widgets.get("source_name")            or None
source_type            = dbutils.widgets.get("source_type")            or None
pipeline_name_widget   = dbutils.widgets.get("pipeline_name")          or None
landing_volume_path    = dbutils.widgets.get("landing_volume_path")    or None
environment            = dbutils.widgets.get("environment")            or "dev"
max_parallel           = int(dbutils.widgets.get("max_parallel")       or 4)
trigger_type           = dbutils.widgets.get("trigger_type")           or "SCHEDULED"
source_system_table    = dbutils.widgets.get("source_system_table")    or SOURCE_SYSTEM_TABLE
ingestion_config_table = dbutils.widgets.get("ingestion_config_table") or INGESTION_CONFIG_TABLE
audit_table            = dbutils.widgets.get("audit_table")            or AUDIT_TABLE

source_system_id = int(source_system_id_raw) if source_system_id_raw else None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolve pipeline name
# MAGIC Priority: **widget value** → auto-detected Databricks Job name → `'manual_run'`

# COMMAND ----------

if pipeline_name_widget:
    # Explicit widget value — use as-is (matches ingestion_config.pipeline_name)
    pipeline_name = pipeline_name_widget
    print(f"pipeline_name from widget: '{pipeline_name}'")
else:
    # Auto-detect from Databricks Job context
    try:
        pipeline_name = (
            dbutils.notebook.entry_point
            .getDbutils().notebook().getContext()
            .jobName().get()
        )
        print(f"pipeline_name from job context: '{pipeline_name}'")
    except Exception:
        pipeline_name = "manual_run"
        print(f"pipeline_name fallback: '{pipeline_name}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate config tables are accessible

# COMMAND ----------

# MAGIC %md
# MAGIC ### Discover ingestion objects for this source

# COMMAND ----------

config_mgr = ConfigManager(
    spark,
    source_system_table    = source_system_table,
    ingestion_config_table = ingestion_config_table,
)

object_ids = config_mgr.get_active_ingestion_objects(
    source_system_id = source_system_id,
    source_name      = source_name,
    source_type      = source_type,
    pipeline_name    = pipeline_name,
)

print(f"\nSource filters applied:")
print(f"  source_system_id : {source_system_id}")
print(f"  source_name      : {source_name}")
print(f"  source_type      : {source_type}")
print(f"  pipeline_name    : {pipeline_name}")
print(f"\nFound {len(object_ids)} ingestion object(s) to run: {object_ids}")

if not object_ids:
    dbutils.notebook.exit("No ingestion objects found for the given source filters.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parallel ingestion (fault-tolerant)

# COMMAND ----------

from ingestion.utils.config_manager import AUDIT_TABLE

actual = spark.table(AUDIT_TABLE).schema
for f in actual:
    print(f.name, f.dataType, "nullable=", f.nullable)

# COMMAND ----------

trigger_id = None
try:
    trigger_id = str(
        dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        .currentRunId().get()
    )
except Exception:
    pass

orchestrator = IngestionOrchestrator(
    spark,
    dbutils,
    source_system_table    = source_system_table,
    ingestion_config_table = ingestion_config_table,
    audit_table            = audit_table,
    pipeline_name          = pipeline_name,
    environment            = environment,
)

def run_one(obj_id: int) -> dict:
    """Run a single ingestion object through the orchestrator."""
    return orchestrator.run(
        ingestion_object_id = obj_id,
        landing_volume_path = landing_volume_path,
        trigger_id          = trigger_id,
        trigger_type        = trigger_type,
    )

results = []
with ThreadPoolExecutor(max_workers=max_parallel) as pool:
    futures = {pool.submit(run_one, oid): oid for oid in object_ids}
    for future in as_completed(futures):
        results.append(future.result())
        print(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Results summary

# COMMAND ----------

print(f"\n{'='*75}")
print(f"{'OBJ ID':>8}  {'STATUS':<10}  {'ROWS':>8}  ERROR")
print(f"{'='*75}")
for r in sorted(results, key=lambda x: x["ingestion_object_id"]):
    icon   = "✅" if r["status"] == "SUCCESS" else "❌"
    error  = (r.get("error") or "")[:50]
    print(f"{r['ingestion_object_id']:>8}  {icon} {r['status']:<8}  {r.get('rows_read', 0):>8}  {error}")
print(f"{'='*75}")

succeeded = [r for r in results if r["status"] == "SUCCESS"]
failed    = [r for r in results if r["status"] == "FAILED"]
print(f"Total: {len(results)} | ✅ Succeeded: {len(succeeded)} | ❌ Failed: {len(failed)}\n")

# COMMAND ----------

if failed:
    failed_ids = [r["ingestion_object_id"] for r in failed]
    raise Exception(
        f"{len(failed)} of {len(results)} ingestion object(s) FAILED. "
        f"Check the audit table for details. Failed IDs: {failed_ids}"
    )

dbutils.notebook.exit(f"SUCCESS: {len(succeeded)}/{len(results)} objects ingested.")
