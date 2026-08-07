# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion — Source System Entry Point
# MAGIC
# MAGIC Discovers and executes all active ingestion tasks for a given source system.
# MAGIC
# MAGIC **Input:** `config_master_id` and `source_system_id`.
# MAGIC The notebook finds the correct child config table from `config_master`,
# MAGIC fetches all active tasks (`Is_Active = 1`), and runs them.
# MAGIC
# MAGIC **Fault tolerance:** a failure on one table does NOT stop the others.
# MAGIC All objects are attempted; a summary is printed at the end. The notebook
# MAGIC raises a final exception only if at least one table failed, so the
# MAGIC Databricks Job task correctly shows FAILED.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install paramiko --quiet

# COMMAND ----------

import sys
sys.path.append("..")   

from ingestion.utils.config_manager import (
    ConfigManager,
    SOURCE_SYSTEM_TABLE,
    CONFIG_MASTER_TABLE,
    AUDIT_TABLE,
)
from ingestion.utils.orchestrator import IngestionOrchestrator
from ingestion.utils.config_manager import IngestionTaskConfig

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("config_master_id",       "",                     "Config Master ID (int, points to child table)")
dbutils.widgets.text("source_system_id",       "",                     "Source System ID (int, gets connection info)")
dbutils.widgets.text("target_catalog",         "hive_metastore",       "Target Catalog (e.g. main, hive_metastore)")
dbutils.widgets.text("pipeline_name",          "",                     "Pipeline Name (blank = auto-detect from job)")
dbutils.widgets.text("landing_volume_path",    "",                     "Landing Volume Base Path (blank = skip landing write)")
dbutils.widgets.text("environment",            "dev",                  "Environment: dev | uat | prod")
dbutils.widgets.text("trigger_type",           "SCHEDULED",            "Trigger Type: SCHEDULED | MANUAL | EVENT")

# COMMAND ----------

config_master_id_raw   = int(dbutils.widgets.get("config_master_id"))
source_system_id_raw   = int(dbutils.widgets.get("source_system_id"))

if not config_master_id_raw or not source_system_id_raw:
    dbutils.notebook.exit("Error: config_master_id and source_system_id are required.")

config_master_id = int(config_master_id_raw)
source_system_id = int(source_system_id_raw)

target_catalog         = dbutils.widgets.get("target_catalog")         or "hive_metastore"
pipeline_name_widget   = dbutils.widgets.get("pipeline_name")          or None
landing_volume_path    = dbutils.widgets.get("landing_volume_path")    or None
environment            = dbutils.widgets.get("environment")            or "dev"
trigger_type           = dbutils.widgets.get("trigger_type")           or "SCHEDULED"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolve pipeline name
# MAGIC Priority: **widget value** → auto-detected Databricks Job name → `'manual_run'`

# COMMAND ----------

if pipeline_name_widget:
    pipeline_name = pipeline_name_widget
    print(f"pipeline_name from widget: '{pipeline_name}'")
else:
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
# MAGIC ### Discover ingestion objects for this source

# COMMAND ----------

config_mgr = ConfigManager(
    spark,
    source_system_table = SOURCE_SYSTEM_TABLE,
    config_master_table = CONFIG_MASTER_TABLE,
    target_catalog      = target_catalog,
)

# Fetch the source system config AND all active tasks from the correct child table
source_sys, tasks = config_mgr.get_active_tasks(
    config_master_id = config_master_id,
    source_system_id = source_system_id
)

print(f"\nSource filters applied:")
print(f"  config_master_id : {config_master_id}")
print(f"  source_system_id : {source_system_id} -> Resolved to: {source_sys.source_name}")
print(f"  target_catalog   : {target_catalog}")
print(f"\nFound {len(tasks)} ingestion task(s) to run.")

if not tasks:
    dbutils.notebook.exit("No active ingestion tasks found for the given source filters.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute Ingestion

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
    audit_table   = AUDIT_TABLE,
    pipeline_name = pipeline_name,
    environment   = environment,
)

def run_one(task: IngestionTaskConfig) -> dict:
    """Run a single ingestion object through the orchestrator."""
    return orchestrator.run(
        source_sys          = source_sys,
        task                = task,
        landing_volume_path = landing_volume_path,
        trigger_id          = trigger_id,
        trigger_type        = trigger_type,
    )

results = []
for task in tasks:
    print(f"Starting task: {task.source_object_name} (Config ID: {task.config_id})...")
    res = run_one(task)
    results.append(res)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Results summary

# COMMAND ----------

print(f"\n{'='*75}")
print(f"{'CONF ID':>8}  {'STATUS':<10}  {'ROWS':>8}  ERROR")
print(f"{'='*75}")
for r in sorted(results, key=lambda x: x["config_id"]):
    icon   = "✅" if r["status"] == "SUCCESS" else "❌"
    error  = (r.get("error") or "")[:50]
    print(f"{r['config_id']:>8}  {icon} {r['status']:<8}  {r.get('rows_read', 0):>8}  {error}")
print(f"{'='*75}")

succeeded = [r for r in results if r["status"] == "SUCCESS"]
failed    = [r for r in results if r["status"] == "FAILED"]
print(f"Total: {len(results)} | ✅ Succeeded: {len(succeeded)} | ❌ Failed: {len(failed)}\n")

# COMMAND ----------

if failed:
    failed_ids = [r["config_id"] for r in failed]
    raise Exception(
        f"{len(failed)} of {len(results)} ingestion object(s) FAILED. "
        f"Check the audit table for details. Failed Config IDs: {failed_ids}"
    )

dbutils.notebook.exit(f"SUCCESS: {len(succeeded)}/{len(results)} objects ingested.")
