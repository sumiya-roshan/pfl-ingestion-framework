# Databricks notebook source
# MAGIC %md
# MAGIC # Get Ingestion Tasks Metadata
# MAGIC
# MAGIC Runs as **Task 0** before the lookup check in the ingestion job.
# MAGIC Fetches active tasks configurations and publishes them for downstream tasks.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
sys.path.append("..")

import json
from ingestion.utils.config_manager import (
    ConfigManager,
    SOURCE_SYSTEM_TABLE,
    CONFIG_MASTER_TABLE,
)

# COMMAND ----------

dbutils.widgets.text("config_master_id",    "",               "Config Master ID")
dbutils.widgets.text("source_system_id",    "",               "Source System ID")
dbutils.widgets.text("target_catalog",      "hive_metastore", "Target Catalog")
dbutils.widgets.text("pipeline_name",       "",               "Pipeline Name")
dbutils.widgets.text("batch_start_date",    "1",              "Batch Start Date")

# COMMAND ----------

config_master_id_raw = dbutils.widgets.get("config_master_id") or None
source_system_id_raw = dbutils.widgets.get("source_system_id") or None
target_catalog       = dbutils.widgets.get("target_catalog")   or "hive_metastore"
pipeline_name        = dbutils.widgets.get("pipeline_name")    or None
batch_start_date     = dbutils.widgets.get("batch_start_date") or "1"

if not config_master_id_raw or not source_system_id_raw:
    dbutils.notebook.exit("Error: config_master_id and source_system_id are required.")

if not pipeline_name:
    dbutils.notebook.exit("Error: pipeline_name widget is required.")

config_master_id  = int(config_master_id_raw)
source_system_id  = int(source_system_id_raw)

# COMMAND ----------

config_mgr = ConfigManager(
    spark,
    source_system_table = SOURCE_SYSTEM_TABLE,
    config_master_table = CONFIG_MASTER_TABLE,
    target_catalog      = target_catalog,
)

source_sys, tasks = config_mgr.get_active_tasks(
    config_master_id = config_master_id,
    source_system_id = source_system_id,
    pipeline_name    = pipeline_name,
    batch_start_date = batch_start_date,
)

print(f"Resolved source : {source_sys.source_name} ({source_sys.source_type})")
print(f"Active tasks    : {len(tasks)}")

if not tasks:
    try:
        dbutils.jobs.taskValues.set(key="active_tasks_metadata", value="")
    except Exception:
        pass
    dbutils.notebook.exit("No active ingestion tasks found.")

# COMMAND ----------

# Serialize source system and task configs
payload = {
    "source_sys": source_sys.to_dict(),
    "tasks": [task.to_dict() for task in tasks]
}
payload_str = json.dumps(payload)

print(f"Publishing {len(tasks)} tasks metadata to taskValues...")

try:
    dbutils.jobs.taskValues.set(key="active_tasks_metadata", value=payload_str)
    print("Successfully set active_tasks_metadata.")
except Exception as e:
    print(f"[INFO] taskValues not available (standalone mode): {e}")

dbutils.notebook.exit(f"Success: Fetched {len(tasks)} active tasks for pipeline '{pipeline_name}'.")
