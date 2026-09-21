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
    CONFIG_MASTER_TABLE,
    MAVIS_STATUS_NOT_STARTED,
    SOURCE_SYSTEM_TABLE,
    ConfigManager,
    resolve_child_table_fqn,
)

# config_master_id reserved for LSQ_Mavis.
MAVIS_CONFIG_MASTER_ID = 999

# COMMAND ----------

dbutils.widgets.text("config_master_id", "", "Config Master ID")
dbutils.widgets.text("source_system_id", "", "Source System ID (RDBMS/NoSQL/S3)")
dbutils.widgets.text("source_name", "", "Lentra only: source name directly")
dbutils.widgets.text("pipeline_name", "", "Pipeline Name")
dbutils.widgets.text("batch_start_date", "", "Batch Start Date (IST timestamp from Main Pipeline)")

# COMMAND ----------

config_master_id_raw = dbutils.widgets.get("config_master_id") or None
source_system_id_raw = dbutils.widgets.get("source_system_id") or None
source_name          = dbutils.widgets.get("source_name") or None
pipeline_name        = dbutils.widgets.get("pipeline_name") or None
batch_start_date     = dbutils.widgets.get("batch_start_date")

if not config_master_id_raw:
    dbutils.notebook.exit("Error: config_master_id is required.")

config_master_id = int(config_master_id_raw)
is_mavis = config_master_id == MAVIS_CONFIG_MASTER_ID

if is_mavis and not source_system_id_raw:
    dbutils.notebook.exit("Error: config_master_id and source_system_id are required.")
if not is_mavis and not source_system_id_raw and not source_name:
    dbutils.notebook.exit("Error: either source_system_id or source_name is required.")

source_system_id = int(source_system_id_raw) if source_system_id_raw else None

# COMMAND ----------

config_mgr = ConfigManager(
    spark,
    source_system_table=SOURCE_SYSTEM_TABLE,
    config_master_table=CONFIG_MASTER_TABLE,
)

is_lentra = False
if not is_mavis:
    child_table_fqn = resolve_child_table_fqn(spark, CONFIG_MASTER_TABLE, config_master_id)
    _child_columns = spark.table(child_table_fqn).columns
    is_lentra = config_mgr.is_lentra_shaped(_child_columns)

if not is_mavis and not is_lentra and not pipeline_name:
    dbutils.notebook.exit("Error: pipeline_name widget is required.")

# COMMAND ----------

# All initialization and database updates are now handled by get_active_pipelines.py
# in the Main Pipeline. We just use the provided batch_start_date as trigger_time.
trigger_time = batch_start_date

# COMMAND ----------

# Fetch active tasks
source_sys, tasks = config_mgr.get_active_tasks(
    config_master_id=config_master_id,
    source_system_id=source_system_id,
    source_name=source_name,
    pipeline_name=None if is_mavis else pipeline_name,
    batch_start_date=trigger_time,
)

print(f"Resolved source : {source_sys.source_name} ({source_sys.source_type})")
print(f"Active tasks    : {len(tasks)}")

if not tasks:
    try:
        dbutils.jobs.taskValues.set(key="active_tasks_metadata", value="")
    except Exception as exc:
        print(f"[INFO] taskValues not available (standalone mode): {exc}")
    dbutils.notebook.exit("No active ingestion tasks found.")

# COMMAND ----------

payload = {
    "source_sys": source_sys.to_dict(),
    "tasks": [task.to_dict() for task in tasks],
    "batch_start_date" : trigger_time,
    "is_lentra": is_lentra,
}
payload_str = json.dumps(payload)

print(f"Publishing {len(tasks)} tasks metadata to taskValues...")

try:
    dbutils.jobs.taskValues.set(key="active_tasks_metadata", value=payload_str)
    print("Successfully set active_tasks_metadata.")
except Exception as e:
    print(f"[INFO] taskValues not available (standalone mode): {e}")

_scope = "LSQ_Mavis" if is_mavis else f"pipeline '{pipeline_name}'"
dbutils.notebook.exit(f"Success: Fetched {len(tasks)} active tasks for {_scope}.")
