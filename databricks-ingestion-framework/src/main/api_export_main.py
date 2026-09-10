# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ingestion — API Export Entry Point
# MAGIC
# MAGIC Executes active **API-export** ingestion tasks (LSQ Mavis today).
# MAGIC
# MAGIC This is the sibling of `main.py`: `main.py` runs connector-based sources
# MAGIC (JDBC / S3 / Mongo / federated) through `IngestionOrchestrator`;
# MAGIC this notebook runs the export-API shape — start → poll → download URL →
# MAGIC download + unzip the archive to the raw S3 landing path — via
# MAGIC `MavisApiExtractor`. The landed files are picked up later by the normal
# MAGIC S3 ingestion config.
# MAGIC
# MAGIC **Input:** `config_master_id` and `source_system_id`.
# MAGIC Tasks come from `taskValues` (Task 0 — `get_tasks.py`) in Job mode, or from
# MAGIC a direct config query in standalone mode.
# MAGIC
# MAGIC **Fault tolerance:** a failure on one task does NOT stop the others. Every
# MAGIC task is attempted; a summary is printed at the end and the notebook raises
# MAGIC only if at least one task failed (each failed row is flagged
# MAGIC `Status = FAILED` by `MavisApiExtractor.run`).

# COMMAND ----------

# MAGIC %pip install requests boto3 --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import sys

sys.path.append("..")

from ingestion.connectors.api_connector import MavisApiConfig
from ingestion.utils.config_manager import (
    CONFIG_MASTER_TABLE,
    SOURCE_SYSTEM_TABLE,
    ConfigManager,
    MavisIngestionTaskConfig,
    SourceSystemConfig,
)
from ingestion.utils.mavis_api_extractor import MavisApiExtractor

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("config_master_id", "", "Config Master ID (int)")
dbutils.widgets.text("source_system_id", "", "Source System ID (int)")
dbutils.widgets.text("batch_start_date", "1", "Batch Start Date")
dbutils.widgets.text("get_tasks_task_key", "get_table_details", "Job task key that published active_tasks_metadata")
dbutils.widgets.text("mavis_prod_api", "", "Override MavisApiConfig.prod_api (blank = use default)")

# COMMAND ----------

config_master_id_raw = dbutils.widgets.get("config_master_id") or None
source_system_id_raw = dbutils.widgets.get("source_system_id") or None
if not config_master_id_raw or not source_system_id_raw:
    dbutils.notebook.exit("Error: config_master_id and source_system_id are required.")

config_master_id = int(config_master_id_raw)
source_system_id = int(source_system_id_raw)
batch_start_date = dbutils.widgets.get("batch_start_date") or "1"
get_tasks_task_key = dbutils.widgets.get("get_tasks_task_key") or "get_table_details"
mavis_prod_api = dbutils.widgets.get("mavis_prod_api") or None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Discover tasks
# MAGIC
# MAGIC Job mode: read what `get_tasks.py` published to `taskValues`.
# MAGIC Standalone mode: query the config tables directly.

# COMMAND ----------

config_mgr = ConfigManager(
    spark,
    source_system_table=SOURCE_SYSTEM_TABLE,
    config_master_table=CONFIG_MASTER_TABLE,
)

payload_str = None
try:
    payload_str = dbutils.jobs.taskValues.get(
        taskKey=get_tasks_task_key,
        key="active_tasks_metadata",
        default=None,
        debugValue=None,
    )
except Exception as exc:
    print(f"[INFO] taskValues not available (standalone mode): {exc}")

if payload_str:
    print(f"[Tasks] Reading active tasks from taskValues ('{get_tasks_task_key}').")
    payload = json.loads(payload_str)
    source_sys = SourceSystemConfig.from_dict(payload["source_sys"])
    tasks = [MavisIngestionTaskConfig.from_dict(t) for t in payload["tasks"]]
    batch_start_date = payload.get("batch_start_date") or batch_start_date
else:
    print("[Tasks] taskValues not available — querying config tables directly.")
    source_sys, tasks = config_mgr.get_active_tasks(
        config_master_id=config_master_id,
        source_system_id=source_system_id,
        pipeline_name=None,
        batch_start_date=batch_start_date,
    )

print(f"Resolved source : {source_sys.source_name} ({source_sys.source_type})")
print(f"Active tasks    : {len(tasks)}")

if not tasks:
    dbutils.notebook.exit("No active API-export tasks found.")

if not all(isinstance(t, MavisIngestionTaskConfig) for t in tasks):
    raise RuntimeError(
        "api_export_main received non-Mavis tasks — check the config_master_id "
        "routes to an export-API source (source_name = 'LSQ_Mavis')."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run each export task (fault-tolerant, sequential)

# COMMAND ----------

api_config = MavisApiConfig(prod_api=mavis_prod_api) if mavis_prod_api else None
extractor = MavisApiExtractor(spark, config_mgr, api_config=api_config)

results: dict[int, tuple[str, object]] = {}
for task in tasks:
    print(f"[api_export] config_id={task.config_id} START ({task.source_object_name})")
    try:
        paths = extractor.run(task)
        results[task.config_id] = ("SUCCESS", paths)
        print(f"[api_export] config_id={task.config_id} SUCCESS")
    except Exception as exc:  # extractor already wrote Status=FAILED
        results[task.config_id] = ("FAILED", str(exc))
        print(f"[api_export] config_id={task.config_id} FAILED: {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary

# COMMAND ----------

succeeded = [c for c, (s, _) in results.items() if s == "SUCCESS"]
failed = [c for c, (s, _) in results.items() if s == "FAILED"]

print(f"Total   : {len(results)}")
print(f"Success : {len(succeeded)} {succeeded}")
print(f"Failed  : {len(failed)} {failed}")
for cid in failed:
    print(f"  config_id={cid}: {results[cid][1]}")

if failed:
    raise RuntimeError(
        f"{len(failed)} of {len(results)} API-export task(s) failed: {failed}"
    )

dbutils.notebook.exit(
    f"Success: {len(succeeded)} API-export task(s) completed for "
    f"{source_sys.source_name}."
)
