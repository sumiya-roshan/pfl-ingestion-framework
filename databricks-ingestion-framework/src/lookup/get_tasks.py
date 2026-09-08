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
from datetime import datetime, timezone
from ingestion.utils.config_manager import (
    CONFIG_MASTER_TABLE,
    MAVIS_STATUS_NOT_STARTED,
    SOURCE_SYSTEM_TABLE,
    ConfigManager,
)

# config_master_id reserved for LSQ_Mavis. Nothing about config resolution is
# special — config_master gives the child table FQN, config_source_system gives
# source_name/retry. This id only tells the notebook to run the Mavis-shaped
# Stage-1 batch reset below (in place of the RDBMS one) and skip pipeline_name.
MAVIS_CONFIG_MASTER_ID = 999

# COMMAND ----------

dbutils.widgets.text("config_master_id", "", "Config Master ID")
dbutils.widgets.text("source_system_id", "", "Source System ID")
dbutils.widgets.text("pipeline_name", "", "Pipeline Name")
dbutils.widgets.text("batch_start_date", "1", "Batch Start Date")

# COMMAND ----------

config_master_id_raw = dbutils.widgets.get("config_master_id") or None
source_system_id_raw = dbutils.widgets.get("source_system_id") or None
pipeline_name        = dbutils.widgets.get("pipeline_name") or None
batch_start_date     = dbutils.widgets.get("batch_start_date") or "1"

if not config_master_id_raw:
    dbutils.notebook.exit("Error: config_master_id is required.")

config_master_id = int(config_master_id_raw)
is_mavis = config_master_id == MAVIS_CONFIG_MASTER_ID

if not source_system_id_raw:
    dbutils.notebook.exit(
        "Error: config_master_id and source_system_id are required."
    )
source_system_id = int(source_system_id_raw)

if not is_mavis and not pipeline_name:
    dbutils.notebook.exit("Error: pipeline_name widget is required.")

# COMMAND ----------

config_mgr = ConfigManager(
    spark,
    source_system_table=SOURCE_SYSTEM_TABLE,
    config_master_table=CONFIG_MASTER_TABLE,
)

# COMMAND ----------

# Stage 1 — batch start: reset Status / Day_Execution_Count and stamp
# sink_batch_started_date ONCE per run. The timestamp is generated here in
# Python and passed as a literal so every matching row gets the exact same
# value. Two source-specific shapes; both feed the same get_active_tasks below.
if is_mavis:
    # LSQ_Mavis two-stage reset (ported from ADF). source_name from
    # config_source_system, table FQN from config_master — resolved the normal
    # ConfigManager way. batch_start_date "1" == fresh run (generate now()),
    # any real value == re-run for that historical batch.
    mavis_source_name = config_mgr.get_source_system(source_system_id).source_name
    mavis_fqn = config_mgr._child_table_fqn(config_master_id)
    _mcols = spark.table(mavis_fqn).columns

    def _c(*names):
        return config_mgr._resolve_col(_mcols, *names)

    _src, _act = _c("source_name", "Source_Name"), _c("is_active", "is_active")
    _st, _dec = _c("status", "Status"), _c("day_execution_count", "Day_Execution_Count")
    _sink = _c("sink_batch_started_date", "sink_batch_started_date")

    if batch_start_date == "1":
        batch_start_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        spark.sql(f"""
            UPDATE {mavis_fqn}
            SET {_st} = '{MAVIS_STATUS_NOT_STARTED}', {_dec} = 0,
                {_sink} = TIMESTAMP '{batch_start_date}'
            WHERE {_src} = '{mavis_source_name}' AND {_act} = 1
              AND (to_date({_sink}) != DATE '{batch_start_date[:10]}'
                   OR {_sink} IS NULL)
        """)
    else:
        spark.sql(f"""
            UPDATE {mavis_fqn}
            SET {_st} = '{MAVIS_STATUS_NOT_STARTED}'
            WHERE {_src} = '{mavis_source_name}' AND {_act} = 1
              AND date_format({_sink}, 'yyyy-MM-dd HH:mm:ss')
                  = date_format(TIMESTAMP '{batch_start_date}', 'yyyy-MM-dd HH:mm:ss')
        """)
    print("mavis batch_start_date", batch_start_date)

elif batch_start_date == "1":
    batch_start_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    spark.sql(f"""
        UPDATE migration_x_catalog.pfl_x_schema.rdbms_ingestion_config
        SET Status = 'In Progress', Day_Execution_Count = 0,
            sink_batch_started_date = TIMESTAMP '{batch_start_date}'
        WHERE Source_Name = 'PG_TEST_RDS'
    """)
    print("batch_start_date", batch_start_date, type(batch_start_date))

# COMMAND ----------

# Stage 2 — same call for every source. get_active_tasks builds
# MavisIngestionTaskConfig objects for the LSQ_Mavis source, IngestionTaskConfig
# otherwise; routing / filtering is identical.
source_sys, tasks = config_mgr.get_active_tasks(
    config_master_id=config_master_id,
    source_system_id=source_system_id,
    pipeline_name=None if is_mavis else pipeline_name,
    batch_start_date=batch_start_date,
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

# Serialize source system and task configs
payload = {
    "source_sys": source_sys.to_dict(),
    "tasks": [task.to_dict() for task in tasks],
    "batch_start_date" : batch_start_date,
}
payload_str = json.dumps(payload)

print(f"Publishing {len(tasks)} tasks metadata to taskValues...")

try:
    dbutils.jobs.taskValues.set(key="active_tasks_metadata", value=payload_str)
    print("Successfully set active_tasks_metadata.")
except Exception as e:
    print(f"[INFO] taskValues not available (standalone mode): {e}")

_scope = "LSQ_Mavis" if is_mavis else f"pipeline '{pipeline_name}'"
dbutils.notebook.exit(
    f"Success: Fetched {len(tasks)} active tasks for {_scope}."
)
