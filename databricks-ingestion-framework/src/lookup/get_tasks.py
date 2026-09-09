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
    SOURCE_SYSTEM_TABLE,
    ConfigManager,
    resolve_child_table_fqn,
)

# COMMAND ----------

dbutils.widgets.text("config_master_id", "", "Config Master ID")
dbutils.widgets.text("source_system_id", "", "Source System ID (RDBMS/NoSQL/S3)")
dbutils.widgets.text("source_name", "", "Lentra only: source name directly (matches the config table's Source_Name exactly) — alternative to source_system_id")
dbutils.widgets.text("pipeline_name", "", "Pipeline Name")
dbutils.widgets.text("batch_start_date", "1", "Batch Start Date")

# COMMAND ----------

config_master_id_raw = dbutils.widgets.get("config_master_id") or None
source_system_id_raw = dbutils.widgets.get("source_system_id") or None
source_name   = dbutils.widgets.get("source_name") or None
pipeline_name        = dbutils.widgets.get("pipeline_name") or None
batch_start_date     = dbutils.widgets.get("batch_start_date") or "1"

if not config_master_id_raw:
    dbutils.notebook.exit("Error: config_master_id is required.")
if not source_system_id_raw and not source_name:
    dbutils.notebook.exit("Error: either source_system_id or source_name is required.")

config_master_id = int(config_master_id_raw)
source_system_id = int(source_system_id_raw) if source_system_id_raw else None

# COMMAND ----------

config_mgr = ConfigManager(
    spark,
    source_system_table=SOURCE_SYSTEM_TABLE,
    config_master_table=CONFIG_MASTER_TABLE,
)

# Resolve the child table early — before the pipeline_name check and batch
# reset below — so the Lentra shape can be detected by its own columns
# (ConfigManager.is_lentra_shaped), the same way get_active_tasks() does.
# Lentra doesn't use pipeline_name for filtering and has its own reset shape
# (Report_Name like 'lentra%hdr', no Day_Execution_Count/batch-date stamping).
child_table_fqn = resolve_child_table_fqn(spark, CONFIG_MASTER_TABLE, config_master_id)
_child_columns = spark.table(child_table_fqn).columns
is_lentra = config_mgr.is_lentra_shaped(_child_columns)

if not is_lentra and not pipeline_name:
    dbutils.notebook.exit("Error: pipeline_name widget is required.")

# COMMAND ----------

# Batch start / reset — different shape per source.
if is_lentra:
    # source_name given directly (the normal Lentra path — no
    # config_source_system row) or resolved via source_system_id (RDBMS-style
    # fallback, if someone did set one up).
    lentra_source_name = source_name or (
        config_mgr.get_source_system(source_system_id).source_name
        if source_system_id
        else None
    )
    if not lentra_source_name:
        dbutils.notebook.exit("Error: source_name (or source_system_id) is required for Lentra sources.")

    src_col = config_mgr.resolve_col(_child_columns, "source_name", "Source_Name")
    active_col = config_mgr.resolve_col(_child_columns, "is_active", "Is_Active")
    status_col = config_mgr.resolve_col(_child_columns, "status", "Status")
    report_col = config_mgr.resolve_col(_child_columns, "Report_Name")

    # Same "1" sentinel -> real UTC timestamp resolution the RDBMS branch
    # does below — without this, batch_start_date stays "1" for the rest of
    # the run (published to main.py as-is, never a real timestamp). Not
    # written back to the table — nothing reads it back for Lentra, it's
    # only needed as the in-memory value published downstream.
    if batch_start_date == "1":
        batch_start_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

    spark.sql(f"""
        UPDATE {child_table_fqn}
        SET {status_col} = 'Not-Started'
        WHERE {report_col} like 'lentra%hdr'
          AND {src_col} = '{lentra_source_name}'
          AND {active_col} = 1
    """)
    print(
        f"[Lentra] reset Status for source_name={lentra_source_name!r}, "
        f"batch_start_date={batch_start_date!r}"
    )

elif batch_start_date == "1":
    # Batch start: flip to In Progress, reset Day_Execution_Count to 0, and stamp
    # sink_batch_started_date ONCE. Generate the UTC timestamp here in Python and
    # pass it as a literal so every matching row gets the exact same value (this is
    # the only place this column is written per run). TODO: move to a batch-init notebook.
    batch_start_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    spark.sql(f"""
        UPDATE migration_x_catalog.pfl_x_schema.rdbms_ingestion_config
        SET Status = 'In Progress', Day_Execution_Count = 0,
            sink_batch_started_date = TIMESTAMP '{batch_start_date}'
        WHERE Source_Name = 'PG_TEST_RDS'
    """)
    print("batch_start_date",batch_start_date, type(batch_start_date))

# dbutils.notebook.run(
#     "./start_batch",  # TODO: point to the actual batch-init notebook
#     600,
#     {
#         "source_id": str(source_system_id),
#         "batch_start_date": batch_start_date,
#     },
# )

# COMMAND ----------

source_sys, tasks = config_mgr.get_active_tasks(
    config_master_id=config_master_id,
    source_system_id=source_system_id,
    source_name=source_name,
    pipeline_name=pipeline_name,
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

# Serialize source system and task configs. is_lentra tells main.py which
# dataclass to reconstruct tasks as (LentraIngestionTaskConfig vs
# IngestionTaskConfig) — without it main.py has no way to know from the
# published JSON alone.
payload = {
    "source_sys": source_sys.to_dict(),
    "tasks": [task.to_dict() for task in tasks],
    "batch_start_date" : batch_start_date,
    "is_lentra": is_lentra,
}
payload_str = json.dumps(payload)

print(f"Publishing {len(tasks)} tasks metadata to taskValues...")

try:
    dbutils.jobs.taskValues.set(key="active_tasks_metadata", value=payload_str)
    print("Successfully set active_tasks_metadata.")
except Exception as e:
    print(f"[INFO] taskValues not available (standalone mode): {e}")

dbutils.notebook.exit(
    f"Success: Fetched {len(tasks)} active tasks for pipeline '{pipeline_name}'."
)
