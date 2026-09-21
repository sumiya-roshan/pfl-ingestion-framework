# Databricks notebook source
# MAGIC %md
# MAGIC # Get Active Pipelines
# MAGIC
# MAGIC Runs as **Task 1** of every source's Main Pipeline (e.g., `finnone_main`, `pg_test_main`).
# MAGIC
# MAGIC ### Responsibilities
# MAGIC 1. Resolves `batch_start_date`:
# MAGIC    - If `"1"` - first/independent load of the day (scheduled trigger). Generates IST now().
# MAGIC    - If a real timestamp - subsequent load triggered by multi_refresh_orchestrator. Uses it as-is.
# MAGIC 2. Queries the child config table to discover which pipeline_name values are active
# MAGIC    for this source and this batch window.
# MAGIC 3. Publishes one boolean taskValues flag per active pipeline name so that downstream
# MAGIC    If/Else condition tasks in the Main Pipeline can route to the correct child job.
# MAGIC 4. Publishes the resolved batch_start_date so ALL child pipelines stamp their rows
# MAGIC    with the exact same timestamp (critical for multi-refresh filtering).
# MAGIC
# MAGIC ### Trigger Scenarios
# MAGIC | Trigger | batch_start_date received | What this notebook does |
# MAGIC |---|---|---|
# MAGIC | Scheduled (first load of day) | "1" | Generates IST now(), fetches ALL active pipelines |
# MAGIC | Multi-refresh orchestrator | real timestamp | Uses it as-is, fetches only pipelines scheduled for that batch |
# MAGIC | Recon orchestrator (FinnOne) | real timestamp | Same as multi-refresh path |

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
sys.path.append("..")

from datetime import datetime, timezone, timedelta

from ingestion.utils.config_manager import (
    CONFIG_MASTER_TABLE,
    SOURCE_SYSTEM_TABLE,
    ConfigManager,
    resolve_child_table_fqn,
)

# IST timezone - matches get_tasks.py trigger_time generation so the timestamp
# published here and the one get_tasks.py would generate standalone are always
# in the same timezone.
IST = timezone(timedelta(hours=5, minutes=30))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("config_master_id", "", "Config Master ID")
dbutils.widgets.text("source_system_id", "", "Source System ID")
dbutils.widgets.text("source_name",      "", "Source Name (alternative to source_system_id)")
dbutils.widgets.text("batch_start_date", "1", "Batch Start Date (1 = generate IST now)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolve inputs

# COMMAND ----------

config_master_id_raw = dbutils.widgets.get("config_master_id") or None
source_system_id_raw = dbutils.widgets.get("source_system_id") or None
source_name          = dbutils.widgets.get("source_name")       or None
batch_start_date_raw = dbutils.widgets.get("batch_start_date") or "1"

if not config_master_id_raw:
    dbutils.notebook.exit("Error: config_master_id is required.")
if not source_system_id_raw and not source_name:
    dbutils.notebook.exit("Error: either source_system_id or source_name is required.")

config_master_id = int(config_master_id_raw)
source_system_id = int(source_system_id_raw) if source_system_id_raw else None

print(f"config_master_id  : {config_master_id}")
print(f"source_system_id  : {source_system_id}")
print(f"source_name       : {source_name}")
print(f"batch_start_date  : {batch_start_date_raw}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Resolve source_name and batch_start_date

# COMMAND ----------

config_mgr = ConfigManager(
    spark,
    source_system_table=SOURCE_SYSTEM_TABLE,
    config_master_table=CONFIG_MASTER_TABLE,
)

# Resolve source_name from source_system_id or use directly
if source_system_id:
    source_sys           = config_mgr.get_source_system(source_system_id)
    resolved_source_name = source_sys.source_name
elif source_name:
    resolved_source_name = source_name
else:
    dbutils.notebook.exit("Error: could not resolve source_name.")

is_first_load = batch_start_date_raw.strip() == "1"

if is_first_load:
    # Scenario 1: First / independent load of the day (scheduled trigger).
    # Generate IST timestamp once here - matches the timezone get_tasks.py uses
    # when it generates trigger_time standalone (datetime.now(ist)).
    # Every child pipeline receives this same value so all config rows get
    # stamped with the SAME timestamp - critical for multi-refresh filtering.
    batch_start_date = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S.%f")
    print(f"[Scenario 1 - First Load] Generated batch_start_date (IST) = {batch_start_date}")
else:
    # Scenario 2: Triggered by multi_refresh_orchestrator or recon pipeline.
    # The orchestrator already stamped config rows with this timestamp before
    # triggering this Main Pipeline. Use it verbatim - get_tasks.py will set
    # trigger_time = batch_start_date directly (skipping its own IST generation).
    batch_start_date = batch_start_date_raw.strip()
    print(f"[Scenario 2 - Orchestrator Triggered] Using batch_start_date = {batch_start_date}")

print(f"Resolved source_name : {resolved_source_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Discover active pipeline_names from the child config table

# COMMAND ----------

child_table_fqn = resolve_child_table_fqn(spark, CONFIG_MASTER_TABLE, config_master_id)
child_df        = spark.table(child_table_fqn)
child_columns   = child_df.columns

# Case-insensitive column resolution (child tables vary in casing across sources)
src_col      = config_mgr.resolve_col(child_columns, "source_name",   "Source_Name")
active_col   = config_mgr.resolve_col(child_columns, "is_active",     "Is_Active")
pipeline_col = config_mgr.resolve_col(child_columns, "pipeline_name", "Pipeline_Name")

if not pipeline_col:
    # Source type has no pipeline_name column (e.g., Lentra, Mavis).
    # Nothing to route - exit cleanly. These sources use a different entry point.
    print(f"[INFO] {child_table_fqn} has no pipeline_name column. No pipeline flags to publish.")
    try:
        dbutils.jobs.taskValues.set(key="batch_start_date", value=batch_start_date)
    except Exception as exc:
        print(f"[INFO] taskValues not available (standalone mode): {exc}")
    dbutils.notebook.exit(
        f"No pipeline_name column in {child_table_fqn} - no routing flags published."
    )

base_filter = f"{src_col} = '{resolved_source_name}' AND {active_col} = 1"

if is_first_load:
    # First load: ALL distinct pipeline_names with at least one active row for
    # this source - no batch date filter needed.
    rows = (
        child_df
        .filter(base_filter)
        .select(pipeline_col)
        .distinct()
        .collect()
    )
    active_pipeline_names = {r[pipeline_col] for r in rows if r[pipeline_col]}

else:
    # Subsequent / orchestrator-triggered load: only the pipelines whose tables
    # were stamped with this exact batch_start_date by the orchestrator.
    date_col   = config_mgr.resolve_col(child_columns, "sink_batch_started_date")
    clean_date = batch_start_date.replace("T", " ").split(".")[0]

    if date_col:
        rows = (
            child_df
            .filter(base_filter)
            .filter(f"date_format({date_col}, 'yyyy-MM-dd HH:mm:ss') = '{clean_date}'")
            .select(pipeline_col)
            .distinct()
            .collect()
        )
        active_pipeline_names = {r[pipeline_col] for r in rows if r[pipeline_col]}
    else:
        # No sink_batch_started_date column - fall back to all active pipelines
        print(
            f"[WARNING] {child_table_fqn} has no sink_batch_started_date column. "
            "Falling back to all active pipelines."
        )
        rows = (
            child_df
            .filter(base_filter)
            .select(pipeline_col)
            .distinct()
            .collect()
        )
        active_pipeline_names = {r[pipeline_col] for r in rows if r[pipeline_col]}

print(f"Active pipeline names : {active_pipeline_names}")

if not active_pipeline_names:
    print("[INFO] No active pipelines found for this source/batch window. Exiting.")
    try:
        dbutils.jobs.taskValues.set(key="batch_start_date", value=batch_start_date)
    except Exception as exc:
        print(f"[INFO] taskValues not available (standalone mode): {exc}")
    dbutils.notebook.exit(
        f"No active pipelines for source='{resolved_source_name}', "
        f"batch_start_date='{batch_start_date}'."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Publish taskValues flags for downstream If/Else tasks

# COMMAND ----------

# One flag per active pipeline_name found in the config table.
# Downstream If/Else task condition syntax (configured in the Databricks UI):
#   {{tasks.get_active_pipelines.values.run_full_load}} == "true"
#
# Key format: "run_<pipeline_name>" - hyphens/spaces replaced with underscores
# so taskValues keys are always valid identifiers.
#
# If/Else tasks whose pipeline was NOT active simply receive no "true" flag and
# evaluate their condition as false - Databricks skips the branch automatically.

for pipeline in active_pipeline_names:
    flag_key = f"run_{pipeline.replace('-', '_').replace(' ', '_')}"
    try:
        dbutils.jobs.taskValues.set(key=flag_key, value="true")
    except Exception as exc:
        print(f"  [INFO] taskValues not available (standalone mode): {exc}")
    print(f"  taskValues['{flag_key}'] = 'true'")

# Always publish the resolved batch_start_date so every child pipeline uses the
# exact same timestamp. get_tasks.py receives this as the batch_start_date widget
# and sets: trigger_time = batch_start_date (skipping its own IST generation).
try:
    dbutils.jobs.taskValues.set(key="batch_start_date", value=batch_start_date)
except Exception as exc:
    print(f"  [INFO] taskValues not available (standalone mode): {exc}")
print(f"  taskValues['batch_start_date'] = '{batch_start_date}'")

# COMMAND ----------

dbutils.notebook.exit(
    f"Success: {len(active_pipeline_names)} pipeline(s) active "
    f"for source='{resolved_source_name}', batch_start_date='{batch_start_date}'. "
    f"Pipelines: {sorted(active_pipeline_names)}"
)
