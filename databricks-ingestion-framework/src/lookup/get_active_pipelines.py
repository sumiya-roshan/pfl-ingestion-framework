# Databricks notebook source
# MAGIC %md
# MAGIC # Get Active Pipelines
# MAGIC
# MAGIC Runs as **Task 1** of every source's Main Pipeline (e.g., `finnone_main`, `cca_main`).
# MAGIC
# MAGIC ### Responsibilities
# MAGIC 1. Resolves `batch_start_date`:
# MAGIC    - If `"1"` - first/independent load of the day (scheduled trigger). Generates IST now().
# MAGIC    - If a real timestamp - subsequent load triggered by `multi_refresh_orchestrator`. Uses it as-is.
# MAGIC 2. Resets tables to 'In Progress' and stamps `batch_start_date` onto the rows (First load only).
# MAGIC 3. Queries the child config table to discover which `pipeline_name` values are active.
# MAGIC 4. Publishes one boolean `taskValues` flag per active pipeline name to route child jobs.

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

IST = timezone(timedelta(hours=5, minutes=30))
MAVIS_CONFIG_MASTER_ID = 999

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
is_mavis = (config_master_id == MAVIS_CONFIG_MASTER_ID)

print(f"config_master_id  : {config_master_id}")
print(f"source_system_id  : {source_system_id}")
print(f"source_name       : {source_name}")
print(f"batch_start_date  : {batch_start_date_raw}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Resolve source_name, generate timestamp, and reset batch

# COMMAND ----------

config_mgr = ConfigManager(
    spark,
    source_system_table=SOURCE_SYSTEM_TABLE,
    config_master_table=CONFIG_MASTER_TABLE,
)

if source_system_id:
    source_sys           = config_mgr.get_source_system(source_system_id)
    resolved_source_name = source_sys.source_name
elif source_name:
    resolved_source_name = source_name
else:
    dbutils.notebook.exit("Error: could not resolve source_name.")

print(f"Resolved source_name : {resolved_source_name}")

# Resolve child table
if is_mavis:
    child_table_fqn = config_mgr._child_table_fqn(config_master_id)
else:
    child_table_fqn = resolve_child_table_fqn(spark, CONFIG_MASTER_TABLE, config_master_id)

child_df      = spark.table(child_table_fqn)
child_columns = child_df.columns

is_lentra    = False if is_mavis else config_mgr.is_lentra_shaped(child_columns)
src_col      = config_mgr.resolve_col(child_columns, "source_name",   "Source_Name")
active_col   = config_mgr.resolve_col(child_columns, "is_active",     "Is_Active")
pipeline_col = config_mgr.resolve_col(child_columns, "pipeline_name", "Pipeline_Name")

is_first_load = batch_start_date_raw.strip() == "1"

if is_first_load:
    # ── Scenario 1: First Load ──
    batch_start_date = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S.%f")
    print(f"[Scenario 1 - First Load] Generated batch_start_date (IST) = {batch_start_date}")
    
    # Batch Reset Logic
    if is_mavis:
        spark.sql(f"""
            UPDATE {child_table_fqn}
            SET status = 'Not_started', Day_Execution_Count = 0,
                sink_batch_started_date = TIMESTAMP '{batch_start_date}'
            WHERE source_name = '{resolved_source_name}' AND is_active = 1
              AND (to_date(sink_batch_started_date) != DATE '{batch_start_date[:10]}'
                   OR sink_batch_started_date IS NULL)
        """)
        print(f"Reset Mavis {child_table_fqn} for {resolved_source_name}")
        
    elif is_lentra:
        status_col = config_mgr.resolve_col(child_columns, "status", "Status")
        report_col = config_mgr.resolve_col(child_columns, "Report_Name")
        spark.sql(f"""
            UPDATE {child_table_fqn}
            SET {status_col} = 'Not-Started'
            WHERE {report_col} like 'lentra%hdr'
              AND {src_col} = '{resolved_source_name}'
              AND {active_col} = 1
        """)
        print(f"Reset Lentra {child_table_fqn} for {resolved_source_name}")
        
    elif pipeline_col:
        status_col = config_mgr.resolve_col(child_columns, "status", "Status")
        day_col    = config_mgr.resolve_col(child_columns, "day_execution_count", "Day_Execution_Count")
        date_col   = config_mgr.resolve_col(child_columns, "sink_batch_started_date")
        
        if status_col and date_col:
            spark.sql(f"""
                UPDATE {child_table_fqn}
                SET {status_col} = 'In Progress', 
                    {day_col} = 0,
                    {date_col} = TIMESTAMP '{batch_start_date}'
                WHERE {src_col} = '{resolved_source_name}'
                  AND {active_col} = 1
            """)
            print(f"Reset RDBMS {child_table_fqn} for {resolved_source_name} to 'In Progress'")

else:
    # ── Scenario 2: Orchestrator Triggered ──
    batch_start_date = batch_start_date_raw.strip()
    print(f"[Scenario 2 - Orchestrator Triggered] Using batch_start_date = {batch_start_date}")
    
    # Mavis still requires status reset on subsequent loads
    if is_mavis:
        spark.sql(f"""
            UPDATE {child_table_fqn}
            SET status = 'Not_started'
            WHERE source_name = '{resolved_source_name}' AND is_active = 1
              AND date_format(sink_batch_started_date, 'yyyy-MM-dd HH:mm:ss')
                  = date_format(TIMESTAMP '{batch_start_date}', 'yyyy-MM-dd HH:mm:ss')
        """)
        print(f"Reset Mavis {child_table_fqn} for {resolved_source_name} (Subsequent Load)")

# CCA/PENANT Initialization
if resolved_source_name in ["CCA", "PENANT"]:
    print(f"Running initialize_execution_flag_&_status for {resolved_source_name}...")
    dbutils.notebook.run(
        "/Workspace/Deployed-Assets/.bundle/pfl-dbx-datalake/dev/files/PFL/Admin/Config/Initialize_Config_Status/initialize_execution_flag_&_status",  
        1800,
        {
            "source_system": resolved_source_name,
            "batch_start_date": batch_start_date,
            "trigger_time": batch_start_date,
        },
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Discover active pipeline_names from the child config table

# COMMAND ----------

if not pipeline_col:
    # Source type has no pipeline_name column (e.g., Lentra, Mavis).
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
    # First load: ALL distinct pipeline_names with at least one active row
    rows = child_df.filter(base_filter).select(pipeline_col).distinct().collect()
    active_pipeline_names = {r[pipeline_col] for r in rows if r[pipeline_col]}
else:
    # Subsequent load: only pipelines whose tables were stamped with this batch_start_date
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
        print(f"[WARNING] {child_table_fqn} has no sink_batch_started_date column.")
        rows = child_df.filter(base_filter).select(pipeline_col).distinct().collect()
        active_pipeline_names = {r[pipeline_col] for r in rows if r[pipeline_col]}

print(f"Active pipeline names : {active_pipeline_names}")

if not active_pipeline_names:
    print("[INFO] No active pipelines found for this source/batch window. Exiting.")
    try:
        dbutils.jobs.taskValues.set(key="batch_start_date", value=batch_start_date)
    except Exception as exc:
        pass
    dbutils.notebook.exit(
        f"No active pipelines for source='{resolved_source_name}', batch_start_date='{batch_start_date}'."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Publish taskValues flags for downstream If/Else tasks

# COMMAND ----------

for pipeline in active_pipeline_names:
    flag_key = f"run_{pipeline.replace('-', '_').replace(' ', '_')}"
    try:
        dbutils.jobs.taskValues.set(key=flag_key, value="true")
    except Exception as exc:
        pass
    print(f"  taskValues['{flag_key}'] = 'true'")

# Always publish the resolved batch_start_date
try:
    dbutils.jobs.taskValues.set(key="batch_start_date", value=batch_start_date)
except Exception as exc:
    pass
print(f"  taskValues['batch_start_date'] = '{batch_start_date}'")

# COMMAND ----------

dbutils.notebook.exit(
    f"Success: {len(active_pipeline_names)} pipeline(s) active "
    f"for source='{resolved_source_name}', batch_start_date='{batch_start_date}'. "
    f"Pipelines: {sorted(active_pipeline_names)}"
)
