# Databricks notebook source
# MAGIC %md
# MAGIC # List Active Ingestion Objects
# MAGIC
# MAGIC First task in the Databricks Jobs for-each pipeline.
# MAGIC
# MAGIC **What it does:**
# MAGIC 1. Auto-detects the current Databricks Job name → used as `pipeline_name` filter
# MAGIC 2. Queries `ingestion_config` for matching rows using optional source filters
# MAGIC 3. Publishes the list as a task value → consumed by the for-each task
# MAGIC
# MAGIC **Output task value:**
# MAGIC   `{{tasks.list_active_objects.values.ingestion_object_ids}}`  → JSON array of ints

# COMMAND ----------

import sys, json
sys.path.append("../src")

from ingestion.utils.config_manager import (
    ConfigManager,
    SOURCE_SYSTEM_TABLE,
    INGESTION_CONFIG_TABLE,
)

# COMMAND ----------

dbutils.widgets.text("source_type_filter",     "",                     "Filter by source_type (blank = all)")
dbutils.widgets.text("source_system_id",       "",                     "Filter by source_system_id (blank = all)")
dbutils.widgets.text("source_name",            "",                     "Filter by source_name (blank = all)")
dbutils.widgets.text("source_system_table",    SOURCE_SYSTEM_TABLE,    "Source System Table (override)")
dbutils.widgets.text("ingestion_config_table", INGESTION_CONFIG_TABLE, "Ingestion Config Table (override)")

# COMMAND ----------

source_type_filter     = dbutils.widgets.get("source_type_filter")     or None
source_system_id_raw   = dbutils.widgets.get("source_system_id")       or None
source_name            = dbutils.widgets.get("source_name")            or None
source_system_table    = dbutils.widgets.get("source_system_table")    or SOURCE_SYSTEM_TABLE
ingestion_config_table = dbutils.widgets.get("ingestion_config_table") or INGESTION_CONFIG_TABLE

source_system_id = int(source_system_id_raw) if source_system_id_raw else None

# COMMAND ----------

# Auto-detect pipeline name from Databricks Job context
try:
    pipeline_name = (
        dbutils.notebook.entry_point
        .getDbutils().notebook().getContext()
        .jobName().get()
    )
    print(f"Pipeline name from job context: '{pipeline_name}'")
except Exception:
    pipeline_name = None
    print("Running interactively — no pipeline_name filter applied")

# COMMAND ----------

config_mgr = ConfigManager(
    spark,
    source_system_table    = source_system_table,
    ingestion_config_table = ingestion_config_table,
)

object_ids = config_mgr.get_active_ingestion_objects(
    source_type      = source_type_filter,
    source_system_id = source_system_id,
    source_name      = source_name,
    pipeline_name    = pipeline_name,
)
print(f"Active ingestion objects ({len(object_ids)}): {object_ids}")

# COMMAND ----------

# Publish as a task value consumed by the for-each task
dbutils.jobs.taskValues.set(key="ingestion_object_ids", value=json.dumps(object_ids))
