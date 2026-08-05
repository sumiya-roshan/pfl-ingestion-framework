# Databricks notebook source
# MAGIC %md
# MAGIC # List Active Ingestion Objects
# MAGIC
# MAGIC First task in the Jobs for-each pipeline (see `jobs/ingestion_job.json`).
# MAGIC Queries `ingestion_config` for all enabled ingestion_object_ids and
# MAGIC publishes them as a task value so the for-each task can fan out one
# MAGIC notebook run per object.

# COMMAND ----------

import sys, json
sys.path.append("../src")

from ingestion.config_manager import (
    ConfigManager,
    SOURCE_SYSTEM_TABLE,
    INGESTION_CONFIG_TABLE,
)

# COMMAND ----------

dbutils.widgets.text("source_type_filter",     "",                     "Filter by source_type (blank = all)")
dbutils.widgets.text("source_system_table",    SOURCE_SYSTEM_TABLE,    "Source System Table (override)")
dbutils.widgets.text("ingestion_config_table", INGESTION_CONFIG_TABLE, "Ingestion Config Table (override)")

source_type_filter     = dbutils.widgets.get("source_type_filter")     or None
source_system_table    = dbutils.widgets.get("source_system_table")    or SOURCE_SYSTEM_TABLE
ingestion_config_table = dbutils.widgets.get("ingestion_config_table") or INGESTION_CONFIG_TABLE

# COMMAND ----------

config_mgr = ConfigManager(
    spark,
    source_system_table    = source_system_table,
    ingestion_config_table = ingestion_config_table,
)

object_ids = config_mgr.get_active_ingestion_objects(source_type=source_type_filter)
print(f"Active ingestion objects ({len(object_ids)}): {object_ids}")

# COMMAND ----------

# Publish as a task value — consumed by the for-each task in ingestion_job.json
# as: {{tasks.list_active_objects.values.ingestion_object_ids}}
dbutils.jobs.taskValues.set(key="ingestion_object_ids", value=json.dumps(object_ids))
