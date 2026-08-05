# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion — Single Object Entry Point
# MAGIC
# MAGIC Accepts `ingestion_object_id` as a widget / job parameter.
# MAGIC
# MAGIC **How to use:**
# MAGIC - Run interactively: set widgets at the top and click *Run All*
# MAGIC - Run via Databricks Job: set parameters in the job definition
# MAGIC - Called by `list_active_sources.py` for-each task in fan-out Jobs pipeline
# MAGIC
# MAGIC **No longer required as widgets (now from config table):**
# MAGIC - `delta_layer`   → read from `ingestion_config.delta_layer`
# MAGIC - `pipeline_name` → auto-detected from Databricks Job context

# COMMAND ----------

# MAGIC %pip install paramiko --quiet

# COMMAND ----------

import sys
sys.path.append("../src")

from ingestion.utils.orchestrator import IngestionOrchestrator
from ingestion.utils.config_manager import SOURCE_SYSTEM_TABLE, INGESTION_CONFIG_TABLE, AUDIT_TABLE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("ingestion_object_id",    "",                     "Ingestion Object ID (int, required)")
dbutils.widgets.text("landing_volume_path",    "",                     "Landing Volume Path (blank = skip landing write)")
dbutils.widgets.text("environment",            "dev",                  "Environment: dev | uat | prod")
dbutils.widgets.text("trigger_type",           "MANUAL",               "Trigger Type: MANUAL | SCHEDULED | EVENT")
dbutils.widgets.text("source_system_table",    SOURCE_SYSTEM_TABLE,    "Source System Table (override)")
dbutils.widgets.text("ingestion_config_table", INGESTION_CONFIG_TABLE, "Ingestion Config Table (override)")
dbutils.widgets.text("audit_table",            AUDIT_TABLE,            "Audit Table (override)")

# COMMAND ----------

ingestion_object_id    = dbutils.widgets.get("ingestion_object_id")
landing_volume_path    = dbutils.widgets.get("landing_volume_path")    or None
environment            = dbutils.widgets.get("environment")            or "dev"
trigger_type           = dbutils.widgets.get("trigger_type")           or "MANUAL"
source_system_table    = dbutils.widgets.get("source_system_table")    or SOURCE_SYSTEM_TABLE
ingestion_config_table = dbutils.widgets.get("ingestion_config_table") or INGESTION_CONFIG_TABLE
audit_table            = dbutils.widgets.get("audit_table")            or AUDIT_TABLE

assert ingestion_object_id, "ingestion_object_id widget must be set"
try:
    ingestion_object_id = int(ingestion_object_id)
except ValueError:
    raise ValueError(f"ingestion_object_id must be an integer, got: '{ingestion_object_id}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Auto-detect pipeline name from Databricks Job context

# COMMAND ----------

try:
    pipeline_name = (
        dbutils.notebook.entry_point
        .getDbutils().notebook().getContext()
        .jobName().get()
    )
except Exception:
    pipeline_name = "manual_run"

print(f"pipeline_name detected: '{pipeline_name}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate config tables are accessible

# COMMAND ----------

for tbl in [source_system_table, ingestion_config_table, audit_table]:
    try:
        spark.table(tbl).limit(1).collect()
        print(f"  ✅ {tbl}")
    except Exception as e:
        raise RuntimeError(f"Cannot access config table '{tbl}': {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Ingestion

# COMMAND ----------

job_run_id = None
try:
    job_run_id = str(
        dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        .currentRunId().get()
    )
except Exception:
    pass

orchestrator = IngestionOrchestrator(
    spark,
    dbutils,
    source_system_table    = source_system_table,
    ingestion_config_table = ingestion_config_table,
    audit_table            = audit_table,
    pipeline_name          = pipeline_name,
    environment            = environment,
)

result = orchestrator.run(
    ingestion_object_id = ingestion_object_id,
    landing_volume_path = landing_volume_path,
    job_run_id          = job_run_id,
    trigger_type        = trigger_type,
)
print(result)

# COMMAND ----------

if result["status"] == "FAILED":
    raise Exception(
        f"ingestion_object_id={ingestion_object_id} FAILED: {result.get('error')}"
    )

dbutils.notebook.exit(str(result))
