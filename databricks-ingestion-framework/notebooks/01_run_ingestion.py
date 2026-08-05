# Databricks notebook source
# MAGIC %md
# MAGIC # Run Ingestion — Single Object
# MAGIC
# MAGIC Entry-point notebook. Accepts `ingestion_object_id` (the PK of a row in
# MAGIC `ingestion_config`) as a widget / job parameter.
# MAGIC
# MAGIC The same notebook is reused across every source via Databricks Jobs
# MAGIC (one task per ingestion_object_id, or a for-each task — see jobs/).

# COMMAND ----------

# MAGIC %pip install paramiko --quiet
dbutils.library.restartPython()

# COMMAND ----------

import sys
sys.path.append("../src")   # adjust if deployed via Databricks Asset Bundles / Repos

from ingestion.orchestrator import IngestionOrchestrator
from ingestion.config_manager import SOURCE_SYSTEM_TABLE, INGESTION_CONFIG_TABLE, AUDIT_TABLE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("ingestion_object_id",    "",                     "Ingestion Object ID (int)")
dbutils.widgets.text("pipeline_name",          "ingestion_framework",  "Pipeline Name")
dbutils.widgets.text("delta_layer",            "BRONZE",               "Delta Layer")
dbutils.widgets.text("trigger_type",           "MANUAL",               "Trigger Type")
dbutils.widgets.text("source_system_table",    SOURCE_SYSTEM_TABLE,    "Source System Table (override)")
dbutils.widgets.text("ingestion_config_table", INGESTION_CONFIG_TABLE, "Ingestion Config Table (override)")
dbutils.widgets.text("audit_table",            AUDIT_TABLE,            "Audit Table (override)")
dbutils.widgets.text("config_file_path",       "",                     "Config JSON Path (dev/test only)")

# COMMAND ----------

ingestion_object_id    = dbutils.widgets.get("ingestion_object_id")
pipeline_name          = dbutils.widgets.get("pipeline_name")    or "ingestion_framework"
delta_layer            = dbutils.widgets.get("delta_layer")      or "BRONZE"
trigger_type           = dbutils.widgets.get("trigger_type")     or "MANUAL"
source_system_table    = dbutils.widgets.get("source_system_table")    or SOURCE_SYSTEM_TABLE
ingestion_config_table = dbutils.widgets.get("ingestion_config_table") or INGESTION_CONFIG_TABLE
audit_table            = dbutils.widgets.get("audit_table")            or AUDIT_TABLE
config_file_path       = dbutils.widgets.get("config_file_path")       or None

assert ingestion_object_id, "ingestion_object_id widget must be set"

try:
    ingestion_object_id = int(ingestion_object_id)
except ValueError:
    raise ValueError(f"ingestion_object_id must be an integer, got: '{ingestion_object_id}'")

# Capture Databricks job run ID for audit traceability
job_run_id = None
try:
    job_run_id = str(
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().currentRunId().get()
    )
except Exception:
    pass   # not running inside a Databricks Job — that's fine

# COMMAND ----------

orchestrator = IngestionOrchestrator(
    spark,
    dbutils,
    source_system_table    = source_system_table,
    ingestion_config_table = ingestion_config_table,
    audit_table            = audit_table,
    json_file_path         = config_file_path,
    pipeline_name          = pipeline_name,
    delta_layer            = delta_layer,
)

result = orchestrator.run(
    ingestion_object_id = ingestion_object_id,
    job_run_id          = job_run_id,
    trigger_type        = trigger_type,
)
print(result)

# COMMAND ----------

dbutils.notebook.exit(str(result))
