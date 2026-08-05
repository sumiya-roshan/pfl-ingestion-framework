# Databricks notebook source
# MAGIC %md
# MAGIC # Run Ingestion - Source Type
# MAGIC
# MAGIC Executes all ingestion objects configured for the given source type.
# MAGIC
# MAGIC Example:
# MAGIC - POSTGRES
# MAGIC - MYSQL
# MAGIC - ORACLE
# MAGIC - MONGODB
# MAGIC - SFTP

# COMMAND ----------

# MAGIC %pip install paramiko --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import sys

sys.path.append("../src")

from ingestion.orchestrator import IngestionOrchestrator
from ingestion.config_manager import (
    SOURCE_SYSTEM_TABLE,
    INGESTION_CONFIG_TABLE,
    AUDIT_TABLE,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("source_type", "POSTGRES", "Source Type")


# COMMAND ----------

source_type = dbutils.widgets.get("source_type").strip().upper()

pipeline_name = "ingestion_framework"
delta_layer =  "BRONZE"
source_system_table = SOURCE_SYSTEM_TABLE
ingestion_config_table = INGESTION_CONFIG_TABLE
audit_table =  AUDIT_TABLE

# COMMAND ----------
job_run_id = None

try:
    job_run_id = str(
        dbutils.notebook.entry_point
        .getDbutils()
        .notebook()
        .getContext()
        .currentRunId()
        .get()
    )
except Exception:
    pass

# COMMAND ----------

orchestrator = IngestionOrchestrator(
    spark=spark,
    dbutils=dbutils,
    source_system_table=source_system_table,
    ingestion_config_table=ingestion_config_table,
    audit_table=audit_table,
    pipeline_name=pipeline_name,
    delta_layer=delta_layer,
)

# COMMAND ----------

results = orchestrator.run_by_source_type(
    source_type=source_type,
    job_run_id=job_run_id,
)

print("=" * 80)
print(f"Source Type : {source_type}")
print("=" * 80)

success = 0
failed = 0

for result in results:

    print(result)

    if result["status"] == "SUCCESS":
        success += 1
    else:
        failed += 1

print("=" * 80)
print(f"Total Objects : {len(results)}")
print(f"Success       : {success}")
print(f"Failed        : {failed}")
print("=" * 80)

if failed > 0:
    raise Exception(f"{failed} ingestion object(s) failed.")

dbutils.notebook.exit(str(results))