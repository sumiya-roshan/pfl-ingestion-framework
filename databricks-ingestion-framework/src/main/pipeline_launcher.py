# Databricks notebook source
# MAGIC %md
# MAGIC # Master Pipeline Launcher (Parent Wrapper)
# MAGIC 
# MAGIC This notebook acts as the single entry point for triggering a pipeline. 
# MAGIC It queries the configuration table for eligible atch_ids and triggers
# MAGIC concurrent runs of the child ingestion job (one per batch).

# COMMAND ----------

dbutils.widgets.text("config_master_id",    "",               "Config Master ID")
dbutils.widgets.text("source_system_id",    "",               "Source System ID")
dbutils.widgets.text("pipeline_name",       "",               "Pipeline Name (e.g. CCA)")
dbutils.widgets.text("batch_start_date",    "1",              "Batch Start Date / Trigger Time")
dbutils.widgets.text("target_catalog",      "hive_metastore", "Target Catalog (where config tables live)")
dbutils.widgets.text("child_job_name",      "",               "Name of the child job to trigger (e.g. CCA_Ingestion_Job)")

# COMMAND ----------

config_master_id_raw = dbutils.widgets.get("config_master_id")
source_system_id_raw = dbutils.widgets.get("source_system_id")
pipeline_name        = dbutils.widgets.get("pipeline_name")
batch_start_date     = dbutils.widgets.get("batch_start_date") or "1"
target_catalog       = dbutils.widgets.get("target_catalog") or "hive_metastore"
child_job_name       = dbutils.widgets.get("child_job_name")

if not config_master_id_raw or not source_system_id_raw or not pipeline_name or not child_job_name:
    dbutils.notebook.exit("Error: config_master_id, source_system_id, pipeline_name, and child_job_name are required.")

config_master_id = int(config_master_id_raw)

# COMMAND ----------

import sys
sys.path.append("..")

from ingestion.utils.config_manager import CONFIG_MASTER_TABLE
from multi_refresh.job_trigger import JobTrigger

# COMMAND ----------

# 1. Resolve child table from config_master
master_rows = (
    spark.table(CONFIG_MASTER_TABLE)
    .filter(f"config_id = {config_master_id}")
    .collect()
)

if not master_rows:
    raise ValueError(f"No entry in {CONFIG_MASTER_TABLE} for config_id={config_master_id}")

m_row = master_rows[0].asDict()
catalog = m_row.get("config_catalog_name")
schema = m_row.get("config_schema_name")
table = m_row.get("config_table_name")

child_table_fqn = f"{catalog}.{schema}.{table}"

# COMMAND ----------

# 2. Query distinct active batch_ids for this pipeline
query = f"SELECT DISTINCT Batch_ID FROM {child_table_fqn} WHERE Pipeline_Name = '{pipeline_name}' AND Is_Active = 1"

if batch_start_date and str(batch_start_date).strip() != "1":
    clean_date = str(batch_start_date).replace("T", " ").split(".")[0]
    query += f" AND date_format(from_utc_timestamp(sink_batch_started_date, 'UTC'), 'yyyy-MM-dd HH:mm:ss') = '{clean_date}'"

print(f"Executing query to find eligible batches:\n{query}")
batches_df = spark.sql(query)
active_batches = [row["Batch_ID"] for row in batches_df.collect() if row["Batch_ID"] is not None]

if not active_batches:
    print(f"No active batches found for pipeline '{pipeline_name}' at '{batch_start_date}'. Exiting gracefully.")
    dbutils.notebook.exit("SUCCESS: No active batches found.")

print(f"Found {len(active_batches)} eligible batch(es): {active_batches}")

# COMMAND ----------

# 3. Trigger concurrent child jobs (one per batch_id)
job_trigger = JobTrigger(dbutils)

for batch_id in active_batches:
    print(f"Triggering child job '{child_job_name}' for Batch ID: {batch_id}")
    job_trigger.run_now_by_name(
        job_name=child_job_name,
        notebook_params={
            "config_master_id": str(config_master_id),
            "source_system_id": str(source_system_id_raw),
            "pipeline_name": pipeline_name,
            "batch_start_date": batch_start_date,
            "batch_id": str(batch_id),
            "target_catalog": target_catalog
        }
    )

print("All batch jobs have been triggered concurrently.")
dbutils.notebook.exit("SUCCESS: Batches triggered.")
