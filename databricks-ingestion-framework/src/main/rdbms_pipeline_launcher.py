# Databricks notebook source
# MAGIC %md
# MAGIC # RDBMS Pipeline Launcher (Parent Wrapper)
# MAGIC
# MAGIC Entry point for **RDBMS-based** ingestion pipelines.
# MAGIC
# MAGIC Queries the RDBMS child config table for distinct active `Batch_ID` values and
# MAGIC triggers **one concurrent Databricks child job run per `Batch_ID`** via the
# MAGIC Databricks Jobs REST API (fire-and-forget). The child job (e.g. the standard
# MAGIC RDBMS ingestion job backed by `main.py`) is responsible for processing all
# MAGIC tables that belong to that `Batch_ID`.
# MAGIC
# MAGIC **Does NOT** wait for the child runs to complete — use Databricks job
# MAGIC dependencies or a separate monitoring step if you need that.

# COMMAND ----------

dbutils.widgets.text("config_master_id",  "",                    "Config Master ID")
dbutils.widgets.text("source_system_id",  "",                    "Source System ID")
dbutils.widgets.text("pipeline_name",     "",                    "Pipeline Name (e.g. CCA)")
dbutils.widgets.text("batch_start_date",  "1",                   "Batch Start Date / Trigger Time")
dbutils.widgets.text("target_catalog",    "migration_x_catalog", "Target Catalog (where config tables live)")
dbutils.widgets.text("child_job_name",    "",                    "Name of the RDBMS child ingestion job to trigger")
dbutils.widgets.text("secret_scope",      "kv-pfl-scope",        "Secret scope for Databricks PAT token")
dbutils.widgets.text("secret_key_pat",    "databricks-pat-token", "Secret key for Databricks PAT token")

# COMMAND ----------

config_master_id_raw = dbutils.widgets.get("config_master_id")
source_system_id_raw = dbutils.widgets.get("source_system_id")
pipeline_name        = dbutils.widgets.get("pipeline_name")
batch_start_date     = dbutils.widgets.get("batch_start_date") or "1"
target_catalog       = dbutils.widgets.get("target_catalog") or "hive_metastore"
child_job_name       = dbutils.widgets.get("child_job_name")
secret_scope         = dbutils.widgets.get("secret_scope")
secret_key_pat       = dbutils.widgets.get("secret_key_pat")

if not config_master_id_raw or not source_system_id_raw or not pipeline_name or not child_job_name:
    dbutils.notebook.exit(
        "Error: config_master_id, source_system_id, pipeline_name, "
        "and child_job_name are all required."
    )

if not secret_scope:
    dbutils.notebook.exit("Error: secret_scope is required (needed for REST API PAT token).")

config_master_id = int(config_master_id_raw)

# COMMAND ----------

import sys
sys.path.append("..")

from ingestion.utils.config_manager import CONFIG_MASTER_TABLE
from multi_refresh.job_trigger import JobTrigger

# COMMAND ----------

# 1. Resolve the RDBMS child config table FQN from config_master
master_rows = (
    spark.table(CONFIG_MASTER_TABLE)
    .filter(f"config_id = {config_master_id}")
    .collect()
)

if not master_rows:
    raise ValueError(
        f"No entry in {CONFIG_MASTER_TABLE} for config_id={config_master_id}"
    )

m_row = master_rows[0].asDict()
child_table_fqn = (
    f"{m_row.get('config_catalog_name')}."
    f"{m_row.get('config_schema_name')}."
    f"{m_row.get('config_table_name')}"
)
print(f"[RDBMS Launcher] Resolved child config table: {child_table_fqn}")

# COMMAND ----------

# 2. Query distinct active Batch_IDs for this RDBMS pipeline
query = (
    f"SELECT DISTINCT Batch_ID "
    f"FROM {child_table_fqn} "
    f"WHERE Pipeline_Name = '{pipeline_name}' AND Is_Active = 1"
)

if batch_start_date and str(batch_start_date).strip() != "1":
    clean_date = str(batch_start_date).replace("T", " ").split(".")[0]
    query += (
        f" AND date_format("
        f"from_utc_timestamp(sink_batch_started_date, 'UTC'), "
        f"'yyyy-MM-dd HH:mm:ss') = '{clean_date}'"
    )

print(f"[RDBMS Launcher] Querying eligible Batch_IDs:\n{query}")
batches_df = spark.sql(query)
active_batches = [
    row["Batch_ID"] for row in batches_df.collect() if row["Batch_ID"] is not None
]

if not active_batches:
    print(
        f"[RDBMS Launcher] No active Batch_IDs found for pipeline "
        f"'{pipeline_name}' at '{batch_start_date}'. Exiting gracefully."
    )
    dbutils.notebook.exit("SUCCESS: No active RDBMS batches found.")

print(f"[RDBMS Launcher] Found {len(active_batches)} Batch_ID(s): {active_batches}")

# COMMAND ----------

# 3. Trigger one concurrent child RDBMS job run per Batch_ID
workspace_url = (
    dbutils.notebook.entry_point
    .getDbutils()
    .notebook()
    .getContext()
    .apiUrl()
    .get()
)
pat_token = dbutils.secrets.get(scope=secret_scope, key=secret_key_pat)
job_trigger = JobTrigger(workspace_url=workspace_url, token=pat_token)

for batch_id in active_batches:
    print(f"[RDBMS Launcher] Triggering child job '{child_job_name}' for Batch_ID={batch_id}")
    run_id = job_trigger.run_now_by_name(
        job_name=child_job_name,
        notebook_params={
            "config_master_id": str(config_master_id),
            "source_system_id": str(source_system_id_raw),
            "pipeline_name":    pipeline_name,
            "batch_start_date": batch_start_date,
            "batch_id":         str(batch_id),
            "target_catalog":   target_catalog,
        },
    )
    print(f"[RDBMS Launcher] run_id={run_id} triggered for Batch_ID={batch_id}")

print(
    f"[RDBMS Launcher] Done. {len(active_batches)} RDBMS child job run(s) triggered concurrently."
)
dbutils.notebook.exit(f"SUCCESS: {len(active_batches)} RDBMS batch run(s) triggered.")
