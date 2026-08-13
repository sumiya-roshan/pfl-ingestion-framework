# Databricks notebook source
# COMMAND ----------



# COMMAND ----------

# MAGIC %pip install paramiko --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import sys
sys.path.append("..")  
from datetime import datetime, timezone 
from ingestion.utils.config_manager import (ConfigManager,SOURCE_SYSTEM_TABLE,CONFIG_MASTER_TABLE,AUDIT_TABLE,)
from ingestion.utils.orchestrator import IngestionOrchestrator
from ingestion.utils.config_manager import IngestionTaskConfig

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

config_master_id_raw   = int(dbutils.widgets.get("config_master_id"))
source_system_id_raw   = int(dbutils.widgets.get("source_system_id"))
target_catalog         = dbutils.widgets.get("target_catalog")         or "migration_x_catalog"
pipeline_name          = dbutils.widgets.get("pipeline_name")          or None
landing_volume_path    = dbutils.widgets.get("landing_volume_path")    or None
environment            = dbutils.widgets.get("environment")            or "dev"
config_master_id = int(config_master_id_raw)
source_system_id = int(source_system_id_raw)
job_run_id = dbutils.widgets.get("run_id")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Databricks Job Context
# MAGIC
# MAGIC Job/run information comes from the Databricks runtime.
# MAGIC Nothing is hardcoded.

# COMMAND ----------

def get_databricks_job_context():

    context = (
        dbutils.notebook.entry_point
        .getDbutils()
        .notebook()
        .getContext()
    )

    def get_context_value(method_name):

        try:
            return getattr(context, method_name)().get()
        except Exception:
            return None

    return {
        "job_id": get_context_value("jobId"),
        "job_name": get_context_value("jobName"),
        "notebook_name": get_context_value("notebookPath"),
        "databricks_url": get_context_value("apiUrl"),
        "trigger_type": get_context_value("triggerType"),
        "trigger_id": get_context_value("triggerId"),
        "trigger_name": get_context_value("triggerName"),
    }


job_context = get_databricks_job_context()
job_context["job_run_id"] = job_run_id
# Job Run ID is the execution identifier.
if not job_run_id:
    raise RuntimeError(
        "Unable to determine Databricks Job Run ID."
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Discover ingestion objects for this source

# COMMAND ----------

config_mgr = ConfigManager(
    spark,
    source_system_table = SOURCE_SYSTEM_TABLE,
    config_master_table = CONFIG_MASTER_TABLE,
    target_catalog      = target_catalog,
)

# Fetch the source system config AND all active tasks from the correct child table
source_sys, tasks = config_mgr.get_active_tasks(config_master_id,source_system_id)

if not tasks:
    dbutils.notebook.exit("No active ingestion tasks found for the given source filters.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute Ingestion

# COMMAND ----------


orchestrator = IngestionOrchestrator(
    spark,
    dbutils,
    audit_table   = AUDIT_TABLE,
    pipeline_name = pipeline_name,
    environment   = environment,
    job_context=job_context,

)

results = []

for task in tasks:

    print(
        f"\nStarting task: "
        f"{task.source_object_name} "
        f"(Config ID: {task.config_id})"
    )

    task_start_time = datetime.now(timezone.utc)

    try:

        result = orchestrator.run(
            source_sys=source_sys,
            task=task,
            landing_volume_path=landing_volume_path,
            job_context=job_context,
            task_start_time=task_start_time,
        )

        results.append(result)

    except Exception as e:

        print(
            f"Task failed: "
            f"{task.source_object_name} "
            f"(Config ID: {task.config_id})"
        )

        results.append({
            "config_id": task.config_id,
            "status": "FAILED",
            "rows_read": 0,
            "rows_copied": 0,
            "rows_deleted": 0,
            "rows_affected": 0,
            "error": str(e),
            "error_code": type(e).__name__,
        })



# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Job Status
# MAGIC
# MAGIC If any ingestion task failed, fail the Databricks Job.
# MAGIC The audit records have already been written by the orchestrator.

# COMMAND ----------
failed = [r for r in results if r.get("status") == "FAILED"]

if failed:
    raise Exception(f"{len(failed)} ingestion task(s) failed.")

dbutils.notebook.exit(
    f"SUCCESS: "
    f"{len(succeeded)}/{len(results)} "
    f"objects ingested successfully. "
    f"Job Run ID: {job_run_id}"
)
