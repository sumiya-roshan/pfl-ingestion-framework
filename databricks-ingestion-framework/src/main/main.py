# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ingestion — Source System Entry Point
# MAGIC
# MAGIC Discovers and executes all active ingestion tasks for a given source system.
# MAGIC
# MAGIC **Input:** `config_master_id` and `source_system_id`.
# MAGIC The notebook finds the correct child config table from `config_master`,
# MAGIC fetches all active tasks (`Is_Active = 1`), and runs them.
# MAGIC
# MAGIC **Fault tolerance:** a failure on one table does NOT stop the others.
# MAGIC All objects are attempted; a summary is printed at the end. The notebook
# MAGIC raises a final exception only if at least one table failed, so the
# MAGIC Databricks Job task correctly shows FAILED.

# COMMAND ----------



# COMMAND ----------

# MAGIC %pip install paramiko --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import sys
from datetime import datetime, timezone

sys.path.append("..")   

from ingestion.utils.config_manager import (
    ConfigManager,
    SOURCE_SYSTEM_TABLE,
    CONFIG_MASTER_TABLE,
    AUDIT_TABLE,
)
from ingestion.utils.orchestrator import IngestionOrchestrator
from ingestion.utils.config_manager import IngestionTaskConfig

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("config_master_id",       "",                     "Config Master ID (int, points to child table)")
dbutils.widgets.text("source_system_id",       "",                     "Source System ID (int, gets connection info)")
dbutils.widgets.text("target_catalog",         "migration_x_catalog",  "Target Catalog")
dbutils.widgets.text("pipeline_name",          "",                     "Pipeline Name (blank = auto-detect from job)")
dbutils.widgets.text("landing_volume_path",    "",                     "Landing Volume Base Path (blank = skip landing write)")
dbutils.widgets.text("environment",            "dev",                  "Environment: dev | uat | prod")

# COMMAND ----------

config_master_id_raw   = int(dbutils.widgets.get("config_master_id"))
source_system_id_raw   = int(dbutils.widgets.get("source_system_id"))

if not config_master_id_raw or not source_system_id_raw:
    dbutils.notebook.exit("Error: config_master_id and source_system_id are required.")

config_master_id = int(config_master_id_raw)
source_system_id = int(source_system_id_raw)

target_catalog         = dbutils.widgets.get("target_catalog")         or "migration_x_catalog"
pipeline_name_widget   = dbutils.widgets.get("pipeline_name")          or None
landing_volume_path    = dbutils.widgets.get("landing_volume_path")    or None
environment            = dbutils.widgets.get("environment")            or "dev"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolve pipeline name
# MAGIC Priority: **widget value** → auto-detected Databricks Job name → `'manual_run'`

# COMMAND ----------

if pipeline_name_widget:
    pipeline_name = pipeline_name_widget
    print(f"pipeline_name from widget: '{pipeline_name}'")
else:
    try:
        pipeline_name = (
            dbutils.notebook.entry_point
            .getDbutils().notebook().getContext()
            .jobName().get()
        )
        print(f"pipeline_name from job context: '{pipeline_name}'")
    except Exception:
        pipeline_name = "manual_run"
        print(f"pipeline_name fallback: '{pipeline_name}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Databricks Job Context
# MAGIC
# MAGIC Job/run information comes from the Databricks runtime.
# MAGIC Nothing is hardcoded.

# COMMAND ----------

job_run_id = dbutils.widgets.get("run_id")
print(f"Current Job Run ID: {job_run_id}")
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
        "trigger_type": get_context_value("triggerType"),
        "trigger_id": get_context_value("triggerId"),
        "trigger_name": get_context_value("triggerName"),
    }


job_context = get_databricks_job_context()
job_context["job_run_id"] = job_run_id

print("\nDatabricks Job Context")
print("=" * 60)
print(f"Job ID       : {job_context.get('job_id')}")
print(f"Job Name     : {job_context.get('job_name')}")
print(f"Trigger Type : {job_context.get('trigger_type')}")
print(f"Trigger ID   : {job_context.get('trigger_id')}")
print(f"Trigger Name : {job_context.get('trigger_name')}")
print("=" * 60)

# Pipeline name should come from Job Name.
pipeline_name = job_context.get("job_name") or pipeline_name

# Job Run ID is the execution identifier.
job_run_id = job_run_id

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
source_sys, tasks = config_mgr.get_active_tasks(
    config_master_id = config_master_id,
    source_system_id = source_system_id
)

print(f"\nSource filters applied:")
print(f"  config_master_id : {config_master_id}")
print(f"  source_system_id : {source_system_id} -> Resolved to: {source_sys.source_name}")
print(f"  target_catalog   : {target_catalog}")
print(f"\nFound {len(tasks)} ingestion task(s) to run.")

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

# def run_one(task: IngestionTaskConfig) -> dict:
#     """Run a single ingestion object through the orchestrator."""
#     return orchestrator.run(
#         source_sys          = source_sys,
#         task                = task,
#         landing_volume_path = landing_volume_path,
#         trigger_id          = trigger_id,
#         trigger_type        = trigger_type,
#     )
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
# MAGIC ## Execution Summary

# COMMAND ----------

print(f"\n{'=' * 85}")
print(
    f"{'CONF ID':>10}  "
    f"{'STATUS':<10}  "
    f"{'ROWS READ':>12}  "
    f"{'ROWS COPIED':>14}  "
    f"ERROR"
)
print(f"{'=' * 85}")

for r in sorted(
    results,
    key=lambda x: x["config_id"]
):

    status = r.get("status", "FAILED")

    icon = (
        "SUCCESS"
        if status == "SUCCESS"
        else "FAILED"
    )

    error = (
        r.get("error") or ""
    )[:500000]

    print(
        f"{r['config_id']:>10}  "
        f"{icon:<10}  "
        f"{r.get('rows_read', 0):>12}  "
        f"{r.get('rows_copied', 0):>14}  "
        f"{error}"
    )

print(f"{'=' * 85}")

succeeded = [
    r for r in results
    if r.get("status") == "SUCCESS"
]

failed = [
    r for r in results
    if r.get("status") == "FAILED"
]

print(
    f"\nTotal     : {len(results)}"
)

print(
    f"Succeeded : {len(succeeded)}"
)

print(
    f"Failed    : {len(failed)}"
)

print(
    f"Job Run ID: {job_run_id}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Job Status
# MAGIC
# MAGIC If any ingestion task failed, fail the Databricks Job.
# MAGIC The audit records have already been written by the orchestrator.

# COMMAND ----------

if failed:

    failed_ids = [
        r["config_id"]
        for r in failed
    ]

    raise Exception(
        f"{len(failed)} of "
        f"{len(results)} ingestion object(s) FAILED. "
        f"Failed Config IDs: {failed_ids}. "
        f"Databricks Job Run ID: {job_run_id}. "
        f"Check the audit table for details."
    )

dbutils.notebook.exit(
    f"SUCCESS: "
    f"{len(succeeded)}/{len(results)} "
    f"objects ingested successfully. "
    f"Job Run ID: {job_run_id}"
)
