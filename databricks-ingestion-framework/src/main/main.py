# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestion — Source System Entry Point
# MAGIC
# MAGIC Discovers and executes all active ingestion tasks for a given source system.
# MAGIC
# MAGIC **Input:** `config_master_id` and `source_system_id`.
# MAGIC The notebook queries `config_master` to find the correct child config table
# MAGIC (e.g. `rdbms_ingestion_config`, `nosql_ingestion_config`, `s3_config_master`),
# MAGIC fetches all active rows (`Is_Active = 1`) for the resolved source name,
# MAGIC and runs them sequentially.
# MAGIC
# MAGIC **All source types use the same flow** — RDBMS, NoSQL, and S3.
# MAGIC The factory routes to the right connector based on `config_source_system.source_type`.
# MAGIC
# MAGIC **Fault tolerance:** a failure on one table does NOT stop the others.
# MAGIC All objects are attempted; a summary is printed at the end. The notebook
# MAGIC raises a final exception only if at least one table failed.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install paramiko boto3 --quiet
# MAGIC

# COMMAND ----------

import sys
sys.path.append("..")

from concurrent.futures import ThreadPoolExecutor, as_completed
from ingestion.utils.config_manager import (
    AUDIT_STATUS_FAILED,
    AUDIT_STATUS_SUCCESS,
    AUDIT_STATUS_SKIPPED,
    ConfigManager,
    IngestionTaskConfig,
    SOURCE_SYSTEM_TABLE,
    CONFIG_MASTER_TABLE,
    AUDIT_TABLE,
)
from ingestion.utils.orchestrator import IngestionOrchestrator

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("config_master_id",    "",               "Config Master ID (int — routes to correct child config table)")
dbutils.widgets.text("source_system_id",    "",               "Source System ID (int — fetches credentials + source_name)")
dbutils.widgets.text("target_catalog",      "",               "Target Catalog (e.g. main, hive_metastore)")
dbutils.widgets.text("pipeline_name",       "",               "Pipeline Name (required)")
dbutils.widgets.text("job_run_id",          "",               "Job Run ID (required) — set to {{job.run_id}} in job config")
# dbutils.widgets.text("trigger_type",        "",               "Trigger Type (required) — SCHEDULED | MANUAL | EVENT")
dbutils.widgets.text("landing_volume_path", "",               "Landing Volume Base Path (blank = skip landing write)")
dbutils.widgets.text("environment",         "dev",            "Environment: dev | uat | prod")
dbutils.widgets.text("audit_table",         AUDIT_TABLE,      "Audit Table (override)")
dbutils.widgets.text("batch_start_date",    "1",              "Batch Start Date")
dbutils.widgets.text("max_workers",         "4",              "Max parallel workers (lookup + extraction per task)")

# COMMAND ----------

config_master_id_raw = dbutils.widgets.get("config_master_id") or None
source_system_id_raw = dbutils.widgets.get("source_system_id") or None

if not config_master_id_raw or not source_system_id_raw:
    dbutils.notebook.exit("Error: config_master_id and source_system_id are required.")

config_master_id     = int(config_master_id_raw)
source_system_id     = int(source_system_id_raw)

target_catalog       = dbutils.widgets.get("target_catalog")       or "hive_metastore"
pipeline_name        = dbutils.widgets.get("pipeline_name")        or None
try:
    job_id              = dbutils.widgets.get("job_id")           or None
except Exception:
    job_id              = None
job_run_id           = dbutils.widgets.get("run_id")           or None
# trigger_type         = dbutils.widgets.get("trigger_type")         or None

if not pipeline_name:
    dbutils.notebook.exit("Error: pipeline_name widget is required and cannot be empty.")
if not job_run_id:
    dbutils.notebook.exit("Error: job_run_id widget is required and cannot be empty.")
# if not trigger_type:
#     dbutils.notebook.exit("Error: trigger_type widget is required and cannot be empty.")

landing_volume_path  = dbutils.widgets.get("landing_volume_path")  or None
environment          = dbutils.widgets.get("environment")          or "dev"
audit_table          = dbutils.widgets.get("audit_table")          or AUDIT_TABLE
batch_start_date_raw = dbutils.widgets.get("batch_start_date")     or "1"
max_workers          = int(dbutils.widgets.get("max_workers") or "4")

# Parse batch_start_date_raw to datetime object if it is from the orchestrator
from datetime import datetime
parsed_batch_start_date = None
if batch_start_date_raw and str(batch_start_date_raw).strip() != "1":
    try:
        clean_dt = str(batch_start_date_raw).replace("T", " ").split(".")[0]
        parsed_batch_start_date = datetime.strptime(clean_dt, "%Y-%m-%d %H:%M:%S")
        print(f"Using parsed batch_start_date: {parsed_batch_start_date}")
    except Exception as parse_err:
        print(f"[Warning] Failed to parse batch_start_date '{batch_start_date_raw}': {parse_err}")


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
    databricks_url = get_context_value("apiUrl")
    try:
        job_id = dbutils.widgets.get("job_id")
    except Exception:
        job_id = None

    databricks_url = (
        f"{databricks_url}/#job/{job_id}"
        if databricks_url and job_id
        else None
    )
    return {
        "job_id": get_context_value("jobId"),
        "job_name": get_context_value("jobName"),
        "notebook_name": get_context_value("notebookPath"),
        "databricks_url": databricks_url,
        "trigger_type": get_context_value("triggerType"),
        "trigger_id": get_context_value("triggerId"),
        "trigger_name": get_context_value("triggerName"),
    }


job_context = get_databricks_job_context()

# ── job_run_id and trigger_type from widget values ──
print(f"job_run_id   : {job_run_id}")
# print(f"trigger_type : {trigger_type}")

job_context["job_run_id"]   = job_run_id
# job_context["trigger_type"] = trigger_type

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolve pipeline name
# MAGIC Always read from the widget value.

# COMMAND ----------

print(f"pipeline_name from widget: '{pipeline_name}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Discover ingestion tasks for this source
# MAGIC
# MAGIC Lookup (presence check) and extraction are coupled per table inside
# MAGIC `IngestionOrchestrator.run()` — there is no separate lookup-only phase.
# MAGIC Every active task is loaded here and handed straight to the thread pool below;
# MAGIC tables with 0 rows in the source are detected and skipped individually as
# MAGIC their turn comes up, right before extraction would have started.

# COMMAND ----------

config_mgr = ConfigManager(
    spark,
    source_system_table = SOURCE_SYSTEM_TABLE,
    config_master_table = CONFIG_MASTER_TABLE,
    target_catalog      = target_catalog,
)

source_sys, tasks = config_mgr.get_active_tasks(
    config_master_id = config_master_id,
    source_system_id = source_system_id,
    pipeline_name    = pipeline_name,
    batch_start_date = batch_start_date_raw,
)

print(f"Resolved source : {source_sys.source_name} ({source_sys.source_type})")
print(f"Active tasks    : {len(tasks)}")

if not tasks:
    dbutils.notebook.exit("No active ingestion tasks found for this pipeline.")

# rdbms_ingestion_config.Max_Workers overrides the widget default when set —
# same value is expected on every row for a given pipeline (see IngestionTaskConfig.max_workers).
config_max_workers = tasks[0].max_workers
if config_max_workers:
    max_workers = config_max_workers
    print(f"Using max_workers={max_workers} from rdbms_ingestion_config.Max_Workers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute Ingestion

# COMMAND ----------

# trigger_id also set to rootRunId for traceability in audit trigger_id column
trigger_id = job_run_id
job_context["trigger_id"] = trigger_id

job_context["trigger_id"] = trigger_id

orchestrator = IngestionOrchestrator(
    spark,
    dbutils,
    audit_table   = audit_table,
    pipeline_name = pipeline_name,
    environment   = environment,
)

def run_one(task: IngestionTaskConfig) -> dict:
    """
    Run a single ingestion task — works for RDBMS, NoSQL, and S3.

    Retries happen inside IngestionOrchestrator.run(), scoped only to the
    source connection pull (connector.extract), using the source system's
    retry_count/retry_interval from config_source_system. Writing/transform
    steps are not retried — a failure there fails the task outright.
    """
    return orchestrator.run(
        source_sys          = source_sys,
        task                = task,
        config_master_id    = config_master_id,   # ← routing table ID from widget
        landing_volume_path = landing_volume_path,
        trigger_id          = trigger_id,
        # trigger_type        = trigger_type,
        job_context          = job_context,
        sink_batch_started_date = parsed_batch_start_date,
    )


results = []
# max_workers = 4 # Adjust depending on cluster size and DB load

tasks_sorted = sorted(tasks, key=lambda t: t.priority)

print(f"\nStarting {len(tasks_sorted)} tasks with ThreadPoolExecutor (max_workers={max_workers})...")
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    future_to_task = {executor.submit(run_one, task): task for task in tasks_sorted}
    
    for future in as_completed(future_to_task):
        task = future_to_task[future]
        try:
            res = future.result()
            results.append(res)
        except Exception as exc:
            print(f"Task {task.source_object_name} (Config ID: {task.config_id}) failed with exception: {exc}")
            results.append({
                "config_id": task.config_id,
                "run_id":   None,
                "status":   AUDIT_STATUS_FAILED,
                "rows_read": 0,
                "error":    str(exc),
            })

# COMMAND ----------

# MAGIC %md
# MAGIC ### Results summary

# COMMAND ----------

STATUS_ICONS = {
    AUDIT_STATUS_SUCCESS: "✅",
    AUDIT_STATUS_SKIPPED: "⏭️",
}

print(f"\n{'='*75}")
print(f"{'CONF ID':>8}  {'STATUS':<10}  {'ROWS':>8}  ERROR")
print(f"{'='*75}")
for r in sorted(results, key=lambda x: x["config_id"]):
    icon   = STATUS_ICONS.get(r["status"], "❌")
    error  = (r.get("error") or "")[:50]
    print(f"{r['config_id']:>8}  {icon} {r['status']:<8}  {r.get('rows_read', 0):>8}  {error}")
print(f"{'='*75}")

succeeded = [r for r in results if r["status"] == AUDIT_STATUS_SUCCESS]
skipped   = [r for r in results if r["status"] == AUDIT_STATUS_SKIPPED]
failed    = [r for r in results if r["status"] == AUDIT_STATUS_FAILED]
print(
    f"Total: {len(results)} | ✅ Succeeded: {len(succeeded)} | "
    f"⏭️ Skipped (0 rows): {len(skipped)} | ❌ Failed: {len(failed)}\n"
)

# COMMAND ----------

if failed:
    failed_ids = [r["config_id"] for r in failed]
    raise Exception(
        f"{len(failed)} of {len(results)} ingestion object(s) FAILED. "
        f"Check the audit table for details. Failed Config IDs: {failed_ids}"
    )

dbutils.notebook.exit(
    f"SUCCESS: {len(succeeded)}/{len(results)} objects ingested "
    f"({len(skipped)} skipped — 0 rows in source)."
)

# COMMAND ----------


