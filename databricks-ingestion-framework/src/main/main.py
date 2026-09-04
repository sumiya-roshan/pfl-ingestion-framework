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

# MAGIC %pip install python-dotenv --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install paramiko boto3 --quiet
# MAGIC

# COMMAND ----------

import json
import sys
from datetime import datetime, timezone
sys.path.append("..")
from concurrent.futures import ThreadPoolExecutor, as_completed
from ingestion.utils.config_manager import (
    AUDIT_STATUS_FAILED,
    AUDIT_STATUS_SKIPPED,
    AUDIT_STATUS_SUCCESS,
    AUDIT_TABLE,
    CONFIG_MASTER_TABLE,
    DEPENDENCY_TABLE,
    SOURCE_SYSTEM_TABLE,
    ConfigManager,
    IngestionTaskConfig,
    SourceSystemConfig,
)
from ingestion.utils.logger import _upload_on_exit, configure_s3_logging, get_logger
from ingestion.utils.orchestrator import IngestionOrchestrator

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("config_master_id",    "",               "Config Master ID (int — routes to correct child config table)")
dbutils.widgets.text("source_system_id",    "",               "Source System ID (int — fetches credentials + source_name)")
dbutils.widgets.text("pipeline_name",       "",               "Pipeline Name (required)")
dbutils.widgets.text("job_run_id",          "",               "Job Run ID (required) — set to {{job.run_id}} in job config")
dbutils.widgets.text("environment",         "dev",            "Environment: dev | uat | prod")
dbutils.widgets.text("batch_start_date",    "1",              "Batch Start Date")
dbutils.widgets.text("silver_notebook_path",    "",           "Workspace path to Silver transformation notebook (blank = skip Silver trigger)")
dbutils.widgets.text("silver_notebook_timeout", "3600",       "Max seconds to wait for each Silver notebook run")

# COMMAND ----------

config_master_id_raw = dbutils.widgets.get("config_master_id") or None
source_system_id_raw = dbutils.widgets.get("source_system_id") or None
if not config_master_id_raw or not source_system_id_raw:
    dbutils.notebook.exit("Error: config_master_id and source_system_id are required.")

config_master_id     = int(config_master_id_raw)
source_system_id     = int(source_system_id_raw)
pipeline_name        = dbutils.widgets.get("pipeline_name")        or None
try:
    job_id           = dbutils.widgets.get("job_id")           or None
except Exception:
    job_id           = None
job_run_id           = dbutils.widgets.get("job_run_id")           or None

if not pipeline_name:
    dbutils.notebook.exit("Error: pipeline_name widget is required and cannot be empty.")
if not job_run_id:
    dbutils.notebook.exit("Error: job_run_id widget is required and cannot be empty.")

environment          = dbutils.widgets.get("environment")          or "dev"
batch_start_date     = dbutils.widgets.get("batch_start_date")     or "1"
logger               = get_logger(environment=environment)

silver_notebook_path    = dbutils.widgets.get("silver_notebook_path")    or None
silver_notebook_timeout = int(dbutils.widgets.get("silver_notebook_timeout") or "3600")


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
        job_id     = dbutils.widgets.get("job_id")
    except Exception:
        job_id     = None
    databricks_url = (
        f"{databricks_url}/#job/{job_id}"
        if databricks_url and job_id
        else None
    )
    return {
        "job_id": get_context_value("jobId"),
        "job_name": get_context_value("jobName"),
        "notebook_name": get_context_value("notebookPath"), #Can change it to point silver notebook path later
        "databricks_url": databricks_url,
        "trigger_type": get_context_value("triggerType"),
        "trigger_id": get_context_value("triggerId"),
        "trigger_name": get_context_value("triggerName"),
    }


job_context = get_databricks_job_context()
job_context["job_run_id"]   = job_run_id

# pipeline_start_time is job-level — captured ONCE here, before the table
# fan-out below, and threaded through job_context so every table's
# DependencyLogger row uses the same value (see orchestrator.run()).
pipeline_start_time = datetime.now(timezone.utc)
job_context["pipeline_start_time"] = pipeline_start_time

print(f"pipeline_start_time (job-level): {pipeline_start_time}")

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
# MAGIC When running as part of a Databricks Job, Task 0 (`get_tasks.py`) queries
# MAGIC the config tables and publishes the active tasks via `taskValues`.
# MAGIC This task reads them from `taskValues` to avoid duplicate config queries.
# MAGIC Falls back to a direct config query when running the notebook standalone
# MAGIC (interactive / manual run without Task 0).

# COMMAND ----------

payload_str = None
try:
    payload_str = dbutils.jobs.taskValues.get(
        taskKey   = "get_table_details",
        key       = "active_tasks_metadata",
        default   = None,
        debugValue = None,
    )
except Exception as exc:
    print(f"[INFO] taskValues not available (standalone mode): {exc}")

# config_mgr is needed regardless of mode — IngestionOrchestrator uses it
# later for Silver_Last_Sink_Date bookkeeping (see orchestrator.run()), even
# in Job mode where task discovery itself is skipped (tasks already came
# from taskValues).
config_mgr = ConfigManager(
    spark,
    source_system_table = SOURCE_SYSTEM_TABLE,
    config_master_table = CONFIG_MASTER_TABLE,
)

if payload_str:
    # ── Job mode: deserialize what get_tasks.py published ──────────────────
    print("[Tasks] Reading active tasks from taskValues (get_table_details task).")
    payload    = json.loads(payload_str)
    source_sys = SourceSystemConfig.from_dict(payload["source_sys"])
    tasks      = [IngestionTaskConfig.from_dict(t) for t in payload["tasks"]]
    batch_start_date = payload.get("batch_start_date")
else:
    # ── Standalone mode: query config tables directly ──────────────────────
    print("[Tasks] taskValues not available — querying config tables directly (standalone mode).")
    source_sys, tasks = config_mgr.get_active_tasks(
        config_master_id = config_master_id,
        source_system_id = source_system_id,
        pipeline_name    = pipeline_name,
        batch_start_date = batch_start_date,
    )

print(f"Resolved source : {source_sys.source_name} ({source_sys.source_type})")
print(f"Active tasks    : {len(tasks)}")

# Configure S3/Volume logging dynamically
resolved_landing_path = source_sys.landing_volume_path
if resolved_landing_path:
    s3_log_path = f"{resolved_landing_path.rstrip('/')}/logs/{pipeline_name}_{job_run_id}.log"
    configure_s3_logging(s3_log_path, dbutils=dbutils)

logger.info(f"Pipeline started for source: {source_sys.source_name} ({source_sys.source_type})")

if not tasks:
    dbutils.notebook.exit("No active ingestion tasks found for this pipeline.")

# NOTE: max_workers is now derived dynamically from the distinct batch_id count
# for this pipeline in the execution section below.
# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute Ingestion

# COMMAND ----------

# trigger_id also set to rootRunId for traceability in audit trigger_id column
trigger_id = job_run_id
job_context["trigger_id"] = trigger_id

orchestrator = IngestionOrchestrator(
    spark,
    dbutils,
    audit_table             = AUDIT_TABLE,
    dependency_table        = DEPENDENCY_TABLE,
    pipeline_name           = pipeline_name,
    environment             = environment,
    silver_notebook_path    = silver_notebook_path,
    silver_notebook_timeout = silver_notebook_timeout,
    config_mgr              = config_mgr,
)



def run_one(task: IngestionTaskConfig) -> dict:
    logger.info(f"Processing table {task.source_object_name}")
    """
    Run a single ingestion task — works for RDBMS, NoSQL, and S3.

    Retries happen inside IngestionOrchestrator.run(), scoped only to the
    source connection pull (connector.extract), using the source system's
    retry_count/retry_interval from config_source_system. Writing/transform
    steps are not retried — a failure there fails the task outright.
    """

    return orchestrator.run(
        source_sys          = source_sys,
        ingest_obj          = task,
        config_master_id    = config_master_id,   # ← routing table ID from widget
        landing_volume_path = resolved_landing_path,
        trigger_id          = trigger_id,
        job_context          = job_context,
        sink_batch_started_date = batch_start_date,
    )



results = []

# ── Batch-level parallelism ────────────────────────────────────────────────
# batch_id  → controls PARALLEL execution: one thread per distinct batch.
# priority  → controls SEQUENTIAL execution of tables WITHIN a batch (ascending).
#
# max_workers is derived dynamically from the distinct batch_id count for this
# pipeline — NOT hardcoded, NOT taken from the widget/config. Tables are never
# assigned to their own threads; a batch's tables run one-by-one inside the
# batch's single thread. Per-table processing (load type, incremental/full,
# retry, timeout, watermark, …) is unchanged — it all still happens in run_one.

# Group tasks by batch_id, tables inside each batch ordered by priority ascending.
batches = {}
for task in sorted(tasks, key=lambda t: t.priority):
    batches.setdefault(task.batch_id, []).append(task)

max_workers = len(batches)   # distinct batch_id count for this pipeline


def run_batch(batch_id, batch_tasks: list) -> list:
    """Run every table in one batch sequentially, in priority order."""
    batch_results = []
    print(
        f"[Batch {batch_id}] Starting {len(batch_tasks)} table(s) sequentially: "
        f"{[t.source_object_name for t in batch_tasks]}"
    )
    for task in batch_tasks:
        try:
            batch_results.append(run_one(task))
        except Exception as exc:
            print(f"Task {task.source_object_name} (Config ID: {task.config_id}) failed with exception: {exc}")
            batch_results.append({
                "config_id": task.config_id,
                "run_id":   None,
                "status":   AUDIT_STATUS_FAILED,
                "rows_read": 0,
                "error":    str(exc),
            })
    return batch_results


print(
    f"\nStarting {len(tasks)} tasks across {len(batches)} batch(es) with "
    f"ThreadPoolExecutor (max_workers={max_workers})..."
)
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    future_to_batch = {
        executor.submit(run_batch, batch_id, batch_tasks): batch_id
        for batch_id, batch_tasks in batches.items()
    }

    for future in as_completed(future_to_batch):
        batch_id = future_to_batch[future]
        try:
            results.extend(future.result())
        except Exception as exc:
            print(f"Batch {batch_id} failed with exception: {exc}")
            for task in batches[batch_id]:
                results.append({
                    "config_id": task.config_id,
                    "run_id":   None,
                    "status":   AUDIT_STATUS_FAILED,
                    "rows_read": 0,
                    "error":    str(exc),
                })

# COMMAND ----------

# MAGIC %md
# MAGIC ### Close the dependency job
# MAGIC
# MAGIC pipeline_end_time isn't known until every table has finished — bulk-stamp
# MAGIC it (and the derived dependency_resolve_time) onto every dependency_master_config
# MAGIC row for this job_run_id in one shot, now that the fan-out above is done.

# COMMAND ----------

orchestrator.dependency.complete_job(job_run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Results summary
# MAGIC
# MAGIC Silver now runs coupled — inline, synchronously — inside each table's
# MAGIC own orchestrator.run() call (see IngestionOrchestrator._trigger_silver),
# MAGIC right after that table's landing write and before its Bronze Delta
# MAGIC write. By the time a task's future resolves above, its Silver run (if
# MAGIC enabled) has already finished, so results already carry it under
# MAGIC "silver_result" — no separate wait step needed.

# COMMAND ----------
# optional can be removed 
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

silver_results = [r["silver_result"] for r in results if r.get("silver_result")]

if silver_results:
    print(f"{'='*75}")
    print(f"{'CONF ID':>8}  {'SILVER STATUS':<14}  TARGET")
    print(f"{'='*75}")
    for r in sorted(silver_results, key=lambda x: x["config_id"]):
        icon = "✅" if r["status"] == "SUCCESS" else "❌"
        print(f"{r['config_id']:>8}  {icon} {r['status']:<12}  {r.get('target', '')}")
    print(f"{'='*75}")

silver_failed = [r for r in silver_results if r["status"] == "FAILED"]
print(
    f"Silver — Total: {len(silver_results)} | "
    f"✅ Succeeded: {len(silver_results) - len(silver_failed)} | ❌ Failed: {len(silver_failed)}\n"
)

# COMMAND ----------


if failed:
    failed_ids = [r["config_id"] for r in failed]
    silver_failed_ids = [r["config_id"] for r in silver_failed]
    logger.critical(
        f"Pipeline cannot continue — {len(failed)} of {len(results)} ingestion object(s) FAILED. "
        f"Failed Config IDs: {failed_ids}"
        f"{len(silver_failed)} of {len(silver_results)} Silver trigger(s) FAILED "
        f"(Config IDs: {silver_failed_ids}). "
        f"Check the audit table and logs above for details."
    )
    _upload_on_exit()


_upload_on_exit()
dbutils.notebook.exit(
    f"SUCCESS: {len(succeeded)}/{len(results)} objects ingested "
    f"({len(skipped)} skipped — 0 rows in source)."
)

# COMMAND ----------

