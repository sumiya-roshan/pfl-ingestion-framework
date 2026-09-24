# Databricks notebook source
# MAGIC %md
# MAGIC # Batch Runner -- Parallel Table Execution for One Batch
# MAGIC
# MAGIC Called by main.py via dbutils.notebook.run() -- one instance per batch_id.
# MAGIC Receives the pre-serialised tasks for its batch and runs them in parallel up to
# MAGIC batch_count workers, submitted in priority order.
# MAGIC
# MAGIC Mirrors ADF ForEach activity with sequential=OFF and batchCount=N.
# MAGIC Each batch runs as an independent notebook on the same cluster, so batches
# MAGIC are isolated from each other while still sharing cluster resources.
# MAGIC
# MAGIC Exit contract: always calls dbutils.notebook.exit(json.dumps(results)).
# MAGIC Never raises -- partial failures are captured so main.py can aggregate.
# MAGIC
# MAGIC dependency_master_config.complete_job() is NOT called here.
# MAGIC main.py calls it once after all batch notebooks finish.

# COMMAND ----------

# MAGIC %pip install python-dotenv --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install paramiko boto3 --quiet

# COMMAND ----------

import json
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

sys.path.append("..")

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

dbutils.widgets.text("batch_id",                "",      "Batch ID to process (int)")
dbutils.widgets.text("batch_count",             "10",    "Max parallel tables in this batch")
dbutils.widgets.text("batch_tasks_json",        "",      "JSON-serialised list of IngestionTaskConfig dicts for this batch")
dbutils.widgets.text("source_sys_json",         "",      "JSON-serialised SourceSystemConfig dict (contains landing_volume_path)")
dbutils.widgets.text("job_context_json",        "",      "JSON-serialised job context dict from main.py")
dbutils.widgets.text("config_master_id",        "",      "Config Master ID (int)")
dbutils.widgets.text("pipeline_name",           "",      "Pipeline Name")
dbutils.widgets.text("job_run_id",              "",      "Job Run ID")
dbutils.widgets.text("trigger_id",              "",      "Trigger ID (defaults to job_run_id)")
dbutils.widgets.text("environment",             "dev",   "Environment: dev | uat | prod")
dbutils.widgets.text("batch_start_date",        "",      "Batch start date as ISO string (empty -> now)")
dbutils.widgets.text("silver_notebook_timeout", "3600",  "Max seconds to wait for each Silver notebook run")

# COMMAND ----------

batch_id_raw            = dbutils.widgets.get("batch_id")
batch_count             = int(dbutils.widgets.get("batch_count") or "10")
batch_tasks_json        = dbutils.widgets.get("batch_tasks_json")
source_sys_json         = dbutils.widgets.get("source_sys_json")
job_context_json        = dbutils.widgets.get("job_context_json") or "{}"
config_master_id        = int(dbutils.widgets.get("config_master_id") or "0")
pipeline_name           = dbutils.widgets.get("pipeline_name") or None
job_run_id              = dbutils.widgets.get("job_run_id") or None
trigger_id              = dbutils.widgets.get("trigger_id") or job_run_id
environment             = dbutils.widgets.get("environment") or "dev"
batch_start_date_str    = dbutils.widgets.get("batch_start_date") or ""
silver_notebook_timeout = int(dbutils.widgets.get("silver_notebook_timeout") or "3600")

if not batch_id_raw:
    dbutils.notebook.exit(json.dumps([{
        "config_id": -1, "status": AUDIT_STATUS_FAILED,
        "error": "batch_id widget is required", "rows_read": 0,
    }]))
if not batch_tasks_json:
    dbutils.notebook.exit(json.dumps([{
        "config_id": -1, "status": AUDIT_STATUS_FAILED,
        "error": "batch_tasks_json widget is required", "rows_read": 0,
    }]))
if not source_sys_json:
    dbutils.notebook.exit(json.dumps([{
        "config_id": -1, "status": AUDIT_STATUS_FAILED,
        "error": "source_sys_json widget is required", "rows_read": 0,
    }]))

batch_id = int(batch_id_raw)

# COMMAND ----------

if batch_start_date_str and batch_start_date_str.strip() not in ("", "1"):
    batch_start_date = datetime.fromisoformat(batch_start_date_str.strip())
else:
    batch_start_date = datetime.now(timezone.utc)

tasks       = [IngestionTaskConfig.from_dict(t) for t in json.loads(batch_tasks_json)]
source_sys  = SourceSystemConfig.from_dict(json.loads(source_sys_json))
job_context = json.loads(job_context_json)

# Derive landing path from source_sys -- same as main.py does, no separate widget needed.
# source_sys.landing_volume_path is already present in source_sys_json (passed by main.py).
resolved_landing_path = source_sys.landing_volume_path

logger = get_logger(environment=environment)

# Configure per-batch S3 log file.
# logger.py appends os.getpid() to the temp filename so concurrent
# batch_runner processes never collide on the same /tmp/ file.
if resolved_landing_path:
    s3_log_path = (
        f"{resolved_landing_path.rstrip('/')}/logs/"
        f"{pipeline_name or source_sys.source_name}_batch{batch_id}_{job_run_id}.log"
    )
    configure_s3_logging(s3_log_path, dbutils=dbutils)

logger.info(
    f"[Batch {batch_id}] Starting {len(tasks)} table(s) with batch_count={batch_count} workers"
)
print(
    f"[Batch {batch_id}] Tables (priority order): "
    f"{[t.source_object_name for t in sorted(tasks, key=lambda t: t.priority)]}"
)

# COMMAND ----------

config_mgr = ConfigManager(
    spark,
    source_system_table=SOURCE_SYSTEM_TABLE,
    config_master_table=CONFIG_MASTER_TABLE,
)

orchestrator = IngestionOrchestrator(
    spark,
    dbutils,
    audit_table             = AUDIT_TABLE,
    dependency_table        = DEPENDENCY_TABLE,
    pipeline_name           = pipeline_name,
    environment             = environment,
    silver_notebook_path    = source_sys.silver_notebook_path,
    silver_notebook_timeout = silver_notebook_timeout,
    config_mgr              = config_mgr,
)


def run_one(task: IngestionTaskConfig) -> dict:
    """Run a single ingestion task -- JDBC extract + optional Silver trigger."""
    logger.info(f"[Batch {batch_id}] Processing table: {task.source_object_name}")
    return orchestrator.run(
        source_sys              = source_sys,
        ingest_obj              = task,
        config_master_id        = config_master_id,
        landing_volume_path     = resolved_landing_path,
        trigger_id              = trigger_id,
        job_context             = job_context,
        sink_batch_started_date = batch_start_date,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute -- parallel within this batch (ADF ForEach batchCount parity)
# MAGIC
# MAGIC Tables submitted in ascending priority order so lower-priority values
# MAGIC are scheduled first when pool slots are free.
# MAGIC All tables up to batch_count fire concurrently -- identical to ADF
# MAGIC ForEach with sequential=OFF and batchCount=batch_count.

# COMMAND ----------

ordered_tasks = sorted(tasks, key=lambda t: t.priority)
results: list[dict] = []

with ThreadPoolExecutor(max_workers=batch_count) as executor:
    future_to_task = {
        executor.submit(run_one, task): task for task in ordered_tasks
    }
    for fut in as_completed(future_to_task):
        task = future_to_task[fut]
        try:
            results.append(fut.result())
        except Exception as exc:
            # Use repr(exc) to always get the exception class name + args even
            # when str(exc) is empty (e.g. plain "raise SomeError()").
            # traceback.format_exc() gives the full stack trace for debugging.
            exc_repr  = repr(exc)
            exc_trace = traceback.format_exc()
            print(
                f"[Batch {batch_id}] ❌ Task {task.source_object_name} "
                f"(Config ID: {task.config_id}) FAILED\n"
                f"  Exception : {exc_repr}\n"
                f"  Traceback :\n{exc_trace}"
            )
            results.append({
                "config_id":     task.config_id,
                "run_id":        None,
                "status":        AUDIT_STATUS_FAILED,
                "rows_read":     0,
                "error":         exc_repr,
                "silver_result": None,
            })

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch-level summary

# COMMAND ----------

STATUS_ICONS = {AUDIT_STATUS_SUCCESS: "✅", AUDIT_STATUS_SKIPPED: "⏭️"}
print(f"\n{'='*75}")
print(f"[Batch {batch_id}] Results")
print(f"{'CONF ID':>8}  {'STATUS':<10}  {'ROWS':>8}  ERROR")
print(f"{'='*75}")
for r in sorted(results, key=lambda x: x["config_id"]):
    icon  = STATUS_ICONS.get(r["status"], "❌")
    error = (r.get("error") or "")[:80]
    print(f"{r['config_id']:>8}  {icon} {r['status']:<8}  {r.get('rows_read', 0):>8}  {error}")
print(f"{'='*75}")

succeeded = [r for r in results if r["status"] == AUDIT_STATUS_SUCCESS]
skipped   = [r for r in results if r["status"] == AUDIT_STATUS_SKIPPED]
failed    = [r for r in results if r["status"] == AUDIT_STATUS_FAILED]
print(
    f"[Batch {batch_id}] Total: {len(results)} | "
    f"✅ Succeeded: {len(succeeded)} | "
    f"⏭️  Skipped: {len(skipped)} | "
    f"❌ Failed: {len(failed)}"
)

_upload_on_exit()

# Return all results to main.py as a JSON string for aggregation.
# dependency_logger.complete_job() is NOT called here.
# main.py calls it once after ALL batch notebooks return.
dbutils.notebook.exit(json.dumps(results))