# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # LSQ Mavis — Task 1: Ingestion Entry Point
# MAGIC
# MAGIC Reads active table configs published by Task 0 (`get_tasks.py`) via `taskValues`,
# MAGIC then fans out to `MavisOrchestrator.run()` — one call per config row — using the
# MAGIC same batch-level parallelism pattern as `relational_db_main/relational_db_main.py`.
# MAGIC
# MAGIC **Location:** src/api_sources/lsq_mavis/
# MAGIC **sys.path:** appends `../..` to reach `src/` on the Python path.
# MAGIC
# MAGIC Per-table flow (handled by MavisOrchestrator):
# MAGIC   API export trigger → poll → ZIP download → unzip CSV → Silver notebook

# COMMAND ----------

# MAGIC %pip install requests --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# Notebooks at src/api_sources/lsq_mavis/ — append ../.. to reach src/ on sys.path
sys.path.append("../..")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("batch_start_date",        "1",    "Batch Start Date (1 = today)")
dbutils.widgets.text("admin_catalog_name",       "",     "Admin catalog name")
dbutils.widgets.text("environment",              "prod", "Environment: dev | uat | prod")
dbutils.widgets.text("job_run_id",               "",     "Job Run ID — set to {{job.run_id}}")
dbutils.widgets.text("pipeline_name",            "",     "Pipeline name")
dbutils.widgets.text("raw_sa_name",              "",     "ADLS Gen2 storage account name (e.g. pflrawsa)")
dbutils.widgets.text("audit_table",              "",     "FQN of audit log table")
dbutils.widgets.text("dependency_table",         "",     "FQN of dependency_master_config table")
dbutils.widgets.text("silver_notebook_path",     "",     "Workspace path to Mavis Silver notebook (blank = skip Silver)")
dbutils.widgets.text("silver_notebook_timeout",  "3600", "Max seconds to wait for Silver notebook")

# COMMAND ----------

batch_start_date        = dbutils.widgets.get("batch_start_date").strip()
admin_catalog_name      = dbutils.widgets.get("admin_catalog_name").strip()
environment             = dbutils.widgets.get("environment").strip() or "prod"
job_run_id              = dbutils.widgets.get("job_run_id").strip()
pipeline_name           = dbutils.widgets.get("pipeline_name").strip()
raw_sa_name             = dbutils.widgets.get("raw_sa_name").strip()
audit_table             = dbutils.widgets.get("audit_table").strip()
dependency_table        = dbutils.widgets.get("dependency_table").strip()
silver_notebook_path    = dbutils.widgets.get("silver_notebook_path").strip() or None
silver_notebook_timeout = int(dbutils.widgets.get("silver_notebook_timeout").strip() or "3600")

for _name, _val in [
    ("admin_catalog_name", admin_catalog_name),
    ("job_run_id",         job_run_id),
    ("pipeline_name",      pipeline_name),
    ("raw_sa_name",        raw_sa_name),
    ("audit_table",        audit_table),
    ("dependency_table",   dependency_table),
]:
    if not _val:
        dbutils.notebook.exit(f"Error: widget '{_name}' is required and cannot be empty.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Databricks Job Context

# COMMAND ----------

def _get_job_context() -> dict:
    ctx = (
        dbutils.notebook.entry_point
        .getDbutils()
        .notebook()
        .getContext()
    )
    def _safe(method):
        try:
            return getattr(ctx, method)().get()
        except Exception:
            return None

    databricks_url = _safe("apiUrl")
    try:
        job_id = dbutils.widgets.get("job_id") or None
    except Exception:
        job_id = None
    if databricks_url and job_id:
        databricks_url = f"{databricks_url}/#job/{job_id}"

    return {
        "job_id":               _safe("jobId"),
        "job_name":             _safe("jobName"),
        "notebook_name":        _safe("notebookPath"),
        "databricks_url":       databricks_url,
        "trigger_type":         _safe("triggerType"),
        "trigger_id":           job_run_id,
        "trigger_name":         _safe("triggerName"),
        "job_run_id":           job_run_id,
        "pipeline_start_time":  datetime.now(timezone.utc),
    }

job_context = _get_job_context()
print(f"pipeline_start_time: {job_context['pipeline_start_time']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read tasks from taskValues (or fall back to direct query in standalone mode)

# COMMAND ----------

from api_sources.lsq_mavis.config import MavisTableConfig

payload_str = None
try:
    payload_str = dbutils.jobs.taskValues.get(
        taskKey    = "get_table_details",
        key        = "mavis_tasks_metadata",
        default    = None,
        debugValue = None,
    )
except Exception as exc:
    print(f"[INFO] taskValues not available (standalone mode): {exc}")

if payload_str:
    print("[Tasks] Reading from taskValues (get_table_details task).")
    payload          = json.loads(payload_str)
    tasks            = [MavisTableConfig.from_dict(t) for t in payload["tasks"]]
    trigger_time_utc = datetime.fromisoformat(payload["trigger_time_utc"])
    config_table_fqn = payload["config_table_fqn"]
else:
    # ── Standalone / manual run — query config table directly ──────────────────
    print("[Tasks] taskValues not available — querying config table directly.")
    from datetime import timedelta
    _IST_OFFSET  = timedelta(hours=5, minutes=30)
    CONFIG_TABLE = f"{admin_catalog_name}.config.tb_mavis_db_ingestion_config"

    if batch_start_date == "1":
        trigger_time_utc = datetime.now(timezone.utc)
        ist_str = (trigger_time_utc + _IST_OFFSET).strftime("%Y-%m-%d %H:%M:%S")
    else:
        ist_naive        = datetime.strptime(batch_start_date.strip(), "%Y-%m-%d %H:%M:%S")
        trigger_time_utc = (ist_naive - _IST_OFFSET).replace(tzinfo=timezone.utc)
        ist_str          = batch_start_date.strip()

    rows = spark.sql(f"""
        SELECT * FROM {CONFIG_TABLE}
        WHERE  Source_Name = 'LSQ_Mavis'
          AND  is_active   = 1
          AND  date_format(sink_batch_started_date, 'yyyy-MM-dd HH:mm:ss')
                 = date_format('{ist_str}', 'yyyy-MM-dd HH:mm:ss')
        ORDER BY Config_ID
    """).collect()

    tasks            = [MavisTableConfig.from_row(r.asDict()) for r in rows]
    config_table_fqn = CONFIG_TABLE

print(f"Active tasks : {len(tasks)}")
for t in tasks:
    print(f"  Config_ID={t.config_id}  Table={t.sink_table_name}  Load={t.load_type}  Batch={t.batch_id}  Priority={t.priority}")

if not tasks:
    dbutils.notebook.exit("No active tasks found. Nothing to ingest.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Ingestion — Batch-level parallelism
# MAGIC
# MAGIC Same pattern as `relational_db_main/relational_db_main.py`:
# MAGIC   - Group tasks by `batch_id`
# MAGIC   - One thread per distinct batch (max_workers = distinct batch count)
# MAGIC   - Tables within a batch run sequentially, ordered by `priority`

# COMMAND ----------

from api_sources.lsq_mavis.orchestrator import MavisOrchestrator
from ingestion.utils.config_manager import AUDIT_STATUS_FAILED, AUDIT_STATUS_SUCCESS

orchestrator = MavisOrchestrator(
    spark                   = spark,
    dbutils                 = dbutils,
    audit_table             = audit_table,
    dependency_table        = dependency_table,
    pipeline_name           = pipeline_name,
    environment             = environment,
    raw_sa_name             = raw_sa_name,
    silver_notebook_path    = silver_notebook_path,
    silver_notebook_timeout = silver_notebook_timeout,
)

# Group tasks by batch_id — same logic as relational_db_main.py
batches: dict[int, list] = {}
for task in sorted(tasks, key=lambda t: t.priority):
    batches.setdefault(task.batch_id, []).append(task)

max_workers = len(batches)
print(
    f"\nStarting {len(tasks)} task(s) across {len(batches)} batch(es) "
    f"(max_workers={max_workers}) ..."
)


def run_one(task: MavisTableConfig) -> dict:
    return orchestrator.run(
        table                   = task,
        trigger_time_utc        = trigger_time_utc,
        job_context             = job_context,
        sink_batch_started_date = trigger_time_utc,
        config_table_fqn        = config_table_fqn,
    )


def run_batch(batch_id: int, batch_tasks: list) -> list:
    """Run every table in one batch sequentially, ordered by priority."""
    results = []
    print(
        f"[Batch {batch_id}] Starting {len(batch_tasks)} table(s): "
        f"{[t.sink_table_name for t in batch_tasks]}"
    )
    for task in batch_tasks:
        try:
            results.append(run_one(task))
        except Exception as exc:
            print(f"  Task {task.sink_table_name} raised unexpectedly: {exc}")
            results.append({
                "config_id": task.config_id,
                "status":    AUDIT_STATUS_FAILED,
                "rows_read": 0,
                "error":     str(exc),
            })
    return results


results = []
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    future_to_batch = {
        executor.submit(run_batch, bid, btasks): bid
        for bid, btasks in batches.items()
    }
    for future in as_completed(future_to_batch):
        bid = future_to_batch[future]
        try:
            results.extend(future.result())
        except Exception as exc:
            print(f"Batch {bid} raised unexpectedly: {exc}")
            for task in batches[bid]:
                results.append({
                    "config_id": task.config_id,
                    "status":    AUDIT_STATUS_FAILED,
                    "rows_read": 0,
                    "error":     str(exc),
                })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

succeeded      = [r for r in results if r["status"] == AUDIT_STATUS_SUCCESS]
failed         = [r for r in results if r["status"] == AUDIT_STATUS_FAILED]
silver_results = [r["silver_result"] for r in results if r.get("silver_result")]
silver_failed  = [r for r in silver_results if r.get("status") == "FAILED"]

print(f"\n{'='*70}")
print(f"{'CONFIG_ID':>10}  {'STATUS':<10}  {'ROWS':>8}  ERROR")
print(f"{'='*70}")
for r in sorted(results, key=lambda x: x["config_id"]):
    icon  = "✅" if r["status"] == AUDIT_STATUS_SUCCESS else "❌"
    error = (r.get("error") or "")[:50]
    print(f"{r['config_id']:>10}  {icon} {r['status']:<8}  {r.get('rows_read', 0):>8}  {error}")
print(f"{'='*70}")
print(
    f"Total: {len(results)} | ✅ Succeeded: {len(succeeded)} | ❌ Failed: {len(failed)}"
)
if silver_results:
    print(
        f"Silver — Total: {len(silver_results)} | "
        f"✅ OK: {len(silver_results) - len(silver_failed)} | "
        f"❌ Failed: {len(silver_failed)}"
    )

# COMMAND ----------

if failed:
    failed_ids        = [r["config_id"] for r in failed]
    silver_failed_ids = [r["config_id"] for r in silver_failed]
    raise Exception(
        f"LSQ Mavis ingestion FAILED for {len(failed)}/{len(results)} table(s). "
        f"Failed Config IDs: {failed_ids}. "
        f"Silver failures (Config IDs): {silver_failed_ids}."
    )

dbutils.notebook.exit(
    f"SUCCESS: {len(succeeded)}/{len(results)} tables ingested."
)
