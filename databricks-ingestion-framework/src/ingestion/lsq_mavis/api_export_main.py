# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Ingestion — API Export Entry Point
# MAGIC
# MAGIC Executes active **API-export** ingestion tasks (LSQ Mavis today).
# MAGIC
# MAGIC This is the sibling of `main.py`: `main.py` runs connector-based sources
# MAGIC (JDBC / S3 / Mongo / federated) through `IngestionOrchestrator`;
# MAGIC this notebook runs the export-API shape — start → poll → download URL →
# MAGIC download + unzip the archive to the raw S3 landing path — via
# MAGIC `MavisApiExtractor`. The landed files are picked up later by the normal
# MAGIC S3 ingestion config.
# MAGIC
# MAGIC **Input:** `config_master_id` and `source_system_id`.
# MAGIC Tasks come from `taskValues` (Task 0 — `get_tasks.py`) in Job mode, or from
# MAGIC a direct config query in standalone mode.
# MAGIC
# MAGIC **Parallelism:** one worker per task (`max_workers` = distinct `config_id`
# MAGIC count), all tables run concurrently. The API steps within one table stay
# MAGIC sequential.
# MAGIC
# MAGIC **Audit & logging:** each task gets one audit row (INPROGRESS → SUCCESS /
# MAGIC FAILED) and the run's logs are uploaded to the source's landing path —
# MAGIC both owned by `MavisApiExtractor`, mirroring `IngestionOrchestrator`.
# MAGIC
# MAGIC **Retry:** the export sequence per task is retried as a unit using
# MAGIC `retry_count` / `retry_interval` from the `config_source_system` row.
# MAGIC
# MAGIC **Fault tolerance:** a failure on one task does NOT stop the others. Every
# MAGIC task is attempted; a summary is printed at the end and the notebook raises
# MAGIC only if at least one task failed (each failed row is flagged
# MAGIC `Status = FAILED` by `MavisApiExtractor.run`).

# COMMAND ----------

# MAGIC %pip install requests boto3 python-dotenv --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import sys
from datetime import datetime, timezone

# notebook lives at src/ingestion/lsq_mavis/ — two levels below src/
sys.path.append("../..")

from ingestion.lsq_mavis.mavis_api_extractor import MavisApiExtractor
from ingestion.utils.config_manager import (
    AUDIT_TABLE,
    CONFIG_MASTER_TABLE,
    MAVIS_SOURCE_NAME,
    SOURCE_SYSTEM_TABLE,
    ConfigManager,
    IngestionTaskConfig,
    MavisIngestionTaskConfig,
    SourceSystemConfig,
)
from ingestion.utils.logger import _upload_on_exit, configure_s3_logging, get_logger

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("config_master_id", "", "Config Master ID (int)")
dbutils.widgets.text("source_system_id", "", "Source System ID (int)")
dbutils.widgets.text("pipeline_name", "", "Pipeline Name (required)")
dbutils.widgets.text("job_run_id", "", "Job Run ID (required) — set to {{job.run_id}} in job config")
dbutils.widgets.text("environment", "dev", "Environment: dev | uat | prod")
dbutils.widgets.text("batch_start_date", "1", "Batch Start Date")
dbutils.widgets.text("silver_notebook_path",
                     "/PFL/Admin/Config/Dependency_Config/silver_notebook_execution_maivs",
                     "Silver notebook to run after extraction (blank = skip)")
dbutils.widgets.text("silver_notebook_timeout", "", "Silver run timeout (s); blank = use query_timeout")

# COMMAND ----------

config_master_id_raw = dbutils.widgets.get("config_master_id") or None
source_system_id_raw = dbutils.widgets.get("source_system_id") or None
if not config_master_id_raw or not source_system_id_raw:
    dbutils.notebook.exit("Error: config_master_id and source_system_id are required.")

config_master_id = int(config_master_id_raw)
source_system_id = int(source_system_id_raw)
pipeline_name = dbutils.widgets.get("pipeline_name") or None
job_run_id = dbutils.widgets.get("job_run_id") or None
if not pipeline_name:
    dbutils.notebook.exit("Error: pipeline_name widget is required and cannot be empty.")
if not job_run_id:
    dbutils.notebook.exit("Error: job_run_id widget is required and cannot be empty.")

environment = dbutils.widgets.get("environment") or "dev"
batch_start_date = dbutils.widgets.get("batch_start_date") or "1"
silver_notebook_path = dbutils.widgets.get("silver_notebook_path") or None
silver_notebook_timeout = int(dbutils.widgets.get("silver_notebook_timeout") or 0) or None
get_tasks_task_key = "get_table_details"  # taskValues task key published by get_tasks.py

logger = get_logger(environment=environment)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks job context
# MAGIC
# MAGIC Job/run metadata for the audit row — nothing hardcoded.

# COMMAND ----------

def get_databricks_job_context():
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()

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
        f"{databricks_url}/#job/{job_id}" if databricks_url and job_id else None
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
job_context["job_run_id"] = job_run_id
job_context["trigger_id"] = job_run_id
job_context["pipeline_start_time"] = datetime.now(timezone.utc)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Discover tasks
# MAGIC
# MAGIC Job mode: read what `get_tasks.py` published to `taskValues`.
# MAGIC Standalone mode: query the config tables directly.

# COMMAND ----------

config_mgr = ConfigManager(
    spark,
    source_system_table=SOURCE_SYSTEM_TABLE,
    config_master_table=CONFIG_MASTER_TABLE,
)

payload_str = None
try:
    payload_str = dbutils.jobs.taskValues.get(
        taskKey=get_tasks_task_key,
        key="active_tasks_metadata",
        default=None,
        debugValue=None,
    )
except Exception as exc:
    print(f"[INFO] taskValues not available (standalone mode): {exc}")

if payload_str:
    print(f"[Tasks] Reading active tasks from taskValues ('{get_tasks_task_key}').")
    payload = json.loads(payload_str)
    source_sys = SourceSystemConfig.from_dict(payload["source_sys"])
    # same payload shape for every source — only the task class differs
    is_mavis = (source_sys.source_name or "").strip().upper() == MAVIS_SOURCE_NAME.upper()
    task_cls = MavisIngestionTaskConfig if is_mavis else IngestionTaskConfig
    tasks = [task_cls.from_dict(t) for t in payload["tasks"]]
    batch_start_date = payload.get("batch_start_date") or batch_start_date
else:
    print("[Tasks] taskValues not available — querying config tables directly.")
    source_sys, tasks = config_mgr.get_active_tasks(
        config_master_id=config_master_id,
        source_system_id=source_system_id,
        pipeline_name=pipeline_name,
        batch_start_date=batch_start_date,
    )

print(f"Resolved source : {source_sys.source_name} ({source_sys.source_type})")
print(f"Active tasks    : {len(tasks)}")

# Configure S3/Volume logging dynamically (same as main.py)
resolved_landing_path = source_sys.landing_volume_path
if resolved_landing_path:
    s3_log_path = f"{resolved_landing_path.rstrip('/')}/logs/{pipeline_name}_{job_run_id}.log"
    configure_s3_logging(s3_log_path, dbutils=dbutils)

logger.info(
    f"API-export started for source: {source_sys.source_name} "
    f"({source_sys.source_type}) — {len(tasks)} task(s)"
)

if not tasks:
    dbutils.notebook.exit("No active API-export tasks found.")

if not all(isinstance(t, MavisIngestionTaskConfig) for t in tasks):
    raise RuntimeError(
        "api_export_main received non-Mavis tasks — check the config_master_id "
        "routes to an export-API source (source_name = 'LSQ_Mavis')."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run export tasks
# MAGIC
# MAGIC `extractor.run_all(...)` fans the tasks out one worker per distinct
# MAGIC `config_id`; the API steps for a single table (start → poll → download URL
# MAGIC → download/unzip → Silver) stay sequential inside `MavisApiExtractor.run`.
# MAGIC The audit row, child-config Status, retry and Silver trigger are all owned
# MAGIC by the extractor. Returns `{config_id: (status, detail)}`.

# COMMAND ----------

# Endpoint base URL (prod_api) comes from each task's own config row; retry /
# interval / timeout come from config_source_system. The parallel fan-out
# (one worker per distinct config_id), the audit row, child-config Status and
# the Silver notebook trigger are all owned by MavisApiExtractor. dbutils is
# passed so it can call dbutils.notebook.run for Silver.
extractor = MavisApiExtractor(
    spark,
    config_mgr,
    audit_table=AUDIT_TABLE,
    environment=environment,
    dbutils=dbutils,
    silver_notebook_path=silver_notebook_path,
    silver_notebook_timeout=silver_notebook_timeout,
)

# {config_id: ("SUCCESS", [zip, csv]) | ("FAILED", "<error>")}
results = extractor.run_all(
    tasks, source_sys, pipeline_name, job_context, config_master_id
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary

# COMMAND ----------

succeeded = [c for c, (s, _) in results.items() if s == "SUCCESS"]
failed = [c for c, (s, _) in results.items() if s == "FAILED"]

print(f"Total   : {len(results)}")
print(f"Success : {len(succeeded)} {succeeded}")
print(f"Failed  : {len(failed)} {failed}")
for cid in failed:
    print(f"  config_id={cid}: {results[cid][1]}")

if failed:
    logger.critical(
        f"API-export — {len(failed)} of {len(results)} task(s) FAILED. "
        f"Failed Config IDs: {failed}. Check the audit table and logs above."
    )
else:
    logger.info(
        f"API-export complete — {len(succeeded)}/{len(results)} task(s) succeeded."
    )

_upload_on_exit()

if failed:
    raise RuntimeError(
        f"{len(failed)} of {len(results)} API-export task(s) failed: {failed}"
    )

dbutils.notebook.exit(
    f"Success: {len(succeeded)} API-export task(s) completed for "
    f"{source_sys.source_name}."
)
