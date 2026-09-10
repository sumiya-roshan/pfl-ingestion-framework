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
# MAGIC
# MAGIC The main cell at the bottom reads top-down: build job context → resolve
# MAGIC tasks → route → run pipeline → summarise → exit. Each stage is a named
# MAGIC function defined in the cells above it.

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
from ingestion.utils.config_manager import (
    AUDIT_STATUS_FAILED,
    AUDIT_STATUS_SKIPPED,
    AUDIT_STATUS_SUCCESS,
    AUDIT_TABLE,
    CONFIG_MASTER_TABLE,
    DEPENDENCY_TABLE,
    MAVIS_SOURCE_NAME,
    SOURCE_SYSTEM_TABLE,
    ConfigManager,
    IngestionTaskConfig,
    MavisIngestionTaskConfig,
    SourceSystemConfig,
)
from ingestion.utils.databricks_context import get_databricks_job_context
from ingestion.utils.logger import _upload_on_exit, configure_s3_logging, get_logger
from ingestion.utils.mavis_api_extractor import MavisApiExtractor
from ingestion.utils.orchestrator import IngestionOrchestrator
from ingestion.utils.pipeline_results import make_task_result, print_results_summary
from ingestion.utils.task_executor import execute_batches, execute_parallel

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
# MAGIC ### Stage functions
# MAGIC
# MAGIC Each stage of the run is a named function; the main cell at the bottom
# MAGIC calls them in order.

# COMMAND ----------

def build_job_context(dbutils, job_run_id: str, job_id: str | None) -> dict:
    """
    Assemble the job/run context dict that is threaded through every task.

    """
    job_context = get_databricks_job_context(dbutils, job_id=job_id)
    job_context["job_run_id"] = job_run_id
    job_context["pipeline_start_time"] = datetime.now(timezone.utc)
    print(f"pipeline_start_time (job-level): {job_context['pipeline_start_time']}")
    return job_context


def resolve_tasks(
    dbutils,
    config_mgr: ConfigManager,
    *,
    config_master_id: int,
    source_system_id: int,
    pipeline_name: str,
    batch_start_date,
):
    """
    Discover the active tasks for this run and normalise ``batch_start_date``.

    Two modes, transparent to the caller:

    * **Job mode** — Task 0 (``get_tasks.py``) already queried the config tables
      and published the active tasks to ``taskValues`` (task key
      ``get_table_details`` / key ``active_tasks_metadata``); deserialize those
      to avoid a duplicate config query.
    * **Standalone mode** — no ``taskValues`` (interactive / manual run without
      Task 0); query the config tables directly.

    """
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

    if payload_str:
        run_mode = "JOB"
        print("[Tasks] Reading active tasks from taskValues (get_table_details task).")
        payload    = json.loads(payload_str)
        source_sys = SourceSystemConfig.from_dict(payload["source_sys"])
        # Same payload shape for every source; only the task class differs —
        # LSQ_Mavis rows carry the export-API fields (MavisIngestionTaskConfig),
        # all others are IngestionTaskConfig.
        is_mavis   = (source_sys.source_name or "").strip().upper() == MAVIS_SOURCE_NAME.upper()
        task_cls   = MavisIngestionTaskConfig if is_mavis else IngestionTaskConfig
        tasks      = [task_cls.from_dict(t) for t in payload["tasks"]]
        batch_start_date = payload.get("batch_start_date")
    else:
        run_mode = "STANDALONE"
        print("[Tasks] taskValues not available — querying config tables directly (standalone mode).")
        source_sys, tasks = config_mgr.get_active_tasks(
            config_master_id = config_master_id,
            source_system_id = source_system_id,
            pipeline_name    = pipeline_name,
            batch_start_date = batch_start_date,
        )

    if isinstance(batch_start_date, str) and batch_start_date.strip() not in ("", "1"):
        batch_start_date = datetime.fromisoformat(
            batch_start_date.strip().replace("T", " ")
        )
    else:
        batch_start_date = datetime.now(timezone.utc)

    print(f"Resolved source : {source_sys.source_name} ({source_sys.source_type})")
    print(f"Active tasks    : {len(tasks)}")
    return source_sys, tasks, batch_start_date, run_mode


def run_pipeline(
    tasks,
    source_sys,
    *,
    is_api_export: bool,
    spark,
    dbutils,
    config_mgr: ConfigManager,
    job_context: dict,
    job_run_id: str,
    config_master_id: int,
    environment: str,
    pipeline_name: str,
    resolved_landing_path: str | None,
    trigger_id: str,
    batch_start_date,
    silver_notebook_path: str | None,
    silver_notebook_timeout: int,
) -> list:
    """
    Build the right per-task runner for this source, fan the tasks out, and
    (connector path only) close the dependency job.

    ``is_api_export`` picks the runner; both return the same result shape:

    * **False** — ``IngestionOrchestrator`` (RDBMS / NoSQL / S3). Silver runs
      COUPLED — inline, synchronously — inside each ``orchestrator.run`` call
      right after that table's landing write and before its Bronze Delta write.
      So by the time a task result comes back its Silver run (if enabled) has
      already finished and is carried on the result under ``silver_result`` — no
      separate wait step.
    * **True** — ``MavisApiExtractor`` (LSQ Mavis export API). It owns its own
      audit row + child-config Status write + logging, mirroring the orchestrator.

    After the fan-out, the connector path bulk-stamps ``pipeline_end_time`` (and
    the derived ``dependency_resolve_time``) onto every ``dependency_master_config``
    row for this ``job_run_id`` in one shot — it isn't known until every table
    has finished. The API-export path has no dependency rows.
    """
    if is_api_export:
        # Endpoint config (base URL, poll cadence, timeouts) comes from
        # MavisApiConfig's defaults — the extractor builds it internally.
        extractor = MavisApiExtractor(
            spark,
            config_mgr,
            audit_table = AUDIT_TABLE,
            environment = environment,
        )

        def run_one(task: MavisIngestionTaskConfig):
            """Run one Mavis export task. ``MavisApiExtractor.run`` owns audit +
            child-config Status + logging, just like ``IngestionOrchestrator.run``
            for the connector path."""
            extractor.run(task, source_sys, pipeline_name, job_context, config_master_id)
            return make_task_result(config_id=task.config_id, status=AUDIT_STATUS_SUCCESS)
    else:
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

        def run_one(task: IngestionTaskConfig):
            """
            Run a single ingestion task — works for RDBMS, NoSQL, and S3.

            Retries happen inside ``IngestionOrchestrator.run``, scoped only to
            the source connection pull (``connector.extract``), using the source
            system's ``retry_count`` / ``retry_interval`` from
            ``config_source_system``. Writing / transform steps are not retried —
            a failure there fails the task outright.
            """
            logger.info(f"Processing table {task.source_object_name}")
            return orchestrator.run(
                source_sys          = source_sys,
                ingest_obj          = task,
                config_master_id    = config_master_id,   # routing table ID from widget
                landing_volume_path = resolved_landing_path,
                trigger_id          = trigger_id,
                job_context         = job_context,
                sink_batch_started_date = batch_start_date,
            )

    if is_api_export:
        # Mavis exports are independent — flat fan-out, one thread per task, no
        # batch_id/priority grouping and no dependency job to close.
        return execute_parallel(tasks, run_one)

    results = execute_batches(tasks, run_one)
    orchestrator.dependency.complete_job(job_run_id)
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run

# COMMAND ----------

job_context = build_job_context(dbutils, job_run_id, job_id)

print(f"pipeline_name from widget: '{pipeline_name}'")

# config_mgr is needed regardless of mode — IngestionOrchestrator uses it later
# for Silver_Last_Sink_Date bookkeeping (see orchestrator.run()), even in Job
# mode where task discovery itself is skipped (tasks already came from taskValues).
config_mgr = ConfigManager(
    spark,
    source_system_table = SOURCE_SYSTEM_TABLE,
    config_master_table = CONFIG_MASTER_TABLE,
)

source_sys, tasks, batch_start_date, run_mode = resolve_tasks(
    dbutils,
    config_mgr,
    config_master_id = config_master_id,
    source_system_id = source_system_id,
    pipeline_name    = pipeline_name,
    batch_start_date = batch_start_date,
)

# Configure S3/Volume logging dynamically
resolved_landing_path = source_sys.landing_volume_path
if resolved_landing_path:
    s3_log_path = f"{resolved_landing_path.rstrip('/')}/logs/{pipeline_name}_{job_run_id}.log"
    configure_s3_logging(s3_log_path, dbutils=dbutils)

logger.info(f"Pipeline started for source: {source_sys.source_name} ({source_sys.source_type})")

if not tasks:
    dbutils.notebook.exit("No active ingestion tasks found for this pipeline.")

# ── Route: connector/orchestrator path vs API-export path ──────────────────────
# source_name = 'LSQ_Mavis' → tasks are MavisIngestionTaskConfig and the export
# flow (MavisApiExtractor) runs instead of IngestionOrchestrator. Everything
# downstream — fan-out, summary, exit — is shared.
_is_api_export = (
    (source_sys.source_name or "").strip().upper() == MAVIS_SOURCE_NAME.upper()
)

if _is_api_export and not all(isinstance(t, MavisIngestionTaskConfig) for t in tasks):
    raise RuntimeError(
        "Source resolves to LSQ_Mavis but the tasks are not "
        "MavisIngestionTaskConfig. Run this notebook standalone — Job-mode "
        "taskValues deserializes rows as IngestionTaskConfig."
    )

logger.info(
    f"Mode: {run_mode} x {'API_EXPORT' if _is_api_export else 'CONNECTOR'}"
)

# trigger_id also set to rootRunId for traceability in audit trigger_id column
trigger_id = job_run_id
job_context["trigger_id"] = trigger_id

results = run_pipeline(
    tasks,
    source_sys,
    is_api_export           = _is_api_export,
    spark                   = spark,
    dbutils                 = dbutils,
    config_mgr              = config_mgr,
    job_context             = job_context,
    job_run_id              = job_run_id,
    config_master_id        = config_master_id,
    environment             = environment,
    pipeline_name           = pipeline_name,
    resolved_landing_path   = resolved_landing_path,
    trigger_id              = trigger_id,
    batch_start_date        = batch_start_date,
    silver_notebook_path    = silver_notebook_path,
    silver_notebook_timeout = silver_notebook_timeout,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Results summary & exit

# COMMAND ----------

silver_results = [r["silver_result"] for r in results if r.get("silver_result")]

print_results_summary(results, silver_results)

succeeded = [r for r in results if r["status"] == AUDIT_STATUS_SUCCESS]
skipped   = [r for r in results if r["status"] == AUDIT_STATUS_SKIPPED]
failed    = [r for r in results if r["status"] == AUDIT_STATUS_FAILED]
silver_failed = [r for r in silver_results if r["status"] == "FAILED"]

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
dbutils.notebook.exit(
    f"SUCCESS: {len(succeeded)}/{len(results)} objects ingested "
    f"({len(skipped)} skipped — 0 rows in source)."
)

# COMMAND ----------
