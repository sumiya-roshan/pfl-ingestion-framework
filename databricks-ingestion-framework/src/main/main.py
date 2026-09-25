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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

sys.path.append("..")

from ingestion.lsq_mavis.mavis_api_extractor import MavisApiExtractor
from ingestion.utils.config_manager import (
    AUDIT_STATUS_FAILED,
    AUDIT_STATUS_SKIPPED,
    AUDIT_STATUS_SUCCESS,
    DEFAULT_CATALOG,
    MAVIS_SOURCE_NAME,
    ConfigManager,
    IngestionTaskConfig,
    LentraIngestionTaskConfig,
    MavisIngestionTaskConfig,
    SourceSystemConfig,
    build_table_refs,
    get_pipeline_notification_recipients,
)
from ingestion.utils.email_notifier import GraphMailNotifier
from ingestion.utils.logger import _upload_on_exit, configure_s3_logging, get_logger
from ingestion.utils.orchestrator import IngestionOrchestrator

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("config_master_id",    "",               "Config Master ID (int — routes to correct child config table)")
dbutils.widgets.text("source_system_id",    "",               "Source System ID (int — fetches credentials + source_name)")
dbutils.widgets.text("source_name",         "",               "Lentra only: source name directly (matches the config table's Source_Name exactly) — alternative to source_system_id")
dbutils.widgets.text("pipeline_name",       "",               "Pipeline Name (required for RDBMS/NoSQL/S3, not Lentra)")
dbutils.widgets.text("job_run_id",          "",               "Job Run ID (required) — set to {{job.run_id}} in job config")
dbutils.widgets.text("environment",         "dev",            "Environment: dev | uat | prod")
dbutils.widgets.text("catalog_name",        DEFAULT_CATALOG,  "Unity Catalog name for admin/config tables — changes per environment")
dbutils.widgets.text("batch_start_date",    "1",              "Batch Start Date")
dbutils.widgets.text("source_to_raw_notebook_path", "",       "Workspace path to src/raw/source_to_raw (required for RDBMS/NoSQL/S3)")
dbutils.widgets.text("source_to_raw_notebook_timeout", "3600", "Max seconds to wait for each Source→Raw notebook run")
dbutils.widgets.text("silver_notebook_timeout", "3600",       "Max seconds to wait for each Silver notebook run")
dbutils.widgets.text("lentra_load_notebook_path", "",       "Lentra only: workspace path to the client-provided load_raw_to_silver notebook")
dbutils.widgets.text("lentra_raw_sa_name",          "",       "Lentra only: base raw landing path (S3 URI / Volume path)")
dbutils.widgets.text("lentra_notebook_timeout",     "3600",   "Lentra only: max seconds to wait for each notebook.run() call (load and classify)")
dbutils.widgets.text("lentra_classify_notebook_path", "",      "Lentra only: workspace path to the classify notebook (only used for the 2 DMS-master sources)")

# COMMAND ----------

config_master_id_raw = dbutils.widgets.get("config_master_id") or None
source_system_id_raw = dbutils.widgets.get("source_system_id") or None
source_name   = dbutils.widgets.get("source_name") or None
if not config_master_id_raw:
    dbutils.notebook.exit("Error: config_master_id is required.")
if not source_system_id_raw and not source_name:
    dbutils.notebook.exit("Error: either source_system_id or source_name is required.")

config_master_id     = int(config_master_id_raw)
source_system_id     = int(source_system_id_raw) if source_system_id_raw else None
pipeline_name        = dbutils.widgets.get("pipeline_name")        or None
try:
    job_id           = dbutils.widgets.get("job_id")           or None
except Exception:
    job_id           = None
job_run_id           = dbutils.widgets.get("job_run_id")           or None

# pipeline_name is required for RDBMS/NoSQL/S3 (filters get_active_tasks) but
# NOT for Lentra, which has no pipeline_name-based filtering — checked again
# below, after task discovery tells us which shape this run is.
if not job_run_id:
    dbutils.notebook.exit("Error: job_run_id widget is required and cannot be empty.")

environment          = dbutils.widgets.get("environment")          or "dev"
catalog_name         = dbutils.widgets.get("catalog_name")         or DEFAULT_CATALOG
batch_start_date     = dbutils.widgets.get("batch_start_date")     or "1"
logger               = get_logger(environment=environment)

# Build fully-qualified table names from the catalog_name job parameter.
# Schema/table names are fixed across environments — only the catalog changes.
_refs                = build_table_refs(catalog_name)
SOURCE_SYSTEM_TABLE          = _refs["source_system_table"]
CONFIG_MASTER_TABLE          = _refs["config_master_table"]
AUDIT_TABLE                  = _refs["audit_table"]
DEPENDENCY_TABLE             = _refs["dependency_table"]
PIPELINE_MASTER_CONFIG_TABLE = _refs["pipeline_master_config_table"]

silver_notebook_timeout = int(dbutils.widgets.get("silver_notebook_timeout") or "3600")
source_to_raw_notebook_path    = dbutils.widgets.get("source_to_raw_notebook_path") or None
source_to_raw_notebook_timeout = int(dbutils.widgets.get("source_to_raw_notebook_timeout") or "3600")


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
    def get_tag_value(tag_name):
        try:
            return context.tags().get(tag_name).get()
        except Exception:
            return None

    # Prefer browserHostName (the dbc-xxx workspace URL) over apiUrl
    host_name = get_tag_value("browserHostName")
    if host_name:
        databricks_url = f"https://{host_name}"
    else:
        databricks_url = get_context_value("apiUrl")

    try:
        job_id = dbutils.widgets.get("job_id")
    except Exception:
        job_id = None
        
    if databricks_url and job_id:
        # Check if URL already has workspace id (o=) or we can just append #job/
        databricks_url = f"{databricks_url}/#job/{job_id}"
    else:
        databricks_url = None

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
    payload = json.loads(payload_str)
    source_sys = SourceSystemConfig.from_dict(payload["source_sys"])

    # same payload shape for every source — only the task class differs
    is_mavis = (source_sys.source_name or "").strip().upper() == MAVIS_SOURCE_NAME.upper()
    is_lentra = bool(payload.get("is_lentra"))
    if is_mavis:
        task_cls = MavisIngestionTaskConfig
    elif is_lentra:
        task_cls = LentraIngestionTaskConfig
    else:
        task_cls = IngestionTaskConfig
    tasks = [task_cls.from_dict(t) for t in payload["tasks"]]
    batch_start_date = payload.get("batch_start_date") or batch_start_date
else:
    # ── Standalone mode: query config tables directly ──────────────────────
    print("[Tasks] taskValues not available — querying config tables directly (standalone mode).")
    source_sys, tasks = config_mgr.get_active_tasks(
        config_master_id = config_master_id,
        source_system_id = source_system_id,
        source_name      = source_name,
        pipeline_name    = pipeline_name,
        batch_start_date = batch_start_date,
    )
    # Empty tasks -> notebook exits right below anyway, so it's fine that
    # is_lentra/is_mavis can't be determined by isinstance() in that case.
    is_mavis = bool(tasks) and isinstance(tasks[0], MavisIngestionTaskConfig)
    is_lentra = bool(tasks) and isinstance(tasks[0], LentraIngestionTaskConfig)

if not is_lentra and not pipeline_name:
    dbutils.notebook.exit("Error: pipeline_name widget is required and cannot be empty.")

# batch_start_date arrives as a string — get_tasks.py stamps it as
# '%Y-%m-%d %H:%M:%S.%f', and the widget default is the sentinel '1'. The
# orchestrator needs a real datetime (landing-file path + business_date), so
# parse it once here.
if isinstance(batch_start_date, str) and batch_start_date.strip() not in ("", "1"):
    batch_start_date = datetime.fromisoformat(
        batch_start_date.strip().replace("T", " ")
    )
else:
    batch_start_date = datetime.now(timezone.utc)
print(type(batch_start_date),batch_start_date)
print(f"Resolved source : {source_sys.source_name} ({source_sys.source_type})")
print(f"Active tasks    : {len(tasks)}")

silver_notebook_path = source_sys.silver_notebook_path

# Route by source — one self-contained branch per family. Defined here so both
# Job and standalone modes have it. IngestionOrchestrator is purely RDBMS
# (JDBC + federated); SFTP / NoSQL / S3 get their own branch when built.
_RDBMS_SOURCE_TYPES = {
    "POSTGRES", "POSTGRESQL", "PG", "MYSQL", "ORACLE", "MSSQL", "SQLSERVER",
    "POSTGRES_FEDERATED",
}
_source_type = (source_sys.source_type or "").strip().upper()
is_mavis = (source_sys.source_name or "").strip().upper() == MAVIS_SOURCE_NAME.upper()
is_rdbms = _source_type in _RDBMS_SOURCE_TYPES

# Configure S3/Volume logging dynamically
resolved_landing_path = source_sys.raw_bucket_path
if resolved_landing_path:
    s3_log_path = f"{resolved_landing_path.rstrip('/')}/logs/{pipeline_name or source_sys.source_name}_{job_run_id}.log"
    configure_s3_logging(s3_log_path, dbutils=dbutils)

logger.info(f"Pipeline started for source: {source_sys.source_name} ({source_sys.source_type})")

if not tasks:
    dbutils.notebook.exit("No active ingestion tasks found for this pipeline.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lentra — separate orchestrator
# MAGIC
# MAGIC Lentra tasks (`LentraIngestionTaskConfig`) don't go through
# MAGIC `IngestionOrchestrator` — the actual connection/processing logic lives in
# MAGIC a client-provided notebook, not a Python connector this framework owns.
# MAGIC `LentraLoader` triggers that notebook per task and writes `Status` back.
# MAGIC This block short-circuits via `dbutils.notebook.exit()`, so none of the
# MAGIC `IngestionOrchestrator` / Silver / dependency-tracking code below ever
# MAGIC runs for Lentra tasks — the rest of this notebook is untouched by this
# MAGIC branch.

# COMMAND ----------

if is_lentra:
    from ingestion.utils.lentra_loader import DMS_API_RESPONSE_FILES_SOURCES, LentraLoader

    lentra_load_notebook_path     = dbutils.widgets.get("lentra_load_notebook_path") or None
    lentra_raw_sa_name            = dbutils.widgets.get("lentra_raw_sa_name") or None
    lentra_notebook_timeout       = int(dbutils.widgets.get("lentra_notebook_timeout") or "3600")
    lentra_classify_notebook_path = dbutils.widgets.get("lentra_classify_notebook_path") or None

    if not lentra_load_notebook_path:
        dbutils.notebook.exit("Error: lentra_load_notebook_path widget is required for Lentra sources.")
    if not lentra_raw_sa_name:
        dbutils.notebook.exit("Error: lentra_raw_sa_name widget is required for Lentra sources.")

    lentra_loader = LentraLoader(
        spark,
        dbutils,
        config_mgr,
        load_notebook_path      = lentra_load_notebook_path,
        raw_sa_name             = lentra_raw_sa_name,
        run_id                  = job_run_id,
        notebook_timeout        = lentra_notebook_timeout,
        classify_notebook_path  = lentra_classify_notebook_path,
        audit_table             = AUDIT_TABLE,
        config_master_id        = config_master_id,
        job_context             = job_context,
    )

    # Publish the same values the client notebook receives as taskValues, so
    # the downstream api_extract/dms_extract Run Job tasks (wired directly in
    # the job graph, gated by a Condition task on dms_master below) can read
    # them via {{tasks.<this task's name>.values.<key>}} without their own
    # config-table lookup. Lentra always resolves to exactly one task per
    # source, so there's no ambiguity about which row's values these are.
    lentra_published_values = LentraLoader.build_params(
        tasks[0], lentra_raw_sa_name, job_run_id
    )
    for _key, _value in lentra_published_values.items():
        try:
            dbutils.jobs.taskValues.set(key=_key, value=_value)
        except Exception as _exc:
            print(f"[Lentra] taskValues not available (standalone mode) for {_key}: {_exc}")

    # dms_master — lets a downstream Condition task in the job graph decide
    # whether to branch into api_extract/dms_extract (visible Run Job tasks,
    # not fired from Python — see lentra_loader.py's module docstring for
    # why). Published as lowercase "true"/"false" strings, matching what a
    # Condition task's EQUAL_TO comparison expects. Computed here regardless
    # of this task's own success/failure — the Condition task's own
    # "Depends on: run_source_ingestion, Run if: All succeeded" is what
    # actually gates on whether the load+classify notebooks succeeded.
    try:
        dbutils.jobs.taskValues.set(
            key="dms_master",
            value=str(tasks[0].source_name in DMS_API_RESPONSE_FILES_SOURCES).lower(),
        )
    except Exception as _exc:
        print(f"[Lentra] taskValues not available (standalone mode) for dms_master: {_exc}")

    # Lentra always resolves to exactly one active row per source_name (see
    # ConfigManager.get_active_tasks), so there's no real parallelism to gain
    # here — a plain sequential loop, not a ThreadPoolExecutor.
    print(f"[Lentra] Running {len(tasks)} task(s)")

    lentra_results = []
    for task in tasks:
        try:
            lentra_results.append(lentra_loader.run(task))
        except Exception as exc:  # defensive — LentraLoader.run shouldn't raise
            print(f"[Lentra] config_id={task.config_id} FAILED (worker error): {exc}")
            lentra_results.append({
                "config_id": task.config_id,
                "report_name": task.report_name,
                "status": AUDIT_STATUS_FAILED,
                "exit_value": None,
                "error": str(exc),
            })

    lentra_succeeded = [r for r in lentra_results if r["status"] == AUDIT_STATUS_SUCCESS]
    lentra_failed = [r for r in lentra_results if r["status"] == AUDIT_STATUS_FAILED]

    print(f"\n{'=' * 75}")
    print(f"{'CONF ID':>8}  {'STATUS':<10}  REPORT")
    print(f"{'=' * 75}")
    for r in sorted(lentra_results, key=lambda x: x["config_id"]):
        icon = "✅" if r["status"] == AUDIT_STATUS_SUCCESS else "❌"
        print(f"{r['config_id']:>8}  {icon} {r['status']:<8}  {r.get('report_name', '')}")
    print(f"{'=' * 75}")
    print(
        f"Total: {len(lentra_results)} | ✅ Succeeded: {len(lentra_succeeded)} | "
        f"❌ Failed: {len(lentra_failed)}\n"
    )

    # ── Pipeline-level (batch summary) notification ─────────────────────────
    # One summary email for this run, separate from any per-row notification.
    # Recipients come from tb_pipeline_master_config (Success_Recipient /
    # Failure_Recipient), looked up by this task's own Pipeline_Name column —
    # Lentra always resolves to exactly one task, so tasks[0].pipeline_name is
    # unambiguous.
    lentra_pipeline_name = tasks[0].pipeline_name
    lentra_success_recipients, lentra_failure_recipients = get_pipeline_notification_recipients(
        spark, lentra_pipeline_name
    )
    lentra_notifier = GraphMailNotifier(dbutils=dbutils, logger=logger)
    lentra_summary_body = (
        f"Pipeline: {lentra_pipeline_name}\n"
        f"Source: {tasks[0].source_name}\n"
        f"Job Run ID: {job_run_id}\n\n"
        f"Total: {len(lentra_results)} | Succeeded: {len(lentra_succeeded)} | "
        f"Failed: {len(lentra_failed)}\n"
    )
    if lentra_failed:
        lentra_summary_body += "\nFailed Config IDs: " + ", ".join(
            str(r["config_id"]) for r in lentra_failed
        )
        lentra_notifier.send_email(
            subject=f"[FAILURE] PIPELINE — {lentra_pipeline_name} / {tasks[0].source_name} (job_run_id={job_run_id})",
            body=lentra_summary_body,
            recipients=lentra_failure_recipients,
        )
    else:
        lentra_notifier.send_email(
            subject=f"[SUCCESS] PIPELINE — {lentra_pipeline_name} / {tasks[0].source_name} (job_run_id={job_run_id})",
            body=lentra_summary_body,
            recipients=lentra_success_recipients,
        )

    if lentra_failed:
        failed_ids = [r["config_id"] for r in lentra_failed]
        logger.critical(
            f"[Lentra] Pipeline cannot continue — {len(lentra_failed)} of "
            f"{len(lentra_results)} task(s) FAILED. Failed Config IDs: {failed_ids}"
        )
        # Included directly in the exit message (not just logged) because
        # plain print()/logger output doesn't render in the Jobs UI's
        # notebook-preview ("Graph" tab) view for a job run — only rich
        # outputs and the final dbutils.notebook.exit() string do.
        failure_details = "; ".join(
            f"config_id={r['config_id']} ({r.get('report_name')}): {r.get('error')}"
            for r in lentra_failed
        )
        dbutils.notebook.exit(
            f"FAILED: {len(lentra_succeeded)}/{len(lentra_results)} Lentra task(s) completed "
            f"({len(lentra_failed)} failed). {failure_details}"
        )

    dbutils.notebook.exit(
        f"SUCCESS: {len(lentra_succeeded)}/{len(lentra_results)} Lentra task(s) completed "
        f"({len(lentra_failed)} failed)."
    )

# NOTE: max_workers is now derived dynamically from the distinct batch_id count
# for this pipeline in the execution section below.
# COMMAND ----------
if is_mavis:

    # Endpoint base URL (prod_api) comes from each task's own config row; retry /
    # interval / timeout come from config_source_system. The parallel fan-out
    # (one worker per distinct config_id), the audit row, child-config Status and
    # the Silver notebook trigger are all owned by MavisApiExtractor.
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

    succeeded = [c for c, (s, _) in results.items() if s == "SUCCESS"]
    failed    = [c for c, (s, _) in results.items() if s == "FAILED"]
    print(f"\n{'='*75}")
    print(f"{'CONF ID':>8}  STATUS   DETAIL")
    print(f"{'='*75}")
    for cid, (status, detail) in sorted(results.items()):
        icon = "✅" if status == "SUCCESS" else "❌"
        print(f"{cid:>8}  {icon} {status:<7}  {str(detail)[:60]}")
    print(f"{'='*75}")
    print(f"Total: {len(results)} | ✅ {len(succeeded)} | ❌ {len(failed)}\n")

    if failed:
        logger.critical(
            f"API-export — {len(failed)} of {len(results)} task(s) FAILED. "
            f"Failed Config IDs: {failed}. Check the audit table and logs above."
        )

    _upload_on_exit()
    if failed:
        raise RuntimeError(
            f"{len(failed)} of {len(results)} API-export task(s) failed: {failed}"
        )
    dbutils.notebook.exit(
        f"SUCCESS: {len(succeeded)}/{len(results)} API-export task(s) completed "
        f"for {source_sys.source_name}."
    )

# ─────────────────────────────────────────────────────────────────────────────
# Connector path — RDBMS only (JDBC + federated). SFTP / NoSQL / S3 get their
# own elif when built. NOTE: the whole if/elif/else is ONE notebook cell — no
# `# COMMAND ----------` between branches or the elif/else breaks.
# ─────────────────────────────────────────────────────────────────────────────
elif is_rdbms:
    # trigger_id also set to rootRunId for traceability in audit trigger_id column
    trigger_id = job_run_id
    job_context["trigger_id"] = trigger_id

    orchestrator = IngestionOrchestrator(
        spark,
        dbutils,
        audit_table                    = AUDIT_TABLE,
        dependency_table               = DEPENDENCY_TABLE,
        pipeline_name                  = pipeline_name,
        environment                    = environment,
        source_to_raw_notebook_path    = source_to_raw_notebook_path,
        source_to_raw_notebook_timeout = source_to_raw_notebook_timeout,
        silver_notebook_path           = silver_notebook_path,
        silver_notebook_timeout        = silver_notebook_timeout,
        config_mgr                     = config_mgr,
    )

    def run_one(task: IngestionTaskConfig) -> dict:
        """
        Run a single ingestion task — works for RDBMS, NoSQL, and S3.

        Retries happen inside IngestionOrchestrator.run(), scoped only to the
        source connection pull (connector.extract), using the source system's
        retry_count/retry_interval from config_source_system. Writing/transform
        steps are not retried — a failure there fails the task outright.
        """
        logger.info(f"Processing table {task.source_object_name}")
        return orchestrator.run(
            source_sys          = source_sys,
            ingest_obj          = task,
            config_master_id    = config_master_id,   # ← routing table ID from widget
            raw_bucket_path = resolved_landing_path,
            trigger_id          = trigger_id,
            job_context          = job_context,
            sink_batch_started_date = batch_start_date,
        )

    results = []

    # ── Batch-level parallelism ────────────────────────────────────────────
    # batch_id → PARALLEL (one thread per distinct batch); priority → SEQUENTIAL
    # within a batch (ascending). max_workers = distinct batch_id count, derived
    # here — NOT hardcoded, NOT from a widget/config. A batch's tables run
    # one-by-one inside that batch's single thread.
    batches = {}
    for task in sorted(tasks, key=lambda t: t.priority):
        batches.setdefault(task.batch_id, []).append(task)

    max_workers = len(batches)

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

    # ── Close the dependency job ──────────────────────────────────────────
    # pipeline_end_time isn't known until every table has finished — bulk-stamp
    # it (and the derived dependency_resolve_time) onto every
    # dependency_master_config row for this job_run_id now that the fan-out is done.
    orchestrator.dependency.complete_job(job_run_id)

    # ── Results summary ──────────────────────────────────────────────────
    # Silver runs coupled inside each orchestrator.run() call, so results
    # already carry it under "silver_result" — no separate wait step.
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

    # ── Pipeline-level (batch summary) notification ─────────────────────────
    # One summary email for this whole run — separate from each table's own
    # per-row email (orchestrator.py, driven by that row's `recipients`
    # column). Recipients come from tb_pipeline_master_config
    # (Success_Recipient / Failure_Recipient), looked up by pipeline_name.
    pipeline_success_recipients, pipeline_failure_recipients = get_pipeline_notification_recipients(
        spark, pipeline_name
    )
    pipeline_summary_body = (
        f"Pipeline: {pipeline_name}\n"
        f"Source: {source_sys.source_name} ({source_sys.source_type})\n"
        f"Job Run ID: {job_run_id}\n\n"
        f"Ingestion — Total: {len(results)} | Succeeded: {len(succeeded)} | "
        f"Skipped: {len(skipped)} | Failed: {len(failed)}\n"
        f"Silver     — Total: {len(silver_results)} | "
        f"Succeeded: {len(silver_results) - len(silver_failed)} | Failed: {len(silver_failed)}\n"
    )
    if failed or silver_failed:
        pipeline_summary_body += (
            "\nFailed Config IDs: " + ", ".join(str(r["config_id"]) for r in failed)
            + "\nSilver-failed Config IDs: " + ", ".join(str(r["config_id"]) for r in silver_failed)
        )
        orchestrator.notifier.send_email(
            subject=f"[FAILURE] PIPELINE — {pipeline_name} (job_run_id={job_run_id})",
            body=pipeline_summary_body,
            recipients=pipeline_failure_recipients,
        )
    else:
        orchestrator.notifier.send_email(
            subject=f"[SUCCESS] PIPELINE — {pipeline_name} (job_run_id={job_run_id})",
            body=pipeline_summary_body,
            recipients=pipeline_success_recipients,
        )

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

else:
    # No branch for this source family yet (SFTP / NoSQL / S3 get their own
    # elif when built). Fail loudly rather than silently doing nothing.
    _upload_on_exit()
    dbutils.notebook.exit(
        f"No handler for source '{source_sys.source_name}' "
        f"(source_type={source_sys.source_type!r}, source_system_id={source_system_id}). "
        f"Known: LSQ_Mavis (API) or RDBMS types {sorted(_RDBMS_SOURCE_TYPES)}."
    )

# COMMAND ----------

