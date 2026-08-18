# Databricks notebook source
# MAGIC %md
# MAGIC # Source Lookup — Pre-Ingestion Row-Count Check
# MAGIC
# MAGIC Runs as **Task 1** before `main.py` (Task 2) in the ingestion job.
# MAGIC
# MAGIC **What it does:**
# MAGIC 1. Loads all active ingestion tasks for the given `config_master_id` + `source_system_id`
# MAGIC    (same as `main.py` — uses `ConfigManager` + filters by `pipeline_name`).
# MAGIC 2. Reads **one row** from `pipeline_lookup_config` for this pipeline to get the
# MAGIC    `lookup_query_template` (e.g. `SELECT COUNT(*) FROM {schema}.{table}`).
# MAGIC 3. Applies the template to every table (substituting `{schema}` / `{table}`) and runs
# MAGIC    all COUNT queries concurrently (ThreadPoolExecutor).
# MAGIC 4. For tables with **0 rows** → inserts a `SKIPPED` audit record.
# MAGIC 5. Publishes the comma-separated list of **non-zero config IDs** via
# MAGIC    `dbutils.jobs.taskValues` so `main.py` (Task 2) can filter its task list.
# MAGIC
# MAGIC **Fail-safe:** If a lookup query errors, the table is included (count treated as > 0).
# MAGIC **Standalone mode:** When run outside a job, taskValues.set() is skipped gracefully.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install paramiko boto3 --quiet
# MAGIC

# COMMAND ----------

import sys
sys.path.append("..")

from ingestion.utils.config_manager import (
    ConfigManager,
    SOURCE_SYSTEM_TABLE,
    CONFIG_MASTER_TABLE,
    AUDIT_TABLE,
    AUDIT_STATUS_SKIPPED,
)
from ingestion.utils.audit import AuditLogger
from ingestion.utils.logger import get_logger
from ingestion.utils.secrets import SecretResolver
from lookup.lookup_executor import LookupExecutor

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

PIPELINE_LOOKUP_CONFIG_TABLE_DEFAULT = "migration_x_catalog.pfl_x_schema.pipeline_lookup_config"

dbutils.widgets.text("config_master_id",             "",                                   "Config Master ID")
dbutils.widgets.text("source_system_id",              "",                                   "Source System ID")
dbutils.widgets.text("target_catalog",                "hive_metastore",                     "Target Catalog")
dbutils.widgets.text("pipeline_name",                 "",                                   "Pipeline Name")
dbutils.widgets.text("job_run_id",                 "",                                  "Job Run ID — set to {{job.run_id}}")
dbutils.widgets.text("job_id",                     "",                                  "Job ID — set to {{job.id}}")
dbutils.widgets.text("environment",                "dev",                               "Environment: dev | uat | prod")
dbutils.widgets.text("audit_table",                AUDIT_TABLE,                         "Audit Table (override)")
dbutils.widgets.text("max_workers",                "4",                                 "Max parallel lookup workers")
dbutils.widgets.text("pipeline_lookup_config_table", PIPELINE_LOOKUP_CONFIG_TABLE_DEFAULT, "pipeline_lookup_config FQN")

# COMMAND ----------

# ── Validate required widgets ─────────────────────────────────────────────────

config_master_id_raw = dbutils.widgets.get("config_master_id") or None
source_system_id_raw = dbutils.widgets.get("source_system_id") or None

if not config_master_id_raw or not source_system_id_raw:
    dbutils.notebook.exit("Error: config_master_id and source_system_id are required.")

config_master_id  = int(config_master_id_raw)
source_system_id  = int(source_system_id_raw)
target_catalog    = dbutils.widgets.get("target_catalog")   or "hive_metastore"
pipeline_name     = dbutils.widgets.get("pipeline_name")    or None
try:
    job_run_id    = dbutils.widgets.get("job_run_id")       or "MANUAL"
except Exception:
    job_run_id    = "MANUAL"

try:
    job_id        = dbutils.widgets.get("job_id")           or "MANUAL"
except Exception:
    job_id        = "MANUAL"

environment       = dbutils.widgets.get("environment")      or "dev"
audit_table       = dbutils.widgets.get("audit_table")      or AUDIT_TABLE
max_workers       = int(dbutils.widgets.get("max_workers")  or "4")
lookup_cfg_table  = (
    dbutils.widgets.get("pipeline_lookup_config_table")
    or PIPELINE_LOOKUP_CONFIG_TABLE_DEFAULT
)

if not pipeline_name:
    dbutils.notebook.exit("Error: pipeline_name widget is required.")

print(f"config_master_id : {config_master_id}")
print(f"source_system_id : {source_system_id}")
print(f"pipeline_name    : {pipeline_name}")
print(f"job_run_id       : {job_run_id}")
print(f"environment      : {environment}")
print(f"max_workers      : {max_workers}")
print(f"lookup_cfg_table : {lookup_cfg_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Active Ingestion Tasks (same as main.py)

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
)

print(f"\nResolved source : {source_sys.source_name} ({source_sys.source_type})")
print(f"Active tasks    : {len(tasks)}")

if not tasks:
    print("No active tasks — publishing empty active_config_ids and exiting.")
    try:
        dbutils.jobs.taskValues.set(key="active_config_ids", value="")
    except Exception:
        pass
    dbutils.notebook.exit("No active ingestion tasks found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Pipeline-Level Lookup Query Template from pipeline_lookup_config
# MAGIC
# MAGIC One row per pipeline. The template is applied to every table in this run.
# MAGIC If no row is found, a default template is auto-generated per table:
# MAGIC `SELECT COUNT(*) FROM {schema}.{table}`

# COMMAND ----------

lookup_query_template: str = None   # None → auto-generate per table

try:
    lookup_cfg_rows = (
        spark.table(lookup_cfg_table)
        .filter(
            f"pipeline_name = '{pipeline_name}' "
            f"AND config_master_id = {config_master_id} "
            f"AND is_active = true"
        )
        .select("lookup_query_template")
        .limit(1)      # one row per pipeline (enforced by UNIQUE constraint)
        .collect()
    )

    if lookup_cfg_rows:
        lookup_query_template = lookup_cfg_rows[0]["lookup_query_template"]  # may be NULL
        print(
            f"\nPipeline lookup template loaded: "
            f"{(lookup_query_template or 'NULL (auto-generate per table)')!r}"
        )
    else:
        print(
            f"\n[INFO] No row in {lookup_cfg_table} for pipeline='{pipeline_name}' "
            f"config_master_id={config_master_id}. "
            f"Will auto-generate SELECT COUNT(*) per table."
        )

except Exception as e:
    print(
        f"\n[WARNING] Could not load pipeline_lookup_config ({e}). "
        f"All lookup queries will be auto-generated."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run Lookup Queries — Template Applied Per Table (Threaded)

# COMMAND ----------

logger  = get_logger(environment=environment)
secrets = SecretResolver(dbutils)
audit   = AuditLogger(spark, audit_table=audit_table)

# LookupExecutor receives the pipeline-level template and applies it per table
executor = LookupExecutor(
    spark                 = spark,
    secrets               = secrets,
    logger                = logger,
    lookup_query_template = lookup_query_template,   # None → auto-gen
)

lookup_results = executor.run_all(
    source_sys  = source_sys,
    tasks       = tasks,
    max_workers = max_workers,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Process Results — Audit Skipped Tables + Build Active ID List

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
job_context["job_run_id"] = job_run_id

active_config_ids = []   # config_ids with count > 0 (passed to main.py)
skipped_count     = 0
error_count       = 0

for result in lookup_results:
    config_id    = result["config_id"]
    object_name  = result["source_object_name"]
    count        = result["count"]
    included     = result["included"]
    error        = result["error"]
    resolved_qry = result["resolved_query"]

    if error and count == -1:
        # Lookup errored — fail-safe, include the table
        error_count += 1
        logger.warning(
            f"[Lookup] config_id={config_id} ({object_name}) — "
            f"lookup error: {error}. Table included (fail-safe)."
        )
        active_config_ids.append(config_id)

    elif count == 0:
        # Zero rows — log SKIPPED to audit, exclude from ingestion
        skipped_count += 1
        logger.info(
            f"[Lookup] config_id={config_id} ({object_name}) — "
            f"0 rows in source. Inserting SKIPPED audit record."
        )
        task_obj = next((t for t in tasks if t.config_id == config_id), None)
        if task_obj:
            try:
                audit.log_skipped_row(
                    task             = task_obj,
                    source_sys       = source_sys,
                    job_context      = job_context,
                    pipeline_name    = pipeline_name,
                    config_master_id = config_master_id,
                    reason           = (
                        f"Source lookup returned 0 rows. "
                        f"Query: {resolved_qry}"
                    ),
                )
            except Exception as audit_exc:
                logger.error(
                    f"[Lookup] Failed to write SKIPPED audit row for "
                    f"config_id={config_id}: {audit_exc}"
                )
    else:
        # Has data — include in ingestion
        active_config_ids.append(config_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Print Summary

# COMMAND ----------

print(f"\n{'='*85}")
print(f"{'CONF ID':>8}  {'OBJECT':<30}  {'COUNT':>10}  {'STATUS':<12}  ERROR")
print(f"{'='*85}")

for result in sorted(lookup_results, key=lambda x: x["config_id"]):
    conf_id  = result["config_id"]
    obj_name = result["source_object_name"][:30]
    count    = result["count"]
    error    = (result.get("error") or "")[:35]

    if count == -1:
        status    = "ERROR"
        count_str = "ERROR"
    elif result["included"]:
        status    = "INCLUDED"
        count_str = str(count)
    else:
        status    = "SKIPPED"
        count_str = "0"

    print(f"{conf_id:>8}  {obj_name:<30}  {count_str:>10}  {status:<12}  {error}")

print(f"{'='*85}")
print(
    f"Total: {len(lookup_results)} | "
    f" Included: {len(active_config_ids)} | "
    f" Skipped: {skipped_count} | "
    f" Errors (fail-safe included): {error_count}"
)
print(f"{'='*85}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Publish Active Config IDs to Task 2 (main.py) via taskValues

# COMMAND ----------

active_ids_str = ",".join(str(cid) for cid in sorted(active_config_ids))
print(f"Publishing active_config_ids → '{active_ids_str}'")

try:
    dbutils.jobs.taskValues.set(key="active_config_ids", value=active_ids_str)
    print("taskValues.set() succeeded.")
except Exception as e:
    # Running outside a job (standalone notebook run) — not an error.
    print(f"[INFO] taskValues not available (standalone mode): {e}")
    print("Lookup summary printed above. No task values were published.")

# COMMAND ----------

if not active_config_ids:
    dbutils.notebook.exit(
        f"Lookup complete — all {len(tasks)} table(s) had 0 rows. "
        f"Ingestion task will have no work to do."
    )

dbutils.notebook.exit(
    f"Lookup complete — {len(active_config_ids)}/{len(tasks)} table(s) have data. "
    f"Skipped: {skipped_count}. Errors (fail-safe included): {error_count}."
)

# COMMAND ----------
