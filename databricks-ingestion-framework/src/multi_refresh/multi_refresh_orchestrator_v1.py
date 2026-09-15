# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Multi-Refresh Orchestrator
# MAGIC
# MAGIC Replicates the ADF `PL_Multi_Refresh_Automation` pipeline inside Databricks.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys

sys.path.append("..")

import datetime
import json
import time

import pytz

from ingestion.utils.logger import get_logger
from multi_refresh.job_trigger import JobTrigger

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------


dbutils.widgets.text("environment", "dev", "Environment: dev | uat | prod")
dbutils.widgets.text("job_run_id", "", "Job Run ID - set to {{job.run_id}}")
dbutils.widgets.text("max_iterations", "200", "Safety: max loop iterations before exit")
dbutils.widgets.text("secret_scope", "", "Secret scope for Databricks PAT token")
dbutils.widgets.text(
    "secret_key_pat", "databricks-pat-token", "Secret key for Databricks PAT token"
)
dbutils.widgets.text("s3_log_path", "", "S3 Log Path (e.g. s3://bucket/logs/)")
dbutils.widgets.text(
    "silver_notebook_path", "", "Path to the eligibility (silver) notebook"
)
dbutils.widgets.text(
    "silver_notebook_timeout", "300", "Timeout in seconds for the silver notebook run"
)

# COMMAND ----------

environment = dbutils.widgets.get("environment") or "dev"
job_run_id = dbutils.widgets.get("job_run_id") or "MANUAL"
max_iterations = int(dbutils.widgets.get("max_iterations") or "200")
secret_scope = dbutils.widgets.get("secret_scope") or None
secret_key_pat = dbutils.widgets.get("secret_key_pat") or "databricks-pat-token"
s3_log_path = dbutils.widgets.get("s3_log_path") or None
silver_notebook_path = dbutils.widgets.get("silver_notebook_path") or None
silver_notebook_timeout = int(dbutils.widgets.get("silver_notebook_timeout") or "300")

if not silver_notebook_path:
    dbutils.notebook.exit("Error: silver_notebook_path widget is required.")

if not secret_scope:
    dbutils.notebook.exit(
        "Error: secret_scope widget is required (needed for REST API PAT token)."
    )



IST = pytz.timezone("Asia/Kolkata")
logger = get_logger(environment=environment)
if s3_log_path:
    from ingestion.utils.logger import configure_s3_logging

    configure_s3_logging(f"{s3_log_path.rstrip('/')}/multi_refresh_{job_run_id}.log")

print(f"admin_catalog_name : {admin_catalog_name}")
print(f"environment        : {environment}")
print(f"max_iterations     : {max_iterations}")

# Workspace URL and PAT token for job triggers
workspace_url = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
)
pat_token = dbutils.secrets.get(scope=secret_scope, key=secret_key_pat)
job_trigger = JobTrigger(workspace_url=workspace_url, token=pat_token)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Job Trigger Dispatcher

# COMMAND ----------


def trigger_eligible_jobs(eligible_pipelines: list, trigger_time_str: str) -> None:
    """
    Triggers one Databricks job per distinct pipeline_name in eligible_pipelines.
    eligible_pipelines is already fully filtered/deduplicated eligibility output
    from the silver notebook's JSON output — no Spark re-query happens here.
    """
    triggered_pipeline_names = set()
    for entry in eligible_pipelines:
        pipeline_name = entry.get("pipeline_name")
        source_name = entry.get("source_name")
        if not pipeline_name or pipeline_name in triggered_pipeline_names:
            continue
        triggered_pipeline_names.add(pipeline_name)
        try:
            run_id = job_trigger.run_now_by_name(
                job_name=pipeline_name,
                notebook_params={
                    "batch_start_date": trigger_time_str,
                    "pipeline_name": pipeline_name,
                    "source_name": source_name,
                },
            )
            logger.info(
                f"[MultiRefresh] Triggered job '{pipeline_name}' source_name={source_name} "
                f"-> run_id={run_id} batch_start_date={trigger_time_str}"
            )
        except Exception as exc:
            logger.error(
                f"[MultiRefresh] Failed to trigger job '{pipeline_name}' "
                f"source_name={source_name}: {exc}"
            )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Main Orchestrator Loop

# COMMAND ----------

iteration = 0
logger.info(f"[MultiRefresh] Orchestrator starting. max_iterations={max_iterations}")

while iteration < max_iterations:
    iteration += 1

    triggerTime = datetime.datetime.now(IST).replace(tzinfo=None)

    trigger_hhmm = triggerTime.strftime("%H:%M")
    trigger_date_str = triggerTime.strftime("%Y-%m-%d")
    trigger_time_str = triggerTime.strftime("%Y-%m-%d %H:%M:%S")

    logger.info(
        f"\n{'=' * 60}\n"
        f"[MultiRefresh] Iteration {iteration}/{max_iterations} | "
        f"triggerTime = {triggerTime} (IST)\n"
        f"{'=' * 60}"
    )

    try:
        raw_output = dbutils.notebook.run(
            silver_notebook_path,
            silver_notebook_timeout,
            {"triggerTime": trigger_time_str},
        )
    except Exception as exc:
        logger.error(f"[MultiRefresh] Silver notebook run failed: {exc}")
        time.sleep(silver_notebook_timeout)
        continue

    try:
        result = json.loads(raw_output)
    except (TypeError, ValueError) as exc:
        logger.error(
            f"[MultiRefresh] Could not parse silver notebook output as JSON: {exc}. "
            f"raw_output={raw_output!r}"
        )
        time.sleep(silver_notebook_timeout)
        continue

    eligible_pipelines = result.get("eligible_pipelines", [])
    is_completed = result.get("is_completed", 0)
    wait_time = result.get("wait_time", 1)

    logger.info(
        f"[MultiRefresh] {len(eligible_pipelines)} eligible pipeline(s) this cycle."
    )

    if eligible_pipelines:
        trigger_eligible_jobs(eligible_pipelines, trigger_time_str)

    logger.info(f"[MultiRefresh] is_completed={is_completed}  wait_time={wait_time}s")

    if is_completed == 1:
        logger.info("[MultiRefresh] All refreshes completed for today. Exiting.")
        dbutils.notebook.exit(
            f"Multi-refresh complete. All tables processed for {trigger_date_str}."
        )

    # Exit if we crossed midnight
    current_date = datetime.datetime.now(IST).date()
    if current_date > triggerTime.date():
        logger.info("[MultiRefresh] Day boundary crossed. Exiting.")
        dbutils.notebook.exit("Day boundary crossed - orchestrator exiting.")

    if max_iterations > 1:
        logger.info(
            f"[MultiRefresh] Sleeping {wait_time}s until next refresh window..."
        )
        time.sleep(wait_time)

# -- Safety exit after max_iterations ------------------------------------------
logger.warning(
    f"[MultiRefresh] Reached max_iterations={max_iterations}. Force-exiting."
)
dbutils.notebook.exit(
    f"Multi-refresh orchestrator exited after max_iterations={max_iterations}."
)