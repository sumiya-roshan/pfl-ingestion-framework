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

import json
import time
import random
import logging
import datetime
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number
import pytz

from multi_refresh.job_trigger import JobTrigger
from ingestion.utils.logger import get_logger

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("admin_catalog_name",    "",      "Admin Catalog Name (e.g. migration_x_catalog)")
dbutils.widgets.text("environment",           "dev",   "Environment: dev | uat | prod")
dbutils.widgets.text("job_run_id",            "",      "Job Run ID - set to {{job.run_id}}")
dbutils.widgets.text("max_iterations",        "200",   "Safety: max loop iterations before exit")
dbutils.widgets.text("secret_scope",          "",      "Secret scope for Databricks PAT token")
dbutils.widgets.text("secret_key_pat",        "databricks-pat-token", "Secret key for Databricks PAT token")
dbutils.widgets.text("s3_log_path",           "",      "S3 Log Path (e.g. s3://bucket/logs/)")

# COMMAND ----------

admin_catalog_name = dbutils.widgets.get("admin_catalog_name") or None
environment        = dbutils.widgets.get("environment")        or "dev"
job_run_id         = dbutils.widgets.get("job_run_id")         or "MANUAL"
max_iterations     = int(dbutils.widgets.get("max_iterations") or "200")
secret_scope       = dbutils.widgets.get("secret_scope")       or None
secret_key_pat     = dbutils.widgets.get("secret_key_pat")     or "databricks-pat-token"
s3_log_path        = dbutils.widgets.get("s3_log_path")        or None

if not admin_catalog_name:
    dbutils.notebook.exit("Error: admin_catalog_name widget is required.")
if not secret_scope:
    dbutils.notebook.exit("Error: secret_scope widget is required (needed for REST API PAT token).")

# Fully-qualified schema configurations
CFG_SCHEMA           = f"{admin_catalog_name}.pfl_x_schema"
TEMP_SCHEMA          = f"{admin_catalog_name}.temp"
BATCH_RUN_CFG_TABLE  = f"{CFG_SCHEMA}.tb_report_batch_run_config"
DEP_MASTER_TABLE     = f"{CFG_SCHEMA}.dependency_master_config"
RDBMS_CFG_TABLE      = f"{CFG_SCHEMA}.rdbms_ingestion_config"
CONFIG_MASTER_TABLE  = f"{CFG_SCHEMA}.config_master"
ELIGIBLE_TEMP_TABLE  = f"{TEMP_SCHEMA}.tb_eligible_objects"

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
    dbutils.notebook.entry_point
    .getDbutils()
    .notebook()
    .getContext()
    .apiUrl()
    .get()
)
pat_token = dbutils.secrets.get(scope=secret_scope, key=secret_key_pat)
job_trigger = JobTrigger(workspace_url=workspace_url, token=pat_token)

# COMMAND ----------

def merge_with_retry(statement: str, max_retries: int = 5) -> None:
    """Execute a Spark SQL write statement with retries on Delta conflicts."""
    for attempt in range(1, max_retries + 1):
        try:
            spark.sql(statement)
            return
        except Exception as e:
            err_msg = str(e)
            if any(term in err_msg for term in ["MetadataChangedException", "ConcurrentAppendException"]):
                if attempt == max_retries:
                    raise
                sleep_duration = random.uniform(1, 5)
                logger.warning(
                    f"[MultiRefresh] Delta write conflict detected. "
                    f"Retrying in {sleep_duration:.2f}s (Attempt {attempt}/{max_retries})...."
                )
                time.sleep(sleep_duration)
            else:
                raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### RDBMS Source Multi Refresh Processor

# COMMAND ----------

def process_rdbms_multi_refresh(multi_refresh_df, trigger_hhmm, trigger_date_str, trigger_time_str) -> int:
    """
    Process RDBMS (Config_Master_ID = 1) schedules.
    Left joins child config + dependency config, saves to temp table, and executes MERGE updates.
    Returns the number of eligible tables found.
    """
    # 1. Filter schedule rows for Config_Master_ID = 1 and keep the latest schedule time per table
    win_spec = Window.partitionBy(col('Config_Master_ID'), col('Config_ID')).orderBy(col('Curent_Refresh_Time').desc())
    table_refresh_df = (
        multi_refresh_df
        .filter("Config_Master_ID = 1")
        .withColumn("row_num", row_number().over(win_spec))
    )
    table_refresh_df = table_refresh_df.filter(col("row_num") == 1).drop("row_num")
    table_refresh_df.createOrReplaceTempView("table_refresh_df")

    eligible_count = table_refresh_df.count()
    if eligible_count == 0:
        return 0

    # 2. Join with child config & dependency master config to filter by Day_Status_Flag and Current_Status_Flag
    eligible_df = spark.sql(f"""
        SELECT * FROM (
            SELECT 
                a.Config_Master_ID,
                a.Config_ID,
                CASE WHEN c.config_id IS NOT NULL THEN 1 ELSE 0 END AS Current_Status_Flag,
                CASE WHEN b.Config_ID IS NOT NULL THEN 1 ELSE 0 END AS Day_Status_Flag,
                a.Curent_Refresh_Time, 
                a.Next_Refresh_Time,
                a.ID
            FROM table_refresh_df a
            LEFT JOIN (
                SELECT Config_Master_ID, Config_ID, Pipeline_Name
                FROM {RDBMS_CFG_TABLE}
                WHERE 
                Day_Execution_Count > 0 AND 
                  to_date(Sink_Batch_Started_Date) = '{trigger_date_str}'
            ) b ON a.Config_Master_ID = b.Config_Master_ID AND a.Config_ID = b.Config_ID
            LEFT JOIN (
                SELECT distinct config_master_id, config_id
                FROM {DEP_MASTER_TABLE}
                WHERE dependency_resolve_time IS NULL 
                  AND Is_Active = true
            ) c ON a.Config_Master_ID = c.config_master_id AND a.Config_ID = c.config_id
        ) final
        WHERE Current_Status_Flag = 0 AND Day_Status_Flag = 1
    """)

    # 3. Add Pipeline_Name to the eligible rows
    eligible_rows = eligible_df.collect()
    result_rows = []
    for r in eligible_rows:
        r_dict = r.asDict()
        res = spark.sql(f"""
            SELECT Pipeline_Name, Source_Name FROM {RDBMS_CFG_TABLE}
            WHERE Config_Master_ID = {r['Config_Master_ID']} AND Config_ID = {r['Config_ID']}
            LIMIT 1
        """).collect()
        if res:
            r_dict["Pipeline_Name"] = res[0]["Pipeline_Name"]
            r_dict["Source_Name"] = res[0]["Source_Name"]
            result_rows.append(r_dict)


    if not result_rows:
        return 0

    eligible_df = spark.createDataFrame(result_rows)
    eligible_count = eligible_df.count()
    logger.info(f"[MultiRefresh][RDBMS] {eligible_count} eligible tables found.")
    
    # Write to temp table for SQL MERGE operations
    eligible_df.write.format("delta").mode("overwrite").saveAsTable(ELIGIBLE_TEMP_TABLE)
    
    # MERGE 1: Update status to 'In Progress' in child config table
    merge_with_retry(f"""
        MERGE INTO {RDBMS_CFG_TABLE} t
        USING {ELIGIBLE_TEMP_TABLE} s
        ON t.Config_Master_ID = s.Config_Master_ID AND t.Config_ID = s.Config_ID
        WHEN MATCHED THEN UPDATE SET 
            t.sink_batch_started_date = '{trigger_time_str} UTC',
            t.Status                  = 'In Progress'
    """)

    # MERGE 2: Reset dependency_resolve_time for downstream dependencies
    merge_with_retry(f"""
        MERGE INTO {DEP_MASTER_TABLE} t
        USING (
            SELECT DISTINCT
                b.config_master_id,
                b.config_id
            FROM {ELIGIBLE_TEMP_TABLE} a
            JOIN {DEP_MASTER_TABLE} b 
              ON a.Config_Master_ID = b.config_master_id
             AND a.Config_ID        = b.config_id
             AND b.config_master_id = 1
            WHERE EXISTS (
                SELECT 1 FROM {BATCH_RUN_CFG_TABLE} c
                WHERE c.Config_Master_ID = b.config_master_id
                  AND c.Config_ID        = b.config_id
            )
        ) s
        ON t.config_master_id = s.config_master_id
       AND t.config_id        = s.config_id
       AND coalesce(to_date(t.dependency_resolve_time), '1900-01-01') = current_date()
        WHEN MATCHED THEN UPDATE SET dependency_resolve_time = null
    """)

    # MERGE 3: Update Last_Sink_Date in schedule table
    merge_with_retry(f"""
        MERGE INTO {BATCH_RUN_CFG_TABLE} t
        USING (
            SELECT DISTINCT s.ID 
            FROM (
                SELECT 
                    ID, Config_Master_ID, Config_ID, Refresh_Time as Curent_Refresh_Time, 
                    coalesce(lead(Refresh_Time) OVER(partition by Config_Master_ID,Config_ID order by Refresh_Time),'23:59') as Next_Refresh_Time
                FROM {BATCH_RUN_CFG_TABLE}
            ) t 
            INNER JOIN {ELIGIBLE_TEMP_TABLE} s 
               ON t.Config_Master_ID = s.Config_Master_ID 
              AND t.Config_ID        = s.Config_ID 
              AND ('{trigger_hhmm}' >= t.Curent_Refresh_Time 
              AND '{trigger_hhmm}' < t.Next_Refresh_Time)
        ) s
        ON t.ID = s.ID
        WHEN MATCHED THEN UPDATE SET t.Last_Sink_Date = '{trigger_time_str}'
    """)

    return eligible_count

# COMMAND ----------

# MAGIC %md
# MAGIC ### Email Delivery Multi Refresh Processor

# COMMAND ----------

def process_email_delivery_multi_refresh(multi_refresh_df, trigger_hhmm, trigger_date_str, trigger_time_str) -> int:
    """
    Process Email Delivery (Config_Master_ID = 15) schedules.
    """
    win_spec = Window.partitionBy(col('Config_Master_ID'), col('Config_ID')).orderBy(col('Curent_Refresh_Time').desc())
    email_data_refresh_df = (
        multi_refresh_df
        .filter("Config_Master_ID = 15")
        .withColumn("row_num", row_number().over(win_spec))
    )
    email_data_refresh_df = email_data_refresh_df.filter(col("row_num") == 1).drop("row_num")
    email_data_refresh_df.createOrReplaceTempView("email_data_refresh_df")

    eligible_count = email_data_refresh_df.count()
    if eligible_count == 0:
        return 0

    logger.info(f"[MultiRefresh][Email] {eligible_count} eligible tables found.")
    
    # Merge INTO Email delivery config table
    merge_with_retry(f"""
        MERGE INTO {CFG_SCHEMA}.tb_sourcedb_email_delivery t
        USING email_data_refresh_df s
        ON t.Config_ID = s.Config_ID 
       AND t.Config_Master_ID = s.Config_Master_ID 
       AND t.Is_Active = 1 
       AND (t.Status <> 'In-Progress' OR to_date(t.sink_batch_started_on) != '{trigger_date_str}') 
       AND (t.Frequency = 'Daily' OR (t.Frequency = 'Monthly' and nvl(t.Report_Execution_Day, 0) = cast(date_format('{trigger_time_str}', 'd') as int)))
        WHEN MATCHED THEN UPDATE SET 
            t.sink_batch_started_on = null,
            t.Status = 'In-Progress'
    """)

    # Update Last_Sink_Date in schedule table
    spark.sql(f"""
        UPDATE {BATCH_RUN_CFG_TABLE}
        SET Last_Sink_Date = '{trigger_time_str}'
        WHERE ID IN (SELECT DISTINCT ID FROM email_data_refresh_df)
    """)

    return eligible_count

# COMMAND ----------

# MAGIC %md
# MAGIC ### Job Trigger and API Dispatcher

# COMMAND ----------

def _resolve_child_table_fqn(config_master_id: int) -> str:
    """
    Resolves the child ingestion-config table for a given Config_Master_ID via
    config_master — same routing ConfigManager.get_active_tasks() uses, so this
    works generically for RDBMS, NoSQL, S3, or any future source type without
    hardcoding a table name here.
    """
    master_rows = (
        spark.table(CONFIG_MASTER_TABLE)
        .filter(f"config_id = {int(config_master_id)}")
        .collect()
    )
    if not master_rows:
        raise ValueError(f"No entry in {CONFIG_MASTER_TABLE} for config_id={config_master_id}")
    m_row = master_rows[0].asDict()
    return f"{m_row.get('config_catalog_name')}.{m_row.get('config_schema_name')}.{m_row.get('config_table_name')}"


def _active_pipeline_names(child_table_fqn: str) -> set:
    """
    Returns the set of distinct Pipeline_Name values with at least one active
    row in the given child config table. Resolves the Pipeline_Name/Is_Active
    column names case-insensitively, since child tables vary in casing
    (rdbms_ingestion_config uses Pipeline_Name/Is_Active; others may not).
    """
    df = spark.table(child_table_fqn)
    pipeline_col = next((c for c in df.columns if c.lower() == "pipeline_name"), None)
    active_col   = next((c for c in df.columns if c.lower() == "is_active"), None)
    if not pipeline_col or not active_col:
        logger.warning(
            f"[MultiRefresh] {child_table_fqn} has no Pipeline_Name/Is_Active "
            f"columns — skipping job-trigger lookup for this table."
        )
        return set()

    rows = (
        df.filter(f"{active_col} = true")
        .select(pipeline_col)
        .distinct()
        .collect()
    )
    return {r[pipeline_col] for r in rows}


def trigger_eligible_jobs(trigger_time_str: str) -> None:
    """
    Joins each eligible table's own child ingestion-config table (resolved
    dynamically per Config_Master_ID via config_master — RDBMS, NoSQL, S3, or
    any future source type) against tb_eligible_objects and triggers the
    matching Databricks Job. Pipeline_Name doubles as the Databricks Job name.

    Note: Is_Active here is per-table (one row per table in the child config),
    not a single pipeline-level kill switch — a pipeline triggers as long as
    at least one of its eligible tables is still active.
    """
    if not spark.catalog.tableExists(ELIGIBLE_TEMP_TABLE):
        return

    eligible_df = spark.table(ELIGIBLE_TEMP_TABLE)
    config_master_ids = [r[0] for r in eligible_df.select("Config_Master_ID").distinct().collect()]

    job_cfg_rows = []
    for cmid in config_master_ids:
        try:
            child_table_fqn = _resolve_child_table_fqn(cmid)
            active_pipelines = _active_pipeline_names(child_table_fqn)
        except Exception as exc:
            logger.error(f"[MultiRefresh] Could not resolve child config table for Config_Master_ID={cmid}: {exc}")
            continue

        rows = (
            eligible_df
            .filter(f"Config_Master_ID = {int(cmid)}")
            .select("Config_Master_ID", "Pipeline_Name", "Source_Name")
            .distinct()
            .collect()
        )
        for row in rows:
            if row["Pipeline_Name"] in active_pipelines:
                job_cfg_rows.append({
                    "Databricks_Job_Name": row["Pipeline_Name"],
                    "Config_Master_ID":    row["Config_Master_ID"],
                    "Pipeline_Name":       row["Pipeline_Name"],
                    "Source_Name":         row["Source_Name"],
                })

    for row in job_cfg_rows:
        job_name      = row["Databricks_Job_Name"]
        cmid          = row["Config_Master_ID"]
        pipeline_name = row["Pipeline_Name"]
        source_name   = row["Source_Name"]
        try:
            run_id = job_trigger.run_now_by_name(
                job_name        = job_name,
                notebook_params = {
                    "batch_start_date": trigger_time_str,
                    "pipeline_name":    pipeline_name,
                    "source_name":      source_name
                },
            )
            logger.info(
                f"[MultiRefresh] Triggered job '{job_name}' for Config_Master_ID={cmid} "
                f"pipeline={pipeline_name} source_name={source_name} ? run_id={run_id}  batch_start_date={trigger_time_str}"
            )
        except Exception as exc:
            logger.error(
                f"[MultiRefresh] Failed to trigger job '{job_name}' "
                f"for Config_Master_ID={cmid} pipeline={pipeline_name}: {exc}"
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
        f"\n{'='*60}\n"
        f"[MultiRefresh] Iteration {iteration}/{max_iterations} | "
        f"triggerTime = {triggerTime} (IST)\n"
        f"{'='*60}"
    )

    # -- Step 1: Query multi-refresh report config IDs (Schedule scan) ----------
    multi_refresh_df = spark.sql(f"""
        SELECT * FROM (
            SELECT 
                ID,
                Object_Name as Task_Name, 
                Object_Type, 
                Config_Master_ID, 
                Config_ID, 
                Refresh_Time as Curent_Refresh_Time, 
                coalesce(
                    lead(Refresh_Time) OVER (
                        PARTITION BY Config_Master_ID, Config_ID 
                        ORDER BY Refresh_Time
                    ),
                    '23:59'
                ) as Next_Refresh_Time, 
                Last_Sink_Date, 
                Is_Active 
            FROM {BATCH_RUN_CFG_TABLE}
        ) a 
        WHERE a.Is_Active = 1
          AND '{trigger_hhmm}' BETWEEN a.Curent_Refresh_Time AND a.Next_Refresh_Time 
          AND to_date(coalesce(a.Last_Sink_Date, '1900-01-01')) != '{trigger_date_str}'
    """)
    multi_refresh_df.createOrReplaceTempView("multi_refresh_eligible_config")

    if multi_refresh_df.count() == 0:
        logger.info("[MultiRefresh] No eligible schedules for this iteration.")
        rdbms_count = 0
        email_count = 0
    else:
        # -- Step 2 & 3: Run RDBMS & Email Multi Refresh Processors ------------
        rdbms_count = process_rdbms_multi_refresh(multi_refresh_df, trigger_hhmm, trigger_date_str, trigger_time_str)
        email_count = process_email_delivery_multi_refresh(multi_refresh_df, trigger_hhmm, trigger_date_str, trigger_time_str)

        # -- Step 4: Dispatch job triggers if any RDBMS tables were processed --
        if rdbms_count > 0:
            trigger_eligible_jobs(trigger_time_str)

        # Cleanup temp table
        try:
            spark.sql(f"DROP TABLE IF EXISTS {ELIGIBLE_TEMP_TABLE}")
        except Exception:
            pass

    # -- Step 5: Check completion and wait time -------------------------------
    # 1. wait_second_flag (Any incomplete runs from past windows?)
    wait_second_flag_rows = spark.sql(f"""
        WITH CTE AS (
            SELECT max(Refresh_Time) as max_refresh_time
            FROM {BATCH_RUN_CFG_TABLE} 
            WHERE Refresh_Time < '{trigger_hhmm}'
              AND to_date(Last_Sink_Date) != '{trigger_date_str}'
              AND Is_Active = 1
        )
        SELECT CASE WHEN count(1) > 0 THEN 1 ELSE 0 END as wait_second_flag 
        FROM {BATCH_RUN_CFG_TABLE} 
        WHERE Refresh_Time < '{trigger_hhmm}'
          AND to_date(Last_Sink_Date) != '{trigger_date_str}'
          AND Is_Active = 1 
          AND Refresh_Time BETWEEN (SELECT max_refresh_time FROM CTE) AND '{trigger_hhmm}'
    """).collect()
    wait_second_flag = wait_second_flag_rows[0][0] if wait_second_flag_rows else 0

    # 2. is_completed (Are all runs for the day completed?)
    is_completed_rows = spark.sql(f"""
        SELECT CASE WHEN to_date(MIN(Last_Sink_Date)) = '{trigger_date_str}' THEN 1 ELSE 0 END as is_completed 
        FROM {BATCH_RUN_CFG_TABLE} 
        WHERE Is_Active = 1 
          AND Refresh_Time IN (
              SELECT max(Refresh_Time) 
              FROM {BATCH_RUN_CFG_TABLE} 
              WHERE Is_Active = 1
          )
    """).collect()
    is_completed = is_completed_rows[0]["is_completed"] if is_completed_rows else 0

    # 3. wait_time (How many seconds until the next schedule window?)
    wait_time_rows = spark.sql(f"""
        SELECT (unix_timestamp(to_timestamp(concat('{trigger_date_str} ', MIN(Refresh_Time), ':00'))) 
                - unix_timestamp(from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'))) AS difference_in_seconds 
        FROM {BATCH_RUN_CFG_TABLE} 
        WHERE Refresh_Time > '{trigger_hhmm}'
          AND Is_Active = 1
    """).collect()
    
    wait_time_raw = wait_time_rows[0]['difference_in_seconds'] if wait_time_rows else None
    wait_time = 1 if wait_time_raw is None or wait_second_flag == 1 else int(wait_time_raw)
    wait_time = max(wait_time, 1)

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
        logger.info(f"[MultiRefresh] Sleeping {wait_time}s until next refresh window...")
        time.sleep(wait_time)

# -- Safety exit after max_iterations ------------------------------------------
logger.warning(f"[MultiRefresh] Reached max_iterations={max_iterations}. Force-exiting.")
dbutils.notebook.exit(f"Multi-refresh orchestrator exited after max_iterations={max_iterations}.")
