# Databricks notebook source
# MAGIC %md
# MAGIC # Multi-Refresh Orchestrator
# MAGIC
# MAGIC Replicates the ADF `PL_Multi_Refresh_Automation` pipeline inside Databricks.
# MAGIC
# MAGIC ## How it works
# MAGIC - Runs as a **long-running Databricks Job** (e.g. triggered at 9 AM, times out at midnight).
# MAGIC - Each loop iteration:
# MAGIC   1. Captures `batch_start_date` (current IST time).
# MAGIC   2. Queries `tb_report_batch_run_config` to find tables whose refresh window is NOW
# MAGIC      and haven't been sinked today.
# MAGIC   3. Cross-checks the ingestion sink config + dependency master config.
# MAGIC   4. Writes eligible rows to a temp table, then fires 3 MERGE statements.
# MAGIC   5. For each eligible Config_Master_ID, resolves the job name from
# MAGIC      `tb_multi_refresh_job_config` and triggers that job via REST API (fire & forget).
# MAGIC   6. Computes `is_completed` and `wait_time`, then either exits or sleeps.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
sys.path.append("..")

import json
import time
import random
import logging
from datetime import datetime

import pytz

from multi_refresh.job_trigger import JobTrigger
from ingestion.utils.secrets import SecretResolver
from ingestion.utils.logger import get_logger

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

# Admin catalog that holds all config tables
# (same as pipeline().globalParameters.admin_catalog_name in ADF)
dbutils.widgets.text("admin_catalog_name",    "",      "Admin Catalog Name (e.g. migration_x_catalog)")
dbutils.widgets.text("environment",           "dev",   "Environment: dev | uat | prod")
dbutils.widgets.text("job_run_id",            "",      "Job Run ID - set to {{job.run_id}}")
# Safety cap: maximum number of loop iterations before forcibly exiting.
# Set high (e.g. 200) for production; useful for testing (set to 1 or 2).
dbutils.widgets.text("max_iterations",        "200",   "Safety: max loop iterations before exit")
# Secret scope that holds the Databricks PAT token for REST API calls
dbutils.widgets.text("secret_scope",          "",      "Secret scope for Databricks PAT token")
dbutils.widgets.text("secret_key_pat",        "databricks-pat-token", "Secret key for Databricks PAT token")

# COMMAND ----------

admin_catalog    = dbutils.widgets.get("admin_catalog_name") or None
environment      = dbutils.widgets.get("environment")        or "dev"
job_run_id       = dbutils.widgets.get("job_run_id")         or "MANUAL"
max_iterations   = int(dbutils.widgets.get("max_iterations") or "200")
secret_scope     = dbutils.widgets.get("secret_scope")       or None
secret_key_pat   = dbutils.widgets.get("secret_key_pat")     or "databricks-pat-token"

if not admin_catalog:
    dbutils.notebook.exit("Error: admin_catalog_name widget is required.")
if not secret_scope:
    dbutils.notebook.exit("Error: secret_scope widget is required (needed for REST API PAT token).")

# Fully-qualified table names (admin catalog uses 'config' schema by convention)
CFG_SCHEMA             = f"{admin_catalog}.config"
TEMP_SCHEMA            = f"{admin_catalog}.temp"
BATCH_RUN_CFG_TABLE    = f"{CFG_SCHEMA}.tb_report_batch_run_config"
DEP_MASTER_TABLE       = f"{CFG_SCHEMA}.tb_dependency_master_config"
MULTI_REFRESH_JOB_CFG  = f"{CFG_SCHEMA}.tb_multi_refresh_job_config"
ELIGIBLE_TEMP_TABLE    = f"{TEMP_SCHEMA}.tb_eligible_objects"

IST = pytz.timezone("Asia/Kolkata")

logger = get_logger(environment=environment)

print(f"admin_catalog_name : {admin_catalog}")
print(f"environment        : {environment}")
print(f"max_iterations     : {max_iterations}")
print(f"BATCH_RUN_CFG_TABLE: {BATCH_RUN_CFG_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolve Databricks Workspace URL and PAT Token

# COMMAND ----------

# Workspace URL from the notebook context (no widget needed)
workspace_url = (
    dbutils.notebook.entry_point
    .getDbutils()
    .notebook()
    .getContext()
    .apiUrl()
    .get()
)

# PAT token from Databricks secret scope
pat_token = dbutils.secrets.get(scope=secret_scope, key=secret_key_pat)

print(f"Workspace URL: {workspace_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multi-Refresh Ingestion Sink Config Table
# MAGIC
# MAGIC We resolve the child config table (equivalent to rdbms_ingestion_config)
# MAGIC dynamically per Config_Master_ID via the config_master routing table.

# COMMAND ----------

from ingestion.utils.config_manager import CONFIG_MASTER_TABLE


def get_sink_table_fqn(config_master_id: int) -> str:
    """
    Resolves the sink config table FQN for a given Config_Master_ID
    by reading the config_master routing table - never hardcoded.
    """
    rows = (
        spark.table(CONFIG_MASTER_TABLE)
        .filter(f"config_id = {config_master_id}")
        .collect()
    )
    if not rows:
        raise ValueError(
            f"No entry in {CONFIG_MASTER_TABLE} for config_id={config_master_id}"
        )
    r = rows[0].asDict()
    return f"{r['config_catalog_name']}.{r['config_schema_name']}.{r['config_table_name']}"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper: Run MERGE with Retry on Concurrent Write Conflicts

# COMMAND ----------

def merge_with_retry(sql: str, max_attempts: int = 5) -> None:
    """
    Executes a MERGE/UPDATE SQL statement.
    Retries on Delta concurrent write conflicts (MetadataChangedException,
    ConcurrentAppendException) with a random back-off - same pattern as the
    original ADF notebook.
    """
    for attempt in range(max_attempts):
        try:
            spark.sql(sql)
            return
        except Exception as exc:
            exc_type = str(type(exc))
            if "MetadataChangedException" in exc_type or "ConcurrentAppendException" in exc_type:
                sleep_sec = random.uniform(1, 5)
                logger.warning(
                    f"[MultiRefresh] Concurrent write conflict on attempt {attempt + 1}. "
                    f"Retrying in {sleep_sec:.1f}s..."
                )
                time.sleep(sleep_sec)
            else:
                raise


# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper: Eligibility Query (mirrors NB_Object_Multi_Refresh_Check)

# COMMAND ----------

def get_eligible_rows(batch_start_date: datetime):
    """
    Replicates the ADF notebook eligibility logic:
      1. Find tb_report_batch_run_config rows where the current IST time
         falls in the [Curent_Refresh_Time, Next_Refresh_Time) window
         and Last_Sink_Date is NOT today.
      2. Cross-check against the ingestion sink config (already sinked today?)
         and dependency master config (dependency resolved = silver done?).
    Returns a Spark DataFrame of eligible rows with columns:
      Config_Master_ID, Config_ID, Curent_Refresh_Time, Next_Refresh_Time
    """
    trigger_time_str  = batch_start_date.strftime("%Y-%m-%d %H:%M:%S")
    trigger_hhmm      = batch_start_date.strftime("%H:%M")
    trigger_date_str  = batch_start_date.strftime("%Y-%m-%d")

    # Step 1: Compute refresh window per (Config_Master_ID, Config_ID)
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW multi_refresh_eligible_config AS
        SELECT * FROM (
            SELECT
                ID,
                Object_Name        AS Task_Name,
                Object_Type,
                Config_Master_ID,
                Config_ID,
                Refresh_Time       AS Curent_Refresh_Time,
                coalesce(
                    lead(Refresh_Time) OVER (
                        PARTITION BY Config_Master_ID, Config_ID
                        ORDER BY to_timestamp(Refresh_Time)
                    ),
                    '23:59'
                ) AS Next_Refresh_Time,
                Last_Sink_Date,
                Is_Active
            FROM {BATCH_RUN_CFG_TABLE}
        ) a
        WHERE
            a.Is_Active = 1
            AND cast('{trigger_hhmm}' AS timestamp)
                BETWEEN cast(a.Curent_Refresh_Time AS timestamp)
                    AND cast(a.Next_Refresh_Time   AS timestamp)
            AND to_date(coalesce(a.Last_Sink_Date, '1900-01-01')) != '{trigger_date_str}'
    """)

    multi_refresh_df = spark.sql("SELECT * FROM multi_refresh_eligible_config")
    if multi_refresh_df.count() == 0:
        return None

    # Step 2: For each Config_Master_ID group, keep only the latest refresh time row
    #         (in case multiple Refresh_Time values are in the same window)
    from pyspark.sql import Window
    from pyspark.sql.functions import col, row_number

    win_spec = Window.partitionBy("Config_Master_ID", "Config_ID").orderBy(
        col("Curent_Refresh_Time").desc()
    )
    table_refresh_df = (
        multi_refresh_df
        .withColumn("row_num", row_number().over(win_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    table_refresh_df.createOrReplaceTempView("table_refresh_df")

    # Step 3: Cross-check sink config (already sinked today = Day_Status_Flag=1)
    #         and dependency master (unresolved dependency = Current_Status_Flag=1 means blocked)
    #         Eligible = Day_Status_Flag=1 AND Current_Status_Flag=0
    eligible_df = spark.sql(f"""
        SELECT * FROM (
            SELECT
                a.Config_Master_ID,
                a.Config_ID,
                CASE WHEN c.Table_Config_ID IS NOT NULL THEN 1 ELSE 0 END AS Current_Status_Flag,
                CASE WHEN b.Config_ID IS NOT NULL      THEN 1 ELSE 0 END AS Day_Status_Flag,
                a.Curent_Refresh_Time,
                a.Next_Refresh_Time
            FROM table_refresh_df a
            LEFT JOIN (
                SELECT Config_Master_ID, Config_ID
                FROM (
                    -- Resolve sink table dynamically per Config_Master_ID is complex in pure SQL;
                    -- we union known config tables routed via config_master (see note below).
                    -- For the cross-check, we query the rdbms child table via config_master routing.
                    SELECT DISTINCT s.Config_Master_ID, s.Config_ID
                    FROM table_refresh_df s
                ) ids
                -- We perform the Day_Status_Flag check inside Spark (Python loop below)
                -- because each Config_Master_ID can point to a different sink table.
                -- This subquery is a placeholder; actual check done in Python.
            ) b ON a.Config_Master_ID = b.Config_Master_ID AND a.Config_ID = b.Config_ID
            LEFT JOIN (
                SELECT DISTINCT Table_Config_Master_ID, Table_Config_ID
                FROM {DEP_MASTER_TABLE}
                WHERE
                    Dependency_Resolved_Time IS NULL
                    AND Delta_Layer = 'Silver'
                    AND Is_Active = 1
            ) c ON a.Config_Master_ID = c.Table_Config_Master_ID
               AND a.Config_ID        = c.Table_Config_ID
        ) final
        WHERE Current_Status_Flag = 0
    """)

    # Step 4: Apply Day_Status_Flag check dynamically per Config_Master_ID
    # (since each Config_Master_ID routes to a different sink table via config_master)
    from pyspark.sql.functions import lit
    import pyspark.sql.functions as F

    eligible_rows = eligible_df.collect()
    result_rows = []

    for row in eligible_rows:
        cmid = row["Config_Master_ID"]
        cid  = row["Config_ID"]
        try:
            sink_table = get_sink_table_fqn(cmid)
            sinked_today = spark.sql(f"""
                SELECT 1 FROM {sink_table}
                WHERE Config_Master_ID = {cmid}
                  AND Config_ID = {cid}
                  AND Day_Execution_Count > 0
                  AND to_date(Sink_Batch_Started_Date) = '{trigger_date_str}'
                LIMIT 1
            """).count() > 0
        except Exception as exc:
            logger.warning(f"[MultiRefresh] Could not check sink table for config_master_id={cmid}: {exc}. Skipping.")
            continue

        if sinked_today:
            result_rows.append(row.asDict())

    if not result_rows:
        return None

    return spark.createDataFrame(result_rows)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper: MERGE State Updates (3 tables)

# COMMAND ----------

def apply_merge_updates(batch_start_date: datetime) -> None:
    """
    Fires the 3 MERGE statements after eligibility is determined:
      1. Ingestion sink config ? mark "In Progress" with sink_batch_started_date
      2. Dependency master    ? reset Dependency_Resolved_Time = NULL
      3. Batch run config     ? set Last_Sink_Date = batch_start_date
    All MERGEs use retry logic for Delta concurrent write conflicts.
    """
    trigger_time_str = str(batch_start_date)
    trigger_hhmm     = batch_start_date.strftime("%H:%M")
    trigger_date_str = batch_start_date.strftime("%Y-%m-%d")

    # Get unique Config_Master_IDs from eligible objects
    cmid_rows = spark.sql(
        f"SELECT DISTINCT Config_Master_ID FROM {ELIGIBLE_TEMP_TABLE}"
    ).collect()

    for row in cmid_rows:
        cmid = row["Config_Master_ID"]
        try:
            sink_table = get_sink_table_fqn(cmid)
        except Exception as exc:
            logger.warning(f"[MultiRefresh] Cannot resolve sink table for config_master_id={cmid}: {exc}")
            continue

        # MERGE 1: Mark eligible rows as "In Progress" in the ingestion sink config table
        merge_with_retry(f"""
            MERGE INTO {sink_table} t
            USING (
                SELECT Config_Master_ID, Config_ID
                FROM {ELIGIBLE_TEMP_TABLE}
                WHERE Config_Master_ID = {cmid}
            ) s
            ON t.Config_Master_ID = s.Config_Master_ID
           AND t.Config_ID        = s.Config_ID
            WHEN MATCHED THEN UPDATE SET
                t.Sink_Batch_Started_Date = '{trigger_time_str}',
                t.Status                  = 'In Progress'
        """)

    # MERGE 2: Reset Dependency_Resolved_Time for eligible tables that have
    #          a dependency entry whose Task source is also in the batch run config
    merge_with_retry(f"""
        MERGE INTO {DEP_MASTER_TABLE} t
        USING (
            SELECT
                b.Task_Config_Master_ID,
                b.Task_Config_ID,
                b.Table_Config_Master_ID,
                b.Table_Config_ID
            FROM {ELIGIBLE_TEMP_TABLE} a
            JOIN {DEP_MASTER_TABLE} b
              ON a.Config_Master_ID = b.Table_Config_Master_ID
             AND a.Config_ID        = b.Table_Config_ID
            WHERE
                b.Task_Config_Master_ID IN (
                    SELECT DISTINCT Config_Master_ID FROM {BATCH_RUN_CFG_TABLE}
                )
                AND b.Task_Config_ID IN (
                    SELECT DISTINCT Config_ID FROM {BATCH_RUN_CFG_TABLE}
                )
        ) s
        ON  t.Task_Config_Master_ID  = s.Task_Config_Master_ID
        AND t.Task_Config_ID         = s.Task_Config_ID
        AND t.Table_Config_Master_ID = s.Table_Config_Master_ID
        AND t.Table_Config_ID        = s.Table_Config_ID
        AND coalesce(to_date(t.Dependency_Resolved_Time), '1900-01-01') = current_date()
        WHEN MATCHED THEN UPDATE SET
            t.Dependency_Resolved_Time = NULL
    """)

    # MERGE 3: Set Last_Sink_Date on tb_report_batch_run_config for processed rows
    merge_with_retry(f"""
        MERGE INTO {BATCH_RUN_CFG_TABLE} t
        USING (
            SELECT DISTINCT s.ID
            FROM (
                SELECT
                    ID,
                    Config_Master_ID,
                    Config_ID,
                    Refresh_Time        AS Curent_Refresh_Time,
                    coalesce(
                        lead(Refresh_Time) OVER (
                            PARTITION BY Config_Master_ID, Config_ID
                            ORDER BY to_timestamp(Refresh_Time)
                        ),
                        '23:59'
                    ) AS Next_Refresh_Time
                FROM {BATCH_RUN_CFG_TABLE}
            ) t
            INNER JOIN {ELIGIBLE_TEMP_TABLE} s
               ON t.Config_Master_ID = s.Config_Master_ID
              AND t.Config_ID        = s.Config_ID
             AND cast('{trigger_hhmm}' AS timestamp) >= cast(t.Curent_Refresh_Time AS timestamp)
             AND cast('{trigger_hhmm}' AS timestamp)  < cast(t.Next_Refresh_Time   AS timestamp)
        ) s
        ON t.ID = s.ID
        WHEN MATCHED THEN UPDATE SET
            t.Last_Sink_Date = '{trigger_time_str}'
    """)

    logger.info("[MultiRefresh] All 3 MERGE updates applied successfully.")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper: Trigger Source Jobs

# COMMAND ----------

def trigger_source_jobs(batch_start_date: datetime, job_trigger: JobTrigger) -> None:
    """
    For each eligible Config_Master_ID in the temp table, looks up the
    corresponding Databricks Job Name from tb_multi_refresh_job_config
    and triggers it fire-and-forget via the REST API.

    tb_multi_refresh_job_config schema:
        Config_Master_ID  INT
        Databricks_Job_Name  STRING   -- must exactly match the Databricks job name
        Is_Active         INT

    The batch_start_date is passed as a notebook_param so the triggered job
    uses the multi-refresh trigger time (not real current time) for watermarking.
    """
    batch_start_str = str(batch_start_date)

    job_cfg_rows = spark.sql(f"""
        SELECT DISTINCT j.Config_Master_ID, j.Databricks_Job_Name
        FROM {MULTI_REFRESH_JOB_CFG} j
        INNER JOIN (
            SELECT DISTINCT Config_Master_ID FROM {ELIGIBLE_TEMP_TABLE}
        ) e ON j.Config_Master_ID = e.Config_Master_ID
        WHERE j.Is_Active = 1
    """).collect()

    if not job_cfg_rows:
        logger.warning(
            "[MultiRefresh] No active job mappings found in tb_multi_refresh_job_config "
            "for eligible Config_Master_IDs. No jobs triggered."
        )
        return

    for row in job_cfg_rows:
        job_name = row["Databricks_Job_Name"]
        cmid     = row["Config_Master_ID"]
        try:
            run_id = job_trigger.run_now_by_name(
                job_name        = job_name,
                notebook_params = {"batch_start_date": batch_start_str},
            )
            logger.info(
                f"[MultiRefresh] Triggered job '{job_name}' for Config_Master_ID={cmid} "
                f"? run_id={run_id}  batch_start_date={batch_start_str}"
            )
        except Exception as exc:
            # Log and continue - a failed trigger for one source should not block others
            logger.error(
                f"[MultiRefresh] Failed to trigger job '{job_name}' "
                f"for Config_Master_ID={cmid}: {exc}"
            )


# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper: Completion Check and Wait Time

# COMMAND ----------

def get_completion_status(batch_start_date: datetime):
    """
    Returns (is_completed: int, wait_time_seconds: int).

    is_completed = 1 when ALL active rows in tb_report_batch_run_config
                       have Last_Sink_Date = today (all refreshes done).

    wait_time    = seconds until the next scheduled Refresh_Time.
                   Returns 1 if there are still incomplete prior-window rows
                   (wait_second_flag = 1) or if there is no next refresh.
    """
    trigger_time_str = str(batch_start_date)
    trigger_date_str = batch_start_date.strftime("%Y-%m-%d")
    trigger_hhmm_ts  = batch_start_date.strftime("%Y-%m-%d %H:%M:%S")

    # is_completed: all active rows for the MAX refresh time have been sinked today
    is_completed = spark.sql(f"""
        SELECT
            CASE WHEN to_date(MIN(Last_Sink_Date)) = '{trigger_date_str}' THEN 1 ELSE 0 END
                AS is_completed
        FROM {BATCH_RUN_CFG_TABLE}
        WHERE Is_Active = 1
          AND to_timestamp(Refresh_Time) IN (
              SELECT max(to_timestamp(Refresh_Time))
              FROM {BATCH_RUN_CFG_TABLE}
              WHERE Is_Active = 1
          )
    """).collect()[0]["is_completed"]

    # wait_second_flag: are there still-unprocessed rows in a past window?
    wait_second_flag = spark.sql(f"""
        WITH CTE AS (
            SELECT max(Refresh_Time) AS max_refresh_time
            FROM {BATCH_RUN_CFG_TABLE}
            WHERE date_format(to_timestamp(Refresh_Time), 'yyyy-MM-dd HH:mm:ss')
                    < date_format('{trigger_hhmm_ts}', 'yyyy-MM-dd HH:mm:ss')
              AND to_date(Last_Sink_Date) != '{trigger_date_str}'
              AND Is_Active = 1
        )
        SELECT
            CASE WHEN count(1) > 0 THEN 1 ELSE 0 END AS wait_second_flag
        FROM {BATCH_RUN_CFG_TABLE}
        WHERE date_format(to_timestamp(Refresh_Time), 'yyyy-MM-dd HH:mm:ss')
                  < date_format('{trigger_hhmm_ts}', 'yyyy-MM-dd HH:mm:ss')
          AND to_date(Last_Sink_Date) != '{trigger_date_str}'
          AND Is_Active = 1
          AND date_format(to_timestamp(Refresh_Time), 'yyyy-MM-dd HH:mm:ss')
                BETWEEN (SELECT date_format(to_timestamp(max_refresh_time), 'yyyy-MM-dd HH:mm:ss') FROM CTE)
                    AND date_format('{trigger_hhmm_ts}', 'yyyy-MM-dd HH:mm:ss')
    """).collect()[0]["wait_second_flag"]

    # wait_time: seconds until the next Refresh_Time after the current batch_start_date
    wait_time_row = spark.sql(f"""
        SELECT
            (unix_timestamp(to_timestamp(MIN(Refresh_Time)))
             - unix_timestamp(from_utc_timestamp(current_timestamp(), 'Asia/Kolkata')))
                AS difference_in_seconds
        FROM {BATCH_RUN_CFG_TABLE}
        WHERE date_format(to_timestamp(Refresh_Time), 'yyyy-MM-dd HH:mm:ss')
                  > date_format('{trigger_hhmm_ts}', 'yyyy-MM-dd HH:mm:ss')
          AND Is_Active = 1
    """).collect()[0]["difference_in_seconds"]

    wait_time = 1 if (wait_time_row is None or wait_second_flag == 1) else int(wait_time_row)
    # Guard against negative wait (clock drift / already past next window)
    wait_time = max(wait_time, 1)

    return int(is_completed), wait_time


# COMMAND ----------

# MAGIC %md
# MAGIC ### Main Loop

# COMMAND ----------

job_trigger = JobTrigger(workspace_url=workspace_url, token=pat_token)

iteration = 0
logger.info(f"[MultiRefresh] Orchestrator starting. max_iterations={max_iterations}")

while iteration < max_iterations:
    iteration += 1
    batch_start_date = datetime.now(IST).replace(tzinfo=None)  # naive IST datetime

    logger.info(
        f"\n{'='*60}\n"
        f"[MultiRefresh] Iteration {iteration}/{max_iterations} | "
        f"batch_start_date = {batch_start_date}\n"
        f"{'='*60}"
    )

    # -- Step 1: Find eligible tables ------------------------------------------
    eligible_df = get_eligible_rows(batch_start_date)

    if eligible_df is None or eligible_df.count() == 0:
        logger.info("[MultiRefresh] No eligible tables for this iteration.")
    else:
        eligible_count = eligible_df.count()
        logger.info(f"[MultiRefresh] {eligible_count} eligible rows found.")

        # -- Step 2: Write to temp table ---------------------------------------
        (
            eligible_df
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(ELIGIBLE_TEMP_TABLE)
        )

        # -- Step 3: Apply MERGE status updates --------------------------------
        apply_merge_updates(batch_start_date)

        # -- Step 4: Trigger source ingestion jobs (fire & forget) -------------
        trigger_source_jobs(batch_start_date, job_trigger)

        # -- Cleanup temp table ------------------------------------------------
        try:
            spark.sql(f"DROP TABLE IF EXISTS {ELIGIBLE_TEMP_TABLE}")
        except Exception:
            pass

    # -- Step 5: Check completion and compute wait time ---------------------
    is_completed, wait_time = get_completion_status(batch_start_date)

    logger.info(
        f"[MultiRefresh] is_completed={is_completed}  wait_time={wait_time}s"
    )

    if is_completed == 1:
        logger.info(
            "[MultiRefresh] All refreshes completed for today. Exiting orchestrator."
        )
        dbutils.notebook.exit(
            f"Multi-refresh complete after {iteration} iterations. "
            f"All tables processed for {batch_start_date.strftime('%Y-%m-%d')}."
        )

    # -- Step 6: Also exit if we crossed midnight ---------------------------
    current_date = datetime.now(IST).date()
    if current_date > batch_start_date.date():
        logger.info("[MultiRefresh] Day boundary crossed. Exiting.")
        dbutils.notebook.exit("Day boundary crossed - orchestrator exiting.")

    logger.info(f"[MultiRefresh] Sleeping {wait_time}s until next refresh window...")
    time.sleep(wait_time)

# -- Safety exit after max_iterations ------------------------------------------
logger.warning(
    f"[MultiRefresh] Reached max_iterations={max_iterations}. Force-exiting."
)
dbutils.notebook.exit(
    f"Multi-refresh orchestrator exited after max_iterations={max_iterations}."
)
