# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FinnOne Prod Replica Recon Check — Orchestrator
# MAGIC
# MAGIC **Single-notebook equivalent of `PL_Finnone_Prod_Replica_Recon_Check` (ADF).**
# MAGIC
# MAGIC Runs in sequence after `dd_validation` (a separate run_job_task in the job graph):
# MAGIC
# MAGIC 1. **Recon Part 1** — calls the PFL recon notebook (`code_part=1`) to get the Oracle
# MAGIC    replica SQL query. Derives `file_present` (1 if query returned, 0 otherwise).
# MAGIC 2. **Oracle Copy** *(only when `file_present == "1"`)* — reads from Oracle via JDBC
# MAGIC    using the `replica_query` returned above, writes row-count Parquet to s3.
# MAGIC    Replaces the ADF `CP_Get_Replica_Tables_Row_Count` Copy activity.
# MAGIC    Oracle credentials + `raw_s3_bucket` (base s3 path) come from
# MAGIC    `tb_source_connection_config` via `ConfigManager.get_source_system()`.
# MAGIC 3. **Recon Part 2** — calls the same PFL recon notebook (`code_part=2`) with all
# MAGIC    parameters the notebook expects (`file_present`, `raw_sa_name`, etc.).
# MAGIC 4. **Trigger Main Job** — fires the existing FinnOne ingestion job
# MAGIC    (`generic_source_to_bronze_ingestion`) via the Databricks REST API
# MAGIC    (`POST /api/2.1/jobs/runs/now`). Fire-and-forget — does **not** wait.
# MAGIC 5. **Poll Until Complete** — queries `tb_sourcedb_ingestion_sink_config` every
# MAGIC    900 s. Sends a progress email via `GraphMailNotifier` whenever `completed_count`
# MAGIC    increases. Exits when `completed_count == total_count`.
# MAGIC
# MAGIC **ADF activities mapped here:**
# MAGIC `SV_Batch_Start_Date`, `NB_Get_Repilca_Query`, `IF_Source_Query_Available`,
# MAGIC `SV_file_present / SV_file_not_present`, `CP_Get_Replica_Tables_Row_Count`,
# MAGIC `NB_Finnone_Prod_Replica_Recon_Check`, `PL_Finnone_Main (waitOnCompletion=false)`,
# MAGIC `LK_Get_Total_Dependent_Table_Count`, `LK_Get_Total_Success_Table_Count`,
# MAGIC `SV_Get_Until_Completion_Status`, `SV_Get_Email_Status`, `SV_Previous_Completed_Count`,
# MAGIC `Wait (900 s)`, `If_Send_Success_Notification`, `Get_Main_Pipeline_Details`.
# MAGIC
# MAGIC **ADF inactive activities skipped:**
# MAGIC `PL_Finnone_Account_Report_Replica`, `LK_Get_Total_Dependent_Table_Count_1`,
# MAGIC `LK_Get_Total_Success_Table_Count_1`, `Get_Main_Pipeline_Details_1`.

# COMMAND ----------

import json
import sys
import time
from datetime import datetime, timezone

import requests

sys.path.append("..")

from ingestion.connectors.jdbc_connector import _build_url
from ingestion.utils.config_manager import (
    CONFIG_MASTER_TABLE,
    SOURCE_SYSTEM_TABLE,
    ConfigManager,
    get_pipeline_notification_recipients,
)
from ingestion.utils.email_notifier import GraphMailNotifier
from ingestion.utils.secrets import SecretResolver

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text(
    "batch_start_date",
    "1",
    "Batch Start Date (IST timestamp 'yyyy-MM-dd HH:mm:ss.f' or '1' for utcNow)",
)
dbutils.widgets.text(
    "source_system_id",
    "",
    "Source System ID — FinnOne Oracle replica row in tb_source_connection_config",
)
dbutils.widgets.text(
    "admin_catalog_name",
    "",
    "Admin catalog name (e.g. pfl_admin_catalog) — used for recon count queries",
)
dbutils.widgets.text(
    "main_job_id",
    "",
    "Databricks Job ID of the existing FinnOne ingestion job (generic_source_to_bronze_ingestion)",
)
dbutils.widgets.text("environment", "prod", "Environment: dev | uat | prod")
dbutils.widgets.text(
    "recon_notebook_path",
    "/PFL/Admin/Config/Data_Reconciliation/finnone_kkk_replica_recon_check_by_adf",
    "Workspace path to the PFL recon notebook",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolve inputs & batch_start_date

# COMMAND ----------

source_system_id_raw = dbutils.widgets.get("source_system_id") or None
if not source_system_id_raw:
    dbutils.notebook.exit("Error: source_system_id is required.")

admin_catalog_name = dbutils.widgets.get("admin_catalog_name") or None
if not admin_catalog_name:
    dbutils.notebook.exit("Error: admin_catalog_name is required.")

main_job_id_raw = dbutils.widgets.get("main_job_id") or None
if not main_job_id_raw:
    dbutils.notebook.exit("Error: main_job_id is required.")

source_system_id   = int(source_system_id_raw)
main_job_id        = int(main_job_id_raw)
environment        = dbutils.widgets.get("environment") or "prod"
recon_notebook_path = dbutils.widgets.get("recon_notebook_path")

batch_start_date_raw = dbutils.widgets.get("batch_start_date") or "1"

# Resolve batch_start_date.
# "1"  → fresh run: generate current IST timestamp (mirrors ADF SV_Batch_Start_Date).
# else → replay: use the given value verbatim.
if batch_start_date_raw.strip() == "1":
    batch_start_date = (
        datetime.now(timezone.utc)
        .astimezone()             # local TZ on the cluster
        .strftime("%Y-%m-%d %H:%M:%S.%f")
    )
else:
    batch_start_date = batch_start_date_raw.strip()

print(f"batch_start_date : {batch_start_date}")
print(f"environment      : {environment}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load source system config
# MAGIC
# MAGIC `ConfigManager.get_source_system()` fetches the FinnOne Oracle row from
# MAGIC `tb_source_connection_config`. This gives us:
# MAGIC - Oracle credentials via `secret_scope` / `secret_key_credentials`
# MAGIC - `raw_bucket_path` → used as `raw_s3_bucket` (base s3 path for Parquet output)

# COMMAND ----------

config_mgr = ConfigManager(
    spark,
    source_system_table=SOURCE_SYSTEM_TABLE,
    config_master_table=CONFIG_MASTER_TABLE,
)
source_sys = config_mgr.get_source_system(source_system_id)
print(f"Source           : {source_sys.source_name} ({source_sys.source_type})")
print(f"raw_bucket_path  : {source_sys.raw_bucket_path}")

# raw_s3_bucket — base s3/S3 path for the Parquet copy output.
raw_s3_bucket = (source_sys.raw_bucket_path or "").rstrip("/")
if not raw_s3_bucket:
    dbutils.notebook.exit(
        "Error: raw_bucket_path is not configured in tb_source_connection_config "
        f"for source_system_id={source_system_id}. "
        "Set it to the S3 base path, e.g. s3://pfl-raw/finnone-replica"
    )

print(f"raw_s3_bucket    : {raw_s3_bucket}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1 — Recon Part 1 (`NB_Get_Repilca_Query`, code_part=1)
# MAGIC
# MAGIC Calls the PFL recon notebook with `code_part=1`.
# MAGIC The notebook returns a JSON string via `dbutils.notebook.exit()`.
# MAGIC We parse `replica_query` from the output and derive `file_present`.

# COMMAND ----------

print("[Recon Part 1] Running recon notebook (code_part=1) ...")

recon_part1_raw = dbutils.notebook.run(
    recon_notebook_path,
    timeout_seconds=43200,  # 12 h — mirrors ADF activity timeout
    arguments={
        "code_part":    "1",
        "trigger_time": batch_start_date,
    },
)

# Parse output — the notebook exits with json.dumps({"replica_query": "<sql>", ...})
try:
    recon_part1_output = json.loads(recon_part1_raw)
    replica_query = recon_part1_output.get("replica_query", "")
except (json.JSONDecodeError, TypeError):
    # Fallback: treat raw output as the query itself (non-JSON exit)
    replica_query = recon_part1_raw or ""

# file_present: "1" if a non-empty replica_query was returned, "0" otherwise.
# Mirrors ADF: IF_Source_Query_Available → SV_file_present / SV_file_not_present.
file_present = "1" if replica_query.strip() else "0"

print(f"file_present     : {file_present}")
print(f"replica_query length : {len(replica_query)} chars")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2 — Oracle Copy (`CP_Get_Replica_Tables_Row_Count`)
# MAGIC *(only when `file_present == "1"`)*
# MAGIC
# MAGIC Reads from the Oracle FinnOne replica using `replica_query` (returned by Part 1)
# MAGIC as the JDBC `dbtable` subquery — exactly what ADF's `OracleSource.oracleReaderQuery`
# MAGIC does. Writes the result as Parquet to s3, mirroring the ADF Copy activity sink.
# MAGIC
# MAGIC Oracle JDBC credentials are resolved via `SecretResolver` using the scope/key
# MAGIC from `tb_source_connection_config` (same credential convention as all other RDBMS
# MAGIC sources in the framework).

# COMMAND ----------

if file_present == "1":
    print("[Oracle Copy] Reading from Oracle replica via JDBC ...")

    secrets  = SecretResolver(dbutils)
    username, password = secrets.get_credentials(
        source_sys.secret_scope, source_sys.secret_key_credentials
    )

    # Build JDBC URL using the shared helper from jdbc_connector.py.
    # For Oracle, _build_url uses oracle_connect_type=service by default:
    #   jdbc:oracle:thin:@//<host>:<port>/<service_name>
    oracle_url = _build_url(
        source_type   = source_sys.source_type,   # "ORACLE"
        host          = source_sys.host,
        port          = source_sys.port or 0,
        database_name = source_sys.database_name or "",
        extra_params  = {},
    )

    # Use replica_query as the JDBC dbtable subquery.
    # This is the direct Databricks equivalent of ADF's oracleReaderQuery option.
    dbtable = f"({replica_query}) _recon_src"

    df_replica = (
        spark.read.format("jdbc")
        .option("url",          oracle_url)
        .option("user",         username)
        .option("password",     password)
        .option("driver",       "oracle.jdbc.OracleDriver")
        .option("dbtable",      dbtable)
        .option("queryTimeout", "7200")     # 2 h — mirrors ADF queryTimeout: "02:00:00"
        .load()
    )

    row_count = df_replica.count()
    print(f"[Oracle Copy] Rows read: {row_count}")

    # Build s3 output path — mirrors the ADF ParquetSink exactly:
    #   filePath : recon_check/all_table/archive/{yyyy-MM-dd}
    #   fileName : all_table_replica_count_{yyyy_MM_dd_HH_mm_ss}.parquet
    _bsd_dt     = datetime.fromisoformat(batch_start_date.split(".")[0].replace("T", " "))
    archive_date = _bsd_dt.strftime("%Y-%m-%d")
    file_ts      = _bsd_dt.strftime("%Y_%m_%d_%H_%M_%S")

    output_path = (
        f"{raw_s3_bucket}/"
        f"recon_check/all_table/archive/{archive_date}/"
        f"all_table_replica_count_{file_ts}.parquet"
    )
    print(f"[Oracle Copy] Writing Parquet → {output_path}")
    df_replica.write.mode("overwrite").parquet(output_path)
    print("[Oracle Copy] Write complete.")

else:
    print("[Oracle Copy] Skipped — replica_query was empty (file_present=0).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3 — Recon Part 2 (`NB_Finnone_Prod_Replica_Recon_Check`, code_part=2)
# MAGIC
# MAGIC Calls the same PFL recon notebook with `code_part=2`.
# MAGIC All parameters match the ADF `NB_Finnone_Prod_Replica_Recon_Check` activity.

# COMMAND ----------

print("[Recon Part 2] Running recon notebook (code_part=2) ...")

dbutils.notebook.run(
    recon_notebook_path,
    timeout_seconds=43200,  # 12 h
    arguments={
        "code_part":    "2",
        "trigger_time": batch_start_date,
        "raw_s3_bucket": raw_s3_bucket,         # S3 base path (replaces container_name + raw_sa_name)
        "schema_name":  "finrep_tab_neo_cas_lms",
        "file_present": file_present,
    },
)

print("[Recon Part 2] Complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4 — Trigger Main Ingestion Job (fire-and-forget)
# MAGIC
# MAGIC Fires the existing `generic_source_to_bronze_ingestion` Databricks job via the
# MAGIC Jobs REST API. `waitOnCompletion=false` in ADF → we do **not** wait for the
# MAGIC triggered run to finish. Only `batch_start_date` is passed as a parameter so
# MAGIC the main job's `get_tasks` step can stamp and filter by the same batch timestamp.

# COMMAND ----------

print("[Trigger Main] Firing FinnOne ingestion job via REST API (fire-and-forget) ...")

_ctx   = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_token = _ctx.apiToken().get()
_host  = _ctx.apiUrl().get()

_trigger_resp = requests.post(
    f"{_host}/api/2.1/jobs/runs/now",
    headers={
        "Authorization":  f"Bearer {_token}",
        "Content-Type":   "application/json",
    },
    json={
        "job_id": main_job_id,
        "notebook_params": {"batch_start_date": batch_start_date},
    },
    timeout=30,
)
_trigger_resp.raise_for_status()

triggered_run_id = _trigger_resp.json()["run_id"]
print(f"[Trigger Main] Job triggered — run_id={triggered_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5 — Poll Until All FinnOne Tables Complete (ADF `Until` loop)
# MAGIC
# MAGIC Mirrors the ADF `Until` loop with its inner activities:
# MAGIC - `LK_Get_Total_Dependent_Table_Count` — total active FinnOne tables in config
# MAGIC - `LK_Get_Total_Success_Table_Count` — tables whose `sink_batch_started_date` date
# MAGIC   matches today's batch
# MAGIC - `SV_Get_Email_Status` / `If_Send_Success_Notification` — progress email when count moves
# MAGIC - `SV_Get_Until_Completion_Status` — break when total == completed
# MAGIC - `Wait (900 s)` — sleep between iterations

# COMMAND ----------

notifier = GraphMailNotifier(dbutils=dbutils)

# Recipients from tb_pipeline_master_config where Pipeline_Name = 'PL_Finnone_Main'
# (mirrors Get_Main_Pipeline_Details notebook in ADF)
success_recipients, _ = get_pipeline_notification_recipients(spark, "PL_Finnone_Main")

batch_date = batch_start_date[:10]   # yyyy-MM-dd portion

# Total active FinnOne tables that participate in recon
# (mirrors LK_Get_Total_Dependent_Table_Count)
_total_count_sql = f"""
    SELECT count(Config_ID) AS total_count
    FROM {admin_catalog_name}.config.tb_sourcedb_ingestion_sink_config a
    JOIN {admin_catalog_name}.config.tb_table_rec_cnt_kkk_config b
      ON  a.Source_Schema_Name = b.TABLE_OWNER
     AND  a.Source_Table_Name  = b.TABLE_NAME
     AND  b.Is_Active          = 1
    WHERE a.Source_Name = 'FinnOne'
      AND a.Tool_Name   = 'ADF'
      AND a.Is_Active   = 1
"""

# Tables that have been ingested for today's batch
# (mirrors LK_Get_Total_Success_Table_Count)
_completed_count_sql = f"""
    SELECT count(Config_ID) AS completed_count
    FROM {admin_catalog_name}.config.tb_sourcedb_ingestion_sink_config a
    JOIN {admin_catalog_name}.config.tb_table_rec_cnt_kkk_config b
      ON  a.Source_Schema_Name = b.TABLE_OWNER
     AND  a.Source_Table_Name  = b.TABLE_NAME
     AND  b.Is_Active          = 1
    WHERE a.Source_Name = 'FinnOne'
      AND a.Tool_Name   = 'ADF'
      AND a.Is_Active   = 1
      AND to_date(sink_batch_started_date) = '{batch_date}'
"""

total_count    = int(spark.sql(_total_count_sql).first()["total_count"])
previous_count = 0   # mirrors ADF variable 'previous_count'

print(f"[Poll] Total tables to complete: {total_count}")
print(f"[Poll] Polling every 900 s until completed == {total_count} ...")

while True:
    completed_count = int(spark.sql(_completed_count_sql).first()["completed_count"])
    print(f"[Poll] completed={completed_count} / {total_count}")

    # SV_Get_Email_Status: send email if completed_count has increased since last check
    if completed_count > previous_count:
        now_ist = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")  # UTC (cluster may not have pytz)
        notifier.send_email(
            subject=(
                f"{environment}: Daily Recon for FinnOne_ADF Tables on {batch_date}"
            ),
            body=(
                f"<p>Hello Team,</p>"
                f"<p>Recon validation successfully completed for "
                f"{completed_count}/{total_count} tables on {now_ist}.</p>"
                f"<p>Regards,</p><p>Data Lake Team</p>"
            ),
            recipients=success_recipients,
        )
        previous_count = completed_count   # SV_Previous_Completed_Count

    # SV_Get_Until_Completion_Status: exit when all tables done
    if completed_count >= total_count:
        print(f"[Poll] All {total_count} tables complete — exiting poll loop.")
        break

    # Wait 900 s before next check (mirrors ADF Wait activity: waitTimeInSeconds=900)
    print("[Poll] Sleeping 900 s ...")
    time.sleep(900)

# COMMAND ----------

dbutils.notebook.exit(
    f"SUCCESS: FinnOne recon orchestration complete — "
    f"{total_count}/{total_count} tables ingested for batch_date={batch_date}."
)
