# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # FinnOne KKK Load — Orchestrator
# MAGIC
# MAGIC **Single-notebook equivalent of `PL_Finnone_Recon_KKK_Load` (ADF).**
# MAGIC
# MAGIC Loads `TAB_NEO_CAS_LMS.TABLE_REC_CNT_KKK` (the FinnOne Oracle row-count
# MAGIC reference table) from Oracle → raw Parquet → Silver, polling every 600 s until
# MAGIC all expected FinnOne tables have appeared in the KKK table for today's batch.
# MAGIC
# MAGIC **Flow:**
# MAGIC 1. **Setup** — fetch `total_count` + KKK config row (`Config_ID`, `Sink_Table_Name`,
# MAGIC    `Sink_Schema_Name`, `Config_Master_ID`) from `tb_sourcedb_ingestion_sink_config`.
# MAGIC    Oracle credentials + `raw_s3_bucket` (S3 base path) come from
# MAGIC    `tb_source_connection_config` via `ConfigManager.get_source_system()`.
# MAGIC 2. **Loop (until `completed_count == total_count`):**
# MAGIC    a. **Oracle Copy** — read today's KKK rows via JDBC, write Parquet to S3.
# MAGIC       Replaces `CP_Load_KKK_Table_Source_To_Raw`.
# MAGIC    b. **Silver notebook** — `dbutils.notebook.run()` calls the existing PFL notebook
# MAGIC       `/PFL/Delta-Lake/Silver/Finnone/load_kkk_raw_to_silver`.
# MAGIC       Returns `row_count`, `completed_count`, `status`, `startDate`, `endDate`,
# MAGIC       `copyDurationInSec`, `errorMessage`.
# MAGIC    c. **Audit** — writes one record to `tb_audit_log` via `AuditLogger`.
# MAGIC       Replaces `EX_PL_Audit_Log_Insertion`.
# MAGIC    d. **Exit check** — if `completed_count >= total_count` → break.
# MAGIC    e. **Wait 600 s** before next iteration.
# MAGIC 3. **Email** — sends completion email via `GraphMailNotifier`.
# MAGIC    Replaces `PL_Email_Notification_Basic_SHIR`.
# MAGIC
# MAGIC **ADF disabled activities skipped:**
# MAGIC `LK_Get_Total_Dependent_Table_Count_1`, `LK_Get_KKK_Config_Details_1`.

# COMMAND ----------

import json
import sys
import time
from datetime import datetime, timezone

sys.path.append("..")

from ingestion.connectors.jdbc_connector import _build_url
from ingestion.utils.audit import AuditLogger
from ingestion.utils.config_manager import (
    AUDIT_TABLE,
    CONFIG_MASTER_TABLE,
    SOURCE_SYSTEM_TABLE,
    ConfigManager,
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
    "Admin catalog name (e.g. pfl_admin_catalog)",
)
dbutils.widgets.text(
    "silver_catalog_name",
    "",
    "Silver catalog name — prefixed to Sink_Schema_Name for the audit target_schema",
)
dbutils.widgets.text(
    "silver_notebook_path",
    "/PFL/Delta-Lake/Silver/Finnone/load_kkk_raw_to_silver",
    "Workspace path to the PFL KKK raw-to-silver notebook",
)
dbutils.widgets.text(
    "email_to",
    "divyanshu.rathore@poonawallafincorp.com",
    "Completion notification recipient(s) — comma-separated",
)
dbutils.widgets.text("environment", "prod", "Environment: dev | uat | prod")
dbutils.widgets.text("job_run_id", "", "Job Run ID — set to {{job.run_id}} in job config")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolve inputs

# COMMAND ----------

source_system_id_raw = dbutils.widgets.get("source_system_id") or None
if not source_system_id_raw:
    dbutils.notebook.exit("Error: source_system_id is required.")

admin_catalog_name = dbutils.widgets.get("admin_catalog_name") or None
if not admin_catalog_name:
    dbutils.notebook.exit("Error: admin_catalog_name is required.")

job_run_id_raw = dbutils.widgets.get("job_run_id") or None
if not job_run_id_raw:
    dbutils.notebook.exit("Error: job_run_id is required.")

source_system_id   = int(source_system_id_raw)
silver_catalog_name = dbutils.widgets.get("silver_catalog_name") or None
silver_notebook_path = dbutils.widgets.get("silver_notebook_path")
environment        = dbutils.widgets.get("environment") or "prod"
job_run_id         = job_run_id_raw

# email_to: comma-separated string → list
email_to_raw = dbutils.widgets.get("email_to") or ""
email_recipients = [e.strip() for e in email_to_raw.split(",") if e.strip()]

batch_start_date_raw = dbutils.widgets.get("batch_start_date") or "1"

# Resolve batch_start_date
if batch_start_date_raw.strip() == "1":
    batch_start_date = (
        datetime.now(timezone.utc)
        .astimezone()
        .strftime("%Y-%m-%d %H:%M:%S.%f")
    )
else:
    batch_start_date = batch_start_date_raw.strip()

batch_date = batch_start_date[:10]   # yyyy-MM-dd portion (IST today)

print(f"batch_start_date : {batch_start_date}")
print(f"batch_date       : {batch_date}")
print(f"environment      : {environment}")
print(f"job_run_id       : {job_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load source system config + KKK config row
# MAGIC
# MAGIC **Source system config** (`tb_source_connection_config`) drives:
# MAGIC - Oracle JDBC credentials
# MAGIC - `raw_s3_bucket` (base S3 path, from `raw_bucket_path`)
# MAGIC
# MAGIC **KKK config row** (`tb_sourcedb_ingestion_sink_config`) drives:
# MAGIC - `Config_ID` → audit table_id + completion SQL filter
# MAGIC - `Sink_Table_Name`, `Sink_Schema_Name` → Silver notebook params
# MAGIC - `Config_Master_ID` → audit config_master_id

# COMMAND ----------

config_mgr = ConfigManager(
    spark,
    source_system_table=SOURCE_SYSTEM_TABLE,
    config_master_table=CONFIG_MASTER_TABLE,
)
source_sys = config_mgr.get_source_system(source_system_id)
print(f"Source           : {source_sys.source_name} ({source_sys.source_type})")
print(f"raw_bucket_path  : {source_sys.raw_bucket_path}")

raw_s3_bucket = (source_sys.raw_bucket_path or "").rstrip("/")
if not raw_s3_bucket:
    dbutils.notebook.exit(
        f"Error: raw_bucket_path not set in tb_source_connection_config "
        f"for source_system_id={source_system_id}. "
        "Set it to the S3 base path, e.g. s3://pfl-raw/finnone-replica"
    )

print(f"raw_s3_bucket    : {raw_s3_bucket}")

# ── LK_Get_KKK_Config_Details (active DatabricksNotebook in ADF) ──────────────
# In ADF this calls the PFL get_lookup_details notebook with a config_query.
# Here we query the Unity Catalog table directly — same SQL, no extra notebook hop.
_kkk_config_sql = f"""
    SELECT Config_ID, Source_Name, Sink_Table_Name, Sink_Schema_Name, Config_Master_ID
    FROM {admin_catalog_name}.config.tb_sourcedb_ingestion_sink_config
    WHERE Source_Name = 'FinnOne'
      AND Tool_Name   = 'ADF'
      AND Is_Active   = 1
      AND Source_Table_Name = 'TABLE_REC_CNT_KKK'
"""
_kkk_cfg_row = spark.sql(_kkk_config_sql).first()
if not _kkk_cfg_row:
    dbutils.notebook.exit(
        "Error: no active KKK config row found in tb_sourcedb_ingestion_sink_config "
        "for Source_Name='FinnOne', Tool_Name='ADF', Source_Table_Name='TABLE_REC_CNT_KKK'."
    )

kkk_config_id      = int(_kkk_cfg_row["Config_ID"])
kkk_source_name    = str(_kkk_cfg_row["Source_Name"])
kkk_sink_table     = str(_kkk_cfg_row["Sink_Table_Name"])
kkk_sink_schema    = str(_kkk_cfg_row["Sink_Schema_Name"])
kkk_config_master  = int(_kkk_cfg_row["Config_Master_ID"]) if _kkk_cfg_row["Config_Master_ID"] else None

print(f"KKK Config_ID       : {kkk_config_id}")
print(f"KKK Sink_Schema     : {kkk_sink_schema}")
print(f"KKK Sink_Table      : {kkk_sink_table}")
print(f"KKK Config_Master_ID: {kkk_config_master}")

# ── LK_Get_Total_Dependent_Table_Count (active, queried ONCE before loop) ─────
# Mirrors: select count(Config_ID) as total_count from ... join tb_table_rec_cnt_kkk_config
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
total_count = int(spark.sql(_total_count_sql).first()["total_count"])
print(f"\nTotal KKK tables expected : {total_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build Oracle JDBC read options (shared across loop iterations)
# MAGIC
# MAGIC Credentials are resolved once using `SecretResolver` — same convention as all
# MAGIC other RDBMS sources in the framework. URL is built via the shared `_build_url`
# MAGIC helper from `jdbc_connector.py`.

# COMMAND ----------

secrets  = SecretResolver(dbutils)
username, password = secrets.get_credentials(
    source_sys.secret_scope, source_sys.secret_key_credentials
)
oracle_url = _build_url(
    source_type   = source_sys.source_type,   # "ORACLE"
    host          = source_sys.host,
    port          = source_sys.port or 0,
    database_name = source_sys.database_name or "",
    extra_params  = {},
)
print(f"Oracle URL (no creds): {oracle_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Poll Loop — Oracle Copy → Silver → Audit (repeats every 600 s)
# MAGIC
# MAGIC Each iteration:
# MAGIC 1. Read today's KKK rows from Oracle via JDBC (`CP_Load_KKK_Table_Source_To_Raw`)
# MAGIC 2. Write to S3 as Parquet
# MAGIC 3. Call PFL Silver notebook (`NB_Load_KKK_Raw_To_Silver`)
# MAGIC 4. Write audit record (`EX_PL_Audit_Log_Insertion` → `AuditLogger`)
# MAGIC 5. Check `completed_count == total_count` → exit if done (`SV_Status`)
# MAGIC 6. Wait 600 s (`Wait` activity) — only if not done yet

# COMMAND ----------

audit_logger = AuditLogger(spark, AUDIT_TABLE)

# Databricks job context — needed by AuditLogger
_nb_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
try:
    _job_id = dbutils.widgets.get("job_id")
except Exception:
    _job_id = None

job_context = {
    "job_run_id":      job_run_id,
    "job_id":          _job_id,
    "trigger_type":    "SCHEDULED",
    "trigger_id":      job_run_id,
    "trigger_name":    "PL_Finnone_Recon_KKK_Load",
    "notebook_name":   _nb_ctx.notebookPath().get() if _nb_ctx else None,
    "databricks_url":  None,
}

iteration  = 0
completed_count = 0

while True:
    iteration += 1
    iter_start = datetime.now(timezone.utc)
    print(f"\n{'='*70}")
    print(f"[Iteration {iteration}] Started at {iter_start.isoformat()}")
    print(f"{'='*70}")

    # ── 1. Oracle Copy (CP_Load_KKK_Table_Source_To_Raw) ─────────────────────
    # Fixed query: today's KKK rows deduplicated by (TABLE_OWNER, TABLE_NAME).
    # formatDateTime(convertTimeZone(utcNow(),'UTC','India Standard Time'),'dd-MMM-yy HH:mm:ss')
    # In Python we use batch_date (yyyy-MM-dd from IST) reformatted to Oracle's DD-MON-YY.
    from datetime import datetime as _dt
    _bsd_dt = _dt.fromisoformat(batch_start_date.split(".")[0].replace("T", " "))
    oracle_date_str = _bsd_dt.strftime("%d-%b-%y %H:%M:%S").upper()   # e.g. 18-SEP-26 00:00:00

    oracle_reader_query = (
        f"SELECT TABLE_OWNER, TABLE_NAME, TABLE_COUNT, "
        f"TO_DATE(COUNT_TIME, 'DD-MON-YY HH24:MI:SS') AS COUNT_TIME "
        f"FROM ("
        f"  SELECT TABLE_OWNER, TABLE_NAME, TABLE_COUNT, "
        f"         TO_DATE(COUNT_TIME, 'DD-MON-YY HH24:MI:SS') AS COUNT_TIME, "
        f"         row_number() over("
        f"           PARTITION BY TABLE_OWNER, TABLE_NAME "
        f"           ORDER BY TO_DATE(COUNT_TIME, 'DD-MON-YY HH24:MI:SS') DESC"
        f"         ) rn "
        f"  FROM TAB_NEO_CAS_LMS.TABLE_REC_CNT_KKK trck "
        f"  WHERE 1=1 "
        f"    AND TRUNC(TO_DATE(COUNT_TIME, 'DD-MON-YY HH24:MI:SS')) "
        f"        = TRUNC(TO_DATE('{oracle_date_str}', 'DD-MON-YY HH24:MI:SS'))"
        f") WHERE rn = 1"
    )

    print(f"[Oracle Copy] Query length: {len(oracle_reader_query)} chars")

    oracle_copy_start = datetime.now(timezone.utc)
    rows_read = 0
    oracle_copy_ok = False
    try:
        df_kkk = (
            spark.read.format("jdbc")
            .option("url",          oracle_url)
            .option("user",         username)
            .option("password",     password)
            .option("driver",       "oracle.jdbc.OracleDriver")
            .option("dbtable",      f"({oracle_reader_query}) _kkk_src")
            .option("queryTimeout", "7200")   # 2 h — mirrors ADF queryTimeout: "02:00:00"
            .load()
        )
        rows_read = df_kkk.count()
        print(f"[Oracle Copy] Rows read: {rows_read}")

        # S3 output path:
        #   containerName : finnone-replica  (embedded in raw_s3_bucket)
        #   filePath      : TAB_NEO_CAS_LMS/TABLE_REC_CNT_KKK/archive/{yyyy-MM-dd}
        #   fileName      : TAB_NEO_CAS_LMS_TABLE_REC_CNT_KKK_{yyyy_MM_dd}.parquet
        archive_date = _bsd_dt.strftime("%Y-%m-%d")
        file_date    = _bsd_dt.strftime("%Y_%m_%d")
        output_path  = (
            f"{raw_s3_bucket}/"
            f"TAB_NEO_CAS_LMS/TABLE_REC_CNT_KKK/archive/{archive_date}/"
            f"TAB_NEO_CAS_LMS_TABLE_REC_CNT_KKK_{file_date}.parquet"
        )
        print(f"[Oracle Copy] Writing Parquet → {output_path}")
        df_kkk.write.mode("overwrite").parquet(output_path)
        oracle_copy_ok = True
        print("[Oracle Copy] Write complete.")
    except Exception as copy_exc:
        print(f"[Oracle Copy] FAILED: {copy_exc}")
        # Write FAILED audit row and continue loop (mirrors ADF retry=0 + continue)
        audit_logger.complete_run(
            audit_run={"job_run_id": job_run_id, "table_id": kkk_config_id},
            status="FAILED",
            rows_read=0,
            error_code="ORACLE_COPY_FAILED",
            error_message=str(copy_exc)[:2000],
        )
        # Wait before retrying
        print(f"[Oracle Copy] Waiting 600 s before retry ...")
        time.sleep(600)
        continue

    # ── 2. Silver Notebook (NB_Load_KKK_Raw_To_Silver) ───────────────────────
    # Calls the existing PFL notebook with the same parameters as the ADF
    # DatabricksNotebook activity. The folder_path mirrors the ADF expression:
    # concat('TAB_NEO_CAS_LMS/TABLE_REC_CNT_KKK/archive/{date}/{file_name}')
    silver_folder_path = (
        f"TAB_NEO_CAS_LMS/TABLE_REC_CNT_KKK/archive/{archive_date}/"
        f"TAB_NEO_CAS_LMS_TABLE_REC_CNT_KKK_{file_date}.parquet"
    )

    print(f"[Silver] Running silver notebook: {silver_notebook_path}")
    silver_start = datetime.now(timezone.utc)
    silver_ok    = False
    silver_output = {}
    silver_error  = None
    try:
        silver_raw = dbutils.notebook.run(
            silver_notebook_path,
            timeout_seconds=43200,   # 12 h
            arguments={
                "key_column":       "TABLE_OWNER,TABLE_NAME,COUNT_TIME",
                "raw_s3_bucket":    raw_s3_bucket,  # S3 base path
                "folder_path":      silver_folder_path,
                "sink_table_name":  kkk_sink_table,
                "sink_schema_name": kkk_sink_schema,
            },
        )
        # Silver notebook exits with json.dumps({
        #   "row_count": <int>,
        #   "completed_count": <int>,
        #   "status": "Succeeded" | "Failed",
        #   "startDate": "<iso>",
        #   "endDate": "<iso>",
        #   "copyDurationInSec": <float>,
        #   "errorMessage": ""
        # })
        try:
            silver_output = json.loads(silver_raw)
        except (json.JSONDecodeError, TypeError):
            silver_output = {"status": "Succeeded", "row_count": 0, "completed_count": 0}
        silver_ok = silver_output.get("status", "").lower() == "succeeded"
        completed_count = int(silver_output.get("completed_count", 0))
        print(f"[Silver] status={silver_output.get('status')}  "
              f"row_count={silver_output.get('row_count')}  "
              f"completed_count={completed_count}/{total_count}")
    except Exception as silver_exc:
        silver_error = str(silver_exc)
        print(f"[Silver] FAILED: {silver_error}")

    silver_end = datetime.now(timezone.utc)

    # ── 3. Audit (EX_PL_Audit_Log_Insertion → AuditLogger) ───────────────────
    # Write a single completed audit record — no start_run/complete_run split
    # because the KKK pipeline writes one record per iteration once the silver
    # notebook finishes, not around a long-running JDBC driver pull.
    target_schema_full = (
        f"{silver_catalog_name}.{kkk_sink_schema}"
        if silver_catalog_name
        else kkk_sink_schema
    )
    audit_status = "SUCCESS" if (oracle_copy_ok and silver_ok) else "FAILED"
    audit_error  = silver_error or (None if silver_ok else silver_output.get("errorMessage"))

    try:
        # Build a minimal object with the attributes AuditLogger.start_run expects.
        # We skip start_run and write a terminal row directly to avoid leaving
        # INPROGRESS rows when the loop iterates multiple times.
        from decimal import Decimal
        from pyspark.sql.types import (
            DateType, DecimalType, IntegerType, LongType,
            StringType, StructField, StructType, TimestampType,
        )

        _audit_row = [(
            kkk_config_master if kkk_config_master is not None else kkk_config_id,
            kkk_config_id,
            0,                                  # department_id
            "SILVER",                           # delta_layer
            kkk_source_name,                    # source_name
            "PL_Finnone_Recon_KKK_Load",        # pipeline_name
            "Full",                             # load_type
            "Daily",                            # frequency
            _bsd_dt.date(),                     # business_date
            str(_job_id or "MANUAL"),           # job_id
            str(job_run_id),                    # job_run_id
            "SCHEDULED",                        # trigger_type
            str(job_run_id),                    # trigger_id
            "PL_Finnone_Recon_KKK_Load",        # trigger_name
            silver_start,                       # trigger_time
            silver_end,                         # end_time
            Decimal(str(
                round((silver_end - silver_start).total_seconds(), 2)
            )),                                 # execution_duration_sec
            "TAB_NEO_CAS_LMS",                  # source_schema
            "TABLE_REC_CNT_KKK",                # source_table
            target_schema_full,                 # target_schema
            "table_rec_cnt_kkk",                # target_table
            int(rows_read),                     # rows_read
            int(silver_output.get("row_count", 0)),   # rows_copied
            int(silver_output.get("rows_deleted", 0)),          # rows_deleted
            int(silver_output.get("data_read_bytes", 0)),       # data_read_bytes
            int(silver_output.get("data_written_bytes", 0)),    # data_written_bytes
            None,                               # throughput_mb_per_sec
            Decimal(str(silver_output.get("copyDurationInSec", 0) or 0)),
            "load_kkk_raw_to_silver",           # databricks_notebook_name
            str(silver_output.get("runPageUrl", "UNKNOWN")),
            "INGESTION",                        # operation_performed
            audit_status,                       # Execution_Status
            None if audit_status == "SUCCESS" else "KKK_LOAD_FAILED",
            str(audit_error)[:2000] if audit_error else None,
        )]

        _audit_schema = audit_logger._schema()
        spark.createDataFrame(_audit_row, schema=_audit_schema).writeTo(AUDIT_TABLE).using("delta").append()
        print(f"[Audit] Written — status={audit_status}, rows_read={rows_read}, "
              f"rows_copied={silver_output.get('row_count', 0)}")
    except Exception as audit_exc:
        # Never let an audit failure abort the pipeline
        print(f"[Audit] WARNING — failed to write audit record: {audit_exc}")

    # ── 4. Exit check (SV_Status) ─────────────────────────────────────────────
    if completed_count >= total_count:
        print(f"\n[Loop] All {total_count} KKK tables present — exiting loop.")
        break

    # ── 5. Wait 600 s (Wait activity) — only if not done ─────────────────────
    print(f"[Loop] completed={completed_count} / {total_count} — sleeping 600 s ...")
    time.sleep(600)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Email notification (PL_Email_Notification_Basic_SHIR)

# COMMAND ----------

notifier = GraphMailNotifier(dbutils=dbutils)
notifier.send_email(
    subject=(
        f"{environment} ALERT! PL_Finnone_Recon_KKK_Load Execution Status"
    ),
    body=(
        f"<p>Hello Team,</p>"
        f"<p>TABLE_REC_CNT_KKK loaded successfully onto the silver layer.</p>"
        f"<p>Batch Date : {batch_date}</p>"
        f"<p>Total tables completed : {completed_count}/{total_count}</p>"
        f"<p>Regards,</p><p>Data Lake Team</p>"
    ),
    recipients=email_recipients,
)
print(f"[Email] Sent completion notification to {email_recipients}")

# COMMAND ----------

dbutils.notebook.exit(
    f"SUCCESS: PL_Finnone_Recon_KKK_Load complete — "
    f"{completed_count}/{total_count} KKK tables loaded for batch_date={batch_date}."
)
