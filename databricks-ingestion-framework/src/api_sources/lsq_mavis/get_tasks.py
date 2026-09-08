# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # LSQ Mavis — Task 0: Initialize Config & Fetch Active Tables
# MAGIC
# MAGIC **Replaces two ADF activities from PL_LSQ_Mavis_Main:**
# MAGIC - `LK_Initialize_Ingestion_Status` — resets Status / Day_Execution_Count / sink_batch_started_date
# MAGIC - `LK_Get_Mavis_config_details`    — fetches active rows for this batch
# MAGIC
# MAGIC Publishes results via `taskValues` for Task 1 (`main.py`) to consume.
# MAGIC
# MAGIC **Location:** src/api_sources/lsq_mavis/
# MAGIC **sys.path:** appends `../..` to reach `src/` on the Python path.

# COMMAND ----------

dbutils.widgets.text("batch_start_date",   "1",    "Batch Start Date (1 = today, else yyyy-MM-dd HH:mm:ss in IST)")
dbutils.widgets.text("admin_catalog_name", "",     "Admin catalog name (e.g. pfl_admin_catalog)")
dbutils.widgets.text("environment",        "prod", "Environment: dev | uat | prod")
dbutils.widgets.text("job_run_id",         "",     "Job Run ID (set to {{job.run_id}} in job config)")

# COMMAND ----------

import json
import sys
from datetime import datetime, timezone, timedelta

# Notebooks at src/api_sources/lsq_mavis/ — append ../.. to reach src/ on sys.path
sys.path.append("../..")

batch_start_date   = dbutils.widgets.get("batch_start_date").strip()
admin_catalog_name = dbutils.widgets.get("admin_catalog_name").strip()
environment        = dbutils.widgets.get("environment").strip() or "prod"
job_run_id         = dbutils.widgets.get("job_run_id").strip()

if not admin_catalog_name:
    dbutils.notebook.exit("Error: admin_catalog_name widget is required and cannot be empty.")
if not job_run_id:
    dbutils.notebook.exit("Error: job_run_id widget is required and cannot be empty.")

CONFIG_TABLE = f"{admin_catalog_name}.config.tb_mavis_db_ingestion_config"
SOURCE_NAME  = "LSQ_Mavis"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Resolve trigger time (IST)
# MAGIC
# MAGIC Mirrors ADF `SV_Trigger_Time`:
# MAGIC   - `batch_start_date == '1'`  → current IST time (fresh daily run)
# MAGIC   - else                        → parse the provided IST datetime string (re-run)

# COMMAND ----------

_IST_OFFSET = timedelta(hours=5, minutes=30)

def _utc_now_ist() -> datetime:
    return (datetime.now(timezone.utc) + _IST_OFFSET).replace(tzinfo=None)

def _parse_ist(s: str) -> datetime:
    """Parse a 'yyyy-MM-dd HH:mm:ss' string (already IST) into a naive datetime."""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(s.strip(), fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse batch_start_date='{s}'. Expected 'yyyy-MM-dd HH:mm:ss'.")

if batch_start_date == "1":
    trigger_time_ist = _utc_now_ist()
    trigger_time_utc = datetime.now(timezone.utc)
else:
    trigger_time_ist = _parse_ist(batch_start_date)
    trigger_time_utc = (trigger_time_ist - _IST_OFFSET).replace(tzinfo=timezone.utc)

trigger_ist_str  = trigger_time_ist.strftime("%Y-%m-%d %H:%M:%S")
trigger_date_str = trigger_time_ist.strftime("%Y-%m-%d")

print(f"trigger_time_ist : {trigger_ist_str}")
print(f"trigger_time_utc : {trigger_time_utc.isoformat()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Initialize ingestion status
# MAGIC
# MAGIC Mirrors ADF `LK_Initialize_Ingestion_Status` config_query:
# MAGIC
# MAGIC **Fresh run** (`batch_start_date == '1'`):
# MAGIC   ```sql
# MAGIC   UPDATE tb_mavis_db_ingestion_config
# MAGIC   SET Status='Not-Started', Day_Execution_Count=0,
# MAGIC       sink_batch_started_date='<trigger_ist>'
# MAGIC   WHERE Source_Name='LSQ_Mavis' AND is_active=1
# MAGIC     AND (to_date(sink_batch_started_date) != '<today_ist>' OR sink_batch_started_date IS NULL)
# MAGIC   ```
# MAGIC
# MAGIC **Re-run** (specific date provided):
# MAGIC   ```sql
# MAGIC   UPDATE tb_mavis_db_ingestion_config
# MAGIC   SET Status='Not-Started'
# MAGIC   WHERE Source_Name='LSQ_Mavis' AND is_active=1
# MAGIC     AND date_format(sink_batch_started_date,'yyyy-MM-dd HH:mm:ss') = '<trigger_ist>'
# MAGIC   ```

# COMMAND ----------

if batch_start_date == "1":
    # Fresh daily run — reset all rows not yet started today, stamp with this run's time
    init_sql = f"""
        UPDATE {CONFIG_TABLE}
        SET    Status                  = 'Not-Started',
               Day_Execution_Count    = 0,
               sink_batch_started_date = '{trigger_ist_str}'
        WHERE  Source_Name = '{SOURCE_NAME}'
          AND  is_active   = 1
          AND  (
                 to_date(sink_batch_started_date) != '{trigger_date_str}'
                 OR sink_batch_started_date IS NULL
               )
    """
else:
    # Re-run for a specific date — only reset rows matching that exact timestamp
    init_sql = f"""
        UPDATE {CONFIG_TABLE}
        SET    Status = 'Not-Started'
        WHERE  Source_Name = '{SOURCE_NAME}'
          AND  is_active   = 1
          AND  date_format(sink_batch_started_date, 'yyyy-MM-dd HH:mm:ss')
                 = date_format('{trigger_ist_str}', 'yyyy-MM-dd HH:mm:ss')
    """

print("Executing init SQL:")
print(init_sql)
spark.sql(init_sql)
print("Config status initialised.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Fetch active config rows for this batch
# MAGIC
# MAGIC Mirrors ADF `LK_Get_Mavis_config_details` config_query:
# MAGIC   ```sql
# MAGIC   SELECT * FROM tb_mavis_db_ingestion_config
# MAGIC   WHERE Source_Name='LSQ_Mavis' AND is_active=1
# MAGIC     AND date_format(sink_batch_started_date,'yyyy-MM-dd HH:mm:ss') = '<trigger_ist>'
# MAGIC   ORDER BY Config_ID
# MAGIC   ```

# COMMAND ----------

fetch_sql = f"""
    SELECT *
    FROM   {CONFIG_TABLE}
    WHERE  Source_Name = '{SOURCE_NAME}'
      AND  is_active   = 1
      AND  date_format(sink_batch_started_date, 'yyyy-MM-dd HH:mm:ss')
             = date_format('{trigger_ist_str}', 'yyyy-MM-dd HH:mm:ss')
    ORDER BY Config_ID
"""

print("Fetching active config rows:")
print(fetch_sql)

rows = spark.sql(fetch_sql).collect()
print(f"Found {len(rows)} active config row(s).")

if not rows:
    dbutils.notebook.exit(
        f"No active rows found in {CONFIG_TABLE} for trigger_time='{trigger_ist_str}'. "
        "Nothing to ingest."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Publish via taskValues

# COMMAND ----------

from api_sources.lsq_mavis.config import MavisTableConfig

tasks = [MavisTableConfig.from_row(r.asDict()) for r in rows]

payload = {
    "tasks":             [t.to_dict() for t in tasks],
    "trigger_time_utc":  trigger_time_utc.isoformat(),
    "batch_start_date":  trigger_ist_str,
    "config_table_fqn":  CONFIG_TABLE,
}

dbutils.jobs.taskValues.set(
    key   = "mavis_tasks_metadata",
    value = json.dumps(payload),
)

print(f"Published {len(tasks)} task(s) to taskValues (key='mavis_tasks_metadata').")
for t in tasks:
    print(f"  Config_ID={t.config_id}  Table={t.sink_table_name}  Load={t.load_type}  Batch={t.batch_id}  Priority={t.priority}")

dbutils.notebook.exit(f"get_tasks OK — {len(tasks)} task(s) published.")
