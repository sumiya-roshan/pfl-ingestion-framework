# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Monitor
# MAGIC
# MAGIC Runs as a **parallel Databricks Job task** alongside `main.py` (no dependency).
# MAGIC
# MAGIC **What it does:**
# MAGIC - Fetches the same list of active tables as `main.py` via `ConfigManager`
# MAGIC - Maintains an in-memory dict tracking Bronze done / Silver done per table
# MAGIC - Polls the audit table every `poll_interval_seconds` to detect when a table's
# MAGIC   Bronze ingestion completes (status = SUCCESS)
# MAGIC - As soon as a table finishes Bronze, triggers the Silver Job via Databricks SDK
# MAGIC - Exits cleanly when all tables have been sent to Silver (or timeout is reached)
# MAGIC
# MAGIC **Databricks Job DAG:**
# MAGIC ```
# MAGIC  Task 1: main.py          ──► (Bronze ingestion, parallel tables)
# MAGIC          ↑ NO dependency
# MAGIC  Task 2: silver/monitor   ──► polls audit ──► triggers Silver Job per table
# MAGIC ```

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys
import time
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append("..")

from ingestion.utils.config_manager import (
    ConfigManager,
    SOURCE_SYSTEM_TABLE,
    CONFIG_MASTER_TABLE,
    AUDIT_TABLE,
)
from silver.silver_processor import SilverProcessor

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("config_master_id",       "",             "Config Master ID (same as main.py)")
dbutils.widgets.text("source_system_id",       "",             "Source System ID (same as main.py)")
dbutils.widgets.text("target_catalog",         "hive_metastore","Target Catalog")
dbutils.widgets.text("audit_table",            AUDIT_TABLE,    "Audit Table (override)")
dbutils.widgets.text("silver_notebook_path",   "",             "Workspace path to Silver transformation notebook")
dbutils.widgets.text("silver_notebook_timeout","3600",         "Max seconds to wait for each Silver notebook run")
dbutils.widgets.text("silver_max_workers",     "4",            "Max parallel Silver notebook runs")
dbutils.widgets.text("poll_interval_seconds",  "30",           "Seconds between audit polls")
dbutils.widgets.text("timeout_minutes",        "120",          "Max wait time before giving up (minutes)")

# COMMAND ----------

config_master_id_raw    = dbutils.widgets.get("config_master_id")       or None
source_system_id_raw    = dbutils.widgets.get("source_system_id")       or None
silver_notebook_path    = dbutils.widgets.get("silver_notebook_path")   or None

if not config_master_id_raw or not source_system_id_raw:
    dbutils.notebook.exit("Error: config_master_id and source_system_id are required.")

if not silver_notebook_path:
    dbutils.notebook.exit("Error: silver_notebook_path is required.")

config_master_id         = int(config_master_id_raw)
source_system_id         = int(source_system_id_raw)
target_catalog           = dbutils.widgets.get("target_catalog")            or "hive_metastore"
audit_table              = dbutils.widgets.get("audit_table")               or AUDIT_TABLE
silver_notebook_timeout  = int(dbutils.widgets.get("silver_notebook_timeout") or "3600")
silver_max_workers       = int(dbutils.widgets.get("silver_max_workers")    or "4")
poll_interval_seconds    = int(dbutils.widgets.get("poll_interval_seconds") or "30")
timeout_minutes          = int(dbutils.widgets.get("timeout_minutes")       or "120")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Discover the same tables as main.py

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
)

if not tasks:
    dbutils.notebook.exit("No active ingestion tasks found. Nothing to monitor.")

print(f"Silver Monitor started.")
print(f"  config_master_id : {config_master_id}")
print(f"  source_name      : {source_sys.source_name}")
print(f"  Tables to watch     : {len(tasks)}")
print(f"  poll interval       : {poll_interval_seconds}s")
print(f"  timeout             : {timeout_minutes} min")
print(f"  silver_notebook     : {silver_notebook_path}")
print(f"  silver_max_workers  : {silver_max_workers}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build in-memory tracking dict

# COMMAND ----------

# Record the moment this monitor started — used to filter the audit table so we
# only look at runs from THIS specific job execution, not historical rows.
job_start_time = datetime.now(timezone.utc)

# In-memory status table: one entry per table
#   bronze_done : True when the audit table shows SUCCESS for this run
#   silver_done : True after Silver Job has been triggered for this table
tables_status = {
    task.config_id: {
        "name":        task.source_object_name,
        "config_id":   task.config_id,
        "target_table": task.target_table or task.source_object_name,
        "bronze_done": False,
        "silver_done": False,
        "silver_run_id": None,
    }
    for task in tasks
}

print("\nInitial tracking state:")
for cid, info in tables_status.items():
    print(f"  config_id={cid} | table='{info['name']}' | bronze_done=False | silver_done=False")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Polling loop — detect Bronze completion, trigger Silver

# COMMAND ----------

processor = SilverProcessor(
    dbutils              = dbutils,
    silver_notebook_path = silver_notebook_path,
    timeout_seconds      = silver_notebook_timeout,
)
timeout_seconds = timeout_minutes * 60
start_time      = time.monotonic()
results         = []   # collects Silver trigger outcomes

def check_bronze_done(config_id: int) -> bool:
    """
    Query the audit table to see if this config_id has a SUCCESS row
    that was written AFTER this monitor started (i.e., in this job run).
    """
    rows = spark.sql(f"""
        SELECT 1
        FROM   {audit_table}
        WHERE  table_id     = {int(config_id)}
          AND  status       = 'SUCCESS'
          AND  trigger_time >= '{job_start_time.strftime('%Y-%m-%d %H:%M:%S')}'
        LIMIT  1
    """).collect()
    return len(rows) > 0


def trigger_silver(info: dict) -> dict:
    """
    Called inside a ThreadPoolExecutor worker thread.
    Triggers the Silver Job for one table and returns a result dict.
    """
    config_id  = info["config_id"]
    table_name = info["target_table"]
    try:
        response = processor.trigger(config_id=config_id, table_name=table_name)
        return {
            "config_id":     config_id,
            "name":          info["name"],
            "status":        "TRIGGERED",
            "silver_run_id": response.run_id,
            "error":         None,
        }
    except Exception as exc:
        return {
            "config_id":     config_id,
            "name":          info["name"],
            "status":        "FAILED",
            "silver_run_id": None,
            "error":         str(exc),
        }


print(f"\n{'='*70}")
print(f"Silver Monitor polling loop started at {job_start_time.isoformat()}")
print(f"{'='*70}\n")

with ThreadPoolExecutor(max_workers=silver_max_workers) as executor:
    pending_futures = {}   # future → info dict, for in-flight Silver triggers

    while True:
        elapsed = time.monotonic() - start_time

        # ── Timeout guard ────────────────────────────────────────────────────
        if elapsed > timeout_seconds:
            pending_names = [
                v["name"] for v in tables_status.values()
                if not v["silver_done"]
            ]
            print(
                f"\n⚠️  Timeout reached after {timeout_minutes} min. "
                f"Tables not yet Silver-triggered: {pending_names}"
            )
            break

        # ── Check each table that isn't Silver-done yet ───────────────────
        for cid, info in tables_status.items():
            if info["silver_done"]:
                continue   # already handled

            # Step 1: Is Bronze complete?
            if not info["bronze_done"]:
                if check_bronze_done(cid):
                    info["bronze_done"] = True
                    print(
                        f"  ✅ Bronze done  — config_id={cid} table='{info['name']}' "
                        f"— submitting Silver trigger..."
                    )
                    # Step 2: Submit Silver trigger to thread pool
                    future = executor.submit(trigger_silver, info)
                    pending_futures[future] = info

        # ── Collect any completed Silver triggers ─────────────────────────
        done_futures = [f for f in list(pending_futures) if f.done()]
        for future in done_futures:
            info = pending_futures.pop(future)
            try:
                res = future.result()
            except Exception as exc:
                res = {
                    "config_id":     info["config_id"],
                    "name":          info["name"],
                    "status":        "FAILED",
                    "silver_run_id": None,
                    "error":         str(exc),
                }
            results.append(res)
            tables_status[info["config_id"]]["silver_done"] = True
            tables_status[info["config_id"]]["silver_run_id"] = res.get("silver_run_id")
            icon = "✅" if res["status"] == "TRIGGERED" else "❌"
            print(
                f"  {icon} Silver {'triggered' if res['status'] == 'TRIGGERED' else 'FAILED'}  "
                f"— config_id={info['config_id']} table='{info['name']}' "
                f"silver_run_id={res.get('silver_run_id')}"
            )

        # ── Exit condition: all tables Silver-done ────────────────────────
        if all(v["silver_done"] for v in tables_status.values()):
            print(f"\n✅ All {len(tasks)} tables have been sent to the Silver Job.")
            break

        # ── Status heartbeat every poll interval ──────────────────────────
        bronze_done_count = sum(1 for v in tables_status.values() if v["bronze_done"])
        silver_done_count = sum(1 for v in tables_status.values() if v["silver_done"])
        print(
            f"  ⏳ [{int(elapsed)}s elapsed] "
            f"Bronze done: {bronze_done_count}/{len(tasks)} | "
            f"Silver triggered: {silver_done_count}/{len(tasks)} | "
            f"Sleeping {poll_interval_seconds}s..."
        )
        time.sleep(poll_interval_seconds)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Results Summary

# COMMAND ----------

print(f"\n{'='*70}")
print(f"{'CONFIG ID':>10}  {'TABLE':<30}  {'STATUS':<12}  SILVER RUN ID")
print(f"{'='*70}")
for r in sorted(results, key=lambda x: x["config_id"]):
    icon  = "✅" if r["status"] == "TRIGGERED" else "❌"
    print(
        f"{r['config_id']:>10}  {r['name']:<30}  "
        f"{icon} {r['status']:<10}  {r.get('silver_run_id') or 'N/A'}"
    )
print(f"{'='*70}")

triggered = [r for r in results if r["status"] == "TRIGGERED"]
failed    = [r for r in results if r["status"] == "FAILED"]
print(f"Total: {len(results)} | ✅ Triggered: {len(triggered)} | ❌ Failed: {len(failed)}\n")

if failed:
    failed_names = [r["name"] for r in failed]
    raise Exception(
        f"{len(failed)} Silver trigger(s) FAILED: {failed_names}. "
        f"Check logs above for details."
    )
