# Databricks notebook source
# MAGIC %md
# MAGIC # Manual check: does query_timeout actually cancel the source-DB query?
# MAGIC
# MAGIC **This is not part of the automated `unittest` suite** — it needs a live JDBC
# MAGIC driver + a real database connection, so it only runs as a Databricks notebook
# MAGIC on a cluster that has the source's JDBC driver jar installed (the same cluster
# MAGIC your ingestion job runs on).
# MAGIC
# MAGIC What it does:
# MAGIC 1. Resolves real connection details for one `source_id` from
# MAGIC    `config_source_system` (via `ConfigManager` + `SecretResolver` — same
# MAGIC    code path the framework uses, no hardcoded credentials).
# MAGIC 2. Issues a driver-specific "sleep" query longer than the configured timeout,
# MAGIC    through the *actual* `JdbcConnector._read_options()` (so we're testing the
# MAGIC    real `opts["queryTimeout"]` wiring, not a hand-rolled one).
# MAGIC 3. Times how long it takes to fail. If it fails at ~`timeout_seconds` instead
# MAGIC    of the full sleep duration, the driver is honoring `queryTimeout`.
# MAGIC 4. Prints the DB-side session query to run *while this cell is executing*, so
# MAGIC    you can watch the source DB actually cancel the session — that's the only
# MAGIC    real proof the database (not just Spark) killed the query.
# MAGIC
# MAGIC **Usage:** set the widgets below, run all cells, then within the timeout
# MAGIC window paste the printed monitoring query into a second notebook/SQL editor
# MAGIC connected to the source DB and watch the session disappear.

# COMMAND ----------

import sys
sys.path.append("..")

import time
from ingestion.utils.config_manager import ConfigManager, SOURCE_SYSTEM_TABLE
from ingestion.utils.secrets import SecretResolver
from ingestion.connectors.jdbc_connector import JdbcConnector, _parse_timeout_to_seconds

# COMMAND ----------

dbutils.widgets.text("source_system_id", "", "source_id from config_source_system to test against")
dbutils.widgets.text("timeout_seconds", "5", "queryTimeout to apply, in seconds")
dbutils.widgets.text("sleep_seconds", "30", "How long the test query sleeps (must be > timeout_seconds)")

source_system_id = int(dbutils.widgets.get("source_system_id"))
timeout_seconds = int(dbutils.widgets.get("timeout_seconds"))
sleep_seconds = int(dbutils.widgets.get("sleep_seconds"))

assert sleep_seconds > timeout_seconds, "sleep_seconds must be greater than timeout_seconds for this test to prove anything"

# COMMAND ----------

# MAGIC %md ### Resolve the real source system config + credentials

# COMMAND ----------

config_mgr = ConfigManager(spark, source_system_table=SOURCE_SYSTEM_TABLE)
source_sys = config_mgr.get_source_system(source_system_id)
secrets = SecretResolver(dbutils)

print(f"source_id   : {source_sys.source_id}")
print(f"source_name : {source_sys.source_name}")
print(f"source_type : {source_sys.source_type}")
print(f"host:port   : {source_sys.host}:{source_sys.port}")

# COMMAND ----------

# MAGIC %md ### Driver-specific "sleep N seconds" query
# MAGIC
# MAGIC Each RDBMS needs a different way to force a query to run for a fixed
# MAGIC duration server-side (this must be actual server-side work, not something
# MAGIC the JDBC driver can short-circuit client-side).

# COMMAND ----------

def sleep_query(source_type: str, seconds: int) -> str:
    st = source_type.upper()
    if st == "POSTGRES":
        return f"SELECT pg_sleep({seconds})"
    if st == "MYSQL":
        return f"SELECT SLEEP({seconds})"
    if st == "MSSQL":
        return f"SELECT 1 WHERE 1=1; WAITFOR DELAY '{time.strftime('%H:%M:%S', time.gmtime(seconds))}'; SELECT 1 AS ok"
    if st == "ORACLE":
        return f"SELECT 1 AS ok FROM dual CONNECT BY LEVEL <= 1 AND DBMS_LOCK.SLEEP({seconds}) IS NULL"
    raise ValueError(f"No sleep-query recipe for source_type='{source_type}' — add one before running this check.")

test_sql = sleep_query(source_sys.source_type, sleep_seconds)
print(f"Test query: {test_sql}")

# COMMAND ----------

# MAGIC %md ### Build a fake task config carrying query_timeout, exactly as lookup.py would stamp it

# COMMAND ----------

class _FakeIngestObj:
    """Mimics the subset of IngestionTaskConfig that JdbcConnector reads."""
    custom_query = None          # overwritten below with the sleep query
    load_type = "FULL"
    incremental_column = None
    incremental_end_value = None
    source_filter = None
    source_schema = None
    source_object_name = None
    data_read_size = None
    query_timeout = None         # set below, format 'HH:mm:ss'

fake_task = _FakeIngestObj()
fake_task.custom_query = test_sql
fake_task.query_timeout = time.strftime("%H:%M:%S", time.gmtime(timeout_seconds))

print(f"ingest_obj.query_timeout = {fake_task.query_timeout!r} "
      f"({_parse_timeout_to_seconds(fake_task.query_timeout)}s parsed)")

# COMMAND ----------

# MAGIC %md ### Print the DB-side monitoring query to run *during* the next cell
# MAGIC
# MAGIC Open a second notebook/SQL client connected to the source DB and run this
# MAGIC in a loop while the next cell executes — this is the only proof the
# MAGIC database itself killed the query, not just Spark giving up.

# COMMAND ----------

MONITOR_QUERIES = {
    "POSTGRES": "SELECT pid, state, query, query_start FROM pg_stat_activity WHERE state = 'active';",
    "MYSQL":    "SHOW PROCESSLIST;",
    "MSSQL":    "SELECT session_id, status, command, start_time FROM sys.dm_exec_requests WHERE command NOT LIKE '%BACKUP%';",
    "ORACLE":   "SELECT sid, sql_id, status, last_call_et FROM v$session WHERE status = 'ACTIVE';",
}
print("Run this on the SOURCE DB (not Databricks) while the next cell runs:\n")
print("    " + MONITOR_QUERIES.get(source_sys.source_type.upper(), "<no recipe for this source_type>"))
print(f"\nExpect the active session/query row to disappear at ~{timeout_seconds}s, "
      f"NOT at ~{sleep_seconds}s.")

# COMMAND ----------

# MAGIC %md ### Run the timed extraction through the real JdbcConnector

# COMMAND ----------

connector = JdbcConnector(spark, source_sys, fake_task, secrets)
options = connector._read_options()
print(f"JDBC read options (password redacted): "
      f"{ {k: ('***' if k == 'password' else v) for k, v in options.items()} }")
assert "queryTimeout" in options, "queryTimeout option was NOT set — check query_timeout parsing before proceeding"

print(f"\nStarting query at {time.strftime('%H:%M:%S')} — "
      f"expect failure around {timeout_seconds}s, watch the source DB now...\n")

t0 = time.time()
try:
    df = (
        spark.read.format("jdbc")
        .options(**options)
        .option("dbtable", f"({test_sql}) _t")
        .load()
    )
    df.collect()
    elapsed = time.time() - t0
    print(f"\n[UNEXPECTED] Query completed successfully after {elapsed:.1f}s "
          f"— queryTimeout was NOT enforced by the driver.")
except Exception as exc:
    elapsed = time.time() - t0
    print(f"\nQuery failed after {elapsed:.1f}s.")
    print(f"Exception: {exc}\n")
    if elapsed < (timeout_seconds + 10):
        print(f"[PASS-ish] Failed close to the configured {timeout_seconds}s timeout, "
              f"not the full {sleep_seconds}s sleep — queryTimeout appears to be enforced client-side "
              f"at minimum. Confirm the DB-side monitoring query above also showed the session "
              f"actually get killed (not just Spark giving up) to be fully sure.")
    else:
        print(f"[FAIL] Took close to the full {sleep_seconds}s sleep duration before failing — "
              f"queryTimeout does NOT appear to be enforced for this driver/version. "
              f"Check driver-specific caveats (e.g. MySQL's enableQueryTimeouts property, "
              f"older Oracle ojdbc versions) or consider a session-level statement_timeout "
              f"set via extra_params/connection_uri instead.")

# COMMAND ----------

# MAGIC %md ### Cleanup note
# MAGIC No Delta table is written by this notebook — it only calls `.load()` +
# MAGIC `.collect()` against the source, so there's nothing to clean up on the
# MAGIC Databricks side. If the DB-side monitoring query still shows a lingering
# MAGIC session after this notebook finishes, that itself is a finding worth
# MAGIC investigating (driver not releasing the connection on cancel).
