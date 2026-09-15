# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Multi-Refresh Eligibility Notebook
# MAGIC
# MAGIC Called by `multi_refresh_orchestrator_v1.py` via `dbutils.notebook.run()`.
# MAGIC Determines which tables are eligible for refresh in the current window,
# MAGIC updates the relevant config tables, and returns a JSON payload with:
# MAGIC   - `eligible_pipelines`: list of {pipeline_name, source_name} dicts to trigger
# MAGIC   - `source_name`:        flat list of source names (legacy / informational)
# MAGIC   - `is_completed`:       1 if all scheduled refreshes for today are done
# MAGIC   - `wait_time`:          seconds until the next refresh window

# COMMAND ----------

import datetime
import json
import os
import random
import time

from pyspark.sql import Window
from pyspark.sql.functions import col, lead, row_number, coalesce, lit, to_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("triggerTime", "", "Trigger Time (ISO: YYYY-MM-DDTHH:MM:SS.ffffff+00)")

# COMMAND ----------

admin_catalog_name = os.getenv("admin_catalog_name")

# Parse triggerTime widget
# Accepts "YYYY-MM-DD HH:MM:SS"  (sent by v1 orchestrator)
# or the legacy ADF format "YYYY-MM-DDTHH:MM:SS.ffffff+00"
_raw_trigger = dbutils.widgets.get("triggerTime")
try:
    triggerTime = datetime.datetime.strptime(_raw_trigger, "%Y-%m-%d %H:%M:%S")
except ValueError:
    # Fallback: strip trailing 2-char timezone offset "+00" and parse
    triggerTime = datetime.datetime.strptime(_raw_trigger[:-2], "%Y-%m-%dT%H:%M:%S.%f")

trigger_hhmm     = triggerTime.strftime("%H:%M")
trigger_date_str = triggerTime.strftime("%Y-%m-%d")
trigger_time_str = triggerTime.strftime("%Y-%m-%d %H:%M:%S")

print(f"triggerTime parsed : {triggerTime}")
print(f"trigger_hhmm       : {trigger_hhmm}")
print(f"trigger_date_str   : {trigger_date_str}")

# Fully-qualified table references (matches the admin catalog layout)
BATCH_RUN_CFG_TABLE = f"{admin_catalog_name}.config.tb_report_batch_run_config"
SINK_CFG_TABLE      = f"{admin_catalog_name}.config.tb_sourcedb_ingestion_sink_config"
DEP_MASTER_TABLE    = f"{admin_catalog_name}.config.tb_dependency_master_config"
ELIGIBLE_TEMP_TABLE = f"{admin_catalog_name}.temp.tb_eligible_objects"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Schedule scan: find configs eligible for this refresh window

# COMMAND ----------

multi_refresh_df = spark.sql(f"""
    SELECT * FROM (
        SELECT
            ID,
            Object_Name                                                          AS Task_Name,
            Object_Type,
            Config_Master_ID,
            Config_ID,
            Refresh_Time                                                         AS Curent_Refresh_Time,
            coalesce(
                lead(Refresh_Time) OVER (
                    PARTITION BY Config_Master_ID, Config_ID
                    ORDER BY to_timestamp(Refresh_Time)
                ),
                '23:59'
            )                                                                    AS Next_Refresh_Time,
            Last_Sink_Date,
            Is_Active
        FROM {BATCH_RUN_CFG_TABLE}
    ) a
    WHERE a.Is_Active = 1
      AND cast('{trigger_hhmm}' AS timestamp) BETWEEN cast(a.Curent_Refresh_Time AS timestamp)
                                                   AND cast(a.Next_Refresh_Time  AS timestamp)
      AND to_date(coalesce(a.Last_Sink_Date, '1900-01-01')) != '{trigger_date_str}'
""")
multi_refresh_df.createOrReplaceTempView("multi_refresh_eligible_config")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - ADF / SourceDB (Config_Master_ID = 2): eligibility check + MERGE updates

# COMMAND ----------

win_spec = Window.partitionBy(col("Config_Master_ID"), col("Config_ID")).orderBy(
    col("Curent_Refresh_Time").desc()
)

table_refresh_df = (
    multi_refresh_df
    .filter("Config_Master_ID = 2")
    .withColumn("row_num", row_number().over(win_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)
table_refresh_df.createOrReplaceTempView("table_refresh_df")

eligible_pipelines = []   # populated below: [{pipeline_name, source_name}, ...]

if table_refresh_df.count() > 0:

    eligible_df = spark.sql(f"""
        SELECT * FROM (
            SELECT
                a.Config_Master_ID,
                a.Config_ID,
                CASE WHEN c.Table_Config_ID IS NOT NULL THEN 1 ELSE 0 END AS Current_Status_Flag,
                CASE WHEN b.Config_ID       IS NOT NULL THEN 1 ELSE 0 END AS Day_Status_Flag,
                a.Curent_Refresh_Time,
                a.Next_Refresh_Time
            FROM table_refresh_df a
            LEFT JOIN (
                SELECT Config_Master_ID, Config_ID
                FROM {SINK_CFG_TABLE}
                WHERE Day_Execution_Count > 0
                  AND Tool_Name = 'ADF'
                  AND to_date(Sink_Batch_Started_Date) = '{trigger_date_str}'
            ) b ON a.Config_Master_ID = b.Config_Master_ID
               AND a.Config_ID        = b.Config_ID
            LEFT JOIN (
                SELECT DISTINCT Table_Config_Master_ID, Table_Config_ID
                FROM {DEP_MASTER_TABLE}
                WHERE Dependency_Resolved_Time IS NULL
                  AND Delta_Layer = 'Silver'
                  AND Is_Active   = 1
            ) c ON a.Config_Master_ID = c.Table_Config_Master_ID
               AND a.Config_ID        = c.Table_Config_ID
        ) final
        WHERE Current_Status_Flag = 0
          AND Day_Status_Flag     = 1
    """)

    # Pipeline-name lookup block
    # For each eligible row, fetch Pipeline_Name + Source_Name from the sink
    # config table and build the eligible_pipelines list that the v1 orchestrator
    # uses to fire Databricks Job runs.
    eligible_rows = eligible_df.collect()
    result_rows   = []

    for r in eligible_rows:
        r_dict = r.asDict()
        res = spark.sql(f"""
            SELECT Pipeline_Name, Source_Name
            FROM {SINK_CFG_TABLE}
            WHERE Config_Master_ID = {r["Config_Master_ID"]}
              AND Config_ID        = {r["Config_ID"]}
            LIMIT 1
        """).collect()
        if res:
            r_dict["Pipeline_Name"] = res[0]["Pipeline_Name"]
            r_dict["Source_Name"]   = res[0]["Source_Name"]
            result_rows.append(r_dict)
            # Build eligible_pipelines payload (deduplicated by pipeline_name)
            pipeline_entry = {
                "pipeline_name": res[0]["Pipeline_Name"],
                "source_name":   res[0]["Source_Name"],
            }
            if pipeline_entry not in eligible_pipelines:
                eligible_pipelines.append(pipeline_entry)

    if result_rows:
        # Persist eligible set to temp table for SQL MERGE operations
        eligible_df = spark.createDataFrame(result_rows)
        eligible_df.write.format("delta").mode("overwrite").saveAsTable(ELIGIBLE_TEMP_TABLE)

        # MERGE 1: mark tables as In Progress in sink config
        while True:
            try:
                spark.sql(f"""
                    MERGE INTO {SINK_CFG_TABLE} t
                    USING {ELIGIBLE_TEMP_TABLE} s
                    ON t.Config_Master_ID = s.Config_Master_ID
                   AND t.Config_ID        = s.Config_ID
                    WHEN MATCHED THEN UPDATE SET
                        t.sink_batch_started_date = '{trigger_time_str}',
                        t.Status                  = 'In Progress'
                """)

                # MERGE 2: reset Dependency_Resolved_Time for downstream tasks
                spark.sql(f"""
                    MERGE INTO {DEP_MASTER_TABLE} t
                    USING (
                        SELECT
                            Task_Config_Master_ID,
                            Task_Config_ID,
                            Table_Config_Master_ID,
                            Table_Config_ID
                        FROM {ELIGIBLE_TEMP_TABLE} a
                        JOIN {DEP_MASTER_TABLE} b
                          ON a.Config_Master_ID = b.Table_Config_Master_ID
                         AND a.Config_ID        = b.Table_Config_ID
                         AND b.Task_Config_Master_ID = 1
                        WHERE EXISTS (
                            SELECT 1
                            FROM {BATCH_RUN_CFG_TABLE} c
                            WHERE c.Config_Master_ID = b.Task_Config_Master_ID
                              AND c.Config_ID        = b.Task_Config_ID
                        )
                    ) s
                    ON  t.Task_Config_Master_ID  = s.Task_Config_Master_ID
                    AND t.Task_Config_ID         = s.Task_Config_ID
                    AND t.Table_Config_Master_ID = s.Table_Config_Master_ID
                    AND t.Table_Config_ID        = s.Table_Config_ID
                    AND coalesce(to_date(t.Dependency_Resolved_Time), '1900-01-01') = current_date()
                    WHEN MATCHED THEN UPDATE SET Dependency_Resolved_Time = null
                """)

                # MERGE 3: stamp Last_Sink_Date in schedule table
                spark.sql(f"""
                    MERGE INTO {BATCH_RUN_CFG_TABLE} t
                    USING (
                        SELECT DISTINCT s.ID
                        FROM (
                            SELECT
                                ID,
                                Config_Master_ID,
                                Config_ID,
                                Refresh_Time AS Curent_Refresh_Time,
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
                              AND (
                                  cast('{trigger_hhmm}' AS timestamp) >= cast(t.Curent_Refresh_Time AS timestamp)
                                  AND cast('{trigger_hhmm}' AS timestamp) <  cast(t.Next_Refresh_Time  AS timestamp)
                              )
                    ) s
                    ON t.ID = s.ID
                    WHEN MATCHED THEN UPDATE SET t.Last_Sink_Date = '{trigger_time_str}'
                """)
                break

            except Exception as e:
                if "MetadataChangedException" in str(type(e)) or "ConcurrentAppendException" in str(type(e)):
                    sleep_duration = random.uniform(1, 5)
                    time.sleep(sleep_duration)
                else:
                    print(e)
                    break

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Derive wait_second_flag (any incomplete runs from earlier windows?)

# COMMAND ----------

wait_second_flag = spark.sql(f"""
    WITH CTE AS (
        SELECT max(Refresh_Time) AS max_refresh_time
        FROM {BATCH_RUN_CFG_TABLE}
        WHERE date_format(to_timestamp(Refresh_Time), 'yyyy-MM-dd HH:mm:ss') < date_format('{trigger_time_str}', 'yyyy-MM-dd HH:mm:ss')
          AND to_date(Last_Sink_Date) != '{trigger_date_str}'
          AND Is_Active = 1
    )
    SELECT CASE WHEN count(1) > 0 THEN 1 ELSE 0 END AS wait_second_flag
    FROM {BATCH_RUN_CFG_TABLE}
    WHERE date_format(to_timestamp(Refresh_Time), 'yyyy-MM-dd HH:mm:ss') < date_format('{trigger_time_str}', 'yyyy-MM-dd HH:mm:ss')
      AND to_date(Last_Sink_Date) != '{trigger_date_str}'
      AND Is_Active = 1
      AND date_format(to_timestamp(Refresh_Time), 'yyyy-MM-dd HH:mm:ss')
          BETWEEN (SELECT date_format(to_timestamp(max_refresh_time), 'yyyy-MM-dd HH:mm:ss') FROM CTE)
              AND date_format('{trigger_time_str}', 'yyyy-MM-dd HH:mm:ss')
""").collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Build eligible_pipelines from all source types

# COMMAND ----------

eligible_pipelines_raw = []

# Source DB Base Tables
if spark.catalog.tableExists(ELIGIBLE_TEMP_TABLE):
    source_name_df = spark.sql(f"""
        SELECT DISTINCT Pipeline_Name, Source_Name
        FROM {SINK_CFG_TABLE} a
        WHERE a.Config_ID IN (
            SELECT DISTINCT Config_ID FROM {ELIGIBLE_TEMP_TABLE}
        )
    """)
    for row in source_name_df.collect():
        eligible_pipelines_raw.append(
            {"pipeline_name": row.Pipeline_Name, "source_name": row.Source_Name}
        )

# Source DB Email Delivery
email_source_name_df = spark.sql(f"""
    SELECT DISTINCT Pipeline_Name, Source_Type
    FROM {admin_catalog_name}.config.tb_sourcedb_email_delivery a
    WHERE date_format(sink_batch_started_on, 'yyyy-MM-dd HH:mm:ss.SSS')
          = date_format('{trigger_time_str}', 'yyyy-MM-dd HH:mm:ss.SSS')
      AND is_active = 1
""")
for row in email_source_name_df.collect():
    eligible_pipelines_raw.append(
        {"pipeline_name": row.Pipeline_Name, "source_name": row.Source_Type}
    )

# Digital Prod NoSQL Tables
nosql_source_name_df = spark.sql(f"""
    SELECT DISTINCT Pipeline_Name, Source_Name
    FROM {admin_catalog_name}.config.tb_nosql_ingestion_config a
    WHERE date_format(sink_batch_started_date, 'yyyy-MM-dd HH:mm:ss.SSS')
          = date_format('{trigger_time_str}', 'yyyy-MM-dd HH:mm:ss.SSS')
      AND is_active = 1
""")
for row in nosql_source_name_df.collect():
    eligible_pipelines_raw.append(
        {"pipeline_name": row.Pipeline_Name, "source_name": row.Source_Name}
    )

# LSQ Mavis multi refresh Tables
mavis_source_name_df = spark.sql(f"""
    SELECT DISTINCT Pipeline_Name, Source_Name
    FROM {admin_catalog_name}.config.tb_mavis_db_ingestion_config a
    WHERE date_format(sink_batch_started_date, 'yyyy-MM-dd HH:mm:ss.SSS')
          = date_format('{trigger_time_str}', 'yyyy-MM-dd HH:mm:ss.SSS')
      AND is_active = 1
""")
for row in mavis_source_name_df.collect():
    eligible_pipelines_raw.append(
        {"pipeline_name": row.Pipeline_Name, "source_name": row.Source_Name}
    )

# Source DB Storage Delivery
storage_data_refresh_df = spark.sql(f"""
    SELECT DISTINCT Pipeline_Name, Source_Type
    FROM {admin_catalog_name}.config.tb_sourcedb_storage_delivery a
    WHERE date_format(sink_batch_started_on, 'yyyy-MM-dd HH:mm:ss.SSS')
          = date_format('{trigger_time_str}', 'yyyy-MM-dd HH:mm:ss.SSS')
      AND is_active = 1
""")
for row in storage_data_refresh_df.collect():
    eligible_pipelines_raw.append(
        {"pipeline_name": row.Pipeline_Name, "source_name": row.Source_Type}
    )

# Dedupe by pipeline_name, keeping the first source_name seen for each
eligible_pipelines = list(
    {
        p["pipeline_name"]: p
        for p in eligible_pipelines_raw
        if p.get("pipeline_name")
    }.values()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - is_completed + wait_time

# COMMAND ----------

# Is every scheduled table for today done?
is_completed = spark.sql(f"""
    SELECT CASE WHEN to_date(MIN(Last_Sink_Date)) = '{trigger_date_str}' THEN 1 ELSE 0 END AS is_completed
    FROM {BATCH_RUN_CFG_TABLE}
    WHERE Is_Active = 1
      AND to_timestamp(Refresh_Time) IN (
          SELECT max(to_timestamp(Refresh_Time))
          FROM {BATCH_RUN_CFG_TABLE}
          WHERE Is_Active = 1
      )
""").select("is_completed").collect()[0][0]

# Seconds until the next refresh window
wait_time_row = spark.sql(f"""
    SELECT (
        unix_timestamp(to_timestamp(MIN(Refresh_Time)))
        - unix_timestamp(from_utc_timestamp(current_timestamp(), 'Asia/Kolkata'))
    ) AS difference_in_seconds
    FROM {BATCH_RUN_CFG_TABLE}
    WHERE date_format(to_timestamp(Refresh_Time), 'yyyy-MM-dd HH:mm:ss') > date_format('{trigger_time_str}', 'yyyy-MM-dd HH:mm:ss')
      AND Is_Active = 1
""").collect()[0]["difference_in_seconds"]

wait_time = 1 if wait_time_row is None or wait_second_flag == 1 else wait_time_row

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6 - Cleanup and exit

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {ELIGIBLE_TEMP_TABLE}")

output_json = {
    "eligible_pipelines": eligible_pipelines,
    "is_completed":       is_completed,
    "wait_time":          wait_time,
}

print("Output JSON:", json.dumps(output_json, indent=2))
dbutils.notebook.exit(json.dumps(output_json))
