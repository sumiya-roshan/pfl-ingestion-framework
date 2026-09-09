# Databricks notebook source
# MAGIC %md
# MAGIC # Lentra — Check Parameters (TEMPORARY TEST STUB)
# MAGIC
# MAGIC Stand-in for the `load_raw_to_silver` task, used ONLY to verify that
# MAGIC `get_load_config`'s taskValues actually reach this task correctly — does
# MAGIC nothing else. Delete this file (and repoint the job at the real
# MAGIC client-provided notebook) once wiring is confirmed.
# MAGIC
# MAGIC Widget names match `get_load_config`'s published taskValue keys exactly
# MAGIC — wire each one in the job as `{{tasks.get_load_config.values.<name>}}`.

# COMMAND ----------

import sys

sys.path.append("..")

from ingestion.utils.logger import get_logger

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets — one per get_load_config taskValue, plus the job-level ones

# COMMAND ----------

dbutils.widgets.text("source_name",              "", "Job-level — logging only")
dbutils.widgets.text("run_id",                   "", "Job-level — set to {{job.run_id}}")
dbutils.widgets.text("Config_ID",                "", "{{tasks.get_load_config.values.Config_ID}}")
dbutils.widgets.text("Config_Master_ID",         "", "{{tasks.get_load_config.values.Config_Master_ID}}")
dbutils.widgets.text("Report_Name",              "", "{{tasks.get_load_config.values.Report_Name}}")
dbutils.widgets.text("Load_Type",                "", "{{tasks.get_load_config.values.Load_Type}}")
dbutils.widgets.text("Compute_Policy_ID",        "", "{{tasks.get_load_config.values.Compute_Policy_ID}}")
dbutils.widgets.text("Cluster_Option",           "", "{{tasks.get_load_config.values.Cluster_Option}}")
dbutils.widgets.text("Worker_No",                "", "{{tasks.get_load_config.values.Worker_No}}")
dbutils.widgets.text("Source_Bucket_Name",       "", "{{tasks.get_load_config.values.Source_Bucket_Name}}")
dbutils.widgets.text("External_Path",            "", "{{tasks.get_load_config.values.External_Path}}")
dbutils.widgets.text("Raw_Sink_Container_Name",  "", "{{tasks.get_load_config.values.Raw_Sink_Container_Name}}")
dbutils.widgets.text("Raw_Sink_File_Path",       "", "{{tasks.get_load_config.values.Raw_Sink_File_Path}}")
dbutils.widgets.text("Silver_Sink_Schema_Name",  "", "{{tasks.get_load_config.values.Silver_Sink_Schema_Name}}")
dbutils.widgets.text("Silver_Sink_table_Name",   "", "{{tasks.get_load_config.values.Silver_Sink_table_Name}}")
dbutils.widgets.text("Access_Key_ID",            "", "{{tasks.get_load_config.values.Access_Key_ID}} — secret NAME, not a raw key")
dbutils.widgets.text("Secret_Access_Key",        "", "{{tasks.get_load_config.values.Secret_Access_Key}} — secret NAME, not a raw key")

# COMMAND ----------

WIDGET_NAMES = [
    "source_name", "run_id",
    "Config_ID", "Config_Master_ID", "Report_Name", "Load_Type",
    "Compute_Policy_ID", "Cluster_Option", "Worker_No",
    "Source_Bucket_Name", "External_Path",
    "Raw_Sink_Container_Name", "Raw_Sink_File_Path",
    "Silver_Sink_Schema_Name", "Silver_Sink_table_Name",
    "Access_Key_ID", "Secret_Access_Key",
]

logger = get_logger()

received = {name: dbutils.widgets.get(name) for name in WIDGET_NAMES}

logger.info("[Lentra] check_parameters — values received:")
missing = []
for name, value in received.items():
    logger.info(f"[Lentra]   {name} = {value!r}")
    if not value:
        missing.append(name)

# COMMAND ----------

if missing:
    dbutils.notebook.exit(f"MISSING/EMPTY: {missing}. See job output above for what was actually received.")

dbutils.notebook.exit(f"All {len(WIDGET_NAMES)} parameters received correctly: {received}")
