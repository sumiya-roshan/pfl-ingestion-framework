# Databricks notebook source
# MAGIC %md
# MAGIC # Lentra — Get Load Config
# MAGIC
# MAGIC Second task in `lentra_main_job`. Equivalent of the ADF "get table
# MAGIC details" lookup — resolves the single active `lentra*hdr` config row for
# MAGIC this source (each source has exactly one row) and publishes its columns
# MAGIC as individual taskValues. The downstream `load_raw_to_silver` task (the
# MAGIC client-provided notebook that handles the actual file processing) takes
# MAGIC them as plain widgets wired as `{{tasks.get_load_config.values.<Column>}}`
# MAGIC — no config-table lookup needed there.
# MAGIC
# MAGIC Builds and runs the SELECT directly — no SQL string is passed in as a
# MAGIC widget.
# MAGIC
# MAGIC The table's location isn't a raw catalog-name widget — it's resolved from
# MAGIC `config_master` via `config_master_id`, using `resolve_child_table_fqn()`
# MAGIC in `config_manager.py` — the same routing helper `ConfigManager` itself
# MAGIC uses for RDBMS/NoSQL/S3, extracted so this notebook doesn't duplicate it.
# MAGIC
# MAGIC **Naming heads-up:** the `config_master_id` widget (routes to the table
# MAGIC itself, via the shared `config_master` routing table) and the
# MAGIC `Config_Master_ID` *column* on `tb_aws_s3_ingestion_config` (a per-row
# MAGIC business field, published below as a taskValue) are two unrelated things
# MAGIC that happen to share almost the same name — don't confuse them.
# MAGIC
# MAGIC **Credentials note:** `Access_Key_ID` / `Secret_Access_Key` are NOT raw
# MAGIC credentials — they're AWS Secrets Manager secret *names* (e.g.
# MAGIC `sec-aws-s3-pfl-datalake-access-key-id`). Safe to publish as-is.

# COMMAND ----------

import sys

sys.path.append("..")

from ingestion.utils.config_manager import CONFIG_MASTER_TABLE, resolve_child_table_fqn
from ingestion.utils.logger import get_logger

# COMMAND ----------

# MAGIC %md
# MAGIC ### Widgets

# COMMAND ----------

dbutils.widgets.text("config_master_id", "", "config_master.config_id routing to tb_aws_s3_ingestion_config")
dbutils.widgets.text("source_name", "", "Source name — matches tb_aws_s3_ingestion_config.Source_Name exactly (e.g. lentra_dealer_dms_hdr, underscores)")

# COMMAND ----------

config_master_id = dbutils.widgets.get("config_master_id") or None
source_name        = dbutils.widgets.get("source_name") or None

if not config_master_id:
    dbutils.notebook.exit("Error: config_master_id widget is required and cannot be empty.")
if not source_name:
    dbutils.notebook.exit("Error: source_name widget is required and cannot be empty.")

logger = get_logger()


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


try:
    CONFIG_TABLE = resolve_child_table_fqn(spark, CONFIG_MASTER_TABLE, config_master_id)
except ValueError as exc:
    dbutils.notebook.exit(f"Error: {exc}")
logger.info(f"[Lentra] config_master_id={config_master_id} → {CONFIG_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolve the active config row for this source
# MAGIC
# MAGIC Worker_No mirrors the ADF CASE expression: multi-node compute policies
# MAGIC get "1:N" (autoscaling) or "N" (fixed) worker specs; everything else gets "0".

# COMMAND ----------

select_sql = f"""
    SELECT *,
        CASE
            WHEN Compute_Policy_Name LIKE '%multi%' AND Cluster_Option = 'Autoscaling'
                THEN concat('1:', string(Worker_Number))
            WHEN Compute_Policy_Name LIKE '%multi%' AND Cluster_Option = 'Fixed'
                THEN string(Worker_Number)
            ELSE '0'
        END AS Worker_No
    FROM {CONFIG_TABLE}
    WHERE Report_Name like 'lentra%hdr'
      AND Source_Name = {_sql_literal(source_name)}
      AND is_active = 1
    ORDER BY Config_ID
"""
logger.info(f"[Lentra] {select_sql}")

result_df = spark.sql(select_sql)
rows = result_df.collect()
logger.info(f"[Lentra] get_load_config — source_name={source_name!r} rows={len(rows)}")

if len(rows) > 1:
    logger.warning(
        f"[Lentra] query returned {len(rows)} rows for source_name={source_name!r} — "
        f"expected exactly one. Only the first row will be published as taskValues."
    )

if not rows:
    dbutils.notebook.exit(f"No active config row found for source_name={source_name!r}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Publish taskValues for load_raw_to_silver

# COMMAND ----------

PUBLISHABLE_COLUMNS = [
    "Config_ID",
    "Config_Master_ID",
    "Report_Name",
    "Load_Type",
    "Compute_Policy_ID",
    "Cluster_Option",
    "Worker_No",
    "Source_Bucket_Name",
    "External_Path",
    "Raw_Sink_Container_Name",
    "Raw_Sink_File_Path",
    "Silver_Sink_Schema_Name",
    "Silver_Sink_table_Name",
    "Access_Key_ID",       # AWS Secrets Manager secret NAME, not a raw credential
    "Secret_Access_Key",   # AWS Secrets Manager secret NAME, not a raw credential
]

first_row = rows[0].asDict()
logger.info(f"[Lentra] resolved row: {first_row}")

published = {}
for col in PUBLISHABLE_COLUMNS:
    if col in first_row:
        value = first_row[col]
        try:
            dbutils.jobs.taskValues.set(key=col, value=value)
            published[col] = value
        except Exception as exc:
            logger.info(f"[INFO] taskValues not available (standalone mode) for {col}: {exc}")

logger.info(f"[Lentra] published taskValues: {published}")

# COMMAND ----------

dbutils.notebook.exit(f"get_load_config complete — source_name={source_name!r}, rows={len(rows)}.")
