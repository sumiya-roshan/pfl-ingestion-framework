# Databricks notebook source
# MAGIC %md
# MAGIC # Lentra — Reset Ingestion Status
# MAGIC
# MAGIC First task in `lentra_main_job`. Equivalent of the ADF lookup:
# MAGIC ```
# MAGIC UPDATE ... SET Status = 'Not-Started'
# MAGIC WHERE Report_Name like 'lentra%hdr' AND Source_Name = '<source_name>' AND is_active = 1
# MAGIC ```
# MAGIC Builds and runs that UPDATE directly against `tb_aws_s3_ingestion_config` —
# MAGIC no SQL string is passed in as a widget.
# MAGIC
# MAGIC The table's location isn't a raw catalog-name widget — it's resolved from
# MAGIC `config_master` via `config_master_id`, using
# MAGIC `resolve_child_table_fqn()` in `config_manager.py` — the same routing
# MAGIC helper `ConfigManager` itself uses for RDBMS/NoSQL/S3, extracted so this
# MAGIC notebook doesn't duplicate it. Requires a row in `config_master` pointing
# MAGIC `config_master_id` at `tb_aws_s3_ingestion_config`'s catalog/schema/table.

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
# MAGIC ### Reset Status for this source's active lentra*hdr rows

# COMMAND ----------

update_sql = f"""
    UPDATE {CONFIG_TABLE}
    SET Status = 'Not-Started'
    WHERE Report_Name like 'lentra%hdr'
      AND Source_Name = {_sql_literal(source_name)}
      AND is_active = 1
"""
logger.info(f"[Lentra] {update_sql}")

result_df = spark.sql(update_sql)
try:
    num_affected = result_df.collect()[0]["num_affected_rows"]
except Exception:
    num_affected = None

logger.info(f"[Lentra] reset_ingestion_status — source_name={source_name!r} rows_reset={num_affected}")

# COMMAND ----------

dbutils.notebook.exit(f"Reset Status for source_name={source_name!r} — rows_reset={num_affected}")
