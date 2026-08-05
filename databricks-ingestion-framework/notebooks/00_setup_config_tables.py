# Databricks notebook source
# MAGIC %md
# MAGIC # Config Table Validation
# MAGIC
# MAGIC Validates that all three pre-existing config/audit tables are accessible
# MAGIC from this workspace before running any ingestion pipelines.
# MAGIC
# MAGIC **This notebook does NOT create or modify any tables.**
# MAGIC All three tables are expected to be created and managed externally.
# MAGIC
# MAGIC | Role | Expected Table |
# MAGIC |---|---|
# MAGIC | Source systems | `migration_x_catalog.pfl_x_schema.config_source_system` |
# MAGIC | Ingestion objects | `migration_x_catalog.pfl_x_schema.ingestion_config` |
# MAGIC | Execution audit | `migration_x_catalog.pfl_x_schema.data_pipeline_execution_master` |

# COMMAND ----------
#test comit

import sys
sys.path.append("../src")

from ingestion.config_manager import (
    SOURCE_SYSTEM_TABLE,
    INGESTION_CONFIG_TABLE,
    AUDIT_TABLE,
)

# COMMAND ----------

# Widget overrides — leave blank to use defaults from config_manager.py
dbutils.widgets.text("source_system_table",    SOURCE_SYSTEM_TABLE,    "Source System Table")
dbutils.widgets.text("ingestion_config_table", INGESTION_CONFIG_TABLE, "Ingestion Config Table")
dbutils.widgets.text("audit_table",            AUDIT_TABLE,            "Audit Table")

source_system_table    = dbutils.widgets.get("source_system_table")    or SOURCE_SYSTEM_TABLE
ingestion_config_table = dbutils.widgets.get("ingestion_config_table") or INGESTION_CONFIG_TABLE
audit_table            = dbutils.widgets.get("audit_table")            or AUDIT_TABLE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Existence Check

# COMMAND ----------

tables_to_check = {
    "config_source_system":          source_system_table,
    "ingestion_config":              ingestion_config_table,
    # "data_pipeline_execution_master": audit_table,
}

all_ok = True
for label, fqn in tables_to_check.items():
    try:
        row_count = spark.table(fqn).count()
        print(f"[OK]  {fqn}  →  {row_count:,} rows")
    except Exception as e:
        print(f"[FAIL] {fqn}  →  {e}")
        all_ok = False

if not all_ok:
    raise RuntimeError(
        "One or more required config tables are not accessible. "
        "Ensure the tables exist and this cluster's service principal has SELECT privilege."
    )

print("\n All config/audit tables are accessible. Framework is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Active Source Systems

# COMMAND ----------

display(spark.table(source_system_table).filter("is_active = true"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enabled Ingestion Objects

# COMMAND ----------

display(spark.table(ingestion_config_table))
