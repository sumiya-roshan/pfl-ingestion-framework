-- Adds the per-table presence-check query template to rdbms_ingestion_config.
--
-- Consumed by LookupExecutor._resolve_query_for_task() via
-- IngestionTaskConfig.lookup_query (set in ConfigManager._build_ingestion_task()).
-- NULL → LookupExecutor auto-generates SELECT {key_cols} FROM {schema}.{table} LIMIT 1
--
-- Supported placeholders (case-insensitive): {schema} / {source_schema},
-- {table} / {source_object_name}, {key_column} / {key_columns}.
-- Example: SELECT 1 FROM {schema}.{table} WHERE {key_column} IS NOT NULL LIMIT 1

ALTER TABLE migration_x_catalog.pfl_x_schema.rdbms_ingestion_config
  ADD COLUMN Lookup_Query_Template STRING;
