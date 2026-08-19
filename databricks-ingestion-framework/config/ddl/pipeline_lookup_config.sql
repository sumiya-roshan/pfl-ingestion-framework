-- ============================================================
-- pipeline_lookup_config
-- ============================================================
-- Stores a COUNT query TEMPLATE at the pipeline level.
-- The source_lookup task applies this template to every table
-- in the pipeline (substituting {schema} and {table} at runtime).
-- Tables that return 0 are individually skipped; tables with
-- data are passed to main.py for ingestion.
--
-- One row per (pipeline_name, config_master_id).
-- If lookup_query_template is NULL, the lookup task auto-generates:
--   SELECT COUNT(*) FROM {source_schema}.{source_object_name}
--
-- Template placeholders (case-insensitive, both supported):
--   {schema}  or  {source_schema}   → task.source_schema
--   {table}   or  {source_object_name} → task.source_object_name
--
-- Usage
-- -----
-- The source_lookup notebook looks up one row by:
--   pipeline_name    = widget value "pipeline_name"
--   config_master_id = widget value "config_master_id"
--   is_active        = true
-- and applies the template to each active task in that pipeline.
-- ============================================================

CREATE TABLE IF NOT EXISTS migration_x_catalog.pfl_x_schema.pipeline_lookup_config (
  id                     BIGINT     GENERATED ALWAYS AS IDENTITY,
  pipeline_name          STRING     NOT NULL  COMMENT 'Pipeline name — must match the pipeline_name widget value in the job',
  config_master_id       BIGINT     NOT NULL  COMMENT 'config_master routing ID — matches the config_master_id widget in the job',
  lookup_query_template  STRING               COMMENT 'COUNT query template applied per table. Supports {schema}/{source_schema} and {table}/{source_object_name} placeholders. Auto-generates SELECT COUNT(*) FROM {schema}.{table} if NULL.',
  is_active              BOOLEAN    NOT NULL  DEFAULT true  COMMENT 'Set to false to disable lookup for this pipeline without deleting the row',
  created_by             STRING,
  created_ts             TIMESTAMP  DEFAULT current_timestamp(),
  updated_by             STRING,
  updated_ts             TIMESTAMP,
  CONSTRAINT pk_pipeline_lookup           PRIMARY KEY (id),
  CONSTRAINT uq_pipeline_lookup_pipeline  UNIQUE (pipeline_name, config_master_id)
) USING DELTA
COMMENT 'Pipeline-level COUNT query template for the source_lookup pre-ingestion task. One row per pipeline.';

-- ── Sample INSERTs ───────────────────────────────────────────────────────────
-- INSERT INTO migration_x_catalog.pfl_x_schema.pipeline_lookup_config
--   (pipeline_name, config_master_id, lookup_query_template, is_active, created_by)
-- VALUES
--
--   -- Auto-generate per table: SELECT COUNT(*) FROM {schema}.{table}
--   ('pfl_rdbms_ingestion', 2, NULL, true, 'admin'),
--
--   -- Custom template: count only today's rows for every table
--   ('pfl_oracle_ingestion', 3,
--    'SELECT COUNT(*) FROM {schema}.{table} WHERE load_date = CURRENT_DATE',
--    true, 'admin'),
--
--   -- Template with just {table} (no schema prefix)
--   ('pfl_mysql_ingestion', 5,
--    'SELECT COUNT(*) FROM {table}',
--    true, 'admin');
