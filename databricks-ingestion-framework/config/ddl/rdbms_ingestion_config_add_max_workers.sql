-- Adds a pipeline-wide thread-pool size to rdbms_ingestion_config.
--
-- ThreadPoolExecutor is created once per pipeline run (main.py), not per
-- table, so this value should be the SAME on every row for a given pipeline
-- (same pattern as Pipeline_Name). main.py reads it off the first loaded task
-- via ConfigManager._build_ingestion_task() -> IngestionTaskConfig.max_workers,
-- and falls back to the max_workers job widget/parameter when NULL.

ALTER TABLE migration_x_catalog.pfl_x_schema.rdbms_ingestion_config
  ADD COLUMN Max_Workers INT;
