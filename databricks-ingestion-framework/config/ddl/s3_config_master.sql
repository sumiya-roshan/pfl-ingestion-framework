-- Dedicated config table for the S3 connector only.
-- Replaces ingestion_config for S3 — one row per report/object
-- (config_master_id), carrying source bucket/path, sink and schedule
-- columns for that report. config_source_system is still used for S3
-- source-system identity and AWS credentials (secret_scope /
-- secret_key_credentials on THIS table are not read — see
-- S3ConfigManager), same two-table split as every other connector.
-- Read exclusively by S3ConfigManager (src/ingestion/utils/config_manager.py).

CREATE TABLE migration_x_catalog.pfl_x_schema.s3_config_master (
  config_master_id        INT,
  source_system_id        INT,
  report_name              STRING,
  key_column                STRING,
  source_name               STRING,
  source_bucket_name        STRING,
  external_path             STRING,
  frequency                 STRING,
  load_type                 STRING,
  raw_sink_bucket_name      STRING,
  raw_sink_file_path        STRING,
  column_delimiter          STRING,
  first_row_header          BOOLEAN,
  bronze_sink_schema_name   STRING,
  bronze_sink_table_name    STRING,
  business_date             DATE,
  status                    STRING,
  raw_last_sink_date        TIMESTAMP,
  silver_last_sink_date     TIMESTAMP,
  is_active                 BOOLEAN,
  sink_batch_start_date     TIMESTAMP,
  recipients                STRING,
  pipeline_name             STRING,
  secret_scope              STRING,
  secret_key_credentials    STRING,
  day_execution_count       INT,
  report_execution_day      STRING,
  compute_policy_name       STRING,
  compute_policy_id         STRING,
  cluster_option            STRING,
  worker_number             INT
) USING DELTA;
