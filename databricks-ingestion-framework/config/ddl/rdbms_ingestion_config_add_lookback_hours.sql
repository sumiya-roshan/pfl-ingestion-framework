-- Adds the configurable lookback window (hours) used to build the dynamic
-- lookup/key-extraction watermark predicate for INCREMENTAL loads.
--
-- Consumed by JdbcConnector._lookback_predicates() via
-- IngestionTaskConfig.lookback_hours (set in ConfigManager._build_ingestion_task()).
-- NULL → defaults to 3 hours.
--
-- Predicate built: incremental_column >= (Silver_Last_Sink_Date - Lookback_Hours)
-- ORed with delta_column_2 when set.

ALTER TABLE migration_x_catalog.pfl_x_schema.rdbms_ingestion_config
  ADD COLUMN Lookback_Hours INT;
