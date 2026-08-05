-- ============================================================================
-- CONNECTION_CONFIG
-- One row per physical source system connection (DB instance, SFTP server, Mongo cluster).
-- Credentials are NEVER stored here — only a reference to a Databricks secret scope/key.
-- ============================================================================
CREATE TABLE migration_x_catalog.pfl_x_schema.config_source_system (
  source_id               BIGINT GENERATED ALWAYS AS IDENTITY,
  source_name             STRING NOT NULL,          
  source_type             STRING NOT NULL,         
  ingest_method            STRING NOT NULL,    
  host                     STRING,
  port                     INT,
  database_name            STRING,
  driver_class             STRING,               
  connection_uri           STRING,                  
  nosql_replica_set        STRING,                                   
  nosql_collection_name    STRING,                 ,
  sftp_root_path           STRING,                  
  sftp_file_pattern        STRING,                  
  sftp_host_key_fingerprint STRING,                  
  landing_volume_path      STRING,                                   
  secret_scope             STRING NOT NULL,
  secret_key_credentials   STRING,                  
  is_active                BOOLEAN,
  created_by               STRING,
  created_ts               TIMESTAMP,
  updated_by               STRING,
  updated_ts               TIMESTAMP
) USING DELTA;
