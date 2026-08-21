from __future__ import annotations
"""
Reads config_source_system and dynamically routes to child ingestion config tables
via the config_master table. Returns typed config objects.

All source types (RDBMS, NoSQL, S3) use the same routing:
  config_master_id → config_master → child table (e.g. rdbms_ingestion_config,
  nosql_ingestion_config, s3_config_master) → active rows for source_name.

Default table locations
-----------------------
  SOURCE_SYSTEM_TABLE = migration_x_catalog.pfl_x_schema.config_source_system
  CONFIG_MASTER_TABLE = migration_x_catalog.pfl_x_schema.config_master
  AUDIT_TABLE         = migration_x_catalog.pfl_x_schema.data_pipeline_execution_master
"""
import json
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

# ── Fully-qualified table name defaults ───────────────────────────────────────
SOURCE_SYSTEM_TABLE = "migration_x_catalog.pfl_x_schema.config_source_system"
CONFIG_MASTER_TABLE = "migration_x_catalog.pfl_x_schema.config_master"
AUDIT_TABLE         = "migration_x_catalog.pfl_x_schema.tb_audit_log"

# Audit lifecycle values shared by the entry point, orchestrator, and logger.
AUDIT_STATUS_INPROGRESS = "INPROGRESS"
AUDIT_STATUS_SUCCESS    = "SUCCESS"
AUDIT_STATUS_FAILED     = "FAILED"
AUDIT_STATUS_SKIPPED    = "SKIPPED"   # Used by source_lookup when a table has 0 rows

# ─────────────────────────────────────────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SourceSystemConfig:
    """One row per physical source system → config_source_system."""
    source_id: int
    source_name: str
    source_type: str           # POSTGRES | MYSQL | ORACLE | SFTP | MONGODB …
    ingest_method: str         # JDBC | SFTP | MONGODB …

    host: Optional[str]
    port: Optional[int]
    database_name: Optional[str]

    driver_class: Optional[str]       
    connection_uri: Optional[str]     

    nosql_replica_set: Optional[str]
    nosql_collection_name: Optional[str]

    sftp_root_path: Optional[str]
    sftp_file_pattern: Optional[str]
    sftp_host_key_fingerprint: Optional[str]

    extra_params: Optional[str]

    secret_scope: str
    secret_key_credentials: Optional[str] 

    is_active: int

    landing_volume_path: Optional[str]

    retry_count: Optional[int]
    retry_interval: Optional[int]

    def to_dict(self) -> dict:
        import decimal
        res = {}
        for k, v in self.__dict__.items():
            if k.startswith('_'):
                continue
            if isinstance(v, decimal.Decimal):
                res[k] = int(v) if v % 1 == 0 else float(v)
            else:
                res[k] = v
        return res

    @classmethod
    def from_dict(cls, d: dict) -> SourceSystemConfig:
        return cls(**d)


@dataclass
class IngestionTaskConfig:
    """Unified configuration for a single ingestion task, reading from flattened child config tables."""
    config_id: int
    source_schema: Optional[str]
    source_object_name: str
    custom_query: Optional[str]
    load_type: str
    incremental_column: Optional[str]
    primary_key_cols: Optional[str]
    target_catalog: str           
    target_schema: str
    target_table: str
    pipeline_name: str
    delta_layer: Optional[str]
    data_read_size: Optional[int]
    file_format: Optional[str]
    write_mode: str
    priority: int
    batch_id: int
    s3_source_bucket_name: Optional[str]
    s3_external_path: Optional[str]
    s3_column_delimiter: Optional[str]
    s3_first_row_header: Optional[bool]
    s3_raw_sink_bucket_name: Optional[str]
    s3_raw_sink_file_path: Optional[str]

    schema_evolution_mode: Optional[str]
    partition_column: Optional[str]
    source_filter: Optional[str]

    
    # Set at runtime by the lookup task from pipeline_lookup_config.lookup_query.
    # None means the LookupExecutor will auto-generate SELECT COUNT(*) FROM ...
    lookup_query: Optional[str] = None

    # Set at runtime by the lookup task from pipeline_lookup_config.query_timeout,
    # format 'HH:mm:ss' (e.g. '12:00:00'). Applied to the JDBC statement so the
    # source DB cancels the query itself on expiry — see JdbcConnector._read_options().
    query_timeout: Optional[str] = None

    @property
    def primary_key_list(self) -> Optional[List[str]]:
        if self.primary_key_cols:
            return [k.strip() for k in self.primary_key_cols.split(",") if k.strip()]
        return None

    @property
    def full_target_table(self) -> str:
        return f"{self.target_catalog}.{self.target_schema}.{self.target_table}"

    @property
    def effective_delta_layer(self) -> str:
        return (self.delta_layer or "BRONZE").upper()

    def to_dict(self) -> dict:
        import decimal
        res = {}
        for k, v in self.__dict__.items():
            if k.startswith('_'):
                continue
            if isinstance(v, decimal.Decimal):
                res[k] = int(v) if v % 1 == 0 else float(v)
            else:
                res[k] = v
        return res

    @classmethod
    def from_dict(cls, d: dict) -> IngestionTaskConfig:
        return cls(**d)


# ─────────────────────────────────────────────────────────────────────────────
# ConfigManager
# ─────────────────────────────────────────────────────────────────────────────

class ConfigManager:
    """
    Loads source system configuration, and routes through config_master to dynamically
    fetch active ingestion tasks from the appropriate child config table.
    """

    def __init__(
        self,
        spark,
        source_system_table: str = SOURCE_SYSTEM_TABLE,
        config_master_table: str = CONFIG_MASTER_TABLE,
        target_catalog: str      = "hive_metastore"
    ):
        self.spark               = spark
        self.source_system_table = source_system_table
        self.config_master_table = config_master_table
        self.target_catalog      = target_catalog

    # ── Row builders ─────────────────────────────────────────────────────────

    @staticmethod
    def _build_source_system(r: dict) -> SourceSystemConfig:
        return SourceSystemConfig(
            source_id               = int(r["source_id"]),
            source_name             = r["source_name"],
            source_type             = str(r["source_type"]).upper(),
            ingest_method           = str(r.get("ingest_method", "JDBC")).upper(),
            host                    = r.get("host"),
            port                    = r.get("port"),
            database_name           = r.get("database_name"),
            driver_class            = r.get("driver_class"),
            connection_uri          = r.get("connection_uri"),
            nosql_replica_set       = r.get("nosql_replica_set"),
            nosql_collection_name   = r.get("nosql_collection_name"),
            sftp_root_path          = r.get("sftp_root_path"),
            sftp_file_pattern       = r.get("sftp_file_pattern"),
            sftp_host_key_fingerprint = r.get("sftp_host_key_fingerprint"),
            secret_scope            = r["secret_scope"],
            secret_key_credentials  = r.get("secret_key_credentials"),
            is_active               = r.get("is_active", 1),
            extra_params            = r.get("extra_params"),
            landing_volume_path     = r.get("landing_volume_path"),
            retry_count             = r.get("retry_count"),
            retry_interval          = r.get("retry_interval"),
        )

    def _build_ingestion_task(self, r: dict) -> IngestionTaskConfig:
        """
        Dynamically falls back across different column aliases to support 
        both RDBMS and NoSQL schema structures without hardcoding.
        """
        source_object = (
            r.get("Source_Table_Name") or 
            r.get("Source_Collection_Name") or 
            r.get("Source_Object_Name") or 
            r.get("report_name") or      # S3: s3_config_master.report_name
            ""
        )
        
        inc_col = (
            r.get("Key_Column") or 
            r.get("Incremental_Column")
        )
        
        pk_cols = (
            r.get("Key_Column") or 
            r.get("Primary_Key_Cols") or
            r.get("key_column")          # S3: s3_config_master.key_column (lowercase)
        )

        load_type = str(r.get("Load_type") or r.get("Load_Type") or r.get("load_type") or "FULL").upper()
        default_write_mode = "overwrite" if load_type == "FULL" else "append"

        # S3: target schema/table use different column names in s3_config_master
        target_schema = (
            r.get("Sink_Schema_Name") or
            r.get("bronze_sink_schema_name")   # S3
        )
        target_table = (
            r.get("Sink_Table_Name") or
            r.get("bronze_sink_table_name")    # S3
        )

        return IngestionTaskConfig(
            config_id           = int(r.get("Config_ID") or r.get("config_id") or r.get("config_master_id") or 0),
            source_schema       = r.get("Source_Schema_Name"),
            source_object_name  = source_object,
            custom_query        = r.get("Source_Query"),
            load_type           = load_type,
            incremental_column  = inc_col,
            primary_key_cols    = pk_cols,
            target_catalog      = self.target_catalog,
            target_schema       = target_schema,
            target_table        = target_table,
            pipeline_name       = r.get("Pipeline_Name") or r.get("pipeline_name"),
            delta_layer         = r.get("Delta_Layer") or r.get("delta_layer"),
            data_read_size      = r.get("data_size") or r.get("data_read_size"),
            file_format         = r.get("file_format"),
            write_mode          = r.get("write_mode") or ("merge" if pk_cols else default_write_mode),
            priority            = r.get("priority") or 0,
            batch_id            = r.get("batch_id"),
            schema_evolution_mode = r.get("schema_evolution_mode"),
            partition_column    = r.get("partition_column"),
            source_filter       = r.get("source_filter"),
            # S3-specific fields — present only in s3_config_master rows
            s3_source_bucket_name   = r.get("s3_source_bucket_name") or r.get("source_bucket_name"),
            s3_external_path        = r.get("s3_external_path")       or r.get("external_path"),
            s3_column_delimiter     = r.get("s3_column_delimiter")    or r.get("column_delimiter"),
            s3_first_row_header     = r.get("s3_first_row_header")    or r.get("first_row_header"),
            s3_raw_sink_bucket_name = r.get("s3_raw_sink_bucket_name") or r.get("raw_sink_bucket_name"),
            s3_raw_sink_file_path   = r.get("s3_raw_sink_file_path")  or r.get("raw_sink_file_path"),
        )

    # ── Database Operations ───────────────────────────────────────────────────

    def get_source_system(self, source_system_id: int) -> SourceSystemConfig:
        rows = (
            self.spark.table(self.source_system_table)
            .filter(f"source_id = {source_system_id} AND is_active = 1")
            .collect()
        )
        if not rows:
            raise ValueError(
                f"No active row in {self.source_system_table} for source_id={source_system_id}"
            )
        return self._build_source_system(rows[0].asDict())

    def get_active_tasks(
        self,
        config_master_id: int,
        source_system_id: int,
        pipeline_name: Optional[str] = None,
        batch_start_date: Optional[str] = None
    ) -> Tuple[SourceSystemConfig, List[IngestionTaskConfig]]:
        """
        1. Fetch Source System by source_system_id to get credentials & source_name.
        2. Fetch the specific child config table location from config_master.
        3. Query the child config table for active tasks for this source_name,
           filtering by pipeline_name if provided.
        4. If batch_start_date is provided and not '1', filter by sink_batch_started_date.
        """
        
        # 1. Resolve source system
        source_sys = self.get_source_system(source_system_id)
        source_name = source_sys.source_name
        print(source_sys)
        print(source_name)

        # 2. Find child config table location from master
        master_rows = (
            self.spark.table(self.config_master_table)
            .filter(f"config_id = {config_master_id}")
            .collect()
        )
        print(master_rows)
        if not master_rows:
            raise ValueError(f"No entry in {self.config_master_table} for config_id={config_master_id}")
        
        m_row = master_rows[0].asDict()
        catalog = m_row.get("config_catalog_name")
        schema = m_row.get("config_schema_name")
        table = m_row.get("config_table_name")
        
        child_table_fqn = f"{catalog}.{schema}.{table}"

        # 3. Query the child config table
        child_df = self.spark.table(child_table_fqn)
        child_cols = [c.lower() for c in child_df.columns]

        # Case-insensitive column resolution
        src_col = next((c for c in child_df.columns if c.lower() == "source_name"), "Source_Name")
        active_col = next((c for c in child_df.columns if c.lower() == "is_active"), "Is_Active")

        # Basic filtering by source system and active status
        filtered_df = child_df.filter(f"{src_col} = '{source_name}' AND {active_col} = 1")

        # Apply multi-refresh batch start date filtering if triggered by orchestrator
        if batch_start_date and str(batch_start_date).strip() != "1":
            date_col = next((c for c in child_df.columns if c.lower() == "sink_batch_started_date"), None)
            if date_col:
                clean_date = str(batch_start_date).replace("T", " ").split(".")[0]
                print(f"[ConfigManager] Filtering active tasks by {date_col} = '{clean_date}'")
                filtered_df = filtered_df.filter(
                    f"date_format({date_col}, 'yyyy-MM-dd HH:mm:ss') = date_format('{clean_date}', 'yyyy-MM-dd HH:mm:ss')"
                )
            else:
                print(f"[ConfigManager] Warning: {child_table_fqn} has no sink_batch_started_date column. Skipping filter.")

        child_rows = filtered_df.orderBy("priority").collect()

        tasks = []
        for r in child_rows:
            task = self._build_ingestion_task(r.asDict())
            # If pipeline_name is specified, only include tasks that match it
            if pipeline_name and task.pipeline_name != pipeline_name:
                continue
            tasks.append(task)

        return source_sys, tasks

    def update_sink_metadata(
        self,
        config_master_id: int,
        ingest_obj: "IngestionTaskConfig",
        sink_batch_started_date,
        rownum: int,
        data_size: int,
    ) -> None:
        """
        Updates business_date, raw_last_sink_date, sink_batch_started_date,
        rownum, and data_size on the child config table row for this task,
        after a successful ingestion run.
        """
        master_rows = (
            self.spark.table(self.config_master_table)
            .filter(f"config_id = {config_master_id}")
            .collect()
        )
        if not master_rows:
            raise ValueError(f"No entry in {self.config_master_table} for config_id={config_master_id}")
        m_row = master_rows[0].asDict()
        child_table_fqn = f"{m_row.get('config_catalog_name')}.{m_row.get('config_schema_name')}.{m_row.get('config_table_name')}"

        business_date = sink_batch_started_date.date()

        # deltacolumn_1 = the source's incremental/watermark column, read from
        # the bronze table just written (not the config table itself).
        raw_last_sink_date = None
        if ingest_obj.incremental_column:
            row = (
                self.spark.table(ingest_obj.full_target_table)
                .agg({ingest_obj.incremental_column: "max"})
                .collect()[0]
            )
            raw_last_sink_date = row[0]

        self.spark.sql(f"""
            UPDATE {child_table_fqn}
            SET business_date           = {self._sql_literal(business_date)},
                raw_last_sink_date      = {self._sql_literal(raw_last_sink_date)},
                sink_batch_started_date = {self._sql_literal(sink_batch_started_date)},
                rownum                  = {int(rownum or 0)},
                data_size               = {int(data_size or 0)}
            WHERE config_id = {int(ingest_obj.config_id)}
        """)

    @staticmethod
    def _sql_literal(value) -> str:
        return "NULL" if value is None else "'" + str(value).replace("'", "''") + "'"
