"""
Reads config_source_system / ingestion_config Delta tables and returns
typed config objects consumed by the rest of the framework.

Also defines S3ConfigManager, which reads the separate, S3-only
s3_config_master table and adapts it into the same SourceSystemConfig /
IngestionObjectConfig shapes — see the S3ConfigManager docstring below.

Default table locations
-----------------------
  SOURCE_SYSTEM_TABLE    = migration_x_catalog.pfl_x_schema.config_source_system
  INGESTION_CONFIG_TABLE = migration_x_catalog.pfl_x_schema.ingestion_config
  AUDIT_TABLE            = main.monitoring.data_pipeline_execution_master
  S3_CONFIG_TABLE         = migration_x_catalog.pfl_x_schema.s3_config_master

DDL note
--------
  ingestion_config requires two columns beyond the original schema:
    pipeline_name  STRING   -- groups tables by Databricks Job name
    delta_layer    STRING   -- e.g. BRONZE; falls back to 'BRONZE' if NULL
  Run once:
    ALTER TABLE migration_x_catalog.pfl_x_schema.ingestion_config
      ADD COLUMNS (pipeline_name STRING, delta_layer STRING);
"""
import json
from dataclasses import dataclass
from typing import Optional, List, Dict

# ── Fully-qualified table name defaults ───────────────────────────────────────
SOURCE_SYSTEM_TABLE    = "migration_x_catalog.pfl_x_schema.config_source_system"
INGESTION_CONFIG_TABLE = "migration_x_catalog.pfl_x_schema.ingestion_config"
AUDIT_TABLE            = "migration_x_catalog.pfl_x_schema.data_pipeline_execution_master"
S3_CONFIG_TABLE        = "migration_x_catalog.pfl_x_schema.s3_config_master"

# s3_config_master has no catalog column — fixed to match config_source_system /
# ingestion_config, which both live under this catalog today.
DEFAULT_S3_TARGET_CATALOG = "migration_x_catalog"


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

    driver_class: Optional[str]       # explicit JDBC driver; falls back to built-in map
    connection_uri: Optional[str]     # full URI override

    nosql_replica_set: Optional[str]
    nosql_collection_name: Optional[str]

    sftp_root_path: Optional[str]
    sftp_file_pattern: Optional[str]
    sftp_host_key_fingerprint: Optional[str]

    extra_params: Optional[str]

    secret_scope: str
    secret_key_credentials: Optional[str]   # key → JSON {"username":…,"password":…}

    is_active: Optional[bool] = True


@dataclass
class IngestionObjectConfig:
    """One row per object (table / view / query / file) → ingestion_config."""
    ingestion_object_id: int
    source_system_id: int

    source_schema: Optional[str]
    source_object_name: str
    source_object_type: str          # TABLE | VIEW | QUERY | FILE

    custom_query: Optional[str]
    source_filter: Optional[str]

    load_type: str                   # FULL | INCREMENTAL
    write_mode: str                  # append | merge | overwrite

    incremental_column: Optional[str]
    incremental_end_value: Optional[str]

    primary_key_cols: Optional[str]  # comma-separated
    partition_column: Optional[str]

    data_read_size: Optional[int]

    file_format: Optional[str]
    sheet_name: Optional[str]

    s3_source_bucket_name: Optional[str]
    s3_external_path: Optional[str]
    s3_column_delimiter: Optional[str]
    s3_first_row_header: Optional[bool]
    s3_raw_sink_bucket_name: Optional[str]
    s3_raw_sink_file_path: Optional[str]

    target_catalog: str
    target_schema: str
    target_table: str

    schema_evolution_mode: Optional[str]

    # Pipeline grouping — matches Databricks Job name
    pipeline_name: str

    # Delta layer (BRONZE / SILVER / GOLD) — from config table, not widget
    delta_layer: Optional[str]

    # ── Derived helpers ────────────────────────────────────────────────────

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
        """Returns delta_layer from config, defaulting to BRONZE."""
        return (self.delta_layer or "BRONZE").upper()


# ─────────────────────────────────────────────────────────────────────────────
# ConfigManager
# ─────────────────────────────────────────────────────────────────────────────

class ConfigManager:
    """
    Loads source system and ingestion object configuration from Delta tables
    or a local JSON file (dev / test only).
    """

    def __init__(
        self,
        spark,
        source_system_table: str = SOURCE_SYSTEM_TABLE,
        ingestion_config_table: str = INGESTION_CONFIG_TABLE,
    ):
        self.spark                 = spark
        self.source_system_table   = source_system_table
        self.ingestion_config_table = ingestion_config_table

        self._source_systems:   Dict[int, SourceSystemConfig]   = {}
        self._ingestion_objects: Dict[int, IngestionObjectConfig] = {}



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
            is_active               = r.get("is_active", True),
            extra_params            = r.get("extra_params"),
        )

    @staticmethod
    def _build_ingestion_object(r: dict) -> IngestionObjectConfig:
        return IngestionObjectConfig(
            ingestion_object_id  = int(r["ingestion_object_id"]),
            source_system_id     = int(r["source_system_id"]),
            source_schema        = r.get("source_schema"),
            source_object_name   = r["source_object_name"],
            source_object_type   = str(r.get("source_object_type", "TABLE")).upper(),
            custom_query         = r.get("custom_query"),
            source_filter        = r.get("source_filter"),
            load_type            = str(r.get("load_type", "FULL")).upper(),
            write_mode           = str(r.get("write_mode", "append")).lower(),
            incremental_column   = r.get("incremental_column"),
            incremental_end_value= r.get("incremental_end_value"),
            primary_key_cols     = r.get("primary_key_cols"),
            partition_column     = r.get("partition_column"),
            data_read_size       = r.get("data_read_size"),
            file_format          = r.get("file_format"),
            sheet_name           = r.get("sheet_name"),
            s3_source_bucket_name   = r.get("s3_source_bucket_name"),
            s3_external_path        = r.get("s3_external_path"),
            s3_column_delimiter     = r.get("s3_column_delimiter"),
            s3_first_row_header     = r.get("s3_first_row_header"),
            s3_raw_sink_bucket_name = r.get("s3_raw_sink_bucket_name"),
            s3_raw_sink_file_path   = r.get("s3_raw_sink_file_path"),
            target_catalog       = r["target_catalog"],
            target_schema        = r["target_schema"],
            target_table         = r["target_table"],
            schema_evolution_mode= r.get("schema_evolution_mode"),
            pipeline_name        = r.get("pipeline_name"),
            delta_layer          = r.get("delta_layer"),
        )

    # ── Delta table readers ───────────────────────────────────────────────────

    def get_source_system(self, source_system_id: int) -> SourceSystemConfig:

        rows = (
            self.spark.table(self.source_system_table)
            .filter(f"source_id = {source_system_id} AND is_active = true")
            .collect()
        )
        if not rows:
            raise ValueError(
                f"No active row in {self.source_system_table} for source_id={source_system_id}"
            )
        return self._build_source_system(rows[0].asDict())

    def get_ingestion_object(self, ingestion_object_id: int) -> IngestionObjectConfig:

        rows = (
            self.spark.table(self.ingestion_config_table)
            .filter(f"ingestion_object_id = {ingestion_object_id}")
            .collect()
        )
        if not rows:
            raise ValueError(
                f"No row in {self.ingestion_config_table} "
                f"for ingestion_object_id={ingestion_object_id}"
            )
        return self._build_ingestion_object(rows[0].asDict())

    def get_active_ingestion_objects(
        self,
        source_type: Optional[str]      = None,
        source_system_id: Optional[int] = None,
        source_name: Optional[str]      = None,
        pipeline_name: Optional[str]    = None,
    ) -> List[int]:
        """
        Return ingestion_object_id values matching the given filters.
        All filters are optional and ANDed together.

        Parameters
        ----------
        source_type      : filter by config_source_system.source_type (e.g. 'POSTGRES')
        source_system_id : filter by config_source_system.source_id
        source_name      : filter by config_source_system.source_name (case-insensitive)
        pipeline_name    : filter by ingestion_config.pipeline_name
        """

        ic = self.spark.table(self.ingestion_config_table)

        if pipeline_name:
            ic = ic.filter(f"pipeline_name = '{pipeline_name}'")

        if source_system_id or source_type or source_name:
            ss = self.spark.table(self.source_system_table).filter("is_active = true")
            if source_system_id:
                ss = ss.filter(f"source_id = {source_system_id}")
            if source_type:
                ss = ss.filter(f"upper(source_type) = '{source_type.upper()}'")
            if source_name:
                ss = ss.filter(f"lower(source_name) = '{source_name.lower()}'")
            ic = ic.join(ss.select("source_id"), ic["source_system_id"] == ss["source_id"], "inner")

        return [
            int(r["ingestion_object_id"])
            for r in ic.select("ingestion_object_id").collect()
        ]


# ─────────────────────────────────────────────────────────────────────────────
# S3ConfigManager
# ─────────────────────────────────────────────────────────────────────────────

class S3ConfigManager:
    """
    Same public surface as ConfigManager (get_source_system /
    get_ingestion_object / get_active_ingestion_objects). Unlike
    ConfigManager, only the ingestion-object side is backed by a different
    table:

      get_source_system()           → reads config_source_system, the SAME
                                       table every other connector uses —
                                       S3 sources get a normal row there
                                       (source_type='S3') for identity and
                                       AWS keys (.secret_scope/
                                       .secret_key_credentials). Unchanged
                                       from before this table existed.

      get_ingestion_object()        → reads s3_config_master instead of
      get_active_ingestion_objects()  ingestion_config. s3_config_master
                                       REPLACES ingestion_config for S3
                                       only: one row per report/object
                                       (config_master_id), carrying the
                                       source bucket/path, sink and
                                       schedule columns for that report.
                                       Its secret_scope/secret_key_credentials
                                       columns are NOT read — AWS auth stays
                                       a source_system-level concern, same as
                                       every other connector.

    This table is read only by S3ConfigManager and is only ever routed to
    S3Connector — no other connector touches it.

    Row → dataclass mapping (s3_config_master → IngestionObjectConfig)
    ---------------------------------------------------------------------
      .ingestion_object_id      = config_master_id
      .source_object_name       = report_name
      .s3_source_bucket_name    = source_bucket_name
      .s3_external_path         = external_path
      .s3_column_delimiter      = column_delimiter (default ',' at read time)
      .s3_first_row_header      = first_row_header (default True at read time)
      .primary_key_cols         = key_column
      .write_mode               = 'merge' when key_column is set, else 'append'
      .target_catalog            = DEFAULT_S3_TARGET_CATALOG (fixed — table has no
                                    catalog column)
      .target_schema             = bronze_sink_schema_name (used as-is)
      .target_table              = bronze_sink_table_name
    """

    def __init__(
        self,
        spark,
        s3_config_table: str = S3_CONFIG_TABLE,
        source_system_table: str = SOURCE_SYSTEM_TABLE,
    ):
        self.spark = spark
        self.s3_config_table = s3_config_table
        self.source_system_table = source_system_table

    # ── Row builders ─────────────────────────────────────────────────────────

    @staticmethod
    def _build_ingestion_object(r: dict) -> IngestionObjectConfig:
        key_column = r.get("key_column")

        return IngestionObjectConfig(
            ingestion_object_id     = int(r["config_master_id"]),
            source_system_id        = int(r["source_system_id"]),
            source_schema           = None,
            source_object_name      = r["report_name"],
            source_object_type      = "FILE",
            custom_query            = None,
            source_filter           = None,
            load_type               = str(r.get("load_type") or "FULL").upper(),
            write_mode              = "merge" if key_column else "append",
            incremental_column      = None,
            incremental_end_value   = None,
            primary_key_cols        = key_column,
            partition_column        = None,
            data_read_size          = None,
            file_format             = "csv",
            sheet_name              = None,
            s3_source_bucket_name   = r.get("source_bucket_name"),
            s3_external_path        = r.get("external_path"),
            s3_column_delimiter     = r.get("column_delimiter"),
            s3_first_row_header     = r.get("first_row_header"),
            s3_raw_sink_bucket_name = r.get("raw_sink_bucket_name"),
            s3_raw_sink_file_path   = r.get("raw_sink_file_path"),
            target_catalog          = DEFAULT_S3_TARGET_CATALOG,
            target_schema           = r["bronze_sink_schema_name"],
            target_table            = r["bronze_sink_table_name"],
            schema_evolution_mode   = None,
            pipeline_name           = r.get("pipeline_name") or "s3_ingestion",
            delta_layer             = "BRONZE",
        )

    # ── Delta table readers ───────────────────────────────────────────────────

    def get_source_system(self, source_system_id: int) -> SourceSystemConfig:
        """Reads config_source_system — same table and row shape every
        other connector uses. S3 sources are just another row there."""
        rows = (
            self.spark.table(self.source_system_table)
            .filter(f"source_id = {source_system_id} AND is_active = true")
            .collect()
        )
        if not rows:
            raise ValueError(
                f"No active row in {self.source_system_table} for source_id={source_system_id}"
            )
        return ConfigManager._build_source_system(rows[0].asDict())

    def get_ingestion_object(self, ingestion_object_id: int) -> IngestionObjectConfig:
        rows = (
            self.spark.table(self.s3_config_table)
            .filter(f"config_master_id = {ingestion_object_id}")
            .collect()
        )
        if not rows:
            raise ValueError(
                f"No row in {self.s3_config_table} for config_master_id={ingestion_object_id}"
            )
        return self._build_ingestion_object(rows[0].asDict())

    def get_active_ingestion_objects(
        self,
        source_type: Optional[str]      = None,
        source_system_id: Optional[int] = None,
        source_name: Optional[str]      = None,
        pipeline_name: Optional[str]    = None,
    ) -> List[int]:
        """
        Return config_master_id values matching the given filters.
        source_type is accepted for interface compatibility with
        ConfigManager but is always 'S3' here, so it only excludes results
        when explicitly set to something else.
        """
        if source_type and source_type.upper() != "S3":
            return []

        df = self.spark.table(self.s3_config_table).filter("is_active = true")

        if source_system_id:
            df = df.filter(f"source_system_id = {source_system_id}")
        if source_name:
            df = df.filter(f"lower(source_name) = '{source_name.lower()}'")
        # if pipeline_name:
        #     df = df.filter(f"pipeline_name = '{pipeline_name}'")

        return [int(r["config_master_id"]) for r in df.select("config_master_id").collect()]
