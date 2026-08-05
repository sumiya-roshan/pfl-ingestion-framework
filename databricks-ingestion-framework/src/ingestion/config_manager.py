"""
Reads config_source_system / ingestion_config Delta tables and returns
typed config objects consumed by the rest of the framework.

All three fully-qualified table names are exposed as module-level constants and
can be overridden when constructing ConfigManager — nothing is hard-coded for a
specific environment.

Default table locations
-----------------------
  SOURCE_SYSTEM_TABLE    = migration_x_catalog.pfl_x_schema.config_source_system
  INGESTION_CONFIG_TABLE = migration_x_catalog.pfl_x_schema.ingestion_config
  AUDIT_TABLE            = main.monitoring.data_pipeline_execution_master
"""
import json
from dataclasses import dataclass
from typing import Optional, List, Dict

# ── Fully-qualified table name defaults (override in ConfigManager.__init__) ──
SOURCE_SYSTEM_TABLE    = "migration_x_catalog.pfl_x_schema.config_source_system"
INGESTION_CONFIG_TABLE = "migration_x_catalog.pfl_x_schema.ingestion_config"
AUDIT_TABLE            = "main.monitoring.data_pipeline_execution_master"


# ─────────────────────────────────────────────────────────────────────────────
# Data classes — mirror the Delta table schemas exactly
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SourceSystemConfig:
    """
    One row per physical source system.
    Maps to: config_source_system
    """
    source_id: int                            # BIGINT IDENTITY — primary key
    source_name: str                          # human-readable name
    source_type: str                          # POSTGRES | MYSQL | ORACLE | SFTP | MONGODB | REST …
    ingest_method: str                        # JDBC | SFTP | MONGODB | REST | API …

    host: Optional[str]
    port: Optional[int]
    database_name: Optional[str]

    # JDBC: explicit driver class; falls back to built-in map when None
    driver_class: Optional[str]
    # Full connection URI override (used as-is when set; skips host/port/db build)
    connection_uri: Optional[str]

    # MongoDB-specific
    nosql_replica_set: Optional[str]
    nosql_collection_name: Optional[str]      # default collection (overridable per ingestion object)

    # SFTP-specific
    sftp_root_path: Optional[str]             # root directory on the SFTP server
    sftp_file_pattern: Optional[str]          # default glob pattern for file matching
    sftp_host_key_fingerprint: Optional[str]  # host key for strict host-key checking

    # Staging / raw landing
    landing_volume_path: Optional[str]        # Databricks Volume path used for S3/raw landing

    # Credentials — stored in Databricks Secrets as a JSON payload:
    #   {"username": "<value>", "password": "<value>"}
    secret_scope: str
    secret_key_credentials: Optional[str]     # secret key name holding the JSON credential

    is_active: Optional[bool] = True


@dataclass
class IngestionObjectConfig:
    """
    One row per object (table / view / query / SFTP file set) to ingest.
    Maps to: ingestion_config
    """
    ingestion_object_id: int                  # BIGINT IDENTITY — primary key
    source_system_id: int                     # FK → config_source_system.source_id

    # Source object identification
    source_schema: Optional[str]              # DB schema / SFTP sub-folder
    source_object_name: str                   # table name / view name / file base name
    source_object_type: str                   # TABLE | VIEW | QUERY | FILE

    custom_query: Optional[str]               # overrides source_schema.source_object_name when set
    source_filter: Optional[str]              # additional WHERE predicate injected at extract time

    # Load behaviour
    load_type: str                            # FULL | INCREMENTAL
    write_mode: str                           # append | merge | overwrite

    incremental_column: Optional[str]         # watermark column (e.g. updated_at, id)
    incremental_end_value: Optional[str]      # initial watermark for first run (no prior data)

    # Key / partitioning
    primary_key_cols: Optional[str]           # comma-separated column names used as merge keys
    partition_column: Optional[str]           # column to partition target Delta table by

    # Read tuning
    data_read_size: Optional[int]             # fetch/batch size hint

    # File-based sources (SFTP, S3, Volumes)
    file_format: Optional[str]                # csv | json | parquet | excel | fixed_width
    sheet_name: Optional[str]                 # for Excel/multi-sheet sources

    # Target Delta table
    target_catalog: str
    target_schema: str
    target_table: str

    # Schema evolution strategy passed to Delta writer
    schema_evolution_mode: Optional[str]      # none | merge | overwrite

    # Lakeflow / pipeline integration
    pipeline_id: Optional[str]

    is_enabled: Optional[bool] = True

    # ── Derived helpers ────────────────────────────────────────────────────

    @property
    def primary_key_list(self) -> Optional[List[str]]:
        """Returns primary_key_cols as a Python list, or None if not set."""
        if self.primary_key_cols:
            return [k.strip() for k in self.primary_key_cols.split(",") if k.strip()]
        return None

    @property
    def full_target_table(self) -> str:
        """Returns the fully-qualified target table name."""
        return f"{self.target_catalog}.{self.target_schema}.{self.target_table}"


# ─────────────────────────────────────────────────────────────────────────────
# ConfigManager
# ─────────────────────────────────────────────────────────────────────────────

class ConfigManager:
    """
    Loads source system and ingestion object configuration from Delta tables
    (or from a local JSON file for unit-testing outside Databricks).

    Parameters
    ----------
    spark                  : active SparkSession
    source_system_table    : fully-qualified name of config_source_system
    ingestion_config_table : fully-qualified name of ingestion_config
    json_file_path         : path to a local JSON file (dev / test only)
    """

    def __init__(
        self,
        spark,
        source_system_table: str = SOURCE_SYSTEM_TABLE,
        ingestion_config_table: str = INGESTION_CONFIG_TABLE,
        json_file_path: Optional[str] = None,
    ):
        self.spark = spark
        self.source_system_table = source_system_table
        self.ingestion_config_table = ingestion_config_table
        self.json_file_path = json_file_path

        # In-memory caches populated only in JSON mode
        self._source_systems: Dict[int, SourceSystemConfig] = {}
        self._ingestion_objects: Dict[int, IngestionObjectConfig] = {}

        if json_file_path:
            self._load_from_json(json_file_path)

    # ── JSON fallback (local / CI testing) ───────────────────────────────────

    def _load_from_json(self, path: str) -> None:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        for ss_dict in data.get("source_systems", []):
            obj = self._build_source_system(ss_dict)
            self._source_systems[obj.source_id] = obj

        for io_dict in data.get("ingestion_objects", []):
            obj = self._build_ingestion_object(io_dict)
            self._ingestion_objects[obj.ingestion_object_id] = obj

    # ── Row-to-dataclass builders (used by both JSON and Delta paths) ─────────

    @staticmethod
    def _build_source_system(r: dict) -> SourceSystemConfig:
        return SourceSystemConfig(
            source_id=int(r["source_id"]),
            source_name=r["source_name"],
            source_type=str(r["source_type"]).upper(),
            ingest_method=str(r.get("ingest_method", "JDBC")).upper(),
            host=r.get("host"),
            port=r.get("port"),
            database_name=r.get("database_name"),
            driver_class=r.get("driver_class"),
            connection_uri=r.get("connection_uri"),
            nosql_replica_set=r.get("nosql_replica_set"),
            nosql_collection_name=r.get("nosql_collection_name"),
            sftp_root_path=r.get("sftp_root_path"),
            sftp_file_pattern=r.get("sftp_file_pattern"),
            sftp_host_key_fingerprint=r.get("sftp_host_key_fingerprint"),
            landing_volume_path=r.get("landing_volume_path"),
            secret_scope=r["secret_scope"],
            secret_key_credentials=r.get("secret_key_credentials"),
            is_active=r.get("is_active", True),
        )

    @staticmethod
    def _build_ingestion_object(r: dict) -> IngestionObjectConfig:
        return IngestionObjectConfig(
            ingestion_object_id=int(r["ingestion_object_id"]),
            source_system_id=int(r["source_system_id"]),
            source_schema=r.get("source_schema"),
            source_object_name=r["source_object_name"],
            source_object_type=str(r.get("source_object_type", "TABLE")).upper(),
            custom_query=r.get("custom_query"),
            source_filter=r.get("source_filter"),
            load_type=str(r.get("load_type", "FULL")).upper(),
            write_mode=str(r.get("write_mode", "append")).lower(),
            incremental_column=r.get("incremental_column"),
            incremental_end_value=r.get("incremental_end_value"),
            primary_key_cols=r.get("primary_key_cols"),
            partition_column=r.get("partition_column"),
            data_read_size=r.get("data_read_size"),
            file_format=r.get("file_format"),
            sheet_name=r.get("sheet_name"),
            target_catalog=r["target_catalog"],
            target_schema=r["target_schema"],
            target_table=r["target_table"],
            schema_evolution_mode=r.get("schema_evolution_mode"),
            pipeline_id=r.get("pipeline_id"),
            is_enabled=r.get("is_enabled", True),
        )

    # ── Delta table readers ───────────────────────────────────────────────────

    def get_source_system(self, source_system_id: int) -> SourceSystemConfig:
        """Return the active SourceSystemConfig for the given source_id."""
        if self.json_file_path:
            obj = self._source_systems.get(source_system_id)
            if obj is None:
                raise ValueError(
                    f"No source_system found in JSON for source_system_id={source_system_id}"
                )
            return obj

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
        """Return the enabled IngestionObjectConfig for the given ingestion_object_id."""
        if self.json_file_path:
            obj = self._ingestion_objects.get(ingestion_object_id)
            if obj is None:
                raise ValueError(
                    f"No ingestion_object found in JSON for ingestion_object_id={ingestion_object_id}"
                )
            return obj

        rows = (
            self.spark.table(self.ingestion_config_table)
            .filter(f"ingestion_object_id = {ingestion_object_id} AND is_enabled = true")
            .collect()
        )
        if not rows:
            raise ValueError(
                f"No enabled row in {self.ingestion_config_table} "
                f"for ingestion_object_id={ingestion_object_id}"
            )
        return self._build_ingestion_object(rows[0].asDict())

    def get_active_ingestion_objects(
        self,
        source_type: Optional[str] = None,
    ) -> List[int]:
        """
        Return all enabled ingestion_object_id values.
        Optionally filter to only objects whose source system matches source_type
        (e.g. 'POSTGRES', 'SFTP', 'MONGODB').
        """
        if self.json_file_path:
            if source_type:
                matching_ss_ids = {
                    sid
                    for sid, ss in self._source_systems.items()
                    if ss.source_type == source_type.upper() and ss.is_active
                }
                return [
                    iid
                    for iid, io in self._ingestion_objects.items()
                    if io.source_system_id in matching_ss_ids and io.is_enabled
                ]
            return [
                iid
                for iid, io in self._ingestion_objects.items()
                if io.is_enabled
            ]

        ic = self.spark.table(self.ingestion_config_table).filter("is_enabled = true")

        if source_type:
            ss = (
                self.spark.table(self.source_system_table)
                .filter(f"is_active = true AND upper(source_type) = '{source_type.upper()}'")
                .select("source_id")
            )
            ic = ic.join(ss, ic["source_system_id"] == ss["source_id"], "inner")

        return [
            int(r["ingestion_object_id"])
            for r in ic.select("ingestion_object_id").collect()
        ]
