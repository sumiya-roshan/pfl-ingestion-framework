"""Configuration-table access for the ingestion framework.

``ConfigManager`` has one responsibility: fetch source and ingestion
configuration rows.  The data classes below intentionally mirror the table
contracts; transformation and connection behaviour belongs in connectors.
"""
import json
from dataclasses import dataclass
from typing import Dict, List, Optional


SOURCE_SYSTEM_TABLE = "migration_x_catalog.pfl_x_schema.config_source_system"
INGESTION_CONFIG_TABLE = "migration_x_catalog.pfl_x_schema.ingestion_config"
AUDIT_TABLE = "migration_x_catalog.pfl_x_schema.data_pipeline_execution_master"


@dataclass
class SourceSystemConfig:
    source_id: int
    source_name: str
    source_type: str
    ingest_method: str
    uc_connection_name: Optional[str]
    host: Optional[str]
    port: Optional[int]
    database_name: Optional[str]
    driver_class: Optional[str]
    nosql_connection_uri: Optional[str]
    nosql_database: Optional[str]
    nosql_collection: Optional[str]
    sftp_root_path: Optional[str]
    sftp_file_format: Optional[str]
    sftp_key_fingerprint: Optional[str]
    landing_volume_path: Optional[str]
    auth_type: Optional[str]
    secret_scope: Optional[str]
    secret_key_credentials: Optional[str]
    target_catalog: Optional[str]
    target_schema: Optional[str]
    target_table: Optional[str]
    is_active: Optional[bool]
    created_by: Optional[str]
    created_ts: Optional[object]
    updated_by: Optional[str]
    updated_ts: Optional[object]


@dataclass
class IngestionObjectConfig:
    ingestion_object_id: int
    source_system_id: int
    source_system_name: Optional[str]
    source_schema: Optional[str]
    source_object_name: str
    source_object_type: Optional[str]
    custom_query: Optional[str]
    source_filter: Optional[str]
    load_type: Optional[str]
    write_mode: Optional[str]
    incremental_column: Optional[str]
    incremental_column_type: Optional[str]
    incremental_end_value: Optional[str]
    watermark_lag_sec: Optional[int]
    primary_key_cols: Optional[str]
    partition_column: Optional[str]
    data_read_size: Optional[int]
    file_format: Optional[str]
    sheet_name: Optional[str]
    target_catalog: Optional[str]
    target_schema: Optional[str]
    target_table: Optional[str]
    target_medallion_layer: Optional[str]
    schema_evolution_mode: Optional[str]
    pipeline_name: Optional[str]
    pipeline_id: Optional[str]
    retry_count: Optional[int]
    created_timestamp: Optional[object]
    updated_timestamp: Optional[object]
    last_updated_by: Optional[str]

    @property
    def primary_key_list(self) -> Optional[List[str]]:
        if self.primary_key_cols:
            return [key.strip() for key in self.primary_key_cols.split(",") if key.strip()]
        return None

    @property
    def full_target_table(self) -> str:
        if not all((self.target_catalog, self.target_schema, self.target_table)):
            raise ValueError("target_catalog, target_schema, and target_table are required")
        return f"{self.target_catalog}.{self.target_schema}.{self.target_table}"


class ConfigManager:
    """Fetch source-system and ingestion-object configuration."""

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
        self._source_systems: Dict[int, SourceSystemConfig] = {}
        self._ingestion_objects: Dict[int, IngestionObjectConfig] = {}
        if json_file_path:
            self._load_from_json(json_file_path)

    def _load_from_json(self, path: str) -> None:
        with open(path, encoding="utf-8") as config_file:
            data = json.load(config_file)
        for row in data.get("source_systems", []):
            source = self._build_source_system(row)
            self._source_systems[source.source_id] = source
        for row in data.get("ingestion_objects", []):
            ingestion = self._build_ingestion_object(row)
            self._ingestion_objects[ingestion.ingestion_object_id] = ingestion

    @staticmethod
    def _build_source_system(row: dict) -> SourceSystemConfig:
        return SourceSystemConfig(**{
            field: row.get(field) for field in SourceSystemConfig.__dataclass_fields__
        })

    @staticmethod
    def _build_ingestion_object(row: dict) -> IngestionObjectConfig:
        return IngestionObjectConfig(**{
            field: row.get(field) for field in IngestionObjectConfig.__dataclass_fields__
        })

    def get_source_system(self, source_system_id: int) -> SourceSystemConfig:
        if self.json_file_path:
            source = self._source_systems.get(source_system_id)
        else:
            row = (
                self.spark.table(self.source_system_table)
                .where(f"source_id = {int(source_system_id)}")
                .first()
            )
            source = self._build_source_system(row.asDict()) if row else None
        if source is None:
            raise ValueError(f"Source configuration not found for source_id={source_system_id}")
        return source

    def get_ingestion_object(self, ingestion_object_id: int) -> IngestionObjectConfig:
        if self.json_file_path:
            ingestion = self._ingestion_objects.get(ingestion_object_id)
        else:
            row = (
                self.spark.table(self.ingestion_config_table)
                .where(f"ingestion_object_id = {int(ingestion_object_id)}")
                .first()
            )
            ingestion = self._build_ingestion_object(row.asDict()) if row else None
        if ingestion is None:
            raise ValueError(
                f"Ingestion configuration not found for ingestion_object_id={ingestion_object_id}"
            )
        return ingestion

    def get_active_ingestion_objects(self, source_type: Optional[str] = None) -> List[int]:
        """Return ingestion object IDs whose source is active, optionally by type."""
        if self.json_file_path:
            active_source_ids = {
                source.source_id
                for source in self._source_systems.values()
                if source.is_active and (not source_type or source.source_type.upper() == source_type.upper())
            }
            return [
                ingestion.ingestion_object_id
                for ingestion in self._ingestion_objects.values()
                if ingestion.source_system_id in active_source_ids
            ]

        sources = self.spark.table(self.source_system_table).where("is_active = true")
        if source_type:
            sources = sources.where(f"upper(source_type) = '{source_type.upper()}'")
        return [
            int(row["ingestion_object_id"])
            for row in (
                self.spark.table(self.ingestion_config_table)
                .join(
                    sources.select("source_id"),
                    "source_system_id = source_id",
                    "inner",
                )
                .select("ingestion_object_id")
                .collect()
            )
        ]

    def get_ingestion_objects(
        self, source_type: Optional[str] = None
    ) -> List[IngestionObjectConfig]:
        """Fetch active-source ingestion configurations, optionally by source type."""
        return [
            self.get_ingestion_object(ingestion_object_id)
            for ingestion_object_id in self.get_active_ingestion_objects(source_type)
        ]
