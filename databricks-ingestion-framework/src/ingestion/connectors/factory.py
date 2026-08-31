"""
Maps source_type → connector implementation.

This is the single file to touch when a new source type is added.
Routing is based on source_system.source_type (case-insensitive).
"""
from ..utils.config_manager import SourceSystemConfig, IngestionTaskConfig
from .base_connector import BaseConnector
from .federated_connector import FederatedConnector
from .sftp_connector import SftpConnector
from .mongo_connector import MongoConnector
from .s3_connector import S3Connector

# ── Connector registry ────────────────────────────────────────────────────────
# Key   : value of config_source_system.source_type (upper-cased)
# Value : BaseConnector subclass to instantiate
#
# All RDBMS source types now route to FederatedConnector (Databricks
# Lakehouse Federation / Unity Catalog foreign catalog).
# JdbcConnector is no longer used — JDBC driver jars and credential
# config in config_source_system are no longer required for these types.
_CONNECTOR_MAP: dict = {
    "POSTGRES":   FederatedConnector,
    "POSTGRESQL": FederatedConnector,
    "PG":         FederatedConnector,
    "MYSQL":      FederatedConnector,
    "ORACLE":     FederatedConnector,
    "MSSQL":      FederatedConnector,
    "SQLSERVER":  FederatedConnector,
    "FEDERATED":  FederatedConnector,   # explicit alias if preferred
    "SFTP":       SftpConnector,
    "MONGODB":    MongoConnector,
    "MONGO":      MongoConnector,
    "S3":         S3Connector,
}


def get_connector(
    spark,
    source_system: SourceSystemConfig,
    ingest_obj: IngestionTaskConfig,
    secrets,
) -> BaseConnector:
    """
    Instantiate and return the correct connector for the given source_type.

    Parameters
    ----------
    spark         : active SparkSession
    source_system : SourceSystemConfig loaded from config_source_system
    ingest_obj    : IngestionTaskConfig loaded from ingestion_config
    secrets       : SecretResolver instance

    Raises
    ------
    ValueError if source_type has no registered connector.
    """
    source_type = source_system.source_type.upper()
    print("source_type",source_type)
    connector_cls = _CONNECTOR_MAP.get(source_type)
    if connector_cls is None:
        raise ValueError(
            f"No connector registered for source_type='{source_type}'. "
            f"Registered types: {sorted(_CONNECTOR_MAP.keys())}"
        )
    return connector_cls(spark, source_system, ingest_obj, secrets)
