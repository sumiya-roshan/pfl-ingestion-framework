"""
Maps source_type → connector implementation.

This is the single file to touch when a new source type is added.
Routing is based on source_system.source_type (case-insensitive).
"""
from ..utils.config_manager import SourceSystemConfig, IngestionTaskConfig
from .base_connector import BaseConnector
from .jdbc_connector import JdbcConnector
from .sftp_connector import SftpConnector
from .mongo_connector import MongoConnector
from .s3_connector import S3Connector
from .federated_connector import FederatedConnector

# ── Connector registry ────────────────────────────────────────────────────────
# Key   : value of config_source_system.source_type (upper-cased)
# Value : BaseConnector subclass to instantiate
_CONNECTOR_MAP: dict = {
    "POSTGRES": JdbcConnector,
    "POSTGRESQL": JdbcConnector,
    "PG":       JdbcConnector,
    "MYSQL":    JdbcConnector,
    "ORACLE":   JdbcConnector,
    "MSSQL":    JdbcConnector,    # SQL Server via JDBC
    "SQLSERVER": JdbcConnector,
    "SFTP":     SftpConnector,
    "MONGODB":  MongoConnector,
    "MONGO":    MongoConnector,
    "S3":       S3Connector,
    "POSTGRES_FEDERATED":   FederatedConnector,
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
