"""
Common interface every connector implements.

The orchestrator only ever talks to this interface, so adding a new source
type means adding one new connector class + one line in connectors/factory.py.
"""
from abc import ABC, abstractmethod
from typing import Optional, Tuple

from pyspark.sql import DataFrame

from ..utils.config_manager import SourceSystemConfig, IngestionObjectConfig


class BaseConnector(ABC):

    def __init__(
        self,
        spark,
        source_system: SourceSystemConfig,
        ingest_obj: IngestionObjectConfig,
        secrets,
    ):
        self.spark = spark
        self.source_system = source_system   # config_source_system row
        self.ingest_obj = ingest_obj         # ingestion_config row
        self.secrets = secrets               # SecretResolver instance

    @abstractmethod
    def extract(self, watermark_start: Optional[str]) -> Tuple[DataFrame, Optional[str]]:
        """
        Extract data from the source and return (DataFrame, max_watermark_or_None).

        Parameters
        ----------
        watermark_start : the last successfully processed watermark value,
                          or None for first run / FULL loads.

        Returns
        -------
        df              : extracted Spark DataFrame
        watermark_end   : max value of incremental_column seen in this batch
                          (None for FULL loads or when no watermark column is set).
        """
        raise NotImplementedError
