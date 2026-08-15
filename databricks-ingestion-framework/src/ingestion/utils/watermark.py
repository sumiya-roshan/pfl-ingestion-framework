from typing import Optional
import logging
from pyspark.sql import SparkSession
from .config_manager import IngestionTaskConfig

def resolve_watermark(
    spark: SparkSession,
    logger: logging.Logger,
    ingest_obj: IngestionTaskConfig
) -> Optional[str]:
    """
    Resolves the high-water mark for incremental loads by querying the target table
    for the maximum value of the incremental column.
    
    Returns None if:
      - Load type is not INCREMENTAL
      - Incremental column is not configured
      - Target table does not exist yet (first run)
      - Error occurs reading the max value
    """
    if ingest_obj.load_type != "INCREMENTAL" or not ingest_obj.incremental_column:
        return None

    full_table = ingest_obj.full_target_table
    if not spark.catalog.tableExists(full_table):
        logger.info(
            f"Watermark: {full_table} does not exist yet — starting full load."
        )
        return None

    col = ingest_obj.incremental_column
    try:
        row = (
            spark.table(full_table)
            .selectExpr(f"MAX(`{col}`) AS max_val")
            .collect()
        )
        if row and row[0]["max_val"] is not None:
            wm = str(row[0]["max_val"])
            logger.info(f"Watermark: max({col})='{wm}' from {full_table}")
            return wm
    except Exception as exc:
        logger.warning(
            f"Watermark: could not read max({col}) from {full_table}: {exc}. "
            f"Falling back to None (full load)."
        )
    return None
