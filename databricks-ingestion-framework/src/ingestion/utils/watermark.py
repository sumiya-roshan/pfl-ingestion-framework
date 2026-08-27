from typing import Optional
from .config_manager import IngestionTaskConfig


def resolve_watermark(ingest_obj: IngestionTaskConfig) -> Optional[str]:
    """
    Resolves the incremental watermark for extraction: Silver_Last_Sink_Date -
    Lookback_Hours, formatted 'YYYY-MM-DD HH:MM:SS'. Also used as the cutoff
    basis for the lookup probe (see JdbcConnector._lookback_cutoff()), so both
    must derive it the same way.

    A pure MAX(incremental_column) off the target table would only catch new
    inserts and miss updates to already-loaded rows — the lookback window
    re-pulls a recent slice via Delta_Column_1/Delta_Column_2 to cover that.

    Returns None if:
      - Load type is not INCREMENTAL
      - Incremental column is not configured
      - No silver_last_sink_date yet (first run — extraction runs unfiltered)
    """
    if (
        ingest_obj.load_type != "INCREMENTAL"
        or not ingest_obj.incremental_column
        or not ingest_obj.silver_last_sink_date
    ):
        return None

    from datetime import datetime, timedelta

    last_sink_str = str(ingest_obj.silver_last_sink_date).replace("T", " ").split(".")[0]
    last_sink = datetime.strptime(last_sink_str, "%Y-%m-%d %H:%M:%S")
    cutoff = last_sink - timedelta(hours=int(ingest_obj.lookback_hours or 3))
    return cutoff.strftime("%Y-%m-%d %H:%M:%S")
