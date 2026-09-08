"""
MavisTableConfig — typed configuration for a single LSQ Mavis ingestion table.

Lives in ingestion/utils/ so it is co-located with the other shared utils
(AuditLogger, DependencyLogger, etc.) and can be imported by both
mavis_orchestrator.py and the entry-point notebooks (lsq_mavis_get_tasks.py /
lsq_mavis_main.py).

All fields are read directly from tb_mavis_db_ingestion_config per-row,
exactly as the ADF pipeline consumes them. No config_source_system row
is required for LSQ Mavis.

Column name resolution is case-insensitive (tries both PascalCase and snake_case)
to tolerate any casing differences in the Spark row dict.
"""

from __future__ import annotations

import json
from dataclasses import dataclass


@dataclass
class MavisTableConfig:
    """One row of tb_mavis_db_ingestion_config, typed for safe access."""

    # ── Identity ──────────────────────────────────────────────────────────────
    config_id: int
    config_master_id: int
    pipeline_name: str | None
    source_name: str

    # ── API connection (all per-row from config table) ────────────────────────
    prod_api: str           # Prod_API       — base URL of the Mavis REST API
    database_id: str        # Database_ID    — Mavis database identifier
    table_id: str           # Table_ID       — Mavis table identifier
    org_code: str           # Org_Code       — organisation code query param
    x_api_key: str          # Prod_API_Key   — x-api-key header value

    # ── Load behaviour ────────────────────────────────────────────────────────
    load_type: str          # Load_Type      — 'Full' | 'Incremental'
    source_filter: str | None   # Source_Filter  — POST body template (JSON)
    to_date: str | None     # To_Date        — watermark for incremental from_date

    # ── Raw landing paths (ADLS Gen2) ─────────────────────────────────────────
    raw_container_name: str     # Raw_Container_Name  (e.g. 'mavis')
    raw_folder_path: str        # Raw_Folder_Path
    raw_file_name: str          # Raw_File_Name

    # ── Silver / sink ─────────────────────────────────────────────────────────
    sink_table_name: str        # Sink_Table_Name
    target_schema: str          # Sink_Schema_Name (e.g. 'lsq_mavis')
    target_catalog: str         # Target_Catalog

    # ── Delta / merge keys ────────────────────────────────────────────────────
    delta_column: str | None    # Delta_Column
    key_column: str | None      # Key_Column

    # ── Notifications ─────────────────────────────────────────────────────────
    table_description: str | None   # Table_Description
    recipients: str | None          # Recipients — JSON array string

    # ── Scheduling / parallelism ──────────────────────────────────────────────
    batch_id: int
    priority: int

    # ── Watermark bookkeeping ─────────────────────────────────────────────────
    silver_last_sink_date: str | None = None

    # ── Convenience properties ────────────────────────────────────────────────

    @property
    def full_target_table(self) -> str:
        return f"{self.target_catalog}.{self.target_schema}.{self.sink_table_name}"

    @property
    def recipient_list(self) -> list[str] | None:
        """Parse Recipients JSON array → list of email strings, or None."""
        if not self.recipients:
            return None
        try:
            parsed = json.loads(self.recipients)
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(parsed, list):
            return None
        return [str(r).strip() for r in parsed if str(r).strip()]

    @property
    def primary_key_list(self) -> list[str] | None:
        """Split Key_Column (comma-separated) into a list."""
        if not self.key_column:
            return None
        return [k.strip() for k in self.key_column.split(",") if k.strip()]

    # ── De/serialisation ──────────────────────────────────────────────────────

    @classmethod
    def from_row(cls, row: dict) -> "MavisTableConfig":
        """
        Build a MavisTableConfig from a Spark row dict (asDict()).
        Resolves column names case-insensitively (PascalCase and snake_case).
        """

        def _get(*keys, default=None):
            for k in keys:
                v = row.get(k)
                if v is not None and str(v).strip() != "":
                    return v
            return default

        def _int(*keys, default=0):
            v = _get(*keys, default=default)
            try:
                return int(float(v)) if v is not None else default
            except (ValueError, TypeError):
                return default

        load_type = str(_get("Load_Type", "load_type", default="Full"))

        return cls(
            config_id=_int("Config_ID", "config_id"),
            config_master_id=_int("Config_Master_ID", "config_master_id"),
            pipeline_name=_get("Pipeline_Name", "pipeline_name"),
            source_name=str(_get("Source_Name", "source_name", default="")),
            # API fields — all from config table rows (Prod_API_Key, Prod_API, etc.)
            prod_api=str(_get("Prod_API", "prod_api", default="")),
            database_id=str(_get("Database_ID", "database_id", default="")),
            table_id=str(_get("Table_ID", "table_id", default="")),
            org_code=str(_get("Org_Code", "org_code", default="")),
            x_api_key=str(_get("Prod_API_Key", "prod_api_key", default="")),
            # Load behaviour
            load_type=load_type,
            source_filter=_get("Source_Filter", "source_filter"),
            to_date=str(_get("To_Date", "to_date", default="") or "") or None,
            # Raw ADLS paths
            raw_container_name=str(_get("Raw_Container_Name", "raw_container_name", default="mavis")),
            raw_folder_path=str(_get("Raw_Folder_Path", "raw_folder_path", default="")),
            raw_file_name=str(_get("Raw_File_Name", "raw_file_name", default="")),
            # Silver / sink
            sink_table_name=str(_get("Sink_Table_Name", "sink_table_name", default="")),
            target_schema=str(_get("Sink_Schema_Name", "sink_schema_name", default="lsq_mavis")),
            target_catalog=str(_get("Target_Catalog", "target_catalog", default="")),
            # Delta / merge keys
            delta_column=_get("Delta_Column", "delta_column"),
            key_column=_get("Key_Column", "key_column"),
            # Notifications
            table_description=_get("Table_Description", "table_description"),
            recipients=_get("Recipients", "recipients"),
            # Scheduling
            batch_id=_int("Batch_ID", "batch_id"),
            priority=_int("Priority", "priority"),
            # Watermark
            silver_last_sink_date=str(_get("Silver_Last_Sink_Date", "silver_last_sink_date", default="") or "") or None,
        )

    def to_dict(self) -> dict:
        """Serialise to a plain dict (for taskValues JSON transport)."""
        return {k: v for k, v in self.__dict__.items()}

    @classmethod
    def from_dict(cls, d: dict) -> "MavisTableConfig":
        """Deserialise from a plain dict (after taskValues JSON transport)."""
        return cls(**d)
