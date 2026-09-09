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

from __future__ import annotations

import decimal
import json
from dataclasses import dataclass
from datetime import date, datetime

# ── Fully-qualified table name defaults ───────────────────────────────────────
SOURCE_SYSTEM_TABLE = "migration_x_catalog.pfl_x_schema.config_source_system"
CONFIG_MASTER_TABLE = "migration_x_catalog.pfl_x_schema.config_master"
AUDIT_TABLE = "migration_x_catalog.pfl_x_schema.tb_audit_log"
DEPENDENCY_TABLE = "migration_x_catalog.pfl_x_schema.dependency_master_config"

# Audit lifecycle values shared by the entry point, orchestrator, and logger.
AUDIT_STATUS_INPROGRESS = "INPROGRESS"
AUDIT_STATUS_SUCCESS = "SUCCESS"
AUDIT_STATUS_FAILED = "FAILED"
AUDIT_STATUS_SKIPPED = "SKIPPED"  # Used by source_lookup when a table has 0 rows

# ─────────────────────────────────────────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────────────────────────────────────────


class _DictSerializable:
    """Shared dict (de)serialisation for the config dataclasses."""

    def to_dict(self) -> dict:
        res = {}
        for k, v in self.__dict__.items():
            if k.startswith("_"):
                continue
            if isinstance(v, decimal.Decimal):
                res[k] = int(v) if v % 1 == 0 else float(v)
            else:
                res[k] = v
        return res

    @classmethod
    def from_dict(cls, d: dict):
        return cls(**d)


@dataclass
class SourceSystemConfig(_DictSerializable):
    """One row per physical source system → config_source_system."""

    source_id: int
    source_name: str
    source_type: str  # POSTGRES | MYSQL | ORACLE | SFTP | MONGODB …
    ingest_method: str  # JDBC | SFTP | MONGODB …

    host: str | None
    port: int | None
    database_name: str | None

    driver_class: str | None
    connection_uri: str | None

    nosql_replica_set: str | None
    nosql_collection_name: str | None

    sftp_root_path: str | None
    sftp_file_pattern: str | None
    sftp_host_key_fingerprint: str | None

    extra_params: str | None

    secret_scope: str
    secret_key_credentials: str | None

    is_active: int

    landing_volume_path: str | None

    retry_count: int | None
    retry_interval: int | None
    query_timeout: str | None = None
    uc_connection_name: str | None = None


@dataclass
class IngestionTaskConfig(_DictSerializable):
    """Unified configuration for a single ingestion task, reading from flattened child config tables."""

    config_id: int
    source_schema: str | None
    source_object_name: str
    custom_query: str | None
    load_type: str
    incremental_column: str | None
    primary_key_cols: str | None
    target_catalog: str
    target_schema: str
    target_table: str
    pipeline_name: str
    delta_layer: str | None
    data_read_size: int | None
    file_format: str | None
    write_mode: str
    priority: int
    batch_id: int
    s3_source_bucket_name: str | None
    s3_external_path: str | None
    s3_column_delimiter: str | None
    s3_first_row_header: bool | None
    s3_raw_sink_bucket_name: str | None
    s3_raw_sink_file_path: str | None

    schema_evolution_mode: str | None
    partition_column: str | None
    source_filter: str | None

    staging_flag: int | None = None

    config_master_id: int | None = None

    silver_last_sink_date: str | None = None
    delta_column_2: str | None = None

    lookback_hours: int | None = None

    child_table_fqn: str | None = (
        None  # the config_master-resolved child table this task came from
    )

    # JSON array string, e.g. ["a@x.com","b@x.com"] 
    recipients: str | None = None

    @property
    def primary_key_list(self) -> list[str] | None:
        if self.primary_key_cols:
            return [k.strip() for k in self.primary_key_cols.split(",") if k.strip()]
        return None

    @property
    def recipient_list(self) -> list[str] | None:
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
    def full_target_table(self) -> str:
        return f"{self.target_catalog}.{self.target_schema}.{self.target_table}"

    @property
    def effective_delta_layer(self) -> str:
        return (self.delta_layer or "BRONZE").upper()


@dataclass
class LentraIngestionTaskConfig(_DictSerializable):
    """
    One row of the Lentra child config table (the S3-ingestion config table
    tracking Lentra report exports) resolved to typed fields. Built by
    ``ConfigManager._build_lentra_task`` inside the normal ``get_active_tasks``
    flow — same routing as every other source, just a different row→object
    step, branched on the child table's own shape (see
    ``ConfigManager.is_lentra_shaped``).

    Unlike LSQ Mavis (one fixed source_name, routed via a dedicated
    config_master_id), Lentra has many distinct Source_Name values that all
    share this same table shape — one active row per source. So detection
    here is by column shape (Report_Name + Silver_Sink_Schema_Name present),
    not a hardcoded id/name, and works regardless of which config_master_id
    ends up routing to this table.

    ``worker_no`` is computed in Python here (mirrors the ADF/SQL CASE
    expression the original ADF lookup used) rather than via a SQL CASE
    clause in ``get_active_tasks``: multi-node compute policies get "1:N"
    (autoscaling) or "N" (fixed); everything else gets "0".

    ``source_config_master_id`` is this ROW's own Config_Master_ID column — a
    per-row business field, unrelated to (and easily confused with) the
    config_master ROUTING id passed into ``get_active_tasks``. Don't conflate
    the two.

    ``access_key_id`` / ``secret_access_key`` are AWS Secrets Manager secret
    *names*, not raw credential values — safe to carry on this object and
    serialize via taskValues.
    """

    config_id: int
    source_config_master_id: int | None
    report_name: str | None
    key_column: str | None
    source_name: str | None
    source_bucket_name: str | None
    external_path: str | None
    frequency: str | None
    load_type: str | None
    raw_sink_container_name: str | None
    raw_sink_file_path: str | None
    column_delimiter: str | None
    first_row_header: bool | None
    silver_sink_schema_name: str | None
    silver_sink_table_name: str | None
    business_date: str | None
    status: str | None
    is_active: int | None
    sink_batch_started_date: str | None
    recipients: str | None
    pipeline_name: str | None
    access_key_id: str | None
    secret_access_key: str | None
    day_execution_count: int | None
    report_execution_day: str | None
    compute_policy_name: str | None
    compute_policy_id: str | None
    cluster_option: str | None
    worker_number: int | None
    worker_no: str | None = None

    child_table_fqn: str | None = None

    @property
    def recipient_list(self) -> list[str] | None:
        if not self.recipients:
            return None
        try:
            parsed = json.loads(self.recipients)
        except (json.JSONDecodeError, TypeError):
            return None
        if not isinstance(parsed, list):
            return None
        return [str(r).strip() for r in parsed if str(r).strip()]


# ─────────────────────────────────────────────────────────────────────────────
# Shared config_master routing helper
# ─────────────────────────────────────────────────────────────────────────────


def resolve_child_table_fqn(spark, config_master_table: str, config_master_id: int) -> str:
    """
    Resolve the child config table FQN routed to by a config_master id — the
    same routing every config-driven source (RDBMS, NoSQL, S3, Lentra, ...)
    uses. Shared so callers outside ConfigManager (e.g. the Lentra notebooks,
    which don't build IngestionTaskConfig/SourceSystemConfig objects and so
    have no reason to instantiate ConfigManager) don't duplicate this lookup.
    """
    rows = (
        spark.table(config_master_table)
        .filter(f"config_id = {int(config_master_id)}")
        .collect()
    )
    if not rows:
        raise ValueError(
            f"No entry in {config_master_table} for config_id={config_master_id}"
        )
    m = rows[0].asDict()
    return (
        f"{m.get('config_catalog_name')}."
        f"{m.get('config_schema_name')}."
        f"{m.get('config_table_name')}"
    )


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
    ):
        self.spark = spark
        self.source_system_table = source_system_table
        self.config_master_table = config_master_table

    # ─────────────────────────────────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────────────────────────────────

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
        source_system_id: int | None = None,
        source_name: str | None = None,
        pipeline_name: str | None = None,
        batch_start_date: str | None = None,
    ) -> tuple[SourceSystemConfig, list[IngestionTaskConfig | LentraIngestionTaskConfig]]:
        """
        1. Resolve source_name — either via source_system_id (fetches
           credentials + source_name from config_source_system, required for
           RDBMS/NoSQL/S3) or source_name given directly (the Lentra path —
           no config_source_system row needed, since Lentra's credentials are
           AWS Secrets Manager secret names carried on the config table
           itself, not config_source_system.secret_scope). Exactly one of
           source_system_id / source_name must be given.
        2. Fetch the specific child config table location from config_master.
        3. Query the child config table for active tasks for this source_name,
           filtering by pipeline_name if provided.
        4. If batch_start_date is provided and not '1', filter by sink_batch_started_date.

        For a Lentra-shaped child table (detected via ``is_lentra_shaped`` —
        see that method for why it's shape-based, not a hardcoded id/name),
        rows are built into LentraIngestionTaskConfig objects instead of
        IngestionTaskConfig — routing/filtering is otherwise identical, plus
        one extra filter (Report_Name like 'lentra%hdr') and a different order
        column, since Lentra's table has no Priority column.
        """

        # 1. Resolve source system / source_name
        if source_system_id is not None:
            source_sys = self.get_source_system(source_system_id)
            resolved_source_name = source_sys.source_name
        elif source_name:
            source_sys = self._placeholder_source_system(source_name)
            resolved_source_name = source_name
        else:
            raise ValueError(
                "get_active_tasks requires either source_system_id or source_name."
            )

        # 2. Find child config table location from master
        child_table_fqn = self._child_table_fqn(config_master_id)

        # 3. Query the child config table
        child_df = self.spark.table(child_table_fqn)
        is_lentra = self.is_lentra_shaped(child_df.columns)

        # Case-insensitive column resolution
        src_col = self.resolve_col(child_df.columns, "source_name", "Source_Name")
        active_col = self.resolve_col(child_df.columns, "is_active", "Is_Active")

        # Basic filtering by source system and active status
        filtered_df = child_df.filter(
            f"{src_col} = '{resolved_source_name}' AND {active_col} = 1"
        )

        if is_lentra:
            report_col = self.resolve_col(child_df.columns, "Report_Name")
            filtered_df = filtered_df.filter(f"{report_col} like 'lentra%hdr'")

        # Apply multi-refresh batch start date filtering if triggered by orchestrator
        if batch_start_date and str(batch_start_date).strip() != "1":
            date_col = self.resolve_col(child_df.columns, "sink_batch_started_date")
            if date_col:
                clean_date = str(batch_start_date).replace("T", " ").split(".")[0]
                print(
                    f"[ConfigManager] Filtering active tasks by {date_col} = '{clean_date}'"
                )
                filtered_df = filtered_df.filter(
                    f"date_format(from_utc_timestamp({date_col}, 'UTC'), 'yyyy-MM-dd HH:mm:ss') = '{clean_date}'"
                )
            else:
                print(
                    f"[ConfigManager] Warning: {child_table_fqn} has no sink_batch_started_date column. Skipping filter."
                )

        # Lentra's table has no Priority column — order by Config_ID instead.
        order_col = "Config_ID" if is_lentra else "priority"
        child_rows = filtered_df.orderBy(order_col).collect()

        tasks = []
        for r in child_rows:
            if is_lentra:
                lentra_task = self._build_lentra_task(r.asDict())
                lentra_task.child_table_fqn = child_table_fqn
                tasks.append(lentra_task)
                continue

            task = self._build_ingestion_task(r.asDict())
            task.config_master_id = config_master_id
            task.child_table_fqn = child_table_fqn
            # If pipeline_name is specified, only include tasks that match it
            if pipeline_name and task.pipeline_name != pipeline_name:
                continue
            tasks.append(task)

        return source_sys, tasks

    def update_sink_metadata(
        self,
        config_master_id: int,
        ingest_obj: IngestionTaskConfig,
        sink_batch_started_date,
        rownum: int,
        data_size: int,
    ) -> None:
        """
        Updates status, business_date, raw_last_sink_date, rownum, and data_size
        on the child config table row for this task.

        sink_batch_started_date is NOT written here — it is stamped once at batch
        start (see get_tasks.py) and must stay constant for the whole run. The
        param is still used to derive business_date.

        Only called from the SUCCESS path in IngestionOrchestrator.run(), so
        status is written as AUDIT_STATUS_SUCCESS unconditionally.

        silver_last_sink_date is intentionally left untouched here — it belongs
        to the (separately coupled) Silver pipeline; see the
        `# trigger_silver [TO DO]` note in orchestrator.py.
        """
        child_table_fqn = self._child_table_fqn(config_master_id)

        business_date = sink_batch_started_date.date()

        # deltacolumn_1 = the source's incremental/watermark column, read from
        # the bronze table just written (not the config table itself). Best-effort:
        # a missing bronze table or a misconfigured Delta_Column_1 (uuid/text id)
        # is skipped so it can't fail the whole UPDATE and leave the row stuck at
        # 'In Progress'.
        raw_last_sink_date = None
        if ingest_obj.incremental_column:
            try:
                max_val = (
                    self.spark.table(ingest_obj.full_target_table)
                    .agg({ingest_obj.incremental_column: "max"})
                    .collect()[0][0]
                )
            except Exception as exc:
                max_val = None
                print(
                    f"[ConfigManager] config_id={ingest_obj.config_id}: could not read "
                    f"MAX({ingest_obj.incremental_column}) from {ingest_obj.full_target_table}: {exc}"
                )
            if isinstance(max_val, (date, datetime)):
                raw_last_sink_date = max_val
            elif max_val is not None:
                print(
                    f"[ConfigManager] config_id={ingest_obj.config_id}: "
                    f"Delta_Column_1 '{ingest_obj.incremental_column}' MAX() is "
                    f"{max_val!r} (not a date/timestamp) — leaving raw_last_sink_date unchanged."
                )

        set_clauses = [
            f"status        = {self._sql_literal(AUDIT_STATUS_SUCCESS)}",
            f"business_date  = {self._sql_literal(business_date)}",
            f"rownum         = {int(rownum )}",
            f"data_size      = {int(data_size)}",
        ]
        if raw_last_sink_date is not None:
            set_clauses.append(
                f"raw_last_sink_date = {self._sql_literal(raw_last_sink_date)}"
            )

        self.spark.sql(f"""
            UPDATE {child_table_fqn}
            SET {", ".join(set_clauses)}
            WHERE config_id = {int(ingest_obj.config_id)}
        """)

    def update_silver_last_sink_date(
        self, child_table_fqn: str, config_id: int
    ) -> None:
        """
        Stamps Silver_Last_Sink_Date = current_timestamp() on this table's row
        in its child config table — called right after Silver completes for
        that table (see IngestionOrchestrator.run()). Column/PK names are
        resolved case-insensitively since child config tables vary
        (Config_ID vs config_id, Silver_Last_Sink_Date vs silver_last_sink_date).
        """
        columns = self.spark.table(child_table_fqn).columns
        config_id_col = self.resolve_col(columns, "config_id", "Config_ID")
        sink_date_col = self.resolve_col(
            columns, "silver_last_sink_date", "Silver_Last_Sink_Date"
        )
        self.spark.sql(f"""
            UPDATE {child_table_fqn}
            SET {sink_date_col} = current_timestamp()
            WHERE {config_id_col} = {int(config_id)}
        """)

    def update_raw_last_sink_time(
        self, child_table_fqn: str, config_id: int
    ) -> None:
        """
        Stamps Raw_Last_Sink_Time = current_timestamp() on this table's row in
        its child config table — called right after the Source→Raw stage
        finishes for that table (see IngestionOrchestrator.run()), the same
        moment the dependency table's source_to_raw_end_time is set. Column/PK
        names are resolved case-insensitively since child config tables vary
        (Config_ID vs config_id, Raw_Last_Sink_Time vs raw_last_sink_time).
        """
        columns = self.spark.table(child_table_fqn).columns
        config_id_col = self.resolve_col(columns, "config_id", "Config_ID")
        sink_time_col = self.resolve_col(
            columns, "raw_last_sink_time", "Raw_Last_Sink_Time"
        )
        self.spark.sql(f"""
            UPDATE {child_table_fqn}
            SET {sink_time_col} = current_timestamp()
            WHERE {config_id_col} = {int(config_id)}
        """)

    def update_status(self, child_table_fqn: str, config_id: int, status: str) -> None:
        """
        Sets Status on this table's row in its child config table — called by
        IngestionOrchestrator.run() to flag 'Failed' when the raw or silver
        layer fails. Column/PK names are resolved case-insensitively.
        """
        columns = self.spark.table(child_table_fqn).columns
        config_id_col = self.resolve_col(columns, "config_id", "Config_ID")
        status_col = self.resolve_col(columns, "status", "Status")
        self.spark.sql(f"""
            UPDATE {child_table_fqn}
            SET {status_col} = '{status}'
            WHERE {config_id_col} = {int(config_id)}
        """)

    # ─────────────────────────────────────────────────────────────────────────
    # Internal helpers — value coercion / name resolution
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _sql_literal(value) -> str:
        return "NULL" if value is None else "'" + str(value).replace("'", "''") + "'"

    @staticmethod
    def _to_int(value) -> int | None:
        """
        Coerces a config-table numeric value to int. Decimal-typed columns
        (e.g. data_size DECIMAL(10,2)) come back from Spark as Python Decimal
        ('3866.00'), which str()'s to a non-integer literal and breaks JDBC
        options like fetchsize that require a plain integer string.
        """
        if value is None:
            return None
        return int(float(value))

    @staticmethod
    def resolve_col(columns, name: str, default: str | None = None) -> str | None:
        """Case-insensitive lookup of `name` among `columns`, else `default`."""
        return next((c for c in columns if c.lower() == name.lower()), default)

    def _child_table_fqn(self, config_master_id: int) -> str:
        """Resolve the child config table FQN routed to by a config_master id."""
        return resolve_child_table_fqn(self.spark, self.config_master_table, config_master_id)

    def is_lentra_shaped(self, columns: list[str]) -> bool:
        """
        Detects the Lentra child table shape by its own columns, rather than a
        hardcoded config_master_id or source_name. Lentra has many distinct
        Source_Name values sharing this one table shape (unlike e.g. LSQ
        Mavis, which is one fixed source_name), so column-shape detection is
        what actually generalizes across all of them — and it works no matter
        which config_master_id ends up routing to this table.
        """
        return bool(
            self.resolve_col(columns, "Report_Name")
            and self.resolve_col(columns, "Silver_Sink_Schema_Name")
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Internal helpers — row → dataclass builders
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _placeholder_source_system(source_name: str) -> SourceSystemConfig:
        """
        A minimal SourceSystemConfig for the Lentra source_name path, which
        has no config_source_system row — Lentra needs no shared connector
        credentials or landing_volume_path there; its own AWS Secrets Manager
        secret names live directly on the config table row. This keeps every
        caller's source_sys.* access (source_name, source_type,
        landing_volume_path, ...) working without a None check at every call
        site — landing_volume_path is None here, which callers already treat
        as "not configured, skip" (e.g. main.py's S3 log path setup).
        """
        return SourceSystemConfig(
            source_id=0,
            source_name=source_name,
            source_type="LENTRA",
            ingest_method="LENTRA",
            host=None,
            port=None,
            database_name=None,
            driver_class=None,
            connection_uri=None,
            nosql_replica_set=None,
            nosql_collection_name=None,
            sftp_root_path=None,
            sftp_file_pattern=None,
            sftp_host_key_fingerprint=None,
            extra_params=None,
            secret_scope="",
            secret_key_credentials=None,
            is_active=1,
            landing_volume_path=None,
            retry_count=None,
            retry_interval=None,
        )

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
            secret_scope=r["secret_scope"],
            secret_key_credentials=r.get("secret_key_credentials"),
            is_active=r.get("is_active", 1),
            extra_params=r.get("extra_params"),
            landing_volume_path=r.get("landing_volume_path"),
            retry_count=r.get("retry_count"),
            retry_interval=r.get("retry_interval"),
            query_timeout=r.get("query_timeout"),
            uc_connection_name=r.get("uc_connection_name"),
        )

    def _build_ingestion_task(
        self, r: dict, child_table_fqn: str | None = None
    ) -> IngestionTaskConfig:
        """
        Dynamically falls back across different column aliases to support
        both RDBMS and NoSQL schema structures without hardcoding.
        """
        source_object = (
            r.get("Source_Table_Name")
            or r.get("Source_Collection_Name")
            or r.get("Source_Object_Name")
            or r.get("report_name")  # S3: s3_config_master.report_name
            or ""
        )

        # incremental_column = Delta_Column_1 in rdbms_ingestion_config.
        # Key_Column is the PRIMARY KEY, NOT the watermark/delta column.
        inc_col = (
            r.get("Delta_Column_1")  # rdbms_ingestion_config
            or r.get("Incremental_Column")  # NoSQL / generic alias
            or r.get("Key_Column")  # legacy fallback only
        )

        pk_cols = (
            r.get("Key_Column")
            or r.get("Primary_Key_Cols")
            or r.get("key_column")  # S3: s3_config_master.key_column (lowercase)
        )

        load_type = str(
            r.get("Load_type") or r.get("Load_Type") or r.get("load_type") or "FULL"
        ).upper()
        default_write_mode = "overwrite" if load_type == "FULL" else "append"

        # S3: target schema/table use different column names in s3_config_master
        target_schema = (
            r.get("Sink_Schema_Name") or r.get("bronze_sink_schema_name")  # S3
        )
        target_table = (
            r.get("Sink_Table_Name") or r.get("bronze_sink_table_name")  # S3
        )

        return IngestionTaskConfig(
            config_id=int(
                r.get("Config_ID")
                or r.get("config_id")
                or r.get("config_master_id")
                or 0
            ),
            source_schema=r.get("Source_Schema_Name"),
            source_object_name=source_object,
            custom_query=r.get("Source_Query"),
            load_type=load_type,
            incremental_column=inc_col,
            primary_key_cols=pk_cols,
            target_catalog=(
                r.get("target_catalog")
                or r.get("Target_Catalog")
                or "hive_metastore"
            ),
            target_schema=target_schema,
            target_table=target_table,
            pipeline_name=r.get("Pipeline_Name") or r.get("pipeline_name"),
            delta_layer=r.get("Delta_Layer") or r.get("delta_layer"),
            data_read_size=self._to_int(r.get("data_size") or r.get("data_read_size")),
            file_format=r.get("file_format"),
            write_mode=r.get("write_mode")
            or ("merge" if pk_cols else default_write_mode),
            priority=self._to_int(r.get("Priority") or r.get("priority"))
            or 0,  # capital P in rdbms config
            batch_id=self._to_int(
                r.get("Batch_ID") or r.get("batch_id")
            ),  # capital B+ID in rdbms config
            schema_evolution_mode=r.get("schema_evolution_mode"),
            partition_column=r.get("partition_column"),
            source_filter=r.get("source_filter"),
            staging_flag=self._to_int(r.get("Staging_Flag") or r.get("staging_flag"))
            or 0,
            # Watermark date for incremental lookup query generation
            silver_last_sink_date=(
                str(
                    r.get("Silver_Last_Sink_Date")
                    or r.get("silver_last_sink_date")
                    or ""
                )
                or None
            ),
            # Secondary delta column (OR condition in lookup WHERE clause)
            delta_column_2=r.get("Delta_Column_2") or r.get("delta_column_2"),
            # Lookback window (hours) for the dynamic lookup/key-extraction predicate
            lookback_hours=self._to_int(
                r.get("Lookback_Hours") or r.get("lookback_hours")
            ),
            # S3-specific fields — present only in s3_config_master rows
            s3_source_bucket_name=r.get("s3_source_bucket_name")
            or r.get("source_bucket_name"),
            s3_external_path=r.get("s3_external_path") or r.get("external_path"),
            s3_column_delimiter=r.get("s3_column_delimiter")
            or r.get("column_delimiter"),
            s3_first_row_header=r.get("s3_first_row_header")
            or r.get("first_row_header"),
            s3_raw_sink_bucket_name=r.get("s3_raw_sink_bucket_name")
            or r.get("raw_sink_bucket_name"),
            s3_raw_sink_file_path=r.get("s3_raw_sink_file_path")
            or r.get("raw_sink_file_path"),
            child_table_fqn=child_table_fqn,
            recipients=r.get("recipients") or r.get("Recipients"),
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Lentra — row builder (same routing as every source; only the row→object
    # step differs, branched inside get_active_tasks via is_lentra_shaped)
    # ─────────────────────────────────────────────────────────────────────────

    def _build_lentra_task(self, r: dict) -> LentraIngestionTaskConfig:
        """
        Row → LentraIngestionTaskConfig. Same style as _build_ingestion_task:
        canonical column name first, lowercase alias as fallback.

        worker_no mirrors the ADF/SQL CASE expression: multi-node compute
        policies get "1:N" (autoscaling) or "N" (fixed) worker specs;
        everything else gets "0".
        """
        compute_policy_name = (
            r.get("Compute_Policy_Name") or r.get("compute_policy_name") or ""
        )
        cluster_option = r.get("Cluster_Option") or r.get("cluster_option") or ""
        worker_number = self._to_int(r.get("Worker_Number") or r.get("worker_number"))

        is_multi = "multi" in compute_policy_name.lower()
        if is_multi and cluster_option == "Autoscaling":
            worker_no = f"1:{worker_number}"
        elif is_multi and cluster_option == "Fixed":
            worker_no = str(worker_number) if worker_number is not None else "0"
        else:
            worker_no = "0"

        return LentraIngestionTaskConfig(
            config_id=self._to_int(r.get("Config_ID") or r.get("config_id")) or 0,
            source_config_master_id=self._to_int(
                r.get("Config_Master_ID") or r.get("config_master_id")
            ),
            report_name=r.get("Report_Name") or r.get("report_name"),
            key_column=r.get("Key_Column") or r.get("key_column"),
            source_name=r.get("Source_Name") or r.get("source_name"),
            source_bucket_name=r.get("Source_Bucket_Name") or r.get("source_bucket_name"),
            external_path=r.get("External_Path") or r.get("external_path"),
            frequency=r.get("Frequency") or r.get("frequency"),
            load_type=str(r.get("Load_Type") or r.get("load_type") or "FULL").upper(),
            raw_sink_container_name=r.get("Raw_Sink_Container_Name")
            or r.get("raw_sink_container_name"),
            raw_sink_file_path=r.get("Raw_Sink_File_Path") or r.get("raw_sink_file_path"),
            column_delimiter=r.get("Column_Delimiter") or r.get("column_delimiter"),
            first_row_header=(
                r.get("First_Row_Header")
                if r.get("First_Row_Header") is not None
                else r.get("first_row_header")
            ),
            silver_sink_schema_name=r.get("Silver_Sink_Schema_Name")
            or r.get("silver_sink_schema_name"),
            silver_sink_table_name=r.get("Silver_Sink_table_Name")
            or r.get("silver_sink_table_name"),
            # Business_Date/Sink_Batch_Start_Date come back from Spark as
            # datetime.date/Timestamp objects (not strings) when the column
            # is actually date/timestamp-typed — json.dumps() (used when
            # publishing tasks via taskValues) can't serialize those, so
            # stringify here, same pattern _build_ingestion_task() uses for
            # Silver_Last_Sink_Date.
            business_date=(
                str(r.get("Business_Date") or r.get("business_date") or "") or None
            ),
            status=r.get("Status") or r.get("status"),
            is_active=self._to_int(r.get("Is_Active") or r.get("is_active")),
            sink_batch_started_date=(
                str(
                    r.get("Sink_Batch_Start_Date")
                    or r.get("sink_batch_started_date")
                    or ""
                )
                or None
            ),
            recipients=r.get("Recipients") or r.get("recipients"),
            pipeline_name=r.get("Pipeline_Name") or r.get("pipeline_name"),
            access_key_id=r.get("Access_Key_ID") or r.get("access_key_id"),
            secret_access_key=r.get("Secret_Access_Key") or r.get("secret_access_key"),
            day_execution_count=self._to_int(
                r.get("Day_Execution_Count") or r.get("day_execution_count")
            ),
            report_execution_day=r.get("Report_Execution_Day")
            or r.get("report_execution_day"),
            compute_policy_name=compute_policy_name or None,
            compute_policy_id=r.get("Compute_Policy_ID") or r.get("compute_policy_id"),
            cluster_option=cluster_option or None,
            worker_number=worker_number,
            worker_no=worker_no,
        )
