from __future__ import annotations

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


@dataclass
class SourceSystemConfig:
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

    # Source query timeout, format 'HH:mm:ss' (e.g. '12:00:00'). Applied to the
    # JDBC statement so the source DB cancels the query itself on expiry — see
    # JdbcConnector._read_options(). None → no timeout applied.
    query_timeout: str | None = None

    # Unity Catalog "Connection" object name
    # Used only by FederatedConnector
    uc_connection_name: str | None = None

    def to_dict(self) -> dict:
        import decimal

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
    def from_dict(cls, d: dict) -> SourceSystemConfig:
        return cls(**d)


@dataclass
class IngestionTaskConfig:
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

    # config_master routing ID this task was loaded under — set by
    # ConfigManager.get_active_tasks().
    config_master_id: int | None = None

    # Watermark date for incremental lookup query generation
    silver_last_sink_date: str | None = None
    # Secondary delta column (OR condition in lookup WHERE clause)
    delta_column_2: str | None = None
    # Hours to look back from silver_last_sink_date when building the
    # dynamic lookup/key-extraction watermark predicate (default 3 if unset).
    # See JdbcConnector._lookback_predicates().
    lookback_hours: int | None = None
    # Per-table lookup/presence-check query template, read from
    # rdbms_ingestion_config.Lookup_Query_Template. Supports {schema}/{table}/
    # {key_column} placeholders — see LookupExecutor._resolve_query_for_task.
    # Required for JDBC sources — LookupExecutor raises if unset.
    lookup_query: str | None = None

    # Thread-pool size for the whole pipeline run, read from
    # rdbms_ingestion_config.Max_Workers — same value repeated on every row for
    # a given pipeline. main.py reads it off the first loaded task.
    max_workers: int | None = None
    child_table_fqn: str | None = (
        None  # the config_master-resolved child table this task came from
    )

    # JSON array string, e.g. ["a@x.com","b@x.com"] — config table's own
    # failure-notification recipients for this table, overriding the
    # notifier's fixed default list when present. See notifier.py.
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

    def to_dict(self) -> dict:
        import decimal

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
    def from_dict(cls, d: dict) -> IngestionTaskConfig:
        return cls(**d)


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
        target_catalog: str = "hive_metastore",
    ):
        self.spark = spark
        self.source_system_table = source_system_table
        self.config_master_table = config_master_table
        self.target_catalog = target_catalog

    # ── Row builders ─────────────────────────────────────────────────────────

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
            target_catalog=self.target_catalog,
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
            # Per-table lookup/presence-check query template — rdbms_ingestion_config.Lookup_Query_Template
            lookup_query=r.get("Lookup_Query_Template")
            or r.get("lookup_query_template"),
            # Pipeline-wide thread pool size — rdbms_ingestion_config.Max_Workers
            max_workers=self._to_int(r.get("Max_Workers") or r.get("max_workers")),
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

    # ── Database Operations ───────────────────────────────────────────────────

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
        source_system_id: int,
        pipeline_name: str | None = None,
        batch_start_date: str | None = None,
    ) -> tuple[SourceSystemConfig, list[IngestionTaskConfig]]:
        """
        1. Fetch Source System by source_system_id to get credentials & source_name.
        2. Fetch the specific child config table location from config_master.
        3. Query the child config table for active tasks for this source_name,
           filtering by pipeline_name if provided.
        4. If batch_start_date is provided and not '1', filter by sink_batch_started_date.
        """

        # 1. Resolve source system
        source_sys = self.get_source_system(source_system_id)
        source_name = source_sys.source_name
        print(source_sys)
        print(source_name)

        # 2. Find child config table location from master
        master_rows = (
            self.spark.table(self.config_master_table)
            .filter(f"config_id = {config_master_id}")
            .collect()
        )
        print(master_rows)
        if not master_rows:
            raise ValueError(
                f"No entry in {self.config_master_table} for config_id={config_master_id}"
            )

        m_row = master_rows[0].asDict()
        catalog = m_row.get("config_catalog_name")
        schema = m_row.get("config_schema_name")
        table = m_row.get("config_table_name")

        child_table_fqn = f"{catalog}.{schema}.{table}"

        # 3. Query the child config table
        child_df = self.spark.table(child_table_fqn)

        # Case-insensitive column resolution
        src_col = next(
            (c for c in child_df.columns if c.lower() == "source_name"), "Source_Name"
        )
        active_col = next(
            (c for c in child_df.columns if c.lower() == "is_active"), "Is_Active"
        )

        # Basic filtering by source system and active status
        filtered_df = child_df.filter(
            f"{src_col} = '{source_name}' AND {active_col} = 1"
        )

        # Apply multi-refresh batch start date filtering if triggered by orchestrator
        if batch_start_date and str(batch_start_date).strip() != "1":
            date_col = next(
                (c for c in child_df.columns if c.lower() == "sink_batch_started_date"),
                None,
            )
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

        child_rows = filtered_df.orderBy("priority").collect()

        tasks = []
        for r in child_rows:
            task = self._build_ingestion_task(r.asDict())
            task.config_master_id = config_master_id
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
        master_rows = (
            self.spark.table(self.config_master_table)
            .filter(f"config_id = {config_master_id}")
            .collect()
        )
        if not master_rows:
            raise ValueError(
                f"No entry in {self.config_master_table} for config_id={config_master_id}"
            )
        m_row = master_rows[0].asDict()
        child_table_fqn = f"{m_row.get('config_catalog_name')}.{m_row.get('config_schema_name')}.{m_row.get('config_table_name')}"

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
            f"rownum         = {int(rownum or 0)}",
            f"data_size      = {int(data_size or 0)}",
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

    @staticmethod
    def _sql_literal(value) -> str:
        return "NULL" if value is None else "'" + str(value).replace("'", "''") + "'"

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
        config_id_col = next(
            (c for c in columns if c.lower() == "config_id"), "Config_ID"
        )
        sink_date_col = next(
            (c for c in columns if c.lower() == "silver_last_sink_date"),
            "Silver_Last_Sink_Date",
        )
        self.spark.sql(f"""
            UPDATE {child_table_fqn}
            SET {sink_date_col} = current_timestamp()
            WHERE {config_id_col} = {int(config_id)}
        """)

    def update_status(self, child_table_fqn: str, config_id: int, status: str) -> None:
        """
        Sets Status on this table's row in its child config table — called by
        IngestionOrchestrator.run() to flag 'Failed' when the raw or silver
        layer fails. Column/PK names are resolved case-insensitively.
        """
        columns = self.spark.table(child_table_fqn).columns
        config_id_col = next(
            (c for c in columns if c.lower() == "config_id"), "Config_ID"
        )
        status_col = next((c for c in columns if c.lower() == "status"), "Status")
        self.spark.sql(f"""
            UPDATE {child_table_fqn}
            SET {status_col} = '{status}'
            WHERE {config_id_col} = {int(config_id)}
        """)
