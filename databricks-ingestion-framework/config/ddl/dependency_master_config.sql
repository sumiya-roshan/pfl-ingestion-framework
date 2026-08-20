-- One row per table per pipeline run, tracking stage-level and pipeline-level
-- timing plus the SLA-driven dependency_resolve_time used by downstream
-- dependents. Row is inserted at pipeline start and updated as each stage
-- (Source→Raw, Raw→Silver, whole pipeline) completes.
--
-- dependency_resolve_time rule:
--   next_trigger_time = pipeline_start_time + INTERVAL 24 HOURS
--   dependency_resolve_time = LEAST(pipeline_end_time, next_trigger_time)
-- i.e. if the pipeline is still running 24h after it started, the dependency
-- is considered resolved at that boundary rather than waiting for the
-- actual (later) completion time. pipeline_start_time doubles as the SLA
-- anchor — there is no separate job-level trigger_time column.
CREATE TABLE IF NOT EXISTS migration_x_catalog.pfl_x_schema.dependency_master_config (
    config_master_id           INT             NOT NULL,
    source_system_id           INT             NOT NULL,
    config_id                  INT             NOT NULL,
    table_name                 STRING          NOT NULL,
    pipeline_name               STRING          NOT NULL,
    is_active                   BOOLEAN         NOT NULL,
    job_run_id                  STRING          NOT NULL,
    business_date                DATE            NOT NULL,
    pipeline_start_time          TIMESTAMP       NOT NULL,
    pipeline_end_time            TIMESTAMP,
    source_to_raw_start_time     TIMESTAMP,
    source_to_raw_end_time       TIMESTAMP,
    raw_to_silver_start_time     TIMESTAMP,
    raw_to_silver_end_time       TIMESTAMP,
    dependency_resolve_time      TIMESTAMP,
    created_at                   TIMESTAMP       DEFAULT current_timestamp()
)
USING DELTA
CLUSTER BY (business_date, table_name)
COMMENT 'Per-table, per-run stage timing + SLA-driven dependency_resolve_time for downstream dependents';
