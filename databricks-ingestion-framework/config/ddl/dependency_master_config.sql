-- One row per table per pipeline run, tracking stage-level and pipeline-level
-- timing plus dependency_resolve_time, the signal downstream teams poll.
-- Row is inserted at pipeline start and updated as each stage (Source→Raw,
-- Raw→Silver, whole pipeline) completes.
--
-- dependency_resolve_time: NULL while this table's Silver run hasn't
-- finished yet (or hasn't started); set to the SAME timestamp as
-- raw_to_silver_end_time the moment that table's Silver run completes.
-- Downstream teams query today's business_date row for this table — a
-- non-NULL dependency_resolve_time means it's safe to consume, NULL means
-- not ready yet. Per-table, not a job-level or capped value.
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
