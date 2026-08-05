-- ============================================================================
-- INGESTION_AUDIT_LOG
-- One row per ingestion run per source. Used for monitoring, retries, and
-- resolving the next incremental watermark.
-- ============================================================================
CREATE TABLE IF NOT EXISTS main.monitoring.data_pipeline_execution_master (
    config_master_id       INT             NOT NULL,
    table_id               INT             NOT NULL,
    delta_layer            STRING          NOT NULL,
    source_name            STRING          NOT NULL,
    pipeline_name          STRING          NOT NULL,
    load_type              STRING          NOT NULL,
    frequency              STRING,
    business_date          DATE            NOT NULL,
    run_id                 STRING          NOT NULL,
    trigger_type           STRING,
    trigger_id             STRING,
    trigger_name           STRING,
    trigger_time           TIMESTAMP       NOT NULL,
    end_time               TIMESTAMP,
    execution_duration_sec DECIMAL(10, 2),
    source_schema          STRING,
    source_table           STRING,
    target_schema          STRING          NOT NULL,
    target_table           STRING          NOT NULL,
    rows_read              BIGINT          DEFAULT 0,
    rows_copied            BIGINT          DEFAULT 0,
    rows_deleted           BIGINT          DEFAULT 0,
    total_cost             DECIMAL(10, 4),
    department_id          INT             NOT NULL,
    status                 STRING          NOT NULL,
    created_at             TIMESTAMP       DEFAULT current_timestamp()
)
USING DELTA
CLUSTER BY (business_date, source_name)
COMMENT 'Framework audit table using Liquid Clustering to prevent over-partitioning';
