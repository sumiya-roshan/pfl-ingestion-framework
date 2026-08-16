-- One audit row per ingestion task.  The row is inserted at task start with
-- status INPROGRESS, then updated to SUCCESS or FAILED when the task finishes.
CREATE TABLE IF NOT EXISTS main.monitoring.data_pipeline_execution_master (
    config_master_id          INT             NOT NULL,
    table_id                  INT             NOT NULL,
    department_id             INT             NOT NULL,
    delta_layer               STRING          NOT NULL,
    source_name               STRING          NOT NULL,
    pipeline_name             STRING          NOT NULL,
    load_type                 STRING          NOT NULL,
    frequency                 STRING,
    business_date             DATE            NOT NULL,
    job_id                    STRING,
    job_run_id                STRING          NOT NULL,
    trigger_type              STRING,
    trigger_id                STRING,
    trigger_name              STRING,
    trigger_time              TIMESTAMP       NOT NULL,
    end_time                  TIMESTAMP,
    execution_duration_sec    DECIMAL(10, 2),
    source_schema             STRING,
    source_table              STRING,
    target_schema             STRING          NOT NULL,
    target_table              STRING          NOT NULL,
    rows_read                 BIGINT          DEFAULT 0,
    rows_copied               BIGINT          DEFAULT 0,
    rows_deleted              BIGINT          DEFAULT 0,
    rows_affected             BIGINT          DEFAULT 0,
    data_read_bytes           BIGINT          DEFAULT 0,
    data_written_bytes        BIGINT          DEFAULT 0,
    throughput_mb_per_sec     DECIMAL(10, 2),
    copy_duration_sec         DECIMAL(10, 2),
    databricks_notebook_name  STRING,
    databricks_url            STRING,
    operation_performed       STRING,
    status                    STRING          NOT NULL,
    error_code                STRING,
    error_message             STRING,
    created_at                TIMESTAMP       DEFAULT current_timestamp()
)
USING DELTA
CLUSTER BY (business_date, source_name)
COMMENT 'Ingestion audit table: INPROGRESS at start, SUCCESS or FAILED at completion';
