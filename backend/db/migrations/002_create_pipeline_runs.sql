-- 002_create_pipeline_runs.sql
-- Pipeline execution tracking

CREATE TABLE IF NOT EXISTS PIPELINE_RUNS (
    id              VARCHAR(36)  NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    dataset_id      VARCHAR(36)  REFERENCES DATASETS(id),
    dag_id          VARCHAR(255),
    run_id          VARCHAR(255) UNIQUE,
    status          VARCHAR(50)  NOT NULL,
    triggered_by    VARCHAR(100),
    rows_ingested   NUMBER(18,0),
    rows_rejected   NUMBER(18,0),
    started_at      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    completed_at    TIMESTAMP_LTZ,
    error_message   TEXT,
    metadata        VARIANT
);

CREATE TABLE IF NOT EXISTS PIPELINE_TASKS (
    id              VARCHAR(36)  NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    run_id          VARCHAR(36)  REFERENCES PIPELINE_RUNS(id),
    task_name       VARCHAR(255) NOT NULL,
    task_type       VARCHAR(100),
    status          VARCHAR(50),
    retry_count     INTEGER DEFAULT 0,
    started_at      TIMESTAMP_LTZ,
    completed_at    TIMESTAMP_LTZ,
    duration_sec    FLOAT,
    error_details   TEXT
);
