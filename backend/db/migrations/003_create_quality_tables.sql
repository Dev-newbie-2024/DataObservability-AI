-- 003_create_quality_tables.sql
-- Data quality runs and column-level metrics

CREATE TABLE IF NOT EXISTS QUALITY_RUNS (
    id              VARCHAR(36)  NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    dataset_id      VARCHAR(36)  REFERENCES DATASETS(id),
    pipeline_run_id VARCHAR(36)  REFERENCES PIPELINE_RUNS(id),
    schema_version  INTEGER,
    total_rows      NUMBER(18,0),
    quality_score   NUMBER(5,2),
    evaluated_at    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS QUALITY_METRICS (
    id              VARCHAR(36)  NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    quality_run_id  VARCHAR(36)  REFERENCES QUALITY_RUNS(id),
    column_name     VARCHAR(255),
    metric_type     VARCHAR(100) NOT NULL,
    metric_value    NUMBER(15,6) NOT NULL,
    threshold       NUMBER(15,6),
    passed          BOOLEAN,
    evaluated_at    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
