-- 004_create_drift_tables.sql
CREATE TABLE IF NOT EXISTS DRIFT_RUNS (
    id                     VARCHAR(36) NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    dataset_id             VARCHAR(36) REFERENCES DATASETS(id),
    pipeline_run_id        VARCHAR(36) REFERENCES PIPELINE_RUNS(id),
    reference_window_start TIMESTAMP_LTZ,
    reference_window_end   TIMESTAMP_LTZ,
    current_window_start   TIMESTAMP_LTZ,
    current_window_end     TIMESTAMP_LTZ,
    overall_drift_score    NUMBER(5,4),
    drift_detected         BOOLEAN,
    evaluated_at           TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS DRIFT_METRICS (
    id              VARCHAR(36) NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    drift_run_id    VARCHAR(36) REFERENCES DRIFT_RUNS(id),
    column_name     VARCHAR(255) NOT NULL,
    drift_method    VARCHAR(50)  NOT NULL,
    statistic_value NUMBER(15,8) NOT NULL,
    p_value         NUMBER(15,8),
    psi_score       NUMBER(10,6),
    drift_detected  BOOLEAN,
    severity        VARCHAR(20),
    evaluated_at    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
