-- 006_create_alert_tables.sql
CREATE TABLE IF NOT EXISTS ALERT_RULES (
    id              VARCHAR(36) NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    dataset_id      VARCHAR(36) REFERENCES DATASETS(id),
    alert_name      VARCHAR(255) NOT NULL,
    alert_type      VARCHAR(100) NOT NULL,
    condition_json  VARIANT NOT NULL,
    severity        VARCHAR(20) DEFAULT 'medium',
    channels        ARRAY,
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS ALERT_HISTORY (
    id              VARCHAR(36) NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    rule_id         VARCHAR(36) REFERENCES ALERT_RULES(id),
    dataset_id      VARCHAR(36) REFERENCES DATASETS(id),
    pipeline_run_id VARCHAR(36) REFERENCES PIPELINE_RUNS(id),
    alert_type      VARCHAR(100),
    severity        VARCHAR(20),
    title           VARCHAR(500),
    body            TEXT,
    context_json    VARIANT,
    fired_at        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    acknowledged_at TIMESTAMP_LTZ,
    resolved_at     TIMESTAMP_LTZ,
    acknowledged_by VARCHAR(255)
);
