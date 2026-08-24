-- 005_create_cost_tables.sql
CREATE TABLE IF NOT EXISTS COST_RECORDS (
    id                VARCHAR(36) NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    dataset_id        VARCHAR(36) REFERENCES DATASETS(id),
    pipeline_run_id   VARCHAR(36) REFERENCES PIPELINE_RUNS(id),
    warehouse_name    VARCHAR(255),
    query_id          VARCHAR(255),
    query_type        VARCHAR(100),
    credits_used      NUMBER(15,8) NOT NULL,
    credits_usd       NUMBER(15,4),
    execution_time_ms NUMBER(18,0),
    bytes_scanned     NUMBER(18,0),
    rows_produced     NUMBER(18,0),
    recorded_at       TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS COST_BUDGETS (
    id              VARCHAR(36) NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    dataset_id      VARCHAR(36) REFERENCES DATASETS(id),
    period_type     VARCHAR(20) NOT NULL,
    budget_credits  NUMBER(15,4) NOT NULL,
    budget_usd      NUMBER(15,2),
    alert_at_pct    INTEGER DEFAULT 80,
    created_at      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    is_active       BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS COST_FORECASTS (
    id                VARCHAR(36) NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    dataset_id        VARCHAR(36) REFERENCES DATASETS(id),
    forecast_date     DATE NOT NULL,
    predicted_credits NUMBER(15,4),
    lower_bound       NUMBER(15,4),
    upper_bound       NUMBER(15,4),
    model_used        VARCHAR(100),
    created_at        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
