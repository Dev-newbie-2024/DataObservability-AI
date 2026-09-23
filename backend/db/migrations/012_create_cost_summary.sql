-- 012_create_cost_summary.sql
-- Denormalised per-run cost summary written by the Cost Agent.
--
-- Relationship to existing tables:
--   COST_RECORDS   → per-query-run cost rows
--   COST_FORECASTS → 30-day forecast rows
--   COST_SUMMARY (this table) → one flat row per agent run, used by dashboards.

CREATE TABLE IF NOT EXISTS COST_SUMMARY (
    id                      VARCHAR(36)   NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    dataset_id              VARCHAR(36),
    pipeline_run_id         VARCHAR(36),
    warehouse_name          VARCHAR(255),
    lookback_days           NUMBER(5,0)   NOT NULL DEFAULT 7,

    -- Credit consumption
    total_credits           NUMBER(15,8)  NOT NULL DEFAULT 0,
    total_usd               NUMBER(15,4)  NOT NULL DEFAULT 0,
    daily_avg_credits       NUMBER(15,8)  NOT NULL DEFAULT 0,
    cost_per_run            NUMBER(15,8)  NOT NULL DEFAULT 0,

    -- Storage
    storage_gb              NUMBER(15,4)  NOT NULL DEFAULT 0,
    storage_cost_usd        NUMBER(15,4)  NOT NULL DEFAULT 0,

    -- 30-day forecast
    forecast_credits_30d    NUMBER(15,4),
    forecast_usd_30d        NUMBER(15,4),

    -- Score
    cost_score              NUMBER(6,2)   NOT NULL DEFAULT 100,
    cost_severity           VARCHAR(20)   NOT NULL DEFAULT 'LOW',
    pipeline_run_count      NUMBER(10,0)  NOT NULL DEFAULT 0,

    evaluated_at            TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
