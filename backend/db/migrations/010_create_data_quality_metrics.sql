-- 010_create_data_quality_metrics.sql
-- Denormalised per-run quality summary written by the Quality Agent.
--
-- Relationship to existing tables:
--   QUALITY_RUNS    → per-run header (tied to PIPELINE_RUNS)
--   QUALITY_METRICS → per-column metric rows (tied to QUALITY_RUNS)
--   DATA_QUALITY_METRICS (this table) → one flat row per agent run
--       used by the dashboard for trend queries without joins.
--
-- All columns are nullable to handle partial-data scenarios gracefully.

CREATE TABLE IF NOT EXISTS DATA_QUALITY_METRICS (
    id                  VARCHAR(36)   NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    dataset_id          VARCHAR(36),
    pipeline_run_id     VARCHAR(36),
    silver_table        VARCHAR(255),

    -- Overall score (0–100, two decimal places)
    quality_score       NUMBER(5,2)   NOT NULL,
    total_rows          NUMBER(18,0)  NOT NULL,
    passed              BOOLEAN       NOT NULL DEFAULT FALSE,

    -- Per-dimension rates (0.000000 – 1.000000)
    null_rate           NUMBER(8,6)   NOT NULL DEFAULT 0,
    duplicate_rate      NUMBER(8,6)   NOT NULL DEFAULT 0,
    type_mismatch_rate  NUMBER(8,6)   NOT NULL DEFAULT 0,
    range_fail_rate     NUMBER(8,6)   NOT NULL DEFAULT 0,

    -- Freshness (hours since most recent Silver row loaded)
    freshness_hours     NUMBER(10,2),

    -- Full column-level metric snapshot for deep dives
    raw_metrics         VARIANT,

    evaluated_at        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
