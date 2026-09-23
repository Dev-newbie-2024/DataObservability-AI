-- 011_create_drift_summary.sql
-- Denormalised per-run drift summary written by the Drift Agent.
--
-- Relationship to existing tables:
--   DRIFT_RUNS    → per-run header (tied to PIPELINE_RUNS)
--   DRIFT_METRICS → per-column metric rows (tied to DRIFT_RUNS)
--   DRIFT_SUMMARY (this table) → one flat row per agent run
--       used by the dashboard for trend queries without joins.

CREATE TABLE IF NOT EXISTS DRIFT_SUMMARY (
    id                  VARCHAR(36)   NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    dataset_id          VARCHAR(36),
    pipeline_run_id     VARCHAR(36),
    silver_table        VARCHAR(255),
    reference_table     VARCHAR(255),

    drift_score         NUMBER(5,4)   NOT NULL DEFAULT 0,
    drift_detected      BOOLEAN       NOT NULL DEFAULT FALSE,
    drift_severity      VARCHAR(20)   NOT NULL DEFAULT 'LOW',

    drifted_columns     NUMBER(5,0)   NOT NULL DEFAULT 0,
    total_columns       NUMBER(5,0)   NOT NULL DEFAULT 0,
    drift_pct           NUMBER(5,2)   NOT NULL DEFAULT 0,
    avg_psi             NUMBER(10,6),
    avg_ks_stat         NUMBER(10,6),

    raw_metrics         VARIANT,
    evaluated_at        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
