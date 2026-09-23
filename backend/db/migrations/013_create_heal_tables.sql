-- 013_create_heal_tables.sql
-- Snowflake tables used by the Self-Healing Agent.
--
-- HEAL_RUNS    → one row per recovery attempt (the "run header")
-- HEAL_ACTIONS → one row per individual action taken within a run
-- HEAL_SUMMARY → flat denormalised row for dashboard trend queries

CREATE TABLE IF NOT EXISTS HEAL_RUNS (
    id                  VARCHAR(36)  NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    dataset_id          VARCHAR(36),
    pipeline_run_id     VARCHAR(36),

    -- Failure classification
    failure_type        VARCHAR(50)  NOT NULL DEFAULT 'SYSTEM',
    failure_reason      VARCHAR(2048),
    failed_task         VARCHAR(255),

    -- Recovery outcome
    healing_strategy    VARCHAR(50)  NOT NULL DEFAULT 'retry',
    recovery_status     VARCHAR(30)  NOT NULL DEFAULT 'PENDING',  -- RESOLVED | FAILED | ESCALATED
    retry_count         NUMBER(5,0)  NOT NULL DEFAULT 0,
    mttr_seconds        NUMBER(15,4),                              -- Mean Time To Recovery

    -- Timestamps
    failure_detected_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    resolved_at         TIMESTAMP_LTZ,
    created_at          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS HEAL_ACTIONS (
    id              VARCHAR(36)  NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    heal_run_id     VARCHAR(36)  REFERENCES HEAL_RUNS(id),
    action_type     VARCHAR(100) NOT NULL,
    action_detail   VARCHAR(4096),
    success         BOOLEAN      NOT NULL DEFAULT FALSE,
    executed_at     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS HEAL_SUMMARY (
    id                  VARCHAR(36)  NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    dataset_id          VARCHAR(36),
    pipeline_run_id     VARCHAR(36),
    failure_type        VARCHAR(50)  NOT NULL DEFAULT 'SYSTEM',
    failure_reason      VARCHAR(2048),
    healing_strategy    VARCHAR(50)  NOT NULL DEFAULT 'retry',
    recovery_status     VARCHAR(30)  NOT NULL DEFAULT 'PENDING',
    retry_count         NUMBER(5,0)  NOT NULL DEFAULT 0,
    mttr_seconds        NUMBER(15,4),
    actions_taken       NUMBER(5,0)  NOT NULL DEFAULT 0,
    actions_succeeded   NUMBER(5,0)  NOT NULL DEFAULT 0,
    quarantined         BOOLEAN      NOT NULL DEFAULT FALSE,
    evaluated_at        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
