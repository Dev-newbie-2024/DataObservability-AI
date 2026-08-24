-- 007_create_agent_sessions.sql
CREATE TABLE IF NOT EXISTS AGENT_SESSIONS (
    id              VARCHAR(36) NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    agent_type      VARCHAR(100) NOT NULL,
    dataset_id      VARCHAR(36) REFERENCES DATASETS(id),
    trigger_event   VARCHAR(255),
    input_context   VARIANT,
    decision        TEXT,
    actions_taken   VARIANT,
    outcome         VARCHAR(50),
    started_at      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    completed_at    TIMESTAMP_LTZ
);
