-- 001_create_datasets.sql
-- Core dataset registry and schema version tracking

CREATE TABLE IF NOT EXISTS DATASETS (
    id              VARCHAR(36)  NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    source_type     VARCHAR(50)  NOT NULL,
    domain          VARCHAR(100),
    bronze_table    VARCHAR(255),
    silver_table    VARCHAR(255),
    gold_table      VARCHAR(255),
    created_at      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    is_active       BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS SCHEMA_VERSIONS (
    id              VARCHAR(36)  NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    dataset_id      VARCHAR(36)  NOT NULL REFERENCES DATASETS(id),
    version         INTEGER      NOT NULL,
    schema_json     VARIANT      NOT NULL,
    fingerprint     VARCHAR(64)  NOT NULL,
    created_at      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    is_current      BOOLEAN DEFAULT TRUE,
    change_summary  TEXT
);

CREATE TABLE IF NOT EXISTS SCHEMA_CHANGE_EVENTS (
    id              VARCHAR(36)  NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    dataset_id      VARCHAR(36)  REFERENCES DATASETS(id),
    old_version_id  VARCHAR(36)  REFERENCES SCHEMA_VERSIONS(id),
    new_version_id  VARCHAR(36)  REFERENCES SCHEMA_VERSIONS(id),
    change_type     VARCHAR(50)  NOT NULL,
    column_name     VARCHAR(255),
    details         VARIANT,
    detected_at     TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    handled_by      VARCHAR(100)
);
