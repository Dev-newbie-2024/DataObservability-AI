-- 008_create_lineage_edges.sql
CREATE TABLE IF NOT EXISTS LINEAGE_EDGES (
    id              VARCHAR(36) NOT NULL DEFAULT UUID_STRING() PRIMARY KEY,
    source_table    VARCHAR(255) NOT NULL,
    source_column   VARCHAR(255),
    target_table    VARCHAR(255) NOT NULL,
    target_column   VARCHAR(255),
    transformation  TEXT,
    pipeline_run_id VARCHAR(36) REFERENCES PIPELINE_RUNS(id),
    created_at      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
