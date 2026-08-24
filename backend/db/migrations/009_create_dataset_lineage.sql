-- 009_create_dataset_lineage.sql
-- Phase 3: Bronze → Silver → Gold row-level lineage tracking
--
-- DATASET_LINEAGE records every layer transition for full data provenance:
--   - Which source table fed which target table
--   - How many rows went in, out, and were rejected
--   - Which pipeline run caused the transition
--   - What transformation type was applied (merge_dedup, aggregate, copy, …)

CREATE TABLE IF NOT EXISTS OBSERVABILITY.DATASET_LINEAGE (
    id                  VARCHAR(36)     NOT NULL DEFAULT UUID_STRING(),
    dataset_id          VARCHAR(36)     NOT NULL,
    pipeline_run_id     VARCHAR(36),
    event_at            TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    source_layer        VARCHAR(20)     NOT NULL,   -- BRONZE | SILVER | GOLD
    target_layer        VARCHAR(20)     NOT NULL,   -- SILVER | GOLD
    source_table        VARCHAR(255)    NOT NULL,
    target_table        VARCHAR(255)    NOT NULL,
    rows_in             NUMBER(18,0)    DEFAULT 0,
    rows_out            NUMBER(18,0)    DEFAULT 0,
    rows_rejected       NUMBER(18,0)    DEFAULT 0,
    transformation_type VARCHAR(50)     DEFAULT 'copy',
    notes               VARCHAR(2000),
    PRIMARY KEY (id),
    FOREIGN KEY (dataset_id)     REFERENCES OBSERVABILITY.DATASETS(id),
    FOREIGN KEY (pipeline_run_id) REFERENCES OBSERVABILITY.PIPELINE_RUNS(id)
);

-- Index for fast dataset lineage lookups
CREATE INDEX IF NOT EXISTS idx_dataset_lineage_dataset_id
    ON OBSERVABILITY.DATASET_LINEAGE (dataset_id);

-- Index for pipeline run correlation
CREATE INDEX IF NOT EXISTS idx_dataset_lineage_run_id
    ON OBSERVABILITY.DATASET_LINEAGE (pipeline_run_id);
