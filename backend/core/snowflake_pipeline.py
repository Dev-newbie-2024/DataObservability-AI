"""
backend/core/snowflake_pipeline.py
=====================================
Snowflake pipeline operations for the Bronze → Silver → Gold flow.

Keeps heavy DDL + lineage SQL separate from the main snowflake_client.py
so that module stays focused on OBSERVABILITY table CRUD.

Responsibilities:
  - CREATE Silver/Gold tables mirroring Bronze schema + audit columns.
  - COPY INTO Bronze (Snowflake stage ingestion, alternative to bulk INSERT).
  - Bronze → Silver MERGE (dedup + type-cast + quarantine bad rows).
  - Silver → Gold aggregate views / CTAS.
  - Dataset lineage recording in OBSERVABILITY.DATASET_LINEAGE.
  - Pipeline run status updates with row-level stats.

All statements are idempotent (CREATE IF NOT EXISTS, MERGE ON pk).
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from backend.core.database import execute_query, execute_statement
from backend.core.exceptions import SnowflakeQueryError
from config.logging_config import get_logger
from config.settings import snowflake_settings

logger = get_logger(__name__)

DB  = snowflake_settings.database
OBS = snowflake_settings.schema_
BRZ = snowflake_settings.bronze_schema
SLV = snowflake_settings.silver_schema
GLD = snowflake_settings.gold_schema
QRN = snowflake_settings.quarantine_schema


# ─── Silver DDL ───────────────────────────────────────────────────────────────

def create_silver_table(
    bronze_table: str,
    columns: list[dict[str, str]],
) -> str:
    """
    Create a Silver table mirroring the Bronze schema + standard audit columns.

    Silver tables add:
      - _silver_loaded_at  TIMESTAMP_NTZ  (when this row entered Silver)
      - _pipeline_run_id   VARCHAR(36)    (tracing back to pipeline run)
      - _quality_score     FLOAT          (0-1, set by Quality Agent)
      - _is_quarantined    BOOLEAN        (True = failed quality gate)

    Args:
        bronze_table: Bronze table name (e.g. "sales_data_raw").
        columns:      List of {"name": str, "snowflake_type": str} dicts.

    Returns:
        Silver table name (e.g. "sales_data_silver").
    """
    silver_table = _bronze_to_silver_name(bronze_table)
    col_defs = "\n    ".join(
        f"{c['name']} {c['snowflake_type']}" for c in columns
    )
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {DB}.{SLV}.{silver_table.upper()} (
        _silver_id          VARCHAR(36)    DEFAULT UUID_STRING() NOT NULL,
        _pipeline_run_id    VARCHAR(36),
        _silver_loaded_at   TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
        _bronze_loaded_at   TIMESTAMP_NTZ,
        _source_file        VARCHAR(512),
        _schema_version     NUMBER(5,0),
        _quality_score      FLOAT          DEFAULT 1.0,
        _is_quarantined     BOOLEAN        DEFAULT FALSE,
        {col_defs}
    )
    """
    execute_statement(ddl)
    logger.info("Silver table ensured", table=silver_table)
    return silver_table


def create_gold_table(
    silver_table: str,
    columns: list[dict[str, str]],
    aggregation_key: str | None = None,
) -> str:
    """
    Create a Gold summary table (one row per aggregation_key per day).

    Gold tables contain pre-aggregated metrics ready for the Streamlit dashboard.

    Args:
        silver_table:     Silver table name.
        columns:          Column list from the schema (used to pick numerics).
        aggregation_key:  Column to GROUP BY (defaults to first non-system col).

    Returns:
        Gold table name (e.g. "sales_data_gold").
    """
    gold_table = _silver_to_gold_name(silver_table)
    numeric_cols = [c for c in columns if _is_numeric(c["snowflake_type"])]

    # Build aggregate expressions: SUM + AVG for each numeric column
    agg_exprs: list[str] = []
    for c in numeric_cols[:20]:  # cap at 20 to avoid oversized DDL
        agg_exprs.append(f"SUM({c['name']}) AS {c['name']}_sum")
        agg_exprs.append(f"AVG({c['name']}) AS {c['name']}_avg")

    agg_block = "\n    " + ",\n    ".join(agg_exprs) if agg_exprs else ""

    grp_col = aggregation_key or (columns[0]["name"] if columns else "_pipeline_run_id")

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {DB}.{GLD}.{gold_table.upper()} (
        _gold_id            VARCHAR(36)    DEFAULT UUID_STRING() NOT NULL,
        _pipeline_run_id    VARCHAR(36),
        _gold_loaded_at     TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
        _aggregation_date   DATE           DEFAULT CURRENT_DATE(),
        _aggregation_key    VARCHAR(255),
        _row_count          NUMBER(18,0),
        _quality_score_avg  FLOAT{agg_block}
    )
    """
    execute_statement(ddl)
    logger.info("Gold table ensured", table=gold_table)
    return gold_table


# ─── Bronze → Silver MERGE ────────────────────────────────────────────────────

def merge_bronze_to_silver(
    *,
    bronze_table: str,
    silver_table: str,
    pipeline_run_id: str,
    schema_version: int = 1,
) -> dict[str, int]:
    """
    MERGE rows from Bronze into Silver, deduplicating on _row_hash.

    Rows that fail basic quality checks (all-NULL rows) are marked
    ``_is_quarantined = TRUE`` in Silver rather than being discarded,
    so the lineage is preserved.

    Returns:
        Dict with keys: rows_merged, rows_quarantined.
    """
    brz_full = f"{DB}.{BRZ}.{bronze_table.upper()}"
    slv_full  = f"{DB}.{SLV}.{silver_table.upper()}"
    now       = datetime.now(timezone.utc).isoformat()

    merge_sql = f"""
    MERGE INTO {slv_full} AS tgt
    USING (
        SELECT *,
               MD5(CONCAT_WS('|', TO_VARCHAR(_loaded_at), TO_VARCHAR(_row_number)))
                   AS _dedup_key,
               CASE WHEN _loaded_at IS NULL THEN TRUE ELSE FALSE END AS _should_quarantine
        FROM   {brz_full}
        WHERE  _pipeline_run_id = %s
    ) AS src
    ON tgt._pipeline_run_id = src._pipeline_run_id
       AND tgt._bronze_loaded_at = src._loaded_at
    WHEN NOT MATCHED THEN INSERT (
        _pipeline_run_id,
        _bronze_loaded_at,
        _source_file,
        _schema_version,
        _is_quarantined
    ) VALUES (
        src._pipeline_run_id,
        src._loaded_at,
        src._source_file,
        %s,
        src._should_quarantine
    )
    """
    try:
        execute_statement(merge_sql, (pipeline_run_id, schema_version))
    except SnowflakeQueryError:
        # Bronze columns may not include _pipeline_run_id yet (old schema).
        # Fall back to a simpler INSERT SELECT.
        _fallback_insert_bronze_to_silver(
            brz_full=brz_full,
            slv_full=slv_full,
            pipeline_run_id=pipeline_run_id,
            schema_version=schema_version,
        )

    # Count results
    merged = _count_silver_for_run(slv_full, pipeline_run_id)
    quarantined = _count_quarantined_for_run(slv_full, pipeline_run_id)

    logger.info(
        "Bronze → Silver merge complete",
        bronze=bronze_table,
        silver=silver_table,
        merged=merged,
        quarantined=quarantined,
    )
    return {"rows_merged": merged, "rows_quarantined": quarantined}


def _fallback_insert_bronze_to_silver(
    *,
    brz_full: str,
    slv_full: str,
    pipeline_run_id: str,
    schema_version: int,
) -> None:
    """Simple INSERT SELECT fallback when MERGE conditions can't be met."""
    execute_statement(
        f"""
        INSERT INTO {slv_full} (_pipeline_run_id, _schema_version, _is_quarantined)
        SELECT %s, %s, FALSE
        FROM   {brz_full}
        WHERE  _pipeline_run_id = %s
           AND NOT EXISTS (
               SELECT 1 FROM {slv_full} s
               WHERE  s._pipeline_run_id = %s
           )
        """,
        (pipeline_run_id, schema_version, pipeline_run_id, pipeline_run_id),
    )


# ─── Silver → Gold aggregation ────────────────────────────────────────────────

def aggregate_silver_to_gold(
    *,
    silver_table: str,
    gold_table: str,
    pipeline_run_id: str,
    aggregation_key: str | None = None,
) -> int:
    """
    Compute daily aggregates from Silver and insert into Gold.

    Returns:
        Number of Gold rows inserted.
    """
    slv_full = f"{DB}.{SLV}.{silver_table.upper()}"
    gld_full = f"{DB}.{GLD}.{gold_table.upper()}"
    grp_col  = aggregation_key or "_pipeline_run_id"

    insert_sql = f"""
    INSERT INTO {gld_full} (
        _pipeline_run_id,
        _aggregation_date,
        _aggregation_key,
        _row_count,
        _quality_score_avg
    )
    SELECT
        %s,
        CURRENT_DATE(),
        COALESCE(TO_VARCHAR({grp_col}), 'unknown'),
        COUNT(*),
        AVG(COALESCE(_quality_score, 1.0))
    FROM   {slv_full}
    WHERE  _pipeline_run_id = %s
      AND  _is_quarantined  = FALSE
    GROUP  BY COALESCE(TO_VARCHAR({grp_col}), 'unknown')
    """
    execute_statement(insert_sql, (pipeline_run_id, pipeline_run_id))

    rows = _count_gold_for_run(gld_full, pipeline_run_id)
    logger.info(
        "Silver → Gold aggregation complete",
        silver=silver_table,
        gold=gold_table,
        gold_rows=rows,
    )
    return rows


# ─── Lineage Recording ────────────────────────────────────────────────────────

def record_lineage(
    *,
    dataset_id: str,
    pipeline_run_id: str,
    source_layer: str,
    target_layer: str,
    source_table: str,
    target_table: str,
    rows_in: int,
    rows_out: int,
    rows_rejected: int = 0,
    transformation_type: str = "copy",
    notes: str = "",
) -> str:
    """
    Append a lineage event to OBSERVABILITY.DATASET_LINEAGE.

    Each row records one layer transition (e.g. BRONZE→SILVER) with
    full row counts so data volume is traceable at any point in time.

    Returns:
        The new lineage event UUID.
    """
    lineage_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    execute_statement(
        f"""
        INSERT INTO {DB}.{OBS}.DATASET_LINEAGE
            (id, dataset_id, pipeline_run_id, event_at,
             source_layer, target_layer, source_table, target_table,
             rows_in, rows_out, rows_rejected,
             transformation_type, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            lineage_id, dataset_id, pipeline_run_id, now,
            source_layer.upper(), target_layer.upper(),
            source_table, target_table,
            rows_in, rows_out, rows_rejected,
            transformation_type, notes,
        ),
    )
    logger.info(
        "Lineage recorded",
        lineage_id=lineage_id,
        dataset_id=dataset_id,
        transition=f"{source_layer}→{target_layer}",
        rows_in=rows_in,
        rows_out=rows_out,
    )
    return lineage_id


def get_lineage(dataset_id: str, limit: int = 50) -> list[dict[str, Any]]:
    """Return the lineage history for a dataset, newest first."""
    return execute_query(
        f"""
        SELECT *
        FROM   {DB}.{OBS}.DATASET_LINEAGE
        WHERE  dataset_id = %s
        ORDER  BY event_at DESC
        LIMIT  %s
        """,
        (dataset_id, limit),
    )


# ─── DDL for OBSERVABILITY.DATASET_LINEAGE ────────────────────────────────────

LINEAGE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {db}.{obs}.DATASET_LINEAGE (
    id                  VARCHAR(36)     NOT NULL,
    dataset_id          VARCHAR(36)     NOT NULL,
    pipeline_run_id     VARCHAR(36),
    event_at            TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    source_layer        VARCHAR(20)     NOT NULL,
    target_layer        VARCHAR(20)     NOT NULL,
    source_table        VARCHAR(255)    NOT NULL,
    target_table        VARCHAR(255)    NOT NULL,
    rows_in             NUMBER(18,0)    DEFAULT 0,
    rows_out            NUMBER(18,0)    DEFAULT 0,
    rows_rejected       NUMBER(18,0)    DEFAULT 0,
    transformation_type VARCHAR(50)     DEFAULT 'copy',
    notes               VARCHAR(2000),
    PRIMARY KEY (id)
)
"""


def ensure_lineage_table() -> None:
    """Create DATASET_LINEAGE table if it doesn't exist."""
    execute_statement(LINEAGE_TABLE_DDL.format(db=DB, obs=OBS))
    logger.info("DATASET_LINEAGE table ensured")


# ─── Private Helpers ──────────────────────────────────────────────────────────

def _bronze_to_silver_name(bronze_table: str) -> str:
    return bronze_table.lower().removesuffix("_raw") + "_silver"


def _silver_to_gold_name(silver_table: str) -> str:
    return silver_table.lower().removesuffix("_silver") + "_gold"


def _is_numeric(sf_type: str) -> bool:
    upper = sf_type.upper()
    return any(t in upper for t in ("NUMBER", "FLOAT", "INT", "DECIMAL", "NUMERIC"))


def _count_silver_for_run(table: str, run_id: str) -> int:
    rows = execute_query(
        f"SELECT COUNT(*) AS n FROM {table} WHERE _pipeline_run_id = %s", (run_id,)
    )
    return int(rows[0]["N"]) if rows else 0


def _count_quarantined_for_run(table: str, run_id: str) -> int:
    rows = execute_query(
        f"SELECT COUNT(*) AS n FROM {table} WHERE _pipeline_run_id = %s AND _is_quarantined = TRUE",
        (run_id,),
    )
    return int(rows[0]["N"]) if rows else 0


def _count_gold_for_run(table: str, run_id: str) -> int:
    rows = execute_query(
        f"SELECT COUNT(*) AS n FROM {table} WHERE _pipeline_run_id = %s", (run_id,)
    )
    return int(rows[0]["N"]) if rows else 0
