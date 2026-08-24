"""
backend/core/snowflake_client.py
=================================
High-level Snowflake operations client.

Wraps `backend.core.database` with domain-specific methods:
  - Schema/table DDL (Bronze table creation, schema evolution)
  - OBSERVABILITY table CRUD (datasets, pipeline_runs, schema_versions, …)
  - Bulk INSERT for Bronze data loading
  - Cost-tracking queries

All methods map directly to one or more SQL statements and raise typed
exceptions from `backend.core.exceptions`.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from backend.core.database import execute_query, execute_statement, get_connection
from backend.core.exceptions import SnowflakeQueryError
from config.constants import SnowflakeSchema
from config.logging_config import get_logger
from config.settings import snowflake_settings

logger = get_logger(__name__)

# Convenience shorthands
DB = snowflake_settings.database
OBS = snowflake_settings.schema_          # OBSERVABILITY
BRZ = snowflake_settings.bronze_schema    # BRONZE
SLV = snowflake_settings.silver_schema    # SILVER


# ─── Schema Helpers ───────────────────────────────────────────────────────────

def ensure_schemas() -> None:
    """Create all required schemas if they don't exist yet."""
    for schema in [OBS, BRZ, SLV, snowflake_settings.gold_schema, snowflake_settings.quarantine_schema]:
        execute_statement(f"CREATE SCHEMA IF NOT EXISTS {DB}.{schema}")
    logger.info("All Snowflake schemas verified")


# ─── OBSERVABILITY.DATASETS ───────────────────────────────────────────────────

def insert_dataset(
    *,
    name: str,
    source_type: str,
    description: str = "",
    domain: str = "",
    bronze_table: str = "",
) -> str:
    """
    Insert a new row into OBSERVABILITY.DATASETS.

    Returns:
        The new dataset UUID.
    """
    dataset_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    execute_statement(
        f"""
        INSERT INTO {DB}.{OBS}.DATASETS
            (id, name, description, source_type, domain, bronze_table,
             created_at, updated_at, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE)
        """,
        (dataset_id, name, description, source_type, domain, bronze_table, now, now),
    )
    logger.info("Dataset registered", dataset_id=dataset_id, name=name)
    return dataset_id


def update_dataset_tables(
    dataset_id: str,
    *,
    bronze_table: str = "",
    silver_table: str = "",
    gold_table: str = "",
) -> None:
    """Update the table name fields after they have been created."""
    execute_statement(
        f"""
        UPDATE {DB}.{OBS}.DATASETS
        SET    bronze_table = %s,
               silver_table = %s,
               gold_table   = %s,
               updated_at   = %s
        WHERE  id = %s
        """,
        (bronze_table, silver_table, gold_table,
         datetime.now(timezone.utc).isoformat(), dataset_id),
    )


def get_dataset_by_id(dataset_id: str) -> dict[str, Any] | None:
    """Return a single dataset row or None."""
    rows = execute_query(
        f"SELECT * FROM {DB}.{OBS}.DATASETS WHERE id = %s AND is_active = TRUE",
        (dataset_id,),
    )
    return rows[0] if rows else None


def get_dataset_by_name(name: str) -> dict[str, Any] | None:
    """Return a single dataset row by name or None."""
    rows = execute_query(
        f"SELECT * FROM {DB}.{OBS}.DATASETS WHERE name = %s AND is_active = TRUE",
        (name,),
    )
    return rows[0] if rows else None


def list_datasets(limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
    """Return paginated list of active datasets, newest first."""
    return execute_query(
        f"""
        SELECT * FROM {DB}.{OBS}.DATASETS
        WHERE  is_active = TRUE
        ORDER  BY created_at DESC
        LIMIT  %s OFFSET %s
        """,
        (limit, offset),
    )


def count_datasets() -> int:
    """Return total count of active datasets."""
    rows = execute_query(
        f"SELECT COUNT(*) AS cnt FROM {DB}.{OBS}.DATASETS WHERE is_active = TRUE"
    )
    return int(rows[0]["CNT"]) if rows else 0


# ─── OBSERVABILITY.SCHEMA_VERSIONS ───────────────────────────────────────────

def insert_schema_version(
    dataset_id: str,
    version: int,
    schema_json: list[dict],
    change_summary: str = "",
) -> str:
    """
    Insert a new schema version row.

    The fingerprint is a SHA-256 of the canonical JSON representation of
    the schema columns so that identical schemas share the same fingerprint.

    Returns:
        The new schema version UUID.
    """
    version_id = str(uuid.uuid4())
    canonical = json.dumps(schema_json, sort_keys=True, ensure_ascii=False)
    fingerprint = hashlib.sha256(canonical.encode()).hexdigest()[:16]
    now = datetime.now(timezone.utc).isoformat()

    # Mark previous versions as non-current
    execute_statement(
        f"UPDATE {DB}.{OBS}.SCHEMA_VERSIONS SET is_current = FALSE WHERE dataset_id = %s",
        (dataset_id,),
    )

    execute_statement(
        f"""
        INSERT INTO {DB}.{OBS}.SCHEMA_VERSIONS
            (id, dataset_id, version, schema_json, fingerprint,
             created_at, is_current, change_summary)
        VALUES (%s, %s, %s, PARSE_JSON(%s), %s, %s, TRUE, %s)
        """,
        (version_id, dataset_id, version, canonical, fingerprint, now, change_summary),
    )
    logger.info(
        "Schema version registered",
        dataset_id=dataset_id,
        version=version,
        fingerprint=fingerprint,
    )
    return version_id


def get_current_schema_version(dataset_id: str) -> dict[str, Any] | None:
    """Return the current schema version row for a dataset, or None."""
    rows = execute_query(
        f"""
        SELECT * FROM {DB}.{OBS}.SCHEMA_VERSIONS
        WHERE  dataset_id = %s AND is_current = TRUE
        ORDER  BY version DESC
        LIMIT  1
        """,
        (dataset_id,),
    )
    return rows[0] if rows else None


def get_schema_version_count(dataset_id: str) -> int:
    """Return the total number of schema versions for a dataset."""
    rows = execute_query(
        f"SELECT COUNT(*) AS cnt FROM {DB}.{OBS}.SCHEMA_VERSIONS WHERE dataset_id = %s",
        (dataset_id,),
    )
    return int(rows[0]["CNT"]) if rows else 0


# ─── OBSERVABILITY.PIPELINE_RUNS ─────────────────────────────────────────────

def insert_pipeline_run(
    *,
    dataset_id: str,
    dag_id: str,
    run_id: str,
    triggered_by: str = "api",
) -> str:
    """Create a pipeline run record in RUNNING state. Returns the run UUID."""
    pk = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    execute_statement(
        f"""
        INSERT INTO {DB}.{OBS}.PIPELINE_RUNS
            (id, dataset_id, dag_id, run_id, status, triggered_by, started_at)
        VALUES (%s, %s, %s, %s, 'running', %s, %s)
        """,
        (pk, dataset_id, dag_id, run_id, triggered_by, now),
    )
    return pk


def complete_pipeline_run(
    run_pk: str,
    *,
    status: str,
    rows_ingested: int = 0,
    rows_rejected: int = 0,
    error_message: str = "",
) -> None:
    """Mark a pipeline run as finished (success/failed/etc.)."""
    now = datetime.now(timezone.utc).isoformat()
    execute_statement(
        f"""
        UPDATE {DB}.{OBS}.PIPELINE_RUNS
        SET    status        = %s,
               rows_ingested = %s,
               rows_rejected = %s,
               completed_at  = %s,
               error_message = %s
        WHERE  id = %s
        """,
        (status, rows_ingested, rows_rejected, now, error_message, run_pk),
    )


# ─── Bronze Table DDL ─────────────────────────────────────────────────────────

def create_bronze_table(table_name: str, columns: list[dict[str, Any]]) -> None:
    """
    Dynamically CREATE the Bronze raw table for a new dataset.

    Args:
        table_name: e.g. "sales_data_raw"
        columns:    List of {"name": str, "snowflake_type": str} dicts.

    The table always includes metadata columns prefixed with _.
    """
    col_defs = ",\n    ".join(
        f"{col['name'].upper()} {col['snowflake_type']}" for col in columns
    )
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {DB}.{BRZ}.{table_name.upper()} (
        _INGESTION_ID    VARCHAR(36)  DEFAULT UUID_STRING(),
        _INGESTION_TS    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
        _SOURCE_FILE     VARCHAR(500),
        _PIPELINE_RUN_ID VARCHAR(36),
        _SCHEMA_VERSION  INTEGER,
        {col_defs}
    )
    """
    execute_statement(ddl)
    logger.info("Bronze table created", table=f"{BRZ}.{table_name}")


def bulk_insert_bronze(
    table_name: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
    *,
    source_file: str = "",
    pipeline_run_id: str = "",
    schema_version: int = 1,
) -> int:
    """
    Insert a batch of rows into a Bronze table using executemany.

    Returns:
        Number of rows inserted.
    """
    meta_cols = ["_SOURCE_FILE", "_PIPELINE_RUN_ID", "_SCHEMA_VERSION"]
    data_cols  = [c.upper() for c in columns]
    all_cols   = meta_cols + data_cols
    placeholders = ", ".join(["%s"] * len(all_cols))

    sql = (
        f"INSERT INTO {DB}.{BRZ}.{table_name.upper()} "
        f"({', '.join(all_cols)}) VALUES ({placeholders})"
    )

    enriched = [
        (source_file, pipeline_run_id, schema_version) + row
        for row in rows
    ]

    with get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.executemany(sql, enriched)
            affected = cur.rowcount or len(enriched)
            logger.info(
                "Bronze bulk insert complete",
                table=table_name,
                rows=affected,
            )
            return affected
        except Exception as exc:
            raise SnowflakeQueryError(
                message=f"Bulk insert into {table_name} failed",
                detail=str(exc),
            ) from exc
        finally:
            cur.close()


# ─── Connectivity Check ───────────────────────────────────────────────────────

def ping() -> bool:
    """Return True if Snowflake is reachable, False otherwise."""
    try:
        rows = execute_query("SELECT CURRENT_VERSION() AS ver")
        return bool(rows)
    except Exception:
        return False
