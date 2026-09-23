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


# ─── Quality Agent Persistence ────────────────────────────────────────────────

def insert_quality_run(
    *,
    dataset_id: str,
    pipeline_run_id: str,
    schema_version: int,
    total_rows: int,
    quality_score: float,
) -> str:
    """
    Insert a row into OBSERVABILITY.QUALITY_RUNS.

    Returns:
        The new quality run UUID (VARCHAR 36).
    """
    run_id = str(uuid.uuid4())
    now    = datetime.now(timezone.utc).isoformat()
    execute_statement(
        f"""
        INSERT INTO {DB}.{OBS}.QUALITY_RUNS
            (id, dataset_id, pipeline_run_id, schema_version,
             total_rows, quality_score, evaluated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (run_id, dataset_id or None, pipeline_run_id or None,
         schema_version, total_rows, quality_score, now),
    )
    logger.info(
        "Quality run recorded",
        quality_run_id=run_id,
        dataset_id=dataset_id,
        quality_score=quality_score,
    )
    return run_id


def insert_quality_metrics(
    quality_run_id: str,
    metric_rows: list[dict[str, Any]],
) -> int:
    """
    Batch-insert column-level metric rows into OBSERVABILITY.QUALITY_METRICS.

    Each dict in ``metric_rows`` must have keys:
        column_name, metric_type, metric_value, threshold (optional), passed (bool).

    Returns:
        Number of rows inserted.
    """
    if not metric_rows:
        return 0

    now = datetime.now(timezone.utc).isoformat()
    sql = (
        f"INSERT INTO {DB}.{OBS}.QUALITY_METRICS "
        f"(quality_run_id, column_name, metric_type, metric_value, threshold, passed, evaluated_at) "
        f"VALUES (%s, %s, %s, %s, %s, %s, %s)"
    )
    params = [
        (
            quality_run_id,
            row["column_name"],
            row["metric_type"],
            float(row["metric_value"]),
            float(row["threshold"]) if row.get("threshold") is not None else None,
            bool(row.get("passed", True)),
            now,
        )
        for row in metric_rows
    ]
    with get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.executemany(sql, params)
            affected = cur.rowcount or len(params)
            logger.info(
                "Quality metrics inserted",
                quality_run_id=quality_run_id,
                rows=affected,
            )
            return affected
        except Exception as exc:
            raise SnowflakeQueryError(
                message="Failed to insert quality metrics",
                detail=str(exc),
            ) from exc
        finally:
            cur.close()


def insert_data_quality_metrics(
    *,
    dataset_id: str,
    pipeline_run_id: str,
    silver_table: str,
    quality_score: float,
    total_rows: int,
    null_rate: float,
    duplicate_rate: float,
    type_mismatch_rate: float,
    range_fail_rate: float,
    freshness_hours: float | None,
    passed: bool,
    raw_metrics: dict[str, Any] | None = None,
) -> str:
    """
    Insert a denormalised quality summary row into OBSERVABILITY.DATA_QUALITY_METRICS.

    This table (created by migration 010) holds one flat row per agent run and
    is the primary source for dashboard trend charts.

    Returns:
        The new row UUID.
    """
    import json as _json

    row_id = str(uuid.uuid4())
    now    = datetime.now(timezone.utc).isoformat()
    raw_json = _json.dumps(raw_metrics, default=str) if raw_metrics else None

    execute_statement(
        f"""
        INSERT INTO {DB}.{OBS}.DATA_QUALITY_METRICS
            (id, dataset_id, pipeline_run_id, silver_table,
             quality_score, total_rows, passed,
             null_rate, duplicate_rate, type_mismatch_rate, range_fail_rate,
             freshness_hours, raw_metrics, evaluated_at)
        SELECT
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, PARSE_JSON(%s), %s
        """,
        (
            row_id,
            dataset_id or None,
            pipeline_run_id or None,
            silver_table or None,
            quality_score,
            total_rows,
            passed,
            null_rate,
            duplicate_rate,
            type_mismatch_rate,
            range_fail_rate,
            freshness_hours,
            raw_json,
            now,
        ),
    )
    logger.info(
        "DATA_QUALITY_METRICS row inserted",
        id=row_id,
        dataset_id=dataset_id,
        quality_score=quality_score,
        passed=passed,
    )
    return row_id


# ─── Drift Agent Persistence ───────────────────────────────────────────────────

def insert_drift_run(
    *,
    dataset_id:           str,
    pipeline_run_id:      str,
    overall_drift_score:  float,
    drift_detected:       bool,
) -> str:
    """
    Insert a row into OBSERVABILITY.DRIFT_RUNS.

    Returns the new run UUID.
    """
    run_id = str(uuid.uuid4())
    now    = datetime.now(timezone.utc).isoformat()
    execute_statement(
        f"""
        INSERT INTO {DB}.{OBS}.DRIFT_RUNS
            (id, dataset_id, pipeline_run_id,
             overall_drift_score, drift_detected, evaluated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            run_id,
            dataset_id or None,
            pipeline_run_id or None,
            round(overall_drift_score, 4),
            drift_detected,
            now,
        ),
    )
    logger.info(
        "Drift run recorded",
        drift_run_id=run_id,
        dataset_id=dataset_id,
        overall_drift_score=overall_drift_score,
        drift_detected=drift_detected,
    )
    return run_id


def insert_drift_metrics(
    drift_run_id: str,
    metric_rows:  list[dict[str, Any]],
) -> int:
    """
    Batch-insert per-column drift rows into OBSERVABILITY.DRIFT_METRICS.

    Each dict in ``metric_rows`` must have keys:
        column_name, drift_method, statistic_value, p_value (optional),
        psi_score (optional), drift_detected (bool), severity (str).

    Returns the number of rows inserted.
    """
    if not metric_rows:
        return 0

    now = datetime.now(timezone.utc).isoformat()
    sql = (
        f"INSERT INTO {DB}.{OBS}.DRIFT_METRICS "
        f"(drift_run_id, column_name, drift_method, statistic_value, "
        f"p_value, psi_score, drift_detected, severity, evaluated_at) "
        f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )
    params = [
        (
            drift_run_id,
            row["column_name"],
            row.get("drift_method", "ks_test+psi"),
            float(row.get("statistic_value", 0.0)),
            float(row["p_value"]) if row.get("p_value") is not None else None,
            float(row["psi_score"]) if row.get("psi_score") is not None else None,
            bool(row.get("drift_detected", False)),
            str(row.get("severity", "LOW")),
            now,
        )
        for row in metric_rows
    ]
    with get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.executemany(sql, params)
            affected = cur.rowcount or len(params)
            logger.info(
                "Drift metrics inserted",
                drift_run_id=drift_run_id,
                rows=affected,
            )
            return affected
        except Exception as exc:
            raise SnowflakeQueryError(
                message="Failed to insert drift metrics",
                detail=str(exc),
            ) from exc
        finally:
            cur.close()


def insert_drift_summary(
    *,
    dataset_id:       str,
    pipeline_run_id:  str,
    silver_table:     str,
    reference_table:  str,
    drift_score:      float,
    drift_detected:   bool,
    drift_severity:   str,
    drifted_columns:  int,
    total_columns:    int,
    drift_pct:        float,
    avg_psi:          float | None,
    avg_ks_stat:      float | None,
    raw_metrics:      dict[str, Any] | None = None,
) -> str:
    """
    Insert a denormalised drift summary row into OBSERVABILITY.DRIFT_SUMMARY.

    Uses ``INSERT INTO ... SELECT`` so PARSE_JSON() works with bind params
    (Snowflake rejects PARSE_JSON in a VALUES clause with bind parameters).

    Returns the new row UUID.
    """
    import json as _json

    row_id   = str(uuid.uuid4())
    now      = datetime.now(timezone.utc).isoformat()
    raw_json = _json.dumps(raw_metrics, default=str) if raw_metrics else None

    execute_statement(
        f"""
        INSERT INTO {DB}.{OBS}.DRIFT_SUMMARY
            (id, dataset_id, pipeline_run_id, silver_table, reference_table,
             drift_score, drift_detected, drift_severity,
             drifted_columns, total_columns, drift_pct,
             avg_psi, avg_ks_stat, raw_metrics, evaluated_at)
        SELECT
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s,
            %s, %s, PARSE_JSON(%s), %s
        """,
        (
            row_id,
            dataset_id or None,
            pipeline_run_id or None,
            silver_table or None,
            reference_table or None,
            round(drift_score, 4),
            drift_detected,
            drift_severity,
            drifted_columns,
            total_columns,
            round(drift_pct, 2),
            round(avg_psi, 6) if avg_psi is not None else None,
            round(avg_ks_stat, 6) if avg_ks_stat is not None else None,
            raw_json,
            now,
        ),
    )
    logger.info(
        "DRIFT_SUMMARY row inserted",
        id=row_id,
        dataset_id=dataset_id,
        drift_score=drift_score,
        drift_detected=drift_detected,
        drift_severity=drift_severity,
    )
    return row_id


# ─── Cost Agent Persistence ────────────────────────────────────────────────────

def insert_cost_record(
    *,
    dataset_id:      str,
    pipeline_run_id: str,
    warehouse_name:  str,
    credits_used:    float,
    credits_usd:     float,
    bytes_scanned:   int,
) -> str:
    """
    Insert a row into OBSERVABILITY.COST_RECORDS.

    Returns the new row UUID.
    """
    row_id = str(uuid.uuid4())
    now    = datetime.now(timezone.utc).isoformat()
    execute_statement(
        f"""
        INSERT INTO {DB}.{OBS}.COST_RECORDS
            (id, dataset_id, pipeline_run_id, warehouse_name,
             query_type, credits_used, credits_usd, bytes_scanned, recorded_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            row_id,
            dataset_id or None,
            pipeline_run_id or None,
            warehouse_name or None,
            "PIPELINE_RUN",
            round(credits_used, 8),
            round(credits_usd, 4),
            bytes_scanned,
            now,
        ),
    )
    logger.info(
        "Cost record inserted",
        id=row_id,
        dataset_id=dataset_id,
        credits_used=credits_used,
        credits_usd=credits_usd,
    )
    return row_id


def insert_cost_forecast(
    *,
    dataset_id:           str,
    forecast_credits_30d: float,
    lower_bound:          float,
    upper_bound:          float,
    daily_breakdown:      list[dict],
) -> int:
    """
    Insert one COST_FORECASTS row for the 30-day aggregate prediction.

    Returns 1 if inserted, 0 on empty data.
    """
    from datetime import date, timedelta

    today = date.today()
    forecast_date = today + timedelta(days=30)
    now = datetime.now(timezone.utc).isoformat()

    execute_statement(
        f"""
        INSERT INTO {DB}.{OBS}.COST_FORECASTS
            (id, dataset_id, forecast_date,
             predicted_credits, lower_bound, upper_bound, model_used, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            str(uuid.uuid4()),
            dataset_id or None,
            str(forecast_date),
            round(forecast_credits_30d, 4),
            round(lower_bound, 4),
            round(upper_bound, 4),
            "linear_extrapolation",
            now,
        ),
    )
    logger.info(
        "Cost forecast inserted",
        dataset_id=dataset_id,
        forecast_date=str(forecast_date),
        predicted_credits=forecast_credits_30d,
    )
    return 1


def insert_cost_summary(
    *,
    dataset_id:           str,
    pipeline_run_id:      str,
    warehouse_name:       str,
    lookback_days:        int,
    total_credits:        float,
    total_usd:            float,
    daily_avg_credits:    float,
    cost_per_run:         float,
    storage_gb:           float,
    storage_cost_usd:     float,
    forecast_credits_30d: float,
    forecast_usd_30d:     float,
    cost_score:           float,
    cost_severity:        str,
    pipeline_run_count:   int,
) -> str:
    """
    Insert a denormalised cost summary row into OBSERVABILITY.COST_SUMMARY.

    Returns the new row UUID.
    """
    row_id = str(uuid.uuid4())
    now    = datetime.now(timezone.utc).isoformat()

    execute_statement(
        f"""
        INSERT INTO {DB}.{OBS}.COST_SUMMARY
            (id, dataset_id, pipeline_run_id, warehouse_name, lookback_days,
             total_credits, total_usd, daily_avg_credits, cost_per_run,
             storage_gb, storage_cost_usd,
             forecast_credits_30d, forecast_usd_30d,
             cost_score, cost_severity, pipeline_run_count, evaluated_at)
        VALUES (%s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s, %s)
        """,
        (
            row_id,
            dataset_id or None,
            pipeline_run_id or None,
            warehouse_name or None,
            lookback_days,
            round(total_credits, 8),
            round(total_usd, 4),
            round(daily_avg_credits, 8),
            round(cost_per_run, 8),
            round(storage_gb, 4),
            round(storage_cost_usd, 4),
            round(forecast_credits_30d, 4),
            round(forecast_usd_30d, 4),
            round(cost_score, 2),
            cost_severity,
            pipeline_run_count,
            now,
        ),
    )
    logger.info(
        "COST_SUMMARY row inserted",
        id=row_id,
        dataset_id=dataset_id,
        cost_score=cost_score,
        cost_severity=cost_severity,
        total_credits=total_credits,
    )
    return row_id


# ─── Self-Healing Agent Persistence ───────────────────────────────────────────

def insert_heal_run(
    *,
    dataset_id:       str,
    pipeline_run_id:  str,
    failure_type:     str,
    failure_reason:   str,
    failed_task:      str,
    healing_strategy: str,
    recovery_status:  str,
    retry_count:      int,
    mttr_seconds:     float,
) -> str:
    """
    Insert a row into OBSERVABILITY.HEAL_RUNS.

    Returns the new run UUID.
    """
    run_id = str(uuid.uuid4())
    now    = datetime.now(timezone.utc).isoformat()
    execute_statement(
        f"""
        INSERT INTO {DB}.{OBS}.HEAL_RUNS
            (id, dataset_id, pipeline_run_id,
             failure_type, failure_reason, failed_task,
             healing_strategy, recovery_status,
             retry_count, mttr_seconds,
             failure_detected_at, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            run_id,
            dataset_id or None,
            pipeline_run_id or None,
            failure_type,
            (failure_reason or "")[:2000],
            failed_task or None,
            healing_strategy,
            recovery_status,
            retry_count,
            round(mttr_seconds, 4),
            now,
            now,
        ),
    )
    logger.info(
        "Heal run recorded",
        heal_run_id=run_id,
        failure_type=failure_type,
        healing_strategy=healing_strategy,
        recovery_status=recovery_status,
        mttr_seconds=mttr_seconds,
    )
    return run_id


def insert_heal_actions(
    heal_run_id: str,
    action_rows: list[dict[str, Any]],
) -> int:
    """
    Batch-insert action rows into OBSERVABILITY.HEAL_ACTIONS.

    Each dict must have: action_type, action_detail, success.

    Returns the number of rows inserted.
    """
    if not action_rows:
        return 0

    now = datetime.now(timezone.utc).isoformat()
    sql = (
        f"INSERT INTO {DB}.{OBS}.HEAL_ACTIONS "
        f"(id, heal_run_id, action_type, action_detail, success, executed_at) "
        f"VALUES (%s, %s, %s, %s, %s, %s)"
    )
    params = [
        (
            str(uuid.uuid4()),
            heal_run_id,
            row.get("action_type", "UNKNOWN"),
            (row.get("action_detail") or "")[:4000],
            bool(row.get("success", False)),
            now,
        )
        for row in action_rows
    ]
    with get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.executemany(sql, params)
            affected = cur.rowcount or len(params)
            logger.info(
                "Heal actions inserted",
                heal_run_id=heal_run_id,
                rows=affected,
            )
            return affected
        except Exception as exc:
            raise SnowflakeQueryError(
                message="Failed to insert heal actions",
                detail=str(exc),
            ) from exc
        finally:
            cur.close()


def insert_heal_summary(
    *,
    dataset_id:        str,
    pipeline_run_id:   str,
    failure_type:      str,
    failure_reason:    str,
    healing_strategy:  str,
    recovery_status:   str,
    retry_count:       int,
    mttr_seconds:      float,
    actions_taken:     int,
    actions_succeeded: int,
    quarantined:       bool,
) -> str:
    """
    Insert a denormalised heal summary row into OBSERVABILITY.HEAL_SUMMARY.

    Returns the new row UUID.
    """
    row_id = str(uuid.uuid4())
    now    = datetime.now(timezone.utc).isoformat()

    execute_statement(
        f"""
        INSERT INTO {DB}.{OBS}.HEAL_SUMMARY
            (id, dataset_id, pipeline_run_id,
             failure_type, failure_reason,
             healing_strategy, recovery_status,
             retry_count, mttr_seconds,
             actions_taken, actions_succeeded,
             quarantined, evaluated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            row_id,
            dataset_id or None,
            pipeline_run_id or None,
            failure_type,
            (failure_reason or "")[:2000],
            healing_strategy,
            recovery_status,
            retry_count,
            round(mttr_seconds, 4),
            actions_taken,
            actions_succeeded,
            quarantined,
            now,
        ),
    )
    logger.info(
        "HEAL_SUMMARY row inserted",
        id=row_id,
        dataset_id=dataset_id,
        failure_type=failure_type,
        recovery_status=recovery_status,
        mttr_seconds=mttr_seconds,
    )
    return row_id
