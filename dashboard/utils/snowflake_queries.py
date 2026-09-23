"""
dashboard/utils/snowflake_queries.py
======================================
Read-only Snowflake query helpers for the Phase 5 dashboard.

All public functions:
  - Are decorated with ``@st.cache_data(ttl=60)`` for one-minute caching.
  - Return typed dicts / lists with sensible defaults when Snowflake is
    unavailable or the table has no rows yet.
  - Never raise exceptions to callers; failures are printed to stderr so the
    Streamlit terminal shows them without crashing the page.

Import pattern::

    from dashboard.utils.snowflake_queries import (
        get_pipeline_run_stats,
        get_latest_quality_score,
        get_latest_drift_severity,
        get_latest_cost_score,
        get_self_healing_success_rate,
        get_overall_health,
        get_quality_trend,
    )

Snowflake tables read
---------------------
  - OBSERVABILITY.PIPELINE_RUNS
  - OBSERVABILITY.PIPELINE_TASKS
  - OBSERVABILITY.DATASET_LINEAGE
  - OBSERVABILITY.DATA_QUALITY_METRICS
  - OBSERVABILITY.DRIFT_SUMMARY
  - OBSERVABILITY.COST_SUMMARY
  - OBSERVABILITY.HEAL_SUMMARY
"""

from __future__ import annotations

import sys
from typing import Any

import streamlit as st

# ── Database / schema shorthands ──────────────────────────────────────────────

try:
    from config.settings import snowflake_settings as _sf
    _DB  = _sf.database    # e.g. OBSERVABILITY_DB
    _OBS = _sf.schema_     # e.g. OBSERVABILITY
except Exception as _e:
    print(f"[snowflake_queries] Could not load snowflake_settings: {_e}", file=sys.stderr)
    _DB  = "OBSERVABILITY_DB"
    _OBS = "OBSERVABILITY"


# ── Internal helpers ──────────────────────────────────────────────────────────

def _run(sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    """
    Execute a SELECT against Snowflake and return rows as dicts.

    Calls ``initialise_pool()`` on every invocation (idempotent — the pool
    implementation guards against double-initialisation with a threading lock).

    Returns an empty list on any error so callers always receive an iterable.
    """
    try:
        from backend.core.database import execute_query, initialise_pool
        try:
            initialise_pool()
        except Exception:
            # Pool may already be initialised; ignore duplicate-init errors.
            pass
        return execute_query(sql, params)
    except Exception as exc:
        print(f"[snowflake_queries] query failed — {exc}", file=sys.stderr)
        return []


def _f(row: dict[str, Any], key: str) -> float | None:
    """Safely coerce a Snowflake row value to float (None on failure)."""
    v = row.get(key)
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ── Pipeline Runs ─────────────────────────────────────────────────────────────

@st.cache_data(ttl=60, show_spinner=False)
def get_pipeline_run_stats() -> dict[str, int]:
    """
    Return aggregate counts of pipeline runs by status.

    Returns:
        dict with keys: ``total``, ``success``, ``failed``, ``running``.
        All values default to 0 when Snowflake is unavailable.
    """
    rows = _run(
        f"""
        SELECT
            COUNT(*)                                              AS total,
            COUNT_IF(LOWER(status) = 'success')                  AS success,
            COUNT_IF(LOWER(status) IN ('failed', 'error'))       AS failed,
            COUNT_IF(LOWER(status) = 'running')                  AS running
        FROM {_DB}.{_OBS}.PIPELINE_RUNS
        """
    )
    if not rows:
        return {"total": 0, "success": 0, "failed": 0, "running": 0}
    r = rows[0]
    return {
        "total":   int(r.get("TOTAL",   0) or 0),
        "success": int(r.get("SUCCESS", 0) or 0),
        "failed":  int(r.get("FAILED",  0) or 0),
        "running": int(r.get("RUNNING", 0) or 0),
    }


@st.cache_data(ttl=60, show_spinner=False)
def get_recent_pipeline_runs(limit: int = 10) -> list[dict[str, Any]]:
    """
    Return the *limit* most recent pipeline run rows.

    Columns returned: ``dataset_id``, ``dag_id``, ``run_id``, ``status``,
    ``rows_ingested``, ``started_at``, ``completed_at``.
    """
    return _run(
        f"""
        SELECT  dataset_id, dag_id, run_id, status,
                rows_ingested, started_at, completed_at
        FROM    {_DB}.{_OBS}.PIPELINE_RUNS
        ORDER   BY started_at DESC NULLS LAST
        LIMIT   {int(limit)}
        """
    )


# ── Data Quality ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=60, show_spinner=False)
def get_latest_quality_score() -> dict[str, Any]:
    """
    Return the single most-recent row from DATA_QUALITY_METRICS.

    Keys: ``score``, ``passed``, ``null_rate``, ``duplicate_rate``,
          ``type_mismatch_rate``, ``evaluated_at``.
    All values are ``None`` when no data exists.
    """
    rows = _run(
        f"""
        SELECT  quality_score, passed, null_rate, duplicate_rate,
                type_mismatch_rate, evaluated_at
        FROM    {_DB}.{_OBS}.DATA_QUALITY_METRICS
        ORDER   BY evaluated_at DESC NULLS LAST
        LIMIT   1
        """
    )
    _empty: dict[str, Any] = {
        "score": None, "passed": None, "null_rate": None,
        "duplicate_rate": None, "type_mismatch_rate": None,
        "evaluated_at": None,
    }
    if not rows:
        return _empty
    r = rows[0]
    return {
        "score":              _f(r, "QUALITY_SCORE"),
        "passed":             r.get("PASSED"),
        "null_rate":          _f(r, "NULL_RATE"),
        "duplicate_rate":     _f(r, "DUPLICATE_RATE"),
        "type_mismatch_rate": _f(r, "TYPE_MISMATCH_RATE"),
        "evaluated_at":       r.get("EVALUATED_AT"),
    }


@st.cache_data(ttl=60, show_spinner=False)
def get_quality_trend(days: int = 7) -> list[dict[str, Any]]:
    """
    Return daily average quality scores for the last *days* days.

    Each item in the returned list has keys: ``day`` (str), ``avg_score`` (float).
    Returns an empty list when there is no data or Snowflake is unavailable.
    """
    rows = _run(
        f"""
        SELECT
            DATE(evaluated_at)   AS day,
            AVG(quality_score)   AS avg_score
        FROM    {_DB}.{_OBS}.DATA_QUALITY_METRICS
        WHERE   evaluated_at >= DATEADD('day', -{int(days)}, CURRENT_TIMESTAMP())
        GROUP   BY DATE(evaluated_at)
        ORDER   BY day ASC
        """
    )
    return [
        {"day": str(r.get("DAY", "")), "avg_score": _f(r, "AVG_SCORE") or 0.0}
        for r in rows
    ]


# ── Drift Monitoring ──────────────────────────────────────────────────────────

@st.cache_data(ttl=60, show_spinner=False)
def get_latest_drift_severity() -> dict[str, Any]:
    """
    Return the single most-recent row from DRIFT_SUMMARY.

    Keys: ``severity``, ``score``, ``detected``, ``drifted_columns``,
          ``total_columns``, ``drift_pct``, ``evaluated_at``.
    All values are ``None`` when no data exists.
    """
    rows = _run(
        f"""
        SELECT  drift_score, drift_severity, drift_detected,
                drifted_columns, total_columns, drift_pct, evaluated_at
        FROM    {_DB}.{_OBS}.DRIFT_SUMMARY
        ORDER   BY evaluated_at DESC NULLS LAST
        LIMIT   1
        """
    )
    _empty: dict[str, Any] = {
        "severity": None, "score": None, "detected": None,
        "drifted_columns": None, "total_columns": None,
        "drift_pct": None, "evaluated_at": None,
    }
    if not rows:
        return _empty
    r = rows[0]
    return {
        "severity":       r.get("DRIFT_SEVERITY"),
        "score":          _f(r, "DRIFT_SCORE"),
        "detected":       r.get("DRIFT_DETECTED"),
        "drifted_columns":r.get("DRIFTED_COLUMNS"),
        "total_columns":  r.get("TOTAL_COLUMNS"),
        "drift_pct":      _f(r, "DRIFT_PCT"),
        "evaluated_at":   r.get("EVALUATED_AT"),
    }


# ── Cost Analytics ────────────────────────────────────────────────────────────

@st.cache_data(ttl=60, show_spinner=False)
def get_latest_cost_score() -> dict[str, Any]:
    """
    Return the single most-recent row from COST_SUMMARY.

    Keys: ``score``, ``severity``, ``total_usd``, ``total_credits``,
          ``forecast_usd_30d``, ``daily_avg_credits``, ``evaluated_at``.
    All values are ``None`` when no data exists.
    """
    rows = _run(
        f"""
        SELECT  cost_score, cost_severity, total_credits, total_usd,
                forecast_credits_30d, forecast_usd_30d,
                daily_avg_credits, evaluated_at
        FROM    {_DB}.{_OBS}.COST_SUMMARY
        ORDER   BY evaluated_at DESC NULLS LAST
        LIMIT   1
        """
    )
    _empty: dict[str, Any] = {
        "score": None, "severity": None, "total_usd": None,
        "total_credits": None, "forecast_usd_30d": None,
        "daily_avg_credits": None, "evaluated_at": None,
    }
    if not rows:
        return _empty
    r = rows[0]
    return {
        "score":             _f(r, "COST_SCORE"),
        "severity":          r.get("COST_SEVERITY"),
        "total_usd":         _f(r, "TOTAL_USD"),
        "total_credits":     _f(r, "TOTAL_CREDITS"),
        "forecast_usd_30d":  _f(r, "FORECAST_USD_30D"),
        "daily_avg_credits": _f(r, "DAILY_AVG_CREDITS"),
        "evaluated_at":      r.get("EVALUATED_AT"),
    }


# ── Self-Healing ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=60, show_spinner=False)
def get_self_healing_success_rate() -> dict[str, Any]:
    """
    Return aggregate self-healing statistics from HEAL_SUMMARY.

    Keys: ``total``, ``recovered``, ``quarantined``, ``success_rate``
          (0–100 float or None), ``avg_mttr`` (seconds or None),
          ``evaluated_at`` (most recent timestamp or None).
    """
    rows = _run(
        f"""
        SELECT
            COUNT(*)                                               AS total,
            COUNT_IF(UPPER(recovery_status) = 'RECOVERED')        AS recovered,
            COUNT_IF(UPPER(recovery_status) = 'QUARANTINED')      AS quarantined,
            AVG(mttr_seconds)                                      AS avg_mttr,
            MAX(evaluated_at)                                      AS evaluated_at
        FROM {_DB}.{_OBS}.HEAL_SUMMARY
        """
    )
    if not rows:
        return {
            "total": 0, "recovered": 0, "quarantined": 0,
            "success_rate": None, "avg_mttr": None, "evaluated_at": None,
        }
    r = rows[0]
    total     = int(r.get("TOTAL",       0) or 0)
    recovered = int(r.get("RECOVERED",   0) or 0)
    return {
        "total":        total,
        "recovered":    recovered,
        "quarantined":  int(r.get("QUARANTINED", 0) or 0),
        "success_rate": round(recovered / total * 100, 1) if total > 0 else None,
        "avg_mttr":     _f(r, "AVG_MTTR"),
        "evaluated_at": r.get("EVALUATED_AT"),
    }


# ── Overall Platform Health ────────────────────────────────────────────────────

@st.cache_data(ttl=60, show_spinner=False)
def get_overall_health() -> dict[str, Any]:
    """
    Compute a composite 0–100 platform health score.

    Weights
    -------
    * Quality score  — 40 %
    * Cost score     — 20 %
    * Heal success   — 20 %
    * Drift bonus    — 20 %

    Drift bonus mapping::

        No drift detected → 100
        LOW severity      →  70
        MEDIUM severity   →  40
        HIGH severity     →  10
        Unknown (None)    →  50  (neutral)

    Returns a dict with keys: ``score`` (float), ``label`` (str),
    ``color`` (hex str), ``any_data`` (bool indicating whether at least one
    agent has produced data).
    """
    q = get_latest_quality_score()
    c = get_latest_cost_score()
    h = get_self_healing_success_rate()
    d = get_latest_drift_severity()

    quality_score = q["score"]        or 0.0
    cost_score    = c["score"]        or 0.0
    heal_rate     = h["success_rate"] or 0.0
    drift_sev     = (d["severity"] or "").upper()
    drift_detected= d["detected"]

    # Drift bonus
    if drift_detected is None:
        drift_bonus = 50.0
    elif not drift_detected:
        drift_bonus = 100.0
    else:
        drift_bonus = {"LOW": 70.0, "MEDIUM": 40.0, "HIGH": 10.0}.get(drift_sev, 30.0)

    composite = (
        quality_score * 0.40
        + cost_score  * 0.20
        + heal_rate   * 0.20
        + drift_bonus * 0.20
    )
    composite = max(0.0, min(100.0, composite))

    if composite >= 85:
        label, color = "Excellent", "#22c55e"
    elif composite >= 70:
        label, color = "Good",      "#84cc16"
    elif composite >= 50:
        label, color = "Fair",      "#f59e0b"
    else:
        label, color = "At Risk",   "#ef4444"

    any_data = any(
        x is not None
        for x in [q["score"], c["score"], h["success_rate"], d["severity"]]
    )

    return {
        "score":    round(composite, 1),
        "label":    label,
        "color":    color,
        "any_data": any_data,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Page 2 — Pipeline Monitoring queries
# Tables: PIPELINE_RUNS · PIPELINE_TASKS · DATASET_LINEAGE
# ═══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=60, show_spinner=False)
def get_pipeline_kpis() -> dict[str, Any]:
    """
    Return aggregate pipeline KPIs for the header metric cards.

    Keys: ``total``, ``success``, ``failed``, ``running``,
          ``avg_duration_sec`` (float or None),
          ``success_rate`` (float 0-100 or None).
    """
    rows = _run(
        f"""
        SELECT
            COUNT(*)                                                      AS total,
            COUNT_IF(LOWER(status) = 'success')                           AS success,
            COUNT_IF(LOWER(status) IN ('failed', 'error'))                AS failed,
            COUNT_IF(LOWER(status) = 'running')                           AS running,
            AVG(
                DATEDIFF('second', started_at, NVL(completed_at, CURRENT_TIMESTAMP()))
            )                                                             AS avg_duration_sec
        FROM {_DB}.{_OBS}.PIPELINE_RUNS
        """
    )
    if not rows:
        return {
            "total": 0, "success": 0, "failed": 0, "running": 0,
            "avg_duration_sec": None, "success_rate": None,
        }
    r = rows[0]
    total   = int(r.get("TOTAL",   0) or 0)
    success = int(r.get("SUCCESS", 0) or 0)
    return {
        "total":            total,
        "success":          success,
        "failed":           int(r.get("FAILED",  0) or 0),
        "running":          int(r.get("RUNNING", 0) or 0),
        "avg_duration_sec": _f(r, "AVG_DURATION_SEC"),
        "success_rate":     round(success / total * 100, 1) if total > 0 else None,
    }


@st.cache_data(ttl=60, show_spinner=False)
def get_recent_dag_runs(limit: int = 15) -> list[dict[str, Any]]:
    """
    Return the *limit* most recent pipeline run rows, newest first.

    Columns: ``dag_id``, ``run_id``, ``status``, ``rows_ingested``,
             ``rows_rejected``, ``started_at``, ``completed_at``,
             ``duration_sec``.
    """
    return _run(
        f"""
        SELECT
            dag_id,
            run_id,
            status,
            rows_ingested,
            rows_rejected,
            started_at,
            completed_at,
            DATEDIFF(
                'second',
                started_at,
                NVL(completed_at, CURRENT_TIMESTAMP())
            ) AS duration_sec
        FROM   {_DB}.{_OBS}.PIPELINE_RUNS
        ORDER  BY started_at DESC NULLS LAST
        LIMIT  {int(limit)}
        """
    )


@st.cache_data(ttl=60, show_spinner=False)
def get_pipeline_duration_trend(days: int = 14) -> list[dict[str, Any]]:
    """
    Return daily average / min / max pipeline duration for trend chart.

    Each item: ``day`` (str), ``avg_sec``, ``min_sec``, ``max_sec``.
    Covers only *completed* runs (success or failed).
    """
    rows = _run(
        f"""
        SELECT
            DATE(started_at)                                       AS day,
            AVG(DATEDIFF('second', started_at, completed_at))      AS avg_sec,
            MIN(DATEDIFF('second', started_at, completed_at))      AS min_sec,
            MAX(DATEDIFF('second', started_at, completed_at))      AS max_sec
        FROM   {_DB}.{_OBS}.PIPELINE_RUNS
        WHERE  completed_at IS NOT NULL
          AND  started_at  >= DATEADD('day', -{int(days)}, CURRENT_TIMESTAMP())
        GROUP  BY DATE(started_at)
        ORDER  BY day ASC
        """
    )
    return [
        {
            "day":     str(r.get("DAY", "")),
            "avg_sec": _f(r, "AVG_SEC") or 0.0,
            "min_sec": _f(r, "MIN_SEC") or 0.0,
            "max_sec": _f(r, "MAX_SEC") or 0.0,
        }
        for r in rows
    ]


@st.cache_data(ttl=60, show_spinner=False)
def get_pipeline_task_timeline(limit_runs: int = 5) -> list[dict[str, Any]]:
    """
    Return task-level rows for the last *limit_runs* pipeline runs.

    Used to build a Gantt / task timeline chart.  Joins PIPELINE_TASKS with
    PIPELINE_RUNS so each row carries the parent ``dag_id`` and ``run_id``.

    Each item: ``run_id``, ``dag_id``, ``task_name``, ``task_type``,
               ``status``, ``started_at``, ``completed_at``,
               ``duration_sec``, ``retry_count``.
    """
    return _run(
        f"""
        SELECT
            t.run_id,
            r.dag_id,
            t.task_name,
            t.task_type,
            t.status,
            t.started_at,
            t.completed_at,
            NVL(t.duration_sec,
                DATEDIFF('second', t.started_at,
                         NVL(t.completed_at, CURRENT_TIMESTAMP()))
            )                           AS duration_sec,
            t.retry_count
        FROM   {_DB}.{_OBS}.PIPELINE_TASKS  t
        JOIN   {_DB}.{_OBS}.PIPELINE_RUNS   r
               ON r.id = t.run_id
        WHERE  r.id IN (
            SELECT id FROM {_DB}.{_OBS}.PIPELINE_RUNS
            ORDER BY started_at DESC NULLS LAST
            LIMIT  {int(limit_runs)}
        )
        ORDER  BY r.started_at DESC, t.started_at ASC NULLS LAST
        """
    )


@st.cache_data(ttl=60, show_spinner=False)
def get_lineage_summary(days: int = 30) -> list[dict[str, Any]]:
    """
    Return aggregated Bronze → Silver → Gold row-flow for the last *days* days.

    Used for the Sankey/flow diagram.  Each item: ``source_layer``,
    ``target_layer``, ``total_rows_in``, ``total_rows_out``,
    ``total_rows_rejected``, ``run_count``.
    """
    rows = _run(
        f"""
        SELECT
            source_layer,
            target_layer,
            SUM(rows_in)       AS total_rows_in,
            SUM(rows_out)      AS total_rows_out,
            SUM(rows_rejected) AS total_rows_rejected,
            COUNT(*)           AS run_count
        FROM   {_DB}.{_OBS}.DATASET_LINEAGE
        WHERE  event_at >= DATEADD('day', -{int(days)}, CURRENT_TIMESTAMP())
        GROUP  BY source_layer, target_layer
        ORDER  BY source_layer, target_layer
        """
    )
    return [
        {
            "source_layer":       r.get("SOURCE_LAYER", ""),
            "target_layer":       r.get("TARGET_LAYER", ""),
            "total_rows_in":      int(r.get("TOTAL_ROWS_IN",       0) or 0),
            "total_rows_out":     int(r.get("TOTAL_ROWS_OUT",      0) or 0),
            "total_rows_rejected":int(r.get("TOTAL_ROWS_REJECTED", 0) or 0),
            "run_count":          int(r.get("RUN_COUNT",           0) or 0),
        }
        for r in rows
    ]


@st.cache_data(ttl=60, show_spinner=False)
def get_pipeline_layer_stats() -> dict[str, Any]:
    """
    Return cumulative row counts per medallion layer from DATASET_LINEAGE.

    Keys: ``bronze_rows``, ``silver_rows``, ``gold_rows``,
          ``total_rejected``, ``latest_event_at``.
    These are *total-ever* values suitable for the Bronze→Silver→Gold cards.
    """
    rows = _run(
        f"""
        SELECT
            SUM(CASE WHEN UPPER(source_layer) = 'BRONZE'
                     THEN rows_in   ELSE 0 END) AS bronze_rows,
            SUM(CASE WHEN UPPER(source_layer) = 'BRONZE'
                     THEN rows_out  ELSE 0 END) AS silver_rows,
            SUM(CASE WHEN UPPER(source_layer) = 'SILVER'
                     THEN rows_out  ELSE 0 END) AS gold_rows,
            SUM(rows_rejected)                  AS total_rejected,
            MAX(event_at)                       AS latest_event_at
        FROM {_DB}.{_OBS}.DATASET_LINEAGE
        """
    )
    if not rows:
        return {
            "bronze_rows": None, "silver_rows": None, "gold_rows": None,
            "total_rejected": None, "latest_event_at": None,
        }
    r = rows[0]
    return {
        "bronze_rows":     int(r.get("BRONZE_ROWS",    0) or 0) or None,
        "silver_rows":     int(r.get("SILVER_ROWS",    0) or 0) or None,
        "gold_rows":       int(r.get("GOLD_ROWS",      0) or 0) or None,
        "total_rejected":  int(r.get("TOTAL_REJECTED", 0) or 0),
        "latest_event_at": r.get("LATEST_EVENT_AT"),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Page 3 — Data Quality Dashboard queries
# Tables: DATA_QUALITY_METRICS · QUALITY_RUNS · QUALITY_METRICS
# ═══════════════════════════════════════════════════════════════════════════════


@st.cache_data(ttl=60, show_spinner=False)
def get_quality_datasets() -> list[str]:
    """
    Return distinct dataset labels from DATA_QUALITY_METRICS for the filter widget.

    Labels are derived from ``silver_table`` first, falling back to ``dataset_id``
    when ``silver_table`` is NULL.  Returns an empty list when no data exists.
    """
    rows = _run(
        f"""
        SELECT DISTINCT
            COALESCE(silver_table, dataset_id, 'unknown') AS dataset_label
        FROM   {_DB}.{_OBS}.DATA_QUALITY_METRICS
        WHERE  silver_table IS NOT NULL OR dataset_id IS NOT NULL
        ORDER  BY dataset_label
        """
    )
    return [str(r.get("DATASET_LABEL", "")) for r in rows if r.get("DATASET_LABEL")]


@st.cache_data(ttl=60, show_spinner=False)
def get_quality_kpis(dataset_id: str | None = None) -> dict[str, Any]:
    """
    Return aggregate + latest-row quality KPIs from DATA_QUALITY_METRICS.

    Keys: ``total_runs``, ``passed_runs``, ``failed_runs``, ``pass_rate``,
          ``latest_score``, ``latest_total_rows``, ``latest_null_rate``,
          ``latest_dup_rate``, ``latest_type_rate``, ``latest_range_rate``,
          ``latest_freshness_hours``, ``latest_evaluated_at``.

    All numeric values are ``None`` when no rows exist.

    Args:
        dataset_id: If provided, restrict to rows where
                    ``COALESCE(silver_table, dataset_id) = dataset_id``.
    """
    _ds = (
        f"AND COALESCE(silver_table, dataset_id) = '{dataset_id}'"
        if dataset_id else ""
    )
    rows = _run(
        f"""
        SELECT
            COUNT(*)                                         AS total_runs,
            COUNT_IF(passed = TRUE)                          AS passed_runs,
            COUNT_IF(passed = FALSE)                         AS failed_runs,
            AVG(CASE WHEN passed = TRUE
                     THEN 100.0 ELSE 0.0 END)               AS pass_rate,
            MAX_BY(quality_score,       evaluated_at)        AS latest_score,
            MAX_BY(total_rows,          evaluated_at)        AS latest_total_rows,
            MAX_BY(null_rate,           evaluated_at)        AS latest_null_rate,
            MAX_BY(duplicate_rate,      evaluated_at)        AS latest_dup_rate,
            MAX_BY(type_mismatch_rate,  evaluated_at)        AS latest_type_rate,
            MAX_BY(range_fail_rate,     evaluated_at)        AS latest_range_rate,
            MAX_BY(freshness_hours,     evaluated_at)        AS latest_freshness_hours,
            MAX(evaluated_at)                                AS latest_evaluated_at
        FROM {_DB}.{_OBS}.DATA_QUALITY_METRICS
        WHERE 1=1 {_ds}
        """
    )
    _empty: dict[str, Any] = {
        "total_runs": 0, "passed_runs": 0, "failed_runs": 0,
        "pass_rate": None, "latest_score": None, "latest_total_rows": None,
        "latest_null_rate": None, "latest_dup_rate": None,
        "latest_type_rate": None, "latest_range_rate": None,
        "latest_freshness_hours": None, "latest_evaluated_at": None,
    }
    if not rows:
        return _empty
    r = rows[0]
    total = int(r.get("TOTAL_RUNS", 0) or 0)
    if total == 0:
        return _empty
    return {
        "total_runs":             total,
        "passed_runs":            int(r.get("PASSED_RUNS",  0) or 0),
        "failed_runs":            int(r.get("FAILED_RUNS",  0) or 0),
        "pass_rate":              _f(r, "PASS_RATE"),
        "latest_score":           _f(r, "LATEST_SCORE"),
        "latest_total_rows":      int(r.get("LATEST_TOTAL_ROWS", 0) or 0) or None,
        "latest_null_rate":       _f(r, "LATEST_NULL_RATE"),
        "latest_dup_rate":        _f(r, "LATEST_DUP_RATE"),
        "latest_type_rate":       _f(r, "LATEST_TYPE_RATE"),
        "latest_range_rate":      _f(r, "LATEST_RANGE_RATE"),
        "latest_freshness_hours": _f(r, "LATEST_FRESHNESS_HOURS"),
        "latest_evaluated_at":    r.get("LATEST_EVALUATED_AT"),
    }


@st.cache_data(ttl=60, show_spinner=False)
def get_quality_score_trend(
    days: int = 30,
    dataset_id: str | None = None,
) -> list[dict[str, Any]]:
    """
    Return daily avg / min / max quality score for the trend area chart.

    Each item: ``day`` (str), ``avg_score``, ``min_score``, ``max_score``,
               ``run_count`` (int).  Covers the last *days* calendar days.

    Args:
        days: Look-back window in calendar days (default 30).
        dataset_id: Optional filter on ``COALESCE(silver_table, dataset_id)``.
    """
    _ds = (
        f"AND COALESCE(silver_table, dataset_id) = '{dataset_id}'"
        if dataset_id else ""
    )
    rows = _run(
        f"""
        SELECT
            DATE(evaluated_at)    AS day,
            AVG(quality_score)    AS avg_score,
            MIN(quality_score)    AS min_score,
            MAX(quality_score)    AS max_score,
            COUNT(*)              AS run_count
        FROM   {_DB}.{_OBS}.DATA_QUALITY_METRICS
        WHERE  evaluated_at >= DATEADD('day', -{int(days)}, CURRENT_TIMESTAMP())
               {_ds}
        GROUP  BY DATE(evaluated_at)
        ORDER  BY day ASC
        """
    )
    return [
        {
            "day":       str(r.get("DAY", "")),
            "avg_score": _f(r, "AVG_SCORE") or 0.0,
            "min_score": _f(r, "MIN_SCORE") or 0.0,
            "max_score": _f(r, "MAX_SCORE") or 0.0,
            "run_count": int(r.get("RUN_COUNT", 0) or 0),
        }
        for r in rows
    ]


@st.cache_data(ttl=60, show_spinner=False)
def get_quality_run_history(
    limit: int = 20,
    dataset_id: str | None = None,
) -> list[dict[str, Any]]:
    """
    Return the most recent quality evaluation rows for the run-history table.

    Queries ``DATA_QUALITY_METRICS`` directly (the denormalised agent-output
    table) to avoid joins.

    Each item: ``id``, ``dataset_label``, ``quality_score``, ``total_rows``,
               ``passed``, ``null_rate``, ``duplicate_rate``,
               ``type_mismatch_rate``, ``range_fail_rate``,
               ``freshness_hours``, ``evaluated_at``.

    Args:
        limit: Maximum rows to return (default 20).
        dataset_id: Optional filter on ``COALESCE(silver_table, dataset_id)``.
    """
    _ds = (
        f"AND COALESCE(silver_table, dataset_id) = '{dataset_id}'"
        if dataset_id else ""
    )
    return _run(
        f"""
        SELECT
            id,
            COALESCE(silver_table, dataset_id, 'unknown') AS dataset_label,
            quality_score,
            total_rows,
            passed,
            null_rate,
            duplicate_rate,
            type_mismatch_rate,
            range_fail_rate,
            freshness_hours,
            evaluated_at
        FROM   {_DB}.{_OBS}.DATA_QUALITY_METRICS
        WHERE  1=1 {_ds}
        ORDER  BY evaluated_at DESC NULLS LAST
        LIMIT  {int(limit)}
        """
    )


@st.cache_data(ttl=60, show_spinner=False)
def get_quality_column_metrics(
    run_id: str | None = None,
) -> list[dict[str, Any]]:
    """
    Return per-column metric rows from QUALITY_METRICS for a specific run.

    If *run_id* is ``None``, the single most-recent quality run is used.

    Each item: ``column_name``, ``metric_type``, ``metric_value``,
               ``threshold``, ``passed``.

    Args:
        run_id: A ``QUALITY_RUNS.id`` value, or ``None`` for the latest run.
    """
    if run_id:
        _rid = f"'{run_id}'"
    else:
        _rid = (
            f"(SELECT id FROM {_DB}.{_OBS}.QUALITY_RUNS"
            f" ORDER BY evaluated_at DESC NULLS LAST LIMIT 1)"
        )
    return _run(
        f"""
        SELECT
            qm.column_name,
            qm.metric_type,
            qm.metric_value,
            qm.threshold,
            qm.passed
        FROM   {_DB}.{_OBS}.QUALITY_METRICS qm
        WHERE  qm.quality_run_id = {_rid}
        ORDER  BY qm.column_name, qm.metric_type
        """
    )
