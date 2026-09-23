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
