"""
ai_agents/quality_agent/gx_runner.py
======================================
Thin wrapper around Great Expectations (GX) that runs an in-memory expectation
suite against a pandas DataFrame.

Design decisions
----------------
* **All GX imports are deferred** to this module so the rest of the agent
  package can be imported and unit-tested without GX installed.
* Uses GX's **EphemeralDataContext + pandas DataSource** (GX >=0.18 / GX 1.x).
  If the GX API is unavailable or raises, ``run_gx_suite`` falls back to pure-
  pandas computation and returns an equivalent result dict.
* The returned dict is always the same shape — callers never branch on
  which path was taken.

Returned dict shape
-------------------
{
    "suite_name":       str,
    "total_rows":       int,
    "gx_available":     bool,
    "column_results": {
        "<col_name>": {
            "null_count":      int,
            "null_rate":       float,          # 0–1
            "type_ok":         bool,
            "unexpected_count": int,           # for type / range checks
        },
        ...
    },
    "duplicate_count":  int,
    "duplicate_rate":   float,                 # 0–1
    "freshness_hours":  float | None,
    "expectations_passed": int,
    "expectations_total":  int,
}
"""

from __future__ import annotations

from typing import Any

import pandas as pd


# ── Snowflake type → pandas dtype family mapping ──────────────────────────────

_SNOWFLAKE_NUMERIC = frozenset({
    "NUMBER", "DECIMAL", "NUMERIC", "INT", "INTEGER", "BIGINT",
    "SMALLINT", "TINYINT", "BYTEINT", "FLOAT", "FLOAT4", "FLOAT8",
    "DOUBLE", "REAL",
})
_SNOWFLAKE_STRING = frozenset({
    "VARCHAR", "CHAR", "CHARACTER", "STRING", "TEXT", "BINARY", "VARBINARY",
})
_SNOWFLAKE_BOOL = frozenset({"BOOLEAN"})
_SNOWFLAKE_DATE = frozenset({
    "DATE", "DATETIME", "TIME",
    "TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_NTZ", "TIMESTAMP_TZ",
})


def _snowflake_family(sf_type: str) -> str:
    """Return 'numeric', 'string', 'bool', 'date', or 'unknown'."""
    base = sf_type.upper().split("(")[0].strip()
    if base in _SNOWFLAKE_NUMERIC:
        return "numeric"
    if base in _SNOWFLAKE_STRING:
        return "string"
    if base in _SNOWFLAKE_BOOL:
        return "bool"
    if base in _SNOWFLAKE_DATE:
        return "date"
    return "unknown"


def _type_ok(series: pd.Series, expected_family: str) -> bool:
    """Return True if the Series dtype is compatible with the expected family."""
    dtype_kind = series.dtype.kind  # 'i','u','f','b','O','M','m'
    if expected_family == "numeric":
        return dtype_kind in ("i", "u", "f")
    if expected_family == "string":
        return dtype_kind == "O"
    if expected_family == "bool":
        return dtype_kind in ("b", "O", "i")
    if expected_family == "date":
        return dtype_kind in ("M",)
    return True  # unknown → assume ok


# ── Freshness helper ──────────────────────────────────────────────────────────

def _compute_freshness(df: pd.DataFrame) -> float | None:
    """
    Return hours since the most-recent ``_SILVER_LOADED_AT`` value.
    Returns ``None`` if the column is absent or unparseable.
    """
    ts_col = "_SILVER_LOADED_AT"
    if ts_col not in df.columns:
        return None
    try:
        series = pd.to_datetime(df[ts_col], utc=True, errors="coerce")
        max_ts = series.max()
        if pd.isnull(max_ts):
            return None
        now = pd.Timestamp.utcnow().tz_localize("UTC") if pd.Timestamp.utcnow().tzinfo is None else pd.Timestamp.utcnow()
        delta = now - max_ts
        return round(delta.total_seconds() / 3600, 2)
    except Exception:
        return None


# ── Pure-pandas fallback ──────────────────────────────────────────────────────

def _pandas_run(
    df: pd.DataFrame,
    suite_name: str,
    column_types: dict[str, str],
) -> dict[str, Any]:
    """Compute quality metrics using only pandas (no GX dependency)."""
    data_cols = [c for c in df.columns if not c.startswith("_")]

    column_results: dict[str, dict[str, Any]] = {}
    expectations_passed = 0
    expectations_total = 0

    for col in data_cols:
        series = df[col]
        null_count = int(series.isna().sum())
        null_rate  = null_count / len(df) if len(df) else 0.0

        sf_type = column_types.get(col.upper(), column_types.get(col, "VARCHAR"))
        family  = _snowflake_family(sf_type)
        t_ok    = _type_ok(series.dropna(), family) if not series.dropna().empty else True

        # Count as expectations
        expectations_total += 2
        if null_rate == 0.0:
            expectations_passed += 1
        if t_ok:
            expectations_passed += 1

        column_results[col] = {
            "null_count":       null_count,
            "null_rate":        round(null_rate, 6),
            "type_ok":          t_ok,
            "unexpected_count": 0 if t_ok else null_count,
        }

    # Duplicate check across data columns
    if data_cols and len(df):
        dup_mask      = df[data_cols].duplicated(keep=False)
        dup_count     = int(dup_mask.sum())
        dup_rate      = dup_count / len(df)
    else:
        dup_count, dup_rate = 0, 0.0

    expectations_total  += 1
    if dup_rate == 0.0:
        expectations_passed += 1

    return {
        "suite_name":          suite_name,
        "total_rows":          len(df),
        "gx_available":        False,
        "column_results":      column_results,
        "duplicate_count":     dup_count,
        "duplicate_rate":      round(dup_rate, 6),
        "freshness_hours":     _compute_freshness(df),
        "expectations_passed": expectations_passed,
        "expectations_total":  expectations_total,
    }


# ── GX in-memory runner ───────────────────────────────────────────────────────

def _gx_run(
    df: pd.DataFrame,
    suite_name: str,
    column_types: dict[str, str],
) -> dict[str, Any]:
    """
    Run expectations via GX EphemeralDataContext + pandas DataSource.
    Only called when GX is confirmed importable.
    """
    import great_expectations as gx

    context = gx.get_context(mode="ephemeral")

    # Register pandas datasource
    ds   = context.sources.add_pandas(name="in_memory_source")
    asset = ds.add_dataframe_asset(name="quality_batch")
    batch_req = asset.build_batch_request(dataframe=df)

    suite = context.add_expectation_suite(expectation_suite_name=suite_name)
    validator = context.get_validator(
        batch_request=batch_req,
        expectation_suite=suite,
    )

    data_cols = [c for c in df.columns if not c.startswith("_")]

    # ── Column-level expectations ─────────────────────────────────────────────
    col_results: dict[str, Any] = {}
    for col in data_cols:
        # Null check
        null_res = validator.expect_column_values_to_not_be_null(col)
        null_rate = (
            null_res.result.get("unexpected_percent", 0.0) / 100.0
            if null_res.result else 0.0
        )

        # Type check
        sf_type = column_types.get(col.upper(), column_types.get(col, "VARCHAR"))
        family  = _snowflake_family(sf_type)
        t_ok    = _type_ok(df[col].dropna(), family) if not df[col].dropna().empty else True

        col_results[col] = {
            "null_count":       int(null_res.result.get("unexpected_count", 0)),
            "null_rate":        round(null_rate, 6),
            "type_ok":          t_ok,
            "unexpected_count": 0 if t_ok else int(df[col].isna().sum()),
        }

    # ── Suite-level: run and collect stats ────────────────────────────────────
    validation_result = validator.validate()
    stats = validation_result.statistics

    # Duplicate check (pure pandas — GX compound uniqueness is complex)
    if data_cols and len(df):
        dup_mask  = df[data_cols].duplicated(keep=False)
        dup_count = int(dup_mask.sum())
        dup_rate  = dup_count / len(df)
    else:
        dup_count, dup_rate = 0, 0.0

    return {
        "suite_name":          suite_name,
        "total_rows":          len(df),
        "gx_available":        True,
        "column_results":      col_results,
        "duplicate_count":     dup_count,
        "duplicate_rate":      round(dup_rate, 6),
        "freshness_hours":     _compute_freshness(df),
        "expectations_passed": stats.get("successful_expectations", 0),
        "expectations_total":  stats.get("evaluated_expectations", 0),
    }


# ── Public entry-point ────────────────────────────────────────────────────────

def run_gx_suite(
    df: pd.DataFrame,
    suite_name: str,
    column_types: dict[str, str],
) -> dict[str, Any]:
    """
    Run a GX expectation suite against ``df`` and return a normalised result dict.

    Falls back to pure-pandas computation if GX is unavailable or raises.

    Args:
        df:            DataFrame containing the Silver table data.
        suite_name:    Name for the expectation suite (e.g. ``"quality_<dataset_id>"``).
        column_types:  Mapping of COLUMN_NAME → Snowflake type string, used for
                       type-compatibility checks.

    Returns:
        Normalised result dict (see module docstring for shape).
    """
    if df.empty:
        return {
            "suite_name":          suite_name,
            "total_rows":          0,
            "gx_available":        False,
            "column_results":      {},
            "duplicate_count":     0,
            "duplicate_rate":      0.0,
            "freshness_hours":     None,
            "expectations_passed": 0,
            "expectations_total":  0,
        }

    try:
        import great_expectations  # noqa: F401 — presence check only
        return _gx_run(df, suite_name, column_types)
    except Exception:
        # GX unavailable, wrong version, or runtime error → silent fallback
        return _pandas_run(df, suite_name, column_types)
