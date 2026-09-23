"""
ai_agents/drift_agent/evidently_runner.py
==========================================
Evidently-based drift detection runner with a pandas fallback.

Public surface
--------------
* ``run_drift_suite(current_df, reference_df, ...)`` → structured dict

Evidently is used when installed (it is listed in ai_agents requirements).
If Evidently is unavailable the runner falls back to scipy KS-test + manual PSI
so the agent degrades gracefully without crashing.
"""

from __future__ import annotations

from typing import Any

from config.logging_config import get_logger

logger = get_logger(__name__)

# ── PSI bin count ─────────────────────────────────────────────────────────────
_PSI_BINS: int = 10
_PSI_DRIFT_THRESHOLD: float = 0.2   # PSI > 0.2 → significant drift
_KS_DRIFT_THRESHOLD:  float = 0.05  # p-value < 0.05 → drift detected


# ── Helpers ───────────────────────────────────────────────────────────────────

def _psi(reference: "pd.Series", current: "pd.Series", bins: int = _PSI_BINS) -> float:  # noqa: F821
    """Compute Population Stability Index between two numeric series."""
    import numpy as np

    ref = reference.dropna().values
    cur = current.dropna().values

    if len(ref) == 0 or len(cur) == 0:
        return 0.0

    # Use reference distribution to define bins
    min_val = min(ref.min(), cur.min())
    max_val = max(ref.max(), cur.max())
    if min_val == max_val:
        return 0.0

    edges = np.linspace(min_val, max_val, bins + 1)
    ref_counts, _ = np.histogram(ref, bins=edges)
    cur_counts, _ = np.histogram(cur, bins=edges)

    # Avoid log(0) by replacing zeros with small epsilon
    eps = 1e-6
    ref_pct = (ref_counts + eps) / (ref_counts.sum() + eps * bins)
    cur_pct = (cur_counts + eps) / (cur_counts.sum() + eps * bins)

    psi = float(np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct)))
    return round(abs(psi), 6)


def _ks_test(reference: "pd.Series", current: "pd.Series") -> dict[str, float]:  # noqa: F821
    """Run KS two-sample test. Returns {statistic, p_value}."""
    try:
        from scipy import stats
        ref = reference.dropna().values
        cur = current.dropna().values
        if len(ref) < 2 or len(cur) < 2:
            return {"statistic": 0.0, "p_value": 1.0}
        stat, pval = stats.ks_2samp(ref, cur)
        return {"statistic": round(float(stat), 6), "p_value": round(float(pval), 6)}
    except ImportError:
        return {"statistic": 0.0, "p_value": 1.0}


def _is_numeric(series: "pd.Series") -> bool:  # noqa: F821
    import pandas as pd
    return pd.api.types.is_numeric_dtype(series)


# ── Pandas fallback ───────────────────────────────────────────────────────────

def _run_pandas_drift(
    current_df: "pd.DataFrame",  # noqa: F821
    reference_df: "pd.DataFrame",  # noqa: F821
) -> dict[str, Any]:
    """
    Pure-pandas + scipy drift detection when Evidently is unavailable.
    Returns the same schema as the Evidently path.
    """
    data_cols = [c for c in current_df.columns if not c.startswith("_")]
    col_results: dict[str, dict[str, Any]] = {}
    drifted = 0

    for col in data_cols:
        if col not in reference_df.columns:
            continue

        cur_series = current_df[col]
        ref_series = reference_df[col]

        if _is_numeric(cur_series) and _is_numeric(ref_series):
            psi  = _psi(ref_series, cur_series)
            ks   = _ks_test(ref_series, cur_series)
            col_drift = psi > _PSI_DRIFT_THRESHOLD or ks["p_value"] < _KS_DRIFT_THRESHOLD
        else:
            # Categorical — compare value-count distributions (chi-squared via PSI proxy)
            psi  = 0.0
            ks   = {"statistic": 0.0, "p_value": 1.0}
            ref_freq = ref_series.value_counts(normalize=True)
            cur_freq = cur_series.value_counts(normalize=True)
            all_cats = set(ref_freq.index) | set(cur_freq.index)
            if all_cats:
                eps = 1e-6
                diff = sum(
                    abs(cur_freq.get(c, eps) - ref_freq.get(c, eps))
                    for c in all_cats
                )
                psi = round(float(diff), 6)
                col_drift = psi > 0.1
            else:
                col_drift = False

        if col_drift:
            drifted += 1

        col_results[col] = {
            "psi":          psi,
            "ks_statistic": ks["statistic"],
            "ks_p_value":   ks["p_value"],
            "drift_detected": col_drift,
        }

    total_cols = len(col_results)
    drift_pct  = round(drifted / total_cols * 100, 2) if total_cols else 0.0
    avg_psi    = round(sum(r["psi"] for r in col_results.values()) / total_cols, 6) if total_cols else 0.0
    avg_ks     = round(sum(r["ks_statistic"] for r in col_results.values()) / total_cols, 6) if total_cols else 0.0

    return {
        "evidently_available": False,
        "drifted_columns":  drifted,
        "total_columns":    total_cols,
        "drift_pct":        drift_pct,
        "avg_psi":          avg_psi,
        "avg_ks_stat":      avg_ks,
        "column_results":   col_results,
    }


# ── Evidently path ────────────────────────────────────────────────────────────

def _run_evidently_drift(
    current_df: "pd.DataFrame",  # noqa: F821
    reference_df: "pd.DataFrame",  # noqa: F821
    suite_name: str,
) -> dict[str, Any]:
    """Run Evidently DataDriftPreset and extract per-column results."""
    from evidently.report import Report
    from evidently.metric_preset import DataDriftPreset

    report = Report(metrics=[DataDriftPreset()])
    report.run(reference_data=reference_df, current_data=current_df)
    result = report.as_dict()

    metrics = result.get("metrics", [])
    col_results: dict[str, dict[str, Any]] = {}
    drifted = 0

    for metric in metrics:
        metric_id = metric.get("metric", "")
        mresult = metric.get("result", {})

        # DataDriftTable contains per-column breakdown
        if "DataDriftTable" in metric_id or "drift_by_columns" in mresult:
            per_col = mresult.get("drift_by_columns", {})
            for col_name, col_data in per_col.items():
                if col_name.startswith("_"):
                    continue
                stat_val = col_data.get("statistic", 0.0) or 0.0
                p_val    = col_data.get("p_value", 1.0)
                col_drift = bool(col_data.get("drift_detected", False))
                if col_drift:
                    drifted += 1

                # Compute PSI independently for the summary
                psi_val = 0.0
                if col_name in current_df.columns and col_name in reference_df.columns:
                    if _is_numeric(current_df[col_name]) and _is_numeric(reference_df[col_name]):
                        psi_val = _psi(reference_df[col_name], current_df[col_name])

                col_results[col_name] = {
                    "psi":           psi_val,
                    "ks_statistic":  float(stat_val) if not _is_numeric(current_df.get(col_name, current_df.iloc[:, 0])) else float(stat_val),
                    "ks_p_value":    float(p_val) if p_val is not None else 1.0,
                    "drift_detected": col_drift,
                }

    total_cols = len(col_results)
    if total_cols == 0:
        # Evidently didn't produce per-column data — fall back
        return _run_pandas_drift(current_df, reference_df)

    drift_pct = round(drifted / total_cols * 100, 2) if total_cols else 0.0
    avg_psi   = round(sum(r["psi"] for r in col_results.values()) / total_cols, 6) if total_cols else 0.0
    avg_ks    = round(sum(r["ks_statistic"] for r in col_results.values()) / total_cols, 6) if total_cols else 0.0

    return {
        "evidently_available": True,
        "drifted_columns":  drifted,
        "total_columns":    total_cols,
        "drift_pct":        drift_pct,
        "avg_psi":          avg_psi,
        "avg_ks_stat":      avg_ks,
        "column_results":   col_results,
    }


# ── Public entry point ────────────────────────────────────────────────────────

def run_drift_suite(
    current_df:    "pd.DataFrame",  # noqa: F821
    reference_df:  "pd.DataFrame",  # noqa: F821
    suite_name:    str = "drift_suite",
) -> dict[str, Any]:
    """
    Detect drift between ``current_df`` and ``reference_df``.

    Tries Evidently first; falls back to pandas + scipy if unavailable.

    Returns a dict with keys:
        evidently_available, drifted_columns, total_columns, drift_pct,
        avg_psi, avg_ks_stat, column_results
    """
    try:
        import evidently  # noqa: F401
        logger.info("Running Evidently drift suite", suite_name=suite_name)
        return _run_evidently_drift(current_df, reference_df, suite_name)
    except ImportError:
        logger.info("Evidently not available — using pandas+scipy fallback", suite_name=suite_name)
        return _run_pandas_drift(current_df, reference_df)
    except Exception as exc:
        logger.warning(
            "Evidently drift suite failed — falling back to pandas",
            error=str(exc),
            suite_name=suite_name,
        )
        return _run_pandas_drift(current_df, reference_df)
