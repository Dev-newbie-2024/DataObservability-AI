"""
dashboard/pages/04_drift_monitoring.py
========================================
Phase 5B — Page 4: Drift Analytics Dashboard

Sections
--------
1. Header          — title, refresh button, last-updated timestamp.
2. Filters         — Dataset selector + Date-range picker (sidebar).
3. KPI Cards       — Drift Score · Severity · Detection Rate
                     Drifted Columns · Avg PSI.
4. Score Trend     — 30-day avg/min/max drift score (Plotly area chart).
5. Severity Dist.  — Bar chart of runs per severity bucket.
6. PSI by Column   — Horizontal bar from DRIFT_METRICS latest run.
7. Drifted/Stable  — Donut: drifted vs stable column count.
8. Run History     — Styled dataframe of DRIFT_RUNS rows.

Tables read (read-only, no writes)
-----------------------------------
  DRIFT_SUMMARY · DRIFT_RUNS · DRIFT_METRICS

Note: ``st.set_page_config`` lives in ``app.py`` — do NOT call it here.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.utils.snowflake_queries import (
    get_drift_column_metrics,
    get_drift_datasets,
    get_drift_kpis,
    get_drift_run_history,
    get_drift_score_trend,
)
from dashboard.utils.styles import inject_global_css

# ── CSS ───────────────────────────────────────────────────────────────────────
inject_global_css()

# ── Constants ─────────────────────────────────────────────────────────────────
_ACCENT   = "#8b5cf6"   # violet — drift theme distinct from quality's indigo
_DETECTED = "#ef4444"
_STABLE   = "#22c55e"
_WARN     = "#f59e0b"

_SEV_COLORS: dict[str, str] = {
    "LOW":      "#22c55e",
    "MEDIUM":   "#f59e0b",
    "HIGH":     "#ef4444",
    "CRITICAL": "#7c3aed",
    "NONE":     "#6b7280",
}


# ── Helper: empty-state placeholder ──────────────────────────────────────────

def _empty_panel(
    icon: str,
    msg: str,
    height: int = 200,
    border_color: str = "rgba(139,92,246,0.25)",
) -> None:
    """Render a centred placeholder card when a chart has no data."""
    st.markdown(
        f"""
        <div style="
            height:{height}px;display:flex;flex-direction:column;
            align-items:center;justify-content:center;
            background:rgba(30,41,59,0.5);
            border:1px dashed {border_color};
            border-radius:12px;color:#64748b;font-size:.875rem;
        ">
            <span style="font-size:2rem;margin-bottom:8px">{icon}</span>
            <span>{msg}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ── Helper: format timestamp ──────────────────────────────────────────────────

def _fmt_ts(ts: object) -> str:
    if ts is None:
        return "—"
    try:
        return ts.strftime("%m-%d %H:%M") if hasattr(ts, "strftime") else str(ts)[:16]
    except Exception:
        return str(ts)[:16]


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1 — Header
# ─────────────────────────────────────────────────────────────────────────────

header_col, refresh_col = st.columns([5, 1])
with header_col:
    st.markdown(
        """
        <div class="page-header">
            <h1>📉 Drift Analytics Dashboard</h1>
            <p class="header-subtitle">
                Drift score trends &nbsp;·&nbsp; PSI / KS per column &nbsp;·&nbsp;
                Severity distribution &nbsp;·&nbsp; Drifted vs stable columns
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
with refresh_col:
    st.markdown("<div style='margin-top:1.1rem'></div>", unsafe_allow_html=True)
    if st.button("🔄 Refresh", use_container_width=True, key="drift_refresh"):
        st.cache_data.clear()
        st.rerun()
    st.caption(f"🕐 {datetime.now(timezone.utc).strftime('%H:%M UTC')}")

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2 — Sidebar Filters
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### 🎛️ Filters")
    st.markdown("---")

    with st.spinner("Loading datasets …"):
        _all_datasets = get_drift_datasets()

    dataset_options = ["All Datasets"] + _all_datasets
    selected_dataset_label = st.selectbox(
        "📦 Dataset",
        dataset_options,
        index=0,
        key="drift_dataset",
        help="Filter all charts and tables to a single dataset.",
    )
    active_dataset: str | None = (
        None if selected_dataset_label == "All Datasets" else selected_dataset_label
    )

    st.markdown("---")

    _today = date.today()
    _default_start = _today - timedelta(days=29)

    date_range = st.date_input(
        "📅 Date Range",
        value=(_default_start, _today),
        min_value=_today - timedelta(days=365),
        max_value=_today,
        key="drift_date_range",
        help="Controls the look-back window for the Drift Score Trend chart.",
    )

    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        _start_date, _end_date = date_range
        trend_days = max(1, (_end_date - _start_date).days + 1)
    else:
        trend_days = 30

    st.caption(f"Look-back window: **{trend_days} day(s)**")
    st.markdown("---")
    st.caption("Data refreshes every 60 s via `@st.cache_data`.")

# ─────────────────────────────────────────────────────────────────────────────
# Fetch all data (cached)
# ─────────────────────────────────────────────────────────────────────────────

with st.spinner("Loading drift metrics …"):
    kpis        = get_drift_kpis(dataset_id=active_dataset)
    score_trend = get_drift_score_trend(days=trend_days, dataset_id=active_dataset)
    run_history = get_drift_run_history(limit=20, dataset_id=active_dataset)
    col_metrics = get_drift_column_metrics(run_id=None)

no_data = kpis["total_runs"] == 0

if no_data:
    st.info(
        "**No drift data yet.**  Run the Drift Detection Agent to populate "
        "DRIFT_SUMMARY, DRIFT_RUNS, and DRIFT_METRICS.",
        icon="ℹ️",
    )

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3 — KPI Cards
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("📊 Drift KPIs")

k1, k2, k3, k4, k5 = st.columns(5)

_score     = kpis["latest_score"]
_sev       = kpis["latest_severity"]
_det_rate  = kpis["detection_rate"]
_drifted   = kpis["latest_drifted_columns"]
_total_col = kpis["latest_total_columns"]
_avg_psi   = kpis["avg_psi"]

with k1:
    _sc = f"{_score:.3f}" if _score is not None else "—"
    st.metric(
        "📉 Drift Score",
        _sc,
        delta=f"{kpis['total_runs']} run(s)" if kpis["total_runs"] > 0 else None,
        delta_color="normal",
        help="Latest overall_drift_score from DRIFT_SUMMARY (0–1 scale)",
    )

with k2:
    _sv = str(_sev).upper() if _sev else "—"
    st.metric(
        "🚦 Severity",
        _sv,
        delta="drift detected" if kpis["detected_runs"] > 0 else "no drift",
        delta_color="inverse" if kpis["detected_runs"] > 0 else "normal",
        help="Latest drift_severity from DRIFT_SUMMARY",
    )

with k3:
    _dr = f"{_det_rate:.1f}%" if _det_rate is not None else "—"
    st.metric(
        "⚡ Detection Rate",
        _dr,
        delta=f"{kpis['detected_runs']} / {kpis['total_runs']} detected",
        delta_color="inverse" if (_det_rate or 0) > 30 else "normal",
        help="% of runs where drift_detected = TRUE",
    )

with k4:
    _dc = str(_drifted) if _drifted is not None else "—"
    if _drifted is not None and _total_col:
        _dc = f"{_drifted} / {_total_col}"
    st.metric(
        "🔀 Drifted Cols",
        _dc,
        delta="columns drifted",
        delta_color="inverse" if (_drifted or 0) > 0 else "normal",
        help="Latest drifted_columns / total_columns from DRIFT_SUMMARY",
    )

with k5:
    _ap = f"{_avg_psi:.4f}" if _avg_psi is not None else "—"
    st.metric(
        "📐 Avg PSI Proxy",
        _ap,
        delta="avg drift score",
        delta_color="inverse" if (_avg_psi or 0) > 0.2 else "normal",
        help="Average drift_score across all runs (proxy for avg PSI)",
    )

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4 — Drift Score Trend  +  Severity Distribution
# ─────────────────────────────────────────────────────────────────────────────

trend_col, sev_col = st.columns([3, 2], gap="large")

# ── 4a. Drift Score Trend ─────────────────────────────────────────────────────
with trend_col:
    st.subheader(f"📈 Drift Score Trend  ·  Last {trend_days} Days")

    if score_trend:
        df_trend = pd.DataFrame(score_trend)

        fig_trend = go.Figure()

        # Min–max band
        fig_trend.add_trace(go.Scatter(
            x=list(df_trend["day"]) + list(df_trend["day"][::-1]),
            y=list(df_trend["max_score"]) + list(df_trend["min_score"][::-1]),
            fill="toself",
            fillcolor="rgba(139,92,246,0.10)",
            line=dict(color="rgba(0,0,0,0)"),
            showlegend=True,
            name="Min–Max Range",
            hoverinfo="skip",
        ))

        # Avg line
        fig_trend.add_trace(go.Scatter(
            x=df_trend["day"],
            y=df_trend["avg_score"],
            mode="lines+markers",
            name="Avg Score",
            line=dict(color=_ACCENT, width=2.5, shape="spline"),
            marker=dict(size=7, color=_ACCENT, line=dict(color="#c4b5fd", width=1.5)),
            hovertemplate="<b>%{x}</b><br>Drift: <b>%{y:.4f}</b><extra></extra>",
        ))

        # 0.2 alert threshold
        fig_trend.add_hline(
            y=0.2,
            line=dict(color=_WARN, width=1, dash="dot"),
            annotation_text="Alert 0.2",
            annotation_position="top left",
            annotation_font=dict(color=_WARN, size=10),
        )

        fig_trend.update_layout(
            height=270,
            margin=dict(l=0, r=10, t=10, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            yaxis=dict(
                title="Drift Score (0–1)",
                gridcolor="rgba(51,65,85,0.6)",
                tickfont=dict(color="#64748b", size=10),
                zeroline=False,
            ),
            xaxis=dict(
                gridcolor="rgba(51,65,85,0.4)",
                tickfont=dict(color="#64748b", size=10),
            ),
            legend=dict(
                orientation="h", x=0, y=1.12,
                font=dict(color="#9ca3af", size=11),
                bgcolor="rgba(0,0,0,0)",
            ),
            font_color="#e5e7eb",
            font_family="Inter",
        )
        st.plotly_chart(fig_trend, use_container_width=True, config={"displayModeBar": False})
    else:
        _empty_panel("📈", "No drift score data in selected date range.")

# ── 4b. Severity Distribution ─────────────────────────────────────────────────
with sev_col:
    st.subheader("🚦 Severity Distribution")

    if score_trend:
        # Group detected_count by day as a proxy for severity stacked bar
        df_sev = pd.DataFrame(score_trend)
        _detected_total  = int(df_sev["detected_count"].sum())
        _run_total       = int(df_sev["run_count"].sum())
        _stable_total    = _run_total - _detected_total

        if _run_total > 0:
            fig_sev = go.Figure()
            fig_sev.add_trace(go.Bar(
                x=df_sev["day"],
                y=df_sev["detected_count"],
                name="Drift Detected",
                marker_color=_DETECTED,
                hovertemplate="<b>%{x}</b><br>Detected: <b>%{y}</b><extra></extra>",
            ))
            fig_sev.add_trace(go.Bar(
                x=df_sev["day"],
                y=[r - d for r, d in zip(
                    df_sev["run_count"], df_sev["detected_count"]
                )],
                name="Stable",
                marker_color=_STABLE,
                hovertemplate="<b>%{x}</b><br>Stable: <b>%{y}</b><extra></extra>",
            ))
            fig_sev.update_layout(
                barmode="stack",
                height=270,
                margin=dict(l=0, r=10, t=10, b=0),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(
                    gridcolor="rgba(51,65,85,0.4)",
                    tickfont=dict(color="#64748b", size=9),
                    tickangle=45,
                ),
                yaxis=dict(
                    title="Run Count",
                    gridcolor="rgba(51,65,85,0.6)",
                    tickfont=dict(color="#64748b", size=10),
                ),
                legend=dict(
                    orientation="h", x=0, y=1.12,
                    font=dict(color="#9ca3af", size=11),
                    bgcolor="rgba(0,0,0,0)",
                ),
                font_color="#e5e7eb",
                font_family="Inter",
            )
            st.plotly_chart(fig_sev, use_container_width=True, config={"displayModeBar": False})
        else:
            _empty_panel("🚦", "No severity data yet.", height=270)
    else:
        _empty_panel("🚦", "No drift run data in selected range.", height=270)

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 5 — PSI by Column  +  Drifted / Stable Donut
# ─────────────────────────────────────────────────────────────────────────────

psi_col, donut_col = st.columns([3, 2], gap="large")

# ── 5a. PSI by Column ─────────────────────────────────────────────────────────
with psi_col:
    st.subheader("📐 PSI by Column  ·  Latest Run")

    if col_metrics:
        df_cm = pd.DataFrame(col_metrics)
        df_cm.columns = [c.lower() for c in df_cm.columns]

        if "psi_score" in df_cm.columns and "column_name" in df_cm.columns:
            df_psi = (
                df_cm[df_cm["psi_score"].notna()]
                .groupby("column_name", as_index=False)["psi_score"]
                .max()
                .sort_values("psi_score", ascending=True)
                .tail(20)   # top 20 by PSI
            )

            if not df_psi.empty:
                _bar_colors = [
                    "#22c55e" if v < 0.1
                    else "#f59e0b" if v < 0.2
                    else "#ef4444"
                    for v in df_psi["psi_score"]
                ]
                fig_psi = go.Figure(go.Bar(
                    x=df_psi["psi_score"],
                    y=df_psi["column_name"],
                    orientation="h",
                    marker=dict(
                        color=_bar_colors,
                        line=dict(color="rgba(0,0,0,0)", width=0),
                    ),
                    text=[f"{v:.4f}" for v in df_psi["psi_score"]],
                    textposition="outside",
                    textfont=dict(color="#e5e7eb", size=10),
                    hovertemplate="%{y}: PSI <b>%{x:.4f}</b><extra></extra>",
                ))
                # 0.2 PSI critical threshold
                fig_psi.add_vline(
                    x=0.2,
                    line=dict(color=_WARN, width=1, dash="dot"),
                    annotation_text="PSI 0.2",
                    annotation_position="top right",
                    annotation_font=dict(color=_WARN, size=9),
                )
                fig_psi.update_layout(
                    height=max(240, len(df_psi) * 28),
                    margin=dict(l=0, r=60, t=20, b=0),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    xaxis=dict(
                        title="PSI Score",
                        gridcolor="rgba(51,65,85,0.6)",
                        tickfont=dict(color="#64748b", size=10),
                    ),
                    yaxis=dict(tickfont=dict(color="#e5e7eb", size=10)),
                    font_color="#e5e7eb",
                    font_family="Inter",
                )
                st.plotly_chart(fig_psi, use_container_width=True, config={"displayModeBar": False})
            else:
                _empty_panel("📐", "No PSI scores in the latest run.", height=240)
        else:
            _empty_panel(
                "📐",
                "DRIFT_METRICS returned but lacks psi_score / column_name columns.",
                height=240,
                border_color="rgba(239,68,68,0.25)",
            )
    else:
        _empty_panel(
            "📐",
            "No column drift metrics — populate DRIFT_METRICS to see this chart.",
            height=240,
        )

# ── 5b. Drifted vs Stable Donut ───────────────────────────────────────────────
with donut_col:
    st.subheader("🔀 Drifted vs Stable  ·  Latest Run")

    _drifted_n = kpis["latest_drifted_columns"] or 0
    _total_n   = kpis["latest_total_columns"] or 0
    _stable_n  = max(0, _total_n - _drifted_n)

    if _total_n > 0:
        fig_donut = go.Figure(go.Pie(
            labels=["Drifted", "Stable"],
            values=[_drifted_n, _stable_n],
            hole=0.62,
            marker=dict(
                colors=[_DETECTED, _STABLE],
                line=dict(color="rgba(0,0,0,0)", width=0),
            ),
            textinfo="percent+label",
            textfont=dict(size=12, color="#e5e7eb"),
            hovertemplate="%{label}: <b>%{value}</b> columns (%{percent})<extra></extra>",
        ))
        fig_donut.add_annotation(
            text=f"<b>{_total_n}</b><br><span style='font-size:10px'>columns</span>",
            x=0.5, y=0.5,
            font=dict(color="#e5e7eb", size=16, family="Inter"),
            showarrow=False,
        )
        fig_donut.update_layout(
            height=300,
            margin=dict(l=0, r=0, t=10, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            showlegend=True,
            legend=dict(
                orientation="h", x=0.15, y=-0.05,
                font=dict(color="#9ca3af", size=11),
                bgcolor="rgba(0,0,0,0)",
            ),
            font_color="#e5e7eb",
            font_family="Inter",
        )
        st.plotly_chart(fig_donut, use_container_width=True, config={"displayModeBar": False})
    else:
        _empty_panel(
            "🔀", "No column-level data in latest run.",
            height=300, border_color="rgba(34,197,94,0.25)",
        )

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6 — Run History Table
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("📋 Recent Drift Runs")

if run_history:
    display_rows = []
    for r in run_history:
        detected_val = r.get("DRIFT_DETECTED")
        if detected_val is None:
            badge = "⚪ —"
        elif detected_val:
            badge = "🔴 Detected"
        else:
            badge = "🟢 Stable"

        def _score_fmt(val: object) -> str:
            if val is None:
                return "—"
            try:
                return f"{float(val):.4f}"
            except (TypeError, ValueError):
                return "—"

        display_rows.append({
            "Dataset":    str(r.get("DATASET_ID", "—")),
            "Score":      _score_fmt(r.get("OVERALL_DRIFT_SCORE")),
            "Status":     badge,
            "Ref Start":  _fmt_ts(r.get("REFERENCE_WINDOW_START")),
            "Ref End":    _fmt_ts(r.get("REFERENCE_WINDOW_END")),
            "Cur Start":  _fmt_ts(r.get("CURRENT_WINDOW_START")),
            "Cur End":    _fmt_ts(r.get("CURRENT_WINDOW_END")),
            "Evaluated":  _fmt_ts(r.get("EVALUATED_AT")),
        })

    df_hist = pd.DataFrame(display_rows)

    def _color_status(val: str) -> str:
        if "Detected" in val:
            return f"color: {_DETECTED}; font-weight: 600"
        if "Stable" in val:
            return f"color: {_STABLE}; font-weight: 600"
        return "color: #6b7280"

    def _color_score(val: str) -> str:
        try:
            s = float(val)
            if s >= 0.2:
                return f"color: {_DETECTED}; font-weight: 600"
            if s >= 0.1:
                return f"color: {_WARN}; font-weight: 600"
            return f"color: {_STABLE}; font-weight: 600"
        except (TypeError, ValueError):
            return "color: #6b7280"

    styled = (
        df_hist.style
        .applymap(_color_status, subset=["Status"])
        .applymap(_color_score,  subset=["Score"])
        .set_properties(**{
            "background-color": "transparent",
            "color": "#e5e7eb",
            "font-size": "0.83rem",
        })
        .set_table_styles([
            {"selector": "thead th", "props": [
                ("background-color", "rgba(139,92,246,0.15)"),
                ("color", "#c4b5fd"),
                ("font-size", "0.72rem"),
                ("font-weight", "600"),
                ("letter-spacing", "0.05em"),
                ("text-transform", "uppercase"),
                ("padding", "8px 12px"),
            ]},
            {"selector": "td", "props": [("padding", "7px 12px")]},
        ])
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)
else:
    st.info("No drift run records found.", icon="📋")

# ─────────────────────────────────────────────────────────────────────────────
# Footer
# ─────────────────────────────────────────────────────────────────────────────

st.divider()
st.markdown(
    """
    <div style="
        text-align:center;color:#475569;font-size:.72rem;
        padding-top:.75rem;
        border-top:1px solid rgba(71,85,105,.25);
    ">
        DataObservability-AI &nbsp;·&nbsp; VTU Major Project — ISE 2025-2026
        &nbsp;·&nbsp; Phase 5B · Page 4: Drift Analytics Dashboard
    </div>
    """,
    unsafe_allow_html=True,
)
