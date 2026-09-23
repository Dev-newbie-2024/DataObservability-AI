"""
dashboard/pages/03_data_quality.py
====================================
Phase 5 — Page 3: Data Quality Dashboard

Sections
--------
1. Header          — title, refresh button, last-updated timestamp.
2. Filters         — Dataset selector + Date-range picker (sidebar).
3. KPI Cards       — Quality Score · Pass Rate · Null Rate
                     Duplicate Rate · Type-Mismatch Rate.
4. Score Trend     — 30-day avg/min/max quality score (Plotly area chart).
5. Pass / Fail     — Donut chart of passed vs failed evaluations.
6. Metric Dist.    — Horizontal bar chart of latest error-rate metrics.
7. Column Quality  — Per-column pass/fail bar chart from QUALITY_METRICS.
8. Run History     — Styled dataframe of recent DATA_QUALITY_METRICS rows.

Tables read (read-only, no writes)
-----------------------------------
  DATA_QUALITY_METRICS · QUALITY_RUNS · QUALITY_METRICS

Note: ``st.set_page_config`` lives in ``app.py`` — do NOT call it here.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.utils.snowflake_queries import (
    get_quality_column_metrics,
    get_quality_datasets,
    get_quality_kpis,
    get_quality_run_history,
    get_quality_score_trend,
)
from dashboard.utils.styles import inject_global_css, score_color

# ── CSS ───────────────────────────────────────────────────────────────────────
inject_global_css()

# ── Constants ─────────────────────────────────────────────────────────────────
_PLOTLY_TEMPLATE = "plotly_dark"
_PASS_COLOR = "#22c55e"
_FAIL_COLOR = "#ef4444"
_ACCENT = "#6366f1"


# ── Helper: empty-state placeholder ──────────────────────────────────────────

def _empty_panel(
    icon: str,
    msg: str,
    height: int = 200,
    border_color: str = "rgba(99,102,241,0.25)",
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
            <h1>✅ Data Quality Dashboard</h1>
            <p class="header-subtitle">
                Quality score trends &nbsp;·&nbsp; Pass / Fail analysis &nbsp;·&nbsp;
                Null · Duplicate · Type-mismatch rates &nbsp;·&nbsp; Column-wise metrics
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
with refresh_col:
    st.markdown("<div style='margin-top:1.1rem'></div>", unsafe_allow_html=True)
    if st.button("🔄 Refresh", use_container_width=True, key="dq_refresh"):
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

    # Dataset selector
    with st.spinner("Loading datasets …"):
        _all_datasets = get_quality_datasets()

    dataset_options = ["All Datasets"] + _all_datasets
    selected_dataset_label = st.selectbox(
        "📦 Dataset",
        dataset_options,
        index=0,
        key="dq_dataset",
        help="Filter all charts and tables to a single dataset.",
    )
    active_dataset: str | None = (
        None if selected_dataset_label == "All Datasets" else selected_dataset_label
    )

    st.markdown("---")

    # Date range — drives the look-back window for trend charts
    _today = date.today()
    _default_start = _today - timedelta(days=29)

    date_range = st.date_input(
        "📅 Date Range",
        value=(_default_start, _today),
        min_value=_today - timedelta(days=365),
        max_value=_today,
        key="dq_date_range",
        help="Controls the look-back window for the Score Trend chart.",
    )

    # Resolve look-back days from selection
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

with st.spinner("Loading quality metrics …"):
    kpis = get_quality_kpis(dataset_id=active_dataset)
    score_trend = get_quality_score_trend(days=trend_days, dataset_id=active_dataset)
    run_history = get_quality_run_history(limit=20, dataset_id=active_dataset)
    col_metrics = get_quality_column_metrics(run_id=None)

no_data = kpis["total_runs"] == 0

if no_data:
    st.info(
        "**No quality data yet.**  Run the Data Quality Agent to populate "
        "DATA_QUALITY_METRICS, QUALITY_RUNS, and QUALITY_METRICS.",
        icon="ℹ️",
    )

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3 — KPI Cards
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("📊 Quality KPIs")

k1, k2, k3, k4, k5 = st.columns(5)

_score = kpis["latest_score"]
_pass_rate = kpis["pass_rate"]
_null = kpis["latest_null_rate"]
_dup = kpis["latest_dup_rate"]
_type = kpis["latest_type_rate"]

with k1:
    _sc = f"{_score:.1f}" if _score is not None else "—"
    st.metric(
        "🏆 Quality Score",
        _sc,
        delta=f"{kpis['total_runs']} eval(s)" if kpis["total_runs"] > 0 else None,
        delta_color="normal",
        help="Latest quality_score from DATA_QUALITY_METRICS (0–100)",
    )

with k2:
    _pr = f"{_pass_rate:.1f}%" if _pass_rate is not None else "—"
    st.metric(
        "✅ Pass Rate",
        _pr,
        delta=f"{kpis['passed_runs']} / {kpis['total_runs']} passed",
        delta_color="normal" if (_pass_rate or 0) >= 80 else "inverse",
        help="Percentage of evaluations where passed = TRUE",
    )

with k3:
    _nr = f"{_null:.2%}" if _null is not None else "—"
    st.metric(
        "🔍 Null Rate",
        _nr,
        delta="↓ good" if (_null or 1) < 0.05 else "↑ high",
        delta_color="normal" if (_null or 1) < 0.05 else "inverse",
        help="Latest null_rate from DATA_QUALITY_METRICS",
    )

with k4:
    _dr = f"{_dup:.2%}" if _dup is not None else "—"
    st.metric(
        "♻️ Duplicate Rate",
        _dr,
        delta="↓ good" if (_dup or 1) < 0.03 else "↑ high",
        delta_color="normal" if (_dup or 1) < 0.03 else "inverse",
        help="Latest duplicate_rate from DATA_QUALITY_METRICS",
    )

with k5:
    _tr = f"{_type:.2%}" if _type is not None else "—"
    st.metric(
        "⚠️ Type Mismatch",
        _tr,
        delta="↓ good" if (_type or 1) < 0.02 else "↑ high",
        delta_color="normal" if (_type or 1) < 0.02 else "inverse",
        help="Latest type_mismatch_rate from DATA_QUALITY_METRICS",
    )

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4 — Quality Score Trend  +  Pass/Fail Donut
# ─────────────────────────────────────────────────────────────────────────────

trend_col, pf_col = st.columns([3, 2], gap="large")

# ── 4a. Score Trend (area chart) ──────────────────────────────────────────────
with trend_col:
    st.subheader(f"📈 Quality Score Trend  ·  Last {trend_days} Days")

    if score_trend:
        df_trend = pd.DataFrame(score_trend)

        fig_trend = go.Figure()

        # Shaded min–max band
        fig_trend.add_trace(go.Scatter(
            x=list(df_trend["day"]) + list(df_trend["day"][::-1]),
            y=list(df_trend["max_score"]) + list(df_trend["min_score"][::-1]),
            fill="toself",
            fillcolor="rgba(99,102,241,0.10)",
            line=dict(color="rgba(0,0,0,0)"),
            showlegend=True,
            name="Min–Max Range",
            hoverinfo="skip",
        ))

        # Average line
        fig_trend.add_trace(go.Scatter(
            x=df_trend["day"],
            y=df_trend["avg_score"],
            mode="lines+markers",
            name="Avg Score",
            line=dict(color=_ACCENT, width=2.5, shape="spline"),
            marker=dict(size=7, color=_ACCENT, line=dict(color="#a5b4fc", width=1.5)),
            hovertemplate="<b>%{x}</b><br>Score: <b>%{y:.1f}</b><extra></extra>",
        ))

        # 80-point reference line
        fig_trend.add_hline(
            y=80,
            line=dict(color="#22c55e", width=1, dash="dot"),
            annotation_text="Target 80",
            annotation_position="top left",
            annotation_font=dict(color="#22c55e", size=10),
        )

        fig_trend.update_layout(
            height=270,
            margin=dict(l=0, r=10, t=10, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            yaxis=dict(
                title="Score (0–100)",
                range=[0, 105],
                gridcolor="rgba(51,65,85,0.6)",
                tickfont=dict(color="#64748b", size=10),
                zeroline=False,
            ),
            xaxis=dict(
                gridcolor="rgba(51,65,85,0.4)",
                tickfont=dict(color="#64748b", size=10),
            ),
            legend=dict(
                orientation="h",
                x=0, y=1.12,
                font=dict(color="#9ca3af", size=11),
                bgcolor="rgba(0,0,0,0)",
            ),
            font_color="#e5e7eb",
            font_family="Inter",
        )
        st.plotly_chart(fig_trend, use_container_width=True, config={"displayModeBar": False})
    else:
        _empty_panel("📈", "No score data in selected date range.")

# ── 4b. Pass / Fail Donut ────────────────────────────────────────────────────
with pf_col:
    st.subheader("✅ Pass / Fail Distribution")

    passed = kpis["passed_runs"]
    failed = kpis["failed_runs"]

    if kpis["total_runs"] > 0:
        fig_pf = go.Figure(go.Pie(
            labels=["Passed", "Failed"],
            values=[passed, failed],
            hole=0.62,
            marker=dict(
                colors=[_PASS_COLOR, _FAIL_COLOR],
                line=dict(color="rgba(0,0,0,0)", width=0),
            ),
            textinfo="percent+label",
            textfont=dict(size=12, color="#e5e7eb"),
            hovertemplate="%{label}: <b>%{value}</b> (%{percent})<extra></extra>",
        ))
        fig_pf.add_annotation(
            text=f"<b>{passed + failed}</b><br><span style='font-size:10px'>total</span>",
            x=0.5, y=0.5,
            font=dict(color="#e5e7eb", size=16, family="Inter"),
            showarrow=False,
        )
        fig_pf.update_layout(
            height=270,
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
        st.plotly_chart(fig_pf, use_container_width=True, config={"displayModeBar": False})
    else:
        _empty_panel(
            "✅", "No pass/fail data yet.",
            height=270, border_color="rgba(34,197,94,0.25)",
        )

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 5 — Metric Distribution  +  Column-wise Quality
# ─────────────────────────────────────────────────────────────────────────────

dist_col, col_qual_col = st.columns([1, 1], gap="large")

# ── 5a. Metric Distribution (latest rates) ───────────────────────────────────
with dist_col:
    st.subheader("📊 Error-Rate Breakdown  ·  Latest Run")

    _metrics_map = {
        "Null Rate":          kpis["latest_null_rate"],
        "Duplicate Rate":     kpis["latest_dup_rate"],
        "Type Mismatch Rate": kpis["latest_type_rate"],
        "Range Fail Rate":    kpis["latest_range_rate"],
    }
    _m_names  = [k for k, v in _metrics_map.items() if v is not None]
    _m_values = [v * 100 for k, v in _metrics_map.items() if v is not None]

    if _m_names:
        _bar_colors = [
            "#22c55e" if v < 5 else "#f59e0b" if v < 15 else "#ef4444"
            for v in _m_values
        ]
        fig_dist = go.Figure(go.Bar(
            x=_m_values,
            y=_m_names,
            orientation="h",
            marker=dict(
                color=_bar_colors,
                line=dict(color="rgba(0,0,0,0)", width=0),
            ),
            text=[f"{v:.2f}%" for v in _m_values],
            textposition="outside",
            textfont=dict(color="#e5e7eb", size=11),
            hovertemplate="%{y}: <b>%{x:.2f}%</b><extra></extra>",
        ))
        fig_dist.update_layout(
            height=240,
            margin=dict(l=0, r=40, t=10, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(
                title="Rate (%)",
                gridcolor="rgba(51,65,85,0.6)",
                tickfont=dict(color="#64748b", size=10),
                ticksuffix="%",
            ),
            yaxis=dict(
                tickfont=dict(color="#e5e7eb", size=11),
                autorange="reversed",
            ),
            font_color="#e5e7eb",
            font_family="Inter",
        )
        st.plotly_chart(fig_dist, use_container_width=True, config={"displayModeBar": False})
    else:
        _empty_panel("📊", "No metric data available for the selected dataset.", height=240)

# ── 5b. Column-wise Quality ──────────────────────────────────────────────────
with col_qual_col:
    st.subheader("🔬 Column-wise Quality  ·  Latest Run")

    if col_metrics:
        df_col = pd.DataFrame(col_metrics)
        df_col.columns = [c.lower() for c in df_col.columns]

        if {"column_name", "passed"}.issubset(df_col.columns):
            df_agg = (
                df_col.groupby("column_name")["passed"]
                .value_counts()
                .unstack(fill_value=0)
                .reset_index()
            )
            _true_col  = True  if True  in df_agg.columns else "True"
            _false_col = False if False in df_agg.columns else "False"
            df_agg["_pass"] = df_agg.get(_true_col,  0)
            df_agg["_fail"] = df_agg.get(_false_col, 0)
            df_agg = df_agg.sort_values("_pass", ascending=True)

            fig_col = go.Figure()
            fig_col.add_trace(go.Bar(
                x=df_agg["_pass"],
                y=df_agg["column_name"],
                name="Passed",
                orientation="h",
                marker_color=_PASS_COLOR,
                hovertemplate="%{y}: <b>%{x}</b> checks passed<extra></extra>",
            ))
            fig_col.add_trace(go.Bar(
                x=df_agg["_fail"],
                y=df_agg["column_name"],
                name="Failed",
                orientation="h",
                marker_color=_FAIL_COLOR,
                hovertemplate="%{y}: <b>%{x}</b> checks failed<extra></extra>",
            ))
            fig_col.update_layout(
                barmode="stack",
                height=240,
                margin=dict(l=0, r=10, t=10, b=0),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(
                    title="Check Count",
                    gridcolor="rgba(51,65,85,0.6)",
                    tickfont=dict(color="#64748b", size=10),
                ),
                yaxis=dict(tickfont=dict(color="#e5e7eb", size=10)),
                legend=dict(
                    orientation="h", x=0, y=1.1,
                    font=dict(color="#9ca3af", size=11),
                    bgcolor="rgba(0,0,0,0)",
                ),
                font_color="#e5e7eb",
                font_family="Inter",
            )
            st.plotly_chart(fig_col, use_container_width=True, config={"displayModeBar": False})
        else:
            _empty_panel(
                "🔬",
                "QUALITY_METRICS rows returned but lack expected columns.",
                height=240,
                border_color="rgba(239,68,68,0.25)",
            )
    else:
        _empty_panel(
            "🔬",
            "No column metrics — populate QUALITY_METRICS to see this chart.",
            height=240,
            border_color="rgba(99,102,241,0.25)",
        )

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6 — Run History Table
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("📋 Recent Quality Evaluations")

if run_history:
    display_rows = []
    for r in run_history:
        passed_val = r.get("PASSED")
        if passed_val is None:
            badge = "⚪ —"
        elif passed_val:
            badge = "🟢 Passed"
        else:
            badge = "🔴 Failed"

        def _pct(val: object) -> str:
            if val is None:
                return "—"
            try:
                return f"{float(val) * 100:.2f}%"
            except (TypeError, ValueError):
                return "—"

        def _score_fmt(val: object) -> str:
            if val is None:
                return "—"
            try:
                return f"{float(val):.1f}"
            except (TypeError, ValueError):
                return "—"

        display_rows.append({
            "Dataset":    str(r.get("DATASET_LABEL", "—")),
            "Score":      _score_fmt(r.get("QUALITY_SCORE")),
            "Status":     badge,
            "Rows":       f"{int(r['TOTAL_ROWS']):,}" if r.get("TOTAL_ROWS") else "—",
            "Null %":     _pct(r.get("NULL_RATE")),
            "Dup %":      _pct(r.get("DUPLICATE_RATE")),
            "Mismatch %": _pct(r.get("TYPE_MISMATCH_RATE")),
            "Range %":    _pct(r.get("RANGE_FAIL_RATE")),
            "Evaluated":  _fmt_ts(r.get("EVALUATED_AT")),
        })

    df_hist = pd.DataFrame(display_rows)

    def _color_status(val: str) -> str:
        if "Passed" in val:
            return f"color: {_PASS_COLOR}; font-weight: 600"
        if "Failed" in val:
            return f"color: {_FAIL_COLOR}; font-weight: 600"
        return "color: #6b7280"

    def _color_score(val: str) -> str:
        try:
            s = float(val)
            return f"color: {score_color(s)}; font-weight: 600"
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
                ("background-color", "rgba(99,102,241,0.15)"),
                ("color", "#a5b4fc"),
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
    st.info("No quality evaluation records found.", icon="📋")

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
        &nbsp;·&nbsp; Phase 5 · Page 3: Data Quality Dashboard
    </div>
    """,
    unsafe_allow_html=True,
)
