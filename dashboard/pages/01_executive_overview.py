"""
dashboard/pages/01_executive_overview.py
==========================================
Phase 5 — Page 1: Executive Overview

Displays platform-level KPIs sourced from all four AI agents:

Sections
--------
1. Header          — title, last-refresh timestamp, manual refresh button.
2. Health Gauge    — Plotly arc gauge showing composite 0–100 platform score.
3. KPI Cards       — Pipeline Runs · Quality Score · Drift Severity ·
                     Cost Score · Self-Healing Success Rate.
4. Trend Chart     — 7-day quality score sparkline (Plotly).
5. Agent Status    — Per-agent last-run timestamp cards.
6. Architecture    — Technology + phase completion tables.
7. Quick Links     — External service buttons.

Note: ``st.set_page_config`` is NOT called here — it lives in ``app.py``.
"""

from __future__ import annotations

from datetime import datetime, timezone

import plotly.graph_objects as go
import streamlit as st

from dashboard.utils.snowflake_queries import (
    get_latest_cost_score,
    get_latest_drift_severity,
    get_latest_quality_score,
    get_overall_health,
    get_pipeline_run_stats,
    get_quality_trend,
    get_self_healing_success_rate,
)
from dashboard.utils.styles import inject_global_css

# ── Inject shared CSS ─────────────────────────────────────────────────────────
inject_global_css()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1 — Header
# ─────────────────────────────────────────────────────────────────────────────

header_col, refresh_col = st.columns([5, 1])

with header_col:
    st.markdown(
        """
        <div class="page-header">
            <h1>📊 Executive Overview</h1>
            <p class="header-subtitle">
                Dataset-Agnostic &nbsp;·&nbsp; Cost-Aware &nbsp;·&nbsp;
                Self-Healing Data Observability Platform
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

with refresh_col:
    st.markdown("<div style='margin-top:1.1rem'></div>", unsafe_allow_html=True)
    if st.button("🔄 Refresh", use_container_width=True, key="exec_overview_refresh"):
        st.cache_data.clear()
        st.rerun()
    utc_now = datetime.now(timezone.utc).strftime("%H:%M UTC")
    st.caption(f"🕐 {utc_now}")

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# Fetch all data (cached — one-minute TTL)
# ─────────────────────────────────────────────────────────────────────────────

with st.spinner("Loading observability data …"):
    pipeline_stats = get_pipeline_run_stats()
    quality        = get_latest_quality_score()
    drift          = get_latest_drift_severity()
    cost           = get_latest_cost_score()
    healing        = get_self_healing_success_rate()
    health         = get_overall_health()
    quality_trend  = get_quality_trend(days=7)

# ─────────────────────────────────────────────────────────────────────────────
# No-data banner
# ─────────────────────────────────────────────────────────────────────────────

if not health["any_data"]:
    st.info(
        "**No observability data yet.** Run the AI agents (Quality, Drift, Cost, "
        "Self-Healing) to populate metrics.  KPI cards will show **—** until "
        "data arrives.",
        icon="ℹ️",
    )

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2 — Overall Platform Health Gauge
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("🏥 Overall Platform Health")

health_score = health["score"]
health_label = health["label"] if health["any_data"] else "No Data"
health_color = health["color"] if health["any_data"] else "#6b7280"

# Arc gauge
fig_gauge = go.Figure(
    go.Indicator(
        mode="gauge+number",
        value=health_score,
        number={
            "suffix":   "/100",
            "font":     {"size": 30, "color": health_color, "family": "Inter"},
        },
        gauge={
            "axis": {
                "range":     [0, 100],
                "tickwidth": 1,
                "tickcolor": "#374151",
                "tickfont":  {"color": "#64748b", "size": 10},
                "nticks":    6,
            },
            "bar":       {"color": health_color, "thickness": 0.22},
            "bgcolor":   "rgba(0,0,0,0)",
            "borderwidth": 0,
            "steps": [
                {"range": [0, 50],   "color": "rgba(239,68,68,0.08)"},
                {"range": [50, 70],  "color": "rgba(245,158,11,0.08)"},
                {"range": [70, 85],  "color": "rgba(132,204,22,0.08)"},
                {"range": [85, 100], "color": "rgba(34,197,94,0.12)"},
            ],
            "threshold": {
                "line":      {"color": "#f59e0b", "width": 3},
                "thickness": 0.78,
                "value":     85,
            },
        },
        title={
            "text": f"<b>{health_label}</b>",
            "font": {"size": 17, "color": health_color, "family": "Inter"},
        },
    )
)
fig_gauge.update_layout(
    height=260,
    margin=dict(l=50, r=50, t=35, b=10),
    paper_bgcolor="rgba(0,0,0,0)",
    font_color="#e5e7eb",
)

# Centre the gauge with narrow column trick
_, gauge_col, _ = st.columns([1, 2, 1])
with gauge_col:
    st.plotly_chart(
        fig_gauge, use_container_width=True, config={"displayModeBar": False}
    )

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3 — KPI Cards
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("📈 Key Performance Indicators")

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

# ── 1. Pipeline Runs ──────────────────────────────────────────────────────────
with kpi1:
    total   = pipeline_stats["total"]
    success = pipeline_stats["success"]
    failed  = pipeline_stats["failed"]
    if total > 0:
        delta_str = f"✅ {success} ok  ❌ {failed} failed"
        delta_clr = "normal"
    else:
        delta_str = None
        delta_clr = "off"
    st.metric(
        label="🔄 Pipeline Runs",
        value=str(total) if total > 0 else "—",
        delta=delta_str,
        delta_color=delta_clr,
        help="Total pipeline runs in OBSERVABILITY.PIPELINE_RUNS",
    )

# ── 2. Latest Quality Score ───────────────────────────────────────────────────
with kpi2:
    q_score  = quality["score"]
    q_passed = quality["passed"]
    if q_passed is True:
        q_delta, q_delta_clr = "PASSED ✅", "normal"
    elif q_passed is False:
        q_delta, q_delta_clr = "FAILED ❌", "inverse"
    else:
        q_delta, q_delta_clr = None, "off"
    st.metric(
        label="✅ Quality Score",
        value=f"{q_score:.1f}" if q_score is not None else "—",
        delta=q_delta,
        delta_color=q_delta_clr,
        help="Latest data quality score (0–100) from DATA_QUALITY_METRICS",
    )

# ── 3. Drift Severity ─────────────────────────────────────────────────────────
with kpi3:
    d_sev   = drift["severity"] or "—"
    d_score = drift["score"]
    d_pct   = drift["drift_pct"]
    if d_pct is not None:
        d_delta     = f"{d_pct:.1f}% columns drifted"
        d_delta_clr = "inverse" if (drift["detected"] or False) else "normal"
    else:
        d_delta, d_delta_clr = None, "off"
    st.metric(
        label="📉 Drift Severity",
        value=d_sev,
        delta=d_delta,
        delta_color=d_delta_clr,
        help="Latest drift severity (LOW / MEDIUM / HIGH) from DRIFT_SUMMARY",
    )

# ── 4. Cost Score ─────────────────────────────────────────────────────────────
with kpi4:
    c_score = cost["score"]
    c_usd   = cost["total_usd"]
    if c_usd is not None:
        c_delta     = f"${c_usd:.2f} credits used"
        c_delta_clr = "normal"
    else:
        c_delta, c_delta_clr = None, "off"
    st.metric(
        label="💰 Cost Score",
        value=f"{c_score:.1f}" if c_score is not None else "—",
        delta=c_delta,
        delta_color=c_delta_clr,
        help="Latest cost efficiency score (0–100, higher = cheaper) from COST_SUMMARY",
    )

# ── 5. Self-Healing Success Rate ──────────────────────────────────────────────
with kpi5:
    h_rate  = healing["success_rate"]
    h_total = healing["total"]
    h_rec   = healing["recovered"]
    if h_total > 0:
        h_delta     = f"{h_rec} / {h_total} incidents healed"
        h_delta_clr = "normal"
    else:
        h_delta, h_delta_clr = None, "off"
    st.metric(
        label="🔧 Heal Success",
        value=f"{h_rate:.1f}%" if h_rate is not None else "—",
        delta=h_delta,
        delta_color=h_delta_clr,
        help="% of pipeline failures auto-healed by the Self-Healing Agent",
    )

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4 — Quality Trend + Agent Status
# ─────────────────────────────────────────────────────────────────────────────

trend_col, status_col = st.columns([3, 2], gap="large")

# ── 7-day quality trend ───────────────────────────────────────────────────────
with trend_col:
    st.subheader("📊 Quality Score — 7-Day Trend")

    if quality_trend:
        import pandas as pd

        df_trend = pd.DataFrame(quality_trend)

        fig_trend = go.Figure()

        # Fill area
        fig_trend.add_trace(
            go.Scatter(
                x=df_trend["day"],
                y=df_trend["avg_score"],
                mode="lines+markers",
                name="Avg Quality",
                line=dict(color="#6366f1", width=2.5, shape="spline"),
                marker=dict(
                    size=7,
                    color="#6366f1",
                    line=dict(color="#a5b4fc", width=1.5),
                ),
                fill="tozeroy",
                fillcolor="rgba(99,102,241,0.10)",
                hovertemplate="<b>%{x}</b><br>Score: <b>%{y:.1f}</b><extra></extra>",
            )
        )

        # Target threshold line
        fig_trend.add_hline(
            y=80,
            line_dash="dot",
            line_color="#f59e0b",
            annotation_text="Target 80",
            annotation_position="bottom right",
            annotation_font_color="#f59e0b",
            annotation_font_size=11,
        )

        fig_trend.update_layout(
            height=230,
            margin=dict(l=0, r=10, t=10, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            yaxis=dict(
                range=[0, 105],
                gridcolor="rgba(51,65,85,0.6)",
                tickfont=dict(color="#64748b", size=10),
                zeroline=False,
            ),
            xaxis=dict(
                gridcolor="rgba(51,65,85,0.4)",
                tickfont=dict(color="#64748b", size=10),
            ),
            showlegend=False,
            font_color="#e5e7eb",
            font_family="Inter",
        )

        st.plotly_chart(
            fig_trend, use_container_width=True, config={"displayModeBar": False}
        )
    else:
        st.markdown(
            """
            <div style="
                height:180px;
                display:flex;
                flex-direction:column;
                align-items:center;
                justify-content:center;
                background:rgba(30,41,59,0.5);
                border:1px dashed rgba(99,102,241,0.25);
                border-radius:12px;
                color:#64748b;
                font-size:0.875rem;
            ">
                <span style="font-size:2rem;margin-bottom:8px;">📈</span>
                <span>No trend data yet — run the Quality Agent to populate.</span>
            </div>
            """,
            unsafe_allow_html=True,
        )

# ── Agent Status cards ────────────────────────────────────────────────────────
with status_col:
    st.subheader("🤖 Agent Status")

    def _fmt_ts(ts: object) -> str:
        """Format a Snowflake timestamp (datetime / str / None) for display."""
        if ts is None:
            return "No data yet"
        try:
            # snowflake-connector returns datetime objects
            if hasattr(ts, "strftime"):
                return ts.strftime("%Y-%m-%d %H:%M")
            return str(ts)[:16]
        except Exception:
            return str(ts)[:16]

    _agents = [
        (
            "Quality Agent",
            quality["evaluated_at"],
            quality["score"] is not None,
            "DATA_QUALITY_METRICS",
        ),
        (
            "Drift Agent",
            drift["evaluated_at"],
            drift["severity"] is not None,
            "DRIFT_SUMMARY",
        ),
        (
            "Cost Agent",
            cost["evaluated_at"],
            cost["score"] is not None,
            "COST_SUMMARY",
        ),
        (
            "Self-Healing Agent",
            healing["evaluated_at"],
            healing["total"] > 0,
            "HEAL_SUMMARY",
        ),
    ]

    for agent_name, last_run_ts, has_data, _table in _agents:
        badge = "🟢" if has_data else "🔴"
        ts    = _fmt_ts(last_run_ts) if has_data else "No data yet"
        st.markdown(
            f"""
            <div class="agent-card">
                <span class="agent-badge">{badge}</span>
                <div>
                    <div class="agent-name">{agent_name}</div>
                    <div class="agent-ts">Last run: {ts}</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 5 — Metric Detail Row (Null / Duplicate / Drift-pct / MTTR)
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("🔬 Latest Metric Snapshot")

m1, m2, m3, m4 = st.columns(4)

with m1:
    nr = quality["null_rate"]
    st.metric(
        "Null Rate",
        f"{nr*100:.2f}%" if nr is not None else "—",
        help="Fraction of NULL cells in latest Silver dataset",
    )

with m2:
    dr = quality["duplicate_rate"]
    st.metric(
        "Duplicate Rate",
        f"{dr*100:.2f}%" if dr is not None else "—",
        help="Fraction of duplicated rows in latest Silver dataset",
    )

with m3:
    dp = drift["drift_pct"]
    st.metric(
        "Drifted Columns",
        (
            f"{int(drift['drifted_columns'])} / {int(drift['total_columns'])}"
            if drift["drifted_columns"] is not None
            else "—"
        ),
        delta=f"{dp:.1f}%" if dp is not None else None,
        delta_color="inverse" if (drift["detected"] or False) else "normal",
        help="Count of columns with detected drift vs total columns",
    )

with m4:
    mttr = healing["avg_mttr"]
    st.metric(
        "Avg MTTR",
        f"{mttr:.1f}s" if mttr is not None else "—",
        help="Mean time to recover (seconds) across all self-healing events",
    )

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6 — Platform Architecture tables
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("🏗️ Platform Architecture")

arch_left, arch_right = st.columns(2, gap="large")

with arch_left:
    st.markdown("**Infrastructure Layers**")
    st.markdown(
        """
| Layer | Technology | Status |
|-------|-----------|--------|
| **Ingestion** | Apache Kafka (KRaft) | ✅ Phase 3 |
| **Orchestration** | Apache Airflow 2.9 | ✅ Phase 3 |
| **Transformation** | dbt + Snowflake | ✅ Phase 1 |
| **Storage** | Snowflake Medallion | ✅ Phase 1 |
| **API** | FastAPI | ✅ Phase 2 |
| **Dashboard** | Streamlit + Plotly | ✅ Phase 5 |
        """
    )

with arch_right:
    st.markdown("**AI Agents**")
    st.markdown(
        """
| Agent | Checks | Status |
|-------|--------|--------|
| **Quality Agent** | Null · Duplicate · Type · Freshness | ✅ Phase 4A |
| **Drift Agent** | PSI · KS-test · Categorical | ✅ Phase 4B |
| **Cost Agent** | Credits · USD · 30d Forecast | ✅ Phase 4C |
| **Self-Healing Agent** | Retry · Quarantine · MTTR | ✅ Phase 4D |
        """
    )

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 7 — Quick Links
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("🔗 Quick Links")

lc1, lc2, lc3, lc4 = st.columns(4)

_backend_url = st.session_state.get("backend_url", "http://localhost:8000")

with lc1:
    st.link_button(
        "📚 FastAPI Docs",
        f"{_backend_url}/docs",
        use_container_width=True,
    )
with lc2:
    _airflow_url = st.session_state.get("airflow_url", "http://localhost:8081")
    st.link_button("✈️ Airflow UI", _airflow_url, use_container_width=True)
with lc3:
    st.link_button("📨 Kafka UI", "http://localhost:8090", use_container_width=True)
with lc4:
    st.link_button(
        "❤️ API Health",
        f"{_backend_url}/health",
        use_container_width=True,
    )

# Footer
st.markdown(
    """
    <div style="
        text-align: center;
        color: #475569;
        font-size: 0.72rem;
        margin-top: 2.5rem;
        padding-top: 1rem;
        border-top: 1px solid rgba(71,85,105,0.25);
    ">
        DataObservability-AI &nbsp;·&nbsp; VTU Major Project — Dept. of ISE 2025-2026
        &nbsp;·&nbsp; Phase 5 Dashboard
    </div>
    """,
    unsafe_allow_html=True,
)
