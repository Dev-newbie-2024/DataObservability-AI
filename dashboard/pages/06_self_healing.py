"""
dashboard/pages/06_self_healing.py
=====================================
Phase 5 -- Page 6: Self-Healing / Automated Recovery Dashboard

Sections
--------
1. Header          -- title, refresh button, last-updated timestamp.
2. Filters         -- Failure-type selector + Date-range picker (sidebar).
3. KPI Cards       -- Total Heals . Recovery Rate . Avg MTTR
                      Quarantined . Escalated.
4. Daily Trend     -- 30-day daily heal count + recovery rate (combo chart).
5. Failure Dist.   -- Donut: failure_type breakdown.
6. Recovery Status -- Bar: RESOLVED / FAILED / ESCALATED counts.
7. MTTR Trend      -- Daily avg MTTR (area chart).
8. Run History     -- Styled dataframe of HEAL_SUMMARY rows.

Tables read (read-only, no writes)
-----------------------------------
  HEAL_SUMMARY . HEAL_RUNS . HEAL_ACTIONS

Column reference (from migration 013_create_heal_tables.sql)
-------------------------------------------------------------
  HEAL_SUMMARY : id, dataset_id, pipeline_run_id, failure_type,
                 failure_reason, healing_strategy, recovery_status,
                 retry_count, mttr_seconds, actions_taken,
                 actions_succeeded, quarantined, evaluated_at
  recovery_status values : RESOLVED | FAILED | ESCALATED | PENDING
  failure_type    values : CONNECTION | QUALITY | DRIFT | SYSTEM

Note: st.set_page_config lives in app.py -- do NOT call it here.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.utils.snowflake_queries import (
    get_heal_action_history,
    get_heal_failure_distribution,
    get_heal_kpis,
    get_heal_run_history,
    get_heal_trend,
)
from dashboard.utils.styles import inject_global_css

# -- CSS -----------------------------------------------------------------------
inject_global_css()

# -- Constants -----------------------------------------------------------------
_ACCENT   = "#06b6d4"   # cyan  -- self-healing theme
_RESOLVED = "#22c55e"   # green
_FAILED   = "#ef4444"   # red
_ESCALATED= "#f59e0b"   # amber
_MUTED    = "#64748b"

_STATUS_COLORS: dict[str, str] = {
    "RESOLVED":  _RESOLVED,
    "FAILED":    _FAILED,
    "ESCALATED": _ESCALATED,
    "PENDING":   "#6b7280",
}

_FTYPE_COLORS: dict[str, str] = {
    "CONNECTION": "#6366f1",
    "QUALITY":    "#10b981",
    "DRIFT":      "#8b5cf6",
    "SYSTEM":     "#f59e0b",
}


# -- Helper: empty-state placeholder -------------------------------------------

def _empty_panel(
    icon: str,
    msg: str,
    height: int = 200,
    border_color: str = "rgba(6,182,212,0.25)",
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


# -- Helper: format timestamp --------------------------------------------------

def _fmt_ts(ts: object) -> str:
    if ts is None:
        return "--"
    try:
        return ts.strftime("%Y-%m-%d %H:%M") if hasattr(ts, "strftime") else str(ts)[:16]
    except Exception:
        return str(ts)[:16]


# -- Helper: format MTTR -------------------------------------------------------

def _fmt_mttr(seconds: float | None) -> str:
    """Convert raw seconds to a human-readable string."""
    if seconds is None:
        return "--"
    try:
        s = float(seconds)
        if s < 60:
            return f"{s:.1f} s"
        if s < 3600:
            return f"{s/60:.1f} min"
        return f"{s/3600:.2f} hr"
    except (TypeError, ValueError):
        return "--"


# -- Helper: shared Plotly layout ---------------------------------------------

def _base_layout(height: int = 270, **extra: object) -> dict:
    base = dict(
        height=height,
        margin=dict(l=0, r=10, t=10, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="#e5e7eb",
        font_family="Inter",
    )
    base.update(extra)
    return base


# =============================================================================
# SECTION 1 -- Header
# =============================================================================

header_col, refresh_col = st.columns([5, 1])
with header_col:
    st.markdown(
        """
        <div class="page-header">
            <h1>&#x1F527; Self-Healing &amp; Automated Recovery</h1>
            <p class="header-subtitle">
                Failure classification &nbsp;&middot;&nbsp; Recovery outcomes
                &nbsp;&middot;&nbsp; MTTR trends &nbsp;&middot;&nbsp; Action history
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
with refresh_col:
    st.markdown("<div style='margin-top:1.1rem'></div>", unsafe_allow_html=True)
    if st.button("\U0001f504 Refresh", use_container_width=True, key="heal_refresh"):
        st.cache_data.clear()
        st.rerun()
    st.caption(f"\U0001f550 {datetime.now(timezone.utc).strftime('%H:%M UTC')}")

st.divider()

# =============================================================================
# SECTION 2 -- Sidebar Filters
# =============================================================================

_FAILURE_TYPES = ["All Types", "CONNECTION", "QUALITY", "DRIFT", "SYSTEM"]

with st.sidebar:
    st.markdown("### \U0001f3f9 Filters")
    st.markdown("---")

    selected_ftype_label = st.selectbox(
        "\U0001f4cc Failure Type",
        _FAILURE_TYPES,
        index=0,
        key="heal_failure_type",
        help="Filter the history table to a specific failure classification.",
    )
    active_ftype: str | None = (
        None if selected_ftype_label == "All Types" else selected_ftype_label
    )

    st.markdown("---")

    _today = date.today()
    _default_start = _today - timedelta(days=29)

    date_range = st.date_input(
        "\U0001f4c5 Date Range",
        value=(_default_start, _today),
        min_value=_today - timedelta(days=365),
        max_value=_today,
        key="heal_date_range",
        help="Controls the look-back window for trend charts.",
    )

    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        _start_date, _end_date = date_range
        trend_days = max(1, (_end_date - _start_date).days + 1)
    else:
        trend_days = 30

    st.caption(f"Look-back window: **{trend_days} day(s)**")
    st.markdown("---")
    st.caption("Data refreshes every 60 s via `@st.cache_data`.")

# =============================================================================
# Fetch all data (cached)
# =============================================================================

with st.spinner("Loading self-healing metrics ..."):
    kpis          = get_heal_kpis()
    trend_data    = get_heal_trend(days=trend_days)
    failure_dist  = get_heal_failure_distribution()
    run_history   = get_heal_run_history(limit=20, failure_type=active_ftype)
    action_history= get_heal_action_history(limit=20)

no_data = kpis["total"] == 0

if no_data:
    st.info(
        "**No self-healing data yet.**  Run the Self-Healing Agent to populate "
        "HEAL_SUMMARY, HEAL_RUNS, and HEAL_ACTIONS.",
        icon="\u2139\ufe0f",
    )

# =============================================================================
# SECTION 3 -- KPI Cards
# =============================================================================

st.subheader("\U0001f4ca Self-Healing KPIs")

k1, k2, k3, k4, k5 = st.columns(5)

with k1:
    st.metric(
        "\U0001f527 Total Heals",
        kpis["total"],
        help="Total self-healing events recorded in HEAL_SUMMARY",
    )

with k2:
    _rr = f"{kpis['recovery_rate']:.1f}%" if kpis["recovery_rate"] is not None else "--"
    st.metric(
        "\u2705 Recovery Rate",
        _rr,
        delta=f"{kpis['resolved']} resolved",
        delta_color="normal",
        help="Percentage of heal events with recovery_status = RESOLVED",
    )

with k3:
    st.metric(
        "\u23f1\ufe0f Avg MTTR",
        _fmt_mttr(kpis["avg_mttr"]),
        help="Average Mean Time To Recovery (mttr_seconds) from HEAL_SUMMARY",
    )

with k4:
    st.metric(
        "\U0001f6ab Quarantined",
        kpis["quarantined"],
        delta=f"{kpis['failed']} failed",
        delta_color="inverse" if kpis["failed"] > 0 else "off",
        help="Datasets moved to quarantine state (quarantined = TRUE)",
    )

with k5:
    st.metric(
        "\u26a0\ufe0f Escalated",
        kpis["escalated"],
        delta="needs attention" if kpis["escalated"] > 0 else "none",
        delta_color="inverse" if kpis["escalated"] > 0 else "off",
        help="Events that exceeded retry limit and require manual intervention",
    )

st.divider()

# =============================================================================
# SECTION 4 -- Daily Trend  +  Failure Distribution
# =============================================================================

trend_col, dist_col = st.columns([3, 2], gap="large")

# -- 4a. Daily heal count + recovery rate combo chart -------------------------
with trend_col:
    st.subheader(f"\U0001f4c8 Daily Heal Trend  \u00b7  Last {trend_days} Days")

    if trend_data:
        df_trend = pd.DataFrame(trend_data)

        fig_trend = go.Figure()

        # Bar: total heals per day
        fig_trend.add_trace(go.Bar(
            x=df_trend["day"],
            y=df_trend["total"],
            name="Total Heals",
            marker_color="rgba(6,182,212,0.6)",
            yaxis="y1",
            hovertemplate="<b>%{x}</b><br>Total: <b>%{y}</b><extra></extra>",
        ))

        # Line: daily recovery rate (right axis)
        fig_trend.add_trace(go.Scatter(
            x=df_trend["day"],
            y=df_trend["recovery_rate"],
            name="Recovery Rate %",
            line=dict(color=_RESOLVED, width=2.5),
            mode="lines+markers",
            marker=dict(size=6, color=_RESOLVED),
            yaxis="y2",
            hovertemplate="<b>%{x}</b><br>Rate: <b>%{y:.1f}%</b><extra></extra>",
        ))

        fig_trend.update_layout(**_base_layout(
            height=290,
            yaxis=dict(
                title="Heals",
                gridcolor="rgba(51,65,85,0.5)",
                tickfont=dict(color=_ACCENT, size=10),
                zeroline=False,
            ),
            yaxis2=dict(
                title="Recovery %",
                overlaying="y",
                side="right",
                range=[0, 105],
                ticksuffix="%",
                tickfont=dict(color=_RESOLVED, size=10),
                zeroline=False,
                showgrid=False,
            ),
            xaxis=dict(
                gridcolor="rgba(51,65,85,0.3)",
                tickfont=dict(color=_MUTED, size=9),
                tickangle=45,
            ),
            legend=dict(
                orientation="h", x=0, y=1.12,
                font=dict(color="#9ca3af", size=11),
                bgcolor="rgba(0,0,0,0)",
            ),
        ))
        st.plotly_chart(fig_trend, use_container_width=True,
                        config={"displayModeBar": False})
    else:
        _empty_panel("\U0001f4c8", "No trend data in selected date range.", height=290)

# -- 4b. Failure type distribution (donut) ------------------------------------
with dist_col:
    st.subheader("\U0001f4cc Failure Type Distribution")

    if failure_dist:
        labels  = [d["failure_type"] for d in failure_dist]
        values  = [d["count"]        for d in failure_dist]
        colors  = [_FTYPE_COLORS.get(lbl, "#64748b") for lbl in labels]

        fig_donut = go.Figure(go.Pie(
            labels=labels,
            values=values,
            hole=0.52,
            marker=dict(
                colors=colors,
                line=dict(color="rgba(15,23,42,0.8)", width=2),
            ),
            textinfo="label+percent",
            textfont=dict(size=11, color="#e5e7eb"),
            hovertemplate="<b>%{label}</b><br>Count: <b>%{value}</b> "
                          "(%{percent})<extra></extra>",
        ))

        fig_donut.add_annotation(
            text=f"<b>{sum(values)}</b><br><span style='font-size:10px'>events</span>",
            x=0.5, y=0.5, font_size=18, showarrow=False,
            font_color="#e5e7eb",
        )

        fig_donut.update_layout(**_base_layout(
            height=290,
            showlegend=True,
            legend=dict(
                orientation="v", x=1.0, y=0.5,
                font=dict(color="#9ca3af", size=10),
                bgcolor="rgba(0,0,0,0)",
            ),
        ))
        st.plotly_chart(fig_donut, use_container_width=True,
                        config={"displayModeBar": False})
    else:
        _empty_panel("\U0001f4cc", "No failure distribution data yet.", height=290)

st.divider()

# =============================================================================
# SECTION 5 -- Recovery Status Breakdown  +  MTTR Trend
# =============================================================================

status_col, mttr_col = st.columns(2, gap="large")

# -- 5a. Recovery status breakdown (horizontal bar) ---------------------------
with status_col:
    st.subheader("\u2705 Recovery Status Breakdown")

    _status_data = [
        ("RESOLVED",  kpis["resolved"],   _RESOLVED),
        ("ESCALATED", kpis["escalated"],  _ESCALATED),
        ("FAILED",    kpis["failed"],     _FAILED),
    ]

    if kpis["total"] > 0:
        fig_bar = go.Figure()
        for status, count, color in _status_data:
            fig_bar.add_trace(go.Bar(
                y=[status],
                x=[count],
                orientation="h",
                name=status,
                marker_color=color,
                marker_line=dict(color="rgba(15,23,42,0.6)", width=1),
                text=[str(count)],
                textposition="outside",
                textfont=dict(color="#e5e7eb", size=11),
                hovertemplate=f"<b>{status}</b>: <b>%{{x}}</b><extra></extra>",
            ))

        fig_bar.update_layout(**_base_layout(
            height=220,
            barmode="overlay",
            xaxis=dict(
                title="Count",
                gridcolor="rgba(51,65,85,0.5)",
                tickfont=dict(color=_MUTED, size=10),
                zeroline=False,
            ),
            yaxis=dict(
                tickfont=dict(color="#e5e7eb", size=11),
                categoryorder="total ascending",
            ),
            showlegend=False,
        ))
        st.plotly_chart(fig_bar, use_container_width=True,
                        config={"displayModeBar": False})
    else:
        _empty_panel("\u2705", "No recovery status data yet.", height=220)

# -- 5b. MTTR trend (area chart) -----------------------------------------------
with mttr_col:
    st.subheader(f"\u23f1\ufe0f MTTR Trend  \u00b7  Last {trend_days} Days")

    _mttr_data = [d for d in trend_data if d.get("avg_mttr", 0) > 0]

    if _mttr_data:
        df_mttr = pd.DataFrame(_mttr_data)

        fig_mttr = go.Figure()
        fig_mttr.add_trace(go.Scatter(
            x=df_mttr["day"],
            y=df_mttr["avg_mttr"],
            fill="tozeroy",
            fillcolor="rgba(6,182,212,0.10)",
            line=dict(color=_ACCENT, width=2.5, shape="spline"),
            mode="lines+markers",
            name="Avg MTTR (s)",
            marker=dict(size=6, color=_ACCENT,
                        line=dict(color="#67e8f9", width=1.5)),
            hovertemplate="<b>%{x}</b><br>Avg MTTR: <b>%{y:.1f} s</b><extra></extra>",
        ))

        fig_mttr.update_layout(**_base_layout(
            yaxis=dict(
                title="Seconds",
                gridcolor="rgba(51,65,85,0.6)",
                tickfont=dict(color=_MUTED, size=10),
                zeroline=False,
            ),
            xaxis=dict(
                gridcolor="rgba(51,65,85,0.4)",
                tickfont=dict(color=_MUTED, size=10),
                tickangle=45,
            ),
        ))
        st.plotly_chart(fig_mttr, use_container_width=True,
                        config={"displayModeBar": False})
    else:
        _empty_panel("\u23f1\ufe0f", "No MTTR data in selected date range.")

st.divider()

# =============================================================================
# SECTION 6 -- Run History Table
# =============================================================================

st.subheader("\U0001f4cb Recent Heal Events")

if run_history:
    display_rows = []
    for r in run_history:
        _mttr_val = r.get("MTTR_SECONDS") or r.get("mttr_seconds")
        _rec_stat = str(r.get("RECOVERY_STATUS") or r.get("recovery_status") or "--")
        _ftype    = str(r.get("FAILURE_TYPE")    or r.get("failure_type")    or "--")

        _ds_id = str(r.get("DATASET_ID") or r.get("dataset_id") or "--")[:24]
        _strat = str(
            r.get("HEALING_STRATEGY") or r.get("healing_strategy") or "--"
        )
        display_rows.append({
            "Dataset":      _ds_id,
            "Failure Type": _ftype,
            "Strategy":     _strat,
            "Status":      _rec_stat,
            "Retries":     int(r.get("RETRY_COUNT")         or r.get("retry_count")        or 0),
            "MTTR":        _fmt_mttr(_mttr_val),
            "Actions \u2714": int(r.get("ACTIONS_SUCCEEDED") or r.get("actions_succeeded") or 0),
            "Quarantined": "\u2705" if (r.get("QUARANTINED") or r.get("quarantined")) else "\u2796",
            "Evaluated":   _fmt_ts(r.get("EVALUATED_AT")    or r.get("evaluated_at")),
        })

    df_hist = pd.DataFrame(display_rows)

    def _color_status(val: str) -> str:
        return f"color: {_STATUS_COLORS.get(val, '#9ca3af')}; font-weight: 600"

    def _color_ftype(val: str) -> str:
        return f"color: {_FTYPE_COLORS.get(val, '#9ca3af')}; font-weight: 600"

    styled = (
        df_hist.style
        .applymap(_color_status, subset=["Status"])
        .applymap(_color_ftype,  subset=["Failure Type"])
        .set_properties(**{
            "background-color": "transparent",
            "color": "#e5e7eb",
            "font-size": "0.83rem",
        })
        .set_table_styles([
            {"selector": "thead th", "props": [
                ("background-color", "rgba(6,182,212,0.15)"),
                ("color", "#67e8f9"),
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
    st.info("No heal events found for the selected filter.", icon="\U0001f4cb")

# =============================================================================
# SECTION 7 -- Action History (most recent heal run)
# =============================================================================

st.divider()
st.subheader("\u26a1 Latest Heal Run Actions")

if action_history:
    act_rows = []
    for a in action_history:
        _suc = a.get("SUCCESS") if a.get("SUCCESS") is not None else a.get("success")
        act_rows.append({
            "Action Type":   str(a.get("ACTION_TYPE")   or a.get("action_type")   or "--"),
            "Detail":        str(a.get("ACTION_DETAIL") or a.get("action_detail") or "--")[:120],
            "Success":       "\u2705" if _suc else "\u274c",
            "Executed At":   _fmt_ts(a.get("EXECUTED_AT") or a.get("executed_at")),
        })

    df_actions = pd.DataFrame(act_rows)

    def _color_success(val: str) -> str:
        if val == "\u2705":
            return f"color: {_RESOLVED}; font-weight:600"
        return f"color: {_FAILED}; font-weight:600"

    styled_act = (
        df_actions.style
        .applymap(_color_success, subset=["Success"])
        .set_properties(**{
            "background-color": "transparent",
            "color": "#e5e7eb",
            "font-size": "0.83rem",
        })
        .set_table_styles([
            {"selector": "thead th", "props": [
                ("background-color", "rgba(6,182,212,0.12)"),
                ("color", "#67e8f9"),
                ("font-size", "0.72rem"),
                ("font-weight", "600"),
                ("letter-spacing", "0.05em"),
                ("text-transform", "uppercase"),
                ("padding", "8px 12px"),
            ]},
            {"selector": "td", "props": [("padding", "7px 12px")]},
        ])
    )
    st.dataframe(styled_act, use_container_width=True, hide_index=True)
else:
    st.info(
        "No action detail available. Heal run actions are stored in HEAL_ACTIONS "
        "and will appear here once the Self-Healing Agent has processed at least one event.",
        icon="\u26a1",
    )

# =============================================================================
# Footer
# =============================================================================

st.divider()
st.markdown(
    """
    <div style="
        text-align:center;color:#475569;font-size:.72rem;
        padding-top:.75rem;
        border-top:1px solid rgba(71,85,105,.25);
    ">
        DataObservability-AI &nbsp;&middot;&nbsp; VTU Major Project -- ISE 2025-2026
        &nbsp;&middot;&nbsp; Phase 5 &middot; Page 6: Self-Healing Dashboard
    </div>
    """,
    unsafe_allow_html=True,
)
