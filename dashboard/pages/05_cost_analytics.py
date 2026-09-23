"""
dashboard/pages/05_cost_analytics.py
=======================================
Phase 5  -- Page 5: Cost Analytics

Sections
--------
1. Header          -- title, refresh button, last-updated timestamp.
2. Filters         -- Warehouse selector + Date-range picker (sidebar).
3. KPI Cards       -- Cost Score . Total Credits . Total Cost (USD)
                     Daily Avg Credits . 30-Day Forecast Cost.
4. Credit Trend    -- 30-day daily credits used (Plotly area chart).
5. Cost Trend      -- 30-day daily cost USD (Plotly area chart).
6. Credits vs Cost -- Dual-axis combo bar+line chart.
7. Forecast        -- 30-day forecast line with confidence band.
8. History Table   -- Styled dataframe of COST_RECORDS rows.

Tables read (read-only, no writes)
-----------------------------------
  COST_SUMMARY . COST_RECORDS . COST_FORECASTS . COST_BUDGETS

Note: st.set_page_config lives in app.py -- do NOT call it here.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.utils.snowflake_queries import (
    get_cost_credit_trend,
    get_cost_forecasts,
    get_cost_kpis,
    get_cost_records,
    get_cost_warehouses,
)
from dashboard.utils.styles import inject_global_css

# -- CSS -----------------------------------------------------------------------
inject_global_css()

# -- Constants -----------------------------------------------------------------
_ACCENT = "#f59e0b"   # amber -- cost theme
_CREDIT = "#6366f1"   # indigo for credits
_COST   = "#10b981"   # emerald for cost USD
_WARN   = "#ef4444"   # red for over-budget
_MUTED  = "#64748b"


# -- Helper: empty-state placeholder -------------------------------------------

def _empty_panel(
    icon: str,
    msg: str,
    height: int = 200,
    border_color: str = "rgba(245,158,11,0.25)",
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


# -- Helper: format date -------------------------------------------------------

def _fmt_ts(ts: object) -> str:
    if ts is None:
        return "--"
    try:
        return ts.strftime("%Y-%m-%d") if hasattr(ts, "strftime") else str(ts)[:10]
    except Exception:
        return str(ts)[:10]


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
            <h1>&#128176; Cost Monitoring Dashboard</h1>
            <p class="header-subtitle">
                Credit usage &nbsp;&middot;&nbsp; Cost trends &nbsp;&middot;&nbsp;
                Warehouse breakdown &nbsp;&middot;&nbsp; 30-day forecast
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
with refresh_col:
    st.markdown("<div style='margin-top:1.1rem'></div>", unsafe_allow_html=True)
    if st.button("\U0001f504 Refresh", use_container_width=True, key="cost_refresh"):
        st.cache_data.clear()
        st.rerun()
    st.caption(f"\U0001f550 {datetime.now(timezone.utc).strftime('%H:%M UTC')}")

st.divider()

# =============================================================================
# SECTION 2 -- Sidebar Filters
# =============================================================================

with st.sidebar:
    st.markdown("### \U0001f39b\ufe0f Filters")
    st.markdown("---")

    with st.spinner("Loading warehouses ..."):
        _all_warehouses = get_cost_warehouses()

    wh_options = ["All Warehouses"] + _all_warehouses
    selected_wh_label = st.selectbox(
        "\U0001f3ed Warehouse",
        wh_options,
        index=0,
        key="cost_warehouse",
        help="Filter all charts and the history table to a single warehouse.",
    )
    active_warehouse: str | None = (
        None if selected_wh_label == "All Warehouses" else selected_wh_label
    )

    st.markdown("---")

    _today = date.today()
    _default_start = _today - timedelta(days=29)

    date_range = st.date_input(
        "\U0001f4c5 Date Range",
        value=(_default_start, _today),
        min_value=_today - timedelta(days=365),
        max_value=_today,
        key="cost_date_range",
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

with st.spinner("Loading cost metrics ..."):
    kpis         = get_cost_kpis(warehouse=active_warehouse)
    credit_trend = get_cost_credit_trend(days=trend_days, warehouse=active_warehouse)
    cost_records = get_cost_records(limit=20, warehouse=active_warehouse)
    forecasts    = get_cost_forecasts(warehouse=active_warehouse, days_ahead=30)

no_data = kpis["total_credits"] is None

if no_data:
    st.info(
        "**No cost data yet.**  Run the Cost Monitoring Agent to populate "
        "COST_SUMMARY, COST_RECORDS, COST_FORECASTS, and COST_BUDGETS.",
        icon="\u2139\ufe0f",
    )

# =============================================================================
# SECTION 3 -- KPI Cards
# =============================================================================

st.subheader("\U0001f4ca Cost KPIs")

k1, k2, k3, k4, k5 = st.columns(5)

_score        = kpis["cost_score"]
_tot_credits  = kpis["total_credits"]
_tot_cost     = kpis["total_cost_usd"]
_daily_avg    = kpis["daily_avg_credits"]
_forecast_usd = kpis["forecast_cost_usd"]

with k1:
    _sc = f"{_score:.1f}" if _score is not None else "--"
    st.metric(
        "\U0001f4af Cost Score",
        _sc,
        help="Average cost_score from COST_SUMMARY (0-100 scale)",
    )

with k2:
    _tc = f"{_tot_credits:,.2f}" if _tot_credits is not None else "--"
    st.metric(
        "\u26a1 Total Credits",
        _tc,
        delta=f"{trend_days}-day window",
        delta_color="off",
        help="Sum of credits_used from COST_SUMMARY in the selected window",
    )

with k3:
    _tcu = f"${_tot_cost:,.2f}" if _tot_cost is not None else "--"
    st.metric(
        "\U0001f4b5 Total Cost (USD)",
        _tcu,
        delta=f"{trend_days}-day window",
        delta_color="off",
        help="Sum of cost_usd from COST_SUMMARY in the selected window",
    )

with k4:
    _da = f"{_daily_avg:,.3f}" if _daily_avg is not None else "--"
    st.metric(
        "\U0001f4c5 Daily Avg Credits",
        _da,
        delta="credits / day",
        delta_color="off",
        help="Average daily credits_used from COST_SUMMARY",
    )

with k5:
    _fc = f"${_forecast_usd:,.2f}" if _forecast_usd is not None else "--"
    _fc_color = "inverse" if (_forecast_usd or 0) > (_tot_cost or 0) else "normal"
    st.metric(
        "\U0001f52e 30-Day Forecast",
        _fc,
        delta="next 30 days",
        delta_color=_fc_color,
        help="Sum of forecast_cost_usd from COST_FORECASTS for the next 30 days",
    )

st.divider()

# =============================================================================
# SECTION 4 -- Credit Trend  +  Cost Trend
# =============================================================================

# Aggregate trend by day (sum across warehouses)
_df_trend: pd.DataFrame = pd.DataFrame()
if credit_trend:
    _df_trend = (
        pd.DataFrame(credit_trend)
        .groupby("day", as_index=False)
        .agg({"credits_used": "sum", "cost_usd": "sum"})
        .sort_values("day")
    )

credit_col, cost_col = st.columns(2, gap="large")

# -- 4a. Credit Usage Trend ---------------------------------------------------
with credit_col:
    st.subheader(f"\u26a1 Credit Usage Trend  \u00b7  Last {trend_days} Days")

    if not _df_trend.empty:
        fig_cr = go.Figure()
        fig_cr.add_trace(go.Scatter(
            x=_df_trend["day"],
            y=_df_trend["credits_used"],
            fill="tozeroy",
            fillcolor="rgba(99,102,241,0.12)",
            line=dict(color=_CREDIT, width=2.5, shape="spline"),
            mode="lines+markers",
            name="Credits Used",
            marker=dict(size=6, color=_CREDIT,
                        line=dict(color="#a5b4fc", width=1.5)),
            hovertemplate="<b>%{x}</b><br>Credits: <b>%{y:,.3f}</b><extra></extra>",
        ))
        fig_cr.update_layout(**_base_layout(
            yaxis=dict(
                title="Credits",
                gridcolor="rgba(51,65,85,0.6)",
                tickfont=dict(color=_MUTED, size=10),
                zeroline=False,
            ),
            xaxis=dict(
                gridcolor="rgba(51,65,85,0.4)",
                tickfont=dict(color=_MUTED, size=10),
            ),
        ))
        st.plotly_chart(fig_cr, use_container_width=True,
                        config={"displayModeBar": False})
    else:
        _empty_panel("\u26a1", "No credit trend data in selected date range.")

# -- 4b. Cost (USD) Trend -----------------------------------------------------
with cost_col:
    st.subheader(f"\U0001f4b5 Cost Trend (USD)  \u00b7  Last {trend_days} Days")

    if not _df_trend.empty:
        fig_cost = go.Figure()
        fig_cost.add_trace(go.Scatter(
            x=_df_trend["day"],
            y=_df_trend["cost_usd"],
            fill="tozeroy",
            fillcolor="rgba(16,185,129,0.12)",
            line=dict(color=_COST, width=2.5, shape="spline"),
            mode="lines+markers",
            name="Cost USD",
            marker=dict(size=6, color=_COST,
                        line=dict(color="#6ee7b7", width=1.5)),
            hovertemplate="<b>%{x}</b><br>Cost: <b>$%{y:,.2f}</b><extra></extra>",
        ))
        fig_cost.update_layout(**_base_layout(
            yaxis=dict(
                title="Cost (USD)",
                gridcolor="rgba(51,65,85,0.6)",
                tickformat="$,.2f",
                tickfont=dict(color=_MUTED, size=10),
                zeroline=False,
            ),
            xaxis=dict(
                gridcolor="rgba(51,65,85,0.4)",
                tickfont=dict(color=_MUTED, size=10),
            ),
        ))
        st.plotly_chart(fig_cost, use_container_width=True,
                        config={"displayModeBar": False})
    else:
        _empty_panel("\U0001f4b5", "No cost trend data in selected date range.")

st.divider()

# =============================================================================
# SECTION 5 -- Credits vs Cost Comparison  +  30-Day Forecast
# =============================================================================

combo_col, forecast_col = st.columns([3, 2], gap="large")

# -- 5a. Credits vs Cost Combo Chart ------------------------------------------
with combo_col:
    st.subheader("\U0001f4ca Credits vs Cost (USD)  \u00b7  Comparison")

    if not _df_trend.empty:
        fig_combo = go.Figure()

        # Bar: credits (left y-axis)
        fig_combo.add_trace(go.Bar(
            x=_df_trend["day"],
            y=_df_trend["credits_used"],
            name="Credits Used",
            marker_color="rgba(99,102,241,0.75)",
            yaxis="y1",
            hovertemplate="<b>%{x}</b><br>Credits: <b>%{y:,.3f}</b><extra></extra>",
        ))

        # Line: cost USD (right y-axis)
        fig_combo.add_trace(go.Scatter(
            x=_df_trend["day"],
            y=_df_trend["cost_usd"],
            name="Cost USD",
            line=dict(color=_COST, width=2.5),
            mode="lines+markers",
            marker=dict(size=6, color=_COST),
            yaxis="y2",
            hovertemplate="<b>%{x}</b><br>Cost: <b>$%{y:,.2f}</b><extra></extra>",
        ))

        fig_combo.update_layout(**_base_layout(
            height=290,
            yaxis=dict(
                title="Credits",
                gridcolor="rgba(51,65,85,0.5)",
                tickfont=dict(color=_CREDIT, size=10),
                zeroline=False,
            ),
            yaxis2=dict(
                title="Cost (USD)",
                overlaying="y",
                side="right",
                tickformat="$,.2f",
                tickfont=dict(color=_COST, size=10),
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
        st.plotly_chart(fig_combo, use_container_width=True,
                        config={"displayModeBar": False})
    else:
        _empty_panel("\U0001f4ca", "No data for comparison chart.", height=290)

# -- 5b. 30-Day Forecast Chart ------------------------------------------------
with forecast_col:
    st.subheader("\U0001f52e 30-Day Cost Forecast")

    if forecasts:
        df_fc = pd.DataFrame(forecasts)

        fig_fc = go.Figure()

        # Confidence band
        fig_fc.add_trace(go.Scatter(
            x=list(df_fc["forecast_date"]) + list(df_fc["forecast_date"][::-1]),
            y=list(df_fc["confidence_upper"]) + list(df_fc["confidence_lower"][::-1]),
            fill="toself",
            fillcolor="rgba(245,158,11,0.10)",
            line=dict(color="rgba(0,0,0,0)"),
            showlegend=True,
            name="Confidence Band",
            hoverinfo="skip",
        ))

        # Forecast line
        fig_fc.add_trace(go.Scatter(
            x=df_fc["forecast_date"],
            y=df_fc["forecast_cost_usd"],
            mode="lines+markers",
            name="Forecast Cost",
            line=dict(color=_ACCENT, width=2.5, dash="dot"),
            marker=dict(size=6, color=_ACCENT),
            hovertemplate="<b>%{x}</b><br>Forecast: <b>$%{y:,.2f}</b><extra></extra>",
        ))

        fig_fc.update_layout(**_base_layout(
            height=290,
            yaxis=dict(
                title="Cost (USD)",
                gridcolor="rgba(51,65,85,0.6)",
                tickformat="$,.2f",
                tickfont=dict(color=_MUTED, size=10),
                zeroline=False,
            ),
            xaxis=dict(
                gridcolor="rgba(51,65,85,0.4)",
                tickfont=dict(color=_MUTED, size=9),
                tickangle=30,
            ),
            legend=dict(
                orientation="h", x=0, y=1.12,
                font=dict(color="#9ca3af", size=11),
                bgcolor="rgba(0,0,0,0)",
            ),
        ))
        st.plotly_chart(fig_fc, use_container_width=True,
                        config={"displayModeBar": False})
    else:
        _empty_panel(
            "\U0001f52e",
            "No forecast data -- populate COST_FORECASTS to see this chart.",
            height=290,
        )

st.divider()

# =============================================================================
# SECTION 6 -- Cost History Table
# =============================================================================

st.subheader("\U0001f4cb Cost Records  \u00b7  Recent History")

if cost_records:
    display_rows = []
    for r in cost_records:
        def _fmt_num(val: object, fmt: str = ".3f") -> str:
            if val is None:
                return "--"
            try:
                return format(float(val), fmt)
            except (TypeError, ValueError):
                return "--"

        display_rows.append({
            "Warehouse":    str(r.get("WAREHOUSE_NAME", "--")),
            "Service Type": str(r.get("SERVICE_TYPE", "--")),
            "Credits Used": _fmt_num(r.get("CREDITS_USED"), ".4f"),
            "Cost (USD)":   f"${_fmt_num(r.get('COST_USD'), '.2f')}",
            "Usage Date":   _fmt_ts(r.get("USAGE_DATE")),
            "Created At":   _fmt_ts(r.get("CREATED_AT")),
        })

    df_hist = pd.DataFrame(display_rows)

    def _color_cost(val: str) -> str:
        try:
            v = float(val.replace("$", "").replace(",", ""))
            if v > 1000:
                return f"color: {_WARN}; font-weight: 600"
            if v > 100:
                return "color: #f59e0b; font-weight: 600"
            return f"color: {_COST}; font-weight: 600"
        except (TypeError, ValueError):
            return "color: #6b7280"

    def _color_credits(val: str) -> str:
        try:
            v = float(val)
            if v > 10:
                return f"color: {_WARN}; font-weight: 600"
            if v > 1:
                return "color: #f59e0b; font-weight: 600"
            return f"color: {_CREDIT}; font-weight: 600"
        except (TypeError, ValueError):
            return "color: #6b7280"

    styled = (
        df_hist.style
        .applymap(_color_cost,    subset=["Cost (USD)"])
        .applymap(_color_credits, subset=["Credits Used"])
        .set_properties(**{
            "background-color": "transparent",
            "color": "#e5e7eb",
            "font-size": "0.83rem",
        })
        .set_table_styles([
            {"selector": "thead th", "props": [
                ("background-color", "rgba(245,158,11,0.15)"),
                ("color", "#fcd34d"),
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
    st.info("No cost records found.", icon="\U0001f4cb")

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
        &nbsp;&middot;&nbsp; Phase 5C &middot; Page 5: Cost Monitoring Dashboard
    </div>
    """,
    unsafe_allow_html=True,
)
