"""
dashboard/pages/02_pipeline_monitoring.py
==========================================
Phase 5 — Page 2: Pipeline Monitoring

Displays the Bronze → Silver → Gold pipeline health with five sections:

1. Header          — title, refresh, last-updated timestamp.
2. KPI Cards       — Total Runs · Success · Failed · Running · Avg Duration.
3. Medallion Flow  — Plotly Sankey diagram (Bronze → Silver → Gold row volumes)
                     + layer stat cards (rows per layer, rejection count).
4. DAG Runs Table  — Recent runs with status badges + colour-coded rows.
5. Duration Trend  — 14-day avg/min/max pipeline duration (Plotly area chart).
6. Task Timeline   — Gantt-style horizontal bar chart of the last 5 runs' tasks.

Tables read (read-only, no writes)
-----------------------------------
  OBSERVABILITY.PIPELINE_RUNS
  OBSERVABILITY.PIPELINE_TASKS
  OBSERVABILITY.DATASET_LINEAGE

Note: ``st.set_page_config`` lives in ``app.py`` — do NOT call it here.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard.utils.snowflake_queries import (
    get_lineage_summary,
    get_pipeline_duration_trend,
    get_pipeline_kpis,
    get_pipeline_layer_stats,
    get_pipeline_task_timeline,
    get_recent_dag_runs,
)
from dashboard.utils.styles import inject_global_css

# ── CSS ───────────────────────────────────────────────────────────────────────
inject_global_css()

# ── Colours ───────────────────────────────────────────────────────────────────
_STATUS_COLOR: dict[str, str] = {
    "success":  "#22c55e",
    "failed":   "#ef4444",
    "error":    "#ef4444",
    "running":  "#6366f1",
    "skipped":  "#6b7280",
    "queued":   "#f59e0b",
    "upstream_failed": "#f97316",
}
_LAYER_COLOR: dict[str, str] = {
    "BRONZE": "#b45309",
    "SILVER": "#6b7280",
    "GOLD":   "#ca8a04",
}
_PLOTLY_TEMPLATE = "plotly_dark"


# ── Reusable empty-state panels ───────────────────────────────────────────────

def _flow_empty() -> None:
    """Render placeholder when no lineage data exists."""
    st.markdown(
        """
        <div style="
            height:220px;display:flex;flex-direction:column;
            align-items:center;justify-content:center;
            background:rgba(30,41,59,0.5);
            border:1px dashed rgba(180,83,9,0.35);
            border-radius:12px;color:#64748b;font-size:.875rem;
        ">
            <span style="font-size:2rem;margin-bottom:8px">🏅</span>
            <span>Run the ingestion DAG to populate the Bronze → Silver → Gold flow.</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _timeline_empty() -> None:
    """Render placeholder when no task timeline data exists."""
    st.markdown(
        """
        <div style="
            height:160px;display:flex;flex-direction:column;
            align-items:center;justify-content:center;
            background:rgba(30,41,59,0.5);
            border:1px dashed rgba(99,102,241,0.25);
            border-radius:12px;color:#64748b;font-size:.875rem;
        ">
            <span style="font-size:2rem;margin-bottom:8px">🗂️</span>
            <span>No task-level data — populate PIPELINE_TASKS to see the Gantt chart.</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1 — Header
# ─────────────────────────────────────────────────────────────────────────────

header_col, refresh_col = st.columns([5, 1])
with header_col:
    st.markdown(
        """
        <div class="page-header">
            <h1>🔄 Pipeline Monitoring</h1>
            <p class="header-subtitle">
                Bronze → Silver → Gold &nbsp;·&nbsp; DAG runs &nbsp;·&nbsp;
                Task execution &nbsp;·&nbsp; Duration trends
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
with refresh_col:
    st.markdown("<div style='margin-top:1.1rem'></div>", unsafe_allow_html=True)
    if st.button("🔄 Refresh", use_container_width=True, key="pipeline_refresh"):
        st.cache_data.clear()
        st.rerun()
    st.caption(f"🕐 {datetime.now(timezone.utc).strftime('%H:%M UTC')}")

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# Fetch all data (cached)
# ─────────────────────────────────────────────────────────────────────────────
with st.spinner("Loading pipeline metrics …"):
    kpis         = get_pipeline_kpis()
    dag_runs     = get_recent_dag_runs(limit=15)
    dur_trend    = get_pipeline_duration_trend(days=14)
    task_rows    = get_pipeline_task_timeline(limit_runs=5)
    lineage_agg  = get_lineage_summary(days=30)
    layer_stats  = get_pipeline_layer_stats()

no_data = kpis["total"] == 0 and layer_stats["bronze_rows"] is None

if no_data:
    st.info(
        "**No pipeline data yet.** Run the ingestion DAG to populate "
        "PIPELINE_RUNS, PIPELINE_TASKS, and DATASET_LINEAGE.",
        icon="ℹ️",
    )

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2 — KPI Cards
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("📈 Pipeline KPIs")

k1, k2, k3, k4, k5 = st.columns(5)

with k1:
    st.metric(
        "🔄 Total Runs",
        kpis["total"] or "—",
        help="All-time pipeline run count from PIPELINE_RUNS",
    )

with k2:
    rate = kpis["success_rate"]
    st.metric(
        "✅ Success Rate",
        f"{rate:.1f}%" if rate is not None else "—",
        delta=f"{kpis['success']} successful",
        delta_color="normal" if kpis["success"] > 0 else "off",
        help="Percentage of runs that completed with status = 'success'",
    )

with k3:
    failed = kpis["failed"]
    st.metric(
        "❌ Failed Runs",
        str(failed) if kpis["total"] > 0 else "—",
        delta="↓ needs attention" if failed > 0 else "✔ all clear",
        delta_color="inverse" if failed > 0 else "normal",
        help="Runs with status 'failed' or 'error'",
    )

with k4:
    running = kpis["running"]
    st.metric(
        "⚡ Running",
        str(running) if kpis["total"] > 0 else "—",
        delta="active" if running > 0 else "idle",
        delta_color="normal" if running > 0 else "off",
        help="Runs currently in 'running' status",
    )

with k5:
    avg_dur = kpis["avg_duration_sec"]
    if avg_dur is not None:
        if avg_dur >= 60:
            dur_label = f"{avg_dur / 60:.1f} min"
        else:
            dur_label = f"{avg_dur:.0f}s"
    else:
        dur_label = "—"
    st.metric(
        "⏱️ Avg Duration",
        dur_label,
        help="Average wall-clock time from started_at to completed_at",
    )

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3 — Bronze → Silver → Gold Flow
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("🏅 Medallion Data Flow  ·  Bronze → Silver → Gold")

flow_chart_col, layer_cards_col = st.columns([3, 2], gap="large")

# ── Sankey / flow diagram ─────────────────────────────────────────────────────
with flow_chart_col:
    if lineage_agg:
        _layers  = ["BRONZE", "SILVER", "GOLD"]
        _node_lbl= ["🟤 Bronze", "⬜ Silver", "🟡 Gold", "🗑️ Rejected"]
        _node_clr= ["#b45309",   "#6b7280",   "#ca8a04",  "#ef4444"]
        _lyr_idx = {lyr: i for i, lyr in enumerate(_layers)}

        sources, targets, values, link_labels = [], [], [], []

        for row in lineage_agg:
            src = row["source_layer"].upper()
            tgt = row["target_layer"].upper()
            if src not in _lyr_idx or tgt not in _lyr_idx:
                continue
            # Forward flow
            if row["total_rows_out"] > 0:
                sources.append(_lyr_idx[src])
                targets.append(_lyr_idx[tgt])
                values.append(row["total_rows_out"])
                link_labels.append(f"{row['total_rows_out']:,} rows ({row['run_count']} runs)")
            # Rejected flow → node 3
            if row["total_rows_rejected"] > 0:
                sources.append(_lyr_idx[src])
                targets.append(3)   # "Rejected" node
                values.append(row["total_rows_rejected"])
                link_labels.append(f"{row['total_rows_rejected']:,} rejected")

        if sources:
            fig_sankey = go.Figure(go.Sankey(
                arrangement="snap",
                node=dict(
                    pad=18,
                    thickness=22,
                    line=dict(color="rgba(0,0,0,0)", width=0),
                    label=_node_lbl,
                    color=_node_clr,
                    hovertemplate="%{label}<br>Total flow: %{value:,}<extra></extra>",
                ),
                link=dict(
                    source=sources,
                    target=targets,
                    value=values,
                    label=link_labels,
                    color=[
                        "rgba(180,83,9,0.35)",    # bronze→silver
                        "rgba(107,114,128,0.35)",  # silver→gold
                        "rgba(239,68,68,0.35)",    # →rejected
                        "rgba(239,68,68,0.35)",
                    ][:len(sources)],
                    hovertemplate="%{label}<extra></extra>",
                ),
            ))
            fig_sankey.update_layout(
                height=300,
                margin=dict(l=0, r=0, t=10, b=0),
                paper_bgcolor="rgba(0,0,0,0)",
                font=dict(color="#e5e7eb", family="Inter", size=12),
            )
            st.plotly_chart(
                fig_sankey, use_container_width=True, config={"displayModeBar": False}
            )
        else:
            _flow_empty()
    else:
        _flow_empty()


# ── Layer stat cards ──────────────────────────────────────────────────────────
with layer_cards_col:
    st.markdown("**Rows per Layer (all-time)**")

    def _layer_card(emoji: str, label: str, rows: int | None, color: str) -> None:
        val = f"{rows:,}" if rows is not None else "—"
        st.markdown(
            f"""
            <div style="
                display:flex;align-items:center;gap:12px;
                padding:12px 16px;
                background:rgba(30,41,59,0.85);
                border:1px solid rgba(255,255,255,0.08);
                border-left:4px solid {color};
                border-radius:10px;margin-bottom:8px;
            ">
                <span style="font-size:1.4rem">{emoji}</span>
                <div>
                    <div style="font-size:.72rem;font-weight:600;letter-spacing:.05em;
                                text-transform:uppercase;color:#94a3b8">{label}</div>
                    <div style="font-size:1.3rem;font-weight:700;color:#f1f5f9">{val}</div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    _layer_card("🟤", "Bronze (raw rows in)",  layer_stats["bronze_rows"],   "#b45309")
    _layer_card("⬜", "Silver (merged rows)",   layer_stats["silver_rows"],   "#6b7280")
    _layer_card("🟡", "Gold (aggregated rows)", layer_stats["gold_rows"],     "#ca8a04")

    rejected = layer_stats["total_rejected"]
    _layer_card(
        "🗑️", "Quarantined / Rejected",
        rejected if rejected is not None else None,
        "#ef4444",
    )

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4 — Recent DAG Runs Table
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("📋 Recent DAG Runs")

if dag_runs:
    # Build display dataframe
    display_rows = []
    for r in dag_runs:
        status_raw = str(r.get("STATUS", "")).lower()
        badge_map  = {
            "success": "🟢 success",
            "failed":  "🔴 failed",
            "error":   "🔴 error",
            "running": "🔵 running",
            "skipped": "⚫ skipped",
            "queued":  "🟡 queued",
        }
        badge = badge_map.get(status_raw, f"⚪ {status_raw}")

        dur = r.get("DURATION_SEC")
        if dur is not None:
            dur_fmtd = f"{dur/60:.1f} min" if float(dur) >= 60 else f"{float(dur):.0f}s"
        else:
            dur_fmtd = "—"

        def _fmt_ts(ts: object) -> str:
            if ts is None:
                return "—"
            try:
                return ts.strftime("%m-%d %H:%M") if hasattr(ts, "strftime") else str(ts)[:16]
            except Exception:
                return str(ts)[:16]

        display_rows.append({
            "DAG":       r.get("DAG_ID", "—"),
            "Run ID":    (lambda v: v[:22] + "…" if len(v) > 22 else v)(str(r.get("RUN_ID", "—"))),
            "Status":    badge,
            "Started":   _fmt_ts(r.get("STARTED_AT")),
            "Completed": _fmt_ts(r.get("COMPLETED_AT")),
            "Duration":  dur_fmtd,
            "Rows In":   f"{int(r['ROWS_INGESTED']):,}" if r.get("ROWS_INGESTED") else "—",
            "Rejected":  f"{int(r['ROWS_REJECTED']):,}" if r.get("ROWS_REJECTED") else "—",
        })

    df_runs = pd.DataFrame(display_rows)

    # Colour-coded status column via styler
    def _color_status(val: str) -> str:
        key = val.split()[-1] if val.startswith(("🟢","🔴","🔵","⚫","🟡","⚪")) else val
        c = _STATUS_COLOR.get(key, "#6b7280")
        return f"color: {c}; font-weight: 600"

    styled = (
        df_runs.style
        .applymap(_color_status, subset=["Status"])
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
    st.info("No DAG runs recorded yet.", icon="📋")

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 5 — Pipeline Duration Trend
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("⏱️ Pipeline Duration — 14-Day Trend")

if dur_trend:
    df_dur = pd.DataFrame(dur_trend)

    # Convert seconds to minutes for readability
    for col in ("avg_sec", "min_sec", "max_sec"):
        df_dur[col] = df_dur[col] / 60.0

    fig_dur = go.Figure()

    # Shaded min–max band
    fig_dur.add_trace(go.Scatter(
        x=list(df_dur["day"]) + list(df_dur["day"][::-1]),
        y=list(df_dur["max_sec"]) + list(df_dur["min_sec"][::-1]),
        fill="toself",
        fillcolor="rgba(99,102,241,0.08)",
        line=dict(color="rgba(0,0,0,0)"),
        showlegend=True,
        name="Min–Max Range",
        hoverinfo="skip",
    ))

    # Avg line
    fig_dur.add_trace(go.Scatter(
        x=df_dur["day"],
        y=df_dur["avg_sec"],
        mode="lines+markers",
        name="Avg Duration",
        line=dict(color="#6366f1", width=2.5, shape="spline"),
        marker=dict(size=7, color="#6366f1", line=dict(color="#a5b4fc", width=1.5)),
        hovertemplate="<b>%{x}</b><br>Avg: <b>%{y:.1f} min</b><extra></extra>",
    ))

    fig_dur.update_layout(
        height=240,
        margin=dict(l=0, r=10, t=10, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend=dict(
            orientation="h",
            x=0, y=1.12,
            font=dict(color="#9ca3af", size=11),
            bgcolor="rgba(0,0,0,0)",
        ),
        yaxis=dict(
            title="Duration (min)",
            gridcolor="rgba(51,65,85,0.6)",
            tickfont=dict(color="#64748b", size=10),
            zeroline=False,
        ),
        xaxis=dict(
            gridcolor="rgba(51,65,85,0.4)",
            tickfont=dict(color="#64748b", size=10),
        ),
        font_color="#e5e7eb",
        font_family="Inter",
    )
    st.plotly_chart(fig_dur, use_container_width=True, config={"displayModeBar": False})
else:
    st.markdown(
        """
        <div style="
            height:160px;display:flex;flex-direction:column;
            align-items:center;justify-content:center;
            background:rgba(30,41,59,0.5);
            border:1px dashed rgba(99,102,241,0.25);
            border-radius:12px;color:#64748b;font-size:.875rem;
        ">
            <span style="font-size:2rem;margin-bottom:8px">⏱️</span>
            <span>No completed runs — trend appears after the first DAG run.</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6 — Task Execution Timeline (Gantt)
# ─────────────────────────────────────────────────────────────────────────────

st.subheader("🗂️ Task Execution Timeline  ·  Last 5 Runs")

if task_rows:
    df_tasks = pd.DataFrame(task_rows)

    # Normalise column names (Snowflake returns uppercase)
    df_tasks.columns = [c.lower() for c in df_tasks.columns]

    # Drop rows with no timestamps
    df_tasks = df_tasks.dropna(subset=["started_at"])
    df_tasks["completed_at"] = df_tasks.apply(
        lambda r: r["completed_at"] if pd.notna(r.get("completed_at")) else r["started_at"],
        axis=1,
    )

    if not df_tasks.empty:
        # Short run label for y-axis
        df_tasks["run_label"] = df_tasks["dag_id"].fillna("run") + " · " + \
            df_tasks["run_id"].astype(str).str[-8:]

        # Status colour mapping for Plotly
        df_tasks["color"] = df_tasks["status"].str.lower().map(
            lambda s: _STATUS_COLOR.get(s, "#6b7280")
        )

        # Plotly timeline (Gantt-style)
        fig_gantt = px.timeline(
            df_tasks,
            x_start="started_at",
            x_end="completed_at",
            y="task_name",
            color="status",
            color_discrete_map=_STATUS_COLOR,
            facet_row="run_label",
            labels={"task_name": "Task", "status": "Status"},
            template=_PLOTLY_TEMPLATE,
        )
        fig_gantt.update_yaxes(autorange="reversed", showgrid=False)
        fig_gantt.update_xaxes(showgrid=True, gridcolor="rgba(51,65,85,0.5)")
        fig_gantt.update_layout(
            height=max(320, len(df_tasks["run_label"].unique()) * 140),
            margin=dict(l=0, r=10, t=40, b=0),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            legend=dict(
                orientation="h", x=0, y=-0.08,
                font=dict(color="#9ca3af", size=11),
                bgcolor="rgba(0,0,0,0)",
                title_text="",
            ),
            font_color="#e5e7eb",
            font_family="Inter",
        )
        st.plotly_chart(
            fig_gantt, use_container_width=True, config={"displayModeBar": False}
        )
    else:
        _timeline_empty()
else:
    _timeline_empty()


def _timeline_empty() -> None:  # noqa: E302
    st.markdown(
        """
        <div style="
            height:160px;display:flex;flex-direction:column;
            align-items:center;justify-content:center;
            background:rgba(30,41,59,0.5);
            border:1px dashed rgba(99,102,241,0.25);
            border-radius:12px;color:#64748b;font-size:.875rem;
        ">
            <span style="font-size:2rem;margin-bottom:8px">🗂️</span>
            <span>No task-level data yet — populate PIPELINE_TASKS to see the Gantt chart.</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


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
        &nbsp;·&nbsp; Phase 5 · Page 2: Pipeline Monitoring
    </div>
    """,
    unsafe_allow_html=True,
)
