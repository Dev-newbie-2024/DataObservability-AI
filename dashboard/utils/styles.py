"""
dashboard/utils/styles.py
==========================
Global CSS injection and style helpers for the Phase 5 Streamlit dashboard.

Public surface
--------------
* ``inject_global_css()``    — inject shared dark-mode stylesheet once per session.
* ``severity_color(sev)``    — map a severity string to a hex colour.
* ``score_color(score)``     — map a 0–100 score to a traffic-light hex colour.
"""

from __future__ import annotations

import streamlit as st

# ── Global dark-mode stylesheet ────────────────────────────────────────────────

_GLOBAL_CSS: str = """
<style>
/* ── Fonts ──────────────────────────────────────────────────────────────────── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

/* ── Root variables ──────────────────────────────────────────────────────────── */
:root {
    --bg-primary:      #0f172a;
    --bg-secondary:    #1e293b;
    --bg-card:         rgba(30, 41, 59, 0.85);
    --bg-card-hover:   rgba(51, 65, 85, 0.9);
    --border:          rgba(148, 163, 184, 0.12);
    --border-hover:    rgba(99, 102, 241, 0.45);
    --text-primary:    #f1f5f9;
    --text-secondary:  #94a3b8;
    --text-muted:      #64748b;
    --accent-indigo:   #6366f1;
    --accent-violet:   #8b5cf6;
    --accent-emerald:  #10b981;
    --accent-amber:    #f59e0b;
    --accent-rose:     #f43f5e;
    --radius:          12px;
    --shadow:          0 4px 24px rgba(0, 0, 0, 0.35);
    --shadow-hover:    0 8px 40px rgba(99, 102, 241, 0.18);
}

/* ── App-wide background ─────────────────────────────────────────────────────── */
[data-testid="stAppViewContainer"] {
    background: linear-gradient(135deg, #0f172a 0%, #111827 50%, #0f1729 100%);
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
}

[data-testid="stHeader"] {
    background: transparent;
}

[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0f172a 0%, #1a2236 100%);
    border-right: 1px solid var(--border);
}

/* ── Main content area padding ───────────────────────────────────────────────── */
[data-testid="stMainBlockContainer"] {
    padding: 1.5rem 2rem 3rem 2rem;
}

/* ── Metric cards ─────────────────────────────────────────────────────────────── */
[data-testid="metric-container"] {
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 1.1rem 1.2rem;
    backdrop-filter: blur(12px);
    transition: transform 0.2s ease, box-shadow 0.2s ease, border-color 0.2s ease;
}

[data-testid="metric-container"]:hover {
    transform: translateY(-3px);
    box-shadow: var(--shadow-hover);
    border-color: var(--border-hover);
}

[data-testid="stMetricLabel"] > div {
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    letter-spacing: 0.04em !important;
    color: var(--text-secondary) !important;
    text-transform: uppercase;
}

[data-testid="stMetricValue"] {
    font-size: 1.75rem !important;
    font-weight: 700 !important;
    color: var(--text-primary) !important;
}

/* ── Dividers ─────────────────────────────────────────────────────────────────── */
hr {
    border-color: var(--border) !important;
    margin: 1.25rem 0 !important;
}

/* ── Buttons ─────────────────────────────────────────────────────────────────── */
[data-testid="baseButton-secondary"] {
    background: rgba(99, 102, 241, 0.12) !important;
    border: 1px solid rgba(99, 102, 241, 0.3) !important;
    color: #a5b4fc !important;
    border-radius: 8px !important;
    font-weight: 500 !important;
    transition: all 0.2s ease !important;
}

[data-testid="baseButton-secondary"]:hover {
    background: rgba(99, 102, 241, 0.22) !important;
    border-color: rgba(99, 102, 241, 0.55) !important;
    box-shadow: 0 0 16px rgba(99, 102, 241, 0.2) !important;
}

/* ── Info / alert boxes ──────────────────────────────────────────────────────── */
[data-testid="stInfo"] {
    background: rgba(99, 102, 241, 0.10);
    border: 1px solid rgba(99, 102, 241, 0.25);
    border-radius: var(--radius);
    color: #c7d2fe;
}

/* ── Tables ───────────────────────────────────────────────────────────────────── */
[data-testid="stMarkdownContainer"] table {
    border-collapse: separate;
    border-spacing: 0;
    width: 100%;
    border-radius: var(--radius);
    overflow: hidden;
    background: var(--bg-card);
    border: 1px solid var(--border);
}

[data-testid="stMarkdownContainer"] th {
    background: rgba(99, 102, 241, 0.15);
    color: #a5b4fc;
    font-size: 0.75rem;
    font-weight: 600;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    padding: 10px 14px;
}

[data-testid="stMarkdownContainer"] td {
    padding: 9px 14px;
    color: var(--text-primary);
    border-top: 1px solid var(--border);
    font-size: 0.88rem;
}

[data-testid="stMarkdownContainer"] tr:hover td {
    background: rgba(99, 102, 241, 0.05);
}

/* ── Subheaders ──────────────────────────────────────────────────────────────── */
[data-testid="stMarkdownContainer"] h2,
h2 {
    font-size: 1.05rem !important;
    font-weight: 600 !important;
    color: var(--text-primary) !important;
    letter-spacing: -0.01em;
    margin-bottom: 0.75rem !important;
}

/* ── Page header block ───────────────────────────────────────────────────────── */
.page-header h1 {
    font-size: 1.85rem !important;
    font-weight: 700 !important;
    background: linear-gradient(135deg, #e0e7ff 0%, #a5b4fc 50%, #818cf8 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    margin-bottom: 0.2rem !important;
}

.header-subtitle {
    color: var(--text-secondary);
    font-size: 0.875rem;
    font-weight: 400;
    margin-top: 0 !important;
}

/* ── Agent status cards ──────────────────────────────────────────────────────── */
.agent-card {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 10px 14px;
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 10px;
    margin-bottom: 8px;
    transition: border-color 0.2s ease;
}

.agent-card:hover {
    border-color: var(--border-hover);
}

.agent-badge {
    font-size: 1.1rem;
    flex-shrink: 0;
}

.agent-name {
    font-size: 0.875rem;
    font-weight: 600;
    color: var(--text-primary);
}

.agent-ts {
    font-size: 0.72rem;
    color: var(--text-muted);
    margin-top: 1px;
}

/* ── Severity chip ───────────────────────────────────────────────────────────── */
.severity-chip {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 999px;
    font-size: 0.72rem;
    font-weight: 600;
    letter-spacing: 0.05em;
    text-transform: uppercase;
}

/* ── Sidebar elements ────────────────────────────────────────────────────────── */
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
    color: var(--text-secondary);
    font-size: 0.82rem;
}

/* ── Link buttons ────────────────────────────────────────────────────────────── */
[data-testid="baseButton-elementContainer"] a {
    font-weight: 500;
    font-size: 0.875rem;
}

/* ── Plotly chart bg transparency ────────────────────────────────────────────── */
.stPlotlyChart {
    border-radius: var(--radius);
    overflow: hidden;
}

/* ── Section fade-in animation ───────────────────────────────────────────────── */
@keyframes fadeInUp {
    from { opacity: 0; transform: translateY(12px); }
    to   { opacity: 1; transform: translateY(0);    }
}

[data-testid="stVerticalBlock"] > div {
    animation: fadeInUp 0.35s ease both;
}

/* ── Spinner colour ──────────────────────────────────────────────────────────── */
[data-testid="stSpinner"] {
    color: #6366f1;
}
</style>
"""


def inject_global_css() -> None:
    """
    Inject the shared dark-mode CSS into the current page.

    Call once at the top of every page module.  Streamlit re-runs each page
    script on every interaction, so the CSS must be re-injected each time.
    """
    st.markdown(_GLOBAL_CSS, unsafe_allow_html=True)


# ── Colour helpers ─────────────────────────────────────────────────────────────

_SEVERITY_COLOURS: dict[str, str] = {
    "LOW":      "#22c55e",   # green-500
    "MEDIUM":   "#f59e0b",   # amber-400
    "HIGH":     "#ef4444",   # red-500
    "CRITICAL": "#7c3aed",   # violet-700
}

_UNKNOWN_COLOUR: str = "#6b7280"   # gray-500


def severity_color(severity: str) -> str:
    """
    Map a severity label to a hex colour string.

    Args:
        severity: One of ``LOW``, ``MEDIUM``, ``HIGH``, ``CRITICAL``
                  (case-insensitive).  Unknown values return gray.

    Returns:
        A CSS hex colour string, e.g. ``"#22c55e"``.
    """
    return _SEVERITY_COLOURS.get(str(severity).upper(), _UNKNOWN_COLOUR)


def score_color(score: float | None) -> str:
    """
    Map a 0–100 score to a traffic-light hex colour.

    Thresholds:
        ≥ 85 → green  (excellent)
        ≥ 70 → lime   (good)
        ≥ 50 → amber  (fair)
        < 50 → red    (at-risk)
        None → gray   (no data)

    Args:
        score: A float in [0, 100] or ``None``.

    Returns:
        A CSS hex colour string.
    """
    if score is None:
        return _UNKNOWN_COLOUR
    if score >= 85:
        return "#22c55e"
    if score >= 70:
        return "#84cc16"
    if score >= 50:
        return "#f59e0b"
    return "#ef4444"
