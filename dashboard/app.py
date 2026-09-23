"""
dashboard/app.py
================
Streamlit multi-page application entry point — Phase 5.

Startup sequence
----------------
1. ``st.set_page_config()`` — must be the very first Streamlit call.
2. Sidebar — platform branding + connection settings persisted in session_state.
3. ``st.navigation()`` — defines all 6 pages; runs the selected page script.

Adding a new page
-----------------
Add a ``st.Page(...)`` entry to the ``_PAGES`` list below.  The ``path``
argument is relative to this file's directory (``dashboard/``).
"""

from __future__ import annotations

import os

import streamlit as st

# ─── Single source of truth for external service URLs ─────────────────────────
_AIRFLOW_URL: str = os.environ.get("AIRFLOW_URL", "http://localhost:8081")
_BACKEND_URL: str = os.environ.get("BACKEND_URL", "http://localhost:8000")

# ─── Page config (MUST be first Streamlit call) ───────────────────────────────
st.set_page_config(
    page_title="DataObservability-AI",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "Get Help": "https://github.com/Dev-newbie-2024/DataObservability-AI",
        "About": (
            "**DataObservability-AI** — Dataset-Agnostic, Cost-Aware and "
            "Self-Healing Data Observability Platform.\n\n"
            "VTU Major Project — Dept. of ISE 2025-2026."
        ),
    },
)

# ─── Sidebar — branding + settings ───────────────────────────────────────────
with st.sidebar:
    st.image(
        "https://img.icons8.com/fluency/96/data-configuration.png",
        width=60,
    )
    st.markdown(
        "<h2 style='margin:0;font-size:1.05rem;color:#e0e7ff'>DataObservability-AI</h2>",
        unsafe_allow_html=True,
    )
    st.caption("VTU Major Project — ISE 2025-2026")
    st.divider()

    # Connection settings — persisted for the entire session so all pages can
    # read st.session_state["backend_url"] / st.session_state["airflow_url"].
    with st.expander("⚙️ Connection Settings", expanded=False):
        backend_url = st.text_input(
            "Backend URL",
            value=st.session_state.get("backend_url", _BACKEND_URL),
            key="sidebar_backend_url",
        )
        st.session_state["backend_url"] = backend_url

        airflow_url = st.text_input(
            "Airflow URL",
            value=st.session_state.get("airflow_url", _AIRFLOW_URL),
            key="sidebar_airflow_url",
        )
        st.session_state["airflow_url"] = airflow_url

    st.divider()
    st.caption(
        "📊 Phase 5 Dashboard  \n"
        "Phases 1–4 ✅ complete  \n"
        "Pages 2–6 🚧 in progress"
    )

# ─── Page definitions ─────────────────────────────────────────────────────────
_PAGES = [
    st.Page(
        "pages/01_executive_overview.py",
        title="Executive Overview",
        icon="📊",
        default=True,
    ),
    st.Page(
        "pages/02_pipeline_monitoring.py",
        title="Pipeline Monitoring",
        icon="🔄",
    ),
    st.Page(
        "pages/03_data_quality.py",
        title="Data Quality",
        icon="✅",
    ),
    st.Page(
        "pages/04_drift_monitoring.py",
        title="Drift Monitoring",
        icon="📉",
    ),
    st.Page(
        "pages/05_cost_analytics.py",
        title="Cost Analytics",
        icon="💰",
    ),
    st.Page(
        "pages/06_self_healing.py",
        title="Self-Healing",
        icon="🔧",
    ),
]

# ─── Run the selected page ────────────────────────────────────────────────────
pg = st.navigation(_PAGES)
pg.run()
