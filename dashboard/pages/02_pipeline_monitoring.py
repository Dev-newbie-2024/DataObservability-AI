"""
dashboard/pages/02_pipeline_monitoring.py
==========================================
Phase 5 — Page 2: Pipeline Monitoring (stub — to be implemented in next session).
"""

from __future__ import annotations

import streamlit as st
from dashboard.utils.styles import inject_global_css

inject_global_css()

st.title("🔄 Pipeline Monitoring")
st.caption("Bronze → Silver → Gold flow · DAG runs · Task status · Duration chart")

st.info(
    "**Coming Soon — Phase 5 Page 2.**  "
    "This page will display the Bronze → Silver → Gold pipeline flow, "
    "recent DAG runs, task status timelines, and pipeline duration charts.",
    icon="🚧",
)
