"""
dashboard/pages/04_drift_monitoring.py
========================================
Phase 5 — Page 4: Drift Monitoring (stub — to be implemented in next session).
"""

from __future__ import annotations

import streamlit as st
from dashboard.utils.styles import inject_global_css

inject_global_css()

st.title("📉 Drift Monitoring")
st.caption("Drift score trend · PSI / KS metrics · Drift severity visualisation")

st.info(
    "**Coming Soon — Phase 5 Page 4.**  "
    "This page will display drift score trends, PSI/KS metrics per column, "
    "and drift severity visualisations from DRIFT_SUMMARY and DRIFT_METRICS.",
    icon="🚧",
)
