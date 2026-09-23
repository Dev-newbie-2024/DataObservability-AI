"""
dashboard/pages/05_cost_analytics.py
======================================
Phase 5 — Page 5: Cost Analytics (stub — to be implemented in next session).
"""

from __future__ import annotations

import streamlit as st
from dashboard.utils.styles import inject_global_css

inject_global_css()

st.title("💰 Cost Analytics")
st.caption("Credits consumed · 30-day forecast · Cost score gauge · Daily usage trend")

st.info(
    "**Coming Soon — Phase 5 Page 5.**  "
    "This page will display Snowflake credit consumption, a 30-day spend forecast, "
    "a cost score gauge, and daily usage trends from COST_SUMMARY and COST_RECORDS.",
    icon="🚧",
)
