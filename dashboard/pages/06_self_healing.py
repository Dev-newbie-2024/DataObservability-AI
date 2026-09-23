"""
dashboard/pages/06_self_healing.py
====================================
Phase 5 — Page 6: Self-Healing (stub — to be implemented in next session).
"""

from __future__ import annotations

import streamlit as st
from dashboard.utils.styles import inject_global_css

inject_global_css()

st.title("🔧 Self-Healing")
st.caption("Failure type distribution · Recovery status · MTTR · Retry vs Quarantine")

st.info(
    "**Coming Soon — Phase 5 Page 6.**  "
    "This page will display failure type distributions, recovery status tracking, "
    "MTTR trends, and retry vs quarantine analytics from HEAL_SUMMARY and HEAL_ACTIONS.",
    icon="🚧",
)
