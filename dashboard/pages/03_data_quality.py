"""
dashboard/pages/03_data_quality.py
====================================
Phase 5 — Page 3: Data Quality (stub — to be implemented in next session).
"""

from __future__ import annotations

import streamlit as st
from dashboard.utils.styles import inject_global_css

inject_global_css()

st.title("✅ Data Quality")
st.caption("Quality score trend · Null / duplicate / type-mismatch charts · Pass vs Fail")

st.info(
    "**Coming Soon — Phase 5 Page 3.**  "
    "This page will display quality score trends, null/duplicate/type-mismatch "
    "charts, and a pass vs fail summary from DATA_QUALITY_METRICS.",
    icon="🚧",
)
