"""
dashboard/app.py
================
Streamlit multi-page application entry point.
Placeholder — full page implementation in Phase 5.
"""

import os
import streamlit as st

# Single source of truth for the Airflow web UI URL.
# Override via the AIRFLOW_URL environment variable (set in docker-compose / .env).
_AIRFLOW_URL: str = os.environ.get("AIRFLOW_URL", "http://localhost:8081")

# ─── Page Config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="DataObservability-AI",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "About": "Dataset-Agnostic, Cost-Aware and Self-Healing Data Observability Platform | VTU ISE 2025-2026",
    },
)

# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/data-configuration.png", width=64)
    st.title("DataObservability-AI")
    st.caption("VTU Major Project — ISE 2025-2026")
    st.divider()
    st.markdown("**Navigation**")
    st.markdown("""
- 📊 Overview *(this page)*
- ✅ Data Quality
- 📉 Drift Monitor
- 💰 Cost Tracker
- 🔄 Pipeline Status
- 🔔 Alerts
    """)
    st.divider()
    backend_url = st.text_input("Backend URL", value="http://localhost:8000")
    st.session_state["backend_url"] = backend_url

# ─── Main Content ─────────────────────────────────────────────────────────────
st.title("📊 DataObservability-AI — Overview")
st.caption("Dataset-Agnostic · Cost-Aware · Self-Healing")

st.info(
    "🚀 **Phase 1 Scaffold** — This is a placeholder dashboard. "
    "Full observability pages will be implemented in Phase 5.",
    icon="ℹ️",
)

# Platform status banner
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(label="Platform Status", value="🟢 Running", delta="All services up")
with col2:
    st.metric(label="Active Datasets", value="—", delta="Phase 2")
with col3:
    st.metric(label="DQ Score", value="—", delta="Phase 2")
with col4:
    st.metric(label="Monthly Cost", value="—", delta="Phase 4")

st.divider()

# Architecture overview
st.subheader("System Architecture")
st.markdown("""
| Layer | Technology | Status |
|-------|-----------|--------|
| **Ingestion** | Apache Kafka (KRaft) | ✅ Running |
| **Orchestration** | Apache Airflow 2.9 | ✅ Running |
| **Transformation** | dbt + Snowflake | ⏳ Phase 2 |
| **Quality** | Great Expectations | ⏳ Phase 2 |
| **Drift Detection** | Evidently AI | ⏳ Phase 3 |
| **Cost Tracking** | Snowflake METERING | ⏳ Phase 4 |
| **AI Agents** | Python (5 agents) | ⏳ Phase 3 |
| **API** | FastAPI | ✅ Running |
| **Dashboard** | Streamlit | ✅ Running |
| **CI/CD** | GitHub Actions | ⏳ Phase 6 |
""")

st.divider()
st.subheader("Quick Links")

link_col1, link_col2, link_col3, link_col4 = st.columns(4)
with link_col1:
    st.link_button("FastAPI Docs", "http://localhost:8000/docs", use_container_width=True)
with link_col2:
    st.link_button("Airflow UI", _AIRFLOW_URL, use_container_width=True)
with link_col3:
    st.link_button("Kafka UI", "http://localhost:8090", use_container_width=True)
with link_col4:
    st.link_button("API Health", "http://localhost:8000/health", use_container_width=True)
