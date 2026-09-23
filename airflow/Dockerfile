# ════════════════════════════════════════════════════════════════════════════
# DataObservability-AI — Airflow Image
# Base  : apache/airflow:2.9.3-python3.12  (official)
# Mode  : LocalExecutor + PostgreSQL metadata DB
#
# Installed packages (orchestration only):
#   apache-airflow-providers-snowflake
#   apache-airflow-providers-apache-kafka
#   snowflake-connector-python
#   confluent-kafka
#   dbt-core / dbt-snowflake
#   pandas / pyarrow
#
# Shared config/ package dependencies (required at DAG parse time):
#   structlog          → config/logging_config.py
#   pydantic-settings  → config/settings.py
#   python-dotenv      → pydantic-settings .env loading
#   tenacity           → backend retry helpers imported by config package
#
# NOT installed here (live in the ai_agents service only):
#   great-expectations, evidently, prophet, scipy
#
# Build-time notes:
#   PIP_DEFAULT_TIMEOUT=120  → 2 min per-package download timeout (env-level)
#   PIP_RETRIES=5            → retry on transient network failures (env-level)
#   --prefer-binary          → use pre-built wheels; avoid slow source-tarball downloads
#   Installs are split into three layers so Docker layer cache saves unchanged
#   layers when only one layer needs to be re-run.
# ════════════════════════════════════════════════════════════════════════════

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.12

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

# Re-declare ARGs after FROM so they are available in subsequent layers
ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.12

# Official Airflow constraint URL — pins every transitive dependency
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    PIP_DEFAULT_TIMEOUT=120 \
    PIP_RETRIES=5

# ─── System build dependencies ────────────────────────────────────────────────
# Required by snowflake-connector-python (cryptography / cffi build)
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        libffi-dev \
        libssl-dev \
        curl \
 && rm -rf /var/lib/apt/lists/*

# Switch back to the unprivileged airflow user for all pip operations
USER airflow

# ─── Layer 0: Shared config/ package runtime dependencies ────────────────────
# The config/ directory is bind-mounted into /opt/airflow/config at runtime.
# Its modules are imported at DAG *parse time* by the Airflow scheduler, so
# every package they depend on must be installed in this image.
#   structlog         - config/logging_config.py
#   pydantic-settings - config/settings.py
#   python-dotenv     - .env file loading by pydantic-settings
#   tenacity          - used by backend retry helpers shared with config
RUN pip install --no-cache-dir \
    --prefer-binary \
    "structlog>=24.2.0" \
    "pydantic-settings>=2.3.0" \
    "python-dotenv>=1.0.0" \
    "tenacity>=8.3.0"

# ─── Layer 1: Airflow providers (constrained) ────────────────────────────────
# Install providers first with the official constraint file so apache-airflow
# itself is never downgraded. --prefer-binary avoids source-tarball timeouts.
RUN pip install --no-cache-dir \
    --prefer-binary \
    --constraint "${CONSTRAINT_URL}" \
    apache-airflow-providers-snowflake \
    apache-airflow-providers-apache-kafka

# ─── Layer 2: Connectors, dbt, and data packages ─────────────────────────────
# Separate layer so Docker cache preserves Layer 1 on rebuild.
# These packages are large (especially dbt-snowflake and snowflake-connector-python)
# so we keep the timeout generous and use binary wheels where available.
RUN pip install --no-cache-dir \
    --prefer-binary \
    snowflake-connector-python \
    confluent-kafka \
    dbt-core \
    dbt-snowflake \
    pandas \
    pyarrow

# ─── Smoke test — verifies the image is functional ───────────────────────────
RUN airflow version

# ─── Runtime PYTHONPATH ───────────────────────────────────────────────────────
# Allows DAGs to import from /opt/airflow/{backend,config,...}
# which are bind-mounted at runtime via docker-compose volumes.
ENV PYTHONPATH="/opt/airflow:${PYTHONPATH:-}"
