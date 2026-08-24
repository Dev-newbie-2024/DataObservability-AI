# Airflow Dockerfile — extends the official image with project-specific providers
# Based on: apache/airflow:2.9.3-python3.12
FROM apache/airflow:2.9.3-python3.12

# Switch to root to install system packages
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user for pip installs
USER airflow

# Install additional Python providers and project dependencies
RUN pip install --no-cache-dir \
    apache-airflow-providers-snowflake>=5.5.0 \
    apache-airflow-providers-apache-kafka>=1.3.0 \
    snowflake-connector-python>=3.10.0 \
    "confluent-kafka>=2.4.0" \
    dbt-core>=1.8.0 \
    dbt-snowflake>=1.8.0 \
    great-expectations>=0.18.0 \
    evidently>=0.4.30 \
    pandas>=2.2.0 \
    numpy>=1.26.0 \
    pyarrow>=16.0.0 \
    scipy>=1.13.0 \
    prophet>=1.1.5 \
    pydantic>=2.7.0 \
    pydantic-settings>=2.3.0 \
    python-dotenv>=1.0.0 \
    structlog>=24.2.0 \
    tenacity>=8.3.0 \
    httpx>=0.27.0

# Set PYTHONPATH so DAGs can import from project modules
ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"
