# ─── Stage 1: Builder ─────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libffi-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install dashboard-specific packages
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --prefix=/install \
        streamlit>=1.35.0 \
        plotly>=5.22.0 \
        pandas>=2.2.0 \
        numpy>=1.26.0 \
        httpx>=0.27.0 \
        pydantic>=2.7.0 \
        pydantic-settings>=2.3.0 \
        python-dotenv>=1.0.0 \
        structlog>=24.2.0 \
        snowflake-connector-python>=3.10.0

# ─── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

RUN groupadd --gid 1001 appuser && \
    useradd --uid 1001 --gid appuser --shell /bin/bash --create-home appuser

WORKDIR /app

COPY --from=builder /install /usr/local

# Copy application source
COPY dashboard/ ./dashboard/
COPY config/ ./config/

# Streamlit config directory
RUN mkdir -p /home/appuser/.streamlit && \
    echo '[server]\nheadless = true\nenableCORS = false\nenableXsrfProtection = false\n' \
    > /home/appuser/.streamlit/config.toml

RUN chown -R appuser:appuser /app /home/appuser/.streamlit

USER appuser

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

EXPOSE 8501

CMD ["streamlit", "run", "dashboard/app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true"]
