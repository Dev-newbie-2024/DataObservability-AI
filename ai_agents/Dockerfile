# ─── Stage 1: Builder ─────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# System dependencies (Prophet needs pystan which needs gcc)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    libgomp1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install lightweight / connector packages first
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --prefer-binary --timeout 300 --retries 10 \
        --prefix=/install \
        "pydantic>=2.7.0" \
        "pydantic-settings>=2.3.0" \
        "python-dotenv>=1.0.0" \
        "structlog>=24.2.0" \
        "tenacity>=8.3.0" \
        "snowflake-connector-python>=3.10.0" \
        "confluent-kafka>=2.4.0" \
        "pandas>=2.2.0" \
        "numpy>=1.26.0" \
        "pyarrow>=16.0.0" \
        "httpx>=0.27.0" \
        "fastapi>=0.111.0" \
        "uvicorn[standard]>=0.30.0"

# Install heavy analytics libraries in a separate layer so a timeout here
# only retries these packages, not the entire install above.
RUN pip install --no-cache-dir --prefer-binary --timeout 300 --retries 10 \
        --prefix=/install \
        "scipy>=1.13.0" \
        "great-expectations>=0.18.0" \
        "evidently>=0.4.30" \
        "prophet>=1.1.5"

# ─── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

# libgomp1 is a runtime dep for Prophet
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1001 appuser && \
    useradd --uid 1001 --gid appuser --shell /bin/bash --create-home appuser

WORKDIR /app

COPY --from=builder /install /usr/local

# Copy agent source
COPY ai_agents/ ./ai_agents/
COPY config/ ./config/

RUN chown -R appuser:appuser /app

USER appuser

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -f http://localhost:8001/health || exit 1

EXPOSE 8001

# Agents expose a minimal health-check HTTP server and run event loops
CMD ["python", "-m", "ai_agents.main"]
