# ─── Stage 1: Builder ─────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

# System dependencies needed to compile some packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first for Docker layer caching
COPY requirements.txt .

# Install all backend-relevant packages
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --prefix=/install \
        fastapi>=0.111.0 \
        "uvicorn[standard]>=0.30.0" \
        httpx>=0.27.0 \
        python-multipart>=0.0.9 \
        pydantic>=2.7.0 \
        pydantic-settings>=2.3.0 \
        python-dotenv>=1.0.0 \
        structlog>=24.2.0 \
        tenacity>=8.3.0 \
        snowflake-connector-python>=3.10.0 \
        "confluent-kafka>=2.4.0" \
        pandas>=2.2.0 \
        numpy>=1.26.0

# ─── Stage 2: Runtime ─────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

# Non-root user for security
RUN groupadd --gid 1001 appuser && \
    useradd --uid 1001 --gid appuser --shell /bin/bash --create-home appuser

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY backend/ ./backend/
COPY config/ ./config/

# Change ownership
RUN chown -R appuser:appuser /app

USER appuser

# Health check
HEALTHCHECK --interval=20s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

EXPOSE 8000

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
