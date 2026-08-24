"""
backend/routers/health.py
==========================
System health and readiness endpoints.

GET /health  — liveness probe (always returns 200 if the process is running)
GET /ready   — readiness probe (checks Snowflake + Kafka connectivity)
"""

from __future__ import annotations

import time

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from backend.core.kafka_producer import get_producer
from backend.core.snowflake_client import ping as snowflake_ping
from backend.models.metadata import HealthResponse, ReadyResponse, ServiceCheck
from config.settings import settings

router = APIRouter(tags=["System"])

# Record process start time for uptime calculation
_START_TIME = time.monotonic()


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Liveness probe",
    description="Returns 200 if the API process is running. Used by Docker HEALTHCHECK.",
)
async def health() -> HealthResponse:
    return HealthResponse(
        status="healthy",
        service="backend",
        version=settings.app_version,
        environment=settings.app_env,
        uptime_seconds=round(time.monotonic() - _START_TIME, 2),
    )


@router.get(
    "/ready",
    summary="Readiness probe",
    description=(
        "Checks connectivity to all critical external dependencies. "
        "Returns 200 if all checks pass, 503 if any check fails."
    ),
)
async def ready() -> JSONResponse:
    checks: list[ServiceCheck] = []
    all_ready = True

    # ── Snowflake ─────────────────────────────────────────────────────────────
    sf_start = time.monotonic()
    try:
        sf_ok = snowflake_ping()
        sf_latency = round((time.monotonic() - sf_start) * 1000, 1)
        checks.append(ServiceCheck(
            name="snowflake",
            status="ok" if sf_ok else "unreachable",
            latency_ms=sf_latency,
            detail="" if sf_ok else "Snowflake did not respond to SELECT 1",
        ))
        if not sf_ok:
            all_ready = False
    except Exception as exc:
        all_ready = False
        checks.append(ServiceCheck(
            name="snowflake",
            status="unreachable",
            detail=str(exc),
        ))

    # ── Kafka (producer is non-blocking — check initialisation only) ──────────
    try:
        producer = get_producer()
        # Producer initialised = True means we at least attempted connection
        kafka_status = "ok" if producer._initialised else "degraded"
        checks.append(ServiceCheck(
            name="kafka",
            status=kafka_status,
            detail="" if kafka_status == "ok" else "Producer not yet initialised",
        ))
    except Exception as exc:
        checks.append(ServiceCheck(
            name="kafka",
            status="degraded",
            detail=str(exc),
        ))

    response_model = ReadyResponse(
        status="ready" if all_ready else "not_ready",
        checks=checks,
    )

    return JSONResponse(
        status_code=200 if all_ready else 503,
        content=response_model.model_dump(),
    )
