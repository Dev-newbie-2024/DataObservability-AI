"""
backend/main.py
===============
FastAPI application entry point.
Placeholder — full router implementation in Phase 2.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from config.logging_config import configure_logging, get_logger
from config.settings import settings

# Configure structured logging at startup
configure_logging()
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan: startup and shutdown hooks."""
    logger.info(
        "Backend starting",
        app_name=settings.app_name,
        version=settings.app_version,
        env=settings.app_env,
    )
    yield
    logger.info("Backend shutting down")


app = FastAPI(
    title="DataObservability-AI API",
    description=(
        "Dataset-Agnostic, Cost-Aware and Self-Healing Data Observability Platform. "
        "VTU Major Project — Dept. of ISE, 2025-2026."
    ),
    version=settings.app_version,
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# ─── CORS ────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if settings.is_development else ["http://dashboard:8501"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Health & Status Endpoints ────────────────────────────────────────────────

@app.get("/health", tags=["System"])
async def health_check() -> JSONResponse:
    """Liveness probe — returns 200 if the API is running."""
    return JSONResponse(
        content={
            "status": "healthy",
            "service": "backend",
            "version": settings.app_version,
            "env": settings.app_env,
        }
    )


@app.get("/ready", tags=["System"])
async def readiness_check() -> JSONResponse:
    """Readiness probe — checks external dependency connectivity."""
    checks: dict[str, str] = {}

    # Snowflake check (placeholder — implement in Phase 2)
    checks["snowflake"] = "not_checked"

    # Kafka check (placeholder — implement in Phase 2)
    checks["kafka"] = "not_checked"

    all_ready = all(v != "unreachable" for v in checks.values())
    return JSONResponse(
        status_code=200 if all_ready else 503,
        content={"status": "ready" if all_ready else "degraded", "checks": checks},
    )


@app.get("/", tags=["System"])
async def root() -> JSONResponse:
    """API root — returns service info."""
    return JSONResponse(
        content={
            "service": settings.app_name,
            "version": settings.app_version,
            "docs": "/docs",
            "health": "/health",
        }
    )


# ─── Placeholder routers (wired in Phase 2) ───────────────────────────────────
# from backend.routers import datasets, pipelines, quality, drift, cost, alerts
# app.include_router(datasets.router, prefix="/api/v1/datasets", tags=["Datasets"])
# app.include_router(pipelines.router, prefix="/api/v1/pipelines", tags=["Pipelines"])
# app.include_router(quality.router,   prefix="/api/v1/quality",   tags=["Quality"])
# app.include_router(drift.router,     prefix="/api/v1/drift",     tags=["Drift"])
# app.include_router(cost.router,      prefix="/api/v1/cost",      tags=["Cost"])
# app.include_router(alerts.router,    prefix="/api/v1/alerts",    tags=["Alerts"])
