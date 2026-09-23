"""
backend/main.py
===============
FastAPI application factory and entry point.

Startup sequence:
  1. Configure structured logging.
  2. Initialise Snowflake connection pool.
  3. Initialise Kafka producer.
  4. Register all routers.
  5. Register custom exception handlers.

Shutdown sequence:
  1. Flush and close Kafka producer.
  2. Close Snowflake connection pool.

Note: FastAPI-specific exception handlers are defined here (not in
``backend.core.exceptions``) so that the core layer remains importable
from environments that do not have FastAPI installed (e.g. Airflow).
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse

from backend.core.database import close_pool, initialise_pool
from backend.core.exceptions import (
    DatasetAlreadyExistsError,
    DatasetNotFoundError,
    EmptyFileError,
    ObservabilityError,
    SchemaInferenceError,
    UnsupportedFileTypeError,
)


# ─── FastAPI Exception Handlers ───────────────────────────────────────────────
# Defined here (not in backend.core.exceptions) so the core layer stays
# framework-agnostic and importable from Airflow without FastAPI.

def _error_body(error_type: str, message: str, detail: str) -> dict:
    return {"error": error_type, "message": message, "detail": detail}


async def dataset_not_found_handler(
    request: Request, exc: DatasetNotFoundError
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content=_error_body("DatasetNotFound", exc.message, exc.detail),
    )


async def dataset_exists_handler(
    request: Request, exc: DatasetAlreadyExistsError
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content=_error_body("DatasetAlreadyExists", exc.message, exc.detail),
    )


async def unsupported_file_handler(
    request: Request, exc: UnsupportedFileTypeError
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
        content=_error_body("UnsupportedFileType", exc.message, exc.detail),
    )


async def observability_error_handler(
    request: Request, exc: ObservabilityError
) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=_error_body("PlatformError", exc.message, exc.detail),
    )
from backend.core.kafka_producer import get_producer
from backend.routers import datasets, health, upload
from config.logging_config import configure_logging, get_logger
from config.settings import settings

# ─── Configure structured logging first ──────────────────────────────────────
configure_logging()
logger = get_logger(__name__)


# ─── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Manage all startup and shutdown side-effects.

    Startup failures for non-critical services (Kafka) are logged as warnings
    rather than crashing the process — the API remains available for read operations.
    """
    logger.info(
        "DataObservability-AI backend starting",
        version=settings.app_version,
        environment=settings.app_env,
    )

    # ── Snowflake pool ────────────────────────────────────────────────────────
    try:
        initialise_pool()
        logger.info("Snowflake connection pool ready")
    except Exception as exc:
        logger.error(
            "Snowflake pool init failed — write endpoints will return 503",
            error=str(exc),
        )

    # ── Kafka producer ────────────────────────────────────────────────────────
    try:
        get_producer().initialise()
        logger.info("Kafka producer ready")
    except Exception as exc:
        logger.warning(
            "Kafka producer init failed — events will be dropped",
            error=str(exc),
        )

    logger.info(
        "Backend ready",
        docs=f"http://{settings.backend_host}:{settings.backend_port}/docs",
    )

    yield  # ← application runs here

    # ── Shutdown ──────────────────────────────────────────────────────────────
    logger.info("Backend shutting down — draining connections")
    try:
        get_producer().close()
    except Exception as exc:
        logger.warning("Kafka producer close error", error=str(exc))
    try:
        close_pool()
    except Exception as exc:
        logger.warning("Snowflake pool close error", error=str(exc))
    logger.info("Backend shutdown complete")


# ─── Application Factory ──────────────────────────────────────────────────────

def create_app() -> FastAPI:
    """Build and return the FastAPI application instance."""

    app = FastAPI(
        title="DataObservability-AI API",
        description=(
            "**Dataset-Agnostic, Cost-Aware and Self-Healing Data Observability Platform**\n\n"
            "VTU Major Project — Dept. of ISE, 2025-2026.\n\n"
            "### Endpoints\n"
            "- `POST /api/v1/upload` — Upload CSV/JSON and ingest to Snowflake Bronze\n"
            "- `GET  /api/v1/datasets` — List all registered datasets\n"
            "- `GET  /api/v1/datasets/{id}` — Get dataset detail with current schema\n"
            "- `GET  /health` — Liveness probe\n"
            "- `GET  /ready`  — Readiness probe (Snowflake + Kafka checks)\n"
        ),
        version=settings.app_version,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
        contact={
            "name": "VTU ISE Project Team",
            "url": "https://github.com/your-org/DataObservability-AI",
        },
        license_info={"name": "MIT"},
    )

    # ─── Middleware ───────────────────────────────────────────────────────────
    app.add_middleware(
        CORSMiddleware,
        allow_origins=(
            ["*"] if settings.is_development
            else ["http://dashboard:8501", "http://localhost:8501"]
        ),
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["*"],
    )
    app.add_middleware(GZipMiddleware, minimum_size=1024)

    # ─── Exception Handlers ───────────────────────────────────────────────────
    app.add_exception_handler(DatasetNotFoundError, dataset_not_found_handler)        # type: ignore[arg-type]
    app.add_exception_handler(DatasetAlreadyExistsError, dataset_exists_handler)      # type: ignore[arg-type]
    app.add_exception_handler(UnsupportedFileTypeError, unsupported_file_handler)     # type: ignore[arg-type]
    app.add_exception_handler(ObservabilityError, observability_error_handler)        # type: ignore[arg-type]

    # ─── Routers ─────────────────────────────────────────────────────────────
    # System (no prefix)
    app.include_router(health.router)

    # API v1 (prefixed)
    API_PREFIX = "/api/v1"
    app.include_router(upload.router,   prefix=API_PREFIX)
    app.include_router(datasets.router, prefix=API_PREFIX)

    # ─── Root redirect ────────────────────────────────────────────────────────
    @app.get("/", include_in_schema=False)
    async def root() -> JSONResponse:
        return JSONResponse(
            content={
                "service": settings.app_name,
                "version": settings.app_version,
                "environment": settings.app_env,
                "docs": "/docs",
                "health": "/health",
                "api": "/api/v1",
            }
        )

    return app


# ─── Entry Point ──────────────────────────────────────────────────────────────

app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "backend.main:app",
        host=settings.backend_host,
        port=settings.backend_port,
        reload=settings.backend_reload,
        workers=settings.backend_workers,
        log_level=settings.log_level.lower(),
    )
