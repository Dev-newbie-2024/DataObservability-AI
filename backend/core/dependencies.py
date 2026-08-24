"""
backend/core/dependencies.py
==============================
FastAPI dependency injection functions.
Import these in routers via `Depends(...)`.
"""

from __future__ import annotations

from fastapi import Header, HTTPException, status

from backend.core.kafka_producer import KafkaProducer, get_producer
from backend.core.snowflake_client import ping
from config.settings import settings


# ─── Snowflake Health Dependency ──────────────────────────────────────────────

def require_snowflake() -> None:
    """
    Dependency that raises HTTP 503 if Snowflake is unreachable.
    Use on write endpoints to fail fast with a clear error.

    Usage in a router::

        @router.post("/upload")
        def upload(_, _sf=Depends(require_snowflake)):
            ...
    """
    if not ping():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Snowflake is currently unreachable. Please try again shortly.",
        )


# ─── Kafka Producer Dependency ────────────────────────────────────────────────

def get_kafka_producer() -> KafkaProducer:
    """
    FastAPI dependency that returns the Kafka producer singleton.

    Usage in a router::

        @router.post("/upload")
        def upload(..., producer: KafkaProducer = Depends(get_kafka_producer)):
            producer.publish(...)
    """
    return get_producer()


# ─── Pagination Dependency ────────────────────────────────────────────────────

class PaginationParams:
    """Reusable pagination query parameters."""

    def __init__(
        self,
        limit: int = 50,
        offset: int = 0,
    ) -> None:
        if limit < 1 or limit > 500:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="`limit` must be between 1 and 500.",
            )
        if offset < 0:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="`offset` must be >= 0.",
            )
        self.limit = limit
        self.offset = offset
