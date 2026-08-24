"""
backend/core/exceptions.py
===========================
Custom exception classes and FastAPI exception handlers.
All domain-specific errors are defined here and mapped to HTTP responses.
"""

from __future__ import annotations

from fastapi import Request, status
from fastapi.responses import JSONResponse


# ─── Domain Exceptions ────────────────────────────────────────────────────────

class ObservabilityError(Exception):
    """Base exception for all platform errors."""

    def __init__(self, message: str, detail: str | None = None) -> None:
        self.message = message
        self.detail = detail or message
        super().__init__(message)


class SnowflakeConnectionError(ObservabilityError):
    """Raised when Snowflake connection cannot be established."""


class SnowflakeQueryError(ObservabilityError):
    """Raised when a Snowflake query fails."""


class KafkaProducerError(ObservabilityError):
    """Raised when a Kafka message cannot be published."""


class DatasetNotFoundError(ObservabilityError):
    """Raised when a dataset ID does not exist in the registry."""

    def __init__(self, dataset_id: str) -> None:
        super().__init__(
            message=f"Dataset '{dataset_id}' not found",
            detail=f"No dataset with id='{dataset_id}' exists in the registry.",
        )


class DatasetAlreadyExistsError(ObservabilityError):
    """Raised when attempting to create a dataset with a duplicate name."""

    def __init__(self, name: str) -> None:
        super().__init__(
            message=f"Dataset '{name}' already exists",
            detail=f"A dataset named '{name}' is already registered. Use a unique name.",
        )


class UnsupportedFileTypeError(ObservabilityError):
    """Raised when an unsupported file type is uploaded."""

    def __init__(self, filename: str) -> None:
        super().__init__(
            message=f"Unsupported file type: '{filename}'",
            detail="Only CSV (.csv) and JSON (.json / .jsonl) files are supported.",
        )


class EmptyFileError(ObservabilityError):
    """Raised when the uploaded file contains no data rows."""


class SchemaInferenceError(ObservabilityError):
    """Raised when schema cannot be inferred from the uploaded file."""


# ─── FastAPI Exception Handlers ───────────────────────────────────────────────

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
