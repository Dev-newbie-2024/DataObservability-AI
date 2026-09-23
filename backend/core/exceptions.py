"""
backend/core/exceptions.py
===========================
Framework-agnostic domain exception classes.

All platform errors are plain Python exceptions so this module can be
imported by any layer (Airflow DAGs, CLI scripts, FastAPI, tests) without
requiring FastAPI to be installed.

FastAPI-specific HTTP exception handlers live in ``backend.main`` where
FastAPI is always present.
"""

from __future__ import annotations


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
