"""
backend/models/metadata.py
===========================
Pydantic v2 models for pipeline runs, upload results, and health responses.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


# ─── Health / Readiness ───────────────────────────────────────────────────────

class ServiceCheck(BaseModel):
    """Status of a single external service dependency."""

    name: str
    status: Literal["ok", "degraded", "unreachable"]
    latency_ms: float | None = None
    detail: str = ""


class HealthResponse(BaseModel):
    """Response model for GET /health."""

    status: Literal["healthy", "degraded", "unhealthy"]
    service: str
    version: str
    environment: str
    uptime_seconds: float


class ReadyResponse(BaseModel):
    """Response model for GET /ready."""

    status: Literal["ready", "degraded", "not_ready"]
    checks: list[ServiceCheck]


# ─── Upload ───────────────────────────────────────────────────────────────────

class UploadRequest(BaseModel):
    """
    Metadata fields submitted alongside the file in a multipart upload.
    These are Form fields — not a JSON body.
    """

    dataset_name: str = Field(
        ...,
        min_length=2,
        max_length=200,
        pattern=r"^[a-zA-Z0-9_\-]+$",
        description="Unique dataset name",
    )
    description: str = Field(default="", max_length=1000)
    domain: str = Field(default="", max_length=100)


class SchemaChangeSummary(BaseModel):
    """Brief description of a single schema change detected during upload."""

    change_type: str           # ADD_COLUMN | DROP_COLUMN | TYPE_CHANGE | NO_CHANGE
    column_name: str
    detail: str = ""


class UploadResponse(BaseModel):
    """Response returned by POST /upload after successful ingestion."""

    model_config = ConfigDict(populate_by_name=True)

    # Dataset identity
    dataset_id: str
    dataset_name: str
    is_new_dataset: bool = Field(
        ..., description="True if this upload created a new dataset"
    )

    # Ingestion stats
    pipeline_run_id: str
    rows_ingested: int
    rows_rejected: int
    bronze_table: str

    # Schema
    schema_version: int
    schema_fingerprint: str
    schema_changes: list[SchemaChangeSummary] = Field(default_factory=list)

    # Kafka event
    kafka_event_id: str | None = None

    # Timestamps
    ingested_at: datetime

    # Next steps hint for the client
    next_steps: list[str] = Field(
        default_factory=lambda: [
            "Run dbt transformation: POST /api/v1/pipelines/{id}/transform",
            "View quality report:    GET  /api/v1/quality/{id}",
        ]
    )


# ─── Pipeline Run ─────────────────────────────────────────────────────────────

class PipelineRunSummary(BaseModel):
    """Brief pipeline run info (embedded in dataset detail responses)."""

    model_config = ConfigDict(populate_by_name=True)

    id: str
    run_id: str
    dag_id: str
    status: str
    rows_ingested: int | None
    rows_rejected: int | None
    started_at: datetime
    completed_at: datetime | None
    triggered_by: str


# ─── Generic API Envelope ─────────────────────────────────────────────────────

class APIResponse(BaseModel):
    """Generic success envelope for simple acknowledgement responses."""

    success: bool = True
    message: str
    data: dict[str, Any] = Field(default_factory=dict)
