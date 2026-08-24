"""
backend/models/dataset.py
==========================
Pydantic v2 models for datasets and schema versioning.

Naming convention:
  - <Entity>Base      — shared fields between request and response
  - <Entity>Create    — request body for creation endpoints
  - <Entity>Response  — full response model returned by the API
  - <Entity>Summary   — lightweight model for list responses
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


# ─── Schema Column ────────────────────────────────────────────────────────────

class SchemaColumn(BaseModel):
    """Represents a single column in an inferred or registered schema."""

    model_config = ConfigDict(populate_by_name=True)

    name: str = Field(..., description="Column name (lowercased, snake_case)")
    snowflake_type: str = Field(..., description="Snowflake SQL data type (e.g. VARCHAR, NUMBER)")
    pandas_dtype: str = Field(..., description="Original pandas dtype string")
    nullable: bool = Field(default=True)
    sample_values: list[Any] = Field(
        default_factory=list,
        description="Up to 5 non-null sample values for preview",
    )

    @field_validator("name", mode="before")
    @classmethod
    def normalise_name(cls, v: str) -> str:
        """Force column names to lowercase, replace spaces with underscores."""
        return str(v).strip().lower().replace(" ", "_").replace("-", "_")


# ─── Schema Version ───────────────────────────────────────────────────────────

class SchemaVersionResponse(BaseModel):
    """A versioned snapshot of a dataset's schema."""

    model_config = ConfigDict(populate_by_name=True)

    id: str
    dataset_id: str
    version: int
    columns: list[SchemaColumn]
    fingerprint: str = Field(..., description="SHA-256 prefix for change detection")
    is_current: bool
    change_summary: str = ""
    created_at: datetime


# ─── Dataset ──────────────────────────────────────────────────────────────────

class DatasetCreate(BaseModel):
    """Request body for registering a new dataset (used internally by upload service)."""

    name: str = Field(
        ...,
        min_length=2,
        max_length=200,
        pattern=r"^[a-zA-Z0-9_\-]+$",
        description="Unique dataset identifier (alphanumeric, dash, underscore)",
        examples=["sales_data", "customer-records"],
    )
    description: str = Field(default="", max_length=1000)
    source_type: Literal["csv", "json", "api", "s3"] = Field(default="csv")
    domain: str = Field(
        default="",
        max_length=100,
        description="Business domain (e.g. finance, healthcare)",
    )

    @field_validator("name", mode="before")
    @classmethod
    def normalise_name(cls, v: str) -> str:
        return str(v).strip().lower()


class DatasetSummary(BaseModel):
    """Lightweight dataset representation for list responses."""

    model_config = ConfigDict(populate_by_name=True)

    id: str
    name: str
    source_type: str
    domain: str
    bronze_table: str
    silver_table: str
    gold_table: str
    is_active: bool
    created_at: datetime
    updated_at: datetime


class DatasetResponse(DatasetSummary):
    """Full dataset representation including current schema version."""

    description: str
    current_schema: SchemaVersionResponse | None = None
    schema_version_count: int = 0


# ─── Dataset List Response (paginated) ────────────────────────────────────────

class DatasetListResponse(BaseModel):
    """Paginated list of datasets."""

    items: list[DatasetSummary]
    total: int
    limit: int
    offset: int
