"""
backend/routers/datasets.py
=============================
Dataset management endpoints.

GET  /datasets        — paginated list of registered datasets
GET  /datasets/{id}   — full detail for a single dataset (includes current schema)
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, Path, Query

from backend.core import snowflake_client
from backend.core.dependencies import PaginationParams
from backend.core.exceptions import DatasetNotFoundError
from backend.models.dataset import (
    DatasetListResponse,
    DatasetResponse,
    DatasetSummary,
    SchemaColumn,
    SchemaVersionResponse,
)
from config.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Datasets"])


# ─── Helpers: Snowflake rows → Pydantic models ────────────────────────────────

def _row_to_summary(row: dict) -> DatasetSummary:
    """Convert a Snowflake DATASETS row (DictCursor) to DatasetSummary."""
    return DatasetSummary(
        id=str(row.get("ID", "")),
        name=str(row.get("NAME", "")),
        source_type=str(row.get("SOURCE_TYPE", "")),
        domain=str(row.get("DOMAIN", "") or ""),
        bronze_table=str(row.get("BRONZE_TABLE", "") or ""),
        silver_table=str(row.get("SILVER_TABLE", "") or ""),
        gold_table=str(row.get("GOLD_TABLE", "") or ""),
        is_active=bool(row.get("IS_ACTIVE", True)),
        created_at=row["CREATED_AT"],
        updated_at=row["UPDATED_AT"],
    )


def _build_schema_version(sv_row: dict) -> SchemaVersionResponse | None:
    """Convert a Snowflake SCHEMA_VERSIONS row to SchemaVersionResponse."""
    if sv_row is None:
        return None

    raw_cols = sv_row.get("SCHEMA_JSON") or sv_row.get("schema_json") or []
    if isinstance(raw_cols, str):
        import json
        raw_cols = json.loads(raw_cols)

    columns = [
        SchemaColumn(
            name=col.get("name", ""),
            snowflake_type=col.get("snowflake_type", "VARCHAR(65535)"),
            pandas_dtype=col.get("pandas_dtype", "object"),
            nullable=col.get("nullable", True),
            sample_values=col.get("sample_values", []),
        )
        for col in (raw_cols or [])
    ]

    return SchemaVersionResponse(
        id=str(sv_row.get("ID", "")),
        dataset_id=str(sv_row.get("DATASET_ID", "")),
        version=int(sv_row.get("VERSION", 1)),
        columns=columns,
        fingerprint=str(sv_row.get("FINGERPRINT", "")),
        is_current=bool(sv_row.get("IS_CURRENT", True)),
        change_summary=str(sv_row.get("CHANGE_SUMMARY", "") or ""),
        created_at=sv_row["CREATED_AT"],
    )


# ─── Endpoints ────────────────────────────────────────────────────────────────

@router.get(
    "/datasets",
    response_model=DatasetListResponse,
    summary="List all datasets",
    description=(
        "Returns a paginated list of all active datasets registered on the platform. "
        "Sorted by creation date descending."
    ),
)
async def list_datasets(
    pagination: PaginationParams = Depends(),
    domain: str | None = Query(
        default=None,
        description="Filter by business domain (e.g. 'finance', 'healthcare')",
    ),
    source_type: str | None = Query(
        default=None,
        description="Filter by source type ('csv', 'json', 'api', 's3')",
    ),
) -> DatasetListResponse:
    rows = snowflake_client.list_datasets(
        limit=pagination.limit,
        offset=pagination.offset,
    )
    total = snowflake_client.count_datasets()

    # Apply optional in-memory filters (Snowflake query filters added in Phase 5)
    if domain:
        rows = [r for r in rows if (r.get("DOMAIN") or "").lower() == domain.lower()]
    if source_type:
        rows = [r for r in rows if (r.get("SOURCE_TYPE") or "").lower() == source_type.lower()]

    items = [_row_to_summary(r) for r in rows]

    logger.debug("Datasets listed", count=len(items), total=total)
    return DatasetListResponse(
        items=items,
        total=total,
        limit=pagination.limit,
        offset=pagination.offset,
    )


@router.get(
    "/datasets/{dataset_id}",
    response_model=DatasetResponse,
    summary="Get dataset detail",
    description=(
        "Returns full detail for a single dataset including its current schema version "
        "and all registered columns."
    ),
    responses={
        404: {"description": "Dataset not found"},
    },
)
async def get_dataset(
    dataset_id: str = Path(
        ...,
        description="Dataset UUID",
        examples=["3fa85f64-5717-4562-b3fc-2c963f66afa6"],
    ),
) -> DatasetResponse:
    row = snowflake_client.get_dataset_by_id(dataset_id)
    if row is None:
        raise DatasetNotFoundError(dataset_id)

    sv_row = snowflake_client.get_current_schema_version(dataset_id)
    sv = _build_schema_version(sv_row)
    version_count = snowflake_client.get_schema_version_count(dataset_id)

    summary = _row_to_summary(row)
    return DatasetResponse(
        **summary.model_dump(),
        description=str(row.get("DESCRIPTION", "") or ""),
        current_schema=sv,
        schema_version_count=version_count,
    )
