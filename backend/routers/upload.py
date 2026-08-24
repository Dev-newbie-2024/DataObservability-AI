"""
backend/routers/upload.py
==========================
POST /upload — accepts a multipart file upload and ingests it into Snowflake Bronze.

Endpoint contract:
  - Content-Type: multipart/form-data
  - Fields:
      file         : UploadFile  (required)
      dataset_name : str         (required)
      description  : str         (optional, default "")
      domain       : str         (optional, default "")
  - Returns: UploadResponse (201 Created)
  - Max file size enforced by `MAX_UPLOAD_SIZE_MB` (default 100 MB).
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, File, Form, UploadFile, status
from fastapi.responses import JSONResponse

from backend.core.dependencies import require_snowflake
from backend.core.exceptions import (
    DatasetAlreadyExistsError,
    EmptyFileError,
    SchemaInferenceError,
    UnsupportedFileTypeError,
)
from backend.models.metadata import UploadResponse
from backend.services import upload_service
from config.logging_config import get_logger
from config.settings import settings

logger = get_logger(__name__)

router = APIRouter(tags=["Upload"])

# 100 MB default maximum upload size
_MAX_UPLOAD_BYTES = 100 * 1024 * 1024


@router.post(
    "/upload",
    response_model=UploadResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Upload a dataset file",
    description=(
        "Upload a CSV or JSON file to ingest it into Snowflake Bronze. "
        "Schema is auto-inferred. Changes from previous uploads are detected "
        "and recorded. A Kafka schema.events message is published on success."
    ),
    responses={
        201: {"description": "File ingested successfully"},
        409: {"description": "Dataset with this name already exists in error state"},
        413: {"description": "File exceeds the maximum allowed size (100 MB)"},
        415: {"description": "Unsupported file type (only CSV and JSON allowed)"},
        422: {"description": "Validation error in form fields"},
        503: {"description": "Snowflake unavailable"},
    },
)
async def upload_dataset(
    file: UploadFile = File(..., description="CSV or JSON file to ingest"),
    dataset_name: str = Form(
        ...,
        min_length=2,
        max_length=200,
        pattern=r"^[a-zA-Z0-9_\-]+$",
        description="Unique dataset name (alphanumeric, dash, underscore)",
        examples=["sales_data", "customer-records"],
    ),
    description: str = Form(default="", max_length=1000),
    domain: str = Form(default="", max_length=100),
    _sf: None = Depends(require_snowflake),
) -> UploadResponse:
    """
    Ingest an uploaded file into Snowflake Bronze.

    Steps executed:
    1. Validate file type and size.
    2. Parse file into a DataFrame.
    3. Infer schema and compute fingerprint.
    4. Register or update dataset in OBSERVABILITY.DATASETS.
    5. Create Bronze table if new; detect changes if existing.
    6. Bulk-insert all rows into Bronze.
    7. Publish schema.events to Kafka (best-effort).
    8. Return UploadResponse with ingestion statistics.
    """
    # ── Size guard ────────────────────────────────────────────────────────────
    file_bytes = await file.read()
    if len(file_bytes) > _MAX_UPLOAD_BYTES:
        return JSONResponse(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            content={
                "error": "FileTooLarge",
                "message": "File exceeds the 100 MB upload limit",
                "detail": f"Received {len(file_bytes) / 1024 / 1024:.1f} MB",
            },
        )

    filename = file.filename or "upload.csv"
    logger.info(
        "Upload request received",
        dataset_name=dataset_name,
        filename=filename,
        size_kb=round(len(file_bytes) / 1024, 1),
    )

    return upload_service.process_upload(
        file_bytes=file_bytes,
        filename=filename,
        dataset_name=dataset_name.strip().lower(),
        description=description,
        domain=domain,
    )
