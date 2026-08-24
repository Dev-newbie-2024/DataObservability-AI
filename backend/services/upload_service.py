"""
backend/services/upload_service.py
=====================================
Orchestrates the end-to-end file upload pipeline:

  1. Parse the uploaded file (CSV or JSON) into a pandas DataFrame.
  2. Infer schema and compute fingerprint.
  3. Lookup or register the dataset in Snowflake OBSERVABILITY.DATASETS.
  4. Create the Bronze table if it's a new dataset.
  5. Detect schema changes if the dataset already exists.
  6. Register a new schema version if schema changed.
  7. Bulk-insert all rows into the Bronze table.
  8. Publish a schema.events message to Kafka.
  9. Mark the pipeline run as complete.
  10. Return a structured UploadResponse.

Design:
  - Stateless — every method accepts all required context as arguments.
  - Uses `tempfile` for safe file handling; temp files are always cleaned up.
  - All Snowflake failures propagate as typed exceptions.
  - Kafka failures are logged and do NOT block the upload response
    (Kafka is best-effort for Bronze loading).
"""

from __future__ import annotations

import io
import re
import uuid
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from backend.core import snowflake_client
from backend.core.exceptions import (
    EmptyFileError,
    SchemaInferenceError,
    UnsupportedFileTypeError,
)
from backend.core.kafka_producer import publish_event
from backend.models.dataset import SchemaColumn
from backend.models.metadata import SchemaChangeSummary, UploadResponse
from backend.services import schema_service
from config.constants import KafkaTopic
from config.logging_config import get_logger

logger = get_logger(__name__)

# Supported file extensions
_SUPPORTED_EXTENSIONS = {".csv", ".json", ".jsonl"}
# Max rows per bulk insert batch (avoids oversized executemany calls)
_BATCH_SIZE = 5_000


# ─── File Parsing ─────────────────────────────────────────────────────────────

def _validate_extension(filename: str) -> str:
    """Return the lowercase extension or raise UnsupportedFileTypeError."""
    dot_pos = filename.rfind(".")
    if dot_pos == -1:
        raise UnsupportedFileTypeError(filename)
    ext = filename[dot_pos:].lower()
    if ext not in _SUPPORTED_EXTENSIONS:
        raise UnsupportedFileTypeError(filename)
    return ext


def parse_file(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """
    Parse uploaded file bytes into a DataFrame.

    Args:
        file_bytes: Raw bytes from the uploaded file.
        filename:   Original filename (used for extension detection).

    Returns:
        A pandas DataFrame with columns matching the file headers.

    Raises:
        UnsupportedFileTypeError: Unknown file extension.
        EmptyFileError:           File parses to zero rows.
        SchemaInferenceError:     File cannot be parsed at all.
    """
    ext = _validate_extension(filename)
    buf = io.BytesIO(file_bytes)

    try:
        if ext == ".csv":
            df = pd.read_csv(buf, low_memory=False)
        elif ext in {".json", ".jsonl"}:
            # Try newline-delimited JSON first, fall back to regular JSON
            content = file_bytes.decode("utf-8", errors="replace")
            try:
                df = pd.read_json(io.StringIO(content), lines=True)
            except ValueError:
                df = pd.read_json(io.StringIO(content))
        else:
            raise UnsupportedFileTypeError(filename)
    except (UnsupportedFileTypeError, EmptyFileError):
        raise
    except Exception as exc:
        raise SchemaInferenceError(
            message=f"Failed to parse '{filename}'",
            detail=str(exc),
        ) from exc

    if df.empty or len(df) == 0:
        raise EmptyFileError(
            message=f"'{filename}' contains no data rows",
            detail="The file was parsed successfully but contained zero rows.",
        )

    # Clean column names
    df.columns = [_clean_col_name(c) for c in df.columns]
    return df


def _clean_col_name(name: str) -> str:
    """Lowercase and sanitise a column name for Snowflake compatibility."""
    name = str(name).strip().lower()
    name = re.sub(r"[^a-z0-9_]", "_", name)  # replace non-alphanumeric with _
    name = re.sub(r"_+", "_", name)            # collapse consecutive underscores
    name = name.strip("_")
    if name[0].isdigit():
        name = "col_" + name                   # Snowflake identifiers can't start with digit
    return name


def _make_bronze_table_name(dataset_name: str) -> str:
    """Derive the Bronze table name from a dataset name."""
    safe = re.sub(r"[^a-z0-9_]", "_", dataset_name.lower()).strip("_")
    return f"{safe}_raw"


# ─── Main Upload Orchestrator ─────────────────────────────────────────────────

def process_upload(
    *,
    file_bytes: bytes,
    filename: str,
    dataset_name: str,
    description: str = "",
    domain: str = "",
) -> UploadResponse:
    """
    Execute the complete upload pipeline.

    Args:
        file_bytes:    Raw bytes from the uploaded multipart file.
        filename:      Original filename (used for extension + source tracking).
        dataset_name:  User-provided dataset name.
        description:   Optional human description.
        domain:        Optional business domain tag.

    Returns:
        UploadResponse with all ingestion metadata.
    """
    ingested_at = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())

    logger.info(
        "Upload started",
        dataset_name=dataset_name,
        filename=filename,
        run_id=run_id,
    )

    # ── 1. Parse ──────────────────────────────────────────────────────────────
    df = parse_file(file_bytes, filename)
    total_rows = len(df)
    logger.info("File parsed", rows=total_rows, columns=len(df.columns))

    # ── 2. Infer schema ───────────────────────────────────────────────────────
    inferred_columns = schema_service.infer_schema(df)
    new_fingerprint = schema_service.compute_fingerprint(inferred_columns)

    # ── 3. Lookup or register dataset ─────────────────────────────────────────
    existing = snowflake_client.get_dataset_by_name(dataset_name)
    is_new_dataset = existing is None

    if is_new_dataset:
        dataset_id = _register_new_dataset(
            dataset_name=dataset_name,
            description=description,
            domain=domain,
            source_type=_ext_to_source_type(filename),
        )
        old_columns: list[SchemaColumn] = []
    else:
        dataset_id = existing["ID"]
        old_columns = _load_current_columns(dataset_id)

    # ── 4. Create Bronze table (new datasets) ─────────────────────────────────
    bronze_table = _make_bronze_table_name(dataset_name)
    if is_new_dataset:
        snowflake_client.create_bronze_table(bronze_table, [
            {"name": c.name, "snowflake_type": c.snowflake_type}
            for c in inferred_columns
        ])
        snowflake_client.update_dataset_tables(dataset_id, bronze_table=bronze_table)

    # ── 5. Detect schema changes ──────────────────────────────────────────────
    schema_changes: list[SchemaChangeSummary] = []
    if not is_new_dataset:
        schema_changes = schema_service.detect_schema_changes(old_columns, inferred_columns)
        if schema_changes:
            logger.info("Schema changes detected", count=len(schema_changes))

    # ── 6. Register schema version ────────────────────────────────────────────
    if is_new_dataset:
        schema_service.register_initial_schema(dataset_id, inferred_columns)
        schema_version = 1
    elif schema_changes:
        schema_service.register_new_schema_version(dataset_id, inferred_columns, schema_changes)
        current = snowflake_client.get_current_schema_version(dataset_id)
        schema_version = int(current["VERSION"]) if current else 1
    else:
        current = snowflake_client.get_current_schema_version(dataset_id)
        schema_version = int(current["VERSION"]) if current else 1

    # ── 7. Create pipeline run record ─────────────────────────────────────────
    run_pk = snowflake_client.insert_pipeline_run(
        dataset_id=dataset_id,
        dag_id="upload_api",
        run_id=run_id,
        triggered_by="api",
    )

    # ── 8. Bulk-insert into Bronze ────────────────────────────────────────────
    rows_ingested = 0
    rows_rejected = 0
    col_names = [c.name for c in inferred_columns]

    try:
        rows_ingested = _bulk_insert(
            df=df,
            col_names=col_names,
            bronze_table=bronze_table,
            source_file=filename,
            pipeline_run_id=run_id,
            schema_version=schema_version,
        )
    except Exception as exc:
        rows_rejected = total_rows
        snowflake_client.complete_pipeline_run(
            run_pk, status="failed", rows_rejected=rows_rejected, error_message=str(exc)
        )
        raise

    snowflake_client.complete_pipeline_run(
        run_pk,
        status="success",
        rows_ingested=rows_ingested,
        rows_rejected=rows_rejected,
    )

    # ── 9. Publish Kafka event (best-effort) ──────────────────────────────────
    kafka_event_id: str | None = None
    try:
        kafka_event_id = publish_event(
            KafkaTopic.SCHEMA_EVENTS,
            payload={
                "dataset_id": dataset_id,
                "dataset_name": dataset_name,
                "is_new_dataset": is_new_dataset,
                "schema_version": schema_version,
                "fingerprint": new_fingerprint,
                "column_count": len(inferred_columns),
                "schema_changes": [c.model_dump() for c in schema_changes],
                "pipeline_run_id": run_id,
                "rows_ingested": rows_ingested,
            },
            dataset_id=dataset_id,
        )
    except Exception as exc:
        logger.warning("Kafka publish failed (non-fatal)", error=str(exc))

    logger.info(
        "Upload complete",
        dataset_id=dataset_id,
        rows_ingested=rows_ingested,
        schema_version=schema_version,
        is_new=is_new_dataset,
    )

    return UploadResponse(
        dataset_id=dataset_id,
        dataset_name=dataset_name,
        is_new_dataset=is_new_dataset,
        pipeline_run_id=run_id,
        rows_ingested=rows_ingested,
        rows_rejected=rows_rejected,
        bronze_table=f"BRONZE.{bronze_table.upper()}",
        schema_version=schema_version,
        schema_fingerprint=new_fingerprint,
        schema_changes=schema_changes,
        kafka_event_id=kafka_event_id,
        ingested_at=ingested_at,
    )


# ─── Private Helpers ─────────────────────────────────────────────────────────

def _register_new_dataset(
    *,
    dataset_name: str,
    description: str,
    domain: str,
    source_type: str,
) -> str:
    """Insert a new DATASETS row and return its UUID."""
    return snowflake_client.insert_dataset(
        name=dataset_name,
        source_type=source_type,
        description=description,
        domain=domain,
    )


def _load_current_columns(dataset_id: str) -> list[SchemaColumn]:
    """
    Load the current schema version's columns from Snowflake.
    Returns an empty list if no schema version exists yet.
    """
    sv = snowflake_client.get_current_schema_version(dataset_id)
    if sv is None:
        return []

    raw_json = sv.get("SCHEMA_JSON") or sv.get("schema_json", [])
    # SCHEMA_JSON comes back as a Python list/dict from snowflake DictCursor
    if isinstance(raw_json, str):
        import json
        raw_json = json.loads(raw_json)

    return [
        SchemaColumn(
            name=col["name"],
            snowflake_type=col["snowflake_type"],
            pandas_dtype=col.get("pandas_dtype", "object"),
            nullable=col.get("nullable", True),
        )
        for col in (raw_json or [])
    ]


def _bulk_insert(
    *,
    df: pd.DataFrame,
    col_names: list[str],
    bronze_table: str,
    source_file: str,
    pipeline_run_id: str,
    schema_version: int,
) -> int:
    """
    Insert the DataFrame into the Bronze table in batches.

    Returns:
        Total rows successfully inserted.
    """
    total_inserted = 0
    records = df[col_names].where(pd.notnull(df[col_names]), None)

    for start in range(0, len(records), _BATCH_SIZE):
        batch = records.iloc[start : start + _BATCH_SIZE]
        rows_as_tuples = [tuple(row) for row in batch.itertuples(index=False, name=None)]

        inserted = snowflake_client.bulk_insert_bronze(
            table_name=bronze_table,
            columns=col_names,
            rows=rows_as_tuples,
            source_file=source_file,
            pipeline_run_id=pipeline_run_id,
            schema_version=schema_version,
        )
        total_inserted += inserted

    return total_inserted


def _ext_to_source_type(filename: str) -> str:
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "csv"
    return ext if ext in {"csv", "json", "jsonl"} else "csv"
