"""
backend/services/schema_service.py
====================================
Schema inference, versioning, and change detection service.

Responsibilities:
  1. Infer a typed schema from a pandas DataFrame (dataset-agnostic).
  2. Map pandas dtypes → Snowflake SQL types.
  3. Compute a deterministic fingerprint for change detection.
  4. Detect and classify schema changes between two versions.
  5. Register new schema versions in Snowflake.

All business logic here is pure-Python and easily unit-testable
(no Snowflake calls in the inference functions).
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import numpy as np
import pandas as pd

from backend.core.exceptions import SchemaInferenceError
from backend.core import snowflake_client
from backend.models.dataset import SchemaColumn
from backend.models.metadata import SchemaChangeSummary
from config.constants import SchemaChangeType
from config.logging_config import get_logger

logger = get_logger(__name__)


# ─── dtype → Snowflake type mapping ──────────────────────────────────────────

_DTYPE_MAP: dict[str, str] = {
    # Integers
    "int8":    "NUMBER(5,0)",
    "int16":   "NUMBER(7,0)",
    "int32":   "NUMBER(10,0)",
    "int64":   "NUMBER(18,0)",
    "uint8":   "NUMBER(5,0)",
    "uint16":  "NUMBER(7,0)",
    "uint32":  "NUMBER(10,0)",
    "uint64":  "NUMBER(18,0)",
    # Floats
    "float16": "FLOAT",
    "float32":  "FLOAT",
    "float64":  "FLOAT",
    # Boolean
    "bool":    "BOOLEAN",
    # Datetime
    "datetime64[ns]":     "TIMESTAMP_NTZ",
    "datetime64[ns, UTC]": "TIMESTAMP_LTZ",
    "datetime64[us]":     "TIMESTAMP_NTZ",
    "datetime64[us, UTC]": "TIMESTAMP_LTZ",
    # Date
    "date":    "DATE",
    # Default / string / object
    "object":  "VARCHAR(65535)",
    "string":  "VARCHAR(65535)",
    "category": "VARCHAR(65535)",
}


def _pandas_dtype_to_snowflake(dtype: Any) -> str:
    """
    Map a pandas dtype to the most appropriate Snowflake SQL type.

    Unknown or complex dtypes fall back to VARCHAR(65535).
    """
    dtype_str = str(dtype)

    # Exact match first
    if dtype_str in _DTYPE_MAP:
        return _DTYPE_MAP[dtype_str]

    # Prefix match for datetime variants not in the dict
    if dtype_str.startswith("datetime64"):
        return "TIMESTAMP_NTZ"
    if dtype_str.startswith("timedelta"):
        return "VARCHAR(100)"
    if "int" in dtype_str:
        return "NUMBER(18,0)"
    if "float" in dtype_str:
        return "FLOAT"

    return "VARCHAR(65535)"


# ─── Schema Inference ─────────────────────────────────────────────────────────

def infer_schema(df: pd.DataFrame, sample_size: int = 5) -> list[SchemaColumn]:
    """
    Infer a typed schema from a pandas DataFrame.

    Args:
        df:          The DataFrame to inspect.
        sample_size: Number of non-null sample values to capture per column.

    Returns:
        List of SchemaColumn objects, one per DataFrame column.

    Raises:
        SchemaInferenceError: If the DataFrame has no columns.
    """
    if df.empty or len(df.columns) == 0:
        raise SchemaInferenceError(
            message="Cannot infer schema from an empty DataFrame",
            detail="The uploaded file has no columns or no data rows.",
        )

    columns: list[SchemaColumn] = []
    for col_name in df.columns:
        series = df[col_name]
        dtype  = series.dtype
        sf_type = _pandas_dtype_to_snowflake(dtype)

        # Attempt type promotion: try to parse object columns as dates / numbers
        if sf_type == "VARCHAR(65535)":
            sf_type = _attempt_type_promotion(series) or sf_type

        # Sample non-null values for preview
        non_null = series.dropna()
        sample_raw = non_null.head(sample_size).tolist()
        sample = [_json_safe(v) for v in sample_raw]

        columns.append(
            SchemaColumn(
                name=col_name,
                snowflake_type=sf_type,
                pandas_dtype=str(dtype),
                nullable=bool(series.isna().any()),
                sample_values=sample,
            )
        )

    logger.debug("Schema inferred", column_count=len(columns))
    return columns


def _attempt_type_promotion(series: pd.Series) -> str | None:
    """
    Try to promote an object-dtype column to a more specific type.
    Returns a Snowflake type string or None if no promotion is possible.
    """
    non_null = series.dropna()
    if len(non_null) == 0:
        return None

    # Try numeric
    try:
        pd.to_numeric(non_null, errors="raise")
        # Check if any decimal points exist
        if non_null.astype(str).str.contains(r"\.").any():
            return "FLOAT"
        return "NUMBER(18,0)"
    except (ValueError, TypeError):
        pass

    # Try datetime
    try:
        pd.to_datetime(non_null, infer_datetime_format=True, errors="raise")
        return "TIMESTAMP_NTZ"
    except (ValueError, TypeError):
        pass

    # Try boolean
    bool_vals = {"true", "false", "1", "0", "yes", "no", "t", "f"}
    if set(non_null.astype(str).str.lower().unique()) <= bool_vals:
        return "BOOLEAN"

    return None


def _json_safe(value: Any) -> Any:
    """Convert numpy scalars to Python native types for JSON serialisation."""
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, (np.ndarray,)):
        return value.tolist()
    if pd.isna(value) if not isinstance(value, (list, dict)) else False:
        return None
    return value


# ─── Fingerprinting ───────────────────────────────────────────────────────────

def compute_fingerprint(columns: list[SchemaColumn]) -> str:
    """
    Compute a short, stable fingerprint for a list of SchemaColumn objects.

    The fingerprint is derived from the SHA-256 of the canonical JSON
    representation (name + snowflake_type only, sorted by name).

    Returns:
        First 16 hex chars of the SHA-256 hash.
    """
    canonical_list = sorted(
        [{"name": c.name, "type": c.snowflake_type} for c in columns],
        key=lambda x: x["name"],
    )
    canonical_str = json.dumps(canonical_list, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(canonical_str.encode()).hexdigest()[:16]


# ─── Schema Change Detection ──────────────────────────────────────────────────

def detect_schema_changes(
    old_columns: list[SchemaColumn],
    new_columns: list[SchemaColumn],
) -> list[SchemaChangeSummary]:
    """
    Compare two schema column lists and return a list of detected changes.

    Detects:
      - ADD_COLUMN    : column present in new but not in old
      - DROP_COLUMN   : column present in old but not in new
      - TYPE_CHANGE   : same column name but different Snowflake type

    Args:
        old_columns: Previously registered schema columns.
        new_columns: Newly inferred schema columns.

    Returns:
        List of SchemaChangeSummary objects (empty if no changes).
    """
    old_map = {c.name: c for c in old_columns}
    new_map = {c.name: c for c in new_columns}

    changes: list[SchemaChangeSummary] = []

    for name, new_col in new_map.items():
        if name not in old_map:
            changes.append(SchemaChangeSummary(
                change_type=SchemaChangeType.ADD_COLUMN,
                column_name=name,
                detail=f"New column '{name}' ({new_col.snowflake_type}) detected",
            ))
        elif old_map[name].snowflake_type != new_col.snowflake_type:
            changes.append(SchemaChangeSummary(
                change_type=SchemaChangeType.TYPE_CHANGE,
                column_name=name,
                detail=(
                    f"Type changed: {old_map[name].snowflake_type}"
                    f" → {new_col.snowflake_type}"
                ),
            ))

    for name in old_map:
        if name not in new_map:
            changes.append(SchemaChangeSummary(
                change_type=SchemaChangeType.DROP_COLUMN,
                column_name=name,
                detail=f"Column '{name}' no longer present in uploaded data",
            ))

    return changes


def columns_to_dict_list(columns: list[SchemaColumn]) -> list[dict]:
    """Serialise a list of SchemaColumn to a plain dict list for Snowflake storage."""
    return [c.model_dump(exclude={"sample_values"}) for c in columns]


# ─── Snowflake Registration ───────────────────────────────────────────────────

def register_initial_schema(
    dataset_id: str,
    columns: list[SchemaColumn],
) -> str:
    """
    Register version 1 of a dataset's schema in Snowflake.

    Returns:
        The new schema version UUID.
    """
    return snowflake_client.insert_schema_version(
        dataset_id=dataset_id,
        version=1,
        schema_json=columns_to_dict_list(columns),
        change_summary="Initial schema (version 1)",
    )


def register_new_schema_version(
    dataset_id: str,
    columns: list[SchemaColumn],
    changes: list[SchemaChangeSummary],
) -> str:
    """
    Register the next schema version after changes are detected.

    Returns:
        The new schema version UUID.
    """
    current = snowflake_client.get_current_schema_version(dataset_id)
    next_version = (int(current["VERSION"]) + 1) if current else 1
    change_text = "; ".join(f"{c.change_type}: {c.column_name}" for c in changes)

    return snowflake_client.insert_schema_version(
        dataset_id=dataset_id,
        version=next_version,
        schema_json=columns_to_dict_list(columns),
        change_summary=change_text,
    )
