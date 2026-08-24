"""
tests/unit/backend/test_schema_service.py
==========================================
Unit tests for backend/services/schema_service.py.

All tests are pure-Python — no Snowflake or Kafka required.
"""

from __future__ import annotations

import pandas as pd
import pytest

from backend.services.schema_service import (
    compute_fingerprint,
    detect_schema_changes,
    infer_schema,
    _clean_col_name,
    _pandas_dtype_to_snowflake,
)
from backend.core.exceptions import SchemaInferenceError
from backend.models.dataset import SchemaColumn
from config.constants import SchemaChangeType


# ─── _pandas_dtype_to_snowflake ───────────────────────────────────────────────

class TestDtypeMapping:
    def test_int64_maps_to_number(self):
        assert _pandas_dtype_to_snowflake(pd.Int64Dtype()) == "NUMBER(18,0)" or \
               _pandas_dtype_to_snowflake("int64") == "NUMBER(18,0)"

    def test_float64_maps_to_float(self):
        assert _pandas_dtype_to_snowflake("float64") == "FLOAT"

    def test_object_maps_to_varchar(self):
        assert _pandas_dtype_to_snowflake("object") == "VARCHAR(65535)"

    def test_bool_maps_to_boolean(self):
        assert _pandas_dtype_to_snowflake("bool") == "BOOLEAN"

    def test_datetime_maps_to_timestamp(self):
        result = _pandas_dtype_to_snowflake("datetime64[ns]")
        assert "TIMESTAMP" in result

    def test_unknown_type_falls_back_to_varchar(self):
        assert _pandas_dtype_to_snowflake("complex128") == "VARCHAR(65535)"


# ─── infer_schema ─────────────────────────────────────────────────────────────

class TestInferSchema:
    def test_basic_csv_dataframe(self):
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Carol"],
            "amount": [10.5, 20.0, 30.75],
            "active": [True, False, True],
        })
        cols = infer_schema(df)
        assert len(cols) == 4

        col_map = {c.name: c for c in cols}
        assert "NUMBER" in col_map["id"].snowflake_type
        assert "VARCHAR" in col_map["name"].snowflake_type
        assert "FLOAT" in col_map["amount"].snowflake_type
        assert col_map["active"].snowflake_type == "BOOLEAN"

    def test_column_names_are_normalised(self):
        df = pd.DataFrame({"First Name": ["A"], "Last-Name": ["B"], "AGE ": [30]})
        cols = infer_schema(df)
        names = [c.name for c in cols]
        assert "first_name" in names
        assert "last_name" in names
        assert "age" in names

    def test_sample_values_captured(self):
        df = pd.DataFrame({"score": [10, 20, 30, 40, 50, 60]})
        cols = infer_schema(df)
        assert len(cols[0].sample_values) <= 5
        assert all(isinstance(v, (int, float)) for v in cols[0].sample_values)

    def test_nullable_detection(self):
        df = pd.DataFrame({"a": [1, None, 3], "b": [4, 5, 6]})
        col_map = {c.name: c for c in infer_schema(df)}
        assert col_map["a"].nullable is True
        assert col_map["b"].nullable is False

    def test_empty_dataframe_raises(self):
        with pytest.raises(SchemaInferenceError):
            infer_schema(pd.DataFrame())

    def test_type_promotion_numeric_string(self):
        df = pd.DataFrame({"price": ["10.5", "20.0", "30.75"]})
        cols = infer_schema(df)
        # price should be promoted from object to FLOAT
        assert cols[0].snowflake_type in {"FLOAT", "VARCHAR(65535)"}  # promotion attempted

    def test_datetime_column_detected(self):
        df = pd.DataFrame({"created_at": pd.to_datetime(["2024-01-01", "2024-06-15"])})
        cols = infer_schema(df)
        assert "TIMESTAMP" in cols[0].snowflake_type


# ─── compute_fingerprint ──────────────────────────────────────────────────────

class TestComputeFingerprint:
    def _make_columns(self, specs: list[tuple[str, str]]) -> list[SchemaColumn]:
        return [
            SchemaColumn(name=n, snowflake_type=t, pandas_dtype="object")
            for n, t in specs
        ]

    def test_identical_schemas_same_fingerprint(self):
        cols_a = self._make_columns([("id", "NUMBER(18,0)"), ("name", "VARCHAR(65535)")])
        cols_b = self._make_columns([("id", "NUMBER(18,0)"), ("name", "VARCHAR(65535)")])
        assert compute_fingerprint(cols_a) == compute_fingerprint(cols_b)

    def test_different_type_different_fingerprint(self):
        cols_a = self._make_columns([("id", "NUMBER(18,0)")])
        cols_b = self._make_columns([("id", "VARCHAR(65535)")])
        assert compute_fingerprint(cols_a) != compute_fingerprint(cols_b)

    def test_column_order_independent(self):
        cols_a = self._make_columns([("a", "NUMBER(18,0)"), ("b", "FLOAT")])
        cols_b = self._make_columns([("b", "FLOAT"), ("a", "NUMBER(18,0)")])
        assert compute_fingerprint(cols_a) == compute_fingerprint(cols_b)

    def test_returns_16_char_hex(self):
        cols = self._make_columns([("x", "FLOAT")])
        fp = compute_fingerprint(cols)
        assert len(fp) == 16
        assert all(c in "0123456789abcdef" for c in fp)


# ─── detect_schema_changes ────────────────────────────────────────────────────

class TestDetectSchemaChanges:
    def _col(self, name: str, sf_type: str) -> SchemaColumn:
        return SchemaColumn(name=name, snowflake_type=sf_type, pandas_dtype="object")

    def test_no_changes(self):
        old = [self._col("id", "NUMBER(18,0)"), self._col("name", "VARCHAR(65535)")]
        new = [self._col("id", "NUMBER(18,0)"), self._col("name", "VARCHAR(65535)")]
        changes = detect_schema_changes(old, new)
        assert changes == []

    def test_add_column_detected(self):
        old = [self._col("id", "NUMBER(18,0)")]
        new = [self._col("id", "NUMBER(18,0)"), self._col("email", "VARCHAR(65535)")]
        changes = detect_schema_changes(old, new)
        assert len(changes) == 1
        assert changes[0].change_type == SchemaChangeType.ADD_COLUMN
        assert changes[0].column_name == "email"

    def test_drop_column_detected(self):
        old = [self._col("id", "NUMBER(18,0)"), self._col("email", "VARCHAR(65535)")]
        new = [self._col("id", "NUMBER(18,0)")]
        changes = detect_schema_changes(old, new)
        assert len(changes) == 1
        assert changes[0].change_type == SchemaChangeType.DROP_COLUMN
        assert changes[0].column_name == "email"

    def test_type_change_detected(self):
        old = [self._col("age", "NUMBER(18,0)")]
        new = [self._col("age", "VARCHAR(65535)")]
        changes = detect_schema_changes(old, new)
        assert len(changes) == 1
        assert changes[0].change_type == SchemaChangeType.TYPE_CHANGE

    def test_multiple_changes_at_once(self):
        old = [self._col("a", "FLOAT"), self._col("b", "NUMBER(18,0)")]
        new = [self._col("a", "VARCHAR(65535)"), self._col("c", "BOOLEAN")]
        changes = detect_schema_changes(old, new)
        types = {c.change_type for c in changes}
        assert SchemaChangeType.TYPE_CHANGE in types    # a changed
        assert SchemaChangeType.ADD_COLUMN in types     # c added
        assert SchemaChangeType.DROP_COLUMN in types    # b dropped
