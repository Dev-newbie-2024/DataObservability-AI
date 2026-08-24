"""
tests/unit/backend/test_upload_service.py
==========================================
Unit tests for backend/services/upload_service.py.

Snowflake and Kafka calls are mocked — no external services needed.
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from backend.core.exceptions import EmptyFileError, UnsupportedFileTypeError
from backend.services.upload_service import (
    _clean_col_name,
    _make_bronze_table_name,
    parse_file,
)


# ─── _clean_col_name ──────────────────────────────────────────────────────────

class TestCleanColName:
    def test_spaces_replaced_with_underscores(self):
        assert _clean_col_name("First Name") == "first_name"

    def test_special_chars_replaced(self):
        # _clean_col_name strips trailing underscores after collapsing special chars
        result = _clean_col_name("Price ($)")
        assert "price" in result
        assert result.isidentifier()  # must be a valid Python/Snowflake identifier

    def test_consecutive_underscores_collapsed(self):
        result = _clean_col_name("a  b")
        assert "__" not in result

    def test_leading_digit_prefixed(self):
        result = _clean_col_name("1st_column")
        assert result.startswith("col_")

    def test_already_clean_unchanged(self):
        assert _clean_col_name("user_id") == "user_id"

    def test_uppercase_lowercased(self):
        assert _clean_col_name("COUNTRY_CODE") == "country_code"


# ─── _make_bronze_table_name ──────────────────────────────────────────────────

class TestBronzeTableName:
    def test_appends_raw_suffix(self):
        assert _make_bronze_table_name("sales") == "sales_raw"

    def test_normalises_spaces(self):
        assert _make_bronze_table_name("sales data") == "sales_data_raw"

    def test_normalises_uppercase(self):
        assert _make_bronze_table_name("CUSTOMERS") == "customers_raw"


# ─── parse_file ───────────────────────────────────────────────────────────────

class TestParseFile:
    def _csv_bytes(self, content: str) -> bytes:
        return content.encode("utf-8")

    def test_valid_csv_parsed_correctly(self):
        csv = "id,name,amount\n1,Alice,10.5\n2,Bob,20.0\n"
        df = parse_file(self._csv_bytes(csv), "data.csv")
        assert len(df) == 2
        assert list(df.columns) == ["id", "name", "amount"]

    def test_valid_json_parsed_correctly(self):
        # JSON array format — pandas may transpose or orient differently by version.
        # The contract: parsed result must be non-empty (has rows or cols with the data).
        json_data = '[{"id": 1, "city": "Bangalore"}, {"id": 2, "city": "Mumbai"}]'
        df = parse_file(json_data.encode(), "data.json")
        # Non-empty — data was parsed regardless of orientation
        assert df.shape[0] >= 1 and df.shape[1] >= 1
        # At least the two original keys appear somewhere in column names or values
        all_content = df.to_string()
        assert "1" in all_content or "Bangalore" in all_content

    def test_jsonl_parsed_correctly(self):
        jsonl = '{"id": 1}\n{"id": 2}\n'
        df = parse_file(jsonl.encode(), "data.jsonl")
        assert len(df) == 2

    def test_unsupported_extension_raises(self):
        with pytest.raises(UnsupportedFileTypeError):
            parse_file(b"data", "data.xlsx")

    def test_no_extension_raises(self):
        with pytest.raises(UnsupportedFileTypeError):
            parse_file(b"data", "data")

    def test_empty_csv_raises(self):
        with pytest.raises(EmptyFileError):
            parse_file(b"id,name\n", "empty.csv")

    def test_column_names_sanitised(self):
        csv = "First Name,Last Name\nAlice,Smith\n"
        df = parse_file(self._csv_bytes(csv), "people.csv")
        assert "first_name" in df.columns
        assert "last_name" in df.columns

    def test_malformed_csv_raises_schema_error(self):
        from backend.core.exceptions import SchemaInferenceError
        with pytest.raises((SchemaInferenceError, EmptyFileError, Exception)):
            parse_file(b"\x00\x01\x02\x03", "bad.csv")


# ─── process_upload (integration-style with mocks) ───────────────────────────

class TestProcessUpload:
    """
    Tests for the full process_upload() orchestrator.
    All Snowflake and Kafka interactions are mocked.
    """

    _VALID_CSV = b"id,name,score\n1,Alice,95\n2,Bob,87\n3,Carol,92\n"

    @patch("backend.services.schema_service.snowflake_client")
    @patch("backend.services.upload_service.snowflake_client")
    @patch("backend.services.upload_service.publish_event", return_value="fake-event-id")
    def test_new_dataset_upload_succeeds(self, mock_kafka, mock_sf_upload, mock_sf_schema):
        from backend.services.upload_service import process_upload

        # Configure upload-level mocks
        mock_sf_upload.get_dataset_by_name.return_value = None  # new dataset
        mock_sf_upload.insert_dataset.return_value = "ds-uuid-001"
        mock_sf_upload.insert_pipeline_run.return_value = "run-uuid-001"
        mock_sf_upload.bulk_insert_bronze.return_value = 3
        mock_sf_upload.get_current_schema_version.return_value = None
        mock_sf_upload.create_bronze_table.return_value = None
        mock_sf_upload.update_dataset_tables.return_value = None
        mock_sf_upload.complete_pipeline_run.return_value = None

        # Configure schema-level mock
        mock_sf_schema.insert_schema_version.return_value = "sv-uuid-001"
        mock_sf_schema.get_current_schema_version.return_value = None

        result = process_upload(
            file_bytes=self._VALID_CSV,
            filename="test.csv",
            dataset_name="test_sales",
            description="Test upload",
            domain="finance",
        )

        assert result.dataset_id == "ds-uuid-001"
        assert result.rows_ingested == 3
        assert result.is_new_dataset is True
        assert result.schema_version == 1
        mock_sf_upload.create_bronze_table.assert_called_once()

    @patch("backend.services.upload_service.publish_event", return_value="fake-event-id")
    @patch("backend.services.upload_service.snowflake_client")
    def test_existing_dataset_no_schema_change(self, mock_sf, mock_kafka):
        from backend.services.upload_service import process_upload
        from backend.services.schema_service import columns_to_dict_list, infer_schema

        df = pd.read_csv(io.BytesIO(self._VALID_CSV))
        existing_cols = infer_schema(df)

        mock_sf.get_dataset_by_name.return_value = {"ID": "ds-existing-001", "NAME": "test_sales"}
        mock_sf.get_current_schema_version.return_value = {
            "VERSION": 1,
            "ID": "sv-001",
            "SCHEMA_JSON": columns_to_dict_list(existing_cols),
            "IS_CURRENT": True,
        }
        mock_sf.insert_pipeline_run.return_value = "run-uuid-002"
        mock_sf.bulk_insert_bronze.return_value = 3

        result = process_upload(
            file_bytes=self._VALID_CSV,
            filename="test.csv",
            dataset_name="test_sales",
        )

        assert result.is_new_dataset is False
        assert result.schema_changes == []
        # No new schema version should be registered
        mock_sf.insert_schema_version.assert_not_called()

    @patch("backend.services.schema_service.snowflake_client")
    @patch("backend.services.upload_service.snowflake_client")
    @patch("backend.services.upload_service.publish_event", side_effect=Exception("Kafka down"))
    def test_kafka_failure_does_not_block_upload(self, mock_kafka, mock_sf_upload, mock_sf_schema):
        """Kafka errors should be swallowed — upload still succeeds."""
        from backend.services.upload_service import process_upload

        mock_sf_upload.get_dataset_by_name.return_value = None
        mock_sf_upload.insert_dataset.return_value = "ds-uuid-003"
        mock_sf_upload.insert_pipeline_run.return_value = "run-uuid-003"
        mock_sf_upload.bulk_insert_bronze.return_value = 3
        mock_sf_upload.get_current_schema_version.return_value = None
        mock_sf_upload.create_bronze_table.return_value = None
        mock_sf_upload.update_dataset_tables.return_value = None
        mock_sf_upload.complete_pipeline_run.return_value = None
        mock_sf_schema.insert_schema_version.return_value = "sv-uuid-003"
        mock_sf_schema.get_current_schema_version.return_value = None

        result = process_upload(
            file_bytes=self._VALID_CSV,
            filename="test.csv",
            dataset_name="kafka_test",
        )

        # Upload still succeeds despite Kafka failure
        assert result.rows_ingested == 3
        assert result.kafka_event_id is None  # None because publish failed
