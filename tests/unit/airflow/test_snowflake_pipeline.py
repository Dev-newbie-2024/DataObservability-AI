"""
tests/unit/airflow/test_snowflake_pipeline.py
===============================================
Unit tests for backend/core/snowflake_pipeline.py.

All Snowflake calls are mocked — no real connection required.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch, call

import pytest

from backend.core.snowflake_pipeline import (
    _bronze_to_silver_name,
    _is_numeric,
    _silver_to_gold_name,
    record_lineage,
    ensure_lineage_table,
    create_silver_table,
    create_gold_table,
)


# ─── Name helpers ─────────────────────────────────────────────────────────────

class TestNameHelpers:
    def test_bronze_to_silver_strips_raw_suffix(self):
        assert _bronze_to_silver_name("sales_data_raw") == "sales_data_silver"

    def test_bronze_to_silver_without_raw_suffix(self):
        # Tables without _raw suffix just get _silver appended
        assert _bronze_to_silver_name("customers") == "customers_silver"

    def test_silver_to_gold_strips_silver_suffix(self):
        assert _silver_to_gold_name("sales_data_silver") == "sales_data_gold"

    def test_silver_to_gold_without_silver_suffix(self):
        assert _silver_to_gold_name("orders") == "orders_gold"


# ─── _is_numeric ──────────────────────────────────────────────────────────────

class TestIsNumeric:
    @pytest.mark.parametrize("sf_type,expected", [
        ("NUMBER(18,0)", True),
        ("FLOAT",        True),
        ("INTEGER",      True),
        ("DECIMAL(10,2)", True),
        ("NUMERIC",      True),
        ("VARCHAR(255)", False),
        ("BOOLEAN",      False),
        ("TIMESTAMP_NTZ", False),
        ("DATE",         False),
    ])
    def test_is_numeric(self, sf_type: str, expected: bool):
        assert _is_numeric(sf_type) is expected


# ─── create_silver_table ──────────────────────────────────────────────────────

class TestCreateSilverTable:
    _COLUMNS = [
        {"name": "id",    "snowflake_type": "NUMBER(18,0)"},
        {"name": "name",  "snowflake_type": "VARCHAR(255)"},
        {"name": "score", "snowflake_type": "FLOAT"},
    ]

    @patch("backend.core.snowflake_pipeline.execute_statement")
    def test_returns_silver_table_name(self, mock_exec):
        result = create_silver_table("sales_raw", self._COLUMNS)
        assert result == "sales_silver"

    @patch("backend.core.snowflake_pipeline.execute_statement")
    def test_calls_create_table_if_not_exists(self, mock_exec):
        create_silver_table("sales_raw", self._COLUMNS)
        mock_exec.assert_called_once()
        sql: str = mock_exec.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert "SILVER" in sql.upper()

    @patch("backend.core.snowflake_pipeline.execute_statement")
    def test_includes_audit_columns(self, mock_exec):
        create_silver_table("sales_raw", self._COLUMNS)
        sql: str = mock_exec.call_args[0][0]
        assert "_silver_id" in sql
        assert "_quality_score" in sql
        assert "_is_quarantined" in sql


# ─── create_gold_table ────────────────────────────────────────────────────────

class TestCreateGoldTable:
    _COLUMNS = [
        {"name": "id",    "snowflake_type": "NUMBER(18,0)"},
        {"name": "revenue","snowflake_type": "FLOAT"},
    ]

    @patch("backend.core.snowflake_pipeline.execute_statement")
    def test_returns_gold_table_name(self, mock_exec):
        result = create_gold_table("sales_silver", self._COLUMNS)
        assert result == "sales_gold"

    @patch("backend.core.snowflake_pipeline.execute_statement")
    def test_includes_aggregate_expressions_for_numeric_cols(self, mock_exec):
        create_gold_table("orders_silver", self._COLUMNS)
        sql: str = mock_exec.call_args[0][0]
        # Should have agg columns for numeric (id, revenue)
        assert "_row_count" in sql
        assert "_quality_score_avg" in sql

    @patch("backend.core.snowflake_pipeline.execute_statement")
    def test_skips_aggregates_for_no_numeric_cols(self, mock_exec):
        str_only = [{"name": "country", "snowflake_type": "VARCHAR(255)"}]
        create_gold_table("geo_silver", str_only)
        sql: str = mock_exec.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql  # still creates table


# ─── record_lineage ───────────────────────────────────────────────────────────

class TestRecordLineage:
    @patch("backend.core.snowflake_pipeline.execute_statement")
    def test_returns_uuid_string(self, mock_exec):
        lineage_id = record_lineage(
            dataset_id="ds-001",
            pipeline_run_id="run-001",
            source_layer="BRONZE",
            target_layer="SILVER",
            source_table="sales_raw",
            target_table="sales_silver",
            rows_in=100,
            rows_out=95,
            rows_rejected=5,
        )
        assert len(lineage_id) == 36  # UUID format
        assert lineage_id.count("-") == 4

    @patch("backend.core.snowflake_pipeline.execute_statement")
    def test_calls_insert_with_correct_layers(self, mock_exec):
        record_lineage(
            dataset_id="ds-001",
            pipeline_run_id="run-001",
            source_layer="silver",   # lowercase input
            target_layer="gold",
            source_table="t_silver",
            target_table="t_gold",
            rows_in=50,
            rows_out=50,
        )
        mock_exec.assert_called_once()
        params = mock_exec.call_args[0][1]
        # source_layer and target_layer should be uppercased
        assert "SILVER" in params
        assert "GOLD" in params

    @patch("backend.core.snowflake_pipeline.execute_statement")
    def test_rows_rejected_defaults_to_zero(self, mock_exec):
        record_lineage(
            dataset_id="ds-001",
            pipeline_run_id="run-001",
            source_layer="BRONZE",
            target_layer="SILVER",
            source_table="t_raw",
            target_table="t_silver",
            rows_in=10,
            rows_out=10,
        )
        params = mock_exec.call_args[0][1]
        # rows_rejected (index 10) should be 0
        assert params[10] == 0


# ─── ensure_lineage_table ─────────────────────────────────────────────────────

class TestEnsureLineageTable:
    @patch("backend.core.snowflake_pipeline.execute_statement")
    def test_calls_create_table_if_not_exists(self, mock_exec):
        ensure_lineage_table()
        sql: str = mock_exec.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert "DATASET_LINEAGE" in sql
