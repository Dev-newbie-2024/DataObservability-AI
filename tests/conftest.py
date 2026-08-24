"""
tests/conftest.py
==================
Shared pytest fixtures for all test layers.
"""

from __future__ import annotations

import os

# Configure logging BEFORE any backend imports to avoid structlog PrintLogger issues
os.environ.setdefault("APP_ENV", "development")
os.environ.setdefault("SNOWFLAKE_ACCOUNT", "test_account.us-east-1")
os.environ.setdefault("SNOWFLAKE_USER", "test_user")
os.environ.setdefault("SNOWFLAKE_PASSWORD", "test_password")
os.environ.setdefault("SNOWFLAKE_DATABASE", "TEST_DB")
os.environ.setdefault("SNOWFLAKE_WAREHOUSE", "TEST_WH")
os.environ.setdefault("SNOWFLAKE_ROLE", "SYSADMIN")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("LOG_LEVEL", "WARNING")

from config.logging_config import configure_logging  # noqa: E402

configure_logging()

import pytest  # noqa: E402



# ─── Set test environment variables before any imports ───────────────────────
@pytest.fixture(scope="session", autouse=True)
def set_test_env() -> None:
    """Ensure a safe test environment is set for all tests."""
    os.environ.setdefault("APP_ENV", "development")
    os.environ.setdefault("SNOWFLAKE_ACCOUNT", "test_account")
    os.environ.setdefault("SNOWFLAKE_USER", "test_user")
    os.environ.setdefault("SNOWFLAKE_PASSWORD", "test_password")
    os.environ.setdefault("SNOWFLAKE_DATABASE", "TEST_DB")
    os.environ.setdefault("SNOWFLAKE_WAREHOUSE", "TEST_WH")
    os.environ.setdefault("SNOWFLAKE_ROLE", "SYSADMIN")
    os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    os.environ.setdefault("LOG_LEVEL", "WARNING")  # Suppress logs in tests


@pytest.fixture
def sample_dataset_context() -> dict:
    """Sample pipeline event context for agent tests."""
    return {
        "dataset_id": "test-uuid-1234",
        "dataset_name": "test_sales",
        "pipeline_run_id": "run-uuid-5678",
        "dag_id": "ingestion_dag",
        "source_type": "csv",
    }


@pytest.fixture
def sample_schema() -> dict:
    """Sample inferred schema for schema agent tests."""
    return {
        "columns": [
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "name", "type": "VARCHAR", "nullable": True},
            {"name": "amount", "type": "FLOAT", "nullable": True},
            {"name": "created_at", "type": "TIMESTAMP", "nullable": True},
        ]
    }


@pytest.fixture
def sample_quality_metrics() -> list[dict]:
    """Sample quality metrics for quality agent tests."""
    return [
        {"column": "id", "metric_type": "null_rate", "value": 0.0, "passed": True},
        {"column": "name", "metric_type": "null_rate", "value": 0.02, "passed": True},
        {"column": "amount", "metric_type": "out_of_range", "value": 0.0, "passed": True},
        {"column": "all", "metric_type": "duplicate_rate", "value": 0.01, "passed": True},
    ]
