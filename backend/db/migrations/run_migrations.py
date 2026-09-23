"""
backend/db/migrations/run_migrations.py
========================================
Runs all Snowflake DDL migration scripts in order.
Execute via: python -m backend.db.run_migrations

All control-plane tables live in the OBSERVABILITY schema in Snowflake.
"""

from __future__ import annotations

import os
from pathlib import Path

import snowflake.connector

from config.logging_config import get_logger
from config.settings import snowflake_settings

logger = get_logger(__name__)

MIGRATIONS_DIR = Path(__file__).parent

SQL_MIGRATIONS = [
    "001_create_datasets.sql",
    "002_create_pipeline_runs.sql",
    "003_create_quality_tables.sql",
    "004_create_drift_tables.sql",
    "005_create_cost_tables.sql",
    "006_create_alert_tables.sql",
    "007_create_agent_sessions.sql",
    "008_create_lineage_edges.sql",
    "009_create_dataset_lineage.sql",
    "010_create_data_quality_metrics.sql",
]


def run_migrations() -> None:
    """Connect to Snowflake and execute all DDL migration scripts."""
    conn = snowflake.connector.connect(**snowflake_settings.connection_params)
    cursor = conn.cursor()

    try:
        # Ensure the OBSERVABILITY schema exists
        cursor.execute(
            f"CREATE SCHEMA IF NOT EXISTS "
            f"{snowflake_settings.database}.{snowflake_settings.schema_}"
        )
        cursor.execute(
            f"USE SCHEMA {snowflake_settings.database}.{snowflake_settings.schema_}"
        )

        for migration_file in SQL_MIGRATIONS:
            migration_path = MIGRATIONS_DIR / migration_file
            if not migration_path.exists():
                logger.warning("Migration file not found — skipping", file=migration_file)
                continue

            sql = migration_path.read_text(encoding="utf-8")

            # Execute each statement separately (split on semicolons)
            statements = [s.strip() for s in sql.split(";") if s.strip()]
            for statement in statements:
                cursor.execute(statement)

            logger.info("Migration applied", file=migration_file)

        logger.info("All migrations completed successfully")

    except Exception as e:
        logger.error("Migration failed", error=str(e))
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run_migrations()
