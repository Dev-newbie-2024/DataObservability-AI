"""
airflow/operators/snowflake_operator.py
=========================================
Custom Airflow operator for Snowflake pipeline steps.

Provides:
  - SnowflakeBronzeToSilverOperator  — MERGE Bronze → Silver
  - SnowflakeSilverToGoldOperator    — Aggregate Silver → Gold
  - SnowflakeLineageOperator         — Record a lineage event

All operators accept XCom values for dynamic dataset context
(dataset_id, pipeline_run_id, bronze_table, silver_table, gold_table).
They also update OBSERVABILITY.PIPELINE_RUNS with their outcome.

Retry behaviour: each operator re-raises on failure so Airflow's
built-in retry (with exponential backoff, configured in the DAG)
handles the retry loop.
"""

from __future__ import annotations

import sys
import os

# Allow importing from project root when running under Airflow
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from typing import Any

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from backend.core import snowflake_client, snowflake_pipeline
from config.logging_config import get_logger

logger = get_logger(__name__)


class SnowflakeBronzeToSilverOperator(BaseOperator):
    """
    Airflow operator that merges Bronze rows into Silver for a given pipeline run.

    XCom inputs (pulled from the ``ingest_task`` task):
      - ``dataset_id``       : str
      - ``pipeline_run_id``  : str
      - ``bronze_table``     : str (e.g. "sales_data_raw")
      - ``silver_table``     : str (e.g. "sales_data_silver")
      - ``schema_version``   : int

    XCom output (pushed under key ``silver_stats``):
      - ``rows_merged``      : int
      - ``rows_quarantined`` : int
    """

    template_fields = ("dataset_id", "pipeline_run_id", "bronze_table", "silver_table")

    @apply_defaults
    def __init__(
        self,
        *,
        dataset_id: str = "",
        pipeline_run_id: str = "",
        bronze_table: str = "",
        silver_table: str = "",
        schema_version: int = 1,
        ingest_task_id: str = "ingest_from_kafka",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
        self.pipeline_run_id = pipeline_run_id
        self.bronze_table = bronze_table
        self.silver_table = silver_table
        self.schema_version = schema_version
        self.ingest_task_id = ingest_task_id

    def execute(self, context: dict[str, Any]) -> dict[str, int]:
        # Pull dynamic values from XCom if not set statically
        ti = context["ti"]
        dataset_id      = self.dataset_id      or ti.xcom_pull(self.ingest_task_id, key="dataset_id")
        pipeline_run_id = self.pipeline_run_id or ti.xcom_pull(self.ingest_task_id, key="pipeline_run_id")
        bronze_table    = self.bronze_table    or ti.xcom_pull(self.ingest_task_id, key="bronze_table")
        silver_table    = self.silver_table    or ti.xcom_pull(self.ingest_task_id, key="silver_table")
        schema_version  = self.schema_version  or (ti.xcom_pull(self.ingest_task_id, key="schema_version") or 1)

        if not all([dataset_id, pipeline_run_id, bronze_table, silver_table]):
            raise ValueError(
                "SnowflakeBronzeToSilverOperator requires dataset_id, "
                "pipeline_run_id, bronze_table, and silver_table."
            )

        logger.info(
            "Bronze → Silver starting",
            dataset_id=dataset_id,
            pipeline_run_id=pipeline_run_id,
            bronze=bronze_table,
            silver=silver_table,
        )

        stats = snowflake_pipeline.merge_bronze_to_silver(
            bronze_table=bronze_table,
            silver_table=silver_table,
            pipeline_run_id=pipeline_run_id,
            schema_version=schema_version,
        )

        # Record lineage
        snowflake_pipeline.record_lineage(
            dataset_id=dataset_id,
            pipeline_run_id=pipeline_run_id,
            source_layer="BRONZE",
            target_layer="SILVER",
            source_table=bronze_table,
            target_table=silver_table,
            rows_in=stats["rows_merged"] + stats["rows_quarantined"],
            rows_out=stats["rows_merged"],
            rows_rejected=stats["rows_quarantined"],
            transformation_type="merge_dedup",
        )

        # Push stats to XCom for downstream tasks
        ti.xcom_push(key="silver_stats", value=stats)
        return stats


class SnowflakeSilverToGoldOperator(BaseOperator):
    """
    Airflow operator that aggregates Silver rows into the Gold summary table.

    XCom inputs (pulled from ``bronze_to_silver`` task):
      - ``silver_table``   : str
      - ``gold_table``     : str
      - ``dataset_id``     : str
      - ``pipeline_run_id``: str

    XCom output (pushed under key ``gold_rows``): int
    """

    template_fields = ("dataset_id", "pipeline_run_id", "silver_table", "gold_table")

    @apply_defaults
    def __init__(
        self,
        *,
        dataset_id: str = "",
        pipeline_run_id: str = "",
        silver_table: str = "",
        gold_table: str = "",
        aggregation_key: str | None = None,
        ingest_task_id: str = "ingest_from_kafka",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.dataset_id = dataset_id
        self.pipeline_run_id = pipeline_run_id
        self.silver_table = silver_table
        self.gold_table = gold_table
        self.aggregation_key = aggregation_key
        self.ingest_task_id = ingest_task_id

    def execute(self, context: dict[str, Any]) -> int:
        ti = context["ti"]
        dataset_id      = self.dataset_id      or ti.xcom_pull(self.ingest_task_id, key="dataset_id")
        pipeline_run_id = self.pipeline_run_id or ti.xcom_pull(self.ingest_task_id, key="pipeline_run_id")
        silver_table    = self.silver_table    or ti.xcom_pull(self.ingest_task_id, key="silver_table")
        gold_table      = self.gold_table      or ti.xcom_pull(self.ingest_task_id, key="gold_table")

        gold_rows = snowflake_pipeline.aggregate_silver_to_gold(
            silver_table=silver_table,
            gold_table=gold_table,
            pipeline_run_id=pipeline_run_id,
            aggregation_key=self.aggregation_key,
        )

        # Record lineage
        snowflake_pipeline.record_lineage(
            dataset_id=dataset_id,
            pipeline_run_id=pipeline_run_id,
            source_layer="SILVER",
            target_layer="GOLD",
            source_table=silver_table,
            target_table=gold_table,
            rows_in=ti.xcom_pull("bronze_to_silver", key="silver_stats", default={}).get("rows_merged", 0),
            rows_out=gold_rows,
            transformation_type="aggregate",
        )

        ti.xcom_push(key="gold_rows", value=gold_rows)
        logger.info("Silver → Gold complete", gold_table=gold_table, rows=gold_rows)
        return gold_rows


class SnowflakeEnsureTablesOperator(BaseOperator):
    """
    Airflow operator that ensures Silver and Gold tables exist for a dataset.

    Must run BEFORE the merge operators. Idempotent.

    XCom inputs (from ingest task):
      - bronze_table, columns (list[dict]), schema_version
    """

    @apply_defaults
    def __init__(
        self,
        *,
        ingest_task_id: str = "ingest_from_kafka",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.ingest_task_id = ingest_task_id

    def execute(self, context: dict[str, Any]) -> dict[str, str]:
        ti = context["ti"]
        bronze_table = ti.xcom_pull(self.ingest_task_id, key="bronze_table")
        columns      = ti.xcom_pull(self.ingest_task_id, key="columns") or []

        silver_table = snowflake_pipeline.create_silver_table(bronze_table, columns)
        gold_table   = snowflake_pipeline.create_gold_table(silver_table, columns)

        # Push table names for downstream operators
        ti.xcom_push(key="silver_table", value=silver_table)
        ti.xcom_push(key="gold_table",   value=gold_table)

        # Update dataset record with Silver/Gold table names
        dataset_id = ti.xcom_pull(self.ingest_task_id, key="dataset_id")
        if dataset_id:
            snowflake_client.update_dataset_tables(
                dataset_id,
                silver_table=silver_table,
                gold_table=gold_table,
            )

        return {"silver_table": silver_table, "gold_table": gold_table}
