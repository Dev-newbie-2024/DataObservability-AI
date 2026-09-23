"""
airflow/dags/ingestion_dag.py
================================
Primary ingestion DAG: Kafka raw.data.events → Bronze → Silver → Gold.

DAG ID      : ingestion_dag
Schedule    : Every 15 minutes (continuous micro-batch)
Catchup     : False
Max active  : 1 (prevents overlapping runs on same topic partition)

Task Flow
---------

  bootstrap_topics                  # ensure Kafka topics exist
        │
  ingest_from_kafka                 # consume batch from raw.data.events
        │
  ┌─────┴──────────────────┐
  │                        │
  ensure_silver_gold_tables   (idempotent DDL)
        │
  bronze_to_silver          # MERGE Bronze → Silver (dedup + quality flag)
        │
  silver_to_gold            # Aggregate Silver → Gold summary rows
        │
  publish_pipeline_complete  # emit quality.events for Quality Agent
        │
  update_pipeline_status    # mark PIPELINE_RUNS as success/failed


Retry Strategy (per task)
--------------------------
  retries          : 3
  retry_delay      : 2 min
  retry_exponential_backoff : True   (2, 4, 8 minutes)

On final failure → PipelineRuns row is marked "failed" by on_failure_callback.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from typing import Any

# Allow importing from project root
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import BranchPythonOperator

# config.constants is import-safe: no env reads, no pydantic validation.
# config.logging_config is intentionally NOT imported at module level because
# it calls configure_logging() on import, which instantiates pydantic-settings
# models (including SnowflakeSettings with required fields). If those env vars
# are absent when the Airflow scheduler parses this file, a ValidationError
# would break the DAG. All logging is therefore done lazily inside callables.
from config.constants import DagId, KafkaTopic


# ─── Callback functions ───────────────────────────────────────────────────────

def _on_task_failure(context: dict[str, Any]) -> None:
    """Callback executed when any task fails after all retries are exhausted."""
    from config.logging_config import get_logger
    _logger = get_logger(__name__)

    task_id   = context.get("task_instance", {}).task_id if context.get("task_instance") else "unknown"
    run_id    = context.get("run_id", "unknown")
    exception = context.get("exception")

    _logger.error(
        "Airflow task failed",
        task_id=task_id,
        run_id=run_id,
        error=str(exception) if exception else "unknown",
    )

    # Try to mark the pipeline run as failed in Snowflake
    try:
        from backend.core import snowflake_client
        pipeline_run_id = context["ti"].xcom_pull("ingest_from_kafka", key="pipeline_run_id")
        if pipeline_run_id:
            snowflake_client.complete_pipeline_run(
                pipeline_run_id,
                status="failed",
                error_message=str(exception) if exception else "Airflow task failure",
            )
    except Exception as exc:
        _logger.warning("Could not update pipeline run on failure", error=str(exc))


# ─── Default arguments ────────────────────────────────────────────────────────

_DEFAULT_ARGS = {
    "owner":                    "observability-platform",
    "depends_on_past":          False,
    "email_on_failure":         False,
    "email_on_retry":           False,
    "retries":                  3,
    "retry_delay":              timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay":          timedelta(minutes=30),
    "on_failure_callback":      _on_task_failure,
}


# ─── Task callables ───────────────────────────────────────────────────────────

def _bootstrap_topics(**context: Any) -> None:
    """Ensure all required Kafka topics exist. Skips gracefully if Kafka is down."""
    from config.logging_config import get_logger
    from backend.core.kafka_admin import ensure_topics
    logger = get_logger(__name__)
    results = ensure_topics(max_retries=3, retry_delay=5.0)
    logger.info("Kafka topic bootstrap complete", results=results)


def _check_has_messages(**context: Any) -> str:
    """
    Branch task: check if there are messages waiting on the topic.
    Returns task_id to execute next.
    """
    from config.logging_config import get_logger
    from backend.core.kafka_consumer import get_topic_lag
    logger = get_logger(__name__)
    lag = get_topic_lag(KafkaTopic.RAW_DATA_EVENTS, group_id="airflow-ingestion")
    if lag == 0:
        logger.info("No messages in topic — skipping ingestion run")
        return "no_messages"
    logger.info("Messages available", lag=lag)
    return "ingest_from_kafka"


def _ingest_from_kafka(**context: Any) -> dict[str, Any]:
    """Pull a batch from raw.data.events and load to Bronze."""
    from airflow.operators.python import get_current_context
    from backend.core.kafka_consumer import consume_batch
    from backend.core import snowflake_client
    import json, uuid

    ti = context["ti"]

    results: list[dict] = []
    for envelope in consume_batch(
        KafkaTopic.RAW_DATA_EVENTS,
        group_id="airflow-ingestion",
        batch_size=500,
        poll_timeout=2.0,
    ):
        payload = envelope.get("payload", {})
        dataset_id      = envelope.get("dataset_id") or payload.get("dataset_id")
        pipeline_run_id = payload.get("pipeline_run_id") or str(uuid.uuid4())
        dataset_name    = payload.get("dataset_name", "unknown")
        schema_version  = int(payload.get("schema_version", 1))
        rows_ingested   = int(payload.get("rows_ingested", 0))

        if not dataset_id:
            continue

        safe_name    = dataset_name.lower().replace("-", "_")
        bronze_table = f"{safe_name}_raw"
        silver_table = f"{safe_name}_silver"
        gold_table   = f"{safe_name}_gold"

        sv = snowflake_client.get_current_schema_version(dataset_id)
        columns: list[dict] = []
        if sv:
            raw_cols = sv.get("SCHEMA_JSON") or sv.get("schema_json") or []
            if isinstance(raw_cols, str):
                raw_cols = json.loads(raw_cols)
            columns = raw_cols or []

        snowflake_client.insert_pipeline_run(
            dataset_id=dataset_id,
            dag_id="ingestion_dag",
            run_id=pipeline_run_id,
            triggered_by="airflow",
        )

        results.append({
            "dataset_id":      dataset_id,
            "pipeline_run_id": pipeline_run_id,
            "bronze_table":    bronze_table,
            "silver_table":    silver_table,
            "gold_table":      gold_table,
            "schema_version":  schema_version,
            "columns":         columns,
            "rows_ingested":   rows_ingested,
        })

    if not results:
        return {"rows_ingested": 0}

    last = results[-1]
    for key in ("dataset_id", "pipeline_run_id", "bronze_table",
                "silver_table", "gold_table", "schema_version", "columns"):
        ti.xcom_push(key=key, value=last.get(key))

    total_rows = sum(r["rows_ingested"] for r in results)
    ti.xcom_push(key="rows_ingested", value=total_rows)
    return {"events": len(results), "rows": total_rows}


def _ensure_silver_gold_tables(**context: Any) -> dict[str, str]:
    """Create Silver and Gold tables if they don't exist yet."""
    from config.logging_config import get_logger
    from backend.core import snowflake_client, snowflake_pipeline
    logger = get_logger(__name__)
    ti = context["ti"]

    bronze_table = ti.xcom_pull("ingest_from_kafka", key="bronze_table")
    columns      = ti.xcom_pull("ingest_from_kafka", key="columns") or []
    dataset_id   = ti.xcom_pull("ingest_from_kafka", key="dataset_id")

    if not bronze_table:
        logger.warning("No bronze_table in XCom — skipping table creation")
        return {}

    silver_table = snowflake_pipeline.create_silver_table(bronze_table, columns)
    gold_table   = snowflake_pipeline.create_gold_table(silver_table, columns)

    ti.xcom_push(key="silver_table", value=silver_table)
    ti.xcom_push(key="gold_table",   value=gold_table)

    if dataset_id:
        snowflake_client.update_dataset_tables(
            dataset_id, silver_table=silver_table, gold_table=gold_table
        )

    return {"silver_table": silver_table, "gold_table": gold_table}


def _bronze_to_silver(**context: Any) -> dict[str, int]:
    """MERGE Bronze → Silver."""
    from config.logging_config import get_logger
    from backend.core import snowflake_pipeline
    logger = get_logger(__name__)
    ti = context["ti"]

    dataset_id      = ti.xcom_pull("ingest_from_kafka",          key="dataset_id")
    pipeline_run_id = ti.xcom_pull("ingest_from_kafka",          key="pipeline_run_id")
    bronze_table    = ti.xcom_pull("ingest_from_kafka",          key="bronze_table")
    silver_table    = ti.xcom_pull("ensure_silver_gold_tables",  key="silver_table")
    schema_version  = ti.xcom_pull("ingest_from_kafka",          key="schema_version") or 1

    if not all([pipeline_run_id, bronze_table, silver_table]):
        logger.warning("Missing XCom values — skipping Bronze→Silver")
        return {}

    stats = snowflake_pipeline.merge_bronze_to_silver(
        bronze_table=bronze_table,
        silver_table=silver_table,
        pipeline_run_id=pipeline_run_id,
        schema_version=schema_version,
    )

    if dataset_id:
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

    ti.xcom_push(key="silver_stats", value=stats)
    return stats


def _silver_to_gold(**context: Any) -> int:
    """Aggregate Silver → Gold."""
    from config.logging_config import get_logger
    from backend.core import snowflake_pipeline
    logger = get_logger(__name__)
    ti = context["ti"]

    dataset_id      = ti.xcom_pull("ingest_from_kafka",         key="dataset_id")
    pipeline_run_id = ti.xcom_pull("ingest_from_kafka",         key="pipeline_run_id")
    silver_table    = ti.xcom_pull("ensure_silver_gold_tables", key="silver_table")
    gold_table      = ti.xcom_pull("ensure_silver_gold_tables", key="gold_table")

    if not all([pipeline_run_id, silver_table, gold_table]):
        logger.warning("Missing XCom values — skipping Silver→Gold")
        return 0

    gold_rows = snowflake_pipeline.aggregate_silver_to_gold(
        silver_table=silver_table,
        gold_table=gold_table,
        pipeline_run_id=pipeline_run_id,
    )

    if dataset_id:
        silver_stats = ti.xcom_pull("bronze_to_silver", key="silver_stats") or {}
        snowflake_pipeline.record_lineage(
            dataset_id=dataset_id,
            pipeline_run_id=pipeline_run_id,
            source_layer="SILVER",
            target_layer="GOLD",
            source_table=silver_table,
            target_table=gold_table,
            rows_in=silver_stats.get("rows_merged", 0),
            rows_out=gold_rows,
            transformation_type="aggregate",
        )

    ti.xcom_push(key="gold_rows", value=gold_rows)
    return gold_rows


def _publish_pipeline_complete(**context: Any) -> str:
    """Emit a quality.events message so the Quality Agent picks up this dataset."""
    from backend.core.kafka_producer import publish_event
    ti = context["ti"]

    dataset_id      = ti.xcom_pull("ingest_from_kafka", key="dataset_id")
    pipeline_run_id = ti.xcom_pull("ingest_from_kafka", key="pipeline_run_id")
    rows_ingested   = ti.xcom_pull("ingest_from_kafka", key="rows_ingested") or 0
    silver_stats    = ti.xcom_pull("bronze_to_silver",  key="silver_stats") or {}
    gold_rows       = ti.xcom_pull("silver_to_gold",    key="gold_rows") or 0

    if not dataset_id:
        return "skipped"

    event_id = publish_event(
        KafkaTopic.QUALITY_EVENTS,
        payload={
            "trigger":          "pipeline_complete",
            "pipeline_run_id":  pipeline_run_id,
            "rows_ingested":    rows_ingested,
            "rows_merged":      silver_stats.get("rows_merged", 0),
            "rows_quarantined": silver_stats.get("rows_quarantined", 0),
            "gold_rows":        gold_rows,
            "dag_run_id":       context.get("run_id"),
        },
        dataset_id=dataset_id,
    )
    from config.logging_config import get_logger
    logger = get_logger(__name__)
    logger.info("Pipeline complete event published", event_id=event_id, dataset_id=dataset_id)
    return event_id


def _update_pipeline_status(**context: Any) -> None:
    """Mark the PIPELINE_RUNS row as success."""
    from backend.core import snowflake_client
    ti = context["ti"]

    pipeline_run_id = ti.xcom_pull("ingest_from_kafka", key="pipeline_run_id")
    rows_ingested   = ti.xcom_pull("ingest_from_kafka", key="rows_ingested") or 0

    if pipeline_run_id:
        snowflake_client.complete_pipeline_run(
            pipeline_run_id,
            status="success",
            rows_ingested=rows_ingested,
        )
        from config.logging_config import get_logger
        logger = get_logger(__name__)
        logger.info("Pipeline run marked success", pipeline_run_id=pipeline_run_id)


# ─── DAG Definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id=DagId.INGESTION.value,
    description="Kafka → Bronze → Silver → Gold ingestion pipeline",
    schedule_interval=timedelta(minutes=15),
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=_DEFAULT_ARGS,
    tags=["ingestion", "kafka", "snowflake", "bronze", "silver", "gold"],
) as dag:

    # 1. Ensure Kafka topics exist
    bootstrap = PythonOperator(
        task_id="bootstrap_topics",
        python_callable=_bootstrap_topics,
    )

    # 2. Branch: skip if no messages
    check_messages = BranchPythonOperator(
        task_id="check_messages",
        python_callable=_check_has_messages,
    )

    no_messages = EmptyOperator(task_id="no_messages")

    # 3. Consume Kafka batch → Bronze
    ingest = PythonOperator(
        task_id="ingest_from_kafka",
        python_callable=_ingest_from_kafka,
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
    )

    # 4. Ensure Silver + Gold tables exist
    ensure_tables = PythonOperator(
        task_id="ensure_silver_gold_tables",
        python_callable=_ensure_silver_gold_tables,
    )

    # 5. MERGE Bronze → Silver
    brz_to_slv = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=_bronze_to_silver,
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
    )

    # 6. Aggregate Silver → Gold
    slv_to_gld = PythonOperator(
        task_id="silver_to_gold",
        python_callable=_silver_to_gold,
        retries=2,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
    )

    # 7. Publish quality.events
    publish_complete = PythonOperator(
        task_id="publish_pipeline_complete",
        python_callable=_publish_pipeline_complete,
    )

    # 8. Mark pipeline run success
    update_status = PythonOperator(
        task_id="update_pipeline_status",
        python_callable=_update_pipeline_status,
        trigger_rule="all_done",  # runs even if upstream partially failed
    )

    # ── Task dependencies ─────────────────────────────────────────────────────
    (
        bootstrap
        >> check_messages
        >> [ingest, no_messages]
    )
    (
        ingest
        >> ensure_tables
        >> brz_to_slv
        >> slv_to_gld
        >> publish_complete
        >> update_status
    )
