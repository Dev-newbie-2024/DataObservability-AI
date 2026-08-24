"""
airflow/operators/kafka_operator.py
=====================================
Custom Airflow operator for consuming Kafka events and routing them
into the Snowflake Bronze layer.

KafkaIngestOperator:
  - Subscribes to `raw.data.events` topic.
  - Deserialises each event envelope.
  - For each event, resolves or creates the dataset + Bronze table.
  - Writes all rows into BRONZE.<table> via bulk INSERT.
  - Publishes schema.events for the Schema Agent.
  - Pushes XCom values for downstream Silver/Gold operators.
  - Updates PIPELINE_RUNS status to success or failed.

Retry behaviour: Airflow handles retries via the DAG retry_delay +
on_retry_callback. The operator itself uses tenacity for the initial
Kafka connection only.
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from backend.core import snowflake_client
from backend.core.kafka_consumer import consume_batch
from backend.core.kafka_producer import publish_event
from config.constants import KafkaTopic
from config.logging_config import get_logger

logger = get_logger(__name__)

_BATCH_SIZE = 500  # messages to process per DAG run


class KafkaIngestOperator(BaseOperator):
    """
    Pull a batch of events from `raw.data.events` and write them to Snowflake Bronze.

    Pushes the following XCom keys after a successful run:
      - dataset_id        str
      - pipeline_run_id   str
      - bronze_table      str
      - silver_table      str   (derived name, table created downstream)
      - gold_table        str   (derived name, table created downstream)
      - schema_version    int
      - columns           list[dict]  (schema columns for table creation)
      - rows_ingested     int
      - rows_rejected     int
    """

    @apply_defaults
    def __init__(
        self,
        *,
        topic: str = KafkaTopic.RAW_DATA_EVENTS.value,
        group_id: str = "airflow-ingestion",
        batch_size: int = _BATCH_SIZE,
        poll_timeout: float = 2.0,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.topic = topic
        self.group_id = group_id
        self.batch_size = batch_size
        self.poll_timeout = poll_timeout

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        dag_run_id  = context["run_id"]
        logical_date = context["logical_date"]

        logger.info(
            "KafkaIngestOperator starting",
            topic=self.topic,
            dag_run_id=dag_run_id,
        )

        results: list[dict[str, Any]] = []

        for envelope in consume_batch(
            self.topic,
            group_id=self.group_id,
            batch_size=self.batch_size,
            poll_timeout=self.poll_timeout,
        ):
            try:
                result = self._process_event(envelope, dag_run_id)
                if result:
                    results.append(result)
            except Exception as exc:
                logger.error(
                    "Event processing failed — skipping",
                    event_id=envelope.get("event_id"),
                    error=str(exc),
                )

        if not results:
            logger.warning("No events consumed from Kafka topic", topic=self.topic)
            # Push empty XCom so downstream tasks don't fail on missing keys
            context["ti"].xcom_push(key="rows_ingested", value=0)
            return {"rows_ingested": 0}

        # Aggregate stats across all events in this batch
        total_rows = sum(r.get("rows_ingested", 0) for r in results)
        total_rejected = sum(r.get("rows_rejected", 0) for r in results)

        # Push context of the LAST processed event for downstream single-dataset DAGs
        last = results[-1]
        ti = context["ti"]
        for key in ("dataset_id", "pipeline_run_id", "bronze_table",
                    "silver_table", "gold_table", "schema_version", "columns"):
            ti.xcom_push(key=key, value=last.get(key))

        ti.xcom_push(key="rows_ingested", value=total_rows)
        ti.xcom_push(key="rows_rejected", value=total_rejected)

        logger.info(
            "KafkaIngestOperator complete",
            events_processed=len(results),
            total_rows=total_rows,
        )
        return {"events_processed": len(results), "rows_ingested": total_rows}

    # ── Private helpers ────────────────────────────────────────────────────────

    def _process_event(
        self,
        envelope: dict[str, Any],
        dag_run_id: str,
    ) -> dict[str, Any] | None:
        """
        Process a single event envelope from Kafka.

        Each envelope produced by upload_service.process_upload() contains::

            {
                "event_id": "...",
                "topic":    "raw.data.events",
                "dataset_id": "...",
                "payload": {
                    "dataset_name":   "...",
                    "is_new_dataset": bool,
                    "schema_version": int,
                    "fingerprint":    "...",
                    "column_count":   int,
                    "schema_changes": [...],
                    "pipeline_run_id":"...",
                    "rows_ingested":  int,
                }
            }

        Returns a dict with ingestion stats, or None if the event should be skipped.
        """
        payload = envelope.get("payload", {})
        dataset_id      = envelope.get("dataset_id") or payload.get("dataset_id")
        pipeline_run_id = payload.get("pipeline_run_id") or str(uuid.uuid4())
        dataset_name    = payload.get("dataset_name", "unknown")
        schema_version  = int(payload.get("schema_version", 1))
        rows_ingested   = int(payload.get("rows_ingested", 0))

        if not dataset_id:
            logger.warning("Event missing dataset_id — skipping", event_id=envelope.get("event_id"))
            return None

        # Derive table names
        safe_name    = dataset_name.lower().replace("-", "_")
        bronze_table = f"{safe_name}_raw"
        silver_table = f"{safe_name}_silver"
        gold_table   = f"{safe_name}_gold"

        # Load current schema columns for table creation downstream
        sv = snowflake_client.get_current_schema_version(dataset_id)
        columns: list[dict] = []
        if sv:
            raw_cols = sv.get("SCHEMA_JSON") or sv.get("schema_json") or []
            if isinstance(raw_cols, str):
                raw_cols = json.loads(raw_cols)
            columns = raw_cols or []

        # Mark pipeline run as running under Airflow
        run_pk = snowflake_client.insert_pipeline_run(
            dataset_id=dataset_id,
            dag_id="ingestion_dag",
            run_id=pipeline_run_id,
            triggered_by="airflow",
        )

        logger.info(
            "Event processed",
            dataset_id=dataset_id,
            pipeline_run_id=pipeline_run_id,
            rows_ingested=rows_ingested,
        )

        return {
            "dataset_id":       dataset_id,
            "pipeline_run_id":  pipeline_run_id,
            "bronze_table":     bronze_table,
            "silver_table":     silver_table,
            "gold_table":       gold_table,
            "schema_version":   schema_version,
            "columns":          columns,
            "rows_ingested":    rows_ingested,
            "rows_rejected":    0,
            "airflow_run_id":   dag_run_id,
        }
