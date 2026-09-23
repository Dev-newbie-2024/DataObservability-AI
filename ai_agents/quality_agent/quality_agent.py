"""
ai_agents/quality_agent/quality_agent.py
==========================================
Quality Agent — validates Silver-layer data and generates a 0–100 quality score.

Public surface
--------------
* ``QualityAgent``          — the agent class (call via ``execute_with_retry``).
* ``start_consumer_loop()`` — blocking Kafka consumer loop, called from
  ``ai_agents.main.run_quality_agent()`` in a background thread.

Trigger
-------
Invoked with a context dict that mirrors the ``quality.events`` Kafka message
published by the Airflow ingestion DAG at the end of every successful run::

    {
        "dataset_id":      str,
        "pipeline_run_id": str,
        "silver_table":    str,     # e.g. "sales_data_silver"
        "rows_merged":     int,     # optional — from XCom
        "rows_ingested":   int,     # optional
    }

Checks performed
----------------
1. **Null rate**          — fraction of NULL cells per data column
2. **Duplicate rate**     — fraction of duplicated rows (data cols only)
3. **Type consistency**   — compares Silver dtype vs registered Snowflake type
4. **Freshness**          — hours since most-recent ``_SILVER_LOADED_AT`` value
5. **Range / completeness** — fraction of rows with any unexpected value

Scoring (weights from config.constants)
----------------------------------------
    score = (
        (1 - null_rate)       * QUALITY_WEIGHT_NULL       * 100   # 30 pts
      + (1 - dup_rate)        * QUALITY_WEIGHT_DUPLICATE  * 100   # 20 pts
      + (1 - type_mis_rate)   * QUALITY_WEIGHT_TYPE       * 100   # 25 pts
      + freshness_score       * QUALITY_WEIGHT_FRESHNESS  * 100   # 15 pts
      + (1 - range_fail_rate) * QUALITY_WEIGHT_RANGE      * 100   # 10 pts
    )
    score = clamp(score, 0, 100)

Persistence
-----------
Results are written to three Snowflake tables:
  - ``OBSERVABILITY.QUALITY_RUNS``        (run header)
  - ``OBSERVABILITY.QUALITY_METRICS``     (column-level metric rows)
  - ``OBSERVABILITY.DATA_QUALITY_METRICS``(denormalised flat summary for dashboards)
"""

from __future__ import annotations

import json
import time
from typing import Any

from ai_agents.base_agent import BaseAgent
from config.constants import (
    AgentName,
    QualityMetricType,
    Severity,
    QUALITY_WEIGHT_DUPLICATE,
    QUALITY_WEIGHT_FRESHNESS,
    QUALITY_WEIGHT_NULL,
    QUALITY_WEIGHT_RANGE,
    QUALITY_WEIGHT_TYPE,
)
from config.settings import dq_settings, snowflake_settings

# ── Runtime imports (heavy; deferred to keep module parse-time lightweight) ────
# All heavy imports are done inside methods so this file can be safely imported
# by Airflow's scheduler parser without pulling in pandas/GX/snowflake.


# ── Freshness scoring curve ────────────────────────────────────────────────────
_FRESHNESS_MAX_HOURS: float = 24.0   # data older than this → freshness score = 0


class QualityAgent(BaseAgent):
    """
    Autonomous data quality validation agent.

    Inherits retry logic, logging, and result formatting from ``BaseAgent``.
    The ``run()`` method is called by ``execute_with_retry()``; do not call it
    directly in production code.
    """

    name = AgentName.QUALITY

    # ── BaseAgent contract ────────────────────────────────────────────────────

    def run(self, context: dict[str, Any]) -> dict[str, Any]:
        """
        Main agent logic — validate Silver data and persist quality metrics.

        Args:
            context: Dict with at minimum ``dataset_id``, ``pipeline_run_id``,
                     ``silver_table``.

        Returns:
            Standard ``BaseAgent._build_result()`` dict enriched with
            ``quality_score``, ``metrics``, and ``quality_run_id``.
        """
        dataset_id      = context.get("dataset_id", "")
        pipeline_run_id = context.get("pipeline_run_id", "")
        silver_table    = context.get("silver_table", "")

        self.logger.info(
            "Quality Agent starting",
            dataset_id=dataset_id,
            pipeline_run_id=pipeline_run_id,
            silver_table=silver_table,
        )

        if not silver_table:
            self.logger.warning("No silver_table in context — skipping quality check")
            return self._build_result(
                status="skipped",
                severity=Severity.INFO,
                actions=["skipped: no silver_table in context"],
            )

        # 1. Load Silver data ──────────────────────────────────────────────────
        df = self._load_silver_data(silver_table)
        total_rows = len(df)
        self.logger.info("Silver data loaded", silver_table=silver_table, rows=total_rows)

        if total_rows == 0:
            self.logger.warning("Silver table is empty — no rows to validate")
            return self._build_result(
                status="skipped",
                severity=Severity.LOW,
                actions=["skipped: silver table has 0 rows"],
                details={"silver_table": silver_table, "total_rows": 0},
            )

        # 2. Fetch registered column types ────────────────────────────────────
        column_types = self._fetch_column_types(dataset_id)

        # 3. Run GX suite (or pandas fallback) ────────────────────────────────
        from ai_agents.quality_agent import gx_runner

        gx_result = gx_runner.run_gx_suite(
            df=df,
            suite_name=f"quality_{dataset_id or 'unknown'}",
            column_types=column_types,
        )
        self.logger.info(
            "GX suite completed",
            gx_available=gx_result["gx_available"],
            expectations_passed=gx_result["expectations_passed"],
            expectations_total=gx_result["expectations_total"],
        )

        # 4. Aggregate dimension rates ─────────────────────────────────────────
        col_results    = gx_result["column_results"]
        null_rate      = self._avg_null_rate(col_results)
        dup_rate       = gx_result["duplicate_rate"]
        type_mis_rate  = self._type_mismatch_rate(col_results)
        freshness_hrs  = gx_result["freshness_hours"]
        freshness_score = self._freshness_score(freshness_hrs)
        range_fail_rate = self._range_fail_rate(col_results, total_rows)

        # 5. Compute overall quality score ─────────────────────────────────────
        quality_score = self._compute_quality_score(
            null_rate=null_rate,
            dup_rate=dup_rate,
            type_mis_rate=type_mis_rate,
            freshness_score=freshness_score,
            range_fail_rate=range_fail_rate,
        )
        passed = quality_score >= dq_settings.min_quality_score
        severity = self._score_to_severity(quality_score)

        self.logger.info(
            "Quality score computed",
            quality_score=quality_score,
            passed=passed,
            null_rate=null_rate,
            dup_rate=dup_rate,
            type_mismatch_rate=type_mis_rate,
            freshness_hours=freshness_hrs,
        )

        # 6. Persist results ───────────────────────────────────────────────────
        from backend.core.snowflake_client import (
            insert_quality_run,
            insert_quality_metrics,
            insert_data_quality_metrics,
        )

        schema_version = int(context.get("schema_version", 1))

        quality_run_id = insert_quality_run(
            dataset_id=dataset_id,
            pipeline_run_id=pipeline_run_id,
            schema_version=schema_version,
            total_rows=total_rows,
            quality_score=round(quality_score, 2),
        )

        column_metric_rows = self._build_column_metric_rows(
            col_results=col_results,
            null_threshold=dq_settings.null_rate_threshold,
            type_threshold=dq_settings.type_mismatch_threshold,
        )
        if column_metric_rows:
            insert_quality_metrics(quality_run_id, column_metric_rows)

        insert_data_quality_metrics(
            dataset_id=dataset_id,
            pipeline_run_id=pipeline_run_id,
            silver_table=silver_table,
            quality_score=round(quality_score, 2),
            total_rows=total_rows,
            null_rate=null_rate,
            duplicate_rate=dup_rate,
            type_mismatch_rate=type_mis_rate,
            range_fail_rate=range_fail_rate,
            freshness_hours=freshness_hrs,
            passed=passed,
            raw_metrics=gx_result,
        )

        self.logger.info(
            "Quality metrics persisted",
            quality_run_id=quality_run_id,
            dataset_id=dataset_id,
        )

        actions = [
            f"loaded {total_rows} rows from {silver_table}",
            f"quality score: {quality_score:.1f}/100 ({'PASS' if passed else 'FAIL'})",
            f"null_rate={null_rate:.4f}, dup_rate={dup_rate:.4f}, "
            f"type_mismatch={type_mis_rate:.4f}",
            f"persisted metrics to QUALITY_RUNS/QUALITY_METRICS/DATA_QUALITY_METRICS",
        ]

        return self._build_result(
            status="resolved" if passed else "escalated",
            severity=severity,
            actions=actions,
            details={
                "quality_run_id":    quality_run_id,
                "quality_score":     quality_score,
                "passed":            passed,
                "total_rows":        total_rows,
                "silver_table":      silver_table,
                "null_rate":         null_rate,
                "duplicate_rate":    dup_rate,
                "type_mismatch_rate": type_mis_rate,
                "freshness_hours":   freshness_hrs,
                "range_fail_rate":   range_fail_rate,
                "gx_available":      gx_result["gx_available"],
            },
        )

    # ── Private helpers ───────────────────────────────────────────────────────

    def _load_silver_data(self, silver_table: str) -> "pd.DataFrame":
        """
        Pull at most 50 000 rows from the Silver table into a DataFrame.
        A 50k row cap keeps memory bounded; the quality metrics are statistical
        estimates and do not require the full table for correctness.
        """
        import pandas as pd
        from backend.core.database import execute_query
        from config.settings import snowflake_settings

        db  = snowflake_settings.database
        # All tables live in the single OBSERVABILITY schema; SILVER/BRONZE/GOLD
        # are logical names only — separate schemas are not provisioned.
        slv = snowflake_settings.schema_

        try:
            rows = execute_query(
                f"SELECT * FROM {db}.{slv}.{silver_table.upper()} LIMIT 50000"
            )
        except Exception as _qe:
            self.logger.warning(
                "Could not load silver table — table may not exist yet",
                silver_table=silver_table,
                error=str(_qe),
            )
            return pd.DataFrame()

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame(rows)

    def _fetch_column_types(self, dataset_id: str) -> dict[str, str]:
        """
        Return {COLUMN_NAME: snowflake_type} from the latest registered schema.
        Falls back to empty dict — type checks will be skipped gracefully.
        """
        if not dataset_id:
            return {}
        try:
            from backend.core.snowflake_client import get_current_schema_version
            import json

            row = get_current_schema_version(dataset_id)
            if not row:
                return {}
            raw = row.get("SCHEMA_JSON") or row.get("schema_json") or []
            if isinstance(raw, str):
                raw = json.loads(raw)
            return {
                col["name"].upper(): col.get("snowflake_type", "VARCHAR")
                for col in raw
                if isinstance(col, dict) and "name" in col
            }
        except Exception as exc:
            self.logger.warning("Could not fetch column types", error=str(exc))
            return {}

    # ── Metric aggregation ────────────────────────────────────────────────────

    @staticmethod
    def _avg_null_rate(col_results: dict[str, dict]) -> float:
        if not col_results:
            return 0.0
        return round(
            sum(v["null_rate"] for v in col_results.values()) / len(col_results), 6
        )

    @staticmethod
    def _type_mismatch_rate(col_results: dict[str, dict]) -> float:
        if not col_results:
            return 0.0
        bad = sum(1 for v in col_results.values() if not v["type_ok"])
        return round(bad / len(col_results), 6)

    @staticmethod
    def _range_fail_rate(col_results: dict[str, dict], total_rows: int) -> float:
        """Use unexpected_count as a proxy for range / format failures."""
        if not col_results or total_rows == 0:
            return 0.0
        total_unexpected = sum(v.get("unexpected_count", 0) for v in col_results.values())
        return round(min(total_unexpected / (len(col_results) * total_rows), 1.0), 6)

    @staticmethod
    def _freshness_score(freshness_hours: float | None) -> float:
        """Convert freshness_hours to a 0–1 score (1 = perfectly fresh)."""
        if freshness_hours is None:
            return 1.0   # unknown → optimistic
        return round(max(0.0, 1.0 - freshness_hours / _FRESHNESS_MAX_HOURS), 6)

    @staticmethod
    def _compute_quality_score(
        null_rate:      float,
        dup_rate:       float,
        type_mis_rate:  float,
        freshness_score: float,
        range_fail_rate: float,
    ) -> float:
        """Weighted 0–100 quality score using weights from config.constants."""
        raw = (
            (1.0 - null_rate)       * QUALITY_WEIGHT_NULL       * 100
            + (1.0 - dup_rate)      * QUALITY_WEIGHT_DUPLICATE  * 100
            + (1.0 - type_mis_rate) * QUALITY_WEIGHT_TYPE       * 100
            + freshness_score       * QUALITY_WEIGHT_FRESHNESS  * 100
            + (1.0 - range_fail_rate) * QUALITY_WEIGHT_RANGE    * 100
        )
        return round(max(0.0, min(100.0, raw)), 2)

    @staticmethod
    def _score_to_severity(score: float) -> Severity:
        if score >= 90:
            return Severity.INFO
        if score >= 75:
            return Severity.LOW
        if score >= 60:
            return Severity.MEDIUM
        if score >= 40:
            return Severity.HIGH
        return Severity.CRITICAL

    @staticmethod
    def _build_column_metric_rows(
        col_results: dict[str, dict],
        null_threshold: float,
        type_threshold: float,
    ) -> list[dict[str, Any]]:
        """
        Convert per-column GX results to the shape expected by
        ``snowflake_client.insert_quality_metrics()``.
        """
        rows: list[dict[str, Any]] = []
        for col, res in col_results.items():
            rows.append({
                "column_name":  col,
                "metric_type":  QualityMetricType.NULL_RATE.value,
                "metric_value": res["null_rate"],
                "threshold":    null_threshold,
                "passed":       res["null_rate"] <= null_threshold,
            })
            rows.append({
                "column_name":  col,
                "metric_type":  QualityMetricType.TYPE_MISMATCH.value,
                "metric_value": 0.0 if res["type_ok"] else 1.0,
                "threshold":    type_threshold,
                "passed":       res["type_ok"],
            })
        return rows

    # ── BaseAgent helper override ─────────────────────────────────────────────

    def _build_result(
        self,
        status: str,
        severity: Severity,
        actions: list[str],
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Extends base result to use ``actions`` kwarg matching call sites."""
        return super()._build_result(
            status=status,
            severity=severity,
            actions=actions,
            details=details,
        )


# ── Module-level consumer loop ────────────────────────────────────────────────

def start_consumer_loop(
    poll_interval_seconds: float = 2.0,
    batch_size: int = 10,
    poll_timeout: float = 1.0,
) -> None:
    """
    Blocking Kafka consumer loop for the Quality Agent service thread.

    Called by ``ai_agents.main.run_quality_agent()``.
    Polls ``quality.events`` indefinitely and dispatches each message to
    ``QualityAgent.execute_with_retry()``.

    The loop uses a bare confluent_kafka Consumer rather than importing
    ``backend.core.kafka_consumer`` because the agents Docker image does not
    mount the ``backend/`` source tree.

    Args:
        poll_interval_seconds: Seconds to sleep between empty-batch cycles.
        batch_size:            Max messages consumed per poll cycle.
        poll_timeout:          Seconds per individual ``consumer.poll()`` call.
    """
    from config.logging_config import get_logger
    from config.constants import KafkaTopic
    from config.settings import kafka_settings

    loop_logger = get_logger(__name__)
    loop_logger.info("Quality Agent consumer loop starting",
                     topic=KafkaTopic.QUALITY_EVENTS.value)

    # ── Verify backend package is importable (requires ./backend volume mount) ──
    try:
        import backend.core.snowflake_client  # noqa: F401
        loop_logger.info("backend.core reachable — persistence enabled")
    except ImportError as _be:
        loop_logger.error(
            "backend.core NOT importable — DATA_QUALITY_METRICS will NOT be written. "
            "Ensure ./backend:/app/backend is mounted in docker-compose agents service.",
            error=str(_be),
        )

    # ── Pre-warm the Snowflake connection pool before consuming ──────────────
    try:
        from backend.core.database import initialise_pool
        initialise_pool()
        loop_logger.info("Snowflake connection pool initialised for agents process")
    except Exception as _pool_exc:
        loop_logger.error(
            "Snowflake pool init failed — agent will retry on each message",
            error=str(_pool_exc),
        )

    # ── Conditional import — degrade gracefully if confluent-kafka absent ──
    try:
        from confluent_kafka import Consumer as _Consumer, KafkaError
        _kafka_ok = True
    except ImportError:
        loop_logger.warning(
            "confluent-kafka not installed — Quality Agent consumer disabled"
        )
        return

    config = {
        **kafka_settings.consumer_config,
        "group.id": f"{kafka_settings.group_id_prefix}quality-agent",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    consumer = _Consumer(config)
    topic = KafkaTopic.QUALITY_EVENTS.value

    try:
        consumer.subscribe([topic])
        loop_logger.info("Quality Agent subscribed", topic=topic)

        agent = QualityAgent()

        while True:
            consumed = 0
            empty_polls = 0
            max_empty = 5

            while consumed < batch_size and empty_polls < max_empty:
                msg = consumer.poll(timeout=poll_timeout)

                if msg is None:
                    empty_polls += 1
                    continue

                if msg.error():
                    err = msg.error()
                    if err.code() == KafkaError._PARTITION_EOF:  # type: ignore
                        empty_polls += 1
                        continue
                    loop_logger.error("Kafka error", error=str(err))
                    empty_polls += 1
                    continue

                empty_polls = 0
                consumed += 1

                import json as _json
                try:
                    raw = msg.value()
                    envelope = _json.loads(raw.decode("utf-8")) if raw else {}
                    consumer.commit(message=msg, asynchronous=False)
                except Exception as decode_exc:
                    loop_logger.error("Message decode failed — skipping",
                                      error=str(decode_exc))
                    consumer.commit(message=msg, asynchronous=False)
                    continue

                # Build agent context from the quality.events envelope ─────
                payload = envelope.get("payload", {})
                dataset_id      = envelope.get("dataset_id") or payload.get("dataset_id", "")
                pipeline_run_id = payload.get("pipeline_run_id", "")
                schema_version  = int(payload.get("schema_version", 1))

                # Resolve silver_table from OBSERVABILITY.DATASETS ─────────
                silver_table = payload.get("silver_table", "")
                if not silver_table and dataset_id:
                    try:
                        from backend.core.snowflake_client import get_dataset_by_id
                        ds_row = get_dataset_by_id(dataset_id)
                        if ds_row:
                            silver_table = (
                                ds_row.get("SILVER_TABLE")
                                or ds_row.get("silver_table")
                                or ""
                            )
                    except Exception as lookup_exc:
                        loop_logger.warning(
                            "Could not resolve silver_table from dataset registry",
                            dataset_id=dataset_id,
                            error=str(lookup_exc),
                        )

                context = {
                    "dataset_id":      dataset_id,
                    "pipeline_run_id": pipeline_run_id,
                    "silver_table":    silver_table,
                    "schema_version":  schema_version,
                    "trigger":         payload.get("trigger", "quality.events"),
                    "rows_ingested":   payload.get("rows_ingested", 0),
                }

                loop_logger.info(
                    "Quality Agent dispatching",
                    dataset_id=dataset_id,
                    pipeline_run_id=pipeline_run_id,
                    silver_table=silver_table,
                )

                try:
                    result = agent.execute_with_retry(context)
                    loop_logger.info(
                        "Quality Agent finished",
                        status=result.get("status"),
                        quality_score=result.get("details", {}).get("quality_score"),
                        dataset_id=dataset_id,
                    )
                except Exception as agent_exc:
                    loop_logger.error(
                        "Quality Agent error (continuing loop)",
                        error=str(agent_exc),
                        dataset_id=dataset_id,
                    )

            # No messages in this cycle — sleep before next poll ───────────
            if consumed == 0:
                time.sleep(poll_interval_seconds)

    except Exception as fatal:
        loop_logger.error(
            "Quality Agent consumer loop crashed",
            error=str(fatal),
        )
    finally:
        consumer.close()
        loop_logger.info("Quality Agent consumer closed")
