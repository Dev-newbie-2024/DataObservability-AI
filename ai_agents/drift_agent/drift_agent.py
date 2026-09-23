"""
ai_agents/drift_agent/drift_agent.py
=====================================
Drift Agent — detects data drift between the current Silver dataset and a
reference baseline, then persists results to Snowflake.

Public surface
--------------
* ``DriftAgent``             — the agent class (call via ``execute_with_retry``).
* ``start_consumer_loop()``  — blocking Kafka consumer loop, called from
  ``ai_agents.main.run_drift_agent()`` in a background thread.

Trigger
-------
Invoked with a context dict mirroring the ``drift.events`` Kafka message::

    {
        "dataset_id":       str,
        "pipeline_run_id":  str,
        "silver_table":     str,    # e.g. "sales_data_silver"
        "reference_table":  str,    # optional — defaults to same table older window
        "schema_version":   int,    # optional
    }

Checks performed
----------------
1. **PSI** (Population Stability Index) — numeric columns
2. **KS statistic + p-value** — numeric columns
3. **Categorical distribution shift** — categorical columns
4. **Overall drift score** — weighted average of per-column drift

Drift Score
-----------
    drift_score = drifted_columns / total_columns   (0.0 – 1.0)
    drift_pct   = drift_score * 100

Severity mapping:
    drift_pct < 20%  → LOW
    drift_pct < 40%  → MEDIUM
    drift_pct >= 40% → HIGH

Persistence
-----------
Results are written to three Snowflake tables:
  - ``OBSERVABILITY.DRIFT_RUNS``    (run header)
  - ``OBSERVABILITY.DRIFT_METRICS`` (per-column metric rows)
  - ``OBSERVABILITY.DRIFT_SUMMARY`` (denormalised flat summary for dashboards)
"""

from __future__ import annotations

import time
from typing import Any

from ai_agents.base_agent import BaseAgent
from config.constants import AgentName, Severity
from config.settings import kafka_settings, snowflake_settings

# Max rows fetched from Snowflake for drift comparison
_MAX_ROWS: int = 50_000
# Minimum rows needed in BOTH current and reference to run drift
_MIN_ROWS: int = 10


class DriftAgent(BaseAgent):
    """
    Autonomous data drift detection agent.

    Inherits retry logic, logging, and result formatting from ``BaseAgent``.
    ``run()`` is called by ``execute_with_retry()``; do not call directly.
    """

    name = AgentName.DRIFT

    # ── BaseAgent contract ─────────────────────────────────────────────────────

    def run(self, context: dict[str, Any]) -> dict[str, Any]:
        """
        Main agent logic — detect drift and persist metrics.

        Args:
            context: Dict with at minimum ``dataset_id``, ``pipeline_run_id``,
                     ``silver_table``.

        Returns:
            Standard ``BaseAgent._build_result()`` dict enriched with
            ``drift_score``, ``drift_pct``, ``drift_detected``, ``drift_severity``.
        """
        dataset_id      = context.get("dataset_id", "")
        pipeline_run_id = context.get("pipeline_run_id", "")
        silver_table    = context.get("silver_table", "")
        reference_table = context.get("reference_table", "") or silver_table
        schema_version  = int(context.get("schema_version", 1))

        self.logger.info(
            "Drift Agent starting",
            dataset_id=dataset_id,
            pipeline_run_id=pipeline_run_id,
            silver_table=silver_table,
            reference_table=reference_table,
        )

        if not silver_table:
            self.logger.warning("No silver_table in context — skipping drift check")
            return self._build_result(
                status="skipped",
                severity=Severity.INFO,
                actions=["skipped: no silver_table in context"],
            )

        # 1. Load current and reference data ────────────────────────────────────
        current_df   = self._load_table(silver_table, window="current")
        reference_df = self._load_table(reference_table, window="reference")

        if len(current_df) < _MIN_ROWS or len(reference_df) < _MIN_ROWS:
            msg = (
                f"Insufficient data: current={len(current_df)} rows, "
                f"reference={len(reference_df)} rows (min={_MIN_ROWS})"
            )
            self.logger.warning(msg)
            return self._build_result(
                status="skipped",
                severity=Severity.LOW,
                actions=[f"skipped: {msg}"],
                details={"current_rows": len(current_df), "reference_rows": len(reference_df)},
            )

        self.logger.info(
            "Data loaded for drift",
            current_rows=len(current_df),
            reference_rows=len(reference_df),
        )

        # 2. Run Evidently (or pandas fallback) ──────────────────────────────────
        from ai_agents.drift_agent import evidently_runner

        drift_result = evidently_runner.run_drift_suite(
            current_df=current_df,
            reference_df=reference_df,
            suite_name=f"drift_{dataset_id or 'unknown'}",
        )

        self.logger.info(
            "Drift suite completed",
            evidently_available=drift_result["evidently_available"],
            drifted_columns=drift_result["drifted_columns"],
            total_columns=drift_result["total_columns"],
            drift_pct=drift_result["drift_pct"],
        )

        # 3. Compute overall drift score and severity ─────────────────────────────
        drifted_cols = drift_result["drifted_columns"]
        total_cols   = drift_result["total_columns"]
        drift_pct    = drift_result["drift_pct"]
        avg_psi      = drift_result["avg_psi"]
        avg_ks       = drift_result["avg_ks_stat"]

        # Drift score: fraction of drifted columns (0.0 – 1.0)
        drift_score = round(drifted_cols / total_cols, 4) if total_cols else 0.0
        drift_detected = drift_pct > 0
        drift_severity = self._score_to_severity(drift_pct)

        self.logger.info(
            "Drift score computed",
            drift_score=drift_score,
            drift_pct=drift_pct,
            drift_detected=drift_detected,
            drift_severity=drift_severity.value,
            avg_psi=avg_psi,
            avg_ks_stat=avg_ks,
        )

        # 4. Persist to Snowflake ─────────────────────────────────────────────────
        from backend.core.snowflake_client import (
            insert_drift_run,
            insert_drift_metrics,
            insert_drift_summary,
        )

        drift_run_id = insert_drift_run(
            dataset_id=dataset_id,
            pipeline_run_id=pipeline_run_id,
            overall_drift_score=drift_score,
            drift_detected=drift_detected,
        )

        col_metric_rows = self._build_col_metric_rows(drift_result["column_results"])
        if col_metric_rows:
            insert_drift_metrics(drift_run_id, col_metric_rows)

        insert_drift_summary(
            dataset_id=dataset_id,
            pipeline_run_id=pipeline_run_id,
            silver_table=silver_table,
            reference_table=reference_table,
            drift_score=drift_score,
            drift_detected=drift_detected,
            drift_severity=drift_severity.value,
            drifted_columns=drifted_cols,
            total_columns=total_cols,
            drift_pct=drift_pct,
            avg_psi=avg_psi,
            avg_ks_stat=avg_ks,
            raw_metrics=drift_result,
        )

        self.logger.info(
            "Drift metrics persisted",
            drift_run_id=drift_run_id,
            dataset_id=dataset_id,
        )

        status = "escalated" if drift_severity == Severity.HIGH else "resolved"
        actions = [
            f"compared {len(current_df)} current rows vs {len(reference_df)} reference rows",
            f"drift_pct={drift_pct:.1f}% ({drifted_cols}/{total_cols} columns drifted)",
            f"severity={drift_severity.value.upper()}  score={drift_score:.4f}",
            f"avg_psi={avg_psi:.4f}  avg_ks={avg_ks:.4f}",
            f"persisted to DRIFT_RUNS/DRIFT_METRICS/DRIFT_SUMMARY",
        ]

        return self._build_result(
            status=status,
            severity=drift_severity,
            actions=actions,
            details={
                "drift_run_id":    drift_run_id,
                "drift_score":     drift_score,
                "drift_pct":       drift_pct,
                "drift_detected":  drift_detected,
                "drift_severity":  drift_severity.value,
                "drifted_columns": drifted_cols,
                "total_columns":   total_cols,
                "avg_psi":         avg_psi,
                "avg_ks_stat":     avg_ks,
                "current_rows":    len(current_df),
                "reference_rows":  len(reference_df),
            },
        )

    # ── Private helpers ────────────────────────────────────────────────────────

    def _load_table(self, table: str, window: str = "current") -> "pd.DataFrame":
        """
        Pull at most _MAX_ROWS rows from a Silver table.

        For the reference window we take the OLDEST half of rows (by
        ``_silver_loaded_at``); for the current window we take the NEWEST half.
        When ``reference_table != silver_table`` we load both tables in full.
        """
        import pandas as pd
        from backend.core.database import execute_query
        from config.settings import snowflake_settings

        db  = snowflake_settings.database
        obs = snowflake_settings.schema_

        limit   = _MAX_ROWS // 2  # half each for current / reference
        order   = "DESC" if window == "current" else "ASC"
        has_ts  = True  # assume _silver_loaded_at exists; catch error if not

        try:
            rows = execute_query(
                f"SELECT * FROM {db}.{obs}.{table.upper()} "
                f"ORDER BY _silver_loaded_at {order} LIMIT {limit}"
            )
        except Exception:
            has_ts = False

        if not has_ts:
            try:
                rows = execute_query(
                    f"SELECT * FROM {db}.{obs}.{table.upper()} LIMIT {limit}"
                )
            except Exception as exc:
                self.logger.warning(
                    "Could not load table for drift",
                    table=table,
                    window=window,
                    error=str(exc),
                )
                return pd.DataFrame()

        return pd.DataFrame(rows) if rows else pd.DataFrame()

    @staticmethod
    def _score_to_severity(drift_pct: float) -> Severity:
        if drift_pct >= 40:
            return Severity.HIGH
        if drift_pct >= 20:
            return Severity.MEDIUM
        return Severity.LOW

    @staticmethod
    def _build_col_metric_rows(column_results: dict[str, dict]) -> list[dict[str, Any]]:
        rows = []
        for col_name, res in column_results.items():
            rows.append({
                "column_name":    col_name,
                "drift_method":   "ks_test+psi",
                "statistic_value": res.get("ks_statistic", 0.0),
                "p_value":         res.get("ks_p_value", 1.0),
                "psi_score":       res.get("psi", 0.0),
                "drift_detected":  bool(res.get("drift_detected", False)),
                "severity":        "HIGH" if res.get("drift_detected") else "LOW",
            })
        return rows


# ── Consumer loop ──────────────────────────────────────────────────────────────

def start_consumer_loop(
    batch_size:   int   = 1,
    poll_timeout: float = 2.0,
) -> None:
    """
    Blocking Kafka consumer loop — called from ``ai_agents.main.run_drift_agent()``
    in a background daemon thread.
    """
    import json

    from config.logging_config import get_logger
    from config.constants import KafkaTopic
    from config.settings import kafka_settings

    loop_logger = get_logger("ai_agents.drift_agent.drift_agent")
    loop_logger.info(
        "Drift Agent consumer loop starting",
        topic=KafkaTopic.DRIFT_EVENTS.value,
    )

    # ── Check backend.core reachability ───────────────────────────────────────
    try:
        import backend.core  # noqa: F401
        loop_logger.info("backend.core reachable — persistence enabled")
    except ImportError as _be:
        loop_logger.error(
            "backend.core NOT importable — drift results will NOT be persisted",
            error=str(_be),
        )

    # ── Pre-warm the Snowflake connection pool ────────────────────────────────
    try:
        from backend.core.database import initialise_pool
        initialise_pool()
        loop_logger.info("Snowflake pool initialised for drift agent")
    except Exception as _pool_exc:
        loop_logger.error(
            "Snowflake pool init failed",
            error=str(_pool_exc),
        )

    # ── Kafka consumer setup ──────────────────────────────────────────────────
    try:
        from confluent_kafka import Consumer as _Consumer, KafkaError
    except ImportError:
        loop_logger.warning("confluent-kafka not installed — Drift Agent consumer disabled")
        return

    config = {
        **kafka_settings.consumer_config,
        "group.id": f"{kafka_settings.group_id_prefix}drift-agent",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    consumer = _Consumer(config)
    topic = KafkaTopic.DRIFT_EVENTS.value

    try:
        consumer.subscribe([topic])
        loop_logger.info("Drift Agent subscribed", topic=topic)

        agent = DriftAgent()

        while True:
            consumed  = 0
            empty_polls = 0
            max_empty   = 5

            while consumed < batch_size and empty_polls < max_empty:
                msg = consumer.poll(timeout=poll_timeout)

                if msg is None:
                    empty_polls += 1
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        loop_logger.error("Kafka error", error=str(msg.error()))
                    continue

                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                except Exception as parse_err:
                    loop_logger.warning("Could not parse drift event", error=str(parse_err))
                    consumer.commit(message=msg)
                    continue

                # Envelope structure: {dataset_id: ..., payload: {...}}
                inner = payload.get("payload", {})
                dataset_id      = payload.get("dataset_id", "") or inner.get("dataset_id", "")
                pipeline_run_id = payload.get("pipeline_run_id", "") or inner.get("pipeline_run_id", "")
                silver_table    = payload.get("silver_table", "") or inner.get("silver_table", "")
                reference_table = payload.get("reference_table", "") or inner.get("reference_table", silver_table)

                # Fallback: resolve silver_table from dataset registry
                if not silver_table and dataset_id:
                    try:
                        from backend.core.snowflake_client import get_dataset_by_id
                        ds_row = get_dataset_by_id(dataset_id)
                        if ds_row:
                            silver_table = (
                                ds_row.get("SILVER_TABLE") or ds_row.get("silver_table") or ""
                            )
                            reference_table = reference_table or silver_table
                    except Exception as lookup_exc:
                        loop_logger.warning(
                            "Could not resolve silver_table from registry",
                            dataset_id=dataset_id,
                            error=str(lookup_exc),
                        )

                loop_logger.info(
                    "Drift Agent dispatching",
                    dataset_id=dataset_id,
                    pipeline_run_id=pipeline_run_id,
                    silver_table=silver_table,
                )

                context = {
                    "dataset_id":      dataset_id,
                    "pipeline_run_id": pipeline_run_id,
                    "silver_table":    silver_table,
                    "reference_table": reference_table,
                    "schema_version":  int(inner.get("schema_version", payload.get("schema_version", 1))),
                }

                result = agent.execute_with_retry(context)

                loop_logger.info(
                    "Drift Agent finished",
                    dataset_id=dataset_id,
                    drift_pct=result.get("details", {}).get("drift_pct"),
                    status=result.get("status"),
                )

                consumer.commit(message=msg)
                consumed += 1
                empty_polls = 0

    except KeyboardInterrupt:
        loop_logger.info("Drift Agent consumer shutting down")
    finally:
        consumer.close()
