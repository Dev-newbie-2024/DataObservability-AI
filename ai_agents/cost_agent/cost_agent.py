"""
ai_agents/cost_agent/cost_agent.py
====================================
Cost Agent — monitors Snowflake credit usage per pipeline run and generates
a 0-100 cost score with a 30-day spend forecast.

Public surface
--------------
* ``CostAgent``              — the agent class.
* ``start_consumer_loop()``  — blocking Kafka consumer loop, called from
  ``ai_agents.main.run_cost_agent()`` in a background thread.

Trigger
-------
Invoked with a context dict mirroring the ``cost.events`` Kafka message::

    {
        "dataset_id":       str,
        "pipeline_run_id":  str,   # optional
        "warehouse_name":   str,   # optional  — default: current warehouse
        "lookback_days":    int,   # optional  — default: 7
    }

Metrics computed
----------------
1. **Credits consumed**       — sum over lookback window (QUERY_HISTORY)
2. **Credits USD**            — credits * USD_PER_CREDIT (default $3.00)
3. **Cost per pipeline run**  — credits / pipeline_run_count
4. **30-day forecast**        — linear extrapolation from daily averages
5. **Cost Score (0–100)**     — higher = cheaper relative to budget ceiling

Cost Score formula:
    budget_ceiling = COST_BUDGET_CEILING_CREDITS (default 100 credits/day)
    daily_avg      = credits_7d / 7
    score          = clamp(100 * (1 - daily_avg / budget_ceiling), 0, 100)

Persistence
-----------
  - ``OBSERVABILITY.COST_RECORDS``  — per-run cost row
  - ``OBSERVABILITY.COST_FORECASTS``— 30-day daily forecast rows
  - ``OBSERVABILITY.COST_SUMMARY``  — denormalised summary for dashboards
"""

from __future__ import annotations

import time
from typing import Any

from ai_agents.base_agent import BaseAgent
from config.constants import AgentName, Severity

# ── Budget defaults (overridden by env/settings if available) ─────────────────
_USD_PER_CREDIT:          float = 3.00
_BUDGET_CEILING_CREDITS:  float = 100.0   # credits/day considered "expensive"
_LOOKBACK_DAYS:           int   = 7


class CostAgent(BaseAgent):
    """
    Autonomous cost-monitoring agent for Snowflake credit consumption.

    Inherits retry logic, logging, and result formatting from ``BaseAgent``.
    """

    name = AgentName.COST

    # ── BaseAgent contract ─────────────────────────────────────────────────────

    def run(self, context: dict[str, Any]) -> dict[str, Any]:
        dataset_id      = context.get("dataset_id", "")
        pipeline_run_id = context.get("pipeline_run_id", "")
        warehouse_name  = context.get("warehouse_name", "")
        lookback_days   = int(context.get("lookback_days", _LOOKBACK_DAYS))

        self.logger.info(
            "Cost Agent starting",
            dataset_id=dataset_id,
            pipeline_run_id=pipeline_run_id,
            warehouse_name=warehouse_name or "all",
            lookback_days=lookback_days,
        )

        # 1. Query Snowflake for credit usage ────────────────────────────────
        usage = self._fetch_credit_usage(lookback_days, warehouse_name)

        total_credits   = usage["total_credits"]
        daily_breakdown = usage["daily_breakdown"]   # list of {date, credits}
        run_count       = usage["pipeline_run_count"]
        bytes_scanned   = usage["bytes_scanned"]

        self.logger.info(
            "Credit usage fetched",
            total_credits=total_credits,
            lookback_days=lookback_days,
            pipeline_runs=run_count,
        )

        # 2. Derived metrics ──────────────────────────────────────────────────
        total_usd       = round(total_credits * _USD_PER_CREDIT, 4)
        daily_avg       = round(total_credits / max(lookback_days, 1), 6)
        cost_per_run    = round(total_credits / max(run_count, 1), 6)
        storage_gb      = round(bytes_scanned / (1024 ** 3), 4)
        storage_cost_usd = round(storage_gb * 0.023, 4)   # $0.023/GB (Snowflake On Demand)

        # 3. 30-day linear forecast ───────────────────────────────────────────
        forecast_credits_30d = round(daily_avg * 30, 4)
        forecast_usd_30d     = round(forecast_credits_30d * _USD_PER_CREDIT, 4)

        # 4. Cost score (0–100, higher = cheaper) ─────────────────────────────
        cost_score   = self._compute_cost_score(daily_avg)
        cost_severity = self._score_to_severity(cost_score)

        self.logger.info(
            "Cost metrics computed",
            total_credits=total_credits,
            total_usd=total_usd,
            daily_avg=daily_avg,
            cost_per_run=cost_per_run,
            forecast_30d_usd=forecast_usd_30d,
            cost_score=cost_score,
            cost_severity=cost_severity.value,
        )

        # 5. Persist ─────────────────────────────────────────────────────────
        from backend.core.snowflake_client import (
            insert_cost_record,
            insert_cost_forecast,
            insert_cost_summary,
        )

        cost_record_id = insert_cost_record(
            dataset_id=dataset_id,
            pipeline_run_id=pipeline_run_id,
            warehouse_name=warehouse_name or "ALL",
            credits_used=total_credits,
            credits_usd=total_usd,
            bytes_scanned=int(bytes_scanned),
        )

        insert_cost_forecast(
            dataset_id=dataset_id,
            forecast_credits_30d=forecast_credits_30d,
            lower_bound=round(forecast_credits_30d * 0.85, 4),
            upper_bound=round(forecast_credits_30d * 1.15, 4),
            daily_breakdown=daily_breakdown,
        )

        insert_cost_summary(
            dataset_id=dataset_id,
            pipeline_run_id=pipeline_run_id,
            warehouse_name=warehouse_name or "ALL",
            lookback_days=lookback_days,
            total_credits=total_credits,
            total_usd=total_usd,
            daily_avg_credits=daily_avg,
            cost_per_run=cost_per_run,
            storage_gb=storage_gb,
            storage_cost_usd=storage_cost_usd,
            forecast_credits_30d=forecast_credits_30d,
            forecast_usd_30d=forecast_usd_30d,
            cost_score=cost_score,
            cost_severity=cost_severity.value,
            pipeline_run_count=run_count,
        )

        self.logger.info(
            "Cost metrics persisted",
            cost_record_id=cost_record_id,
            dataset_id=dataset_id,
        )

        status = "escalated" if cost_severity == Severity.HIGH else "resolved"
        actions = [
            f"fetched {lookback_days}-day credit usage from Snowflake",
            f"total_credits={total_credits:.4f}  total_usd=${total_usd:.2f}",
            f"daily_avg={daily_avg:.4f} credits/day  cost_per_run={cost_per_run:.4f}",
            f"storage={storage_gb:.2f} GB  storage_cost=${storage_cost_usd:.4f}",
            f"30d_forecast={forecast_credits_30d:.2f} credits  (${forecast_usd_30d:.2f})",
            f"cost_score={cost_score:.1f}/100  severity={cost_severity.value.upper()}",
        ]

        return self._build_result(
            status=status,
            severity=cost_severity,
            actions=actions,
            details={
                "cost_record_id":       cost_record_id,
                "total_credits":        total_credits,
                "total_usd":            total_usd,
                "daily_avg_credits":    daily_avg,
                "cost_per_run":         cost_per_run,
                "storage_gb":           storage_gb,
                "storage_cost_usd":     storage_cost_usd,
                "forecast_credits_30d": forecast_credits_30d,
                "forecast_usd_30d":     forecast_usd_30d,
                "cost_score":           cost_score,
                "cost_severity":        cost_severity.value,
                "pipeline_run_count":   run_count,
                "lookback_days":        lookback_days,
            },
        )

    # ── Private helpers ────────────────────────────────────────────────────────

    def _fetch_credit_usage(
        self, lookback_days: int, warehouse_name: str
    ) -> dict[str, Any]:
        """
        Query Snowflake QUERY_HISTORY for credit usage over the lookback window.

        Falls back to ACCOUNT_USAGE.QUERY_HISTORY if INFORMATION_SCHEMA is not
        accessible, and to zero-cost defaults if neither works.
        """
        from backend.core.database import execute_query
        from config.settings import snowflake_settings

        db  = snowflake_settings.database
        obs = snowflake_settings.schema_

        wh_filter = f"AND warehouse_name = '{warehouse_name}'" if warehouse_name else ""

        # Primary source: INFORMATION_SCHEMA.QUERY_HISTORY (session-level)
        try:
            sql = f"""
                SELECT
                    TO_DATE(start_time)                  AS query_date,
                    SUM(credits_used_cloud_services)     AS credits_day,
                    SUM(bytes_scanned)                   AS bytes_day
                FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                    DATE_RANGE_START => DATEADD('day', -{lookback_days}, CURRENT_TIMESTAMP()),
                    DATE_RANGE_END   => CURRENT_TIMESTAMP(),
                    RESULT_LIMIT     => 10000
                ))
                WHERE execution_status = 'SUCCESS'
                {wh_filter}
                GROUP BY 1
                ORDER BY 1
            """
            rows = execute_query(sql)
            return self._aggregate_usage(rows, lookback_days, db, obs)
        except Exception as primary_exc:
            self.logger.warning(
                "INFORMATION_SCHEMA.QUERY_HISTORY unavailable — trying ACCOUNT_USAGE",
                error=str(primary_exc),
            )

        # Fallback: SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY (latency ~45 min)
        try:
            sql = f"""
                SELECT
                    TO_DATE(start_time)                  AS query_date,
                    SUM(credits_used_cloud_services)     AS credits_day,
                    SUM(bytes_scanned)                   AS bytes_day
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE start_time >= DATEADD('day', -{lookback_days}, CURRENT_TIMESTAMP())
                  AND execution_status = 'SUCCESS'
                  {wh_filter}
                GROUP BY 1
                ORDER BY 1
            """
            rows = execute_query(sql)
            return self._aggregate_usage(rows, lookback_days, db, obs)
        except Exception as fallback_exc:
            self.logger.warning(
                "ACCOUNT_USAGE fallback also failed — using platform COST_RECORDS",
                error=str(fallback_exc),
            )

        # Last resort: read from OUR OWN COST_RECORDS table
        try:
            sql = f"""
                SELECT
                    TO_DATE(recorded_at)  AS query_date,
                    SUM(credits_used)     AS credits_day,
                    0                     AS bytes_day
                FROM {db}.{obs}.COST_RECORDS
                WHERE recorded_at >= DATEADD('day', -{lookback_days}, CURRENT_TIMESTAMP())
                GROUP BY 1
                ORDER BY 1
            """
            rows = execute_query(sql)
            return self._aggregate_usage(rows, lookback_days, db, obs)
        except Exception:
            pass

        # Absolute fallback — synthetic zero-cost result so agent doesn't crash
        self.logger.warning("All usage sources unavailable — returning zero-cost baseline")
        return {
            "total_credits":   0.0,
            "bytes_scanned":   0.0,
            "pipeline_run_count": 1,
            "daily_breakdown": [],
        }

    def _aggregate_usage(
        self, rows: list[dict], lookback_days: int, db: str, obs: str
    ) -> dict[str, Any]:
        """Aggregate raw query rows into summary metrics."""
        from backend.core.database import execute_query

        total_credits = 0.0
        total_bytes   = 0.0
        daily_breakdown: list[dict] = []

        for row in rows:
            # Snowflake DictCursor returns uppercase keys
            credits = float(row.get("CREDITS_DAY") or row.get("credits_day") or 0)
            byt     = float(row.get("BYTES_DAY") or row.get("bytes_day") or 0)
            date    = str(row.get("QUERY_DATE") or row.get("query_date") or "")
            total_credits += credits
            total_bytes   += byt
            daily_breakdown.append({"date": date, "credits": round(credits, 6)})

        # Get pipeline run count from our PIPELINE_RUNS table
        run_count = 1
        try:
            cnt_rows = execute_query(
                f"SELECT COUNT(*) AS cnt FROM {db}.{obs}.PIPELINE_RUNS "
                f"WHERE created_at >= DATEADD('day', -{lookback_days}, CURRENT_TIMESTAMP())"
            )
            run_count = max(int((cnt_rows[0].get("CNT") or cnt_rows[0].get("cnt") or 1)), 1)
        except Exception:
            pass

        return {
            "total_credits":      round(total_credits, 6),
            "bytes_scanned":      total_bytes,
            "pipeline_run_count": run_count,
            "daily_breakdown":    daily_breakdown,
        }

    @staticmethod
    def _compute_cost_score(daily_avg: float) -> float:
        """
        Map daily average credit consumption to a 0-100 score.
        score=100 → free (0 credits/day)
        score=0   → at or above budget ceiling
        """
        score = 100.0 * max(0.0, 1.0 - daily_avg / _BUDGET_CEILING_CREDITS)
        return round(min(max(score, 0.0), 100.0), 2)

    @staticmethod
    def _score_to_severity(cost_score: float) -> Severity:
        if cost_score < 40:
            return Severity.HIGH     # very expensive
        if cost_score < 70:
            return Severity.MEDIUM
        return Severity.LOW


# ── Consumer loop ──────────────────────────────────────────────────────────────

def start_consumer_loop(
    batch_size:   int   = 1,
    poll_timeout: float = 2.0,
) -> None:
    """
    Blocking Kafka consumer loop — called from ``ai_agents.main.run_cost_agent()``
    in a background daemon thread.
    """
    import json

    from config.logging_config import get_logger
    from config.constants import KafkaTopic
    from config.settings import kafka_settings

    loop_logger = get_logger("ai_agents.cost_agent.cost_agent")
    loop_logger.info(
        "Cost Agent consumer loop starting",
        topic=KafkaTopic.COST_EVENTS.value,
    )

    # ── backend.core reachability ─────────────────────────────────────────────
    try:
        import backend.core  # noqa: F401
        loop_logger.info("backend.core reachable — persistence enabled")
    except ImportError as _be:
        loop_logger.error("backend.core NOT importable", error=str(_be))

    # ── Pre-warm Snowflake pool ───────────────────────────────────────────────
    try:
        from backend.core.database import initialise_pool
        initialise_pool()
        loop_logger.info("Snowflake pool initialised for cost agent")
    except Exception as _pool_exc:
        loop_logger.error("Snowflake pool init failed", error=str(_pool_exc))

    # ── Kafka consumer ────────────────────────────────────────────────────────
    try:
        from confluent_kafka import Consumer as _Consumer, KafkaError
    except ImportError:
        loop_logger.warning("confluent-kafka not installed — Cost Agent consumer disabled")
        return

    config = {
        **kafka_settings.consumer_config,
        "group.id": f"{kafka_settings.group_id_prefix}cost-agent",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    consumer = _Consumer(config)
    topic = KafkaTopic.COST_EVENTS.value

    try:
        consumer.subscribe([topic])
        loop_logger.info("Cost Agent subscribed", topic=topic)

        agent = CostAgent()

        while True:
            consumed    = 0
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
                    empty_polls += 1
                    continue

                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                except Exception as parse_err:
                    loop_logger.warning("Could not parse cost event", error=str(parse_err))
                    consumer.commit(message=msg)
                    continue

                # Envelope: {dataset_id: ..., payload: {...}}
                inner           = payload.get("payload", {})
                dataset_id      = payload.get("dataset_id", "") or inner.get("dataset_id", "")
                pipeline_run_id = inner.get("pipeline_run_id", "")
                warehouse_name  = inner.get("warehouse_name", "")
                lookback_days   = int(inner.get("lookback_days", _LOOKBACK_DAYS))

                loop_logger.info(
                    "Cost Agent dispatching",
                    dataset_id=dataset_id,
                    pipeline_run_id=pipeline_run_id,
                    warehouse_name=warehouse_name or "all",
                )

                context = {
                    "dataset_id":      dataset_id,
                    "pipeline_run_id": pipeline_run_id,
                    "warehouse_name":  warehouse_name,
                    "lookback_days":   lookback_days,
                }

                result = agent.execute_with_retry(context)

                loop_logger.info(
                    "Cost Agent finished",
                    dataset_id=dataset_id,
                    cost_score=result.get("details", {}).get("cost_score"),
                    status=result.get("status"),
                )

                consumer.commit(message=msg)
                consumed    += 1
                empty_polls  = 0

    except KeyboardInterrupt:
        loop_logger.info("Cost Agent consumer shutting down")
    finally:
        consumer.close()
