"""
ai_agents/self_healing_agent/self_healing_agent.py
====================================================
Self-Healing Agent — classifies pipeline failures and executes automatic
recovery strategies (retry, quarantine, status update, event emission).

Public surface
--------------
* ``SelfHealingAgent``       — the agent class.
* ``start_consumer_loop()``  — blocking Kafka consumer loop called from
  ``ai_agents.main.run_self_healing_agent()`` in a background thread.

Trigger
-------
Invoked with a context dict mirroring the ``heal.commands`` Kafka message::

    {
        "dataset_id":       str,
        "pipeline_run_id":  str,
        "failure_reason":   str,   # free-text error message
        "failed_task":      str,   # e.g. "bronze_to_silver"
        "retry_count":      int,   # how many times already retried
        "trigger":          str,   # "quality_failure" | "dag_failure" | ...
    }

Failure classification
----------------------
The agent classifies the failure using keyword matching on ``failure_reason``:

    CONNECTION → snowflake / kafka / timeout / connection / socket
    QUALITY    → null / duplicate / schema / type mismatch / quality
    DRIFT      → drift / distribution / psi / ks_test
    SYSTEM     → everything else

Healing strategies
------------------
    CONNECTION → RETRY  (up to max_retries)
    QUALITY    → QUARANTINE (move dataset to quarantine state)
    DRIFT      → RETRY then escalate
    SYSTEM     → RETRY once, then escalate

Persistence
-----------
  - ``OBSERVABILITY.HEAL_RUNS``    — per-run header
  - ``OBSERVABILITY.HEAL_ACTIONS`` — per-action rows
  - ``OBSERVABILITY.HEAL_SUMMARY`` — denormalised flat row for dashboards
"""

from __future__ import annotations

import time
from typing import Any

from ai_agents.base_agent import BaseAgent
from config.constants import AgentName, HealingStrategy, PipelineStatus, Severity

# ── Failure classification keywords ───────────────────────────────────────────
_CONNECTION_KEYWORDS = frozenset({
    "snowflake", "kafka", "timeout", "connection", "socket",
    "network", "refused", "broker", "ssl", "tls",
})
_QUALITY_KEYWORDS = frozenset({
    "null", "duplicate", "schema", "type mismatch", "quality",
    "completeness", "freshness", "range", "validation",
})
_DRIFT_KEYWORDS = frozenset({
    "drift", "distribution", "psi", "ks_test", "shift", "evidently",
})

# ── Strategy table ─────────────────────────────────────────────────────────────
_STRATEGY: dict[str, HealingStrategy] = {
    "CONNECTION": HealingStrategy.RETRY,
    "QUALITY":    HealingStrategy.QUARANTINE,
    "DRIFT":      HealingStrategy.RETRY,
    "SYSTEM":     HealingStrategy.RETRY,
}

# ── Max auto-retries before escalation ────────────────────────────────────────
_MAX_AUTO_RETRIES = 2


class SelfHealingAgent(BaseAgent):
    """
    Autonomous self-healing agent — classifies failures and executes recovery.

    Inherits retry logic, logging, and result formatting from ``BaseAgent``.
    """

    name = AgentName.SELF_HEALING

    # ── BaseAgent contract ─────────────────────────────────────────────────────

    def run(self, context: dict[str, Any]) -> dict[str, Any]:
        dataset_id      = context.get("dataset_id", "")
        pipeline_run_id = context.get("pipeline_run_id", "")
        failure_reason  = context.get("failure_reason", "unknown failure")
        failed_task     = context.get("failed_task", "")
        retry_count     = int(context.get("retry_count", 0))
        trigger         = context.get("trigger", "dag_failure")

        self.logger.info(
            "Self-Healing Agent starting",
            dataset_id=dataset_id,
            pipeline_run_id=pipeline_run_id,
            failed_task=failed_task,
            retry_count=retry_count,
            trigger=trigger,
        )

        t_start = time.monotonic()

        # 1. Classify failure ──────────────────────────────────────────────────
        failure_type = self._classify_failure(failure_reason)
        strategy     = _STRATEGY[failure_type]

        self.logger.info(
            "Failure classified",
            failure_type=failure_type,
            strategy=strategy.value,
            retry_count=retry_count,
        )

        # 2. Execute recovery strategy ─────────────────────────────────────────
        actions:       list[dict[str, Any]] = []
        quarantined    = False
        recovery_status = "RESOLVED"

        if strategy == HealingStrategy.QUARANTINE:
            # Quarantine: mark dataset as quarantined, do not retry
            ok = self._quarantine_dataset(dataset_id, pipeline_run_id, failure_reason)
            actions.append({
                "action_type":   "QUARANTINE_DATASET",
                "action_detail": f"Quarantined dataset={dataset_id} reason={failure_reason[:200]}",
                "success":       ok,
            })
            quarantined = ok
            recovery_status = "RESOLVED" if ok else "FAILED"

        elif strategy == HealingStrategy.RETRY:
            if retry_count < _MAX_AUTO_RETRIES:
                # Attempt retry: update pipeline status → HEALING
                ok = self._update_pipeline_status(
                    pipeline_run_id, PipelineStatus.HEALING
                )
                actions.append({
                    "action_type":   "UPDATE_PIPELINE_STATUS",
                    "action_detail": f"Set pipeline_run_id={pipeline_run_id} to HEALING",
                    "success":       ok,
                })
                # Emit recovery event back to quality.events / the appropriate topic
                emit_ok = self._emit_recovery_event(dataset_id, pipeline_run_id, failure_type)
                actions.append({
                    "action_type":   "EMIT_RECOVERY_EVENT",
                    "action_detail": f"Emitted retry event for failure_type={failure_type}",
                    "success":       emit_ok,
                })
                recovery_status = "RESOLVED"
            else:
                # Max retries exceeded → escalate by quarantining
                self.logger.warning(
                    "Max retries exceeded — quarantining",
                    retry_count=retry_count,
                    dataset_id=dataset_id,
                )
                ok = self._quarantine_dataset(dataset_id, pipeline_run_id, failure_reason)
                actions.append({
                    "action_type":   "QUARANTINE_AFTER_MAX_RETRIES",
                    "action_detail": f"Quarantined after {retry_count} retries",
                    "success":       ok,
                })
                quarantined = ok
                recovery_status = "ESCALATED"

        # Always update final pipeline status
        final_status = (
            PipelineStatus.SUCCESS if recovery_status == "RESOLVED"
            else PipelineStatus.QUARANTINED if quarantined
            else PipelineStatus.FAILED
        )
        status_ok = self._update_pipeline_status(pipeline_run_id, final_status)
        actions.append({
            "action_type":   "UPDATE_PIPELINE_FINAL_STATUS",
            "action_detail": f"Set pipeline status to {final_status.value}",
            "success":       status_ok,
        })

        # 3. Compute MTTR ─────────────────────────────────────────────────────
        mttr_seconds = round(time.monotonic() - t_start, 3)
        actions_succeeded = sum(1 for a in actions if a["success"])

        self.logger.info(
            "Healing actions complete",
            failure_type=failure_type,
            strategy=strategy.value,
            recovery_status=recovery_status,
            mttr_seconds=mttr_seconds,
            actions=len(actions),
            succeeded=actions_succeeded,
            quarantined=quarantined,
        )

        # 4. Persist ─────────────────────────────────────────────────────────
        from backend.core.snowflake_client import (
            insert_heal_run,
            insert_heal_actions,
            insert_heal_summary,
        )

        heal_run_id = insert_heal_run(
            dataset_id=dataset_id,
            pipeline_run_id=pipeline_run_id,
            failure_type=failure_type,
            failure_reason=failure_reason,
            failed_task=failed_task,
            healing_strategy=strategy.value,
            recovery_status=recovery_status,
            retry_count=retry_count,
            mttr_seconds=mttr_seconds,
        )

        insert_heal_actions(heal_run_id, actions)

        insert_heal_summary(
            dataset_id=dataset_id,
            pipeline_run_id=pipeline_run_id,
            failure_type=failure_type,
            failure_reason=failure_reason,
            healing_strategy=strategy.value,
            recovery_status=recovery_status,
            retry_count=retry_count,
            mttr_seconds=mttr_seconds,
            actions_taken=len(actions),
            actions_succeeded=actions_succeeded,
            quarantined=quarantined,
        )

        self.logger.info(
            "Heal metrics persisted",
            heal_run_id=heal_run_id,
            dataset_id=dataset_id,
        )

        agent_severity = Severity.HIGH if recovery_status == "ESCALATED" else Severity.LOW
        return self._build_result(
            status="escalated" if recovery_status == "ESCALATED" else "resolved",
            severity=agent_severity,
            actions=[a["action_detail"] for a in actions],
            details={
                "heal_run_id":      heal_run_id,
                "failure_type":     failure_type,
                "healing_strategy": strategy.value,
                "recovery_status":  recovery_status,
                "retry_count":      retry_count,
                "mttr_seconds":     mttr_seconds,
                "actions_taken":    len(actions),
                "actions_succeeded": actions_succeeded,
                "quarantined":      quarantined,
            },
        )

    # ── Private helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _classify_failure(failure_reason: str) -> str:
        """Classify failure into CONNECTION / QUALITY / DRIFT / SYSTEM."""
        lower = (failure_reason or "").lower()
        if any(k in lower for k in _CONNECTION_KEYWORDS):
            return "CONNECTION"
        if any(k in lower for k in _QUALITY_KEYWORDS):
            return "QUALITY"
        if any(k in lower for k in _DRIFT_KEYWORDS):
            return "DRIFT"
        return "SYSTEM"

    def _quarantine_dataset(
        self, dataset_id: str, pipeline_run_id: str, reason: str
    ) -> bool:
        """Mark dataset as quarantined in DATASETS and PIPELINE_RUNS tables."""
        try:
            from backend.core.database import execute_statement
            from config.settings import snowflake_settings
            db  = snowflake_settings.database
            obs = snowflake_settings.schema_

            if dataset_id:
                execute_statement(
                    f"UPDATE {db}.{obs}.DATASETS "
                    f"SET status = 'quarantined', updated_at = CURRENT_TIMESTAMP() "
                    f"WHERE id = %s",
                    (dataset_id,),
                )
            if pipeline_run_id:
                execute_statement(
                    f"UPDATE {db}.{obs}.PIPELINE_RUNS "
                    f"SET status = 'quarantined', completed_at = CURRENT_TIMESTAMP() "
                    f"WHERE id = %s",
                    (pipeline_run_id,),
                )
            self.logger.info(
                "Dataset quarantined",
                dataset_id=dataset_id,
                pipeline_run_id=pipeline_run_id,
            )
            return True
        except Exception as exc:
            self.logger.warning("Quarantine update failed", error=str(exc))
            return False

    def _update_pipeline_status(
        self, pipeline_run_id: str, status: PipelineStatus
    ) -> bool:
        """Update PIPELINE_RUNS.status for the given run id."""
        if not pipeline_run_id:
            return True  # Nothing to update — treat as no-op success
        try:
            from backend.core.database import execute_statement
            from config.settings import snowflake_settings
            db  = snowflake_settings.database
            obs = snowflake_settings.schema_

            execute_statement(
                f"UPDATE {db}.{obs}.PIPELINE_RUNS "
                f"SET status = %s, updated_at = CURRENT_TIMESTAMP() "
                f"WHERE id = %s",
                (status.value, pipeline_run_id),
            )
            self.logger.info(
                "Pipeline status updated",
                pipeline_run_id=pipeline_run_id,
                status=status.value,
            )
            return True
        except Exception as exc:
            self.logger.warning("Pipeline status update failed", error=str(exc))
            return False

    def _emit_recovery_event(
        self, dataset_id: str, pipeline_run_id: str, failure_type: str
    ) -> bool:
        """
        Emit a recovery event to notify downstream agents.
        CONNECTION/SYSTEM → quality.events  (re-trigger quality check)
        QUALITY           → (no re-trigger — quarantined)
        DRIFT             → drift.events    (re-trigger drift check)
        """
        from config.constants import KafkaTopic

        topic_map = {
            "CONNECTION": KafkaTopic.QUALITY_EVENTS,
            "SYSTEM":     KafkaTopic.QUALITY_EVENTS,
            "DRIFT":      KafkaTopic.DRIFT_EVENTS,
        }
        topic = topic_map.get(failure_type)
        if topic is None:
            return True  # QUALITY failures are quarantined — no retry event

        try:
            from backend.core.kafka_producer import publish_event
            publish_event(
                topic,
                payload={
                    "trigger":          "self_healing_retry",
                    "pipeline_run_id":  pipeline_run_id,
                    "failure_type":     failure_type,
                },
                dataset_id=dataset_id,
            )
            self.logger.info(
                "Recovery event emitted",
                topic=topic.value,
                dataset_id=dataset_id,
            )
            return True
        except Exception as exc:
            self.logger.warning("Failed to emit recovery event", error=str(exc))
            return False


# ── Consumer loop ──────────────────────────────────────────────────────────────

def start_consumer_loop(
    batch_size:   int   = 1,
    poll_timeout: float = 2.0,
) -> None:
    """
    Blocking Kafka consumer loop — called from
    ``ai_agents.main.run_self_healing_agent()`` in a background daemon thread.
    """
    import json

    from config.logging_config import get_logger
    from config.constants import KafkaTopic
    from config.settings import kafka_settings

    loop_logger = get_logger("ai_agents.self_healing_agent.self_healing_agent")
    loop_logger.info(
        "Self-Healing Agent consumer loop starting",
        topic=KafkaTopic.HEAL_COMMANDS.value,
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
        loop_logger.info("Snowflake pool initialised for self-healing agent")
    except Exception as _pool_exc:
        loop_logger.error("Snowflake pool init failed", error=str(_pool_exc))

    # ── Kafka consumer setup ──────────────────────────────────────────────────
    try:
        from confluent_kafka import Consumer as _Consumer, KafkaError
    except ImportError:
        loop_logger.warning("confluent-kafka not installed — Self-Healing Agent disabled")
        return

    config = {
        **kafka_settings.consumer_config,
        "group.id": f"{kafka_settings.group_id_prefix}self-healing-agent",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    consumer = _Consumer(config)
    topic = KafkaTopic.HEAL_COMMANDS.value

    try:
        consumer.subscribe([topic])
        loop_logger.info("Self-Healing Agent subscribed", topic=topic)

        agent = SelfHealingAgent()

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
                    loop_logger.warning("Could not parse heal command", error=str(parse_err))
                    consumer.commit(message=msg)
                    continue

                # Envelope: {dataset_id: ..., payload: {...}}
                inner           = payload.get("payload", {})
                dataset_id      = payload.get("dataset_id", "") or inner.get("dataset_id", "")
                pipeline_run_id = inner.get("pipeline_run_id", "")
                failure_reason  = inner.get("failure_reason", "unknown failure")
                failed_task     = inner.get("failed_task", "")
                retry_count     = int(inner.get("retry_count", 0))
                trigger         = inner.get("trigger", "dag_failure")

                loop_logger.info(
                    "Self-Healing Agent dispatching",
                    dataset_id=dataset_id,
                    pipeline_run_id=pipeline_run_id,
                    failure_reason=failure_reason[:80],
                    retry_count=retry_count,
                )

                context = {
                    "dataset_id":      dataset_id,
                    "pipeline_run_id": pipeline_run_id,
                    "failure_reason":  failure_reason,
                    "failed_task":     failed_task,
                    "retry_count":     retry_count,
                    "trigger":         trigger,
                }

                result = agent.execute_with_retry(context)

                loop_logger.info(
                    "Self-Healing Agent finished",
                    dataset_id=dataset_id,
                    recovery_status=result.get("details", {}).get("recovery_status"),
                    mttr_seconds=result.get("details", {}).get("mttr_seconds"),
                    status=result.get("status"),
                )

                consumer.commit(message=msg)
                consumed    += 1
                empty_polls  = 0

    except KeyboardInterrupt:
        loop_logger.info("Self-Healing Agent consumer shutting down")
    finally:
        consumer.close()
