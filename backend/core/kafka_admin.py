"""
backend/core/kafka_admin.py
============================
Kafka topic administration utilities.

Responsibilities:
  - Create topics if they don't exist (idempotent).
  - Verify topic existence.
  - Used at application startup and by the Airflow DAG bootstrap task.

Degrades gracefully when confluent-kafka is not installed.
"""

from __future__ import annotations

import time
from typing import Any

from config.constants import KafkaTopic
from config.logging_config import get_logger
from config.settings import kafka_settings

logger = get_logger(__name__)

try:
    from confluent_kafka.admin import AdminClient, NewTopic
    _ADMIN_AVAILABLE = True
except ImportError:
    AdminClient = None          # type: ignore[assignment,misc]
    NewTopic = None             # type: ignore[assignment,misc]
    _ADMIN_AVAILABLE = False
    print("[kafka_admin.py] WARNING: confluent-kafka not installed — topic admin disabled")


# ─── Topic specs (name → partitions, replication_factor) ─────────────────────

TOPIC_SPECS: dict[str, tuple[int, int]] = {
    KafkaTopic.RAW_DATA_EVENTS.value:  (3, 1),  # high-throughput ingestion
    KafkaTopic.SCHEMA_EVENTS.value:    (1, 1),  # low-volume, ordered
    KafkaTopic.QUALITY_EVENTS.value:   (1, 1),
    KafkaTopic.DRIFT_EVENTS.value:     (1, 1),
    KafkaTopic.COST_EVENTS.value:      (1, 1),
    KafkaTopic.HEAL_COMMANDS.value:    (1, 1),  # commands must be ordered
}


def _get_admin_client() -> Any:
    """Return a configured AdminClient or raise ImportError."""
    if not _ADMIN_AVAILABLE:
        raise ImportError("confluent-kafka is not installed")
    return AdminClient({"bootstrap.servers": kafka_settings.bootstrap_servers})


def ensure_topics(
    max_retries: int = 5,
    retry_delay: float = 2.0,
) -> dict[str, str]:
    """
    Create all platform Kafka topics if they don't already exist.

    Idempotent — safe to call at every startup.  Uses exponential backoff
    when the broker is temporarily unavailable.

    Args:
        max_retries:  Number of connection attempts before giving up.
        retry_delay:  Base delay in seconds (doubles each retry).

    Returns:
        Dict mapping topic name → "created" | "exists" | "error:<msg>".
    """
    if not _ADMIN_AVAILABLE:
        logger.warning("Kafka admin unavailable — skipping topic creation")
        return {t: "skipped" for t in TOPIC_SPECS}

    result: dict[str, str] = {}
    attempt = 0

    while attempt < max_retries:
        try:
            client = _get_admin_client()
            existing = _list_existing_topics(client)

            topics_to_create = [
                NewTopic(
                    topic=name,
                    num_partitions=partitions,
                    replication_factor=replication,
                )
                for name, (partitions, replication) in TOPIC_SPECS.items()
                if name not in existing
            ]

            if not topics_to_create:
                logger.info("All Kafka topics already exist")
                return {t: "exists" for t in TOPIC_SPECS}

            fs = client.create_topics(topics_to_create, request_timeout=10.0)
            for topic, future in fs.items():
                try:
                    future.result()
                    result[topic] = "created"
                    logger.info("Kafka topic created", topic=topic)
                except Exception as exc:
                    err_str = str(exc)
                    if "TOPIC_ALREADY_EXISTS" in err_str or "TopicExistsException" in err_str:
                        result[topic] = "exists"
                    else:
                        result[topic] = f"error:{err_str}"
                        logger.error("Failed to create Kafka topic", topic=topic, error=err_str)

            # Fill in any topics that were already present
            for name in TOPIC_SPECS:
                if name not in result:
                    result[name] = "exists" if name in existing else "skipped"

            return result

        except Exception as exc:
            attempt += 1
            delay = retry_delay * (2 ** (attempt - 1))
            logger.warning(
                "Kafka admin connection failed — retrying",
                attempt=attempt,
                max_retries=max_retries,
                retry_in=delay,
                error=str(exc),
            )
            if attempt >= max_retries:
                logger.error("Kafka admin exhausted retries — topics not created", error=str(exc))
                return {t: f"error:{exc}" for t in TOPIC_SPECS}
            time.sleep(delay)

    return result  # pragma: no cover


def _list_existing_topics(client: Any) -> set[str]:
    """Return the set of existing topic names from the cluster metadata."""
    metadata = client.list_topics(timeout=10)
    return set(metadata.topics.keys())


def topic_exists(topic: KafkaTopic | str) -> bool:
    """Return True if the given topic exists on the broker."""
    if not _ADMIN_AVAILABLE:
        return False
    try:
        topic_str = topic.value if isinstance(topic, KafkaTopic) else topic
        client = _get_admin_client()
        existing = _list_existing_topics(client)
        return topic_str in existing
    except Exception as exc:
        logger.warning("topic_exists check failed", error=str(exc))
        return False
