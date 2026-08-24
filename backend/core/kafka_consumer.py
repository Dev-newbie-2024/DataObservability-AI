"""
backend/core/kafka_consumer.py
================================
Reusable Kafka consumer base for the Airflow pipeline and agents.

Design:
  - Stateless helper functions (not a class) — simpler to mock and test.
  - Uses confluent_kafka Consumer with manual commit (no auto-commit).
  - Returns a generator so callers can process messages incrementally.
  - Degrades gracefully when confluent-kafka is not installed.

Primary use: Airflow operators pull messages from `raw.data.events`
and process them through the Bronze → Silver → Gold pipeline.
"""

from __future__ import annotations

import json
from collections.abc import Generator
from typing import Any

from config.constants import KafkaTopic
from config.logging_config import get_logger
from config.settings import kafka_settings

logger = get_logger(__name__)

try:
    from confluent_kafka import Consumer as _Consumer
    from confluent_kafka import KafkaError, KafkaException, Message
    _KAFKA_AVAILABLE = True
except ImportError:
    _Consumer = None            # type: ignore[assignment,misc]
    KafkaError = None           # type: ignore[assignment,misc]
    KafkaException = Exception  # type: ignore[assignment,misc]
    Message = None              # type: ignore[assignment,misc]
    _KAFKA_AVAILABLE = False
    print("[kafka_consumer.py] WARNING: confluent-kafka not installed — consumer disabled")


def _build_consumer(group_id: str, extra_config: dict | None = None) -> Any:
    """
    Instantiate a configured confluent-kafka Consumer.

    Args:
        group_id:     Consumer group ID (appended to group_id_prefix).
        extra_config: Additional consumer config key-value pairs.

    Returns:
        A confluent_kafka.Consumer instance.
    """
    if not _KAFKA_AVAILABLE:
        raise ImportError("confluent-kafka is not installed")

    config = {
        **kafka_settings.consumer_config,
        "group.id": f"{kafka_settings.group_id_prefix}{group_id}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        **(extra_config or {}),
    }
    return _Consumer(config)


def consume_batch(
    topic: KafkaTopic | str,
    *,
    group_id: str,
    batch_size: int = 100,
    poll_timeout: float = 1.0,
    max_empty_polls: int = 5,
) -> Generator[dict[str, Any], None, None]:
    """
    Consume up to ``batch_size`` messages from a Kafka topic.

    Yields decoded message envelopes (the full JSON envelope, not just payload).
    Commits offsets after each yielded message.

    Args:
        topic:           Kafka topic to consume from.
        group_id:        Consumer group suffix.
        batch_size:      Maximum messages to yield per call.
        poll_timeout:    Seconds to wait per poll call.
        max_empty_polls: Stop after this many consecutive empty polls.

    Yields:
        Decoded message dict (event envelope with ``payload`` key).
    """
    if not _KAFKA_AVAILABLE:
        logger.warning("Kafka consumer unavailable — no messages yielded")
        return

    topic_str = topic.value if isinstance(topic, KafkaTopic) else topic
    consumer = _build_consumer(group_id)

    try:
        consumer.subscribe([topic_str])
        logger.info("Kafka consumer subscribed", topic=topic_str, group_id=group_id)

        yielded = 0
        empty_polls = 0

        while yielded < batch_size and empty_polls < max_empty_polls:
            msg = consumer.poll(timeout=poll_timeout)

            if msg is None:
                empty_polls += 1
                continue

            if msg.error():
                err = msg.error()
                if err.code() == KafkaError._PARTITION_EOF:  # type: ignore[union-attr]
                    # Reached end of partition — not an error
                    empty_polls += 1
                    continue
                logger.error("Kafka consumer error", error=str(err), topic=topic_str)
                empty_polls += 1
                continue

            empty_polls = 0  # reset on successful message

            try:
                raw = msg.value()
                envelope = json.loads(raw.decode("utf-8")) if raw else {}
                consumer.commit(message=msg, asynchronous=False)
                yielded += 1
                logger.debug(
                    "Kafka message consumed",
                    topic=topic_str,
                    offset=msg.offset(),
                    partition=msg.partition(),
                    event_id=envelope.get("event_id"),
                )
                yield envelope
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.error(
                    "Kafka message decode failed — skipping",
                    error=str(exc),
                    topic=topic_str,
                )
                consumer.commit(message=msg, asynchronous=False)  # don't replay bad msgs

    finally:
        consumer.close()
        logger.info("Kafka consumer closed", topic=topic_str, messages_consumed=yielded)


def get_topic_lag(topic: KafkaTopic | str, group_id: str) -> int:
    """
    Return the approximate consumer lag (sum of all partition lags) for a group.

    Returns -1 if lag cannot be determined (Kafka unavailable).
    """
    if not _KAFKA_AVAILABLE:
        return -1

    topic_str = topic.value if isinstance(topic, KafkaTopic) else topic
    consumer = _build_consumer(group_id)
    try:
        consumer.subscribe([topic_str])
        metadata = consumer.list_topics(topic=topic_str, timeout=5.0)
        total_lag = 0
        for partition in metadata.topics[topic_str].partitions.values():
            lo, hi = consumer.get_watermark_offsets(
                partition, timeout=5.0, cached=False
            )
            committed = consumer.committed([partition], timeout=5.0)
            committed_offset = committed[0].offset if committed else lo
            total_lag += max(0, hi - max(lo, committed_offset))
        return total_lag
    except Exception as exc:
        logger.warning("get_topic_lag failed", error=str(exc))
        return -1
    finally:
        consumer.close()
