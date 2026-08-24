"""
backend/core/kafka_producer.py
================================
Singleton Confluent Kafka producer with structured event publishing.

Design:
  - One producer instance per process (thread-safe by confluent-kafka design).
  - All messages are JSON-serialised with a standard envelope.
  - Delivery errors are logged and re-raised as KafkaProducerError.
  - `flush()` is called at app shutdown to drain any in-flight messages.
"""

from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from backend.core.exceptions import KafkaProducerError
from config.constants import KafkaTopic
from config.logging_config import get_logger
from config.settings import kafka_settings

logger = get_logger(__name__)

# ─── Conditional import — gracefully degrade if confluent-kafka not installed ─

try:
    from confluent_kafka import KafkaException
    from confluent_kafka import Producer as _ConfluentProducer
    _KAFKA_AVAILABLE = True
except ImportError:
    _KAFKA_AVAILABLE = False
    _ConfluentProducer = None  # type: ignore[assignment,misc]
    KafkaException = Exception  # type: ignore[assignment,misc]
    logger.warning("confluent-kafka not installed — Kafka publishing disabled")


# ─── Delivery Callback ────────────────────────────────────────────────────────

def _delivery_callback(err: Any, msg: Any) -> None:
    """Called by confluent-kafka after each message is delivered (or fails)."""
    if err:
        logger.error(
            "Kafka delivery failed",
            topic=msg.topic() if msg else "unknown",
            error=str(err),
        )
    else:
        logger.debug(
            "Kafka message delivered",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )


# ─── Producer Singleton ───────────────────────────────────────────────────────

class KafkaProducer:
    """
    Thin wrapper around confluent_kafka.Producer that:
      - Builds the standard event envelope.
      - Serialises to JSON.
      - Handles delivery errors.
      - Provides graceful degradation when Kafka is unavailable.
    """

    def __init__(self) -> None:
        self._producer: Any = None
        self._lock = threading.Lock()
        self._initialised = False

    def initialise(self) -> None:
        """Create the underlying confluent-kafka Producer. Call at startup."""
        if not _KAFKA_AVAILABLE:
            logger.warning("Kafka unavailable — producer will run in no-op mode")
            self._initialised = True
            return

        with self._lock:
            if self._initialised:
                return
            try:
                self._producer = _ConfluentProducer(
                    {
                        **kafka_settings.producer_config,
                        "on_delivery": _delivery_callback,
                    }
                )
                self._initialised = True
                logger.info(
                    "Kafka producer initialised",
                    bootstrap_servers=kafka_settings.bootstrap_servers,
                )
            except KafkaException as exc:
                logger.warning(
                    "Kafka producer init failed — running in no-op mode",
                    error=str(exc),
                )
                self._initialised = True  # Mark as init so we don't retry constantly

    def publish(
        self,
        topic: KafkaTopic | str,
        payload: dict[str, Any],
        *,
        key: str | None = None,
        dataset_id: str | None = None,
    ) -> str:
        """
        Publish a JSON event to a Kafka topic.

        The message is wrapped in a standard envelope::

            {
                "event_id":   "<uuid>",
                "topic":      "<topic name>",
                "dataset_id": "<uuid or null>",
                "published_at": "<ISO 8601>",
                "payload":    { ... }
            }

        Args:
            topic:      KafkaTopic enum value or raw topic name string.
            payload:    The event data dict.
            key:        Optional Kafka message key (used for partitioning).
            dataset_id: Optional dataset identifier (added to envelope).

        Returns:
            The event_id UUID string for traceability.
        """
        if not self._initialised:
            self.initialise()

        event_id = str(uuid.uuid4())
        topic_str = topic.value if isinstance(topic, KafkaTopic) else topic

        envelope: dict[str, Any] = {
            "event_id": event_id,
            "topic": topic_str,
            "dataset_id": dataset_id,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "payload": payload,
        }
        message_bytes = json.dumps(envelope, default=str).encode("utf-8")
        key_bytes = (key or dataset_id or event_id).encode("utf-8")

        if self._producer is None:
            # No-op mode — log and return
            logger.debug(
                "Kafka no-op publish",
                topic=topic_str,
                event_id=event_id,
            )
            return event_id

        try:
            self._producer.produce(
                topic=topic_str,
                key=key_bytes,
                value=message_bytes,
            )
            # Trigger delivery callbacks for any buffered messages
            self._producer.poll(0)
            logger.debug(
                "Kafka event queued",
                topic=topic_str,
                event_id=event_id,
                dataset_id=dataset_id,
            )
            return event_id
        except KafkaException as exc:
            raise KafkaProducerError(
                message=f"Failed to publish to topic '{topic_str}'",
                detail=str(exc),
            ) from exc

    def flush(self, timeout: float = 10.0) -> None:
        """Flush all buffered messages. Call at app shutdown."""
        if self._producer is not None:
            remaining = self._producer.flush(timeout=timeout)
            if remaining > 0:
                logger.warning("Kafka flush incomplete", remaining_messages=remaining)
            else:
                logger.info("Kafka producer flushed successfully")

    def close(self) -> None:
        """Flush and release producer resources."""
        self.flush()
        self._producer = None
        self._initialised = False
        logger.info("Kafka producer closed")


# ─── Module-level singleton ───────────────────────────────────────────────────

_producer: KafkaProducer | None = None
_producer_lock = threading.Lock()


def get_producer() -> KafkaProducer:
    """Return the module-level KafkaProducer singleton."""
    global _producer
    if _producer is None:
        with _producer_lock:
            if _producer is None:
                _producer = KafkaProducer()
    return _producer


def publish_event(
    topic: KafkaTopic | str,
    payload: dict[str, Any],
    *,
    key: str | None = None,
    dataset_id: str | None = None,
) -> str:
    """
    Module-level convenience function — publish without obtaining the singleton manually.

    Returns:
        The event_id UUID string.
    """
    return get_producer().publish(topic, payload, key=key, dataset_id=dataset_id)
