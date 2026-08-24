"""
tests/unit/airflow/test_kafka_consumer.py
==========================================
Unit tests for backend/core/kafka_consumer.py.

All Kafka interactions are mocked — no broker required.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch, call

import pytest

from backend.core.kafka_consumer import consume_batch, get_topic_lag
from config.constants import KafkaTopic


def _make_mock_message(value: dict, offset: int = 0, partition: int = 0) -> MagicMock:
    """Build a mock confluent_kafka.Message with the given JSON payload."""
    import json
    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = json.dumps(value).encode("utf-8")
    msg.offset.return_value = offset
    msg.partition.return_value = partition
    msg.topic.return_value = KafkaTopic.RAW_DATA_EVENTS.value
    return msg


def _make_eof_message() -> MagicMock:
    """Build a mock message that signals partition EOF."""
    from unittest.mock import PropertyMock
    msg = MagicMock()
    err = MagicMock()
    err.code.return_value = -191  # confluent_kafka.KafkaError._PARTITION_EOF
    msg.error.return_value = err
    return msg


class TestConsumeBatch:
    """Tests for consume_batch() generator."""

    @patch("backend.core.kafka_consumer._KAFKA_AVAILABLE", False)
    def test_yields_nothing_when_kafka_unavailable(self):
        results = list(consume_batch(KafkaTopic.RAW_DATA_EVENTS, group_id="test"))
        assert results == []

    @patch("backend.core.kafka_consumer._KAFKA_AVAILABLE", True)
    @patch("backend.core.kafka_consumer._build_consumer")
    def test_yields_decoded_envelopes(self, mock_build):
        events = [
            {"event_id": "1", "payload": {"dataset_name": "sales"}},
            {"event_id": "2", "payload": {"dataset_name": "orders"}},
        ]
        messages = [_make_mock_message(e, offset=i) for i, e in enumerate(events)]
        # After messages, return None enough times to hit max_empty_polls
        poll_returns = messages + [None] * 6

        consumer = MagicMock()
        consumer.poll.side_effect = poll_returns
        mock_build.return_value = consumer

        results = list(consume_batch(
            KafkaTopic.RAW_DATA_EVENTS,
            group_id="test",
            batch_size=10,
            max_empty_polls=5,
        ))

        assert len(results) == 2
        assert results[0]["event_id"] == "1"
        assert results[1]["event_id"] == "2"
        # commit called once per message
        assert consumer.commit.call_count == 2

    @patch("backend.core.kafka_consumer._KAFKA_AVAILABLE", True)
    @patch("backend.core.kafka_consumer._build_consumer")
    def test_respects_batch_size_limit(self, mock_build):
        events = [{"event_id": str(i)} for i in range(20)]
        messages = [_make_mock_message(e) for e in events]

        consumer = MagicMock()
        consumer.poll.side_effect = messages
        mock_build.return_value = consumer

        results = list(consume_batch(
            KafkaTopic.RAW_DATA_EVENTS,
            group_id="test",
            batch_size=5,
        ))
        assert len(results) == 5

    @patch("backend.core.kafka_consumer._KAFKA_AVAILABLE", True)
    @patch("backend.core.kafka_consumer._build_consumer")
    def test_skips_malformed_messages(self, mock_build):
        bad_msg = MagicMock()
        bad_msg.error.return_value = None
        bad_msg.value.return_value = b"NOT JSON {{{"
        bad_msg.offset.return_value = 0
        bad_msg.partition.return_value = 0

        good_msg = _make_mock_message({"event_id": "ok"})
        poll_returns = [bad_msg, good_msg] + [None] * 6

        consumer = MagicMock()
        consumer.poll.side_effect = poll_returns
        mock_build.return_value = consumer

        results = list(consume_batch(
            KafkaTopic.RAW_DATA_EVENTS,
            group_id="test",
            batch_size=5,
            max_empty_polls=5,
        ))

        # Only the good message should be yielded
        assert len(results) == 1
        assert results[0]["event_id"] == "ok"
        # Bad message still committed (don't replay bad msgs)
        assert consumer.commit.call_count == 2

    @patch("backend.core.kafka_consumer._KAFKA_AVAILABLE", True)
    @patch("backend.core.kafka_consumer._build_consumer")
    def test_consumer_closed_on_completion(self, mock_build):
        consumer = MagicMock()
        consumer.poll.return_value = None  # immediate EOF
        mock_build.return_value = consumer

        list(consume_batch(
            KafkaTopic.RAW_DATA_EVENTS,
            group_id="test",
            max_empty_polls=1,
        ))

        consumer.close.assert_called_once()

    @patch("backend.core.kafka_consumer._KAFKA_AVAILABLE", True)
    @patch("backend.core.kafka_consumer._build_consumer")
    def test_consumer_closed_on_exception(self, mock_build):
        consumer = MagicMock()
        consumer.poll.side_effect = Exception("broker crash")
        mock_build.return_value = consumer

        with pytest.raises(Exception, match="broker crash"):
            list(consume_batch(KafkaTopic.RAW_DATA_EVENTS, group_id="test"))

        consumer.close.assert_called_once()


class TestGetTopicLag:
    @patch("backend.core.kafka_consumer._KAFKA_AVAILABLE", False)
    def test_returns_minus_one_when_unavailable(self):
        assert get_topic_lag(KafkaTopic.RAW_DATA_EVENTS, group_id="test") == -1

    @patch("backend.core.kafka_consumer._KAFKA_AVAILABLE", True)
    @patch("backend.core.kafka_consumer._build_consumer")
    def test_returns_minus_one_on_exception(self, mock_build):
        consumer = MagicMock()
        consumer.list_topics.side_effect = Exception("connection refused")
        mock_build.return_value = consumer

        result = get_topic_lag(KafkaTopic.RAW_DATA_EVENTS, group_id="test")
        assert result == -1
