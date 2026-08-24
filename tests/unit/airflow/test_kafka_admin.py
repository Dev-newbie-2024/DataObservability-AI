"""
tests/unit/airflow/test_kafka_admin.py
========================================
Unit tests for backend/core/kafka_admin.py.

All tests mock the AdminClient — no real Kafka broker required.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from backend.core.kafka_admin import (
    TOPIC_SPECS,
    _list_existing_topics,
    ensure_topics,
    topic_exists,
)
from config.constants import KafkaTopic


class TestTopicSpecs:
    """Verify that all KafkaTopic enum values have corresponding specs."""

    def test_all_kafka_topics_have_specs(self):
        for topic in KafkaTopic:
            assert topic.value in TOPIC_SPECS, (
                f"KafkaTopic.{topic.name} ({topic.value!r}) missing from TOPIC_SPECS"
            )

    def test_partition_counts_are_positive(self):
        for name, (partitions, _) in TOPIC_SPECS.items():
            assert partitions >= 1, f"Topic {name!r} has invalid partition count"

    def test_replication_factors_are_positive(self):
        for name, (_, replication) in TOPIC_SPECS.items():
            assert replication >= 1, f"Topic {name!r} has invalid replication factor"


class TestEnsureTopics:
    """Tests for ensure_topics() with mocked AdminClient."""

    def _make_admin_mock(self, existing_topics: set[str]) -> MagicMock:
        """Return a MagicMock AdminClient with the given topics pre-existing."""
        admin = MagicMock()

        # list_topics returns a metadata object with a .topics dict
        metadata = MagicMock()
        metadata.topics = {t: MagicMock() for t in existing_topics}
        admin.list_topics.return_value = metadata

        # create_topics returns {topic: Future} where Future.result() returns None
        def _create_topics(new_topics, **kwargs):
            futures = {}
            for nt in new_topics:
                f = MagicMock()
                f.result.return_value = None  # success
                futures[nt.topic] = f
            return futures

        admin.create_topics.side_effect = _create_topics
        return admin

    @patch("backend.core.kafka_admin._ADMIN_AVAILABLE", True)
    @patch("backend.core.kafka_admin._get_admin_client")
    @patch("backend.core.kafka_admin.NewTopic")
    def test_creates_all_topics_when_none_exist(self, mock_new_topic, mock_get_admin):
        mock_new_topic.side_effect = lambda topic, **kw: MagicMock(topic=topic)
        mock_get_admin.return_value = self._make_admin_mock(existing_topics=set())
        results = ensure_topics(max_retries=1)
        assert all(v == "created" for v in results.values()), results

    @patch("backend.core.kafka_admin._ADMIN_AVAILABLE", True)
    @patch("backend.core.kafka_admin._get_admin_client")
    @patch("backend.core.kafka_admin.NewTopic")
    def test_skips_existing_topics(self, mock_new_topic, mock_get_admin):
        mock_new_topic.side_effect = lambda topic, **kw: MagicMock(topic=topic)
        all_topics = set(TOPIC_SPECS.keys())
        mock_get_admin.return_value = self._make_admin_mock(existing_topics=all_topics)
        results = ensure_topics(max_retries=1)
        # All already exist → no create call needed
        assert all(v == "exists" for v in results.values()), results

    @patch("backend.core.kafka_admin._ADMIN_AVAILABLE", False)
    def test_returns_skipped_when_kafka_unavailable(self):
        results = ensure_topics(max_retries=1)
        assert all(v == "skipped" for v in results.values()), results

    @patch("backend.core.kafka_admin._ADMIN_AVAILABLE", True)
    @patch("backend.core.kafka_admin._get_admin_client")
    @patch("backend.core.kafka_admin.NewTopic")
    def test_creates_only_missing_topics(self, mock_new_topic, mock_get_admin):
        mock_new_topic.side_effect = lambda topic, **kw: MagicMock(topic=topic)
        existing = {KafkaTopic.SCHEMA_EVENTS.value, KafkaTopic.QUALITY_EVENTS.value}
        mock_get_admin.return_value = self._make_admin_mock(existing_topics=existing)
        results = ensure_topics(max_retries=1)
        for t in existing:
            assert results[t] == "exists", f"{t} should be 'exists'"
        for t in TOPIC_SPECS:
            if t not in existing:
                assert results[t] == "created", f"{t} should be 'created'"

    @patch("backend.core.kafka_admin._ADMIN_AVAILABLE", True)
    @patch("backend.core.kafka_admin._get_admin_client")
    def test_exhausts_retries_on_persistent_failure(self, mock_get_admin):
        mock_get_admin.side_effect = Exception("broker unreachable")
        results = ensure_topics(max_retries=2, retry_delay=0.01)
        assert all(v.startswith("error:") for v in results.values()), results


class TestTopicExists:
    @patch("backend.core.kafka_admin._ADMIN_AVAILABLE", True)
    @patch("backend.core.kafka_admin._get_admin_client")
    def test_returns_true_for_existing_topic(self, mock_get_admin):
        admin = MagicMock()
        metadata = MagicMock()
        metadata.topics = {KafkaTopic.SCHEMA_EVENTS.value: MagicMock()}
        admin.list_topics.return_value = metadata
        mock_get_admin.return_value = admin

        assert topic_exists(KafkaTopic.SCHEMA_EVENTS) is True

    @patch("backend.core.kafka_admin._ADMIN_AVAILABLE", True)
    @patch("backend.core.kafka_admin._get_admin_client")
    def test_returns_false_for_missing_topic(self, mock_get_admin):
        admin = MagicMock()
        metadata = MagicMock()
        metadata.topics = {}
        admin.list_topics.return_value = metadata
        mock_get_admin.return_value = admin

        assert topic_exists("non.existent.topic") is False

    @patch("backend.core.kafka_admin._ADMIN_AVAILABLE", False)
    def test_returns_false_when_kafka_unavailable(self):
        assert topic_exists(KafkaTopic.RAW_DATA_EVENTS) is False
