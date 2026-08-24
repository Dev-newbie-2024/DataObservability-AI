"""
ai_agents/base_agent.py
========================
Abstract base class for all five observability agents.
Every agent inherits from BaseAgent and implements run().
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import Any

from config.constants import AgentName, Severity
from config.logging_config import get_logger
from config.settings import agent_settings


class BaseAgent(ABC):
    """
    Abstract base for all observability agents.

    Subclasses must implement:
        - run() : main agent logic called per event or schedule
    """

    name: AgentName  # Set by each subclass

    def __init__(self) -> None:
        self.logger = get_logger(self.__class__.__name__)
        self.max_retries = agent_settings.max_retries
        self.retry_delay = agent_settings.retry_delay_seconds

    @abstractmethod
    def run(self, context: dict[str, Any]) -> dict[str, Any]:
        """
        Execute the agent's main logic.

        Args:
            context: Event context dict (from Kafka message or Airflow XCom)

        Returns:
            Result dict with at minimum:
                - status: "resolved" | "escalated" | "failed"
                - actions_taken: list of action descriptions
                - severity: Severity enum value
        """
        ...

    def execute_with_retry(self, context: dict[str, Any]) -> dict[str, Any]:
        """
        Wraps run() with retry logic.
        On all retries exhausted, returns escalation result.
        """
        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.info(
                    "Agent executing",
                    agent=self.name,
                    attempt=attempt,
                    dataset_id=context.get("dataset_id"),
                )
                result = self.run(context)
                self.logger.info(
                    "Agent completed",
                    agent=self.name,
                    status=result.get("status"),
                    attempt=attempt,
                )
                return result

            except Exception as exc:
                last_error = exc
                self.logger.warning(
                    "Agent attempt failed",
                    agent=self.name,
                    attempt=attempt,
                    max_retries=self.max_retries,
                    error=str(exc),
                )
                if attempt < self.max_retries:
                    time.sleep(self.retry_delay)

        # All retries exhausted
        self.logger.error(
            "Agent escalating after all retries",
            agent=self.name,
            error=str(last_error),
        )
        return {
            "status": "escalated",
            "severity": Severity.CRITICAL,
            "actions_taken": [f"escalated after {self.max_retries} retries"],
            "error": str(last_error),
        }

    def _build_result(
        self,
        status: str,
        severity: Severity,
        actions: list[str],
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Helper to build a standardised agent result dict."""
        return {
            "agent": self.name,
            "status": status,
            "severity": severity,
            "actions_taken": actions,
            "details": details or {},
        }
