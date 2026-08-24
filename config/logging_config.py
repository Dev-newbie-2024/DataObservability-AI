"""
config/logging_config.py
========================
Structured JSON logging configuration using structlog.
Provides a consistent log format across all services.

Usage:
    from config.logging_config import get_logger
    logger = get_logger(__name__)
    logger.info("message", key="value", dataset_id="abc")
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

from config.settings import settings


def configure_logging() -> None:
    """
    Configure structlog with JSON rendering in production,
    console (coloured) rendering in development.
    Should be called once at application startup.
    """
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]

    if settings.is_production:
        # JSON output for log aggregation systems
        processors = [
            *shared_processors,
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ]
    else:
        # Pretty console output for development
        processors = [
            *shared_processors,
            structlog.processors.ExceptionRenderer(),
            structlog.dev.ConsoleRenderer(colors=True),
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )

    # Also configure stdlib logging to route through structlog
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
    )

    # Suppress noisy library loggers
    for noisy_logger in [
        "snowflake.connector",
        "urllib3",
        "httpx",
        "confluent_kafka",
        "great_expectations",
    ]:
        logging.getLogger(noisy_logger).setLevel(logging.WARNING)


def get_logger(name: str) -> structlog.BoundLogger:
    """
    Get a bound structlog logger.

    Args:
        name: Logger name (typically __name__ of the calling module)

    Returns:
        Bound structlog logger with the module name pre-bound.

    Example:
        logger = get_logger(__name__)
        logger.info("pipeline started", dataset="sales_data", rows=10_000)
    """
    return structlog.get_logger(name)


# Module-level call — configure when this module is first imported
configure_logging()
