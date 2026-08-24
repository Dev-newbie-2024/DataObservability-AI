"""
config/logging_config.py
========================
Structured JSON logging using structlog + stdlib logging.

Architecture
------------
structlog is configured with ``LoggerFactory()`` so every bound logger
wraps a real ``logging.Logger`` — this is why ``add_logger_name`` works
(it reads ``logger.name``, which only exists on stdlib loggers, not
PrintLogger).

The stdlib root logger uses ``ProcessorFormatter`` as its formatter so
that foreign records from uvicorn, asyncio, and third-party libraries
are also rendered through the same shared processor chain.

Usage
-----
    from config.logging_config import configure_logging, get_logger

    configure_logging()                         # once at startup
    logger = get_logger(__name__)
    logger.info("event", dataset_id="abc", rows=1_000)
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog
from structlog.stdlib import ProcessorFormatter

from config.settings import settings


def configure_logging() -> None:
    """
    Wire structlog to the stdlib logging framework.

    Idempotent — safe to call multiple times (duplicate handlers are skipped).
    """
    log_level: int = getattr(logging, settings.log_level.upper(), logging.INFO)

    # ── Shared processors (run for both structlog and stdlib records) ─────────
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,           # reads logger.name → stdlib ✓
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]

    # ── Pick renderer based on environment ────────────────────────────────────
    if settings.is_production:
        final_renderer: Any = structlog.processors.JSONRenderer()
        exc_renderer: Any = structlog.processors.format_exc_info
    else:
        final_renderer = structlog.dev.ConsoleRenderer(colors=True)
        exc_renderer = structlog.processors.ExceptionRenderer()

    # ── Configure structlog (uses stdlib LoggerFactory so .name exists) ───────
    structlog.configure(
        processors=[
            *shared_processors,
            exc_renderer,
            # Bridge: tells structlog to hand the event dict to ProcessorFormatter
            ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),   # ← stdlib, not Print
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # ── Configure the stdlib root logger to render via ProcessorFormatter ─────
    formatter = ProcessorFormatter(
        # processors applied to foreign (non-structlog) stdlib log records
        foreign_pre_chain=shared_processors,
        # final renderer applied to ALL records (structlog and foreign)
        processors=[
            ProcessorFormatter.remove_processors_meta,
            final_renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(log_level)

    # Prevent duplicate handlers if configure_logging() is called again
    if not any(isinstance(h, logging.StreamHandler) and
               isinstance(getattr(h, "formatter", None), ProcessorFormatter)
               for h in root.handlers):
        root.addHandler(handler)

    # ── Silence noisy third-party loggers ─────────────────────────────────────
    for noisy in [
        "snowflake.connector",
        "urllib3",
        "httpx",
        "confluent_kafka",
        "great_expectations",
        "botocore",
        "boto3",
    ]:
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """
    Return a bound structlog logger for the given module name.

    Args:
        name: Typically ``__name__`` of the calling module.

    Returns:
        A ``structlog.stdlib.BoundLogger`` with ``name`` pre-bound.

    Example::

        logger = get_logger(__name__)
        logger.info("pipeline started", dataset="sales_data", rows=10_000)
    """
    return structlog.get_logger(name)


# ── Module-level initialisation ───────────────────────────────────────────────
# configure_logging() is called when this module is first imported so that
# every module that does ``from config.logging_config import get_logger``
# gets a working logger without needing to call configure_logging() itself.
configure_logging()
