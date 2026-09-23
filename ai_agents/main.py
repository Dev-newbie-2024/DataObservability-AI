"""
ai_agents/main.py
=================
AI Agents service entry point.
Exposes a minimal FastAPI health-check server so Docker healthcheck works.
Agent event loops are registered here (stubs in Phase 1).
Full agent implementation in Phase 3.
"""

from __future__ import annotations

import threading

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from config.logging_config import configure_logging, get_logger
from config.settings import settings

configure_logging()
logger = get_logger(__name__)

# ─── Health Server (minimal FastAPI) ──────────────────────────────────────────
health_app = FastAPI(title="AI Agents Health Server", docs_url=None, redoc_url=None)


@health_app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse(
        content={
            "status": "healthy",
            "service": "ai-agents",
            "agents": [
                "schema_agent",
                "quality_agent",
                "drift_agent",
                "cost_agent",
                "self_healing_agent",
            ],
            "phase": "4D-all-agents-active",
        }
    )


# ─── Agent Stubs ──────────────────────────────────────────────────────────────

def run_schema_agent() -> None:
    """Schema Agent — listens on schema.events topic. Phase 3."""
    logger.info("Schema Agent initialised (stub)", phase=1)


def run_quality_agent() -> None:
    """Quality Agent — runs GX checkpoints. Phase 4."""
    from ai_agents.quality_agent.quality_agent import start_consumer_loop
    start_consumer_loop()


def run_drift_agent() -> None:
    """Drift Agent — runs Evidently reports. Phase 4B."""
    from ai_agents.drift_agent.drift_agent import start_consumer_loop
    start_consumer_loop()


def run_cost_agent() -> None:
    """Cost Agent — monitors Snowflake credits. Phase 4C."""
    from ai_agents.cost_agent.cost_agent import start_consumer_loop
    start_consumer_loop()


def run_self_healing_agent() -> None:
    """Self-Healing Agent — processes heal.commands. Phase 4D."""
    from ai_agents.self_healing_agent.self_healing_agent import start_consumer_loop
    start_consumer_loop()


if __name__ == "__main__":
    logger.info(
        "AI Agents service starting",
        app_name=settings.app_name,
        env=settings.app_env,
    )

    # Start agent stubs in background threads (Phase 3: replace with Kafka consumers)
    agents = [
        run_schema_agent,
        run_quality_agent,
        run_drift_agent,
        run_cost_agent,
        run_self_healing_agent,
    ]
    for agent_fn in agents:
        t = threading.Thread(target=agent_fn, daemon=True)
        t.start()

    # Start health-check HTTP server
    uvicorn.run(health_app, host="0.0.0.0", port=8001, log_level="warning")
