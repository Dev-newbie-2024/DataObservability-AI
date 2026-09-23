"""ai_agents/self_healing_agent — Automatic pipeline failure recovery agent."""

from ai_agents.self_healing_agent.self_healing_agent import (
    SelfHealingAgent,
    start_consumer_loop,
)

__all__ = ["SelfHealingAgent", "start_consumer_loop"]
