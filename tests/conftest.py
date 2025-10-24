"""Shared pytest fixtures."""

from __future__ import annotations

import pytest

from agentic_py.core.agent_registry import AgentRegistry
from agentic_py.models.agent_profile import AgentProfile


@pytest.fixture()
def registry_with_sample_agents() -> AgentRegistry:
    """Provide a registry populated with sample agent profiles."""
    registry = AgentRegistry()
    registry.register(
        AgentProfile(identifier="agent-alpha", display_name="Agent Alpha"),
    )
    registry.register(
        AgentProfile(identifier="agent-beta", display_name="Agent Beta"),
    )
    return registry
