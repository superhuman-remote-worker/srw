"""Test helpers for the agent's connector materializers (``agent.connectors``)."""

from __future__ import annotations

from typing import Any

from agent.connectors import RuntimeContext, deliveries_from_payload
from agent.connectors.connections import ManagedConnectionMaterializer
from agent.connectors.mcp import McpClientMaterializer


def open_harness(entries: list[dict[str, Any]]) -> tuple[dict, dict]:
    """The harness phase without MCP discovery: ``(connections, clients)``.

    Managed connections open and the MCP manager is constructed (which only
    validates), exactly as an attach does before discovery.
    """
    rt = RuntimeContext(execution="worker")
    deliveries = deliveries_from_payload(entries)
    for materializer in (ManagedConnectionMaterializer(), McpClientMaterializer()):
        materializer.materialize(
            [d for d in deliveries if d.routes_to(materializer.form)], rt
        )
    return rt.connections, rt.clients
