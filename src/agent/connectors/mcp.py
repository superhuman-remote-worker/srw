"""``mcp_client``: MCP servers the agent process connects to.

One ``MCPManager`` aggregates every attached server in the ``mcp`` harness
slot, because the tools read the slot by kind. Construction only validates;
discovery runs in :meth:`McpClientMaterializer.ready` and degrades per
server. The manager parses today's payload entries and annotates them in
place with each server's status and tools, which the README then lists.
Registering the discovered tools in the process-wide tool registry stays
with the caller, which owns that registry.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from agent.connectors.base import Delivery, FactsLines, RuntimeContext
from agent.connectors.slots import connection_slot
from shared.connectors.builtin import MCP_SPEC
from shared.connectors.contract import managed_mcp_driver

logger = logging.getLogger(__name__)

#: The harness slot the MCP manager takes.
MCP_SLOT = connection_slot(MCP_SPEC) or ""


class McpClientMaterializer:
    form = "mcp_client"

    def materialize(self, deliveries: Sequence[Delivery], rt: RuntimeContext) -> None:
        if not deliveries:
            return
        from agent.tools.mcp import MCPManager

        rt.connections[MCP_SLOT] = MCPManager(
            [delivery.entry for delivery in deliveries]
        )

    async def ready(self, rt: RuntimeContext) -> None:
        """Discover every server's tools and annotate the entries."""
        manager = rt.connections.get(MCP_SLOT)
        if manager is None:
            return
        try:
            await manager.connect_all()
        except Exception as e:
            logger.warning(
                "Unexpected MCP discovery failure (%s, %s); continuing",
                rt.execution,
                type(e).__name__,
            )
        try:
            manager.annotate_configs()
        except Exception as e:
            logger.warning(
                "Could not annotate MCP servers (%s); continuing",
                type(e).__name__,
            )

    def facts(
        self, deliveries: Sequence[Delivery], rt: RuntimeContext
    ) -> list[FactsLines]:
        out: list[FactsLines] = []
        for delivery in deliveries:
            ds = delivery.entry
            name = ds.get("name", "Unnamed")
            transport = str(
                (ds.get("credentials") or {}).get("transport") or "http"
            ).lower()
            if delivery.spec is not None and managed_mcp_driver(delivery.spec):
                # Hosted by SRW behind its front (D5a): the agent holds a
                # lease token, never the server's upstream credential.
                transport = "managed by SRW"
            status = ds.get("_mcp_status") or "not connected yet"
            tools = ds.get("_mcp_tools") or []
            if status == "connected":
                shown = ", ".join(f"`{tool_name}`" for tool_name in tools[:40])
                if not shown:
                    shown = "_no tools advertised_"
                more = f" (+{len(tools) - 40} more)" if len(tools) > 40 else ""
                line = (
                    f"- **{name}** (mcp, {len(tools)} tools) — "
                    f"{transport}; {shown}{more}"
                )
            else:
                line = f"- **{name}** (mcp, {transport}) — {status}"
            out.append(FactsLines("MCP Servers", delivery.index, [line]))
        return out
