"""Agent-side MCP slice from datasource config through live tool loading."""

import sys
import textwrap

import pytest

from agent.core.datasource_setup import process_datasources
from shared.runtime.core.loader import get_all_tool_names, load_config_from_resolved
from agent.tools.context import ToolContext
from agent.tools.registry import (
    TOOL_REGISTRY,
    expand_tool_wildcards,
    load_tools,
    register_mcp_tools,
)

ECHO_SERVER = textwrap.dedent(
    """
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP("echo")

    @mcp.tool()
    def echo(text: str) -> str:
        \"\"\"Echo the input back.\"\"\"
        return f"echo: {text}"

    mcp.run(transport="stdio")
    """
)


@pytest.fixture(autouse=True)
def _clean_registry():
    yield
    for name in [
        name
        for name, metadata in TOOL_REGISTRY.items()
        if metadata.get("category") == "mcp"
    ]:
        del TOOL_REGISTRY[name]


def test_resolved_config_preserves_mcp_wildcard():
    config = load_config_from_resolved(
        {
            "agent": {
                "agent_id": "test",
                "display_name": "Test",
                "tools": {"mcp": ["*"]},
            },
            "prompts": {},
            "instructions": {},
        }
    )
    assert config.tools.mcp == ["*"]
    assert "*" in get_all_tool_names(config)


@pytest.mark.asyncio
async def test_full_job_path_slice(tmp_path):
    script = tmp_path / "echo_server.py"
    script.write_text(ECHO_SERVER)
    datasource = {
        "type": "mcp",
        "name": "Echo",
        "connection_url": None,
        "credentials": {
            "transport": "stdio",
            "command": sys.executable,
            "args": [str(script)],
            "env": {},
        },
    }

    connections, _ = process_datasources([datasource])
    manager = connections["mcp"]
    await manager.connect_all()
    try:
        register_mcp_tools(manager)
        manager.annotate_configs()
        assert datasource["_mcp_status"] == "connected"

        names = expand_tool_wildcards(["*"])
        assert names == ["mcp__echo__echo"]

        tools = load_tools(names, ToolContext(datasources={"mcp": manager}))
        result = await tools[0].coroutine(text="hi")
        assert "echo: hi" in str(result)
    finally:
        await manager.aclose()
