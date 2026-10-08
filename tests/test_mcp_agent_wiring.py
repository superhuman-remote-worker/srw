"""Agent-side MCP slice from datasource config through live tool loading."""

import asyncio
import socket
import sys
import textwrap

import pytest

from tests._connector_runtime import open_harness
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
    import sys
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP("echo", host="127.0.0.1", port=int(sys.argv[1]), log_level="ERROR")

    @mcp.tool()
    def echo(text: str) -> str:
        \"\"\"Echo the input back.\"\"\"
        return f"echo: {text}"

    mcp.run(transport="streamable-http")
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


async def _wait_for_port(port: int) -> None:
    for _ in range(100):
        try:
            _reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.close()
            await writer.wait_closed()
            return
        except OSError:
            await asyncio.sleep(0.05)
    raise AssertionError(f"test MCP server did not listen on port {port}")


@pytest.mark.asyncio
async def test_full_job_path_slice(tmp_path):
    script = tmp_path / "echo_server.py"
    script.write_text(ECHO_SERVER)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script),
        str(port),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    datasource = {
        "type": "mcp",
        "name": "Echo",
        "connection_url": f"http://127.0.0.1:{port}/mcp",
        "credentials": {"transport": "http"},
    }

    manager = None
    try:
        await _wait_for_port(port)
        connections, _ = open_harness([datasource])
        manager = connections["mcp"]
        await manager.connect_all()
        register_mcp_tools(manager)
        manager.annotate_configs()
        assert datasource["_mcp_status"] == "connected"

        names = expand_tool_wildcards(["*"])
        assert names == ["mcp__echo__echo"]

        tools = load_tools(names, ToolContext(datasources={"mcp": manager}))
        result = await tools[0].coroutine(text="hi")
        assert "echo: hi" in str(result)
    finally:
        if manager is not None:
            await manager.aclose()
        server.terminate()
        await server.wait()
