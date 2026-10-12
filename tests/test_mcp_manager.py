"""MCPManager lifecycle: parse, connect, discover, degrade, and close.

Integration tests run a real MCP server over HTTP (or SSE) as a subprocess
via ``sys.executable``. They require no external network or service. The
agent never runs a stdio server itself (connector drivers D5b): a stdio
entry is refused, never spawned.
"""

import asyncio
import socket
import sys
import textwrap
from contextlib import asynccontextmanager

import pytest

from agent.tools.mcp.manager import MCPManager, parse_mcp_config
from shared.connectors.builtin import MCP_STDIO_RETIRED

HTTP_ECHO_SERVER = textwrap.dedent(
    """
    import sys
    from mcp.server.fastmcp import FastMCP

    port = int(sys.argv[1])
    transport = sys.argv[2]
    mcp = FastMCP(
        "echo",
        host="127.0.0.1",
        port=port,
        log_level="ERROR",
    )

    @mcp.tool()
    def echo(text: str) -> str:
        \"\"\"Echo the input back.\"\"\"
        return f"echo: {text}"

    @mcp.tool()
    def add(a: int, b: int) -> int:
        \"\"\"Add two integers.\"\"\"
        return a + b

    mcp.run(transport=transport)
    """
)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _http_ds(port: int, name="Echo Server", transport="http", path="/mcp"):
    return {
        "type": "mcp",
        "name": name,
        "connection_url": f"http://127.0.0.1:{port}{path}",
        "credentials": {"transport": transport},
    }


def _stdio_ds(name="Local Server"):
    """What an orchestrator from before D5b sent for a stdio server."""
    return {
        "type": "mcp",
        "name": name,
        "connection_url": None,
        "credentials": {
            "transport": "stdio",
            "command": sys.executable,
            "args": ["-c", "raise SystemExit(0)"],
            "env": {},
        },
    }


async def _wait_for_port(port: int) -> None:
    for _ in range(100):
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.close()
            await writer.wait_closed()
            return
        except OSError:
            await asyncio.sleep(0.05)
    raise AssertionError(f"test MCP server did not listen on port {port}")


@asynccontextmanager
async def _echo_server(tmp_path, transport="streamable-http"):
    """Run the echo server; yields its process and port."""
    script = tmp_path / "http_echo_server.py"
    script.write_text(HTTP_ECHO_SERVER)
    port = _free_port()
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script),
        str(port),
        transport,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        await _wait_for_port(port)
        yield process, port
    finally:
        if process.returncode is None:
            process.terminate()
        await process.wait()


class TestParseConfig:
    def test_http_requires_url(self):
        with pytest.raises(ValueError, match="connection_url"):
            parse_mcp_config(
                {
                    "type": "mcp",
                    "name": "x",
                    "connection_url": None,
                    "credentials": {"transport": "http"},
                }
            )

    @pytest.mark.parametrize("transport", ["stdio", " STDIO "])
    def test_stdio_is_refused(self, transport):
        entry = _stdio_ds()
        entry["credentials"]["transport"] = transport
        with pytest.raises(ValueError) as caught:
            parse_mcp_config(entry)
        assert str(caught.value) == MCP_STDIO_RETIRED

    def test_unknown_transport_rejected(self):
        with pytest.raises(ValueError, match="transport"):
            parse_mcp_config(
                {
                    "type": "mcp",
                    "name": "x",
                    "connection_url": "http://h",
                    "credentials": {"transport": "carrier-pigeon"},
                }
            )

    def test_bearer_auth_becomes_header(self):
        cfg = parse_mcp_config(
            {
                "type": "mcp",
                "name": "x",
                "connection_url": "https://h/mcp",
                "credentials": {
                    "transport": "http",
                    "auth": {"type": "bearer", "token": "tok123"},
                },
            }
        )
        assert cfg.headers == {"Authorization": "Bearer tok123"}

    def test_defaults_to_http_transport(self):
        cfg = parse_mcp_config(
            {
                "type": "mcp",
                "name": "x",
                "connection_url": "https://h/mcp",
                "credentials": {},
            }
        )
        assert cfg.transport == "http"


@pytest.mark.asyncio
async def test_connect_discover_call_close(tmp_path):
    async with _echo_server(tmp_path) as (_process, port):
        ds = _http_ds(port)
        manager = MCPManager([ds])
        await manager.connect_all()
        try:
            tools = manager.get_langchain_tools()
            names = {tool.name for tool in tools}
            assert "mcp__echo_server__echo" in names
            assert "mcp__echo_server__add" in names
            echo = next(tool for tool in tools if tool.name.endswith("__echo"))
            result = await echo.coroutine(text="hi")
            assert "echo: hi" in str(result)
            manager.annotate_configs()
            assert ds["_mcp_status"] == "connected"
            assert "mcp__echo_server__echo" in ds["_mcp_tools"]
        finally:
            await manager.aclose()


@pytest.mark.asyncio
async def test_unreachable_server_degrades_not_raises(tmp_path):
    broken = _http_ds(_free_port(), name="Broken")
    async with _echo_server(tmp_path) as (_process, port):
        good = _http_ds(port, name="Good")
        manager = MCPManager([broken, good])
        await manager.connect_all()
        try:
            manager.annotate_configs()
            assert broken["_mcp_status"].startswith("unavailable")
            assert good["_mcp_status"] == "connected"
            assert all(
                tool.name.startswith("mcp__good__")
                for tool in manager.get_langchain_tools()
            )
        finally:
            await manager.aclose()


@pytest.mark.asyncio
async def test_a_stdio_entry_is_unavailable_and_never_started(tmp_path):
    """An older orchestrator's stdio entry never gets a connection task, so
    nothing is spawned; the status tells the agent why."""
    async with _echo_server(tmp_path) as (_process, port):
        stdio = _stdio_ds()
        good = _http_ds(port, name="Good")
        manager = MCPManager([stdio, good])
        await manager.connect_all()
        try:
            manager.annotate_configs()
            refused = manager._handles[0]
            assert (refused.config, refused.task) == (None, None)
            assert stdio["_mcp_status"] == (
                f"unavailable: invalid config ({MCP_STDIO_RETIRED})"
            )
            assert stdio["_mcp_tools"] == []
            assert good["_mcp_status"] == "connected"
            assert all(
                tool.name.startswith("mcp__good__")
                for tool in manager.get_langchain_tools()
            )
        finally:
            await manager.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("client_transport", "server_transport", "path"),
    [
        ("http", "streamable-http", "/mcp"),
        ("sse", "sse", "/sse"),
    ],
)
async def test_remote_transport_discover_and_call(
    tmp_path,
    client_transport,
    server_transport,
    path,
):
    async with _echo_server(tmp_path, server_transport) as (_process, port):
        manager = MCPManager(
            [
                _http_ds(
                    port,
                    name=f"Remote {client_transport}",
                    transport=client_transport,
                    path=path,
                )
            ]
        )
        try:
            await manager.connect_all()
            echo = next(
                tool
                for tool in manager.get_langchain_tools()
                if tool.name.endswith("__echo")
            )
            assert "echo: remote" in str(await echo.coroutine(text="remote"))
        finally:
            await manager.aclose()


@pytest.mark.asyncio
async def test_invalid_config_marked_invalid():
    manager = MCPManager(
        [
            {
                "type": "mcp",
                "name": "bad",
                "connection_url": None,
                "credentials": {},
            }
        ]
    )
    await manager.connect_all()
    assert manager.statuses["bad"].startswith("unavailable")
    await manager.aclose()


@pytest.mark.asyncio
async def test_sync_close_inside_running_loop(tmp_path):
    async with _echo_server(tmp_path) as (_process, port):
        manager = MCPManager([_http_ds(port)])
        await manager.connect_all()
        manager.close()
        await asyncio.sleep(0.5)
        assert all(handle.task.done() for handle in manager._handles)


@pytest.mark.asyncio
async def test_tool_error_returns_string_not_raise(tmp_path):
    async with _echo_server(tmp_path) as (process, port):
        manager = MCPManager([_http_ds(port)])
        await manager.connect_all()
        try:
            echo = next(
                tool
                for tool in manager.get_langchain_tools()
                if tool.name.endswith("__echo")
            )
            handle = manager._handles[0]
            handle.shutdown.set()
            await asyncio.sleep(0.3)
            # The reconnect finds nothing listening.
            process.terminate()
            await process.wait()

            result = await echo.coroutine(text="hi")

            assert isinstance(result, str)
            assert "MCP" in result and "error" in result.lower()
        finally:
            await manager.aclose()


@pytest.mark.asyncio
async def test_reconnect_once_revives_tool(tmp_path):
    async with _echo_server(tmp_path) as (_process, port):
        manager = MCPManager([_http_ds(port)])
        await manager.connect_all()
        try:
            echo = next(
                tool
                for tool in manager.get_langchain_tools()
                if tool.name.endswith("__echo")
            )
            handle = manager._handles[0]
            handle.shutdown.set()
            await asyncio.sleep(0.3)

            result = await echo.coroutine(text="revived")

            assert "echo: revived" in str(result)
            assert handle.reconnected_once is True
        finally:
            await manager.aclose()
