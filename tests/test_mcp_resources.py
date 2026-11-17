"""Reading and saving an MCP server's resources (Phase D).

knowledge-base/knowledge/features/mcp_client_2026_07_28_elicitation.md,
owner decision D2 (2026-11-17). A generation API answers with a
``resource_link`` that only ``resources/read`` resolves. Two tools serve every
server that offers resources: ``read_mcp_resource`` shows one to the model
through the result converter, and ``save_mcp_resource`` writes one into the
workspace without passing it through the context.

The integration tests run a real mcp 2.x server as a subprocess and load the
tools the way both runtimes do, through ``load_tools`` with a ToolContext.
"""

from __future__ import annotations

import asyncio
import base64
import sys
import textwrap
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from agent.core.workspace import WorkspaceManager
from agent.services.image_content import extract_image_tags
from agent.tools.context import ToolContext
from agent.tools.mcp import manager as manager_module
from agent.tools.mcp.manager import (
    MCP_RESOURCE_TEXT_LIMIT,
    READ_RESOURCE_TOOL,
    SAVE_RESOURCE_TOOL,
    MCPManager,
    _render_resource,
    _ServerHandle,
)
from agent.tools.registry import TOOL_REGISTRY, load_tools, register_mcp_tools
from shared.permission_modes import READ, WRITE
from tests._fs_backend import FilesystemTestBackend
from tests.test_mcp_manager import _free_port, _wait_for_port

# A 1x1 PNG.
PNG = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQ"
    "DwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
)
ZIP_BYTES = b"PK\x05\x06" + b"\x00" * 18

SERVER = textwrap.dedent(
    f"""
    import base64, sys
    from mcp.server.mcpserver import MCPServer
    from mcp.types import ResourceLink

    port = int(sys.argv[1])
    mcp = MCPServer("gen", log_level="ERROR")

    @mcp.resource("notes://readme", mime_type="text/markdown")
    def readme() -> str:
        return "# Read me\\nhello"

    @mcp.resource("blobs://{{blob_id}}", mime_type="image/png")
    def blob(blob_id: str) -> bytes:
        return base64.b64decode("{PNG}")

    @mcp.resource("files://archive.zip", mime_type="application/zip")
    def archive() -> bytes:
        return {ZIP_BYTES!r}

    @mcp.tool()
    def generate() -> list:
        \"\"\"Answer with a link to the generated picture.\"\"\"
        return [ResourceLink(type="resource_link", name="out.png",
                             uri="blobs://b-1", mimeType="image/png")]

    mcp.run(transport="streamable-http", host="127.0.0.1", port=port)
    """
)


@asynccontextmanager
async def _server(tmp_path):
    script = tmp_path / "gen_server.py"
    script.write_text(SERVER)
    port = _free_port()
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script),
        str(port),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    manager = None
    try:
        await _wait_for_port(port)
        manager = MCPManager(
            [
                {
                    "type": "mcp",
                    "name": "Gen",
                    "connection_url": f"http://127.0.0.1:{port}/mcp",
                    "credentials": {"transport": "http"},
                }
            ]
        )
        await manager.connect_all()
        yield manager
    finally:
        if manager is not None:
            await manager.aclose()
        if process.returncode is None:
            process.terminate()
        await process.wait()


def _workspace(tmp_path) -> WorkspaceManager:
    root = tmp_path / "workspace"
    root.mkdir()
    workspace = WorkspaceManager(
        job_id="job-resources", base_path=root, backend=FilesystemTestBackend(root)
    )
    workspace.initialize()
    return workspace


def _loaded(manager, *, workspace=None, sees_images: bool = False) -> dict:
    """The MCP tools a runtime loads: registered, then bound to its context."""
    register_mcp_tools(manager)
    context = ToolContext(
        workspace_manager=workspace,
        datasources={"mcp": manager},
        config={"multimodal": sees_images},
    )
    names = [name for name, meta in TOOL_REGISTRY.items() if meta["category"] == "mcp"]
    return {tool.name: tool for tool in load_tools(names, context)}


async def _as_job(tool, **arguments) -> str:
    """The content a job's ToolNode puts in the ToolMessage."""
    message = await tool.ainvoke(
        {"type": "tool_call", "id": "call-1", "name": tool.name, "args": arguments}
    )
    return message.content


@pytest.mark.asyncio
async def test_a_resource_link_is_read_and_seen_as_an_image(tmp_path):
    async with _server(tmp_path) as manager:
        tools = _loaded(manager, sees_images=True)
        link = await tools["mcp__gen__generate"].ainvoke({})
        assert link == "[resource link] out.png: blobs://b-1 (image/png)"

        shown = await _as_job(
            tools[READ_RESOURCE_TOOL], server="gen", uri="blobs://b-1"
        )
        _text, images = extract_image_tags(shown)
        assert [(image.mime_type, image.base64_data) for image in images] == [
            ("image/png", PNG)
        ]


@pytest.mark.asyncio
async def test_text_comes_back_as_text_and_a_binary_is_described(tmp_path):
    async with _server(tmp_path) as manager:
        tools = _loaded(manager)
        read = tools[READ_RESOURCE_TOOL]

        text = await read.ainvoke({"server": "gen", "uri": "notes://readme"})
        assert text == "[resource notes://readme (text/markdown)]\n# Read me\nhello"

        binary = await read.ainvoke({"server": "gen", "uri": "files://archive.zip"})
        assert binary == (
            f"[resource files://archive.zip: application/zip, {len(ZIP_BYTES)} bytes, "
            f"not shown: save it into the workspace with {SAVE_RESOURCE_TOOL}]"
        )

        image = await read.ainvoke({"server": "gen", "uri": "blobs://b-1"})
        assert "not shown: this model cannot see images" in image


@pytest.mark.asyncio
async def test_a_resource_is_saved_into_the_workspace_byte_for_byte(tmp_path):
    workspace = _workspace(tmp_path)
    async with _server(tmp_path) as manager:
        tools = _loaded(manager, workspace=workspace)
        save = tools[SAVE_RESOURCE_TOOL]

        result = await _as_job(
            save, server="gen", uri="blobs://b-1", path="output/out.png"
        )
        target = workspace.get_path("output/out.png")
        assert result == f"Saved: {target} ({len(base64.b64decode(PNG)):,} bytes)"
        assert target.read_bytes() == base64.b64decode(PNG)

        await save.ainvoke(
            {"server": "gen", "uri": "files://archive.zip", "path": "a.zip"}
        )
        assert workspace.get_path("a.zip").read_bytes() == ZIP_BYTES


@pytest.mark.asyncio
async def test_an_existing_file_is_replaced_only_when_asked(tmp_path):
    workspace = _workspace(tmp_path)
    workspace.write_file("notes.md", "mine")
    async with _server(tmp_path) as manager:
        save = _loaded(manager, workspace=workspace)[SAVE_RESOURCE_TOOL]
        arguments = {"server": "gen", "uri": "notes://readme", "path": "notes.md"}

        refused = await save.ainvoke(arguments)
        assert refused.startswith("Error: ") and "already exists" in refused
        assert workspace.read_file("notes.md") == "mine"

        await save.ainvoke({**arguments, "overwrite": True})
        assert workspace.read_file("notes.md") == "# Read me\nhello"


@pytest.mark.asyncio
async def test_without_a_workspace_save_says_so(tmp_path):
    async with _server(tmp_path) as manager:
        save = _loaded(manager)[SAVE_RESOURCE_TOOL]
        result = await save.ainvoke(
            {"server": "gen", "uri": "notes://readme", "path": "x.md"}
        )
        assert result == (
            "Error: MCP tool Gen/save_mcp_resource failed: this agent has no "
            "workspace to save the file into."
        )


@pytest.mark.asyncio
async def test_an_unknown_resource_reports_the_servers_words(tmp_path):
    async with _server(tmp_path) as manager:
        read = _loaded(manager)[READ_RESOURCE_TOOL]
        result = await read.ainvoke({"server": "gen", "uri": "nope://missing"})
        assert result.startswith("Error: MCP tool Gen/read_mcp_resource failed: ")
        assert "nope://missing" in result
        # The server answered: its session is alive.
        assert manager.statuses == {"Gen": "connected"}


@pytest.mark.asyncio
async def test_an_unknown_server_names_the_ones_that_offer_resources(tmp_path):
    async with _server(tmp_path) as manager:
        read = _loaded(manager)[READ_RESOURCE_TOOL]
        result = await read.coroutine(server="other", uri="notes://readme")
        assert result == (
            "Error: read_mcp_resource failed: no connected MCP server 'other' "
            "offers resources (servers that do: gen)."
        )


@pytest.mark.asyncio
async def test_the_resource_tools_read_and_write_as_their_effects_say(tmp_path):
    async with _server(tmp_path) as manager:
        register_mcp_tools(manager)
        assert TOOL_REGISTRY[READ_RESOURCE_TOOL]["effect"] == READ
        assert TOOL_REGISTRY[SAVE_RESOURCE_TOOL]["effect"] == WRITE
        register_mcp_tools(None)
        assert READ_RESOURCE_TOOL not in TOOL_REGISTRY


def _handle(slug: str, *, offers: bool, status: str = "connected") -> _ServerHandle:
    handle = _ServerHandle(
        ds={"name": slug.title()}, config=None, slug=slug, status=status
    )
    handle.offers_resources = offers
    return handle


def test_no_resource_tools_without_a_server_that_offers_resources():
    manager = MCPManager([])
    manager._handles = [
        _handle("plain", offers=False),
        _handle("gone", offers=True, status="unavailable: ConnectError"),
    ]
    assert manager.get_langchain_tools() == []

    manager._handles.append(_handle("gen", offers=True))
    tools = manager.get_langchain_tools()
    assert [tool.name for tool in tools] == [READ_RESOURCE_TOOL, SAVE_RESOURCE_TOOL]
    assert tools[0].args_schema["properties"]["server"]["enum"] == ["gen"]


def test_a_managed_server_offers_no_resources_through_its_front():
    """The front forwards tool calls only (drivers/mcp-front allowedMethods)."""
    client = SimpleNamespace(server_capabilities=SimpleNamespace(resources=object()))
    assert manager_module._offers_resources(client) is True
    assert manager_module._offers_resources(SimpleNamespace()) is False


def test_a_long_text_is_cut_with_a_way_to_keep_all_of_it():
    long = "x" * (MCP_RESOURCE_TEXT_LIMIT + 5)
    result = SimpleNamespace(
        contents=[SimpleNamespace(uri="notes://big", mime_type=None, text=long)]
    )
    shown = _render_resource(result, sees_images=False)
    assert shown.startswith("[resource notes://big]\n" + "x" * 10)
    assert shown.endswith(
        f"[… 5 more characters not shown: save the whole resource with "
        f"{SAVE_RESOURCE_TOOL}]"
    )
