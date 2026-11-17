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
import io
import json
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

    # Servers that send a binary resource as a text item (image_generation
    # FM19): one as URL-safe, unpadded, line-wrapped base64, one unusable.
    @mcp.resource("textblobs://good", mime_type="image/png")
    def text_good() -> str:
        raw = "{PNG}".rstrip("=").translate(str.maketrans("+/", "-_"))
        return "\\n".join(raw[i:i + 20] for i in range(0, len(raw), 20))

    @mcp.resource("textblobs://bad", mime_type="image/png")
    def text_bad() -> str:
        return "\\u2030PNG\\r\\n this is the file's bytes read as text"

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
async def test_a_binary_resource_sent_as_text_is_never_saved_corrupt(tmp_path):
    workspace = _workspace(tmp_path)
    async with _server(tmp_path) as manager:
        tools = _loaded(manager, workspace=workspace, sees_images=True)
        save, read = tools[SAVE_RESOURCE_TOOL], tools[READ_RESOURCE_TOOL]

        # Base64 in any common spelling decodes to the real bytes.
        await save.ainvoke(
            {"server": "gen", "uri": "textblobs://good", "path": "good.png"}
        )
        assert workspace.get_path("good.png").read_bytes() == base64.b64decode(PNG)
        shown = await _as_job(read, server="gen", uri="textblobs://good")
        _text, images = extract_image_tags(shown)
        assert [image.base64_data for image in images] == [PNG]

        # Anything else is refused instead of written as a broken PNG.
        refused = await save.ainvoke(
            {"server": "gen", "uri": "textblobs://bad", "path": "bad.png"}
        )
        assert refused.startswith("Error: ")
        assert "not base64" in refused and "download it" in refused
        assert not workspace.get_path("bad.png").exists()
        described = await read.ainvoke({"server": "gen", "uri": "textblobs://bad"})
        assert "sent as text that is not base64" in described


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


# --- full-size originals (image_generation WP0.2/WP6) ------------------------
#
# Civitai answers run_step/get_workflow with image objects holding a signed
# `url` (the full-size file) and returns a resource link to a JPEG preview
# of at most 1024 px. The model only ever sees the URL redacted (its `sig` is
# a credential), so the manager remembers it and save_mcp_resource saves the
# original itself.


def _civitai_result(blob_id: str, url: str) -> SimpleNamespace:
    text = "Workflow 5065068-20261117175527611-1r13 Succeeded.\n" + json.dumps(
        [
            {
                "name": "$0",
                "status": "succeeded",
                "output": {
                    "images": [
                        {
                            "id": blob_id,
                            "width": 2048,
                            "height": 2048,
                            "previewUrl": url + "&preview=1",
                            "url": url,
                        }
                    ]
                },
            }
        ]
    )
    return SimpleNamespace(
        content=[SimpleNamespace(type="text", text=text)], structured_content=None
    )


def _png_bytes(size) -> bytes:
    from PIL import Image

    out = io.BytesIO()
    Image.new("RGB", size, (10, 160, 90)).save(out, format="PNG")
    return out.getvalue()


def _preview_result(uri: str, preview: bytes) -> SimpleNamespace:
    return SimpleNamespace(
        contents=[
            SimpleNamespace(
                uri=uri,
                mime_type="image/jpeg",
                text=None,
                blob=base64.b64encode(preview).decode(),
            )
        ]
    )


@pytest.fixture
def originals(monkeypatch, tmp_path):
    """A server handle, a workspace saver and a fake blob host."""
    import httpx

    served: dict[str, httpx.Response] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        assert "authorization" not in request.headers  # no credentials go out
        return served.get(str(request.url), httpx.Response(404))

    monkeypatch.setattr(
        manager_module, "_ORIGINAL_TRANSPORT", httpx.MockTransport(handler)
    )
    from shared import url_safety

    def check(url, **_kwargs):
        if "10.0.0.5" in url:
            raise url_safety.UnsafeURLError("10.0.0.5 is a private address")
        return url

    monkeypatch.setattr(url_safety, "check_public_http_url", check)
    workspace = _workspace(tmp_path)
    handle = SimpleNamespace(ds={"name": "Civitai"}, name="Civitai", original_urls={})

    def save_file(path, data, overwrite):
        target = workspace.get_path(path)
        if target.exists() and not overwrite:
            return f"Error: {target} already exists; nothing was written."
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        return f"Saved: {target} ({len(data):,} bytes)"

    return SimpleNamespace(
        handle=handle, served=served, workspace=workspace, save_file=save_file
    )


@pytest.mark.asyncio
async def test_the_full_size_original_is_saved_instead_of_the_preview(originals):
    import httpx
    from PIL import Image

    url = "https://orchestration-new.civitai.com/v2/consumer/blobs/b-9.png?sig=abc123def456ghi789"
    full = _png_bytes((2048, 2048))
    originals.served[url] = httpx.Response(200, content=full)
    manager_module._remember_originals(
        originals.handle, _civitai_result("b-9.png", url)
    )
    assert originals.handle.original_urls == {"b-9.png": url}

    preview = _png_bytes((1024, 1024))
    result = await manager_module._save_resource(
        originals.handle,
        SAVE_RESOURCE_TOOL,
        _preview_result("spine://blobs/b-9.png", preview),
        {"uri": "spine://blobs/b-9.png", "path": "output/fox.jpg"},
        originals.save_file,
    )
    assert "the full-size original; saved as output/fox.png, since it is PNG" in result
    saved = originals.workspace.get_path("output/fox.png").read_bytes()
    assert Image.open(io.BytesIO(saved)).size == (2048, 2048)
    assert not originals.workspace.get_path("output/fox.jpg").exists()


@pytest.mark.asyncio
async def test_a_redirect_to_a_private_address_falls_back_to_the_preview(originals):
    import httpx

    url = "https://blobs.example.com/b-1.png?sig=abc123def456ghi789"
    originals.served[url] = httpx.Response(
        302, headers={"location": "https://10.0.0.5/steal"}
    )
    originals.handle.original_urls["b-1.png"] = url
    preview = _png_bytes((64, 64))
    result = await manager_module._save_resource(
        originals.handle,
        SAVE_RESOURCE_TOOL,
        _preview_result("spine://blobs/b-1.png", preview),
        {"uri": "spine://blobs/b-1.png", "path": "output/p.png"},
        originals.save_file,
    )
    assert "(the preview: the full-size original could not be downloaded:" in result
    assert "not public" in result
    assert originals.workspace.get_path("output/p.png").read_bytes() == preview


@pytest.mark.asyncio
async def test_an_existing_file_is_not_replaced_by_the_original_or_the_preview(
    originals,
):
    import httpx

    url = "https://blobs.example.com/b-2.png?sig=abc123def456ghi789"
    originals.served[url] = httpx.Response(200, content=_png_bytes((32, 32)))
    originals.handle.original_urls["b-2.png"] = url
    originals.workspace.get_path("output").mkdir(parents=True, exist_ok=True)
    originals.workspace.get_path("output/keep.png").write_bytes(b"mine")
    result = await manager_module._save_resource(
        originals.handle,
        SAVE_RESOURCE_TOOL,
        _preview_result("spine://blobs/b-2.png", _png_bytes((16, 16))),
        {"uri": "spine://blobs/b-2.png", "path": "output/keep.png"},
        originals.save_file,
    )
    assert result.startswith("Error:") and "already exists" in result
    assert originals.workspace.get_path("output/keep.png").read_bytes() == b"mine"


def test_only_https_ids_with_urls_are_remembered_and_the_list_is_bounded(monkeypatch):
    handle = SimpleNamespace(original_urls={})
    result = SimpleNamespace(
        content=[
            SimpleNamespace(
                type="text",
                text='[resource link] x\n{"id": "a", "url": "http://plain/a"}',
            )
        ],
        structured_content={"images": [{"id": "b", "url": "https://h/b"}]},
    )
    manager_module._remember_originals(handle, result)
    assert handle.original_urls == {"b": "https://h/b"}
    monkeypatch.setattr(manager_module, "_MAX_REMEMBERED_ORIGINALS", 3)
    for n in range(5):
        manager_module._remember_originals(
            handle,
            SimpleNamespace(
                content=[], structured_content={"id": f"i{n}", "url": f"https://h/{n}"}
            ),
        )
    assert list(handle.original_urls) == ["i2", "i3", "i4"]
