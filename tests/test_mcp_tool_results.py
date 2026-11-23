"""What the model reads for an MCP tool's result, and how often the tool runs.

knowledge-base/knowledge/features/mcp_client_2026_07_28_elicitation.md,
Phase A. Before it:

- every result was wrapped in the adapter's ``(content, artifact)`` tuple,
  so a Session read ``('echo: hi', None)`` and a job ``'["echo: hi", null]'``;
- an image arrived as its base64 repr;
- a server's error text was dropped;
- a tool's own error on a remote server reconnected and ran the tool again.

The integration tests run a real MCP server as a subprocess that counts
each execution in a file, and call it the way both runtimes do: a Session's
loop calls ``tool.ainvoke(args)``, a job's ToolNode ``tool.ainvoke(tool_call)``.
"""

from __future__ import annotations

import asyncio
import sys
import textwrap
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from mcp import types

from agent.services.image_content import extract_image_tags
from agent.tools.mcp import manager as manager_module
from agent.tools.mcp.manager import MCPManager, _render_result, _ServerHandle
from tests.test_mcp_manager import _free_port, _wait_for_port

# A 1x1 PNG.
PNG = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQ"
    "DwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
)

SERVER = textwrap.dedent(
    f"""
    import sys
    from mcp.server.mcpserver import MCPServer
    from mcp.server.mcpserver.exceptions import ToolError
    from mcp.types import ImageContent, ResourceLink

    port, ledger = int(sys.argv[1]), sys.argv[2]
    mcp = MCPServer("gen", log_level="ERROR")

    def executed(name):
        with open(ledger, "a") as f:
            f.write(name + "\\n")

    @mcp.tool()
    def echo(text: str) -> str:
        \"\"\"Echo the input back.\"\"\"
        executed("echo")
        return f"echo: {{text}}"

    @mcp.tool()
    def charge(amount: int) -> str:
        \"\"\"Spend credit, and fail like a paid API does.\"\"\"
        executed("charge")
        # A failure the tool anticipated: the SDK passes its words on.
        raise ToolError(f"insufficient credit for {{amount}}")

    @mcp.tool()
    def picture() -> ImageContent:
        \"\"\"An inline image.\"\"\"
        executed("picture")
        return ImageContent(type="image", data="{PNG}", mimeType="image/png")

    @mcp.tool()
    def workflow() -> dict:
        \"\"\"A JSON answer with a resource link, as a generation API gives.\"\"\"
        executed("workflow")
        return {{"workflowId": "wf-1", "status": "submitted"}}

    mcp.run(transport="streamable-http", host="127.0.0.1", port=port)
    """
)


@asynccontextmanager
async def _server(tmp_path):
    script = tmp_path / "gen_server.py"
    script.write_text(SERVER)
    ledger = tmp_path / "executions.txt"
    ledger.write_text("")
    port = _free_port()
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script),
        str(port),
        str(ledger),
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
        yield manager, ledger
    finally:
        if manager is not None:
            await manager.aclose()
        if process.returncode is None:
            process.terminate()
        await process.wait()


def _tool(tools, suffix: str):
    return next(tool for tool in tools if tool.name.endswith(suffix))


def _executions(ledger, name: str) -> int:
    return ledger.read_text().split().count(name)


async def _as_job(tool, **arguments) -> str:
    """The content a job's ToolNode puts in the ToolMessage."""
    message = await tool.ainvoke(
        {"type": "tool_call", "id": "call-1", "name": tool.name, "args": arguments}
    )
    return message.content


@pytest.mark.asyncio
async def test_a_text_result_is_plain_text_in_a_session_and_in_a_job(tmp_path):
    async with _server(tmp_path) as (manager, _ledger):
        echo = _tool(manager.get_langchain_tools(), "__echo")
        assert await echo.ainvoke({"text": "hi"}) == "echo: hi"
        assert await _as_job(echo, text="hi") == "echo: hi"


@pytest.mark.asyncio
async def test_a_current_server_is_spoken_to_in_2026_07_28(tmp_path):
    async with _server(tmp_path) as (manager, _ledger):
        assert manager._handles[0].session.protocol_version == "2026-07-28"


@pytest.mark.asyncio
async def test_a_json_result_reads_as_json_once(tmp_path):
    async with _server(tmp_path) as (manager, _ledger):
        workflow = _tool(manager.get_langchain_tools(), "__workflow")
        answer = await workflow.ainvoke({})
        # FastMCP sends the JSON as text and as structuredContent; the
        # model reads it once.
        assert answer.count("wf-1") == 1, answer
        assert '"workflowId"' in answer


@pytest.mark.asyncio
async def test_a_failed_call_runs_once_and_the_model_reads_the_servers_words(
    tmp_path,
):
    async with _server(tmp_path) as (manager, ledger):
        tools = manager.get_langchain_tools()
        charge = _tool(tools, "__charge")

        first = await charge.ainvoke({"amount": 12})
        assert _executions(ledger, "charge") == 1
        assert first.startswith("Error: MCP tool Gen/charge failed:"), first
        assert "insufficient credit for 12" in first
        # The server is fine; nothing tells the model to give up on it.
        assert "Continue without" not in first
        assert manager.statuses == {"Gen": "connected"}

        second = await _as_job(charge, amount=5)
        assert _executions(ledger, "charge") == 2
        assert "insufficient credit for 5" in second

        # The session the errors came over still serves.
        assert await _tool(tools, "__echo").ainvoke({"text": "ok"}) == "echo: ok"
        assert manager._handles[0].reconnected_once is False


@pytest.mark.asyncio
async def test_an_image_reaches_only_a_model_that_sees_images(tmp_path):
    async with _server(tmp_path) as (manager, _ledger):
        seeing = _tool(
            manager.get_langchain_tools(sees_images=lambda: True), "__picture"
        )
        answer = await seeing.ainvoke({})
        cleaned, images = extract_image_tags(answer)
        assert [image.mime_type for image in images] == ["image/png"]
        assert images[0].base64_data == PNG
        assert PNG not in cleaned

        for tools in (
            manager.get_langchain_tools(sees_images=lambda: False),
            manager.get_langchain_tools(),  # no check: assume it cannot
        ):
            described = await _tool(tools, "__picture").ainvoke({})
            assert PNG not in described
            assert "image/png" in described and "cannot see images" in described
            assert extract_image_tags(described)[1] == []


@pytest.mark.asyncio
async def test_a_json_rpc_error_from_a_remote_server_is_its_answer_not_a_dead_session():
    from mcp.shared.exceptions import MCPError

    manager = MCPManager(
        [{"type": "mcp", "name": "R", "connection_url": "https://r.example/mcp"}]
    )
    handle = manager._handles[0]
    handle.status = "connected"
    handle.task = asyncio.create_task(asyncio.sleep(3600))
    sent: list[str] = []

    class Session:
        @staticmethod
        async def send_request(request, _result_type):
            sent.append(request.params.name)
            raise MCPError(code=-32602, message="amount must be positive")

    handle.session = SimpleNamespace(session=Session())
    restarts: list[bool] = []

    async def restart(_handle, *, force=False):
        restarts.append(force)
        return False

    manager._restart_server = restart
    try:
        answer = await manager._guarded(handle, "charge")(amount=-1)
        assert answer == (
            "Error: MCP tool R/charge failed: amount must be positive."
        ), answer
        assert sent == ["charge"]
        assert restarts == []
        assert handle.status == "connected"
    finally:
        handle.task.cancel()


@pytest.mark.asyncio
async def test_a_dead_session_still_reconnects_and_retries(monkeypatch):
    """The one retry left: the session died, so the call never got an answer."""
    manager = MCPManager(
        [{"type": "mcp", "name": "R", "connection_url": "https://r.example/mcp"}]
    )
    handle = manager._handles[0]
    handle.status = "connected"
    handle.task = asyncio.create_task(asyncio.sleep(3600))
    calls: list[str] = []

    class DeadSession:
        @staticmethod
        async def send_request(request, _result_type):
            calls.append("dead")
            raise ConnectionError("gone")

    class FreshSession:
        @staticmethod
        async def send_request(request, _result_type):
            calls.append("fresh")
            return types.CallToolResult(
                content=[types.TextContent(type="text", text="done")]
            )

    handle.session = SimpleNamespace(session=DeadSession())

    async def restart(_handle, *, force=False):
        handle.session = SimpleNamespace(session=FreshSession())
        handle.status = "connected"
        return True

    manager._restart_server = restart
    try:
        assert await manager._guarded(handle, "go")() == "done"
        assert calls == ["dead", "fresh"]
    finally:
        handle.task.cancel()


# -- the converter, block by block ------------------------------------------


def _handle() -> _ServerHandle:
    return _ServerHandle(
        ds={"name": "Gen"}, config=None, slug="gen", status="connected"
    )


def _render(*content, sees_images=True, **result) -> str:
    return _render_result(
        _handle(),
        "run",
        types.CallToolResult(content=list(content), **result),
        sees_images,
    )


def test_text_blocks_are_joined():
    assert (
        _render(
            types.TextContent(type="text", text="one"),
            types.TextContent(type="text", text="two"),
        )
        == "one\ntwo"
    )


def test_structured_content_shows_only_without_content_blocks():
    assert _render(structuredContent={"id": "wf-1"}) == '{"id": "wf-1"}'
    image = types.ImageContent(type="image", data=PNG, mimeType="image/png")
    shown = _render(image, structuredContent={"type": "image", "data": PNG})
    assert shown.count(PNG) == 1


def test_an_image_the_model_cannot_view_is_described():
    svg = types.ImageContent(type="image", data=PNG, mimeType="image/svg+xml")
    assert "not an image type the model can view" in _render(svg)
    broken = types.ImageContent(type="image", data="not base64!", mimeType="image/png")
    assert "malformed" in _render(broken)


def test_an_oversized_image_is_described(monkeypatch):
    monkeypatch.setattr(manager_module, "MCP_INLINE_IMAGE_LIMIT", 10)
    image = types.ImageContent(type="image", data=PNG, mimeType="image/png")
    rendered = _render(image)
    assert "too large" in rendered and PNG not in rendered


def test_audio_is_described():
    audio = types.AudioContent(type="audio", data="AAAA", mimeType="audio/wav")
    assert (
        _render(audio)
        == "[audio: audio/wav, 3 bytes, not shown: the model cannot hear audio]"
    )


def test_a_resource_link_names_its_uri():
    link = types.ResourceLink(
        type="resource_link",
        uri="spine://blobs/abc",
        name="result.png",
        mimeType="image/png",
        description="the generated image",
    )
    assert _render(link) == (
        "[resource link] result.png: spine://blobs/abc (image/png) - the generated image"
    )


def test_an_embedded_resource_shows_its_text_or_its_image():
    text = types.EmbeddedResource(
        type="resource",
        resource=types.TextResourceContents(
            uri="file:///notes.md", mimeType="text/markdown", text="# Notes"
        ),
    )
    assert _render(text) == "[resource file:///notes.md]\n# Notes"
    image = types.EmbeddedResource(
        type="resource",
        resource=types.BlobResourceContents(
            uri="spine://blobs/abc", mimeType="image/png", blob=PNG
        ),
    )
    assert extract_image_tags(_render(image))[1][0].base64_data == PNG
    other = types.EmbeddedResource(
        type="resource",
        resource=types.BlobResourceContents(
            uri="spine://blobs/zip", mimeType="application/zip", blob="AAAA"
        ),
    )
    assert (
        _render(other)
        == "[resource spine://blobs/zip: application/zip, 3 bytes, not shown]"
    )


def test_an_error_result_is_reported_in_the_servers_words():
    failed = _render(
        types.TextContent(type="text", text="insufficient credit"), isError=True
    )
    assert failed == "Error: MCP tool Gen/run failed: insufficient credit."
    silent = _render(isError=True)
    assert silent == (
        "Error: MCP tool Gen/run failed: the server reported an error without a message."
    )


def test_an_error_result_never_carries_an_image():
    image = types.ImageContent(type="image", data=PNG, mimeType="image/png")
    failed = _render(image, isError=True)
    assert failed.startswith("Error:") and PNG not in failed


def test_a_long_error_is_cut():
    failed = _render(types.TextContent(type="text", text="x" * 10_000), isError=True)
    assert len(failed) < manager_module.MCP_ERROR_TEXT_LIMIT + 100
    assert failed.endswith("[…]")


def test_an_empty_result_says_so():
    assert _render() == "(the tool returned no content)"


# -- 2026-07-28 answers that are not results yet ------------------------------


class _Session:
    """A client session that answers ``tools/call`` from a script."""

    def __init__(self, *answers, elicit=None):
        self.answers = list(answers)
        self.sent: list = []
        self.asked: list = []
        self.elicit = elicit

    async def send_request(self, request, _result_type):
        self.sent.append(request.params)
        answer = self.answers.pop(0)
        if isinstance(answer, BaseException):
            raise answer
        return answer

    async def dispatch_input_request(self, context, request):
        self.asked.append((context.request_id, request.params.message))
        return self.elicit


class _Client:
    def __init__(self, session):
        self.session = session
        self.listed: list[str] = []

    async def list_tools(self, cursor=None, cache_mode="use"):
        self.listed.append(cache_mode)
        return types.ListToolsResult(tools=[])


def _done(text="done"):
    return types.CallToolResult(content=[types.TextContent(type="text", text=text)])


def _asks(state="s1"):
    question = types.ElicitRequest(
        params=types.ElicitRequestFormParams(
            message="Spend 12 Buzz?",
            requested_schema={"type": "object", "properties": {}},
        )
    )
    return types.InputRequiredResult(
        input_requests={"q1": question}, request_state=state
    )


@pytest.mark.asyncio
async def test_a_stale_tool_copy_is_listed_again_and_the_call_resent_once():
    from mcp.shared.exceptions import MCPError

    mismatch = MCPError(code=types.HEADER_MISMATCH, message="schema changed")
    client = _Client(_Session(mismatch, _done()))
    result = await manager_module._send_tool_call(client, "go", {"a": 1})
    assert result.content[0].text == "done"
    assert client.listed == ["refresh"]
    assert len(client.session.sent) == 2

    # A second refusal is the server's answer.
    client = _Client(_Session(mismatch, mismatch))
    with pytest.raises(MCPError):
        await manager_module._send_tool_call(client, "go", {})
    assert len(client.session.sent) == 2


@pytest.mark.asyncio
async def test_an_answered_question_goes_back_with_the_servers_state():
    accepted = types.ElicitResult(action="accept", content={})
    session = _Session(_asks("s1"), _done("charged"), elicit=accepted)
    result = await manager_module._send_tool_call(_Client(session), "charge", {})
    assert result.content[0].text == "charged"
    assert session.asked == [("q1", "Spend 12 Buzz?")]
    first, second = session.sent
    assert first.request_state is None and first.input_responses is None
    assert second.request_state == "s1"
    assert second.input_responses == {"q1": accepted}


@pytest.mark.asyncio
async def test_state_only_rounds_are_resent_until_the_result():
    pending = types.InputRequiredResult(request_state="s2")
    session = _Session(pending, _done())
    assert (await manager_module._send_tool_call(_Client(session), "go", {})).content
    assert [params.request_state for params in session.sent] == [None, "s2"]


@pytest.mark.asyncio
async def test_a_question_the_client_cannot_answer_ends_the_call_unrepeated():
    """Without an elicitation callback the SDK declines; the server has
    answered, so the call is reported, never re-sent after a reconnect."""
    manager = MCPManager(
        [{"type": "mcp", "name": "R", "connection_url": "https://r.example/mcp"}]
    )
    handle = manager._handles[0]
    handle.status = "connected"
    handle.task = asyncio.create_task(asyncio.sleep(3600))
    refused = types.ErrorData(code=-32600, message="Elicitation not supported")
    session = _Session(_asks(), elicit=refused)
    handle.session = _Client(session)
    restarts: list[bool] = []

    async def restart(_handle, *, force=False):
        restarts.append(force)
        return False

    manager._restart_server = restart
    try:
        answer = await manager._guarded(handle, "charge")()
        assert answer.startswith("Error: MCP tool R/charge failed:"), answer
        assert "Elicitation not supported" in answer
        assert len(session.sent) == 1
        assert restarts == []
        assert handle.status == "connected"
    finally:
        handle.task.cancel()
