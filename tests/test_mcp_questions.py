"""Questions an MCP server asks mid-call (elicitation, Phase C).

knowledge-base/knowledge/features/mcp_client_2026_07_28_elicitation.md, owner
decisions of 2026-11-17: each connector's ``server_questions`` setting is
``decline`` (the default) or ``accept``; ``accept`` answers a yes/no by
itself; a runtime that can ask a person (an interactive Session) does so
through its handler; without one, the question is declined and the model
reads why. A person's answering time does not count against the call's
timeout.

The integration tests run a real mcp 2.x server that asks the way a paid API
does ("Spend 12 Buzz?") in both protocol eras: a 2026-07-28 connection asks
through an "input required" answer, and a handshake-era one (the server
refuses ``server/discover``, as SRW's managed front does) asks on the call's
open stream.
"""

from __future__ import annotations

import asyncio
import sys
import textwrap
from contextlib import asynccontextmanager

import pytest

from agent.tools.mcp import manager as manager_module
from agent.tools.mcp.manager import (
    RUNNING_TOOL_CALL,
    MCPManager,
    QuestionAnswer,
    ServerQuestion,
)
from shared.mcp_questions import (
    ACCEPT,
    DECLINE,
    confirmation_content,
    content_problem,
    form_schema_problem,
    resolve_setting,
)
from tests.test_mcp_manager import _free_port, _wait_for_port

SERVER = textwrap.dedent(
    """
    import json, sys
    from pydantic import BaseModel
    from mcp import types
    from mcp.server.mcpserver import Context, MCPServer

    port, ledger, legacy = int(sys.argv[1]), sys.argv[2], sys.argv[3] == "legacy"
    mcp = MCPServer("paid", log_level="ERROR")

    def executed(name):
        with open(ledger, "a") as f:
            f.write(name + "\\n")

    class Confirm(BaseModel):
        value: bool

    class Pick(BaseModel):
        size: str
        note: str

    CONFIRM = {"type": "object", "properties": {"value": {"type": "boolean"}},
               "required": ["value"]}
    PICK = {"type": "object", "required": ["size", "note"], "properties": {
        "size": {"type": "string", "enum": ["small", "large"]},
        "note": {"type": "string", "maxLength": 20}}}

    async def ask(ctx, message, model, schema):
        # 2026-07-28: the answer comes back with the retried call.
        if ctx.input_responses:
            answer = ctx.input_responses["q"]
            return answer.action, answer.content
        try:
            result = await ctx.elicit(message, model)
        except Exception as exc:
            if type(exc).__name__ != "NoBackChannelError":
                raise
            return None, types.InputRequiredResult(
                input_requests={"q": types.ElicitRequest(
                    params=types.ElicitRequestFormParams(
                        message=message, requested_schema=schema))},
                request_state="asked",
            )
        data = result.data.model_dump() if result.action == "accept" else None
        return result.action, data

    @mcp.tool()
    async def spend(amount: int, ctx: Context) -> object:
        \"\"\"Spend credit, after asking.\"\"\"
        action, content = await ask(
            ctx, f"Spend {amount} Buzz to run imageGen?", Confirm, CONFIRM)
        if action is None:
            return content
        if action == "accept" and content == {"value": True}:
            executed("spend")
            return f"spent {amount}"
        return f"not spent: {action}"

    @mcp.tool()
    async def pick(ctx: Context) -> object:
        \"\"\"Ask for a size and a note.\"\"\"
        action, content = await ask(ctx, "Which size?", Pick, PICK)
        if action is None:
            return content
        executed("pick")
        return f"pick: {action} {json.dumps(content, sort_keys=True)}"

    @mcp.tool()
    async def sign_in(ctx: Context) -> object:
        \"\"\"Ask the user to sign in on the server's own page.\"\"\"
        if ctx.input_responses:
            answer = ctx.input_responses["q"]
            return f"sign_in: {answer.action} {answer.content}"
        return types.InputRequiredResult(
            input_requests={"q": types.ElicitRequest(
                params=types.ElicitRequestURLParams(
                    message="Sign in to Civitai",
                    url="https://civitai.example/login",
                    elicitation_id="login-1"))},
            request_state="asked",
        )

    app = mcp.streamable_http_app()

    async def front(scope, receive, send):
        # A handshake-era server: refuse the 2026-07-28 probe, as SRW's
        # managed front does, so the client falls back to initialize.
        if legacy and scope["type"] == "http" and scope["method"] == "POST":
            if dict(scope["headers"]).get(b"mcp-method") == b"server/discover":
                body = b""
                while True:
                    message = await receive()
                    body += message.get("body", b"")
                    if not message.get("more_body"):
                        break
                request = json.loads(body)
                answer = json.dumps({"jsonrpc": "2.0", "id": request["id"],
                    "error": {"code": -32601, "message": "Method not found"}}).encode()
                await send({"type": "http.response.start", "status": 200,
                    "headers": [(b"content-type", b"application/json")]})
                await send({"type": "http.response.body", "body": answer})
                return
        await app(scope, receive, send)

    import uvicorn
    uvicorn.run(front, host="127.0.0.1", port=port, log_level="error")
    """
)

ERAS = ("modern", "legacy")
PROTOCOL = {"modern": "2026-07-28", "legacy": "2025-11-25"}


@asynccontextmanager
async def _server(tmp_path, era: str, *, setting: str | None = None, questions=None):
    script = tmp_path / "paid_server.py"
    script.write_text(SERVER)
    ledger = tmp_path / "executions.txt"
    ledger.write_text("")
    port = _free_port()
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script),
        str(port),
        str(ledger),
        era,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    entry = {
        "type": "mcp",
        "name": "Civitai",
        "connection_url": f"http://127.0.0.1:{port}/mcp",
        "credentials": {"transport": "http"},
    }
    if setting is not None:
        entry["server_questions"] = setting
    manager = None
    try:
        await _wait_for_port(port)
        manager = MCPManager([entry], questions=questions)
        await manager.connect_all()
        handle = manager._handles[0]
        assert handle.status == "connected"
        assert handle.session.protocol_version == PROTOCOL[era]
        yield manager, ledger
    finally:
        if manager is not None:
            await manager.aclose()
        if process.returncode is None:
            process.terminate()
        await process.wait()


def _tool(manager, suffix: str):
    return next(t for t in manager.get_langchain_tools() if t.name.endswith(suffix))


def _executions(ledger, name: str) -> int:
    return ledger.read_text().split().count(name)


ASKED = "[server question] Civitai asked “Spend 12 Buzz to run imageGen?”"


@pytest.mark.asyncio
@pytest.mark.parametrize("era", ERAS)
async def test_a_job_declines_by_default_and_the_model_reads_why(tmp_path, era):
    async with _server(tmp_path, era) as (manager, ledger):
        result = await _tool(manager, "__spend").ainvoke({"amount": 12})
        assert result == (
            f"{ASKED}. SRW declined it: this connector's setting for server "
            "questions is decline. The user can change it to accept in the "
            "connector's settings.\nnot spent: decline"
        )
        assert _executions(ledger, "spend") == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("era", ERAS)
async def test_accept_answers_a_yes_no_by_itself(tmp_path, era):
    async with _server(tmp_path, era, setting=ACCEPT) as (manager, ledger):
        result = await _tool(manager, "__spend").ainvoke({"amount": 12})
        assert result == (
            f"{ASKED}. SRW accepted it: this connector's setting for "
            "server questions is accept.\nspent 12"
        )
        assert _executions(ledger, "spend") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("era", ERAS)
async def test_accept_never_fills_in_typed_input(tmp_path, era):
    async with _server(tmp_path, era, setting=ACCEPT) as (manager, ledger):
        result = await _tool(manager, "__pick").ainvoke({})
        assert result.startswith(
            "[server question] Civitai asked “Which size?”. SRW declined "
            "it: it needs typed input, which only a person can give.\n"
        )
        assert result.endswith("pick: decline null")


@pytest.mark.asyncio
@pytest.mark.parametrize("era", ERAS)
async def test_a_person_answers_through_the_runtimes_handler(tmp_path, era):
    asked: list[ServerQuestion] = []

    async def questions(question: ServerQuestion) -> QuestionAnswer:
        asked.append(question)
        return QuestionAnswer("accept", {"size": "large", "note": "for the cover"})

    async with _server(tmp_path, era, questions=questions) as (manager, ledger):
        tool = _tool(manager, "__pick")
        token = RUNNING_TOOL_CALL.set((tool.name, "call-7"))
        try:
            result = await tool.ainvoke({})
        finally:
            RUNNING_TOOL_CALL.reset(token)
        assert result == (
            "[server question] Civitai asked “Which size?”. The user "
            'accepted it.\npick: accept {"note": "for the cover", "size": "large"}'
        )
        (question,) = asked
        assert (question.server, question.server_slug) == ("Civitai", "civitai")
        assert (question.tool, question.tool_call_id) == (
            "mcp__civitai__pick",
            "call-7",
        )
        assert (question.mode, question.message) == ("form", "Which size?")
        assert question.requested_schema["required"] == ["size", "note"]
        assert question.confirmation is None
        assert question.setting == DECLINE


@pytest.mark.asyncio
@pytest.mark.parametrize("era", ERAS)
async def test_a_slow_answer_does_not_time_the_call_out(tmp_path, era, monkeypatch):
    monkeypatch.setattr(manager_module, "MCP_CALL_TIMEOUT", 1.0)

    async def questions(question: ServerQuestion) -> QuestionAnswer:
        await asyncio.sleep(2.5)
        return QuestionAnswer("accept", dict(question.confirmation))

    async with _server(tmp_path, era, questions=questions) as (manager, ledger):
        result = await _tool(manager, "__spend").ainvoke({"amount": 12})
        assert result == f"{ASKED}. The user accepted it.\nspent 12"
        assert _executions(ledger, "spend") == 1


@pytest.mark.asyncio
async def test_always_accept_in_this_session_answers_the_next_one_itself(tmp_path):
    calls = 0

    async def questions(question: ServerQuestion) -> QuestionAnswer:
        nonlocal calls
        calls += 1
        return QuestionAnswer("accept", dict(question.confirmation), always_accept=True)

    async with _server(tmp_path, "modern", questions=questions) as (manager, ledger):
        spend = _tool(manager, "__spend")
        assert (await spend.ainvoke({"amount": 12})).endswith("spent 12")
        second = await spend.ainvoke({"amount": 12})
        assert second == (
            f"{ASKED}. SRW accepted it: the user chose to always accept this "
            "server's yes/no questions in this session.\nspent 12"
        )
        assert (calls, _executions(ledger, "spend")) == (1, 2)


@pytest.mark.asyncio
async def test_nobody_to_ask_declines_and_an_unfitting_answer_sends_none(tmp_path):
    answers = [None, QuestionAnswer("accept", {"size": "huge", "note": "x"})]

    async def questions(question: ServerQuestion) -> QuestionAnswer | None:
        return answers.pop(0)

    async with _server(tmp_path, "modern", questions=questions) as (manager, ledger):
        # An autonomous Session's handler has nobody to ask: the setting holds.
        declined = await _tool(manager, "__spend").ainvoke({"amount": 12})
        assert declined.endswith(
            "server questions is decline. The user can "
            "change it to accept in the connector's settings."
            "\nnot spent: decline"
        )
        misfit = await _tool(manager, "__pick").ainvoke({})
        assert misfit.startswith(
            "[server question] Civitai asked “Which size?”. The answer "
            "did not fit the server's form ('size' is not one of the choices), "
            "so SRW sent none.\n"
        )
        assert misfit.endswith("pick: cancel null")


@pytest.mark.asyncio
async def test_a_link_only_a_person_can_open_is_declined_in_a_job(tmp_path):
    async with _server(tmp_path, "modern", setting=ACCEPT) as (manager, ledger):
        result = await _tool(manager, "__sign_in").ainvoke({})
        assert result == (
            "[server question] Civitai asked \u201cSign in to Civitai\u201d. SRW "
            "declined it: it asks to open a link, which only a person can do."
            "\nsign_in: decline None"
        )


@pytest.mark.asyncio
async def test_a_person_is_shown_the_link_and_accepting_sends_no_content(tmp_path):
    asked: list[ServerQuestion] = []

    async def questions(question: ServerQuestion) -> QuestionAnswer:
        asked.append(question)
        return QuestionAnswer("accept", {"ignored": True})

    async with _server(tmp_path, "modern", questions=questions) as (manager, ledger):
        result = await _tool(manager, "__sign_in").ainvoke({})
        assert result.endswith("The user accepted it.\nsign_in: accept None")
        (question,) = asked
        assert (question.mode, question.url) == ("url", "https://civitai.example/login")
        assert (question.requested_schema, question.confirmation) == (None, None)


# -- the rules ---------------------------------------------------------------


def test_a_setting_resolves_session_then_connector_then_default():
    assert resolve_setting(None, None) == DECLINE
    assert resolve_setting(None, ACCEPT) == ACCEPT
    assert resolve_setting(DECLINE, ACCEPT) == DECLINE
    assert resolve_setting(DECLINE, DECLINE, ACCEPT) == ACCEPT
    assert resolve_setting("maybe", "sometimes") == DECLINE


@pytest.mark.parametrize(
    ("schema", "content"),
    [
        ({"type": "object", "properties": {}}, {}),
        (
            {"type": "object", "properties": {"value": {"type": "boolean"}}},
            {"value": True},
        ),
        (
            {
                "type": "object",
                "properties": {
                    "quality": {"type": "string", "enum": ["a", "b"], "default": "b"},
                    "count": {"type": "integer", "default": 1},
                },
            },
            {"quality": "b", "count": 1},
        ),
        (
            {
                "type": "object",
                "properties": {"size": {"type": "string", "enum": ["s", "l"]}},
            },
            None,
        ),
        (
            {
                "type": "object",
                "properties": {"a": {"type": "boolean"}, "b": {"type": "boolean"}},
            },
            None,
        ),
    ],
)
def test_a_confirmation_is_a_form_nobody_needs_to_type_into(schema, content):
    assert confirmation_content(schema) == content


@pytest.mark.parametrize(
    "schema",
    [
        None,
        {"type": "array"},
        {"type": "object", "properties": {"x": {"type": "object"}}},
        {"type": "object", "properties": {"x": {"type": "string", "format": "ipv4"}}},
        {"type": "object", "properties": {}, "required": ["missing"]},
        {"type": "object", "properties": {"x": {"type": "array", "items": {}}}},
    ],
)
def test_a_form_outside_the_flat_rules_is_refused(schema):
    assert form_schema_problem(schema) is not None


FORM = {
    "type": "object",
    "required": ["size"],
    "properties": {
        "size": {"type": "string", "oneOf": [{"const": "s"}, {"const": "l"}]},
        "email": {"type": "string", "format": "email"},
        "count": {"type": "integer", "minimum": 1, "maximum": 4},
        "tags": {
            "type": "array",
            "items": {"type": "string", "enum": ["x", "y"]},
            "maxItems": 1,
        },
    },
}


@pytest.mark.parametrize(
    ("content", "problem"),
    [
        ({"size": "s", "email": "a@b.co", "count": 2, "tags": ["x"]}, None),
        ({}, "'size' is required"),
        ({"size": "m"}, "'size' is not one of the choices"),
        ({"size": "s", "email": "nope"}, "'email' is not a valid email"),
        ({"size": "s", "count": 9}, "'count' is above 4"),
        ({"size": "s", "count": 1.5}, "'count' must be a whole number"),
        ({"size": "s", "tags": ["x", "y"]}, "'tags' allows at most 1 choices"),
        ({"size": "s", "other": 1}, "'other' is not a field of the form"),
    ],
)
def test_an_answer_must_fit_the_form(content, problem):
    assert content_problem(FORM, content) == problem
