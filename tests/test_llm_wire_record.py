"""Every model call leaves a complete row: what went out, when, and how it ended.

Before this, ``llm_requests`` held the answered request with three parameters
read off the client object (temperature, model name, max_tokens), stamped with
the time the row was written. A try that raised left no row, a reply a guard
threw away left none either, and what the SDK actually put in the body
(``reasoning_effort``, ``thinking``, ``tool_choice`` ...) was nowhere.

Pinned here:

- the HTTP clients record one wire attempt per request the SDK sends: the send
  time, the body's parameters as sent, its shape, size and SHA-256, and how it
  ended — and never a header;
- a reply carries its attempts to the archiver, which stores them as
  ``request.wire`` and takes them off the message;
- a failed call's error carries them, and a caller's own list has them even
  when the call is cancelled by a timeout;
- ``archive`` / ``archive_error`` store ``metrics.started_at`` and
  ``metrics.attempt``, and a rejected reply as a row without a chat entry;
- the execute node archives every try that raised and every reply it drops;
- ``AUDIT_FULL_TOOL_IO`` keeps a tool step's full arguments and result.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import patch

import httpx
import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from agent.core.archiver import LLMArchiver, full_tool_io_enabled
from agent.core.context import ToolRetryManager
from shared.runtime.llm import reasoning_chat
from shared.runtime.llm.reasoning_chat import (
    WIRE_ATTEMPTS_KEY,
    ReasoningChatOpenAI,
    record_wire_attempts,
    take_wire_attempts,
    wire_attempts_of,
)
from tests import test_execute_prepared_layout as layout
from tests.test_archiver_pg import FakeWriter

pytestmark = pytest.mark.filterwarnings(
    "ignore:Parameters .*reasoning_effort.* should be specified explicitly:UserWarning"
)

URL = "https://wire-fixture.invalid/v1"
API_KEY = "sk-fixture-secret-key"
TOOL = {
    "type": "function",
    "function": {
        "name": "read_note",
        "description": "Read a workspace note.",
        "parameters": {"type": "object", "properties": {}},
    },
}


def _completion(message: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": "fixture",
        "object": "chat.completion",
        "created": 0,
        "model": "fixture-model",
        "choices": [{"index": 0, "finish_reason": "stop", "message": message}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
    }


OK_MESSAGE = {"role": "assistant", "content": "Answer."}


def _llm(monkeypatch, handler, **kwargs) -> ReasoningChatOpenAI:
    """A client whose two HTTP clients answer from ``handler``."""
    sync_client = reasoning_chat.ReasoningCapturingClient
    async_client = reasoning_chat.AsyncReasoningCapturingClient
    monkeypatch.setattr(
        reasoning_chat,
        "ReasoningCapturingClient",
        lambda **kw: sync_client(**kw, transport=httpx.MockTransport(handler)),
    )
    monkeypatch.setattr(
        reasoning_chat,
        "AsyncReasoningCapturingClient",
        lambda **kw: async_client(**kw, transport=httpx.MockTransport(handler)),
    )
    kwargs.setdefault("max_retries", 0)
    return ReasoningChatOpenAI(
        model="fixture-model", api_key=API_KEY, base_url=URL, **kwargs
    )


async def _close(llm: ReasoningChatOpenAI) -> None:
    llm.http_client.close()
    await llm.http_async_client.aclose()


# ---------------------------------------------------------------------------
# The record the HTTP layer makes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_a_reply_carries_what_was_sent(monkeypatch, mode):
    bodies: List[bytes] = []

    def handler(request: httpx.Request) -> httpx.Response:
        bodies.append(request.content)
        return httpx.Response(
            200, json=_completion(OK_MESSAGE), headers={"x-request-id": "req-1"}
        )

    llm = _llm(
        monkeypatch,
        handler,
        temperature=0.0,
        top_p=0.95,
        model_kwargs={"reasoning_effort": "high"},
        extra_body={"thinking": {"type": "enabled"}},
        replay_reasoning_content=True,
    )
    try:
        bound = llm.bind_tools([TOOL], tool_choice="auto", parallel_tool_calls=False)
        history = [
            SystemMessage(content="S"),
            HumanMessage(content="Go."),
            AIMessage(
                content="",
                tool_calls=[{"name": "read_note", "args": {}, "id": "c1"}],
                additional_kwargs={"reasoning_content": "abcde"},
            ),
            HumanMessage(content="Continue."),
        ]
        before = datetime.now(timezone.utc)
        response = (
            await bound.ainvoke(history) if mode == "async" else bound.invoke(history)
        )
        after = datetime.now(timezone.utc)

        attempts = response.response_metadata[WIRE_ATTEMPTS_KEY]
        assert len(attempts) == 1
        attempt = attempts[0]

        sent_at = datetime.fromisoformat(attempt["sent_at"])
        assert sent_at.tzinfo is not None and sent_at.utcoffset().total_seconds() == 0
        assert before.replace(microsecond=0) <= sent_at <= after

        # The parameters are the body's own, the conversation and tools are not.
        params = attempt["params"]
        assert params["model"] == "fixture-model"
        assert params["reasoning_effort"] == "high"
        assert params["thinking"] == {"type": "enabled"}
        assert params["top_p"] == 0.95
        assert params["tool_choice"] == "auto"
        assert params["parallel_tool_calls"] is False
        assert not {"messages", "tools", "input"} & set(params)
        sent_body = json.loads(bodies[0])
        assert params == {
            k: v for k, v in sent_body.items() if k not in ("messages", "tools")
        }

        assert attempt["shape"] == {
            "messages": 4,
            "roles": {"system": 1, "user": 2, "assistant": 1},
            "reasoning_content": {
                "assistant_messages": 1,
                "present": 1,
                "non_empty": 1,
                "chars": 5,
            },
            "tools": 1,
        }
        assert attempt["bytes"] == len(bodies[0])
        assert attempt["sha256"] == hashlib.sha256(bodies[0]).hexdigest()
        assert attempt["host"] == "wire-fixture.invalid"
        assert attempt["path"] == "/v1/chat/completions"
        assert attempt["status"] == 200
        assert attempt["x_request_id"] == "req-1"
        assert isinstance(attempt["elapsed_ms"], int)

        # Never the key, and nothing the recorder kept for itself.
        assert API_KEY not in json.dumps(attempts)
        assert not [k for k in attempt if k.startswith("_")]
    finally:
        await _close(llm)


@pytest.mark.asyncio
async def test_each_sdk_retry_is_its_own_attempt(monkeypatch):
    calls = [0]

    def handler(request: httpx.Request) -> httpx.Response:
        calls[0] += 1
        if calls[0] == 1:
            return httpx.Response(
                503, json={"error": {"message": "busy"}}, headers={"retry-after": "0"}
            )
        return httpx.Response(200, json=_completion(OK_MESSAGE))

    llm = _llm(monkeypatch, handler, max_retries=1)
    try:
        response = await llm.ainvoke([HumanMessage(content="hi")])
        attempts = response.response_metadata[WIRE_ATTEMPTS_KEY]
        assert [a["status"] for a in attempts] == [503, 200]
        assert attempts[0]["retry_after"] == "0"
        assert attempts[0]["sha256"] == attempts[1]["sha256"]
        assert attempts[0]["sent_at"] <= attempts[1]["sent_at"]
    finally:
        await _close(llm)


@pytest.mark.asyncio
async def test_a_failed_call_keeps_its_attempts(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": {"message": "boom"}})

    llm = _llm(monkeypatch, handler)
    try:
        with record_wire_attempts() as mine:
            with pytest.raises(Exception) as raised:
                await llm.ainvoke([HumanMessage(content="hi")])
        on_error = wire_attempts_of(raised.value)
        assert [a["status"] for a in on_error] == [500]
        assert mine == on_error
        assert wire_attempts_of(ValueError("unrelated")) == []
    finally:
        await _close(llm)


@pytest.mark.asyncio
async def test_a_transport_error_is_recorded(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("name resolution failed", request=request)

    llm = _llm(monkeypatch, handler)
    try:
        with record_wire_attempts() as mine:
            with pytest.raises(Exception):
                await llm.ainvoke([HumanMessage(content="hi")])
        assert len(mine) == 1
        assert mine[0]["error"] == "ConnectError"
        assert "name resolution failed" in mine[0]["error_message"]
        assert "status" not in mine[0]
    finally:
        await _close(llm)


@pytest.mark.asyncio
async def test_a_timed_out_call_is_in_the_callers_list(monkeypatch):
    """``wait_for`` cancels the call and raises its own TimeoutError: only the
    caller's list still knows a request went out."""

    async def handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(30)
        return httpx.Response(200, json=_completion(OK_MESSAGE))

    llm = _llm(monkeypatch, handler)
    try:
        with record_wire_attempts() as mine:
            with pytest.raises(asyncio.TimeoutError) as raised:
                await asyncio.wait_for(
                    llm.ainvoke([HumanMessage(content="hi")]), timeout=0.05
                )
        assert wire_attempts_of(raised.value) == []
        assert len(mine) == 1
        assert mine[0]["error"] == "CancelledError"
        assert mine[0]["params"]["model"] == "fixture-model"
    finally:
        await _close(llm)


@pytest.mark.asyncio
async def test_a_request_refused_before_the_network_says_so(monkeypatch):
    sent = [0]

    def handler(request: httpx.Request) -> httpx.Response:
        sent[0] += 1
        return httpx.Response(200, json=_completion(OK_MESSAGE))

    llm = _llm(monkeypatch, handler, max_context_tokens=5)
    try:
        with record_wire_attempts() as mine:
            with pytest.raises(Exception):
                await llm.ainvoke([HumanMessage(content="far too long " * 50)])
        assert sent[0] == 0
        assert [a.get("not_sent") for a in mine] == ["context_overflow"]
        assert mine[0]["status"] == 413
    finally:
        await _close(llm)


@pytest.mark.asyncio
async def test_a_streamed_reply_carries_its_attempt(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        def chunk(delta: Dict[str, Any], finish=None) -> bytes:
            data = {
                "id": "s",
                "object": "chat.completion.chunk",
                "created": 0,
                "model": "fixture-model",
                "choices": [{"index": 0, "delta": delta, "finish_reason": finish}],
            }
            return f"data: {json.dumps(data)}\n\n".encode()

        stream = (
            chunk({"role": "assistant", "content": "Ans"})
            + chunk({"content": "wer."})
            + chunk({}, "stop")
            + b"data: [DONE]\n\n"
        )
        return httpx.Response(
            200, content=stream, headers={"content-type": "text/event-stream"}
        )

    llm = _llm(monkeypatch, handler)
    try:
        merged = None
        async for part in llm.astream([HumanMessage(content="hi")]):
            merged = part if merged is None else merged + part
        assert merged.content == "Answer."
        attempts = merged.response_metadata[WIRE_ATTEMPTS_KEY]
        assert len(attempts) == 1
        assert attempts[0]["params"]["stream"] is True
        assert attempts[0]["status"] == 200
    finally:
        await _close(llm)


def test_no_listener_no_record(monkeypatch):
    """A request made straight on the HTTP client records nothing."""
    client = reasoning_chat.ReasoningCapturingClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=_completion(OK_MESSAGE))
        )
    )
    try:
        request = client.build_request(
            "POST", f"{URL}/chat/completions", json={"model": "m", "messages": []}
        )
        assert reasoning_chat._begin_wire_attempt(request) is None
        assert client.send(request).status_code == 200
    finally:
        client.close()


def test_take_wire_attempts_removes_them():
    message = AIMessage(
        content="x",
        response_metadata={"model_name": "m", WIRE_ATTEMPTS_KEY: [{"status": 200}]},
    )
    assert take_wire_attempts(message) == [{"status": 200}]
    assert message.response_metadata == {"model_name": "m"}
    assert take_wire_attempts(message) == []
    assert take_wire_attempts(object()) == []


# ---------------------------------------------------------------------------
# What the archiver stores
# ---------------------------------------------------------------------------


@pytest.fixture
def fw():
    return FakeWriter()


@pytest.fixture
def archiver(fw):
    return LLMArchiver(writer=fw)


WIRE = [
    {
        "sent_at": "2026-11-14T10:00:00.250+00:00",
        "params": {"model": "m", "reasoning_effort": "high"},
        "status": 200,
        "_t0": 1.0,
    }
]


def _reply(**metadata) -> AIMessage:
    return AIMessage(content="hi", response_metadata={"model_name": "m", **metadata})


class TestArchive:
    def test_wire_moves_from_the_reply_to_the_row(self, archiver, fw):
        response = _reply(**{WIRE_ATTEMPTS_KEY: list(WIRE)})
        archiver.archive("job", "universal", [HumanMessage("q")], response, "m")

        row = fw.llm_rows[0]
        assert row["request"]["wire"] == [
            {k: v for k, v in WIRE[0].items() if k != "_t0"}
        ]
        assert WIRE_ATTEMPTS_KEY not in row["response"]["response_metadata"]
        assert WIRE_ATTEMPTS_KEY not in response.response_metadata
        # Without a caller's start time, the first send time is the start.
        assert row["metrics"]["started_at"] == WIRE[0]["sent_at"]
        assert "attempt" not in row["metrics"]

    def test_a_replayed_message_never_carries_it(self, archiver, fw):
        """A reply whose wire was not taken (audit down at the time) must not
        put it into every later request that replays the message."""
        earlier = _reply(**{WIRE_ATTEMPTS_KEY: list(WIRE)})
        archiver.archive("job", "universal", [earlier], _reply(), "m")
        stored = fw.llm_rows[0]["request"]["messages"][0]
        assert stored["response_metadata"] == {"model_name": "m"}
        assert "wire" not in fw.llm_rows[0]["request"]

    def test_wire_is_taken_even_when_the_store_is_down(self):
        response = _reply(**{WIRE_ATTEMPTS_KEY: list(WIRE)})
        down = LLMArchiver(writer=FakeWriter(ready=False))
        assert down.archive("job", "universal", [], response, "m") is None
        assert WIRE_ATTEMPTS_KEY not in response.response_metadata

    def test_started_at_and_attempt(self, archiver, fw):
        started = datetime(2026, 11, 14, 9, 59, 58, 120000, tzinfo=timezone.utc)
        archiver.archive(
            "job",
            "universal",
            [HumanMessage("q")],
            _reply(**{WIRE_ATTEMPTS_KEY: list(WIRE)}),
            "m",
            started_at=started,
            attempt=2,
        )
        metrics = fw.llm_rows[0]["metrics"]
        assert metrics["started_at"] == "2026-11-14T09:59:58.120+00:00"
        assert metrics["attempt"] == 2
        # The row's own timestamp stays the write time.
        assert fw.llm_rows[0]["timestamp"] > started

    def test_a_call_without_wire_or_start_has_neither(self, archiver, fw):
        archiver.archive("job", "universal", [HumanMessage("q")], _reply(), "m")
        row = fw.llm_rows[0]
        assert "wire" not in row["request"]
        assert "started_at" not in row["metrics"]

    def test_a_rejected_reply_is_a_row_without_a_chat_entry(self, archiver, fw):
        archiver.archive(
            "job",
            "universal",
            [HumanMessage("q")],
            _reply(),
            "m",
            metadata={"project_id": "p"},
            rejected="response_degeneration",
        )
        row = fw.llm_rows[0]
        assert row["metadata"]["status"] == "rejected"
        assert row["metadata"]["rejected"] == {"reason": "response_degeneration"}
        assert row["metadata"]["project_id"] == "p"
        assert row["response"]["content"] == "hi"
        assert fw.chat_rows == []

    def test_an_accepted_reply_keeps_its_chat_entry(self, archiver, fw):
        archiver.archive("job", "universal", [HumanMessage("q")], _reply(), "m")
        assert fw.llm_rows[0]["metadata"] is None
        assert len(fw.chat_rows) == 1


class TestArchiveError:
    def test_a_failed_try_is_a_full_row(self, archiver, fw):
        started = datetime(2026, 11, 14, 10, 0, 0, tzinfo=timezone.utc)
        archiver.archive_error(
            "job",
            "universal",
            [HumanMessage("q")],
            "m",
            "boom",
            "APITimeoutError",
            latency_ms=600000,
            iteration=7,
            tool_schemas=[TOOL],
            model_kwargs={"temperature": 0.0},
            started_at=started,
            attempt=1,
            wire_attempts=WIRE,
        )
        row = fw.llm_rows[0]
        assert row["metadata"]["status"] == "error"
        assert row["metadata"]["error"] == {
            "type": "APITimeoutError",
            "message": "boom",
        }
        assert row["iteration"] == 7
        assert row["response"] == {}
        assert row["request"]["tools"] == [TOOL]
        assert row["request"]["model_kwargs"] == {"temperature": 0.0}
        assert row["request"]["wire"][0]["params"]["reasoning_effort"] == "high"
        assert "_t0" not in row["request"]["wire"][0]
        assert row["metrics"]["started_at"] == "2026-11-14T10:00:00.000+00:00"
        assert row["metrics"]["attempt"] == 1

    def test_the_older_call_shape_still_works(self, archiver, fw):
        archiver.archive_error("job", "universal", [], "m", "boom", "ValueError")
        row = fw.llm_rows[0]
        assert row["iteration"] is None
        assert "wire" not in row["request"]
        assert "started_at" not in row["metrics"]


class TestFullToolIO:
    ARGS = {"query": "MATCH (n) RETURN n " * 40, "params": {"limit": 5}}
    RESULT = "row " * 400

    def _step(self, archiver, fw):
        audit_id = archiver.audit_tool_call(
            job_id="job",
            agent_type="universal",
            iteration=1,
            tool_name="cypher_execute",
            call_id="c1",
            arguments=self.ARGS,
        )
        archiver.update_tool_result(audit_id, self.RESULT, True, 12)
        return fw.pre_rows[0]["payload"]["tool"], fw.posts[0][1]["tool"]

    def test_off_by_default_only_previews(self, archiver, fw, monkeypatch):
        monkeypatch.delenv("AUDIT_FULL_TOOL_IO", raising=False)
        assert full_tool_io_enabled() is False
        call, result = self._step(archiver, fw)
        assert "arguments_full" not in call
        assert len(call["arguments"]["query"]) < len(self.ARGS["query"])
        assert "result" not in result
        assert result["result_size_bytes"] == len(self.RESULT)

    @pytest.mark.parametrize("value", ["1", "true", "on"])
    def test_on_keeps_arguments_and_result_whole(
        self, archiver, fw, monkeypatch, value
    ):
        monkeypatch.setenv("AUDIT_FULL_TOOL_IO", value)
        call, result = self._step(archiver, fw)
        assert call["arguments_full"] == self.ARGS
        assert result["result"] == self.RESULT
        # The previews the debug view reads are unchanged.
        assert len(call["arguments"]["query"]) < len(self.ARGS["query"])
        assert len(result["result_preview"]) < len(self.RESULT)

    def test_a_failed_tool_keeps_its_full_error(self, archiver, fw, monkeypatch):
        monkeypatch.setenv("AUDIT_FULL_TOOL_IO", "1")
        audit_id = archiver.audit_tool_call(
            job_id="job",
            agent_type="universal",
            iteration=1,
            tool_name="t",
            call_id="c1",
            arguments={},
        )
        error = "constraint violated " * 100
        archiver.update_tool_result(audit_id, "", False, 3, error=error)
        assert fw.posts[0][1]["tool"]["error_full"] == error


# ---------------------------------------------------------------------------
# The execute node
# ---------------------------------------------------------------------------


class _Unavailable(Exception):
    """A provider 503, as the SDK raises it (transient: retried)."""

    status_code = 503


class FlakyLLM(layout.CapturingLLM):
    """Raises the scripted errors first, then answers."""

    def __init__(self, errors: List[BaseException], responses=None) -> None:
        super().__init__(responses)
        self._errors = list(errors)

    async def ainvoke(self, prepared, **kwargs):
        if self._errors:
            self.requests.append(list(prepared))
            raise self._errors.pop(0)
        return await super().ainvoke(prepared, **kwargs)


async def _run_archived(node, state, fw):
    """One execute turn with a real archiver over the fake writer."""
    with patch("agent.graph.get_archiver", return_value=LLMArchiver(writer=fw)):
        patches = layout._patches()
        for p in patches:
            p.start()
        try:
            return await node(state)
        finally:
            for p in patches:
                p.stop()


env = layout.env


@pytest.mark.asyncio
async def test_execute_archives_every_try_that_raised(env, fw):
    answer = AIMessage(
        content="",
        tool_calls=[{"name": "read_file", "args": {"path": "b.md"}, "id": "c2"}],
    )
    llm = FlakyLLM([_Unavailable("503 upstream busy")], [answer])
    node = layout._make_node(
        env, llm, retry_manager=ToolRetryManager(max_retries=2, base_delay=0.0)
    )
    before = datetime.now(timezone.utc)
    result = await _run_archived(node, layout._state(), fw)
    assert not result.get("error")

    assert len(fw.llm_rows) == 2
    failed, answered = fw.llm_rows
    assert failed["metadata"]["status"] == "error"
    assert failed["metadata"]["error"]["type"] == "_Unavailable"
    assert failed["metrics"]["attempt"] == 1
    assert failed["iteration"] == answered["iteration"]
    assert failed["request"]["message_count"] == answered["request"]["message_count"]
    assert failed["request"]["tools"] == answered["request"]["tools"]
    assert answered["metrics"]["attempt"] == 2
    assert answered["metadata"] != {"status": "error"}

    for row in (failed, answered):
        started = datetime.fromisoformat(row["metrics"]["started_at"])
        assert before.replace(microsecond=0) <= started <= row["timestamp"]
    assert failed["metrics"]["started_at"] <= answered["metrics"]["started_at"]
    # Only the answered try is part of the conversation.
    assert len(fw.chat_rows) == 1


@pytest.mark.asyncio
async def test_execute_archives_a_reply_a_guard_drops(env, fw):
    """A degenerate reply is replaced by a recovery nudge; it still gets a row."""
    degenerate = AIMessage(content="</tool_call>" * 80)
    llm = layout.CapturingLLM([degenerate])
    node = layout._make_node(env, llm)
    result = await _run_archived(node, layout._state(), fw)

    assert not result.get("error")
    assert degenerate not in result["messages"]
    assert len(fw.llm_rows) == 1
    row = fw.llm_rows[0]
    assert row["metadata"]["status"] == "rejected"
    assert row["metadata"]["rejected"]["reason"] == "response_degeneration"
    assert row["response"]["content"] == degenerate.content
    assert row["metrics"]["attempt"] == 1
    assert fw.chat_rows == []
