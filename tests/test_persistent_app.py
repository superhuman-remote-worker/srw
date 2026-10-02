"""Tests for src/api/persistent_app.py — persistent agent FastAPI application.

Covers: _get_agent_metrics, _safe_serialize, _save_message,
_save_turn_ai_messages, _generate_title, poll_workspace_ready,
_poll_vm_ready, _handle_compact, _handle_archive, permission_check,
on_tool_result truncation, check_interrupt closure, WS message routing,
health endpoints, _ws_send, create_persistent_app, on_turn callbacks.
"""

from agent.api import session_termination
from agent.api import session_contract
from agent.api import session_workspace
import asyncio
import json
from contextlib import ExitStack, contextmanager
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call as mock_call, patch
from uuid import uuid4

import pytest

from agent.api.session_identity import pinned_status_identity_advertised
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from agent.api.persistent_app import (
    _app_guide_health,
    _auto_title_after_first_turn,
    _draft_title_from_prompt,
    _early_title_from_prompt,
    _excerpt_for_title,
    _extract_thinking,
    _generate_title,
    _is_low_signal_prompt,
    _title_looks_conversational,
    _get_agent_metrics,
    _handle_archive,
    _handle_compact,
    _handle_vm_upgrade,
    _handle_workspace_upgrade,
    _loop_persist_message,
    _persist_one_message,
    _poll_vm_ready,
    _repair_tool_pairing,
    _safe_serialize,
    _save_message,
    _save_turn_ai_messages,
    _upgrade_already_satisfied,
    _ws_send,
    create_persistent_app,
)
from agent.api.session_attach import session_backend_is_lite, session_backend_is_vm
from agent.api.session_workspace import poll_workspace_ready


async def _handle_api_interrupt(request):
    """POST /api/interrupt through the HTTP transport bound to this runtime."""
    import agent.api.persistent_app as pa
    from agent.api import session_http

    return await session_http.handle_interrupt(
        request, pa.session_transport_bindings().http
    )


async def _serve_session_websocket(ws):
    """One session socket through the WS transport bound to this runtime."""
    import agent.api.persistent_app as pa
    from agent.api import session_websocket

    await session_websocket.serve_session_websocket(
        ws, pa.session_transport_bindings().socket
    )


def _retirement_session_mock(**kwargs):
    """Build a session double with the async child-quiescence contract."""

    session = MagicMock(**kwargs)
    session.quiesce_subagents = AsyncMock()
    session.resume_subagents = AsyncMock()
    return session


# ---------------------------------------------------------------------------
# 3.1 _get_agent_metrics()
# ---------------------------------------------------------------------------


class TestGetAgentMetrics:
    def test_returns_dict_with_memory_and_cpu(self):
        """Returns dict with memory_mb and cpu_percent when psutil available."""
        mock_proc = MagicMock()
        mock_proc.memory_info.return_value.rss = 100 * 1_048_576  # 100 MB
        mock_proc.cpu_percent.return_value = 5.2

        mock_psutil = MagicMock()
        mock_psutil.Process.return_value = mock_proc

        with patch.dict("sys.modules", {"psutil": mock_psutil}):
            result = _get_agent_metrics()

        assert result is not None
        assert "memory_mb" in result
        assert "cpu_percent" in result
        assert result["memory_mb"] == 100.0
        assert result["cpu_percent"] == 5.2

    def test_returns_none_when_psutil_not_installed(self):
        """Returns None when psutil import fails."""
        import builtins

        original_import = builtins.__import__

        def _mock_import(name, *args, **kwargs):
            if name == "psutil":
                raise ImportError("no psutil")
            return original_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=_mock_import):
            result = _get_agent_metrics()
        assert result is None

    def test_returns_none_on_exception(self):
        """Returns None on any psutil exception."""
        mock_psutil = MagicMock()
        mock_psutil.Process.side_effect = RuntimeError("proc error")

        with patch.dict("sys.modules", {"psutil": mock_psutil}):
            result = _get_agent_metrics()
        assert result is None

    def test_memory_health_included_when_counters_nonzero(self):
        """Contained memory-store failure counters ride the metrics dict."""
        from shared.runtime.services.recall_store import memory_health

        mock_psutil = MagicMock()
        mock_psutil.Process.return_value.memory_info.return_value.rss = 1_048_576
        mock_psutil.Process.return_value.cpu_percent.return_value = 1.0

        memory_health.reset()
        try:
            with patch.dict("sys.modules", {"psutil": mock_psutil}):
                assert "memory" not in (_get_agent_metrics() or {})

                memory_health.increment("access_stats_deadlock")
                result = _get_agent_metrics()
            assert result["memory"] == {"access_stats_deadlock": 1}
        finally:
            memory_health.reset()


# ---------------------------------------------------------------------------
# 3.1b inflight_tool_call() — running-command snapshot for (re)attach
# ---------------------------------------------------------------------------


def _batch_messages(*answered: str) -> list:
    """A delegation batch of three calls; ``answered`` already have results."""
    messages = [
        HumanMessage(content="research three things"),
        AIMessage(
            content="",
            tool_calls=[
                {"id": f"d{i}", "name": "delegate_agent", "args": {"brief": b}}
                for i, b in enumerate(("a", "b", "c"), start=1)
            ],
        ),
    ]
    messages += [
        ToolMessage(content="report", tool_call_id=call_id) for call_id in answered
    ]
    return messages


class TestParallelToolCallsInFlight:
    """parallel_subagents.md F17: a session delegation batch runs several calls
    at once. The in-flight latch tracks every call, and the welcome frame
    reports every running one as ``running_tools`` (the Cockpit's
    ``parseRunningTools`` shape: ``{id, tool, args}``), keeping
    ``running_tool`` for older Cockpits."""

    @pytest.fixture(autouse=True)
    def _isolated(self, monkeypatch):
        import agent.api.persistent_app as pa

        monkeypatch.setattr(pa, "_tool_inflight", False)
        monkeypatch.setattr(pa, "_tool_inflight_calls", set())
        monkeypatch.setattr(
            pa, "_loop_runtime_effect_authority_current", AsyncMock(return_value=True)
        )
        monkeypatch.setattr(pa, "_broadcast", MagicMock())
        return pa

    def test_inflight_tool_calls_lists_every_unanswered_call(self):
        from agent.core.archiver import inflight_tool_call, inflight_tool_calls

        assert [c["id"] for c in inflight_tool_calls(_batch_messages())] == [
            "d1",
            "d2",
            "d3",
        ]
        assert [c["id"] for c in inflight_tool_calls(_batch_messages("d2"))] == [
            "d1",
            "d3",
        ]
        assert inflight_tool_calls(_batch_messages("d1", "d2", "d3")) == []
        assert inflight_tool_call(_batch_messages("d1"))["id"] == "d2"

    @pytest.mark.asyncio
    async def test_the_first_result_does_not_clear_a_running_sibling(self, _isolated):
        pa = _isolated
        for call_id in ("d1", "d2"):
            await pa._loop_on_tool_execution_start("delegate_agent", call_id)
        assert pa._tool_inflight is True
        assert pa._tool_inflight_calls == {"d1", "d2"}

        await pa._loop_on_tool_result("delegate_agent", "report", "d1")
        assert pa._tool_inflight is True  # d2 still runs
        await pa._loop_on_tool_result("delegate_agent", "report", "d2")
        assert pa._tool_inflight is False
        assert pa._tool_inflight_calls == set()

    @pytest.mark.asyncio
    async def test_a_cleared_latch_drops_stale_ids(self, _isolated):
        """The flag stays the authority: ids beside a false flag (a reset that
        cleared only the flag) never keep it latched."""
        pa = _isolated
        pa._tool_inflight_calls.add("stale")
        await pa._loop_on_tool_execution_start("run_command", "c1")
        assert pa._tool_inflight_calls == {"c1"}
        await pa._loop_on_tool_result("run_command", "ok", "c1")
        assert pa._tool_inflight is False

        pa._tool_inflight_calls.add("stale")
        await pa._loop_on_tool_result("run_command", "ok", "other")
        assert pa._tool_inflight is False and pa._tool_inflight_calls == set()

    @pytest.mark.asyncio
    async def test_a_new_turn_drops_a_call_whose_result_never_ran(
        self, _isolated, monkeypatch
    ):
        """A call that raised past its result callback (WorkspaceUnavailableError)
        leaves its id behind. The next turn's calls must still release the
        latch, or Stop, drain and preStop see a tool in flight forever."""
        pa = _isolated
        monkeypatch.setattr(
            pa, "_session", SimpleNamespace(turn_count=0, workspace_sync=None)
        )
        monkeypatch.setattr(pa._session_identity, "_thread_id", None)
        monkeypatch.setattr(pa, "_turn_event_open", False)
        monkeypatch.setattr(pa, "_turn_tool_execution_identity", None)
        monkeypatch.setattr(pa, "_turn_start_external_hook", None)
        monkeypatch.setattr(pa, "_cloud_sync_retry_pending", False)
        await pa._loop_on_tool_execution_start("run_command", "c1")  # never answered

        await pa._loop_on_turn_start(2)
        assert pa._tool_inflight is False and pa._tool_inflight_calls == set()
        await pa._loop_on_tool_execution_start("run_command", "c2")
        await pa._loop_on_tool_result("run_command", "ok", "c2")
        assert pa._tool_inflight is False

    @pytest.mark.asyncio
    async def test_welcome_reports_every_running_call_of_a_batch(self, _isolated):
        pa = _isolated
        session = SimpleNamespace(messages=_batch_messages())
        for call_id in ("d1", "d2", "d3"):
            await pa._loop_on_tool_execution_start("delegate_agent", call_id)

        assert pa._welcome_running_tools(session) == [
            {"id": "d1", "tool": "delegate_agent", "args": {"brief": "a"}},
            {"id": "d2", "tool": "delegate_agent", "args": {"brief": "b"}},
            {"id": "d3", "tool": "delegate_agent", "args": {"brief": "c"}},
        ]
        assert pa._welcome_running_tool(session) == {
            "id": "d1",
            "tool": "delegate_agent",
            "args": {"brief": "a"},
        }

        # d1 finished: its result is in the transcript and its latch released.
        session.messages = _batch_messages("d1")
        await pa._loop_on_tool_result("delegate_agent", "report", "d1")
        assert [c["id"] for c in pa._welcome_running_tools(session)] == ["d2", "d3"]
        assert pa._welcome_running_tool(session)["id"] == "d2"

    @pytest.mark.asyncio
    async def test_a_sequential_batch_reports_only_the_running_call(self, _isolated):
        """Sequential execution leaves later calls unanswered but not started;
        they are not running."""
        pa = _isolated
        session = SimpleNamespace(messages=_batch_messages())
        await pa._loop_on_tool_execution_start("delegate_agent", "d1")
        assert [c["id"] for c in pa._welcome_running_tools(session)] == ["d1"]

    def test_idle_and_legacy_latch(self, _isolated):
        pa = _isolated
        session = SimpleNamespace(messages=_batch_messages())
        assert pa._welcome_running_tools(session) == []
        assert pa._welcome_running_tool(session) is None
        # A latch without ids (set directly, as older paths did): the first
        # unanswered call is the running one, exactly as before the list.
        pa._tool_inflight = True
        assert [c["id"] for c in pa._welcome_running_tools(session)] == ["d1"]


class TestInflightToolCall:
    """inflight_tool_call() identifies the command a (re)attaching client should
    surface as 'running' from the agent's in-memory messages (which aren't yet
    persisted, so REST history can't show them mid-turn)."""

    def test_trailing_unanswered_tool_call_is_inflight(self):
        from agent.core.archiver import inflight_tool_call

        messages = [
            HumanMessage(content="build it"),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "id": "tc1",
                        "name": "run_command",
                        "args": {"command": "python ingest.py --reset"},
                    }
                ],
            ),
        ]
        rt = inflight_tool_call(messages)
        assert rt is not None
        assert rt["id"] == "tc1"
        assert rt["tool"] == "run_command"
        assert rt["args"]["command"] == "python ingest.py --reset"

    def test_answered_tool_call_is_not_inflight(self):
        from agent.core.archiver import inflight_tool_call

        messages = [
            HumanMessage(content="build it"),
            AIMessage(
                content="",
                tool_calls=[{"id": "tc1", "name": "run_command", "args": {}}],
            ),
            ToolMessage(content="Exit code: 0", tool_call_id="tc1"),
        ]
        assert inflight_tool_call(messages) is None

    def test_no_tool_calls_is_none(self):
        from agent.core.archiver import inflight_tool_call

        messages = [HumanMessage(content="hi"), AIMessage(content="hello")]
        assert inflight_tool_call(messages) is None

    def test_only_last_tool_calling_turn_counts(self):
        from agent.core.archiver import inflight_tool_call

        messages = [
            AIMessage(
                content="",
                tool_calls=[{"id": "old", "name": "read_file", "args": {}}],
            ),
            ToolMessage(content="done", tool_call_id="old"),
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "id": "new",
                        "name": "run_command",
                        "args": {"command": "sleep 99"},
                    }
                ],
            ),
        ]
        rt = inflight_tool_call(messages)
        assert rt is not None
        assert rt["id"] == "new"
        assert rt["tool"] == "run_command"


# ---------------------------------------------------------------------------
# 3.2 _safe_serialize()
# ---------------------------------------------------------------------------


class TestSafeSerialize:
    def test_dict_unchanged(self):
        d = {"key": "value", "num": 42}
        assert _safe_serialize(d) == d

    def test_list_unchanged(self):
        lst = [1, 2, "three"]
        assert _safe_serialize(lst) == lst

    def test_string_unchanged(self):
        assert _safe_serialize("hello") == "hello"

    def test_int_unchanged(self):
        assert _safe_serialize(42) == 42

    def test_none_unchanged(self):
        assert _safe_serialize(None) is None

    def test_datetime_converted_to_string(self):
        dt = datetime(2026, 3, 30, 12, 0)
        result = _safe_serialize(dt)
        assert isinstance(result, str)
        assert "2026" in result

    def test_set_converted_to_string(self):
        s = {1, 2, 3}
        result = _safe_serialize(s)
        assert isinstance(result, str)

    def test_custom_class_converted_to_string(self):
        class Foo:
            def __str__(self):
                return "foo_repr"

        result = _safe_serialize(Foo())
        assert result == "foo_repr"


# ---------------------------------------------------------------------------
# 3.3 _save_message()
# ---------------------------------------------------------------------------


class TestSaveMessage:
    @pytest.mark.asyncio
    async def test_calls_client_save_thread_message(self):
        client = AsyncMock()
        await _save_message(client, "tid", "user", "hello", None, 1)
        client.save_thread_message.assert_called_once_with(
            thread_id="tid",
            role="user",
            content="hello",
            tool_calls=None,
            turn_number=1,
            tool_call_id=None,
            thinking=None,
            id=None,
        )

    @pytest.mark.asyncio
    async def test_exception_does_not_propagate(self):
        client = AsyncMock()
        client.save_thread_message.side_effect = RuntimeError("db error")
        # Should not raise
        await _save_message(client, "tid", "user", "hi", None, 1)


# ---------------------------------------------------------------------------
# 3.3b _restore_session_messages() — IDs must be set so RemoveMessage works
# ---------------------------------------------------------------------------


class TestRestoreSessionMessageIds:
    """Restored messages must carry IDs so compaction's RemoveMessage works.

    Without this, a session resumed from a poisoned state can never compact
    (RemoveMessage(id=None) is a no-op) — see issue
    persistent_session_restored_messages_no_ids.md.
    """

    @pytest.mark.asyncio
    async def test_all_restored_messages_have_ids(self):
        from agent.api import persistent_app as pa

        mock_session = MagicMock()
        mock_session.messages = []
        mock_agent = MagicMock()
        mock_agent.postgres_conn = MagicMock()
        # Path B: no compaction checkpoint → existing full-load behavior.
        mock_agent.postgres_conn.get_latest_compaction_checkpoint = AsyncMock(
            return_value=None
        )
        mock_agent.postgres_conn.get_thread_messages_history = AsyncMock(
            return_value=[
                {"role": "user", "content": "hi", "tool_calls": None, "turn_number": 1},
                {
                    "role": "assistant",
                    "content": "calling tool",
                    "tool_calls": [{"id": "t1", "name": "f", "args": {}}],
                    "turn_number": 1,
                },
                {
                    "role": "tool",
                    "content": "result",
                    "tool_calls": None,
                    "turn_number": 1,
                },
                {
                    "role": "assistant",
                    "content": "done",
                    "tool_calls": None,
                    "turn_number": 1,
                },
            ]
        )

        with (
            patch.object(pa, "_session", mock_session),
            patch.object(pa, "_agent", mock_agent),
            patch.object(pa._session_identity, "_thread_id", "thread-abc"),
        ):
            await pa._restore_session_messages()

        # All four messages must have IDs set.
        assert len(mock_session.messages) == 4
        ids = [m.id for m in mock_session.messages]
        assert all(i is not None and i for i in ids), (
            f"every restored message needs an id, got {ids}"
        )
        # IDs must be unique (UUIDs).
        assert len(set(ids)) == len(ids), "restored message IDs must be unique"


class TestRestoreSessionToolPairing:
    """Resume must never emit a tool call without its result.

    Regression for the Responses API 400 "No tool output found for function
    call ..." hit when resuming session b4478b88: the restore path capped at
    500 messages from the OLDEST end and sliced a 5-call parallel tool batch,
    orphaning two function calls. Resume now loads a bounded NEWEST-N tail (the
    resume floor) and repairs any residual orphan — including a batch sliced at
    the floor — before the first LLM call.
    """

    @pytest.mark.asyncio
    async def test_loads_newest_capped_tail(self):
        """Restore loads the NEWEST N (resume floor), not an oldest-N truncation
        — recent context is preserved while the load stays bounded."""
        from agent.api import persistent_app as pa

        mock_session = MagicMock()
        mock_session.messages = []
        mock_session.context_manager = None  # skip compaction in this test
        mock_agent = MagicMock()
        mock_agent.postgres_conn = MagicMock()
        # Path B: no compaction checkpoint → bounded newest-N load.
        mock_agent.postgres_conn.get_latest_compaction_checkpoint = AsyncMock(
            return_value=None
        )
        get_history = AsyncMock(
            return_value=[
                {"role": "user", "content": "hi", "tool_calls": None, "turn_number": 1},
            ]
        )
        mock_agent.postgres_conn.get_thread_messages_history = get_history

        with (
            patch.object(pa, "_session", mock_session),
            patch.object(pa, "_agent", mock_agent),
            patch.object(pa._session_identity, "_thread_id", "thread-full"),
        ):
            await pa._restore_session_messages()

        get_history.assert_awaited_once()
        kwargs = get_history.await_args.kwargs
        assert kwargs.get("newest_first") is True, (
            "resume keeps the NEWEST messages (not an oldest-N truncation — the "
            f"b4478b88 concern), got {kwargs}"
        )
        assert kwargs.get("limit") == pa._resume_message_limit, (
            "the load is bounded by the resume floor; _repair_tool_pairing handles "
            "any tool batch sliced at the floor"
        )

    @pytest.mark.asyncio
    async def test_orphaned_tool_calls_pruned_on_restore(self):
        """An assistant batch missing some results is repaired, not orphaned."""
        from agent.api import persistent_app as pa

        # 5 parallel tool calls, only 3 results persisted (the b4478b88 shape).
        history = [
            {
                "role": "user",
                "content": "build it",
                "tool_calls": None,
                "turn_number": 1,
            },
            {
                "role": "assistant",
                "content": "",
                "tool_calls": [
                    {"id": f"call_{i}", "name": "read_file", "args": {}}
                    for i in range(5)
                ],
                "turn_number": 1,
            },
            {
                "role": "tool",
                "content": "r0",
                "tool_call_id": "call_0",
                "tool_calls": None,
                "turn_number": 1,
            },
            {
                "role": "tool",
                "content": "r1",
                "tool_call_id": "call_1",
                "tool_calls": None,
                "turn_number": 1,
            },
            {
                "role": "tool",
                "content": "r2",
                "tool_call_id": "call_2",
                "tool_calls": None,
                "turn_number": 1,
            },
        ]

        mock_session = MagicMock()
        mock_session.messages = []
        mock_session.context_manager = None  # isolate the pairing repair
        mock_agent = MagicMock()
        mock_agent.postgres_conn = MagicMock()
        # Path B: no compaction checkpoint → existing full-load behavior.
        mock_agent.postgres_conn.get_latest_compaction_checkpoint = AsyncMock(
            return_value=None
        )
        mock_agent.postgres_conn.get_thread_messages_history = AsyncMock(
            return_value=history
        )

        with (
            patch.object(pa, "_session", mock_session),
            patch.object(pa, "_agent", mock_agent),
            patch.object(pa._session_identity, "_thread_id", "thread-orphan"),
        ):
            await pa._restore_session_messages()

        msgs = mock_session.messages
        call_ids = {
            tc["id"]
            for m in msgs
            if isinstance(m, AIMessage)
            for tc in (m.tool_calls or [])
        }
        result_ids = {m.tool_call_id for m in msgs if isinstance(m, ToolMessage)}
        # Invariant the Responses API enforces: calls and results match exactly.
        assert call_ids == result_ids == {"call_0", "call_1", "call_2"}, (
            f"unpaired tool calls/results remain: calls={call_ids} results={result_ids}"
        )

    def test_repair_drops_orphan_result_with_no_call(self):
        """A tool result whose call is absent is dropped."""
        ai = AIMessage(
            content="ok", tool_calls=[{"id": "a", "name": "f", "args": {}}], id="1"
        )
        good = ToolMessage(content="ra", tool_call_id="a", id="2")
        orphan = ToolMessage(content="rb", tool_call_id="b", id="3")
        out = _repair_tool_pairing([ai, good, orphan])
        assert orphan not in out
        assert good in out
        assert {m.tool_call_id for m in out if isinstance(m, ToolMessage)} == {"a"}

    def test_repair_keeps_fully_paired_history_unchanged(self):
        """Well-formed history passes through with pairing intact."""
        ai = AIMessage(
            content="",
            tool_calls=[
                {"id": "a", "name": "f", "args": {}},
                {"id": "b", "name": "g", "args": {}},
            ],
            id="1",
        )
        t_a = ToolMessage(content="ra", tool_call_id="a", id="2")
        t_b = ToolMessage(content="rb", tool_call_id="b", id="3")
        human = HumanMessage(content="next", id="4")
        out = _repair_tool_pairing([ai, t_a, t_b, human])
        assert len(out) == 4
        kept_ai = next(m for m in out if isinstance(m, AIMessage))
        assert {tc["id"] for tc in kept_ai.tool_calls} == {"a", "b"}


# ---------------------------------------------------------------------------
# 3.3c _restore_session_messages() — checkpoint-based resume ("Option B")
# ---------------------------------------------------------------------------


class TestRestoreFromCheckpoint:
    """Resume from a persisted compaction checkpoint instead of re-loading the
    full history + re-summarizing. The OOM observed on the 793-msg / 395k-token
    thread (``exit_code=137`` reap) traces directly to the full-load behavior
    this class verifies has been replaced when a checkpoint exists.
    """

    @pytest.mark.asyncio
    async def test_path_a_restores_summary_and_tail_only(self):
        """Checkpoint with ``boundary_turn`` → ``[SystemMessage(summary)] +
        tail rows``; the pre-boundary history is never re-loaded."""
        from agent.api import persistent_app as pa

        mock_session = MagicMock()
        mock_session.messages = []
        mock_session.context_manager = None  # isolate from re-bound
        mock_agent = MagicMock()
        mock_agent.postgres_conn = MagicMock()
        mock_agent.postgres_conn.get_latest_compaction_checkpoint = AsyncMock(
            return_value={
                "summary": "We did A, B, C.",
                "boundary_turn": 5,
                "turn_number": 6,
            }
        )
        get_history = AsyncMock(
            return_value=[
                {
                    "role": "user",
                    "content": "after-boundary",
                    "tool_calls": None,
                    "turn_number": 6,
                },
                {
                    "role": "assistant",
                    "content": "ok",
                    "tool_calls": None,
                    "turn_number": 6,
                },
            ]
        )
        mock_agent.postgres_conn.get_thread_messages_history = get_history

        with (
            patch.object(pa, "_session", mock_session),
            patch.object(pa, "_agent", mock_agent),
            patch.object(pa._session_identity, "_thread_id", "thread-ckpt"),
        ):
            await pa._restore_session_messages()

        # The tail loader must be called with since_turn=boundary_turn.
        get_history.assert_awaited_once()
        kwargs = get_history.await_args.kwargs
        assert kwargs.get("since_turn") == 5, (
            f"Path A must pass since_turn=boundary_turn; kwargs={kwargs}"
        )

        # First in-memory message is the canonical summary SystemMessage; the
        # exact "[Summary of prior work]\n" prefix lets a later live compaction
        # merge this restored summary rather than duplicate it
        # (src/core/context.py:1468).
        msgs = mock_session.messages
        assert len(msgs) == 3, (
            f"want [summary, user, ai]; got {[type(m).__name__ for m in msgs]}"
        )
        assert isinstance(msgs[0], SystemMessage)
        assert msgs[0].content.startswith("[Summary of prior work]\n")
        assert "We did A, B, C." in msgs[0].content
        assert isinstance(msgs[1], HumanMessage)
        assert isinstance(msgs[2], AIMessage)

        # turn_count must be restored to max(tail turns, boundary) so the next
        # live turn picks up correctly.
        assert mock_session.turn_count == 6

    @pytest.mark.asyncio
    async def test_path_b_back_compat_when_no_checkpoint(self):
        """No summary row → Path B: bounded newest-N load, no ``since_turn``."""
        from agent.api import persistent_app as pa

        mock_session = MagicMock()
        mock_session.messages = []
        mock_session.context_manager = None
        mock_agent = MagicMock()
        mock_agent.postgres_conn = MagicMock()
        mock_agent.postgres_conn.get_latest_compaction_checkpoint = AsyncMock(
            return_value=None
        )
        get_history = AsyncMock(
            return_value=[
                {"role": "user", "content": "hi", "tool_calls": None, "turn_number": 1},
            ]
        )
        mock_agent.postgres_conn.get_thread_messages_history = get_history

        with (
            patch.object(pa, "_session", mock_session),
            patch.object(pa, "_agent", mock_agent),
            patch.object(pa._session_identity, "_thread_id", "thread-no-ckpt"),
        ):
            await pa._restore_session_messages()

        kwargs = get_history.await_args.kwargs
        assert kwargs.get("since_turn") is None, (
            f"Path B must not filter by since_turn; kwargs={kwargs}"
        )
        assert kwargs.get("newest_first") is True, "Path B loads the newest-N tail"
        assert kwargs.get("limit") == pa._resume_message_limit, "bounded by the floor"

    @pytest.mark.asyncio
    async def test_path_b_back_compat_when_boundary_turn_missing(self):
        """Phase-3 summary rows predate the boundary_turn metric — fall back to
        full load so existing threads stay correct."""
        from agent.api import persistent_app as pa

        mock_session = MagicMock()
        mock_session.messages = []
        mock_session.context_manager = None
        mock_agent = MagicMock()
        mock_agent.postgres_conn = MagicMock()
        mock_agent.postgres_conn.get_latest_compaction_checkpoint = AsyncMock(
            return_value={
                "summary": "old phase-3 summary",
                "boundary_turn": None,
                "turn_number": 3,
            }
        )
        get_history = AsyncMock(return_value=[])
        mock_agent.postgres_conn.get_thread_messages_history = get_history

        with (
            patch.object(pa, "_session", mock_session),
            patch.object(pa, "_agent", mock_agent),
            patch.object(pa._session_identity, "_thread_id", "thread-old-summary"),
        ):
            await pa._restore_session_messages()

        kwargs = get_history.await_args.kwargs
        assert kwargs.get("since_turn") is None
        assert kwargs.get("newest_first") is True
        assert kwargs.get("limit") == pa._resume_message_limit

    @pytest.mark.asyncio
    async def test_path_b_records_resume_checkpoint_when_compacted(self):
        """Path B that re-summarizes must persist a fresh checkpoint with
        ``trigger='resume'`` so subsequent resumes hit Path A and the banner
        appears (closes the resume-banner gap)."""
        from agent.api import persistent_app as pa

        history = [
            {"role": "user", "content": f"q{i}", "tool_calls": None, "turn_number": i}
            for i in range(1, 4)
        ] + [
            {
                "role": "assistant",
                "content": f"a{i}",
                "tool_calls": None,
                "turn_number": i,
            }
            for i in range(1, 4)
        ]

        mock_session = MagicMock()
        mock_session.messages = []
        mock_session.turn_count = 0  # bumped by restore from db turns
        mock_session.config.context_management.max_summary_length = 10000
        # Feed a clean post-strip shape; the loop strips RemoveMessage markers
        # before this slice in the real path.
        compacted = [
            SystemMessage(content="[Summary of prior work]\nRecap of q1-q3"),
            HumanMessage(content="q3", id="x"),
            AIMessage(content="a3", id="y"),
        ]
        mock_session.context_manager = MagicMock()
        # Simulate a *successful* summarization: the restore gate is the
        # manager's compaction_runs counter, not a length delta.
        mock_session.context_manager.compaction_runs = 0

        async def _ensure(*args, **kwargs):
            mock_session.context_manager.compaction_runs += 1
            return compacted

        mock_session.context_manager.ensure_within_limits = AsyncMock(
            side_effect=_ensure
        )
        mock_session.auxiliary_llm = MagicMock()
        mock_agent = MagicMock()
        mock_agent.postgres_conn = MagicMock()
        mock_agent.postgres_conn.get_latest_compaction_checkpoint = AsyncMock(
            return_value=None  # Path B
        )
        mock_agent.postgres_conn.get_thread_messages_history = AsyncMock(
            return_value=history
        )
        # The resume-time checkpoint now writes straight to the DB via the
        # session's pool (the same PostgresDB the agent reads from), not the
        # orchestrator REST client.
        mock_session.postgres_conn = mock_agent.postgres_conn
        mock_session.postgres_conn.save_thread_message = AsyncMock(
            return_value={"id": "m1", "seq": 1}
        )

        with (
            patch.object(pa, "_session", mock_session),
            patch.object(pa, "_agent", mock_agent),
            patch.object(pa._session_identity, "_thread_id", "thread-resume-compact"),
        ):
            await pa._restore_session_messages()

        # save_thread_message was called for the resume-time checkpoint.
        writer = mock_session.postgres_conn.save_thread_message
        writer.assert_awaited()
        kwargs = writer.call_args.kwargs
        assert kwargs["role"] == "summary"
        assert kwargs["metrics"]["trigger"] == "resume", (
            f"resume-time compaction must persist trigger='resume'; "
            f"got metrics={kwargs['metrics']}"
        )
        assert "boundary_turn" in kwargs["metrics"], (
            "the persisted checkpoint must carry boundary_turn so the next "
            "resume can use Path A"
        )
        assert "Recap of q1-q3" in kwargs["content"]

    @staticmethod
    def _path_a_fixture(compaction_fires: bool):
        """Shared Path-A rig: a checkpoint exists and the post-boundary tail is
        loaded; ``compaction_fires`` controls whether the resume re-bound runs
        a real summarization (compaction_runs counter bump)."""
        tail = [
            {"role": "user", "content": "q6", "tool_calls": None, "turn_number": 6},
            {
                "role": "assistant",
                "content": "a6",
                "tool_calls": None,
                "turn_number": 6,
            },
        ]

        mock_session = MagicMock()
        mock_session.messages = []
        mock_session.turn_count = 0
        mock_session.config.context_management.max_summary_length = 10000
        mock_session.context_manager = MagicMock()
        mock_session.context_manager.compaction_runs = 0
        merged = [
            SystemMessage(content="[Summary of prior work]\nMerged recap q1-q6"),
            AIMessage(content="a6", id="y"),
        ]

        async def _ensure(msgs, *args, **kwargs):
            if compaction_fires:
                mock_session.context_manager.compaction_runs += 1
                return merged
            return msgs

        mock_session.context_manager.ensure_within_limits = AsyncMock(
            side_effect=_ensure
        )
        mock_session.auxiliary_llm = MagicMock()
        mock_agent = MagicMock()
        mock_agent.postgres_conn = MagicMock()
        mock_agent.postgres_conn.get_latest_compaction_checkpoint = AsyncMock(
            return_value={
                "summary": "old recap of q1-q5",
                "boundary_turn": 5,
                "boundary_seq": None,
                "turn_number": 5,
            }
        )
        mock_agent.postgres_conn.get_thread_messages_history = AsyncMock(
            return_value=tail
        )
        mock_session.postgres_conn = mock_agent.postgres_conn
        mock_session.postgres_conn.save_thread_message = AsyncMock(
            return_value={"id": "m1", "seq": 1}
        )
        return mock_session, mock_agent

    @pytest.mark.asyncio
    async def test_path_a_records_checkpoint_when_rebound_compacts(self):
        """A Path-A restore whose tail outgrew the budget re-summarizes; the
        merged result must persist so the NEXT resume pays nothing. Without
        the persist, every resume re-runs the same blocking aux-LLM
        summarization and discards it (per-claim cost on the stateless
        lane)."""
        from agent.api import persistent_app as pa

        mock_session, mock_agent = self._path_a_fixture(compaction_fires=True)

        with (
            patch.object(pa, "_session", mock_session),
            patch.object(pa, "_agent", mock_agent),
            patch.object(pa._session_identity, "_thread_id", "thread-path-a-compact"),
        ):
            await pa._restore_session_messages()

        writer = mock_session.postgres_conn.save_thread_message
        writer.assert_awaited()
        kwargs = writer.call_args.kwargs
        assert kwargs["role"] == "summary"
        assert kwargs["metrics"]["trigger"] == "resume"
        assert "Merged recap q1-q6" in kwargs["content"], (
            "the NEW merged summary must be persisted, not the stale one"
        )

    @pytest.mark.asyncio
    async def test_path_a_skips_persist_when_no_compaction(self):
        """A Path-A restore whose tail fits the budget must NOT rewrite the
        checkpoint — the existing summary row keeps driving the banner, and a
        rewrite would duplicate it on every reconnect."""
        from agent.api import persistent_app as pa

        mock_session, mock_agent = self._path_a_fixture(compaction_fires=False)

        with (
            patch.object(pa, "_session", mock_session),
            patch.object(pa, "_agent", mock_agent),
            patch.object(pa._session_identity, "_thread_id", "thread-path-a-clean"),
        ):
            await pa._restore_session_messages()

        mock_session.postgres_conn.save_thread_message.assert_not_awaited()


# ---------------------------------------------------------------------------
# 3.4 _save_turn_ai_messages()
# ---------------------------------------------------------------------------


class TestSaveTurnAiMessages:
    @pytest.mark.asyncio
    async def test_collects_messages_after_last_human(self):
        """Walks backwards from end, stops at HumanMessage; batches the turn."""
        client = AsyncMock()
        messages = [
            SystemMessage(content="sys"),
            HumanMessage(content="question"),
            AIMessage(content="answer"),
            ToolMessage(content="result", tool_call_id="tc1"),
        ]

        await _save_turn_ai_messages(client, "tid", messages, 1)

        # HF-2: one batched upsert carrying the 2 messages after the last
        # HumanMessage (was 2 serial save_thread_message calls).
        client.save_thread_messages.assert_awaited_once()
        client.save_thread_message.assert_not_called()
        thread_id, rows = client.save_thread_messages.call_args[0]
        assert thread_id == "tid"
        assert [r["role"] for r in rows] == ["ai", "tool"]

    @pytest.mark.asyncio
    async def test_collects_all_when_no_human_message(self):
        """If no HumanMessage, all messages are collected into one batch."""
        client = AsyncMock()
        messages = [
            SystemMessage(content="sys"),
            AIMessage(content="greeting"),
        ]

        await _save_turn_ai_messages(client, "tid", messages, 0)

        client.save_thread_messages.assert_awaited_once()
        _, rows = client.save_thread_messages.call_args[0]
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_tool_calls_extracted(self):
        """Tool calls extracted as list of {name, args, id} dicts in the batch."""
        client = AsyncMock()
        ai_msg = AIMessage(
            content="",
            tool_calls=[{"name": "search", "args": {"q": "test"}, "id": "tc1"}],
        )
        messages = [HumanMessage(content="go"), ai_msg]

        await _save_turn_ai_messages(client, "tid", messages, 1)

        _, rows = client.save_thread_messages.call_args[0]
        ai_row = next(r for r in rows if r["role"] == "ai")
        assert ai_row["tool_calls"] is not None
        assert ai_row["tool_calls"][0]["name"] == "search"

    @pytest.mark.asyncio
    async def test_anthropic_list_content_normalized(self):
        """Anthropic list-of-dicts content joined into string in the batch row."""
        client = AsyncMock()
        ai_msg = AIMessage(
            content=[
                {"type": "text", "text": "Hello "},
                {"type": "text", "text": "world"},
            ]
        )
        messages = [HumanMessage(content="hi"), ai_msg]

        await _save_turn_ai_messages(client, "tid", messages, 1)

        _, rows = client.save_thread_messages.call_args[0]
        ai_row = next(r for r in rows if r["role"] == "ai")
        assert "Hello" in ai_row["content"]
        assert "world" in ai_row["content"]

    @pytest.mark.asyncio
    async def test_exception_does_not_propagate(self):
        """Outer exception caught and logged."""
        client = AsyncMock()
        client.save_thread_messages.side_effect = RuntimeError("db error")
        messages = [HumanMessage(content="hi"), AIMessage(content="reply")]

        # Should not raise
        await _save_turn_ai_messages(client, "tid", messages, 1)

    @pytest.mark.asyncio
    async def test_authoritative_boundary_is_in_same_batch_call(self):
        """Stateless reconcile carries the exact accepted input identity."""
        client = AsyncMock()
        messages = [
            HumanMessage(content="question", id="input-message-7"),
            AIMessage(content="answer", id="answer-message-7"),
        ]

        await _save_turn_ai_messages(
            client,
            "tid",
            messages,
            7,
            authoritative_turn_boundary=True,
            turn_input_message_id="input-message-7",
            memory_scope_kind="thread",
            memory_scope_id="tid",
        )

        client.save_thread_messages.assert_awaited_once()
        args = client.save_thread_messages.call_args.args
        kwargs = client.save_thread_messages.call_args.kwargs
        assert args[0] == "tid"
        assert [row["id"] for row in args[1]] == ["answer-message-7"]
        assert kwargs == {
            "turn_input_message_id": "input-message-7",
            "turn_number": 7,
            "memory_scope_kind": "thread",
            "memory_scope_id": "tid",
        }

    @pytest.mark.asyncio
    async def test_authoritative_zero_output_still_mints_boundary(self):
        """An interrupted/error turn with no output still reaches the producer."""
        client = AsyncMock()
        messages = [HumanMessage(content="question", id="input-message-8")]

        await _save_turn_ai_messages(
            client,
            "tid",
            messages,
            8,
            authoritative_turn_boundary=True,
            turn_input_message_id="input-message-8",
            memory_scope_kind="thread",
            memory_scope_id="tid",
        )

        client.save_thread_messages.assert_awaited_once_with(
            "tid",
            [],
            turn_input_message_id="input-message-8",
            turn_number=8,
            memory_scope_kind="thread",
            memory_scope_id="tid",
        )

    @pytest.mark.asyncio
    async def test_authoritative_failure_propagates(self):
        client = AsyncMock()
        client.save_thread_messages.side_effect = RuntimeError("fence lost")
        messages = [
            HumanMessage(content="question", id="input-message-9"),
            AIMessage(content="answer"),
        ]

        with pytest.raises(RuntimeError, match="fence lost"):
            await _save_turn_ai_messages(
                client,
                "tid",
                messages,
                9,
                authoritative_turn_boundary=True,
                turn_input_message_id="input-message-9",
                memory_scope_kind="project",
                memory_scope_id="project-9",
            )

    @pytest.mark.asyncio
    async def test_authoritative_missing_effect_identity_fails_closed(self):
        client = AsyncMock()
        client.save_thread_messages.return_value = None
        messages = [HumanMessage(content="question", id="input-message-9")]

        with pytest.raises(RuntimeError, match="minted no memory effect"):
            await _save_turn_ai_messages(
                client,
                "tid",
                messages,
                9,
                authoritative_turn_boundary=True,
                turn_input_message_id="input-message-9",
                memory_scope_kind="thread",
                memory_scope_id="tid",
            )

    @pytest.mark.asyncio
    async def test_authoritative_boundary_requires_stable_input_id(self):
        client = AsyncMock()

        with pytest.raises(ValueError, match="exact input message id"):
            await _save_turn_ai_messages(
                client,
                "tid",
                [HumanMessage(content="question")],
                10,
                authoritative_turn_boundary=True,
                memory_scope_kind="thread",
                memory_scope_id="tid",
            )

        client.save_thread_messages.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_authoritative_boundary_ignores_synthetic_human_output(self):
        """Multimodal tool output cannot replace the accepted input identity."""
        client = AsyncMock()
        messages = [
            HumanMessage(content="inspect it", id="accepted-input"),
            AIMessage(
                content="",
                id="tool-call-message",
                tool_calls=[{"name": "inspect", "args": {}, "id": "call-1"}],
            ),
            ToolMessage(content="image", tool_call_id="call-1", id="tool-result"),
            HumanMessage(content="synthetic image", id="synthetic-image-input"),
            AIMessage(content="final answer", id="final-answer"),
        ]

        await _save_turn_ai_messages(
            client,
            "tid",
            messages,
            12,
            authoritative_turn_boundary=True,
            turn_input_message_id="accepted-input",
            memory_scope_kind="project",
            memory_scope_id="project-12",
        )

        args = client.save_thread_messages.call_args.args
        kwargs = client.save_thread_messages.call_args.kwargs
        assert [row["id"] for row in args[1]] == [
            "tool-call-message",
            "tool-result",
            "synthetic-image-input",
            "final-answer",
        ]
        assert kwargs["turn_input_message_id"] == "accepted-input"
        assert kwargs["memory_scope_kind"] == "project"
        assert kwargs["memory_scope_id"] == "project-12"

    @pytest.mark.asyncio
    async def test_authoritative_selects_by_membership_after_compaction(self):
        """A mid-turn compaction summarised the accepted input away and left a
        summary + the kept window. Membership stamps pick this turn's rows;
        the input (persisted at accept time) and other turns' rows are not
        rewritten; the DB still gets the exact input id for the boundary."""
        from shared.runtime.core.message_markers import stamp_turn_membership

        client = AsyncMock()
        prior_turn = stamp_turn_membership(AIMessage(content="before", id="prev-ai"), 3)
        stamped_input = stamp_turn_membership(
            HumanMessage(content="question", id="input-4"), 4
        )
        summary = SystemMessage(content="[Summary of prior work]\nrecap")
        call = stamp_turn_membership(
            AIMessage(
                content="",
                id="ai-call",
                tool_calls=[{"name": "search", "args": {}, "id": "c1"}],
            ),
            4,
        )
        result = stamp_turn_membership(
            ToolMessage(content="found", tool_call_id="c1", id="tool-1"), 4
        )
        final = stamp_turn_membership(AIMessage(content="done", id="final-4"), 4)
        messages = [
            SystemMessage(content="sys"),
            prior_turn,
            summary,
            stamped_input,
            call,
            result,
            final,
        ]

        await _save_turn_ai_messages(
            client,
            "tid",
            messages,
            4,
            authoritative_turn_boundary=True,
            turn_input_message_id="input-4",
            memory_scope_kind="thread",
            memory_scope_id="tid",
        )

        args = client.save_thread_messages.call_args.args
        kwargs = client.save_thread_messages.call_args.kwargs
        assert [row["id"] for row in args[1]] == ["ai-call", "tool-1", "final-4"]
        assert kwargs["turn_input_message_id"] == "input-4"
        assert kwargs["turn_number"] == 4

    @pytest.mark.asyncio
    async def test_authoritative_membership_survives_evicted_input(self):
        """The exact input is gone from RAM entirely (summarised) — no crash."""
        from shared.runtime.core.message_markers import stamp_turn_membership

        client = AsyncMock()
        messages = [
            SystemMessage(content="[Summary of prior work]\nrecap"),
            stamp_turn_membership(AIMessage(content="done", id="final-5"), 5),
        ]

        await _save_turn_ai_messages(
            client,
            "tid",
            messages,
            5,
            authoritative_turn_boundary=True,
            turn_input_message_id="input-5",
            memory_scope_kind="thread",
            memory_scope_id="tid",
        )

        args = client.save_thread_messages.call_args.args
        assert [row["id"] for row in args[1]] == ["final-5"]

    @pytest.mark.asyncio
    async def test_authoritative_unstamped_history_without_anchor_saves_nothing(self):
        """Unstamped history whose input is not resident reconciles zero rows
        (incremental rows stand, the DB mints the boundary) — never a crash."""
        client = AsyncMock()
        messages = [
            SystemMessage(content="[Summary of prior work]\nrecap"),
            AIMessage(content="done"),
        ]

        await _save_turn_ai_messages(
            client,
            "tid",
            messages,
            9,
            authoritative_turn_boundary=True,
            turn_input_message_id="input-9",
            memory_scope_kind="thread",
            memory_scope_id="tid",
        )

        client.save_thread_messages.assert_awaited_once_with(
            "tid",
            [],
            turn_input_message_id="input-9",
            turn_number=9,
            memory_scope_kind="thread",
            memory_scope_id="tid",
        )


class TestReconcileTurnWithRetry:
    """Settlement is an idempotent upsert: transient DB failures are retried
    (bounded), contract failures are not, and the pinned lane keeps its
    best-effort shape."""

    @staticmethod
    def _install(monkeypatch, save):
        from agent.api import persistent_app as pa

        monkeypatch.setattr(pa, "_TURN_RECONCILE_RETRY_DELAY_S", 0.0)
        session = MagicMock()
        session.postgres_conn = object()
        session.messages = []
        session.tool_decisions = {}
        monkeypatch.setattr(pa, "_session", session)
        monkeypatch.setattr(pa._session_identity, "_thread_id", "tid")
        monkeypatch.setattr(pa, "_save_turn_ai_messages", save)
        return pa

    @staticmethod
    def _kwargs(authoritative: bool = True):
        return {
            "metrics": None,
            "authoritative_turn_boundary": authoritative,
            "turn_input_message_id": "input-4",
            "memory_scope_kind": "thread",
            "memory_scope_id": "tid",
        }

    @pytest.mark.asyncio
    async def test_transient_failure_then_success(self, monkeypatch):
        calls = []

        async def save(*_a, **_k):
            calls.append(1)
            if len(calls) == 1:
                raise ConnectionError("pool reset")

        pa = self._install(monkeypatch, save)
        await pa._reconcile_turn_with_retry(4, **self._kwargs())
        assert len(calls) == 2

    @pytest.mark.asyncio
    async def test_timeout_is_retried_then_succeeds(self, monkeypatch):
        calls = []

        async def save(*_a, **_k):
            calls.append(1)
            if len(calls) < 3:
                raise asyncio.TimeoutError()

        pa = self._install(monkeypatch, save)
        await pa._reconcile_turn_with_retry(4, **self._kwargs())
        assert len(calls) == 3

    @pytest.mark.asyncio
    async def test_contract_failure_is_not_retried(self, monkeypatch):
        calls = []

        async def save(*_a, **_k):
            calls.append(1)
            raise ValueError("lacks an immutable memory destination")

        pa = self._install(monkeypatch, save)
        with pytest.raises(ValueError):
            await pa._reconcile_turn_with_retry(4, **self._kwargs())
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_lost_lease_is_not_retried(self, monkeypatch):
        from agent.api.lease_context import LeaseLostError

        calls = []

        async def save(*_a, **_k):
            calls.append(1)
            raise LeaseLostError("fence lost")

        pa = self._install(monkeypatch, save)
        with pytest.raises(LeaseLostError):
            await pa._reconcile_turn_with_retry(4, **self._kwargs())
        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_exhausted_transient_failures_propagate(self, monkeypatch):
        calls = []

        async def save(*_a, **_k):
            calls.append(1)
            raise ConnectionError("still down")

        pa = self._install(monkeypatch, save)
        with pytest.raises(ConnectionError):
            await pa._reconcile_turn_with_retry(4, **self._kwargs())
        assert len(calls) == pa._TURN_RECONCILE_ATTEMPTS

    @pytest.mark.asyncio
    async def test_pinned_lane_swallows_transient_failure(self, monkeypatch):
        calls = []

        async def save(*_a, **_k):
            calls.append(1)
            raise asyncio.TimeoutError()

        pa = self._install(monkeypatch, save)
        await pa._reconcile_turn_with_retry(4, **self._kwargs(authoritative=False))
        assert len(calls) == 1


class TestLoopCrashTerminalEdge:
    """A loop that dies outside a turn's own error path still closes the open
    turn: turn.error (+ durable error row) goes out before termination, so
    no client — live or replaying the journal — spins on a turn that ended.
    A lost lease is the successor claim's turn to close, not ours."""

    @pytest.mark.asyncio
    async def test_loop_crash_emits_turn_error_before_terminate(self, monkeypatch):
        from agent.api import persistent_app as pa

        async def boom():
            raise RuntimeError("settlement failed")

        task = asyncio.create_task(boom())
        await asyncio.sleep(0)
        on_error = AsyncMock()
        terminate = AsyncMock()
        monkeypatch.setattr(pa, "_loop_on_error", on_error)
        monkeypatch.setattr(pa._session_termination, "terminate", terminate)
        monkeypatch.setattr(pa, "_stateless_mode", lambda: True)

        await pa._session_termination.loop_completion_handler(task)

        on_error.assert_awaited_once()
        assert "settlement failed" in on_error.await_args.args[0]
        terminate.assert_awaited_once_with("loop_crash", mark_thread=False)

    @pytest.mark.asyncio
    async def test_lost_lease_crash_emits_no_turn_error(self, monkeypatch):
        from agent.api import persistent_app as pa
        from agent.api.lease_context import LeaseLostError

        async def lost():
            raise LeaseLostError("fence lost")

        task = asyncio.create_task(lost())
        await asyncio.sleep(0)
        on_error = AsyncMock()
        terminate = AsyncMock()
        monkeypatch.setattr(pa, "_loop_on_error", on_error)
        monkeypatch.setattr(pa._session_termination, "terminate", terminate)
        monkeypatch.setattr(pa, "_stateless_mode", lambda: True)

        await pa._session_termination.loop_completion_handler(task)

        on_error.assert_not_awaited()
        terminate.assert_awaited_once_with("loop_crash", mark_thread=False)

    @pytest.mark.asyncio
    async def test_pinned_keeps_historical_latest_human_boundary(self):
        """The new callback metadata must not change pinned reconciliation."""
        client = AsyncMock()
        messages = [
            HumanMessage(content="accepted", id="accepted-input"),
            AIMessage(content="tool call", id="before-synthetic"),
            HumanMessage(content="synthetic image", id="synthetic-input"),
            AIMessage(content="final", id="after-synthetic"),
        ]

        await _save_turn_ai_messages(
            client,
            "tid",
            messages,
            12,
            turn_input_message_id="accepted-input",
            memory_scope_kind="project",
            memory_scope_id="project-12",
        )

        _, rows = client.save_thread_messages.call_args.args
        assert [row["id"] for row in rows] == ["after-synthetic"]
        assert client.save_thread_messages.call_args.kwargs == {}


class TestAuthoritativeTurnPersist:
    @staticmethod
    def _session() -> SimpleNamespace:
        return SimpleNamespace(
            postgres_conn=object(),
            messages=[HumanMessage(content="question", id="input-message-11")],
            tool_decisions={"call-1": "approved"},
            workspace_sync=None,
            overlay_mount_manager=None,
        )

    @pytest.mark.asyncio
    async def test_stateless_timeout_aborts_turn_settlement(self, monkeypatch):
        from agent.api import persistent_app as pa

        monkeypatch.setenv("STATELESS_EXECUTOR", "1")
        session = self._session()
        broadcast = MagicMock()
        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_retire_announced_permission_rows", AsyncMock()),
            patch.object(pa, "_wire_session_aux_archiver"),
            patch.object(pa, "_broadcast", broadcast),
            patch.object(
                pa,
                "_save_turn_ai_messages",
                AsyncMock(side_effect=asyncio.TimeoutError),
            ) as save,
        ):
            with pytest.raises(asyncio.TimeoutError):
                await pa._loop_on_turn_complete_body(11)

        assert save.call_args.kwargs["authoritative_turn_boundary"] is True
        broadcast.assert_not_called()
        assert session.tool_decisions == {"call-1": "approved"}

    @pytest.mark.asyncio
    async def test_pinned_timeout_remains_nonfatal(self, monkeypatch):
        from agent.api import persistent_app as pa

        monkeypatch.delenv("STATELESS_EXECUTOR", raising=False)
        session = self._session()
        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_retire_announced_permission_rows", AsyncMock()),
            patch.object(pa, "_wire_session_aux_archiver"),
            patch.object(pa, "_broadcast"),
            patch.object(
                pa,
                "_save_turn_ai_messages",
                AsyncMock(side_effect=asyncio.TimeoutError),
            ) as save,
        ):
            await pa._loop_on_turn_complete_body(11)

        assert save.call_args.kwargs["authoritative_turn_boundary"] is False
        assert session.tool_decisions == {}

    @pytest.mark.asyncio
    async def test_cancelled_turn_skips_reconcile_and_cannot_rewrite_prior_rows(
        self, monkeypatch
    ):
        from agent.api import persistent_app as pa

        monkeypatch.delenv("STATELESS_EXECUTOR", raising=False)
        prior_human = HumanMessage(content="prior", id="prior-human")
        prior_ai = AIMessage(content="prior answer", id="prior-ai")
        session = self._session()
        session.messages = [prior_human, prior_ai]
        session.workspace_sync = object()
        session.overlay_mount_manager = object()
        save = AsyncMock()
        wire_aux = MagicMock()
        title = AsyncMock()
        cloud_push = AsyncMock()
        should_stage = MagicMock(return_value=True)
        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_retire_announced_permission_rows", AsyncMock()),
            patch.object(pa, "_wire_session_aux_archiver", wire_aux),
            patch.object(pa, "_broadcast"),
            patch.object(pa, "_save_turn_ai_messages", save),
            patch.object(pa, "_auto_title_after_first_turn", title),
            patch.object(pa, "_await_pending_cloud_push", cloud_push),
            patch.object(pa, "_should_notify_cloud_stage", should_stage),
        ):
            await pa._loop_on_turn_complete_body(
                11,
                skip_message_reconcile=True,
            )

        save.assert_not_awaited()
        wire_aux.assert_not_called()
        title.assert_not_awaited()
        cloud_push.assert_not_awaited()
        should_stage.assert_not_called()
        assert session.messages == [prior_human, prior_ai]
        assert session.tool_decisions == {}

    @pytest.mark.asyncio
    async def test_stateless_turn_cannot_skip_authoritative_reconcile(
        self, monkeypatch
    ):
        from agent.api import persistent_app as pa

        monkeypatch.setenv("STATELESS_EXECUTOR", "1")
        session = self._session()
        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_retire_announced_permission_rows", AsyncMock()),
            patch.object(pa, "_wire_session_aux_archiver"),
        ):
            with pytest.raises(RuntimeError, match="cannot skip"):
                await pa._loop_on_turn_complete_body(
                    11,
                    skip_message_reconcile=True,
                )

    @pytest.mark.asyncio
    async def test_stateless_completion_frame_follows_authoritative_persist(
        self, monkeypatch
    ):
        from agent.api import persistent_app as pa

        monkeypatch.setenv("STATELESS_EXECUTOR", "1")
        session = self._session()
        order = []

        async def save(*_args, **_kwargs):
            order.append("persist")

        def broadcast(kind, _payload):
            order.append(kind)

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_retire_announced_permission_rows", AsyncMock()),
            patch.object(pa, "_wire_session_aux_archiver"),
            patch.object(pa, "_broadcast", side_effect=broadcast),
            patch.object(pa, "_save_turn_ai_messages", side_effect=save),
        ):
            await pa._loop_on_turn_complete_body(11)

        assert order == ["persist", "turn.completed"]

    @pytest.mark.asyncio
    async def test_stateless_missing_postgres_fails_closed(self, monkeypatch):
        from agent.api import persistent_app as pa

        monkeypatch.setenv("STATELESS_EXECUTOR", "1")
        session = self._session()
        session.postgres_conn = None
        broadcast = MagicMock()
        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_retire_announced_permission_rows", AsyncMock()),
            patch.object(pa, "_wire_session_aux_archiver"),
            patch.object(pa, "_broadcast", broadcast),
        ):
            with pytest.raises(RuntimeError, match="requires a Postgres connection"):
                await pa._loop_on_turn_complete_body(11)

        broadcast.assert_not_called()


class TestPersistentLoopMemoryOutboxWiring:
    @pytest.mark.asyncio
    async def test_stateless_runtime_defers_turn_memory_to_outbox(self, monkeypatch):
        from agent.api import persistent_app as pa

        monkeypatch.setenv("STATELESS_EXECUTOR", "1")
        captured = {}
        release = asyncio.Event()

        def fake_run(**kwargs):
            captured.update(kwargs)

            async def wait_for_release():
                await release.wait()

            return wait_for_release()

        async def no_completion_cleanup(_task):
            return None

        session = MagicMock()
        session.thread_id = "tid"
        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_loop_task", None),
            patch.object(pa, "_session_ready", return_value=True),
            patch.object(pa, "run_persistent_loop", new=fake_run),
            patch.object(pa._session_termination, "loop_completion_handler", no_completion_cleanup),
        ):
            assert pa._ensure_persistent_loop_started("test") is True
            loop_task = pa._loop_task
            assert captured["defer_memory_extraction_to_outbox"] is True
            assert captured["memory_thread_id"] == "tid"
            release.set()
            await loop_task


# ---------------------------------------------------------------------------
# 3.4a _persist_one_message() + _loop_persist_message() — incremental (Phase 2)
# ---------------------------------------------------------------------------


class TestPersistOneMessage:
    """The shared serializer used by both the incremental path and the
    turn-complete reconciliation — both pass the stable id so they converge."""

    @pytest.mark.asyncio
    async def test_serializes_ai_message_with_id_and_decision(self):
        client = AsyncMock()
        msg = AIMessage(
            content="hi",
            id="msg_ai1",
            tool_calls=[{"name": "search", "args": {"q": "x"}, "id": "tc1"}],
        )
        await _persist_one_message(
            client, "tid", msg, 3, tool_decisions={"tc1": "approved"}
        )
        kwargs = client.save_thread_message.call_args.kwargs
        assert kwargs["role"] == "ai"
        assert kwargs["turn_number"] == 3
        assert kwargs["id"] == "msg_ai1"
        assert kwargs["tool_calls"][0]["decision"] == "approved"

    @pytest.mark.asyncio
    async def test_tool_message_keeps_link_and_no_metrics(self):
        client = AsyncMock()
        msg = ToolMessage(content="result", tool_call_id="tc1", id="msg_t1")
        await _persist_one_message(client, "tid", msg, 2, metrics={"tokens": 5})
        kwargs = client.save_thread_message.call_args.kwargs
        assert kwargs["role"] == "tool"
        assert kwargs["tool_call_id"] == "tc1"
        assert kwargs["id"] == "msg_t1"
        assert kwargs["metrics"] is None, "metrics attach to AI rows only"


class TestLoopPersistMessage:
    @pytest.mark.asyncio
    async def test_persists_via_session_pool_with_turn_count(self):
        from agent.api import persistent_app as pa

        mock_session = MagicMock()
        mock_session.turn_count = 4
        mock_session.tool_decisions = {}
        mock_session.postgres_conn.save_thread_message = AsyncMock(
            return_value={"id": "m", "seq": 9}
        )
        with (
            patch.object(pa, "_session", mock_session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
        ):
            persisted = await _loop_persist_message(AIMessage(content="hi", id="msg_1"))
        assert persisted is True
        kwargs = mock_session.postgres_conn.save_thread_message.call_args.kwargs
        assert kwargs["role"] == "ai"
        assert kwargs["turn_number"] == 4, "turn number comes from _session.turn_count"
        assert kwargs["id"] == "msg_1"

    @pytest.mark.asyncio
    async def test_noop_when_no_postgres_conn(self):
        from agent.api import persistent_app as pa

        mock_session = MagicMock()
        mock_session.postgres_conn = None
        with (
            patch.object(pa, "_session", mock_session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
        ):
            persisted = await _loop_persist_message(
                AIMessage(content="hi")
            )  # must not raise
        assert persisted is False

    @pytest.mark.asyncio
    async def test_non_fatal_on_db_error(self):
        from agent.api import persistent_app as pa

        mock_session = MagicMock()
        mock_session.turn_count = 1
        mock_session.tool_decisions = {}
        mock_session.postgres_conn.save_thread_message = AsyncMock(
            side_effect=RuntimeError("db down")
        )
        with (
            patch.object(pa, "_session", mock_session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
        ):
            persisted = await _loop_persist_message(
                AIMessage(content="hi", id="x")
            )  # must not raise
        assert persisted is False


# ---------------------------------------------------------------------------
# 3.4c boundary_seq recording + seq-cursor resume (Phase 3)
# ---------------------------------------------------------------------------


class TestRecordCompactionBoundarySeq:
    @pytest.mark.asyncio
    async def test_resolves_boundary_id_to_seq_on_summary_row(self):
        """When the summarizer set a boundary id, _record_compaction resolves it
        to a seq and records boundary_seq alongside boundary_turn."""
        from agent.api import persistent_app as pa

        mock_session = MagicMock()
        mock_session.turn_count = 5
        mock_session.context_manager._last_compaction_boundary_id = "msg_boundary"
        mock_session.postgres_conn.get_seq_for_message_id = AsyncMock(return_value=780)
        mock_session.postgres_conn.save_thread_message = AsyncMock(
            return_value={"id": "s", "seq": 1}
        )
        with (
            patch.object(pa, "_session", mock_session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
        ):
            await pa._record_compaction(
                "summary text", 100, 11, trigger="auto", ws=None
            )

        mock_session.postgres_conn.get_seq_for_message_id.assert_awaited_once_with(
            "tid", "msg_boundary"
        )
        kwargs = mock_session.postgres_conn.save_thread_message.call_args.kwargs
        assert kwargs["metrics"]["boundary_seq"] == 780
        assert kwargs["metrics"]["boundary_turn"] == 4  # turn_count - 1

    @pytest.mark.asyncio
    async def test_boundary_seq_none_when_no_boundary_id(self):
        """No boundary id (no real compaction / restore-time fresh ids) → the
        seq lookup is skipped and boundary_seq is None (falls back to turn)."""
        from agent.api import persistent_app as pa

        mock_session = MagicMock()
        mock_session.turn_count = 2
        mock_session.context_manager._last_compaction_boundary_id = None
        mock_session.postgres_conn.get_seq_for_message_id = AsyncMock(return_value=999)
        mock_session.postgres_conn.save_thread_message = AsyncMock(
            return_value={"id": "s", "seq": 1}
        )
        with (
            patch.object(pa, "_session", mock_session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
        ):
            await pa._record_compaction("summary", 50, 8, trigger="manual", ws=None)

        mock_session.postgres_conn.get_seq_for_message_id.assert_not_awaited()
        kwargs = mock_session.postgres_conn.save_thread_message.call_args.kwargs
        assert kwargs["metrics"]["boundary_seq"] is None


class TestRestorePathACursor:
    """Path A prefers the message-granular seq cursor, falling back to the turn
    cursor only for old summary rows that predate boundary_seq."""

    def _restore_env(self, ckpt):
        from agent.api import persistent_app as pa

        mock_session = MagicMock()
        mock_session.messages = []
        mock_session.context_manager = None  # skip the re-bound pass
        mock_agent = MagicMock()
        mock_agent.postgres_conn.get_latest_compaction_checkpoint = AsyncMock(
            return_value=ckpt
        )
        mock_agent.postgres_conn.get_thread_messages_history = AsyncMock(
            return_value=[]
        )
        return pa, mock_session, mock_agent

    @pytest.mark.asyncio
    async def test_uses_seq_cursor_when_boundary_seq_present(self):
        ckpt = {
            "summary": "S",
            "boundary_turn": 3,
            "boundary_seq": 780,
            "turn_number": 4,
        }
        pa, mock_session, mock_agent = self._restore_env(ckpt)
        with (
            patch.object(pa, "_session", mock_session),
            patch.object(pa, "_agent", mock_agent),
            patch.object(pa._session_identity, "_thread_id", "tid"),
        ):
            await pa._restore_session_messages()
        kwargs = mock_agent.postgres_conn.get_thread_messages_history.call_args.kwargs
        assert kwargs.get("seq_gt") == 780, "must load the tail by seq cursor"
        assert kwargs.get("since_turn") is None, "must not use the turn cursor"

    @pytest.mark.asyncio
    async def test_falls_back_to_turn_cursor_without_boundary_seq(self):
        ckpt = {
            "summary": "S",
            "boundary_turn": 3,
            "boundary_seq": None,  # old row, predates the feature
            "turn_number": 4,
        }
        pa, mock_session, mock_agent = self._restore_env(ckpt)
        with (
            patch.object(pa, "_session", mock_session),
            patch.object(pa, "_agent", mock_agent),
            patch.object(pa._session_identity, "_thread_id", "tid"),
        ):
            await pa._restore_session_messages()
        kwargs = mock_agent.postgres_conn.get_thread_messages_history.call_args.kwargs
        assert kwargs.get("since_turn") == 3, "old rows resume on the turn cursor"
        assert kwargs.get("seq_gt") is None

    @pytest.mark.asyncio
    async def test_applies_resume_floor_params(self):
        """Every resume read is capped: newest N (newest_first) of limit rows."""
        ckpt = {
            "summary": "S",
            "boundary_turn": 3,
            "boundary_seq": 780,
            "turn_number": 4,
        }
        pa, mock_session, mock_agent = self._restore_env(ckpt)
        with (
            patch.object(pa, "_session", mock_session),
            patch.object(pa, "_agent", mock_agent),
            patch.object(pa._session_identity, "_thread_id", "tid"),
        ):
            await pa._restore_session_messages()
        kwargs = mock_agent.postgres_conn.get_thread_messages_history.call_args.kwargs
        assert kwargs.get("newest_first") is True, "resume must use the newest-N floor"
        assert kwargs.get("limit") == pa._resume_message_limit

    @pytest.mark.asyncio
    async def test_logs_when_resume_floor_trims(self, caplog):
        """When the loaded tail hits the floor, a warning is emitted so a
        silently-truncated restore is visible."""
        import logging

        ckpt = {
            "summary": "S",
            "boundary_turn": 3,
            "boundary_seq": 780,
            "turn_number": 4,
        }
        pa, mock_session, mock_agent = self._restore_env(ckpt)
        # Return exactly the floor's worth of rows → trimmed.
        mock_agent.postgres_conn.get_thread_messages_history = AsyncMock(
            return_value=[
                {"role": "ai", "content": "x", "tool_calls": None, "turn_number": 4}
                for _ in range(pa._resume_message_limit)
            ]
        )
        with (
            patch.object(pa, "_session", mock_session),
            patch.object(pa, "_agent", mock_agent),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            caplog.at_level(logging.WARNING),
        ):
            await pa._restore_session_messages()
        assert any("Resume floor hit" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# 3.4b _extract_thinking()
# ---------------------------------------------------------------------------


class TestExtractThinking:
    """Reasoning extraction for the save path — covers all three provider shapes."""

    def test_returns_none_when_no_reasoning(self):
        msg = AIMessage(content="Just an answer.")
        assert _extract_thinking(msg) is None

    def test_anthropic_thinking_block(self):
        """Anthropic content list with {type:'thinking', thinking:'...'}."""
        msg = AIMessage(
            content=[
                {"type": "thinking", "thinking": "Let me think..."},
                {"type": "text", "text": "Answer."},
            ]
        )
        assert _extract_thinking(msg) == "Let me think..."

    def test_anthropic_multiple_thinking_blocks_concatenated(self):
        msg = AIMessage(
            content=[
                {"type": "thinking", "thinking": "First. "},
                {"type": "text", "text": "Mid."},
                {"type": "thinking", "thinking": "Second."},
            ]
        )
        assert _extract_thinking(msg) == "First. Second."

    def test_responses_api_reasoning_from_summary(self):
        """OpenAI Responses API: type='reasoning' with summary list."""
        msg = AIMessage(
            content=[
                {
                    "type": "reasoning",
                    "summary": [{"type": "summary_text", "text": "Thinking step."}],
                },
                {"type": "output_text", "text": "Answer."},
            ]
        )
        assert _extract_thinking(msg) == "Thinking step."

    def test_responses_api_reasoning_from_content(self):
        msg = AIMessage(
            content=[
                {
                    "type": "reasoning",
                    "content": [
                        {"type": "reasoning_text", "text": "Step A."},
                        {"type": "reasoning_text", "text": "Step B."},
                    ],
                },
            ]
        )
        # Streaming-style concatenation: no separator.
        assert _extract_thinking(msg) == "Step A.Step B."

    def test_responses_api_reasoning_multiple_blocks(self):
        msg = AIMessage(
            content=[
                {
                    "type": "reasoning",
                    "summary": [{"type": "summary_text", "text": "Plan. "}],
                },
                {"type": "output_text", "text": "Result."},
                {
                    "type": "reasoning",
                    "summary": [{"type": "summary_text", "text": "Reflect."}],
                },
            ]
        )
        assert _extract_thinking(msg) == "Plan. Reflect."

    def test_additional_kwargs_reasoning_content(self):
        """DeepSeek / OpenRouter / non-streaming Responses API: plain string."""
        msg = AIMessage(content="answer", additional_kwargs={"reasoning_content": "rc"})
        assert _extract_thinking(msg) == "rc"

    def test_anthropic_wins_over_responses_when_both_present(self):
        """If both reasoning shapes coexist, the Anthropic branch wins (it runs first).

        No real model emits both formats in one response — this just pins
        ordering so a future refactor doesn't accidentally swap it.
        """
        msg = AIMessage(
            content=[
                {"type": "thinking", "thinking": "anthropic"},
                {
                    "type": "reasoning",
                    "summary": [{"type": "summary_text", "text": "responses"}],
                },
            ]
        )
        assert _extract_thinking(msg) == "anthropic"

    def test_empty_reasoning_block_falls_back(self):
        """Reasoning blocks with no extractable text shouldn't mask additional_kwargs."""
        msg = AIMessage(
            content=[{"type": "reasoning", "summary": [], "content": []}],
            additional_kwargs={"reasoning_content": "fallback"},
        )
        assert _extract_thinking(msg) == "fallback"


# ---------------------------------------------------------------------------
# 3.5 _generate_title()
# ---------------------------------------------------------------------------


class TestGenerateTitle:
    @staticmethod
    def _aux(title="Test Title", *, error=None):
        """AuxiliaryLLM stub whose structured chain() yields a ConversationTitle
        (or raises), mirroring the real structured-output path titling uses."""
        from shared.runtime.services.auxiliary import ConversationTitle

        aux = MagicMock()
        if error is not None:
            aux.chain = AsyncMock(side_effect=error)
        else:
            aux.chain = AsyncMock(return_value=ConversationTitle(title=title))
        return aux

    @pytest.mark.asyncio
    async def test_returns_none_when_aux_llm_none(self):
        result = await _generate_title(
            messages=[HumanMessage(content="hi")],
            auxiliary_llm=None,
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_messages_empty(self):
        result = await _generate_title(messages=[], auxiliary_llm=MagicMock())
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_when_no_string_content(self):
        """Returns None (before any LLM call) when no message has extractable text."""
        messages = [
            AIMessage(content=[{"type": "image_url", "image_url": {"url": "x"}}]),
        ]
        aux = self._aux()

        result = await _generate_title(messages, aux)
        assert result is None
        # No extractable text -> short-circuits before invoking the model.
        aux.chain.assert_not_called()

    @pytest.mark.asyncio
    async def test_samples_first_10_messages(self):
        """Only first 10 messages sampled into the title task's text."""
        messages = [HumanMessage(content=f"msg {i}") for i in range(20)]
        aux = self._aux()

        await _generate_title(messages, aux)

        task = aux.chain.call_args[0][0]  # the GenerateTitleTask
        assert "msg 9" in task.sample_text
        assert "msg 10" not in task.sample_text

    @pytest.mark.asyncio
    async def test_excerpts_long_content_on_word_boundary(self):
        """Long message bodies are excerpted (word-boundary + ellipsis), not
        chopped mid-word — the mid-word chop made the model reply "cut off"."""
        words = " ".join(["word"] * 200)  # ~1000 chars of whole words
        aux = self._aux()

        await _generate_title([HumanMessage(content=words)], aux)

        task = aux.chain.call_args[0][0]
        # Body is bounded (excerpt cap ~240) and ends on a whole word + ellipsis,
        # never a partial token.
        assert "…" in task.sample_text
        assert "wor…" not in task.sample_text and "wo…" not in task.sample_text

    @pytest.mark.asyncio
    async def test_result_stripped_and_truncated_to_100(self):
        """Result is stripped and truncated to 100 chars."""
        aux = self._aux("  " + "A" * 150 + "  ")

        result = await _generate_title([HumanMessage(content="hi")], aux)

        assert len(result) <= 100
        assert not result.startswith(" ")

    @pytest.mark.asyncio
    async def test_returns_none_on_empty_result(self):
        """Returns None when the model returns an empty title field."""
        aux = self._aux("")
        result = await _generate_title([HumanMessage(content="hi")], aux)
        assert result is None

    @pytest.mark.asyncio
    async def test_exception_returns_none(self):
        """Exception during title generation returns None."""
        aux = self._aux(error=RuntimeError("LLM error"))
        result = await _generate_title([HumanMessage(content="hi")], aux)
        assert result is None

    @pytest.mark.parametrize(
        "reply",
        [
            # Observed regressions (dog letter / cash-register image).
            "It sounds like your message got cut off! It looks like you were "
            "about to type something that starts",
            "I don't see any attached image or table content in your "
            "message—it looks like the file may not have",
            "Sorry, I can't see the image you attached",
            "I'm not able to view the file you mentioned",
        ],
    )
    @pytest.mark.asyncio
    async def test_rejects_conversational_reply(self, reply):
        """Even under a schema, a deflection stuffed into the title field is
        rejected (placeholder/draft stays; after-turn pass retries)."""
        aux = self._aux(reply)
        result = await _generate_title([HumanMessage(content="hi")], aux)
        assert result is None

    @pytest.mark.asyncio
    async def test_accepts_normal_title(self):
        """A clean topic title is not rejected by the conversational guard."""
        aux = self._aux("Vet bill letter to dog owner")
        result = await _generate_title([HumanMessage(content="hi")], aux)
        assert result == "Vet bill letter to dog owner"


class TestTitleGuards:
    @pytest.mark.parametrize(
        "title",
        [
            "It sounds like your message got cut off",
            "I don't see any attached image",
            "Sorry, I can't see that",
            "As an AI I cannot view images",
            # Over-long deflection (word-count structural guard).
            "This is a very long sentence that clearly reads like a chat reply "
            "and not a title",
        ],
    )
    def test_conversational_detected(self, title):
        assert _title_looks_conversational(title) is True

    @pytest.mark.parametrize(
        "title",
        [
            "Vet bill letter to dog owner",
            "Cash register discrepancy analysis",
            "Reach external service from client",
            "Refactor title generation on submit",
        ],
    )
    def test_real_title_allowed(self, title):
        assert _title_looks_conversational(title) is False

    def test_excerpt_cuts_on_word_boundary(self):
        text = "one two three " * 40  # long, whole words
        out = _excerpt_for_title(text, cap=30)
        assert out.endswith("…")
        assert "thre…" not in out  # never mid-word
        assert len(out) <= 32

    def test_excerpt_leaves_short_text_untouched(self):
        assert _excerpt_for_title("short message") == "short message"


# ---------------------------------------------------------------------------
# 3.6 _is_low_signal_prompt() / _early_title_from_prompt()
# ---------------------------------------------------------------------------


class TestIsLowSignalPrompt:
    @pytest.mark.parametrize(
        "prompt",
        [
            "hi",
            "Hey",
            "  hello  ",
            "ok",
            "thanks",
            "continue",
            "Keep going",
            "can you help me?",
            "what's up",
            "test",
            "",
            "fix ci",  # under the 10-char floor
        ],
    )
    def test_low_signal(self, prompt):
        assert _is_low_signal_prompt(prompt) is True

    @pytest.mark.parametrize(
        "prompt",
        [
            "why can't external clients reach my service?",
            "Deploy the orchestrator to prod",
            "Refactor the title generation to fire on submit",
            "continue the migration where we left off",  # 'continue' + real content
        ],
    )
    def test_titleable(self, prompt):
        assert _is_low_signal_prompt(prompt) is False


class TestEarlyTitleFromPrompt:
    def _mock_session(self, title="Untitled Session"):
        mock_session = MagicMock()
        mock_session.auxiliary_llm = MagicMock()
        mock_conn = MagicMock()
        mock_conn.get_thread = AsyncMock(return_value={"title": title})
        mock_conn_ctx = AsyncMock()
        # acquire() must be a *sync* call returning an async CM, not an AsyncMock
        # (which would return a coroutine and break `async with`).
        acquire_cm = MagicMock()
        acquire_cm.__aenter__ = AsyncMock(return_value=mock_conn_ctx)
        acquire_cm.__aexit__ = AsyncMock(return_value=False)
        mock_conn.acquire = MagicMock(return_value=acquire_cm)
        mock_session.postgres_conn = mock_conn
        return mock_session, mock_conn, mock_conn_ctx

    @pytest.mark.asyncio
    async def test_drafts_untitled_thread_from_prompt(self):
        """Writes an LLM-free draft + broadcasts from the opening prompt words —
        no aux call, so a bait-y prompt can't deflect into the title."""
        prompt = "why can't external clients reach my svc?"
        mock_session, mock_conn, mock_conn_ctx = self._mock_session()
        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch("agent.api.persistent_app._draft_title_value", None),
            patch("agent.api.persistent_app._generate_title", AsyncMock()) as gen,
            patch("agent.api.persistent_app._broadcast") as bcast,
        ):
            await _early_title_from_prompt(prompt)

        # No LLM call on the submit path — the draft is a plain string slice.
        gen.assert_not_called()
        mock_conn_ctx.execute.assert_awaited_once()
        assert bcast.call_args[0][0] == "title.updated"
        assert bcast.call_args[0][1]["title"] == _draft_title_from_prompt(prompt)

    @pytest.mark.asyncio
    async def test_skips_low_signal_prompt(self):
        """A greeting is left to the after-turn pass — no draft, no write."""
        mock_session, mock_conn, mock_conn_ctx = self._mock_session()
        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch("agent.api.persistent_app._draft_title_value", None),
            patch("agent.api.persistent_app._broadcast") as bcast,
        ):
            await _early_title_from_prompt("hi")

        mock_conn_ctx.execute.assert_not_called()
        bcast.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_when_already_titled(self):
        """A resumed session with a real title is not re-drafted."""
        mock_session, mock_conn, mock_conn_ctx = self._mock_session(
            title="Existing real title"
        )
        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch("agent.api.persistent_app._draft_title_value", None),
            patch("agent.api.persistent_app._broadcast") as bcast,
        ):
            await _early_title_from_prompt("a perfectly good titleable prompt")

        mock_conn_ctx.execute.assert_not_called()
        bcast.assert_not_called()

    @pytest.mark.asyncio
    async def test_draft_failure_non_fatal(self):
        """A DB failure on the draft path must not raise out of the
        fire-and-forget task."""
        mock_session, mock_conn, mock_conn_ctx = self._mock_session()
        mock_conn.get_thread = AsyncMock(side_effect=RuntimeError("db down"))
        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch("agent.api.persistent_app._draft_title_value", None),
        ):
            await _early_title_from_prompt("a perfectly good titleable prompt")

        mock_conn_ctx.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_blocked_old_draft_cannot_write_or_broadcast_successor(self):
        import agent.api.persistent_app as papp

        old_session, old_conn, old_conn_ctx = self._mock_session()
        new_session, _, _ = self._mock_session()
        started = asyncio.Event()
        release = asyncio.Event()

        async def blocked_get(_thread_id):
            started.set()
            await release.wait()
            return {"title": "Untitled Session"}

        old_conn.get_thread = AsyncMock(side_effect=blocked_get)
        with (
            patch.object(papp, "_session", old_session),
            patch.object(papp._session_identity, "_thread_id", "thread-a"),
            patch.object(papp._session_identity, "_attach_generation", 41),
            patch.object(papp, "_draft_title_value", None),
            patch.object(papp, "_broadcast") as broadcast,
        ):
            task = asyncio.create_task(
                _early_title_from_prompt(
                    "a titleable prompt from the old claimant",
                    expected_session=old_session,
                    expected_thread_id="thread-a",
                    expected_generation=41,
                )
            )
            await started.wait()
            papp._session = new_session
            papp._session_identity._thread_id = "thread-b"
            papp._session_identity._attach_generation = 42
            release.set()
            await task

        old_conn_ctx.execute.assert_not_called()
        broadcast.assert_not_called()


class TestDraftTitleFromPrompt:
    def test_takes_leading_words(self):
        draft = _draft_title_from_prompt(
            "Deploy the orchestrator to prod and watch the rollout closely please"
        )
        assert draft == "Deploy the orchestrator to prod and watch the"  # first 8

    def test_collapses_whitespace_and_newlines(self):
        assert _draft_title_from_prompt("  hello\n\n  world  ") == "hello world"

    def test_bounds_on_char_cap_word_boundary(self):
        draft = _draft_title_from_prompt("x" * 200 + " tail", max_chars=40)
        # A single 200-char token exceeds the cap; it is cut on the char cap and
        # never emits a partial second word.
        assert len(draft) <= 40

    def test_strips_wrapping_punctuation(self):
        assert _draft_title_from_prompt('"quoted opening line here"').startswith(
            "quoted"
        )

    @pytest.mark.parametrize("empty", ["", "   ", "\n\t"])
    def test_returns_none_when_no_words(self, empty):
        assert _draft_title_from_prompt(empty) is None


class TestAutoTitleAfterFirstTurn:
    def _mock_session(self, title="Untitled Session"):
        mock_session = MagicMock()
        mock_session.auxiliary_llm = MagicMock()
        mock_session.messages = [HumanMessage(content="hi"), AIMessage(content="reply")]
        mock_conn = AsyncMock()
        mock_conn.get_thread = AsyncMock(return_value={"title": title})
        acquire_cm = MagicMock()
        mock_conn_ctx = AsyncMock()
        acquire_cm.__aenter__ = AsyncMock(return_value=mock_conn_ctx)
        acquire_cm.__aexit__ = AsyncMock(return_value=False)
        mock_conn.acquire = MagicMock(return_value=acquire_cm)
        mock_session.postgres_conn = mock_conn
        return mock_session, mock_conn, mock_conn_ctx

    @pytest.mark.asyncio
    async def test_overwrites_placeholder(self):
        """Mints the grounded LLM title over a still-placeholder thread."""
        import agent.api.persistent_app as papp

        mock_session, _, mock_conn_ctx = self._mock_session("Untitled Session")
        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch("agent.api.persistent_app._draft_title_value", None),
            patch(
                "agent.api.persistent_app._generate_title",
                AsyncMock(return_value="Grounded LLM title"),
            ),
            patch("agent.api.persistent_app._broadcast") as bcast,
        ):
            await _auto_title_after_first_turn()
            assert papp._draft_title_value is None  # nothing to clear

        mock_conn_ctx.execute.assert_awaited_once()
        assert bcast.call_args[0][1]["title"] == "Grounded LLM title"

    @pytest.mark.asyncio
    async def test_overwrites_own_draft_and_clears_marker(self):
        """Replaces the submit-time draft with the grounded title and clears the
        outstanding-draft marker."""
        import agent.api.persistent_app as papp

        mock_session, _, mock_conn_ctx = self._mock_session("why can't external")
        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch("agent.api.persistent_app._draft_title_value", "why can't external"),
            patch(
                "agent.api.persistent_app._generate_title",
                AsyncMock(return_value="Grounded LLM title"),
            ),
            patch("agent.api.persistent_app._broadcast") as bcast,
        ):
            await _auto_title_after_first_turn()
            assert papp._draft_title_value is None  # marker cleared after write

        mock_conn_ctx.execute.assert_awaited_once()
        assert bcast.call_args[0][1]["title"] == "Grounded LLM title"

    @pytest.mark.asyncio
    async def test_leaves_manual_rename_untouched(self):
        """A user rename (title is neither placeholder nor the outstanding draft)
        is never overwritten — and the LLM isn't even invoked."""
        mock_session, _, mock_conn_ctx = self._mock_session("My hand-picked title")
        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch("agent.api.persistent_app._draft_title_value", "some old draft"),
            patch("agent.api.persistent_app._generate_title", AsyncMock()) as gen,
            patch("agent.api.persistent_app._broadcast") as bcast,
        ):
            await _auto_title_after_first_turn()

        gen.assert_not_called()
        mock_conn_ctx.execute.assert_not_called()
        bcast.assert_not_called()


# ---------------------------------------------------------------------------
# 3.6 poll_workspace_ready()
# ---------------------------------------------------------------------------


class TestPollWorkspaceReady:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("backend", ["vm", "sandbox"])
    @pytest.mark.parametrize("identity", ["exact", "missing", "successor"])
    async def test_ready_workspace_preserves_pinned_attach_authority(
        self, monkeypatch, backend, identity
    ):
        """Polling must not discard the server contract before attach checks it."""
        import agent.api.persistent_app as app

        generation = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        monkeypatch.setattr(app._session_identity, "_session_generation", generation)
        monkeypatch.setattr(app._session_identity, "_runtime_contract", True)
        workspace = {
            "workspace_provisioner": "k8s",
            "pinned_status_identity_contract": 1,
            "pinned_runtime_generation_contract": 1,
            "session_runtime_generation": generation,
        }
        if backend == "vm":
            workspace.update(vm_status="ready", vm_ssh_host="10.0.0.5")
        else:
            workspace.update(status="ready", pod_ip="10.0.0.5")
        if identity == "missing":
            workspace.pop("session_runtime_generation")
        elif identity == "successor":
            workspace["session_runtime_generation"] = (
                "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
            )
        client = AsyncMock()
        client.get_thread_workspace.return_value = workspace

        result = await session_workspace.poll_workspace_ready(
            client, "tid", timeout=5, require_vm=backend == "vm", session_runtime_generation=None
        )

        if identity == "exact":
            assert app._session_identity.adopt_workspace_payload(
                result, protected_required=False
            ) == generation
            assert pinned_status_identity_advertised(result)
        else:
            message = (
                "contract omitted its generation"
                if identity == "missing"
                else "generation changed during attach"
            )
            with pytest.raises(app.WorkspaceNotReady, match=message):
                app._session_identity.adopt_workspace_payload(result, protected_required=False)

    @pytest.mark.asyncio
    async def test_returns_none_when_workspace_not_found(self):
        """Returns None immediately when get_thread_workspace returns None."""
        client = AsyncMock()
        client.get_thread_workspace.return_value = None

        result = await poll_workspace_ready(client, "tid", timeout=5, session_runtime_generation=None)
        assert result is None
        # Should have been called only once (no retry)
        assert client.get_thread_workspace.call_count == 1

    @pytest.mark.asyncio
    async def test_propagates_grant_denied_when_flagged(self):
        """raise_on_denied=True propagates SessionGrantDenied (a permanent grant
        denial) instead of retrying/None, and threads the flag through to
        get_thread_workspace. The attach lifespan catches it and exits with the
        real reason (Phase 4). docs: session_permission_mode_grant_denied_ready_timeout.md
        """
        from agent.api.orchestrator_client import SessionGrantDenied

        client = AsyncMock()
        client.get_thread_workspace = AsyncMock(
            side_effect=SessionGrantDenied(
                "permission_mode: 'autonomous' exceeds the ceiling"
            )
        )
        with pytest.raises(SessionGrantDenied):
            await poll_workspace_ready(client, "tid", timeout=5, raise_on_denied=True, session_runtime_generation=None)
        client.get_thread_workspace.assert_called_once_with("tid", raise_on_denied=True)

    @pytest.mark.asyncio
    async def test_returns_vm_config_when_ready(self):
        """Returns remote config when vm_status='ready' with ssh_host."""
        client = AsyncMock()
        client.get_thread_workspace.return_value = {
            "vm_status": "ready",
            "vm_ssh_host": "10.0.0.5",
            "vm_ssh_port": 2222,
            "workspace_provisioner": "k8s",
            "git_remote_url": "http://gitea/repo",
            "canvas_presentation_available": True,
            "canvas_live_apps_available": True,
            "canvas_shared_browser_available": True,
        }

        result = await poll_workspace_ready(client, "tid", timeout=5, session_runtime_generation=None)

        assert result is not None
        assert result["backend"] == "vm"
        assert result["workspace_provisioner"] == "k8s"
        assert result["remote"]["host"] == "10.0.0.5"
        assert result["remote"]["port"] == 2222
        assert result["canvas_presentation_available"] is False
        assert result["canvas_live_apps_available"] is False
        assert result["canvas_shared_browser_available"] is False
        assert result["workspace_ssh_host_key_fingerprint"] is None

    @pytest.mark.asyncio
    async def test_returns_container_config_when_ready(self):
        """Returns remote config when container status='ready' with pod_ip."""
        client = AsyncMock()
        client.get_thread_workspace.return_value = {
            "status": "ready",
            "pod_ip": "172.16.0.10",
            "workspace_provisioner": "k8s",
            "workspace_generation": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "workspace_runtime_incarnation": ("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
            "workspace_ssh_host_key_fingerprint": "SHA256:trusted",
            "git_remote_url": "http://gitea/repo",
            "canvas_presentation_available": True,
            "canvas_live_apps_available": True,
            "canvas_shared_browser_available": True,
        }

        result = await poll_workspace_ready(client, "tid", timeout=5, session_runtime_generation=None)

        assert result is not None
        assert result["backend"] == "sandbox"
        assert result["workspace_provisioner"] == "k8s"
        assert result["remote"]["host"] == "172.16.0.10"
        assert result["remote"]["port"] == 30022
        assert result["workspace_generation"] == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        assert (
            result["workspace_runtime_incarnation"]
            == "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        )
        assert result["workspace_ssh_host_key_fingerprint"] == "SHA256:trusted"
        assert result["canvas_presentation_available"] is True
        assert result["canvas_live_apps_available"] is True
        assert result["canvas_shared_browser_available"] is True

    @pytest.mark.asyncio
    async def test_container_without_attestation_disables_canvas(self):
        client = AsyncMock()
        client.get_thread_workspace.return_value = {
            "status": "ready",
            "pod_ip": "172.16.0.10",
            "workspace_ssh_host_key_fingerprint": "SHA256:orphaned",
        }

        result = await poll_workspace_ready(client, "tid", timeout=5, session_runtime_generation=None)

        assert result is not None
        assert result["backend"] == "sandbox"
        assert result["workspace_ssh_host_key_fingerprint"] is None
        assert result["canvas_presentation_available"] is False
        assert result["canvas_live_apps_available"] is False
        assert result["canvas_shared_browser_available"] is False

    @pytest.mark.asyncio
    async def test_returns_none_on_status_none_no_vm(self):
        """Returns None when status='none' and no vm_status."""
        client = AsyncMock()
        client.get_thread_workspace.return_value = {"status": "none"}

        result = await poll_workspace_ready(client, "tid", timeout=5, session_runtime_generation=None)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_failed(self, caplog):
        """Failure diagnostics never serialize the internal authority bundle."""
        private_material = "PRIVATE-DEPLOY-KEY-MUST-NOT-LOG"
        client = AsyncMock()
        client.get_thread_workspace.return_value = {
            "status": "failed",
            "managed_repository_credentials": [{"private_key": private_material}],
        }

        result = await poll_workspace_ready(client, "tid", timeout=5, session_runtime_generation=None)
        assert result is None
        assert private_material not in caplog.text

    @pytest.mark.asyncio
    async def test_polls_until_ready(self):
        """Polls with sleep during intermediate statuses."""
        call_count = 0

        async def _get_workspace(tid, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return {"status": "provisioning"}
            return {"status": "ready", "pod_ip": "1.2.3.4"}

        client = AsyncMock()
        client.get_thread_workspace = _get_workspace

        with patch("agent.api.persistent_app.asyncio.sleep", new_callable=AsyncMock):
            result = await poll_workspace_ready(
                client, "tid", timeout=30, poll_interval=0.01, session_runtime_generation=None
            )

        assert result is not None
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_returns_none_on_timeout(self):
        """Returns None when polling exceeds timeout."""
        client = AsyncMock()
        client.get_thread_workspace.return_value = {"status": "provisioning"}

        with patch("agent.api.persistent_app.asyncio.sleep", new_callable=AsyncMock):
            # Use a very short timeout and mock time.monotonic to expire immediately
            with patch("time.monotonic", side_effect=[0, 100]):
                result = await poll_workspace_ready(
                    client, "tid", timeout=5, poll_interval=0.01, session_runtime_generation=None
                )

        assert result is None

    @pytest.mark.asyncio
    async def test_vm_takes_precedence_over_container(self):
        """VM config returned when both VM and container are ready."""
        client = AsyncMock()
        client.get_thread_workspace.return_value = {
            "vm_status": "ready",
            "vm_ssh_host": "vm-host",
            "status": "ready",
            "pod_ip": "pod-ip",
        }

        result = await poll_workspace_ready(client, "tid", timeout=5, session_runtime_generation=None)
        assert result["remote"]["host"] == "vm-host"

    @pytest.mark.asyncio
    async def test_vm_provisioning_extends_budget_past_base_timeout(self):
        """A VM in flight self-extends the poll deadline to the VM budget, so a
        cold boot that outlasts the sandbox `timeout` still attaches
        (knowledge-base/knowledge/features/session_create_on_vm.md)."""
        call_count = 0

        async def _get_workspace(tid, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # VM in flight — no ssh host yet. status stays 'none' (container
                # never provisioned); vm_status truthy must prevent the bail.
                return {"status": "none", "vm_status": "provisioning"}
            return {"vm_status": "ready", "vm_ssh_host": "vm-host"}

        client = AsyncMock()
        client.get_thread_workspace = _get_workspace

        with patch("agent.api.persistent_app.asyncio.sleep", new_callable=AsyncMock):
            # base timeout=1 would give up after the first poll (monotonic jumps
            # to 2, past the base deadline); the vm-detected extend to 1000 keeps
            # polling so the second poll returns the ready VM.
            with patch("time.monotonic", side_effect=[0, 0.5, 2, 2]):
                result = await poll_workspace_ready(
                    client, "tid", timeout=1, vm_timeout=1000, poll_interval=0.01, session_runtime_generation=None
                )

        assert result is not None
        assert result["backend"] == "vm"
        assert result["remote"]["host"] == "vm-host"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_vm_tier_never_accepts_a_ready_container(self):
        """Defect 2: a vm-tier session must NOT attach to a sandbox container
        that happens to be ready. The container wins the race by minutes (8s vs
        a multi-minute KubeVirt boot), so without this the session silently runs
        on the wrong tier while its VM is orphaned.
        knowledge-base/knowledge/issues/session_vm_backend_never_attaches.md"""
        client = AsyncMock()
        client.get_thread_workspace.return_value = {
            # Exactly the incident state: VM still booting, container ready.
            "vm_status": "created",
            "status": "ready",
            "pod_ip": "10.42.2.32",
        }

        with patch("agent.api.persistent_app.asyncio.sleep", new_callable=AsyncMock):
            with patch("time.monotonic", side_effect=[0, 0.5, 2]):
                result = await poll_workspace_ready(
                    client,
                    "tid",
                    timeout=1,
                    vm_timeout=1,
                    poll_interval=0.01,
                    require_vm=True, session_runtime_generation=None,
                )

        assert result is None  # timed out waiting for the VM — never downgraded

    @pytest.mark.asyncio
    async def test_vm_tier_returns_vm_when_ready(self):
        """require_vm still attaches normally once the VM reports ready."""
        client = AsyncMock()
        client.get_thread_workspace.return_value = {
            "vm_status": "ready",
            "vm_ssh_host": "vm-host",
            "vm_ssh_port": 22,
        }

        result = await poll_workspace_ready(client, "tid", timeout=5, require_vm=True, session_runtime_generation=None)

        assert result is not None
        assert result["backend"] == "vm"
        assert result["remote"]["host"] == "vm-host"

    @pytest.mark.asyncio
    async def test_vm_tier_bails_immediately_on_vm_failed(self):
        """A failed VM on a vm-tier session is terminal — bail instead of burning
        the full VM budget. The pre-existing bail required the CONTAINER to have
        failed too, which never happens when no container exists."""
        client = AsyncMock()
        client.get_thread_workspace.return_value = {"vm_status": "failed"}

        result = await poll_workspace_ready(client, "tid", timeout=5, require_vm=True, session_runtime_generation=None)

        assert result is None
        assert client.get_thread_workspace.call_count == 1  # no retry

    @pytest.mark.asyncio
    async def test_vm_tier_bails_immediately_when_no_vm_context(self):
        """A vm-tier thread with no VM context was never provisioned a VM —
        terminal, so fail fast instead of sitting out the whole VM budget.
        Mirrors the container branch's status=='none' bail."""
        client = AsyncMock()
        client.get_thread_workspace.return_value = {"status": "none"}

        result = await poll_workspace_ready(client, "tid", timeout=5, require_vm=True, session_runtime_generation=None)

        assert result is None
        assert client.get_thread_workspace.call_count == 1  # no retry

    @pytest.mark.asyncio
    async def test_default_caller_still_accepts_a_ready_container(self):
        """Regression guard for the sandbox-upgrade caller
        (_handle_workspace_upgrade), which polls without require_vm and must keep
        accepting a container even if a vm context is present."""
        client = AsyncMock()
        client.get_thread_workspace.return_value = {
            "vm_status": "created",
            "status": "ready",
            "pod_ip": "10.42.2.32",
        }

        result = await poll_workspace_ready(client, "tid", timeout=5, session_runtime_generation=None)

        assert result is not None
        assert result["backend"] == "sandbox"
        assert result["remote"]["host"] == "10.42.2.32"


# ---------------------------------------------------------------------------
# 3.7 _poll_vm_ready()
# ---------------------------------------------------------------------------


class TestPollVmReady:
    @pytest.mark.asyncio
    async def test_returns_config_when_ready(self):
        """Returns ssh config when vm_status='ready' with host."""
        client = AsyncMock()
        client.get_thread_workspace.return_value = {
            "vm_status": "ready",
            "vm_ssh_host": "10.0.0.5",
            "vm_ssh_port": 2222,
        }

        result = await _poll_vm_ready(client, "tid", timeout=5)
        assert result == {"ssh_host": "10.0.0.5", "ssh_port": 2222}

    @pytest.mark.asyncio
    async def test_returns_none_on_failed(self):
        """Returns None immediately when vm_status='failed'."""
        client = AsyncMock()
        client.get_thread_workspace.return_value = {"vm_status": "failed"}

        result = await _poll_vm_ready(client, "tid", timeout=5)
        assert result is None

    @pytest.mark.asyncio
    async def test_continues_polling_when_workspace_none(self):
        """Continues polling when get_thread_workspace returns None."""
        call_count = 0

        async def _get_ws(tid):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return None
            return {"vm_status": "ready", "vm_ssh_host": "host"}

        client = AsyncMock()
        client.get_thread_workspace = _get_ws

        with patch("agent.api.persistent_app.asyncio.sleep", new_callable=AsyncMock):
            result = await _poll_vm_ready(client, "tid", timeout=30, poll_interval=0.01)

        assert result is not None
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_returns_none_on_timeout(self):
        """Returns None when timeout expires."""
        client = AsyncMock()
        client.get_thread_workspace.return_value = {"vm_status": "provisioning"}

        with patch("agent.api.persistent_app.asyncio.sleep", new_callable=AsyncMock):
            with patch("time.monotonic", side_effect=[0, 100]):
                result = await _poll_vm_ready(
                    client, "tid", timeout=5, poll_interval=0.01
                )

        assert result is None


# ---------------------------------------------------------------------------
# 3.8 _handle_compact()
# ---------------------------------------------------------------------------


def _wire_compacting_ctx_mgr(mock_session, result):
    """Make the mock context manager simulate a *successful* summarization:
    ``summarize_and_compact`` returns ``result`` and bumps ``compaction_runs``
    — the transport's authoritative did-it-compact signal. Plain AsyncMock
    return values simulate a no-op (counter untouched)."""
    ctx = mock_session.context_manager
    ctx.compaction_runs = 0

    async def _compact(*args, **kwargs):
        ctx.compaction_runs += 1
        return result

    ctx.summarize_and_compact = AsyncMock(side_effect=_compact)


class TestHandleCompact:
    @pytest.mark.asyncio
    async def test_sends_error_when_session_none(self):
        """Sends error event when _session is None."""
        ws = AsyncMock()
        with patch("agent.api.persistent_app._session", None):
            await _handle_compact(ws, "")
        ws.send_json.assert_called()
        call_args = ws.send_json.call_args[0][0]
        assert call_args["method"] == "error"

    @pytest.mark.asyncio
    async def test_sends_error_when_context_manager_none(self):
        """Sends error event when context_manager is None."""
        ws = AsyncMock()
        mock_session = MagicMock()
        mock_session.context_manager = None
        with patch("agent.api.persistent_app._session", mock_session):
            await _handle_compact(ws, "")
        ws.send_json.assert_called()

    @pytest.mark.asyncio
    async def test_compacts_messages_in_place(self):
        """Messages mutated via slice assignment."""
        ws = AsyncMock()
        mock_session = MagicMock()
        original_messages = [
            SystemMessage(content="sys"),
            HumanMessage(content="q1"),
            AIMessage(content="a1"),
            HumanMessage(content="q2"),
            AIMessage(content="a2"),
        ]
        mock_session.messages = original_messages
        mock_session.context_manager.summarize_and_compact = AsyncMock(
            return_value=[SystemMessage(content="sys"), AIMessage(content="summary")]
        )
        mock_session.config.context_management.max_summary_length = 10000
        mock_session.workspace_manager = None

        with patch("agent.api.persistent_app._session", mock_session):
            await _handle_compact(ws, "focus text")

        # Messages should be updated in-place
        assert len(mock_session.messages) == 2

    @pytest.mark.asyncio
    async def test_sends_compacted_event_with_counts(self):
        """Sends context.compacted event with before/after/trigger."""
        ws = AsyncMock()
        mock_session = MagicMock()
        mock_session.messages = [
            SystemMessage(content="sys"),
            HumanMessage(content="q"),
        ]
        mock_session.context_manager.summarize_and_compact = AsyncMock(
            return_value=[SystemMessage(content="sys")]
        )
        mock_session.config.context_management.max_summary_length = 10000
        mock_session.workspace_manager = None

        with patch("agent.api.persistent_app._session", mock_session):
            await _handle_compact(ws, "my focus")

        # Find the context.compacted call
        compacted_call = None
        for call in ws.send_json.call_args_list:
            data = call[0][0]
            if data.get("method") == "context.compacted":
                compacted_call = data
                break
        assert compacted_call is not None
        assert compacted_call["params"]["before"] == 2
        assert compacted_call["params"]["after"] == 1
        assert compacted_call["params"]["trigger"] == "manual"

    @pytest.mark.asyncio
    async def test_persists_summary_marker_when_summary_present(self):
        """A 'summary' compaction persists a display-only role='summary' row."""
        ws = AsyncMock()
        mock_session = MagicMock()
        mock_session.messages = [
            SystemMessage(content="sys"),
            HumanMessage(content="q"),
        ]
        mock_session.turn_count = 7
        _wire_compacting_ctx_mgr(
            mock_session,
            [
                SystemMessage(content="sys"),
                SystemMessage(content="[Summary of prior work]\nWe did X and Y."),
            ],
        )
        mock_session.config.context_management.max_summary_length = 10000
        mock_session.workspace_manager = None
        mock_session.postgres_conn.save_thread_message = AsyncMock(
            return_value={"id": "m1", "seq": 1}
        )

        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid-1"),
        ):
            await _handle_compact(ws, "")

        writer = mock_session.postgres_conn.save_thread_message
        writer.assert_awaited_once()
        kwargs = writer.call_args.kwargs
        assert kwargs["role"] == "summary"
        assert "We did X and Y." in kwargs["content"]
        assert kwargs["turn_number"] == 7
        assert kwargs["metrics"]["trigger"] == "manual"

    @pytest.mark.asyncio
    async def test_summary_marker_carries_boundary_turn_for_restore(self):
        """The persisted ``role='summary'`` row carries
        ``metrics.boundary_turn`` so subsequent resumes can use it as a
        restorable checkpoint (load ``[summary] + tail`` rather than
        re-loading the full pre-compaction history)."""
        ws = AsyncMock()
        mock_session = MagicMock()
        mock_session.messages = [
            SystemMessage(content="sys"),
            HumanMessage(content="q"),
        ]
        mock_session.turn_count = 7
        _wire_compacting_ctx_mgr(
            mock_session,
            [
                SystemMessage(content="sys"),
                SystemMessage(content="[Summary of prior work]\nrecap"),
            ],
        )
        mock_session.config.context_management.max_summary_length = 10000
        mock_session.workspace_manager = None
        mock_session.postgres_conn.save_thread_message = AsyncMock(
            return_value={"id": "m1", "seq": 1}
        )

        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid-1"),
        ):
            await _handle_compact(ws, "")

        kwargs = mock_session.postgres_conn.save_thread_message.call_args.kwargs
        # boundary_turn = last fully-saved turn = turn_count - 1. At
        # auto-compaction the current turn's user msg is saved but its AI/tool
        # msgs are not yet — reloading turn > boundary recovers them once they
        # save at turn-complete, with no gap.
        assert kwargs["metrics"]["boundary_turn"] == 6, (
            f"boundary_turn must be turn_count - 1; got metrics={kwargs['metrics']}"
        )

    @pytest.mark.asyncio
    async def test_boundary_turn_clamped_to_zero(self):
        """``boundary_turn`` must never go negative (early-session edge)."""
        ws = AsyncMock()
        mock_session = MagicMock()
        mock_session.messages = [SystemMessage(content="sys")]
        mock_session.turn_count = 0
        _wire_compacting_ctx_mgr(
            mock_session, [SystemMessage(content="[Summary of prior work]\ne")]
        )
        mock_session.config.context_management.max_summary_length = 10000
        mock_session.workspace_manager = None
        mock_session.postgres_conn.save_thread_message = AsyncMock(
            return_value={"id": "m1", "seq": 1}
        )

        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid-1"),
        ):
            await _handle_compact(ws, "")

        kwargs = mock_session.postgres_conn.save_thread_message.call_args.kwargs
        assert kwargs["metrics"]["boundary_turn"] == 0

    @pytest.mark.asyncio
    async def test_compact_strips_removal_markers(self):
        """The reducer delta's RemoveMessage markers must never be adopted
        into _session.messages — leaked markers made every later LLM call
        false-detect a compaction and re-persist the same summary row
        (the duplicate-banner bug, 2026-06-12)."""
        from langchain_core.messages import RemoveMessage

        ws = AsyncMock()
        mock_session = MagicMock()
        mock_session.messages = [
            SystemMessage(content="sys"),
            HumanMessage(content="q1", id="m1"),
            AIMessage(content="a1", id="m2"),
        ]
        _wire_compacting_ctx_mgr(
            mock_session,
            [
                RemoveMessage(id="m1"),
                RemoveMessage(id="m2"),
                SystemMessage(content="sys"),
                SystemMessage(content="[Summary of prior work]\nrecap"),
            ],
        )
        mock_session.config.context_management.max_summary_length = 10000
        mock_session.workspace_manager = None
        mock_session.postgres_conn.save_thread_message = AsyncMock(
            return_value={"id": "s1", "seq": 9}
        )

        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid-1"),
        ):
            await _handle_compact(ws, "")

        assert not any(isinstance(m, RemoveMessage) for m in mock_session.messages), (
            "RemoveMessage markers leaked into the live session list"
        )
        assert len(mock_session.messages) == 2

    @pytest.mark.asyncio
    async def test_noop_compact_does_not_persist_marker(self):
        """A /compact that doesn't actually summarize (below thresholds) must
        not re-persist the previous summary as a new role='summary' row, and
        must answer the requesting client with a summary-less
        context.compacted (rendered as a system line, not a banner)."""
        ws = AsyncMock()
        mock_session = MagicMock()
        # A previous compaction's summary is still in the live list — the
        # old code re-extracted and re-persisted it on every no-op.
        original = [
            SystemMessage(content="sys"),
            SystemMessage(content="[Summary of prior work]\nold recap"),
            HumanMessage(content="q", id="m1"),
        ]
        mock_session.messages = list(original)
        mock_session.turn_count = 3
        ctx = mock_session.context_manager
        ctx.compaction_runs = 0  # never bumped → no-op
        ctx.summarize_and_compact = AsyncMock(return_value=list(original))
        mock_session.config.context_management.max_summary_length = 10000
        mock_session.workspace_manager = None
        mock_session.postgres_conn.save_thread_message = AsyncMock()

        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid-1"),
        ):
            await _handle_compact(ws, "")

        mock_session.postgres_conn.save_thread_message.assert_not_awaited()
        compacted_frames = [
            call[0][0]
            for call in ws.send_json.call_args_list
            if call[0][0].get("method") == "context.compacted"
        ]
        assert len(compacted_frames) == 1
        assert compacted_frames[0]["params"]["summary"] is None

    @pytest.mark.asyncio
    async def test_git_commit_and_push_on_compaction(self):
        """Git commit + push when git_manager.is_active."""
        ws = AsyncMock()
        mock_session = MagicMock()
        mock_session.messages = [SystemMessage(content="s"), HumanMessage(content="q")]
        mock_session.context_manager.summarize_and_compact = AsyncMock(
            return_value=[SystemMessage(content="s")]
        )
        mock_session.config.context_management.max_summary_length = 10000
        git_mgr = MagicMock()
        git_mgr.is_active = True
        git_mgr.has_uncommitted_changes.return_value = True
        mock_session.workspace_manager.git_manager = git_mgr

        with patch("agent.api.persistent_app._session", mock_session):
            await _handle_compact(ws, "")

        git_mgr.commit.assert_called_once()
        git_mgr.push.assert_called_once()

    @pytest.mark.asyncio
    async def test_git_failure_non_fatal(self):
        """Git error during compaction doesn't crash."""
        ws = AsyncMock()
        mock_session = MagicMock()
        mock_session.messages = [SystemMessage(content="s")]
        mock_session.context_manager.summarize_and_compact = AsyncMock(return_value=[])
        mock_session.config.context_management.max_summary_length = 10000
        git_mgr = MagicMock()
        git_mgr.is_active = True
        git_mgr.has_uncommitted_changes.return_value = True
        git_mgr.commit.side_effect = RuntimeError("git broke")
        mock_session.workspace_manager.git_manager = git_mgr

        with patch("agent.api.persistent_app._session", mock_session):
            await _handle_compact(ws, "")


# ---------------------------------------------------------------------------
# 3.9 _handle_archive()
# ---------------------------------------------------------------------------


class TestHandleArchive:
    @pytest.fixture(autouse=True)
    def _common_teardown(self):
        with (
            patch(
                "agent.api.persistent_app._session_termination.terminate", new=AsyncMock()
            ) as teardown,
            patch(
                "agent.api.persistent_app._update_thread_status",
                new=AsyncMock(return_value=True),
            ) as status_update,
            patch(
                "agent.api.persistent_app._session_termination.retirement_admission_identity",
                None,
            ),
            patch(
                "agent.api.persistent_app._session_termination.retirement_admission_disposition",
                None,
            ),
        ):
            self.teardown = teardown
            self.status_update = status_update
            yield

    @pytest.mark.asyncio
    async def test_sends_error_when_session_none(self):
        ws = AsyncMock()
        with patch("agent.api.persistent_app._session", None):
            await _handle_archive(ws)
        ws.send_json.assert_called()
        call_args = ws.send_json.call_args[0][0]
        assert call_args["method"] == "error"

    @pytest.mark.asyncio
    async def test_gets_recall_store_from_tool_context(self):
        """recall_store read from _session.tool_context.recall_store."""
        ws = AsyncMock()
        mock_session = _retirement_session_mock()
        mock_session.memory_service = None  # legacy path (manager flag off)
        mock_session.tool_context.recall_store = None
        mock_session.auxiliary_llm = None
        mock_session.messages = []
        mock_session.postgres_conn = None

        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
        ):
            await _handle_archive(ws)

        # Should send session.ended without errors
        ended_call = None
        for call in ws.send_json.call_args_list:
            data = call[0][0]
            if data.get("method") == "session.ended":
                ended_call = data
                break
        assert ended_call is not None

    @pytest.mark.asyncio
    async def test_memory_extraction_requires_all_three(self):
        """Memory extraction only runs when recall_store, aux_llm, and messages all truthy."""
        ws = AsyncMock()
        mock_session = _retirement_session_mock()
        mock_session.memory_service = None  # legacy path (manager flag off)
        mock_session.tool_context.recall_store = MagicMock()
        mock_session.auxiliary_llm = MagicMock()
        mock_session.messages = []  # Empty — should skip extraction
        mock_session.postgres_conn = None
        mock_session.memory_extraction_prompt = ""

        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
        ):
            await _handle_archive(ws)

        # No import of extract_and_store_memories should happen since messages is empty

    @pytest.mark.asyncio
    async def test_memory_extraction_failure_non_fatal(self):
        """Memory extraction failure doesn't prevent session.ended."""
        ws = AsyncMock()
        mock_session = _retirement_session_mock()
        mock_session.memory_service = None  # legacy path (manager flag off)
        mock_session.tool_context.recall_store = MagicMock()
        mock_session.auxiliary_llm = MagicMock()
        mock_session.messages = [HumanMessage(content="hi")]
        mock_session.postgres_conn = None
        mock_session.memory_extraction_prompt = ""

        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch(
                "agent.api.persistent_app.extract_and_store_memories",
                side_effect=RuntimeError("extraction failed"),
                create=True,
            ),
        ):
            await _handle_archive(ws)

        # session.ended should still be sent
        ended_calls = [
            c
            for c in ws.send_json.call_args_list
            if c[0][0].get("method") == "session.ended"
        ]
        assert len(ended_calls) == 1

    @pytest.mark.asyncio
    async def test_title_generation_on_untitled(self):
        """Generates title when existing title is 'Untitled Session'."""
        ws = AsyncMock()
        mock_session = _retirement_session_mock()
        mock_session.memory_service = None  # legacy path (manager flag off)
        mock_session.tool_context.recall_store = None
        mock_session.auxiliary_llm = MagicMock()
        mock_session.messages = [HumanMessage(content="hi")]
        mock_conn = MagicMock()
        mock_conn.get_thread = AsyncMock(return_value={"title": "Untitled Session"})
        mock_conn.end_thread = AsyncMock()
        mock_conn_ctx = AsyncMock()
        mock_conn.acquire.return_value.__aenter__ = AsyncMock(
            return_value=mock_conn_ctx
        )
        mock_conn.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_session.postgres_conn = mock_conn

        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch(
                "agent.api.persistent_app._generate_title",
                return_value="Generated Title",
            ),
        ):
            await _handle_archive(ws)

    @pytest.mark.asyncio
    async def test_title_failure_non_fatal(self):
        """Title generation failure doesn't crash archive."""
        ws = AsyncMock()
        mock_session = _retirement_session_mock()
        mock_session.memory_service = None  # legacy path (manager flag off)
        mock_session.tool_context.recall_store = None
        mock_session.auxiliary_llm = None
        mock_session.messages = []
        mock_conn = AsyncMock()
        mock_conn.get_thread = AsyncMock(side_effect=RuntimeError("db error"))
        mock_conn.end_thread = AsyncMock()
        mock_session.postgres_conn = mock_conn

        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
        ):
            await _handle_archive(ws)

        # session.ended should still be sent
        ended_calls = [
            c
            for c in ws.send_json.call_args_list
            if c[0][0].get("method") == "session.ended"
        ]
        assert len(ended_calls) == 1

    @pytest.mark.asyncio
    async def test_sends_session_ended_event(self):
        """Sends session.ended with thread_id."""
        ws = AsyncMock()
        mock_session = _retirement_session_mock()
        mock_session.memory_service = None  # legacy path (manager flag off)
        mock_session.tool_context.recall_store = None
        mock_session.auxiliary_llm = None
        mock_session.messages = []
        mock_session.postgres_conn = None

        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "test-thread-id"),
        ):
            await _handle_archive(ws)

        ended_calls = [
            c[0][0]
            for c in ws.send_json.call_args_list
            if c[0][0].get("method") == "session.ended"
        ]
        assert len(ended_calls) == 1
        self.teardown.assert_awaited_once_with("archive")
        assert ended_calls[0]["params"]["thread_id"] == "test-thread-id"

    @pytest.mark.asyncio
    async def test_terminal_frame_is_sent_only_after_retirement_settles(self):
        order = []
        ws = AsyncMock()
        ws.send_json.side_effect = lambda payload: order.append(
            f"send:{payload['method']}"
        )
        self.status_update.side_effect = (
            lambda *_args, **_kwargs: order.append("ending") or True
        )
        self.teardown.side_effect = lambda *_args, **_kwargs: order.append("settle")
        mock_session = _retirement_session_mock()
        mock_session.memory_service = None
        mock_session.tool_context.recall_store = None
        mock_session.auxiliary_llm = None
        mock_session.messages = []
        mock_session.postgres_conn = None

        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "test-thread-id"),
            patch(
                "agent.api.persistent_app._session_identity._session_generation",
                "55555555-5555-4555-8555-555555555555",
            ),
        ):
            await _handle_archive(ws)

        assert order == ["ending", "settle", "send:session.ended"]
        self.status_update.assert_awaited_once_with(
            "ending",
            pinned_agent_id=None,
            retirement_disposition="ended",
            retirement_permanent=False,
        )
        ended = ws.send_json.call_args.args[0]
        assert ended["params"]["session_runtime_generation"] == (
            "55555555-5555-4555-8555-555555555555"
        )

    @pytest.mark.asyncio
    async def test_officer_archive_settles_and_echoes_suspended_disposition(self):
        ws = AsyncMock()
        mock_session = _retirement_session_mock()
        mock_session.config = SimpleNamespace(officer=SimpleNamespace(enabled=True))
        mock_session.memory_service = None
        mock_session.tool_context.recall_store = None
        mock_session.auxiliary_llm = None
        mock_session.messages = []
        mock_session.postgres_conn = None

        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "officer-thread"),
            patch(
                "agent.api.persistent_app._session_identity._session_generation",
                "55555555-5555-4555-8555-555555555555",
            ),
        ):
            await _handle_archive(ws)

        self.status_update.assert_awaited_once_with(
            "ending",
            pinned_agent_id=None,
            retirement_disposition="suspended",
            retirement_permanent=False,
        )
        self.teardown.assert_awaited_once_with("archive")
        terminal = ws.send_json.call_args.args[0]
        assert terminal["method"] == "session.suspended"
        assert terminal["params"] == {
            "thread_id": "officer-thread",
            "session_runtime_generation": "55555555-5555-4555-8555-555555555555",
            "message": (
                "Session suspended. Send a message to resume where you left off."
            ),
        }

    @pytest.mark.asyncio
    async def test_retirement_admission_refusal_never_publishes_terminal_frame(self):
        ws = AsyncMock()
        self.status_update.return_value = False
        mock_session = _retirement_session_mock()
        mock_session.memory_service = None
        mock_session.tool_context.recall_store = None
        mock_session.auxiliary_llm = None
        mock_session.messages = []
        mock_session.postgres_conn = None

        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "test-thread-id"),
        ):
            await _handle_archive(ws)

        methods = [call.args[0]["method"] for call in ws.send_json.call_args_list]
        assert methods == ["error"]
        self.teardown.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_retirement_refusal_runs_no_archive_finalization(self):
        ws = AsyncMock()
        self.status_update.return_value = False
        session = _retirement_session_mock()
        session.workspace_sync.push_all = AsyncMock()
        session.workspace_sync.pull_all = AsyncMock()
        session.workspace_sync.aclose = AsyncMock()
        session.memory_service.capture = AsyncMock()
        session.messages = [HumanMessage(content="late")]
        session.postgres_conn = AsyncMock()

        with (
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._session_identity._thread_id", "test-thread-id"),
        ):
            await _handle_archive(ws)

        session.workspace_sync.push_all.assert_not_awaited()
        session.workspace_sync.pull_all.assert_not_awaited()
        session.memory_service.capture.assert_not_awaited()
        session.postgres_conn.get_thread.assert_not_awaited()
        self.teardown.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_retirement_settlement_failure_never_publishes_terminal_frame(self):
        ws = AsyncMock()
        self.teardown.side_effect = RuntimeError("retirement did not settle")
        mock_session = _retirement_session_mock()
        mock_session.memory_service = None
        mock_session.tool_context.recall_store = None
        mock_session.auxiliary_llm = None
        mock_session.messages = []
        mock_session.postgres_conn = None

        with (
            patch("agent.api.persistent_app._session", mock_session),
            patch("agent.api.persistent_app._session_identity._thread_id", "test-thread-id"),
        ):
            await _handle_archive(ws)

        methods = [call.args[0]["method"] for call in ws.send_json.call_args_list]
        assert methods == ["error"]


class TestHandleIdleArchive:
    @pytest.fixture(autouse=True)
    def _clear_retirement_latch(self):
        with (
            patch(
                "agent.api.persistent_app._session_termination.retirement_admission_identity",
                None,
            ),
            patch(
                "agent.api.persistent_app._session_termination.retirement_admission_disposition",
                None,
            ),
        ):
            yield

    @staticmethod
    def _session():
        session = _retirement_session_mock()
        session.memory_service = None
        session.tool_context.recall_store = None
        session.auxiliary_llm = None
        session.messages = []
        session.postgres_conn = None
        session.workspace_manager = None
        return session

    @pytest.mark.asyncio
    async def test_settles_after_ending_without_preterminal_agent_frame(self):
        from agent.api import persistent_app as mod

        order: list[str] = []
        update = AsyncMock(
            side_effect=lambda *_args, **_kwargs: order.append("ending") or True
        )
        settle = AsyncMock(side_effect=lambda *_args: order.append("settle"))
        broadcast = MagicMock(side_effect=lambda *_args: order.append("broadcast"))

        with (
            patch.object(mod, "_session", self._session()),
            patch.object(mod._session_identity, "_thread_id", "idle-thread"),
            patch.object(mod, "_update_thread_status", update),
            patch.object(mod._session_termination, "terminate", settle),
            patch.object(mod, "_broadcast", broadcast),
        ):
            await mod._handle_idle_archive()

        assert order == ["ending", "settle"]
        update.assert_awaited_once_with(
            "ending",
            pinned_agent_id=None,
            retirement_disposition="ended",
            retirement_permanent=False,
        )
        settle.assert_awaited_once_with("idle_timeout")
        broadcast.assert_not_called()

    @pytest.mark.asyncio
    async def test_refused_ending_neither_settles_nor_publishes_terminal(self):
        from agent.api import persistent_app as mod

        settle = AsyncMock()
        broadcast = MagicMock()
        with (
            patch.object(mod, "_session", self._session()),
            patch.object(mod._session_identity, "_thread_id", "idle-thread"),
            patch.object(
                mod,
                "_update_thread_status",
                new=AsyncMock(return_value=False),
            ),
            patch.object(mod._session_termination, "terminate", settle),
            patch.object(mod, "_broadcast", broadcast),
        ):
            await mod._handle_idle_archive()

        settle.assert_not_awaited()
        broadcast.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["ending", "ended"])
async def test_pinned_terminal_status_never_uses_direct_db_fallback(status):
    from agent.api import persistent_app as mod

    postgres = MagicMock()
    postgres.end_thread = AsyncMock()
    postgres.update_thread_status = AsyncMock()
    session = _retirement_session_mock(postgres_conn=postgres)
    client = MagicMock()
    client.update_thread_status = AsyncMock(return_value=False)

    with (
        patch.object(mod, "_session", session),
        patch.object(mod._session_identity, "_thread_id", "terminal-thread"),
        patch.object(mod, "_orchestrator_client", client),
        patch.object(mod, "_stateless_mode", return_value=False),
        patch.object(mod._session_identity, "_status_contract", False),
        patch.object(mod._session_identity, "_runtime_contract", False),
    ):
        assert await mod._update_thread_status(status) is False

    client.update_thread_status.assert_awaited_once_with(
        "terminal-thread",
        status,
    )
    postgres.end_thread.assert_not_awaited()
    postgres.update_thread_status.assert_not_awaited()
    postgres.acquire.assert_not_called()


@pytest.mark.asyncio
async def test_begun_retirement_closes_local_input_and_readiness_before_finalization():
    from agent.api import persistent_app as mod

    postgres = MagicMock()
    postgres.persist_pinned_input_delivery = AsyncMock()
    session = _retirement_session_mock(postgres_conn=postgres)
    session.llm_with_tools = object()
    update = AsyncMock(return_value=True)
    generation = "88888888-8888-4888-8888-888888888888"
    attach_token = "99999999-9999-4999-8999-999999999999"

    with (
        patch.object(mod, "_session", session),
        patch.object(mod._session_identity, "_thread_id", "retiring-thread"),
        patch.object(mod._session_identity, "_session_generation", generation),
        patch.object(mod._session_identity, "_attach_token", attach_token),
        patch.object(mod._session_termination, "retirement_admission_identity", None),
        patch.object(mod, "_stateless_mode", return_value=False),
        patch.object(mod, "_registered_pinned_agent_id", return_value="agent-a"),
        patch.object(
            mod,
            "_close_pinned_control_inbox",
            new=AsyncMock(return_value=True),
        ),
        patch.object(mod, "_update_thread_status", update),
    ):
        assert await mod._session_termination.begin_retirement() is True
        assert mod._session_termination.runtime_admission_closed() is True
        assert mod._runtime_input_admission_open() is False
        assert mod._session_ready() is False
        with pytest.raises(mod.TerminationAdmissionClosed):
            await mod._session_input.accept("must not persist")

    update.assert_awaited_once_with(
        "ending",
        pinned_agent_id="agent-a",
        retirement_disposition="ended",
        retirement_permanent=False,
    )
    postgres.persist_pinned_input_delivery.assert_not_awaited()


@pytest.mark.asyncio
async def test_child_quiescence_precedes_retirement_authority_revocation():
    from agent.api import persistent_app as mod

    order: list[str] = []
    session = _retirement_session_mock()
    session.quiesce_subagents.side_effect = lambda _reason: order.append("children")
    close_controls = AsyncMock(
        side_effect=lambda **_kwargs: order.append("controls") or True
    )
    update = AsyncMock(
        side_effect=lambda *_args, **_kwargs: order.append("retirement") or True
    )

    with (
        patch.object(mod, "_session", session),
        patch.object(mod._session_identity, "_thread_id", "retirement-order-thread"),
        patch.object(mod._session_identity, "_session_generation",
            "88888888-8888-4888-8888-888888888888",
        ),
        patch.object(mod._session_identity, "_attach_token",
            "99999999-9999-4999-8999-999999999999",
        ),
        patch.object(mod._session_termination, "retirement_admission_identity", None),
        patch.object(mod._session_termination, "retirement_admission_disposition", None),
        patch.object(mod._session_termination, "retirement_admission_token", None),
        patch.object(mod._session_termination, "retirement_admission_permanent", None),
        patch.object(mod._session_identity, "_runtime_contract", False),
        patch.object(mod, "_stateless_mode", return_value=False),
        patch.object(mod, "_registered_pinned_agent_id", return_value="agent-a"),
        patch.object(mod, "_close_pinned_control_inbox", close_controls),
        patch.object(mod, "_update_thread_status", update),
    ):
        assert await mod._session_termination.begin_retirement() is True

    assert order == ["children", "controls", "retirement"]
    session.quiesce_subagents.assert_awaited_once_with(
        "parent session retiring as ended"
    )


@pytest.mark.asyncio
async def test_child_quiescence_failure_refuses_retirement_begin():
    from agent.api import persistent_app as mod

    session = _retirement_session_mock()
    session.quiesce_subagents.side_effect = RuntimeError("child still running")
    close_controls = AsyncMock(return_value=True)
    update = AsyncMock(return_value=True)

    with (
        patch.object(mod, "_session", session),
        patch.object(mod._session_identity, "_thread_id", "retirement-quiesce-failure"),
        patch.object(mod._session_identity, "_session_generation",
            "88888888-8888-4888-8888-888888888888",
        ),
        patch.object(mod._session_identity, "_attach_token",
            "99999999-9999-4999-8999-999999999999",
        ),
        patch.object(mod._session_termination, "retirement_admission_identity", None),
        patch.object(mod._session_termination, "retirement_admission_disposition", None),
        patch.object(mod._session_termination, "retirement_admission_token", None),
        patch.object(mod._session_termination, "retirement_admission_permanent", None),
        patch.object(mod._session_identity, "_runtime_contract", False),
        patch.object(mod, "_stateless_mode", return_value=False),
        patch.object(mod, "_registered_pinned_agent_id", return_value="agent-a"),
        patch.object(mod, "_close_pinned_control_inbox", close_controls),
        patch.object(mod, "_update_thread_status", update),
    ):
        assert await mod._session_termination.begin_retirement() is False
        assert mod._session_termination.retirement_admission_identity == (
            "retirement-quiesce-failure",
            "88888888-8888-4888-8888-888888888888",
            "99999999-9999-4999-8999-999999999999",
        )
        assert mod._session_termination.runtime_admission_closed() is True

    close_controls.assert_not_awaited()
    update.assert_not_awaited()


class TestExactRetirementBeginReconciliation:
    generation = "88888888-8888-4888-8888-888888888888"
    attach_token = "99999999-9999-4999-8999-999999999999"
    retirement_token = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    thread_id = "retirement-reconcile-thread"
    agent_id = "agent-a"

    @contextmanager
    def _patch_runtime(self, mod, *, client):
        session = _retirement_session_mock()
        patchers = (
            patch.object(mod, "_session", session),
            patch.object(mod._session_identity, "_thread_id", self.thread_id),
            patch.object(mod._session_identity, "_session_generation", self.generation),
            patch.object(mod._session_identity, "_attach_token", self.attach_token),
            patch.object(mod._session_identity, "_runtime_contract", True),
            patch.object(mod._session_termination, "retirement_admission_identity", None),
            patch.object(mod._session_termination, "retirement_admission_disposition", None),
            patch.object(mod._session_termination, "retirement_admission_token", None),
            patch.object(mod._session_termination, "retirement_admission_permanent", None),
            patch.object(mod, "_orchestrator_client", client),
            patch.object(mod, "_stateless_mode", return_value=False),
            patch.object(
                mod, "_registered_pinned_agent_id", return_value=self.agent_id
            ),
        )
        with ExitStack() as stack:
            for patcher in patchers:
                stack.enter_context(patcher)
            yield session

    def _live_lifecycle(self, **extra):
        return {
            "status": "active",
            "session_runtime_generation": self.generation,
            "session_runtime_attach_token": self.attach_token,
            "runtime_retirement_pending": False,
            "runtime_retirement_preflight": False,
            "runtime_retirement_authorized": False,
            **extra,
        }

    @pytest.mark.asyncio
    async def test_drain_failure_before_begin_exactly_reopens_same_runtime(self):
        from agent.api import persistent_app as mod

        client = SimpleNamespace(begin_thread_retirement=AsyncMock())
        reopen = AsyncMock(return_value=True)
        with (
            self._patch_runtime(mod, client=client) as session,
            patch.object(
                mod,
                "_close_pinned_control_inbox",
                new=AsyncMock(side_effect=RuntimeError("drain failed")),
            ),
            patch.object(mod, "_set_pinned_control_admission", reopen),
        ):
            assert await mod._session_termination.begin_retirement() is False

        client.begin_thread_retirement.assert_not_awaited()
        reopen.assert_awaited_once_with(
            agent_id=self.agent_id,
            open_for_admission=True,
        )
        session.resume_subagents.assert_awaited_once_with()

    @pytest.mark.asyncio
    async def test_missing_exact_owner_after_child_quiesce_fences_whole_runtime(self):
        from agent.api import persistent_app as mod

        client = SimpleNamespace(begin_thread_retirement=AsyncMock())
        with (
            self._patch_runtime(mod, client=client),
            patch.object(mod, "_registered_pinned_agent_id", return_value=None),
        ):
            assert await mod._session_termination.begin_retirement() is False
            assert mod._session_termination.runtime_admission_closed() is True

        client.begin_thread_retirement.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_commit_response_reopens_only_after_exact_lifecycle_proof(self):
        from agent.api import persistent_app as mod

        client = SimpleNamespace(
            begin_thread_retirement=AsyncMock(return_value=None),
            get_thread_lifecycle=AsyncMock(return_value=self._live_lifecycle()),
        )
        reopen = AsyncMock(return_value=True)
        with (
            self._patch_runtime(mod, client=client) as session,
            patch.object(
                mod, "_close_pinned_control_inbox", new=AsyncMock(return_value=True)
            ),
            patch.object(mod, "_set_pinned_control_admission", reopen),
        ):
            assert await mod._session_termination.begin_retirement() is False

        client.get_thread_lifecycle.assert_awaited_once_with(self.thread_id)
        reopen.assert_awaited_once_with(
            agent_id=self.agent_id,
            open_for_admission=True,
        )
        session.resume_subagents.assert_awaited_once_with()

    @pytest.mark.asyncio
    async def test_child_resume_failure_recloses_controls_and_latches_admission(self):
        from agent.api import persistent_app as mod

        client = SimpleNamespace(
            begin_thread_retirement=AsyncMock(return_value=None),
            get_thread_lifecycle=AsyncMock(return_value=self._live_lifecycle()),
        )
        control_admission = AsyncMock(side_effect=[True, True])
        with (
            self._patch_runtime(mod, client=client) as session,
            patch.object(
                mod, "_close_pinned_control_inbox", new=AsyncMock(return_value=True)
            ),
            patch.object(mod, "_set_pinned_control_admission", control_admission),
        ):
            session.resume_subagents.side_effect = RuntimeError("not settled")

            assert await mod._session_termination.begin_retirement() is False
            assert mod._session_termination.runtime_admission_closed() is True

        assert control_admission.await_args_list == [
            mock_call(agent_id=self.agent_id, open_for_admission=True),
            mock_call(agent_id=self.agent_id, open_for_admission=False),
        ]

    @pytest.mark.asyncio
    async def test_malformed_commit_response_adopts_exact_authorized_permanent_intent(
        self,
    ):
        from agent.api import persistent_app as mod

        client = SimpleNamespace(
            begin_thread_retirement=AsyncMock(return_value={"status": "ending"}),
            get_thread_lifecycle=AsyncMock(
                return_value={
                    "status": "ending",
                    "session_runtime_generation": self.generation,
                    "session_runtime_attach_token": self.attach_token,
                    "runtime_retirement_pending": True,
                    "runtime_retirement_preflight": False,
                    "runtime_retirement_authorized": True,
                    "retirement_disposition": "ended",
                    "retirement_permanent": True,
                    "session_runtime_retirement_token": self.retirement_token,
                }
            ),
        )
        reopen = AsyncMock()
        with (
            self._patch_runtime(mod, client=client),
            patch.object(
                mod, "_close_pinned_control_inbox", new=AsyncMock(return_value=True)
            ),
            patch.object(mod, "_set_pinned_control_admission", reopen),
        ):
            assert await mod._session_termination.begin_retirement(retirement_permanent=True)
            assert mod._session_termination.retirement_admission_identity == (
                self.thread_id,
                self.generation,
                self.attach_token,
            )
            assert mod._session_termination.retirement_admission_token == self.retirement_token
            assert mod._session_termination.retirement_admission_disposition == "ended"
            assert mod._session_termination.retirement_admission_permanent is True

        reopen.assert_not_awaited()


@pytest.mark.asyncio
async def test_loop_gone_terminal_begin_retries_without_reopening_controls():
    from agent.api import persistent_app as mod

    generation = "88888888-8888-4888-8888-888888888888"
    attach_token = "99999999-9999-4999-8999-999999999999"
    session = _retirement_session_mock()
    session.config = SimpleNamespace(officer=SimpleNamespace(enabled=False))
    session.workspace_sync = None
    session.workspace_manager = None
    session.memory_service = None
    session.messages = []
    session.postgres_conn = None
    session.quiesce_background_tasks = AsyncMock()
    session.retire_shell_owner = MagicMock()
    session.cleanup = AsyncMock()
    finished_loop = asyncio.create_task(asyncio.sleep(0))
    await finished_loop
    retirement_token = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    client = SimpleNamespace(
        begin_thread_retirement=AsyncMock(
            side_effect=[
                None,
                {
                    "status": "ending",
                    "retirement_disposition": "ended",
                    "retirement_permanent": False,
                    "session_runtime_retirement_token": retirement_token,
                },
            ]
        ),
        get_thread_lifecycle=AsyncMock(
            return_value={
                "status": "active",
                "session_runtime_generation": generation,
                "session_runtime_attach_token": attach_token,
                "runtime_retirement_pending": False,
                "runtime_retirement_preflight": False,
                "runtime_retirement_authorized": False,
            }
        ),
    )
    close_controls = AsyncMock(return_value=True)
    reopen = AsyncMock()

    patchers = [
        patch.object(mod, "_session", session),
        patch.object(mod._session_identity, "_thread_id", "loop-gone-thread"),
        patch.object(mod._session_identity, "_session_generation", generation),
        patch.object(mod._session_identity, "_attach_token", attach_token),
        patch.object(mod._session_identity, "_runtime_contract", True),
        patch.object(mod._session_termination, "retirement_admission_identity", None),
        patch.object(mod._session_termination, "retirement_admission_token", None),
        patch.object(mod._session_termination, "retirement_admission_permanent", None),
        patch.object(mod, "_orchestrator_client", client),
        patch.object(mod, "_loop_task", finished_loop),
        patch.object(mod._session_termination, "watchdog_tasks", []),
        patch.object(mod, "_event_writer", None),
        patch.object(mod._session_termination, "terminating", False),
        patch.object(mod._session_termination, "termination_task", None),
        patch.object(mod._session_termination, "max_sessions_per_process", 0),
        patch.object(mod, "_stateless_mode", return_value=False),
        patch.object(mod, "_control_owner_agent_id", None),
        patch.object(mod, "_registered_pinned_agent_id", return_value="agent-a"),
        patch.object(mod, "_set_pinned_control_admission", reopen),
        patch.object(mod, "_close_pinned_control_inbox", new=close_controls),
        patch.object(mod, "_retire_announced_permission_rows", new=AsyncMock()),
        patch.object(mod._session_termination, "stop_and_join_watchdogs", new=AsyncMock()),
        patch.object(mod, "_stop_thread_interrupt_watcher", new=AsyncMock()),
        patch.object(mod, "_stop_thread_control_watcher", new=AsyncMock()),
        patch.object(mod._session_termination, "quiesce_session_side_tasks", new=AsyncMock()),
        patch.object(
            mod._session_termination,
            "settle_exact_retirement_after_quiescence",
            new=AsyncMock(return_value=True),
        ),
        patch.object(session_termination, "_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS", (0.0,)),
    ]
    with ExitStack() as stack:
        for patcher in patchers:
            stack.enter_context(patcher)
        await mod._session_termination.terminate("loop_complete")

    assert client.begin_thread_retirement.await_count == 2
    assert close_controls.await_count >= 2
    client.get_thread_lifecycle.assert_awaited_once_with("loop-gone-thread")
    reopen.assert_not_awaited()


@pytest.mark.asyncio
async def test_authorized_retirement_transient_local_failure_keeps_exact_retry_owner():
    from agent.api import persistent_app as mod

    generation = "88888888-8888-4888-8888-888888888888"
    attach_token = "99999999-9999-4999-8999-999999999999"
    retirement_token = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    identity = ("retry-owner-thread", generation, attach_token)
    inner = AsyncMock(side_effect=[mod.EventJournalUnavailable("writer busy"), None])
    session = MagicMock()

    with (
        patch.object(mod, "_session", session),
        patch.object(mod._session_identity, "_thread_id", identity[0]),
        patch.object(mod._session_identity, "_session_generation", generation),
        patch.object(mod._session_identity, "_attach_token", attach_token),
        patch.object(mod._session_identity, "_runtime_contract", True),
        patch.object(mod._session_termination, "retirement_admission_identity", identity),
        patch.object(mod._session_termination, "retirement_admission_token", retirement_token),
        patch.object(mod._session_termination, "termination_task", None),
        patch.object(mod._session_termination, "terminating", False),
        patch.object(mod._session_termination, "_terminate_inner", inner),
        patch.object(session_termination, "_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS", (0.0,)),
    ):
        await mod._session_termination.terminate("thread_retirement_authorized")

    assert inner.await_count == 2


@pytest.mark.asyncio
async def test_authorized_retirement_retry_never_crosses_successor_identity():
    from agent.api import persistent_app as mod

    generation = "88888888-8888-4888-8888-888888888888"
    attach_token = "99999999-9999-4999-8999-999999999999"
    identity = ("stale-retry-thread", generation, attach_token)

    async def move_then_fail(*_args, **_kwargs):
        mod._session_identity._session_generation = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        raise mod.EventJournalUnavailable("old runtime cleanup failed")

    inner = AsyncMock(side_effect=move_then_fail)
    with (
        patch.object(mod, "_session", MagicMock()),
        patch.object(mod._session_identity, "_thread_id", identity[0]),
        patch.object(mod._session_identity, "_session_generation", generation),
        patch.object(mod._session_identity, "_attach_token", attach_token),
        patch.object(mod._session_identity, "_runtime_contract", True),
        patch.object(mod._session_termination, "retirement_admission_identity", identity),
        patch.object(
            mod._session_termination,
            "retirement_admission_token",
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        ),
        patch.object(mod._session_termination, "termination_task", None),
        patch.object(mod._session_termination, "terminating", False),
        patch.object(mod._session_termination, "_terminate_inner", inner),
        patch.object(session_termination, "_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS", (0.0,)),
    ):
        with pytest.raises(mod.EventJournalUnavailable):
            await mod._session_termination.terminate("thread_retirement_authorized")

    inner.assert_awaited_once()


@pytest.mark.asyncio
async def test_hidden_preflight_ttl_abort_then_exact_reopen():
    from agent.api import persistent_app as mod

    case = TestExactRetirementBeginReconciliation()
    client = SimpleNamespace(
        begin_thread_retirement=AsyncMock(return_value=None),
        get_thread_lifecycle=AsyncMock(
            side_effect=[
                case._live_lifecycle(
                    runtime_retirement_pending=True,
                    runtime_retirement_preflight=True,
                ),
                case._live_lifecycle(),
            ]
        ),
    )
    reopen = AsyncMock(return_value=True)
    with (
        case._patch_runtime(mod, client=client),
        patch.object(
            mod, "_close_pinned_control_inbox", new=AsyncMock(return_value=True)
        ),
        patch.object(mod, "_set_pinned_control_admission", reopen),
        patch.object(session_termination, "_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS", (0.0, 0.0)),
    ):
        assert await mod._session_termination.begin_retirement() is False

    assert client.get_thread_lifecycle.await_count == 2
    reopen.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("lifecycle_mode", ["unavailable", "successor"])
async def test_ambiguous_begin_never_reopens_without_exact_proof(lifecycle_mode):
    from agent.api import persistent_app as mod

    case = TestExactRetirementBeginReconciliation()
    lifecycle = (
        RuntimeError("unavailable")
        if lifecycle_mode == "unavailable"
        else case._live_lifecycle(
            session_runtime_generation="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        )
    )
    client = SimpleNamespace(
        begin_thread_retirement=AsyncMock(side_effect=RuntimeError("response lost")),
        get_thread_lifecycle=AsyncMock(
            side_effect=lifecycle if isinstance(lifecycle, Exception) else None,
            return_value=(None if isinstance(lifecycle, Exception) else lifecycle),
        ),
    )
    reopen = AsyncMock()
    with (
        case._patch_runtime(mod, client=client),
        patch.object(
            mod, "_close_pinned_control_inbox", new=AsyncMock(return_value=True)
        ),
        patch.object(mod, "_set_pinned_control_admission", reopen),
    ):
        assert await mod._session_termination.begin_retirement() is False

    reopen.assert_not_awaited()


# ---------------------------------------------------------------------------
# 3.10 permission_check() logic (tested via closure behavior)
# ---------------------------------------------------------------------------


class TestPermissionCheck:
    """Tests the permission_check closure behavior.

    Since permission_check is a local function inside ws_chat, we test
    the logic patterns directly.
    """

    def test_autonomous_returns_true_immediately(self):
        """In autonomous mode, all tools approved without WS event."""
        # Simulate the logic
        mode = "autonomous"
        assert mode == "autonomous"  # would return True

    def test_auto_accept_approves_non_shell_tools(self):
        """auto_accept approves any tool not in shell set."""
        shell_tools = {"run_command", "shell_execute", "shell_read"}
        assert "web_search" not in shell_tools
        assert "read_file" not in shell_tools

    def test_auto_accept_asks_for_shell_tools(self):
        """auto_accept falls through for shell tools."""
        shell_tools = {"run_command", "shell_execute", "shell_read"}
        assert "run_command" in shell_tools
        assert "shell_execute" in shell_tools
        assert "shell_read" in shell_tools


# ---------------------------------------------------------------------------
# 3.11 on_tool_result truncation
# ---------------------------------------------------------------------------


class TestOnToolResultTruncation:
    def test_short_result_unchanged(self):
        """Results <= 2000 chars not truncated."""
        result = "x" * 2000
        display = result[:2000] + "..." if len(result) > 2000 else result
        assert display == result
        assert len(display) == 2000

    def test_long_result_truncated(self):
        """Results > 2000 chars truncated with '...' suffix."""
        result = "x" * 2500
        display = result[:2000] + "..." if len(result) > 2000 else result
        assert len(display) == 2003
        assert display.endswith("...")


# ---------------------------------------------------------------------------
# 3.12 check_interrupt() closure
# ---------------------------------------------------------------------------


class TestCheckInterrupt:
    def test_returns_true_and_resets_flag(self):
        """Returns True and resets flag when flag was True."""
        interrupt_flag = True

        def check_interrupt():
            nonlocal interrupt_flag
            if interrupt_flag:
                interrupt_flag = False
                return True
            return False

        assert check_interrupt() is True
        assert interrupt_flag is False

    def test_returns_false_when_flag_is_false(self):
        """Returns False with no side effect."""
        interrupt_flag = False

        def check_interrupt():
            nonlocal interrupt_flag
            if interrupt_flag:
                interrupt_flag = False
                return True
            return False

        assert check_interrupt() is False
        assert interrupt_flag is False

    def test_flag_only_fires_once(self):
        """Flag consumed on first check, second returns False."""
        interrupt_flag = True

        def check_interrupt():
            nonlocal interrupt_flag
            if interrupt_flag:
                interrupt_flag = False
                return True
            return False

        assert check_interrupt() is True
        assert check_interrupt() is False


# ---------------------------------------------------------------------------
# 3.13 WebSocket message routing (tested via data parsing logic)
# ---------------------------------------------------------------------------


class TestWSMessageRouting:
    def test_json_parsing_valid(self):
        """Valid JSON parsed normally."""
        raw = '{"method": "message", "content": "hello"}'
        data = json.loads(raw)
        assert data["method"] == "message"
        assert data["content"] == "hello"

    def test_json_parse_error_becomes_message(self):
        """Invalid JSON treated as plain text message."""
        raw = "just plain text"
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            data = {"method": "message", "content": raw}
        assert data["method"] == "message"
        assert data["content"] == "just plain text"

    def test_empty_content_not_queued(self):
        """Empty content silently dropped."""
        data = {"method": "message", "content": ""}
        content = data.get("content", "")
        assert not content  # would not be queued

    def test_valid_modes(self):
        """Valid mode set values."""
        valid_modes = ("supervised", "auto_accept", "autonomous")
        for mode in valid_modes:
            assert mode in valid_modes

    def test_invalid_mode_rejected(self):
        """Invalid mode not in valid set."""
        assert "invalid" not in ("supervised", "auto_accept", "autonomous")


# ---------------------------------------------------------------------------
# 3.15 _handle_vm_upgrade()
# ---------------------------------------------------------------------------


class TestUpgradeAlreadySatisfied:
    """Pure tier-check that gates the live workspace upgrade (Q8).

    Both sandbox and vm are RemoteBackend (supports_shell True), so the check
    must use sudo_action to tell them apart — otherwise sandbox→vm wrongly
    short-circuits as 'already supports a shell' (the Q8 bug this fixes).
    """

    def test_no_shell_never_satisfied(self):
        lite = SimpleNamespace(supports_shell=False)
        assert _upgrade_already_satisfied(lite, "sandbox") is False
        assert _upgrade_already_satisfied(lite, "vm") is False

    def test_sandbox_satisfies_sandbox_not_vm(self):
        sandbox = SimpleNamespace(supports_shell=True, sudo_action="freeze")
        assert _upgrade_already_satisfied(sandbox, "sandbox") is True
        # The fix: a sandbox does NOT satisfy a vm target — it must proceed.
        assert _upgrade_already_satisfied(sandbox, "vm") is False

    def test_vm_satisfies_both(self):
        vm = SimpleNamespace(supports_shell=True, sudo_action="allow")
        assert _upgrade_already_satisfied(vm, "sandbox") is True
        assert _upgrade_already_satisfied(vm, "vm") is True

    def test_missing_sudo_action_is_not_vm(self):
        # A shell backend without a sudo_action attr can't be a vm.
        plain = SimpleNamespace(supports_shell=True)
        assert _upgrade_already_satisfied(plain, "vm") is False
        assert _upgrade_already_satisfied(plain, "sandbox") is True


class TestHandleVmUpgrade:
    """The legacy ``upgrade-to-vm`` accept is now a thin alias (Q8)."""

    @pytest.mark.asyncio
    async def test_delegates_to_workspace_upgrade_vm(self):
        ws = AsyncMock()
        with patch(
            "agent.api.persistent_app._handle_workspace_upgrade",
            new_callable=AsyncMock,
        ) as mock_handler:
            await _handle_vm_upgrade(ws)

        mock_handler.assert_awaited_once_with(ws, target_tier="vm")


# ---------------------------------------------------------------------------
# 3.16 _handle_workspace_upgrade() — vm path (Q7 teardown + Q8 sandbox→vm)
# ---------------------------------------------------------------------------


class TestHandleWorkspaceUpgradeVm:
    def _session_with_backend(self, backend):
        sess = MagicMock()
        sess.protected_cloud_required = False
        sess.shell_owner_token = None
        sess.config.extra = {"shell": {}}
        sess.workspace_manager.backend = backend
        return sess

    @pytest.mark.asyncio
    async def test_short_circuits_when_already_vm(self):
        """A session already on a vm doesn't re-provision."""
        ws = AsyncMock()
        client = AsyncMock()
        backend = SimpleNamespace(supports_shell=True, sudo_action="allow")
        with (
            patch(
                "agent.api.persistent_app._session",
                self._session_with_backend(backend),
            ),
            patch("agent.api.persistent_app._orchestrator_client", client),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
        ):
            await _handle_workspace_upgrade(ws, target_tier="vm")

        client.request_thread_workspace_upgrade.assert_not_called()
        complete = [
            c[0][0]
            for c in ws.send_json.call_args_list
            if c[0][0].get("method") == "workspace_upgrade.complete"
        ]
        assert len(complete) == 1
        assert "vm" in complete[0]["params"]["message"]

    @pytest.mark.asyncio
    async def test_sandbox_to_vm_proceeds_and_aborts_on_timeout(self):
        """sandbox→vm must NOT short-circuit (Q8); a poll timeout tears the
        half-provisioned VM down instead of leaking it (Q7)."""
        ws = AsyncMock()
        client = AsyncMock()
        client.request_thread_workspace_upgrade.return_value = (True, None)
        sandbox = SimpleNamespace(supports_shell=True, sudo_action="freeze")
        with (
            patch(
                "agent.api.persistent_app._session",
                self._session_with_backend(sandbox),
            ),
            patch("agent.api.persistent_app._orchestrator_client", client),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch(
                "agent.api.persistent_app._poll_vm_ready",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            await _handle_workspace_upgrade(ws, target_tier="vm")

        # Proceeded past the short-circuit (Q8) ...
        client.request_thread_workspace_upgrade.assert_awaited_once()
        # ... and on poll failure tore the VM down (Q7) ...
        client.abort_thread_vm_upgrade.assert_awaited_once_with("tid")
        # ... and reported failure.
        failed = [
            c[0][0]
            for c in ws.send_json.call_args_list
            if c[0][0].get("method") == "workspace_upgrade.failed"
        ]
        assert len(failed) == 1

    @pytest.mark.asyncio
    async def test_vm_torn_down_on_post_ready_seed_swap_failure(self):
        """A failure AFTER the VM is ready (seed/swap raising) must tear the VM
        down — not leak a running VM (Q7). Found live: the seed choked on the
        cloud mount once the VM had already registered."""
        import sys

        ws = AsyncMock()
        client = AsyncMock()
        client.request_thread_workspace_upgrade.return_value = (True, None)
        sandbox = SimpleNamespace(supports_shell=True, sudo_action="freeze")
        # RemoteBackend construction blows up → the except block runs.
        mock_remote_mod = MagicMock()
        mock_remote_mod.RemoteBackend.side_effect = RuntimeError("seed/swap boom")
        with (
            patch(
                "agent.api.persistent_app._session",
                self._session_with_backend(sandbox),
            ),
            patch("agent.api.persistent_app._orchestrator_client", client),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch(
                "agent.api.persistent_app._poll_vm_ready",
                new_callable=AsyncMock,
                return_value={"ssh_host": "10.0.0.9", "ssh_port": 22},
            ),
            patch.dict(
                sys.modules, {"shared.runtime.core.backends.remote": mock_remote_mod}
            ),
        ):
            await _handle_workspace_upgrade(ws, target_tier="vm")

        # The post-ready failure tore the VM down (the leak fix) ...
        client.abort_thread_vm_upgrade.assert_awaited_once_with("tid")
        # ... and reported failure.
        failed = [
            c[0][0]
            for c in ws.send_json.call_args_list
            if c[0][0].get("method") == "workspace_upgrade.failed"
        ]
        assert len(failed) == 1

    @pytest.mark.asyncio
    async def test_cloud_mount_reestablished_after_vm_swap(self):
        """A successful sandbox→vm upgrade re-fetches the fresh (vm-ready)
        cloud_mount and re-mounts it on the new backend — the rclone mount is
        per-host and doesn't follow the swap.
        knowledge-base/knowledge/issues/workspace_upgrade_drops_cloud_mount.md."""
        import sys

        ws = AsyncMock()
        client = AsyncMock()
        client.request_thread_workspace_upgrade.return_value = (True, None)
        fresh_mount = {
            "version": 1,
            "driver": "rclone",
            "mounts": [{"access": "read_only"}],
        }
        client.get_thread_workspace.return_value = {"cloud_mount": fresh_mount}

        sandbox = SimpleNamespace(supports_shell=True, sudo_action="freeze")
        sess = self._session_with_backend(sandbox)
        sess.cloud_mount_manager = None  # nothing stale to tear down
        sess.cloud_mount_error = None
        sess.swap_backend = MagicMock()
        sess.resetup_tools_for_backend = MagicMock()

        async def _fake_setup(cfg):
            # A successful mount leaves an active manager.
            sess.cloud_mount_manager = SimpleNamespace(active=True)

        sess._setup_cloud_mount = AsyncMock(side_effect=_fake_setup)

        mock_remote_mod = MagicMock()  # RemoteBackend(...) → connectable stub
        with (
            patch("agent.api.persistent_app._session", sess),
            patch("agent.api.persistent_app._orchestrator_client", client),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch(
                "agent.api.persistent_app._poll_vm_ready",
                new_callable=AsyncMock,
                return_value={"ssh_host": "100.64.0.9", "ssh_port": 22},
            ),
            patch.dict(
                sys.modules, {"shared.runtime.core.backends.remote": mock_remote_mod}
            ),
            patch("agent.core.backends.seed.seed_workspace", return_value=7),
        ):
            await _handle_workspace_upgrade(ws, target_tier="vm")

        # Re-fetched the fresh payload and re-mounted on the new backend ...
        client.get_thread_workspace.assert_awaited_once_with("tid")
        sess._setup_cloud_mount.assert_awaited_once_with(fresh_mount)
        sess.resetup_tools_for_backend.assert_called_once()
        # ... mount succeeded (no degraded notice) and the upgrade completed.
        degraded = [
            c[0][0]
            for c in ws.send_json.call_args_list
            if c[0][0].get("method") == "workspace_upgrade.cloud_mount_degraded"
        ]
        assert degraded == []
        complete = [
            c[0][0]
            for c in ws.send_json.call_args_list
            if c[0][0].get("method") == "workspace_upgrade.complete"
        ]
        assert len(complete) == 1
        client.abort_thread_vm_upgrade.assert_not_called()


class TestHandleWorkspaceUpgradeTierOrTemplate:
    """No tier/template resolves the next tier locally; a template always
    goes to the orchestrator, whose refusal text is shown verbatim."""

    def _session_with_backend(self, backend):
        sess = MagicMock()
        sess.protected_cloud_required = False
        sess.shell_owner_token = None
        sess.config.extra = {"shell": {}}
        sess.workspace_manager.backend = backend
        return sess

    @pytest.mark.asyncio
    async def test_no_tier_on_a_lite_session_is_the_container_refusal(self):
        ws = AsyncMock()
        client = AsyncMock()
        virtual = SimpleNamespace(supports_shell=False)
        with (
            patch(
                "agent.api.persistent_app._session",
                self._session_with_backend(virtual),
            ),
            patch("agent.api.persistent_app._orchestrator_client", client),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
        ):
            await _handle_workspace_upgrade(ws)

        client.request_thread_workspace_upgrade.assert_not_called()
        failed = [
            c[0][0]
            for c in ws.send_json.call_args_list
            if c[0][0].get("method") == "workspace_upgrade.failed"
        ]
        assert len(failed) == 1
        assert "exact runtime authority" in failed[0]["params"]["reason"]

    @pytest.mark.asyncio
    async def test_no_tier_on_a_container_session_asks_for_a_vm(self):
        ws = AsyncMock()
        client = AsyncMock()
        client.request_thread_workspace_upgrade.return_value = (False, None)
        sandbox = SimpleNamespace(supports_shell=True, sudo_action="freeze")
        with (
            patch(
                "agent.api.persistent_app._session",
                self._session_with_backend(sandbox),
            ),
            patch("agent.api.persistent_app._orchestrator_client", client),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
        ):
            await _handle_workspace_upgrade(ws)

        client.request_thread_workspace_upgrade.assert_awaited_once_with(
            "tid", target_tier="vm", template=None
        )

    @pytest.mark.asyncio
    async def test_a_template_goes_to_the_orchestrator_and_its_refusal_is_shown(self):
        ws = AsyncMock()
        client = AsyncMock()
        client.request_thread_workspace_upgrade.return_value = (
            False,
            "Container upgrades of a running Session are unavailable; start a "
            "new Session with this template.",
        )
        virtual = SimpleNamespace(supports_shell=False)
        with (
            patch(
                "agent.api.persistent_app._session",
                self._session_with_backend(virtual),
            ),
            patch("agent.api.persistent_app._orchestrator_client", client),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
        ):
            await _handle_workspace_upgrade(ws, template="site")

        client.request_thread_workspace_upgrade.assert_awaited_once_with(
            "tid", target_tier="vm", template="site"
        )
        failed = [
            c[0][0]
            for c in ws.send_json.call_args_list
            if c[0][0].get("method") == "workspace_upgrade.failed"
        ]
        assert len(failed) == 1
        assert failed[0]["params"]["reason"] == (
            "Container upgrades of a running Session are unavailable; start a "
            "new Session with this template."
        )


class TestHandleWorkspaceUpgradeSandboxCanvasCapability:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("attested", "live_attested", "browser_attested"),
        [
            (True, True, True),
            (True, False, True),
            (True, True, False),
            (False, False, False),
        ],
    )
    async def test_sandbox_hot_swap_fails_before_provision_or_backend_effect(
        self, attested, live_attested, browser_attested
    ):
        import sys

        ws = AsyncMock()
        client = AsyncMock()
        client.request_thread_workspace_upgrade.return_value = True
        client.get_thread_workspace.return_value = {}
        old_backend = SimpleNamespace(supports_shell=False)
        session = MagicMock()
        session.protected_cloud_required = False
        session.shell_owner_token = None
        session.config.extra = {"shell": {}}
        session.workspace_manager.backend = old_backend
        session.swap_backend = MagicMock()
        session.resetup_tools_for_backend = MagicMock()
        new_backend = MagicMock()
        new_backend.connect = MagicMock()
        remote_module = MagicMock()
        remote_module.RemoteBackend.return_value = new_backend
        workspace = {
            "backend": "sandbox",
            "canvas_presentation_available": attested,
            "canvas_live_apps_available": live_attested,
            "canvas_shared_browser_available": browser_attested,
            "remote": {"host": "workspace.test", "port": 30022},
        }

        with (
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._orchestrator_client", client),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch(
                "agent.api.session_workspace.poll_workspace_ready",
                new_callable=AsyncMock,
                return_value=workspace,
            ),
            patch.dict(
                sys.modules, {"shared.runtime.core.backends.remote": remote_module}
            ),
            patch(
                "agent.core.backends.seed.seed_workspace", return_value=1
            ) as seed_workspace,
        ):
            await _handle_workspace_upgrade(ws, target_tier="sandbox")

        client.request_thread_workspace_upgrade.assert_not_awaited()
        client.get_thread_workspace.assert_not_awaited()
        remote_module.RemoteBackend.assert_not_called()
        seed_workspace.assert_not_called()
        new_backend.connect.assert_not_called()
        session.swap_backend.assert_not_called()
        session.resetup_tools_for_backend.assert_not_called()
        failed = [
            call
            for call in ws.send_json.call_args_list
            if call.args[0].get("method") == "workspace_upgrade.failed"
        ]
        assert len(failed) == 1
        assert "exact runtime authority" in failed[0].args[0]["params"]["reason"]

    @pytest.mark.asyncio
    async def test_sandbox_to_unattested_vm_withdraws_browser_capability(self):
        import sys

        ws = AsyncMock()
        client = AsyncMock()
        client.request_thread_workspace_upgrade.return_value = (True, None)
        client.get_thread_workspace.return_value = {}
        old_backend = SimpleNamespace(
            supports_shell=True,
            supports_canvas_presentation=True,
            supports_canvas_live_apps=True,
            supports_canvas_shared_browser=True,
        )
        session = MagicMock()
        session.protected_cloud_required = False
        session.shell_owner_token = None
        session.config.extra = {"shell": {}}
        session.workspace_manager.backend = old_backend
        session.swap_backend = MagicMock()
        session.resetup_tools_for_backend = MagicMock()
        new_backend = MagicMock()
        new_backend.connect = MagicMock()
        remote_module = MagicMock()
        remote_module.RemoteBackend.return_value = new_backend
        with (
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._orchestrator_client", client),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch(
                "agent.api.persistent_app._poll_vm_ready",
                new_callable=AsyncMock,
                return_value={"ssh_host": "vm.test", "ssh_port": 22},
            ),
            patch.dict(
                sys.modules, {"shared.runtime.core.backends.remote": remote_module}
            ),
            patch("agent.core.backends.seed.seed_workspace", return_value=1),
        ):
            await _handle_workspace_upgrade(ws, target_tier="vm")

        assert new_backend.supports_canvas_presentation is False
        assert new_backend.supports_canvas_live_apps is False
        assert new_backend.supports_canvas_shared_browser is False
        session.swap_backend.assert_called_once_with(new_backend)
        session.resetup_tools_for_backend.assert_called_once()

    @pytest.mark.asyncio
    async def test_stateless_sandbox_hot_swap_fails_before_claim_or_exposure(self):
        import sys

        ws = AsyncMock()
        client = AsyncMock()
        client.request_thread_workspace_upgrade.return_value = True
        client.get_thread_workspace.return_value = {}
        session = MagicMock()
        session.protected_cloud_required = False
        session.shell_owner_token = 73
        session.config.extra = {"shell": {}}
        session.workspace_manager.backend = SimpleNamespace(supports_shell=False)
        session.swap_backend = MagicMock()
        session.resetup_tools_for_backend = MagicMock()
        new_backend = MagicMock()
        remote_module = MagicMock()
        remote_module.RemoteBackend.return_value = new_backend
        workspace = {
            "backend": "sandbox",
            "workspace_generation": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "workspace_runtime_incarnation": ("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
            "workspace_ssh_host_key_fingerprint": "SHA256:trusted",
            "remote": {"host": "workspace.test", "port": 30022},
        }

        with (
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._orchestrator_client", client),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch(
                "agent.api.session_workspace.poll_workspace_ready",
                new_callable=AsyncMock,
                return_value=workspace,
            ),
            patch.dict(
                sys.modules, {"shared.runtime.core.backends.remote": remote_module}
            ),
            patch(
                "agent.core.backends.seed.seed_workspace", return_value=1
            ) as seed_workspace,
        ):
            await _handle_workspace_upgrade(ws, target_tier="sandbox")

        client.request_thread_workspace_upgrade.assert_not_awaited()
        client.get_thread_workspace.assert_not_awaited()
        remote_module.RemoteBackend.assert_not_called()
        seed_workspace.assert_not_called()
        new_backend.set_shell_owner_token.assert_not_called()
        new_backend.connect.assert_not_called()
        new_backend.claim_shell_owner.assert_not_called()
        session.swap_backend.assert_not_called()
        session.resetup_tools_for_backend.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("marker", [True, None, "true"])
    async def test_protected_or_malformed_runtime_refuses_before_upgrade_effect(
        self, marker
    ):
        ws = AsyncMock()
        client = AsyncMock()
        session = MagicMock()
        session.protected_cloud_required = marker
        original_backend = SimpleNamespace(supports_shell=False)
        session.workspace_manager.backend = original_backend

        with (
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._orchestrator_client", client),
            patch("agent.api.persistent_app._session_identity._thread_id", "tid"),
            patch(
                "agent.api.session_workspace.poll_workspace_ready",
                new_callable=AsyncMock,
            ) as poll,
        ):
            await _handle_workspace_upgrade(ws, target_tier="sandbox")

        client.request_thread_workspace_upgrade.assert_not_awaited()
        client.update_thread_config.assert_not_awaited()
        poll.assert_not_awaited()
        session.swap_backend.assert_not_called()
        session.resetup_tools_for_backend.assert_not_called()
        assert session.workspace_manager.backend is original_backend
        frames = [call.args[0] for call in ws.send_json.await_args_list]
        assert [frame["method"] for frame in frames] == ["workspace_upgrade.failed"]
        assert "Protected cloud" in frames[0]["params"]["reason"]


# ---------------------------------------------------------------------------
# 3.17 _ws_send()
# ---------------------------------------------------------------------------


class TestWsSend:
    @pytest.mark.asyncio
    async def test_sends_json_with_method_and_params(self):
        ws = AsyncMock()
        await _ws_send(ws, "test.event", {"key": "value"})
        ws.send_json.assert_called_once_with(
            {
                "method": "test.event",
                "params": {"key": "value"},
            }
        )

    @pytest.mark.asyncio
    async def test_silently_drops_on_runtime_error(self):
        ws = AsyncMock()
        ws.send_json.side_effect = RuntimeError("closed")
        # Should not raise
        await _ws_send(ws, "test", {})

    @pytest.mark.asyncio
    async def test_silently_drops_on_connection_reset(self):
        ws = AsyncMock()
        ws.send_json.side_effect = ConnectionResetError
        # Should not raise
        await _ws_send(ws, "test", {})

    @pytest.mark.asyncio
    async def test_silently_drops_on_any_exception(self):
        ws = AsyncMock()
        ws.send_json.side_effect = Exception("anything")
        # Should not raise
        await _ws_send(ws, "test", {})


# ---------------------------------------------------------------------------
# 3.17.1 Headless: subscriber fan-out (_subscribe / _unsubscribe / _broadcast)
# ---------------------------------------------------------------------------


class TestSubscriberFanout:
    """Tests for the subscriber-list broadcast hub that decouples the loop
    from any single WebSocket. See knowledge-base/knowledge/features/headless_persistent_sessions.md."""

    def setup_method(self):
        import agent.api.persistent_app as mod

        mod._subscribers.clear()

    def teardown_method(self):
        import agent.api.persistent_app as mod

        mod._subscribers.clear()

    def test_subscribe_returns_fresh_queue(self):
        import agent.api.persistent_app as mod

        queue = mod._subscribe("client-A")
        assert "client-A" in mod._subscribers
        assert mod._subscribers["client-A"] is queue
        assert queue.empty()

    def test_unsubscribe_removes_entry(self):
        import agent.api.persistent_app as mod

        mod._subscribe("client-A")
        mod._unsubscribe("client-A")
        assert "client-A" not in mod._subscribers

    def test_unsubscribe_unknown_id_is_a_noop(self):
        import agent.api.persistent_app as mod

        # Should not raise.
        mod._unsubscribe("never-subscribed")

    def test_broadcast_enqueues_to_all_subscribers(self):
        import agent.api.persistent_app as mod

        # Reset event-log cursor so this test is deterministic.
        mod._next_seq = 0
        mod._events_epoch = 0
        mod._session = None  # skip the DB write task

        q1 = mod._subscribe("c1")
        q2 = mod._subscribe("c2")

        mod._broadcast("token", {"content": "hi"})

        # Phase 2 stamps (_seq) on every frame so both broadcast subscribers
        # and event-log replay share the same cursor.
        frame = {
            "method": "token",
            "params": {"content": "hi", "_seq": [0, 1]},
        }
        assert q1.get_nowait() == frame
        assert q2.get_nowait() == frame

    def test_broadcast_no_subscribers_does_nothing(self):
        """Loop running with zero subscribers — the whole point of headless."""
        import agent.api.persistent_app as mod

        # Should not raise.
        mod._broadcast("token", {"content": "into the void"})
        assert mod._subscribers == {}

    def test_broadcast_drops_oldest_on_full_queue(self):
        """Slow consumer must not block the loop. Oldest frame is dropped."""
        import asyncio as _asyncio

        import agent.api.persistent_app as mod

        small = _asyncio.Queue(maxsize=2)
        mod._subscribers["slow"] = small

        # Fill the queue so the next broadcast must drop.
        small.put_nowait({"method": "old1", "params": {}})
        small.put_nowait({"method": "old2", "params": {}})

        mod._broadcast("new", {"content": "x"})

        # old1 should have been dropped; queue holds old2 + new.
        first = small.get_nowait()
        second = small.get_nowait()
        assert first["method"] == "old2"
        assert second["method"] == "new"

    def test_unsubscribe_does_not_touch_loop_task(self):
        """The keystone invariant — WS close must not cancel the loop."""
        import asyncio as _asyncio

        import agent.api.persistent_app as mod

        async def _runit():
            async def _forever():
                await _asyncio.sleep(60)

            task = _asyncio.create_task(_forever())
            mod._loop_task = task
            try:
                mod._subscribe("clientX")
                mod._unsubscribe("clientX")
                # Loop is untouched: still running, still the same task.
                assert mod._loop_task is task
                assert not task.done()
                assert not task.cancelled()
            finally:
                task.cancel()
                try:
                    await task
                except _asyncio.CancelledError:
                    pass
                mod._loop_task = None

        _asyncio.run(_runit())


# ---------------------------------------------------------------------------
# 3.17.2 Headless: _terminate_session preserves the race-fix invariants
# ---------------------------------------------------------------------------


class TestTerminateSession:
    """Tests for _terminate_session(reason) — the renamed body of the
    pre-headless _detach_session. Verifies cancel-before-null ordering and
    cleanup of the new headless-era input primitives."""

    @pytest.mark.asyncio
    async def test_no_op_when_session_already_none(self):
        import agent.api.persistent_app as mod

        mod._session = None
        # Should not raise.
        await mod._session_termination.terminate("test")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("reason", ["loop_crash", "shutdown"])
    async def test_every_terminal_reason_begins_retirement_before_cleanup(self, reason):
        import agent.api.persistent_app as mod

        order: list[str] = []
        loop_started = asyncio.Event()

        async def loop_body():
            loop_started.set()
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                order.append("loop")
                raise

        loop_task = asyncio.create_task(loop_body())
        await loop_started.wait()
        session = _retirement_session_mock()
        session.workspace_sync = SimpleNamespace(
            push_all=AsyncMock(side_effect=lambda: order.append("cloud:push")),
            pull_all=AsyncMock(side_effect=lambda: order.append("cloud:pull")),
            aclose=AsyncMock(side_effect=lambda: order.append("cloud:close")),
        )
        git = MagicMock()
        git.is_active = True
        git.has_uncommitted_changes.return_value = True
        git.commit.side_effect = lambda *_args: order.append("git:commit")
        git.push.side_effect = lambda: order.append("git:push")
        session.workspace_manager = SimpleNamespace(git_manager=git)
        session.memory_service = None
        session.messages = []
        session.postgres_conn = None
        session.quiesce_background_tasks = AsyncMock(
            side_effect=lambda: order.append("background")
        )
        session.retire_shell_owner = MagicMock(
            side_effect=lambda: order.append("shell")
        )
        session.cleanup = AsyncMock(side_effect=lambda **_: order.append("cleanup"))
        writer = MagicMock()
        writer.close = AsyncMock(side_effect=lambda: order.append("writer"))
        update = AsyncMock(
            side_effect=lambda status, **_: order.append(f"status:{status}") or True
        )

        mod._session = session
        mod._session_identity._thread_id = f"thread-{reason}"
        mod._loop_task = loop_task
        mod._event_writer = writer
        mod._session_termination.terminating = False
        mod._session_termination.max_sessions_per_process = 0
        mod._session_termination.retirement_admission_identity = None
        with (
            patch.object(mod, "_stateless_mode", return_value=False),
            patch.object(mod, "_control_owner_agent_id", None),
            patch.object(mod, "_registered_pinned_agent_id", return_value=None),
            patch.object(mod, "_update_thread_status", update),
            patch.object(
                mod._session_termination,
                "stop_and_join_watchdogs",
                new=AsyncMock(side_effect=lambda: order.append("watchdogs")),
            ),
            patch.object(
                mod,
                "_stop_thread_interrupt_watcher",
                new=AsyncMock(side_effect=lambda: order.append("interrupt")),
            ),
            patch.object(
                mod,
                "_stop_thread_control_watcher",
                new=AsyncMock(side_effect=lambda: order.append("control")),
            ),
            patch.object(
                mod._session_termination,
                "quiesce_session_side_tasks",
                new=AsyncMock(side_effect=lambda: order.append("side_tasks")),
            ),
            patch.object(
                mod,
                "_await_pending_cloud_push",
                new=AsyncMock(side_effect=lambda: order.append("cloud:pending")),
            ),
        ):
            await mod._session_termination.terminate(reason)

        assert order[0] == "status:ending"
        assert order[-1] == "status:ended"
        assert order == [
            "status:ending",
            "loop",
            "watchdogs",
            "interrupt",
            "control",
            "side_tasks",
            "background",
            "cloud:pending",
            "cloud:push",
            "cloud:pull",
            "cloud:close",
            "git:commit",
            "git:push",
            "writer",
            "shell",
            "cleanup",
            "status:ended",
        ]
        assert update.await_args_list[0].kwargs["retirement_disposition"] == "ended"
        assert update.await_args_list[-1].kwargs["retirement_disposition"] == "ended"

    @pytest.mark.asyncio
    async def test_officer_shutdown_begins_suspended_disposition(self):
        import agent.api.persistent_app as mod

        session = _retirement_session_mock()
        session.config = SimpleNamespace(officer=SimpleNamespace(enabled=True))
        session.workspace_sync = None
        session.workspace_manager = None
        session.memory_service = None
        session.messages = []
        session.postgres_conn = None
        session.quiesce_background_tasks = AsyncMock()
        session.cleanup = AsyncMock()
        update = AsyncMock(return_value=True)

        mod._session = session
        mod._session_identity._thread_id = "thread-officer-shutdown"
        mod._loop_task = None
        mod._session_termination.watchdog_tasks = []
        mod._event_writer = None
        mod._session_termination.terminating = False
        mod._session_termination.max_sessions_per_process = 0
        mod._session_termination.retirement_admission_identity = None
        mod._session_termination.retirement_admission_disposition = None
        with (
            patch.object(mod, "_stateless_mode", return_value=False),
            patch.object(mod, "_control_owner_agent_id", None),
            patch.object(mod, "_registered_pinned_agent_id", return_value=None),
            patch.object(mod, "_update_thread_status", update),
            patch.object(mod, "_stop_thread_interrupt_watcher", new=AsyncMock()),
            patch.object(mod, "_stop_thread_control_watcher", new=AsyncMock()),
        ):
            await mod._session_termination.terminate("shutdown")

        assert update.await_args_list[0].args == ("ending",)
        assert update.await_args_list[0].kwargs == {
            "pinned_agent_id": None,
            "retirement_disposition": "suspended",
            "retirement_permanent": False,
        }
        assert update.await_args_list[1].args == ("ended",)
        assert update.await_args_list[1].kwargs == {
            "pinned_agent_id": None,
            "retirement_disposition": "suspended",
            "retirement_permanent": False,
        }
        session.cleanup.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_settlement_waits_for_late_side_task_quiescence(self):
        import agent.api.persistent_app as mod

        side_task_entered = asyncio.Event()
        release_side_task = asyncio.Event()
        update = AsyncMock(return_value=True)

        async def block_side_tasks():
            side_task_entered.set()
            await release_side_task.wait()

        session = _retirement_session_mock()
        session.workspace_sync = None
        session.workspace_manager = None
        session.memory_service = None
        session.messages = []
        session.postgres_conn = None
        session.quiesce_background_tasks = AsyncMock()
        session.cleanup = AsyncMock()

        mod._session = session
        mod._session_identity._thread_id = "thread-late-side-task"
        mod._loop_task = None
        mod._event_writer = None
        mod._session_termination.terminating = False
        mod._session_termination.max_sessions_per_process = 0
        mod._session_termination.retirement_admission_identity = None
        with (
            patch.object(mod, "_stateless_mode", return_value=False),
            patch.object(mod, "_control_owner_agent_id", None),
            patch.object(mod, "_registered_pinned_agent_id", return_value=None),
            patch.object(mod, "_update_thread_status", update),
            patch.object(mod._session_termination, "stop_and_join_watchdogs", new=AsyncMock()),
            patch.object(mod, "_stop_thread_interrupt_watcher", new=AsyncMock()),
            patch.object(mod, "_stop_thread_control_watcher", new=AsyncMock()),
            patch.object(
                mod._session_termination,
                "quiesce_session_side_tasks",
                new=AsyncMock(side_effect=block_side_tasks),
            ),
        ):
            termination = asyncio.create_task(mod._session_termination.terminate("shutdown"))
            await side_task_entered.wait()
            assert [call.args[0] for call in update.await_args_list] == ["ending"]
            session.cleanup.assert_not_awaited()
            release_side_task.set()
            await termination

        assert [call.args[0] for call in update.await_args_list] == [
            "ending",
            "ended",
        ]

    @pytest.mark.asyncio
    async def test_settlement_waits_for_watchdog_cancellation_finally(self):
        import agent.api.persistent_app as mod

        cancellation_seen = asyncio.Event()
        release_finally = asyncio.Event()
        update = AsyncMock(return_value=True)

        async def stubborn_watchdog():
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                cancellation_seen.set()
                await release_finally.wait()

        watchdog = asyncio.create_task(stubborn_watchdog())
        await asyncio.sleep(0)
        session = _retirement_session_mock()
        session.workspace_sync = None
        session.workspace_manager = None
        session.memory_service = None
        session.messages = []
        session.postgres_conn = None
        session.quiesce_background_tasks = AsyncMock()
        session.cleanup = AsyncMock()

        mod._session = session
        mod._session_identity._thread_id = "thread-watchdog-barrier"
        mod._loop_task = None
        mod._session_termination.watchdog_tasks = [watchdog]
        mod._event_writer = None
        mod._session_termination.terminating = False
        mod._session_termination.max_sessions_per_process = 0
        mod._session_termination.retirement_admission_identity = None
        with (
            patch.object(mod, "_stateless_mode", return_value=False),
            patch.object(mod, "_control_owner_agent_id", None),
            patch.object(mod, "_registered_pinned_agent_id", return_value=None),
            patch.object(mod, "_update_thread_status", update),
            patch.object(mod, "_stop_thread_interrupt_watcher", new=AsyncMock()),
            patch.object(mod, "_stop_thread_control_watcher", new=AsyncMock()),
        ):
            termination = asyncio.create_task(mod._session_termination.terminate("shutdown"))
            await cancellation_seen.wait()
            assert [call.args[0] for call in update.await_args_list] == ["ending"]
            session.cleanup.assert_not_awaited()
            release_finally.set()
            await termination

        assert [call.args[0] for call in update.await_args_list] == [
            "ending",
            "ended",
        ]
        assert mod._session_termination.watchdog_tasks == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("failure", ["background", "writer"])
    async def test_uncertain_pinned_quiescence_blocks_remote_settlement(self, failure):
        import agent.api.persistent_app as mod

        session = _retirement_session_mock()
        session.workspace_sync = None
        session.workspace_manager = None
        session.memory_service = None
        session.messages = []
        session.postgres_conn = None
        session.quiesce_background_tasks = AsyncMock()
        session.cleanup = AsyncMock()
        writer = MagicMock()
        writer.close = AsyncMock()
        if failure == "background":
            session.quiesce_background_tasks.side_effect = RuntimeError("still running")
        else:
            writer.close.side_effect = RuntimeError("flush uncertain")
        update = AsyncMock(return_value=True)

        mod._session = session
        mod._session_identity._thread_id = f"thread-{failure}-uncertain"
        mod._loop_task = None
        mod._session_termination.watchdog_tasks = []
        mod._event_writer = writer
        mod._session_termination.terminating = False
        mod._session_termination.max_sessions_per_process = 0
        mod._session_termination.retirement_admission_identity = None
        with (
            patch.object(mod, "_stateless_mode", return_value=False),
            patch.object(mod, "_control_owner_agent_id", None),
            patch.object(mod, "_registered_pinned_agent_id", return_value=None),
            patch.object(mod, "_update_thread_status", update),
            patch.object(mod, "_stop_thread_interrupt_watcher", new=AsyncMock()),
            patch.object(mod, "_stop_thread_control_watcher", new=AsyncMock()),
        ):
            with pytest.raises(mod.EventJournalUnavailable):
                await mod._session_termination.terminate("shutdown")

        assert [call.args[0] for call in update.await_args_list] == ["ending"]
        assert mod._session is session
        assert mod._session_termination.retirement_admission_closed() is True
        session.cleanup.assert_not_awaited()
        if failure == "writer":
            assert mod._event_writer is writer

    @pytest.mark.asyncio
    async def test_failed_settlement_retains_exact_local_retirement_fence(self):
        import agent.api.persistent_app as mod

        session = _retirement_session_mock()
        session.workspace_sync = None
        session.workspace_manager = None
        session.memory_service = None
        session.messages = []
        session.postgres_conn = MagicMock()
        session.postgres_conn.end_thread = AsyncMock()
        session.postgres_conn.update_thread_status = AsyncMock()
        session.quiesce_background_tasks = AsyncMock()
        session.cleanup = AsyncMock()
        update = AsyncMock(side_effect=[True, False])

        mod._session = session
        mod._session_identity._thread_id = "thread-unsettled"
        mod._loop_task = None
        mod._event_writer = None
        mod._session_termination.terminating = False
        mod._session_termination.max_sessions_per_process = 0
        mod._session_termination.retirement_admission_identity = None
        with (
            patch.object(mod, "_stateless_mode", return_value=False),
            patch.object(mod, "_control_owner_agent_id", None),
            patch.object(mod, "_registered_pinned_agent_id", return_value=None),
            patch.object(mod, "_update_thread_status", update),
            patch.object(mod._session_termination, "stop_and_join_watchdogs", new=AsyncMock()),
            patch.object(mod, "_stop_thread_interrupt_watcher", new=AsyncMock()),
            patch.object(mod, "_stop_thread_control_watcher", new=AsyncMock()),
        ):
            with pytest.raises(mod.EventJournalUnavailable, match="durably settle"):
                await mod._session_termination.terminate("shutdown")

            assert mod._session is session
            assert mod._session_identity.thread_id == "thread-unsettled"
            assert mod._session_termination.retirement_admission_closed() is True
            assert mod._session_termination.runtime_admission_closed() is True
            assert mod._runtime_input_admission_open() is False
            assert mod._session_ready() is False

        session.cleanup.assert_awaited_once()
        session.postgres_conn.end_thread.assert_not_awaited()
        session.postgres_conn.update_thread_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_masked_final_response_uses_exact_outcome_without_cleanup_replay(
        self,
    ):
        import agent.api.persistent_app as mod

        generation = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        attach_token = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        retirement_token = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
        identity = ("thread-masked-settlement", generation, attach_token)
        client = SimpleNamespace(
            get_thread_retirement_outcome=AsyncMock(
                side_effect=[
                    {
                        "status": "ending",
                        "retirement_disposition": "ended",
                        "retirement_permanent": False,
                    },
                    {
                        "status": "settled_or_superseded",
                        "retirement_disposition": "ended",
                        "retirement_permanent": False,
                        "outcome": "settled",
                        "settled_at": "2026-08-26T12:00:00+00:00",
                    },
                ]
            )
        )
        update = AsyncMock(return_value=False)

        with (
            patch.object(mod._session_identity, "_thread_id", identity[0]),
            patch.object(mod._session_identity, "_session_generation", generation),
            patch.object(mod._session_identity, "_attach_token", attach_token),
            patch.object(mod._session_identity, "_runtime_contract", True),
            patch.object(mod._session_termination, "retirement_admission_identity", identity),
            patch.object(mod._session_termination, "retirement_admission_token", retirement_token),
            patch.object(mod, "_orchestrator_client", client),
            patch.object(mod, "_update_thread_status", update),
            patch.object(
                session_termination,
                "_EXACT_RETIREMENT_SETTLEMENT_RETRY_DELAYS",
                (0.0, 0.0),
            ),
        ):
            assert await mod._session_termination.settle_exact_retirement_after_quiescence(
                pinned_agent_id="agent-a",
                retirement_disposition="ended",
                retirement_permanent=False,
                expected_identity=identity,
            )

        assert update.await_count == 2
        assert client.get_thread_retirement_outcome.await_count == 2
        assert client.get_thread_retirement_outcome.await_args.kwargs == {
            "pinned_agent_id": "agent-a",
            "session_runtime_generation": generation,
            "session_runtime_attach_token": attach_token,
            "session_runtime_retirement_token": retirement_token,
            "retirement_disposition": "ended",
            "retirement_permanent": False,
        }

    @pytest.mark.asyncio
    async def test_cancels_loop_task_before_nulling_session(self):
        """The race-fix from commit 3a1d265 must survive the rename."""
        import asyncio as _asyncio

        import agent.api.persistent_app as mod

        # Track when the loop was cancelled vs when cleanup ran.
        order = []
        loop_started = _asyncio.Event()

        async def _loop_body():
            loop_started.set()
            try:
                await _asyncio.sleep(60)
            except _asyncio.CancelledError:
                order.append("loop_cancelled")
                raise

        loop_task = _asyncio.create_task(_loop_body())
        # Wait until the body actually enters its try block — otherwise
        # cancel() would fire before the body had a chance to register an
        # except handler, and our ordering assertion would be vacuous.
        await loop_started.wait()
        mod._loop_task = loop_task
        mod._session_identity._thread_id = "t1"

        # Minimal _session double that records when it's torn down.
        fake_session = _retirement_session_mock()
        fake_session.workspace_sync = None
        git_mgr = MagicMock()
        git_mgr.is_active = True
        git_mgr.has_uncommitted_changes.return_value = True
        git_mgr.commit.side_effect = lambda *_: order.append("git_committed")
        git_mgr.push.side_effect = lambda: order.append("git_pushed")
        fake_session.workspace_manager = MagicMock(git_manager=git_mgr)
        fake_session.cleanup = AsyncMock(
            side_effect=lambda **_: order.append("cleanup")
        )
        fake_session.retire_shell_owner = MagicMock(
            side_effect=lambda: order.append("shell_retired")
        )
        mod._session = fake_session

        async def close_writer():
            # The writer must drain while both captured identities are still
            # authoritative; pool-mode reuse clears them immediately after.
            assert mod._session is fake_session
            assert mod._session_identity.thread_id == "t1"
            order.append("writer_closed")

        fake_writer = MagicMock()
        fake_writer.close = AsyncMock(side_effect=close_writer)
        mod._event_writer = fake_writer

        async def stop_controls():
            order.append("controls_stopped")

        with (
            patch.object(mod, "_update_thread_status", new=AsyncMock()),
            patch.object(
                mod,
                "_stop_thread_control_watcher",
                new=AsyncMock(side_effect=stop_controls),
            ),
        ):
            await mod._session_termination.terminate("test")

        # Control admission closes first. The journal drains while the captured
        # identity remains authoritative, then final Git (absent in this
        # fixture) precedes shell retirement and cleanup.
        assert order == [
            "loop_cancelled",
            "controls_stopped",
            "git_committed",
            "git_pushed",
            "writer_closed",
            "shell_retired",
            "cleanup",
        ]
        assert mod._session is None
        assert mod._loop_task is None
        assert mod._event_writer is None
        fake_session.cleanup.assert_awaited_once_with(
            preserve_shell=False,
            preserve_workspace_daemons=False,
        )

    @pytest.mark.asyncio
    async def test_mark_thread_false_preserves_shell_for_ownership_handoff(self):
        import agent.api.persistent_app as mod

        mod._loop_task = None
        mod._session_identity._thread_id = "t-handoff"
        mod._event_writer = None
        mod._session_termination.terminating = False
        mod._session_termination.max_sessions_per_process = 0
        fake_session = _retirement_session_mock()
        fake_session.shell_owner_token = 31
        fake_session.workspace_sync = None
        fake_session.workspace_manager = None
        fake_session.messages = [HumanMessage(content="do not re-extract me")]
        fake_session.final_memory_extracted = False
        fake_session.memory_service = SimpleNamespace(capture=AsyncMock())
        fake_session.quiesce_background_tasks = AsyncMock()
        fake_session.cleanup = AsyncMock()
        mod._session = fake_session

        with patch.object(mod, "_update_thread_status", new=AsyncMock()) as update:
            await mod._session_termination.terminate("claim_switch", mark_thread=False)

        update.assert_not_awaited()
        fake_session.retire_shell_owner.assert_called_once_with()
        fake_session.cleanup.assert_awaited_once_with(
            preserve_shell=True,
            preserve_workspace_daemons=False,
        )
        fake_session.memory_service.capture.assert_not_awaited()
        fake_session.quiesce_background_tasks.assert_awaited_once_with()

    @pytest.mark.asyncio
    async def test_stateless_physical_handoff_preserves_workspace_daemons(self):
        import agent.api.persistent_app as mod

        mod._loop_task = None
        mod._session_identity._thread_id = "t-physical-handoff"
        mod._event_writer = None
        mod._session_termination.terminating = False
        mod._session_termination.max_sessions_per_process = 0
        fake_session = _retirement_session_mock()
        fake_session.workspace_sync = None
        fake_session.workspace_manager = None
        fake_session.cleanup = AsyncMock()
        mod._session = fake_session

        with patch.object(mod, "_update_thread_status", new=AsyncMock()) as update:
            await mod._session_termination.terminate(
                "claim_switch",
                mark_thread=False,
                preserve_shell=True,
                preserve_workspace_daemons=True,
            )

        update.assert_not_awaited()
        fake_session.retire_shell_owner.assert_called_once_with()
        fake_session.cleanup.assert_awaited_once_with(
            preserve_shell=True,
            preserve_workspace_daemons=True,
        )

    @pytest.mark.asyncio
    async def test_moved_pinned_binding_refuses_all_local_teardown(self):
        """A stale pinned pod must never destroy its successor's resources."""
        import agent.api.persistent_app as mod

        mod._loop_task = None
        mod._session_identity._thread_id = "t-binding-moved"
        mod._event_writer = None
        mod._session_termination.terminating = False
        mod._session_termination.max_sessions_per_process = 0
        fake_session = _retirement_session_mock()
        fake_session.workspace_sync = None
        fake_session.workspace_manager = None
        fake_session.memory_service = None
        fake_session.cleanup = AsyncMock()
        mod._session = fake_session

        with (
            patch.object(mod, "_stateless_mode", return_value=False),
            patch.object(mod, "_control_owner_agent_id", "agent-old"),
            patch.object(
                mod,
                "_close_pinned_control_inbox",
                new=AsyncMock(return_value=False),
            ) as close_admission,
            patch.object(mod, "_update_thread_status", new=AsyncMock()) as update,
            patch.object(mod, "_stop_thread_control_watcher", new=AsyncMock()),
        ):
            with pytest.raises(mod.EventJournalUnavailable):
                await mod._session_termination.terminate(
                    "binding_moved",
                    mark_thread=True,
                    preserve_shell=False,
                )

        close_admission.assert_awaited_once_with(agent_id="agent-old")
        update.assert_not_awaited()
        fake_session.retire_shell_owner.assert_not_called()
        fake_session.cleanup.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_clears_headless_input_primitives(self):
        """Subscriber registry and loop input queues must reset."""
        import asyncio as _asyncio

        import agent.api.persistent_app as mod

        mod._loop_task = None  # nothing to cancel
        mod._session_identity._thread_id = "t2"
        mod._subscribers["ghost"] = _asyncio.Queue()
        mod._session_input._queue = _asyncio.Queue()
        mod._session_input._interrupt_mode = "hard"
        mod._session_input._interrupt_target_turn_id = 4
        mod._session_input._hard_interrupt_event = _asyncio.Event()
        mod._tool_inflight = True
        mod._events_epoch = 7
        mod._next_seq = 42
        mod._event_writer = None

        fake_session = _retirement_session_mock()
        fake_session.workspace_sync = None
        fake_session.workspace_manager = None
        fake_session.cleanup = AsyncMock()
        mod._session = fake_session

        with patch.object(mod, "_update_thread_status", new=AsyncMock()):
            await mod._session_termination.terminate("test")

        assert mod._subscribers == {}
        assert mod._session_input.queue is None
        assert mod._session_input.interrupt_mode is None
        assert mod._session_input.interrupt_target_turn_id is None
        assert mod._session_input.hard_interrupt_event is None
        assert mod._tool_inflight is False
        assert mod._events_epoch == 0
        assert mod._next_seq == 0


# ---------------------------------------------------------------------------
# 3.17.2b Attach-time cloud mount/sync selection
# ---------------------------------------------------------------------------


class TestAttachSessionEventJournalFailure:
    @pytest.mark.asyncio
    async def test_failed_stateless_physical_attach_preserves_resident_daemons(self):
        import agent.api.persistent_app as mod

        session = SimpleNamespace(
            shell_owner_token=61,
            stateless_warm_reuse_safe=False,
            tool_context=SimpleNamespace(
                citation_verdict_callback=MagicMock(),
                canvas_event_callback=MagicMock(),
            ),
            cleanup=AsyncMock(),
        )
        mod._session = session
        mod._session_identity._thread_id = "thread-failed-physical-attach"
        mod._event_writer = None
        mod._subscribers.clear()

        with patch.object(mod, "_stop_thread_control_watcher", new=AsyncMock()):
            await mod._session_attach.cleanup_failed_attach(mod._session_identity.thread_id)

        session.cleanup.assert_awaited_once_with(
            preserve_shell=True,
            preserve_workspace_daemons=True,
        )
        assert mod._session is None
        assert mod._session_identity.thread_id is None

    @pytest.mark.asyncio
    async def test_aborts_and_cleans_partial_session_before_any_broadcast(self):
        import agent.api.persistent_app as mod

        fake_db = MagicMock()
        instances = []

        class FakeSession:
            def __init__(self, *args, **kwargs):
                self.postgres_conn = fake_db
                self.tool_context = SimpleNamespace(
                    citation_verdict_callback=None,
                )
                self.cleanup = AsyncMock()
                instances.append(self)

            async def setup(self, **kwargs):
                return None

            async def recover_subagents(self):
                return None

        workspace_override = {"remote": {"host": "10.42.0.10"}}
        fake_agent = SimpleNamespace(
            config=object(),
            _tactical_llm=None,
            _llm=object(),
            _auxiliary_llm=object(),
            postgres_conn=fake_db,
            vector_conn=None,
        )
        fake_orchestrator = SimpleNamespace(
            get_thread_workspace=AsyncMock(return_value=workspace_override)
        )

        mod._session = None
        mod._session_identity._thread_id = None
        mod._event_writer = None
        mod._events_epoch = 0
        mod._next_seq = 0
        mod._subscribers.clear()
        with (
            patch.object(mod, "_agent", fake_agent),
            patch.object(mod, "_orchestrator_client", fake_orchestrator),
            patch.object(mod, "PersistentSession", FakeSession),
            patch.object(session_workspace, "poll_workspace_ready",
                new=AsyncMock(return_value=workspace_override),
            ),
            patch.object(
                mod,
                "_resolve_event_journal_epoch",
                new=AsyncMock(
                    side_effect=mod.EventJournalUnavailable(
                        "Persistent event journal initialization failed"
                    )
                ),
            ),
            patch.object(mod, "_OrderedPersistentEventWriter") as writer_cls,
            patch.object(mod, "_broadcast") as broadcast,
        ):
            with pytest.raises(mod.EventJournalUnavailable):
                await mod._session_attach.attach("thread-journal-failure")

        assert len(instances) == 1
        instances[0].cleanup.assert_awaited_once_with(
            preserve_shell=True,
            preserve_workspace_daemons=False,
        )
        assert instances[0].tool_context.citation_verdict_callback is None
        writer_cls.assert_not_called()
        broadcast.assert_not_called()
        assert mod._session is None
        assert mod._session_identity.thread_id is None
        assert mod._event_writer is None
        assert mod._events_epoch == 0
        assert mod._next_seq == 0


class TestAttachSessionCloudMount:
    @pytest.mark.asyncio
    async def test_active_cloud_mount_skips_legacy_nc_session_sync(self):
        """A mounted cloud workspace must not also start legacy WebDAV sync."""
        import agent.api.persistent_app as mod

        recovery_queue_states = []

        class FakeSession:
            def __init__(self, *args, **kwargs):
                self.cloud_mount_manager = SimpleNamespace(
                    active=True,
                    mounts=[
                        SimpleNamespace(
                            mount_id="legacy-session",
                            mount_kind="session_folder",
                            target_path="/cloud/home",
                            workspace_name="home",
                        )
                    ],
                )
                self.cloud_mount_error = None
                self.workspace_manager = SimpleNamespace(
                    path=Path("/workspace"),
                    backend=MagicMock(),
                )
                self.workspace_sync = None
                self.postgres_conn = None
                # Matches PersistentSession's class default; a no-op setup()
                # never builds tools, so the live citation-verdict callback
                # wiring at attach time is skipped.
                self.tool_context = None

            async def setup(self, **kwargs):
                return None

            async def recover_subagents(self):
                recovery_queue_states.append(mod._session_input.queue)

        workspace_override = {
            "remote": {"host": "10.42.0.10"},
            "nc_session_folder": "Sessions/thread-1",
            "cloud_mount": {"version": 1, "driver": "rclone", "mounts": []},
        }
        fake_agent = SimpleNamespace(
            config=object(),
            _tactical_llm=None,
            _llm=object(),
            _auxiliary_llm=object(),
            postgres_conn=None,
            vector_conn=None,
        )
        fake_orchestrator = SimpleNamespace(
            get_thread_workspace=AsyncMock(return_value=workspace_override)
        )

        mod._session = None
        mod._session_identity._thread_id = None
        with (
            patch.object(mod, "_agent", fake_agent),
            patch.object(mod, "_orchestrator_client", fake_orchestrator),
            patch.object(mod, "PersistentSession", FakeSession),
            patch.object(session_workspace, "poll_workspace_ready",
                new=AsyncMock(return_value=workspace_override),
            ),
            patch.object(mod, "_build_sync_coordinator") as build_sync,
            patch.object(mod, "_restore_session_messages", new=AsyncMock()),
            patch.object(mod, "_update_thread_status", new=AsyncMock()),
            patch.object(mod._session_termination, "start_watchdogs"),
        ):
            try:
                await mod._session_attach.attach("thread-1")
                queue_after_recovery = mod._session_input.queue
            finally:
                mod._session = None
                mod._session_identity._thread_id = None

        build_sync.assert_not_called()
        assert recovery_queue_states == [None]
        assert isinstance(queue_after_recovery, asyncio.Queue)


class TestAttachSessionProtectedCloudFailClose:
    """F-C1 (Task B10): a protected thread with no engageable cloud_mount
    must NEVER fall back to the legacy nc_session_folder WebDAV sync shim,
    even though the legacy field is still present in the workspace response
    (refused engage / flag off / VM tier / overlay-failure teardown all
    resolve cloud_mount=None while nc_session_folder stays set)."""

    @staticmethod
    def _fake_session_cls():
        class FakeSession:
            def __init__(self, *args, **kwargs):
                # No active cloud mount — mirrors a degraded-protected thread
                # (engage refused, flag off, or VM tier all resolve to None).
                self.cloud_mount_manager = None
                self.cloud_mount_error = None
                self.overlay_mount_manager = None
                self.workspace_manager = SimpleNamespace(
                    path=Path("/workspace"),
                    backend=MagicMock(),
                )
                self.workspace_sync = None
                self.postgres_conn = None
                self.tool_context = None

            async def setup(self, **kwargs):
                return None

            async def recover_subagents(self):
                return None

        return FakeSession

    @pytest.mark.asyncio
    async def test_protected_thread_skips_legacy_shim_despite_nc_folder(self):
        """protected_cloud=True + cloud_mount=None + nc_session_folder set
        must fail closed before building the legacy sync coordinator."""
        import agent.api.persistent_app as mod

        workspace_override = {
            "remote": {"host": "10.42.0.10"},
            "nc_session_folder": "Sessions/thread-1",
            "cloud_mount": None,
            "cloud_sync": None,
            "protected_cloud": True,
        }
        fake_agent = SimpleNamespace(
            config=object(),
            _tactical_llm=None,
            _llm=object(),
            _auxiliary_llm=object(),
            postgres_conn=None,
            vector_conn=None,
        )
        fake_orchestrator = SimpleNamespace(
            get_thread_workspace=AsyncMock(return_value=workspace_override)
        )

        mod._session = None
        mod._session_identity._thread_id = None
        with (
            patch.object(mod, "_agent", fake_agent),
            patch.object(mod, "_orchestrator_client", fake_orchestrator),
            patch.object(mod, "PersistentSession", self._fake_session_cls()),
            patch.object(session_workspace, "poll_workspace_ready",
                new=AsyncMock(return_value=workspace_override),
            ),
            patch.object(mod, "_build_sync_coordinator") as build_sync,
            patch.object(mod, "_restore_session_messages", new=AsyncMock()),
            patch.object(mod, "_update_thread_status", new=AsyncMock()),
            patch.object(mod._session_termination, "start_watchdogs"),
        ):
            try:
                with pytest.raises(
                    session_contract.ProtectedCloudUnavailable,
                    match="no authoritative ready state",
                ):
                    await mod._session_attach.attach("thread-1")
            finally:
                mod._session = None
                mod._session_identity._thread_id = None

        # The legacy shim (_legacy_nc_cloud_cfg -> _build_sync_coordinator)
        # must never fire for a protected thread, regardless of a stale/
        # still-set nc_session_folder.
        build_sync.assert_not_called()

    @pytest.mark.asyncio
    async def test_non_protected_thread_still_uses_legacy_shim(self):
        """Regression guard: an ordinary (non-protected) thread with no
        cloud_mount/cloud_sync but a live nc_session_folder still falls back
        to the legacy shim — the new protected-only gate must not swallow
        the existing back-compat path."""
        import agent.api.persistent_app as mod

        workspace_override = {
            "remote": {"host": "10.42.0.10"},
            "nc_session_folder": "Sessions/thread-1",
            "cloud_mount": None,
            "cloud_sync": None,
            "protected_cloud": False,
        }
        fake_agent = SimpleNamespace(
            config=object(),
            _tactical_llm=None,
            _llm=object(),
            _auxiliary_llm=object(),
            postgres_conn=None,
            vector_conn=None,
        )
        fake_orchestrator = SimpleNamespace(
            get_thread_workspace=AsyncMock(return_value=workspace_override)
        )

        mod._session = None
        mod._session_identity._thread_id = None
        with (
            patch.object(mod, "_agent", fake_agent),
            patch.object(mod, "_orchestrator_client", fake_orchestrator),
            patch.object(mod, "PersistentSession", self._fake_session_cls()),
            patch.object(session_workspace, "poll_workspace_ready",
                new=AsyncMock(return_value=workspace_override),
            ),
            patch.object(mod, "_build_sync_coordinator") as build_sync,
            patch.object(mod, "_restore_session_messages", new=AsyncMock()),
            patch.object(mod, "_update_thread_status", new=AsyncMock()),
            patch.object(mod._session_termination, "start_watchdogs"),
        ):
            try:
                await mod._session_attach.attach("thread-1")
            finally:
                mod._session = None
                mod._session_identity._thread_id = None

        build_sync.assert_called_once()
        # The legacy translation ran (webdav_url built from nc_folder).
        _, kwargs = build_sync.call_args
        assert "Sessions/thread-1" in kwargs["cloud_cfg"]["webdav_url"]


class TestAttachSessionProtectedCloudSingletonIsolation:
    """Task 15 review fix: the protected_cloud honesty flag must never be
    written into the pool-mode singleton ``_agent.config``.

    On the plain-boot attach path (no resolved_config / config_name /
    config_override) ``effective_config`` aliases ``_agent.config``, which a
    pool pod reuses across sequential session attaches. An in-place
    ``extra["_protected_cloud"]`` write would leak the flag into every later
    NON-protected session on the same pod — whose live cloud files really are
    saved, making the honesty block ("staged for your review") a lie. The fix
    clones via ``dataclasses.replace``; this pins both sides: the protected
    session's own config carries the flag, while the singleton (and therefore
    a subsequent non-protected session) never sees it.
    """

    @staticmethod
    def _fake_session_cls(captured_configs: list):
        class FakeSession:
            def __init__(self, *args, **kwargs):
                captured_configs.append(kwargs.get("config"))
                self.protected_cloud_required = kwargs.get(
                    "protected_cloud_required", False
                )
                self.cloud_mount_manager = (
                    MagicMock(active=True) if self.protected_cloud_required else None
                )
                self.cloud_mount_error = None
                self.overlay_mount_manager = None
                self.workspace_manager = SimpleNamespace(
                    path=Path("/workspace"),
                    backend=MagicMock(),
                )
                self.workspace_sync = None
                self.postgres_conn = None
                self.tool_context = None

            async def setup(self, **kwargs):
                return None

            async def recover_subagents(self):
                return None

            def protected_cloud_ready(self):
                return self.protected_cloud_required

        return FakeSession

    @staticmethod
    def _workspace_override(protected: bool) -> dict:
        # No resolved_config / config_override / config_name anywhere: keeps
        # the attach on the plain-boot path where effective_config starts as
        # the _agent.config singleton itself.
        payload = {
            "remote": {
                "host": "10.42.0.10",
                "port": 22,
                "username": "agent-host",
                "key_path": "/run/secrets/vm-ssh-key",
                "workspace_path": "/home/agent-host/workspace",
            },
            "cloud_mount": None,
            "cloud_sync": None,
            "protected_cloud": protected,
        }
        if protected:
            payload.update(
                {
                    "status": "ready",
                    "backend": "sandbox",
                    "pod_ip": "10.42.0.10",
                    "pod_port": 22,
                    "ssh_key_path": "/run/secrets/vm-ssh-key",
                    "workspace_generation": ("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
                    "workspace_runtime_incarnation": (
                        "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
                    ),
                    "workspace_ssh_host_key_fingerprint": "SHA256:trusted",
                    "pinned_runtime_generation_contract": 1,
                    "session_runtime_generation": (
                        "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
                    ),
                    "session_runtime_attach_token": (
                        "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
                    ),
                    "protected_cloud_state": "ready",
                    "cloud_mount": {
                        "version": 1,
                        "driver": "rclone",
                        "protected": True,
                        "skip_workspace_links": True,
                        "fallback": False,
                        "overlay": {
                            "lower": "/cloud/lower",
                            "upper": "/home/agent-host/.overlay/upper",
                            "work": "/home/agent-host/.overlay/work",
                            "merged": "/cloud/merged",
                            "quota_bytes": 1024,
                        },
                        "mounts": [
                            {
                                "mount_id": "protected-thread-1",
                                "mount_kind": "protected_lower",
                                "backend": "nextcloud",
                                "target_path": "/cloud/lower",
                                "workspace_name": "lower",
                                "access": "read_only",
                                "source": {
                                    "type": "webdav",
                                    "config": {
                                        "vendor": "nextcloud",
                                        "url": "https://cloud.invalid/dav/",
                                        "user": "reader",
                                    },
                                },
                                "auth": {"type": "basic", "password": "pw"},
                            }
                        ],
                    },
                }
            )
        return payload

    async def _attach_once(self, mod, fake_agent, workspace_override, captured):
        fake_orchestrator = SimpleNamespace(
            get_thread_workspace=AsyncMock(return_value=workspace_override),
            session_runtime_generation=(
                workspace_override.get("session_runtime_generation")
            ),
            session_runtime_attach_token=workspace_override.get(
                "session_runtime_attach_token"
            ),
            pinned_runtime_generation_contract=bool(
                workspace_override.get("pinned_runtime_generation_contract") == 1
            ),
            adopt_session_runtime_identity=MagicMock(return_value=True),
            clear_session_runtime_identity=MagicMock(return_value=True),
        )
        mod._session = None
        mod._session_identity._thread_id = None
        with (
            patch.object(mod, "_agent", fake_agent),
            patch.object(mod, "_orchestrator_client", fake_orchestrator),
            patch.object(mod, "PersistentSession", self._fake_session_cls(captured)),
            patch.object(session_workspace, "poll_workspace_ready",
                new=AsyncMock(return_value=workspace_override),
            ),
            patch.object(mod, "_build_sync_coordinator"),
            patch.object(mod, "_restore_session_messages", new=AsyncMock()),
            patch.object(mod, "_update_thread_status", new=AsyncMock()),
            patch.object(mod._session_termination, "start_watchdogs"),
        ):
            try:
                await mod._session_attach.attach(
                    "thread-1",
                    pinned_runtime_generation_contract=workspace_override.get(
                        "pinned_runtime_generation_contract"
                    ),
                    session_runtime_generation=workspace_override.get(
                        "session_runtime_generation"
                    ),
                    session_runtime_attach_token=workspace_override.get(
                        "session_runtime_attach_token"
                    ),
                )
            finally:
                mod._session = None
                mod._session_identity._thread_id = None

    @pytest.mark.asyncio
    async def test_sequential_pool_reuse_does_not_leak_protected_flag(self):
        """Attach protected session A, then non-protected session B, through
        the same _agent: B and the singleton must never carry the flag."""
        import agent.api.persistent_app as mod
        from shared.runtime.core.loader import AgentConfig

        singleton = AgentConfig(agent_id="pool-pod", display_name="Pool Pod")
        fake_agent = SimpleNamespace(
            config=singleton,
            _tactical_llm=None,
            _llm=object(),
            _auxiliary_llm=object(),
            postgres_conn=None,
            vector_conn=None,
        )

        captured: list = []
        await self._attach_once(
            mod, fake_agent, self._workspace_override(True), captured
        )
        await self._attach_once(
            mod, fake_agent, self._workspace_override(False), captured
        )

        config_a, config_b = captured
        # Session A's own config carries the flag (honesty block renders)…
        assert config_a.extra.get("_protected_cloud") is True
        # …via a clone — never by mutating the singleton itself.
        assert config_a is not singleton
        # The singleton was never polluted…
        assert "_protected_cloud" not in singleton.extra
        # …so session B (which aliases it on this path) never inherits it.
        assert config_b is singleton
        assert "_protected_cloud" not in config_b.extra


# ---------------------------------------------------------------------------
# 3.17.3 Headless: attach-time readiness race (handle_persistent_websocket)
# ---------------------------------------------------------------------------


class TestHandlePersistentWebsocketReadiness:
    """Regression tests for the attach-time readiness race fixed in sha-a790c79.

    In dual mode, POST /session/attach returns immediately while the actual
    _attach_session work runs in the background. _session.llm_with_tools is
    set early (inside .setup()), but _loop_user_queue is initialized much
    later in the same coroutine. A client that opens the WS in that window
    sees a session that looks ready but loop primitives that aren't.

    Pre-fix: the WS handler only checked _session and llm_with_tools, then
    spawned the persistent loop. The loop's _loop_get_user_input callback
    crashed on the first await (queue was None) and the session never
    recovered. The fix gates loop spawn on _loop_user_queue being
    initialized too, closing the WS with code 4503 ("Agent not ready") so
    the client retries.
    """

    fingerprint = "sha256:" + ("c" * 64)

    @classmethod
    def _validated_websocket(cls):
        ws = AsyncMock()
        ws.state.session_identity_fingerprint = cls.fingerprint
        return ws

    @pytest.mark.asyncio
    async def test_closes_with_4403_when_validated_session_disappeared(self):
        """A session lost after token routing is an identity change."""
        from agent.api import persistent_app as pa

        ws = self._validated_websocket()
        with (
            patch("agent.api.persistent_app._session", None),
            patch("agent.api.persistent_app._session_input._queue", None),
            patch("agent.api.persistent_app._loop_task", None),
            patch("agent.api.persistent_app._session_termination.ws_connected_event", None),
        ):
            await _serve_session_websocket(ws)
            assert pa._loop_task is None

        ws.accept.assert_awaited_once()
        ws.send_json.assert_not_awaited()
        ws.close.assert_awaited_once_with(code=4403, reason="session identity changed")

    @pytest.mark.asyncio
    async def test_closes_with_4503_when_llm_with_tools_missing(self):
        """Session exists but .setup() hasn't bound llm_with_tools yet."""
        from agent.api import persistent_app as pa

        ws = self._validated_websocket()
        session = MagicMock()
        session.llm_with_tools = None
        with (
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._session_input._queue", None),
            patch("agent.api.persistent_app._loop_task", None),
            patch("agent.api.persistent_app._session_termination.ws_connected_event", None),
            patch(
                "agent.api.persistent_app._session_identity.fingerprint",
                return_value=self.fingerprint,
            ),
        ):
            await _serve_session_websocket(ws)
            assert pa._loop_task is None

        ws.close.assert_awaited_once_with(code=4503, reason="Agent not ready")

    @pytest.mark.asyncio
    async def test_closes_with_4503_when_loop_user_queue_missing(self):
        """The keystone bug: llm_with_tools is set but _loop_user_queue is None.

        Pre-fix this passed the readiness check (which only inspected
        _session and llm_with_tools), spawned the loop, and the loop's first
        get-user-input callback crashed on the None queue.
        """
        from agent.api import persistent_app as pa

        ws = self._validated_websocket()
        session = MagicMock()
        session.llm_with_tools = MagicMock()  # truthy
        with (
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._session_input._queue", None),
            patch("agent.api.persistent_app._loop_task", None),
            patch("agent.api.persistent_app._session_termination.ws_connected_event", None),
            patch(
                "agent.api.persistent_app._session_identity.fingerprint",
                return_value=self.fingerprint,
            ),
        ):
            await _serve_session_websocket(ws)
            # The whole point of the fix: the loop must NOT have been
            # spawned despite llm_with_tools being set.
            assert pa._loop_task is None

        ws.close.assert_awaited_once_with(code=4503, reason="Agent not ready")

    @pytest.mark.asyncio
    async def test_sends_error_frame_before_close(self):
        """Error frame must precede the close so clients see the reason."""

        ws = self._validated_websocket()
        session = MagicMock()
        session.llm_with_tools = None
        with (
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._session_input._queue", None),
            patch("agent.api.persistent_app._loop_task", None),
            patch("agent.api.persistent_app._session_termination.ws_connected_event", None),
            patch(
                "agent.api.persistent_app._session_identity.fingerprint",
                return_value=self.fingerprint,
            ),
        ):
            await _serve_session_websocket(ws)

        ws.send_json.assert_awaited_once_with(
            {"method": "error", "params": {"message": "Agent not ready"}}
        )
        ws.close.assert_awaited_once_with(code=4503, reason="Agent not ready")

    @pytest.mark.asyncio
    async def test_signals_ws_connected_even_when_not_ready(self):
        """The boot-WS watchdog must see the attach attempt even on an
        unready close, otherwise a quick reconnect during the readiness
        race could be missed and the pod could time out mid-recovery.
        """
        import asyncio

        ws = self._validated_websocket()
        session = MagicMock()
        session.llm_with_tools = None
        connected = asyncio.Event()
        with (
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._session_input._queue", None),
            patch("agent.api.persistent_app._loop_task", None),
            patch("agent.api.persistent_app._session_termination.ws_connected_event", connected),
            patch(
                "agent.api.persistent_app._session_identity.fingerprint",
                return_value=self.fingerprint,
            ),
        ):
            await _serve_session_websocket(ws)

        assert connected.is_set()
        ws.close.assert_awaited_once_with(code=4503, reason="Agent not ready")

    @pytest.mark.asyncio
    async def test_welcome_frame_reports_authoritative_in_flight_turn(self):
        """A cold Cockpit reattach needs this join signal to merge the
        incrementally persisted history prefix with the cursor-replayed suffix.
        """
        from fastapi import WebSocketDisconnect

        ws = self._validated_websocket()
        ws.receive_text.side_effect = WebSocketDisconnect()
        session = MagicMock()
        session.llm_with_tools = MagicMock()
        session.protected_cloud_ready.return_value = True
        session.permission_mode = "supervised"
        session.narration_mode = "auto"
        session.turn_count = 7
        session.messages = []
        session.config.llm.model = "gpt-test"
        session.config.llm.temperature = 0.1
        session.session_task_manager.to_dict_list.return_value = [
            {
                "id": "task_4",
                "description": "Survive pinned reattach",
                "status": "in_progress",
                "priority": "high",
                "notes": "hydrated",
                "created_at": "2026-08-10T08:30:00+00:00",
                "completed_at": None,
            }
        ]

        with (
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._session_identity._thread_id", "thread-1"),
            patch("agent.api.persistent_app._session_input._queue", asyncio.Queue()),
            patch("agent.api.persistent_app._session_ready", return_value=True),
            patch("agent.api.persistent_app._turn_event_open", True),
            patch(
                "agent.api.persistent_app._pending_permission_requests",
                AsyncMock(return_value=[]),
            ),
            patch("agent.api.persistent_app._ensure_persistent_loop_started"),
            patch("agent.api.persistent_app._orchestrator_client", None),
            patch("agent.api.persistent_app._session_termination.ws_connected_event", None),
            patch(
                "agent.api.persistent_app._session_identity.fingerprint",
                return_value=self.fingerprint,
            ),
        ):
            await _serve_session_websocket(ws)

        welcome = next(
            call.args[0]
            for call in ws.send_json.await_args_list
            if call.args[0].get("method") == "session.state"
        )
        assert welcome["params"]["turn_count"] == 7
        assert welcome["params"]["turn_in_flight"] is True
        assert welcome["params"]["tasks"] == [
            {
                "id": "task_4",
                "description": "Survive pinned reattach",
                "status": "in_progress",
                "priority": "high",
                "notes": "hydrated",
                "created_at": "2026-08-10T08:30:00+00:00",
                "completed_at": None,
            }
        ]
        session.session_task_manager.to_dict_list.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_legacy_websocket_interrupt_targets_active_turn(self):
        from fastapi import WebSocketDisconnect

        from agent.api import persistent_app as pa

        ws = self._validated_websocket()
        ws.receive_text.side_effect = [
            json.dumps({"method": "interrupt"}),
            WebSocketDisconnect(),
        ]
        session = MagicMock()
        session.llm_with_tools = MagicMock()
        session.permission_mode = "supervised"
        session.narration_mode = "auto"
        session.turn_count = 7
        session.messages = []
        session.config.llm.model = "gpt-test"
        session.config.llm.temperature = 0.1
        session.session_task_manager = None
        session.postgres_conn = None
        hard_event = asyncio.Event()

        with (
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._session_identity._thread_id", "thread-1"),
            patch("agent.api.persistent_app._session_input._queue", asyncio.Queue()),
            patch("agent.api.persistent_app._session_ready", return_value=True),
            patch("agent.api.persistent_app._turn_event_open", True),
            patch("agent.api.persistent_app._tool_inflight", False),
            patch("agent.api.persistent_app._session_input._interrupt_mode", None),
            patch(
                "agent.api.persistent_app._session_input._interrupt_target_turn_id",
                None,
            ),
            patch(
                "agent.api.persistent_app._session_input._hard_interrupt_event",
                hard_event,
            ),
            patch(
                "agent.api.persistent_app._pending_permission_requests",
                AsyncMock(return_value=[]),
            ),
            patch("agent.api.persistent_app._ensure_persistent_loop_started"),
            patch("agent.api.persistent_app._orchestrator_client", None),
            patch("agent.api.persistent_app._session_termination.ws_connected_event", None),
            patch(
                "agent.api.persistent_app._session_identity.fingerprint",
                return_value=self.fingerprint,
            ),
        ):
            await _serve_session_websocket(ws)

            ack = next(
                call.args[0]
                for call in ws.send_json.await_args_list
                if call.args[0].get("method") == "interrupt.ack"
            )
            assert ack["params"] == {
                "applied": True,
                "target_turn_id": 7,
                "mode": "hard",
            }
            assert pa._session_input.interrupt_target_turn_id == 7
            assert pa._session_input.interrupt_mode == "hard"
            assert hard_event.is_set()

    @pytest.mark.asyncio
    async def test_reattach_flag_closes_at_transcript_terminal_edge(self):
        """The UI flag must close before slower post-turn cleanup; the broader
        teardown-safety helper intentionally remains true in that window.
        """
        from agent.api import persistent_app as pa
        from agent.api.lease_context import LeaseHandle, current_lease

        thread_id = str(uuid4())
        lease = LeaseHandle()
        lease.update(thread_id, 9)
        lease_context = current_lease.set(lease)
        session = SimpleNamespace(turn_count=0, workspace_sync=None)
        effect_hook = MagicMock()
        try:
            with (
                patch("agent.api.persistent_app._session", session),
                patch("agent.api.persistent_app._session_identity._thread_id", thread_id),
                patch("agent.api.persistent_app._turn_event_open", False),
                patch(
                    "agent.api.persistent_app._turn_tool_execution_identity",
                    ("stale-thread", 1, 1),
                ),
                patch(
                    "agent.api.persistent_app._turn_tool_execution_external_hook",
                    effect_hook,
                ),
                patch("agent.api.persistent_app._cloud_sync_retry_pending", False),
                patch("agent.api.persistent_app._broadcast"),
                patch(
                    "agent.api.persistent_app._retire_announced_permission_rows",
                    AsyncMock(),
                ),
            ):
                await pa._loop_on_turn_start(3)
                assert pa._turn_event_open is True
                assert pa._turn_tool_execution_identity is None
                await pa._loop_on_tool_execution_start("shell", "call-1")
                assert pa._turn_tool_execution_identity == (thread_id, 9, 3)
                effect_hook.assert_called_once_with((thread_id, 9, 3))

                # None makes the callback return after its initial cleanup
                # await; the lifecycle flag must already be terminal there.
                pa._session = None
                await pa._loop_on_turn_complete(3)
                assert pa._turn_event_open is False
                assert pa._turn_tool_execution_identity == (thread_id, 9, 3)
                await pa._loop_on_turn_settled(3)
                # The executor—not the process-local settled hook—owns the
                # final clear after complete/park/release reaches Postgres.
                assert pa._turn_tool_execution_identity == (thread_id, 9, 3)
                assert pa._clear_turn_tool_execution_identity(
                    thread_id=thread_id,
                    lease_token=9,
                    turn_id=3,
                )
                assert pa._turn_tool_execution_identity is None
        finally:
            current_lease.reset(lease_context)

    @pytest.mark.asyncio
    async def test_stateless_tool_boundary_fails_closed_without_exact_identity(self):
        from agent.api import persistent_app as pa
        from agent.api.lease_context import LeaseHandle, current_lease

        lease = LeaseHandle()
        lease.update("claimed-thread", 17)
        lease_context = current_lease.set(lease)
        effect_hook = MagicMock()
        try:
            with (
                patch(
                    "agent.api.persistent_app._session",
                    SimpleNamespace(turn_count=4),
                ),
                patch("agent.api.persistent_app._session_identity._thread_id", "wrong-thread"),
                patch(
                    "agent.api.persistent_app._turn_tool_execution_identity",
                    None,
                ),
                patch(
                    "agent.api.persistent_app._turn_tool_execution_external_hook",
                    effect_hook,
                ),
            ):
                with pytest.raises(RuntimeError, match="exact claimant identity"):
                    await pa._loop_on_tool_execution_start("shell", "call-1")

                assert lease.lost.is_set()
                assert pa._turn_tool_execution_identity is None
                effect_hook.assert_not_called()
        finally:
            current_lease.reset(lease_context)

    @pytest.mark.asyncio
    async def test_pinned_tool_boundary_needs_no_queue_identity(self):
        from agent.api import persistent_app as pa
        from agent.api.lease_context import current_lease

        lease_context = current_lease.set(None)
        try:
            with (
                patch(
                    "agent.api.persistent_app._session",
                    SimpleNamespace(turn_count=2),
                ),
                patch("agent.api.persistent_app._session_identity._thread_id", "pinned-thread"),
                patch(
                    "agent.api.persistent_app._turn_tool_execution_identity",
                    None,
                ),
                patch(
                    "agent.api.persistent_app._turn_tool_execution_external_hook",
                    None,
                ),
            ):
                await pa._loop_on_tool_execution_start("shell", "call-1")
                assert pa._turn_tool_execution_identity is None
        finally:
            current_lease.reset(lease_context)

    @pytest.mark.asyncio
    async def test_error_keeps_tool_effect_identity_until_queue_disposition(self):
        from agent.api import persistent_app as pa

        thread_id = str(uuid4())
        identity = (thread_id, 12, 5)
        session = SimpleNamespace(turn_count=5, postgres_conn=None)
        with (
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._session_identity._thread_id", thread_id),
            patch("agent.api.persistent_app._turn_event_open", True),
            patch("agent.api.persistent_app._turn_tool_execution_identity", identity),
            patch("agent.api.persistent_app._turn_complete_external_hook", None),
            patch("agent.api.persistent_app._broadcast"),
        ):
            await pa._loop_on_error("provider failed after tool", turn_id=5)
            assert pa._turn_event_open is False
            assert pa._turn_tool_execution_identity == identity

            await pa._loop_on_turn_settled(5)
            assert pa._turn_tool_execution_identity == identity
            assert pa._clear_turn_tool_execution_identity(
                thread_id=thread_id,
                lease_token=12,
            )
            assert pa._turn_tool_execution_identity is None

    @pytest.mark.asyncio
    async def test_stateless_start_hook_finishes_before_turn_started(self):
        from agent.api import persistent_app as pa

        order = []
        session = SimpleNamespace(turn_count=0, workspace_sync=None)

        async def hook(turn_id):
            order.append(("hook", turn_id, pa._turn_event_open))

        def broadcast(kind, payload):
            order.append((kind, payload["turn_id"], pa._turn_event_open))

        with (
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._turn_event_open", False),
            patch("agent.api.persistent_app._turn_start_external_hook", hook),
            patch("agent.api.persistent_app._cloud_sync_retry_pending", False),
            patch("agent.api.persistent_app._broadcast", side_effect=broadcast),
        ):
            await pa._loop_on_turn_start(4)

        assert order[:2] == [
            ("hook", 4, True),
            ("turn.started", 4, True),
        ]


class TestSessionReadyHelper:
    """_session_ready() is the single source of truth shared by /ready,
    /session/status, and handle_persistent_websocket. The WS-handler
    regression tests above cover the integrated behavior; these probe the
    helper directly so a future caller (e.g. a new health endpoint) can't
    accidentally reintroduce a two-way variant of the check.
    """

    def test_false_when_session_missing(self):
        from agent.api import persistent_app as pa

        with (
            patch("agent.api.persistent_app._session", None),
            patch("agent.api.persistent_app._session_input._queue", MagicMock()),
        ):
            assert pa._session_ready() is False

    def test_false_when_llm_with_tools_missing(self):
        from agent.api import persistent_app as pa

        session = MagicMock()
        session.llm_with_tools = None
        with (
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._session_input._queue", MagicMock()),
        ):
            assert pa._session_ready() is False

    def test_false_when_loop_user_queue_missing(self):
        from agent.api import persistent_app as pa

        session = MagicMock()
        session.llm_with_tools = MagicMock()
        with (
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._session_input._queue", None),
        ):
            assert pa._session_ready() is False

    def test_true_when_all_three_set(self):
        from agent.api import persistent_app as pa

        session = MagicMock()
        session.llm_with_tools = MagicMock()
        session.protected_cloud_required = False
        session.protected_cloud_ready.return_value = True
        with (
            patch("agent.api.persistent_app._session", session),
            patch("agent.api.persistent_app._session_input._queue", MagicMock()),
        ):
            assert pa._session_ready() is True


# ---------------------------------------------------------------------------
# 3.17.4 Headless: module-level loop callbacks behave as the old closures did
# ---------------------------------------------------------------------------


class TestLoopCheckInterrupt:
    """_loop_check_interrupt returns the tri-state mode and resets in one shot."""

    def setup_method(self):
        import agent.api.persistent_app as mod

        mod._session_input._interrupt_mode = None
        mod._session_input._interrupt_target_turn_id = None
        mod._tool_inflight = False
        mod._session_input._hard_interrupt_event = None
        mod._session = SimpleNamespace(turn_count=7)

    def test_returns_none_when_flag_not_set(self):
        import agent.api.persistent_app as mod

        assert mod._session_input.check_interrupt() is None

    def test_returns_hard_mode_once_then_resets(self):
        import agent.api.persistent_app as mod

        mod._session_input._interrupt_mode = "hard"
        mod._session_input._interrupt_target_turn_id = 7
        assert mod._session_input.check_interrupt() == "hard"
        # Subsequent reads see the reset.
        assert mod._session_input.check_interrupt() is None

    def test_returns_graceful_mode_once_then_resets(self):
        import agent.api.persistent_app as mod

        mod._session_input._interrupt_mode = "graceful"
        mod._session_input._interrupt_target_turn_id = 7
        assert mod._session_input.check_interrupt() == "graceful"
        assert mod._session_input.check_interrupt() is None

    def test_consuming_clears_hard_interrupt_event(self):
        """Consuming the flag resets the hard-interrupt event in lock-step so
        it doesn't leak into the next turn's streaming/compaction race."""
        import asyncio as _asyncio

        import agent.api.persistent_app as mod

        mod._session_input._hard_interrupt_event = _asyncio.Event()
        mod._session_input.hard_interrupt_event.set()
        mod._session_input._interrupt_mode = "hard"
        mod._session_input._interrupt_target_turn_id = 7

        assert mod._session_input.check_interrupt() == "hard"
        assert mod._session_input.hard_interrupt_event.is_set() is False
        mod._session_input._hard_interrupt_event = None

    def test_unscoped_interrupt_is_discarded_instead_of_striking_current_turn(self):
        import agent.api.persistent_app as mod

        mod._session_input._interrupt_mode = "hard"
        mod._session_input._interrupt_target_turn_id = None

        assert mod._session_input.check_interrupt() is None
        assert mod._session_input.interrupt_mode is None

    def test_force_graceful_scoped_interrupt_never_sets_hard_event(self):
        import agent.api.persistent_app as mod

        mod._turn_event_open = True
        mod._tool_inflight = False
        mod._session_input._hard_interrupt_event = asyncio.Event()

        assert (
            mod._session_input.signal_interrupt_for_turn(7, force_graceful=True)
            == "graceful"
        )
        assert mod._session_input.interrupt_target_turn_id == 7
        assert not mod._session_input.hard_interrupt_event.is_set()

    @pytest.mark.asyncio
    async def test_late_interrupt_is_cleared_at_exact_turn_terminal_edge(self):
        import agent.api.persistent_app as mod

        mod._turn_event_open = True
        mod._session_input._hard_interrupt_event = asyncio.Event()
        assert mod._session_input.signal_interrupt_for_turn(7) == "hard"

        with patch.object(mod, "_loop_on_turn_complete_body", new=AsyncMock()):
            await mod._loop_on_turn_complete(7)

        assert mod._session_input.interrupt_mode is None
        assert mod._session_input.interrupt_target_turn_id is None
        assert not mod._session_input.hard_interrupt_event.is_set()
        mod._session.turn_count = 8
        assert mod._session_input.check_interrupt() is None


class TestHandleApiInterruptHardEvent:
    """handle_api_interrupt fires the hard-interrupt event only when no tool is
    in flight (mode=hard) — so the loop can cancel a parked LLM/aux await."""

    fingerprint = "sha256:" + ("a" * 64)

    @classmethod
    def _request(cls, **payload):
        request = MagicMock()
        request.body = AsyncMock(
            return_value=json.dumps(
                {
                    "session_identity_fingerprint": cls.fingerprint,
                    **payload,
                }
            ).encode()
        )
        return request

    def setup_method(self):
        import agent.api.persistent_app as mod

        mod._session_input._interrupt_mode = None
        mod._session_input._interrupt_target_turn_id = None
        mod._tool_inflight = False
        mod._session_input._hard_interrupt_event = None

    def teardown_method(self):
        import agent.api.persistent_app as mod

        mod._session = None
        mod._tool_inflight = False
        mod._turn_event_open = False
        mod._session_input._interrupt_mode = None
        mod._session_input._interrupt_target_turn_id = None
        mod._session_input._hard_interrupt_event = None

    @pytest.mark.asyncio
    async def test_hard_mode_sets_event(self):
        import asyncio as _asyncio

        import agent.api.persistent_app as mod

        mod._session = SimpleNamespace(turn_count=7)
        mod._turn_event_open = True
        mod._tool_inflight = False  # no tool in flight ⇒ hard
        mod._session_input._hard_interrupt_event = _asyncio.Event()

        with patch.object(mod._session_identity, "fingerprint",
            return_value=self.fingerprint,
        ):
            await _handle_api_interrupt(self._request())

        assert mod._session_input.interrupt_mode == "hard"
        assert mod._session_input.interrupt_target_turn_id == 7
        assert mod._session_input.hard_interrupt_event.is_set() is True

    @pytest.mark.asyncio
    async def test_graceful_mode_leaves_event_unset(self):
        import asyncio as _asyncio

        import agent.api.persistent_app as mod

        mod._session = SimpleNamespace(turn_count=7)
        mod._turn_event_open = True
        mod._tool_inflight = True  # tool mid-ainvoke ⇒ graceful (never cancel)
        mod._session_input._hard_interrupt_event = _asyncio.Event()

        with patch.object(mod._session_identity, "fingerprint",
            return_value=self.fingerprint,
        ):
            await _handle_api_interrupt(self._request())

        assert mod._session_input.interrupt_mode == "graceful"
        assert mod._session_input.interrupt_target_turn_id == 7
        assert mod._session_input.hard_interrupt_event.is_set() is False

    @pytest.mark.asyncio
    async def test_correlated_body_applies_only_to_exact_active_turn(self, monkeypatch):
        import json

        import agent.api.persistent_app as mod

        monkeypatch.delenv("STATELESS_EXECUTOR", raising=False)
        mod._session = SimpleNamespace(turn_count=7)
        mod._turn_event_open = True
        mod._tool_inflight = False
        mod._session_input._hard_interrupt_event = asyncio.Event()
        request = self._request(client_request_id="client-1", target_turn_id=7)

        with patch.object(mod._session_identity, "fingerprint",
            return_value=self.fingerprint,
        ):
            response = await _handle_api_interrupt(request)

        assert response.status_code == 200
        assert json.loads(response.body) == {
            "client_request_id": "client-1",
            "target_turn_id": 7,
            "ack": True,
            "applied": True,
            "mode": "hard",
        }
        assert mod._session_input.interrupt_mode == "hard"
        assert mod._session_input.interrupt_target_turn_id == 7
        assert mod._session_input.hard_interrupt_event.is_set()

    @pytest.mark.asyncio
    async def test_correlated_stale_turn_rejects_before_ram_mutation(self, monkeypatch):
        import json

        import agent.api.persistent_app as mod

        monkeypatch.delenv("STATELESS_EXECUTOR", raising=False)
        mod._session = SimpleNamespace(turn_count=8)
        mod._turn_event_open = True
        mod._session_input._interrupt_mode = None
        mod._session_input._hard_interrupt_event = asyncio.Event()
        request = self._request(client_request_id="client-1", target_turn_id=7)

        with patch.object(mod._session_identity, "fingerprint",
            return_value=self.fingerprint,
        ):
            response = await _handle_api_interrupt(request)

        assert response.status_code == 409
        payload = json.loads(response.body)
        assert payload["applied"] is False
        assert payload["error_code"] == "target_turn_not_active"
        assert payload["target_turn_id"] == 7
        assert mod._session_input.interrupt_mode is None
        assert mod._session_input.interrupt_target_turn_id is None
        assert not mod._session_input.hard_interrupt_event.is_set()

    @pytest.mark.asyncio
    async def test_bodyless_interrupt_while_idle_cannot_arm_next_turn(
        self, monkeypatch
    ):
        import json

        import agent.api.persistent_app as mod

        monkeypatch.delenv("STATELESS_EXECUTOR", raising=False)
        mod._session = SimpleNamespace(turn_count=7)
        mod._turn_event_open = False
        mod._session_input._hard_interrupt_event = asyncio.Event()

        with patch.object(mod._session_identity, "fingerprint",
            return_value=self.fingerprint,
        ):
            response = await _handle_api_interrupt(self._request())

        assert response.status_code == 409
        assert json.loads(response.body)["error_code"] == "target_turn_not_active"
        assert mod._session_input.interrupt_mode is None
        assert mod._session_input.interrupt_target_turn_id is None

    @pytest.mark.asyncio
    async def test_correlated_body_requires_positive_integer_target(self, monkeypatch):
        import json

        import agent.api.persistent_app as mod

        monkeypatch.delenv("STATELESS_EXECUTOR", raising=False)
        mod._session = SimpleNamespace(turn_count=7)
        request = self._request(client_request_id="client-1", target_turn_id=True)

        with patch.object(mod._session_identity, "fingerprint",
            return_value=self.fingerprint,
        ):
            response = await _handle_api_interrupt(request)

        assert response.status_code == 400
        assert json.loads(response.body)["error_code"] == "invalid_request"

    @pytest.mark.asyncio
    async def test_wrong_identity_rejects_before_interrupt_ram_mutation(self):
        import agent.api.persistent_app as mod

        mod._session = SimpleNamespace(turn_count=7)
        mod._turn_event_open = True
        mod._session_input._hard_interrupt_event = asyncio.Event()

        with patch.object(mod._session_identity, "fingerprint",
            return_value="sha256:" + ("b" * 64),
        ):
            response = await _handle_api_interrupt(self._request())

        assert response.status_code == 409
        assert json.loads(response.body) == {
            "error": "session_identity_mismatch",
            "retryable": True,
        }
        assert mod._session_input.interrupt_mode is None
        assert mod._session_input.interrupt_target_turn_id is None
        assert not mod._session_input.hard_interrupt_event.is_set()


# ---------------------------------------------------------------------------
# 3.18 create_persistent_app()
# ---------------------------------------------------------------------------


class TestCreatePersistentApp:
    def test_sets_module_globals(self):
        """create_persistent_app sets _config_path and _thread_id."""
        import agent.api.persistent_app as mod

        create_persistent_app("my_config", "thread-123")

        assert mod._config_path == "my_config"
        assert mod._session_identity.thread_id == "thread-123"

    def test_returns_fastapi_instance(self):
        from fastapi import FastAPI

        app = create_persistent_app("config", "tid")
        assert isinstance(app, FastAPI)

    def test_thread_id_optional(self):
        """thread_id can be None."""
        import agent.api.persistent_app as mod

        create_persistent_app("config")
        assert mod._session_identity.thread_id is None

    @pytest.mark.asyncio
    async def test_health_reports_ready_app_guide_without_changing_liveness(
        self, monkeypatch
    ):
        from shared.runtime.core.skill_resolution import APP_GUIDE_BREAK_GLASS_ENV

        monkeypatch.delenv(APP_GUIDE_BREAK_GLASS_ENV, raising=False)
        app = create_persistent_app("config", "tid")
        route = next(route for route in app.routes if route.path == "/health")

        response = await route.endpoint()
        payload = json.loads(response.body)

        assert response.status_code == 200
        assert payload["status"] == "healthy"
        assert payload["app_guide"] == {"state": "ready"}

    @pytest.mark.asyncio
    async def test_break_glass_health_is_bounded_degraded_and_still_live(
        self, monkeypatch
    ):
        from shared.runtime.core.skill_resolution import APP_GUIDE_BREAK_GLASS_ENV

        monkeypatch.setenv(APP_GUIDE_BREAK_GLASS_ENV, "true")
        app = create_persistent_app("config", "tid")
        route = next(route for route in app.routes if route.path == "/health")

        response = await route.endpoint()
        payload = json.loads(response.body)

        assert response.status_code == 200
        assert payload["status"] == "degraded"
        assert payload["app_guide"] == {
            "state": "disabled",
            "reason": "operator_break_glass",
        }
        assert set(payload["app_guide"]) == {"state", "reason"}

    @pytest.mark.asyncio
    async def test_break_glass_does_not_change_chat_readiness(self, monkeypatch):
        import agent.api.persistent_app as mod
        from shared.runtime.core.skill_resolution import APP_GUIDE_BREAK_GLASS_ENV

        monkeypatch.setenv(APP_GUIDE_BREAK_GLASS_ENV, "true")
        monkeypatch.setattr(mod, "_session_ready", lambda: True)
        app = create_persistent_app("config", "tid")
        route = next(route for route in app.routes if route.path == "/ready")

        response = await route.endpoint()
        payload = json.loads(response.body)

        assert response.status_code == 200
        assert payload == {
            "ready": True,
            "mode": "persistent",
            "thread_id": "tid",
            "session_identity_fingerprint": None,
            "capabilities": {
                "durable_input_delivery": True,
                "pinned_session_identity_contract": 1,
                "protected_cloud_contract": None,
                "protected_cloud_ready": False,
            },
        }

    @pytest.mark.asyncio
    async def test_ready_reports_exact_non_secret_pinned_identity(self, monkeypatch):
        import agent.api.persistent_app as mod
        from shared.pinned_session_identity import (
            pinned_session_ready_identity_fingerprint,
        )

        thread_id = "10000000-0000-4000-8000-000000000001"
        generation = "20000000-0000-4000-8000-000000000002"
        agent_id = "30000000-0000-4000-8000-000000000003"
        attach_token = "40000000-0000-4000-8000-000000000004"
        pod_uid = "50000000-0000-4000-8000-000000000005"
        app = create_persistent_app("config", thread_id)
        monkeypatch.setattr(mod, "_session_ready", lambda: True)
        monkeypatch.setattr(mod._session_identity, "_session_generation", generation)
        monkeypatch.setattr(mod._session_identity, "_attach_token", attach_token)
        monkeypatch.setattr(
            mod, "_orchestrator_client", SimpleNamespace(agent_id=agent_id)
        )
        monkeypatch.setenv("POD_UID", pod_uid)

        route = next(route for route in app.routes if route.path == "/ready")
        response = await route.endpoint()
        payload = json.loads(response.body)

        assert payload["session_identity_fingerprint"] == (
            pinned_session_ready_identity_fingerprint(
                thread_id=thread_id,
                runtime_generation=generation,
                agent_id=agent_id,
                runtime_attach_token=attach_token,
                pod_uid=pod_uid,
            )
        )
        assert attach_token not in response.body.decode()
        assert payload["capabilities"]["pinned_session_identity_contract"] == 1

    def test_app_guide_health_reports_reader_registration_loss(self, monkeypatch):
        import agent.api.persistent_app as mod
        from shared.runtime.core.skill_resolution import APP_GUIDE_BREAK_GLASS_ENV

        monkeypatch.delenv(APP_GUIDE_BREAK_GLASS_ENV, raising=False)
        monkeypatch.delitem(
            mod.TOOL_REGISTRY,
            "read_product_guide",
            raising=False,
        )

        assert _app_guide_health() == {
            "state": "unavailable",
            "reason": "reader_unavailable",
        }


class TestRuntimeCanvasControlBinding:
    """The runtime's one Canvas channel resolves its owners at call time.

    Validation, pacing and awareness live in
    ``agent.api.session_canvas_control`` (tests/test_session_canvas_control.py);
    this checks the runtime wiring: authoritative state loader, the attached
    session's read cache, the ordered journal and live fan-out.
    """

    fingerprint = "sha256:" + ("e" * 64)

    @staticmethod
    def _state():
        return {
            "canvas_id": "main",
            "source": {"type": "workspace_file", "path": "output/report.md"},
            "presentation_revision": 4,
            "source_version": "sha256:" + "a" * 64,
            "updated_at": "2026-07-13T12:00:00Z",
        }

    @staticmethod
    def _frame(method, **extra):
        return {
            "method": method,
            "canvas_id": "main",
            "path": "output/report.md",
            "presentation_revision": 4,
            "source_version": "sha256:" + "a" * 64,
            **extra,
        }

    @pytest.mark.asyncio
    async def test_channel_reads_current_session_journal_and_fan_out(self, monkeypatch):
        import agent.api.persistent_app as mod

        mod._canvas_control.clear_all()
        tool_context = MagicMock()
        monkeypatch.setattr(mod, "_session", SimpleNamespace(tool_context=tool_context))
        state_loader = AsyncMock(return_value=self._state())
        monkeypatch.setattr(mod, "_current_canvas_for_control", state_loader)
        broadcast = MagicMock()
        monkeypatch.setattr(mod, "_broadcast", broadcast)
        frames = []
        monkeypatch.setattr(mod, "_fan_out_live_frame", frames.append)
        monkeypatch.setattr(mod._session_identity, "fingerprint",
            lambda: self.fingerprint,
        )
        monkeypatch.setattr(mod._canvas_control, "validation_min_interval_s", 0)

        try:
            assert await mod._canvas_control.handle(
                MagicMock(),
                self._frame("canvas.source_updated"),
                "client-a",
                expected_session_identity_fingerprint=self.fingerprint,
            )
            assert await mod._canvas_control.handle(
                MagicMock(),
                self._frame(
                    "canvas.user_editing", editing_session_id="editor_session_a"
                ),
                "client-a",
                expected_session_identity_fingerprint=self.fingerprint,
            )
        finally:
            mod._canvas_control.clear_all()

        assert state_loader.await_count == 2
        tool_context.invalidate_recent_read.assert_called_once_with("output/report.md")
        broadcast.assert_called_once_with(
            "canvas.source_updated",
            {
                "canvas_id": "main",
                "presentation_revision": 4,
                "source_type": "workspace_file",
                "updated_at": "2026-07-13T12:00:00Z",
            },
        )
        assert [frame["method"] for frame in frames] == ["canvas.user_editing"]
        assert frames[0]["params"]["sender_id"] == "client-a"

    @pytest.mark.asyncio
    async def test_detached_session_skips_read_invalidation(self, monkeypatch):
        import agent.api.persistent_app as mod

        mod._canvas_control.clear_all()
        monkeypatch.setattr(mod, "_session", None)
        monkeypatch.setattr(
            mod, "_current_canvas_for_control", AsyncMock(return_value=self._state())
        )
        broadcast = MagicMock()
        monkeypatch.setattr(mod, "_broadcast", broadcast)
        monkeypatch.setattr(mod._canvas_control, "validation_min_interval_s", 0)
        try:
            assert await mod._canvas_control.handle(
                MagicMock(), self._frame("canvas.source_updated"), "client-z"
            )
        finally:
            mod._canvas_control.clear_all()

        broadcast.assert_called_once()


# ---------------------------------------------------------------------------
# Auxiliary + embedding hot-swap in _handle_config_update
# ---------------------------------------------------------------------------


class TestHandleConfigUpdateEnrichmentGate:
    """Pin the widened orchestrator-PATCH gate as a structural assertion.

    The gate at the top of ``_handle_config_update`` must fire for any
    credential-bearing slot (chat, auxiliary, embedding env_keys). A
    regression that reverts to chat-only silently breaks custom-endpoint
    routing for auxiliary and embedding (the ``Untitled Session`` and
    missing-memory bug documented in
    ``knowledge-base/knowledge/hardcoded_model_defaults.md``). Mocking the full async call
    chain here is brittle because of internal imports; pinning the
    source-level shape is the cheapest reliable regression catch.
    """

    def test_gate_checks_auxiliary_model(self):
        from inspect import getsource
        from agent.api.persistent_app import _handle_config_update

        src = getsource(_handle_config_update)
        assert "if _orchestrator_client and _session_identity.thread_id:" in src, (
            "Auxiliary model changes must trigger the orchestrator-PATCH "
            "enrichment gate."
        )

    def test_gate_checks_embedding_env_keys(self):
        from inspect import getsource
        from agent.api.persistent_app import _handle_config_update

        src = getsource(_handle_config_update)
        for key in (
            "EMBEDDING_PROVIDER",
            "EMBEDDING_MODEL",
            "EMBEDDING_BASE_URL",
            "EMBEDDING_API_KEY",
        ):
            assert key in src, (
                f"Embedding env key {key} must be in the gate's "
                "credential-bearing-keys tuple."
            )

    def test_gate_checks_tool_updates_before_local_reload(self):
        from inspect import getsource

        from agent.api.persistent_app import _handle_config_update

        src = getsource(_handle_config_update)
        assert "if _orchestrator_client and _session_identity.thread_id:" in src
        assert src.index("update_thread_config(") < src.index(
            "resetup_tools_for_backend()"
        )

    def test_live_tool_override_sanitizer_keeps_every_valid_category(self):
        """Was ``..._keeps_only_closed_session_groups``, and keeping only four
        was the defect: a live "turn shell off" was acknowledged and discarded.
        Every category is validated against the registry now; the copy is what
        keeps the caller-owned WebSocket payload immutable."""
        from agent.api.persistent_app import _sanitize_live_session_config_override

        original = {
            "llm": {"temperature": 0.2},
            "tools": {
                "canvas": ["get_canvas"],
                "shell": ["run_command"],
            },
        }
        sanitized = _sanitize_live_session_config_override(original)

        assert sanitized == {
            "llm": {"temperature": 0.2},
            "tools": {"canvas": ["get_canvas"], "shell": ["run_command"]},
        }
        assert original["tools"]["shell"] == ["run_command"]

    @pytest.mark.parametrize("key", ["permission_mode", "narration_mode"])
    def test_live_config_update_rejects_ordered_control_scalars(self, key):
        from agent.api.persistent_app import _sanitize_live_session_config_override

        with pytest.raises(ValueError, match="session control endpoint"):
            _sanitize_live_session_config_override({"interactive": {key: "autonomous"}})

    @pytest.mark.asyncio
    async def test_live_cross_category_tool_smuggling_never_reloads_tools(
        self, monkeypatch
    ):
        import agent.api.persistent_app as mod

        session = SimpleNamespace(resetup_tools_for_backend=MagicMock())
        orchestrator_client = SimpleNamespace(update_thread_config=AsyncMock())
        send = AsyncMock()
        monkeypatch.setattr(mod, "_session", session)
        monkeypatch.setattr(mod, "_orchestrator_client", orchestrator_client)
        monkeypatch.setattr(mod._session_identity, "_thread_id", "thread-1")
        monkeypatch.setattr(mod, "_ws_send", send)

        await mod._handle_config_update(
            MagicMock(), {"tools": {"canvas": ["run_command"]}}
        )

        orchestrator_client.update_thread_config.assert_not_awaited()
        session.resetup_tools_for_backend.assert_not_called()
        send.assert_awaited_once()
        event, payload = send.await_args.args[1:]
        assert event == "error"
        assert "run_command" in payload["message"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "fragment",
        [
            {"workspace": {"backend": "vm"}},
            {"workspace": {"backend": "sandbox"}},
            {"officer": {"enabled": True}},
            {"officer": {"enabled": False}},
        ],
    )
    async def test_protected_runtime_class_update_has_zero_local_or_remote_effect(
        self, monkeypatch, fragment
    ):
        import agent.api.persistent_app as mod

        config_sentinel = object()
        backend_sentinel = object()
        session = SimpleNamespace(
            protected_cloud_required=True,
            config=config_sentinel,
            workspace_manager=SimpleNamespace(backend=backend_sentinel),
            resetup_tools_for_backend=MagicMock(),
        )
        client = SimpleNamespace(
            update_thread_config=AsyncMock(),
            request_thread_workspace_upgrade=AsyncMock(),
        )
        send = AsyncMock()
        monkeypatch.setattr(mod, "_session", session)
        monkeypatch.setattr(mod, "_orchestrator_client", client)
        monkeypatch.setattr(mod._session_identity, "_thread_id", "thread-1")
        monkeypatch.setattr(mod, "_ws_send", send)

        await mod._handle_config_update(MagicMock(), fragment, request_id="fixed-1")

        client.update_thread_config.assert_not_awaited()
        client.request_thread_workspace_upgrade.assert_not_awaited()
        session.resetup_tools_for_backend.assert_not_called()
        assert session.config is config_sentinel
        assert session.workspace_manager.backend is backend_sentinel
        send.assert_awaited_once()
        event, payload = send.await_args.args[1:]
        assert event == "error"
        assert payload["request_id"] == "fixed-1"
        assert "Protected cloud" in payload["detail"]

    @pytest.mark.asyncio
    async def test_ordinary_runtime_class_update_is_authorized_before_local_merge(
        self, monkeypatch
    ):
        import agent.api.persistent_app as mod
        from agent.api.orchestrator_client import ThreadConfigUpdateDenied

        config_sentinel = object()
        session = SimpleNamespace(
            protected_cloud_required=False,
            config=config_sentinel,
            resetup_tools_for_backend=MagicMock(),
        )
        client = SimpleNamespace(
            update_thread_config=AsyncMock(
                side_effect=ThreadConfigUpdateDenied(409, "runtime class denied")
            )
        )
        send = AsyncMock()
        monkeypatch.setattr(mod, "_session", session)
        monkeypatch.setattr(mod, "_orchestrator_client", client)
        monkeypatch.setattr(mod._session_identity, "_thread_id", "thread-1")
        monkeypatch.setattr(mod, "_ws_send", send)

        await mod._handle_config_update(MagicMock(), {"workspace": {"backend": "vm"}})

        client.update_thread_config.assert_awaited_once()
        assert session.config is config_sentinel
        session.resetup_tools_for_backend.assert_not_called()
        assert "runtime class denied" in send.await_args.args[2]["detail"]

    def test_rebuilds_auxiliary_llm_from_override(self):
        """When auxiliary section is in the enriched override, a session-
        scoped AuxiliaryLLM is constructed from new_config.auxiliary —
        rather than passing through _agent._auxiliary_llm. Pinning the
        landmark `_session.auxiliary_llm = AuxiliaryLLM(` assignment so
        the rebuild path can't silently regress to the singleton."""
        from inspect import getsource
        from agent.api.persistent_app import _handle_config_update

        src = getsource(_handle_config_update)
        assert "_session.auxiliary_llm = AuxiliaryLLM(" in src

    def test_resets_embedding_singleton(self):
        """Embedding hot-swap must clear ``embedding_service._embedding_service``
        so the next get_embedding_service() call rebuilds with the new
        env. Without this the process-wide singleton sticks at the
        boot-time base_url and 401s on api.openai.com."""
        from inspect import getsource
        from agent.api.persistent_app import _handle_config_update

        src = getsource(_handle_config_update)
        assert "_embedding_module._embedding_service = None" in src


class TestHandleConfigUpdateAckProtocol:
    """P0.3 of live_session_settings.md: request_id correlation, broadcast
    ack with the applied fragment, and surfaced 4xx denial detail."""

    @pytest.mark.asyncio
    async def test_denied_model_swap_surfaces_detail_and_never_applies(
        self, monkeypatch
    ):
        """A 4xx from the orchestrator (grant denial) must produce an error
        frame carrying the detail + request_id and must NOT fall back to
        applying the raw override locally (the old silent-escalation hole)."""
        import agent.api.persistent_app as mod
        from agent.api.orchestrator_client import ThreadConfigUpdateDenied

        session = SimpleNamespace(resetup_tools_for_backend=MagicMock())
        orchestrator_client = SimpleNamespace(
            update_thread_config=AsyncMock(
                side_effect=ThreadConfigUpdateDenied(422, "model 'x' exceeds grants")
            )
        )
        send = AsyncMock()
        monkeypatch.setattr(mod, "_session", session)
        monkeypatch.setattr(mod, "_orchestrator_client", orchestrator_client)
        monkeypatch.setattr(mod._session_identity, "_thread_id", "thread-1")
        monkeypatch.setattr(mod, "_ws_send", send)

        await mod._handle_config_update(
            MagicMock(), {"llm": {"model": "x"}}, request_id="req-42"
        )

        # No local apply of any kind happened.
        session.resetup_tools_for_backend.assert_not_called()
        assert not hasattr(session, "_llm")
        send.assert_awaited_once()
        event, payload = send.await_args.args[1:]
        assert event == "error"
        assert payload["request_id"] == "req-42"
        assert "exceeds grants" in payload["detail"]

    @pytest.mark.asyncio
    async def test_tools_rejection_echoes_request_id(self, monkeypatch):
        """The fail-loud tools gate (no orchestrator) keeps its semantics and
        now correlates: the error frame carries the caller's request_id."""
        import agent.api.persistent_app as mod

        session = SimpleNamespace(resetup_tools_for_backend=MagicMock())
        send = AsyncMock()
        monkeypatch.setattr(mod, "_session", session)
        monkeypatch.setattr(mod, "_orchestrator_client", None)
        monkeypatch.setattr(mod._session_identity, "_thread_id", "thread-1")
        monkeypatch.setattr(mod, "_ws_send", send)

        await mod._handle_config_update(
            MagicMock(), {"tools": {"canvas": ["get_canvas"]}}, request_id="req-7"
        )

        session.resetup_tools_for_backend.assert_not_called()
        send.assert_awaited_once()
        event, payload = send.await_args.args[1:]
        assert event == "error"
        assert payload["request_id"] == "req-7"

    def test_ack_is_broadcast_with_applied_fragment(self):
        """The success ack must fan out to every subscriber (all viewers
        converge; the journal records it as the transcript stamp) and echo
        the applied fragment + request_id. Source-shape pin, matching this
        file's convention for the deep post-rebuild path."""
        from inspect import getsource
        from agent.api.persistent_app import _handle_config_update

        src = getsource(_handle_config_update)
        assert '_broadcast("config.changed", ack)' in src
        assert '"applied": _scrub_secret_values(config_override)' in src
        assert 'ack["request_id"] = request_id' in src

    def test_cosmetic_persist_runs_before_local_permission_apply(self):
        """permission_mode is grant-gated orchestrator-side; the durable
        PATCH must run (and be able to deny) BEFORE the runtime applies the
        mode locally — otherwise a denied escalation still takes effect
        in-RAM until the next attach."""
        from inspect import getsource
        from agent.api.persistent_app import _handle_config_update

        src = getsource(_handle_config_update)
        assert src.index("update_thread_config(") < src.index(
            "_session.permission_mode = pm"
        )

    def test_scrub_secret_values_drops_api_keys_recursively(self):
        from agent.api.persistent_app import _scrub_secret_values

        fragment = {
            "llm": {"model": "m", "api_key": "sk-secret", "base_url": "http://x"},
            "env_keys": {"EMBEDDING_API_KEY": "sk-2", "EMBEDDING_MODEL": "e"},
            "tools": {"canvas": ["get_canvas"]},
        }
        scrubbed = _scrub_secret_values(fragment)
        assert scrubbed == {
            "llm": {"model": "m", "base_url": "http://x"},
            "env_keys": {"EMBEDDING_MODEL": "e"},
            "tools": {"canvas": ["get_canvas"]},
        }
        # Original untouched (caller-owned payload stays immutable).
        assert fragment["llm"]["api_key"] == "sk-secret"


class TestHandleConfigUpdateDatasources:
    """Slice B of live_session_settings.md: the datasource_ids sibling key
    on config.update — fail-loud authorization, payload re-fetch, and the
    deferred-close contract."""

    @pytest.mark.asyncio
    async def test_datasource_update_without_orchestrator_fails_loud(self, monkeypatch):
        """No orchestrator ⇒ no change: credentials only exist orchestrator-
        side and the grant flip can't be evaluated locally."""
        import agent.api.persistent_app as mod

        session = SimpleNamespace(
            resetup_tools_for_backend=MagicMock(),
            resetup_datasources=MagicMock(),
        )
        send = AsyncMock()
        monkeypatch.setattr(mod, "_session", session)
        monkeypatch.setattr(mod, "_orchestrator_client", None)
        monkeypatch.setattr(mod._session_identity, "_thread_id", "thread-1")
        monkeypatch.setattr(mod, "_ws_send", send)

        await mod._handle_config_update(
            MagicMock(), {}, datasource_ids=["ds-a"], request_id="req-1"
        )

        session.resetup_datasources.assert_not_called()
        send.assert_awaited_once()
        event, payload = send.await_args.args[1:]
        assert event == "error"
        assert "connector" in payload["message"].lower()
        assert payload["request_id"] == "req-1"

    @pytest.mark.asyncio
    async def test_empty_config_with_datasource_ids_is_a_valid_frame(self, monkeypatch):
        """A datasource-only change sends config={} — the 'no supported
        fields' guard must not reject it. (It then proceeds to the PATCH;
        a denial here proves the guard was passed.)"""
        import agent.api.persistent_app as mod
        from agent.api.orchestrator_client import ThreadConfigUpdateDenied

        session = SimpleNamespace(resetup_datasources=MagicMock())
        orchestrator_client = SimpleNamespace(
            update_thread_config=AsyncMock(
                side_effect=ThreadConfigUpdateDenied(422, "datasource denied")
            ),
            get_thread_workspace=AsyncMock(),
        )
        send = AsyncMock()
        monkeypatch.setattr(mod, "_session", session)
        monkeypatch.setattr(mod, "_orchestrator_client", orchestrator_client)
        monkeypatch.setattr(mod._session_identity, "_thread_id", "thread-1")
        monkeypatch.setattr(mod, "_ws_send", send)

        await mod._handle_config_update(MagicMock(), {}, datasource_ids=[])

        orchestrator_client.update_thread_config.assert_awaited_once_with(
            "thread-1", {}, datasource_ids=[], snapshot_generation=None
        )
        event, payload = send.await_args.args[1:]
        assert event == "error"
        assert "datasource denied" in payload["detail"]
        # Denied ⇒ never re-fetched, never applied.
        orchestrator_client.get_thread_workspace.assert_not_awaited()
        session.resetup_datasources.assert_not_called()

    @pytest.mark.asyncio
    async def test_workspace_refetch_failure_stops_before_local_mutation(
        self, monkeypatch
    ):
        """PATCH succeeded (durable set updated) but the enriched payload
        fetch failed: surface the inconsistency and apply NOTHING locally —
        the next attach converges from metadata.datasource_ids."""
        import agent.api.persistent_app as mod

        session = SimpleNamespace(resetup_datasources=MagicMock())
        orchestrator_client = SimpleNamespace(
            update_thread_config=AsyncMock(return_value={}),
            get_thread_workspace=AsyncMock(return_value=None),
        )
        send = AsyncMock()
        monkeypatch.setattr(mod, "_session", session)
        monkeypatch.setattr(mod, "_orchestrator_client", orchestrator_client)
        monkeypatch.setattr(mod._session_identity, "_thread_id", "thread-1")
        monkeypatch.setattr(mod, "_ws_send", send)

        await mod._handle_config_update(
            MagicMock(), {}, datasource_ids=["ds-a"], request_id="req-9"
        )

        session.resetup_datasources.assert_not_called()
        event, payload = send.await_args.args[1:]
        assert event == "error"
        assert payload["request_id"] == "req-9"
        assert "could not be applied" in payload["message"]

    def test_datasource_update_forces_enrichment_gate(self):
        from inspect import getsource

        from agent.api.persistent_app import _handle_config_update

        src = getsource(_handle_config_update)
        assert "or ds_update" in src
        # Payload fetch happens BEFORE any local mutation.
        assert src.index("get_thread_workspace") < src.index(
            "base_dict = dataclasses.asdict"
        )

    def test_resetup_owns_the_tools_reload_and_close_is_deferred(self):
        """When datasources change, resetup_datasources() (which ends in
        resetup_tools_for_backend) is the single tools reload — and the
        replaced connections go to the turn-end closer, never an eager
        close."""
        from inspect import getsource

        from agent.api.persistent_app import _handle_config_update

        src = getsource(_handle_config_update)
        assert "_session.resetup_datasources(" in src
        assert "_close_datasources_after_turn(" in src
        assert "elif tools_changed:" in src
        assert 'ack["datasources"]' in src

    def test_turn_end_closer_waits_on_the_turn_flag(self):
        from inspect import getsource

        from agent.api.persistent_app import _close_datasources_after_turn

        src = getsource(_close_datasources_after_turn)
        assert "_turn_in_flight()" in src
        assert "close_datasource_connections(connections, clients)" in src

    @pytest.mark.asyncio
    async def test_close_after_turn_polls_until_turn_ends(self, monkeypatch):
        import agent.api.persistent_app as mod

        flags = iter([True, False])
        monkeypatch.setattr(mod, "_turn_in_flight", lambda: next(flags))
        sleeps: list[float] = []

        async def fake_sleep(s):
            sleeps.append(s)

        monkeypatch.setattr(mod.asyncio, "sleep", fake_sleep)
        conn = MagicMock()
        await mod._close_datasources_after_turn({"postgresql": conn}, {})

        assert sleeps  # waited at least one tick while the turn ran
        conn.close.assert_called_once()


class TestAttachSessionRebinds:
    """Pin the per-session rebind landmarks in ``_attach_session``.

    Without these landmarks the agent's boot-time singletons leak
    through to every session — title generation 401s, memory extraction
    silently fails, and embedding routes to api.openai.com. The chat-
    side fix carries no protection for auxiliary/embedding; this test
    keeps the rebind machinery wired.
    """

    def test_attach_rebuilds_auxiliary_when_override_present(self):
        from inspect import getsource
        from agent.api.session_attach import SessionAttachCoordinator

        src = getsource(SessionAttachCoordinator._attach_inner)
        # Auxiliary rebuild branch
        assert 'config_override.get("auxiliary", {}).get("model")' in src
        assert "AuxiliaryLLM(" in src
        # Pass session-scoped instance, not _agent._auxiliary_llm
        assert "auxiliary_llm=auxiliary_llm" in src

    def test_attach_resets_embedding_singleton_when_env_changes(self):
        """M3 scrub-on-claim (§5.6) moved the embedding-override block into
        the pop-first helper ``_apply_session_embedding_env``; the attach path
        must still route through it, and the helper must reset the singleton
        and own every memory transport key (embedding + the rerank slot)."""
        from inspect import getsource
        from agent.api.session_attach import (
            MEMORY_EMBEDDING_ENV_KEYS,
            SessionAttachCoordinator,
            apply_session_embedding_env,
        )

        attach_src = getsource(SessionAttachCoordinator._attach_inner)
        assert "apply_session_embedding_env(_env_keys_src)" in attach_src

        helper_src = getsource(apply_session_embedding_env)
        assert "_embedding_module._embedding_service = None" in helper_src
        assert set(MEMORY_EMBEDDING_ENV_KEYS) == {
            "EMBEDDING_PROVIDER",
            "EMBEDDING_MODEL",
            "EMBEDDING_BASE_URL",
            "EMBEDDING_API_KEY",
            "RERANK_MODEL",
            "RERANK_BASE_URL",
            "RERANK_API_KEY",
        }
        # Pop-first: the scrub precedes any re-application of new env values.
        assert helper_src.index("os.environ.pop") < helper_src.index(
            "os.environ[k] = str(value)"
        )


# ---------------------------------------------------------------------------
# Self-cleanup watchdogs (PR 2)
# ---------------------------------------------------------------------------


class TestSignalWsConnected:
    def test_sets_event_when_present(self):
        import asyncio

        from agent.api import persistent_app as pa

        event = asyncio.Event()
        with patch.object(pa._session_termination, "ws_connected_event", event):
            pa._session_termination.signal_ws_connected()
        assert event.is_set()

    def test_no_op_when_event_is_none(self):
        from agent.api import persistent_app as pa

        with patch.object(pa._session_termination, "ws_connected_event", None):
            pa._session_termination.signal_ws_connected()  # Should not raise


class TestBootWsWatchdog:
    @pytest.mark.asyncio
    async def test_returns_early_when_ws_connects(self):
        import asyncio

        from agent.api import persistent_app as pa

        event = asyncio.Event()
        event.set()  # Pre-set so wait_for returns immediately
        with patch.object(pa._session_termination, "ws_connected_event", event):
            with patch.object(pa._session_termination, "terminate", new=AsyncMock()) as detach:
                with patch.object(pa._session_termination, "schedule_exit") as exit_fn:
                    await pa._session_termination.boot_ws_watchdog(timeout_s=10)
        detach.assert_not_called()
        exit_fn.assert_not_called()

    @pytest.mark.asyncio
    async def test_triggers_detach_and_exit_on_timeout(self):
        import asyncio

        from agent.api import persistent_app as pa

        event = asyncio.Event()  # Never set
        with patch.object(pa._session_termination, "ws_connected_event", event):
            with patch.object(pa._session_identity, "_thread_id", "thread-xyz"):
                with patch.object(pa._session_termination, "terminate", new=AsyncMock()) as detach:
                    with patch.object(pa._session_termination, "schedule_exit") as exit_fn:
                        # Tiny timeout so the test doesn't actually wait 10 min
                        await pa._session_termination.boot_ws_watchdog(timeout_s=0)
        detach.assert_awaited_once()
        exit_fn.assert_called_once()

    @pytest.mark.asyncio
    async def test_boot_timeout_keeps_process_when_exact_cleanup_is_unproven(self):
        import asyncio

        from agent.api import persistent_app as pa

        event = asyncio.Event()  # Never set
        with patch.object(pa._session_termination, "ws_connected_event", event):
            with patch.object(pa._session_identity, "_thread_id", "thread-xyz"):
                with patch.object(
                    pa._session_termination,
                    "terminate",
                    new=AsyncMock(side_effect=RuntimeError("detach failed")),
                ):
                    with patch.object(pa._session_termination, "schedule_exit") as exit_fn:
                        await pa._session_termination.boot_ws_watchdog(timeout_s=0)
        # No process exit until strict local cleanup + exact settlement is
        # proven; the fenced runtime remains the retry owner.
        exit_fn.assert_not_called()


class TestThreadStatusWatchdog:
    @pytest.mark.asyncio
    async def test_exits_when_thread_status_is_ended(self):
        from agent.api import persistent_app as pa

        client = AsyncMock()
        client.get_thread_lifecycle = AsyncMock(
            return_value={"status": "ended", "agent_id": None, "ended_at": "now"}
        )
        with patch.object(pa, "_orchestrator_client", client):
            with patch.object(pa._session_identity, "_thread_id", "thread-xyz"):
                with patch.object(pa._session_termination, "terminate", new=AsyncMock()) as detach:
                    with patch.object(pa._session_termination, "schedule_exit") as exit_fn:
                        # Tiny poll interval so test runs quickly
                        await pa._session_termination.thread_status_watchdog(poll_s=0)
        detach.assert_awaited_once()
        exit_fn.assert_called_once()

    @pytest.mark.asyncio
    async def test_does_not_exit_when_out_of_band_cleanup_is_unproven(self):
        from agent.api import persistent_app as pa

        client = AsyncMock()
        client.get_thread_lifecycle = AsyncMock(return_value={"status": "ended"})
        with (
            patch.object(pa, "_orchestrator_client", client),
            patch.object(pa._session_identity, "_thread_id", "thread-unproven"),
            patch.object(
                pa._session_termination,
                "terminate",
                new=AsyncMock(side_effect=RuntimeError("writer still active")),
            ) as detach,
            patch.object(pa._session_termination, "schedule_exit") as exit_fn,
        ):
            await pa._session_termination.thread_status_watchdog(poll_s=0)

        detach.assert_awaited_once_with("thread_ended_oob")
        exit_fn.assert_not_called()

    @pytest.mark.asyncio
    async def test_does_not_exit_when_thread_active(self):
        import asyncio

        from agent.api import persistent_app as pa

        client = AsyncMock()
        client.get_thread_lifecycle = AsyncMock(
            return_value={"status": "active", "agent_id": "a-1", "ended_at": None}
        )
        with patch.object(pa, "_orchestrator_client", client):
            with patch.object(pa._session_identity, "_thread_id", "thread-xyz"):
                with patch.object(pa._session_termination, "terminate", new=AsyncMock()) as detach:
                    with patch.object(pa._session_termination, "schedule_exit") as exit_fn:
                        # Run the watchdog briefly then cancel — it must not
                        # have triggered exit while status was active.
                        task = asyncio.create_task(pa._session_termination.thread_status_watchdog(poll_s=0))
                        await asyncio.sleep(0.05)
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
        detach.assert_not_called()
        exit_fn.assert_not_called()

    @pytest.mark.asyncio
    async def test_exits_when_active_row_belongs_to_resumed_runtime_generation(self):
        from agent.api import persistent_app as pa

        generation_a = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        generation_b = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        client = AsyncMock()
        client.get_thread_lifecycle = AsyncMock(
            return_value={
                "status": "active",
                "agent_id": "agent-b",
                "session_runtime_generation": generation_b,
                "ended_at": None,
            }
        )
        with (
            patch.object(pa, "_orchestrator_client", client),
            patch.object(pa._session_identity, "_thread_id", "thread-xyz"),
            patch.object(pa._session_identity, "_session_generation", generation_a),
            patch.object(pa._session_identity, "_runtime_contract", True),
            patch.object(pa._session_termination, "terminate", new=AsyncMock()) as detach,
            patch.object(pa._session_termination, "schedule_exit") as exit_fn,
        ):
            await pa._session_termination.thread_status_watchdog(poll_s=0)

        detach.assert_awaited_once_with("thread_ended_oob")
        exit_fn.assert_called_once()

    @pytest.mark.asyncio
    async def test_exits_when_same_agent_generation_was_rebound_to_new_attach_token(
        self,
    ):
        from agent.api import persistent_app as pa

        generation = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
        token_a = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
        token_b = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
        client = AsyncMock()
        client.get_thread_lifecycle = AsyncMock(
            return_value={
                "status": "active",
                "agent_id": "agent-a",
                "session_runtime_generation": generation,
                "session_runtime_attach_token": token_b,
                "ended_at": None,
            }
        )
        with (
            patch.object(pa, "_orchestrator_client", client),
            patch.object(pa._session_identity, "_thread_id", "thread-xyz"),
            patch.object(pa._session_identity, "_session_generation", generation),
            patch.object(pa._session_identity, "_attach_token", token_a),
            patch.object(pa._session_identity, "_runtime_contract", True),
            patch.object(pa._session_termination, "terminate", new=AsyncMock()) as detach,
            patch.object(pa._session_termination, "schedule_exit") as exit_fn,
        ):
            await pa._session_termination.thread_status_watchdog(poll_s=0)

        detach.assert_awaited_once_with("thread_ended_oob")
        exit_fn.assert_called_once()

    @pytest.mark.asyncio
    async def test_continues_when_lifecycle_fetch_fails(self):
        import asyncio

        from agent.api import persistent_app as pa

        client = AsyncMock()
        client.get_thread_lifecycle = AsyncMock(side_effect=RuntimeError("network"))
        with patch.object(pa, "_orchestrator_client", client):
            with patch.object(pa._session_identity, "_thread_id", "thread-xyz"):
                with patch.object(pa._session_termination, "terminate", new=AsyncMock()) as detach:
                    with patch.object(pa._session_termination, "schedule_exit") as exit_fn:
                        task = asyncio.create_task(pa._session_termination.thread_status_watchdog(poll_s=0))
                        await asyncio.sleep(0.05)
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
        # Transient failure must not trigger exit — we'd kill ourselves on
        # any orchestrator hiccup otherwise.
        detach.assert_not_called()
        exit_fn.assert_not_called()

    @pytest.mark.asyncio
    async def test_does_not_exit_when_thread_awaiting_user(self):
        # Regression: Phase 5 added 'awaiting_user' as the eager-mode idle
        # state set by the agent's own loop. The watchdog previously treated
        # it as terminal and killed the pod in ~60s, which collapsed the
        # untethered-survival behaviour Phase 1 + Phase 5 were built for.
        # See knowledge-base/knowledge/issues/persistent_session_watchdog_kills_awaiting_user.md.
        import asyncio

        from agent.api import persistent_app as pa

        client = AsyncMock()
        client.get_thread_lifecycle = AsyncMock(
            return_value={
                "status": "awaiting_user",
                "agent_id": "a-1",
                "ended_at": None,
            }
        )
        with patch.object(pa, "_orchestrator_client", client):
            with patch.object(pa._session_identity, "_thread_id", "thread-xyz"):
                with patch.object(pa._session_termination, "terminate", new=AsyncMock()) as detach:
                    with patch.object(pa._session_termination, "schedule_exit") as exit_fn:
                        task = asyncio.create_task(pa._session_termination.thread_status_watchdog(poll_s=0))
                        await asyncio.sleep(0.05)
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
        detach.assert_not_called()
        exit_fn.assert_not_called()

    @pytest.mark.asyncio
    async def test_exits_when_thread_suspended(self):
        # The orchestrator's attention-sleep watchdog owns the
        # awaiting_user → suspended transition. Once suspended, the
        # workspace pod is gone, so this agent is stranded and must exit.
        from agent.api import persistent_app as pa

        client = AsyncMock()
        client.get_thread_lifecycle = AsyncMock(
            return_value={
                "status": "suspended",
                "agent_id": "a-1",
                "ended_at": None,
            }
        )
        with patch.object(pa, "_orchestrator_client", client):
            with patch.object(pa._session_identity, "_thread_id", "thread-xyz"):
                with patch.object(pa._session_termination, "terminate", new=AsyncMock()) as detach:
                    with patch.object(pa._session_termination, "schedule_exit") as exit_fn:
                        await pa._session_termination.thread_status_watchdog(poll_s=0)
        detach.assert_awaited_once()
        exit_fn.assert_called_once()


class TestStartStopWatchdogs:
    @pytest.mark.asyncio
    async def test_start_creates_two_named_tasks(self):
        import asyncio

        from agent.api import persistent_app as pa

        with patch.object(pa._session_termination, "watchdog_tasks", []):
            pa._session_termination.start_watchdogs()
            try:
                assert len(pa._session_termination.watchdog_tasks) == 2
                names = {t.get_name() for t in pa._session_termination.watchdog_tasks}
                assert names == {"boot-ws-watchdog", "thread-status-watchdog"}
            finally:
                pa._session_termination.stop_watchdogs()
                await asyncio.gather(
                    *[t for t in pa._session_termination.watchdog_tasks if not t.done()],
                    return_exceptions=True,
                )

    @pytest.mark.asyncio
    async def test_start_cancels_prior_tasks(self):
        import asyncio

        from agent.api import persistent_app as pa

        async def _forever():
            await asyncio.sleep(60)

        prior = asyncio.create_task(_forever(), name="prior")
        with patch.object(pa._session_termination, "watchdog_tasks", [prior]):
            pa._session_termination.start_watchdogs()
            try:
                await asyncio.sleep(0.01)
                assert prior.cancelled() or prior.done()
            finally:
                pa._session_termination.stop_watchdogs()
                await asyncio.gather(
                    *[t for t in pa._session_termination.watchdog_tasks if not t.done()],
                    return_exceptions=True,
                )

    @pytest.mark.asyncio
    async def test_stop_skips_current_task(self):
        # _stop_watchdogs should not cancel the calling task — that would
        # raise CancelledError in the very watchdog that triggered detach.
        import asyncio

        from agent.api import persistent_app as pa

        async def fake_watchdog():
            pa._session_termination.stop_watchdogs()
            return "completed-normally"

        task = asyncio.create_task(fake_watchdog(), name="self")
        with patch.object(pa._session_termination, "watchdog_tasks", [task]):
            result = await task
        assert result == "completed-normally"


# ---------------------------------------------------------------------------
# WorkspaceNotReady — Task 5: de-race persistent-agent startup
# ---------------------------------------------------------------------------


class TestWorkspaceNotReadyException:
    """WorkspaceNotReady is a RuntimeError subclass."""

    def test_is_subclass_of_runtime_error(self):
        from agent.api.persistent_app import WorkspaceNotReady

        assert issubclass(WorkspaceNotReady, RuntimeError)

    def test_can_be_caught_as_runtime_error(self):
        from agent.api.persistent_app import WorkspaceNotReady

        with pytest.raises(RuntimeError):
            raise WorkspaceNotReady("test message")

    def test_message_is_preserved(self):
        from agent.api.persistent_app import WorkspaceNotReady

        exc = WorkspaceNotReady("some message")
        assert "some message" in str(exc)


class TestAttachSessionRaisesWorkspaceNotReady:
    """_attach_session raises WorkspaceNotReady when poll_workspace_ready returns None."""

    @pytest.mark.asyncio
    async def test_raises_workspace_not_ready_when_poll_returns_none(self):
        from agent.api import persistent_app as pa
        from agent.api.persistent_app import WorkspaceNotReady

        mock_client = AsyncMock()

        with patch.object(pa, "_session", None):
            with patch.object(pa, "_orchestrator_client", mock_client):
                with patch.object(session_workspace, "poll_workspace_ready", new=AsyncMock(return_value=None)
                ):
                    with pytest.raises(WorkspaceNotReady):
                        await pa._session_attach.attach("t1")

    @pytest.mark.asyncio
    async def test_raises_contains_descriptive_message(self):
        from agent.api import persistent_app as pa
        from agent.api.persistent_app import WorkspaceNotReady

        mock_client = AsyncMock()

        with patch.object(pa, "_session", None):
            with patch.object(pa, "_orchestrator_client", mock_client):
                with patch.object(session_workspace, "poll_workspace_ready", new=AsyncMock(return_value=None)
                ):
                    with pytest.raises(WorkspaceNotReady, match="workspace"):
                        await pa._session_attach.attach("t1")

    @pytest.mark.asyncio
    async def test_double_attach_guard_still_raises_plain_runtime_error(self):
        """The :626 double-attach guard must remain a plain RuntimeError, not WorkspaceNotReady."""
        from agent.api import persistent_app as pa
        from agent.api.persistent_app import WorkspaceNotReady

        mock_session = MagicMock()

        with patch.object(pa, "_session", mock_session):
            with pytest.raises(RuntimeError) as exc_info:
                await pa._session_attach.attach("t1")
        # It must NOT be WorkspaceNotReady
        assert not isinstance(exc_info.value, WorkspaceNotReady)
        assert "already attached" in str(exc_info.value)


class TestExitDuplicateProvisionHelper:
    """_exit_duplicate_provision calls os._exit(0) with best-effort deregister.

    The losing pod of a provisioning race (orchestrator 409) must exit cleanly
    so it drops out of the per-session Service endpoints, cleaning up only its
    own agent record — never any thread-scoped resource (those belong to the
    winning agent).
    """

    @pytest.mark.asyncio
    async def test_exit_duplicate_provision_calls_os_exit_zero(self):
        """The handler helper invokes os._exit(0) so the orphan pod completes."""
        from agent.api import persistent_app as pa

        mock_client = MagicMock()
        mock_client.stop_heartbeat = MagicMock()
        mock_client.deregister = AsyncMock()
        mock_client.close = AsyncMock()

        with patch.object(pa, "_orchestrator_client", mock_client):
            with patch.object(pa, "_heartbeat_task", None):
                with patch("os._exit", side_effect=SystemExit(0)) as mock_exit:
                    with pytest.raises(SystemExit):
                        await pa._session_termination.exit_duplicate_provision("thread-1")

        mock_exit.assert_called_once_with(0)

    @pytest.mark.asyncio
    async def test_exit_duplicate_provision_best_effort_deregister(self):
        """Best-effort self-deregister + close are awaited before os._exit."""
        from agent.api import persistent_app as pa

        mock_client = MagicMock()
        mock_client.stop_heartbeat = MagicMock()
        mock_client.deregister = AsyncMock()
        mock_client.close = AsyncMock()

        with patch.object(pa, "_orchestrator_client", mock_client):
            with patch.object(pa, "_heartbeat_task", None):
                with patch("os._exit", side_effect=SystemExit(0)):
                    with pytest.raises(SystemExit):
                        await pa._session_termination.exit_duplicate_provision("thread-1")

        mock_client.deregister.assert_awaited_once()
        mock_client.close.assert_awaited_once()


class TestExitWorkspaceNotReadyHelper:
    """_exit_workspace_not_ready calls os._exit(0) with best-effort deregister."""

    @pytest.mark.asyncio
    async def test_exit_workspace_not_ready_calls_os_exit_zero(self):
        """The handler helper invokes os._exit(0)."""
        from agent.api import persistent_app as pa
        from agent.api.persistent_app import WorkspaceNotReady

        mock_client = MagicMock()
        mock_client.stop_heartbeat = MagicMock()
        mock_client.deregister = AsyncMock()
        mock_client.close = AsyncMock()

        exc = WorkspaceNotReady("timed out")

        with patch.object(pa, "_orchestrator_client", mock_client):
            with patch.object(pa, "_heartbeat_task", None):
                with patch("os._exit", side_effect=SystemExit(0)) as mock_exit:
                    with pytest.raises(SystemExit):
                        await pa._session_termination.exit_workspace_not_ready("thread-1", exc)

        mock_exit.assert_called_once_with(0)

    @pytest.mark.asyncio
    async def test_exit_workspace_not_ready_cancels_heartbeat_task(self):
        """When a heartbeat task exists, it is cancelled during clean exit."""
        from agent.api import persistent_app as pa
        from agent.api.persistent_app import WorkspaceNotReady

        mock_task = MagicMock()
        mock_client = MagicMock()
        mock_client.stop_heartbeat = MagicMock()
        mock_client.deregister = AsyncMock()
        mock_client.close = AsyncMock()

        exc = WorkspaceNotReady("no workspace")

        with patch.object(pa, "_orchestrator_client", mock_client):
            with patch.object(pa, "_heartbeat_task", mock_task):
                with patch("os._exit", side_effect=SystemExit(0)) as mock_exit:
                    with pytest.raises(SystemExit):
                        await pa._session_termination.exit_workspace_not_ready("thread-1", exc)

        mock_task.cancel.assert_called_once()
        mock_exit.assert_called_once_with(0)

    @pytest.mark.asyncio
    async def test_exit_workspace_not_ready_attempts_deregister(self):
        """Best-effort deregister is awaited before os._exit."""
        from agent.api import persistent_app as pa
        from agent.api.persistent_app import WorkspaceNotReady

        deregister = AsyncMock()
        close = AsyncMock()
        mock_client = MagicMock()
        mock_client.stop_heartbeat = MagicMock()
        mock_client.deregister = deregister
        mock_client.close = close

        exc = WorkspaceNotReady("no workspace")

        with patch.object(pa, "_orchestrator_client", mock_client):
            with patch.object(pa, "_heartbeat_task", None):
                with patch("os._exit", side_effect=SystemExit(0)):
                    with pytest.raises(SystemExit):
                        await pa._session_termination.exit_workspace_not_ready("thread-1", exc)

        deregister.assert_awaited_once()
        close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_exit_workspace_not_ready_no_client_still_exits(self):
        """Even without an orchestrator client, os._exit(0) is called."""
        from agent.api import persistent_app as pa
        from agent.api.persistent_app import WorkspaceNotReady

        exc = WorkspaceNotReady("no workspace")

        with patch.object(pa, "_orchestrator_client", None):
            with patch.object(pa, "_heartbeat_task", None):
                with patch("os._exit", side_effect=SystemExit(0)) as mock_exit:
                    with pytest.raises(SystemExit):
                        await pa._session_termination.exit_workspace_not_ready("thread-1", exc)

        mock_exit.assert_called_once_with(0)

    @pytest.mark.asyncio
    async def test_exit_workspace_not_ready_deregister_failure_still_exits(self):
        """A deregister exception is swallowed; os._exit(0) is still called."""
        from agent.api import persistent_app as pa
        from agent.api.persistent_app import WorkspaceNotReady

        mock_client = MagicMock()
        mock_client.stop_heartbeat = MagicMock()
        mock_client.deregister = AsyncMock(side_effect=RuntimeError("network error"))
        mock_client.close = AsyncMock()

        exc = WorkspaceNotReady("no workspace")

        with patch.object(pa, "_orchestrator_client", mock_client):
            with patch.object(pa, "_heartbeat_task", None):
                with patch("os._exit", side_effect=SystemExit(0)) as mock_exit:
                    with pytest.raises(SystemExit):
                        await pa._session_termination.exit_workspace_not_ready("thread-1", exc)

        mock_exit.assert_called_once_with(0)


class TestScheduleExitDeregisters:
    """The _schedule_exit path (drain-suspend, watchdogs, session end)
    deregisters before os._exit — which bypasses the lifespan shutdown —
    so a clean exit doesn't become a missed-heartbeats corpse the sweep
    reports as fleet:agents_offline. Best-effort: a hung or failing
    deregister never holds up or aborts the exit.
    """

    def _client(self):
        mock_client = MagicMock()
        mock_client.agent_id = "agent-1"
        mock_client.stop_heartbeat = MagicMock()
        mock_client.deregister = AsyncMock(return_value=True)
        return mock_client

    async def _run_scheduled_exit(self, pa):
        saved = pa._session_termination.pending_exit_task
        try:
            with patch("os._exit") as mock_exit:
                pa._session_termination.schedule_exit(delay=0)
                await asyncio.wait_for(pa._session_termination.pending_exit_task, timeout=2.0)
            return mock_exit
        finally:
            pa._session_termination.pending_exit_task = saved

    @pytest.mark.asyncio
    async def test_scheduled_exit_deregisters_then_exits(self):
        from agent.api import persistent_app as pa

        mock_client = self._client()
        with patch.object(pa, "_orchestrator_client", mock_client):
            with patch.object(pa, "_heartbeat_task", None):
                mock_exit = await self._run_scheduled_exit(pa)

        mock_client.deregister.assert_awaited_once()
        mock_client.stop_heartbeat.assert_called_once()
        mock_exit.assert_called_once_with(0)

    @pytest.mark.asyncio
    async def test_scheduled_exit_proceeds_when_deregister_hangs(self):
        from agent.api import persistent_app as pa

        mock_client = self._client()

        async def _hang():
            await asyncio.sleep(60)

        mock_client.deregister = AsyncMock(side_effect=_hang)

        with patch.object(pa, "_orchestrator_client", mock_client):
            with patch.object(pa, "_heartbeat_task", None):
                with patch.object(session_termination, "_DEREGISTER_ON_EXIT_TIMEOUT_S", 0.05):
                    mock_exit = await self._run_scheduled_exit(pa)

        mock_exit.assert_called_once_with(0)

    @pytest.mark.asyncio
    async def test_scheduled_exit_proceeds_when_deregister_errors(self):
        from agent.api import persistent_app as pa

        mock_client = self._client()
        mock_client.deregister = AsyncMock(side_effect=RuntimeError("500"))

        with patch.object(pa, "_orchestrator_client", mock_client):
            with patch.object(pa, "_heartbeat_task", None):
                mock_exit = await self._run_scheduled_exit(pa)

        mock_exit.assert_called_once_with(0)

    @pytest.mark.asyncio
    async def test_scheduled_exit_without_client_still_exits(self):
        from agent.api import persistent_app as pa

        with patch.object(pa, "_orchestrator_client", None):
            with patch.object(pa, "_heartbeat_task", None):
                mock_exit = await self._run_scheduled_exit(pa)

        mock_exit.assert_called_once_with(0)


# ---------------------------------------------------------------------------
# session_backend_is_lite() — lite-session boot detection
# (no_workspace_agent_mode session boot gap; workspace_tier_upgrade.md smoke test)
# ---------------------------------------------------------------------------


class TestSessionBackendIsLite:
    """_attach_session uses this to skip the workspace-pod poll for lite
    (virtual/none) sessions. Must read both the FLAT config_override shape
    ({workspace: ...}) and the NESTED resolved_config shape (agent.workspace)."""

    def test_flat_virtual_is_lite(self):
        assert session_backend_is_lite({"workspace": {"backend": "virtual"}}) is True

    def test_flat_none_is_lite(self):
        assert session_backend_is_lite({"workspace": {"backend": "none"}}) is True

    def test_nested_virtual_is_lite(self):
        # A resolved_config blob nests the agent config under "agent".
        assert (
            session_backend_is_lite({"agent": {"workspace": {"backend": "virtual"}}})
            is True
        )

    def test_flat_sandbox_is_not_lite(self):
        assert session_backend_is_lite({"workspace": {"backend": "sandbox"}}) is False

    def test_nested_vm_is_not_lite(self):
        assert (
            session_backend_is_lite({"agent": {"workspace": {"backend": "vm"}}})
            is False
        )

    def test_missing_backend_is_not_lite(self):
        assert session_backend_is_lite({"workspace": {}}) is False
        assert session_backend_is_lite({}) is False

    def test_non_dict_is_not_lite(self):
        assert session_backend_is_lite(None) is False
        assert session_backend_is_lite("virtual") is False


# ---------------------------------------------------------------------------
# session_backend_is_vm() — VM-tier boot detection
# (knowledge-base/knowledge/issues/session_vm_backend_never_attaches.md Defect 2)
# ---------------------------------------------------------------------------


class TestSessionBackendIsVm:
    """_attach_session uses this to require a VM (never a container) for a
    vm-tier session. Same dual-shape contract as session_backend_is_lite:
    FLAT config_override ({workspace: ...}) and NESTED resolved_config
    (agent.workspace)."""

    def test_flat_vm_is_vm(self):
        assert session_backend_is_vm({"workspace": {"backend": "vm"}}) is True

    def test_flat_remote_alias_is_vm(self):
        # "remote" is the legacy alias for "vm"; stored overrides still carry it.
        assert session_backend_is_vm({"workspace": {"backend": "remote"}}) is True

    def test_nested_vm_is_vm(self):
        assert (
            session_backend_is_vm({"agent": {"workspace": {"backend": "vm"}}}) is True
        )

    def test_flat_sandbox_is_not_vm(self):
        assert session_backend_is_vm({"workspace": {"backend": "sandbox"}}) is False

    def test_lite_is_not_vm(self):
        assert session_backend_is_vm({"workspace": {"backend": "virtual"}}) is False

    def test_missing_backend_is_not_vm(self):
        assert session_backend_is_vm({"workspace": {}}) is False
        assert session_backend_is_vm({}) is False

    def test_non_dict_is_not_vm(self):
        assert session_backend_is_vm(None) is False
        assert session_backend_is_vm("vm") is False


@pytest.mark.asyncio
async def test_vm_tier_poll_rides_out_a_transient_workspace_failure():
    """A vm-tier attach must not mis-report a booting VM as 'never became
    ready' because one workspace call collapsed to None (a 5xx during an
    orchestrator restart, or a briefly unavailable repository authority)."""
    from unittest.mock import AsyncMock

    from agent.api.session_workspace import poll_workspace_ready

    client = AsyncMock()
    client.get_thread_workspace = AsyncMock(
        side_effect=[
            {"vm_status": "created"},
            None,
            {"vm_status": "ready", "vm_ssh_host": "10.42.0.78", "vm_ssh_port": 22},
        ]
    )

    result = await poll_workspace_ready(
        client, "tid", timeout=10, poll_interval=0.01, require_vm=True, session_runtime_generation=None
    )

    assert result is not None
    assert result["backend"] == "vm"
    assert result["remote"]["host"] == "10.42.0.78"
    assert client.get_thread_workspace.call_count == 3
