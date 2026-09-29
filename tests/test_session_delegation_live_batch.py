"""The live session delegation batch (parallel_subagents §6.3, §6.4, §8; WP3b).

Every ``delegate_agent`` call of a batch ends with exactly one truthful result
in provider order, except when the turn itself is cancelled (shutdown, drain):
then nothing is written and the cancellation propagates, so the successor's
settle owns the batch. What the batch writes before a child starts, what a
failed save does, how a failing call and Stop are reported, and the
recovery-turn ban (D3) are pinned here. The fan-out decision itself is the
orchestrator's; tests pin it with ``tests._fanout_gate``.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

import agent.persistent_graph as persistent_graph
from agent.persistent_graph import (
    DELEGATION_CANCELLED_HEADER,
    DelegationResultNotDurable,
    PermissionOutcome,
    TurnResult,
    _execute_turn,
    run_persistent_loop,
)
from agent.subagents import RecordingLedger
from agent.subagents.runtime import (
    NOT_STARTED_HEADER,
    STOPPED_HEADER,
    SubagentCall,
    stopped_not_started_text,
)
from agent.tools.delegation.delegate_agent import NOT_STARTED_MARKER
from shared.runtime.core.workspace_backend import WorkspaceUnavailableError
from shared.session_subagent_batch import not_started_result_text
from tests._fake_chat_model import FakeChatModel, text_turn, tool_turn
from tests._fanout_gate import set_session_fanout
from tests.test_delegate_agent_tool import install, make_parent, the_tool
from tests.test_persistent_delegation_batch import (
    _callbacks,
    _config,
    _context_manager,
    _tool,
    _tool_call,
)

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _Parent:
    """The parent model: one scripted response per provider call."""

    reasoning = None

    def __init__(self, responses: List[AIMessage]):
        self._responses = list(responses)
        self.calls = 0

    async def astream(self, _messages, **_kwargs):
        self.calls += 1
        yield self._responses.pop(0)


def _batch(*call_ids: str) -> AIMessage:
    return AIMessage(
        content="",
        tool_calls=[
            _tool_call("delegate_agent", call_id, prompt=f"brief {call_id}")
            for call_id in call_ids
        ],
    )


def _real_batch(*call_ids: str) -> AIMessage:
    """A batch the real ``delegate_agent`` schema accepts."""
    return AIMessage(
        content="",
        tool_calls=[
            {
                "name": "delegate_agent",
                "id": call_id,
                "args": {
                    "description": f"inspect {call_id}",
                    "prompt": f"Report evidence for {call_id}.",
                    "subagent_type": "explorer",
                },
            }
            for call_id in call_ids
        ],
    )


def _session_context(runtime: Any = None, **delegation: Any) -> SimpleNamespace:
    return SimpleNamespace(
        knowledge_bindings=[],
        citation_engine=None,
        subagent_runtime=runtime if runtime is not None else MagicMock(),
        _fork_source=None,
        _subagent_parent_kind="session",
        config={"delegation": dict(delegation)},
    )


def _results(messages: List[Any]) -> Dict[str, str]:
    return {m.tool_call_id: m.content for m in messages if isinstance(m, ToolMessage)}


def _result_order(messages: List[Any]) -> List[str]:
    return [m.tool_call_id for m in messages if isinstance(m, ToolMessage)]


async def _turn(
    llm: Any,
    tools: Dict[str, Any],
    callbacks: Any,
    tool_context: Any,
    messages: Optional[List[Any]] = None,
) -> TurnResult:
    if messages is None:
        messages = []
    messages[:0] = [SystemMessage(content="sys"), HumanMessage(content="go")]
    return await _execute_turn(
        llm_with_tools=llm,
        tool_map=tools,
        context_manager=_context_manager(),
        messages=messages,
        callbacks=callbacks,
        llm_timeout=600,
        auxiliary_llm=None,
        config=_config(),
        tool_context=tool_context,
        turn_id=1,
    )


class _Recorder:
    """``persist_message`` that acknowledges durably and records the order."""

    def __init__(self, refuse: Optional[set] = None):
        self.saved: List[Any] = []
        self.attempts: Dict[str, List[str]] = {}
        self.refuse = refuse or set()

    async def __call__(self, message: Any) -> bool:
        if isinstance(message, ToolMessage):
            self.attempts.setdefault(message.tool_call_id, []).append(message.id)
            if message.tool_call_id in self.refuse:
                return False
        self.saved.append(message)
        return True

    @property
    def results(self) -> List[str]:
        return [m.tool_call_id for m in self.saved if isinstance(m, ToolMessage)]


class _OneStop:
    """``check_interrupt``: one graceful Stop once ``when`` is set."""

    def __init__(self, when: asyncio.Event):
        self.when = when
        self.fired = 0

    def __call__(self) -> Optional[str]:
        if not self.fired and self.when.is_set():
            self.fired += 1
            return "graceful"
        return None


# ---------------------------------------------------------------------------
# The per-turn maximum (§6.4)
# ---------------------------------------------------------------------------


async def test_calls_above_the_per_turn_maximum_are_refused_across_batches(
    monkeypatch,
):
    set_session_fanout(monkeypatch)
    ran: List[str] = []

    async def _delegate(call: dict):
        ran.append(call["id"])
        return f"report {call['id']}"

    delegate = _tool("delegate_agent", _delegate)
    llm = _Parent([_batch("a1", "a2"), _batch("b1", "b2"), AIMessage(content="done")])
    callbacks = _callbacks()
    messages: List[Any] = []

    result = await _turn(
        llm,
        {"delegate_agent": delegate},
        callbacks,
        _session_context(session_max_calls_per_turn=3),
        messages,
    )

    assert ran == ["a1", "a2", "b1"]
    results = _results(messages)
    assert results["b2"] == (
        "Error: delegate_agent call refused: this turn has reached its maximum "
        "of 3 delegate_agent calls. This subagent did not run. Do not retry it "
        "in this turn: work with the reports you have, or do the rest yourself."
    )
    assert _result_order(messages) == ["a1", "a2", "b1", "b2"]
    # Never announced or gated: nobody is asked to approve a call that cannot run.
    assert [c.args[2] for c in callbacks.permission_check.await_args_list] == [
        "a1",
        "a2",
        "b1",
    ]
    announced = [
        [call["id"] for call in awaited.args[0]]
        for awaited in callbacks.announce_permission_batch.await_args_list
    ]
    assert announced == [["a1", "a2"], ["b1"]]
    # Its card still opens and closes, as an error.
    callbacks.on_tool_result.assert_any_await(
        "delegate_agent", results["b2"], "b2", is_error=True
    )
    assert result.tool_calls_made == 3
    assert llm.calls == 3  # the model then answered


async def test_a_declined_call_counts_toward_the_per_turn_maximum(monkeypatch):
    """Every call within the maximum counts, the declined ones too: the
    maximum bounds the calls of a turn, not only the children that ran."""
    set_session_fanout(monkeypatch)
    ran: List[str] = []

    async def _delegate(call: dict):
        ran.append(call["id"])
        return f"report {call['id']}"

    async def _permission(_name, _args, call_id):
        return call_id != "a2"

    callbacks = _callbacks(permission_check=AsyncMock(side_effect=_permission))
    messages: List[Any] = []
    await _turn(
        _Parent([_batch("a1", "a2"), _batch("b1", "b2"), AIMessage(content="done")]),
        {"delegate_agent": _tool("delegate_agent", _delegate)},
        callbacks,
        _session_context(session_max_calls_per_turn=3),
        messages,
    )

    assert ran == ["a1", "b1"]
    results = _results(messages)
    assert results["a2"] == "User declined this tool call."
    assert results["b2"].startswith(
        "Error: delegate_agent call refused: this turn has reached its maximum "
        "of 3 delegate_agent calls."
    )
    assert [c.args[2] for c in callbacks.permission_check.await_args_list] == [
        "a1",
        "a2",
        "b1",
    ]


async def test_without_fan_out_a_turn_has_no_per_turn_maximum(monkeypatch):
    set_session_fanout(monkeypatch, allowed=False)
    delegate = _tool("delegate_agent", AsyncMock(return_value="report"))
    llm = _Parent([_batch("a1", "a2"), _batch("b1", "b2"), AIMessage(content="done")])
    messages: List[Any] = []

    result = await _turn(
        llm,
        {"delegate_agent": delegate},
        _callbacks(),
        _session_context(session_max_calls_per_turn=1),
        messages,
    )

    assert delegate.ainvoke.await_count == 4
    assert set(_results(messages).values()) == {"report"}
    assert result.tool_calls_made == 4


# ---------------------------------------------------------------------------
# Results written before any child starts; strict persistence (F5)
# ---------------------------------------------------------------------------


async def test_declined_and_refused_results_are_saved_before_any_child_starts(
    monkeypatch,
):
    set_session_fanout(monkeypatch)
    recorder = _Recorder()
    saved_when_started: List[List[str]] = []

    async def _delegate(call: dict):
        saved_when_started.append(list(recorder.results))
        await asyncio.sleep(0)
        return f"report {call['id']}"

    async def _permission(_name, _args, call_id):
        if call_id == "c2":
            return PermissionOutcome.DECLINED
        return PermissionOutcome.APPROVED

    delegate = _tool("delegate_agent", _delegate)
    callbacks = _callbacks(
        persist_message=recorder,
        require_delegation_persistence=True,
        permission_check=AsyncMock(side_effect=_permission),
    )
    messages: List[Any] = []
    await _turn(
        _Parent([_batch("c1", "c2", "c3", "c4"), AIMessage(content="done")]),
        {"delegate_agent": delegate},
        callbacks,
        _session_context(session_max_calls_per_turn=3),
        messages,
    )

    # All decisions were durable before the first child started, so a crash
    # during the batch can never report a declined call as "not started".
    assert saved_when_started == [["c2", "c4"], ["c2", "c4"]]
    assert recorder.results == ["c2", "c4", "c1", "c3"]
    assert _results(messages)["c2"] == "User declined this tool call."
    # The live transcript keeps provider order.
    assert _result_order(messages) == ["c1", "c2", "c3", "c4"]


async def test_an_unsaved_result_stops_the_turn_before_another_provider_call(
    monkeypatch,
):
    monkeypatch.setattr(persistent_graph, "_DELEGATION_RESULT_PERSIST_RETRY_S", 0)
    recorder = _Recorder(refuse={"c1"})
    delegate = _tool("delegate_agent", AsyncMock(return_value="report"))
    llm = _Parent([_batch("c1", "c2"), AIMessage(content="must not be asked")])
    messages: List[Any] = []

    with pytest.raises(DelegationResultNotDurable, match="could not be saved"):
        await _turn(
            llm,
            {"delegate_agent": delegate},
            _callbacks(persist_message=recorder, require_delegation_persistence=True),
            _session_context(),
            messages,
        )

    # No final answer can make the batch look delivered: the successor's
    # settle (or the turn-end reconcile) still owns it.
    assert llm.calls == 1
    # Bounded retries of the SAME row, never a second row; once one result is
    # unsaved the rest are left to the reconcile, which retries them by id.
    assert list(recorder.attempts) == ["c1"]
    assert len(recorder.attempts["c1"]) == 3
    assert len(set(recorder.attempts["c1"])) == 1
    # Every call still has its one result in the live transcript.
    assert _result_order(messages) == ["c1", "c2"]


async def test_a_raising_save_counts_as_unsaved(monkeypatch):
    monkeypatch.setattr(persistent_graph, "_DELEGATION_RESULT_PERSIST_RETRY_S", 0)
    attempts = 0

    async def _persist(message):
        nonlocal attempts
        if isinstance(message, ToolMessage):
            attempts += 1
            raise ConnectionError("database gone")
        return True

    with pytest.raises(DelegationResultNotDurable) as raised:
        await _turn(
            _Parent([_batch("c1"), AIMessage(content="never")]),
            {"delegate_agent": _tool("delegate_agent", AsyncMock(return_value="r"))},
            _callbacks(persist_message=_persist, require_delegation_persistence=True),
            _session_context(),
        )
    assert attempts == 3
    assert isinstance(raised.value.__cause__, ConnectionError)


async def test_an_unsaved_decline_starts_no_child(monkeypatch):
    monkeypatch.setattr(persistent_graph, "_DELEGATION_RESULT_PERSIST_RETRY_S", 0)
    recorder = _Recorder(refuse={"c2"})
    delegate = _tool("delegate_agent", AsyncMock(return_value="report"))

    async def _permission(_name, _args, call_id):
        return call_id != "c2"

    messages: List[Any] = []
    with pytest.raises(DelegationResultNotDurable, match="before any subagent started"):
        await _turn(
            _Parent([_batch("c1", "c2"), AIMessage(content="never")]),
            {"delegate_agent": delegate},
            _callbacks(
                persist_message=recorder,
                require_delegation_persistence=True,
                permission_check=AsyncMock(side_effect=_permission),
            ),
            _session_context(),
            messages,
        )
    delegate.ainvoke.assert_not_awaited()
    assert _result_order(messages) == ["c2"]


# ---------------------------------------------------------------------------
# A failing call does not take its siblings down
# ---------------------------------------------------------------------------


async def test_a_child_failure_is_its_own_result_and_the_turn_goes_on():
    async def _delegate(call: dict):
        if call["id"] == "c1":
            raise RuntimeError("ledger refused the child")
        await asyncio.sleep(0.02)
        return f"report {call['id']}"

    callbacks = _callbacks()
    llm = _Parent([_batch("c1", "c2"), AIMessage(content="done")])
    messages: List[Any] = []

    result = await _turn(
        llm,
        {"delegate_agent": _tool("delegate_agent", _delegate)},
        callbacks,
        _session_context(),
        messages,
    )

    assert _results(messages) == {
        "c1": "Tool execution error: ledger refused the child",
        "c2": "report c2",
    }
    callbacks.on_tool_result.assert_any_await(
        "delegate_agent",
        "Tool execution error: ledger refused the child",
        "c1",
        is_error=True,
    )
    assert result.tool_calls_made == 2
    assert llm.calls == 2


async def test_a_call_refused_at_its_effect_boundary_leaves_siblings_running():
    """The effect boundary is a safety fence: the turn must not continue past
    it. The siblings already running finish, every call gets its result, and
    only then does the error end the turn."""
    sibling_done: List[str] = []

    async def _execution_start(_name, call_id):
        if call_id == "c2":
            await asyncio.sleep(0.01)
            raise RuntimeError("stateless tool execution lacks an exact claimant")

    async def _delegate(call: dict):
        await asyncio.sleep(0.05)
        sibling_done.append(call["id"])
        return f"report {call['id']}"

    delegate = _tool("delegate_agent", _delegate)
    recorder = _Recorder()
    llm = _Parent([_batch("c1", "c2", "c3"), AIMessage(content="never")])
    messages: List[Any] = []

    with pytest.raises(RuntimeError, match="exact claimant"):
        await _turn(
            llm,
            {"delegate_agent": delegate},
            _callbacks(
                on_tool_execution_start=AsyncMock(side_effect=_execution_start),
                persist_message=recorder,
                require_delegation_persistence=True,
            ),
            _session_context(),
            messages,
        )

    assert sorted(sibling_done) == ["c1", "c3"]
    results = _results(messages)
    assert results["c1"] == "report c1" and results["c3"] == "report c3"
    assert results["c2"] == (
        "Error: delegate_agent did not start this subagent (RuntimeError: "
        "stateless tool execution lacks an exact claimant). It did no work and "
        "changed nothing."
    )
    assert _result_order(messages) == ["c1", "c2", "c3"]
    assert recorder.results == ["c1", "c2", "c3"]
    assert llm.calls == 1


async def test_a_dead_workspace_cancels_the_siblings_and_says_so():
    sibling_started = asyncio.Event()
    sibling_cancelled = asyncio.Event()

    async def _delegate(call: dict):
        if call["id"] == "c1":
            await sibling_started.wait()
            raise WorkspaceUnavailableError("workspace disappeared")
        sibling_started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            sibling_cancelled.set()
            raise

    recorder = _Recorder()
    messages: List[Any] = []
    with pytest.raises(WorkspaceUnavailableError):
        await _turn(
            _Parent([_batch("c1", "c2"), AIMessage(content="never")]),
            {"delegate_agent": _tool("delegate_agent", _delegate)},
            _callbacks(persist_message=recorder, require_delegation_persistence=True),
            _session_context(),
            messages,
        )

    assert sibling_cancelled.is_set()
    results = _results(messages)
    assert results["c1"] == "Tool execution error: workspace disappeared"
    assert results["c2"].startswith(DELEGATION_CANCELLED_HEADER + "\n")
    assert "because the workspace became unavailable" in results["c2"]
    assert "It produced no final report" in results["c2"]
    assert recorder.results == ["c1", "c2"]


async def test_a_dead_workspace_wins_over_an_earlier_boundary_failure():
    """The turn ends with the workspace error even when another call failed
    first: its message is the one that tells the user how to recover."""
    c3_started = asyncio.Event()
    c3_cancelled = asyncio.Event()

    async def _execution_start(_name, call_id):
        if call_id == "c1":
            raise RuntimeError("stateless tool execution lacks an exact claimant")

    async def _delegate(call: dict):
        if call["id"] == "c2":
            await c3_started.wait()
            await asyncio.sleep(0.02)  # after c1's failure was observed
            raise WorkspaceUnavailableError("workspace disappeared")
        c3_started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            c3_cancelled.set()
            raise

    recorder = _Recorder()
    messages: List[Any] = []
    with pytest.raises(WorkspaceUnavailableError, match="workspace disappeared"):
        await _turn(
            _Parent([_batch("c1", "c2", "c3"), AIMessage(content="never")]),
            {"delegate_agent": _tool("delegate_agent", _delegate)},
            _callbacks(
                on_tool_execution_start=AsyncMock(side_effect=_execution_start),
                persist_message=recorder,
                require_delegation_persistence=True,
            ),
            _session_context(),
            messages,
        )

    assert c3_cancelled.is_set()
    results = _results(messages)
    assert results["c1"].startswith("Error: delegate_agent did not start")
    assert results["c2"] == "Tool execution error: workspace disappeared"
    assert results["c3"].startswith(DELEGATION_CANCELLED_HEADER)
    assert recorder.results == ["c1", "c2", "c3"]


# ---------------------------------------------------------------------------
# Stop reaches the children (D4)
# ---------------------------------------------------------------------------


async def test_stop_reaches_the_batch_and_every_call_still_gets_a_result():
    running = asyncio.Event()
    released = asyncio.Event()

    class Runtime:
        def __init__(self):
            self.stops = 0
            self.batches: List[int] = []

        def begin_batch(self, n):
            self.batches.append(n)

        async def stop_foreground_batch(self):
            self.stops += 1
            released.set()

    async def _delegate(call: dict):
        if call["id"] == "c3":  # queued behind the cap
            await released.wait()
            return stopped_not_started_text()
        running.set()
        await released.wait()
        return f"{STOPPED_HEADER}\npartial {call['id']}"

    runtime = Runtime()
    stop = _OneStop(running)
    recorder = _Recorder()
    llm = _Parent([_batch("c1", "c2", "c3"), AIMessage(content="never")])
    messages: List[Any] = []

    result = await _turn(
        llm,
        {"delegate_agent": _tool("delegate_agent", _delegate)},
        _callbacks(
            check_interrupt=stop,
            persist_message=recorder,
            require_delegation_persistence=True,
        ),
        _session_context(runtime),
        messages,
    )

    assert runtime.stops == 1 and stop.fired == 1
    assert result.interrupted is True
    assert llm.calls == 1  # the turn ends without a final answer
    assert _result_order(messages) == ["c1", "c2", "c3"]
    results = _results(messages)
    assert results["c1"].startswith(STOPPED_HEADER)
    assert results["c3"].startswith(NOT_STARTED_HEADER)
    assert recorder.results == ["c1", "c2", "c3"]
    assert result.tool_calls_made == 3
    assert isinstance(messages[-1], ToolMessage)


async def test_stop_reaches_real_children_running_and_queued(tmp_path, monkeypatch):
    """Cap 1, two calls: the running child gets ``graceful_stop`` (one
    tool-less turn for a partial answer) and returns STOPPED; the queued call
    returns NOT STARTED without a child row."""
    set_session_fanout(monkeypatch)
    ctx, _ = make_parent(tmp_path, max_concurrent=1)
    ctx.config["delegation"]["session_max_concurrent"] = 1
    ctx._subagent_parent_kind = "session"
    ctx._subagent_execution_lane = "stateless"
    child_started = asyncio.Event()
    installed: Dict[str, Any] = {}

    class SlowChild(FakeChatModel):
        """Still in its first provider call when the Stop lands."""

        async def astream(self, messages, **kw):
            child_started.set()
            while not installed["runtime"].foreground_batch_stopped:
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.02)
            async for chunk in super().astream(messages, **kw):
                yield chunk

    made: List[FakeChatModel] = []

    def factory(_config, _limits):
        model = SlowChild(
            [
                tool_turn("read_file", {"path": "notes/hello.md"}, "child-read"),
                text_turn("partial findings"),
            ]
        )
        made.append(model)
        return model

    from agent.subagents.driver import SubagentDriver

    graceful_stops: List[tuple] = []
    original_graceful_stop = SubagentDriver.graceful_stop

    async def _spy_graceful_stop(self, reason="parent requested stop", **kw):
        graceful_stops.append((self.handle, reason, self.running))
        return await original_graceful_stop(self, reason, **kw)

    monkeypatch.setattr(SubagentDriver, "graceful_stop", _spy_graceful_stop)
    ledger = RecordingLedger()
    runtime = install(ctx, factory=factory, ledger=ledger)
    installed["runtime"] = runtime
    stop = _OneStop(child_started)
    llm = _Parent([_real_batch("c1", "c2"), AIMessage(content="never")])
    messages: List[Any] = []

    result = await _turn(
        llm,
        {"delegate_agent": the_tool(ctx)},
        _callbacks(check_interrupt=stop),
        ctx,
        messages,
    )

    assert result.interrupted is True and llm.calls == 1
    # Stop reached the running child: graceful_stop, while it was working.
    assert [(reason, running) for _, reason, running in graceful_stops] == [
        ("stop request", True)
    ]
    results = _results(messages)
    assert results["c1"].startswith(STOPPED_HEADER + "\n")
    assert "because this turn was stopped" in results["c1"]
    assert "partial and unverified" in results["c1"]
    assert "partial findings" in results["c1"]
    assert "· interrupted:stopped ·" in results["c1"]
    assert "call delegate_agent again with a task limited to" in results["c1"]
    assert results["c2"] == stopped_not_started_text()
    # One child only; its tool call never ran and its synthesis had no tools.
    assert len(made) == 1 and len(made[0].calls) == 2
    assert [fields["parent_tool_call_id"] for _, fields in ledger.opened] == ["c1"]
    terminal = [fields for _, fields in ledger.updates if fields.get("status")]
    assert terminal[-1]["status"] == "interrupted"
    assert terminal[-1]["outcome"] == "interrupted:stopped"
    assert runtime.foreground_batch_stopped is True
    runtime.begin_batch(1)  # the next batch starts unstopped
    assert runtime.foreground_batch_stopped is False


async def test_a_child_whose_row_opens_after_the_stop_never_runs(tmp_path):
    """The Stop lands while a child's row is being opened: the child is not
    yet active, so ``stop_foreground_batch`` cannot reach it. It must still
    end stopped before its first provider call."""
    ctx, _ = make_parent(tmp_path)
    opening = asyncio.Event()
    release = asyncio.Event()

    class SlowOpenLedger(RecordingLedger):
        async def open(self, subagent_id, **fields):
            opening.set()
            await release.wait()
            await super().open(subagent_id, **fields)

    made: List[FakeChatModel] = []

    def factory(_config, _limits):
        model = FakeChatModel([text_turn("must not be asked")])
        made.append(model)
        return model

    ledger = SlowOpenLedger()
    runtime = install(ctx, factory=factory, ledger=ledger)
    runtime.begin_batch(1)
    call = SubagentCall(
        tool_call_id="c1", subagent_type="explorer", prompt="Report evidence."
    )
    task = asyncio.create_task(runtime.run_foreground(call))
    await opening.wait()
    assert await runtime.stop_foreground_batch() == 0  # nothing active yet
    release.set()
    envelope = await asyncio.wait_for(task, timeout=5)

    assert [len(model.calls) for model in made] == [0]
    assert envelope.startswith(STOPPED_HEADER + "\n")
    assert "It left no report." in envelope
    assert "· interrupted:stopped · 0 turns" in envelope
    assert len(ledger.opened) == 1
    terminal = [fields for _, fields in ledger.updates if fields.get("status")]
    assert terminal[-1]["outcome"] == "interrupted:stopped"


async def test_the_live_markers_match_what_the_model_was_told():
    assert NOT_STARTED_HEADER == NOT_STARTED_MARKER
    assert not_started_result_text().startswith(NOT_STARTED_HEADER + "\n")
    assert stopped_not_started_text().startswith(NOT_STARTED_MARKER + "\n")
    assert STOPPED_HEADER == "[delegate_agent: STOPPED - did not finish]"


# ---------------------------------------------------------------------------
# Shutdown is not a failure (item 7)
# ---------------------------------------------------------------------------


async def test_a_cancelled_turn_writes_no_result_and_propagates():
    both_running = asyncio.Event()
    started: List[str] = []
    finished: List[str] = []

    async def _delegate(call: dict):
        started.append(call["id"])
        if len(started) == 2:
            both_running.set()
        try:
            await asyncio.Future()
        finally:
            finished.append(call["id"])

    class Runtime:
        stop_foreground_batch = AsyncMock()

        def begin_batch(self, _n):
            pass

    runtime = Runtime()
    recorder = _Recorder()
    on_tool_result = AsyncMock()
    messages: List[Any] = []
    task = asyncio.create_task(
        _turn(
            _Parent([_batch("c1", "c2"), AIMessage(content="never")]),
            {"delegate_agent": _tool("delegate_agent", _delegate)},
            _callbacks(
                persist_message=recorder,
                require_delegation_persistence=True,
                on_tool_result=on_tool_result,
            ),
            _session_context(runtime),
            messages,
        )
    )
    await both_running.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert sorted(finished) == ["c1", "c2"]  # joined, none outlives the turn
    assert recorder.results == []  # the successor settles the batch
    assert _result_order(messages) == []
    on_tool_result.assert_not_awaited()
    runtime.stop_foreground_batch.assert_not_awaited()  # not a Stop


async def test_a_cancelled_real_batch_records_cancelled_children_only(
    tmp_path, monkeypatch
):
    set_session_fanout(monkeypatch)
    ctx, _ = make_parent(tmp_path, max_concurrent=2)
    ctx.config["delegation"]["session_max_concurrent"] = 2
    ctx._subagent_parent_kind = "session"
    ctx._subagent_execution_lane = "stateless"
    started = asyncio.Event()

    class HangingChild(FakeChatModel):
        async def astream(self, messages, **kw):
            started.set()
            await asyncio.Event().wait()
            yield  # pragma: no cover

    ledger = RecordingLedger()
    install(ctx, factory=lambda _c, _l: HangingChild([]), ledger=ledger)
    recorder = _Recorder()
    task = asyncio.create_task(
        _turn(
            _Parent([_real_batch("c1", "c2"), AIMessage(content="never")]),
            {"delegate_agent": the_tool(ctx)},
            _callbacks(persist_message=recorder),
            ctx,
        )
    )
    await started.wait()
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert recorder.results == []
    terminal = [fields for _, fields in ledger.updates if fields.get("status")]
    assert {fields["status"] for fields in terminal} == {"cancelled"}
    assert len(terminal) == 2


# ---------------------------------------------------------------------------
# The recovery-turn ban (D3)
# ---------------------------------------------------------------------------


_CONTINUATION = {
    "id": "continuation-event-1",
    "content": "[recovered delegation results]",
    "role": "event",
    "source": "subagent",
    "supersedes_input_seq": 4,
}


async def _recovery_turn(tool_context: Any, monkeypatch) -> List[tuple]:
    seen: List[tuple] = []

    async def _execute_stub(**kwargs):
        ctx = kwargs["tool_context"]
        seen.append(
            (ctx._stateless_subagent_recovery_active, ctx._current_input_message_id)
        )
        return TurnResult(turn_id=0, messages_added=0, tool_calls_made=0)

    monkeypatch.setattr(persistent_graph, "_execute_turn", _execute_stub)
    await run_persistent_loop(
        llm_with_tools=MagicMock(),
        tools=[],
        context_manager=_context_manager(),
        config=_config(),
        system_prompt="system",
        callbacks=_callbacks(
            get_user_input=AsyncMock(
                side_effect=[dict(_CONTINUATION), asyncio.CancelledError()]
            )
        ),
        messages=[],
        tool_context=tool_context,
    )
    return seen


async def test_without_fan_out_the_recovery_turn_keeps_its_ban(monkeypatch):
    set_session_fanout(monkeypatch, allowed=False)
    ctx = SimpleNamespace(_subagent_parent_kind="session")
    assert await _recovery_turn(ctx, monkeypatch) == [(True, "continuation-event-1")]
    assert ctx._stateless_subagent_recovery_active is False  # cleared after


async def test_with_fan_out_the_recovery_turn_has_no_ban(monkeypatch):
    set_session_fanout(monkeypatch)
    ctx = SimpleNamespace(_subagent_parent_kind="session")
    assert await _recovery_turn(ctx, monkeypatch) == [(False, "continuation-event-1")]


@pytest.mark.parametrize("allowed", [True, False])
async def test_a_recovery_turn_delegates_under_its_continuation(
    tmp_path, monkeypatch, allowed
):
    """End to end through the loop and the tool: with fan-out allowed the
    recovery turn delegates, and the child names the continuation event as
    its parent input (the server accepts an event as a parent input); without
    it, the tool refuses exactly as before."""
    set_session_fanout(monkeypatch, allowed=allowed)
    ctx, _ = make_parent(tmp_path)
    ctx._subagent_parent_kind = "session"
    ctx._subagent_execution_lane = "pinned"
    seen: List[Dict[str, Any]] = []

    class Runtime:
        batch_size = 1

        def begin_batch(self, n):
            self.batch_size = n

        async def run_foreground(self, call):
            seen.append(
                {
                    "input": ctx._current_input_message_id,
                    "turn": ctx._current_turn_count,
                    "ai": ctx._current_ai_message_id,
                    "call": call.tool_call_id,
                }
            )
            return "child report"

        async def run_background(self, call):  # pragma: no cover
            raise AssertionError("foreground only")

    ctx.subagent_runtime = Runtime()
    tool = the_tool(ctx)
    delegate_call = AIMessage(
        content="",
        id="recovery-ai-1",
        tool_calls=[
            {
                "name": "delegate_agent",
                "id": "again-1",
                "args": {
                    "description": "finish the rest",
                    "prompt": "Finish what is still missing.",
                    "subagent_type": "explorer",
                },
            }
        ],
    )
    llm = _Parent([delegate_call, AIMessage(content="done")])
    messages: List[Any] = []
    await run_persistent_loop(
        llm_with_tools=llm,
        tools=[tool],
        context_manager=_context_manager(),
        config=_config(),
        system_prompt="system",
        callbacks=_callbacks(
            get_user_input=AsyncMock(
                side_effect=[dict(_CONTINUATION), asyncio.CancelledError()]
            )
        ),
        messages=messages,
        tool_context=ctx,
        initial_turn_count=3,
    )

    result = _results(messages)["again-1"]
    if allowed:
        assert result == "child report"
        assert seen == [
            {
                "input": "continuation-event-1",
                "turn": 4,
                "ai": "recovery-ai-1",
                "call": "again-1",
            }
        ]
    else:
        assert result.startswith("Error: delegate_agent is disabled")
        assert seen == []
