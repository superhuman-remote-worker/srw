"""A graceful executor shutdown during a session's delegation batch (WP3d).

Design: knowledge-base/knowledge/features/parallel_subagents.md §6.3 (the
Shutdown bullet), §7 (Shutdown row), §8 ("Graceful shutdown during the
batch").

The stateless executor's own abort carries its cause (``shutdown`` or
``lease_lost``) beside the mode and the target turn. The batch stops its
children only for a person's Stop; a platform abort stays pending, the batch
keeps running, and the cancellation that follows writes no result. Children
the cancellation interrupts after ``leave_foreground_for_successor`` keep the
durable row a crash leaves, so the successor's settle interrupts them. The
dispose step (release instead of park) is pinned in ``test_turn_executor.py``;
the whole §8 row on Postgres in ``test_session_delegation_shutdown_pg.py``.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import AIMessage, ToolMessage

import agent.persistent_graph as persistent_graph
from agent.api.session_input import (
    INTERRUPT_CAUSE_LEASE_LOST,
    INTERRUPT_CAUSE_SHUTDOWN,
    INTERRUPT_CAUSE_USER,
    INTERRUPT_CAUSES,
)
from agent.persistent_graph import (
    USER_INTERRUPT_CAUSE,
    DelegationResultNotDurable,
    _await_turn_interrupt,
)
from agent.subagents import RecordingLedger, SessionHost, SubagentRuntime
from agent.subagents.runtime import STOPPED_HEADER, SubagentCall
from tests._fake_chat_model import HANG, FakeChatModel, text_turn, tool_turn
from tests._fanout_gate import set_session_fanout
from tests.test_delegate_agent_tool import install, make_parent, the_tool
from tests.test_persistent_delegation_batch import _callbacks, _tool
from tests.test_session_delegation_live_batch import (
    _batch,
    _Parent,
    _real_batch,
    _Recorder,
    _results,
    _session_context,
    _turn,
)
from tests.test_session_input_runtime import _runtime as _input_runtime
from tests.test_session_input_runtime import _World
from tests.test_session_subagent_runtime_strict import (
    StrictSessionLedger,
    _capture_builds,
)
from tests.test_subagent_runtime import make_parent as make_runtime_parent


@pytest.fixture(autouse=True)
def _fast_batch_poll(monkeypatch):
    monkeypatch.setattr(persistent_graph, "_DELEGATION_INTERRUPT_POLL_S", 0.01)


def _owner(*, turn: int = 1, tool_inflight: bool = True):
    """The session's real input owner, inside turn ``turn``."""
    world = _World(turn_open=True, turn_count=turn, tool_inflight=tool_inflight)
    owner = _input_runtime(world)
    owner.begin_attach()
    return world, owner


def _owner_callbacks(owner, **overrides):
    return _callbacks(
        check_interrupt=owner.check_interrupt,
        peek_interrupt_cause=owner.peek_interrupt_cause,
        hard_interrupt_event=owner.hard_interrupt_event,
        **overrides,
    )


async def _settle_polls(n: int = 5) -> None:
    for _ in range(n):
        await asyncio.sleep(0.01)


# ---------------------------------------------------------------------------
# The cause travels with the interrupt
# ---------------------------------------------------------------------------


def test_the_loop_and_the_input_owner_name_one_person_cause():
    assert USER_INTERRUPT_CAUSE == INTERRUPT_CAUSE_USER
    assert INTERRUPT_CAUSES == {"user", "shutdown", "lease_lost"}


@pytest.mark.parametrize(
    "cause",
    [INTERRUPT_CAUSE_USER, INTERRUPT_CAUSE_SHUTDOWN, INTERRUPT_CAUSE_LEASE_LOST],
)
def test_the_cause_travels_with_the_mode_and_target(cause):
    _world, owner = _owner()

    assert owner.signal_interrupt_for_turn(1, force_graceful=True, cause=cause) == (
        "graceful"
    )
    assert (owner.interrupt_mode, owner.interrupt_target_turn_id) == ("graceful", 1)
    assert owner.interrupt_cause == cause
    # Peeking never consumes.
    assert owner.peek_interrupt_cause() == cause
    assert owner.peek_interrupt_cause() == cause
    assert owner.interrupt_mode == "graceful"

    assert owner.check_interrupt() == "graceful"
    assert owner.interrupt_cause is None
    assert owner.peek_interrupt_cause() is None


def test_an_untagged_signal_is_a_person_stop():
    """The interrupt API, the legacy socket/REST verbs and rewind pass no
    cause."""
    _world, owner = _owner(tool_inflight=False)
    assert owner.signal_interrupt_for_turn(1) == "hard"
    assert owner.interrupt_cause == INTERRUPT_CAUSE_USER
    assert owner.peek_interrupt_cause() == INTERRUPT_CAUSE_USER


def test_the_cause_is_cleared_with_the_interrupt():
    _world, owner = _owner()
    owner.signal_interrupt_for_turn(1, cause=INTERRUPT_CAUSE_SHUTDOWN)
    assert owner.clear_interrupt(target_turn_id=2) is False  # another turn's
    assert owner.interrupt_cause == INTERRUPT_CAUSE_SHUTDOWN
    assert owner.clear_interrupt(target_turn_id=1) is True
    assert owner.interrupt_cause is None

    owner.signal_interrupt_for_turn(1, cause=INTERRUPT_CAUSE_LEASE_LOST)
    owner.begin_attach()
    assert owner.interrupt_cause is None

    owner.signal_interrupt_for_turn(1, cause=INTERRUPT_CAUSE_LEASE_LOST)
    owner.teardown()
    assert owner.interrupt_cause is None


def test_a_person_stop_keeps_its_cause_under_a_platform_abort():
    world, owner = _owner()
    owner.signal_interrupt_for_turn(1)
    assert owner.signal_interrupt_for_turn(
        1, force_graceful=True, cause=INTERRUPT_CAUSE_SHUTDOWN
    ) == ("graceful")
    assert owner.interrupt_cause == INTERRUPT_CAUSE_USER

    owner.clear_interrupt()
    owner.signal_interrupt_for_turn(1, cause=INTERRUPT_CAUSE_SHUTDOWN)
    owner.signal_interrupt_for_turn(1)
    assert owner.interrupt_cause == INTERRUPT_CAUSE_USER

    # A person's Stop for an earlier turn is not carried into the next one.
    world.turn_count = 2
    owner.signal_interrupt_for_turn(2, cause=INTERRUPT_CAUSE_SHUTDOWN)
    assert owner.interrupt_cause == INTERRUPT_CAUSE_SHUTDOWN


def test_a_stale_interrupt_peeks_as_nothing_and_check_discards_it():
    world, owner = _owner()
    owner.signal_interrupt_for_turn(1, cause=INTERRUPT_CAUSE_SHUTDOWN)
    world.turn_count = 2
    assert owner.peek_interrupt_cause() is None
    assert owner.interrupt_mode == "graceful"  # peek changed nothing
    assert owner.check_interrupt() is None
    assert (owner.interrupt_mode, owner.interrupt_cause) == (None, None)


def test_an_unknown_cause_is_refused_without_a_signal():
    _world, owner = _owner()
    with pytest.raises(ValueError, match="unknown interrupt cause"):
        owner.signal_interrupt_for_turn(1, cause="drain")
    assert (owner.interrupt_mode, owner.interrupt_cause) == (None, None)


# ---------------------------------------------------------------------------
# The batch watcher
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cause", "tool_inflight", "mode"),
    [
        (INTERRUPT_CAUSE_SHUTDOWN, True, "graceful"),
        (INTERRUPT_CAUSE_LEASE_LOST, False, "hard"),  # the hard event is set
    ],
)
async def test_the_watcher_leaves_a_platform_abort_pending(cause, tool_inflight, mode):
    world, owner = _owner(tool_inflight=tool_inflight)
    assert owner.signal_interrupt_for_turn(1, cause=cause) == mode
    watcher = asyncio.create_task(
        _await_turn_interrupt(
            owner.check_interrupt,
            owner.hard_interrupt_event,
            owner.peek_interrupt_cause,
        )
    )
    await _settle_polls()
    assert not watcher.done()
    assert (owner.interrupt_mode, owner.interrupt_cause) == (mode, cause)

    # A person's Stop on top of it is consumed at once.
    world.tool_inflight = True
    owner.signal_interrupt_for_turn(1)
    assert await asyncio.wait_for(watcher, timeout=1) == "graceful"
    assert owner.interrupt_mode is None


@pytest.mark.asyncio
async def test_without_a_cause_reader_every_interrupt_is_a_stop():
    """A transport that wires no ``peek_interrupt_cause`` keeps today's
    behaviour (the child driver, older callers)."""
    _world, owner = _owner()
    owner.signal_interrupt_for_turn(1, cause=INTERRUPT_CAUSE_SHUTDOWN)
    mode = await asyncio.wait_for(
        _await_turn_interrupt(owner.check_interrupt, owner.hard_interrupt_event),
        timeout=1,
    )
    assert mode == "graceful" and owner.interrupt_mode is None


class _BlockingBatch:
    """Two blocked ``delegate_agent`` calls and a runtime whose Stop releases
    them with STOPPED results."""

    def __init__(self) -> None:
        self.both_running = asyncio.Event()
        self.released = asyncio.Event()
        self.started: List[str] = []
        self.stops = 0
        self.batches: List[int] = []

    def begin_batch(self, n: int) -> None:
        self.batches.append(n)

    async def stop_foreground_batch(self) -> None:
        self.stops += 1
        self.released.set()

    async def delegate(self, call: dict) -> str:
        self.started.append(call["id"])
        if len(self.started) == 2:
            self.both_running.set()
        await self.released.wait()
        return f"{STOPPED_HEADER}\npartial {call['id']}"


@pytest.mark.asyncio
async def test_a_shutdown_abort_does_not_stop_the_batch_and_the_cancel_writes_nothing():
    _world, owner = _owner()
    batch = _BlockingBatch()
    recorder = _Recorder()
    on_tool_result = AsyncMock()
    messages: List[Any] = []
    task = asyncio.create_task(
        _turn(
            _Parent([_batch("c1", "c2"), AIMessage(content="never")]),
            {"delegate_agent": _tool("delegate_agent", batch.delegate)},
            _owner_callbacks(
                owner,
                persist_message=recorder,
                require_delegation_persistence=True,
                on_tool_result=on_tool_result,
            ),
            _session_context(batch),
            messages,
        )
    )
    await asyncio.wait_for(batch.both_running.wait(), timeout=2)

    # stop() after its shutdown window: the platform's abort, forced graceful.
    assert (
        owner.signal_interrupt_for_turn(
            1, force_graceful=True, cause=INTERRUPT_CAUSE_SHUTDOWN
        )
        == "graceful"
    )
    await _settle_polls()
    assert batch.stops == 0  # not a Stop: the children keep running
    assert not task.done()
    assert owner.interrupt_cause == INTERRUPT_CAUSE_SHUTDOWN  # left pending

    # Then the executor cancels the turn: nothing is written.
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert recorder.results == []
    assert [m for m in messages if isinstance(m, ToolMessage)] == []
    on_tool_result.assert_not_awaited()
    assert batch.stops == 0


@pytest.mark.asyncio
async def test_a_person_stop_still_reaches_the_batch():
    _world, owner = _owner()
    batch = _BlockingBatch()
    recorder = _Recorder()
    llm = _Parent([_batch("c1", "c2"), AIMessage(content="never")])
    messages: List[Any] = []
    task = asyncio.create_task(
        _turn(
            llm,
            {"delegate_agent": _tool("delegate_agent", batch.delegate)},
            _owner_callbacks(
                owner, persist_message=recorder, require_delegation_persistence=True
            ),
            _session_context(batch),
            messages,
        )
    )
    await asyncio.wait_for(batch.both_running.wait(), timeout=2)
    assert owner.signal_interrupt_for_turn(1) == "graceful"  # a person's Stop

    result = await asyncio.wait_for(task, timeout=2)

    assert batch.stops == 1
    assert result.interrupted is True and llm.calls == 1
    assert recorder.results == ["c1", "c2"]
    assert all(text.startswith(STOPPED_HEADER) for text in _results(messages).values())
    assert owner.interrupt_mode is None  # consumed by the batch


# ---------------------------------------------------------------------------
# Children a shutdown cancellation interrupts keep a crash's durable state
# ---------------------------------------------------------------------------


class _ByBrief(FakeChatModel):
    """Answers the brief of a call in ``finishes`` at once; any other child
    stays in its first provider call until it is cancelled."""

    def __init__(self, finishes: set, started: List[str]):
        super().__init__([])
        self.finishes = finishes
        self.started = started

    async def astream(self, messages, **kw):
        self.calls.append(list(messages))
        brief = " ".join(str(m.content) for m in messages if m.type == "human")
        call_id = next(
            (word.rstrip(".") for word in brief.split() if word.startswith("c")),
            None,
        )
        self.started.append(call_id)
        if call_id in self.finishes:
            for chunk in text_turn(f"report of {call_id}"):
                await asyncio.sleep(0)
                yield chunk
            return
        await asyncio.Event().wait()
        yield  # pragma: no cover


@pytest.mark.asyncio
async def test_a_shutdown_cancellation_leaves_unfinished_children_as_a_crash_does(
    tmp_path, monkeypatch
):
    """Four calls, cap 2: c1 finishes, c2 and c3 are running, c4 is queued.
    After the mark the cancellation writes no terminal row for c2 and c3, c1
    keeps its completed row, c4 never opened one, and nothing is left for
    ``quiesce`` to commit."""
    set_session_fanout(monkeypatch)
    ctx, _ = make_parent(tmp_path, max_concurrent=2)
    ctx.config["delegation"]["session_max_concurrent"] = 2
    ctx._subagent_parent_kind = "session"
    ctx._subagent_execution_lane = "stateless"
    started: List[str] = []
    ledger = RecordingLedger()
    runtime = install(
        ctx, factory=lambda _c, _l: _ByBrief({"c1"}, started), ledger=ledger
    )
    recorder = _Recorder()
    task = asyncio.create_task(
        _turn(
            _Parent([_real_batch("c1", "c2", "c3", "c4"), AIMessage(content="never")]),
            {"delegate_agent": the_tool(ctx)},
            _callbacks(persist_message=recorder),
            ctx,
        )
    )
    deadline = asyncio.get_running_loop().time() + 5
    # c1's terminal write follows the release of its slot, so c3 can start
    # first: wait for both.
    while sorted(started) != ["c1", "c2", "c3"] or not any(
        fields.get("status") for _, fields in ledger.updates
    ):
        assert asyncio.get_running_loop().time() < deadline, started
        await asyncio.sleep(0.01)
    await asyncio.sleep(0.05)

    runtime.leave_foreground_for_successor()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    by_id = {subagent_id: fields for subagent_id, fields in ledger.opened}
    opened_calls = [fields["parent_tool_call_id"] for fields in by_id.values()]
    assert sorted(opened_calls) == ["c1", "c2", "c3"]  # c4 never opened a row
    assert sorted(call for call, _ in runtime.foreground_opened) == ["c1", "c2", "c3"]
    assert {child for _, child in runtime.foreground_opened} == set(by_id)
    terminal = [
        (by_id[subagent_id]["parent_tool_call_id"], fields)
        for subagent_id, fields in ledger.updates
        if fields.get("status")
    ]
    assert [(call, fields["status"]) for call, fields in terminal] == [
        ("c1", "completed")
    ]
    assert terminal[0][1]["outcome"] == "completed"
    assert recorder.results == []  # the successor settles the batch
    assert runtime._foreground_terminal_pending == {}
    await runtime.quiesce("session background work quiescing")
    assert [fields for _, fields in ledger.updates if fields.get("status")] == [
        terminal[0][1]
    ]


@pytest.mark.asyncio
async def test_a_session_child_left_for_the_successor_writes_no_terminal_row(
    tmp_path,
):
    """The strict session ledger: without the mark a cancellation commits
    ``cancelled`` (``test_session_subagent_runtime_strict``); with it the row
    stays as opened, nothing is owed, and quiesce commits nothing."""
    ctx, _ = make_runtime_parent(tmp_path)
    ledger = StrictSessionLedger()
    model = FakeChatModel([HANG])
    host = SessionHost(
        thread_id="11111111-2222-4333-8444-555555555555",
        agent_type="persistent",
        tool_context=ctx,
        admission_fn=lambda: True,
        effect_authority_fn=lambda: True,
    )
    runtime = SubagentRuntime.from_context(
        ctx,
        host,
        ledger=ledger,
        llm_factory=lambda _c, _l: model,
        driver_kwargs={
            "watcher_poll_interval": 0.01,
            "archiver": None,
            "archive_fn": lambda **kwargs: None,
        },
    )
    ctx._parent_host = host
    ctx.subagent_runtime = runtime
    runtime.begin_batch(1)
    running = asyncio.create_task(
        runtime.run_foreground(
            SubagentCall(tool_call_id="c1", subagent_type="explorer", prompt="Go.")
        )
    )
    await asyncio.wait_for(model.hang_started.wait(), 5)
    ((opened_id, _fields),) = ledger.opened
    assert runtime.foreground_opened == [("c1", opened_id)]

    runtime.leave_foreground_for_successor()
    assert runtime.foreground_left_for_successor is True
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running

    assert [fields for _, fields in ledger.updates if fields.get("status")] == []
    assert runtime._foreground_terminal_pending == {}
    await runtime.quiesce("session background work quiescing")
    assert ledger.updates == []
    # A new batch forgets the opened children; the mark stays.
    runtime.begin_batch(2)
    assert runtime.foreground_opened == []
    assert runtime.foreground_left_for_successor is True
    await runtime.close()


def _session_runtime(ctx, ledger, factory, **kwargs) -> SubagentRuntime:
    host = SessionHost(
        thread_id="11111111-2222-4333-8444-555555555555",
        agent_type="persistent",
        tool_context=ctx,
        admission_fn=lambda: True,
        effect_authority_fn=lambda: True,
    )
    runtime = SubagentRuntime.from_context(
        ctx,
        host,
        ledger=ledger,
        llm_factory=factory,
        driver_kwargs={
            "watcher_poll_interval": 0.01,
            "archiver": None,
            "archive_fn": lambda **kw: None,
        },
        **kwargs,
    )
    ctx._parent_host = host
    ctx.subagent_runtime = runtime
    return runtime


def _call(call_id: str) -> SubagentCall:
    return SubagentCall(tool_call_id=call_id, subagent_type="explorer", prompt="Go.")


@pytest.mark.asyncio
async def test_no_child_starts_once_the_batch_is_handed_to_the_successor(
    tmp_path, monkeypatch
):
    """Cap 1: c1 runs, c2 is queued. After the mark, c1's slot frees (its
    cancellation lands first) and c2 takes it; a call c3 arrives later. Neither
    builds a child, opens a row, calls a provider or returns a result: both
    wait for the turn's cancellation, and the successor reports them as not
    started."""
    ctx, _ = make_runtime_parent(tmp_path)
    ledger = StrictSessionLedger()
    builds = _capture_builds(monkeypatch)
    models: List[FakeChatModel] = []

    def factory(_config, _limits):
        model = FakeChatModel([HANG])
        models.append(model)
        return model

    runtime = _session_runtime(ctx, ledger, factory, max_concurrent=1)
    runtime.begin_batch(3)
    first = asyncio.create_task(runtime.run_foreground(_call("c1")))
    await asyncio.wait_for(
        _until(lambda: models and models[0].hang_started.is_set()), 5
    )
    queued = asyncio.create_task(runtime.run_foreground(_call("c2")))
    await _settle_polls()
    assert [fields["parent_tool_call_id"] for _, fields in ledger.opened] == ["c1"]

    runtime.leave_foreground_for_successor()
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    late = asyncio.create_task(runtime.run_foreground(_call("c3")))
    await _settle_polls(10)

    assert not queued.done() and not late.done()  # no result
    assert [fields["parent_tool_call_id"] for _, fields in ledger.opened] == ["c1"]
    assert len(builds) == 1 and len(models) == 1  # no child, no provider call
    assert [call for call, _ in runtime.foreground_opened] == ["c1"]

    for task in (queued, late):
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert ledger.updates == []  # c1 left running; c2 and c3 never had a row
    assert runtime._foreground_terminal_pending == {}
    await runtime.quiesce("session background work quiescing")
    assert ledger.updates == []
    await runtime.close()


@pytest.mark.asyncio
async def test_a_row_opened_across_the_hand_over_never_reaches_its_provider(
    tmp_path, monkeypatch
):
    """The mark lands while a child's row is being opened: the row stays as
    opened (the successor interrupts it) and the child makes no provider
    call."""
    ctx, _ = make_runtime_parent(tmp_path)
    opening = asyncio.Event()
    release = asyncio.Event()

    class SlowOpenLedger(StrictSessionLedger):
        async def open(self, subagent_id, **fields):
            opening.set()
            await release.wait()
            return await super().open(subagent_id, **fields)

    ledger = SlowOpenLedger()
    builds = _capture_builds(monkeypatch)
    model = FakeChatModel([text_turn("must never be asked")])
    runtime = _session_runtime(ctx, ledger, lambda _c, _l: model)
    runtime.begin_batch(1)
    task = asyncio.create_task(runtime.run_foreground(_call("c1")))
    await asyncio.wait_for(opening.wait(), 5)

    runtime.leave_foreground_for_successor()
    release.set()
    await _settle_polls(10)

    assert not task.done()
    assert model.calls == []
    assert len(ledger.opened) == 1
    assert [call for call, _ in runtime.foreground_opened] == ["c1"]
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert ledger.updates == []
    assert len(builds) == 1 and builds[0].released is True
    await runtime.close()


async def _until(predicate) -> None:
    while not predicate():
        await asyncio.sleep(0.01)


# ---------------------------------------------------------------------------
# Lease loss keeps stopping children at their own authority check
# ---------------------------------------------------------------------------


class _LostAuthorityLedger(StrictSessionLedger):
    """Refuses every child write once the parent's lease is gone."""

    def __init__(self, lost: Dict[str, bool]):
        super().__init__()
        self.lost = lost
        self.refused: List[Dict[str, Any]] = []

    async def update(self, subagent_id: str, **fields: Any) -> None:
        if self.lost["value"]:
            self.refused.append(dict(fields))
            raise RuntimeError("parent authority is no longer current")
        await super().update(subagent_id, **fields)


@pytest.mark.asyncio
async def test_lease_loss_stops_children_at_their_authority_check_not_by_stop(
    tmp_path, monkeypatch
):
    set_session_fanout(monkeypatch)
    ctx, _ = make_parent(tmp_path, max_concurrent=2)
    ctx.config["delegation"]["session_max_concurrent"] = 2
    ctx._subagent_parent_kind = "session"
    ctx._subagent_execution_lane = "stateless"
    lost = {"value": False}
    in_first_call = asyncio.Event()
    release = asyncio.Event()
    models: List[FakeChatModel] = []

    class FirstCallWaits(FakeChatModel):
        async def astream(self, messages, **kw):
            if not self.calls:
                models_waiting.append(self)
                if len(models_waiting) == 2:
                    in_first_call.set()
                await release.wait()
            async for chunk in super().astream(messages, **kw):
                yield chunk

    models_waiting: List[FakeChatModel] = []

    def factory(_config, _limits):
        model = FirstCallWaits(
            [
                tool_turn("read_file", {"path": "notes/hello.md"}, "child-read"),
                text_turn("must never be asked"),
            ]
        )
        models.append(model)
        return model

    async def effect_authority() -> bool:
        return not lost["value"]

    ledger = _LostAuthorityLedger(lost)
    host = SessionHost(
        thread_id="11111111-2222-4333-8444-555555555555",
        agent_type="persistent",
        tool_context=ctx,
        admission_fn=lambda: not lost["value"],
        effect_authority_fn=effect_authority,
    )
    runtime = SubagentRuntime.from_context(
        ctx,
        host,
        ledger=ledger,
        llm_factory=factory,
        driver_kwargs={
            "watcher_poll_interval": 0.01,
            "archiver": None,
            "archive_fn": lambda **kwargs: None,
        },
    )
    ctx._parent_host = host
    ctx.subagent_runtime = runtime
    stops = 0
    real_stop = runtime.stop_foreground_batch

    async def counted_stop(**kw):
        nonlocal stops
        stops += 1
        return await real_stop(**kw)

    monkeypatch.setattr(runtime, "stop_foreground_batch", counted_stop)
    _world, owner = _owner()

    async def persist(message) -> Optional[bool]:
        return not (lost["value"] and isinstance(message, ToolMessage))

    task = asyncio.create_task(
        _turn(
            _Parent([_real_batch("c1", "c2"), AIMessage(content="never")]),
            {"delegate_agent": the_tool(ctx)},
            _owner_callbacks(
                owner,
                persist_message=persist,
                require_delegation_persistence=True,
            ),
            ctx,
        )
    )
    await asyncio.wait_for(in_first_call.wait(), timeout=5)

    # The heartbeat finds the lease gone: authority is lost, and the executor
    # signals its lease_lost abort.
    lost["value"] = True
    assert owner.signal_interrupt_for_turn(1, cause=INTERRUPT_CAUSE_LEASE_LOST)
    await _settle_polls()
    assert stops == 0  # the batch left the abort pending
    release.set()

    with pytest.raises(DelegationResultNotDurable):
        await asyncio.wait_for(task, timeout=10)

    # No provider call after the loss, and no Stop was needed to get there.
    assert [len(model.calls) for model in models] == [1, 1]
    assert stops == 0 and runtime.foreground_batch_stopped is False
    # Their saves were refused (the terminal rows and the parent's results).
    assert ledger.refused and all(
        fields.get("status") not in {None, "queued", "running"}
        for fields in ledger.refused
    )
    assert owner.interrupt_cause == INTERRUPT_CAUSE_LEASE_LOST  # left pending
    await runtime.abandon("parent authority lost")
