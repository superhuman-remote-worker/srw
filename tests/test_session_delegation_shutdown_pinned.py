"""A graceful pinned shutdown during a session's delegation batch (P4).

Design: knowledge-base/knowledge/features/parallel_subagents.md §14.1 (the
graceful-shutdown bullet), §14.2 P4, D4 and D7.

Only the platform sets the pinned runtime's termination fence (preStop, the
process shutdown); a person's End goes through retirement admission. Its
first activation hands the running batch to the successor: each running
child goes on to its next boundary, where the closed provider admission ends
it, and its row then ends ``interrupted:parent_restart`` instead of
``interrupted:drain``, before the retirement that follows could cancel it. No
call returns a result, so the parent writes none; every call is held until
the termination cancels the turn, which writes nothing either. Termination
quiescence accepts the held batch, ``quiesce`` without settlement authority
passes it, and the successor's settle answers every call (on Postgres in
``test_session_delegation_shutdown_pinned_pg.py``). The stateless twin is
``test_session_delegation_shutdown.py``.
"""

from __future__ import annotations

import asyncio
import re
from types import SimpleNamespace
from typing import Any, List
from unittest.mock import MagicMock

import pytest
from langchain_core.messages import AIMessage

import agent.api.persistent_app as pa
import agent.persistent_graph as persistent_graph
from agent.subagents import SessionHost, SubagentRuntime
from agent.subagents.runtime import SubagentCall
from tests._fake_chat_model import FakeChatModel, text_turn, tool_turn
from tests._fanout_gate import set_session_fanout
from tests.test_delegate_agent_tool import make_parent, the_tool
from tests.test_persistent_delegation_batch import _callbacks
from tests.test_session_delegation_live_batch import (
    _Parent,
    _real_batch,
    _Recorder,
    _turn,
)
from tests.test_subagent_background_runtime import StrictLedger

PARENT = "11111111-2222-4333-8444-555555555555"
RESTART = ("interrupted", "interrupted:parent_restart")
NOT_STARTED = ("interrupted", "interrupted:not_started")


@pytest.fixture(autouse=True)
def _fast_batch_poll(monkeypatch):
    monkeypatch.setattr(persistent_graph, "_DELEGATION_INTERRUPT_POLL_S", 0.01)


@pytest.fixture
def fence(monkeypatch, tmp_path):
    """The pinned runtime's real termination owner, unfenced, in a turn."""

    owner = pa._session_termination
    monkeypatch.delenv("STATELESS_EXECUTOR", raising=False)
    monkeypatch.setattr(owner, "termination_sentinel_path", tmp_path / "terminating")
    monkeypatch.setattr(owner, "termination_admission_fenced", False)
    monkeypatch.setattr(owner, "termination_fence_reason", None)
    monkeypatch.setattr(
        pa,
        "_session_input",
        SimpleNamespace(awaiting_input=False, wake_parked_wait=lambda _item: None),
    )
    monkeypatch.setattr(pa, "_tool_inflight", True)
    monkeypatch.setattr(pa, "_turn_event_open", True)
    monkeypatch.setattr(pa, "_loop_task", None)
    return owner


def _open() -> bool:
    return not pa._session_termination.termination_admission_closed()


def _pinned_runtime(ctx, ledger, factory, monkeypatch, **kwargs) -> SubagentRuntime:
    """A pinned session's runtime whose children read the real fence."""

    ctx.config["delegation"]["session_max_concurrent"] = kwargs.pop("cap", 2)
    ctx._subagent_parent_kind = "session"
    ctx._subagent_execution_lane = "pinned"
    host = SessionHost(
        thread_id=PARENT,
        agent_type="persistent",
        tool_context=ctx,
        admission_fn=_open,
        effect_authority_fn=kwargs.pop("effect_authority", _open),
        settlement_authority_fn=_open,
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
    monkeypatch.setattr(
        pa,
        "_session",
        SimpleNamespace(tool_context=ctx, auxiliary_llm=None, memory_service=None),
    )
    return runtime


class _Child(FakeChatModel):
    """Keyed by the call id in its brief. A call in ``finishes`` answers at
    once. Any other child waits in its first provider call until ``release``,
    then asks for a tool, whose effect the fence refuses; its second answer
    is never asked for once the fence is closed."""

    def __init__(self, finishes: set, release: asyncio.Event, started: List[str]):
        super().__init__([])
        self.finishes = finishes
        self.release = release
        self.started = started

    async def astream(self, messages, **kw):
        self.calls.append(list(messages))
        brief = " ".join(str(m.content) for m in messages if m.type == "human")
        match = re.search(r"\b(c\d+)\b", brief)
        call_id = match.group(1) if match else None
        if len(self.calls) == 1:
            self.started.append(call_id)
        if call_id in self.finishes:
            for chunk in text_turn(f"report of {call_id}"):
                await asyncio.sleep(0)
                yield chunk
            return
        if len(self.calls) == 1:
            await self.release.wait()
            for chunk in tool_turn(
                "read_file", {"path": "notes/hello.md"}, f"{call_id}-read"
            ):
                await asyncio.sleep(0)
                yield chunk
            return
        for chunk in text_turn(f"late answer of {call_id}"):
            yield chunk


def _terminal(ledger: StrictLedger) -> dict[str, tuple[str, str]]:
    """``call id -> (status, outcome)`` of every terminal row written."""

    calls = {child: fields["parent_tool_call_id"] for child, fields in ledger.opened}
    return {
        calls[child]: (fields["status"], fields.get("outcome"))
        for child, fields in ledger.updates
        if fields.get("status") not in {None, "queued", "running"}
    }


async def _until(predicate, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        assert asyncio.get_running_loop().time() < deadline
        await asyncio.sleep(0.01)


# ---------------------------------------------------------------------------
# The §14.2 P4 row: the fence lands during a batch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_the_fence_hands_a_running_batch_to_the_successor(
    tmp_path, monkeypatch, fence
):
    """Four calls, cap 2: c1 finished before the fence, c2 and c3 run, c4 is
    queued. The fence's first activation hands the batch over. c2 and c3 end
    at their next boundary as interrupted by the restart (never
    ``interrupted:drain``), c4 never opens a row, the parent writes no
    result, quiescence is reached with the turn still open, ``quiesce``
    without authority passes and writes nothing, and the termination's
    cancellation writes nothing."""

    set_session_fanout(monkeypatch)
    ctx, _ = make_parent(tmp_path, max_concurrent=2)
    release, started = asyncio.Event(), []
    ledger = StrictLedger()
    runtime = _pinned_runtime(
        ctx, ledger, lambda _c, _l: _Child({"c1"}, release, started), monkeypatch
    )
    recorder = _Recorder()
    turn = asyncio.create_task(
        _turn(
            _Parent([_real_batch("c1", "c2", "c3", "c4"), AIMessage(content="never")]),
            {"delegate_agent": the_tool(ctx)},
            _callbacks(persist_message=recorder),
            ctx,
        )
    )
    monkeypatch.setattr(pa, "_loop_task", turn)
    await _until(
        lambda: sorted(started) == ["c1", "c2", "c3"] and "c1" in _terminal(ledger)
    )
    assert fence.termination_quiescent() is False

    assert fence.activate_termination_admission_fence("kubernetes_prestop") is True
    assert runtime.foreground_left_for_successor is True
    # c2 and c3 are still on their way to a boundary.
    assert runtime.foreground_held_for_successor is False
    assert fence.termination_quiescent() is False
    release.set()
    assert await fence.wait_for_termination_quiescence(5.0) is True

    assert not turn.done()
    assert recorder.results == []  # the successor settles the batch
    assert _terminal(ledger) == {
        "c1": ("completed", "completed"),
        "c2": RESTART,
        "c3": RESTART,
    }
    opened = [fields["parent_tool_call_id"] for _, fields in ledger.opened]
    assert sorted(opened) == ["c1", "c2", "c3"]  # c4 never opened a row
    assert not any(
        fields.get("outcome") == "interrupted:drain" for _, fields in ledger.updates
    )
    for _, fields in ledger.updates:
        if fields.get("outcome") == RESTART[1]:
            assert fields["error"] == "the parent runtime restarted"
            assert fields["turns"] == 1
    # The refused tool never ran, and nobody asked for a second answer.
    assert started.count("c2") == started.count("c3") == 1

    # The retirement's quiesce: the fence revoked settlement authority, and
    # nothing is left to commit.
    writes = len(ledger.updates)
    runtime._recovery_complete = True
    await runtime.quiesce("parent session retiring as ended")
    assert len(ledger.updates) == writes
    assert not turn.done()

    # The termination cancels the loop: every held call ends, nothing written.
    turn.cancel()
    with pytest.raises(asyncio.CancelledError):
        await turn
    assert recorder.results == []
    assert len(ledger.updates) == writes
    assert runtime.foreground_held_for_successor is False

    # A second activation hands nothing over again.
    assert fence.activate_termination_admission_fence("shutdown") is False


@pytest.mark.asyncio
async def test_no_call_returns_once_handed_over(tmp_path, monkeypatch, fence):
    """Two calls, both running at the fence, none queued: a single call that
    returned would be written with its siblings once all had ended."""

    set_session_fanout(monkeypatch)
    ctx, _ = make_parent(tmp_path, max_concurrent=2)
    release, started = asyncio.Event(), []
    ledger = StrictLedger()
    runtime = _pinned_runtime(
        ctx, ledger, lambda _c, _l: _Child(set(), release, started), monkeypatch
    )
    recorder = _Recorder()
    turn = asyncio.create_task(
        _turn(
            _Parent([_real_batch("c1", "c2"), AIMessage(content="never")]),
            {"delegate_agent": the_tool(ctx)},
            _callbacks(persist_message=recorder),
            ctx,
        )
    )
    monkeypatch.setattr(pa, "_loop_task", turn)
    await _until(lambda: sorted(started) == ["c1", "c2"])

    fence.activate_termination_admission_fence("kubernetes_prestop")
    release.set()
    assert await fence.wait_for_termination_quiescence(5.0) is True
    await asyncio.sleep(0.05)

    assert not turn.done()
    assert recorder.results == []
    assert _terminal(ledger) == {"c1": RESTART, "c2": RESTART}
    assert runtime.foreground_held_for_successor is True
    turn.cancel()
    with pytest.raises(asyncio.CancelledError):
        await turn
    assert recorder.results == []


@pytest.mark.asyncio
async def test_a_row_opened_across_the_hand_over_ends_before_its_provider(
    tmp_path, monkeypatch, fence
):
    """The hand-over lands while a child's row is being opened: the child
    makes no provider call, its row ends ``interrupted:not_started`` (the
    settle reports it NOT STARTED and takes no text for it), and its call is
    held."""

    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    ledger.allow_open.clear()
    model = FakeChatModel([text_turn("must never be asked")])
    runtime = _pinned_runtime(ctx, ledger, lambda _c, _l: model, monkeypatch)
    runtime.begin_batch(1)
    task = asyncio.create_task(
        runtime.run_foreground(
            SubagentCall(tool_call_id="c1", subagent_type="explorer", prompt="Go.")
        )
    )
    await asyncio.wait_for(ledger.open_started.wait(), 5)

    fence.activate_termination_admission_fence("kubernetes_prestop")
    ledger.allow_open.set()
    await _until(lambda: runtime.foreground_held_for_successor)

    assert not task.done()
    assert model.calls == []
    ((_, fields),) = [u for u in ledger.updates if u[1].get("status")]
    assert (fields["status"], fields["outcome"]) == NOT_STARTED
    assert fields["error"] == "the parent runtime restarted"
    assert fields["turns"] == 0 and fields["tokens"] == 0
    assert fields["report_path"] is None
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len([u for u in ledger.updates if u[1].get("status")]) == 1


@pytest.mark.asyncio
async def test_a_held_call_waits_for_a_sibling_still_running(
    tmp_path, monkeypatch, fence
):
    """c1 runs, c2's row is being opened at the hand-over. c2 is held at
    once, but the batch counts as held only when c1 has ended too: until
    then quiescence must keep the preStop waiting."""

    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    release, started = asyncio.Event(), []
    runtime = _pinned_runtime(
        ctx, ledger, lambda _c, _l: _Child(set(), release, started), monkeypatch
    )
    runtime.begin_batch(2)
    tasks = [
        asyncio.create_task(
            runtime.run_foreground(
                SubagentCall(
                    tool_call_id="c1",
                    subagent_type="explorer",
                    prompt="Report evidence for c1.",
                )
            )
        )
    ]
    await _until(lambda: started == ["c1"])
    ledger.open_started.clear()
    ledger.allow_open.clear()
    tasks.append(
        asyncio.create_task(
            runtime.run_foreground(
                SubagentCall(tool_call_id="c2", subagent_type="explorer", prompt="Go.")
            )
        )
    )
    await asyncio.wait_for(ledger.open_started.wait(), 5)

    fence.activate_termination_admission_fence("kubernetes_prestop")
    ledger.allow_open.set()
    await _until(lambda: "c2" in _terminal(ledger))
    await asyncio.sleep(0.05)
    assert _terminal(ledger) == {"c2": NOT_STARTED}
    assert len(runtime._successor_waiters) == 1
    assert runtime.foreground_held_for_successor is False
    assert fence.termination_quiescent() is False

    release.set()
    await _until(lambda: runtime.foreground_held_for_successor)
    assert _terminal(ledger) == {"c1": RESTART, "c2": NOT_STARTED}
    assert fence.termination_quiescent() is True
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    assert all(task.cancelled() for task in tasks)


@pytest.mark.asyncio
async def test_a_child_finishing_across_the_hand_over_keeps_its_report(
    tmp_path, monkeypatch, fence
):
    """Its provider call was answered after the fence: the child completed.
    Its own row and spilled report stand for the successor to replay, and its
    call is held all the same: after the hand-over no call returns."""

    ctx, root = make_parent(tmp_path)
    ledger = StrictLedger()
    entered, release = asyncio.Event(), asyncio.Event()

    class Answers(FakeChatModel):
        async def astream(self, messages, **kw):
            self.calls.append(list(messages))
            entered.set()
            await release.wait()
            for chunk in text_turn("the finished report"):
                yield chunk

    runtime = _pinned_runtime(ctx, ledger, lambda _c, _l: Answers([]), monkeypatch)
    runtime.begin_batch(1)
    task = asyncio.create_task(
        runtime.run_foreground(
            SubagentCall(tool_call_id="c1", subagent_type="explorer", prompt="Go.")
        )
    )
    await asyncio.wait_for(entered.wait(), 5)
    fence.activate_termination_admission_fence("kubernetes_prestop")
    release.set()
    await _until(lambda: runtime.foreground_held_for_successor)

    assert not task.done()
    ((_, fields),) = [u for u in ledger.updates if u[1].get("status")]
    assert (fields["status"], fields["outcome"]) == ("completed", "completed")
    assert fields["report_path"] and (root / fields["report_path"]).is_file()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_a_background_child_is_not_handed_over(tmp_path, monkeypatch, fence):
    """Background children keep today's end at the fence: their own
    ``interrupted:drain`` terminal and evidence delivery."""

    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    release, started = asyncio.Event(), []
    runtime = _pinned_runtime(
        ctx, ledger, lambda _c, _l: _Child(set(), release, started), monkeypatch
    )
    receipt = await runtime.run_background(
        SubagentCall(
            tool_call_id="c9",
            subagent_type="explorer",
            prompt="Report evidence for c9.",
            run_in_background=True,
        )
    )
    assert "queued" in receipt
    await _until(lambda: started == ["c9"])

    fence.activate_termination_admission_fence("kubernetes_prestop")
    release.set()
    await asyncio.wait_for(ledger.terminal.wait(), 5)

    ((_, fields),) = ledger.terminal_calls
    assert (fields["status"], fields["outcome"]) == (
        "interrupted",
        "interrupted:drain",
    )
    assert runtime.foreground_held_for_successor is False
    await runtime.close()


@pytest.mark.asyncio
async def test_a_persons_end_still_stops_the_children(tmp_path, monkeypatch, fence):
    """D4, unchanged: without the fence a person's End quiesces the runtime
    with authority, and every running child ends ``interrupted:stopped``."""

    set_session_fanout(monkeypatch)
    ctx, _ = make_parent(tmp_path, max_concurrent=2)
    release, started = asyncio.Event(), []
    ledger = StrictLedger()
    runtime = _pinned_runtime(
        ctx, ledger, lambda _c, _l: _Child(set(), release, started), monkeypatch
    )
    recorder = _Recorder()
    turn = asyncio.create_task(
        _turn(
            _Parent([_real_batch("c1", "c2"), AIMessage(content="stopped")]),
            {"delegate_agent": the_tool(ctx)},
            _callbacks(persist_message=recorder),
            ctx,
        )
    )
    await _until(lambda: sorted(started) == ["c1", "c2"])

    quiescing = asyncio.create_task(runtime.quiesce("parent session retiring as ended"))
    await _until(lambda: all(d._stopped for d in runtime.active.values()))
    release.set()  # each child's checkpoint then runs its stop synthesis
    await asyncio.wait_for(quiescing, 5)
    await asyncio.wait_for(turn, 5)

    assert runtime.foreground_left_for_successor is False
    assert _terminal(ledger) == {
        "c1": ("interrupted", "interrupted:stopped"),
        "c2": ("interrupted", "interrupted:stopped"),
    }
    assert sorted(recorder.results) == ["c1", "c2"]


@pytest.mark.asyncio
async def test_a_build_failing_across_the_hand_over_returns_nothing(
    tmp_path, monkeypatch, fence
):
    """Its environment failed while the hand-over landed: no row was opened,
    and the call is held instead of answering with the failure."""

    import agent.subagents.runtime as runtime_mod

    building, fail = asyncio.Event(), asyncio.Event()

    async def failing_build(*args: Any, **kwargs: Any) -> Any:
        building.set()
        await fail.wait()
        raise RuntimeError("workspace went away")

    monkeypatch.setattr(runtime_mod, "build_child", failing_build)
    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    runtime = _pinned_runtime(
        ctx, ledger, lambda _c, _l: FakeChatModel([]), monkeypatch
    )
    task = asyncio.create_task(
        runtime.run_foreground(
            SubagentCall(tool_call_id="c1", subagent_type="explorer", prompt="Go.")
        )
    )
    await asyncio.wait_for(building.wait(), 5)
    fence.activate_termination_admission_fence("kubernetes_prestop")
    fail.set()
    await _until(lambda: runtime.foreground_held_for_successor)

    assert not task.done()
    assert ledger.opened == [] and ledger.updates == []
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_a_call_after_a_refused_retirement_quiesce_is_held(
    tmp_path, monkeypatch, fence
):
    """Without preStop, SIGTERM's retirement may quiesce while a child still
    runs: the quiesce refuses (no settlement authority) and closes admission.
    A call that reaches the runtime after it is held, never answered with
    the quiescing error."""

    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    release, started = asyncio.Event(), []
    runtime = _pinned_runtime(
        ctx, ledger, lambda _c, _l: _Child(set(), release, started), monkeypatch
    )
    runtime._recovery_complete = True
    running = asyncio.create_task(
        runtime.run_foreground(
            SubagentCall(
                tool_call_id="c1",
                subagent_type="explorer",
                prompt="Report evidence for c1.",
            )
        )
    )
    await _until(lambda: started == ["c1"])
    fence.activate_termination_admission_fence("shutdown")

    with pytest.raises(RuntimeError, match="settlement authority"):
        await runtime.quiesce("parent session retiring as ended")
    late = asyncio.create_task(
        runtime.run_foreground(
            SubagentCall(tool_call_id="c2", subagent_type="explorer", prompt="Go.")
        )
    )
    release.set()
    await _until(lambda: runtime.foreground_held_for_successor)

    assert not late.done() and not running.done()
    assert _terminal(ledger) == {"c1": RESTART}
    await runtime.quiesce("parent session retiring as ended")  # the retry
    for task in (running, late):
        task.cancel()
    await asyncio.gather(running, late, return_exceptions=True)
    assert _terminal(ledger) == {"c1": RESTART}


@pytest.mark.asyncio
async def test_abandon_releases_held_calls_without_writes(tmp_path, monkeypatch, fence):
    """Authority loss after the hand-over must not wait forever on a call
    that never returns."""

    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    runtime = _pinned_runtime(
        ctx, ledger, lambda _c, _l: FakeChatModel([text_turn("x")]), monkeypatch
    )
    fence.activate_termination_admission_fence("kubernetes_prestop")
    task = asyncio.create_task(
        runtime.run_foreground(
            SubagentCall(tool_call_id="c1", subagent_type="explorer", prompt="Go.")
        )
    )
    await _until(lambda: runtime.foreground_held_for_successor)

    await asyncio.wait_for(runtime.abandon("parent authority lost"), 5)
    with pytest.raises(asyncio.CancelledError):
        await task
    assert ledger.opened == [] and ledger.updates == []


# ---------------------------------------------------------------------------
# Review fixes: wide batches, a proof straddling the fence, failure paths
# ---------------------------------------------------------------------------


def _calls(n: int) -> List[SubagentCall]:
    return [
        SubagentCall(
            tool_call_id=f"c{index}",
            subagent_type="explorer",
            prompt=f"Report evidence for c{index}.",
        )
        for index in range(1, n + 1)
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(("cap", "n"), [(1, 3), (2, 5), (2, 7)])
async def test_a_batch_wider_than_twice_its_cap_is_held_whole(
    tmp_path, monkeypatch, fence, cap, n
):
    """Every call queued behind the cap takes a freed slot only to pass it
    on: a held call keeps none, so all n calls end up held, quiescence is
    reached and the retirement's quiesce passes with nothing to write."""

    ctx, _ = make_parent(tmp_path, max_concurrent=cap)
    release, started = asyncio.Event(), []
    ledger = StrictLedger()
    runtime = _pinned_runtime(
        ctx,
        ledger,
        lambda _c, _l: _Child(set(), release, started),
        monkeypatch,
        cap=cap,
    )
    runtime._recovery_complete = True
    runtime.begin_batch(n)
    tasks = [asyncio.create_task(runtime.run_foreground(c)) for c in _calls(n)]
    await _until(lambda: len(started) == cap)

    fence.activate_termination_admission_fence("kubernetes_prestop")
    release.set()
    await _until(lambda: runtime.foreground_held_for_successor)

    assert len(runtime._successor_waiters) == n
    assert runtime._semaphore.held == 0 and runtime._semaphore.waiting == 0
    assert fence.termination_quiescent() is True
    assert not any(task.done() for task in tasks)
    ran = {f"c{index}" for index in range(1, cap + 1)}
    assert _terminal(ledger) == {call_id: RESTART for call_id in ran}
    assert {f["parent_tool_call_id"] for _, f in ledger.opened} == ran
    writes = len(ledger.updates)
    await runtime.quiesce("parent session retiring as ended")
    assert len(ledger.updates) == writes
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    assert all(task.cancelled() for task in tasks)


@pytest.mark.asyncio
@pytest.mark.parametrize(("proof", "end"), [(1, NOT_STARTED), (2, RESTART)])
async def test_a_proof_in_flight_when_the_fence_closes_ends_for_the_restart(
    tmp_path, monkeypatch, fence, proof, end
):
    """The child's exact effect-authority proof is awaiting the database
    when the fence closes admission, and the fence fails it. That is the
    shutdown, not a lost authority: before the first provider call the
    child never ran, before its first tool effect it had."""

    ctx, _ = make_parent(tmp_path)
    in_proof, finish_proof, proofs = asyncio.Event(), asyncio.Event(), []

    async def effect_authority() -> bool:
        proofs.append(1)
        if len(proofs) != proof:
            return _open()
        in_proof.set()
        await finish_proof.wait()  # the round trip
        return _open()

    model = FakeChatModel(
        [
            tool_turn("read_file", {"path": "notes/hello.md"}, "c1-read"),
            text_turn("never asked"),
        ]
    )
    ledger = StrictLedger()
    runtime = _pinned_runtime(
        ctx,
        ledger,
        lambda _c, _l: model,
        monkeypatch,
        effect_authority=effect_authority,
    )
    runtime.begin_batch(1)
    task = asyncio.create_task(runtime.run_foreground(_calls(1)[0]))
    await asyncio.wait_for(in_proof.wait(), 5)

    fence.activate_termination_admission_fence("kubernetes_prestop")
    finish_proof.set()
    await _until(lambda: runtime.foreground_held_for_successor)

    assert _terminal(ledger) == {"c1": end}
    assert len(model.calls) == proof - 1
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_a_create_failing_after_the_hand_over_returns_nothing(
    tmp_path, monkeypatch, fence
):
    """The durable create fails once the batch is handed over: the call is
    held instead of becoming a "Tool execution error" result. Its handle
    stays unsettled (the create may have committed), so the retirement's
    quiesce keeps refusing and a forced stop leaves the row to the
    successor."""

    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    ledger.allow_open.clear()
    runtime = _pinned_runtime(
        ctx, ledger, lambda _c, _l: FakeChatModel([]), monkeypatch
    )
    runtime._recovery_complete = True
    task = asyncio.create_task(runtime.run_foreground(_calls(1)[0]))
    await asyncio.wait_for(ledger.open_started.wait(), 5)

    fence.activate_termination_admission_fence("kubernetes_prestop")
    ledger.refuse_open = True
    ledger.allow_open.set()
    await _until(lambda: runtime.foreground_held_for_successor)

    assert not task.done()
    assert ledger.updates == []
    with pytest.raises(RuntimeError, match="settlement authority"):
        await runtime.quiesce("parent session retiring as ended")
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_a_release_failing_after_the_hand_over_returns_nothing(
    tmp_path, monkeypatch, fence
):
    import agent.subagents.runtime as runtime_mod

    real_build, building, built = runtime_mod.build_child, asyncio.Event(), []
    proceed = asyncio.Event()

    async def build(*args: Any, **kwargs: Any) -> Any:
        building.set()
        await proceed.wait()
        child = await real_build(*args, **kwargs)

        async def release() -> None:
            raise RuntimeError("worktree removal failed")

        child.release = release
        built.append(child)
        return child

    monkeypatch.setattr(runtime_mod, "build_child", build)
    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    runtime = _pinned_runtime(
        ctx, ledger, lambda _c, _l: FakeChatModel([]), monkeypatch
    )
    runtime._recovery_complete = True
    task = asyncio.create_task(runtime.run_foreground(_calls(1)[0]))
    await asyncio.wait_for(building.wait(), 5)

    fence.activate_termination_admission_fence("kubernetes_prestop")
    proceed.set()
    await _until(lambda: runtime.foreground_held_for_successor)

    assert built and not task.done()
    assert ledger.opened == [] and ledger.updates == []
    await runtime.quiesce("parent session retiring as ended")
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_a_report_failing_to_spill_after_the_hand_over_still_ends_the_row(
    tmp_path, monkeypatch, fence
):
    """A child that finished across the hand-over keeps its own end even when
    its report cannot be spilled; the call is held, nothing is returned."""

    import agent.subagents.runtime as runtime_mod

    def no_envelope(*args: Any, **kwargs: Any) -> str:
        raise OSError("workspace gone")

    monkeypatch.setattr(runtime_mod, "build_envelope", no_envelope)
    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    entered, release = asyncio.Event(), asyncio.Event()

    class Answers(FakeChatModel):
        async def astream(self, messages, **kw):
            self.calls.append(list(messages))
            entered.set()
            await release.wait()
            for chunk in text_turn("the finished report"):
                yield chunk

    runtime = _pinned_runtime(ctx, ledger, lambda _c, _l: Answers([]), monkeypatch)
    task = asyncio.create_task(runtime.run_foreground(_calls(1)[0]))
    await asyncio.wait_for(entered.wait(), 5)
    fence.activate_termination_admission_fence("kubernetes_prestop")
    release.set()
    await _until(lambda: runtime.foreground_held_for_successor)

    assert not task.done()
    assert _terminal(ledger) == {"c1": ("completed", "completed")}
    ((_, fields),) = [u for u in ledger.updates if u[1].get("status")]
    assert fields["report_path"] is None
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_a_refused_call_after_the_hand_over_returns_nothing(
    tmp_path, monkeypatch, fence
):
    """Even a call refused before any child (an unknown type) is the
    successor's once the batch is handed over: it reports NOT STARTED."""

    ctx, _ = make_parent(tmp_path)
    ledger = StrictLedger()
    runtime = _pinned_runtime(
        ctx, ledger, lambda _c, _l: FakeChatModel([]), monkeypatch
    )
    fence.activate_termination_admission_fence("kubernetes_prestop")
    task = asyncio.create_task(
        runtime.run_foreground(
            SubagentCall(tool_call_id="c1", subagent_type="nobody", prompt="Go.")
        )
    )
    await _until(lambda: runtime.foreground_held_for_successor)

    assert not task.done()
    assert ledger.opened == []
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    # Without the hand-over the same call is refused with its error.
    other = _pinned_runtime(
        ctx, StrictLedger(), lambda _c, _l: FakeChatModel([]), monkeypatch
    )
    refused = await other.run_foreground(
        SubagentCall(tool_call_id="c2", subagent_type="nobody", prompt="Go.")
    )
    assert refused.startswith("Error: unknown subagent_type")


# ---------------------------------------------------------------------------
# The composition: only the fence's first activation, only the pinned lane
# ---------------------------------------------------------------------------


def _stub_runtime(monkeypatch, *, held: bool = False) -> MagicMock:
    runtime = MagicMock()
    runtime.foreground_held_for_successor = held
    monkeypatch.setattr(
        pa,
        "_session",
        SimpleNamespace(
            tool_context=SimpleNamespace(subagent_runtime=runtime),
            auxiliary_llm=None,
            memory_service=None,
        ),
    )
    return runtime


def test_only_the_first_activation_hands_the_batch_over(monkeypatch, fence):
    runtime = _stub_runtime(monkeypatch)

    assert fence.activate_termination_admission_fence("kubernetes_prestop") is True
    assert fence.activate_termination_admission_fence("shutdown") is False

    runtime.leave_foreground_for_successor.assert_called_once_with(retiring=True)


def test_a_stateless_executor_hands_its_batch_over_itself(monkeypatch, fence):
    runtime = _stub_runtime(monkeypatch)
    monkeypatch.setenv("STATELESS_EXECUTOR", "1")

    fence.activate_termination_admission_fence("shutdown")

    runtime.leave_foreground_for_successor.assert_not_called()


@pytest.mark.parametrize("session", [None, SimpleNamespace(tool_context=None)])
def test_no_runtime_no_hand_over_and_the_fence_still_latches(
    monkeypatch, fence, session
):
    monkeypatch.setattr(pa, "_session", session)

    assert fence.activate_termination_admission_fence("kubernetes_prestop") is True
    assert fence.termination_admission_closed() is True


def test_a_failed_hand_over_never_breaks_the_fence(monkeypatch, fence):
    runtime = _stub_runtime(monkeypatch)
    runtime.leave_foreground_for_successor.side_effect = RuntimeError("boom")

    assert fence.activate_termination_admission_fence("kubernetes_prestop") is True
    assert fence.termination_admission_closed() is True


@pytest.mark.parametrize(
    ("fenced", "held", "aux_inflight", "quiescent"),
    [
        (False, True, 0, False),  # never without the fence
        (True, False, 0, False),  # a child still runs
        (True, True, 1, False),  # an auxiliary provider call still runs
        (True, True, 0, True),
    ],
)
def test_quiescence_accepts_a_held_batch_only_behind_the_fence(
    monkeypatch, fence, fenced, held, aux_inflight, quiescent
):
    _stub_runtime(monkeypatch, held=held)
    pa._session.auxiliary_llm = SimpleNamespace(provider_calls_inflight=aux_inflight)
    monkeypatch.setattr(pa, "_loop_task", MagicMock(done=MagicMock(return_value=False)))
    if fenced:
        monkeypatch.setattr(fence, "termination_admission_fenced", True)

    assert fence.termination_quiescent() is quiescent


def test_a_mock_runtime_never_counts_as_held(monkeypatch, fence):
    runtime = _stub_runtime(monkeypatch)
    runtime.foreground_held_for_successor = MagicMock()  # truthy, not True
    monkeypatch.setattr(fence, "termination_admission_fenced", True)
    monkeypatch.setattr(pa, "_loop_task", MagicMock(done=MagicMock(return_value=False)))

    assert fence.termination_quiescent() is False


def test_the_not_started_end_is_the_settles():
    """The agent cannot import the orchestrator; the two strings must agree."""

    from agent.subagents import batch_recovery
    from orchestrator.database import session_subagent_recovery as settle

    assert (
        batch_recovery.PARENT_RESTART_NOT_STARTED_OUTCOME
        == settle.PARENT_RESTART_NOT_STARTED_OUTCOME
    )
    assert batch_recovery.PARENT_RESTART_OUTCOME == settle.PARENT_RESTART_OUTCOME
    assert batch_recovery.PARENT_RESTART_STATUS == settle.PARENT_RESTART_STATUS
