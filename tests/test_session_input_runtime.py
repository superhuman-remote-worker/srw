"""SessionInputRuntime driven only through its ports, with no persistent runtime.

Every instance here gets its own fake ports and fake ledger. Two instances in
one process must not share a queue, claims, a reclaim lock, interrupt state or
the parked window; identity must be read at each operation, never at
construction.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Optional
from uuid import uuid4

import pytest

from agent.api.lease_context import LeaseHandle
from agent.api.session_contract import (
    DurableInputUnavailable,
    SessionIdentityMismatch,
    TerminationAdmissionClosed,
)
from agent.api.session_input import (
    InputWaitPlan,
    SessionInputPorts,
    SessionInputRuntime,
    SessionRuntimeIdentity,
)


class _Ledger:
    """A small durable-delivery fake that records the order of effects."""

    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.mark_gate: Optional[asyncio.Event] = None
        self.after_persist = None
        self.observed_queue_sizes: list[int] = []
        self.runtime: Optional[SessionInputRuntime] = None

    def _queue_size(self) -> int:
        queue = self.runtime.queue if self.runtime is not None else None
        return queue.qsize() if queue is not None else -1

    async def persist_pinned_input_delivery(self, **row):
        self.calls.append(("persist", row))
        self.observed_queue_sizes.append(self._queue_size())
        delivery_id = row["delivery_id"]
        inserted = delivery_id not in self.rows
        if inserted:
            self.rows[delivery_id] = {
                **row,
                "state": "owned",
                "claim_generation": 1,
                "message_id": f"m-{delivery_id[:8]}",
                "owner_runtime_generation": row["runtime_generation"],
            }
        if self.after_persist is not None:
            self.after_persist()
        return {**self.rows[delivery_id], "transcript_inserted": inserted}

    async def claim_pending_pinned_input_deliveries(self, **identity):
        self.calls.append(("claim", identity))
        result = []
        for row in self.rows.values():
            if row["state"] in {"admitted", "settled", "cancelled"}:
                continue
            if (
                row["owner_runtime_generation"] != identity["runtime_generation"]
                or row["state"] == "deferred"
            ):
                row.update(
                    state="owned",
                    claim_generation=row["claim_generation"] + 1,
                    owner_runtime_generation=identity["runtime_generation"],
                )
            result.append(dict(row))
        return result

    async def mark_pinned_input_delivery_queued(self, **row):
        self.calls.append(("mark", row))
        self.observed_queue_sizes.append(self._queue_size())
        if self.mark_gate is not None:
            await self.mark_gate.wait()
        current = self.rows[row["delivery_id"]]
        if current["claim_generation"] != row["claim_generation"] or current[
            "state"
        ] not in {"owned", "queued"}:
            return False
        current["state"] = "queued"
        return True

    async def transition_pinned_input_delivery(self, **row):
        self.calls.append(("transition", row))
        current = self.rows.get(row["delivery_id"])
        if current is None or current["claim_generation"] != row["claim_generation"]:
            return False
        current["state"] = {
            "unadmit": "deferred",
        }.get(row["transition"], row["transition"])
        return True

    async def transition_stateless_input_delivery(self, **row):
        self.calls.append(("stateless", row))
        return True


@dataclass
class _World:
    """The mutable runtime facts one instance's ports read at call time."""

    ledger: _Ledger = field(default_factory=_Ledger)
    thread_id: Optional[str] = "thread-1"
    process_generation: Optional[str] = "proc-1"
    session_generation: Optional[str] = "sess-1"
    attach_token: Optional[str] = "attach-1"
    agent_id: Optional[str] = "agent-1"
    pod_uid: Optional[str] = "pod-1"
    lease: Optional[LeaseHandle] = None
    attach_generation: int = 1
    turn_count: int = 0
    closed: bool = False
    cloud_ready: bool = True
    fingerprint: Optional[str] = "sha256:" + "0" * 64
    cancellation: bool = True
    turn_open: bool = False
    tool_inflight: bool = False
    stateless: bool = False
    broadcasts: list = field(default_factory=list)
    side_tasks: set = field(default_factory=set)
    human_inputs: list = field(default_factory=list)
    identity_reads: int = 0
    plan: InputWaitPlan = field(default_factory=InputWaitPlan)
    wait_steps: list = field(default_factory=list)
    session: Any = None

    def __post_init__(self) -> None:
        self.session = _Session(self)


class _Session:
    def __init__(self, world: _World) -> None:
        self._world = world
        self.postgres_conn = world.ledger

    @property
    def turn_count(self) -> int:
        return self._world.turn_count


def _runtime(world: _World, *, poll_seconds: float = 0.01) -> SessionInputRuntime:
    def identity() -> SessionRuntimeIdentity:
        world.identity_reads += 1
        return SessionRuntimeIdentity(
            thread_id=world.thread_id,
            process_generation=world.process_generation,
            session_generation=world.session_generation,
            attach_token=world.attach_token,
            agent_id=world.agent_id,
            pod_uid=world.pod_uid,
            lease=world.lease,
            attach_generation=world.attach_generation,
        )

    def track(task):
        world.side_tasks.add(task)
        task.add_done_callback(world.side_tasks.discard)
        return task

    runtime_ref: dict[str, SessionInputRuntime] = {}

    async def begin_input_wait() -> InputWaitPlan:
        world.wait_steps.append(("begin", runtime_ref["rt"].awaiting_input))
        await asyncio.sleep(0)
        world.wait_steps.append(("begin-after-await", runtime_ref["rt"].awaiting_input))
        return world.plan

    runtime = SessionInputRuntime(
        SessionInputPorts(
            session=lambda: world.session,
            identity=identity,
            stateless_mode=lambda: world.stateless,
            runtime_admission_closed=lambda: world.closed,
            protected_cloud_ready=lambda: world.cloud_ready,
            identity_fingerprint=lambda: world.fingerprint,
            cancellation_enabled=lambda: world.cancellation,
            turn_open=lambda: world.turn_open,
            tool_inflight=lambda: world.tool_inflight,
            broadcast=lambda method, params: world.broadcasts.append((method, params)),
            track_side_task=track,
            human_input_accepted=lambda content: world.human_inputs.append(content),
            begin_input_wait=begin_input_wait,
        ),
        poll_seconds=poll_seconds,
    )
    runtime_ref["rt"] = runtime
    world.ledger.runtime = runtime
    return runtime


def _attached(world: _World) -> SessionInputRuntime:
    runtime = _runtime(world)
    runtime.begin_attach()
    runtime.open_queue()
    return runtime


def _drain(queue: asyncio.Queue) -> list:
    items = []
    while not queue.empty():
        items.append(queue.get_nowait())
    return items


# ---------------------------------------------------------------------------
# Independent instances
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_two_runtimes_share_no_queue_claims_lock_interrupt_or_parked_window():
    world_a, world_b = _World(), _World(thread_id="thread-2", agent_id="agent-2")
    a, b = _attached(world_a), _attached(world_b)

    assert a.queue is not b.queue
    assert a.reclaim_lock is not b.reclaim_lock
    assert a.hard_interrupt_event is not b.hard_interrupt_event

    accepted = await a.accept("to A only", delivery_id=str(uuid4()))
    assert accepted.enqueued is True
    assert a.queue.qsize() == 1 and b.queue.empty()
    assert a.queued_claims == {(accepted.delivery_id, 1)}
    assert b.queued_claims == frozenset()
    assert [kind for kind, _ in world_b.ledger.calls] == []

    world_a.turn_open, world_a.turn_count = True, 4
    world_b.turn_open, world_b.turn_count = True, 4
    assert a.signal_interrupt_for_turn(4) == "hard"
    assert a.interrupt_mode == "hard" and a.hard_interrupt_event.is_set()
    assert b.interrupt_mode is None and not b.hard_interrupt_event.is_set()
    assert b.check_interrupt() is None
    assert a.check_interrupt() == "hard"

    async with a.reclaim_lock:
        # B's reclaim does not wait on A's lock.
        assert await asyncio.wait_for(b.reclaim_pending(), timeout=1) == set()

    assert len(_drain(a.queue)) == 1
    getter = asyncio.create_task(a.get_user_input())
    for _ in range(10):
        await asyncio.sleep(0)
    assert a.awaiting_input is True and b.awaiting_input is False
    getter.cancel()
    await asyncio.gather(getter, return_exceptions=True)
    assert a.awaiting_input is False

    a.teardown()
    assert a.queue is None and a.queued_claims == frozenset()
    assert b.queue is not None and b.hard_interrupt_event is not None


def test_state_lives_on_the_instance_not_the_class():
    one, two = _runtime(_World()), _runtime(_World())
    one.begin_attach()
    two.begin_attach()
    one._queued_claims.add(("d", 1))
    assert two.queued_claims == frozenset()
    assert vars(one).keys() >= {
        "_queue",
        "_queued_claims",
        "_reclaim_lock",
        "_protected_reclaim_task",
        "_interrupt_mode",
        "_interrupt_target_turn_id",
        "_hard_interrupt_event",
        "_awaiting_input",
    }


# ---------------------------------------------------------------------------
# Identity: read at each operation, never captured
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_identity_is_read_per_operation_not_at_construction():
    world = _World(process_generation=None)
    runtime = _runtime(world)
    assert world.identity_reads == 0
    runtime.begin_attach()
    runtime.open_queue()
    with pytest.raises(DurableInputUnavailable):
        await runtime.accept("no identity yet", delivery_id=str(uuid4()))
    assert [kind for kind, _ in world.ledger.calls] == []

    world.process_generation = "proc-2"
    delivery = str(uuid4())
    accepted = await runtime.accept("now", delivery_id=delivery)
    persist = world.ledger.calls[0][1]
    assert (persist["runtime_generation"], persist["runtime_attach_token"]) == (
        "proc-2",
        "attach-1",
    )

    world.attach_token = "attach-rotated"
    world.session_generation = "sess-rotated"
    assert await runtime.settle_delivery(delivery, accepted.claim_generation)
    settle = world.ledger.calls[-1][1]
    assert settle["runtime_attach_token"] == "attach-rotated"
    assert settle["session_runtime_generation"] == "sess-rotated"


def test_pinned_identity_follows_the_current_reader():
    """The effect-authority proof reads the pinned identity through the owner."""

    world = _World()
    runtime = _runtime(world)
    assert runtime.pinned_identity() == ("agent-1", "pod-1", "proc-1", "attach-1")
    world.attach_token, world.process_generation = "attach-2", "proc-2"
    assert runtime.pinned_identity() == ("agent-1", "pod-1", "proc-2", "attach-2")
    world.pod_uid = " "
    with pytest.raises(DurableInputUnavailable):
        runtime.pinned_identity()


@pytest.mark.asyncio
async def test_current_lease_handle_is_read_at_each_transition():
    world = _World(stateless=True)
    runtime = _attached(world)
    handle = LeaseHandle()
    handle.update("thread-1", 3, executor_id="exec", pod_uid="pod-x")
    world.lease = handle
    assert await runtime.transition_claimed("d", 1, "admitted", turn_number=1)
    handle.update("thread-1", 4, executor_id="exec", pod_uid="pod-x")
    assert await runtime.transition_claimed("d", 1, "settled")
    replacement = LeaseHandle()
    replacement.update("thread-1", 7, executor_id="exec-2", pod_uid="pod-y")
    world.lease = replacement
    assert await runtime.transition_claimed("d", 1, "deferred", reason="r")
    tokens = [
        row["lease_token"] for kind, row in world.ledger.calls if kind == "stateless"
    ]
    assert tokens == [3, 4, 7]
    world.lease = LeaseHandle()  # inactive handle: the pinned path, no identity
    world.agent_id = None
    assert await runtime.transition_claimed("d", 1, "settled") is False


# ---------------------------------------------------------------------------
# Durable commit before publication
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_commit_and_claim_precede_queue_publication():
    world = _World()
    runtime = _attached(world)
    accepted = await runtime.accept("ordered", delivery_id=str(uuid4()))
    kinds = [kind for kind, _ in world.ledger.calls]
    assert kinds == ["persist", "claim", "mark"]
    # The queue was empty at the commit and at the queued-state CAS.
    assert world.ledger.observed_queue_sizes == [0, 0]
    assert accepted.enqueued and runtime.queue.qsize() == 1
    assert world.human_inputs == ["ordered"]


@pytest.mark.asyncio
async def test_refused_publication_cas_publishes_nothing():
    world = _World()
    runtime = _attached(world)
    original = world.ledger.mark_pinned_input_delivery_queued

    async def _refuse(**row):
        await original(**row)
        return False

    world.ledger.mark_pinned_input_delivery_queued = _refuse
    accepted = await runtime.accept("refused", delivery_id=str(uuid4()))
    assert (accepted.enqueued, accepted.deferred) == (False, False)
    assert runtime.queue.empty() and runtime.queued_claims == frozenset()


@pytest.mark.asyncio
async def test_admission_closing_after_commit_defers_without_publishing():
    world = _World()
    runtime = _attached(world)
    world.ledger.after_persist = lambda: setattr(world, "closed", True)
    accepted = await runtime.accept("late fence", delivery_id=str(uuid4()))
    assert accepted.deferred and accepted.delivery_state == "deferred"
    assert runtime.queue.empty()
    kind, row = world.ledger.calls[-1]
    assert (kind, row["transition"], row["reason"]) == (
        "transition",
        "deferred",
        "runtime_terminating_after_persist",
    )


@pytest.mark.asyncio
async def test_refusals_happen_before_any_durable_effect():
    world = _World()
    runtime = _attached(world)
    with pytest.raises(SessionIdentityMismatch):
        await runtime.accept(
            "x", expected_session_identity_fingerprint="sha256:" + "1" * 64
        )
    world.closed = True
    with pytest.raises(TerminationAdmissionClosed):
        await runtime.accept("x")
    assert world.ledger.calls == []


@pytest.mark.asyncio
async def test_concurrent_publishers_of_one_claim_queue_it_once():
    world = _World()
    runtime = _attached(world)
    delivery = str(uuid4())
    world.ledger.rows[delivery] = {
        "delivery_id": delivery,
        "state": "owned",
        "claim_generation": 2,
        "message_id": "m1",
        "content": "c",
        "role": "human",
        "source": "direct_human",
        "owner_runtime_generation": "proc-1",
    }
    world.ledger.mark_gate = asyncio.Event()
    publishers = [
        asyncio.create_task(runtime.queue_claimed(dict(world.ledger.rows[delivery])))
        for _ in range(3)
    ]
    for _ in range(5):
        await asyncio.sleep(0)
    world.ledger.mark_gate.set()
    assert sorted(await asyncio.gather(*publishers)) == [False, False, True]
    assert len(_drain(runtime.queue)) == 1


# ---------------------------------------------------------------------------
# The input wait and the parked window
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parked_window_opens_only_after_the_turn_boundary_step():
    world = _World(stateless=True)
    runtime = _attached(world)
    getter = asyncio.create_task(runtime.get_user_input())
    for _ in range(10):
        await asyncio.sleep(0)
    assert world.wait_steps == [("begin", False), ("begin-after-await", False)]
    assert runtime.awaiting_input is True
    runtime.queue.put_nowait({"content": "next"})
    assert await getter == {"content": "next"}
    assert runtime.awaiting_input is False


@pytest.mark.asyncio
async def test_timeout_plan_returns_its_item_or_raises_and_closes_the_window():
    world = _World(stateless=True)
    runtime = _attached(world)
    world.plan = InputWaitPlan(0.01, lambda: {"content": "backstop", "role": "event"})
    assert await runtime.get_user_input() == {"content": "backstop", "role": "event"}
    assert runtime.awaiting_input is False

    class _Idle(Exception):
        pass

    def _raise():
        raise _Idle

    world.plan = InputWaitPlan(0.01, _raise)
    with pytest.raises(_Idle) as raised:
        await runtime.get_user_input()
    assert isinstance(raised.value.__context__, asyncio.TimeoutError)
    assert runtime.awaiting_input is False


@pytest.mark.asyncio
async def test_closed_admission_parks_without_consuming_and_wakes_nothing():
    world = _World(stateless=True, closed=True)
    runtime = _attached(world)
    runtime.queue.put_nowait({"content": "durable, for the successor"})
    getter = asyncio.create_task(runtime.get_user_input())
    for _ in range(10):
        await asyncio.sleep(0)
    assert runtime.awaiting_input is True
    assert world.wait_steps == []
    assert runtime.queue.qsize() == 1
    getter.cancel()
    await asyncio.gather(getter, return_exceptions=True)
    assert runtime.awaiting_input is False
    assert runtime.queue.qsize() == 1


@pytest.mark.asyncio
async def test_missing_queue_fails_loudly_and_wake_needs_a_parked_wait():
    world = _World()
    runtime = _runtime(world)
    runtime.begin_attach()
    with pytest.raises(RuntimeError, match="input queue not initialized"):
        await runtime.get_user_input()
    runtime.open_queue()
    runtime.wake_parked_wait("sentinel")
    assert runtime.queue.empty()


@pytest.mark.asyncio
async def test_pinned_wait_polls_the_durable_inbox_each_interval():
    world = _World()
    runtime = _attached(world)
    with pytest.raises(asyncio.TimeoutError):
        await runtime.wait_for_input(runtime.queue, timeout=0.05)
    claims = [kind for kind, _ in world.ledger.calls if kind == "claim"]
    assert len(claims) >= 3

    stateless = _World(stateless=True)
    quiet = _attached(stateless)
    with pytest.raises(asyncio.TimeoutError):
        await quiet.wait_for_input(quiet.queue, timeout=0.02)
    assert stateless.ledger.calls == []


def test_lifecycle_resets_and_teardown():
    runtime = _runtime(_World())
    runtime.begin_attach()
    first_event, first_lock = runtime.hard_interrupt_event, runtime.reclaim_lock
    assert runtime.queue is None
    queue = runtime.open_queue()
    queue.put_nowait(1)
    queue.put_nowait(2)
    assert runtime.drain_queue() == 2 and queue.empty()
    runtime._queued_claims.add(("d", 1))
    runtime.teardown()
    assert (runtime.queue, runtime.hard_interrupt_event) == (None, None)
    assert runtime.queued_claims == frozenset()
    assert runtime.drain_queue() == 0
    runtime.begin_attach()
    assert runtime.hard_interrupt_event is not first_event
    assert runtime.reclaim_lock is not first_lock


# ---------------------------------------------------------------------------
# Interrupts
# ---------------------------------------------------------------------------


def test_interrupt_signal_clear_and_check_on_one_instance():
    world = _World(turn_open=True, turn_count=2)
    runtime = _runtime(world)
    runtime.begin_attach()
    assert runtime.signal_interrupt_for_turn(1) is None
    assert runtime.signal_interrupt_for_turn(2, force_graceful=True) == "graceful"
    assert not runtime.hard_interrupt_event.is_set()
    assert runtime.clear_interrupt(target_turn_id=1) is False
    assert runtime.interrupt_mode == "graceful"
    world.turn_count = 3
    assert runtime.check_interrupt() is None  # stale target discarded
    assert (runtime.interrupt_mode, runtime.interrupt_target_turn_id) == (None, None)
    world.tool_inflight = True
    assert runtime.signal_interrupt_for_turn(3) == "graceful"
    world.tool_inflight = False
    assert runtime.signal_interrupt_for_turn(3) == "hard"
    assert runtime.hard_interrupt_event.is_set()
    assert runtime.check_interrupt() == "hard"
    assert not runtime.hard_interrupt_event.is_set()
    world.turn_open = False
    assert runtime.signal_interrupt_for_turn(3) is None


# ---------------------------------------------------------------------------
# Protected-cloud reclaim task
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_protected_reclaim_task_is_tracked_single_flight_and_life_scoped():
    world = _World(cloud_ready=False)
    runtime = _runtime(world, poll_seconds=0.01)
    runtime.begin_attach()
    runtime.open_queue()
    reclaims = []

    async def _reclaim():
        reclaims.append(world.attach_generation)
        return set()

    runtime.reclaim_pending = _reclaim
    runtime.schedule_protected_reclaim()
    task = runtime.protected_reclaim_task
    assert task in world.side_tasks
    runtime.schedule_protected_reclaim()
    assert runtime.protected_reclaim_task is task

    # Another attach generation: the task exits without reclaiming.
    world.attach_generation = 2
    world.cloud_ready = True
    await asyncio.wait_for(task, timeout=2)
    assert reclaims == [] and runtime.protected_reclaim_task is None

    world.cloud_ready = False
    runtime.schedule_protected_reclaim()
    second = runtime.protected_reclaim_task
    await asyncio.sleep(0)
    second.cancel()
    await asyncio.gather(second, return_exceptions=True)
    assert runtime.protected_reclaim_task is None
    assert second not in world.side_tasks

    runtime.schedule_protected_reclaim()
    third = runtime.protected_reclaim_task
    world.cloud_ready = True
    await asyncio.wait_for(third, timeout=2)
    assert reclaims == [2]

    world.closed = True
    runtime.schedule_protected_reclaim()
    assert runtime.protected_reclaim_task is None
