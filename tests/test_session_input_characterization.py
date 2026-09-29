"""Characterization of persistent-session input admission, delivery and interrupts.

Written against ``131dd22ee`` before R3.3a moved this state out of
``persistent_app``. The suite drives only stable seams:

* the transport operations ``session_transport_bindings()`` hands the HTTP and
  socket transports (``accept_input``, ``signal_interrupt``, ``input_queue``);
* the loop callbacks exactly as ``_ensure_persistent_loop_started`` wires them
  into ``PersistentLoopCallbacks``;
* the attach and teardown lifecycle entry points.

The arrangement and read-only state view are isolated in one adapter block
below. The extraction may change that block and nothing else in this file.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
import pytest_asyncio

import agent.api.persistent_app as pa
from agent.api.lease_context import LeaseHandle, current_lease
from agent.api.session_contract import DurableInputUnavailable
from agent.database.postgres_db import PostgresDB as AgentPostgresDB
from tests.test_stateless_input_delivery_real_postgres import (
    _schema_applied,  # noqa: F401 - fixture
    _seed_pinned_thread,
    db,  # noqa: F401 - fixture
    pg_dsn,  # noqa: F401 - fixture
)


# ---------------------------------------------------------------------------
# Arrangement adapter — the only block the R3.3a extraction may change.
# ---------------------------------------------------------------------------


def _arrange_input_state(monkeypatch, *, queue_open: bool = True) -> None:
    """A freshly attached input runtime: queue, interrupt, claims and lock."""

    owner = pa._session_input
    monkeypatch.setattr(owner, "_queue", asyncio.Queue() if queue_open else None)
    monkeypatch.setattr(owner, "_interrupt_mode", None)
    monkeypatch.setattr(owner, "_interrupt_target_turn_id", None)
    monkeypatch.setattr(owner, "_hard_interrupt_event", asyncio.Event())
    monkeypatch.setattr(owner, "_reclaim_lock", asyncio.Lock())
    monkeypatch.setattr(owner, "_protected_reclaim_task", None)
    monkeypatch.setattr(owner, "_awaiting_input", False)
    monkeypatch.setattr(owner, "_queued_claims", set())


def _restart_input_process(monkeypatch) -> str:
    """Model a process death after its RAM queue was lost; new generation."""

    _arrange_input_state(monkeypatch)
    generation = str(uuid4())
    monkeypatch.setattr(pa._session_identity, "_process_generation", generation)
    return generation


def _input_view() -> SimpleNamespace:
    """Read-only view of the runtime's input and interrupt state."""

    owner = pa._session_input
    return SimpleNamespace(
        queue=owner.queue,
        claims=owner.queued_claims,
        mode=owner.interrupt_mode,
        target=owner.interrupt_target_turn_id,
        hard_event=owner.hard_interrupt_event,
        awaiting=owner.awaiting_input,
        reclaim_lock=owner.reclaim_lock,
        reclaim_task=owner.protected_reclaim_task,
    )


def _note_queued_claim(key: tuple[str, int]) -> None:
    """Record a published claim the way queue publication does."""

    pa._session_input._queued_claims.add(key)


def _replace_reclaim(monkeypatch, fake) -> None:
    """Stand in for the durable reclaim the protected-heal task awaits."""

    monkeypatch.setattr(pa._session_input, "reclaim_pending", fake)


def _snapshot_input_state():
    """The owner object's state, which the module-global snapshot misses."""

    owner = pa._session_input
    return dict(vars(owner)), set(owner._queued_claims)


def _restore_input_state(snapshot) -> None:
    attrs, claims = snapshot
    owner = pa._session_input
    vars(owner).clear()
    vars(owner).update(attrs)
    owner._queued_claims.clear()
    owner._queued_claims.update(claims)


def _runtime_ops() -> SimpleNamespace:
    """Runtime operations that are not transport or loop-callback ports."""

    owner = pa._session_input
    return SimpleNamespace(
        reclaim=owner.reclaim_pending,
        queue_claimed=owner.queue_claimed,
        schedule_protected_reclaim=owner.schedule_protected_reclaim,
        transition=owner.transition_claimed,
        clear_interrupt=owner.clear_interrupt,
    )


@pytest.fixture(autouse=True)
def _restore_runtime_globals():
    """Real attach/teardown mutate process globals; restore every one."""

    import types

    saved = {
        name: value
        for name, value in vars(pa).items()
        if not name.startswith("__")
        and not isinstance(value, (types.ModuleType, types.FunctionType, type))
    }
    input_state = _snapshot_input_state()
    contents = {
        name: (value.copy() if hasattr(value, "copy") else list(value))
        for name, value in saved.items()
        if isinstance(value, (dict, set, list))
    }
    yield
    for name in [n for n in vars(pa) if n not in saved and not n.startswith("__")]:
        if not isinstance(
            getattr(pa, name), (types.ModuleType, types.FunctionType, type)
        ):
            delattr(pa, name)
    _restore_input_state(input_state)
    for name, value in saved.items():
        setattr(pa, name, value)
        if name in contents:
            value.clear()
            if isinstance(value, dict):
                value.update(contents[name])
            elif isinstance(value, set):
                value.update(contents[name])
            else:
                value.extend(contents[name])


# ---------------------------------------------------------------------------
# Seams shared by base and candidate
# ---------------------------------------------------------------------------


def _operations():
    return pa.session_transport_bindings().http.operations


def _runtime_view():
    return pa.session_transport_bindings().http.runtime


async def _loop_callbacks(monkeypatch):
    """Capture the callbacks the runtime wires into the persistent loop."""

    captured: dict = {}
    parked = asyncio.Event()

    async def _fake_loop(**kwargs):
        captured.update(kwargs)
        await parked.wait()

    async def _no_completion(_task):
        return None

    monkeypatch.setattr(pa, "run_persistent_loop", _fake_loop)
    monkeypatch.setattr(pa, "_loop_completion_handler", _no_completion)
    monkeypatch.setattr(pa, "_loop_task", None)
    session = pa._session
    for name in (
        "llm_with_tools",
        "tools",
        "context_manager",
        "config",
        "system_prompt",
        "messages",
        "auxiliary_llm",
        "recall_store",
        "knowledge_store",
        "project_id",
        "project_ids",
        "tool_context",
        "memory_extraction_prompt",
        "memory_service",
        "thread_id",
    ):
        if not hasattr(session, name):
            setattr(session, name, MagicMock())
    assert pa._ensure_persistent_loop_started("characterization") is True
    for _ in range(5):
        await asyncio.sleep(0)
        if "callbacks" in captured:
            break
    return captured, parked


def _open_admission(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(pa, "_TERMINATION_SENTINEL_PATH", tmp_path / "terminating")
    monkeypatch.setattr(pa, "_termination_admission_fenced", False)
    monkeypatch.setattr(pa, "_termination_fence_reason", None)
    monkeypatch.setattr(pa, "_retirement_admission_identity", None)
    monkeypatch.setattr(pa, "_turn_event_open", False)
    monkeypatch.setattr(pa, "_tool_inflight", False)
    monkeypatch.setattr(pa, "_broadcast", MagicMock())
    monkeypatch.delenv("STATELESS_EXECUTOR", raising=False)


def _drain(queue: asyncio.Queue) -> list:
    items = []
    while not queue.empty():
        items.append(queue.get_nowait())
    return items


# ---------------------------------------------------------------------------
# Real PostgreSQL: the runtime's durable admission against the actual ledger
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def pinned_runtime(db, pg_dsn, monkeypatch, tmp_path):  # noqa: F811 - fixture
    """An attached pinned runtime whose session talks to real PostgreSQL."""

    _user_id, thread_id, agent_id = await _seed_pinned_thread(db)
    async with db.acquire() as conn:
        thread = await conn.fetchrow(
            "SELECT runtime_generation, runtime_attach_token FROM threads WHERE id=$1",
            thread_id,
        )
    agent_db = AgentPostgresDB(
        connection_string=pg_dsn,
        min_connections=1,
        max_connections=6,
    )
    await agent_db.connect()
    _open_admission(monkeypatch, tmp_path)
    _arrange_input_state(monkeypatch)
    monkeypatch.setenv("POD_UID", "pod-pinned")
    monkeypatch.setattr(pa._session_identity, "_thread_id", str(thread_id))
    monkeypatch.setattr(
        pa, "_orchestrator_client", SimpleNamespace(agent_id=str(agent_id))
    )
    monkeypatch.setattr(
        pa._session_identity, "_session_generation", str(thread["runtime_generation"])
    )
    monkeypatch.setattr(
        pa._session_identity, "_attach_token", str(thread["runtime_attach_token"])
    )
    monkeypatch.setattr(pa._session_identity, "_process_generation", str(uuid4()))
    monkeypatch.setattr(pa._session_identity, "_runtime_contract", False)
    monkeypatch.setattr(
        pa,
        "_session",
        SimpleNamespace(
            postgres_conn=agent_db,
            turn_count=0,
            protected_cloud_required=False,
        ),
    )
    try:
        yield SimpleNamespace(
            db=db, agent_db=agent_db, thread_id=thread_id, monkeypatch=monkeypatch
        )
    finally:
        await agent_db.close()


async def _delivery(db, delivery_id) -> dict:  # noqa: F811 - fixture name
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state, claim_generation, owner_runtime_generation "
            "FROM thread_input_deliveries WHERE delivery_id=$1",
            UUID(str(delivery_id)),
        )
    return dict(row) if row is not None else {}


async def _transcript_rows(db, thread_id) -> int:  # noqa: F811 - fixture name
    async with db.acquire() as conn:
        return int(
            await conn.fetchval(
                "SELECT count(*) FROM thread_messages WHERE thread_id=$1", thread_id
            )
        )


class _ProcessDeath(BaseException):
    """A controlled fault: the process stops between two statements."""


@pytest.mark.asyncio
async def test_commit_without_publication_is_reclaimed_once_by_a_new_process(
    pinned_runtime, monkeypatch
):
    agent_db = pinned_runtime.agent_db
    real_persist = agent_db.persist_pinned_input_delivery
    delivery_id = str(uuid4())

    async def _persist_then_die(**kwargs):
        await real_persist(**kwargs)
        raise _ProcessDeath

    monkeypatch.setattr(agent_db, "persist_pinned_input_delivery", _persist_then_die)
    with pytest.raises(_ProcessDeath):
        await _operations().accept_input(
            "committed, never queued", delivery_id=delivery_id
        )
    committed = await _delivery(pinned_runtime.db, delivery_id)
    assert committed["state"] == "owned"
    assert _drain(_input_view().queue) == []
    assert await _transcript_rows(pinned_runtime.db, pinned_runtime.thread_id) == 1

    monkeypatch.setattr(agent_db, "persist_pinned_input_delivery", real_persist)
    successor = _restart_input_process(monkeypatch)
    reclaimed = await _runtime_ops().reclaim()
    assert reclaimed == {(delivery_id, committed["claim_generation"] + 1)}
    after = await _delivery(pinned_runtime.db, delivery_id)
    assert after["state"] == "queued"
    assert str(after["owner_runtime_generation"]) == successor
    items = _drain(_input_view().queue)
    assert [(i["delivery_id"], i["claim_generation"]) for i in items] == [
        (delivery_id, committed["claim_generation"] + 1)
    ]

    # The client's retry of the lost acknowledgement is a duplicate: it does
    # not publish again and does not insert another transcript row.
    retry = await _operations().accept_input(
        "committed, never queued", delivery_id=delivery_id
    )
    assert retry.duplicate is True
    assert retry.enqueued is False
    assert _drain(_input_view().queue) == []
    assert await _transcript_rows(pinned_runtime.db, pinned_runtime.thread_id) == 1


@pytest.mark.asyncio
async def test_same_process_poll_publishes_a_committed_unqueued_claim_once(
    pinned_runtime, monkeypatch
):
    agent_db = pinned_runtime.agent_db
    real_mark = agent_db.mark_pinned_input_delivery_queued
    delivery_id = str(uuid4())
    failures = {"left": 1}

    async def _mark_fails_once(**kwargs):
        if failures["left"]:
            failures["left"] -= 1
            raise ConnectionError("publication step lost its connection")
        return await real_mark(**kwargs)

    monkeypatch.setattr(agent_db, "mark_pinned_input_delivery_queued", _mark_fails_once)
    with pytest.raises(ConnectionError):
        await _operations().accept_input("publish me later", delivery_id=delivery_id)
    assert (await _delivery(pinned_runtime.db, delivery_id))["state"] == "owned"
    assert _drain(_input_view().queue) == []

    first = await _runtime_ops().reclaim()
    second = await _runtime_ops().reclaim()
    claim = (await _delivery(pinned_runtime.db, delivery_id))["claim_generation"]
    assert first == {(delivery_id, claim)}
    assert second == set()
    assert (await _delivery(pinned_runtime.db, delivery_id))["state"] == "queued"
    assert len(_drain(_input_view().queue)) == 1


@pytest.mark.asyncio
async def test_concurrent_duplicate_admission_publishes_one_queue_item(
    pinned_runtime,
):
    delivery_id = str(uuid4())
    results = await asyncio.gather(
        *[
            _operations().accept_input("once only", delivery_id=delivery_id)
            for _ in range(4)
        ]
    )
    assert sum(1 for result in results if result.enqueued) == 1
    assert {result.delivery_state for result in results} == {"queued"}
    assert sum(1 for result in results if not result.duplicate) == 1
    assert len(_drain(_input_view().queue)) == 1
    assert await _transcript_rows(pinned_runtime.db, pinned_runtime.thread_id) == 1


@pytest.mark.asyncio
async def test_concurrent_publication_of_one_live_claim_queues_it_once(
    pinned_runtime,
):
    """Two publishers of the same claim both pass the DB CAS; one item."""

    delivery_id = str(uuid4())
    accepted = await _operations().accept_input("claim", delivery_id=delivery_id)
    assert _drain(_input_view().queue) and accepted.enqueued
    # A successor generation re-owns the row; two publishers race it outside
    # the reclaim lock (a same-process retry and the durable poll).
    _restart_input_process(pinned_runtime.monkeypatch)
    rows = await pinned_runtime.agent_db.claim_pending_pinned_input_deliveries(
        thread_id=str(pinned_runtime.thread_id),
        agent_id=pa._orchestrator_client.agent_id,
        pod_uid="pod-pinned",
        runtime_generation=pa._session_identity.process_generation,
        session_runtime_generation=pa._session_identity.session_generation,
        runtime_attach_token=pa._session_identity.attach_token,
    )
    row = dict(rows[0])
    assert row["claim_generation"] == accepted.claim_generation + 1
    results = await asyncio.gather(
        *[_runtime_ops().queue_claimed(dict(row)) for _ in range(3)]
    )
    assert sorted(results) == [False, False, True]
    assert len(_drain(_input_view().queue)) == 1
    assert _input_view().claims == frozenset(
        {(delivery_id, accepted.claim_generation + 1)}
    )


@pytest.mark.asyncio
async def test_new_generation_admits_while_the_old_claim_is_refused(
    pinned_runtime, monkeypatch, tmp_path
):
    delivery_id = str(uuid4())
    accepted = await _operations().accept_input(
        "queued in RAM", delivery_id=delivery_id
    )
    assert accepted.enqueued is True
    old_generation = accepted.claim_generation

    _restart_input_process(monkeypatch)
    reclaimed = await _runtime_ops().reclaim()
    assert reclaimed == {(delivery_id, old_generation + 1)}
    captured, parked = await _loop_callbacks(monkeypatch)
    callbacks = captured["callbacks"]
    try:
        assert (
            await callbacks.admit_input_delivery(delivery_id, old_generation, 1)
            is False
        )
        assert (await _delivery(pinned_runtime.db, delivery_id))["state"] == "queued"
        assert (
            await callbacks.admit_input_delivery(delivery_id, old_generation + 1, 1)
            is True
        )
        assert await callbacks.settle_input_delivery(delivery_id, old_generation + 1)
        assert (await _delivery(pinned_runtime.db, delivery_id))["state"] == "settled"
        assert (delivery_id, old_generation + 1) not in _input_view().claims
    finally:
        parked.set()
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_replaced_attach_token_is_read_at_admission_and_refused(
    pinned_runtime, monkeypatch
):
    delivery_id = str(uuid4())
    accepted = await _operations().accept_input("fenced", delivery_id=delivery_id)
    captured, parked = await _loop_callbacks(monkeypatch)
    callbacks = captured["callbacks"]
    try:
        # Identity changes after construction and after the claim; the admit
        # reads the current one and the database refuses the stale claim.
        monkeypatch.setattr(pa._session_identity, "_attach_token", str(uuid4()))
        assert (
            await callbacks.admit_input_delivery(
                delivery_id, accepted.claim_generation, 1
            )
            is False
        )
        assert (await _delivery(pinned_runtime.db, delivery_id))["state"] == "queued"
        monkeypatch.setattr(pa._session_identity, "_process_generation", None)
        with pytest.raises(DurableInputUnavailable):
            await _operations().accept_input("no identity", delivery_id=str(uuid4()))
    finally:
        parked.set()
        await asyncio.sleep(0)


# ---------------------------------------------------------------------------
# Stateless lease: the current handle, never a captured one
# ---------------------------------------------------------------------------


class _LeaseRecordingDB:
    def __init__(self) -> None:
        self.stateless: list[dict] = []
        self.pinned: list[dict] = []

    async def transition_stateless_input_delivery(self, **kwargs):
        self.stateless.append(kwargs)
        return True

    async def transition_pinned_input_delivery(self, **kwargs):
        self.pinned.append(kwargs)
        return True


@pytest.mark.asyncio
async def test_stateless_transitions_follow_lease_renewal_and_replacement(
    monkeypatch, tmp_path
):
    _open_admission(monkeypatch, tmp_path)
    _arrange_input_state(monkeypatch)
    recording = _LeaseRecordingDB()
    thread_id = str(uuid4())
    monkeypatch.setattr(pa._session_identity, "_thread_id", thread_id)
    monkeypatch.setattr(
        pa,
        "_session",
        SimpleNamespace(
            postgres_conn=recording, turn_count=3, protected_cloud_required=False
        ),
    )
    handle = LeaseHandle()
    handle.update(thread_id, 5, executor_id="exec-a", pod_uid="pod-a")
    token = current_lease.set(handle)
    try:
        transition = _runtime_ops().transition
        assert await transition("d1", 1, "admitted", turn_number=4)
        handle.update(thread_id, 6, executor_id="exec-a", pod_uid="pod-a")
        assert await transition("d1", 1, "settled")
        replacement = LeaseHandle()
        replacement.update(thread_id, 9, executor_id="exec-b", pod_uid="pod-b")
        current_lease.set(replacement)
        assert await transition("d2", 2, "deferred", reason="retry")
        foreign = LeaseHandle()
        foreign.update(str(uuid4()), 11, executor_id="exec-c", pod_uid="pod-c")
        current_lease.set(foreign)
        assert await transition("d3", 1, "admitted", turn_number=5) is False
        anonymous = LeaseHandle()
        anonymous.update(thread_id, 12, executor_id=None, pod_uid="pod-d")
        current_lease.set(anonymous)
        assert await transition("d4", 1, "admitted", turn_number=5) is False
    finally:
        current_lease.reset(token)

    assert [
        (call["lease_token"], call["executor_id"], call["pod_uid"], call["delivery_id"])
        for call in recording.stateless
    ] == [
        (5, "exec-a", "pod-a", "d1"),
        (6, "exec-a", "pod-a", "d1"),
        (9, "exec-b", "pod-b", "d2"),
    ]
    assert recording.pinned == []


# ---------------------------------------------------------------------------
# Interrupt target, replacement and the old turn's clear
# ---------------------------------------------------------------------------


def _interrupt_runtime(monkeypatch, tmp_path, *, turn: int, tool: bool = False):
    _open_admission(monkeypatch, tmp_path)
    _arrange_input_state(monkeypatch)
    monkeypatch.setattr(pa, "_session", SimpleNamespace(turn_count=turn))
    monkeypatch.setattr(pa, "_turn_event_open", True)
    monkeypatch.setattr(pa, "_tool_inflight", tool)


def test_interrupt_belongs_to_one_turn_and_an_old_clear_cannot_erase_a_newer_one(
    monkeypatch, tmp_path
):
    _interrupt_runtime(monkeypatch, tmp_path, turn=7)
    signal = _operations().signal_interrupt
    clear = _runtime_ops().clear_interrupt

    assert signal(6) is None
    assert (_input_view().mode, _input_view().target) == (None, None)
    assert signal(7) == "hard"
    assert _input_view().hard_event.is_set()

    # Turn 8 starts; turn 7's interrupt was never consumed. A late request for
    # 7 is refused, and 8 gets its own.
    pa._session.turn_count = 8
    assert signal(7) is None
    assert (_input_view().mode, _input_view().target) == ("hard", 7)
    assert signal(8) == "hard"
    assert (_input_view().mode, _input_view().target) == ("hard", 8)

    # Turn 7's terminal edge runs late: it must not erase turn 8's interrupt.
    assert clear(target_turn_id=7) is False
    assert (_input_view().mode, _input_view().target) == ("hard", 8)
    assert _input_view().hard_event.is_set()
    assert clear(target_turn_id=8) is True
    assert (_input_view().mode, _input_view().target) == (None, None)
    assert not _input_view().hard_event.is_set()


def test_interrupt_refused_when_no_turn_is_open_and_graceful_during_tools(
    monkeypatch, tmp_path
):
    _interrupt_runtime(monkeypatch, tmp_path, turn=3)
    monkeypatch.setattr(pa, "_turn_event_open", False)
    signal = _operations().signal_interrupt
    assert signal(3) is None
    assert _input_view().mode is None

    monkeypatch.setattr(pa, "_turn_event_open", True)
    monkeypatch.setattr(pa, "_tool_inflight", True)
    assert signal(3) == "graceful"
    assert not _input_view().hard_event.is_set()
    assert _runtime_ops().clear_interrupt(target_turn_id=3) is True

    # The stateless executor's lease-loss abort forces graceful mode even when
    # no tool is running; it must never arm the hard-cancel event.
    monkeypatch.setattr(pa, "_tool_inflight", False)
    assert signal(3, force_graceful=True) == "graceful"
    assert (_input_view().mode, _input_view().target) == ("graceful", 3)
    assert not _input_view().hard_event.is_set()


@pytest.mark.asyncio
async def test_loop_check_consumes_its_own_turn_and_discards_a_stale_target(
    monkeypatch, tmp_path
):
    _interrupt_runtime(monkeypatch, tmp_path, turn=4)
    monkeypatch.setattr(
        pa,
        "_session",
        SimpleNamespace(
            turn_count=4,
            llm_with_tools=MagicMock(),
            protected_cloud_required=False,
            postgres_conn=None,
        ),
    )
    captured, parked = await _loop_callbacks(monkeypatch)
    callbacks = captured["callbacks"]
    try:
        assert callbacks.hard_interrupt_event is _input_view().hard_event
        assert _operations().signal_interrupt(4) == "hard"
        assert callbacks.check_interrupt() == "hard"
        assert callbacks.check_interrupt() is None
        assert not _input_view().hard_event.is_set()

        assert _operations().signal_interrupt(4) == "hard"
        pa._session.turn_count = 5
        assert callbacks.check_interrupt() is None
        assert (_input_view().mode, _input_view().target) == (None, None)
        assert not _input_view().hard_event.is_set()
    finally:
        parked.set()
        await asyncio.sleep(0)


# ---------------------------------------------------------------------------
# Queue, claims and interrupt state across attach → teardown → next attach
# ---------------------------------------------------------------------------


class _AttachSession:
    def __init__(self, *args, **kwargs):
        self.cloud_mount_manager = None
        self.cloud_mount_error = None
        self.overlay_mount_manager = None
        self.workspace_manager = SimpleNamespace(path="/workspace", backend=MagicMock())
        self.workspace_sync = None
        self.postgres_conn = None
        self.tool_context = None
        self.llm_with_tools = MagicMock()
        self.protected_cloud_required = False
        self.turn_count = 0
        self.memory_service = None
        self.config = MagicMock()
        self.cleanup = AsyncMock()
        self.retire_shell_owner = MagicMock()
        self.recovery_views: list = []

    async def setup(self, **kwargs):
        return None

    async def recover_subagents(self):
        self.recovery_views.append(_input_view())

    async def quiesce_background_tasks(self):
        return None

    async def quiesce_subagents(self, *args, **kwargs):
        return None

    async def resume_subagents(self, *args, **kwargs):
        return None


async def _attach(monkeypatch, thread_id: str) -> _AttachSession:
    fake_agent = SimpleNamespace(
        config=object(),
        _tactical_llm=None,
        _llm=object(),
        _auxiliary_llm=object(),
        postgres_conn=None,
        vector_conn=None,
    )
    workspace = {"remote": {"host": "10.42.0.10"}}
    monkeypatch.setattr(pa, "_agent", fake_agent)
    monkeypatch.setattr(
        pa,
        "_orchestrator_client",
        SimpleNamespace(
            agent_id=None, get_thread_workspace=AsyncMock(return_value=workspace)
        ),
    )
    monkeypatch.setattr(pa, "PersistentSession", _AttachSession)
    monkeypatch.setattr(pa, "_poll_workspace_ready", AsyncMock(return_value=workspace))
    monkeypatch.setattr(pa, "_restore_session_messages", AsyncMock())
    monkeypatch.setattr(pa, "_update_thread_status", AsyncMock(return_value=True))
    monkeypatch.setattr(pa, "_start_watchdogs", MagicMock())
    monkeypatch.setattr(pa, "_build_sync_coordinator", MagicMock())
    monkeypatch.setattr(pa, "_session", None)
    monkeypatch.setattr(pa._session_identity, "_thread_id", None)
    await pa._attach_session(thread_id)
    return pa._session


@pytest.mark.asyncio
async def test_attach_teardown_attach_never_carries_input_state(monkeypatch, tmp_path):
    _open_admission(monkeypatch, tmp_path)
    _arrange_input_state(monkeypatch, queue_open=False)
    monkeypatch.setattr(pa, "_loop_task", None)
    monkeypatch.setattr(pa, "_event_writer", None)
    monkeypatch.setattr(pa, "_terminating", False)
    monkeypatch.setattr(pa, "_termination_task", None)
    monkeypatch.setattr(pa, "_max_sessions_per_process", 0)
    monkeypatch.setattr(pa, "_subscribers", {})

    first = await _attach(monkeypatch, "thread-one")
    recovery = first.recovery_views[0]
    assert recovery.queue is None  # published only after subagent recovery
    one = _input_view()
    first_generation = pa._session_identity.process_generation
    assert first_generation
    assert isinstance(one.queue, asyncio.Queue)
    assert one.claims == frozenset()
    assert (one.mode, one.target) == (None, None)
    assert isinstance(one.hard_event, asyncio.Event) and not one.hard_event.is_set()
    assert _runtime_view().input_queue() is one.queue

    one.queue.put_nowait({"content": "left over", "id": "m1"})
    _note_queued_claim(("left-over", 1))
    assert ("left-over", 1) in _input_view().claims
    monkeypatch.setattr(pa, "_turn_event_open", True)
    pa._session.turn_count = 2
    assert _operations().signal_interrupt(2) == "hard"
    monkeypatch.setattr(pa, "_turn_event_open", False)

    await pa._terminate_session("characterization", mark_thread=False)
    gone = _input_view()
    assert pa._session_identity.process_generation is None
    assert gone.queue is None
    assert gone.claims == frozenset()
    assert (gone.mode, gone.target, gone.hard_event) == (None, None, None)
    assert _runtime_view().input_queue() is None
    assert pa._session_ready() is False

    await _attach(monkeypatch, "thread-two")
    two = _input_view()
    assert pa._session_identity.process_generation not in (None, first_generation)
    assert two.queue is not one.queue and two.queue.empty()
    assert two.claims == frozenset()
    assert (two.mode, two.target) == (None, None)
    assert two.hard_event is not one.hard_event and not two.hard_event.is_set()
    assert two.reclaim_lock is not one.reclaim_lock
    await pa._terminate_session("characterization", mark_thread=False)


# ---------------------------------------------------------------------------
# The parked window and termination wake
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parked_window_is_exactly_the_input_wait_and_termination_wakes_it(
    monkeypatch, tmp_path
):
    _open_admission(monkeypatch, tmp_path)
    _arrange_input_state(monkeypatch)
    monkeypatch.setenv("STATELESS_EXECUTOR", "1")  # plain queue wait, no DB poll
    monkeypatch.setattr(pa, "_officer_cfg", lambda: None)
    monkeypatch.setattr(
        pa,
        "_session",
        SimpleNamespace(
            turn_count=0,
            llm_with_tools=MagicMock(),
            protected_cloud_required=False,
            postgres_conn=None,
            tool_context=None,
            config=SimpleNamespace(
                interactive=SimpleNamespace(idle_timeout_minutes=0), headless=None
            ),
        ),
    )
    captured, parked = await _loop_callbacks(monkeypatch)
    callbacks = captured["callbacks"]
    try:
        assert pa._turn_in_flight() is True
        getter = asyncio.create_task(callbacks.get_user_input())
        for _ in range(20):
            await asyncio.sleep(0)
            if _input_view().awaiting:
                break
        assert _input_view().awaiting is True
        assert pa._turn_in_flight() is False
        assert pa._session_parked() is True
        pa._broadcast.assert_any_call("ready", {})

        assert pa.activate_termination_admission_fence("characterization") is True
        item = await asyncio.wait_for(getter, timeout=2)
        assert item == pa._TERMINATION_QUEUE_SENTINEL
        assert _input_view().awaiting is False
        assert pa._turn_in_flight() is True
    finally:
        parked.set()
        await asyncio.sleep(0)


# ---------------------------------------------------------------------------
# Protected-cloud reclaim task: single flight, tracked, cancelled at teardown
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_protected_reclaim_is_single_flight_and_joined_with_side_tasks(
    monkeypatch, tmp_path
):
    _open_admission(monkeypatch, tmp_path)
    _arrange_input_state(monkeypatch)
    ready = {"value": False}
    session = SimpleNamespace(
        postgres_conn=MagicMock(),
        turn_count=0,
        protected_cloud_required=True,
        protected_cloud_ready=lambda: ready["value"],
    )
    monkeypatch.setattr(pa, "_session", session)
    monkeypatch.setattr(pa._session_identity, "_thread_id", "thread-protected")
    monkeypatch.setattr(pa, "_session_side_tasks", set())
    reclaims = AsyncMock(return_value=set())
    _replace_reclaim(monkeypatch, reclaims)

    _runtime_ops().schedule_protected_reclaim()
    task = _input_view().reclaim_task
    assert task is not None and task in pa._session_side_tasks
    _runtime_ops().schedule_protected_reclaim()
    assert _input_view().reclaim_task is task
    await asyncio.sleep(0)  # the task is now parked in its heal poll

    await pa._quiesce_session_side_tasks()
    assert task.cancelled()
    assert _input_view().reclaim_task is None
    reclaims.assert_not_awaited()


@pytest.mark.asyncio
async def test_protected_reclaim_runs_once_when_the_same_life_heals(
    monkeypatch, tmp_path
):
    _open_admission(monkeypatch, tmp_path)
    _arrange_input_state(monkeypatch)
    ready = {"value": False}
    session = SimpleNamespace(
        postgres_conn=MagicMock(),
        turn_count=0,
        protected_cloud_required=True,
        protected_cloud_ready=lambda: ready["value"],
    )
    monkeypatch.setattr(pa, "_session", session)
    monkeypatch.setattr(pa._session_identity, "_thread_id", "thread-heals")
    monkeypatch.setattr(pa, "_session_side_tasks", set())
    reclaim = AsyncMock(return_value=set())
    _replace_reclaim(monkeypatch, reclaim)
    monkeypatch.setattr(pa.asyncio, "sleep", _fast_sleep)

    _runtime_ops().schedule_protected_reclaim()
    task = _input_view().reclaim_task
    await _fast_sleep(0)
    ready["value"] = True
    await asyncio.wait_for(task, timeout=2)
    assert reclaim.await_count == 1
    assert _input_view().reclaim_task is None

    # A different session life: the scheduled task exits without reclaiming.
    ready["value"] = False
    _runtime_ops().schedule_protected_reclaim()
    replaced = _input_view().reclaim_task
    monkeypatch.setattr(pa, "_session", SimpleNamespace(**vars(session)))
    ready["value"] = True
    await asyncio.wait_for(replaced, timeout=2)
    assert reclaim.await_count == 1
    assert _input_view().reclaim_task is None


_REAL_SLEEP = asyncio.sleep


async def _fast_sleep(_delay, *args, **kwargs):
    await _REAL_SLEEP(0)
