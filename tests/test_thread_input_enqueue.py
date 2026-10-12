"""Stateless-lane enqueue-on-input (stateless_agents.md §5.3.1, M4).

POST /api/persistent/threads/{id}/input on a thread with
``execution_lane='stateless'`` must: persist the human message row
(indistinguishable from the agent's accept-time persist), advance the
run_queue input watermark, and admit the unit — all in ONE transaction on ONE
connection — then answer with the pinned path's response shape (accepted /
turn_id top-level, "queue" object instead of "agent"). The pinned path stays
byte-identical (legacy forward, per-turn lock); the per-turn in-process lock
is skipped ONLY for the stateless lane.

House pattern: direct coroutine calls on the owning router
(``orchestrator.routers.thread_transport``) with one application's transport
dependencies built from the fakes below. A patch lands on the module that looks
the name up at call time: the router module for its own globals, the
``pinned_forwarding`` service module for the pinned forwarding functions (the
router calls them as ``pinned_forwarding.<fn>``), and the per-application
``ThreadTurnLocks`` instance for the turn locks.
"""

import uuid
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from orchestrator.routers import thread_transport
from orchestrator.schemas.thread_transport import ThreadInputRequest
from orchestrator.services import pinned_forwarding
from orchestrator.services.stateless_input_admission import StatelessInputDependencies
from orchestrator.services.thread_turn_locks import ThreadTurnLocks
from shared.pinned_session_identity import PinnedSessionBinding
from orchestrator.application import preparation as preparation_composition
from orchestrator.application import transport as transport_composition
from orchestrator.services import container_provisioner as container_provisioner_module
from orchestrator.services import session_provisioner as session_provisioner_module
from orchestrator.services import (
    stateless_workspace_scheduler as stateless_workspace_scheduler_module,
)
from orchestrator.services import workspace_suspension as workspace_suspension_module

THREAD_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
USER = {"id": "user-1", "is_admin": False}
_USE_DB_THREAD = object()


def _pinned_binding() -> PinnedSessionBinding:
    return PinnedSessionBinding(
        thread_id=THREAD_ID,
        runtime_generation="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        agent_id="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        runtime_attach_token="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        agent_hostname="persistent-aaaaaaaaaaaa",
        pod_namespace="srw",
        pod_uid="pod-uid-a",
        pod_ip="10.0.0.9",
        pod_port=8001,
        agent_status="session",
    )


class _FakeTxn:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        self._conn.txn_depth += 1
        self._conn.txn_enters += 1
        return self

    async def __aexit__(self, *exc):
        self._conn.txn_depth -= 1
        return False


class FakeConn:
    """Scripted asyncpg connection: records (kind, query, args, txn_depth)."""

    def __init__(
        self,
        *,
        message_seq=41,
        admit_state="queued",
        watermarks=None,
        locked_thread=_USE_DB_THREAD,
    ):
        self.calls = []
        self.txn_depth = 0
        self.txn_enters = 0
        self._message_seq = message_seq
        self._admit_state = admit_state
        self._locked_thread = locked_thread
        self._watermarks = watermarks or {
            "state": "queued",
            "input_seq": message_seq,
            "consumed_seq": None,
            "control_input_seq": 0,
            "control_consumed_seq": 0,
        }

    def transaction(self):
        return _FakeTxn(self)

    async def fetchval(self, q, *a):
        self.calls.append(("fetchval", q, a, self.txn_depth))
        if "INSERT INTO thread_messages" in q:
            return self._message_seq
        if "status = 'created'" in q and "status = 'suspended'" in q:
            return THREAD_ID
        if "run_queue" in q:
            # record_input_seq's one-statement CTE
            return self._admit_state
        return None

    async def execute(self, q, *a):
        self.calls.append(("execute", q, a, self.txn_depth))

    async def fetchrow(self, q, *a):
        self.calls.append(("fetchrow", q, a, self.txn_depth))
        if "FROM threads" in q and "FOR UPDATE" in q:
            assert self.txn_depth == 1
            return self._locked_thread
        if "FROM run_queue" in q:
            return {
                "park_reason": None,
                "parked_at": None,
                "last_error": None,
                "attempts_since_completion": 0,
                "max_attempts": 3,
                "attach_failures": 0,
                "run_after": None,
                **self._watermarks,
            }
        return None


class FakeDB:
    def __init__(self, thread, conn):
        self._thread = thread
        self._conn = conn
        if conn._locked_thread is _USE_DB_THREAD:
            conn._locked_thread = thread
        self.get_thread_calls = 0

    async def get_thread(self, tid):
        self.get_thread_calls += 1
        return self._thread

    def acquire(self):
        conn = self._conn

        class _A:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return False

        return _A()


def _stateless_thread(**over):
    thread = {
        "id": THREAD_ID,
        "user_id": "user-1",
        "execution_lane": "stateless",
        "agent_id": None,
        "total_turns": 5,
        "status": "active",
        "metadata": {"config_override": {"workspace": {"backend": "virtual"}}},
    }
    thread.update(over)
    return thread


def _k8s_sandbox_metadata(*, status="ready"):
    generation = "11111111-1111-4111-8111-111111111111"
    workspace = {"status": status, "provisioner": "k8s"}
    metadata = {
        "config_override": {"workspace": {"backend": "sandbox"}},
        "workspace_container": workspace,
    }
    if status == "ready":
        workspace.update(
            {
                "pod_ip": "10.42.0.25",
                "port": 30022,
                "pod_name": "ws-thread-aaaaaaaaaaaa",
                "namespace": "agent-workspaces",
                "_canvas_workspace_generation": generation,
                "_runtime_incarnation": "22222222-2222-4222-8222-222222222222",
            }
        )
        metadata["_workspace_binding"] = {
            "generation": generation,
            "kind": "remote",
            "backing_id": "k8s-pvc:agent-workspaces:pvc-uid",
            "ssh_host_key_fingerprint": "SHA256:trusted",
        }
    return metadata


def _unexpected(what: str) -> MagicMock:
    return MagicMock(side_effect=AssertionError(f"unexpected {what}"))


def _deps(
    db,
    *,
    schedule=None,
    stateless_input=None,
    turn_locks=None,
    owner=None,
):
    """One application's transport collaborators, built from this file's fakes.

    ``require_approved_user`` resolves the fixed caller (the ``/input`` gate);
    the owner gate is reached only by ``/interrupt`` and must be supplied by
    the test that reaches it. An unpatched workspace-ensure scheduler fails
    loudly if a case reaches it without saying so.
    """

    async def _fake_user(request, _db):
        return dict(USER)

    if stateless_input is None:
        stateless_input = StatelessInputDependencies(
            store=db,
            schedule_stateless_workspace_ensure=(
                schedule if schedule is not None else _unexpected("workspace ensure")
            ),
        )
    return thread_transport.ThreadTransportDependencies(
        store=db,
        require_thread_owner=(
            owner
            if owner is not None
            else AsyncMock(side_effect=AssertionError("unexpected owner gate"))
        ),
        require_approved_user=_fake_user,
        forwarding=pinned_forwarding.PinnedForwardingDependencies(
            store=db,
            workspace_suspension=SimpleNamespace(is_enabled=False),
            protected_cloud_delivery_state=AsyncMock(
                side_effect=AssertionError("unexpected protected-cloud read")
            ),
        ),
        stateless_input=stateless_input,
        turn_locks=turn_locks if turn_locks is not None else ThreadTurnLocks(),
    )


@pytest.mark.asyncio
async def test_stateless_input_single_transaction_and_response_parity():
    """Insert + threads bump + record_input_seq share one conn/transaction;
    the watermark read happens post-commit; response carries accepted/turn_id
    top-level with the queue object nested."""
    conn = FakeConn(message_seq=41)
    db = FakeDB(_stateless_thread(), conn)
    schedule = MagicMock()

    out = await thread_transport.thread_input(
        THREAD_ID,
        ThreadInputRequest(content="hello queue"),
        MagicMock(),
        dependencies=_deps(db, schedule=schedule),
    )

    # --- transaction shape: exactly one txn; all three writes inside it ---
    assert conn.txn_enters == 1
    insert = next(c for c in conn.calls if "INSERT INTO thread_messages" in c[1])
    bump = next(c for c in conn.calls if "GREATEST(total_turns" in c[1])
    admit = next(c for c in conn.calls if c[0] == "fetchval" and "run_queue" in c[1])
    watermark = next(
        c for c in conn.calls if c[0] == "fetchrow" and "FROM run_queue" in c[1]
    )
    assert insert[3] == 1, "message insert must run inside the transaction"
    assert bump[3] == 1, "threads bump must run inside the transaction"
    assert admit[3] == 1, "record_input_seq must share the SAME transaction"
    assert watermark[3] == 0, "queue_depth read happens after commit"
    order = [conn.calls.index(c) for c in (insert, bump, admit, watermark)]
    assert order == sorted(order), "insert -> bump -> admit -> watermark order"

    # --- message row mirrors the agent's accept-time persist ---
    row_id, thread_id_arg, content, turn_number = insert[2]
    assert thread_id_arg == THREAD_ID
    assert content == "hello queue"
    assert turn_number == 6  # total_turns + 1
    assert "'human'" in insert[1]  # role literal in the mirrored insert
    # row id is the agent's own uuid5 coercion of the minted msg_ id
    from agent.database.postgres_db import _coerce_row_id

    raw_msg_id = out["queue"]["message_id"]
    assert raw_msg_id.startswith("msg_") and len(raw_msg_id) == 4 + 24
    assert row_id == _coerce_row_id(raw_msg_id)
    # threads bump args mirror save_thread_message's
    assert bump[2] == (THREAD_ID, 6)

    # --- admission args: unit_id=thread_id, kind, watermark, fair_key ---
    a = admit[2]
    assert a[0] == uuid.UUID(THREAD_ID)
    assert a[1] == "session_turn"
    assert a[2] == 41
    assert a[3] == "user-1"

    # --- response parity ---
    assert out["accepted"] is True
    assert out["turn_id"] == 6
    assert "agent" not in out
    assert out["queue"]["state"] == "queued"
    assert out["queue"]["queue_depth"] == 1
    assert out["queue"]["input_seq"] == 41
    schedule.assert_not_called()


@pytest.mark.asyncio
async def test_stateless_input_skips_per_turn_lock(monkeypatch):
    """The in-process per-turn lock registry is replica-unsafe and unnecessary
    on the queue lane — it must not even be consulted."""
    conn = FakeConn()
    db = FakeDB(_stateless_thread(), conn)
    turn_locks = ThreadTurnLocks()
    lock_spy = MagicMock(side_effect=AssertionError("lock must not be used"))
    monkeypatch.setattr(turn_locks, "ensure", lock_spy)
    forward_spy = AsyncMock(side_effect=AssertionError("no agent forward"))
    monkeypatch.setattr(pinned_forwarding, "forward_to_agent", forward_spy)

    out = await thread_transport.thread_input(
        THREAD_ID,
        ThreadInputRequest(content="x"),
        MagicMock(),
        dependencies=_deps(db, turn_locks=turn_locks),
    )
    assert out["accepted"] is True
    lock_spy.assert_not_called()
    forward_spy.assert_not_awaited()
    assert turn_locks.locks == {} and turn_locks.inflight == {}


@pytest.mark.asyncio
async def test_stateless_input_rejects_empty_content():
    conn = FakeConn()
    db = FakeDB(_stateless_thread(), conn)

    with pytest.raises(HTTPException) as exc:
        await thread_transport.thread_input(
            THREAD_ID,
            ThreadInputRequest(content=""),
            MagicMock(),
            dependencies=_deps(db),
        )
    assert exc.value.status_code == 400
    assert conn.calls == []  # nothing persisted


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["vm", "future-tier", None])
async def test_stateless_input_rejects_unsupported_workspace_before_writes(backend):
    metadata = (
        {"config_override": {"workspace": {"backend": backend}}}
        if backend is not None
        else {}
    )
    conn = FakeConn()
    db = FakeDB(_stateless_thread(metadata=metadata), conn)

    with pytest.raises(HTTPException) as exc:
        await thread_transport.thread_input(
            THREAD_ID,
            ThreadInputRequest(content="must not queue"),
            MagicMock(),
            dependencies=_deps(db),
        )

    assert exc.value.status_code == 409
    assert "virtual/none" in str(exc.value.detail)
    assert conn.calls == []


@pytest.mark.asyncio
async def test_stateless_input_accepts_attested_k8s_sandbox():
    conn = FakeConn()
    db = FakeDB(
        _stateless_thread(metadata=_k8s_sandbox_metadata()),
        conn,
    )
    schedule = MagicMock()

    def _after_commit(thread_id):
        assert thread_id == THREAD_ID
        assert conn.txn_depth == 0

    schedule.side_effect = _after_commit

    out = await thread_transport.thread_input(
        THREAD_ID,
        ThreadInputRequest(content="sandbox turn"),
        MagicMock(),
        dependencies=_deps(db, schedule=schedule),
    )

    assert out["accepted"] is True
    assert conn.txn_enters == 1
    schedule.assert_called_once_with(THREAD_ID)


@pytest.mark.asyncio
async def test_awaiting_user_sandbox_input_commits_before_workspace_ensure():
    """A waiting thread is admitted durably, then wakes the physical workspace."""
    thread = _stateless_thread(status="awaiting_user", metadata=_k8s_sandbox_metadata())
    conn = FakeConn()
    db = FakeDB(thread, conn)
    observed_depths = []
    schedule = MagicMock(
        side_effect=lambda _thread_id: observed_depths.append(conn.txn_depth)
    )

    out = await thread_transport.thread_input(
        THREAD_ID,
        ThreadInputRequest(content="continue from approval"),
        MagicMock(),
        dependencies=_deps(db, schedule=schedule),
    )

    assert out["accepted"] is True
    assert observed_depths == [0]
    admit = next(c for c in conn.calls if c[0] == "fetchval" and "run_queue" in c[1])
    assert admit[3] == 1


@pytest.mark.asyncio
async def test_stateless_input_rechecks_locked_workspace_before_any_write():
    """A preflight-valid row cannot authorize Docker evidence under lock."""
    preflight = _stateless_thread()
    locked = _stateless_thread(
        metadata={
            "config_override": {"workspace": {"backend": "sandbox"}},
            "workspace_container": {"status": "ready", "provisioner": "docker"},
        }
    )
    conn = FakeConn(locked_thread=locked)
    db = FakeDB(preflight, conn)

    with pytest.raises(HTTPException) as exc:
        await thread_transport.thread_input(
            THREAD_ID,
            ThreadInputRequest(content="must not race into the queue"),
            MagicMock(),
            dependencies=_deps(db),
        )

    assert exc.value.status_code == 409
    assert conn.txn_enters == 1
    assert len(conn.calls) == 1
    locked_read = conn.calls[0]
    assert locked_read[0] == "fetchrow"
    assert "FROM threads" in locked_read[1]
    assert "FOR UPDATE" in locked_read[1]
    assert locked_read[3] == 1


@pytest.mark.asyncio
async def test_stateless_input_rechecks_locked_protected_cloud_before_any_write():
    """A protected marker landing after preflight cannot enter run_queue."""
    preflight = _stateless_thread()
    locked_metadata = _k8s_sandbox_metadata()
    locked_metadata["protected_cloud"] = True
    conn = FakeConn(
        locked_thread=_stateless_thread(metadata=locked_metadata),
    )
    db = FakeDB(preflight, conn)

    with pytest.raises(HTTPException) as exc:
        await thread_transport.thread_input(
            THREAD_ID,
            ThreadInputRequest(content="must remain pinned"),
            MagicMock(),
            dependencies=_deps(db),
        )

    assert exc.value.status_code == 409
    assert conn.txn_enters == 1
    assert len(conn.calls) == 1
    locked_read = conn.calls[0]
    assert locked_read[0] == "fetchrow"
    assert "FROM threads" in locked_read[1]
    assert "FOR UPDATE" in locked_read[1]
    assert locked_read[3] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("expected_revision", "current_revision"),
    [(None, 3), (2, 3), (4, 3)],
)
async def test_stateless_input_refuses_a_stale_conversation_revision_before_writes(
    expected_revision, current_revision
):
    """The locked row's conversation_revision is authoritative: a client that
    rendered an older (or no) revision is refused before any message, bump or
    queue write lands."""
    conn = FakeConn(
        locked_thread=_stateless_thread(conversation_revision=current_revision)
    )
    db = FakeDB(_stateless_thread(), conn)

    with pytest.raises(HTTPException) as exc:
        await thread_transport.thread_input(
            THREAD_ID,
            ThreadInputRequest(
                content="rendered from a stale view",
                expected_conversation_revision=expected_revision,
            ),
            MagicMock(),
            dependencies=_deps(db),
        )

    assert exc.value.status_code == 409
    assert exc.value.detail == {
        "code": "session_view_stale",
        "reason": "conversation_revision_changed",
        "conversation_revision": current_revision,
    }
    assert conn.txn_enters == 1
    assert len(conn.calls) == 1
    locked_read = conn.calls[0]
    assert locked_read[0] == "fetchrow"
    assert "FOR UPDATE" in locked_read[1]
    assert locked_read[3] == 1


@pytest.mark.asyncio
async def test_stateless_input_admits_on_the_current_conversation_revision():
    conn = FakeConn(locked_thread=_stateless_thread(conversation_revision=3))
    db = FakeDB(_stateless_thread(), conn)

    out = await thread_transport.thread_input(
        THREAD_ID,
        ThreadInputRequest(content="current view", expected_conversation_revision=3),
        MagicMock(),
        dependencies=_deps(db),
    )

    assert out["accepted"] is True
    assert out["conversation_revision"] == 3
    admit = next(c for c in conn.calls if c[0] == "fetchval" and "run_queue" in c[1])
    assert admit[3] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["enabled", "conference"])
@pytest.mark.parametrize("value", [None, 0, "", [], {}, "yes", 1])
async def test_stateless_input_refuses_malformed_session_class_before_writes(
    field,
    value,
):
    metadata = {
        "config_override": {
            "workspace": {"backend": "virtual"},
            "officer": {field: value},
        }
    }
    conn = FakeConn()
    db = FakeDB(_stateless_thread(metadata=metadata), conn)

    with pytest.raises(HTTPException) as exc:
        await thread_transport.thread_input(
            THREAD_ID,
            ThreadInputRequest(content="must remain pinned"),
            MagicMock(),
            dependencies=_deps(db),
        )

    assert exc.value.status_code == 409
    assert conn.calls == []


@pytest.mark.asyncio
async def test_ended_stateless_input_requires_explicit_resume_before_writes():
    conn = FakeConn()
    db = FakeDB(_stateless_thread(status="ended"), conn)

    with pytest.raises(HTTPException, match="status=ended") as exc:
        await thread_transport.thread_input(
            THREAD_ID,
            ThreadInputRequest(content="must resume first"),
            MagicMock(),
            dependencies=_deps(db),
        )

    assert exc.value.status_code == 409
    assert conn.txn_enters == 1
    assert len(conn.calls) == 1


@pytest.mark.asyncio
async def test_suspended_sandbox_input_commits_then_schedules_workspace_restore(
    monkeypatch,
):
    """Drives the application's own stateless-input composition: its
    post-commit scheduler is main's R1.B05 bridge, which builds the ensure
    call from main's store, provisioner and suspension service at call time
    (hence the ``postgres_db`` patch on main is consulted, not inert)."""
    import asyncio

    from orchestrator import main as orch_main

    thread = _stateless_thread(
        status="suspended",
        metadata=_k8s_sandbox_metadata(status="suspended"),
    )
    conn = FakeConn()
    db = FakeDB(thread, conn)
    monkeypatch.setattr(orch_main.app.state.resources, "postgres_db", db)
    ensure_workspace = AsyncMock()
    monkeypatch.setattr(
        session_provisioner_module, "ensure_session_workspace", ensure_workspace
    )
    orch_main.app.state.resources.stateless_workspace_ensure_registry.discard(THREAD_ID)
    stateless_input = transport_composition.stateless_input_dependencies(
        orch_main.app.state.resources
    )
    assert stateless_input.store is db

    out = await thread_transport.thread_input(
        THREAD_ID,
        ThreadInputRequest(content="wake and continue"),
        MagicMock(),
        dependencies=_deps(db, stateless_input=stateless_input),
    )
    await asyncio.sleep(0)

    assert out["accepted"] is True
    wake = next(
        c
        for c in conn.calls
        if c[0] == "fetchval"
        and "status = 'created'" in c[1]
        and "status = 'suspended'" in c[1]
    )
    assert wake[3] == 1
    assert conn.calls.index(wake) < next(
        index
        for index, call in enumerate(conn.calls)
        if call[0] == "fetchval" and "INSERT INTO thread_messages" in call[1]
    )
    ensure_workspace.assert_awaited_once_with(
        THREAD_ID,
        db=db,
        provisioner=container_provisioner_module.container_provisioner,
        suspension=workspace_suspension_module.workspace_suspension_service,
    )
    admit = next(c for c in conn.calls if c[0] == "fetchval" and "run_queue" in c[1])
    assert admit[3] == 1


@pytest.mark.asyncio
async def test_stateless_input_accepts_none_workspace():
    conn = FakeConn()
    db = FakeDB(
        _stateless_thread(
            metadata={"config_override": {"workspace": {"backend": "none"}}}
        ),
        conn,
    )
    schedule = MagicMock()

    out = await thread_transport.thread_input(
        THREAD_ID,
        ThreadInputRequest(content="lite"),
        MagicMock(),
        dependencies=_deps(db, schedule=schedule),
    )

    assert out["accepted"] is True
    assert conn.txn_enters == 1
    schedule.assert_not_called()


@pytest.mark.asyncio
async def test_stateless_workspace_ensure_scheduler_is_single_flight(monkeypatch):
    import asyncio

    from orchestrator import main as orch_main

    started = asyncio.Event()
    release = asyncio.Event()
    ensure = AsyncMock()

    async def _ensure(*_args, **_kwargs):
        started.set()
        await release.wait()

    ensure.side_effect = _ensure
    monkeypatch.setattr(session_provisioner_module, "ensure_session_workspace", ensure)
    # R1.B05 moved the module dict into an application-owned registry; the
    # single-flight property under test is unchanged.
    orch_main.app.state.resources.stateless_workspace_ensure_registry.discard(THREAD_ID)

    first = stateless_workspace_scheduler_module.schedule_stateless_workspace_ensure(
        THREAD_ID,
        dependencies=preparation_composition.stateless_workspace_schedule_dependencies(
            orch_main.app.state.resources
        ),
    )
    await started.wait()
    second = stateless_workspace_scheduler_module.schedule_stateless_workspace_ensure(
        THREAD_ID,
        dependencies=preparation_composition.stateless_workspace_schedule_dependencies(
            orch_main.app.state.resources
        ),
    )

    assert second is first
    ensure.assert_awaited_once()
    release.set()
    await first
    await asyncio.sleep(0)
    assert (
        orch_main.app.state.resources.stateless_workspace_ensure_registry.get(THREAD_ID)
        is None
    )


@pytest.mark.asyncio
async def test_stateless_input_owner_gate():
    """Same fail-closed owner semantics as resolve_thread_for_forwarding."""
    conn = FakeConn()
    db = FakeDB(_stateless_thread(user_id="somebody-else"), conn)

    with pytest.raises(HTTPException) as exc:
        await thread_transport.thread_input(
            THREAD_ID,
            ThreadInputRequest(content="x"),
            MagicMock(),
            dependencies=_deps(db),
        )
    assert exc.value.status_code == 403

    db_missing = FakeDB(None, conn)
    with pytest.raises(HTTPException) as exc:
        await thread_transport.thread_input(
            THREAD_ID,
            ThreadInputRequest(content="x"),
            MagicMock(),
            dependencies=_deps(db_missing),
        )
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_pinned_thread_takes_legacy_forward_with_lock(monkeypatch):
    """A pinned-lane thread must go through the untouched forwarding path:
    resolve_thread_for_forwarding + per-turn lock + agent forward, response
    nesting the agent object."""
    import asyncio

    pinned = _stateless_thread(execution_lane="pinned")
    conn = FakeConn()
    db = FakeDB(pinned, conn)

    binding = _pinned_binding()
    resolve_spy = AsyncMock(return_value=(pinned, binding))
    monkeypatch.setattr(pinned_forwarding, "resolve_thread_for_forwarding", resolve_spy)
    revalidate_spy = AsyncMock(return_value=binding)
    monkeypatch.setattr(
        pinned_forwarding,
        "revalidate_pinned_forwarding_binding",
        revalidate_spy,
    )
    forward_spy = AsyncMock(
        return_value={"accepted": True, "turn_id": 5, "queue_depth": 1}
    )
    monkeypatch.setattr(pinned_forwarding, "forward_to_agent", forward_spy)
    turn_locks = ThreadTurnLocks()
    lock_spy = MagicMock(side_effect=turn_locks.ensure)
    monkeypatch.setattr(turn_locks, "ensure", lock_spy)
    # Neutralize the 5-minute deferred cleanup task so the loop closes clean.
    monkeypatch.setattr(turn_locks, "schedule_cleanup", MagicMock())
    deps = _deps(db, turn_locks=turn_locks)

    out = await thread_transport.thread_input(
        THREAD_ID, ThreadInputRequest(content="hi"), MagicMock(), dependencies=deps
    )
    await asyncio.sleep(0)

    resolve_spy.assert_awaited_once_with(
        THREAD_ID, dict(USER), dependencies=deps.forwarding
    )
    lock_spy.assert_called_once_with(THREAD_ID, 6)
    revalidate_spy.assert_awaited_once_with(binding, store=db)
    forward_spy.assert_awaited_once()
    args = forward_spy.await_args.args
    assert args[1] == "/api/input"
    assert args[2] == {"content": "hi", "turn_id": 6}
    assert forward_spy.await_args.kwargs == {"store": db}
    assert out == {"accepted": True, "turn_id": 6, "agent": forward_spy.return_value}
    # The queue lane's transaction machinery must not have been touched.
    assert conn.txn_enters == 0


@pytest.mark.asyncio
async def test_pinned_duplicate_turn_returns_409_while_the_first_is_in_flight(
    monkeypatch,
):
    """Two tabs racing on the same pinned turn share one lock in this
    application's registry: the second sees it held and gets the 409
    ``turn_in_flight`` refusal instead of a second forward."""
    import asyncio
    import json

    pinned = _stateless_thread(execution_lane="pinned")
    db = FakeDB(pinned, FakeConn())
    binding = _pinned_binding()
    monkeypatch.setattr(
        pinned_forwarding,
        "resolve_thread_for_forwarding",
        AsyncMock(return_value=(pinned, binding)),
    )
    monkeypatch.setattr(
        pinned_forwarding,
        "revalidate_pinned_forwarding_binding",
        AsyncMock(return_value=binding),
    )
    entered = asyncio.Event()
    release = asyncio.Event()

    async def _held_forward(*_args, **_kwargs):
        entered.set()
        await release.wait()
        return {"accepted": True}

    forward = AsyncMock(side_effect=_held_forward)
    monkeypatch.setattr(pinned_forwarding, "forward_to_agent", forward)
    turn_locks = ThreadTurnLocks()
    monkeypatch.setattr(turn_locks, "schedule_cleanup", MagicMock())
    deps = _deps(db, turn_locks=turn_locks)

    first = asyncio.create_task(
        thread_transport.thread_input(
            THREAD_ID,
            ThreadInputRequest(content="tab one"),
            MagicMock(),
            dependencies=deps,
        )
    )
    await entered.wait()
    try:
        # Bounded: without the refusal the duplicate would queue on the lock.
        duplicate = await asyncio.wait_for(
            thread_transport.thread_input(
                THREAD_ID,
                ThreadInputRequest(content="tab two"),
                MagicMock(),
                dependencies=deps,
            ),
            timeout=5,
        )
    finally:
        release.set()
    out = await first

    assert duplicate.status_code == 409
    assert json.loads(duplicate.body) == {
        "error": "turn_in_flight",
        "turn_id": 6,
        "thread_id": THREAD_ID,
    }
    forward.assert_awaited_once()
    assert out == {"accepted": True, "turn_id": 6, "agent": {"accepted": True}}


@pytest.mark.asyncio
async def test_uncorrelated_interrupt_on_stateless_lane_returns_422():
    conn = FakeConn()
    db = FakeDB(_stateless_thread(), conn)
    owner = AsyncMock(return_value=(dict(USER), _stateless_thread()))

    with pytest.raises(HTTPException) as exc:
        await thread_transport.thread_interrupt(
            THREAD_ID, MagicMock(), None, dependencies=_deps(db, owner=owner)
        )
    assert exc.value.status_code == 422
    assert "target_turn_id" in exc.value.detail


@pytest.mark.asyncio
async def test_interrupt_on_pinned_lane_still_forwards(monkeypatch):
    pinned = _stateless_thread(execution_lane="pinned")
    db = FakeDB(pinned, FakeConn())
    owner = AsyncMock(return_value=(dict(USER), pinned))
    binding = _pinned_binding()
    monkeypatch.setattr(
        pinned_forwarding,
        "resolve_thread_for_forwarding",
        AsyncMock(return_value=(pinned, binding)),
    )
    forward_spy = AsyncMock(return_value={"ok": True})
    monkeypatch.setattr(pinned_forwarding, "forward_to_agent", forward_spy)

    out = await thread_transport.thread_interrupt(
        THREAD_ID, MagicMock(), None, dependencies=_deps(db, owner=owner)
    )
    forward_spy.assert_awaited_once_with(binding, "/api/interrupt", {}, store=db)
    assert out == {"accepted": True, "agent": {"ok": True}}


@pytest.mark.asyncio
async def test_exact_forward_rechecks_after_client_entry_and_adds_fingerprint(
    monkeypatch,
):
    binding = _pinned_binding()
    store_sentinel = object()
    order: list[str] = []
    observed: dict = {}

    class _Response:
        status_code = 202
        text = '{"accepted":true}'
        headers = {}

        @staticmethod
        def json():
            return {"accepted": True}

    class _Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            order.append("client_enter")
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, url, *, json):
            order.append("post")
            observed.update(url=url, json=json)
            return _Response()

    async def _revalidate(current, *, store):
        order.append("db_recheck")
        assert current is binding
        assert store is store_sentinel
        return binding

    monkeypatch.setattr(pinned_forwarding.httpx, "AsyncClient", _Client)
    monkeypatch.setattr(
        pinned_forwarding,
        "revalidate_pinned_forwarding_binding",
        _revalidate,
    )

    result = await pinned_forwarding.forward_to_agent(
        binding,
        "/api/input",
        {"content": "hello", "turn_id": 6},
        store=store_sentinel,
    )

    assert result == {"accepted": True}
    assert order == ["client_enter", "db_recheck", "post"]
    assert observed == {
        "url": "http://10.0.0.9:8001/api/input",
        "json": {
            "content": "hello",
            "turn_id": 6,
            "session_identity_fingerprint": binding.session_identity_fingerprint,
        },
    }


@pytest.mark.asyncio
async def test_exact_forward_binding_loss_after_client_entry_sends_nothing(
    monkeypatch,
):
    binding = _pinned_binding()
    post = AsyncMock()

    class _Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, *args, **kwargs):
            return await post(*args, **kwargs)

    refusal = HTTPException(
        status_code=409,
        detail={
            "code": "session_binding_invalid",
            "pinned_runtime_generation_contract": 1,
            "session_runtime_generation": binding.runtime_generation,
        },
    )
    monkeypatch.setattr(pinned_forwarding.httpx, "AsyncClient", _Client)
    monkeypatch.setattr(
        pinned_forwarding,
        "revalidate_pinned_forwarding_binding",
        AsyncMock(side_effect=refusal),
    )

    with pytest.raises(HTTPException) as caught:
        await pinned_forwarding.forward_to_agent(
            binding,
            "/api/input",
            {"content": "must not move"},
            store=object(),
        )

    assert caught.value is refusal
    post.assert_not_awaited()


@pytest.mark.asyncio
async def test_agent_identity_mismatch_becomes_generation_bound_refusal(monkeypatch):
    binding = _pinned_binding()

    class _Response:
        status_code = 409
        text = '{"error":"session_identity_mismatch"}'
        headers = {}

        @staticmethod
        def json():
            return {"error": "session_identity_mismatch", "retryable": True}

    class _Client:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def post(self, *args, **kwargs):
            return _Response()

    monkeypatch.setattr(pinned_forwarding.httpx, "AsyncClient", _Client)
    monkeypatch.setattr(
        pinned_forwarding,
        "revalidate_pinned_forwarding_binding",
        AsyncMock(return_value=binding),
    )

    with pytest.raises(HTTPException) as caught:
        await pinned_forwarding.forward_to_agent(
            binding, "/api/interrupt", {}, store=object()
        )

    assert caught.value.status_code == 409
    assert caught.value.detail == {
        "code": "session_binding_invalid",
        "message": "This session binding is no longer authoritative.",
        "pinned_runtime_generation_contract": 1,
        "session_runtime_generation": binding.runtime_generation,
    }


@pytest.mark.asyncio
async def test_pinned_input_rechecks_binding_after_turn_lock_before_forward(
    monkeypatch,
):
    pinned = _stateless_thread(execution_lane="pinned")
    binding = _pinned_binding()
    db = FakeDB(pinned, FakeConn())
    monkeypatch.setattr(
        pinned_forwarding,
        "resolve_thread_for_forwarding",
        AsyncMock(return_value=(pinned, binding)),
    )
    order: list[str] = []

    class _Lock:
        @staticmethod
        def locked():
            return False

        async def __aenter__(self):
            order.append("lock_enter")
            return self

        async def __aexit__(self, *args):
            order.append("lock_exit")
            return False

    turn_locks = ThreadTurnLocks()
    monkeypatch.setattr(turn_locks, "ensure", lambda *_: _Lock())
    monkeypatch.setattr(turn_locks, "schedule_cleanup", MagicMock())
    refusal = HTTPException(
        status_code=409,
        detail={"code": "session_binding_invalid"},
    )

    async def _reject(_binding, *, store):
        assert store is db
        order.append("binding_recheck")
        raise refusal

    monkeypatch.setattr(
        pinned_forwarding,
        "revalidate_pinned_forwarding_binding",
        _reject,
    )
    forward = AsyncMock()
    monkeypatch.setattr(pinned_forwarding, "forward_to_agent", forward)

    with pytest.raises(HTTPException) as caught:
        await thread_transport.thread_input(
            THREAD_ID,
            ThreadInputRequest(content="stay exact"),
            MagicMock(),
            dependencies=_deps(db, turn_locks=turn_locks),
        )

    assert caught.value is refusal
    assert order == ["lock_enter", "binding_recheck", "lock_exit"]
    forward.assert_not_awaited()
