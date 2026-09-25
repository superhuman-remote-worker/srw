"""Lane-free exact-turn interrupt admission and orchestrator routing."""

from __future__ import annotations

import inspect
import json
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from orchestrator.services.thread_interrupt_inbox import (
    AdmittedInterrupt,
    InterruptAdmissionError,
    admit_thread_interrupt,
    find_existing_thread_interrupt,
)
from shared.session_retirement import STATELESS_STOP_KEYS


THREAD_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
OWNER_ID = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")


def _virtual_metadata(**extra):
    return {
        "config_override": {"workspace": {"backend": "virtual"}},
        **extra,
    }


class _Transaction:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        self.conn.transaction_depth += 1
        return self

    async def __aexit__(self, *_exc):
        self.conn.transaction_depth -= 1


class _Acquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *_exc):
        return None


class _DB:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _Acquire(self.conn)


class _Conn:
    def __init__(
        self,
        *,
        existing=None,
        thread=None,
        queue=None,
        inserted=None,
    ):
        self.existing = existing
        self.thread = thread or {
            "id": THREAD_ID,
            "user_id": OWNER_ID,
            "execution_lane": "stateless",
            "agent_id": None,
            "status": "active",
            "metadata": _virtual_metadata(),
        }
        self.queue = queue or {
            "unit_kind": "session_turn",
            "state": "leased",
            "lease_token": 12,
            "leased_by": "stateless-agent-a",
            "interrupt_admission_lease_token": 12,
            "interrupt_admission_turn_id": 7,
        }
        self.inserted = inserted
        self.calls = []
        self.transaction_depth = 0

    def transaction(self):
        return _Transaction(self)

    async def fetchrow(self, sql, *args):
        self.calls.append((sql, args, self.transaction_depth))
        if "FROM threads" in sql:
            return self.thread
        if "FROM thread_interrupt_requests" in sql:
            return self.existing
        if "FROM run_queue" in sql:
            return self.queue
        if "INSERT INTO thread_interrupt_requests" in sql:
            return self.inserted or {
                "id": uuid4(),
                "client_request_id": args[1],
                "target_turn_id": args[2],
                "accepted_lease_token": args[3],
                "accepted_leased_by": args[4],
                "outcome": None,
            }
        raise AssertionError(sql)


@pytest.mark.asyncio
async def test_admission_captures_exact_open_lease_without_waking_queue():
    request_id = uuid4()
    conn = _Conn()

    result = await admit_thread_interrupt(
        _DB(conn),
        thread_id=THREAD_ID,
        owner_user_id=OWNER_ID,
        client_request_id=request_id,
        target_turn_id=7,
        requested_by=str(OWNER_ID),
    )

    assert result.client_request_id == request_id
    assert result.target_turn_id == 7
    assert result.accepted_lease_token == 12
    assert result.accepted_leased_by == "stateless-agent-a"
    assert result.state == "pending"
    assert result.duplicate is False
    insert = next(call for call in conn.calls if "INSERT INTO" in call[0])
    assert insert[2] == 1
    assert insert[1][3:5] == (12, "stateless-agent-a")
    # Admission neither wakes/requeues nor advances an unrelated watermark.
    assert all("UPDATE run_queue" not in sql for sql, _args, _depth in conn.calls)


@pytest.mark.asyncio
async def test_exact_retry_returns_original_before_closed_gate_check():
    client_request_id = uuid4()
    existing = {
        "id": uuid4(),
        "client_request_id": client_request_id,
        "target_turn_id": 7,
        "accepted_lease_token": 12,
        "accepted_leased_by": "stateless-agent-a",
        "outcome": "applied",
    }
    conn = _Conn(
        existing=existing,
        thread={
            "id": THREAD_ID,
            "user_id": OWNER_ID,
            "execution_lane": "stateless",
            "agent_id": None,
            "status": "ended",
            "metadata": _virtual_metadata(
                _stateless_workspace_retirement_pending=False
            ),
        },
        queue={},
    )

    result = await admit_thread_interrupt(
        _DB(conn),
        thread_id=THREAD_ID,
        owner_user_id=OWNER_ID,
        client_request_id=client_request_id,
        target_turn_id=7,
        requested_by=str(OWNER_ID),
    )

    assert result.id == existing["id"]
    assert result.state == "applied"
    assert result.duplicate is True
    assert all("FROM run_queue" not in sql for sql, _args, _depth in conn.calls)


def _assert_no_queue_or_insert(conn):
    assert all("FROM run_queue" not in sql for sql, _args, _depth in conn.calls)
    assert all(
        "INSERT INTO thread_interrupt_requests" not in sql
        for sql, _args, _depth in conn.calls
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [None, "", "idle", "suspended", "ended"])
async def test_new_interrupt_refuses_ineligible_lifecycle_before_queue(status):
    conn = _Conn(thread=_Conn().thread | {"status": status})

    with pytest.raises(InterruptAdmissionError, match="currently able"):
        await admit_thread_interrupt(
            _DB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=uuid4(),
            target_turn_id=7,
            requested_by=str(OWNER_ID),
        )

    _assert_no_queue_or_insert(conn)


@pytest.mark.asyncio
@pytest.mark.parametrize("marker", sorted(STATELESS_STOP_KEYS))
@pytest.mark.parametrize("falsey_value", [None, False, 0, "", [], {}])
async def test_new_interrupt_refuses_present_falsey_stop_marker_before_queue(
    marker, falsey_value
):
    conn = _Conn(
        thread=_Conn().thread
        | {"metadata": _virtual_metadata(**{marker: falsey_value})}
    )

    with pytest.raises(InterruptAdmissionError, match="currently able"):
        await admit_thread_interrupt(
            _DB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=uuid4(),
            target_turn_id=7,
            requested_by=str(OWNER_ID),
        )

    _assert_no_queue_or_insert(conn)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "metadata",
    [
        None,
        "not-json",
        _virtual_metadata(protected_cloud=True),
        _virtual_metadata(protected_cloud=None),
        _virtual_metadata(vm="malformed"),
        _virtual_metadata(vm={"status": "ready"}),
        _virtual_metadata(
            config_override={
                "workspace": {"backend": "virtual"},
                "officer": {"enabled": True},
            }
        ),
        _virtual_metadata(
            config_override={
                "workspace": {"backend": "virtual"},
                "officer": {"conference": True},
            }
        ),
        _virtual_metadata(
            config_override={
                "workspace": {"backend": "virtual"},
                "officer": {"enabled": "false"},
            }
        ),
        {
            "config_override": {"workspace": {"backend": "sandbox"}},
            "workspace_container": {"provisioner": "docker", "status": "ready"},
        },
    ],
    ids=[
        "null-metadata",
        "malformed-metadata-json",
        "protected-cloud",
        "malformed-protected-cloud",
        "malformed-vm",
        "vm-present",
        "officer",
        "conference",
        "malformed-officer-bit",
        "docker-sandbox",
    ],
)
async def test_new_interrupt_refuses_unsupported_session_before_queue(metadata):
    conn = _Conn(thread=_Conn().thread | {"metadata": metadata})

    with pytest.raises(InterruptAdmissionError, match="workspace binding"):
        await admit_thread_interrupt(
            _DB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=uuid4(),
            target_turn_id=7,
            requested_by=str(OWNER_ID),
        )

    _assert_no_queue_or_insert(conn)


@pytest.mark.asyncio
async def test_reused_uuid_for_different_target_fails_before_gate_check():
    client_request_id = uuid4()
    conn = _Conn(
        existing={
            "id": uuid4(),
            "client_request_id": client_request_id,
            "target_turn_id": 6,
            "accepted_lease_token": 11,
            "accepted_leased_by": "stateless-agent-a",
            "outcome": None,
        }
    )

    with pytest.raises(InterruptAdmissionError, match="different turn"):
        await admit_thread_interrupt(
            _DB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=client_request_id,
            target_turn_id=7,
            requested_by=str(OWNER_ID),
        )
    assert all("FROM run_queue" not in sql for sql, _args, _depth in conn.calls)


@pytest.mark.asyncio
async def test_preflight_finds_masked_commit_without_consulting_current_lane():
    client_request_id = uuid4()
    existing = {
        "id": uuid4(),
        "client_request_id": client_request_id,
        "target_turn_id": 7,
        "accepted_lease_token": 12,
        "accepted_leased_by": "stateless-agent-a",
        "outcome": "applied",
    }
    conn = _Conn(
        existing=existing,
        thread={
            "id": THREAD_ID,
            "user_id": OWNER_ID,
            "execution_lane": "pinned",
            "agent_id": uuid4(),
        },
    )

    result = await find_existing_thread_interrupt(
        _DB(conn),
        thread_id=THREAD_ID,
        owner_user_id=OWNER_ID,
        client_request_id=client_request_id,
        target_turn_id=7,
    )

    assert result is not None
    assert result.id == existing["id"]
    assert result.duplicate is True
    assert all("FROM run_queue" not in sql for sql, _args, _depth in conn.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "queue_patch",
    [
        {"state": "done"},
        {"lease_token": 13},
        {"interrupt_admission_lease_token": None},
        {"interrupt_admission_turn_id": 8},
        {"leased_by": None},
        {"unit_kind": "worker_batch"},
    ],
)
async def test_admission_fails_closed_when_exact_gate_is_not_open(queue_patch):
    queue = _Conn().queue | queue_patch
    conn = _Conn(queue=queue)

    with pytest.raises(InterruptAdmissionError, match="no longer accepting"):
        await admit_thread_interrupt(
            _DB(conn),
            thread_id=THREAD_ID,
            owner_user_id=OWNER_ID,
            client_request_id=uuid4(),
            target_turn_id=7,
            requested_by=str(OWNER_ID),
        )
    assert all("INSERT INTO" not in sql for sql, _args, _depth in conn.calls)


def test_public_envelope_is_complete_and_js_safe():
    from orchestrator.schemas.thread_transport import ThreadInterruptRequest

    with pytest.raises(ValidationError):
        ThreadInterruptRequest(client_request_id=uuid4())
    with pytest.raises(ValidationError):
        ThreadInterruptRequest(client_request_id=uuid4(), target_turn_id=0)
    with pytest.raises(ValidationError):
        ThreadInterruptRequest(client_request_id=uuid4(), target_turn_id=True)
    with pytest.raises(ValidationError):
        ThreadInterruptRequest(client_request_id=uuid4(), target_turn_id=2_147_483_648)
    assert ThreadInterruptRequest().client_request_id is None  # pinned legacy


def _owner_deps(monkeypatch, thread):
    """One application's transport collaborators with ``thread`` owned by the
    caller. ``find_existing_thread_interrupt`` is looked up in the router
    module at call time, so the no-prior-request default is patched there."""
    from types import SimpleNamespace

    from orchestrator.routers import thread_transport
    from orchestrator.services.thread_turn_locks import ThreadTurnLocks

    user = {"id": str(OWNER_ID), "is_admin": False}
    monkeypatch.setattr(
        thread_transport,
        "find_existing_thread_interrupt",
        AsyncMock(return_value=None),
    )
    deps = thread_transport.ThreadTransportDependencies(
        store=MagicMock(),
        require_thread_owner=AsyncMock(return_value=(user, thread)),
        require_approved_user=AsyncMock(
            side_effect=AssertionError("interrupt gates on the owner check")
        ),
        forwarding=SimpleNamespace(),
        stateless_input=SimpleNamespace(),
        turn_locks=ThreadTurnLocks(),
    )
    return user, deps


@pytest.mark.asyncio
async def test_pinned_legacy_empty_body_forwards_byte_identical_payload(monkeypatch):
    from orchestrator.routers import thread_transport
    from orchestrator.services import pinned_forwarding

    thread = {
        "id": str(THREAD_ID),
        "user_id": str(OWNER_ID),
        "execution_lane": "pinned",
        "agent_id": str(uuid4()),
    }
    user, deps = _owner_deps(monkeypatch, thread)
    agent = {"id": "agent-a", "pod_ip": "10.0.0.2", "pod_port": 8001}
    resolve = AsyncMock(return_value=(thread, agent))
    forward = AsyncMock(return_value={"ack": True, "mode": "hard"})
    monkeypatch.setattr(pinned_forwarding, "resolve_thread_for_forwarding", resolve)
    monkeypatch.setattr(pinned_forwarding, "forward_to_agent", forward)

    result = await thread_transport.thread_interrupt(
        str(THREAD_ID), MagicMock(), None, dependencies=deps
    )

    resolve.assert_awaited_once_with(str(THREAD_ID), user, dependencies=deps.forwarding)
    forward.assert_awaited_once_with(agent, "/api/interrupt", {}, store=deps.store)
    assert result == {"accepted": True, "agent": {"ack": True, "mode": "hard"}}


@pytest.mark.asyncio
async def test_pinned_correlated_envelope_forwards_target_intact(monkeypatch):
    from orchestrator.routers import thread_transport
    from orchestrator.schemas.thread_transport import ThreadInterruptRequest
    from orchestrator.services import pinned_forwarding

    thread = {
        "id": str(THREAD_ID),
        "user_id": str(OWNER_ID),
        "execution_lane": "pinned",
        "agent_id": str(uuid4()),
    }
    _user, deps = _owner_deps(monkeypatch, thread)
    agent = {"id": "agent-a", "pod_ip": "10.0.0.2", "pod_port": 8001}
    monkeypatch.setattr(
        pinned_forwarding,
        "resolve_thread_for_forwarding",
        AsyncMock(return_value=(thread, agent)),
    )
    forward = AsyncMock(return_value={"ack": True, "mode": "hard"})
    monkeypatch.setattr(pinned_forwarding, "forward_to_agent", forward)
    client_request_id = uuid4()
    body = ThreadInterruptRequest(
        client_request_id=client_request_id,
        target_turn_id=7,
    )

    await thread_transport.thread_interrupt(
        str(THREAD_ID), MagicMock(), body, dependencies=deps
    )

    forward.assert_awaited_once_with(
        agent,
        "/api/interrupt",
        {
            "client_request_id": str(client_request_id),
            "target_turn_id": 7,
        },
        store=deps.store,
    )


@pytest.mark.asyncio
async def test_masked_stateless_retry_is_returned_before_pinned_forward(monkeypatch):
    from orchestrator.routers import thread_transport
    from orchestrator.schemas.thread_transport import ThreadInterruptRequest
    from orchestrator.services import pinned_forwarding

    thread = {
        "id": str(THREAD_ID),
        "user_id": str(OWNER_ID),
        "execution_lane": "pinned",
        "agent_id": str(uuid4()),
    }
    _user, deps = _owner_deps(monkeypatch, thread)
    client_request_id = uuid4()
    existing = AdmittedInterrupt(
        id=uuid4(),
        client_request_id=client_request_id,
        target_turn_id=7,
        accepted_lease_token=12,
        accepted_leased_by="old-stateless-agent",
        state="applied",
        duplicate=True,
    )
    monkeypatch.setattr(
        thread_transport,
        "find_existing_thread_interrupt",
        AsyncMock(return_value=existing),
    )
    forward = AsyncMock(side_effect=AssertionError("retry must not hit pinned agent"))
    monkeypatch.setattr(pinned_forwarding, "forward_to_agent", forward)

    response = await thread_transport.thread_interrupt(
        str(THREAD_ID),
        MagicMock(),
        ThreadInterruptRequest(
            client_request_id=client_request_id,
            target_turn_id=7,
        ),
        dependencies=deps,
    )

    assert response.status_code == 202
    assert json.loads(response.body)["state"] == "applied"
    assert json.loads(response.body)["duplicate"] is True
    forward.assert_not_awaited()


@pytest.mark.asyncio
async def test_stateless_route_returns_admission_only(monkeypatch):
    from orchestrator.routers import thread_transport
    from orchestrator.schemas.thread_transport import ThreadInterruptRequest
    from orchestrator.services import pinned_forwarding

    thread = {
        "id": str(THREAD_ID),
        "user_id": str(OWNER_ID),
        "execution_lane": "stateless",
        "agent_id": None,
    }
    _user, deps = _owner_deps(monkeypatch, thread)
    client_request_id = uuid4()
    admitted = AdmittedInterrupt(
        id=uuid4(),
        client_request_id=client_request_id,
        target_turn_id=7,
        accepted_lease_token=12,
        accepted_leased_by="stateless-agent-a",
        state="pending",
        duplicate=False,
    )
    admit = AsyncMock(return_value=admitted)
    monkeypatch.setattr(thread_transport, "admit_thread_interrupt", admit)
    forward = AsyncMock(side_effect=AssertionError("stateless path must not forward"))
    monkeypatch.setattr(pinned_forwarding, "forward_to_agent", forward)

    response = await thread_transport.thread_interrupt(
        str(THREAD_ID),
        MagicMock(),
        ThreadInterruptRequest(
            client_request_id=client_request_id,
            target_turn_id=7,
        ),
        dependencies=deps,
    )

    assert response.status_code == 202
    payload = json.loads(response.body)
    assert payload == {
        "accepted": True,
        "request_id": str(admitted.id),
        "client_request_id": str(client_request_id),
        "target_turn_id": 7,
        "state": "pending",
        "duplicate": False,
    }
    forward.assert_not_awaited()
    assert admit.await_args.args == (deps.store,)


def test_event_pruner_preserves_pending_interrupt_receipts():
    from orchestrator.services import retention_sweepers

    source = inspect.getsource(retention_sweepers.thread_events_prune_sweeper)
    assert source.count("request.id = thread_events.interrupt_request_id") == 2
    assert source.count("FROM thread_interrupt_requests request") == 2
    assert source.count("request.outcome = 'applied'") == 2
    assert source.count("'consumed_input_seq'") == 2
