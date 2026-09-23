"""Owner queue endpoints + the shared ``queue`` block
(stateless_turn_resilience.md step 2 API contract).

``GET /api/persistent/threads/{id}/queue`` and ``POST …/queue/retry`` are
owner-gated routes of ``routers/thread_transport``; ``/connection`` (sessions
router) and ``/input`` carry the same block. The handlers are driven directly
with one application's transport dependencies built from fakes for the DB and
the gate; the route inventory proves the application mounts them.
"""

from __future__ import annotations

import contextlib
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from orchestrator import main
from orchestrator.routers import sessions as sessions_routes
from orchestrator.routers import thread_transport
from orchestrator.services import stateless_queue_state as sqs
from orchestrator.services.thread_turn_locks import ThreadTurnLocks
from shared.run_queue import PARK_REASON_ATTACH_FAILED, PARK_REASON_CLAIM_LOSS_HOLD
from shared.session_retirement import CLAIM_LOSS_HOLD_KEY

THREAD = str(uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"))
NOW = datetime(2026, 9, 8, 15, 0, tzinfo=timezone.utc)
USER = {"id": "u1", "is_admin": False}


class _Conn:
    def __init__(self, authority=None):
        self.authority = authority
        self.fetchrow_calls = 0

    @contextlib.asynccontextmanager
    async def transaction(self):
        yield

    async def fetchrow(self, query, *args):
        self.fetchrow_calls += 1
        return self.authority


class _Db:
    def __init__(self, conn):
        self.conn = conn

    @contextlib.asynccontextmanager
    async def acquire(self):
        yield self.conn


def _parked(reason=PARK_REASON_ATTACH_FAILED, attempts=3):
    return {
        "state": "parked",
        "park_reason": reason,
        "parked_at": NOW,
        "last_error": "boom",
        "attempts": attempts,
        "max_attempts": 5,
        "attach_failures": 3,
        "pending_input": True,
        "run_after": NOW,
    }


@pytest.fixture
def owner(monkeypatch):
    thread = {"id": THREAD, "user_id": "u1", "metadata": {}}
    gate = AsyncMock(return_value=(USER, thread))
    # The retry audit is looked up in the router module at call time.
    monkeypatch.setattr(thread_transport, "log_security_event", AsyncMock())
    return gate


def _deps(gate, conn=None):
    """One application's transport collaborators: its store and owner gate.

    The queue routes consult nothing else; the input/forwarding collaborators
    are inert placeholders.
    """
    return thread_transport.ThreadTransportDependencies(
        store=_Db(conn if conn is not None else _Conn()),
        require_thread_owner=gate,
        require_approved_user=AsyncMock(
            side_effect=AssertionError("queue routes gate on the owner check")
        ),
        forwarding=SimpleNamespace(),
        stateless_input=SimpleNamespace(),
        turn_locks=ThreadTurnLocks(),
    )


def _wire(monkeypatch, *, state, authority, unpark=True):
    import shared.run_queue as rq

    conn = _Conn(authority=authority)
    # The retry handler imports the statement at call time (shared.run_queue);
    # the block builder bound it at import (stateless_queue_state). Patch both.
    reader = AsyncMock(return_value=state)
    monkeypatch.setattr(rq, "queue_state_for", reader)
    monkeypatch.setattr(sqs, "queue_state_for", reader)
    unpark_mock = AsyncMock(return_value=unpark)
    monkeypatch.setattr(rq, "unpark_unit", unpark_mock)
    return conn, unpark_mock


# ---------------------------------------------------------------------------
# GET …/queue
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_queue_state_returns_the_block_for_the_owner(owner, monkeypatch):
    conn, _ = _wire(monkeypatch, state=_parked(), authority=None)
    body = await thread_transport.thread_queue_state(
        THREAD, request=object(), dependencies=_deps(owner, conn)
    )
    assert body["thread_id"] == THREAD
    assert body["queue"] == {
        "state": "parked",
        "park_reason": PARK_REASON_ATTACH_FAILED,
        "parked_at": NOW.isoformat(),
        "retryable": True,
        "attempts": 3,
        "pending_input": True,
        "cloud_push": None,
    }
    owner.assert_awaited_once()


@pytest.mark.asyncio
async def test_queue_state_reports_none_when_never_enqueued(owner, monkeypatch):
    conn, _ = _wire(monkeypatch, state=None, authority=None)
    body = await thread_transport.thread_queue_state(
        THREAD, request=object(), dependencies=_deps(owner, conn)
    )
    assert body["queue"]["state"] == "none" and body["queue"]["retryable"] is False


@pytest.mark.asyncio
async def test_queue_state_rejects_a_non_uuid_before_the_gate(owner):
    with pytest.raises(HTTPException) as err:
        await thread_transport.thread_queue_state(
            "ad7eb761", request=object(), dependencies=_deps(owner)
        )
    assert err.value.status_code == 404
    owner.assert_not_awaited()


@pytest.mark.asyncio
async def test_queue_state_gate_denial_propagates():
    gate = AsyncMock(side_effect=HTTPException(status_code=403, detail="no"))
    with pytest.raises(HTTPException) as err:
        await thread_transport.thread_queue_state(
            THREAD, request=object(), dependencies=_deps(gate)
        )
    assert err.value.status_code == 403


# ---------------------------------------------------------------------------
# POST …/queue/retry
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_unparks_a_retryable_park_and_audits(owner, monkeypatch):
    conn, unpark = _wire(
        monkeypatch,
        state=_parked(),
        authority={"execution_lane": "stateless", "metadata": {}},
    )
    body = await thread_transport.thread_queue_retry(
        THREAD, request=object(), dependencies=_deps(owner, conn)
    )
    assert body == {
        "thread_id": THREAD,
        "unit_id": THREAD,
        "state": "queued",
        "park_reason": PARK_REASON_ATTACH_FAILED,
    }
    unpark.assert_awaited_once()
    assert unpark.await_args.kwargs["unit_id"] == THREAD
    assert conn.fetchrow_calls == 1  # the FOR UPDATE authority read
    audit = thread_transport.log_security_event
    audit.assert_awaited_once()
    assert audit.await_args.kwargs["event_type"] == "queue_retry"
    assert audit.await_args.kwargs["resource_id"] == THREAD


@pytest.mark.asyncio
async def test_retry_404_when_not_parked(owner, monkeypatch):
    conn, unpark = _wire(
        monkeypatch,
        state={**_parked(), "state": "queued"},
        authority={"execution_lane": "stateless", "metadata": {}},
    )
    with pytest.raises(HTTPException) as err:
        await thread_transport.thread_queue_retry(
            THREAD, request=object(), dependencies=_deps(owner, conn)
        )
    assert err.value.status_code == 404
    unpark.assert_not_awaited()


@pytest.mark.asyncio
async def test_retry_404_when_no_queue_row_or_thread(owner, monkeypatch):
    conn, _ = _wire(
        monkeypatch,
        state=None,
        authority={"execution_lane": "stateless", "metadata": {}},
    )
    with pytest.raises(HTTPException) as err:
        await thread_transport.thread_queue_retry(
            THREAD, request=object(), dependencies=_deps(owner, conn)
        )
    assert err.value.status_code == 404
    conn, _ = _wire(monkeypatch, state=_parked(), authority=None)
    with pytest.raises(HTTPException) as err:
        await thread_transport.thread_queue_retry(
            THREAD, request=object(), dependencies=_deps(owner, conn)
        )
    assert err.value.status_code == 404


@pytest.mark.asyncio
async def test_retry_409_codes(owner, monkeypatch):
    # claim-loss hold marker on the thread
    conn, unpark = _wire(
        monkeypatch,
        state=_parked(),
        authority={
            "execution_lane": "stateless",
            "metadata": {CLAIM_LOSS_HOLD_KEY: {}},
        },
    )
    with pytest.raises(HTTPException) as err:
        await thread_transport.thread_queue_retry(
            THREAD, request=object(), dependencies=_deps(owner, conn)
        )
    assert err.value.status_code == 409
    assert err.value.detail["code"] == sqs.RETRY_REFUSAL_CLAIM_LOSS_HOLD
    unpark.assert_not_awaited()
    # hold by reason alone
    conn, unpark = _wire(
        monkeypatch,
        state=_parked(reason=PARK_REASON_CLAIM_LOSS_HOLD),
        authority={"execution_lane": "stateless", "metadata": {}},
    )
    with pytest.raises(HTTPException) as err:
        await thread_transport.thread_queue_retry(
            THREAD, request=object(), dependencies=_deps(owner, conn)
        )
    assert err.value.detail["code"] == sqs.RETRY_REFUSAL_CLAIM_LOSS_HOLD
    # non-retryable free-form reason
    conn, unpark = _wire(
        monkeypatch,
        state=_parked(reason="loop_died_interrupt_drain_failed"),
        authority={"execution_lane": "stateless", "metadata": {}},
    )
    with pytest.raises(HTTPException) as err:
        await thread_transport.thread_queue_retry(
            THREAD, request=object(), dependencies=_deps(owner, conn)
        )
    assert err.value.detail == {
        "code": sqs.RETRY_REFUSAL_NOT_RETRYABLE,
        "park_reason": "loop_died_interrupt_drain_failed",
    }
    unpark.assert_not_awaited()


@pytest.mark.asyncio
async def test_retry_gate_denial_never_reads_the_queue(monkeypatch):
    gate = AsyncMock(side_effect=HTTPException(status_code=403, detail="no"))
    conn, unpark = _wire(monkeypatch, state=_parked(), authority=None)
    with pytest.raises(HTTPException):
        await thread_transport.thread_queue_retry(
            THREAD, request=object(), dependencies=_deps(gate, conn)
        )
    unpark.assert_not_awaited()
    assert conn.fetchrow_calls == 0


# ---------------------------------------------------------------------------
# /connection carries the same block; a read failure never blocks readiness
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_connection_queue_block_reads_and_degrades(monkeypatch):
    monkeypatch.setattr(sqs, "queue_state_for", AsyncMock(return_value=_parked()))
    block = await sessions_routes._read_queue_block(
        _Db(_Conn()), {"id": THREAD, "metadata": {}}
    )
    assert block["state"] == "parked" and block["retryable"] is True
    # no acquire() on the db → None, never an error
    assert await sessions_routes._read_queue_block(object(), {"id": THREAD}) is None
    monkeypatch.setattr(
        sqs, "queue_state_for", AsyncMock(side_effect=RuntimeError("db"))
    )
    assert (
        await sessions_routes._read_queue_block(
            _Db(_Conn()), {"id": THREAD, "metadata": {}}
        )
        is None
    )
    assert "queue" in sessions_routes.StatelessConnectionResponse.model_fields
    assert "queue" in sessions_routes.PinnedConnectionResponse.model_fields


# ---------------------------------------------------------------------------
# Mounted + gated
# ---------------------------------------------------------------------------


def test_queue_routes_are_mounted():
    from tests._route_inventory import mounted_routes

    routes = mounted_routes(main.app)
    assert ("GET", "/api/persistent/threads/{thread_id}/queue") in routes
    assert ("POST", "/api/persistent/threads/{thread_id}/queue/retry") in routes


# ---------------------------------------------------------------------------
# cloud_push (commit-then-effects, step 4a)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_queue_block_reports_no_cloud_push_when_nothing_pending(
    owner, monkeypatch
):
    conn, _ = _wire(monkeypatch, state=_parked(), authority=None)
    conn.fetch = AsyncMock(return_value=[])  # pending_push_state → no rows
    body = await thread_transport.thread_queue_state(
        THREAD, request=object(), dependencies=_deps(owner, conn)
    )
    assert body["queue"]["cloud_push"] is None


@pytest.mark.asyncio
async def test_queue_block_reports_the_off_slot_push(owner, monkeypatch):
    conn, _ = _wire(monkeypatch, state={**_parked(), "state": "done"}, authority=None)
    conn.fetch = AsyncMock(
        return_value=[
            {
                "mount_id": "legacy-session",
                "push_progress": {
                    "planned": 17,
                    "files": {
                        "a.md": {
                            "sha256": "a" * 64,
                            "size": 3,
                            "remote_etag": "e",
                            "state": "uploaded",
                        }
                    },
                },
                "push_heartbeat_at": NOW,
                "push_owner_pod": "srw-agent-stateless-x",
                "push_failed_at": None,
                "push_error": None,
                "owner_alive": True,
            }
        ]
    )
    body = await thread_transport.thread_queue_state(
        THREAD, request=object(), dependencies=_deps(owner, conn)
    )
    assert body["queue"]["cloud_push"] == {
        "pending": 1,
        "uploaded": 1,
        "total": 17,
        "owner_alive": True,
        "failed": False,
    }


@pytest.mark.asyncio
async def test_queue_block_survives_a_cloud_push_read_failure(owner, monkeypatch):
    conn, _ = _wire(monkeypatch, state=_parked(), authority=None)
    conn.fetch = AsyncMock(side_effect=RuntimeError("db down"))
    body = await thread_transport.thread_queue_state(
        THREAD, request=object(), dependencies=_deps(owner, conn)
    )
    assert body["queue"]["state"] == "parked"
    assert body["queue"]["cloud_push"] is None
