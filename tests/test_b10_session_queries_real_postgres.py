"""R1.B10's moved SQL, executed against a real PostgreSQL schema.

The extraction moved B10's inline SQL verbatim into its new owners. Unit suites
drive those statements through scripted fakes, which cannot catch a bind-type,
lock-order or schema mismatch. These cases run the real statements against
``schema_current.sql`` in a throwaway PostgreSQL 15 container: stateless input
admission and its revision fence, the owner queue retry, the permission
decision CAS, real magic-link tokens through GET/POST/extend (including replay,
expiry and the extend cap), the stateless and pinned decision-wake fences, the
two sweep selects, and SSE replay/epoch handling on the thread journal.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio
from fastapi import HTTPException
from starlette.requests import Request
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.routers import thread_permissions, thread_transport
from orchestrator.services import (
    headless_notifications,
    magic_link_pages,
    session_attention,
    stateless_input_admission,
    thread_event_stream,
    thread_permissions as thread_permission_operations,
)
from shared.run_queue import (
    PARK_REASON_ATTACH_FAILED,
    PARK_REASON_CLAIM_LOSS_HOLD,
    UNIT_KIND_SESSION_TURN,
    claim_unit,
    complete_unit,
    park_unit,
    record_input_seq,
)

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)
VIRTUAL = {"config_override": {"workspace": {"backend": "virtual"}}}


@pytest.fixture(scope="module")
def pg_dsn():
    try:
        container = PostgresContainer("postgres:15")
        container.start()
    except Exception as exc:
        pytest.skip(f"local PostgreSQL container unavailable: {exc}")
    try:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )
    finally:
        container.stop()


@pytest_asyncio.fixture(scope="module")
async def _schema_applied(pg_dsn):
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(SCHEMA_FILE.read_text())
    finally:
        await conn.close()


@pytest_asyncio.fixture
async def db(pg_dsn, _schema_applied):
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=8)
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute(
            "TRUNCATE run_queue, thread_events, thread_permission_requests, "
            "magic_link_tokens, thread_messages, security_events, threads, "
            "users CASCADE"
        )
    try:
        yield store
    finally:
        await store.close()


async def _user(db: PostgresDB) -> UUID:
    user_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (id, display_name, email) VALUES ($1, 'b10', $2)",
            user_id,
            f"{user_id}@example.test",
        )
    return user_id


async def _thread(
    db: PostgresDB,
    *,
    lane: str = "stateless",
    status: str = "active",
    metadata: dict | None = None,
    user_id: UUID | None = None,
) -> tuple[UUID, UUID]:
    owner = user_id or await _user(db)
    thread_id = uuid4()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO threads (id, user_id, status, execution_lane, config_name, "
            "metadata) VALUES ($1, $2, $3, $4, 'session_base', $5::jsonb)",
            thread_id,
            owner,
            status,
            lane,
            json.dumps(metadata if metadata is not None else VIRTUAL),
        )
    return owner, thread_id


async def _thread_row(db: PostgresDB, thread_id: UUID) -> dict:
    row = await db.get_thread(str(thread_id))
    assert row is not None
    return dict(row)


async def _permission(db: PostgresDB, thread_id: UUID, *, status="pending") -> UUID:
    async with db.acquire() as conn:
        return await conn.fetchval(
            "INSERT INTO thread_permission_requests "
            "(thread_id, tool_call_id, tool_name, tool_args, status) "
            "VALUES ($1, $2, 'run_command', $3::jsonb, $4) RETURNING id",
            thread_id,
            f"call-{uuid4().hex[:8]}",
            json.dumps({"cmd": "ls"}),
            status,
        )


# --------------------------------------------------------------------------- #
# Stateless input admission
# --------------------------------------------------------------------------- #


def _admission(db: PostgresDB, schedule=None):
    return stateless_input_admission.StatelessInputDependencies(
        store=db, schedule_stateless_workspace_ensure=schedule or MagicMock()
    )


@pytest.mark.asyncio
async def test_stateless_admission_is_atomic_and_refuses_a_stale_revision(db):
    _owner, thread_id = await _thread(db)
    thread = await _thread_row(db, thread_id)

    first = await stateless_input_admission.admit_stateless_input(
        thread, "hello", dependencies=_admission(db)
    )
    assert (first["accepted"], first["turn_id"]) == (True, 1)
    assert first["queue"]["state"] == "queued"
    async with db.acquire() as conn:
        messages = await conn.fetch(
            "SELECT role, content, turn_number, seq FROM thread_messages "
            "WHERE thread_id=$1",
            thread_id,
        )
        queue = await conn.fetchrow(
            "SELECT state, input_seq FROM run_queue WHERE unit_id=$1", thread_id
        )
        await conn.execute(
            "UPDATE threads SET conversation_revision = 1 WHERE id=$1", thread_id
        )
    assert [(m["role"], m["content"], m["turn_number"]) for m in messages] == [
        ("human", "hello", 1)
    ]
    assert (queue["state"], queue["input_seq"]) == ("queued", messages[0]["seq"])

    with pytest.raises(HTTPException) as refused:
        await stateless_input_admission.admit_stateless_input(
            thread, "stale view", dependencies=_admission(db)
        )
    assert refused.value.status_code == 409
    assert refused.value.detail["code"] == "session_view_stale"
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM thread_messages WHERE thread_id=$1", thread_id
            )
            == 1
        )

    second = await stateless_input_admission.admit_stateless_input(
        thread, "current view", 1, dependencies=_admission(db)
    )
    assert (second["turn_id"], second["conversation_revision"]) == (2, 1)


@pytest.mark.asyncio
async def test_suspended_stateless_input_wakes_inside_the_admission(db):
    _owner, thread_id = await _thread(db, status="suspended")
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET awaiting_user_since = now(), extend_count = 2 "
            "WHERE id=$1",
            thread_id,
        )
    await stateless_input_admission.admit_stateless_input(
        await _thread_row(db, thread_id), "wake", dependencies=_admission(db)
    )
    row = await _thread_row(db, thread_id)
    assert row["status"] == "created"
    assert row["awaiting_user_since"] is None and row["extend_count"] == 0


# --------------------------------------------------------------------------- #
# Owner queue retry
# --------------------------------------------------------------------------- #


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/",
            "headers": [(b"user-agent", b"b10-real-pg")],
            "client": ("127.0.0.1", 12345),
            "query_string": b"",
        }
    )


async def _parked_unit(db: PostgresDB, thread_id: UUID, reason: str) -> None:
    async with db.acquire() as conn:
        await record_input_seq(
            conn,
            unit_id=thread_id,
            unit_kind=UNIT_KIND_SESSION_TURN,
            input_seq=1,
            fair_key=None,
        )
        claim = await claim_unit(
            conn,
            unit_kind=UNIT_KIND_SESSION_TURN,
            pod_name="pod-a",
            prefer_unit_id=thread_id,
            affinity_grace_seconds=0,
        )
        assert claim is not None
        assert (
            await park_unit(
                conn, unit_id=thread_id, lease_token=claim.lease_token, reason=reason
            )
            == "parked"
        )


def _transport(db: PostgresDB, owner: UUID, thread: dict):
    async def gate(request, store, thread_id):
        del request
        assert store is db and thread_id == str(thread["id"])
        return {"id": str(owner), "is_admin": False}, thread

    return thread_transport.ThreadTransportDependencies(
        store=db,
        require_thread_owner=gate,
        require_approved_user=AsyncMock(),
        forwarding=SimpleNamespace(),
        stateless_input=_admission(db),
        turn_locks=SimpleNamespace(),
    )


@pytest.mark.asyncio
async def test_owner_retry_revives_a_retryable_parked_unit_exactly_once(db):
    owner, thread_id = await _thread(db)
    await _parked_unit(db, thread_id, PARK_REASON_ATTACH_FAILED)
    deps = _transport(db, owner, await _thread_row(db, thread_id))

    revived = await thread_transport.thread_queue_retry(
        str(thread_id), _request(), dependencies=deps
    )
    assert revived["state"] == "queued"
    with pytest.raises(HTTPException) as again:
        await thread_transport.thread_queue_retry(
            str(thread_id), _request(), dependencies=deps
        )
    assert (again.value.status_code, again.value.detail) == (404, "Unit is not parked")
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM security_events WHERE event_type='queue_retry' "
                "AND resource_id=$1",
                str(thread_id),
            )
            == 1
        )


@pytest.mark.asyncio
async def test_owner_retry_refuses_a_claim_loss_hold(db):
    owner, thread_id = await _thread(db)
    await _parked_unit(db, thread_id, PARK_REASON_CLAIM_LOSS_HOLD)
    deps = _transport(db, owner, await _thread_row(db, thread_id))
    with pytest.raises(HTTPException) as refused:
        await thread_transport.thread_queue_retry(
            str(thread_id), _request(), dependencies=deps
        )
    assert refused.value.status_code == 409
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT state FROM run_queue WHERE unit_id=$1", thread_id
            )
            == "parked"
        )


# --------------------------------------------------------------------------- #
# Permission decision and magic links
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_permission_decision_is_one_cas_per_request(db):
    _owner, thread_id = await _thread(db)
    request_id = await _permission(db, thread_id)
    decided = await thread_permission_operations.decide_permission_request(
        db, str(thread_id), str(request_id), "deny", decided_by="u-1"
    )
    assert (decided["status"], decided["decision"]) == ("denied", "deny")
    with pytest.raises(HTTPException) as again:
        await thread_permission_operations.decide_permission_request(
            db, str(thread_id), str(request_id), "approve", decided_by="u-1"
        )
    assert (again.value.status_code, again.value.detail) == (409, "Already denied")
    _other_owner, other_thread = await _thread(db)
    with pytest.raises(HTTPException) as wrong_thread:
        await thread_permission_operations.decide_permission_request(
            db, str(other_thread), str(request_id), "approve", decided_by="u-1"
        )
    assert wrong_thread.value.status_code == 404
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, decided_by, decided_at FROM thread_permission_requests "
            "WHERE id=$1",
            request_id,
        )
    assert (row["status"], row["decided_by"]) == ("denied", "u-1")
    assert row["decided_at"] is not None


def _permissions(db: PostgresDB, wake=None):
    return thread_permissions.ThreadPermissionDependencies(
        store=db,
        require_thread_owner=AsyncMock(),
        notification_service=SimpleNamespace(resolve_source=AsyncMock()),
        cockpit_url=lambda: "https://cockpit.test",
        wake_after_permission_decision=wake or AsyncMock(),
    )


async def _token(db, owner, thread_id, request_id, **kwargs) -> str:
    raw, _token_id = await headless_notifications.generate_magic_link_token(
        db,
        purpose="permission",
        user_id=str(owner),
        approval_id=str(request_id),
        thread_id=str(thread_id),
        intended_decision="approved",
        **kwargs,
    )
    return raw


@pytest.mark.asyncio
async def test_real_token_get_does_not_consume_post_consumes_once(db):
    owner, thread_id = await _thread(db)
    request_id = await _permission(db, thread_id)
    raw = await _token(db, owner, thread_id, request_id)
    wake = AsyncMock()
    deps = _permissions(db, wake)

    page = await thread_permissions.magic_link_get(raw, dependencies=deps)
    assert page.status_code == 200
    assert await headless_notifications.validate_magic_link(db, raw) is not None

    decided = await thread_permissions.magic_link_post(raw, dependencies=deps)
    await asyncio.sleep(0)
    assert decided.status_code == 200
    replay = await thread_permissions.magic_link_post(raw, dependencies=deps)
    assert replay.status_code == 404
    wake.assert_called_once_with(str(thread_id), permission_request_id=str(request_id))
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, decided_by FROM thread_permission_requests WHERE id=$1",
            request_id,
        )
        token = await conn.fetchrow(
            "SELECT used_at, consumed_decision FROM magic_link_tokens "
            "WHERE approval_id=$1",
            request_id,
        )
    assert (row["status"], row["decided_by"]) == ("approved", f"user:{owner}")
    assert token["used_at"] is not None and token["consumed_decision"] == "approved"


@pytest.mark.asyncio
async def test_real_token_expired_and_already_decided_paths(db):
    owner, thread_id = await _thread(db)
    request_id = await _permission(db, thread_id)
    expired = await _token(db, owner, thread_id, request_id, ttl_seconds=-1)
    deps = _permissions(db)
    assert (
        await thread_permissions.magic_link_get(expired, dependencies=deps)
    ).status_code == 404
    assert (
        await thread_permissions.magic_link_post(expired, dependencies=deps)
    ).status_code == 404

    live = await _token(db, owner, thread_id, request_id)
    await thread_permission_operations.decide_permission_request(
        db, str(thread_id), str(request_id), "deny", decided_by="cockpit"
    )
    assert (
        await thread_permissions.magic_link_get(live, dependencies=deps)
    ).status_code == 409
    lost = await thread_permissions.magic_link_post(live, dependencies=deps)
    assert lost.status_code == 409
    deps.wake_after_permission_decision.assert_not_called()


@pytest.mark.asyncio
async def test_real_token_extend_bumps_until_the_cap_without_consuming(db, monkeypatch):
    owner, thread_id = await _thread(db, lane="pinned", status="awaiting_user")
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET awaiting_user_since = now() - interval '50 minutes' "
            "WHERE id=$1",
            thread_id,
        )
    request_id = await _permission(db, thread_id)
    raw = await _token(db, owner, thread_id, request_id)
    monkeypatch.setattr(magic_link_pages, "MAGIC_EXTEND_CAP", 2)
    deps = _permissions(db)

    bodies = [
        (
            await thread_permissions.magic_link_extend(raw, dependencies=deps)
        ).body.decode()
        for _ in range(3)
    ]
    assert "1 extends remaining" in bodies[0]
    assert "0 extends remaining" in bodies[1]
    assert "Extend limit reached" in bodies[2]
    row = await _thread_row(db, thread_id)
    assert row["extend_count"] == 2
    assert await headless_notifications.validate_magic_link(db, raw) is not None


# --------------------------------------------------------------------------- #
# Decision wake fences
# --------------------------------------------------------------------------- #


def _attention(db: PostgresDB, **overrides):
    values = dict(
        store=db,
        container_provisioner=object(),
        workspace_suspension=SimpleNamespace(is_enabled=True),
        persistent_provisioner=None,
        persistent_thread_recycler=lambda: None,
        emit_session_provisioning_failure=AsyncMock(),
        thread_retirement_operations=MagicMock(),
        notification_service=object(),
        cockpit_url=lambda: "https://cockpit.test",
    )
    values.update(overrides)
    return session_attention.SessionAttentionDependencies(**values)


async def _queued_turn(db: PostgresDB, thread_id: UUID, *, consumed: bool) -> None:
    async with db.acquire() as conn:
        await record_input_seq(
            conn,
            unit_id=thread_id,
            unit_kind=UNIT_KIND_SESSION_TURN,
            input_seq=1,
            fair_key=None,
        )
        if consumed:
            claim = await claim_unit(
                conn,
                unit_kind=UNIT_KIND_SESSION_TURN,
                pod_name="pod-a",
                prefer_unit_id=thread_id,
                affinity_grace_seconds=0,
            )
            assert claim is not None
            assert (
                await complete_unit(
                    conn,
                    unit_id=thread_id,
                    lease_token=claim.lease_token,
                    consumed_seq=1,
                )
                == "done"
            )


@pytest.mark.asyncio
async def test_stateless_wake_takes_only_the_exact_decided_fresh_continuation(
    db, monkeypatch
):
    ensure = AsyncMock()
    monkeypatch.setattr(session_attention, "ensure_session_workspace", ensure)
    _owner, thread_id = await _thread(db, status="awaiting_user")
    await _queued_turn(db, thread_id, consumed=False)
    pending = await _permission(db, thread_id)
    decided = await _permission(db, thread_id, status="approved")

    await session_attention.wake_after_permission_decision(
        str(thread_id), permission_request_id=str(pending), dependencies=_attention(db)
    )
    assert (await _thread_row(db, thread_id))["status"] == "awaiting_user"
    ensure.assert_not_awaited()

    await session_attention.wake_after_permission_decision(
        str(thread_id), permission_request_id=str(decided), dependencies=_attention(db)
    )
    assert (await _thread_row(db, thread_id))["status"] == "active"
    ensure.assert_awaited_once()


@pytest.mark.asyncio
async def test_stateless_wake_never_revives_a_consumed_turn(db, monkeypatch):
    ensure = AsyncMock()
    monkeypatch.setattr(session_attention, "ensure_session_workspace", ensure)
    _owner, thread_id = await _thread(db, status="suspended")
    await _queued_turn(db, thread_id, consumed=True)
    decided = await _permission(db, thread_id, status="denied")

    await session_attention.wake_after_permission_decision(
        str(thread_id), permission_request_id=str(decided), dependencies=_attention(db)
    )
    assert (await _thread_row(db, thread_id))["status"] == "suspended"
    ensure.assert_not_awaited()
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT state FROM run_queue WHERE unit_id=$1", thread_id
            )
            == "done"
        )


@pytest.mark.asyncio
async def test_pinned_wake_publishes_only_to_the_exact_generation(db, monkeypatch):
    ensure = AsyncMock()
    monkeypatch.setattr(session_attention, "ensure_session_workspace", ensure)
    _owner, thread_id = await _thread(
        db, lane="pinned", status="awaiting_user", metadata={}
    )
    before = await _thread_row(db, thread_id)
    decided = await _permission(db, thread_id, status="approved")

    await session_attention.wake_after_permission_decision(
        str(thread_id), permission_request_id=str(decided), dependencies=_attention(db)
    )
    after = await _thread_row(db, thread_id)
    assert after["status"] == "active"
    assert after["runtime_generation"] == before["runtime_generation"]

    # A wake that captured an older life's generation must not wake this one.
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET status='awaiting_user' WHERE id=$1", thread_id
        )
    stale = dict(before)
    stale["status"] = "awaiting_user"
    stale["runtime_generation"] = uuid4()
    real_get = db.get_thread
    first = True

    async def get_thread_once_stale(tid):
        nonlocal first
        if first:
            first = False
            return stale
        return await real_get(tid)

    monkeypatch.setattr(db, "get_thread", get_thread_once_stale)
    await session_attention.wake_after_permission_decision(
        str(thread_id), permission_request_id=str(decided), dependencies=_attention(db)
    )
    assert (await real_get(str(thread_id)))["status"] == "awaiting_user"


# --------------------------------------------------------------------------- #
# Sweep selects
# --------------------------------------------------------------------------- #


async def _one_tick(body, deps) -> None:
    shutdown = asyncio.Event()

    async def stop_soon():
        await asyncio.sleep(0.5)
        shutdown.set()

    stopper = asyncio.create_task(stop_soon())
    await asyncio.wait_for(body(shutdown, dependencies=deps), timeout=10)
    await stopper


@pytest.mark.asyncio
async def test_attention_select_finds_only_expired_non_officer_pinned_waits(
    db, monkeypatch
):
    monkeypatch.setattr(session_attention, "ATTENTION_SLEEP_INTERVAL_S", 3600)
    monkeypatch.setattr(session_attention, "ATTENTION_SLEEP_MINUTES", 60)
    _o1, expired = await _thread(
        db,
        lane="pinned",
        status="awaiting_user",
        metadata={"config_override": {"headless": {"attention_sleep_minutes": 1}}},
    )
    _o2, fresh = await _thread(db, lane="pinned", status="awaiting_user", metadata={})
    _o3, officer = await _thread(
        db,
        lane="pinned",
        status="awaiting_user",
        metadata={
            "config_override": {
                "officer": {"enabled": True},
                "headless": {"attention_sleep_minutes": 1},
            }
        },
    )
    _o4, never = await _thread(
        db,
        lane="pinned",
        status="awaiting_user",
        metadata={"config_override": {"headless": {"attention_sleep_minutes": 0}}},
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE threads SET awaiting_user_since = now() - interval '5 minutes'"
        )
    ended = AsyncMock(return_value={"status": "suspended"})
    retirement = SimpleNamespace(end_thread_flow=ended)
    await _one_tick(
        session_attention.attention_sleep_sweeper,
        _attention(db, thread_retirement_operations=lambda: retirement),
    )
    assert [call.args[0] for call in ended.await_args_list] == [str(expired)]
    kwargs = ended.await_args.kwargs
    assert kwargs["settle_status"] == "suspended" and kwargs["permanent"] is False
    del fresh, officer, never


@pytest.mark.asyncio
async def test_permission_reminder_select_records_each_aged_unrecorded_gate(
    db, monkeypatch
):
    monkeypatch.setenv("HEADLESS_NOTIFY_AGE_S", "0")
    monkeypatch.setenv("HEADLESS_NOTIFY_INTERVAL_S", "3600")
    _owner, thread_id = await _thread(db)
    request_id = await _permission(db, thread_id)
    await _permission(db, thread_id, status="approved")
    record = AsyncMock(return_value={"status": "recorded"})
    monkeypatch.setattr(headless_notifications, "record_permission_pending", record)
    await asyncio.sleep(0.05)

    await _one_tick(session_attention.thread_permission_notify_sweeper, _attention(db))

    assert [call.kwargs["row"]["id"] for call in record.await_args_list] == [request_id]
    assert record.await_args.kwargs["cockpit_external_url"] == "https://cockpit.test"


# --------------------------------------------------------------------------- #
# SSE replay on the real journal
# --------------------------------------------------------------------------- #


class _StreamRequest:
    def __init__(self, cursor: str | None, polls: int = 1) -> None:
        self.headers = {"Last-Event-ID": cursor} if cursor else {}
        self.query_params: dict[str, str] = {}
        self._polls = polls

    async def is_disconnected(self) -> bool:
        self._polls -= 1
        return self._polls < 0


async def _frames(thread: dict, db: PostgresDB, cursor: str | None) -> list[str]:
    response = await thread_event_stream.open_thread_event_stream(
        str(thread["id"]),
        _StreamRequest(cursor),
        user={
            "id": str(thread["user_id"]),
            "auth_method": "pat",
            "scopes": ["chat:read"],
        },
        thread=thread,
        dependencies=thread_event_stream.ThreadEventStreamDependencies(
            store=db, require_thread_owner=AsyncMock()
        ),
    )
    frames = []
    async for chunk in response.body_iterator:
        frames.append(chunk if isinstance(chunk, str) else chunk.decode())
    return frames


@pytest.mark.asyncio
async def test_stream_replays_after_cursor_and_anchors_past_completed_turns(db):
    _owner, thread_id = await _thread(db, lane="pinned", metadata={})
    async with db.acquire() as conn:
        for seq, kind in enumerate(
            ["turn.started", "turn.completed", "turn.started", "tool.started"], 1
        ):
            await conn.execute(
                "INSERT INTO thread_events (thread_id, epoch, seq, kind, payload) "
                "VALUES ($1, 0, $2, $3, '{}'::jsonb)",
                thread_id,
                seq,
                kind,
            )
    thread = await _thread_row(db, thread_id)

    after_cursor = await _frames(thread, db, "0:1")
    assert after_cursor[0] == ": open\n\n"
    assert [f.split("\n")[0] for f in after_cursor[1:]] == [
        "id: 0:2",
        "id: 0:3",
        "id: 0:4",
    ]

    no_cursor = await _frames(thread, db, None)
    assert [f.split("\n")[0] for f in no_cursor[1:]] == ["id: 0:3", "id: 0:4"]

    mismatched = await _frames(thread, db, "7:2")
    assert "gone_beyond_horizon" in mismatched[1] and "epoch_mismatch" in mismatched[1]
    assert json.loads(mismatched[1].split("data: ")[1])["params"]["server_seq"] == 4
