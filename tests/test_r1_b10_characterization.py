"""R1.B10 characterization: behaviour the extraction must keep, pinned first.

These cases were first written against the pre-extraction application (routes
declared directly on ``orchestrator.main``) and committed before any B10 code
moved. They cover wire behaviour no existing suite asserted: the session
citation read, the owner permission-decision endpoint, magic-link GET/POST
refusals and decision labels, and the Officer daily-ceiling brake reached
through the application's own usage ledger rather than an explicit test ledger.

After the extraction only the surfaces changed: the routes are mounted from
their new routers with explicit per-application factories. Every assertion is
unchanged. The ceiling brake still reaches the ledger through
``session_wake``'s own application lookup here; closing that caller is the
next, separate step.
"""

from __future__ import annotations

import contextlib
import json
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

import orchestrator.main as main
from orchestrator.routers import thread_history, thread_permissions
from orchestrator.services import session_wake

from ._mounted_router import mount_router

THREAD_ID = "11111111-2222-4333-8444-555555555555"
APPROVAL_ID = "99999999-8888-4777-8666-555555555555"
OWNER = {"id": "owner-1", "is_admin": False}


class _Conn:
    """Serves scripted ``fetchrow``/``fetch`` answers in order; records calls."""

    def __init__(self, fetchrow=(), fetch=(), fail: Exception | None = None):
        self._fetchrow = list(fetchrow)
        self._fetch = list(fetch)
        self._fail = fail
        self.calls: list[tuple[str, str, tuple]] = []

    async def fetchrow(self, query, *args):
        self.calls.append(("fetchrow", query, args))
        if self._fail is not None:
            raise self._fail
        return self._fetchrow.pop(0) if self._fetchrow else None

    async def fetch(self, query, *args):
        self.calls.append(("fetch", query, args))
        if self._fail is not None:
            raise self._fail
        return self._fetch.pop(0) if self._fetch else []


class _Store:
    def __init__(self, conn: _Conn):
        self.conn = conn

    @contextlib.asynccontextmanager
    async def acquire(self):
        yield self.conn


def _owner_gate(*, raises: HTTPException | None = None):
    thread = {"id": THREAD_ID, "user_id": OWNER["id"], "metadata": {}}
    if raises is not None:
        return AsyncMock(side_effect=raises)
    return AsyncMock(return_value=(dict(OWNER), thread))


# --------------------------------------------------------------------------- #
# Application surfaces (the extracted routers, one application each)
# --------------------------------------------------------------------------- #


def _history_client(monkeypatch, *, vector_store, gate):
    del monkeypatch
    app = mount_router(
        thread_history.router,
        factories={
            "thread_history_dependencies_factory": (
                lambda: thread_history.ThreadHistoryDependencies(
                    store=object(),
                    vector_db=vector_store,
                    require_thread_owner=gate,
                )
            )
        },
    )
    return TestClient(app, raise_server_exceptions=False)


def _permission_client(monkeypatch, *, store, gate, notifier, wake):
    del monkeypatch
    app = mount_router(
        thread_permissions.router,
        factories={
            "thread_permission_dependencies_factory": (
                lambda: thread_permissions.ThreadPermissionDependencies(
                    store=store,
                    require_thread_owner=gate,
                    notification_service=notifier,
                    cockpit_url=lambda: "https://cockpit.test",
                    wake_after_permission_decision=wake,
                )
            )
        },
    )
    return TestClient(app, raise_server_exceptions=False)


def _patch_magic(monkeypatch, *, validate, consume=None):
    from orchestrator.services import headless_notifications

    monkeypatch.setattr(headless_notifications, "validate_magic_link", validate)
    consume = consume if consume is not None else AsyncMock()
    monkeypatch.setattr(headless_notifications, "consume_magic_link", consume)
    return consume


# --------------------------------------------------------------------------- #
# Session citations
# --------------------------------------------------------------------------- #


def test_citations_are_owner_gated_counted_and_never_return_raw_metadata(
    monkeypatch,
):
    created = datetime(2026, 9, 23, 10, 0, tzinfo=timezone.utc)
    conn = _Conn(
        fetchrow=[{"total": 2}],
        fetch=[
            [
                {
                    "id": 7,
                    "claim": "a claim",
                    "source_id": 3,
                    "source_name": "Doc",
                    "source_type": "document",
                    "source_identifier": "doc://1",
                    "verification_status": "verified",
                    "confidence": "high",
                    "created_at": created,
                    "metadata": json.dumps(
                        {"cloud": {"snapshot_blob_key": "blob/7", "anchor": "x"}}
                    ),
                },
                {
                    "id": 8,
                    "claim": "web claim",
                    "source_id": 4,
                    "source_name": "Web",
                    "source_type": "website",
                    "source_identifier": "https://example.test",
                    "verification_status": "pending",
                    "confidence": "medium",
                    "created_at": created,
                    "metadata": None,
                },
            ]
        ],
    )
    gate = _owner_gate()
    client = _history_client(monkeypatch, vector_store=_Store(conn), gate=gate)

    response = client.get(
        f"/api/persistent/threads/{THREAD_ID}/citations?limit=5&offset=1"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert body["thread_id"] == THREAD_ID
    assert [c["id"] for c in body["citations"]] == [7, 8]
    assert all("metadata" not in c for c in body["citations"])
    gate.assert_awaited_once()
    assert gate.await_args.args[2] == THREAD_ID
    select = [call for call in conn.calls if call[0] == "fetch"][0]
    assert select[2] == (THREAD_ID, 5, 1)


@pytest.mark.parametrize("query", ["limit=0", "limit=501", "offset=-1"])
def test_citation_paging_bounds_are_validated(monkeypatch, query):
    client = _history_client(
        monkeypatch, vector_store=_Store(_Conn()), gate=_owner_gate()
    )
    response = client.get(f"/api/persistent/threads/{THREAD_ID}/citations?{query}")
    assert response.status_code == 422


def test_citation_owner_refusal_happens_before_any_read(monkeypatch):
    conn = _Conn()
    client = _history_client(
        monkeypatch,
        vector_store=_Store(conn),
        gate=_owner_gate(raises=HTTPException(status_code=403, detail="Not yours")),
    )
    response = client.get(f"/api/persistent/threads/{THREAD_ID}/citations")
    assert response.status_code == 403
    assert conn.calls == []


def test_citation_read_failure_is_a_500(monkeypatch):
    client = _history_client(
        monkeypatch,
        vector_store=_Store(_Conn(fail=RuntimeError("vector down"))),
        gate=_owner_gate(),
    )
    response = client.get(f"/api/persistent/threads/{THREAD_ID}/citations")
    assert response.status_code == 500


# --------------------------------------------------------------------------- #
# Owner permission decision
# --------------------------------------------------------------------------- #


def _approve(client, decision):
    return client.post(
        f"/api/persistent/threads/{THREAD_ID}/approve/{APPROVAL_ID}",
        json={"decision": decision},
    )


def test_owner_approve_decides_once_and_resolves_the_notification(monkeypatch):
    conn = _Conn(
        fetchrow=[
            {"id": APPROVAL_ID, "status": "pending", "tool_call_id": "call-1"},
            {"id": APPROVAL_ID, "status": "approved", "tool_call_id": "call-1"},
        ]
    )
    notifier = SimpleNamespace(resolve_source=AsyncMock())
    client = _permission_client(
        monkeypatch,
        store=_Store(conn),
        gate=_owner_gate(),
        notifier=notifier,
        wake=AsyncMock(),
    )

    response = _approve(client, "approve")

    assert response.status_code == 200
    assert response.json() == {
        "accepted": True,
        "decision": "approve",
        "approval_id": APPROVAL_ID,
        "status": "approved",
        "tool_call_id": "call-1",
    }
    lookup, update = conn.calls
    assert lookup[2] == (APPROVAL_ID, THREAD_ID)
    assert update[2] == (APPROVAL_ID, "approved", OWNER["id"])
    assert "status = 'pending'" in update[1]
    notifier.resolve_source.assert_awaited_once_with(
        "permission_request", APPROVAL_ID, resolved_by=f"user:{OWNER['id']}"
    )


@pytest.mark.parametrize(
    ("rows", "decision", "status", "detail"),
    [
        ([], "maybe", 400, "decision must be 'approve' or 'deny'"),
        ([], "approve", 404, "Permission request not found for this thread"),
        (
            [{"id": APPROVAL_ID, "status": "denied", "tool_call_id": "c"}],
            "approve",
            409,
            "Already denied",
        ),
        (
            [{"id": APPROVAL_ID, "status": "pending", "tool_call_id": "c"}, None],
            "deny",
            409,
            "Already decided (race lost)",
        ),
    ],
)
def test_owner_approve_refusals_keep_their_shape_and_resolve_nothing(
    monkeypatch, rows, decision, status, detail
):
    notifier = SimpleNamespace(resolve_source=AsyncMock())
    client = _permission_client(
        monkeypatch,
        store=_Store(_Conn(fetchrow=rows)),
        gate=_owner_gate(),
        notifier=notifier,
        wake=AsyncMock(),
    )
    response = _approve(client, decision)
    assert response.status_code == status
    assert response.json()["detail"] == detail
    notifier.resolve_source.assert_not_awaited()


def test_owner_approve_is_owner_gated(monkeypatch):
    conn = _Conn()
    client = _permission_client(
        monkeypatch,
        store=_Store(conn),
        gate=_owner_gate(raises=HTTPException(status_code=403, detail="Not yours")),
        notifier=SimpleNamespace(resolve_source=AsyncMock()),
        wake=AsyncMock(),
    )
    assert _approve(client, "approve").status_code == 403
    assert conn.calls == []


# --------------------------------------------------------------------------- #
# Magic links
# --------------------------------------------------------------------------- #


def test_magic_get_invalid_token_is_404_and_consumes_nothing(monkeypatch):
    client = _permission_client(
        monkeypatch,
        store=_Store(_Conn()),
        gate=_owner_gate(),
        notifier=SimpleNamespace(),
        wake=AsyncMock(),
    )
    consume = _patch_magic(monkeypatch, validate=AsyncMock(return_value=None))
    response = client.get("/magic/approve/bad")
    assert response.status_code == 404
    assert "Link expired or already used" in response.text
    assert 'href="https://cockpit.test"' in response.text
    consume.assert_not_awaited()


def test_magic_get_decided_request_is_409(monkeypatch):
    client = _permission_client(
        monkeypatch,
        store=_Store(
            _Conn(
                fetchrow=[{"id": APPROVAL_ID, "tool_name": "t", "status": "approved"}]
            )
        ),
        gate=_owner_gate(),
        notifier=SimpleNamespace(),
        wake=AsyncMock(),
    )
    consume = _patch_magic(
        monkeypatch,
        validate=AsyncMock(return_value={"approval_id": APPROVAL_ID}),
    )
    response = client.get("/magic/approve/tok")
    assert response.status_code == 409
    assert "Already decided" in response.text
    consume.assert_not_awaited()


def test_magic_get_renders_a_confirmation_without_consuming(monkeypatch):
    long_args = {"cmd": "x" * 900}
    client = _permission_client(
        monkeypatch,
        store=_Store(
            _Conn(
                fetchrow=[
                    {
                        "id": APPROVAL_ID,
                        "tool_name": "run_command",
                        "tool_args": json.dumps(long_args),
                        "status": "pending",
                    }
                ]
            )
        ),
        gate=_owner_gate(),
        notifier=SimpleNamespace(),
        wake=AsyncMock(),
    )
    consume = _patch_magic(
        monkeypatch,
        validate=AsyncMock(
            return_value={"approval_id": APPROVAL_ID, "intended_decision": "denied"}
        ),
    )
    response = client.get("/magic/approve/tok")
    assert response.status_code == 200
    assert "Confirm: Deny" in response.text
    assert "… (truncated)" in response.text
    assert 'action="/magic/approve/tok"' in response.text
    assert 'action="/magic/extend/tok"' in response.text
    consume.assert_not_awaited()


@pytest.mark.parametrize(
    ("validated", "consumed", "cas_row", "status", "text"),
    [
        (None, None, None, 404, "Link expired or already used"),
        (
            {"id": "t1", "intended_decision": "approved"},
            None,
            None,
            409,
            "Already used",
        ),
        (
            {"id": "t1", "intended_decision": "approved"},
            {"approval_id": APPROVAL_ID, "user_id": "owner-1"},
            None,
            409,
            "Already decided",
        ),
    ],
)
def test_magic_post_refusals_schedule_no_wake(
    monkeypatch, validated, consumed, cas_row, status, text
):
    wake = AsyncMock()
    client = _permission_client(
        monkeypatch,
        store=_Store(_Conn(fetchrow=[cas_row])),
        gate=_owner_gate(),
        notifier=SimpleNamespace(),
        wake=wake,
    )
    _patch_magic(
        monkeypatch,
        validate=AsyncMock(return_value=validated),
        consume=AsyncMock(return_value=consumed),
    )
    response = client.post("/magic/approve/tok")
    assert response.status_code == status
    assert text in response.text
    wake.assert_not_called()


@pytest.mark.parametrize(
    ("consumed_user", "decided_by"),
    [("owner-1", "user:owner-1"), (None, "magic_link")],
)
def test_magic_post_denial_labels_the_decider_and_fences_the_wake(
    monkeypatch, consumed_user, decided_by
):
    conn = _Conn(
        fetchrow=[
            {
                "id": APPROVAL_ID,
                "status": "denied",
                "tool_call_id": "call-1",
                "tool_name": "run_command",
                "thread_id": THREAD_ID,
            }
        ]
    )
    wake = AsyncMock()
    client = _permission_client(
        monkeypatch,
        store=_Store(conn),
        gate=_owner_gate(),
        notifier=SimpleNamespace(),
        wake=wake,
    )
    consume = _patch_magic(
        monkeypatch,
        validate=AsyncMock(return_value={"id": "t1", "intended_decision": "denied"}),
        consume=AsyncMock(
            return_value={"approval_id": APPROVAL_ID, "user_id": consumed_user}
        ),
    )
    response = client.post("/magic/approve/tok")
    assert response.status_code == 200
    assert "Tool denied" in response.text
    consume.assert_awaited_once()
    assert consume.await_args.args[1:] == ("t1", "denied")
    (update,) = conn.calls
    assert update[2] == (APPROVAL_ID, "denied", decided_by)
    wake.assert_called_once_with(THREAD_ID, permission_request_id=APPROVAL_ID)


def test_magic_extend_without_a_thread_is_400(monkeypatch):
    client = _permission_client(
        monkeypatch,
        store=_Store(_Conn()),
        gate=_owner_gate(),
        notifier=SimpleNamespace(),
        wake=AsyncMock(),
    )
    _patch_magic(
        monkeypatch,
        validate=AsyncMock(
            return_value={"approval_id": APPROVAL_ID, "thread_id": None}
        ),
    )
    response = client.post("/magic/extend/tok")
    assert response.status_code == 400
    assert "This link is not bound to a thread." in response.text


def test_magic_extend_on_a_decided_request_is_a_200_already_decided(monkeypatch):
    conn = _Conn(fetchrow=[{"extend_count": 1}, {"tool_name": "t", "status": "denied"}])
    client = _permission_client(
        monkeypatch,
        store=_Store(conn),
        gate=_owner_gate(),
        notifier=SimpleNamespace(),
        wake=AsyncMock(),
    )
    consume = _patch_magic(
        monkeypatch,
        validate=AsyncMock(
            return_value={"approval_id": APPROVAL_ID, "thread_id": THREAD_ID}
        ),
    )
    response = client.post("/magic/extend/tok")
    assert response.status_code == 200
    assert "Already decided" in response.text
    consume.assert_not_awaited()
    bump = conn.calls[0]
    assert "status = 'awaiting_user'" in bump[1]
    assert bump[2][0] == THREAD_ID


# --------------------------------------------------------------------------- #
# Officer daily-ceiling brake through the application's own ledger
# --------------------------------------------------------------------------- #


def _ceiling_thread(ceiling: int) -> dict:
    return {
        "id": THREAD_ID,
        "user_id": "legate-1",
        "metadata": {
            "config_override": {
                "officer": {"enabled": True, "daily_token_ceiling": ceiling}
            }
        },
    }


def _ledger(tokens: int, *, available: bool = True, raises: bool = False):
    ledger = SimpleNamespace(is_available=available)
    if raises:
        ledger.query_usage = AsyncMock(side_effect=RuntimeError("metering down"))
    else:
        ledger.query_usage = AsyncMock(
            return_value={
                "by_category": [{"unit": "prompt-token", "quantity": tokens}],
                "total_cost_usd": 0.0,
            }
        )
    return ledger


def _drain_store():
    delivery_id = str(uuid.uuid4())
    rows = [{"id": 5, "thread_id": THREAD_ID, "source": "timer", "dedup_key": "timer"}]
    assigned = [{**rows[0], "delivery_id": delivery_id}]
    return SimpleNamespace(
        claim_pending_session_wake_events=AsyncMock(return_value=rows),
        assign_session_wake_delivery_groups=AsyncMock(return_value=assigned),
        get_session_wake_delivery_group=AsyncMock(return_value=assigned),
        get_thread=AsyncMock(return_value=_ceiling_thread(1_000)),
        defer_session_wake_events=AsyncMock(),
        persist_thread_input_delivery=AsyncMock(
            return_value={"thread_id": THREAD_ID, "state": "admitted"}
        ),
        finish_session_wake_events=AsyncMock(),
        defer_session_wake_events_for_input=AsyncMock(),
        release_session_wake_events=AsyncMock(),
        merge_thread_officer_state=AsyncMock(),
    )


def _stub_sitrep(monkeypatch):
    from orchestrator.services import notification_service as ns
    from orchestrator.services import sitrep

    monkeypatch.setattr(
        sitrep, "build_wake_message", AsyncMock(return_value=("wake", None))
    )
    monkeypatch.setattr(ns.notification_service, "record", AsyncMock())


@pytest.mark.asyncio
async def test_over_budget_officer_wake_defers_to_utc_midnight_through_app_ledger(
    monkeypatch,
):
    _stub_sitrep(monkeypatch)
    ledger = _ledger(1_000)
    monkeypatch.setattr(main, "usage_ledger", ledger)
    store = _drain_store()

    delivered = await session_wake.drain_pending_event_wakes(store)

    assert delivered == 0
    ledger.query_usage.assert_awaited_once()
    assert ledger.query_usage.await_args.kwargs["ref_id"] == THREAD_ID
    store.defer_session_wake_events.assert_awaited_once()
    fire_at = store.defer_session_wake_events.await_args.kwargs["fire_at"]
    now = datetime.now(timezone.utc)
    assert fire_at == (now + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    store.persist_thread_input_delivery.assert_not_awaited()
    store.merge_thread_officer_state.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ledger",
    [
        None,
        _ledger(1_000, available=False),
        _ledger(1_000, raises=True),
        _ledger(999),
    ],
    ids=["no-ledger", "unavailable", "query-fails", "under-budget"],
)
async def test_officer_wake_fails_open_or_delivers_under_budget(monkeypatch, ledger):
    _stub_sitrep(monkeypatch)
    monkeypatch.setattr(main, "usage_ledger", ledger)
    store = _drain_store()

    delivered = await session_wake.drain_pending_event_wakes(store)

    assert delivered == 1
    store.defer_session_wake_events.assert_not_awaited()
    store.persist_thread_input_delivery.assert_awaited_once()
    store.finish_session_wake_events.assert_awaited_once_with([5])
