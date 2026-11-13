"""The chat-pool stall rule, its cached reader, and the admin notification."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from orchestrator.services import chat_pool_stall as stall
from orchestrator.services.stateless_capacity import QueueDemand

T0 = datetime(2026, 11, 13, 10, 0, 0, tzinfo=timezone.utc)


def _demand(
    *,
    session_turn=0,
    busy=0,
    age=None,
    runnable_at=None,
    worker_batch=0,
    oldest_queued_age_s=None,
):
    return QueueDemand(
        observed_at=T0,
        busy=busy,
        runnable_session_turn=session_turn,
        runnable_worker_batch=worker_batch,
        runnable_total=session_turn + worker_batch,
        oldest_queued_age_s=oldest_queued_age_s,
        oldest_session_turn_runnable_at=runnable_at,
        oldest_session_turn_age_s=age,
    )


@pytest.mark.parametrize(
    ("demand", "expected"),
    [
        (_demand(session_turn=1, busy=0, age=301), True),
        (_demand(session_turn=1, busy=1, age=3600), False),
        (_demand(session_turn=0, busy=0, age=None), False),
        (_demand(session_turn=1, busy=0, age=299), False),
        (
            _demand(
                session_turn=0,
                worker_batch=3,
                busy=0,
                oldest_queued_age_s=9999,
            ),
            False,
        ),
    ],
)
def test_stalled_needs_runnable_turns_no_leases_and_age(demand, expected):
    assert stall.chat_pool_stalled(demand, threshold_s=300) is expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(None, 300.0), ("30", 60.0), ("abc", 300.0), ("600", 600.0)],
)
def test_threshold_env_default_floor_and_garbage(monkeypatch, raw, expected):
    if raw is None:
        monkeypatch.delenv(stall.STALL_THRESHOLD_ENV, raising=False)
    else:
        monkeypatch.setenv(stall.STALL_THRESHOLD_ENV, raw)
    assert stall.stall_threshold_s() == expected


def _stalled(runnable_at=T0, age=900.0, n=2):
    return _demand(session_turn=n, age=age, runnable_at=runnable_at)


def _watch(monkeypatch, demands):
    seq = iter(demands)

    async def fake_read_demand(conn):
        return next(seq)

    monkeypatch.setattr(stall, "read_demand", fake_read_demand)
    notifications = MagicMock()
    notifications.record_chat_executors_unavailable = AsyncMock()
    notifications.resolve_source = AsyncMock()
    return stall.ChatPoolStallWatch(
        MagicMock(), notifications, threshold_s=300.0
    ), notifications


@pytest.mark.asyncio
async def test_watch_notifies_once_per_incident(monkeypatch):
    other = T0 + timedelta(hours=1)
    watch, notes = _watch(
        monkeypatch, [_stalled(), _stalled(), _stalled(runnable_at=other)]
    )
    for _ in range(2):
        assert await watch.run_once() is True
    notes.record_chat_executors_unavailable.assert_awaited_once_with(
        incident="2026-11-13T10:00:00+00:00", runnable=2, oldest_age_s=900.0
    )
    await watch.run_once()
    assert notes.record_chat_executors_unavailable.await_count == 2
    assert (
        notes.record_chat_executors_unavailable.await_args.kwargs["incident"]
        == "2026-11-13T11:00:00+00:00"
    )


@pytest.mark.asyncio
async def test_watch_resolves_when_the_stall_clears(monkeypatch):
    watch, notes = _watch(monkeypatch, [_stalled(), _demand(), _demand()])
    for _ in range(3):
        await watch.run_once()
    notes.resolve_source.assert_awaited_once_with(
        "chat_pool", "stateless", resolved_by="system", raise_on_error=True
    )


@pytest.mark.asyncio
async def test_watch_resolves_once_after_start(monkeypatch):
    watch, notes = _watch(monkeypatch, [_demand(), _demand()])
    await watch.run_once()
    await watch.run_once()
    notes.resolve_source.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        RuntimeError("down"),
        {"error": "admin lookup failed"},
        {"notified": 1, "failed": 1, "notification_ids": ["a"]},
    ],
    ids=["raises", "error-dict", "partial"],
)
async def test_failed_alert_is_retried_next_tick_then_stops(monkeypatch, failure):
    watch, notes = _watch(monkeypatch, [_stalled(), _stalled(), _stalled()])
    ok = {"notified": 2, "failed": 0, "notification_ids": ["a", "b"]}
    effects = [failure, ok]
    notes.record_chat_executors_unavailable.side_effect = effects
    for _ in range(3):
        assert await watch.run_once() is True  # never raises
    assert notes.record_chat_executors_unavailable.await_count == 2
    incidents = {
        c.kwargs["incident"]
        for c in notes.record_chat_executors_unavailable.await_args_list
    }
    assert incidents == {"2026-11-13T10:00:00+00:00"}


@pytest.mark.asyncio
async def test_failed_resolve_is_retried_next_healthy_tick_then_stops(monkeypatch):
    watch, notes = _watch(monkeypatch, [_stalled(), _demand(), _demand(), _demand()])
    notes.record_chat_executors_unavailable.return_value = {"notified": 1, "failed": 0}
    notes.resolve_source.side_effect = [RuntimeError("db down"), []]
    for _ in range(4):
        await watch.run_once()
    assert notes.resolve_source.await_count == 2


@pytest.mark.asyncio
async def test_read_chat_pool_stalled_caches_for_ten_seconds(monkeypatch):
    stall._reset_cache()
    calls = []

    async def fake_read_demand(conn):
        calls.append(1)
        return _stalled()

    monkeypatch.setattr(stall, "read_demand", fake_read_demand)
    monkeypatch.delenv(stall.STALL_THRESHOLD_ENV, raising=False)
    now = [100.0]
    assert await stall.read_chat_pool_stalled(object(), clock=lambda: now[0]) is True
    now[0] = 109.9
    await stall.read_chat_pool_stalled(object(), clock=lambda: now[0])
    assert len(calls) == 1
    now[0] = 110.1
    await stall.read_chat_pool_stalled(object(), clock=lambda: now[0])
    assert len(calls) == 2
    stall._reset_cache()


@pytest.mark.asyncio
async def test_record_chat_executors_unavailable_notifies_every_admin():
    from orchestrator.services.notification_service import (
        NotificationService,
        RecordResult,
    )

    svc = NotificationService()
    db = MagicMock()
    admin1, admin2 = str(uuid4()), str(uuid4())
    db.list_admin_user_ids = AsyncMock(return_value=[admin1, admin2])
    svc.connect(db, MagicMock(), MagicMock())
    svc.record = AsyncMock(
        side_effect=lambda **kw: RecordResult(
            f"n-{kw['recipient_id'][:4]}", True, {"in_app": True}
        )
    )
    res = await svc.record_chat_executors_unavailable(
        incident="2026-11-13T10:00:00+00:00", runnable=3, oldest_age_s=905.0
    )
    assert res["notified"] == 2
    calls = [c.kwargs for c in svc.record.await_args_list]
    assert {c["recipient_id"] for c in calls} == {admin1, admin2}
    for c in calls:
        assert c["category"] == "chat_executors_unavailable"
        assert c["dedup_key"] == "chat_executors_unavailable:2026-11-13T10:00:00+00:00"
        assert (c["source_kind"], c["source_id"]) == ("chat_pool", "stateless")
        assert c["subject"] == "Chat is unavailable: no agent is taking messages"
        assert c["body"] == (
            "3 chat message(s) have been waiting for 15 min and no chat executor "
            "is working on any of them. Open Admin › Capacity and check the "
            "srw-agent-stateless pods."
        )


@pytest.mark.asyncio
async def test_resolve_source_raise_on_error_distinguishes_failure():
    from orchestrator.services.notification_service import NotificationService

    svc = NotificationService()
    db = MagicMock()
    db.resolve_notifications_by_source = AsyncMock(side_effect=RuntimeError("x"))
    svc.connect(db, MagicMock(), MagicMock())
    assert (
        await svc.resolve_source("chat_pool", "stateless", resolved_by="system") == []
    )
    with pytest.raises(RuntimeError):
        await svc.resolve_source(
            "chat_pool", "stateless", resolved_by="system", raise_on_error=True
        )


@pytest.mark.asyncio
async def test_record_chat_executors_unavailable_counts_a_failed_admin_row():
    from orchestrator.services.notification_service import (
        NotificationService,
        RecordResult,
    )

    svc = NotificationService()
    db = MagicMock()
    good, bad = str(uuid4()), str(uuid4())
    db.list_admin_user_ids = AsyncMock(return_value=[bad, good])
    svc.connect(db, MagicMock(), MagicMock())

    async def record(**kw):
        if kw["recipient_id"] == bad:
            raise RuntimeError("db blip")
        return RecordResult("n-1", True, {"in_app": True})

    svc.record = AsyncMock(side_effect=record)
    res = await svc.record_chat_executors_unavailable(
        incident="2026-11-13T10:00:00+00:00", runnable=1, oldest_age_s=400.0
    )
    assert res["notified"] == 1
    assert res["failed"] == 1
