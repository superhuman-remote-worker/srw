"""Unified notification feed against a real PostgreSQL (migration 0193).

Mocked ``conn.execute`` cannot catch what these are for: the idempotent
insert racing itself, the partial-unique claim index, keyset pagination with
tied timestamps, and the CHECK constraints. Guarded testcontainers fixture
(skips without a container runtime); applies ``schema_current.sql`` once, so
a stale snapshot fails here before it fails on k3d.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import asyncpg
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.postgres import PostgresDB
from orchestrator.security import crypto
from orchestrator.services.notification_catalog import notification_id
from orchestrator.services.notification_service import NotificationService
from orchestrator.services.notification_feed import NotificationFeedService

SCHEMA_FILE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "orchestrator"
    / "database"
    / "schema_current.sql"
)

USER = str(uuid.uuid4())
OTHER = str(uuid.uuid4())


@pytest.mark.asyncio
async def test_lifecycle_hint_crosses_real_postgres_replicas_only_for_owner(
    pg_dsn, monkeypatch
):
    """Separate LISTEN leases, pg_notify, and a killed listener prove recovery."""
    from orchestrator.services import notification_feed as feed_module

    monkeypatch.setattr(feed_module, "_LISTENER_CHECK_SECONDS", 0.05)
    monkeypatch.setattr(feed_module, "_RECONNECT_MIN_SECONDS", 0.3)
    first_db = PostgresDB(
        connection_string=pg_dsn, min_connections=1, max_connections=3
    )
    second_db = PostgresDB(
        connection_string=pg_dsn,
        min_connections=1,
        max_connections=3,
        server_settings={"application_name": "srw_lifecycle_test_replica_b"},
    )
    await first_db.connect()
    await second_db.connect()
    first, second = NotificationFeedService(), NotificationFeedService()
    stop_first, stop_second = asyncio.Event(), asyncio.Event()
    first_task = asyncio.create_task(first.run_lifecycle_bridge(first_db, stop_first))
    second_task = asyncio.create_task(
        second.run_lifecycle_bridge(second_db, stop_second)
    )
    try:
        await asyncio.wait_for(
            asyncio.gather(
                first.lifecycle_bridge_ready.wait(),
                second.lifecycle_bridge_ready.wait(),
            ),
            5,
        )
        owner, stranger, thread, generation = (str(uuid.uuid4()) for _ in range(4))
        local = first.subscribe_sse(owner)
        remote = second.subscribe_sse(owner)
        foreign = second.subscribe_sse(stranger)
        first.publish_lifecycle(
            owner, thread, "provisioning", session_runtime_generation=generation
        )
        for stream in (local, remote):
            hint = await asyncio.wait_for(stream.get(), 5)
            assert hint == {
                "type": "session.lifecycle",
                "thread_id": thread,
                "state": "provisioning",
                "session_runtime_generation": generation,
            }
        assert local.empty()  # receiving our own NOTIFY is not a second delivery
        assert foreign.empty()
        listener_pid = await first_db.fetchval(
            "SELECT pid FROM pg_stat_activity WHERE datname=current_database() "
            "AND application_name='srw_lifecycle_test_replica_b'"
        )
        assert listener_pid is not None
        assert await first_db.fetchval("SELECT pg_terminate_backend($1)", listener_pid)

        async def listener_lost():
            while second.lifecycle_bridge_ready.is_set():
                await asyncio.sleep(0.01)

        await asyncio.wait_for(listener_lost(), 5)
        assert await asyncio.wait_for(remote.get(), 5) is None
        gap = second.subscribe_sse(owner)
        assert await asyncio.wait_for(gap.get(), 1) is None

        async def listener_reinstalled():
            while True:
                pid = await first_db.fetchval(
                    "SELECT pid FROM pg_stat_activity WHERE datname=current_database() "
                    "AND application_name='srw_lifecycle_test_replica_b'"
                )
                if (
                    pid is not None
                    and pid != listener_pid
                    and second.lifecycle_bridge_ready.is_set()
                ):
                    return
                await asyncio.sleep(0.01)

        await asyncio.wait_for(listener_reinstalled(), 5)
        recovered = second.subscribe_sse(owner)
        new_generation = str(uuid.uuid4())
        first.publish_lifecycle(
            owner, thread, "ready", session_runtime_generation=new_generation
        )
        assert (await asyncio.wait_for(recovered.get(), 5))[
            "session_runtime_generation"
        ] == new_generation
        assert not first_task.done() and not second_task.done()
    finally:
        stop_first.set()
        stop_second.set()
        await asyncio.wait_for(asyncio.gather(first_task, second_task), 5)
        await first_db.close()
        await second_db.close()
    assert not first.lifecycle_bridge_ready.is_set()
    assert not second.lifecycle_bridge_ready.is_set()


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
async def db(pg_dsn, _schema_applied, monkeypatch):
    monkeypatch.setenv("APP_ENCRYPTION_KEY", "G" * 32)
    crypto.reset_cipher_cache()
    store = PostgresDB(connection_string=pg_dsn, min_connections=1, max_connections=8)
    await store.connect()
    async with store.acquire() as conn:
        await conn.execute(
            "TRUNCATE notifications, notification_deliveries, notification_steps CASCADE"
        )
    try:
        yield store
    finally:
        await store.close()
        crypto.reset_cipher_cache()


def _insert_kwargs(**overrides):
    kwargs = dict(
        recipient_kind="user",
        recipient_id=USER,
        category="review_queue",
        severity="normal",
        subject="Job abc completed — review required",
        body="body",
        source_kind="job",
        source_id="job-1",
        dedup_key="freeze_notification:cmd-1",
        actions=[{"type": "approve", "params": {"job_id": "job-1"}}],
        payload={"job_id": "job-1"},
    )
    kwargs.update(overrides)
    kwargs.setdefault(
        "notification_id",
        str(
            notification_id(
                kwargs["recipient_kind"], kwargs["recipient_id"], kwargs["dedup_key"]
            )
        ),
    )
    return kwargs


async def _count(db, sql, *args):
    async with db.acquire() as conn:
        return await conn.fetchval(sql, *args)


class TestInsertOnce:
    @pytest.mark.asyncio
    async def test_twenty_concurrent_inserts_yield_one_row_and_one_winner(self, db):
        results = await asyncio.gather(
            *[db.insert_notification_once(**_insert_kwargs()) for _ in range(20)]
        )
        assert results.count(True) == 1
        assert results.count(False) == 19
        assert await _count(db, "SELECT count(*) FROM notifications") == 1
        assert (
            await _count(
                db,
                "SELECT count(*) FROM notification_deliveries WHERE channel='in_app' AND state='sent'",
            )
            == 1
        )

    @pytest.mark.asyncio
    async def test_same_key_two_recipients_two_rows(self, db):
        assert await db.insert_notification_once(**_insert_kwargs()) is True
        assert (
            await db.insert_notification_once(**_insert_kwargs(recipient_id=OTHER))
            is True
        )
        assert await _count(db, "SELECT count(*) FROM notifications") == 2

    @pytest.mark.asyncio
    async def test_replay_with_a_different_identity_raises(self, db):
        await db.insert_notification_once(**_insert_kwargs())
        with pytest.raises(RuntimeError, match="different payload"):
            await db.insert_notification_once(**_insert_kwargs(category="incident"))
        with pytest.raises(RuntimeError, match="different payload"):
            await db.insert_notification_once(**_insert_kwargs(source_id="job-2"))
        # Subject/body drift on replay is tolerated — identity is the key.
        assert (
            await db.insert_notification_once(**_insert_kwargs(subject="edited"))
            is False
        )

    @pytest.mark.asyncio
    async def test_constraints_hold(self, db):
        with pytest.raises(asyncpg.CheckViolationError):
            await db.insert_notification_once(**_insert_kwargs(severity="urgent"))
        with pytest.raises(asyncpg.CheckViolationError):
            await db.insert_notification_once(
                **_insert_kwargs(source_kind="job", source_id=None)
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await db.insert_notification_once(**_insert_kwargs(dedup_key=""))


class TestDeliveryClaims:
    @pytest.mark.asyncio
    async def test_concurrent_claims_one_wins_and_failed_frees_the_slot(self, db):
        kwargs = _insert_kwargs()
        await db.insert_notification_once(**kwargs)
        nid = kwargs["notification_id"]
        claims = await asyncio.gather(
            *[
                db.claim_notification_delivery(
                    notification_id=nid, channel="email", recipient_address="a@b"
                )
                for _ in range(10)
            ]
        )
        won = [c for c in claims if c]
        assert len(won) == 1
        # A pending claim still holds the slot.
        assert (
            await db.claim_notification_delivery(notification_id=nid, channel="email")
            is None
        )
        assert await db.settle_notification_delivery(won[0], state="failed", error="x")
        # Settling twice is a no-op (the CAS on state='pending').
        assert not await db.settle_notification_delivery(won[0], state="sent")
        second = await db.claim_notification_delivery(
            notification_id=nid, channel="email"
        )
        assert second and second != won[0]
        assert await db.settle_notification_delivery(
            second, state="sent", provider_msg_id="<m@x>"
        )
        # A sent claim holds the slot for good.
        assert (
            await db.claim_notification_delivery(notification_id=nid, channel="email")
            is None
        )
        async with db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT attempt, state, provider_msg_id FROM notification_deliveries "
                "WHERE notification_id=$1 AND channel='email' ORDER BY attempt",
                uuid.UUID(nid),
            )
        assert [(r["attempt"], r["state"]) for r in rows] == [
            (1, "failed"),
            (2, "sent"),
        ]
        assert rows[1]["provider_msg_id"] == "<m@x>"

    @pytest.mark.asyncio
    async def test_channels_are_independent_slots(self, db):
        kwargs = _insert_kwargs()
        await db.insert_notification_once(**kwargs)
        nid = kwargs["notification_id"]
        assert await db.claim_notification_delivery(
            notification_id=nid, channel="email"
        )
        assert await db.claim_notification_delivery(notification_id=nid, channel="ntfy")


class TestFeed:
    async def _seed(self, db, n):
        ids = []
        for i in range(n):
            kwargs = _insert_kwargs(dedup_key=f"k{i}", source_id=f"job-{i}")
            await db.insert_notification_once(**kwargs)
            ids.append(kwargs["notification_id"])
        return ids

    @pytest.mark.asyncio
    async def test_keyset_pagination_with_tied_timestamps(self, db):
        ids = await self._seed(db, 7)
        # Force ties: every row gets the same created_at.
        async with db.acquire() as conn:
            await conn.execute("UPDATE notifications SET created_at = now()")
        seen: list[str] = []
        before = None
        pages = 0
        while True:
            rows, before = await db.list_notifications_page(
                recipient_kind="user", recipient_id=USER, before=before, limit=3
            )
            seen.extend(str(r["id"]) for r in rows)
            pages += 1
            if before is None:
                break
        assert pages == 3
        assert sorted(seen) == sorted(ids)
        assert len(seen) == len(set(seen))

    @pytest.mark.asyncio
    async def test_status_and_category_filters(self, db):
        await self._seed(db, 3)
        await db.insert_notification_once(
            **_insert_kwargs(dedup_key="inc", category="incident", severity="critical")
        )
        rows, _ = await db.list_notifications_page(
            recipient_kind="user", recipient_id=USER, categories=["incident"]
        )
        assert [r["category"] for r in rows] == ["incident"]
        await db.resolve_notifications_by_source(
            source_kind="job", source_id="job-0", resolved_by="user:x"
        )
        pending, _ = await db.list_notifications_page(
            recipient_kind="user", recipient_id=USER, status="pending"
        )
        assert len(pending) == 3
        resolved, _ = await db.list_notifications_page(
            recipient_kind="user", recipient_id=USER, status="resolved"
        )
        assert [r["source_id"] for r in resolved] == ["job-0"]
        with pytest.raises(ValueError):
            await db.list_notifications_page(
                recipient_kind="user", recipient_id=USER, status="bogus"
            )

    @pytest.mark.asyncio
    async def test_engagement_and_counts(self, db):
        ids = await self._seed(db, 3)
        counts = await db.get_notification_counts(
            recipient_kind="user", recipient_id=USER
        )
        assert (counts["unseen"], counts["unread"], counts["pending"]) == (3, 3, 3)
        assert counts["by_category"]["review_queue"] == {"pending": 3, "unseen": 3}

        stamped = await db.mark_notifications_seen(
            recipient_kind="user", recipient_id=USER, ids=ids[:2]
        )
        assert sorted(str(s["id"]) for s in stamped) == sorted(ids[:2])
        # Idempotent, never regresses, ignores foreign ids.
        again = await db.mark_notifications_seen(
            recipient_kind="user", recipient_id=USER, ids=ids[:2]
        )
        assert again == []
        foreign = await db.mark_notifications_seen(
            recipient_kind="user", recipient_id=OTHER, ids=ids
        )
        assert foreign == []

        # read implies seen (the CHECK), even when the row was never seen.
        row = await db.mark_notification_read_v2(
            recipient_kind="user", recipient_id=USER, notification_id=ids[2]
        )
        assert row["read_at"] is not None and row["seen_at"] is not None
        counts = await db.get_notification_counts(
            recipient_kind="user", recipient_id=USER
        )
        assert (counts["unseen"], counts["unread"]) == (0, 2)

        row = await db.stamp_notification_interacted(ids[0])
        assert row["interacted_at"] and row["read_at"] and row["seen_at"]

        archived = await db.archive_notification(
            recipient_kind="user", recipient_id=USER, notification_id=ids[0]
        )
        assert archived["archived_at"] is not None
        rows, _ = await db.list_notifications_page(
            recipient_kind="user", recipient_id=USER
        )
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_resolve_by_source_settles_every_recipient_once(self, db):
        await db.insert_notification_once(**_insert_kwargs())
        await db.insert_notification_once(**_insert_kwargs(recipient_id=OTHER))
        first = await db.resolve_notifications_by_source(
            source_kind="job", source_id="job-1", resolved_by="officer:t1"
        )
        assert len(first) == 2
        assert {r["resolved_by"] for r in first} == {"officer:t1"}
        second = await db.resolve_notifications_by_source(
            source_kind="job", source_id="job-1", resolved_by="user:u"
        )
        assert second == []


class TestRecordReplay:
    """The completion journal replays a callback if the process dies between
    the send and the effect mark. record() twice with one command id must
    produce one row, one SSE frame and one email."""

    def _service(self, db, feed):
        svc = NotificationService()
        svc.connect(db=db, email_service=MagicMock(), notification_feed=feed)
        svc._email_service.send_notification_email = AsyncMock(
            return_value=(True, "<one@srw>")
        )
        svc._get_user = AsyncMock(
            return_value={"id": USER, "email": "legate@example.org"}
        )
        return svc

    @pytest.mark.asyncio
    async def test_effect_replay_sends_once(self, db):
        feed = MagicMock()
        svc = self._service(db, feed)

        async def effect_callback():
            # `high` (immediate email); the deferred review_queue class has
            # its own replay test in TestSteps.
            return await svc.record(
                recipient_id=USER,
                category="budget_exceeded",
                dedup_key="freeze_notification:cmd-9",
                subject="s",
                body="b",
                source_kind="job",
                source_id="job-9",
                action_params={"job_id": "job-9"},
            )

        first = await effect_callback()
        second = await effect_callback()  # the replay
        assert first.inserted is True and second.inserted is False
        assert first.notification_id == second.notification_id
        assert first.deliveries["email"] is True
        assert second.deliveries["email"] == "already_delivered"
        assert svc._email_service.send_notification_email.await_count == 1
        frames = [c.kwargs["event_type"] for c in feed.broadcast.call_args_list]
        assert frames.count("notification") == 1
        assert await _count(db, "SELECT count(*) FROM notifications") == 1
        assert (
            await _count(
                db,
                "SELECT count(*) FROM notification_deliveries WHERE channel='email' AND state='sent'",
            )
            == 1
        )

    @pytest.mark.asyncio
    async def test_failed_send_is_retried_by_the_replay(self, db):
        feed = MagicMock()
        svc = self._service(db, feed)
        svc._email_service.send_notification_email = AsyncMock(
            side_effect=[OSError("smtp down"), (True, "<two@srw>")]
        )
        kwargs = dict(
            recipient_id=USER,
            category="incident",
            dedup_key="llm_give_up_operator_alert:cmd-3",
            subject="s",
            body="b",
            source_kind="job",
            source_id="job-3",
        )
        first = await svc.record(**kwargs)
        assert first.deliveries["email"] is False
        second = await svc.record(**kwargs)
        assert second.inserted is False and second.deliveries["email"] is True
        async with db.acquire() as conn:
            states = await conn.fetch(
                "SELECT attempt, state FROM notification_deliveries "
                "WHERE channel='email' ORDER BY attempt"
            )
        assert [(r["attempt"], r["state"]) for r in states] == [
            (1, "failed"),
            (2, "sent"),
        ]
        assert feed.broadcast.call_count == 1


def _step_rows(
    nid, *, due_at, channels=("email",), conditions=("not_seen", "not_resolved")
):
    return [
        {
            "step_index": i,
            "step_kind": channel,
            "due_at": due_at,
            "conditions": list(conditions),
            "batch_key": "review_queue",
        }
        for i, channel in enumerate(channels)
    ]


async def _steps(db, nid):
    return await db.list_notification_steps(nid)


class TestSteps:
    """Migration 0194: the deferred-step table under the sweeper's access
    pattern — claim under contention, lease expiry, cancel-on-resolve in the
    same statement, defer/retry releasing the claim."""

    @pytest.mark.asyncio
    async def test_steps_land_in_the_insert_transaction_and_replay_adds_none(self, db):
        kwargs = _insert_kwargs()
        nid = kwargs["notification_id"]
        due = datetime.now(timezone.utc) + timedelta(minutes=5)
        rows = _step_rows(nid, due_at=due, channels=("email", "ntfy"))
        assert await db.insert_notification_once(**kwargs, steps=rows) is True
        assert await db.insert_notification_once(**kwargs, steps=rows) is False
        steps = await _steps(db, nid)
        assert [(s["step_index"], s["step_kind"], s["state"]) for s in steps] == [
            (0, "email", "pending"),
            (1, "ntfy", "pending"),
        ]
        assert steps[0]["conditions"] == ["not_seen", "not_resolved"]
        assert steps[0]["batch_key"] == "review_queue"
        # The explicit insert is idempotent per step index too (quiet-hours
        # deferral runs on every record() call).
        assert await db.insert_notification_steps(nid, rows) == 0
        extra = [{**rows[0], "step_index": 100}]
        assert await db.insert_notification_steps(nid, extra) == 1
        assert len(await _steps(db, nid)) == 3

    @pytest.mark.asyncio
    async def test_constraints_hold(self, db):
        kwargs = _insert_kwargs()
        nid = kwargs["notification_id"]
        await db.insert_notification_once(**kwargs)
        due = datetime.now(timezone.utc)
        with pytest.raises(asyncpg.CheckViolationError):
            await db.insert_notification_steps(
                nid, [{**_step_rows(nid, due_at=due)[0], "step_kind": "in_app"}]
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await db.insert_notification_steps(
                str(uuid.uuid4()), _step_rows(nid, due_at=due)
            )
        with pytest.raises(ValueError):
            await db.settle_notification_steps([1], state="pending")

    @pytest.mark.asyncio
    async def test_concurrent_claims_split_disjointly_and_the_lease_expires(self, db):
        past = datetime.now(timezone.utc) - timedelta(seconds=1)
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        for i in range(6):
            kwargs = _insert_kwargs(dedup_key=f"k{i}", source_id=f"job-{i}")
            await db.insert_notification_once(
                **kwargs,
                steps=_step_rows(
                    kwargs["notification_id"], due_at=past if i < 5 else future
                ),
            )
        results = await asyncio.gather(
            *[
                db.claim_due_notification_steps(worker_id=f"w{n}", limit=10)
                for n in range(4)
            ]
        )
        claimed = [s for batch in results for s in batch]
        assert len(claimed) == 5  # the future one is not due
        assert len({s["id"] for s in claimed}) == 5
        assert {s["attempt"] for s in claimed} == {1}
        assert all(s["claimed_by"].startswith("w") for s in claimed)
        # Joined notification fields ride along for the engine.
        assert claimed[0]["category"] == "review_queue"
        assert claimed[0]["payload"] == {"job_id": "job-1"} or claimed[0]["payload"]
        assert "seen_at" in claimed[0] and "source_id" in claimed[0]
        # Still leased: nothing to claim.
        assert await db.claim_due_notification_steps(worker_id="w9", limit=10) == []
        # Lease expired (simulate): claimable again, attempt increments.
        async with db.acquire() as conn:
            # (claimed_by IS NULL) = (claimed_at IS NULL) is a CHECK — only
            # the claimed rows may carry a stale lease.
            await conn.execute(
                "UPDATE notification_steps SET claimed_at = now() - interval '11 minutes'"
                " WHERE claimed_by IS NOT NULL"
            )
        again = await db.claim_due_notification_steps(worker_id="w9", limit=10)
        assert len(again) == 5 and {s["attempt"] for s in again} == {2}

    @pytest.mark.asyncio
    async def test_resolve_cancels_pending_steps_in_the_same_statement(self, db):
        due = datetime.now(timezone.utc) + timedelta(minutes=5)
        a = _insert_kwargs(dedup_key="a", source_id="job-a")
        b = _insert_kwargs(dedup_key="b", source_id="job-b")
        for kwargs in (a, b):
            await db.insert_notification_once(
                **kwargs,
                steps=_step_rows(
                    kwargs["notification_id"], due_at=due, channels=("email", "ntfy")
                ),
            )
        rows = await db.resolve_notifications_by_source(
            source_kind="job", source_id="job-a", resolved_by="officer:t1"
        )
        assert len(rows) == 1
        a_steps = await _steps(db, a["notification_id"])
        assert {s["state"] for s in a_steps} == {"cancelled"}
        assert {s["detail"] for s in a_steps} == {"resolved:officer:t1"}
        assert all(s["settled_at"] is not None for s in a_steps)
        b_steps = await _steps(db, b["notification_id"])
        assert {s["state"] for s in b_steps} == {"pending"}
        # Cancelled steps are never claimed.
        assert await db.claim_due_notification_steps(worker_id="w", limit=10) == []
        # By id (act() with resolve=True) does the same.
        await db.resolve_notification(b["notification_id"], resolved_by="user:u")
        assert {s["state"] for s in await _steps(db, b["notification_id"])} == {
            "cancelled"
        }

    @pytest.mark.asyncio
    async def test_defer_uncounts_the_attempt_and_retry_keeps_it(self, db):
        kwargs = _insert_kwargs()
        nid = kwargs["notification_id"]
        past = datetime.now(timezone.utc) - timedelta(seconds=1)
        await db.insert_notification_once(**kwargs, steps=_step_rows(nid, due_at=past))
        [step] = await db.claim_due_notification_steps(worker_id="w", limit=10)
        assert step["attempt"] == 1
        later = datetime.now(timezone.utc) + timedelta(hours=8)
        assert (
            await db.defer_notification_steps(
                [step["id"]], due_at=later, detail="quiet_hours"
            )
            == 1
        )
        [row] = await _steps(db, nid)
        assert row["attempt"] == 0 and row["claimed_by"] is None
        assert row["claimed_at"] is None and row["state"] == "pending"
        assert row["detail"] == "quiet_hours"
        assert abs((row["due_at"] - later).total_seconds()) < 1
        # Not due any more.
        assert await db.claim_due_notification_steps(worker_id="w", limit=10) == []
        async with db.acquire() as conn:
            await conn.execute("UPDATE notification_steps SET due_at = now()")
        [step] = await db.claim_due_notification_steps(worker_id="w", limit=10)
        assert step["attempt"] == 1
        assert (
            await db.retry_notification_steps(
                [step["id"]], due_at=later, detail="retry:smtp"
            )
            == 1
        )
        [row] = await _steps(db, nid)
        assert row["attempt"] == 1 and row["claimed_by"] is None

    @pytest.mark.asyncio
    async def test_settle_moves_only_pending_rows(self, db):
        kwargs = _insert_kwargs()
        nid = kwargs["notification_id"]
        past = datetime.now(timezone.utc) - timedelta(seconds=1)
        await db.insert_notification_once(
            **kwargs, steps=_step_rows(nid, due_at=past, channels=("email", "ntfy"))
        )
        steps = await _steps(db, nid)
        ids = [s["id"] for s in steps]
        assert (
            await db.settle_notification_steps(ids[:1], state="done", detail="batch:x")
            == 1
        )
        assert (
            await db.settle_notification_steps(ids, state="skipped", detail="seen") == 1
        )
        rows = await _steps(db, nid)
        assert [(r["state"], r["detail"]) for r in rows] == [
            ("done", "batch:x"),
            ("skipped", "seen"),
        ]
        assert all(r["settled_at"] is not None for r in rows)
        assert await db.claim_due_notification_steps(worker_id="w", limit=10) == []


class TestStepsEndToEnd:
    """record() → steps → sweeper → delivery ledger, against the real tables."""

    def _service(self, db):
        svc = NotificationService()
        svc.connect(db=db, email_service=MagicMock(), notification_feed=MagicMock())
        svc._email_service.is_configured = True
        svc._email_service.send_notification_email = AsyncMock(
            return_value=(True, "<digest@srw>")
        )
        svc._get_user = AsyncMock(
            return_value={"id": USER, "email": "legate@example.org"}
        )
        return svc

    async def _record(self, svc, n):
        return await svc.record(
            recipient_id=USER,
            category="review_queue",
            dedup_key=f"freeze_notification:cmd-{n}",
            subject=f"Job {n} completed — review required",
            body="please review",
            source_kind="job",
            source_id=f"job-{n}",
            action_params={"job_id": f"job-{n}"},
        )

    @pytest.mark.asyncio
    async def test_review_queue_record_plans_bucketed_steps_and_mails_nothing(self, db):
        svc = self._service(db)
        before = datetime.now(timezone.utc)
        first = await self._record(svc, 1)
        replay = await self._record(svc, 1)
        assert first.inserted and not replay.inserted
        svc._email_service.send_notification_email.assert_not_awaited()
        assert "email" not in first.deliveries
        steps = await _steps(db, first.notification_id)
        assert [s["step_kind"] for s in steps] == [
            "email",
            "ntfy",
            "slack_webhook",
            "discord_webhook",
        ]
        # No officer anywhere in this schema → 5 minutes, bucketed to 15.
        due = steps[0]["due_at"]
        assert before + timedelta(minutes=5) <= due <= before + timedelta(minutes=20)
        assert due.minute % 15 == 0 and due.second == 0
        assert steps[0]["conditions"] == ["not_seen", "not_resolved"]
        assert steps[0]["batch_key"] == "review_queue"
        assert first.deliveries["scheduled"]["email"] == due.isoformat()
        assert (
            await _count(
                db,
                "SELECT count(*) FROM notification_deliveries WHERE channel <> 'in_app'",
            )
            == 0
        )
        # Not due: the sweeper has nothing.
        from orchestrator.services.notification_steps import process_due_steps

        stats = await process_due_steps(db=db, service=svc, worker_id="w")
        assert stats == {"claimed": 0}

    @pytest.mark.asyncio
    async def test_due_steps_seen_skip_resolved_cancel_rest_digest(self, db):
        from orchestrator.services.notification_steps import process_due_steps

        svc = self._service(db)
        results = [await self._record(svc, n) for n in range(1, 5)]
        ids = [r.notification_id for r in results]
        # Simulate time passing and the user's behaviour meanwhile.
        async with db.acquire() as conn:
            await conn.execute("UPDATE notification_steps SET due_at = now()")
        await db.mark_notifications_seen(
            recipient_kind="user", recipient_id=USER, ids=[ids[0]]
        )
        await db.resolve_notifications_by_source(
            source_kind="job", source_id="job-2", resolved_by="officer:t1"
        )
        stats = await process_due_steps(db=db, service=svc, worker_id="w")
        # 4 rows × 4 channels: only email is configured (3 webhooks are not);
        # email: 1 seen → skipped, 1 resolved → cancelled (never claimed),
        # 2 unseen+unresolved → one digest.
        assert stats["batches"] == 1 and stats["sent"] == 2
        assert svc._email_service.send_notification_email.await_count == 1
        kwargs = svc._email_service.send_notification_email.call_args.kwargs
        assert kwargs["subject"] == "2 review queue items waiting for you"
        assert f"/inbox?n={ids[2]}" in kwargs["body_md"]
        assert f"/inbox?n={ids[3]}" in kwargs["body_md"]

        by_nid = {}
        for nid in ids:
            by_nid[nid] = {
                s["step_kind"]: (s["state"], s["detail"]) for s in await _steps(db, nid)
            }
        assert by_nid[ids[0]]["email"] == ("skipped", "condition:not_seen")
        assert by_nid[ids[1]]["email"] == ("cancelled", "resolved:officer:t1")
        assert by_nid[ids[2]]["email"][0] == "done"
        assert by_nid[ids[3]]["email"] == by_nid[ids[2]]["email"]
        assert by_nid[ids[2]]["ntfy"] == ("skipped", "channel_unconfigured")

        async with db.acquire() as conn:
            deliveries = await conn.fetch(
                "SELECT notification_id, state, step_index, batch_id, provider_msg_id "
                "FROM notification_deliveries WHERE channel='email' ORDER BY notification_id"
            )
        assert {str(d["notification_id"]) for d in deliveries} == {ids[2], ids[3]}
        assert {d["state"] for d in deliveries} == {"sent"}
        assert {d["step_index"] for d in deliveries} == {0}
        assert len({d["batch_id"] for d in deliveries}) == 1
        assert {d["provider_msg_id"] for d in deliveries} == {"<digest@srw>"}
        assert by_nid[ids[2]]["email"][1] == f"batch:{deliveries[0]['batch_id']}"

        # A second pass finds nothing: everything is settled.
        assert await process_due_steps(db=db, service=svc, worker_id="w") == {
            "claimed": 0
        }

    @pytest.mark.asyncio
    async def test_provider_failure_retries_then_the_claim_ledger_stops_a_double(
        self, db
    ):
        from orchestrator.services.notification_steps import process_due_steps

        svc = self._service(db)
        svc._email_service.send_notification_email = AsyncMock(
            side_effect=[OSError("smtp down"), (True, "<ok@srw>")]
        )
        result = await self._record(svc, 7)
        async with db.acquire() as conn:
            await conn.execute("UPDATE notification_steps SET due_at = now()")
        stats = await process_due_steps(db=db, service=svc, worker_id="w")
        assert stats["retried"] == 1
        [email] = [
            s
            for s in await _steps(db, result.notification_id)
            if s["step_kind"] == "email"
        ]
        assert email["state"] == "pending" and email["attempt"] == 1
        assert email["detail"].startswith("retry:smtp down")
        assert email["due_at"] > datetime.now(timezone.utc) + timedelta(minutes=4)
        # The failed claim freed the slot; the retry sends.
        async with db.acquire() as conn:
            await conn.execute("UPDATE notification_steps SET due_at = now()")
        stats = await process_due_steps(db=db, service=svc, worker_id="w")
        assert stats["sent"] == 1
        async with db.acquire() as conn:
            states = await conn.fetch(
                "SELECT attempt, state FROM notification_deliveries "
                "WHERE channel='email' ORDER BY attempt"
            )
        assert [(r["attempt"], r["state"]) for r in states] == [
            (1, "failed"),
            (2, "sent"),
        ]


class TestCutoverBackfill:
    """Migration 0195: the items the legacy joins derived on read become feed
    rows, minted with the same uuid5 the orchestrator uses, with the same
    action shapes the catalog serialises, and only for OPEN items."""

    MIGRATION = (
        Path(__file__).resolve().parents[1]
        / "src/orchestrator/database/migrations/app/0196_notifications_cutover.sql"
    )

    async def _seed(self, db):
        owner = uuid.uuid4()
        other = uuid.uuid4()
        job_review = uuid.uuid4()
        job_done = uuid.uuid4()
        job_sudo = uuid.uuid4()
        sudo_req = uuid.uuid4()
        thread_key = f"th-{uuid.uuid4().hex[:6]}"
        async with db.acquire() as conn:
            for uid in (owner, other):
                await conn.execute(
                    "INSERT INTO users (id, display_name) VALUES ($1, 'Owner')", uid
                )
            for jid, status in (
                (job_review, "pending_review"),
                (job_done, "completed"),
                (job_sudo, "processing"),
            ):
                await conn.execute(
                    "INSERT INTO jobs (id, description, status, user_id, config_name) "
                    "VALUES ($1, 'Publish the demo', $2, $3, 'worker_base')",
                    jid,
                    status,
                    owner,
                )
            await conn.execute(
                "INSERT INTO sudo_approval_requests "
                "(id, job_id, vm_name, command, arguments, requesting_user, "
                " target_user, status, request_type) "
                "VALUES ($1, $2, 'vm-1', 'apt-get install jq', '{}', 'agent', "
                "        'root', 'pending', 'sudo_command')",
                sudo_req,
                job_sudo,
            )
            # An outbound agent message with no later human reply …
            await conn.execute(
                "INSERT INTO message_log (job_id, user_id, thread_id, direction, "
                " subject, message, status) VALUES ($1, $2, $3, 'outbound', "
                " 'Need input', 'Which colour?', 'sent')",
                job_sudo,
                owner,
                thread_key,
            )
            # … and one that was answered (latest row inbound) → not backfilled.
            answered = f"th-{uuid.uuid4().hex[:6]}"
            await conn.execute(
                "INSERT INTO message_log (job_id, user_id, thread_id, direction, "
                " subject, message, status, created_at) VALUES ($1, $2, $3, "
                " 'outbound', 'Q', 'q', 'sent', now() - interval '2 minutes')",
                job_sudo,
                owner,
                answered,
            )
            await conn.execute(
                "INSERT INTO message_log (job_id, user_id, thread_id, direction, "
                " subject, message, status) VALUES ($1, $2, $3, 'inbound', "
                " 'Re: Q', 'blue', 'received')",
                job_sudo,
                owner,
                answered,
            )
            # A job-less outbound row (an officer/session notice) has no reply
            # path → not backfilled.
            await conn.execute(
                "INSERT INTO message_log (job_id, user_id, thread_id, direction, "
                " subject, message, status) VALUES (NULL, $1, $2, 'outbound', "
                " 'Officer notice', 'held', 'sent')",
                owner,
                f"th-{uuid.uuid4().hex[:6]}",
            )
        return {
            "owner": owner,
            "job_review": job_review,
            "job_done": job_done,
            "sudo_req": sudo_req,
            "job_sudo": job_sudo,
            "thread_key": thread_key,
        }

    @pytest.mark.asyncio
    async def test_backfills_open_items_once_with_catalog_shaped_actions(self, db):
        seed = await self._seed(db)
        sql = self.MIGRATION.read_text()
        async with db.acquire() as conn:
            await conn.execute(sql)
            await conn.execute(sql)  # idempotent: ON CONFLICT (id) DO NOTHING
            rows = await conn.fetch(
                "SELECT * FROM notifications WHERE recipient_id = $1 ORDER BY category",
                seed["owner"],
            )
        by_cat = {r["category"]: db._notification_row(r) for r in rows}
        assert set(by_cat) == {"review_queue", "sudo_request", "agent_message"}
        assert len(rows) == 3

        review = by_cat["review_queue"]
        assert review["source_id"] == str(seed["job_review"])
        assert review["dedup_key"] == f"backfill:job:{seed['job_review']}"
        # Minted like notification_id(): the orchestrator lands on the same row.
        assert str(review["id"]) == str(
            notification_id("user", str(seed["owner"]), review["dedup_key"])
        )
        assert [a["type"] for a in review["actions"]] == ["approve", "resume", "open"]
        assert review["actions"][1]["input_name"] == "feedback"
        assert all(
            a["params"] == {"job_id": str(seed["job_review"])}
            for a in review["actions"]
        )
        assert review["payload"]["backfill"] is True

        sudo = by_cat["sudo_request"]
        assert sudo["severity"] == "critical"
        assert sudo["source_id"] == str(seed["sudo_req"])
        assert [a["type"] for a in sudo["actions"]] == ["approve", "deny", "open"]
        assert sudo["actions"][0]["params"]["request_id"] == str(seed["sudo_req"])

        message = by_cat["agent_message"]
        assert message["source_kind"] == "message_thread"
        assert message["source_id"] == seed["thread_key"]
        assert message["subject"] == "Need input"
        assert [a["type"] for a in message["actions"]] == ["reply", "open"]
        assert message["actions"][0]["params"]["thread_id"] == seed["thread_key"]

        # Nothing was recorded for the completed job or the answered thread.
        async with db.acquire() as conn:
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM notifications WHERE source_id = $1",
                    str(seed["job_done"]),
                )
                == 0
            )
            comment = await conn.fetchval(
                "SELECT obj_description('public.notification_queue'::regclass)"
            )
        assert comment and comment.startswith("RETIRED (0195")
