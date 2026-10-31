"""Live sessions on a migrated PostgreSQL (live_session_jobs.md §4.3, §4.4, §5.5, §7.3).

The session store's SQL (dedupe on insert, the one-job bind, monotonic
transitions, the pod's session lease, adoption and the journal), the live
automation store methods and their schema rules (the partial unique index and
the live-needs-connector CHECK), and the lease exchange's live routes end to
end against real rows: a phone-line connector, its pod's driver identity, a
``live_voice`` catalog model on an endpoint, and the usage ledger row an
ended session bills.
"""

from __future__ import annotations

import base64
import json
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI, HTTPException

from orchestrator.database.migrate import run_migrations
from orchestrator.database.postgres import PostgresDB
from orchestrator.routers.live import LiveDependencies
from orchestrator.routers.live import router as live_router
from orchestrator.services import connector_service_images
from orchestrator.services.connector_service_images import ServiceImageSettings
from orchestrator.services.connector_service_launch import endpoint_url
from orchestrator.services.live.admission import LiveAdmission, hand_off
from orchestrator.services.live.dispatch import live_session_config
from orchestrator.services.live.inbound import (
    LiveInboundDependencies,
    LiveTaskRegistry,
    start_inbound,
)
from orchestrator.services.live.exchange import (
    EventsBody,
    LeaseBody,
    LiveExchange,
    StateBody,
)
from orchestrator.services.live.instructions import build_live_voice
from orchestrator.services.live.pods import LivePodError, pod_endpoint
from orchestrator.services.live.store import LEASE_SECONDS, LiveSessionStore
from shared.connectors.builtin import GPT_LIVE_SIP_SPEC
from shared.connectors.leases import (
    DRIVER_IDENTITY_PREFIX,
    last_four,
    mint_token,
    token_digest,
)
from shared.runtime.live.webhook import IncomingCall, sign_webhook

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / "src/orchestrator/database/migrations/app"
SECRET = "whsec_" + base64.b64encode(b"live-sessions-postgres").decode()
DIGEST = "sha256:" + "a" * 64
GENERATION = "hmac-sha256:" + "b" * 64
MODEL = "gpt-live-1"


@pytest.fixture(scope="module")
def pg():
    from testcontainers.postgres import PostgresContainer

    container = None
    try:
        container = PostgresContainer("pgvector/pgvector:pg15")
        container.start()
    except Exception as exc:
        pytest.skip(f"No disposable PostgreSQL runtime: {type(exc).__name__}")
    try:
        yield {
            "dsn": container.get_connection_url().replace(
                "postgresql+psycopg2://", "postgresql://"
            ),
            "initialized": False,
        }
    finally:
        container.stop()


@pytest_asyncio.fixture
async def db(pg):
    db = PostgresDB(pg["dsn"], min_connections=1, max_connections=8)
    await db.connect()
    try:
        if not pg["initialized"]:
            await run_migrations(db.pool, MIGRATIONS)
            pg["initialized"] = True
        async with db.acquire() as conn:
            await conn.execute(
                "TRUNCATE live_sessions, live_session_events, "
                "connector_driver_identities CASCADE"
            )
            await conn.execute("DELETE FROM automations")
        yield db
    finally:
        await db.close()


@pytest.fixture
def store(db):
    return LiveSessionStore(db)


@pytest.mark.asyncio
async def test_a_fresh_database_migrates_in_one_pass(pg):
    """A new installation (CI, a reset k3d, a schema snapshot) migrates the
    whole directory in one run (the runner applies every transactional
    migration before any ``.notx`` one), and never has a telephony table."""
    name = f"fresh_{uuid4().hex[:8]}"
    admin = await asyncpg.connect(pg["dsn"])
    try:
        await admin.execute(f'CREATE DATABASE "{name}"')
    finally:
        await admin.close()
    fresh = PostgresDB(
        pg["dsn"].rsplit("/", 1)[0] + f"/{name}", min_connections=1, max_connections=2
    )
    await fresh.connect()
    try:
        await run_migrations(fresh.pool, MIGRATIONS)
        async with fresh.acquire() as conn:
            assert await conn.fetchval("SELECT to_regclass('public.live_sessions')")
            assert not await conn.fetchval("SELECT to_regclass('public.calls')")
    finally:
        await fresh.close()


# -- helpers -----------------------------------------------------------------


async def _connector(db, *, ds_type="gpt_live_sip", config=None) -> str:
    row = await db.create_datasource(
        f"line-{uuid4().hex[:10]}",
        ds_type,
        config=config if config is not None else {"model": MODEL},
        credentials={"webhook_secret": SECRET},
    )
    return str(row["id"])


def _incoming(session_id: str, webhook_id: str | None = None) -> IncomingCall:
    return IncomingCall(
        webhook_id=webhook_id or f"wh_{uuid4().hex}",
        event_id=f"evt_{uuid4().hex}",
        event_type="live.transport.incoming",
        session_id=session_id,
        sip_headers=(
            ("From", '"Caller" <sip:+4960521829990@sip.example;tag=x>'),
            ("To", "<sip:+4969123456@sip.example>"),
            ("X-Custom", "1"),
        ),
    )


async def _user(db) -> str:
    user = await db.create_user(f"Live {uuid4().hex[:6]}")
    return str(user["id"])


async def _project(db, owner: str) -> str:
    project = await db.create_project(f"Live project {uuid4().hex[:6]}")
    await db.add_project_member(str(project["id"]), owner, "owner")
    return str(project["id"])


async def _job(db, user_id: str | None = None, project_id: str | None = None) -> str:
    job = await db.create_job("live job", user_id=user_id, project_id=project_id)
    return str(job["id"])


async def _ringing(store, connector_id: str, session: str | None = None) -> dict:
    row = await store.insert_incoming(
        connector_id, _incoming(session or f"rtc_{uuid4().hex}")
    )
    assert row is not None
    return row


async def _active(store, connector_id: str) -> dict:
    row = await _ringing(store, connector_id)
    applied, row = await store.transition(row["id"], "active")
    assert applied
    return row


async def _identity(
    db,
    connector_id: str,
    *,
    driver: str = GPT_LIVE_SIP_SPEC.name,
    service_pod: bool = True,
    ready: bool = True,
    revoked: bool = False,
) -> tuple[str, str]:
    """A driver identity row and its ``sdi_`` token."""
    token = mint_token(DRIVER_IDENTITY_PREFIX)
    async with db.acquire() as conn:
        identity_id = await conn.fetchval(
            """INSERT INTO connector_driver_identities (token_hash,
                token_last_four, connector_id, driver, image_digest,
                pod_namespace, pod_name, credential_generation,
                image_reference, ready_at, revoked_at, revoke_reason)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            RETURNING id""",
            token_digest(token),
            last_four(token),
            UUID(connector_id),
            driver,
            DIGEST if service_pod else None,
            "srw-connectors" if service_pod else None,
            f"srw-drv-{uuid4().hex[:12]}" if service_pod else None,
            # One live pod per pod key: a fresh generation per identity.
            (GENERATION[:-12] + uuid4().hex[:12]) if service_pod else None,
            "ghcr.io/srw/driver-gpt-live-sip:1" if service_pod else None,
            datetime.now(timezone.utc) if ready else None,
            datetime.now(timezone.utc) if revoked else None,
            "test" if revoked else None,
        )
    return str(identity_id), token


async def _live_voice_model(db, *, enabled: bool = True) -> dict:
    endpoint = await db.create_system_llm_endpoint(
        f"live-{uuid4().hex[:8]}",
        "https://live.example.test/v1/",
        "sk-live-secret",
        "sk-l",
    )
    # A model id names one catalog row: drop an earlier test's row first.
    previous = await db.find_model_by_model_id(MODEL)
    if previous is not None:
        await db.delete_model(str(previous["id"]))
    model = await db.create_model(
        provider_kind="endpoint",
        provider_ref=str(endpoint["id"]),
        model_id=MODEL,
        display_label="GPT-Live",
        capabilities=["live_voice"],
        family="default",
        params_json={"adapter": "openai-live", "voice": "marin"},
        enabled=enabled,
    )
    return {"endpoint": endpoint, "model": model}


# =============================================================================
# Sessions
# =============================================================================


class TestInsertAndEnsure:
    @pytest.mark.asyncio
    async def test_an_incoming_session_is_ringing_with_its_numbers(self, db, store):
        connector = await _connector(db)
        row = await store.insert_incoming(connector, _incoming("rtc_one", "wh_one"))
        assert row["status"] == "ringing"
        assert row["origin"] == "webhook"
        assert row["webhook_id"] == "wh_one"
        assert row["provider_session_id"] == "rtc_one"
        assert str(row["connector_id"]) == connector
        assert row["job_id"] is None
        assert row["metadata"]["from_number"] == "+4960521829990"
        assert row["metadata"]["to_number"] == "+4969123456"
        assert {"name": "X-Custom", "value": "1"} in row["metadata"]["sip_headers"]

    @pytest.mark.asyncio
    async def test_a_redelivery_is_a_no_op(self, db, store):
        connector = await _connector(db)
        assert await store.insert_incoming(connector, _incoming("rtc_a", "wh_a"))
        # The same session under another webhook id: the provider retried.
        assert (
            await store.insert_incoming(connector, _incoming("rtc_a", "wh_b")) is None
        )
        # The same webhook id naming another session: a replay.
        assert (
            await store.insert_incoming(connector, _incoming("rtc_b", "wh_a")) is None
        )
        async with db.acquire() as conn:
            assert await conn.fetchval("SELECT count(*) FROM live_sessions") == 1

    @pytest.mark.asyncio
    async def test_a_session_id_is_unique_per_connector_only(self, db, store):
        one, two = await _connector(db), await _connector(db)
        assert await store.insert_incoming(one, _incoming("rtc_shared"))
        assert await store.insert_incoming(two, _incoming("rtc_shared"))

    @pytest.mark.asyncio
    async def test_ensure_session_creates_once_and_returns_the_row(self, db, store):
        connector = await _connector(db)
        async with db.acquire() as conn:
            first = await store.ensure_session(
                conn,
                connector_id=connector,
                provider_session_id="rtc_api",
                origin="api",
            )
            again = await store.ensure_session(
                conn,
                connector_id=connector,
                provider_session_id="rtc_api",
                origin="manifest",
            )
        assert first["status"] == "ringing" and first["origin"] == "api"
        assert again["id"] == first["id"] and again["origin"] == "api"

    @pytest.mark.asyncio
    async def test_ensure_session_finds_the_webhooks_row(self, db, store):
        connector = await _connector(db)
        ringing = await _ringing(store, connector, "rtc_hook")
        async with db.acquire() as conn:
            row = await store.ensure_session(
                conn,
                connector_id=connector,
                provider_session_id="rtc_hook",
                origin="api",
            )
        assert row["id"] == ringing["id"] and row["origin"] == "webhook"

    @pytest.mark.asyncio
    async def test_the_origin_check_refuses_an_unknown_origin(self, db, store):
        connector = await _connector(db)
        async with db.acquire() as conn:
            with pytest.raises(asyncpg.CheckViolationError):
                await store.ensure_session(
                    conn,
                    connector_id=connector,
                    provider_session_id="rtc_x",
                    origin="telephony",
                )


class TestBind:
    @pytest.mark.asyncio
    async def test_a_session_binds_one_job_only(self, db, store):
        connector = await _connector(db)
        session = await _ringing(store, connector)
        first, second = await _job(db), await _job(db)
        async with db.acquire() as conn:
            assert await store.bind_job(conn, session["id"], first) is True
            assert await store.bind_job(conn, session["id"], second) is False
            assert await store.bind_job(conn, session["id"], first) is False
        row = await store.get(session["id"])
        assert str(row["job_id"]) == first

    @pytest.mark.asyncio
    async def test_get_filters_by_job_and_job_sessions_lists_views(self, db, store):
        connector = await _connector(db)
        session = await _ringing(store, connector)
        job, other = await _job(db), await _job(db)
        async with db.acquire() as conn:
            await store.bind_job(conn, session["id"], job)
        assert (await store.get(session["id"], job_id=job))["id"] == session["id"]
        assert await store.get(session["id"], job_id=other) is None
        assert await store.get(uuid4()) is None
        (view,) = await store.job_sessions(job)
        assert view["id"] == session["id"]
        assert view["from_number"] == "+4960521829990"
        assert view["to_number"] == "+4969123456"
        assert "handoff" not in view and "metadata" not in view
        assert await store.job_sessions(other) == []

    @pytest.mark.asyncio
    async def test_note_admission_keeps_what_it_is_not_given(self, db, store):
        connector = await _connector(db)
        session = await _ringing(store, connector)
        owner = await _user(db)
        project = await _project(db, owner)
        await store.note_admission(
            session["id"], project_id=project, model=MODEL, handoff={"job_id": "x"}
        )
        await store.note_admission(session["id"])
        row = await store.get(session["id"])
        assert str(row["project_id"]) == project
        assert row["model"] == MODEL
        assert row["handoff"] == {"job_id": "x"}
        assert row["automation_id"] is None


class TestTransitions:
    @pytest.mark.asyncio
    async def test_ringing_becomes_active_and_records_its_accept(self, db, store):
        connector = await _connector(db)
        session = await _ringing(store, connector)
        accepted = datetime(2026, 10, 13, 9, 0, tzinfo=timezone.utc)
        applied, row = await store.transition(
            session["id"], "active", accepted_at=accepted
        )
        assert applied and row["status"] == "active"
        assert row["accepted_at"] == accepted and row["attached_at"] is None

    @pytest.mark.asyncio
    async def test_active_on_active_records_the_first_attach_only(self, db, store):
        connector = await _connector(db)
        session = await _active(store, connector)
        first = datetime(2026, 10, 13, 9, 0, 5, tzinfo=timezone.utc)
        applied, row = await store.transition(
            session["id"], "active", attached_at=first, accepted_at=first
        )
        assert applied and row["status"] == "active"
        assert row["attached_at"] == first
        # The accept time stays the first one.
        assert row["accepted_at"] == session["accepted_at"]
        applied, row = await store.transition(
            session["id"], "active", attached_at=first + timedelta(seconds=9)
        )
        assert applied and row["attached_at"] == first

    @pytest.mark.asyncio
    async def test_ended_follows_active_only_and_only_once(self, db, store):
        connector = await _connector(db)
        ringing = await _ringing(store, connector)
        applied, row = await store.transition(ringing["id"], "ended")
        assert not applied and row["status"] == "ringing"

        session = await _active(store, connector)
        ended_at = datetime(2026, 10, 13, 9, 5, tzinfo=timezone.utc)
        applied, row = await store.transition(
            session["id"],
            "ended",
            ended_at=ended_at,
            close_reason="remote_hangup",
            usage_seconds=42.5,
        )
        assert applied and row["status"] == "ended"
        assert row["ended_at"] == ended_at
        assert row["close_reason"] == "remote_hangup"
        assert row["usage_seconds"] == Decimal("42.5")
        applied, row = await store.transition(
            session["id"], "ended", close_reason="expired", usage_seconds=99
        )
        assert not applied
        assert row["close_reason"] == "remote_hangup"
        assert row["usage_seconds"] == Decimal("42.5")
        # Nothing follows an ended session.
        for status in ("active", "failed", "rejected"):
            applied, row = await store.transition(session["id"], status)
            assert not applied and row["status"] == "ended"

    @pytest.mark.asyncio
    async def test_failed_follows_ringing_or_active_and_keeps_its_error(
        self, db, store
    ):
        connector = await _connector(db)
        ringing = await _ringing(store, connector)
        applied, row = await store.transition(
            ringing["id"], "failed", error="handoff failed: " + "x" * 2000
        )
        assert applied and row["status"] == "failed"
        assert row["error"].startswith("handoff failed: ")
        assert len(row["error"]) == 1000
        assert row["ended_at"] is not None

        active = await _active(store, connector)
        applied, row = await store.transition(active["id"], "failed", error="lost")
        assert applied and row["status"] == "failed" and row["error"] == "lost"

    @pytest.mark.asyncio
    async def test_rejected_follows_ringing_only(self, db, store):
        connector = await _connector(db)
        ringing = await _ringing(store, connector)
        applied, row = await store.transition(
            ringing["id"], "rejected", close_reason="no_automation"
        )
        assert applied and row["status"] == "rejected"
        assert row["close_reason"] == "no_automation"
        applied, row = await store.transition(ringing["id"], "active")
        assert not applied and row["status"] == "rejected"

        active = await _active(store, connector)
        applied, row = await store.transition(active["id"], "rejected")
        assert not applied and row["status"] == "active"

    @pytest.mark.asyncio
    async def test_an_unknown_status_is_refused_before_any_write(self, db, store):
        connector = await _connector(db)
        session = await _ringing(store, connector)
        with pytest.raises(ValueError):
            await store.transition(session["id"], "ringing-ish")

    @pytest.mark.asyncio
    async def test_an_unknown_session_is_not_applied(self, db, store):
        assert await store.transition(uuid4(), "active") == (False, None)

    @pytest.mark.asyncio
    async def test_a_terminal_status_releases_the_pods_lease(self, db, store):
        connector = await _connector(db)
        session = await _active(store, connector)
        identity, _ = await _identity(db, connector)
        owned, _ = await store.lease(session["id"], identity)
        assert owned
        applied, row = await store.transition(session["id"], "ended")
        assert applied
        assert row["owner_identity_id"] is None and row["lease_until"] is None


# =============================================================================
# The pod's session lease and adoption
# =============================================================================


async def _expire(db, session_id) -> None:
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE live_sessions SET lease_until = now() - interval '1 second' "
            "WHERE id = $1",
            UUID(str(session_id)),
        )


class TestLease:
    @pytest.mark.asyncio
    async def test_claim_renew_and_another_pod_refused_until_expiry(self, db, store):
        connector = await _connector(db)
        session = await _active(store, connector)
        mine, _ = await _identity(db, connector)
        theirs, _ = await _identity(db, connector)

        owned, until = await store.lease(session["id"], mine)
        assert owned and until is not None
        remaining = (until - datetime.now(timezone.utc)).total_seconds()
        assert LEASE_SECONDS - 5 < remaining <= LEASE_SECONDS
        owned, renewed = await store.lease(session["id"], mine)
        assert owned and renewed >= until

        assert await store.lease(session["id"], theirs) == (False, None)
        await _expire(db, session["id"])
        owned, _ = await store.lease(session["id"], theirs)
        assert owned
        row = await store.get(session["id"])
        assert str(row["owner_identity_id"]) == theirs
        assert await store.lease(session["id"], mine) == (False, None)

    @pytest.mark.asyncio
    async def test_release_frees_the_session_for_the_holder_only(self, db, store):
        connector = await _connector(db)
        session = await _active(store, connector)
        mine, _ = await _identity(db, connector)
        theirs, _ = await _identity(db, connector)
        assert (await store.lease(session["id"], mine))[0]
        # Another pod's release changes nothing.
        assert await store.lease(session["id"], theirs, release=True) == (False, None)
        assert await store.lease(session["id"], theirs) == (False, None)
        await store.lease(session["id"], mine, release=True)
        row = await store.get(session["id"])
        assert row["owner_identity_id"] is None and row["lease_until"] is None
        assert (await store.lease(session["id"], theirs))[0]

    @pytest.mark.asyncio
    async def test_a_ringing_session_may_be_leased_an_over_one_not(self, db, store):
        connector = await _connector(db)
        identity, _ = await _identity(db, connector)
        ringing = await _ringing(store, connector)
        assert (await store.lease(ringing["id"], identity))[0]
        ended = await _active(store, connector)
        await store.transition(ended["id"], "ended")
        assert await store.lease(ended["id"], identity) == (False, None)

    @pytest.mark.asyncio
    async def test_adoptable_lists_unheld_active_sessions_with_pod_marks(
        self, db, store
    ):
        connector = await _connector(db)
        other_connector = await _connector(db)
        mine, _ = await _identity(db, connector)
        theirs, _ = await _identity(db, connector)

        free = await _active(store, connector)
        held = await _active(store, connector)
        expired = await _active(store, connector)
        own = await _active(store, connector)
        await _ringing(store, connector)  # not accepted yet: not adoptable
        await _active(store, other_connector)  # another line's
        assert (await store.lease(held["id"], theirs))[0]
        assert (await store.lease(expired["id"], theirs))[0]
        await _expire(db, expired["id"])
        assert (await store.lease(own["id"], mine))[0]

        sid = str(free["id"])
        await store.write_events(
            free["id"],
            [
                {"event_id": f"srw_greeting_{sid}", "kind": "instruction"},
                {"event_id": f"srw_fallback_{sid}_d1", "kind": "fallback"},
                {"event_id": f"srw_max_duration_{sid}", "kind": "system"},
                {"event_id": "evt_engine_1", "kind": "transcript"},
                # LIKE's underscore is escaped: these are no pod marks.
                {"event_id": "srwXgreetingY1", "kind": "system"},
                {"event_id": "srw_greetings", "kind": "system"},
                {"event_id": None, "kind": "system"},
            ],
        )

        adoptable = await store.adoptable(connector, mine)
        ids = {row["id"] for row in adoptable}
        assert ids == {free["id"], expired["id"], own["id"]}
        by_id = {row["id"]: row for row in adoptable}
        assert by_id[free["id"]]["sent_event_ids"] == sorted(
            [
                f"srw_fallback_{sid}_d1",
                f"srw_greeting_{sid}",
                f"srw_max_duration_{sid}",
            ]
        )
        assert by_id[own["id"]]["sent_event_ids"] == []
        # The holder sees its own held session too.
        theirs_view = {row["id"] for row in await store.adoptable(connector, theirs)}
        assert held["id"] in theirs_view and own["id"] not in theirs_view


# =============================================================================
# The journal
# =============================================================================


class TestJournal:
    @pytest.mark.asyncio
    async def test_events_are_deduplicated_on_event_id_and_read_with_it(
        self, db, store
    ):
        connector = await _connector(db)
        session = await _active(store, connector)
        events = [
            {
                "event_id": "evt_1",
                "kind": "transcript",
                "speaker": "caller",
                "text": "Hallo",
                "start_ms": 0,
                "end_ms": 900,
            },
            {
                "event_id": "evt_2",
                "kind": "delegation",
                "delegation_id": "del_1",
                "payload": {"question": "Wann?"},
            },
            {"event_id": None, "kind": "system", "text": "a"},
        ]
        assert await store.write_events(session["id"], events) == 3
        # A replay inserts only what has no event id.
        assert await store.write_events(session["id"], events) == 1
        rows = await store.events(session["id"])
        assert [row["event_id"] for row in rows] == ["evt_1", "evt_2", None, None]
        assert rows[0]["speaker"] == "caller" and rows[0]["end_ms"] == 900
        assert rows[0]["payload"] == {}
        assert rows[1]["payload"] == {"question": "Wann?"}
        assert rows[1]["delegation_id"] == "del_1"
        assert [row["id"] for row in rows] == sorted(row["id"] for row in rows)
        assert set(rows[0]) == {
            "id",
            "event_id",
            "kind",
            "speaker",
            "text",
            "start_ms",
            "end_ms",
            "delegation_id",
            "payload",
            "created_at",
        }

    @pytest.mark.asyncio
    async def test_reads_page_after_a_cursor(self, db, store):
        connector = await _connector(db)
        session = await _active(store, connector)
        await store.write_events(
            session["id"],
            [{"event_id": f"evt_{i}", "kind": "transcript"} for i in range(5)],
        )
        first = await store.events(session["id"], limit=2)
        assert [row["event_id"] for row in first] == ["evt_0", "evt_1"]
        rest = await store.events(session["id"], after_id=first[-1]["id"])
        assert [row["event_id"] for row in rest] == ["evt_2", "evt_3", "evt_4"]

    @pytest.mark.asyncio
    async def test_the_same_event_id_in_two_sessions_is_two_rows(self, db, store):
        connector = await _connector(db)
        one, two = await _active(store, connector), await _active(store, connector)
        for session in (one, two):
            assert (
                await store.write_events(
                    session["id"], [{"event_id": "evt_x", "kind": "closed"}]
                )
                == 1
            )

    @pytest.mark.asyncio
    async def test_an_unknown_session_is_a_lookup_error(self, db, store):
        with pytest.raises(LookupError):
            await store.write_events(uuid4(), [{"kind": "system"}])

    @pytest.mark.asyncio
    async def test_an_unknown_kind_or_too_many_events_write_nothing(self, db, store):
        connector = await _connector(db)
        session = await _active(store, connector)
        with pytest.raises(ValueError):
            await store.write_events(
                session["id"],
                [
                    {"event_id": "evt_ok", "kind": "transcript"},
                    {"event_id": "evt_bad", "kind": "usage"},
                ],
            )
        # The batch is one transaction: the good row went with the bad one.
        assert await store.events(session["id"]) == []
        with pytest.raises(ValueError):
            await store.write_events(session["id"], [{"kind": "system"}] * 501)

    @pytest.mark.asyncio
    async def test_deleting_a_session_deletes_its_journal(self, db, store):
        connector = await _connector(db)
        session = await _active(store, connector)
        await store.write_events(session["id"], [{"kind": "system"}])
        async with db.acquire() as conn:
            await conn.execute("DELETE FROM live_sessions WHERE id=$1", session["id"])
            assert (
                await conn.fetchval(
                    "SELECT count(*) FROM live_session_events WHERE session_id=$1",
                    session["id"],
                )
                == 0
            )


# =============================================================================
# Live automations
# =============================================================================


async def _automation(db, owner: str, connector: str | None, **overrides) -> dict:
    kwargs = {
        "owner_id": owner,
        "name": f"desk-{uuid4().hex[:6]}",
        "trigger_type": "live",
        "expert": "receptionist",
        "prompt": "Answer the line",
        "trigger_connector_id": connector,
        "job_shape": {
            "execution": {"connectors": {"telefon": {"ref": {"name": "line"}}}},
            "workspace": None,
            "primary_connector": "telefon",
        },
    }
    kwargs.update(overrides)
    return await db.create_automation(**kwargs)


class TestLiveAutomations:
    @pytest.mark.asyncio
    async def test_a_live_automation_stores_its_connector_and_shape(self, db):
        owner, connector = await _user(db), await _connector(db)
        row = await _automation(db, owner, connector)
        assert row["trigger_type"] == "live"
        assert str(row["trigger_connector_id"]) == connector
        assert row["cron_expr"] is None and row["next_run_at"] is None
        assert row["job_shape"]["workspace"] is None
        assert row["job_shape"]["primary_connector"] == "telefon"
        fetched = await db.get_automation(str(row["id"]))
        assert fetched["job_shape"] == row["job_shape"]

    @pytest.mark.asyncio
    async def test_a_live_automation_without_a_connector_is_refused(self, db):
        owner = await _user(db)
        with pytest.raises(asyncpg.CheckViolationError) as error:
            await _automation(db, owner, None)
        assert error.value.constraint_name == "live_trigger_has_connector"

    @pytest.mark.asyncio
    async def test_one_enabled_live_automation_per_connector(self, db):
        owner, connector = await _user(db), await _connector(db)
        holder = await _automation(db, owner, connector)
        with pytest.raises(asyncpg.UniqueViolationError) as error:
            await _automation(db, owner, connector)
        assert error.value.constraint_name == "uq_automations_live_primary"
        # A disabled one may wait beside it, and cannot be enabled under it.
        spare = await _automation(db, owner, connector, enabled=False)
        with pytest.raises(asyncpg.UniqueViolationError) as error:
            await db.update_automation(str(spare["id"]), enabled=True)
        assert error.value.constraint_name == "uq_automations_live_primary"
        # Another connector is free.
        assert await _automation(db, owner, await _connector(db))
        await db.update_automation(str(holder["id"]), enabled=False)
        enabled = await db.update_automation(str(spare["id"]), enabled=True)
        assert enabled["enabled"] is True

    @pytest.mark.asyncio
    async def test_the_lookup_finds_the_enabled_holder(self, db):
        owner, connector = await _user(db), await _connector(db)
        assert await db.live_automation_for_connector(connector) is None
        await _automation(db, owner, connector, enabled=False)
        assert await db.live_automation_for_connector(connector) is None
        holder = await _automation(db, owner, connector)
        found = await db.live_automation_for_connector(connector)
        assert found["id"] == holder["id"]
        assert isinstance(found["job_shape"], dict)
        assert (
            await db.live_automation_for_connector(
                connector, exclude_id=str(holder["id"])
            )
            is None
        )
        # A cron automation never listens on a connector.
        await db.create_automation(
            owner_id=owner,
            name="nightly",
            trigger_type="cron",
            expert="developer",
            prompt="p",
            cron_expr="0 3 * * *",
        )
        assert (await db.live_automation_for_connector(connector))["id"] == holder["id"]

    @pytest.mark.asyncio
    async def test_the_daily_fire_budget(self, db):
        owner, connector = await _user(db), await _connector(db)
        row = await _automation(db, owner, connector, max_fires_per_day=2)
        automation = str(row["id"])
        assert await db.claim_live_automation_fire(automation) is True
        assert await db.claim_live_automation_fire(automation) is True
        assert await db.claim_live_automation_fire(automation) is False
        fetched = await db.get_automation(automation)
        assert fetched["fires_today_count"] == 2
        # A new day starts a new budget.
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE automations SET fires_today_date = CURRENT_DATE - 1 "
                "WHERE id = $1",
                UUID(automation),
            )
        assert await db.claim_live_automation_fire(automation) is True
        assert (await db.get_automation(automation))["fires_today_count"] == 1
        # A disabled automation never fires.
        await db.update_automation(automation, enabled=False)
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE automations SET fires_today_date = CURRENT_DATE - 1 "
                "WHERE id = $1",
                UUID(automation),
            )
        assert await db.claim_live_automation_fire(automation) is False

    @pytest.mark.asyncio
    async def test_a_fire_records_its_job_or_why_not(self, db):
        owner, connector = await _user(db), await _connector(db)
        row = await _automation(db, owner, connector)
        automation = str(row["id"])
        job = await _job(db, owner)
        await db.record_live_automation_fire(automation, job_id=job, status=None)
        fired = await db.get_automation(automation)
        assert str(fired["last_job_id"]) == job
        assert fired["run_count"] == 1
        assert fired["last_status"] is None
        assert fired["last_fired_at"] is not None
        await db.record_live_automation_fire(
            automation, job_id=None, status="refused: daily fire limit reached"
        )
        refused = await db.get_automation(automation)
        assert str(refused["last_job_id"]) == job
        assert refused["run_count"] == 1
        assert refused["last_status"] == "refused: daily fire limit reached"
        assert refused["last_fired_at"] >= fired["last_fired_at"]

    @pytest.mark.asyncio
    async def test_the_shape_and_connector_are_updatable(self, db):
        owner, connector = await _user(db), await _connector(db)
        moved = await _connector(db)
        row = await _automation(db, owner, connector)
        updated = await db.update_automation(
            str(row["id"]),
            trigger_connector_id=moved,
            job_shape={"execution": {"connectors": {}}, "primary_connector": "x"},
        )
        assert str(updated["trigger_connector_id"]) == moved
        assert updated["job_shape"] == {
            "execution": {"connectors": {}},
            "primary_connector": "x",
        }

    @pytest.mark.asyncio
    async def test_a_listened_on_connector_cannot_be_deleted(self, db):
        owner, connector = await _user(db), await _connector(db)
        await _automation(db, owner, connector)
        async with db.acquire() as conn:
            with pytest.raises(asyncpg.ForeignKeyViolationError):
                await conn.execute(
                    "DELETE FROM datasources WHERE id = $1", UUID(connector)
                )

    @pytest.mark.asyncio
    async def test_no_rate_is_seeded(self, db):
        # usage_rates ships empty; the live ledger prices gpt-live-1 with its
        # list price until an admin configures a rate (services/live/ledger).
        async with db.acquire() as conn:
            rate = await conn.fetchval(
                "SELECT rate_usd FROM usage_rates WHERE unit='live-voice-second'"
            )
        assert rate is None


# =============================================================================
# The pod endpoint and the dispatch injection
# =============================================================================


@pytest.fixture
def hosting(monkeypatch):
    monkeypatch.setitem(
        connector_service_images._state,
        "settings",
        ServiceImageSettings(service_namespace="srw-connectors"),
    )


class TestPodEndpointAndDispatch:
    @pytest.mark.asyncio
    async def test_the_endpoint_names_the_ready_pods_digest(self, db, hosting):
        connector = await _connector(db)
        assert await pod_endpoint(db, connector) is None
        await _identity(db, connector, ready=False)
        assert await pod_endpoint(db, connector) is None
        await _identity(db, connector, revoked=True)
        assert await pod_endpoint(db, connector) is None
        await _identity(db, connector)
        assert await pod_endpoint(db, connector) == endpoint_url(
            namespace="srw-connectors",
            connector_id=connector,
            digest=DIGEST,
            port=8080,
        )

    @pytest.mark.asyncio
    async def test_no_endpoint_while_hosting_is_off(self, db, monkeypatch):
        monkeypatch.setitem(
            connector_service_images._state, "settings", ServiceImageSettings()
        )
        connector = await _connector(db)
        await _identity(db, connector)
        assert await pod_endpoint(db, connector) is None

    @pytest.mark.asyncio
    async def test_a_bound_jobs_dispatch_names_its_session_and_pod(
        self, db, store, hosting
    ):
        connector = await _connector(db)
        await _identity(db, connector)
        session = await _ringing(store, connector, "rtc_dispatch")
        job = await _job(db)
        async with db.acquire() as conn:
            await store.bind_job(conn, session["id"], job)
        context = {"live_session": {"session_id": str(session["id"])}}
        config = await live_session_config(db, {"id": job, "context": context})
        assert config == {
            "session_id": str(session["id"]),
            "connector_id": connector,
            "provider_session_id": "rtc_dispatch",
            "endpoint_url": endpoint_url(
                namespace="srw-connectors",
                connector_id=connector,
                digest=DIGEST,
                port=8080,
            ),
        }
        # Nothing secret in it.
        assert "token" not in str(config) and SECRET not in str(config)
        assert await live_session_config(db, {"id": job, "context": {}}) is None
        other = await _job(db)
        with pytest.raises(ValueError):
            await live_session_config(db, {"id": other, "context": context})


# =============================================================================
# Admission's writes: the bind in the job's INSERT transaction, the handoff
# =============================================================================


def _admission(store, session: dict, connector_id: str) -> LiveAdmission:
    voice = build_live_voice(
        "voice",
        {"driver": "live", "live": {"persona": "Desk.", "greeting": "Hello."}},
    )
    connector = {"id": connector_id, "config": {"model": MODEL}}
    return LiveAdmission(
        session=session, connector=connector, voice=voice, sessions=store
    )


class TestAdmissionWrites:
    @pytest.mark.asyncio
    async def test_the_job_and_its_bind_commit_together(self, db, store):
        connector = await _connector(db)
        session = await _ringing(store, connector)
        admission = _admission(store, session, connector)
        async with db.acquire() as conn, conn.transaction():
            job = await db.create_job("live job", context={"a": 1}, conn=conn)
            await admission.on_created(conn, str(job["id"]))
        assert str((await store.get(session["id"]))["job_id"]) == str(job["id"])

        # A second job for the same session: 409, and its row is gone with
        # the failed bind.
        with pytest.raises(HTTPException) as error:
            async with db.acquire() as conn, conn.transaction():
                second = await db.create_job("second", conn=conn)
                await admission.on_created(conn, str(second["id"]))
        assert error.value.status_code == 409
        assert error.value.detail["code"] == "live_session_bound"
        assert await db.get_job(str(second["id"])) is None

    @pytest.mark.asyncio
    async def test_hand_off_of_a_created_row_activates_the_session(self, db, store):
        owner, connector = await _user(db), await _connector(db)
        project = await _project(db, owner)
        session = await _ringing(store, connector)
        admission = _admission(store, session, connector)
        async with db.acquire() as conn, conn.transaction():
            job = await db.create_job(
                "live job",
                user_id=owner,
                project_id=project,
                context={"live_session": admission.context_value},
                conn=conn,
            )
            await admission.on_created(conn, str(job["id"]))
        pods = SimpleNamespace(handoff=AsyncMock(return_value={"status": "active"}))
        row = await hand_off(admission, job, pods)
        assert row["status"] == "active" and row["accepted_at"] is not None
        stored = await store.get(session["id"])
        assert str(stored["project_id"]) == project
        assert stored["model"] == MODEL
        assert stored["handoff"]["job_id"] == str(job["id"])
        assert stored["handoff"]["accept"]["voice"] == "marin"
        pods.handoff.assert_awaited_once()
        assert pods.handoff.await_args.args == (connector, stored["handoff"])

    @pytest.mark.asyncio
    async def test_an_inbound_fire_binds_hands_off_and_records_its_automation(
        self, db, store
    ):
        """The webhook path end to end on the store: the automation's job is
        admitted (here: created, bound and handed off as admit_job does), the
        session names its automation, the fire is counted and recorded."""
        owner, connector = await _user(db), await _connector(db)
        automation = await _automation(db, owner, connector)
        session = await _ringing(store, connector, "rtc_inbound")
        pods = SimpleNamespace(
            handoff=AsyncMock(return_value={"status": "active"}), reject=AsyncMock()
        )
        created = {}

        async def create_job(owner_id, command):
            assert owner_id == owner
            assert command.execution.session.id == "rtc_inbound"
            admission = _admission(store, session, connector)
            async with db.acquire() as conn, conn.transaction():
                job = await db.create_job(
                    command.description,
                    user_id=owner_id,
                    context=command.context,
                    conn=conn,
                )
                await admission.on_created(conn, str(job["id"]))
            # admit_job hands off the created row, which carries no context.
            await hand_off(admission, job, pods)
            created["job"] = job
            return job

        dependencies = LiveInboundDependencies(
            db=db, pods=pods, tasks=LiveTaskRegistry(), admit_as_owner=create_job
        )
        await start_inbound(session, await db.get_datasource(connector), dependencies)

        job_id = str(created["job"]["id"])
        stored = await store.get(session["id"])
        assert stored["status"] == "active"
        assert str(stored["job_id"]) == job_id
        assert str(stored["automation_id"]) == str(automation["id"])
        fired = await db.get_automation(str(automation["id"]))
        assert str(fired["last_job_id"]) == job_id
        assert fired["run_count"] == 1 and fired["fires_today_count"] == 1
        assert fired["last_status"] is None
        pods.reject.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_inbound_session_without_an_automation_is_rejected(
        self, db, store
    ):
        connector = await _connector(db)
        session = await _ringing(store, connector)
        pods = SimpleNamespace(handoff=AsyncMock(), reject=AsyncMock())
        dependencies = LiveInboundDependencies(
            db=db, pods=pods, tasks=LiveTaskRegistry(), admit_as_owner=AsyncMock()
        )
        await start_inbound(session, await db.get_datasource(connector), dependencies)
        pods.reject.assert_awaited_once_with(
            connector, str(session["id"]), session["provider_session_id"], 480
        )
        stored = await store.get(session["id"])
        assert stored["status"] == "rejected"
        assert stored["close_reason"] == "no_automation"
        dependencies.admit_as_owner.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_failed_handoff_fails_the_session_with_its_reason(self, db, store):
        connector = await _connector(db)
        session = await _ringing(store, connector)
        admission = _admission(store, session, connector)
        job = await db.create_job("live job")
        pods = SimpleNamespace(
            handoff=AsyncMock(side_effect=LivePodError("pod not ready", status=503))
        )
        row = await hand_off(admission, job, pods)
        assert row["status"] == "failed"
        assert row["error"] == "handoff failed: pod not ready"


# =============================================================================
# The lease exchange's live routes, end to end
# =============================================================================


@pytest_asyncio.fixture
async def line(db, store):
    owner = await _user(db)
    project = await _project(db, owner)
    catalog = await _live_voice_model(db)
    connector = await _connector(db)
    identity, token = await _identity(db, connector)
    ledger = SimpleNamespace(is_available=True, record_events=AsyncMock(return_value=1))
    return SimpleNamespace(
        owner=owner,
        project=project,
        catalog=catalog,
        connector=connector,
        identity=identity,
        token=token,
        ledger=ledger,
        exchange=LiveExchange(db=db, ledger=ledger, service_hosting=True),
    )


class TestLiveExchange:
    @pytest.mark.asyncio
    async def test_the_pod_is_its_live_identity_only(self, db, line):
        pod = await line.exchange.pod(line.token)
        assert pod.identity_id == line.identity
        assert pod.connector_id == line.connector
        assert pod.driver == GPT_LIVE_SIP_SPEC.name
        assert await line.exchange.pod(mint_token(DRIVER_IDENTITY_PREFIX)) is None
        assert await line.exchange.pod("sdi_not-a-token") is None
        assert await line.exchange.pod("") is None
        _, revoked = await _identity(db, line.connector, revoked=True)
        assert await line.exchange.pod(revoked) is None
        # A bind-time driver identity is no service pod.
        _, bind_time = await _identity(db, line.connector, service_pod=False)
        assert await line.exchange.pod(bind_time) is None
        # Another connector's pod, not a live driver.
        generic = await _connector(db, ds_type="generic", config={})
        _, other = await _identity(db, generic, driver="srw.echo/v1")
        assert await line.exchange.pod(other) is None
        # A live connector's identity claiming another driver.
        _, mismatched = await _identity(db, line.connector, driver="srw.echo/v1")
        assert await line.exchange.pod(mismatched) is None
        # A process that hosts no service pods answers none.
        off = LiveExchange(db=db, ledger=line.ledger, service_hosting=False)
        assert await off.pod(line.token) is None

    @pytest.mark.asyncio
    async def test_the_credential_is_the_live_voice_models_endpoint(self, db, line):
        pod = await line.exchange.pod(line.token)
        outcome = await line.exchange.credential(pod)
        assert outcome.status == 200
        assert outcome.body["api_key"] == "sk-live-secret"
        assert outcome.body["api_base"] == "https://live.example.test/v1"
        assert outcome.body["ws_base"] == "wss://live.example.test/v1"
        assert outcome.body["max_cache_seconds"] == 300
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE models SET enabled = false WHERE model_id = $1", MODEL
            )
        outcome = await line.exchange.credential(pod)
        assert outcome.status == 409
        assert outcome.body == {"error": "no_live_voice_model"}

    @pytest.mark.asyncio
    async def test_a_session_ends_once_and_bills_its_seconds_once(
        self, db, store, line
    ):
        pod = await line.exchange.pod(line.token)
        session = await _ringing(store, line.connector, "rtc_bill")
        job = await _job(db, line.owner, line.project)
        async with db.acquire() as conn:
            await store.bind_job(conn, session["id"], job)
        await store.note_admission(
            session["id"], project_id=line.project, model=MODEL, handoff={"x": 1}
        )
        sid = str(session["id"])

        outcome = await line.exchange.state(pod, sid, StateBody(status="active"))
        assert outcome.status == 200 and outcome.body == {"status": "active"}
        ended_at = datetime(2026, 10, 13, 10, 0, tzinfo=timezone.utc)
        body = StateBody(
            status="ended",
            ended_at=ended_at,
            close_reason="remote_hangup",
            usage_seconds=42.5,
        )
        outcome = await line.exchange.state(pod, sid, body)
        assert outcome.status == 200 and outcome.body == {"status": "ended"}
        line.ledger.record_events.assert_awaited_once()
        (events,) = line.ledger.record_events.await_args.args
        (event,) = events
        assert event.source == "live"
        assert event.unit == "live-voice-second"
        assert event.category == "llm"
        assert event.resource == MODEL
        assert event.source_id == sid
        assert event.ts == ended_at
        assert Decimal(str(event.quantity)) == Decimal("42.5")
        assert event.ref_kind == "job" and event.ref_id == job
        assert event.user_id == line.owner
        assert event.project_id == line.project
        assert event.details == {"connector_id": line.connector}

        # A repeated report is not applied and bills nothing more.
        outcome = await line.exchange.state(pod, sid, body)
        assert outcome.status == 409 and outcome.body == {"status": "ended"}
        line.ledger.record_events.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_an_ended_session_without_seconds_bills_nothing(
        self, db, store, line
    ):
        pod = await line.exchange.pod(line.token)
        session = await _active(store, line.connector)
        outcome = await line.exchange.state(
            pod, str(session["id"]), StateBody(status="ended")
        )
        assert outcome.status == 200
        line.ledger.record_events.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_another_lines_session_is_unknown(self, db, store, line):
        pod = await line.exchange.pod(line.token)
        other = await _active(store, await _connector(db))
        sid = str(other["id"])
        for outcome in (
            await line.exchange.state(pod, sid, StateBody(status="failed")),
            await line.exchange.events(pod, sid, EventsBody(events=[])),
            await line.exchange.lease(pod, sid, LeaseBody()),
            await line.exchange.state(pod, "not-a-uuid", StateBody(status="ended")),
            await line.exchange.state(pod, str(uuid4()), StateBody(status="ended")),
        ):
            assert outcome.status == 404
            assert outcome.body == {"error": "unknown_session"}
        assert (await store.get(sid))["status"] == "active"

    @pytest.mark.asyncio
    async def test_events_lease_and_adoption(self, db, store, line):
        pod = await line.exchange.pod(line.token)
        session = await _active(store, line.connector)
        await store.note_admission(session["id"], handoff={"session_id": "s"})
        unhanded = await _active(store, line.connector)
        sid = str(session["id"])

        body = EventsBody(
            events=[
                {"event_id": f"srw_greeting_{sid}", "kind": "instruction"},
                {"event_id": "evt_1", "kind": "transcript", "text": "Hallo"},
            ]
        )
        assert (await line.exchange.events(pod, sid, body)).body == {"inserted": 2}
        assert (await line.exchange.events(pod, sid, body)).body == {"inserted": 0}
        bad = EventsBody(events=[{"kind": "usage"}])
        outcome = await line.exchange.events(pod, sid, bad)
        assert outcome.status == 422

        outcome = await line.exchange.lease(pod, sid, LeaseBody())
        assert outcome.status == 200 and outcome.body["owned"] is True
        assert outcome.body["lease_until"]

        outcome = await line.exchange.adopt(pod)
        assert outcome.status == 200
        (adopted,) = outcome.body["sessions"]
        assert adopted["session_id"] == sid
        assert adopted["provider_session_id"] == session["provider_session_id"]
        assert adopted["handoff"] == {"session_id": "s"}
        assert adopted["sent_event_ids"] == [f"srw_greeting_{sid}"]
        assert adopted["accepted_at"] is not None
        assert str(unhanded["id"]) not in {
            row["session_id"] for row in outcome.body["sessions"]
        }

        outcome = await line.exchange.lease(pod, sid, LeaseBody(release=True))
        assert outcome.body == {"owned": False, "lease_until": None}


# =============================================================================
# The HTTP adapters (routers/live.py) on the real store
# =============================================================================


class _Tasks:
    """Records admission tasks without running them."""

    def __init__(self):
        self.keys: list[str] = []

    def start(self, key, coroutine):
        self.keys.append(key)
        coroutine.close()


def _signed(body: bytes, *, secret: str = SECRET, webhook_id: str | None = None):
    webhook_id = webhook_id or f"wh_{uuid4().hex}"
    timestamp = int(time.time())
    return {
        "content-type": "application/json",
        "webhook-id": webhook_id,
        "webhook-timestamp": str(timestamp),
        "webhook-signature": sign_webhook(secret, webhook_id, timestamp, body),
    }


def _incoming_body(session_id: str) -> bytes:
    return json.dumps(
        {
            "id": f"evt_{uuid4().hex}",
            "type": "live.transport.incoming",
            "created_at": int(time.time()),
            "data": {
                "type": "sip",
                "session_id": session_id,
                "sip_headers": [
                    {"name": "From", "value": "<sip:+4960521829990@sip.example>"},
                    {"name": "To", "value": "<sip:+4969123456@sip.example>"},
                ],
            },
        }
    ).encode()


@pytest_asyncio.fixture
async def http(db, store):
    tasks = _Tasks()
    access = SimpleNamespace(
        job=AsyncMock(return_value=({}, {})), internal=AsyncMock(return_value=None)
    )
    inbound = LiveInboundDependencies(
        db=db,
        pods=SimpleNamespace(handoff=AsyncMock(), reject=AsyncMock()),
        tasks=tasks,
        admit_as_owner=AsyncMock(),
    )
    app = FastAPI()
    app.include_router(live_router)
    app.state.live_dependencies_factory = lambda: LiveDependencies(
        store=store,
        inbound=inbound,
        require_internal=access.internal,
        require_job_access=access.job,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://live"
    ) as client:
        yield SimpleNamespace(client=client, tasks=tasks, access=access)


class TestLiveRouter:
    @pytest.mark.asyncio
    async def test_the_webhook_records_a_session_once_and_admits_it_once(
        self, db, store, http
    ):
        connector = await _connector(db)
        body = _incoming_body("rtc_http")
        response = await http.client.post(
            f"/api/live/webhooks/{connector}", content=body, headers=_signed(body)
        )
        assert response.status_code == 200 and response.json() == {"received": True}
        async with db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM live_sessions WHERE provider_session_id='rtc_http'"
            )
        assert row["status"] == "ringing" and row["origin"] == "webhook"
        assert http.tasks.keys == [f"live-admit-{row['id']}"]

        # OpenAI redelivers until it sees a 2xx: a no-op.
        response = await http.client.post(
            f"/api/live/webhooks/{connector}", content=body, headers=_signed(body)
        )
        assert response.status_code == 200
        assert len(http.tasks.keys) == 1

    @pytest.mark.asyncio
    async def test_the_webhook_refuses_what_it_cannot_verify(self, db, http):
        connector = await _connector(db)
        body = _incoming_body("rtc_bad")
        other = "whsec_" + base64.b64encode(b"another secret").decode()
        for url, headers, status in (
            (f"/api/live/webhooks/{uuid4()}", _signed(body), 404),
            ("/api/live/webhooks/not-a-uuid", _signed(body), 404),
            (f"/api/live/webhooks/{connector}", _signed(body, secret=other), 401),
            (f"/api/live/webhooks/{connector}", {}, 401),
        ):
            response = await http.client.post(url, content=body, headers=headers)
            assert response.status_code == status
        generic = await _connector(db, ds_type="generic", config={})
        response = await http.client.post(
            f"/api/live/webhooks/{generic}", content=body, headers=_signed(body)
        )
        assert response.status_code == 404
        async with db.acquire() as conn:
            assert await conn.fetchval("SELECT count(*) FROM live_sessions") == 0
        assert http.tasks.keys == []

    @pytest.mark.asyncio
    async def test_a_jobs_sessions_and_journal(self, db, store, http):
        connector = await _connector(db)
        session = await _active(store, connector)
        job, other = await _job(db), await _job(db)
        async with db.acquire() as conn:
            await store.bind_job(conn, session["id"], job)
        await store.write_events(
            session["id"],
            [{"event_id": f"evt_{i}", "kind": "transcript"} for i in range(3)],
        )
        response = await http.client.get(f"/api/jobs/{job}/live-sessions")
        assert response.status_code == 200
        (view,) = response.json()["sessions"]
        assert view["id"] == str(session["id"])
        assert view["from_number"] == "+4960521829990"
        assert http.access.job.await_args.args[2] == job

        url = f"/api/jobs/{job}/live-sessions/{session['id']}/events"
        page = (await http.client.get(url, params={"limit": 2})).json()
        assert [event["event_id"] for event in page["events"]] == ["evt_0", "evt_1"]
        rest = (
            await http.client.get(url, params={"after_id": page["next_after_id"]})
        ).json()
        assert [event["event_id"] for event in rest["events"]] == ["evt_2"]
        empty = (
            await http.client.get(url, params={"after_id": rest["next_after_id"]})
        ).json()
        assert empty == {"events": [], "next_after_id": rest["next_after_id"]}

        # Another job's session is not this job's.
        response = await http.client.get(
            f"/api/jobs/{other}/live-sessions/{session['id']}/events"
        )
        assert response.status_code == 404

    @pytest.mark.asyncio
    async def test_the_read_side_is_the_jobs_readers_only(self, db, store, http):
        http.access.job.side_effect = HTTPException(403, "no")
        job = await _job(db)
        response = await http.client.get(f"/api/jobs/{job}/live-sessions")
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_the_stream_of_an_ended_session_drains_and_ends(
        self, db, store, http
    ):
        connector = await _connector(db)
        session = await _active(store, connector)
        job = await _job(db)
        async with db.acquire() as conn:
            await store.bind_job(conn, session["id"], job)
        await store.write_events(
            session["id"], [{"event_id": "evt_closed", "kind": "closed"}]
        )
        await store.transition(session["id"], "ended")
        response = await http.client.get(
            f"/api/jobs/{job}/live-sessions/{session['id']}/stream"
        )
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        frames = response.text.split("\n\n")
        assert frames[0].startswith("event: session\n")
        assert json.loads(frames[0].split("data: ", 1)[1])["status"] == "ended"
        assert frames[1].startswith("event: session_event\n")
        assert json.loads(frames[1].split("data: ", 1)[1])["event_id"] == "evt_closed"

    @pytest.mark.asyncio
    async def test_the_internal_journal_api(self, db, store, http):
        connector = await _connector(db)
        session = await _active(store, connector)
        url = f"/api/internal/live/sessions/{session['id']}/events"
        events = {
            "events": [
                {"event_id": "evt_a", "kind": "answer", "delegation_id": "d1"},
                {"event_id": "evt_b", "kind": "follow_up", "text": "Call back"},
            ]
        }
        response = await http.client.post(url, json=events)
        assert response.status_code == 200 and response.json() == {"inserted": 2}
        assert (await http.client.post(url, json=events)).json() == {"inserted": 0}
        http.access.internal.assert_awaited()

        read = (await http.client.get(url)).json()
        assert [event["event_id"] for event in read["events"]] == ["evt_a", "evt_b"]
        assert read["session"]["id"] == str(session["id"])
        assert read["session"]["status"] == "active"

        unknown = f"/api/internal/live/sessions/{uuid4()}/events"
        assert (await http.client.get(unknown)).status_code == 404
        assert (await http.client.post(unknown, json=events)).status_code == 404
        bad = {"events": [{"kind": "usage"}]}
        assert (await http.client.post(url, json=bad)).status_code == 422
        many = {"events": [{"kind": "system"}] * 501}
        assert (await http.client.post(url, json=many)).status_code == 422

    @pytest.mark.asyncio
    async def test_the_internal_api_needs_the_internal_key(self, db, store, http):
        http.access.internal.side_effect = HTTPException(401, "internal only")
        connector = await _connector(db)
        session = await _active(store, connector)
        url = f"/api/internal/live/sessions/{session['id']}/events"
        assert (await http.client.get(url)).status_code == 401
        response = await http.client.post(url, json={"events": [{"kind": "system"}]})
        assert response.status_code == 401
        assert await store.events(session["id"]) == []
