"""P2 integration: migrated PostgreSQL and the real fake Live HTTP/WebSocket API."""

from __future__ import annotations

import asyncio
import base64
import json
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
import pytest_asyncio

from orchestrator.database.migrate import run_migrations
from orchestrator.database.postgres import PostgresDB
from orchestrator.routers.telephony import TelephonyDependencies, router
from orchestrator.security import access
from orchestrator.security.access import redact_config_override
from orchestrator.services.telephony import recorder as recorder_module
from orchestrator.services.telephony.front import TelephonyFront
from orchestrator.services.telephony.jobs import create_phone_job, dispatch_config
from orchestrator.services.telephony.recorder import (
    Recorder,
    TelephonyTaskRegistry,
    client_for,
)
from orchestrator.services.telephony.store import TelephonyStore, settings_view
from shared.runtime.telephony.gpt_live import build_session_config
from shared.runtime.telephony.webhook import parse_incoming_call, sign_webhook
from tests._fake_gpt_live import DEFAULT_KEY, FakeGptLive
from fastapi import FastAPI

ROOT = Path(__file__).resolve().parents[1]
SECRET = "whsec_" + base64.b64encode(b"p2-integration" * 3).decode()


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
            await run_migrations(
                db.pool, ROOT / "src/orchestrator/database/migrations/app"
            )
            pg["initialized"] = True
        async with db.acquire() as conn:
            await conn.execute(
                "TRUNCATE calls, telephony_lines, telephony_settings CASCADE"
            )
        yield db
    finally:
        await db.close()


@pytest_asyncio.fixture
async def fake():
    async with FakeGptLive() as fake:
        yield fake


@pytest_asyncio.fixture
async def phone(db, fake, monkeypatch):
    store = TelephonyStore(db)
    owner = await db.create_user("Phone integration")
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE users SET is_approved=true,is_admin=true WHERE id=$1", owner["id"]
        )
    project = await db.create_project("Phone integration")
    await db.add_project_member(str(project["id"]), str(owner["id"]), "owner")
    settings = await store.save(
        "telephony_settings",
        {
            "name": "Fake project",
            "openai_project_id": "proj_fake",
            "project_id": project["id"],
            "webhook_secret": SECRET,
            "api_key": DEFAULT_KEY,
            "api_base": fake.api_base,
            "ws_base": fake.ws_base,
        },
    )
    line = await store.save(
        "telephony_lines",
        {
            "settings_id": settings["id"],
            "project_id": project["id"],
            "owner_user_id": owner["id"],
            "name": "Reception",
            "number": "+49601234",
            "is_default": True,
            "expert": "general-worker",
            "greeting": "Guten Tag. Ich bin die KI-Assistentin.",
            "persona": "Sie sind die Rezeption.",
            "fact_sheet": "Das Bad öffnet um 9 Uhr.",
        },
    )
    registry = TelephonyTaskRegistry()
    front = TelephonyFront(store, registry, Mock())
    front._create_job = AsyncMock()
    monkeypatch.setattr(recorder_module, "MONITOR_SECONDS", 0.02)
    app = FastAPI()
    app.include_router(router)
    app.state.telephony_dependencies_factory = lambda: TelephonyDependencies(
        store=store, front=front
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://phone"
    ) as http:
        try:
            yield SimpleNamespace(
                store=store,
                settings=settings,
                line=line,
                owner=owner,
                project=project,
                registry=registry,
                front=front,
                fake=fake,
                http=http,
                app=app,
            )
        finally:
            await registry.drain()


async def eventually(predicate, timeout=5):
    async with asyncio.timeout(timeout):
        while True:
            value = await predicate()
            if value:
                return value
            await asyncio.sleep(0.01)


async def incoming(phone, *, to_number="proj_translated", webhook_id="wh_integration"):
    session_id = await phone.fake.incoming_call(to_number=to_number)
    body = json.dumps(phone.fake.webhook_payload(session_id)).encode()
    timestamp = int(time.time())
    headers = {
        "webhook-id": webhook_id,
        "webhook-timestamp": str(timestamp),
        "webhook-signature": sign_webhook(SECRET, webhook_id, timestamp, body),
    }
    response = await phone.http.post(
        "/api/telephony/openai/webhook", content=body, headers=headers
    )
    assert response.status_code == 200
    async with phone.store.db.acquire() as conn:
        call_id = await conn.fetchval(
            "SELECT id FROM calls WHERE session_id=$1", session_id
        )
    return session_id, call_id, body, headers


async def events_of(phone, call_id, kind):
    return [
        event for event in await phone.store.events(call_id) if event["kind"] == kind
    ]


class TestPostgresLiveFront:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("number", ["+49601234", "proj_translated"])
    async def test_signature_dedupe_line_and_accept(self, phone, number):
        session_id, call_id, body, headers = await incoming(phone, to_number=number)
        await phone.fake.wait_until(session_id, lambda s: bool(s.commands))
        response = await phone.http.post(
            "/api/telephony/openai/webhook", content=body, headers=headers
        )
        assert response.status_code == 200
        changed_id = {
            **headers,
            "webhook-id": "wh_new_delivery",
            "webhook-signature": sign_webhook(
                SECRET, "wh_new_delivery", int(headers["webhook-timestamp"]), body
            ),
        }
        assert (
            await phone.http.post(
                "/api/telephony/openai/webhook", content=body, headers=changed_id
            )
        ).status_code == 200
        session = phone.fake.session(session_id)
        assert [action for action, _ in session.rest_calls].count("accept") == 1
        config = session.accept_config
        assert (
            config["type"] == "live" and config["audio"]["output"]["voice"] == "marin"
        )
        assert (
            "Sie sind die Rezeption." in config["instructions"]
            and "Das Bad öffnet" in config["instructions"]
        )
        saved = await phone.store.get_call(call_id)
        assert saved["line_id"] == phone.line["id"] and saved["status"] == "active"
        assert (saved["accepted_at"] - saved["created_at"]).total_seconds() < 2
        assert session.commands[0]["type"] == "session.instructions.append"
        assert session.commands[0]["delegation_id"] is None
        assert (
            '"Guten Tag. Ich bin die KI-Assistentin."' in session.commands[0]["content"]
        )

    @pytest.mark.asyncio
    async def test_disabled_lines_reject(self, phone):
        await phone.store.save("telephony_lines", {"enabled": False}, phone.line["id"])
        session_id, call_id, _, _ = await incoming(phone)
        await phone.fake.wait_until(session_id, lambda s: s.status == "rejected")

        async def rejected():
            return (await phone.store.get_call(call_id))["status"] == "rejected"

        await eventually(rejected)
        assert phone.fake.session(session_id).reject_status == 404

    @pytest.mark.asyncio
    async def test_transcripts_replay_delegation_usage_and_closed(self, phone):
        session_id, call_id, _, _ = await incoming(phone)
        await phone.fake.wait_until(session_id, lambda s: bool(s.commands))
        event = await phone.fake.emit(
            session_id,
            {
                "type": "session.input_transcript.delta",
                "delta": "Hallo",
                "start_ms": 3,
                "end_ms": 7,
            },
        )
        await phone.fake.say(session_id, "assistant", "Guten Tag")
        delegation = await phone.fake.delegate(session_id)
        await phone.fake.emit(
            session_id, {"type": "session.usage.updated", "usage": {"seconds": 12}}
        )
        await phone.fake.emit(
            session_id, {"type": "transport.dtmf.received", "event": "5"}
        )
        await phone.fake.emit(
            session_id,
            {"type": "error", "error": {"code": "test_error", "message": "refused"}},
        )
        await eventually(lambda: events_of(phone, call_id, "error"))
        # Drop the recorder socket, then replay the same engine event across a
        # fresh Sideband's local dedupe window: DB uniqueness is the authority.
        await phone.fake.session(session_id).sockets[0].close()
        await phone.fake.wait_until(session_id, lambda s: s.attach_count >= 2)
        await phone.fake.emit(session_id, event)
        await phone.fake.say(session_id, "caller", "Noch eine Frage")

        async def all_transcripts():
            rows = await events_of(phone, call_id, "transcript")
            return rows if len(rows) == 3 else None

        rows = await eventually(all_transcripts)
        assert [row["text"] for row in rows].count("Hallo") == 1
        assert rows[0]["start_ms"] == 3 and rows[0]["speaker"] == "caller"
        assert (await events_of(phone, call_id, "delegation"))[0][
            "delegation_id"
        ] == delegation
        assert (await phone.store.get_call(call_id))["usage_seconds"] == 12
        assert not await events_of(phone, call_id, "usage")
        assert len(await events_of(phone, call_id, "instruction")) == 1
        phone.fake.session(session_id).usage_seconds = 18
        await phone.fake.remote_hangup(session_id)
        await eventually(lambda: events_of(phone, call_id, "closed"))
        saved = await phone.store.get_call(call_id)
        assert saved["status"] == "ended" and saved["ended_at"]
        assert saved["close_reason"] == "remote_hangup" and saved["usage_seconds"] == 18

        async def released():
            return (await phone.store.get_call(call_id))["recorder_owner"] is None

        await eventually(released)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("attached", [False, True])
    async def test_fallback_only_without_actor(self, phone, attached):
        await phone.store.save(
            "telephony_lines", {"no_job_fallback_s": 1}, phone.line["id"]
        )
        session_id, call_id, _, _ = await incoming(phone)
        await phone.fake.wait_until(session_id, lambda s: bool(s.commands))
        if attached:
            async with phone.store.db.acquire() as conn:
                await conn.execute(
                    "UPDATE calls SET attached_at=now() WHERE id=$1", call_id
                )
        delegation = await phone.fake.delegate(session_id)
        if attached:
            await asyncio.sleep(1.2)
            assert not await events_of(phone, call_id, "fallback")
            assert not any(
                c["type"] == "session.commentary.append"
                for c in phone.fake.session(session_id).commands
            )
        else:
            command = await phone.fake.wait_for_command(
                session_id, "session.commentary.append"
            )
            assert (
                command["delegation_id"] == delegation
                and "Ich habe Ihr Anliegen notiert" in command["content"]
            )
            assert (
                len(await eventually(lambda: events_of(phone, call_id, "fallback")))
                == 1
            )

    @pytest.mark.asyncio
    async def test_max_duration_hangup(self, phone):
        await phone.store.save(
            "telephony_lines", {"max_duration_s": 1}, phone.line["id"]
        )
        session_id, call_id, _, _ = await incoming(phone)
        await phone.fake.wait_until(session_id, lambda s: s.status == "closed")
        await eventually(lambda: events_of(phone, call_id, "closed"))
        rows = await events_of(phone, call_id, "hangup")
        assert len(rows) == 1 and rows[0]["speaker"] == "recorder"
        assert rows[0]["payload"]["reason"] == "max_duration"
        assert (await phone.store.get_call(call_id))["status"] == "ended"

    @pytest.mark.asyncio
    async def test_expired_lease_takeover(self, phone):
        session_id = await phone.fake.incoming_call()
        pending = parse_incoming_call(
            phone.fake.webhook_payload(session_id), "wh_takeover"
        )
        call = await phone.store.insert_incoming(phone.settings["id"], pending)
        await phone.store.set_line(call["id"], phone.line)
        async with client_for(phone.settings, DEFAULT_KEY) as client:
            await client.accept(
                session_id, build_session_config(instructions="Reception")
            )
        await phone.store.activate(call["id"])
        async with phone.store.db.acquire() as conn:
            await conn.execute(
                "UPDATE calls SET recorder_owner='old-replica',recorder_lease_until=now()-interval '1 second' WHERE id=$1",
                call["id"],
            )
        recorder = Recorder(phone.store, phone.registry)
        await recorder.sweep()
        await phone.fake.wait_until(session_id, lambda s: bool(s.commands))
        saved = await phone.store.get_call(call["id"])
        assert saved["recorder_owner"] == phone.registry.owner
        assert await phone.store.claim("other-replica", call["id"]) == []
        assert not await phone.store.renew(call["id"], "old-replica")
        await phone.store.release(call["id"], "old-replica")
        assert (await phone.store.get_call(call["id"]))[
            "recorder_owner"
        ] == phone.registry.owner

    @pytest.mark.asyncio
    async def test_gone_session_closes_only_after_retry_window(
        self, phone, monkeypatch
    ):
        monkeypatch.setattr(recorder_module, "LOST_RETRY_SECONDS", 0.1)
        session_id, call_id, _, _ = await incoming(phone)
        await phone.fake.wait_until(session_id, lambda s: bool(s.commands))
        session = phone.fake.session(session_id)
        session.status = "closed"
        await session.sockets[0].close()

        async def lost():
            return (await phone.store.get_call(call_id))[
                "close_reason"
            ] == "recorder_lost"

        await eventually(lost)
        assert (await phone.store.get_call(call_id))["status"] == "ended"

    @pytest.mark.asyncio
    async def test_shutdown_preserves_active_call_and_releases_lease(self, phone):
        session_id, call_id, _, _ = await incoming(phone)
        await phone.fake.wait_until(session_id, lambda s: bool(s.commands))
        await phone.registry.drain()
        saved = await phone.store.get_call(call_id)
        assert saved["status"] == "active" and saved["ended_at"] is None
        assert saved["recorder_owner"] is None
        assert phone.fake.session(session_id).status == "accepted"


class TestPostgresJobAndRoutes:
    @pytest.mark.asyncio
    async def test_job_creation_credentials_and_journal_routes(
        self, phone, monkeypatch
    ):
        session_id, call_id, _, _ = await incoming(phone)
        await phone.fake.wait_until(session_id, lambda s: bool(s.commands))
        call = await phone.store.get_call(call_id)
        job = await create_phone_job(phone.store, call, phone.line)
        saved_job = await phone.store.db.get_job(str(job["id"]))
        context = saved_job["context"]
        context = json.loads(context) if isinstance(context, str) else context
        override = saved_job["config_override"]
        override = json.loads(override) if isinstance(override, str) else override
        # get_job does not project origin; read the column itself.
        async with phone.store.db.acquire() as conn:
            origin = await conn.fetchval(
                "SELECT origin FROM jobs WHERE id=$1", job["id"]
            )
        assert origin == "telephony" and saved_job["execution_lane"] == "pinned"
        assert override["workspace"]["backend"] == "none" and not saved_job["repo_name"]
        assert context["telephony"]["call_id"] == str(call_id)
        config = await dispatch_config(phone.store.db, saved_job)
        assert config["api_key"] == DEFAULT_KEY and config["session_id"] == session_id
        assert DEFAULT_KEY not in json.dumps(
            redact_config_override({"telephony": config})
        )
        # Real job authorization, using only the already-resolved account seam.
        user = {
            **phone.owner,
            "is_admin": True,
            "real_is_admin": True,
            "is_approved": True,
        }
        monkeypatch.setattr(
            access, "require_approved_user", AsyncMock(return_value=user)
        )
        monkeypatch.setattr(access, "_INTERNAL_KEY", "p2-internal")
        key = {"X-Internal-Key": "p2-internal"}
        job_id = str(job["id"])
        response = await phone.http.get(f"/api/jobs/{job_id}/calls")
        assert response.json()["calls"][0]["id"] == str(call_id)
        url = f"/api/internal/telephony/calls/{call_id}/events"
        body = {
            "events": [
                {
                    "event_id": "actor_gap",
                    "kind": "transcript",
                    "speaker": "caller",
                    "text": "Recovered gap",
                }
            ]
        }
        assert (await phone.http.post(url, json=body, headers=key)).json() == {
            "inserted": 1
        }
        assert (await phone.http.post(url, json=body, headers=key)).json() == {
            "inserted": 0
        }
        attached = await phone.http.post(
            f"/api/internal/telephony/calls/{call_id}/attach",
            json={"job_id": job_id},
            headers=key,
        )
        second = await phone.http.post(
            f"/api/internal/telephony/calls/{call_id}/attach",
            json={"job_id": job_id},
            headers=key,
        )
        assert attached.json()["attached_at"] == second.json()["attached_at"]
        await phone.fake.remote_hangup(session_id)
        await eventually(lambda: events_of(phone, call_id, "closed"))
        first = (
            await phone.http.get(f"/api/jobs/{job_id}/calls/{call_id}/events?limit=1")
        ).json()
        assert len(first["events"]) == 1
        tail = (
            await phone.http.get(
                f"/api/jobs/{job_id}/calls/{call_id}/events?after_id={first['next_after_id']}"
            )
        ).json()
        assert all(event["id"] > first["next_after_id"] for event in tail["events"])
        stream = await phone.http.get(f"/api/jobs/{job_id}/calls/{call_id}/stream")
        assert "event: call_event\n" in stream.text and "Recovered gap" in stream.text
        assert await phone.store.delete("telephony_lines", phone.line["id"])
        historical = await phone.store.get_call(call_id)
        assert historical["line_id"] is None and historical["line_name"] == "Reception"
        resumed = await dispatch_config(phone.store.db, saved_job)
        assert resumed["line"]["id"] == str(phone.line["id"])

    @pytest.mark.asyncio
    async def test_settings_encrypt_and_system_key_fallback(self, phone):
        stored = await phone.store.get_settings(phone.settings["id"])
        assert stored["webhook_secret_encrypted"].startswith("v1:")
        assert stored["api_key_encrypted"].startswith("v1:")
        assert SECRET not in json.dumps(settings_view(stored), default=str)
        await phone.store.db.upsert_system_api_key(
            "openai", "sk-system-p2", "sk-system"
        )
        stored = await phone.store.save(
            "telephony_settings", {"api_key": None}, phone.settings["id"]
        )
        assert await phone.store.api_key(stored) == "sk-system-p2"

    @pytest.mark.asyncio
    async def test_admin_crud_secret_hiding_and_null_rejection(
        self, phone, monkeypatch
    ):
        user = {
            **phone.owner,
            "is_admin": True,
            "real_is_admin": True,
            "is_approved": True,
        }
        monkeypatch.setattr(
            access, "require_approved_user", AsyncMock(return_value=user)
        )
        response = await phone.http.get("/api/telephony/settings")
        assert response.status_code == 200
        assert response.json()["settings"][0]["webhook_secret_set"]
        assert SECRET not in response.text and DEFAULT_KEY not in response.text
        assert (
            await phone.http.patch(
                f"/api/telephony/settings/{phone.settings['id']}",
                json={"webhook_secret": None},
            )
        ).status_code == 422
        response = await phone.http.patch(
            f"/api/telephony/lines/{phone.line['id']}", json={"name": "New desk"}
        )
        assert response.json()["name"] == "New desk"
        assert (
            await phone.http.delete(f"/api/telephony/lines/{phone.line['id']}")
        ).status_code == 200
        assert (
            await phone.http.delete(f"/api/telephony/settings/{phone.settings['id']}")
        ).status_code == 200


class TestTelephonyLineCli:
    """``python -m orchestrator.operator_cli.telephony_line`` (in-pod setup)."""

    @pytest.mark.asyncio
    async def test_settings_and_line_upsert_by_name_with_secrets_from_stdin(self, db):
        import io

        from orchestrator.operator_cli.telephony_line import run

        owner = await db.create_user("Line CLI owner")
        project = await db.create_project("Line CLI")
        created = await run(
            [
                "settings",
                "--name",
                "Demo",
                "--openai-project-id",
                "proj_demo",
                "--project-id",
                str(project["id"]),
            ],
            stdin=io.StringIO(SECRET + "\nsk-demo-key\n"),
            db=db,
        )
        assert created["webhook_secret_set"] is True and created["api_key_set"] is True
        assert SECRET not in json.dumps(created, default=str)
        async with db.acquire() as conn:
            stored = await conn.fetchrow(
                "SELECT webhook_secret_encrypted, api_key_encrypted FROM telephony_settings WHERE name='Demo'"
            )
        assert (
            SECRET not in stored["webhook_secret_encrypted"]
            and "sk-demo-key" not in stored["api_key_encrypted"]
        )
        # A second run without secrets keeps them and updates the rest.
        updated = await run(
            ["settings", "--name", "Demo", "--openai-project-id", "proj_demo2"],
            stdin=io.StringIO(""),
            db=db,
        )
        assert (
            updated["id"] == created["id"]
            and updated["openai_project_id"] == "proj_demo2"
        )
        assert updated["webhook_secret_set"] is True

        line_json = json.dumps(
            {
                "number": "+4960521829990",
                "is_default": True,
                "greeting": "Guten Tag.",
                "persona": "Sie sind die Rezeption.",
                "fact_sheet": "Bad: 32 Grad.",
            }
        )
        argv = [
            "line",
            "--name",
            "Empfang",
            "--settings-name",
            "Demo",
            "--project-id",
            str(project["id"]),
            "--owner-id",
            str(owner["id"]),
        ]
        line = await run(argv, stdin=io.StringIO(line_json), db=db)
        again = await run(
            argv, stdin=io.StringIO(json.dumps({"voice": "cedar"})), db=db
        )
        assert (
            again["id"] == line["id"]
            and again["voice"] == "cedar"
            and again["number"] == "+4960521829990"
        )
        listed = await run(["show"], stdin=io.StringIO(""), db=db)
        assert [entry["name"] for entry in listed["lines"]] == ["Empfang"]
        assert listed["lines"][0]["fact_sheet_chars"] == len("Bad: 32 Grad.")

    @pytest.mark.asyncio
    async def test_line_rejects_unknown_fields_and_new_settings_need_a_secret(self, db):
        import io

        from orchestrator.operator_cli.telephony_line import run

        with pytest.raises(SystemExit):
            await run(
                ["settings", "--name", "No secret", "--openai-project-id", "proj_x"],
                stdin=io.StringIO(""),
                db=db,
            )
        with pytest.raises(SystemExit):
            await run(
                ["settings", "--name", "Bad secret", "--openai-project-id", "proj_x"],
                stdin=io.StringIO("not-a-whsec\n"),
                db=db,
            )
        await run(
            ["settings", "--name", "S", "--openai-project-id", "proj_x"],
            stdin=io.StringIO(SECRET + "\n"),
            db=db,
        )
        with pytest.raises(SystemExit):
            await run(
                [
                    "line",
                    "--name",
                    "L",
                    "--settings-name",
                    "S",
                    "--project-id",
                    "00000000-0000-0000-0000-000000000000",
                    "--owner-id",
                    "00000000-0000-0000-0000-000000000000",
                ],
                stdin=io.StringIO(json.dumps({"api_key": "nope"})),
                db=db,
            )
