"""A live connector pod's routes on the lease exchange port (live_session_jobs §5.5).

The HTTP adapter (``routers/connector_lease_exchange.py``) with a fake
``LiveExchange``: every ``/v1/live/*`` route authenticates the pod first and
answers ``no-store``. The real ``LiveExchange`` decisions against a fake
database: which identity is a live pod, a session of another connector, the
usage row of an ended session. The exchange port's body cap, raised for the
live routes only. The lease introspection's execution ids (§5.4).
"""

from __future__ import annotations

import contextlib
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from orchestrator.application import connectors as connectors_composition
from orchestrator.services import connector_lease_exchange as lease_exchange
from orchestrator.services.connector_bind_time_launch import RESULT_PATH
from orchestrator.services.connector_lease_exchange import (
    EXCHANGE_PATH,
    ConnectorLeaseExchange,
)
from orchestrator.services.live import exchange as exchange_module
from orchestrator.services.live.exchange import (
    LIVE_PREFIX,
    MAX_LIVE_BODY_BYTES,
    EventsBody,
    LeaseBody,
    LiveExchange,
    LiveOutcome,
    LivePod,
    StateBody,
    live_engine,
    ws_base_for,
)
from shared.connectors.builtin import GPT_LIVE_SIP_SPEC
from shared.connectors.leases import mint_token

CONNECTOR = "00000000-0000-4000-8000-0000000001c1"
OTHER_CONNECTOR = "00000000-0000-4000-8000-0000000001c2"
SESSION = "00000000-0000-4000-8000-0000000005e1"
POD = LivePod(
    identity_id="00000000-0000-4000-8000-0000000001d1",
    connector_id=CONNECTOR,
    driver="x",
)


# =============================================================================
# The HTTP adapter
# =============================================================================


class _FakeLive:
    """A ``LiveExchange`` double: a pod only for ``sdi_ok``; each operation
    answers its configured outcome and records its call."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self.outcomes: dict[str, LiveOutcome] = {
            "credential": LiveOutcome(200, {"api_key": "k"}),
            "adopt": LiveOutcome(200, {"sessions": []}),
            "state": LiveOutcome(200, {"status": "active"}),
            "events": LiveOutcome(200, {"inserted": 1}),
            "lease": LiveOutcome(200, {"owned": True, "lease_until": None}),
        }

    async def pod(self, token: str) -> LivePod | None:
        self.calls.append(("pod", (token,)))
        return POD if token == "sdi_ok" else None

    def _answer(self, name: str, *args: Any) -> LiveOutcome:
        self.calls.append((name, args))
        return self.outcomes[name]

    async def credential(self, pod):
        return self._answer("credential", pod)

    async def adopt(self, pod):
        return self._answer("adopt", pod)

    async def state(self, pod, session_id, body):
        return self._answer("state", pod, session_id, body)

    async def events(self, pod, session_id, body):
        return self._answer("events", pod, session_id, body)

    async def lease(self, pod, session_id, body):
        return self._answer("lease", pod, session_id, body)

    def operations(self) -> list[str]:
        return [name for name, _ in self.calls if name != "pod"]


@pytest.fixture
def live_client():
    fake = _FakeLive()
    app = connectors_composition.connector_lease_exchange_app(SimpleNamespace())
    app.state.live_exchange_factory = lambda: fake
    return TestClient(app), fake


#: Each route with a valid body.
ROUTES = [
    ("/v1/live/credential", None),
    ("/v1/live/sessions/adopt", None),
    (f"/v1/live/sessions/{SESSION}/state", {"status": "active"}),
    (
        f"/v1/live/sessions/{SESSION}/events",
        {"events": [{"event_id": "e1", "kind": "transcript", "text": "hi"}]},
    ),
    (f"/v1/live/sessions/{SESSION}/lease", {}),
]


def _post(client, path, body, headers=None):
    if body is None:
        return client.post(path, headers=headers or {})
    return client.post(path, json=body, headers=headers or {})


class TestLiveRoutesAuthenticateThePod:
    @pytest.mark.parametrize(("path", "body"), ROUTES)
    @pytest.mark.parametrize(
        "headers",
        [
            {},
            {"Authorization": "Bearer sdi_bad"},
            {"Authorization": "Basic sdi_ok"},
            {"Authorization": "sdi_ok"},
        ],
    )
    def test_a_caller_that_is_no_live_pod_is_401_no_store(
        self, live_client, path, body, headers
    ):
        client, fake = live_client
        response = _post(client, path, body, headers)
        assert response.status_code == 401
        assert response.json() == {"error": "not_a_live_driver_pod"}
        assert response.headers["cache-control"] == "no-store"
        assert fake.operations() == []

    @pytest.mark.parametrize(("path", "body"), ROUTES)
    def test_the_pod_is_served_no_store(self, live_client, path, body):
        client, fake = live_client
        response = _post(client, path, body, {"Authorization": "Bearer sdi_ok"})
        assert response.status_code == 200
        assert response.headers["cache-control"] == "no-store"
        assert len(fake.operations()) == 1
        name, args = fake.calls[-1]
        assert args[0] is POD
        if name in ("state", "events", "lease"):
            assert args[1] == SESSION


class TestLiveRoutesPassTheOutcomeThrough:
    def test_credential_answer(self, live_client):
        client, fake = live_client
        fake.outcomes["credential"] = LiveOutcome(409, {"error": "no_live_voice_model"})
        response = client.post(
            "/v1/live/credential", headers={"Authorization": "Bearer sdi_ok"}
        )
        assert response.status_code == 409
        assert response.json() == {"error": "no_live_voice_model"}

    def test_unknown_session_is_404(self, live_client):
        client, fake = live_client
        fake.outcomes["events"] = LiveOutcome(404, {"error": "unknown_session"})
        response = client.post(
            f"/v1/live/sessions/{SESSION}/events",
            json={"events": []},
            headers={"Authorization": "Bearer sdi_ok"},
        )
        assert response.status_code == 404
        assert response.json() == {"error": "unknown_session"}
        assert response.headers["cache-control"] == "no-store"

    def test_a_state_not_applied_is_409_with_the_current_status(self, live_client):
        client, fake = live_client
        fake.outcomes["state"] = LiveOutcome(409, {"status": "ended"})
        response = client.post(
            f"/v1/live/sessions/{SESSION}/state",
            json={"status": "ended", "usage_seconds": 12.5},
            headers={"Authorization": "Bearer sdi_ok"},
        )
        assert response.status_code == 409
        assert response.json() == {"status": "ended"}
        body = fake.calls[-1][1][2]
        assert isinstance(body, StateBody)
        assert body.status == "ended" and body.usage_seconds == 12.5

    def test_the_lease_release_flag_is_forwarded(self, live_client):
        client, fake = live_client
        client.post(
            f"/v1/live/sessions/{SESSION}/lease",
            json={"release": True},
            headers={"Authorization": "Bearer sdi_ok"},
        )
        body = fake.calls[-1][1][2]
        assert isinstance(body, LeaseBody) and body.release is True

    def test_events_are_parsed(self, live_client):
        client, fake = live_client
        client.post(
            f"/v1/live/sessions/{SESSION}/events",
            json={
                "events": [
                    {
                        "event_id": "e1",
                        "kind": "transcript",
                        "speaker": "caller",
                        "text": "Hallo",
                        "start_ms": 0,
                        "end_ms": 900,
                    }
                ]
            },
            headers={"Authorization": "Bearer sdi_ok"},
        )
        body = fake.calls[-1][1][2]
        assert isinstance(body, EventsBody)
        assert body.events[0].text == "Hallo" and body.events[0].payload == {}


class TestLiveBodyValidation:
    @pytest.mark.parametrize(
        ("path", "body"),
        [
            (f"/v1/live/sessions/{SESSION}/state", {"status": "active", "x": 1}),
            (f"/v1/live/sessions/{SESSION}/state", {"status": "ringing"}),
            (f"/v1/live/sessions/{SESSION}/state", {"status": "rejected"}),
            (
                f"/v1/live/sessions/{SESSION}/state",
                {"status": "ended", "usage_seconds": -1},
            ),
            (
                f"/v1/live/sessions/{SESSION}/events",
                {"events": [{"kind": "transcript"}] * 501},
            ),
            (
                f"/v1/live/sessions/{SESSION}/events",
                {"events": [{"kind": "transcript", "extra": True}]},
            ),
            (f"/v1/live/sessions/{SESSION}/lease", {"release": True, "x": 1}),
        ],
    )
    def test_an_invalid_body_is_422_without_an_echo(self, live_client, path, body):
        client, fake = live_client
        response = client.post(
            path, json=body, headers={"Authorization": "Bearer sdi_ok"}
        )
        assert response.status_code == 422
        assert response.json() == {"error": "invalid_request"}
        assert response.headers["cache-control"] == "no-store"
        assert fake.operations() == []

    def test_five_hundred_events_are_one_write(self, live_client):
        client, fake = live_client
        response = client.post(
            f"/v1/live/sessions/{SESSION}/events",
            json={"events": [{"kind": "transcript"}] * 500},
            headers={"Authorization": "Bearer sdi_ok"},
        )
        assert response.status_code == 200
        assert fake.operations() == ["events"]


# =============================================================================
# The real LiveExchange decisions
# =============================================================================


class _Conn:
    def __init__(self, row: Any) -> None:
        self.row = row
        self.args: list[tuple[Any, ...]] = []

    async def fetchrow(self, _sql: str, *args: Any) -> Any:
        self.args.append(args)
        return self.row


class _DB:
    def __init__(self, identity: Any = None, connector: Any = None) -> None:
        self.conn = _Conn(identity)
        self.get_datasource = AsyncMock(return_value=connector)
        self.resolve_catalog_model = AsyncMock(return_value=None)
        self.get_job = AsyncMock(return_value=None)

    @contextlib.asynccontextmanager
    async def acquire(self):
        yield self.conn


def _identity(**over: Any) -> dict[str, Any]:
    row = {
        "id": uuid4(),
        "connector_id": CONNECTOR,
        "driver": GPT_LIVE_SIP_SPEC.name,
        "revoked": False,
        "service_pod": True,
    }
    row.update(over)
    return row


def _line(**over: Any) -> dict[str, Any]:
    row = {
        "id": CONNECTOR,
        "type": "gpt_live_sip",
        "config": {"model": "gpt-live-1"},
    }
    row.update(over)
    return row


class TestLivePodIdentity:
    @pytest.mark.asyncio
    async def test_a_live_service_pod_is_its_connector(self):
        identity = _identity()
        db = _DB(identity, _line())
        token = mint_token("sdi")
        pod = await LiveExchange(db=db).pod(token)
        assert pod == LivePod(
            identity_id=str(identity["id"]),
            connector_id=CONNECTOR,
            driver=GPT_LIVE_SIP_SPEC.name,
        )
        from shared.connectors.leases import token_digest

        assert db.conn.args == [(token_digest(token),)]
        db.get_datasource.assert_awaited_once_with(CONNECTOR)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "make",
        [
            lambda: "",
            lambda: "sdi_short",
            lambda: mint_token("scl"),
            lambda: "Bearer " + mint_token("sdi"),
        ],
        ids=["empty", "short", "lease-token", "with-scheme"],
    )
    async def test_a_malformed_token_never_reaches_the_database(self, make):
        token = make()
        db = _DB(_identity(), _line())
        assert await LiveExchange(db=db).pod(token) is None
        assert db.conn.args == []

    @pytest.mark.asyncio
    async def test_hosting_off_refuses_every_pod(self):
        db = _DB(_identity(), _line())
        exchange = LiveExchange(db=db, service_hosting=False)
        assert await exchange.pod(mint_token("sdi")) is None
        assert db.conn.args == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("identity", "connector"),
        [
            (None, _line()),
            (_identity(revoked=True), _line()),
            # A bind-time or lease-probe identity is no service pod.
            (_identity(service_pod=False), _line()),
            # The connector is gone.
            (_identity(), None),
            # Not a live driver's connector.
            (_identity(driver="srw.generic/v1"), _line(type="generic")),
            # The identity's driver is not the connector's.
            (_identity(driver="srw.other/v1"), _line()),
        ],
    )
    async def test_anything_else_is_refused(self, identity, connector):
        db = _DB(identity, connector)
        assert await LiveExchange(db=db).pod(mint_token("sdi")) is None


def _exchange(session: dict | None = None, **db_over: Any):
    db = _DB(None, _line())
    for key, value in db_over.items():
        setattr(db, key, value)
    exchange = LiveExchange(db=db, ledger=SimpleNamespace(is_available=True))
    exchange.sessions = SimpleNamespace(
        get=AsyncMock(return_value=session),
        transition=AsyncMock(),
        write_events=AsyncMock(return_value=2),
        lease=AsyncMock(),
        adoptable=AsyncMock(return_value=[]),
    )
    return exchange


def _session(**over: Any) -> dict[str, Any]:
    row = {
        "id": SESSION,
        "connector_id": CONNECTOR,
        "provider_session_id": "rtc_1",
        "status": "active",
        "job_id": None,
    }
    row.update(over)
    return row


class TestLiveSessionsOfThePod:
    @pytest.mark.asyncio
    async def test_a_session_of_another_connector_is_unknown(self):
        exchange = _exchange(_session(connector_id=OTHER_CONNECTOR))
        for outcome in (
            await exchange.state(POD, SESSION, StateBody(status="ended")),
            await exchange.events(POD, SESSION, EventsBody(events=[])),
            await exchange.lease(POD, SESSION, LeaseBody()),
        ):
            assert outcome == LiveOutcome(404, {"error": "unknown_session"})
        exchange.sessions.transition.assert_not_awaited()
        exchange.sessions.write_events.assert_not_awaited()
        exchange.sessions.lease.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_session_id_that_is_no_uuid_is_unknown_unread(self):
        exchange = _exchange(_session())
        outcome = await exchange.state(POD, "rtc_1", StateBody(status="active"))
        assert outcome.status == 404
        exchange.sessions.get.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_missing_session_is_unknown(self):
        exchange = _exchange(None)
        outcome = await exchange.events(POD, SESSION, EventsBody(events=[]))
        assert outcome == LiveOutcome(404, {"error": "unknown_session"})

    @pytest.mark.asyncio
    async def test_ended_writes_the_usage_row(self, monkeypatch):
        usage = AsyncMock(return_value=1)
        monkeypatch.setattr(exchange_module, "record_session_usage", usage)
        exchange = _exchange(_session())
        ended = _session(status="ended", usage_seconds=42)
        exchange.sessions.transition.return_value = (True, ended)
        ended_at = datetime(2026, 10, 13, 9, 0, tzinfo=timezone.utc)
        outcome = await exchange.state(
            POD,
            SESSION,
            StateBody(
                status="ended",
                ended_at=ended_at,
                close_reason="remote_hangup",
                usage_seconds=42,
            ),
        )
        assert outcome == LiveOutcome(200, {"status": "ended"})
        exchange.sessions.transition.assert_awaited_once_with(
            SESSION,
            "ended",
            accepted_at=None,
            attached_at=None,
            ended_at=ended_at,
            close_reason="remote_hangup",
            usage_seconds=42,
            error=None,
        )
        usage.assert_awaited_once_with(exchange.ledger, exchange.db, ended)

    @pytest.mark.asyncio
    async def test_active_writes_no_usage(self, monkeypatch):
        usage = AsyncMock()
        monkeypatch.setattr(exchange_module, "record_session_usage", usage)
        exchange = _exchange(_session(status="ringing"))
        exchange.sessions.transition.return_value = (True, _session())
        outcome = await exchange.state(POD, SESSION, StateBody(status="active"))
        assert outcome == LiveOutcome(200, {"status": "active"})
        usage.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_status_not_applied_is_409_and_bills_nothing(self, monkeypatch):
        usage = AsyncMock()
        monkeypatch.setattr(exchange_module, "record_session_usage", usage)
        exchange = _exchange(_session(status="ended"))
        exchange.sessions.transition.return_value = (False, _session(status="ended"))
        outcome = await exchange.state(
            POD, SESSION, StateBody(status="ended", usage_seconds=3)
        )
        assert outcome == LiveOutcome(409, {"status": "ended"})
        usage.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_unknown_event_kind_is_422_and_nothing_is_written(self):
        exchange = _exchange(_session())
        body = EventsBody(
            events=[{"kind": "transcript"}, {"kind": "usage", "event_id": "u1"}]
        )
        outcome = await exchange.events(POD, SESSION, body)
        assert outcome == LiveOutcome(422, {"error": "unknown_event_kind"})
        exchange.sessions.write_events.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_events_are_written(self):
        exchange = _exchange(_session())
        body = EventsBody(events=[{"kind": "transcript", "event_id": "e1"}])
        outcome = await exchange.events(POD, SESSION, body)
        assert outcome == LiveOutcome(200, {"inserted": 2})
        session_id, events = exchange.sessions.write_events.await_args.args
        assert session_id == SESSION
        assert events[0]["event_id"] == "e1" and events[0]["payload"] == {}

    @pytest.mark.asyncio
    async def test_the_lease_is_the_pods_identity(self):
        exchange = _exchange(_session())
        until = datetime(2026, 10, 13, 9, 0, tzinfo=timezone.utc)
        exchange.sessions.lease.return_value = (True, until)
        outcome = await exchange.lease(POD, SESSION, LeaseBody())
        assert outcome == LiveOutcome(
            200, {"owned": True, "lease_until": until.isoformat()}
        )
        exchange.sessions.lease.assert_awaited_once_with(
            SESSION, POD.identity_id, release=False
        )
        exchange.sessions.lease.return_value = (False, None)
        outcome = await exchange.lease(POD, SESSION, LeaseBody(release=True))
        assert outcome.body == {"owned": False, "lease_until": None}
        assert exchange.sessions.lease.await_args.kwargs == {"release": True}

    @pytest.mark.asyncio
    async def test_adopt_lists_only_sessions_with_a_handoff(self):
        exchange = _exchange()
        accepted = datetime(2026, 10, 13, 9, 0, tzinfo=timezone.utc)
        job_id = uuid4()
        exchange.sessions.adoptable.return_value = [
            _session(
                id=SESSION,
                job_id=job_id,
                accepted_at=accepted,
                attached_at=None,
                handoff={"session_id": SESSION},
                sent_event_ids=["srw_greeting_" + SESSION],
            ),
            _session(id=str(uuid4()), handoff={}, sent_event_ids=[]),
        ]
        outcome = await exchange.adopt(POD)
        assert outcome.status == 200
        assert outcome.body == {
            "sessions": [
                {
                    "session_id": SESSION,
                    "provider_session_id": "rtc_1",
                    "job_id": str(job_id),
                    "accepted_at": accepted.isoformat(),
                    "attached_at": None,
                    "handoff": {"session_id": SESSION},
                    "sent_event_ids": ["srw_greeting_" + SESSION],
                }
            ]
        }
        exchange.sessions.adoptable.assert_awaited_once_with(CONNECTOR, POD.identity_id)


class TestLiveCredential:
    @pytest.mark.asyncio
    async def test_the_connectors_model_endpoint_key(self):
        db = _DB(None, _line())
        db.resolve_catalog_model.return_value = {
            "api_key": "sk-live",
            "provider_kind": "endpoint",
            "endpoint_base_url": "https://llm.example.test/v1/",
            "model_id": "gpt-live-1",
        }
        outcome = await LiveExchange(db=db).credential(POD)
        assert outcome == LiveOutcome(
            200,
            {
                "api_key": "sk-live",
                "api_base": "https://llm.example.test/v1",
                "ws_base": "wss://llm.example.test/v1",
                "headers": {},
                "max_cache_seconds": 300,
            },
        )
        db.resolve_catalog_model.assert_awaited_once_with(
            "gpt-live-1", capability="live_voice"
        )
        db.get_datasource.assert_awaited_once_with(CONNECTOR)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("connector", "model"),
        [
            (None, None),
            (_line(config={}), None),
            (_line(), None),
            (_line(), {"api_key": None, "endpoint_base_url": "https://x"}),
            (_line(), {"api_key": "k", "provider_kind": "endpoint"}),
            (
                _line(),
                {"api_key": "k", "provider_kind": "system", "provider_ref": "acme"},
            ),
        ],
    )
    async def test_no_usable_model_is_409(self, connector, model):
        db = _DB(None, connector)
        db.resolve_catalog_model.return_value = model
        outcome = await LiveExchange(db=db).credential(POD)
        assert outcome == LiveOutcome(409, {"error": "no_live_voice_model"})


class TestLiveEngine:
    @pytest.mark.asyncio
    async def test_a_system_provider_uses_its_known_base(self):
        db = SimpleNamespace(
            resolve_catalog_model=AsyncMock(
                return_value={
                    "api_key": "sk",
                    "provider_kind": "system",
                    "provider_ref": "openai",
                    "endpoint_base_url": "https://ignored.test",
                    "model_id": "gpt-live-1",
                }
            )
        )
        assert await live_engine(db, "gpt-live-1") == {
            "api_key": "sk",
            "api_base": "https://api.openai.com/v1",
            "ws_base": "wss://api.openai.com/v1",
            "model": "gpt-live-1",
        }

    @pytest.mark.asyncio
    async def test_an_endpoint_row_uses_its_base_url(self):
        db = SimpleNamespace(
            resolve_catalog_model=AsyncMock(
                return_value={
                    "api_key": "sk",
                    "provider_kind": "endpoint",
                    "endpoint_base_url": "http://gateway.local:8080/v1",
                }
            )
        )
        engine = await live_engine(db, "gpt-live-1")
        assert engine["api_base"] == "http://gateway.local:8080/v1"
        assert engine["ws_base"] == "ws://gateway.local:8080/v1"
        # Without the row's model id the requested id stands.
        assert engine["model"] == "gpt-live-1"

    @pytest.mark.parametrize(
        ("base", "ws"),
        [
            ("https://api.openai.com/v1", "wss://api.openai.com/v1"),
            ("http://h:81/v1", "ws://h:81/v1"),
            ("https://h/v1?x=1#f", "wss://h/v1"),
        ],
    )
    def test_ws_base_for(self, base, ws):
        assert ws_base_for(base) == ws


# =============================================================================
# The exchange port's body cap
# =============================================================================


async def _asgi_call(app, *, body_chunks, headers=(), path=EXCHANGE_PATH):
    sent: list[dict] = []
    chunks = list(body_chunks)

    async def receive():
        if chunks:
            chunk = chunks.pop(0)
            return {"type": "http.request", "body": chunk, "more_body": bool(chunks)}
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "root_path": "",
        "query_string": b"",
        "headers": [(k.encode(), v.encode()) for k, v in headers],
        "client": ("10.42.0.9", 40000),
        "server": ("127.0.0.1", 8088),
    }
    await app(scope, receive, send)
    return sent


async def _echo(scope, receive, send):
    body = b""
    while True:
        message = await receive()
        body += message.get("body", b"")
        if not message.get("more_body"):
            break
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": str(len(body)).encode()})


EVENTS_PATH = f"/v1/live/sessions/{SESSION}/events"


def _bounded(**kwargs):
    return connectors_composition.BodyLimit(
        _echo, prefix_limits={LIVE_PREFIX: MAX_LIVE_BODY_BYTES}, **kwargs
    )


class TestLiveBodyLimit:
    @pytest.mark.asyncio
    async def test_a_journal_batch_passes_on_a_live_path_only(self):
        body = b"x" * (64 * 1024)
        headers = [("content-length", str(len(body)))]
        sent = await _asgi_call(
            _bounded(), body_chunks=[body], headers=headers, path=EVENTS_PATH
        )
        assert sent[0]["status"] == 200 and sent[1]["body"] == str(len(body)).encode()
        sent = await _asgi_call(
            _bounded(), body_chunks=[body], headers=headers, path=EXCHANGE_PATH
        )
        assert sent[0]["status"] == 413

    @pytest.mark.asyncio
    async def test_the_live_cap_is_one_mebibyte(self):
        assert MAX_LIVE_BODY_BYTES == 1024 * 1024
        exact = b"x" * MAX_LIVE_BODY_BYTES
        sent = await _asgi_call(
            _bounded(),
            body_chunks=[exact],
            headers=[("content-length", str(len(exact)))],
            path=EVENTS_PATH,
        )
        assert sent[0]["status"] == 200
        over = MAX_LIVE_BODY_BYTES + 1
        sent = await _asgi_call(
            _bounded(),
            body_chunks=[b"x"],
            headers=[("content-length", str(over))],
            path=EVENTS_PATH,
        )
        assert sent[0]["status"] == 413
        # Streamed without a declared length, too.
        sent = await _asgi_call(
            _bounded(),
            body_chunks=[exact, b"x"],
            path="/v1/live/credential",
        )
        assert sent[0]["status"] == 413

    @pytest.mark.asyncio
    async def test_the_prefix_is_a_path_prefix(self):
        body = b"x" * 8192
        for path in ("/v1/live", "/v1/livex/a", "/x/v1/live/a"):
            sent = await _asgi_call(
                _bounded(),
                body_chunks=[body],
                headers=[("content-length", "8192")],
                path=path,
            )
            assert sent[0]["status"] == 413, path

    @pytest.mark.asyncio
    async def test_an_exact_path_limit_wins_over_a_prefix(self):
        limited = _bounded(limit=16, path_limits={"/v1/live/sessions/adopt": 32})
        sent = await _asgi_call(
            limited,
            body_chunks=[b"x" * 40],
            headers=[("content-length", "40")],
            path="/v1/live/sessions/adopt",
        )
        assert sent[0]["status"] == 413
        sent = await _asgi_call(
            limited,
            body_chunks=[b"x" * 40],
            headers=[("content-length", "40")],
            path=EVENTS_PATH,
        )
        assert sent[0]["status"] == 200

    def test_the_server_raises_the_cap_for_live_routes_only(self):
        config = connectors_composition.exchange_server_config(object())
        app = config.app
        assert isinstance(app, connectors_composition.BodyLimit)
        assert app.limit == connectors_composition.MAX_BODY_BYTES == 4096
        assert app.prefix_limits == {LIVE_PREFIX: MAX_LIVE_BODY_BYTES}
        assert app.path_limits == {RESULT_PATH: connectors_composition.MAX_RESULT_BYTES}
        assert app._limit_for(EVENTS_PATH) == MAX_LIVE_BODY_BYTES
        assert app._limit_for(EXCHANGE_PATH) == 4096
        assert app._limit_for(RESULT_PATH) == connectors_composition.MAX_RESULT_BYTES

    def test_a_large_journal_batch_reaches_the_route_through_the_real_port(self):
        fake = _FakeLive()
        app = connectors_composition.connector_lease_exchange_app(SimpleNamespace())
        app.state.live_exchange_factory = lambda: fake
        bounded = TestClient(connectors_composition.exchange_server_config(app).app)
        events = [{"kind": "transcript", "text": "x" * 1000} for _ in range(100)]
        response = bounded.post(
            EVENTS_PATH,
            json={"events": events},
            headers={"Authorization": "Bearer sdi_ok"},
        )
        assert response.status_code == 200
        assert fake.operations() == ["events"]
        response = bounded.post(
            EXCHANGE_PATH,
            json={"lease_token": "x" * 100, "operation": "read", "pad": "x" * 8192},
        )
        assert response.status_code == 413


# =============================================================================
# The lease introspection names the execution holding the lease (§5.4)
# =============================================================================


class _IntrospectStore:
    def __init__(self, row: dict[str, Any]) -> None:
        self.row = row

    @contextlib.asynccontextmanager
    async def acquire(self):
        yield SimpleNamespace(fetchrow=AsyncMock(return_value=self.row))


def _lease_row(**over: Any) -> dict[str, Any]:
    row = {
        "identity_id": "i-1",
        "identity_connector_id": CONNECTOR,
        "identity_driver": GPT_LIVE_SIP_SPEC.name,
        "identity_revoked": False,
        "identity_service_pod": True,
        "lease_id": "l-1",
        "connector_id": CONNECTOR,
        "driver": GPT_LIVE_SIP_SPEC.name,
        "access": "Session",
        "lease_revoked": False,
        "revoke_reason": None,
        "lease_unexpired": True,
        "expires_at": datetime(2026, 10, 13, 9, 0, tzinfo=timezone.utc)
        + timedelta(minutes=5),
        "lease_job_id": None,
        "lease_thread_id": None,
    }
    row.update(over)
    return row


class TestIntrospectNamesTheExecution:
    @pytest.mark.asyncio
    async def test_a_jobs_lease_carries_its_job_id(self):
        job_id = uuid4()
        exchange = ConnectorLeaseExchange(
            store=_IntrospectStore(_lease_row(lease_job_id=job_id)), drivers=None
        )
        outcome = await exchange.introspect(
            identity_token=mint_token("sdi"), lease_token=mint_token("scl")
        )
        assert outcome.status == 200
        assert outcome.body["active"] is True
        assert outcome.body["connector_id"] == CONNECTOR
        assert outcome.body["job_id"] == str(job_id)
        assert outcome.body["thread_id"] is None

    @pytest.mark.asyncio
    async def test_a_sessions_lease_carries_its_thread_id(self):
        thread_id = uuid4()
        exchange = ConnectorLeaseExchange(
            store=_IntrospectStore(_lease_row(lease_thread_id=thread_id)),
            drivers=None,
        )
        outcome = await exchange.introspect(
            identity_token=mint_token("sdi"), lease_token=mint_token("scl")
        )
        assert outcome.body["job_id"] is None
        assert outcome.body["thread_id"] == str(thread_id)

    @pytest.mark.asyncio
    async def test_an_expired_lease_names_nothing(self):
        exchange = ConnectorLeaseExchange(
            store=_IntrospectStore(
                _lease_row(lease_unexpired=False, lease_job_id=uuid4())
            ),
            drivers=None,
        )
        outcome = await exchange.introspect(
            identity_token=mint_token("sdi"), lease_token=mint_token("scl")
        )
        assert outcome.body == {"active": False}

    def test_the_check_query_reads_the_lease_owner(self):
        sql = " ".join(lease_exchange._CHECK.split())
        assert "lease_job_id" in sql and "lease_thread_id" in sql
