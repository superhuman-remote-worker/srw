"""Asterisk call source against a fake ARI; local_voice_gateway.md §4.2.

FakeAsterisk answers the ARI calls the source makes, sends Stasis events over
the events WebSocket, and connects back to the gateway's media listener the
way chan_websocket does for an externalMedia channel. The real behaviour was
checked against Asterisk 22.11 with the WP0 bench (tests/voice_bench).
"""

from __future__ import annotations

import asyncio
import base64
import json
from contextlib import AsyncExitStack
from datetime import datetime, timedelta, timezone

import aiohttp
import pytest
import pytest_asyncio
import yaml
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from shared.runtime.live.webhook import parse_incoming_call, verify_webhook
from voice_gateway.asterisk.source import (
    AsteriskCallSource,
    AsteriskSettings,
    refer_uri,
    reject_cause,
)
from voice_gateway.config import Config, ConfigError, Line, load_config
from voice_gateway.engines.echo import EchoEngine
from voice_gateway.interfaces import FRAME_BYTES
from voice_gateway.server import Gateway
from voice_gateway.session import ProtocolError

KEY = "gateway-test-key"
SECRET = "whsec_" + base64.b64encode(b"gateway-test-signing-key").decode()
PASSWORD = "ari-test-password"
ACCEPT = {
    "session": {
        "type": "live",
        "model": "echo",
        "instructions": "Generic facts.",
        "audio": {"output": {"voice": "marin"}},
        "delegation": {"type": "client"},
    }
}
HEADERS = {
    "From": ['"Anrufer" <sip:1000@pbx.test>;tag=a1'],
    "To": ["<sip:2000@addon.test>"],
    "Diversion": ["<sip:200@pbx.test>;reason=unconditional"],
}


async def until(predicate, timeout=3.0):
    async with asyncio.timeout(timeout):
        while not (result := predicate()):
            await asyncio.sleep(0.01)
        return result


class FakeAsterisk:
    """The ARI slice the source uses, plus chan_websocket's media connection."""

    def __init__(self):
        self.calls: list[tuple[str, str, dict]] = []
        self.ws: web.WebSocketResponse | None = None
        self.connects = 0
        self.headers: dict[str, dict[str, list[str]]] = {}
        self.variables: dict[str, dict[str, str]] = {}
        self.lines: dict[str, str] = {}
        self.channels: list[dict] = []
        self.media_url: str | None = None
        self.media: dict[str, aiohttp.ClientWebSocketResponse] = {}
        self.media_mode = "connect"
        self.transfer = ("SUCCESS", "0")
        self.dial = "answer"
        self.tasks: set[asyncio.Task] = set()
        self.http: aiohttp.ClientSession | None = None
        self.app = web.Application()
        self.app.router.add_get("/ari/events", self.events)
        self.app.router.add_route("*", "/ari/{tail:.*}", self.rest)
        self.auth = "Basic " + base64.b64encode(f"srw:{PASSWORD}".encode()).decode()

    def spawn(self, coroutine) -> None:
        task = asyncio.create_task(coroutine)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    def made(self, method: str, path: str, **params) -> bool:
        return any(
            m == method
            and p == path
            and all(q.get(k) == str(v) for k, v in params.items())
            for m, p, q in self.calls
        )

    async def emit(self, kind: str, channel: str, **data) -> None:
        await until(lambda: self.ws is not None and not self.ws.closed)
        await self.ws.send_json({"type": kind, "channel": {"id": channel}, **data})

    async def incoming(self, channel="caller-1", line="bench", headers=HEADERS):
        self.headers[channel] = headers
        self.lines[channel] = line
        self.variables.setdefault(channel, {})["SRW_PBX"] = "pbx.test:5060"
        await self.ws.send_json(
            {
                "type": "StasisStart",
                "args": [line],
                "channel": {"id": channel, "caller": {"number": "1000"}},
            }
        )

    async def events(self, request: web.Request) -> web.StreamResponse:
        if request.headers.get("Authorization") != self.auth:
            return web.Response(status=401)
        assert request.query["app"] == "srw-voice"
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.ws = ws
        self.connects += 1
        async for _ in ws:
            pass
        return ws

    async def rest(self, request: web.Request) -> web.Response:
        if request.headers.get("Authorization") != self.auth:
            return web.Response(status=401)
        path = "/" + request.match_info["tail"]
        params = dict(request.query)
        self.calls.append((request.method, path, params))
        parts = path.strip("/").split("/")
        if request.method == "GET" and path == "/channels":
            return web.json_response(self.channels)
        if request.method == "GET" and parts[-1] == "variable":
            name = params["variable"]
            if name.startswith("PJSIP_HEADER(read,"):
                header, index = name[len("PJSIP_HEADER(read,") : -1].split(",")
                values = self.headers.get(parts[1], {}).get(header, [])
                if int(index) > len(values):
                    return web.json_response(
                        {"message": "Error With Function"}, status=500
                    )
                return web.json_response({"value": values[int(index) - 1]})
            value = self.variables.get(parts[1], {}).get(name)
            if value is None:
                return web.json_response({"message": "not found"}, status=404)
            return web.json_response({"value": value})
        if request.method == "POST" and path == "/channels/externalMedia":
            if self.media_mode == "connect":
                self.spawn(
                    self.connect_media(params["channelId"], params["external_host"])
                )
            elif self.media_mode == "fail":
                # chan_websocket could not reach the gateway: ARI said 200,
                # then the channel ends.
                self.spawn(self.emit("ChannelDestroyed", params["channelId"], cause=3))
            return web.json_response({"id": params["channelId"]})
        if request.method == "POST" and parts[-1] == "continue":
            self.spawn(self.run_transfer(parts[1]))
        elif request.method == "POST" and len(parts) == 2 and "endpoint" in params:
            self.spawn(self.originate(parts[1], params["appArgs"]))
        elif request.method == "DELETE" and parts[0] == "channels":
            self.spawn(self.destroyed(parts[1]))
        return web.Response(status=204)

    async def connect_media(self, channel: str, connection: str) -> None:
        assert connection == "srw-gateway"
        ws = await self.http.ws_connect(
            self.media_url,
            headers={"Authorization": self.auth},
            protocols=("media",),
        )
        self.media[channel] = ws
        await ws.send_str(
            json.dumps(
                {
                    "event": "MEDIA_START",
                    "connection_id": connection,
                    "channel": f"WebSocket/{connection}/0x1",
                    "channel_id": channel,
                    "format": "slin16",
                    "optimal_frame_size": FRAME_BYTES,
                    "ptime": 20,
                }
            )
        )
        # Answered after MEDIA_START, then it enters the app.
        await self.emit("StasisStart", channel, args=[])

    async def run_transfer(self, channel: str) -> None:
        await self.emit("StasisEnd", channel)
        await self.emit(
            "StasisStart",
            channel,
            args=[self.lines[channel], "transfer", *self.transfer],
        )

    async def originate(self, channel: str, args: str) -> None:
        if self.dial == "answer":
            await self.emit("StasisStart", channel, args=[args])
        else:
            await self.emit("ChannelDestroyed", channel, cause=19)

    async def destroyed(self, channel: str) -> None:
        if channel in self.media:
            await self.media.pop(channel).close()
        await self.emit("ChannelDestroyed", channel, cause=16)


@pytest_asyncio.fixture
async def stack():
    async with AsyncExitStack() as stack:

        async def start(**settings):
            fake = FakeAsterisk()
            fake.http = await stack.enter_async_context(aiohttp.ClientSession())
            ari = await stack.enter_async_context(TestServer(fake.app))
            deliveries = []

            async def receive(request):
                body = await request.read()
                payload = verify_webhook(SECRET, request.headers, body)
                deliveries.append(
                    parse_incoming_call(payload, request.headers["webhook-id"])
                )
                return web.json_response({"ok": True})

            receiver = web.Application()
            receiver.router.add_post("/api/live/webhooks/connector", receive)
            sink = await stack.enter_async_context(TestServer(receiver))
            config = Config(
                "127.0.0.1",
                18440,
                (KEY.encode(),),
                str(sink.make_url("")).rstrip("/"),
                {"bench": Line("bench", "connector", b"gateway-test-signing-key")},
                {"echo": {"engine": "echo"}},
            )
            source = AsteriskCallSource(
                AsteriskSettings(
                    ari_url=str(ari.make_url("")).rstrip("/"),
                    ari_user="srw",
                    password=PASSWORD,
                    media_listen="127.0.0.1:0",
                    **settings,
                )
            )
            gateway = Gateway(config, source=source)
            client = await stack.enter_async_context(
                TestClient(
                    TestServer(gateway.app), headers={"Authorization": f"Bearer {KEY}"}
                )
            )
            port = source.media.runner.addresses[0][1]
            fake.media_url = f"ws://127.0.0.1:{port}/media"
            await until(lambda: fake.ws is not None)
            return gateway, client, fake, deliveries

        yield start


async def ring(fake, deliveries, gateway, channel="caller-1"):
    await fake.incoming(channel)
    await until(lambda: deliveries)
    return gateway.sessions[deliveries[-1].session_id]


async def answered(client, fake, deliveries, gateway):
    session = await ring(fake, deliveries, gateway)
    response = await client.post(f"/v1/live/sessions/{session.id}/accept", json=ACCEPT)
    assert response.status == 200, await response.text()
    return session


@pytest.mark.asyncio
async def test_incoming_call_rings_and_announces_headers(stack):
    gateway, client, fake, deliveries = await stack()
    session = await ring(fake, deliveries, gateway)
    assert fake.made(
        "POST", "/applications/srw-voice/subscription", eventSource="channel:caller-1"
    )
    assert fake.made("POST", "/channels/caller-1/ring")
    names = [name for name, _ in deliveries[0].sip_headers]
    assert names == ["From", "To", "Diversion"]
    assert deliveries[0].sip_header("From") == HEADERS["From"][0]
    assert session.state == "ringing"


@pytest.mark.asyncio
async def test_accept_bridges_the_caller_with_its_audio_and_echoes(stack):
    gateway, client, fake, deliveries = await stack()
    session = await answered(client, fake, deliveries, gateway)
    media, bridge = f"{session.id}-media", f"{session.id}-bridge"
    assert fake.made("POST", "/channels/caller-1/answer")
    assert fake.made("POST", f"/bridges/{bridge}", type="mixing")
    assert fake.made(
        "POST",
        "/channels/externalMedia",
        app="srw-voice",
        channelId=media,
        external_host="srw-gateway",
        transport="websocket",
        encapsulation="none",
        format="slin16",
    )
    assert fake.made("POST", f"/bridges/{bridge}/addChannel", channel=media)
    assert session.audio is not None and session.state == "active"
    # The echo engine plays the caller's frames straight back.
    ws = fake.media[media]
    await ws.send_bytes(b"\1" * FRAME_BYTES * 3)
    back = [await ws.receive(timeout=2) for _ in range(3)]
    assert [m.data for m in back] == [b"\1" * FRAME_BYTES] * 3


@pytest.mark.asyncio
async def test_dtmf_and_caller_hangup_reach_the_driver(stack):
    gateway, client, fake, deliveries = await stack()
    session = await answered(client, fake, deliveries, gateway)
    async with client.ws_connect(f"/v1/live/sessions/{session.id}/attach") as ws:
        assert (await ws.receive_json(timeout=2))["type"] == "session.started"
        await fake.emit("ChannelDtmfReceived", "caller-1", digit="5", duration_ms=100)
        event = await ws.receive_json(timeout=2)
        assert event["type"] == "transport.dtmf.received" and event["event"] == "5"
        await fake.emit("ChannelDestroyed", "caller-1", cause=16)
        events = [await ws.receive_json(timeout=2) for _ in range(2)]
    assert events[-1]["type"] == "session.closed"
    assert events[-1]["reason"] == "remote_hangup"
    assert fake.made("DELETE", f"/channels/{session.id}-media")
    assert fake.made("DELETE", f"/bridges/{session.id}-bridge")
    await until(lambda: not gateway.source.calls)


@pytest.mark.asyncio
async def test_caller_hangup_while_ringing_closes_the_session(stack):
    gateway, client, fake, deliveries = await stack()
    session = await ring(fake, deliveries, gateway)
    await fake.emit("ChannelDestroyed", "caller-1", cause=16)
    await until(lambda: session.state == "closed")
    assert session.close_reason == "remote_hangup"


@pytest.mark.asyncio
async def test_driver_hangup_hangs_up_every_channel(stack):
    gateway, client, fake, deliveries = await stack()
    session = await answered(client, fake, deliveries, gateway)
    response = await client.post(f"/v1/live/sessions/{session.id}/hangup")
    assert response.status == 200
    assert session.close_reason == "close_requested"
    for path in ("/channels/caller-1", f"/channels/{session.id}-media"):
        assert fake.made("DELETE", path)
    assert fake.made("DELETE", f"/bridges/{session.id}-bridge")


@pytest.mark.parametrize(
    "status, cause",
    [(480, 19), (486, 17), (603, 16), (404, 1), (403, 21), (503, 34), (500, 38)],
)
@pytest.mark.asyncio
async def test_reject_hangs_up_with_the_cause_for_that_status(stack, status, cause):
    gateway, client, fake, deliveries = await stack()
    session = await ring(fake, deliveries, gateway)
    response = await client.post(
        f"/v1/live/sessions/{session.id}/reject", json={"status_code": status}
    )
    assert response.status == 200
    assert fake.made("DELETE", "/channels/caller-1", reason_code=cause)
    assert not fake.made("POST", "/channels/caller-1/answer")


@pytest.mark.parametrize(
    "status, cause",
    [(302, 19), (420, 19), (499, 19), (505, 38), (600, 16), (606, 16)],
)
def test_reject_cause_falls_back_to_the_status_class(status, cause):
    assert reject_cause(status) == cause


@pytest.mark.parametrize("mode", ["never", "fail"])
@pytest.mark.asyncio
async def test_audio_that_never_starts_fails_the_accept_cleanly(stack, mode):
    gateway, client, fake, deliveries = await stack(media_timeout_s=0.5)
    fake.media_mode = mode
    session = await ring(fake, deliveries, gateway)
    response = await client.post(f"/v1/live/sessions/{session.id}/accept", json=ACCEPT)
    assert response.status == 502
    assert (await response.json())["error"]["code"] == "media_unavailable"
    assert session.state == "closed" and session.close_reason == "connection_lost"
    assert fake.made("DELETE", "/channels/caller-1")
    assert fake.made("DELETE", f"/bridges/{session.id}-bridge")


@pytest.mark.asyncio
async def test_refer_sends_the_blind_transfer_from_the_dialplan(stack):
    gateway, client, fake, deliveries = await stack()
    session = await answered(client, fake, deliveries, gateway)
    response = await client.post(
        f"/v1/live/sessions/{session.id}/refer",
        json={"target_uri": "sip:3000@pbx.test"},
    )
    assert response.status == 200
    assert fake.made(
        "POST",
        "/channels/caller-1/variable",
        variable="SRW_REFER_TO",
        value="sip:3000@pbx.test",
    )
    assert fake.made("POST", "/channels/caller-1/continue", context="srw-transfer")
    # TRANSFERSTATUS=SUCCESS: the PBX has the caller; the source lets go.
    await until(lambda: session.state == "closed")
    assert session.close_reason == "remote_hangup"
    assert fake.made("DELETE", "/channels/caller-1")
    assert not any(p.endswith("-dial") for _, p, _ in fake.calls)


@pytest.mark.asyncio
async def test_refused_refer_dials_the_target_and_bridges_the_caller(stack):
    gateway, client, fake, deliveries = await stack()
    fake.transfer = ("FAILURE", "603")
    session = await answered(client, fake, deliveries, gateway)
    bridge, media, dial = (
        f"{session.id}-{part}" for part in ("bridge", "media", "dial")
    )
    response = await client.post(
        f"/v1/live/sessions/{session.id}/refer",
        json={"target_uri": "sip:3000@pbx.test"},
    )
    assert response.status == 200
    await until(lambda: session.state == "closed")
    assert session.close_reason == "remote_hangup"
    # The caller came back to the voice while the target rang ...
    adds = [
        q["channel"] for m, p, q in fake.calls if p == f"/bridges/{bridge}/addChannel"
    ]
    assert adds == ["caller-1", media, "caller-1", dial]
    assert fake.made(
        "POST",
        f"/channels/{dial}",
        endpoint="PJSIP/srw-bench/sip:3000@pbx.test",
        app="srw-voice",
        appArgs="SRW_DIAL",
        callerId="1000",
    )
    # ... and once it answered, caller and target share the bridge.
    assert fake.made("POST", f"/bridges/{bridge}/removeChannel", channel=media)
    assert fake.made("DELETE", f"/channels/{media}")
    assert not fake.made("DELETE", "/channels/caller-1")
    # The target hanging up ends the caller's call too.
    await fake.emit("ChannelDestroyed", dial, cause=16)
    await until(lambda: fake.made("DELETE", "/channels/caller-1"))
    await until(lambda: not gateway.source.calls)


@pytest.mark.asyncio
async def test_unreached_target_leaves_the_caller_with_the_voice(stack):
    gateway, client, fake, deliveries = await stack()
    fake.transfer, fake.dial = ("FAILURE", "403"), "fail"
    session = await answered(client, fake, deliveries, gateway)
    async with client.ws_connect(f"/v1/live/sessions/{session.id}/attach") as ws:
        assert (await ws.receive_json(timeout=2))["type"] == "session.started"
        response = await client.post(
            f"/v1/live/sessions/{session.id}/refer",
            json={"target_uri": "sip:3000@pbx.test"},
        )
        assert response.status == 200
        await until(lambda: fake.made("POST", f"/channels/{session.id}-dial"))
        await asyncio.sleep(0.1)
        assert session.state == "active"
        # The call goes on: DTMF still arrives, and a new transfer may start.
        await fake.emit("ChannelDtmfReceived", "caller-1", digit="9")
        assert (await ws.receive_json(timeout=2))["event"] == "9"
    response = await client.post(
        f"/v1/live/sessions/{session.id}/refer", json={"target_uri": "tel:+49 30 1234"}
    )
    assert response.status == 200
    assert fake.made(
        "POST", "/channels/caller-1/variable", value="sip:+49301234@pbx.test:5060"
    )


@pytest.mark.asyncio
async def test_refer_refuses_targets_that_could_escape_the_dialplan(stack):
    gateway, client, fake, deliveries = await stack()
    session = await answered(client, fake, deliveries, gateway)
    for target in (
        "sip:3000@pbx,1",
        "sip:${EXTEN}@pbx",
        "sip:3000@pbx test",
        "http://x",
    ):
        response = await client.post(
            f"/v1/live/sessions/{session.id}/refer", json={"target_uri": target}
        )
        assert response.status == 400
        assert (await response.json())["error"]["code"] == "invalid_target_uri"
    assert not fake.made("POST", "/channels/caller-1/continue")


def test_refer_uri():
    assert refer_uri(" sip:3000@pbx;user=phone ", None) == "sip:3000@pbx;user=phone"
    assert (
        refer_uri("sips:*97@[2001:db8::1]:5061", None) == "sips:*97@[2001:db8::1]:5061"
    )
    assert refer_uri("tel:+49-30-1234", "pbx:5060") == "sip:+49301234@pbx:5060"
    for bad in ("tel:+4930", "sip:a@b'c", "sip:a@b\nc", "sip:" + "a" * 300):
        with pytest.raises(ProtocolError):
            refer_uri(bad, None if bad.startswith("tel") else "pbx")


@pytest.mark.asyncio
async def test_strays_in_the_app_are_hung_up(stack):
    gateway, client, fake, deliveries = await stack()
    await fake.emit("StasisStart", "old-media", args=[])
    await fake.emit("StasisStart", "old-dial", args=["SRW_DIAL"])
    await fake.emit(
        "StasisStart", "old-caller", args=["bench", "transfer", "FAILURE", "603"]
    )
    for channel in ("old-media", "old-dial", "old-caller"):
        await until(lambda c=channel: fake.made("DELETE", f"/channels/{c}"))
    assert not deliveries


@pytest.mark.asyncio
async def test_unknown_line_is_turned_away(stack):
    gateway, client, fake, deliveries = await stack()
    await fake.incoming("caller-9", line="nobody")
    await until(lambda: fake.made("DELETE", "/channels/caller-9", reason_code=19))
    assert not deliveries and not gateway.sessions


@pytest.mark.asyncio
async def test_reconnect_reconciles_missed_hangups_and_old_calls(stack):
    gateway, client, fake, deliveries = await stack()
    session = await answered(client, fake, deliveries, gateway)
    old = (datetime.now(timezone.utc) - timedelta(minutes=5)).strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    )[:-3] + "+0000"
    fake.channels = [
        {
            "id": "left-behind",
            "creationtime": old,
            "dialplan": {"app_name": "Stasis", "app_data": "srw-voice,bench"},
        },
        {
            "id": "someone-elses",
            "creationtime": old,
            "dialplan": {"app_name": "Dial", "app_data": "PJSIP/3000"},
        },
    ]
    # The events socket drops; meanwhile the caller hung up.
    await fake.ws.close()
    await until(lambda: fake.connects == 2, timeout=5)
    await until(lambda: session.state == "closed")
    assert session.close_reason == "remote_hangup"
    await until(lambda: fake.made("DELETE", "/channels/left-behind"))
    assert not fake.made("DELETE", "/channels/someone-elses")


@pytest.mark.asyncio
async def test_echo_engine_without_audio_does_nothing():
    class Bare:
        audio = None

    engine = EchoEngine()
    await engine.start(Bare(), {})
    assert engine.task is None
    await engine.append(Bare(), "instructions", "Hallo", None)
    await engine.stop(Bare())


# -- settings ---------------------------------------------------------------------------------


def gateway_config(tmp_path, source) -> dict:
    (tmp_path / "key").write_text("test-api-key\n")
    (tmp_path / "webhook").write_text(SECRET)
    (tmp_path / "asterisk-password").write_text(PASSWORD + "\n")
    raw = {
        "api_key_files": ["key"],
        "orchestrator_url": "http://127.0.0.1:8085",
        "lines": [
            {"name": "bench", "connector_id": "c", "webhook_secret_file": "webhook"}
        ],
        "profiles": {"echo": {"engine": "echo"}},
        "source": source,
    }
    path = tmp_path / "gateway.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


ASTERISK = {
    "kind": "asterisk",
    "ari_url": "http://127.0.0.1:8088",
    "ari_user": "srw",
    "password_file": "asterisk-password",
}


def test_asterisk_source_from_config(tmp_path):
    config = load_config(gateway_config(tmp_path, ASTERISK))
    gateway = Gateway(config)
    assert isinstance(gateway.source, AsteriskCallSource)
    settings = gateway.source.settings
    assert settings.password == PASSWORD and PASSWORD not in repr(settings)
    assert (settings.app, settings.media_listen, settings.media_connection) == (
        "srw-voice",
        "127.0.0.1:18441",
        "srw-gateway",
    )


@pytest.mark.parametrize(
    "change, message",
    [
        ({"kind": "freeswitch"}, "source.kind"),
        ({"extra": 1}, "unknown asterisk settings"),
        ({"ari_url": "http://srw:pw@127.0.0.1:8088"}, "ari_url"),
        ({"ari_url": "http://127.0.0.1:8088/ari"}, "ari_url"),
        ({"password_file": "missing"}, "password_file"),
        ({"media_listen": "gateway:18441"}, "media_listen"),
        ({"media_timeout_s": 0}, "media_timeout_s"),
    ],
)
def test_invalid_asterisk_settings(tmp_path, change, message):
    with pytest.raises(ConfigError, match=message):
        load_config(gateway_config(tmp_path, {**ASTERISK, **change}))
