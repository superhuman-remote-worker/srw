"""GPT-Live gateway protocol gates; local_voice_gateway.md §4.1.

The Go helper uses an unchanged copy of the driver's contract.go to normalize
raw engine events, then validates the shared SSE shapes and journal rows.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import shutil
import subprocess
from contextlib import AsyncExitStack
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from shared.runtime.live.contract import event_from_wire
from shared.runtime.live.webhook import parse_incoming_call, verify_webhook
from voice_gateway.config import Config, Line
from voice_gateway.scripted import ScriptedCallSource, ScriptedEngine
from voice_gateway.server import Gateway
from voice_gateway.test_client import GatewayClient

KEY = "gateway-test-key"
SECRET = "whsec_" + base64.b64encode(b"gateway-test-signing-key").decode()
HEADERS = {"Authorization": f"Bearer {KEY}"}
ACCEPT = {
    "session": {
        "type": "live",
        "model": "scripted",
        "instructions": "Generic facts and live rules.",
        "audio": {"output": {"voice": "marin"}},
        "delegation": {"type": "client"},
    }
}


async def until(predicate, timeout=3):
    async with asyncio.timeout(timeout):
        while not (result := predicate()):
            await asyncio.sleep(0.01)
        return result


@pytest_asyncio.fixture
async def running():
    async with AsyncExitStack() as stack:

        async def start(*, webhook=None, enabled=True, **kwargs):
            deliveries = []

            async def receive(request):
                body = await request.read()
                payload = verify_webhook(SECRET, request.headers, body)
                call = parse_incoming_call(payload, request.headers["webhook-id"])
                deliveries.append((dict(request.headers), body, call))
                if webhook:
                    return await webhook(request, payload)
                return web.json_response({"ok": True})

            receiver = web.Application()
            receiver.router.add_post("/api/live/webhooks/connector", receive)
            sink = await stack.enter_async_context(TestServer(receiver))
            config = Config(
                "127.0.0.1",
                18440,
                (KEY.encode(),),
                str(sink.make_url("")),
                {"demo": Line("demo", "connector", b"gateway-test-signing-key")},
                {"scripted": {"engine": "scripted"}},
                enabled,
            )
            gateway = Gateway(config, **kwargs)
            server = TestServer(gateway.app)
            client = await stack.enter_async_context(
                TestClient(server, headers=HEADERS)
            )
            gateway.http = await stack.enter_async_context(
                aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=5),
                )
            )
            return gateway, client, deliveries

        yield start


async def incoming(client):
    response = await client.post(
        "/test/calls",
        json={
            "line": "demo",
            "from_number": "+491700000000",
            "to_number": "+496000000000",
        },
    )
    assert response.status == 201, await response.text()
    return (await response.json())["session_id"]


async def accept(client, sid):
    response = await client.post(f"/v1/live/sessions/{sid}/accept", json=ACCEPT)
    assert response.status == 200, await response.text()


async def ws_event(ws, expected):
    event = await ws.receive_json(timeout=3)
    assert event["type"] == expected, event
    assert event["event_id"]
    return event


@pytest.mark.asyncio
async def test_signed_webhook_and_accept_body_reach_engine(running):
    engine = ScriptedEngine()
    engine.start = AsyncMock()
    source = ScriptedCallSource()
    source.answer = AsyncMock()
    gateway, client, deliveries = await running(
        source=source, engines={"scripted": lambda: engine}
    )
    sid = await incoming(client)
    await until(lambda: deliveries)
    call = deliveries[0][2]
    assert call.session_id == sid
    assert call.sip_header("From") == "sip:+491700000000@gateway.example"
    assert call.sip_header("To") == "sip:+496000000000@gateway.example"
    await accept(client, sid)
    engine.start.assert_awaited_once_with(gateway.sessions[sid], ACCEPT)
    source.answer.assert_awaited_once_with(gateway.sessions[sid])
    ws = await client.ws_connect(f"/v1/live/sessions/{sid}/attach")
    await ws_event(ws, "session.started")
    # The engine never speaks until the driver appends its greeting.
    await ws.send_json(
        {
            "type": "session.instructions.append",
            "event_id": "greet",
            "delegation_id": None,
            "content": "Hello from an AI assistant.",
        }
    )
    ack = await ws_event(ws, "session.instructions.appended")
    assert ack["client_event_id"] == "greet"
    speech = await ws_event(ws, "session.output_transcript.delta")
    assert speech["delta"] == "Hello from an AI assistant."
    await ws.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["accept", "reject"])
async def test_decided_once_and_terminal_controls(running, action):
    source = ScriptedCallSource()
    source.reject, source.refer, source.hangup = AsyncMock(), AsyncMock(), AsyncMock()
    gateway, client, _ = await running(source=source)
    sid = await incoming(client)
    path = f"/v1/live/sessions/{sid}"
    body = ACCEPT if action == "accept" else {"status_code": 480}
    assert (await client.post(f"{path}/{action}", json=body)).status == 200
    for next_action, data in (("accept", ACCEPT), ("reject", {"status_code": 480})):
        response = await client.post(f"{path}/{next_action}", json=data)
        assert response.status == 409
        assert (await response.json())["error"]["code"] == "decision_already_made"
    session = gateway.sessions[sid]
    if action == "accept":
        assert (
            await client.post(
                f"{path}/refer", json={"target_uri": "sip:operator@example.com"}
            )
        ).status == 200
        source.refer.assert_awaited_once_with(session, "sip:operator@example.com")
        assert session.state == "active"
        assert (await client.post(f"{path}/hangup")).status == 200
        source.hangup.assert_awaited_once_with(session)
        assert session.close_reason == "close_requested"
    else:
        source.reject.assert_awaited_once_with(session, 480)
        assert session.reject_status == 480
    assert session.state == "closed"
    assert (await client.post(f"{path}/hangup")).status == 409
    assert (
        await client.post(
            f"{path}/refer", json={"target_uri": "sip:operator@example.com"}
        )
    ).status == 409
    assert (await client.get(f"{path}/attach")).status == 409


@pytest.mark.asyncio
async def test_validation_leaves_session_ringing(running):
    gateway, client, _ = await running()
    sid = await incoming(client)
    path = f"/v1/live/sessions/{sid}"
    for body in (
        {},
        {"session": []},
        {"session": {"type": "live", "model": "unknown"}},
        {"session": {**ACCEPT["session"], "instructions": "x" * 48001}},
        {"session": {**ACCEPT["session"], "audio": []}},
        {"session": {**ACCEPT["session"], "delegation": {"type": "server"}}},
    ):
        response = await client.post(f"{path}/accept", json=body)
        assert response.status == 400
        assert (await response.json())["error"]["code"] in {
            "invalid_session",
            "model_not_found",
        }
    for status in (0, 700, True, "480"):
        assert (
            await client.post(f"{path}/reject", json={"status_code": status})
        ).status == 400
    assert gateway.sessions[sid].state == "ringing"
    await accept(client, sid)
    assert (await client.post(f"{path}/refer", json={})).status == 400


@pytest.mark.asyncio
async def test_auth_unknown_inactive_and_malformed_bodies(running):
    _, client, _ = await running()
    sid = await incoming(client)
    for path in (f"/v1/live/sessions/{sid}/attach", "/test/calls/unknown"):
        for value in ("", "Bearer wrong", KEY):
            response = await client.get(path, headers={"Authorization": value})
            assert response.status == 401
            assert (await response.json())["error"]["code"] == "invalid_api_key"
    assert (
        await client.post(
            f"/v1/live/sessions/{sid}/accept",
            json=ACCEPT,
            headers={"Authorization": "Bearer wrong"},
        )
    ).status == 401
    assert (await client.get("/v1/live/sessions/unknown/attach")).status == 404
    assert (
        await client.post("/v1/live/sessions/unknown/accept", json=ACCEPT)
    ).status == 404
    assert (await client.get(f"/v1/live/sessions/{sid}/attach")).status == 409
    for data in ("[]", "null", "not-json"):
        response = await client.post(f"/v1/live/sessions/{sid}/accept", data=data)
        assert response.status == 400
        assert (await response.json())["error"]["code"] == "invalid_body"
    assert (
        await client.post(f"/v1/live/sessions/{sid}/unknown", json={})
    ).status == 404


@pytest.mark.asyncio
async def test_appends_delegation_silence_errors_and_text_frames(running):
    _, client, _ = await running()
    sid = await incoming(client)
    await accept(client, sid)
    ws = await client.ws_connect(f"/v1/live/sessions/{sid}/attach")
    await ws_event(ws, "session.started")
    assert (await client.post(f"/test/calls/{sid}/delegate", json={})).status == 409
    await client.post(
        f"/test/calls/{sid}/say",
        json={
            "speaker": "caller",
            "text": "Please check the schedule.",
            "start_ms": 10,
            "end_ms": 30,
        },
    )
    spoken = await ws_event(ws, "session.input_transcript.delta")
    assert (spoken["start_ms"], spoken["end_ms"]) == (10, 30)
    delegation = (
        await (await client.post(f"/test/calls/{sid}/delegate", json={})).json()
    )["delegation_id"]
    assert (await ws_event(ws, "session.delegation.created"))["delegation"][
        "id"
    ] == delegation
    for kind in ("instructions", "commentary", "thinking"):
        await ws.send_json(
            {
                "type": f"session.{kind}.append",
                "event_id": kind,
                "content": "ä" * 1500,
                "delegation_id": delegation,
            }
        )
        assert (await ws_event(ws, f"session.{kind}.appended"))[
            "client_event_id"
        ] == kind
        if kind != "thinking":
            assert (await ws_event(ws, "session.output_transcript.delta"))[
                "delta"
            ] == "ä" * 1500
    # A thinking append produces no output: the next frame is this error.
    for content, did, code in (
        ("answer", "missing", "unknown_delegation"),
        ("", delegation, "empty_content"),
        ("  ", delegation, "empty_content"),
        ("x" * 2001, delegation, "content_too_long"),
    ):
        await ws.send_json(
            {
                "type": "session.commentary.append",
                "event_id": "bad",
                "content": content,
                "delegation_id": did,
            }
        )
        error = (await ws_event(ws, "error"))["error"]
        assert error["code"] == code
        assert error["client_event_id"] == "bad"
    await ws.send_str("{bad")
    assert (await ws_event(ws, "error"))["error"]["code"] == "invalid_command"
    await ws.send_json({"type": "unknown"})
    assert (await ws_event(ws, "error"))["error"]["code"] == "unknown_command"
    await ws.send_bytes(b"binary is not supported")
    assert (await ws.receive(timeout=3)).type == aiohttp.WSMsgType.CLOSE


@pytest.mark.asyncio
async def test_replay_original_ids_multiple_attach_and_window(running):
    gateway, client, _ = await running(replay_window_s=0.15)
    sid = await incoming(client)
    await accept(client, sid)
    first = await client.ws_connect(f"/v1/live/sessions/{sid}/attach")
    started = await ws_event(first, "session.started")
    second = await client.ws_connect(f"/v1/live/sessions/{sid}/attach")
    assert await ws_event(second, "session.started") == started
    await client.post(f"/test/calls/{sid}/say", json={"text": "A caller turn"})
    event = await ws_event(first, "session.input_transcript.delta")
    assert await ws_event(second, "session.input_transcript.delta") == event
    await first.close()
    third = await client.ws_connect(f"/v1/live/sessions/{sid}/attach")
    assert await ws_event(third, "session.started") == started
    assert await ws_event(third, "session.input_transcript.delta") == event
    await asyncio.sleep(0.17)
    fourth = await client.ws_connect(f"/v1/live/sessions/{sid}/attach")
    await client.post(f"/test/calls/{sid}/dtmf", json={"digit": "#"})
    fresh = await ws_event(fourth, "transport.dtmf.received")
    assert fresh["event"] == "#"
    assert gateway.sessions[sid].attach_count == 4
    assert len(gateway.sessions[sid].history) == 1
    await second.close()
    await third.close()
    await fourth.close()


@pytest.mark.asyncio
async def test_usage_close_and_remote_hangup_while_ringing(running):
    gateway, client, _ = await running(usage_interval_s=0.02)
    sid = await incoming(client)
    await accept(client, sid)
    ws = await client.ws_connect(f"/v1/live/sessions/{sid}/attach")
    await ws_event(ws, "session.started")
    usage = await ws_event(ws, "session.usage.updated")
    assert usage["usage"]["seconds"] > 0
    await client.post(f"/test/calls/{sid}/remote_hangup", json={})
    events = []
    while True:
        event = await ws.receive_json(timeout=3)
        events.append(event)
        if event["type"] == "session.closed":
            break
    assert events[-1]["reason"] == "remote_hangup"
    assert events[-1]["usage"]["seconds"] >= usage["usage"]["seconds"]
    assert (await ws.receive(timeout=3)).type == aiohttp.WSMsgType.CLOSE
    other = await incoming(client)
    await client.post(f"/test/calls/{other}/remote_hangup", json={})
    assert gateway.sessions[other].state == "closed"
    assert gateway.sessions[other].seconds == 0
    assert (
        await client.post(f"/v1/live/sessions/{other}/accept", json=ACCEPT)
    ).status == 409


@pytest.mark.asyncio
async def test_ringing_deadline_and_webhook_retries(running):
    async def unavailable(request, payload):
        return web.Response(status=503)

    gateway, client, deliveries = await running(
        webhook=unavailable, ringing_timeout_s=0.6
    )
    sid = await incoming(client)
    session = gateway.sessions[sid]
    await until(lambda: session.state == "closed")
    assert session.reject_status == 480
    assert session.close_reason == "expired"
    assert len(deliveries) >= 2
    assert len({headers["webhook-id"] for headers, _, _ in deliveries}) == 1
    assert len({body for _, body, _ in deliveries}) == 1
    count = len(deliveries)
    await asyncio.sleep(0.05)
    assert len(deliveries) == count
    assert (
        await client.post(f"/v1/live/sessions/{sid}/accept", json=ACCEPT)
    ).status == 409


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 401, 302])
async def test_permanent_webhook_failure_is_not_retried(running, status):
    async def refuse(request, payload):
        return web.Response(status=status)

    _, client, deliveries = await running(webhook=refuse, ringing_timeout_s=0.3)
    await incoming(client)
    await asyncio.sleep(0.35)
    assert len(deliveries) == 1


@pytest.mark.asyncio
async def test_test_control_off_and_key_rotation(running):
    gateway, client, _ = await running(enabled=False)
    assert (await client.post("/test/calls", json={})).status == 404
    assert (await client.get("/test/calls/unknown")).status == 404
    gateway.config = replace(gateway.config, api_keys=(b"old-key", KEY.encode()))
    assert (
        await client.get(
            "/v1/live/sessions/unknown/attach",
            headers={"Authorization": "Bearer old-key"},
        )
    ).status == 404


@pytest.mark.asyncio
async def test_test_client_long_poll_commands_and_session_close(running):
    gateway, client, _ = await running()
    remote = GatewayClient(str(client.make_url("")), "demo", KEY)
    await remote.start()
    await remote.http.close()
    remote.http = aiohttp.ClientSession(headers=HEADERS)
    try:
        sid = await remote.incoming_call(
            from_number="+491700000000", to_number="+496000000000"
        )
        await accept(client, sid)
        ws = await client.ws_connect(f"/v1/live/sessions/{sid}/attach")
        await ws_event(ws, "session.started")
        waiter = asyncio.create_task(
            remote.wait_for_command(sid, "session.thinking.append", timeout=2)
        )
        await ws.send_json(
            {
                "type": "session.thinking.append",
                "event_id": "context",
                "content": "quiet context",
            }
        )
        assert (await waiter)["content"] == "quiet context"
        await ws_event(ws, "session.thinking.appended")
        await remote.say(sid, "caller", "A request")
        await ws_event(ws, "session.input_transcript.delta")
        did = await remote.delegate(sid)
        assert (await ws_event(ws, "session.delegation.created"))["delegation"][
            "id"
        ] == did
        await ws.send_json({"type": "session.close"})
        await ws_event(ws, "session.usage.updated")
        await ws_event(ws, "session.closed")
        assert gateway.sessions[sid].close_reason == "close_requested"
        await remote.wait_until(sid, lambda s: s.status == "closed")
        response = await client.get(f"/test/calls/{sid}/commands?after=99&timeout=0")
        assert response.status == 204
    finally:
        await remote.stop()


@pytest.fixture(scope="module")
def go_contract(tmp_path_factory):
    if shutil.which("go") is None:
        pytest.skip("Go toolchain is not installed")
    root = Path(__file__).resolve().parents[1]
    build = tmp_path_factory.mktemp("gateway-go-contract")
    for src in (
        root / "drivers/gpt-live-sip/contract.go",
        root / "tests/fixtures/voice_gateway_contract/main.go",
    ):
        shutil.copyfile(src, build / src.name)
    binary = build / "contract"
    result = subprocess.run(
        ["go", "build", "-o", str(binary), "main.go", "contract.go"],
        cwd=build,
        env={**os.environ, "GOCACHE": str(build / "cache"), "GOTOOLCHAIN": "local"},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr

    def parse(events, mode=None):
        result = subprocess.run(
            [str(binary), *([mode] if mode else [])],
            input="".join(json.dumps(event) + "\n" for event in events),
            capture_output=True,
            text=True,
            check=True,
        )
        return [json.loads(line) for line in result.stdout.splitlines()]

    return parse


@pytest.mark.asyncio
async def test_every_emitted_event_through_real_go_parser_and_shared_contract(
    running, go_contract
):
    from shared.runtime.live.contract import JOURNAL_KINDS

    gateway, client, _ = await running()
    sid = await incoming(client)
    await accept(client, sid)
    ws = await client.ws_connect(f"/v1/live/sessions/{sid}/attach")
    raw = [await ws_event(ws, "session.started")]
    await client.post(f"/test/calls/{sid}/say", json={"text": "Please check this."})
    raw.append(await ws_event(ws, "session.input_transcript.delta"))
    response = await client.post(f"/test/calls/{sid}/delegate", json={})
    did = (await response.json())["delegation_id"]
    raw.append(await ws_event(ws, "session.delegation.created"))
    for kind in ("instructions", "thinking", "commentary"):
        await ws.send_json(
            {
                "type": f"session.{kind}.append",
                "event_id": kind,
                "delegation_id": did,
                "content": "A scripted answer.",
            }
        )
        raw.append(await ws_event(ws, f"session.{kind}.appended"))
        if kind != "thinking":
            raw.append(await ws_event(ws, "session.output_transcript.delta"))
    await ws.send_json(
        {
            "type": "session.commentary.append",
            "event_id": "bad",
            "delegation_id": "unknown",
            "content": "An answer",
        }
    )
    raw.append(await ws_event(ws, "error"))
    await client.post(f"/test/calls/{sid}/dtmf", json={"digit": "5"})
    raw.append(await ws_event(ws, "transport.dtmf.received"))
    await client.post(f"/test/calls/{sid}/remote_hangup", json={})
    raw.append(await ws_event(ws, "session.usage.updated"))
    raw.append(await ws_event(ws, "session.closed"))
    assert len({event["event_id"] for event in raw}) == len(raw)
    expected_types = [
        "OtherEvent",
        "TranscriptDelta",
        "DelegationCreated",
        "AppendAcknowledged",
        "TranscriptDelta",
        "AppendAcknowledged",
        "AppendAcknowledged",
        "TranscriptDelta",
        "EngineError",
        "DtmfReceived",
        "UsageUpdated",
        "SessionClosed",
    ]
    parsed = go_contract(raw)
    for wire, row, expected in zip(raw, parsed, expected_types, strict=True):
        event = event_from_wire(row["event"])
        assert type(event).__name__ == expected
        assert event.event_id == wire["event_id"]
        if row["journal"]:
            assert row["journal"]["kind"] in JOURNAL_KINDS
            assert row["journal"]["event_id"] == wire["event_id"]
    assert parsed[-1]["event"]["usage_seconds"] == gateway.sessions[sid].seconds
    assert parsed[8]["event"]["code"] == "unknown_delegation"
    assert parsed[8]["event"]["client_event_id"] == "bad"


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", ["cancel", "human", "no-automation", "missed"])
async def test_gateway_call_script_scenarios_with_scripted_pod(
    running, monkeypatch, tmp_path, scenario
):
    """Exercise the gate's gateway mode; database/job results are test doubles."""
    from types import SimpleNamespace

    from scripts import live_fake_call as script

    actors = []

    async def pod(sid):
        if scenario == "no-automation":
            await client.post(
                f"/v1/live/sessions/{sid}/reject", json={"status_code": 480}
            )
            return
        await accept(client, sid)
        ws = await client.ws_connect(f"/v1/live/sessions/{sid}/attach")
        await ws.send_json(
            {
                "type": "session.instructions.append",
                "event_id": "greeting",
                "content": "Hello, this is an AI assistant.",
            }
        )
        async for message in ws:
            if message.type != aiohttp.WSMsgType.TEXT:
                continue
            event = json.loads(message.data)
            if event["type"] == "session.delegation.created":
                await ws.send_json(
                    {
                        "type": "session.commentary.append",
                        "event_id": "answer",
                        "delegation_id": event["delegation"]["id"],
                        "content": "Your request is handled.",
                    }
                )
                if scenario == "human":
                    await client.post(
                        f"/v1/live/sessions/{sid}/refer",
                        json={"target_uri": "sip:operator@example.com"},
                    )
            if event["type"] == "session.closed":
                break
        await ws.close()

    async def webhook(request, payload):
        actors.append(asyncio.create_task(pod(payload["data"]["session_id"])))
        return web.json_response({"ok": True})

    gateway, client, _ = await running(webhook=webhook)

    class Client(GatewayClient):
        async def start(self):
            self.http = aiohttp.ClientSession(headers=HEADERS)
            return self

    def row(args, sid):
        session = gateway.sessions[sid]
        if session.decision == "reject":
            return ["local", "rejected", "", "", "no_automation", ""]
        return [
            "local",
            "ended" if session.state == "closed" else "active",
            "test-job",
            "" if scenario == "missed" else "attached",
            session.close_reason or "",
            "",
        ]

    def sql(args, query, *, audit=False):
        if "SELECT status FROM jobs" in query:
            return "pending_review"
        return "0.5 seconds" if audit else "0.5"

    async def poll(predicate, *, timeout, interval=1.0):
        async with asyncio.timeout(min(timeout, 3)):
            while not (value := predicate()):
                await asyncio.sleep(0.01)
            return value

    monkeypatch.setattr(script, "GatewayClient", Client)
    monkeypatch.setattr(script, "_session_row", row)
    monkeypatch.setattr(script, "_sql", sql)
    monkeypatch.setattr(script, "_poll", poll)
    keyfile = tmp_path / "key"
    keyfile.write_text(KEY)
    args = SimpleNamespace(
        gateway=str(client.make_url("")),
        test_control_url=None,
        line="demo",
        api_key=KEY,
        api_key_file=str(keyfile),
        webhook_secret="",
        webhook_secret_file="",
        orchestrator="http://orchestrator.test",
        connector_id="connector",
        scenario=scenario,
        from_number="+491700000000",
        to_number="+496000000000",
        insecure=False,
        attach_timeout=3,
        answer_timeout=3,
        finish_timeout=3,
    )
    try:
        assert await script.play(args) == 0
        await asyncio.gather(*actors)
        session = next(iter(gateway.sessions.values()))
        speech = [
            event["delta"]
            for _, event in session.history
            if event["type"] == "session.output_transcript.delta"
        ]
        if scenario != "no-automation":
            assert speech.count("Hello, this is an AI assistant.") == 1
            assert session.seconds > 0
        assert session.state == "closed"
    finally:
        for actor in actors:
            actor.cancel()
        await asyncio.gather(*actors, return_exceptions=True)


@pytest.mark.asyncio
async def test_concurrent_accept_and_reject_have_one_winner(running):
    source = ScriptedCallSource()
    source.answer, source.reject = AsyncMock(), AsyncMock()
    gateway, client, _ = await running(source=source)
    sid = await incoming(client)
    responses = await asyncio.gather(
        client.post(f"/v1/live/sessions/{sid}/accept", json=ACCEPT),
        client.post(f"/v1/live/sessions/{sid}/reject", json={"status_code": 480}),
    )
    assert sorted(response.status for response in responses) == [200, 409]
    assert source.answer.await_count + source.reject.await_count == 1
    assert gateway.sessions[sid].decision in {"accept", "reject"}


@pytest.mark.asyncio
async def test_test_control_invalid_inputs_and_long_poll_timeout(running):
    _, client, _ = await running()
    for body in (
        {},
        {"line": "demo", "sip_headers": []},
        {"line": "demo", "sip_headers": [None]},
    ):
        assert (await client.post("/test/calls", json=body)).status == 400
    assert (
        await client.post(
            "/test/calls",
            json={
                "line": "missing",
                "from_number": "+491700000000",
                "to_number": "+496000000000",
            },
        )
    ).status == 404
    sid = await incoming(client)
    await accept(client, sid)
    assert (
        await client.post(f"/test/calls/{sid}/say", json={"text": ""})
    ).status == 400
    assert (
        await client.post(
            f"/test/calls/{sid}/say", json={"text": "hello", "start_ms": -1}
        )
    ).status == 400
    assert (
        await client.post(f"/test/calls/{sid}/dtmf", json={"digit": "bad"})
    ).status == 400
    for query in ("after=-1", "after=invalid", "timeout=31", "timeout=nan"):
        assert (await client.get(f"/test/calls/{sid}/commands?{query}")).status == 400
    assert (await client.get(f"/test/calls/{sid}/commands?timeout=0.01")).status == 204


@pytest.mark.asyncio
async def test_real_driver_clipping_edge_is_accepted(running, go_contract):
    # The unchanged driver adds " …" after a 1500-code-point cut if there is
    # no sentence/word boundary. The caller must still hear that answer; only
    # content beyond anything the driver sends is refused.
    clipped = go_contract([{"text": "a" * 1600}], mode="clip")[0]["content"]
    assert len(clipped) == 1502
    _, client, _ = await running()
    sid = await incoming(client)
    await accept(client, sid)
    ws = await client.ws_connect(f"/v1/live/sessions/{sid}/attach")
    await ws_event(ws, "session.started")
    await ws.send_json(
        {
            "type": "session.commentary.append",
            "event_id": "long-answer",
            "content": clipped,
        }
    )
    acked = await ws_event(ws, "session.commentary.appended")
    assert acked["client_event_id"] == "long-answer"
    await ws.send_json(
        {
            "type": "session.commentary.append",
            "event_id": "too-long",
            "content": "a" * 2001,
        }
    )
    while (event := await ws.receive_json(timeout=3))["type"] != "error":
        pass
    assert event["error"]["code"] == "content_too_long"
    assert event["error"]["client_event_id"] == "too-long"
    await ws.close()


@pytest.mark.asyncio
async def test_closed_session_is_forgotten_after_retention(running):
    gateway, client, _ = await running(retention_s=0.05)
    sid = await incoming(client)
    await accept(client, sid)
    response = await client.post(f"/v1/live/sessions/{sid}/hangup")
    assert response.status == 200
    await until(lambda: sid not in gateway.sessions)
    response = await client.post(f"/v1/live/sessions/{sid}/hangup")
    assert response.status == 404


def test_continuing_transcript_pieces_carry_their_space():
    """Readers join a speaker's consecutive deltas as they are (the cockpit's
    timeline, the job's transcript); a cascade emits whole pieces, e.g. one
    caller turn that VAD split into two STT results (A1 found
    "letzte Wochenach einer schicken")."""
    from agent.subagents.live import Transcript
    from voice_gateway.session import Session

    session = Session(Line("fake", "connector", b"key"), [], None, {})
    session.state = "active"
    pieces = [
        ("assistant", "Guten Tag."),
        ("caller", "Können Sie mir die Rechnung von meinem Aufenthalt letzte Woche"),
        ("caller", "noch einmal schicken?"),
        ("assistant", "Der Check-out ist bis 11 Uhr möglich."),
        ("assistant", "Möchten Sie später auschecken?"),
        ("assistant", " Schon mit Leerzeichen."),
        ("caller", ""),
        ("caller", "Ja."),
        ("caller", "Bitte prüfen Sie das."),
        "delegate",
        ("assistant", "Einen Moment, ich schaue nach."),
        "usage",
        ("assistant", "Sie ist da."),
    ]
    for piece in pieces:
        if piece == "delegate":
            session.delegate()
        elif piece == "usage":
            session.usage()
        else:
            session.transcript(*piece)
    deltas = [
        (event["type"].split(".")[1], event["delta"])
        for _, event in session.history
        if event["type"].endswith("_transcript.delta")
    ]
    assert [text for _, text in deltas] == [
        "Guten Tag.",
        "Können Sie mir die Rechnung von meinem Aufenthalt letzte Woche",
        " noch einmal schicken?",
        "Der Check-out ist bis 11 Uhr möglich.",
        " Möchten Sie später auschecken?",
        " Schon mit Leerzeichen.",
        "",
        "Ja.",
        " Bitte prüfen Sie das.",
        # A delegation (a journal row, a new response) starts a new stretch;
        # a usage update does not.
        "Einen Moment, ich schaue nach.",
        " Sie ist da.",
    ]
    # The job's own transcript reads the turns back intact.
    transcript = Transcript()
    for kind, text in deltas:
        transcript.add("caller" if kind == "input_transcript" else "assistant", text)
    assert [t.text for t in transcript.turns][1:3] == [
        "Können Sie mir die Rechnung von meinem Aufenthalt letzte Woche "
        "noch einmal schicken?",
        "Der Check-out ist bis 11 Uhr möglich. Möchten Sie später auschecken?"
        " Schon mit Leerzeichen.",
    ]


@pytest.mark.asyncio
async def test_models_lists_the_profiles_as_live_voice(running):
    """SRW's Admin → Models tests and imports the gateway's endpoint through
    GET /v1/models; each profile declares live_voice, whatever its name."""
    gateway, client, _ = await running(enabled=False)
    response = await client.get("/v1/models")
    assert response.status == 200
    assert await response.json() == {
        "object": "list",
        "data": [
            {
                "id": "scripted",
                "object": "model",
                "owned_by": "srw-voice-gateway",
                "capabilities": ["live_voice"],
            }
        ],
    }
    assert (
        await client.get("/v1/models", headers={"Authorization": "x"})
    ).status == 401
