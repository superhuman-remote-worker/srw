"""P0 of phone calls: the call contract, webhook verification and the GPT-Live client.

The client runs against ``tests/_fake_gpt_live.py`` over real HTTP and
WebSocket connections on localhost.
"""

from __future__ import annotations

import asyncio
import base64
import json
import time

import pytest
import pytest_asyncio

from tests._fake_gpt_live import DEFAULT_KEY, FakeGptLive
from shared.runtime.telephony.contract import (
    MAX_APPEND_CHARS,
    AppendAcknowledged,
    AppendKind,
    CloseReason,
    DelegationCreated,
    DtmfReceived,
    EngineError,
    OtherEvent,
    SessionClosed,
    Speaker,
    TranscriptDelta,
    TransportProgress,
    UsageUpdated,
    clip_append_content,
)
from shared.runtime.telephony.gpt_live import (
    AppendRejectedError,
    GptLiveClient,
    GptLiveError,
    LiveEndpoints,
    build_session_config,
    parse_event,
)
from shared.runtime.telephony.webhook import (
    IncomingCall,
    WebhookVerificationError,
    parse_incoming_call,
    sign_webhook,
    sip_uri_user,
    verify_webhook,
)

SECRET = "whsec_" + base64.b64encode(b"k" * 32).decode()


def _headers(webhook_id: str, timestamp: int, signature: str) -> dict:
    return {
        "webhook-id": webhook_id,
        "webhook-timestamp": str(timestamp),
        "webhook-signature": signature,
    }


class TestWebhookVerification:
    def test_round_trip(self):
        body = json.dumps({"type": "live.transport.incoming", "data": {}}).encode()
        now = int(time.time())
        payload = verify_webhook(
            SECRET, _headers("wh_1", now, sign_webhook(SECRET, "wh_1", now, body)), body
        )
        assert payload["type"] == "live.transport.incoming"

    def test_header_names_are_case_insensitive(self):
        body = b"{}"
        now = int(time.time())
        headers = {
            "Webhook-Id": "wh_1",
            "Webhook-Timestamp": str(now),
            "Webhook-Signature": sign_webhook(SECRET, "wh_1", now, body),
        }
        assert verify_webhook(SECRET, headers, body) == {}

    def test_wrong_secret_is_refused(self):
        body = b"{}"
        now = int(time.time())
        other = "whsec_" + base64.b64encode(b"x" * 32).decode()
        with pytest.raises(WebhookVerificationError):
            verify_webhook(
                SECRET,
                _headers("wh_1", now, sign_webhook(other, "wh_1", now, body)),
                body,
            )

    def test_tampered_body_is_refused(self):
        now = int(time.time())
        signature = sign_webhook(SECRET, "wh_1", now, b'{"a":1}')
        with pytest.raises(WebhookVerificationError):
            verify_webhook(SECRET, _headers("wh_1", now, signature), b'{"a":2}')

    def test_stale_timestamp_is_refused(self):
        body = b"{}"
        old = int(time.time()) - 3600
        with pytest.raises(WebhookVerificationError, match="tolerance"):
            verify_webhook(
                SECRET,
                _headers("wh_1", old, sign_webhook(SECRET, "wh_1", old, body)),
                body,
            )

    def test_any_signature_of_a_rotation_matches(self):
        body = b"{}"
        now = int(time.time())
        other = "whsec_" + base64.b64encode(b"x" * 32).decode()
        both = f"{sign_webhook(other, 'wh_1', now, body)} {sign_webhook(SECRET, 'wh_1', now, body)}"
        assert verify_webhook(SECRET, _headers("wh_1", now, both), body) == {}

    def test_missing_headers_are_refused(self):
        with pytest.raises(WebhookVerificationError, match="missing"):
            verify_webhook(SECRET, {}, b"{}")


class TestIncomingCallParsing:
    def test_new_event(self):
        call = parse_incoming_call(
            {
                "id": "evt_1",
                "type": "live.transport.incoming",
                "created_at": 1,
                "data": {
                    "type": "sip",
                    "session_id": "live_123",
                    "sip_headers": [
                        {
                            "name": "To",
                            "value": "<sip:+4960521829990@sip.api.openai.com;user=phone>",
                        }
                    ],
                },
            },
            "wh_1",
        )
        assert isinstance(call, IncomingCall)
        assert call.session_id == "live_123"
        assert call.event_id == "evt_1"
        assert sip_uri_user(call.sip_header("to")) == "+4960521829990"

    def test_legacy_event_has_no_data_type(self):
        call = parse_incoming_call(
            {"type": "live.call.incoming", "data": {"session_id": "live_9"}}, "wh_2"
        )
        assert call is not None and call.session_id == "live_9"

    def test_non_sip_and_other_events_are_ignored(self):
        assert (
            parse_incoming_call(
                {
                    "type": "live.transport.incoming",
                    "data": {"type": "webrtc", "session_id": "x"},
                },
                "w",
            )
            is None
        )
        assert (
            parse_incoming_call({"type": "response.completed", "data": {}}, "w") is None
        )

    def test_call_without_session_is_malformed(self):
        with pytest.raises(ValueError):
            parse_incoming_call(
                {"type": "live.transport.incoming", "data": {"type": "sip"}}, "w"
            )

    @pytest.mark.parametrize(
        "value,expected",
        [
            ('"Anna" <sip:+4917600000000@host;tag=a>', "+4917600000000"),
            ("sip:proj_abc@sip.api.openai.com:5061;transport=tls", "proj_abc"),
            ("tel:+4960521829990", "+4960521829990"),
            (None, None),
        ],
    )
    def test_sip_uri_user(self, value, expected):
        assert sip_uri_user(value) == expected


class TestContract:
    def test_short_content_is_untouched(self):
        assert clip_append_content("  Hallo.  ") == "Hallo."

    def test_long_content_is_cut_at_a_sentence(self):
        text = ("Das ist ein Satz. " * 200).strip()
        clipped = clip_append_content(text)
        assert len(clipped) <= MAX_APPEND_CHARS
        assert clipped.endswith(".")


class TestParseEvent:
    def test_mapping(self):
        assert parse_event({"type": "session.input_audio.append", "audio": "x"}) is None
        delta = parse_event(
            {
                "type": "session.input_transcript.delta",
                "event_id": "e1",
                "delta": "Hallo",
                "start_ms": 5,
                "end_ms": 9,
            }
        )
        assert delta == TranscriptDelta(Speaker.CALLER, "Hallo", 5, 9, "e1")
        assert (
            parse_event(
                {"type": "session.output_transcript.delta", "delta": "Guten Tag"}
            ).speaker
            is Speaker.ASSISTANT
        )
        created = parse_event(
            {
                "type": "session.delegation.created",
                "offset_ms": 10,
                "delegation": {"id": "item_1", "target": "client"},
            }
        )
        assert created == DelegationCreated("item_1", 10, "client", None)
        ack = parse_event(
            {"type": "session.commentary.appended", "client_event_id": "c1"}
        )
        assert isinstance(ack, AppendAcknowledged) and ack.kind is AppendKind.COMMENTARY
        err = parse_event(
            {
                "type": "error",
                "error": {"code": "bad", "message": "no", "client_event_id": "c2"},
            }
        )
        assert isinstance(err, EngineError) and err.client_event_id == "c2"
        assert parse_event(
            {"type": "transport.dtmf.received", "event": "5"}
        ) == DtmfReceived("5")
        assert parse_event(
            {"type": "transport.failed", "error": {"code": "busy"}}
        ) == TransportProgress("failed", "busy")
        usage = parse_event(
            {
                "type": "session.usage.updated",
                "usage": {"seconds": 12},
                "context_window": {"usage_ratio": 0.4},
            }
        )
        assert usage == UsageUpdated(12.0, 0.4)
        closed = parse_event(
            {
                "type": "session.closed",
                "reason": "remote_hangup",
                "usage": {"seconds": 30},
            }
        )
        assert closed == SessionClosed(
            CloseReason.REMOTE_HANGUP, 30.0, None, "remote_hangup"
        )
        assert (
            parse_event({"type": "session.closed", "reason": "new_reason"}).reason
            is CloseReason.UNKNOWN
        )
        assert isinstance(parse_event({"type": "session.updated"}), OtherEvent)


@pytest_asyncio.fixture
async def fake():
    server = FakeGptLive()
    await server.start()
    try:
        yield server
    finally:
        await server.stop()


def _client(fake: FakeGptLive, key: str = DEFAULT_KEY) -> GptLiveClient:
    return GptLiveClient(
        key, LiveEndpoints(api_base=fake.api_base, ws_base=fake.ws_base)
    )


async def _collect(sideband, count: int, timeout: float = 3.0) -> list:
    events = []
    while len(events) < count:
        event = await sideband.next_event(timeout)
        if event is None:
            break
        events.append(event)
    return events


class TestGptLiveClient:
    @pytest.mark.asyncio
    async def test_accept_sends_a_live_session(self, fake):
        session_id = await fake.incoming_call()
        async with _client(fake) as client:
            await client.accept(
                session_id,
                build_session_config(instructions="Sie sind ...", voice="marin"),
            )
        config = fake.session(session_id).accept_config
        assert config["type"] == "live"
        assert config["model"] == "gpt-live-1"
        assert config["delegation"] == {"type": "client"}
        assert "format" not in config["audio"]["output"]

    @pytest.mark.asyncio
    async def test_second_decision_is_refused(self, fake):
        session_id = await fake.incoming_call()
        async with _client(fake) as client:
            await client.reject(session_id, 486)
            with pytest.raises(GptLiveError) as raised:
                await client.accept(session_id, build_session_config(instructions="x"))
        assert raised.value.status == 409
        assert raised.value.code == "decision_already_made"
        assert fake.session(session_id).reject_status == 486

    @pytest.mark.asyncio
    async def test_reject_status_is_validated(self, fake):
        async with _client(fake) as client:
            with pytest.raises(ValueError):
                await client.reject("live_x", 200)

    @pytest.mark.asyncio
    async def test_bad_key_is_an_http_error(self, fake):
        session_id = await fake.incoming_call()
        async with _client(fake, key="sk-wrong") as client:
            with pytest.raises(GptLiveError) as raised:
                await client.accept(session_id, build_session_config(instructions="x"))
        assert raised.value.status == 401

    @pytest.mark.asyncio
    async def test_sideband_replays_and_deduplicates(self, fake):
        session_id = await fake.incoming_call()
        async with _client(fake) as client:
            await client.accept(session_id, build_session_config(instructions="x"))
            first = await fake.emit(
                session_id,
                {"type": "session.output_transcript.delta", "delta": "Guten Tag"},
            )
            await fake.say(session_id, "caller", "Hallo")
            sideband = await client.attach(session_id)
            replayed = await _collect(sideband, 2)
            assert [e.text for e in replayed] == ["Guten Tag", "Hallo"]
            # The same event again (a second replay) is dropped.
            await fake.emit(session_id, dict(first))
            await fake.say(session_id, "caller", "Haben Sie ein Schwimmbad?")
            nxt = await sideband.next_event(3)
            assert nxt.text == "Haben Sie ein Schwimmbad?"
            await sideband.detach()
        assert (
            fake.session(session_id).status == "accepted"
        )  # detach leaves the call up

    @pytest.mark.asyncio
    async def test_append_is_acknowledged_and_unknown_delegation_is_rejected(
        self, fake
    ):
        session_id = await fake.incoming_call()
        async with _client(fake) as client:
            await client.accept(session_id, build_session_config(instructions="x"))
            sideband = await client.attach(session_id)
            ack = await sideband.append(
                AppendKind.INSTRUCTIONS, "Begrüßen Sie jetzt.", timeout=3
            )
            assert ack.kind is AppendKind.INSTRUCTIONS
            delegation_id = await fake.delegate(session_id)
            ack = await sideband.append(
                AppendKind.COMMENTARY,
                "Ja, es gibt ein Solebad.",
                delegation_id=delegation_id,
                timeout=3,
            )
            assert ack.kind is AppendKind.COMMENTARY
            with pytest.raises(AppendRejectedError) as raised:
                await sideband.append(
                    AppendKind.COMMENTARY, "x", delegation_id="item_unknown", timeout=3
                )
            assert raised.value.error.code == "unknown_delegation"
            sent = await fake.wait_for_command(
                session_id,
                "session.commentary.append",
                lambda c: c["delegation_id"] == delegation_id,
            )
            assert sent["content"] == "Ja, es gibt ein Solebad."
            await sideband.detach()

    @pytest.mark.asyncio
    async def test_hangup_closes_every_sideband(self, fake):
        session_id = await fake.incoming_call()
        async with _client(fake) as client:
            await client.accept(session_id, build_session_config(instructions="x"))
            recorder = await client.attach(session_id)
            actor = await client.attach(session_id)
            await client.hangup(session_id)
            for sideband in (recorder, actor):
                events = await _collect(sideband, 5)
                assert isinstance(events[-1], SessionClosed)
                assert events[-1].reason is CloseReason.CLOSE_REQUESTED
                assert await sideband.wait_closed(3)
                assert sideband.closed_event is not None

    @pytest.mark.asyncio
    async def test_close_session_waits_for_closed(self, fake):
        session_id = await fake.incoming_call()
        async with _client(fake) as client:
            await client.accept(session_id, build_session_config(instructions="x"))
            sideband = await client.attach(session_id)
            closed = await sideband.close_session(timeout=3)
        assert closed is not None and closed.reason is CloseReason.CLOSE_REQUESTED

    @pytest.mark.asyncio
    async def test_remote_hangup_ends_iteration(self, fake):
        session_id = await fake.incoming_call()
        async with _client(fake) as client:
            await client.accept(session_id, build_session_config(instructions="x"))
            sideband = await client.attach(session_id)
            await fake.say(session_id, "caller", "Tschüss")
            await fake.remote_hangup(session_id)
            events = [event async for event in sideband]
        assert isinstance(events[0], TranscriptDelta)
        assert events[-1].reason is CloseReason.REMOTE_HANGUP

    @pytest.mark.asyncio
    async def test_pending_append_fails_when_the_sideband_drops(self, fake):
        fake.ack_delay_s = 1.0
        session_id = await fake.incoming_call()
        async with _client(fake) as client:
            await client.accept(session_id, build_session_config(instructions="x"))
            sideband = await client.attach(session_id)
            future = await sideband.send_append(AppendKind.THINKING, "Kontext")
            await fake.remote_hangup(session_id)
            with pytest.raises(Exception):
                await asyncio.wait_for(future, 3)

    @pytest.mark.asyncio
    async def test_outbound_create_returns_the_session(self, fake):
        async with _client(fake) as client:
            session_id = await client.create_outbound(
                build_session_config(instructions="Rückruf"),
                destination="+4917600000000",
                provider_url="sips:sip.telnyx.com:5061",
                username="u",
                password="p",
                caller_number="+4960521829990",
            )
        body = fake.session(session_id).create_body
        assert body["transport"]["trunk"]["auth"]["type"] == "digest"
        assert body["transport"]["destination"] == "+4917600000000"
