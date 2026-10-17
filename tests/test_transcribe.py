"""Tests for the persistent-session speech-to-text path.

Covers the transcribe service (``orchestrator/services/transcribe.py``) and the
``POST /api/persistent/threads/{id}/transcribe`` and ``POST /api/voice/transcribe``
endpoint wiring.

The service is tested in isolation by patching the shared credential resolver
and the ``AsyncOpenAI`` client (mirrors ``tests/test_audio_helper.py``). The
endpoint is checked for route registration and exercised by calling the handler
directly with mocked auth + service — the orchestrator main app is too heavy for
a full TestClient, matching ``tests/test_admin_providers_api.py``.
"""

from __future__ import annotations

import io
import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

from orchestrator.services.capability_credentials import CapabilityCredentials  # noqa: E402


def _credentials(model, base_url=None, api_key=None):
    return CapabilityCredentials(model=model, base_url=base_url, api_key=api_key)


def _mock_db() -> MagicMock:
    """A postgres_db double whose pool-reading methods are awaitable no-ops."""
    db = MagicMock()
    db.get_user_settings = AsyncMock(return_value={})
    db.resolve_api_keys_for_job = AsyncMock(return_value={})
    return db


def _mock_openai(create_return):
    """Build a (class, client) pair where audio.transcriptions.create resolves
    to ``create_return`` and close() is awaitable."""
    client = MagicMock()
    client.audio.transcriptions.create = AsyncMock(return_value=create_return)
    client.close = AsyncMock()
    cls = MagicMock(return_value=client)
    return cls, client


# ---------------------------------------------------------------------------
# Service: transcribe_thread_audio
# ---------------------------------------------------------------------------


class TestTranscribeService:
    @pytest.mark.asyncio
    async def test_returns_text_on_success(self):
        from orchestrator.services.transcribe import transcribe_thread_audio

        cls, client = _mock_openai("hello world")
        with (
            patch("orchestrator.services.transcribe.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.transcribe.resolve_capability_credentials",
                AsyncMock(return_value=_credentials("whisper-1", None, "sk-key")),
            ),
        ):
            text = await transcribe_thread_audio(
                audio_bytes=b"\x00\x01\x02",
                filename="voice.webm",
                user_id="u1",
                postgres_db=_mock_db(),
            )
        assert text == "hello world"
        client.audio.transcriptions.create.assert_awaited_once()
        client.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_none_when_no_model_configured(self):
        from orchestrator.services.transcribe import transcribe_thread_audio

        cls, _ = _mock_openai("unused")
        with (
            patch("orchestrator.services.transcribe.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.transcribe.resolve_capability_credentials",
                AsyncMock(return_value=None),
            ),
        ):
            text = await transcribe_thread_audio(
                audio_bytes=b"\x00\x01",
                filename="voice.webm",
                user_id="u1",
                postgres_db=_mock_db(),
            )
        assert text is None
        cls.assert_not_called()  # no client built when STT is unconfigured

    @pytest.mark.asyncio
    async def test_raises_when_no_api_key(self):
        """A configured model with no key is broken, not off → raise (→ 502)."""
        from orchestrator.services.transcribe import (
            TranscriptionError,
            transcribe_thread_audio,
        )

        cls, _ = _mock_openai("x")
        with (
            patch("orchestrator.services.transcribe.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.transcribe.resolve_capability_credentials",
                AsyncMock(return_value=_credentials("whisper-1", None, None)),
            ),
        ):
            with pytest.raises(TranscriptionError):
                await transcribe_thread_audio(
                    audio_bytes=b"\x00",
                    filename="voice.webm",
                    user_id="u1",
                    postgres_db=_mock_db(),
                )
        cls.assert_not_called()  # no client built without a key

    @pytest.mark.asyncio
    async def test_none_on_empty_audio(self):
        from orchestrator.services.transcribe import transcribe_thread_audio

        text = await transcribe_thread_audio(
            audio_bytes=b"",
            filename="voice.webm",
            user_id="u1",
            postgres_db=_mock_db(),
        )
        assert text is None

    @pytest.mark.asyncio
    async def test_raises_on_openai_error(self):
        """A transcription call failure is broken, not off → raise (→ 502)."""
        from orchestrator.services.transcribe import (
            TranscriptionError,
            transcribe_thread_audio,
        )

        client = MagicMock()
        client.audio.transcriptions.create = AsyncMock(side_effect=RuntimeError("boom"))
        client.close = AsyncMock()
        cls = MagicMock(return_value=client)
        with (
            patch("orchestrator.services.transcribe.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.transcribe.resolve_capability_credentials",
                AsyncMock(return_value=_credentials("whisper-1", None, "sk-key")),
            ),
        ):
            with pytest.raises(TranscriptionError):
                await transcribe_thread_audio(
                    audio_bytes=b"\x00\x01",
                    filename="voice.webm",
                    user_id="u1",
                    postgres_db=_mock_db(),
                )
        client.close.assert_awaited_once()  # cleaned up even on failure

    def test_timeout_scales_with_payload_size(self):
        from orchestrator.services.transcribe import (
            _STT_TIMEOUT_MAX,
            _STT_TIMEOUT_MIN,
            _stt_timeout,
        )

        assert _stt_timeout(1000) == _STT_TIMEOUT_MIN  # tiny clip → floor
        assert _stt_timeout(10**9) == _STT_TIMEOUT_MAX  # huge clip → ceiling
        mid = _stt_timeout(4_000_000)  # ~a few-minute clip → between the bounds
        assert _STT_TIMEOUT_MIN < mid < _STT_TIMEOUT_MAX

    @pytest.mark.asyncio
    async def test_empty_transcript_returns_empty_not_none(self):
        """Silence (empty transcript) is a success, distinct from the None
        'no model' signal, so it must not collapse into a 204."""
        from orchestrator.services.transcribe import transcribe_thread_audio

        cls, _ = _mock_openai("")  # endpoint returns an empty transcript
        with (
            patch("orchestrator.services.transcribe.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.transcribe.resolve_capability_credentials",
                AsyncMock(return_value=_credentials("whisper-1", None, "sk-key")),
            ),
        ):
            text = await transcribe_thread_audio(
                audio_bytes=b"\x00",
                filename="voice.webm",
                user_id="u1",
                postgres_db=_mock_db(),
            )
        assert text == ""

    @pytest.mark.asyncio
    async def test_handles_object_response_with_text_attr(self):
        from orchestrator.services.transcribe import transcribe_thread_audio

        resp_obj = MagicMock()
        resp_obj.text = "  spoken words  "
        cls, _ = _mock_openai(resp_obj)  # not a str → service reads .text
        with (
            patch("orchestrator.services.transcribe.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.transcribe.resolve_capability_credentials",
                AsyncMock(return_value=_credentials("whisper-1", None, "sk-key")),
            ),
        ):
            text = await transcribe_thread_audio(
                audio_bytes=b"\x00",
                filename="voice.webm",
                user_id="u1",
                postgres_db=_mock_db(),
            )
        assert text == "spoken words"

    @pytest.mark.asyncio
    async def test_unwraps_json_string_blob(self):
        """Non-compliant endpoints return a `{"text": ...}` JSON *string* instead
        of a parsed object — the JSON must be unwrapped, not leaked verbatim."""
        from orchestrator.services.transcribe import transcribe_thread_audio

        cls, _ = _mock_openai('{"text": "Hey there"}')
        with (
            patch("orchestrator.services.transcribe.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.transcribe.resolve_capability_credentials",
                AsyncMock(return_value=_credentials("whisper-1", None, "sk-key")),
            ),
        ):
            text = await transcribe_thread_audio(
                audio_bytes=b"\x00",
                filename="voice.webm",
                user_id="u1",
                postgres_db=_mock_db(),
            )
        assert text == "Hey there"

    @pytest.mark.asyncio
    async def test_handles_dict_result(self):
        from orchestrator.services.transcribe import transcribe_thread_audio

        cls, _ = _mock_openai({"text": "Hi"})
        with (
            patch("orchestrator.services.transcribe.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.transcribe.resolve_capability_credentials",
                AsyncMock(return_value=_credentials("whisper-1", None, "sk-key")),
            ),
        ):
            text = await transcribe_thread_audio(
                audio_bytes=b"\x00",
                filename="voice.webm",
                user_id="u1",
                postgres_db=_mock_db(),
            )
        assert text == "Hi"

    @pytest.mark.asyncio
    async def test_plain_string_passthrough(self):
        from orchestrator.services.transcribe import transcribe_thread_audio

        cls, _ = _mock_openai("Just plain words")
        with (
            patch("orchestrator.services.transcribe.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.transcribe.resolve_capability_credentials",
                AsyncMock(return_value=_credentials("whisper-1", None, "sk-key")),
            ),
        ):
            text = await transcribe_thread_audio(
                audio_bytes=b"\x00",
                filename="voice.webm",
                user_id="u1",
                postgres_db=_mock_db(),
            )
        assert text == "Just plain words"


# ---------------------------------------------------------------------------
# Endpoint: POST /api/persistent/threads/{thread_id}/transcribe
# ---------------------------------------------------------------------------


def _voice_route_deps(*, db=None, ledger=None, approved=None):
    """Route dependencies for the extracted voice router.

    The transcribe endpoint moved out of ``main`` into
    ``orchestrator.routers.voice`` (R1.B02); its collaborators arrive injected
    rather than as patched module globals. The STT service is still reached by
    patching ``orchestrator.services.transcribe``, exactly as before.
    """
    from orchestrator.routers.voice import VoiceDependencies as _RouteDeps
    from orchestrator.services.voice import VoiceDependencies as _OpDeps

    store = db if db is not None else MagicMock()

    async def _owner(_request, _store, _thread_id):
        return {"id": "u1"}, {"id": "t1"}

    async def _approved(_request, _store):
        return {"id": "u1"}

    return _RouteDeps(
        store=store,
        operations=_OpDeps(store=store, logger=MagicMock(), ledger=ledger),
        require_approved_user=approved or _approved,
        require_thread_owner=_owner,
    )


def _upload(data: bytes):
    from fastapi import UploadFile

    return UploadFile(file=io.BytesIO(data), filename="voice.webm")


class TestTranscribeEndpoint:
    def test_route_is_registered(self):
        from orchestrator.main import app
        from tests._route_inventory import mounted_routes

        routes = mounted_routes(app)
        assert (
            "POST",
            "/api/persistent/threads/{thread_id}/transcribe",
        ) in routes

    @pytest.mark.asyncio
    async def test_returns_text(self):
        from orchestrator.routers import voice as voice_routes

        with patch(
            "orchestrator.services.transcribe.transcribe_thread_audio",
            AsyncMock(return_value="hello world"),
        ):
            resp = await voice_routes.transcribe_thread_audio_endpoint(
                thread_id="t1",
                request=MagicMock(),
                audio=_upload(b"\x00\x01\x02"),
                dependencies=_voice_route_deps(),
            )
        assert resp.status_code == 200
        assert json.loads(resp.body) == {"text": "hello world"}

    @pytest.mark.asyncio
    async def test_204_when_unavailable(self):
        from orchestrator.routers import voice as voice_routes

        with patch(
            "orchestrator.services.transcribe.transcribe_thread_audio",
            AsyncMock(return_value=None),
        ):
            resp = await voice_routes.transcribe_thread_audio_endpoint(
                thread_id="t1",
                request=MagicMock(),
                audio=_upload(b"\x00\x01"),
                dependencies=_voice_route_deps(),
            )
        assert resp.status_code == 204

    @pytest.mark.asyncio
    async def test_502_on_transcription_error(self):
        """A configured model that fails → 502 (honest error), not a silent 204."""
        from fastapi import HTTPException

        from orchestrator.routers import voice as voice_routes
        from orchestrator.services.transcribe import TranscriptionError

        with patch(
            "orchestrator.services.transcribe.transcribe_thread_audio",
            AsyncMock(side_effect=TranscriptionError("down")),
        ):
            with pytest.raises(HTTPException) as exc:
                await voice_routes.transcribe_thread_audio_endpoint(
                    thread_id="t1",
                    request=MagicMock(),
                    audio=_upload(b"\x00\x01"),
                    dependencies=_voice_route_deps(),
                )
        assert exc.value.status_code == 502

    @pytest.mark.asyncio
    async def test_400_on_empty_audio(self):
        from fastapi import HTTPException

        from orchestrator.routers import voice as voice_routes

        with pytest.raises(HTTPException) as exc:
            await voice_routes.transcribe_thread_audio_endpoint(
                thread_id="t1",
                request=MagicMock(),
                audio=_upload(b""),
                dependencies=_voice_route_deps(),
            )
        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_413_when_too_large(self):
        from fastapi import HTTPException

        from orchestrator.routers import voice as voice_routes

        big = _upload(b"\x00" * (25 * 1024 * 1024 + 1))
        with pytest.raises(HTTPException) as exc:
            await voice_routes.transcribe_thread_audio_endpoint(
                thread_id="t1",
                request=MagicMock(),
                audio=big,
                dependencies=_voice_route_deps(),
            )
        assert exc.value.status_code == 413


# ---------------------------------------------------------------------------
# Endpoint: POST /api/voice/transcribe (landing draft, no session yet)
# ---------------------------------------------------------------------------


class TestAccountTranscribeEndpoint:
    def test_route_is_registered(self):
        from orchestrator.main import app
        from tests._route_inventory import mounted_routes

        assert ("POST", "/api/voice/transcribe") in mounted_routes(app)

    @pytest.mark.asyncio
    async def test_returns_text_and_attributes_no_thread(self):
        from orchestrator.routers import voice as voice_routes

        service = AsyncMock(return_value="draft words")
        with patch("orchestrator.services.transcribe.transcribe_thread_audio", service):
            resp = await voice_routes.transcribe_audio_endpoint(
                request=MagicMock(),
                audio=_upload(b"\x00\x01\x02"),
                dependencies=_voice_route_deps(),
            )
        assert resp.status_code == 200
        assert json.loads(resp.body) == {"text": "draft words"}
        assert service.await_args.kwargs["user_id"] == "u1"
        assert service.await_args.kwargs["ref_id"] is None

    @pytest.mark.asyncio
    async def test_204_when_unavailable(self):
        from orchestrator.routers import voice as voice_routes

        with patch(
            "orchestrator.services.transcribe.transcribe_thread_audio",
            AsyncMock(return_value=None),
        ):
            resp = await voice_routes.transcribe_audio_endpoint(
                request=MagicMock(),
                audio=_upload(b"\x00\x01"),
                dependencies=_voice_route_deps(),
            )
        assert resp.status_code == 204

    @pytest.mark.asyncio
    async def test_502_on_transcription_error(self):
        from fastapi import HTTPException

        from orchestrator.routers import voice as voice_routes
        from orchestrator.services.transcribe import TranscriptionError

        with patch(
            "orchestrator.services.transcribe.transcribe_thread_audio",
            AsyncMock(side_effect=TranscriptionError("down")),
        ):
            with pytest.raises(HTTPException) as exc:
                await voice_routes.transcribe_audio_endpoint(
                    request=MagicMock(),
                    audio=_upload(b"\x00\x01"),
                    dependencies=_voice_route_deps(),
                )
        assert exc.value.status_code == 502

    @pytest.mark.asyncio
    async def test_unapproved_user_is_refused_before_transcribing(self):
        from fastapi import HTTPException

        from orchestrator.routers import voice as voice_routes

        async def _refuse(_request, _store):
            raise HTTPException(status_code=403, detail="Account pending approval")

        service = AsyncMock(return_value="never")
        with patch("orchestrator.services.transcribe.transcribe_thread_audio", service):
            with pytest.raises(HTTPException) as exc:
                await voice_routes.transcribe_audio_endpoint(
                    request=MagicMock(),
                    audio=_upload(b"\x00\x01"),
                    dependencies=_voice_route_deps(approved=_refuse),
                )
        assert exc.value.status_code == 403
        service.assert_not_awaited()


# ---------------------------------------------------------------------------
# Usage metering (rate-limiting v2): STT is metered per request, non-fatally.
# ---------------------------------------------------------------------------


def _mock_ledger(*, available=True):
    led = MagicMock()
    led.is_available = available
    led.record_events = AsyncMock(return_value=1)
    return led


class TestTranscribeMetering:
    @pytest.mark.asyncio
    async def test_records_stt_request_event(self):
        from orchestrator.services.transcribe import transcribe_thread_audio

        cls, _ = _mock_openai("hi")
        led = _mock_ledger()
        with (
            patch("orchestrator.services.transcribe.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.transcribe.resolve_capability_credentials",
                AsyncMock(return_value=_credentials("whisper-1", None, "sk-key")),
            ),
        ):
            text = await transcribe_thread_audio(
                audio_bytes=b"\x00\x01\x02",
                filename="voice.webm",
                user_id="u1",
                postgres_db=_mock_db(),
                ledger=led,
                ref_id="t1",
            )
        assert text == "hi"
        led.record_events.assert_awaited_once()
        (events,), _ = led.record_events.call_args
        ev = events[0]
        assert ev.category == "stt"
        assert ev.unit == "stt-request"
        assert ev.quantity == 1
        assert ev.resource == "whisper-1"
        assert ev.ref_kind == "thread"
        assert ev.ref_id == "t1"
        assert ev.details["bytes"] == 3

    @pytest.mark.asyncio
    async def test_draft_dictation_records_an_unreferenced_event(self):
        from orchestrator.services.transcribe import transcribe_thread_audio

        cls, _ = _mock_openai("hi")
        led = _mock_ledger()
        with (
            patch("orchestrator.services.transcribe.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.transcribe.resolve_capability_credentials",
                AsyncMock(return_value=_credentials("whisper-1", None, "sk-key")),
            ),
        ):
            await transcribe_thread_audio(
                audio_bytes=b"\x00",
                filename="voice.webm",
                user_id="u1",
                postgres_db=_mock_db(),
                ledger=led,
                ref_id=None,
            )
        (events,), _ = led.record_events.call_args
        assert (events[0].ref_kind, events[0].ref_id) == (None, None)
        assert events[0].user_id == "u1"

    @pytest.mark.asyncio
    async def test_metering_failure_does_not_drop_transcript(self):
        from orchestrator.services.transcribe import transcribe_thread_audio

        cls, _ = _mock_openai("hi")
        led = _mock_ledger()
        led.record_events = AsyncMock(side_effect=RuntimeError("audit down"))
        with (
            patch("orchestrator.services.transcribe.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.transcribe.resolve_capability_credentials",
                AsyncMock(return_value=_credentials("whisper-1", None, "sk-key")),
            ),
        ):
            text = await transcribe_thread_audio(
                audio_bytes=b"\x00",
                filename="voice.webm",
                user_id="u1",
                postgres_db=_mock_db(),
                ledger=led,
                ref_id="t1",
            )
        # The metering write is OUTSIDE the transcribe try — a failure here must
        # never cost the user their transcript.
        assert text == "hi"

    @pytest.mark.asyncio
    async def test_no_write_when_ledger_unavailable(self):
        from orchestrator.services.transcribe import transcribe_thread_audio

        cls, _ = _mock_openai("hi")
        led = _mock_ledger(available=False)
        with (
            patch("orchestrator.services.transcribe.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.transcribe.resolve_capability_credentials",
                AsyncMock(return_value=_credentials("whisper-1", None, "sk-key")),
            ),
        ):
            text = await transcribe_thread_audio(
                audio_bytes=b"\x00",
                filename="voice.webm",
                user_id="u1",
                postgres_db=_mock_db(),
                ledger=led,
                ref_id="t1",
            )
        assert text == "hi"
        led.record_events.assert_not_awaited()
