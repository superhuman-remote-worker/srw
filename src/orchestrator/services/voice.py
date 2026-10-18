"""Voice operations: read-aloud (TTS), speech-to-text, and the voice library.

Thin behavioral adapters over :mod:`orchestrator.services.tts` and
:mod:`orchestrator.services.transcribe`. Those modules own model resolution,
credential lookup, chunking and metering; everything here is the HTTP-facing
shape: the disabled-feature ``204``, the error-code → status mapping, the
admin add-gate, and the SSE framing of the streaming planner.

Provider keys never reach this layer — every ElevenLabs/OpenAI call is made
inside the TTS service with credentials it resolves itself, and the responses
built here carry audio, text and voice metadata only.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Awaitable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from fastapi import HTTPException
from fastapi.responses import JSONResponse, Response, StreamingResponse

from orchestrator.services import transcribe as transcribe_service
from orchestrator.services import tts as tts_service

#: The ElevenLabs Voice Library add gate's system-settings key.
TTS_LIBRARY_SETTING_KEY = "tts.elevenlabs_library_enabled"

#: Whisper's own upload ceiling.
STT_MAX_UPLOAD_BYTES = 25 * 1024 * 1024

# Map a TTS synthesis failure code → HTTP status. Deliberately NEVER 401/403
# for "auth": the cockpit auth interceptor redirects to login on 401, and a
# *provider* key problem must not look like the user's own session expiring.
# The {code, message} body is what the cockpit actually switches on.
TTS_ERROR_STATUS = {
    "payment_required": 402,
    "rate_limit": 429,
    "auth": 502,
    "generic": 502,
}


class VoiceStore(Protocol):
    def get_system_setting(self, key: str) -> Awaitable[Mapping[str, Any] | None]: ...
    def get_user_settings(
        self, user_id: str
    ) -> Awaitable[Mapping[str, Any] | None]: ...
    def resolve_default_for_capability(self, capability: str) -> Awaitable[Any]: ...


class VoiceLogger(Protocol):
    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None: ...


@dataclass(frozen=True)
class VoiceDependencies:
    store: VoiceStore
    logger: VoiceLogger
    #: The metering ledger; ``None`` before ``lifespan`` has built it.
    ledger: Any = None


def tts_synthesis_http_error(exc: Exception) -> HTTPException:
    """A TTS synthesis failure → an HTTP error the cockpit can localize:
    status by code (payment→402, rate→429, else→502), with a machine-readable
    ``{code, message}`` body so the UI shows "this voice needs a paid plan"
    instead of a generic "synthesis failed"."""
    code = getattr(exc, "code", "generic") or "generic"
    status = TTS_ERROR_STATUS.get(code, 502)
    return HTTPException(status_code=status, detail={"code": code, "message": str(exc)})


# =============================================================================
# Settings surface: preview, account voices, community library
# =============================================================================


async def preview_tts_voice(
    *, user_id: str, body: Mapping[str, Any], dependencies: VoiceDependencies
) -> Response:
    """Synthesize a short canned phrase in a candidate voice.

    ``204`` when no TTS model is configured (UI treats as feature-off);
    ``422`` when the custom sample exceeds ``_PREVIEW_TEXT_MAX``; ``502``
    when a configured model fails to synthesize.
    """
    voice = (body.get("voice") or "").strip()
    language = (body.get("language") or "en").strip() or "en"
    text = body.get("text") or ""
    if len(text) > tts_service._PREVIEW_TEXT_MAX:
        raise HTTPException(
            status_code=422,
            detail=(
                "Preview text must be at most "
                f"{tts_service._PREVIEW_TEXT_MAX} characters"
            ),
        )

    try:
        audio = await tts_service.synthesize_voice_preview(
            voice=voice or None,
            language=language,
            user_id=user_id,
            postgres_db=dependencies.store,
            text=text or None,
            ledger=dependencies.ledger,
        )
    except tts_service.TtsSynthesisError as exc:
        raise tts_synthesis_http_error(exc) from exc

    if audio is None:
        return Response(status_code=204)
    return JSONResponse({"audio": base64.b64encode(audio).decode("ascii")})


async def list_tts_voices(
    *, user_id: str, dependencies: VoiceDependencies
) -> dict[str, Any]:
    """Voices offered by the caller's configured TTS backend.

    A listing failure degrades to an empty list (the picker falls back to
    free text), never a 5xx. The provider key never reaches the browser —
    every ElevenLabs call is proxied server-side.
    """
    return await tts_service.list_account_voices(
        user_id=user_id, postgres_db=dependencies.store
    )


async def elevenlabs_library_enabled(*, dependencies: VoiceDependencies) -> bool:
    """The Voice Library *add* gate. DECIDED (tts_vendor_providers.md, OQ 1):
    library adds + designed voices consume the deployment account's plan-limited
    voice slots, so account-mutating actions ship behind this admin flag —
    default OFF (fail-closed, unlike the fail-open ``user_experts`` switch).
    Browsing / previewing the library stays ungated."""
    try:
        row = await dependencies.store.get_system_setting(TTS_LIBRARY_SETTING_KEY)
    except Exception:
        dependencies.logger.exception(
            "elevenlabs_library flag read failed; fail-closed"
        )
        return False
    value = (row or {}).get("value") or {}
    return isinstance(value, dict) and value.get("enabled") is True


async def search_tts_library(
    *,
    user_id: str,
    filters: Mapping[str, Any],
    dependencies: VoiceDependencies,
) -> dict[str, Any]:
    """Search the ElevenLabs community Voice Library (read-only, ungated).

    Returns ``{backend, voices, has_more, error, add_enabled}``; ``add_enabled``
    mirrors the admin flag so the browser knows whether to offer "Add to
    deployment". Failures degrade to an empty list with a readable ``error``,
    never a 5xx.
    """
    result = await tts_service.search_voice_library(
        user_id=user_id, postgres_db=dependencies.store, filters=dict(filters)
    )
    result["add_enabled"] = await elevenlabs_library_enabled(dependencies=dependencies)
    return result


async def add_tts_library_voice(
    *, user_id: str, body: Mapping[str, Any], dependencies: VoiceDependencies
) -> dict[str, Any]:
    """Copy a Library voice into the deployment ElevenLabs account.

    Behind the ``tts.elevenlabs_library_enabled`` admin flag (default off)
    because it consumes a plan-limited account voice slot shared by every user.
    ElevenLabs slot-limit / validation errors surface as a readable message with
    their status, not a 500.
    """
    if not await elevenlabs_library_enabled(dependencies=dependencies):
        raise HTTPException(
            status_code=403,
            detail="Adding voices from the library is disabled for this deployment.",
        )
    public_owner_id = (body.get("public_owner_id") or "").strip()
    voice_id = (body.get("voice_id") or "").strip()
    new_name = (body.get("new_name") or "").strip()
    if not public_owner_id or not voice_id or not new_name:
        raise HTTPException(
            status_code=422,
            detail="public_owner_id, voice_id and new_name are required.",
        )
    try:
        return await tts_service.add_library_voice(
            user_id=user_id,
            postgres_db=dependencies.store,
            public_owner_id=public_owner_id,
            voice_id=voice_id,
            new_name=new_name,
        )
    except tts_service.TtsLibraryError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.message) from exc


# =============================================================================
# Thread read-aloud and dictation
# =============================================================================


def _require_content(body: Mapping[str, Any]) -> str:
    content = (body.get("content") or "").strip()
    if not content:
        raise HTTPException(status_code=400, detail="Missing 'content' in request body")
    return content


async def synthesize_thread_message_tts(
    *,
    thread_id: str,
    user_id: str,
    body: Mapping[str, Any],
    dependencies: VoiceDependencies,
) -> Response:
    """Generate speech audio for a chat message.

    ``{"text": <spoken text>, "audio": <base64 MP3>}`` on success — ``text`` is
    the formulation-rewritten version actually read aloud, so the UI can surface
    it. ``204`` when no TTS model is configured (the cockpit treats this as
    "feature off"). ``502`` when a model is configured but synthesis fails — so
    the button shows an error instead of silently doing nothing.
    """
    content = _require_content(body)
    reformulate = bool(body.get("reformulate", True))
    language = (body.get("language") or "en").strip() or "en"

    try:
        result = await tts_service.generate_message_tts(
            content=content,
            language=language,
            reformulate=reformulate,
            user_id=user_id,
            postgres_db=dependencies.store,
            ledger=dependencies.ledger,
            ref_id=thread_id,
        )
    except tts_service.TtsSynthesisError as exc:
        raise tts_synthesis_http_error(exc) from exc

    if result is None:
        # 204: TTS disabled / not configured. The cockpit treats this as a
        # disabled-feature signal rather than an error.
        return Response(status_code=204)
    spoken_text, audio = result
    return JSONResponse(
        {"text": spoken_text, "audio": base64.b64encode(audio).decode("ascii")}
    )


async def plan_thread_message_tts(
    *,
    thread_id: str,
    user_id: str,
    body: Mapping[str, Any],
    dependencies: VoiceDependencies,
) -> Response:
    """Plan a (possibly long) message into ordered, speakable chunks.

    ``{"chunks": [str, ...], "rewritten": bool}``; ``rewritten`` is ``False``
    when the auxiliary LLM was unavailable and the raw markdown was split
    deterministically, so the UI can say "rewriting skipped". ``204`` when no
    TTS model is configured. ``502`` only on an unexpected planner error.
    """
    content = _require_content(body)
    reformulate = bool(body.get("reformulate", True))

    try:
        plan = await tts_service.plan_tts_chunks(
            content=content,
            user_id=user_id,
            postgres_db=dependencies.store,
            ledger=dependencies.ledger,
            ref_id=thread_id,
            reformulate=reformulate,
        )
    except Exception as exc:
        dependencies.logger.exception(
            "TTS chunk planning failed for thread %s", thread_id
        )
        raise HTTPException(status_code=502, detail="TTS planning failed") from exc

    if plan is None:
        return Response(status_code=204)
    return JSONResponse({"chunks": plan["chunks"], "rewritten": plan["rewritten"]})


async def stream_thread_message_tts_plan(
    *,
    thread_id: str,
    user_id: str,
    body: Mapping[str, Any],
    dependencies: VoiceDependencies,
) -> StreamingResponse:
    """Streaming (SSE) counterpart of the planner: emit each speakable chunk the
    moment the auxiliary LLM produces it, so the client synthesizes + starts
    playing chunk 1 while the rest still generate.

    Wire (``text/event-stream``):
        ``event: chunk`` → ``{"index", "text", "rewritten"}`` per ready chunk
        ``event: done``  → ``{"total", "rewritten"}`` (terminal)
        ``event: unavailable`` → ``{}`` when no TTS model is configured (the
            client treats this like the planner's ``204``)
        ``event: error`` → ``{"message"}`` on an unexpected stream failure
    """
    content = _require_content(body)

    async def event_stream():
        # Kickstart comment: fires the reader immediately and defeats proxy idle
        # buffering (mirrors the other SSE routes).
        yield ": open\n\n"
        try:
            async for ev in tts_service.stream_tts_chunks(
                content=content,
                user_id=user_id,
                postgres_db=dependencies.store,
                ledger=dependencies.ledger,
                ref_id=thread_id,
            ):
                etype = ev.get("type")
                if etype == "unavailable":
                    yield "event: unavailable\ndata: {}\n\n"
                    return
                if etype == "chunk":
                    yield (
                        "event: chunk\ndata: "
                        + json.dumps(
                            {
                                "index": ev["index"],
                                "text": ev["text"],
                                "rewritten": ev["rewritten"],
                            }
                        )
                        + "\n\n"
                    )
                elif etype == "done":
                    yield (
                        "event: done\ndata: "
                        + json.dumps(
                            {
                                "total": ev.get("total", 0),
                                "rewritten": ev.get("rewritten", False),
                            }
                        )
                        + "\n\n"
                    )
                    return
        except Exception:
            dependencies.logger.exception(
                "TTS plan stream failed for thread %s", thread_id
            )
            yield 'event: error\ndata: {"message": "stream failed"}\n\n'

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


async def transcribe_audio(
    *,
    thread_id: str | None,
    user_id: str,
    data: bytes,
    filename: str,
    dependencies: VoiceDependencies,
) -> Response:
    """Transcribe a recorded voice message to text (speech-to-text).

    ``thread_id`` only attributes the usage event; the model and key are the
    user's. ``None`` is a dictation on the landing draft, before any session
    exists.

    ``{"text": "..."}`` on success (``text`` may be ``""`` for silence).
    ``204`` **only** when no STT model is configured (the cockpit attaches the
    audio silently). ``502`` when a configured model fails, so the composer
    shows an honest error rather than a silent no-op. ``400`` for empty audio;
    ``413`` when the clip exceeds Whisper's 25 MB limit.
    """
    if not data:
        raise HTTPException(status_code=400, detail="Empty audio upload")
    if len(data) > STT_MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Audio too large (max 25 MB)")

    try:
        text = await transcribe_service.transcribe_thread_audio(
            audio_bytes=data,
            filename=filename,
            user_id=user_id,
            postgres_db=dependencies.store,
            ledger=dependencies.ledger,
            ref_id=thread_id,
        )
    except transcribe_service.TranscriptionError as exc:
        raise HTTPException(status_code=502, detail="Transcription failed") from exc

    if text is None:
        # 204: no STT model configured. The cockpit treats this as "attach
        # audio only" rather than an error.
        return Response(status_code=204)
    return JSONResponse({"text": text})


async def voice_capabilities(
    *, user_id: str, dependencies: VoiceDependencies
) -> dict[str, bool]:
    """Whether the caller has a usable TTS / STT model configured.

    "Available" means a model *resolves* — the user's ``default_<cap>_model``
    setting or the system default for the capability; a configured-but-broken
    endpoint still reports available and surfaces a real error (``502``) on use,
    which is the honest signal we want.
    """
    settings = await dependencies.store.get_user_settings(user_id) or {}

    async def _has(capability: str, setting_key: str) -> bool:
        if settings.get(setting_key):
            return True
        return bool(await dependencies.store.resolve_default_for_capability(capability))

    return {
        "tts": await _has("tts", "default_tts_model"),
        "stt": await _has("whisper", "default_whisper_model"),
    }
