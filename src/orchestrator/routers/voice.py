"""HTTP adapters for read-aloud (TTS), dictation (STT) and the voice library."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Body, Depends, File, Request, UploadFile
from fastapi.responses import Response, StreamingResponse

from orchestrator.security.auth import require_approved_user
from orchestrator.security.access import require_thread_owner
from orchestrator.services import voice

router = APIRouter()


@dataclass(frozen=True)
class VoiceDependencies:
    store: Any
    operations: voice.VoiceDependencies
    require_approved_user: Callable[..., Awaitable[Any]] = require_approved_user
    require_thread_owner: Callable[..., Awaitable[Any]] = require_thread_owner


def get_voice_dependencies(request: Request) -> VoiceDependencies:
    return request.app.state.voice_dependencies_factory()


# =============================================================================
# Settings: preview, account voices, community library
# =============================================================================


@router.post("/api/settings/tts/preview")
async def preview_tts_voice(
    request: Request,
    body: dict[str, Any] = Body(...),
    *,
    dependencies: VoiceDependencies = Depends(get_voice_dependencies),
) -> Response:
    """Synthesize a short canned phrase in a candidate voice so the settings
    voice picker can preview how it sounds before the user saves it.

    Body:
        ``voice`` (str, optional) — the candidate voice id; empty/omitted means
        "Auto" (resolve like normal read-aloud).
        ``language`` (str, default ``"en"``) — selects the preview phrase and
        the Auto-voice default.
        ``text`` (str, optional) — custom sample text to audition the voice on,
        spoken verbatim; empty/omitted uses the canned phrase. Capped at
        ``_PREVIEW_TEXT_MAX`` chars (``422`` if exceeded).

    Returns JSON ``{"audio": <base64 MP3>}``. ``204`` when no TTS model is
    configured (UI treats as feature-off); ``502`` when a configured model
    fails to synthesize.
    """
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await voice.preview_tts_voice(
        user_id=str(user["id"]), body=body, dependencies=dependencies.operations
    )


@router.get("/api/settings/tts/voices")
async def list_tts_voices(
    request: Request,
    *,
    dependencies: VoiceDependencies = Depends(get_voice_dependencies),
) -> dict[str, Any]:
    """Voices offered by the caller's configured TTS backend, for the Settings
    read-aloud picker.

    ElevenLabs → live account voices (name + accent/gender labels + hosted
    ``preview_url``), fetched server-side and cached ~5 min. Kokoro/OpenAI →
    empty list (the cockpit holds their static catalogs locally). No TTS model
    configured → ``backend: null``. The key never reaches the browser — every
    ElevenLabs call is proxied here.

    Shape: ``{"backend": <str|null>, "voices": [{id, name, labels,
    preview_url}]}``. A listing failure degrades to an empty list (the picker
    falls back to free-text), never a 5xx.
    """
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await voice.list_tts_voices(
        user_id=str(user["id"]), dependencies=dependencies.operations
    )


@router.get("/api/settings/tts/library")
async def search_tts_library(
    request: Request,
    *,
    dependencies: VoiceDependencies = Depends(get_voice_dependencies),
) -> dict[str, Any]:
    """Search the ElevenLabs community Voice Library (read-only, ungated).

    Proxies ``/v1/shared-voices`` server-side (the key never reaches the
    browser), passing through ``search / language / accent / gender / age /
    page``. Returns ``{backend, voices, has_more, error, add_enabled}``;
    ``add_enabled`` mirrors the admin flag so the browser knows whether to offer
    "Add to deployment". Only ElevenLabs returns results; other backends → empty.
    Failures degrade to an empty list with a readable ``error``, never a 5xx.
    """
    user = await dependencies.require_approved_user(request, dependencies.store)
    q = request.query_params
    filters = {
        "search": q.get("search"),
        "language": q.get("language"),
        "accent": q.get("accent"),
        "gender": q.get("gender"),
        "age": q.get("age"),
        "page": q.get("page"),
    }
    return await voice.search_tts_library(
        user_id=str(user["id"]),
        filters=filters,
        dependencies=dependencies.operations,
    )


@router.post("/api/settings/tts/library/add")
async def add_tts_library_voice(
    request: Request,
    body: dict[str, Any] = Body(...),
    *,
    dependencies: VoiceDependencies = Depends(get_voice_dependencies),
) -> dict[str, Any]:
    """Copy a Library voice into the deployment ElevenLabs account.

    Behind the ``tts.elevenlabs_library_enabled`` admin flag (default off)
    because it consumes a plan-limited account voice slot shared by every user.
    On success the account-voice cache is invalidated so the voice appears in the
    Settings picker immediately. ElevenLabs slot-limit / validation errors
    surface as a readable message with their status, not a 500."""
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await voice.add_tts_library_voice(
        user_id=str(user["id"]), body=body, dependencies=dependencies.operations
    )


# =============================================================================
# Thread read-aloud and dictation
# =============================================================================


@router.post("/api/persistent/threads/{thread_id}/tts")
async def synthesize_thread_message_tts(
    thread_id: str,
    request: Request,
    body: dict[str, Any] = Body(...),
    *,
    dependencies: VoiceDependencies = Depends(get_voice_dependencies),
) -> Response:
    """Generate speech audio for a chat message.

    Body:
        ``content`` (str, required) — text to speak (typically an assistant
        message).
        ``reformulate`` (bool, default ``True``) — when true, runs an
        auxiliary LLM pass to rewrite the text for natural narration
        (strips markdown, summarizes code blocks, etc.).
        ``language`` (str, default ``"en"``) — selects the TTS voice.

    Returns:
        JSON ``{"text": <spoken text>, "audio": <base64 MP3>}`` on success —
        ``text`` is the formulation-rewritten version actually read aloud, so
        the UI can surface it. ``204 No Content`` when no TTS model is
        configured (the cockpit treats this as "feature off"). ``502`` when a
        model is configured but synthesis fails — so the button shows an error
        instead of silently doing nothing.
    """
    user, _thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    return await voice.synthesize_thread_message_tts(
        thread_id=thread_id,
        user_id=str(user["id"]),
        body=body,
        dependencies=dependencies.operations,
    )


@router.post("/api/persistent/threads/{thread_id}/tts/plan")
async def plan_thread_message_tts(
    thread_id: str,
    request: Request,
    body: dict[str, Any] = Body(...),
    *,
    dependencies: VoiceDependencies = Depends(get_voice_dependencies),
) -> Response:
    """Plan a (possibly long) message into ordered, speakable chunks.

    The client synthesizes each chunk via ``POST …/tts`` with
    ``reformulate=false`` (chunks are already cleaned) and plays them as a
    progressive playlist — so a long message reads start-to-finish without a
    single multi-minute request and without truncation.

    Body:
        ``content`` (str, required) — the message text to read aloud.
        ``reformulate`` (bool, default true) — when ``false``, skip the auxiliary
        LLM and return the markdown-stripped deterministic split immediately (the
        UI's "read it as-is" bailout).

    Returns:
        JSON ``{"chunks": [str, ...], "rewritten": bool}`` — ``chunks`` has one
        entry for a short message, several (each ≤ 4096 chars, split at natural
        breakpoints, first one kept short) for a long one; ``rewritten`` is
        ``False`` when the auxiliary LLM was unavailable and the raw markdown was
        split deterministically, so the UI can say "rewriting skipped".
        ``204`` when no TTS model is configured. ``502`` only on an unexpected
        planner error (the planner has deterministic fallbacks, so this is rare).
    """
    user, _thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    return await voice.plan_thread_message_tts(
        thread_id=thread_id,
        user_id=str(user["id"]),
        body=body,
        dependencies=dependencies.operations,
    )


@router.post("/api/persistent/threads/{thread_id}/tts/plan/stream")
async def stream_thread_message_tts_plan(
    thread_id: str,
    request: Request,
    body: dict[str, Any] = Body(...),
    *,
    dependencies: VoiceDependencies = Depends(get_voice_dependencies),
) -> StreamingResponse:
    """Streaming (SSE) counterpart of ``/tts/plan``: emit each speakable chunk the
    moment the auxiliary LLM produces it, so the client synthesizes + starts
    playing chunk 1 while the rest still generate. This is the read-aloud path the
    UI prefers — it makes time-to-first-audio ≈ first-chunk latency (~seconds)
    instead of whole-message latency, which is what retired the 30 s planner
    timeout.

    Body: ``content`` (str, required).

    Wire (``text/event-stream``):
        ``event: chunk`` → ``{"index", "text", "rewritten"}`` per ready chunk
        ``event: done``  → ``{"total", "rewritten"}`` (terminal)
        ``event: unavailable`` → ``{}`` when no TTS model is configured (the
            client treats this like ``/tts/plan``'s ``204``)
        ``event: error`` → ``{"message"}`` on an unexpected stream failure
    """
    user, _thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    return await voice.stream_thread_message_tts_plan(
        thread_id=thread_id,
        user_id=str(user["id"]),
        body=body,
        dependencies=dependencies.operations,
    )


@router.post("/api/persistent/threads/{thread_id}/transcribe")
async def transcribe_thread_audio_endpoint(
    thread_id: str,
    request: Request,
    audio: UploadFile = File(...),
    *,
    dependencies: VoiceDependencies = Depends(get_voice_dependencies),
) -> Response:
    """Transcribe a recorded voice message to text (speech-to-text).

    The cockpit composer POSTs the recorded blob here when the user stops
    recording; the returned text is dropped into the message input (editable)
    while the audio is also kept as an attachment. Transcription is server-side
    via the user's configured Whisper model, with auto-detected language.

    Returns:
        ``{"text": "..."}`` on success (``text`` may be ``""`` for silence).
        ``204 No Content`` **only** when no STT model is configured (the cockpit
        attaches the audio silently). ``502`` when a configured model fails, so
        the composer shows an honest error rather than a silent no-op. ``400``
        for empty audio; ``413`` when the clip exceeds 25 MB (Whisper's limit).
    """
    user, _thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )

    data = await audio.read()
    return await voice.transcribe_audio(
        thread_id=thread_id,
        user_id=str(user["id"]),
        data=data,
        filename=audio.filename or "voice.webm",
        dependencies=dependencies.operations,
    )


@router.post("/api/voice/transcribe")
async def transcribe_audio_endpoint(
    request: Request,
    audio: UploadFile = File(...),
    *,
    dependencies: VoiceDependencies = Depends(get_voice_dependencies),
) -> Response:
    """Transcribe a recording made before any session exists.

    The landing draft (New chat) creates its session on first send, so the
    composer has no thread to transcribe through. Nothing here needs one: the
    STT model and key are the user's own. Same body and responses as
    ``POST /api/persistent/threads/{thread_id}/transcribe``; the usage event
    simply carries no thread. A session keeps using its own route.
    """
    user = await dependencies.require_approved_user(request, dependencies.store)

    data = await audio.read()
    return await voice.transcribe_audio(
        thread_id=None,
        user_id=str(user["id"]),
        data=data,
        filename=audio.filename or "voice.webm",
        dependencies=dependencies.operations,
    )


@router.get("/api/voice/capabilities")
async def voice_capabilities(
    request: Request,
    *,
    dependencies: VoiceDependencies = Depends(get_voice_dependencies),
) -> dict:
    """Whether the caller has a usable TTS / STT model configured.

    Lets the cockpit render the read-aloud and mic buttons disabled-with-reason
    up front instead of a dead click that silently answers ``204``
    (the "students said read doesn't work" report). "Available" means a model
    *resolves* — the user's ``default_<cap>_model`` setting or the system
    default for the capability; a configured-but-broken endpoint still reports
    available and surfaces a real error (``502``) on use, which is the honest
    signal we want. Mirrors the resolution the TTS/STT services perform, minus
    the key/endpoint lookup (cheap, and a missing key is a real error, not
    "off").
    """
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await voice.voice_capabilities(
        user_id=str(user["id"]), dependencies=dependencies.operations
    )
