"""Speech-to-text for the persistent-chat composer.

Mirrors ``services/tts.py``: resolves the user's Whisper (STT) model +
credentials via the shared per-user resolver, then calls an OpenAI-compatible
``audio.transcriptions`` endpoint. Language is **auto-detected** (no hint sent)
so mixed-language speakers transcribe correctly.

Returns ``None`` **only** when no STT model is configured (→ the endpoint
answers 204, the "feature off" signal). When a model *is* configured but the
transcription fails (missing key, upstream error, timeout on a long clip), it
raises :class:`TranscriptionError` so the endpoint answers ``502`` — a failure
must read as an honest error, not as "feature off" (Phase 0's broken ≠ off
rule; mirrors ``tts.TtsSynthesisError``). No caching: every recording is unique.
"""

from __future__ import annotations

import io
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from openai import AsyncOpenAI

from shared.speech_transcript import extract_transcript as _extract_transcript

from orchestrator.services.capability_credentials import resolve_capability_credentials
from orchestrator.services.usage_ledger import UsageEvent, UsageLedger

logger = logging.getLogger(__name__)


class TranscriptionError(RuntimeError):
    """Raised when an STT model *is* configured but transcription fails, so the
    endpoint can answer ``502`` instead of the ``204`` "feature off" signal."""


# Long dictations need headroom: a 12-min opus clip is a few MB and can take a
# while to transcribe. Scale the client timeout with the payload size (bounded)
# rather than a flat 60 s that would spuriously kill a long-but-valid clip.
_STT_TIMEOUT_MIN = 120.0
_STT_TIMEOUT_MAX = 600.0
_STT_TIMEOUT_BYTES_PER_SEC = 15000.0


def _stt_timeout(size_bytes: int) -> float:
    return max(
        _STT_TIMEOUT_MIN, min(_STT_TIMEOUT_MAX, size_bytes / _STT_TIMEOUT_BYTES_PER_SEC)
    )


async def transcribe_thread_audio(
    *,
    audio_bytes: bytes,
    filename: str,
    user_id: str,
    postgres_db,
    ledger: Optional[UsageLedger] = None,
    ref_id: Optional[str] = None,
    project_id: Optional[str] = None,
) -> Optional[str]:
    """Transcribe recorded audio to text.

    Returns the transcript (possibly ``""`` for silence) on success, ``None``
    when no STT model is configured (→ 204), and raises
    :class:`TranscriptionError` when a configured model fails (→ 502). Model +
    credential resolution mirrors the TTS path: user setting
    (``default_whisper_model``) > system default for the ``whisper``
    capability, then the model's endpoint/provider key.
    """
    if not audio_bytes:
        return None

    # Both the resolver inputs are drawn from the same pools the dispatcher and
    # TTS use, keyed on the authenticated user.
    user_settings = await postgres_db.get_user_settings(user_id) or {}
    resolved_keys = await postgres_db.resolve_api_keys_for_job(
        user_id=user_id, project_id=None
    )

    creds = await resolve_capability_credentials(
        capability="whisper",
        user_settings=user_settings,
        user_id=user_id,
        resolved_keys=resolved_keys,
        postgres_db=postgres_db,
    )
    if creds is None:
        logger.info("No STT (whisper) model configured for user %s", user_id)
        return None
    model = creds.wire_model
    base_url = creds.base_url
    api_key = creds.api_key

    if not api_key:
        logger.warning(
            "Transcription aborted: no API key (stage=transcribe model=%s base_url=%s)",
            model,
            base_url,
        )
        raise TranscriptionError(f"No API key for STT model {model}")

    # max_retries=0 (fail fast, honest error not a multi-minute backoff) + a
    # size-scaled timeout so long-but-valid clips aren't spuriously killed.
    client = AsyncOpenAI(
        api_key=api_key,
        base_url=base_url,
        timeout=_stt_timeout(len(audio_bytes)),
        max_retries=0,
    )
    try:
        # The OpenAI SDK infers the audio format from the filename extension,
        # so pass a (name, fileobj) tuple carrying the real extension (.webm).
        # Use the SDK's default JSON response format — NOT "text". With "text"
        # the SDK returns the raw HTTP body, and OpenAI-compatible endpoints that
        # answer with a JSON object even for a text request then leak the literal
        # `{"text": "..."}` blob into the transcript.
        file_tuple = (filename or "voice.webm", io.BytesIO(audio_bytes))
        result = await client.audio.transcriptions.create(
            model=model,
            file=file_tuple,
        )
    except Exception as exc:
        logger.exception(
            "Transcription failed (stage=transcribe model=%s base_url=%s bytes=%d)",
            model,
            base_url,
            len(audio_bytes),
        )
        # A configured model that failed is "broken", not "off" — raise so the
        # endpoint answers 502 and the composer surfaces an honest error.
        raise TranscriptionError(f"Transcription failed for model {model}") from exc
    finally:
        await client.close()

    # Usage metering (rate-limiting v2) — non-load-bearing and OUTSIDE the
    # transcribe try, so a ledger hiccup can never drop a good transcript. STT
    # is metered per request (duration isn't known server-side without decoding);
    # the audio size rides in details as a cost proxy.
    if ledger is not None and getattr(ledger, "is_available", False):
        try:
            await ledger.record_events(
                [
                    UsageEvent(
                        category="stt",
                        resource=model,
                        quantity=1,
                        unit="stt-request",
                        source="orchestrator",
                        source_id=uuid.uuid4().hex,
                        ts=datetime.now(timezone.utc),
                        user_id=user_id,
                        project_id=project_id,
                        # No thread on the landing draft: an unreferenced
                        # event, like the TTS voice preview's.
                        ref_kind="thread" if ref_id else None,
                        ref_id=ref_id,
                        details={"bytes": len(audio_bytes)},
                    )
                ]
            )
        except Exception:
            logger.debug("STT usage metering failed (non-fatal)", exc_info=True)

    # Return the transcript verbatim — an empty string (silence) is a valid
    # success, distinct from the ``None`` "no model configured" signal, so it
    # must not collapse to a 204.
    return _extract_transcript(result)
