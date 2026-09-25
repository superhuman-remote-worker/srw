"""Tests for the persistent-chat text-to-speech path.

Covers the TTS service (``orchestrator/services/tts.py``) and the
``POST /api/persistent/threads/{id}/tts`` endpoint wiring in
``orchestrator/routers/voice.py``.

Mirrors ``tests/test_transcribe.py``: the service is tested in isolation by
patching the shared credential resolver and ``AsyncOpenAI``; the endpoint is
exercised by calling the handler directly with injected dependencies (the
orchestrator main app is too heavy for a full TestClient).
"""

from __future__ import annotations

import base64
import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

from orchestrator.services.capability_credentials import CapabilityCredentials  # noqa: E402


def _credentials(model, base_url=None, api_key=None, *, provider=None, params=None):
    return CapabilityCredentials(
        model=model,
        base_url=base_url,
        api_key=api_key,
        provider=provider,
        params=params or {},
    )


@pytest.fixture(autouse=True)
def _clear_tts_caches():
    """The audio/formulation caches are module-level and persist across the
    process — clear them so tests don't bleed cache hits into one another."""
    from orchestrator.services import tts

    tts._audio_cache.clear()
    tts._formulation_cache.clear()
    tts._plan_cache.clear()
    yield


def _voice_route_deps(*, db=None, ledger=None, user=None, thread=None):
    """Route dependencies for the extracted voice router.

    The voice endpoints moved out of ``main`` into
    ``orchestrator.routers.voice`` (R1.B02), so their collaborators arrive
    injected: the tests hand them a store, a ledger and stub gates instead of
    patching module globals. The TTS/STT services themselves are still reached
    by patching ``orchestrator.services.tts`` / ``.transcribe``, exactly as
    before.
    """
    from orchestrator.routers.voice import VoiceDependencies as _RouteDeps
    from orchestrator.services.voice import VoiceDependencies as _OpDeps

    store = db if db is not None else MagicMock()
    resolved_user = user or {"id": "u1"}
    resolved_thread = thread or {"id": "t1"}

    async def _approved(_request, _store):
        return resolved_user

    async def _owner(_request, _store, _thread_id):
        return resolved_user, resolved_thread

    return _RouteDeps(
        store=store,
        operations=_OpDeps(store=store, logger=MagicMock(), ledger=ledger),
        require_approved_user=_approved,
        require_thread_owner=_owner,
    )


def _mock_db() -> MagicMock:
    """A postgres_db double whose pool-reading methods are awaitable no-ops."""
    db = MagicMock()
    db.get_user_settings = AsyncMock(return_value={})
    db.resolve_api_keys_for_job = AsyncMock(return_value={})
    db.resolve_catalog_model = AsyncMock(return_value=None)  # no per-model voice
    return db


def _mock_openai(*, speech=b"MP3", speech_error=None, chat_text="reworded"):
    """Build a (class, client) pair for the TTS paths.

    ``speech`` is returned as ``audio.speech.create(...).content``; pass
    ``speech_error`` to raise instead. ``chat_text`` backs the formulation
    ``chat.completions.create`` call.
    """
    client = MagicMock()
    if speech_error is not None:
        client.audio.speech.create = AsyncMock(side_effect=speech_error)
    else:
        client.audio.speech.create = AsyncMock(return_value=MagicMock(content=speech))
    msg = MagicMock()
    msg.content = chat_text
    client.chat.completions.create = AsyncMock(
        return_value=MagicMock(choices=[MagicMock(message=msg)])
    )
    client.close = AsyncMock()
    cls = MagicMock(return_value=client)
    return cls, client


def _mock_openai_chat(content: str, *, finish_reason: str = "stop"):
    """(class, client) whose chat.completions.create returns ``content`` with
    the given finish_reason — for the chunk-planner tests."""
    msg = MagicMock()
    msg.content = content
    choice = MagicMock(message=msg, finish_reason=finish_reason)
    client = MagicMock()
    client.chat.completions.create = AsyncMock(return_value=MagicMock(choices=[choice]))
    client.close = AsyncMock()
    cls = MagicMock(return_value=client)
    return cls, client


def _caps(tts=("kokoro-strix", None, "sk-key"), aux=("gemma-aux", None, "sk-key")):
    """AsyncMock for _resolve_capability_credentials keyed on capability."""

    def _resolve(*, capability, **_):
        value = {"tts": tts, "auxiliary": aux}.get(capability)
        return _credentials(*value) if isinstance(value, tuple) else value

    return AsyncMock(side_effect=_resolve)


# ---------------------------------------------------------------------------
# Service: generate_message_tts
# ---------------------------------------------------------------------------


class TestGenerateMessageTts:
    @pytest.mark.asyncio
    async def test_returns_text_and_audio_on_success(self):
        from orchestrator.services.tts import generate_message_tts

        cls, client = _mock_openai(speech=b"AUDIOBYTES")
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(return_value=_credentials("tts-1", None, "sk-key")),
            ),
        ):
            result = await generate_message_tts(
                content="short clean text",
                language="en",
                reformulate=False,
                user_id="u1",
                postgres_db=_mock_db(),
            )
        assert result == ("short clean text", b"AUDIOBYTES")
        client.audio.speech.create.assert_awaited_once()
        client.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_none_when_no_model_configured(self):
        from orchestrator.services.tts import generate_message_tts

        cls, _ = _mock_openai()
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(return_value=None),
            ),
        ):
            result = await generate_message_tts(
                content="hi there",
                language="en",
                reformulate=False,
                user_id="u1",
                postgres_db=_mock_db(),
            )
        assert result is None
        cls.assert_not_called()  # no client built when TTS is unconfigured

    @pytest.mark.asyncio
    async def test_none_on_empty_content(self):
        from orchestrator.services.tts import generate_message_tts

        result = await generate_message_tts(
            content="   ",
            language="en",
            reformulate=False,
            user_id="u1",
            postgres_db=_mock_db(),
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_raises_when_synthesis_fails(self):
        """A configured model whose synthesis call errors must raise (→ 502),
        not silently return None (→ 204) — the whole point of the fix."""
        from orchestrator.services.tts import TtsSynthesisError, generate_message_tts

        cls, client = _mock_openai(speech_error=RuntimeError("upstream 503"))
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(return_value=_credentials("tts-1", None, "sk-key")),
            ),
        ):
            with pytest.raises(TtsSynthesisError):
                await generate_message_tts(
                    content="some text to read",
                    language="en",
                    reformulate=False,
                    user_id="u1",
                    postgres_db=_mock_db(),
                )
        client.close.assert_awaited_once()  # cleaned up even on failure

    @pytest.mark.asyncio
    async def test_raises_when_no_api_key(self):
        """A configured model with no resolvable key is a real misconfig — it
        must raise (→ 502), not look like 'not configured' (→ 204)."""
        from orchestrator.services.tts import TtsSynthesisError, generate_message_tts

        cls, client = _mock_openai(speech=b"X")
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(return_value=_credentials("tts-1", None, None)),  # no api_key
            ),
        ):
            with pytest.raises(TtsSynthesisError):
                await generate_message_tts(
                    content="text to speak",
                    language="en",
                    reformulate=False,
                    user_id="u1",
                    postgres_db=_mock_db(),
                )
        client.audio.speech.create.assert_not_awaited()  # never tried without a key

    @pytest.mark.asyncio
    async def test_reformulation_rewrites_spoken_text(self):
        """With reformulate=True and markdown present, the returned spoken text
        is the auxiliary-LLM rewrite, not the raw markdown."""
        from orchestrator.services.tts import generate_message_tts

        cls, client = _mock_openai(speech=b"AUDIO", chat_text="plain spoken prose")
        markdown = "Here is a table:\n\n| a | b |\n| - | - |\n| 1 | 2 |\n\nThat's all."
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(return_value=_credentials("tts-1", None, "sk-key")),
            ),
        ):
            result = await generate_message_tts(
                content=markdown,
                language="en",
                reformulate=True,
                user_id="u1",
                postgres_db=_mock_db(),
            )
        assert result is not None
        spoken, audio = result
        assert spoken == "plain spoken prose"
        assert audio == b"AUDIO"
        client.chat.completions.create.assert_awaited_once()  # formulation ran

    @pytest.mark.asyncio
    async def test_uses_voice_from_params_json(self):
        """A TTS catalog model with params_json.voice uses that voice."""
        from orchestrator.services.tts import generate_message_tts

        cls, client = _mock_openai(speech=b"AUDIO")
        db = _mock_db()
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(
                    return_value=_credentials(
                        "kokoro-strix",
                        None,
                        "sk-key",
                        params={"voice": "af_heart"},
                    )
                ),
            ),
        ):
            result = await generate_message_tts(
                content="short clean text",
                language="en",
                reformulate=False,
                user_id="u1",
                postgres_db=db,
            )
        assert result is not None
        assert client.audio.speech.create.call_args.kwargs["voice"] == "af_heart"
        db.resolve_catalog_model.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_falls_back_to_default_voice_without_params(self):
        """No params_json voice → the per-language default (alloy for en)."""
        from orchestrator.services.tts import generate_message_tts

        cls, client = _mock_openai(speech=b"AUDIO")
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(return_value=_credentials("kokoro-strix", None, "sk-key")),
            ),
        ):
            await generate_message_tts(
                content="short clean text",
                language="en",
                reformulate=False,
                user_id="u1",
                postgres_db=_mock_db(),  # resolve_catalog_model → None
            )
        assert client.audio.speech.create.call_args.kwargs["voice"] == "alloy"


class TestSynthesizeVoicePreview:
    """The settings voice-picker preview: canned phrase, candidate voice,
    no aux rewrite."""

    @pytest.mark.asyncio
    async def test_uses_candidate_voice_and_returns_audio(self):
        from orchestrator.services.tts import synthesize_voice_preview

        cls, client = _mock_openai(speech=b"PREVIEW")
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(return_value=_credentials("kokoro-strix", None, "sk-key")),
            ),
        ):
            audio = await synthesize_voice_preview(
                voice="af_nova",
                language="en",
                user_id="u1",
                postgres_db=_mock_db(),
            )
        assert audio == b"PREVIEW"
        assert client.audio.speech.create.call_args.kwargs["voice"] == "af_nova"
        # A canned phrase never triggers the aux rewrite LLM.
        client.chat.completions.create.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_auto_voice_resolves_language_default(self):
        from orchestrator.services.tts import synthesize_voice_preview

        cls, client = _mock_openai(speech=b"PREVIEW")
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(return_value=_credentials("kokoro-strix", None, "sk-key")),
            ),
        ):
            await synthesize_voice_preview(
                voice="",  # Auto
                language="en",
                user_id="u1",
                postgres_db=_mock_db(),
            )
        assert client.audio.speech.create.call_args.kwargs["voice"] == "alloy"

    @pytest.mark.asyncio
    async def test_german_language_speaks_german_phrase(self):
        from orchestrator.services.tts import _PREVIEW_TEXT, synthesize_voice_preview

        cls, client = _mock_openai(speech=b"PREVIEW")
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(return_value=_credentials("kokoro-strix", None, "sk-key")),
            ),
        ):
            await synthesize_voice_preview(
                voice="af_nova",
                language="de-DE",
                user_id="u1",
                postgres_db=_mock_db(),
            )
        assert (
            client.audio.speech.create.call_args.kwargs["input"] == _PREVIEW_TEXT["de"]
        )

    @pytest.mark.asyncio
    async def test_custom_text_spoken_verbatim(self):
        from orchestrator.services.tts import synthesize_voice_preview

        cls, client = _mock_openai(speech=b"PREVIEW")
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(return_value=_credentials("kokoro-strix", None, "sk-key")),
            ),
        ):
            await synthesize_voice_preview(
                voice="af_nova",
                language="en",
                user_id="u1",
                postgres_db=_mock_db(),
                text="  Guten Tag, mein Name ist Klaus.  ",
            )
        # Spoken verbatim (stripped), overriding the canned phrase; still no
        # aux rewrite.
        assert (
            client.audio.speech.create.call_args.kwargs["input"]
            == "Guten Tag, mein Name ist Klaus."
        )
        client.chat.completions.create.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_custom_text_clamped_to_max(self):
        from orchestrator.services.tts import (
            _PREVIEW_TEXT_MAX,
            synthesize_voice_preview,
        )

        cls, client = _mock_openai(speech=b"PREVIEW")
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(return_value=_credentials("kokoro-strix", None, "sk-key")),
            ),
        ):
            await synthesize_voice_preview(
                voice="af_nova",
                language="en",
                user_id="u1",
                postgres_db=_mock_db(),
                text="x" * (_PREVIEW_TEXT_MAX + 100),
            )
        # The service clamps defensively even though the endpoint 422s first.
        assert (
            len(client.audio.speech.create.call_args.kwargs["input"])
            == _PREVIEW_TEXT_MAX
        )

    @pytest.mark.asyncio
    async def test_returns_none_when_no_tts_model(self):
        from orchestrator.services.tts import synthesize_voice_preview

        with patch(
            "orchestrator.services.tts._resolve_capability_credentials",
            AsyncMock(return_value=None),
        ):
            audio = await synthesize_voice_preview(
                voice="af_nova",
                language="en",
                user_id="u1",
                postgres_db=_mock_db(),
            )
        assert audio is None

    @pytest.mark.asyncio
    async def test_raises_on_synthesis_failure(self):
        from orchestrator.services.tts import (
            TtsSynthesisError,
            synthesize_voice_preview,
        )

        cls, _ = _mock_openai(speech_error=RuntimeError("boom"))
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(return_value=_credentials("kokoro-strix", None, "sk-key")),
            ),
        ):
            with pytest.raises(TtsSynthesisError):
                await synthesize_voice_preview(
                    voice="af_nova",
                    language="en",
                    user_id="u1",
                    postgres_db=_mock_db(),
                )


# ---------------------------------------------------------------------------
# Phase 3: content-language voice selection, user voice choice, instructions
# ---------------------------------------------------------------------------


class TestVoiceSelectionAndInstructions:
    def test_detect_language_english(self):
        from orchestrator.services.tts import _detect_language

        assert _detect_language("This is a plain English sentence about a cat.") == "en"

    def test_detect_language_german_umlaut(self):
        from orchestrator.services.tts import _detect_language

        assert _detect_language("Größe und Qualität sind wichtig.") == "de"

    def test_detect_language_german_function_words(self):
        from orchestrator.services.tts import _detect_language

        assert (
            _detect_language("Der Test ist nicht fertig und das ist ein Problem.")
            == "de"
        )

    @pytest.mark.asyncio
    async def test_voice_follows_content_language_not_request_hint(self):
        """A German message uses the German default voice even when the request's
        language hint says 'en' (voice follows content, fixing defect 5)."""
        from orchestrator.services.tts import DEFAULT_VOICE_DE, generate_message_tts

        cls, client = _mock_openai(speech=b"AUDIO")
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(return_value=_credentials("gpt-4o-mini-tts", None, "sk-key")),
            ),
        ):
            await generate_message_tts(
                content="Das ist ein deutscher Satz mit Umlauten: schön und gut.",
                language="en",
                reformulate=False,
                user_id="u1",
                postgres_db=_mock_db(),
            )
        assert client.audio.speech.create.call_args.kwargs["voice"] == DEFAULT_VOICE_DE

    @pytest.mark.asyncio
    async def test_user_default_voice_overrides_catalog(self):
        from orchestrator.services.tts import generate_message_tts

        cls, client = _mock_openai(speech=b"AUDIO")
        db = _mock_db()
        db.get_user_settings = AsyncMock(return_value={"default_tts_voice": "shimmer"})
        db.resolve_catalog_model = AsyncMock(
            return_value={"params_json": {"voice": "af_heart"}}
        )
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(return_value=_credentials("kokoro", None, "sk-key")),
            ),
        ):
            await generate_message_tts(
                content="short clean text",
                language="en",
                reformulate=False,
                user_id="u1",
                postgres_db=db,
            )
        assert client.audio.speech.create.call_args.kwargs["voice"] == "shimmer"

    @pytest.mark.asyncio
    async def test_per_language_voice_map(self):
        from orchestrator.services.tts import generate_message_tts

        cls, client = _mock_openai(speech=b"AUDIO")
        db = _mock_db()
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(
                    return_value=_credentials(
                        "gpt-4o-mini-tts",
                        None,
                        "sk-key",
                        params={"voices": {"en": "alloy", "de": "onyx"}},
                    )
                ),
            ),
        ):
            await generate_message_tts(
                content="Der Satz ist auf Deutsch und schön.",
                language="en",
                reformulate=False,
                user_id="u1",
                postgres_db=db,
            )
        assert client.audio.speech.create.call_args.kwargs["voice"] == "onyx"
        db.resolve_catalog_model.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_instructions_passed_through_when_set(self):
        from orchestrator.services.tts import generate_message_tts

        cls, client = _mock_openai(speech=b"AUDIO")
        db = _mock_db()
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(
                    return_value=_credentials(
                        "gpt-4o-mini-tts",
                        None,
                        "sk-key",
                        params={
                            "voice": "sage",
                            "instructions": "warm and unhurried",
                        },
                    )
                ),
            ),
        ):
            await generate_message_tts(
                content="short clean text",
                language="en",
                reformulate=False,
                user_id="u1",
                postgres_db=db,
            )
        kwargs = client.audio.speech.create.call_args.kwargs
        assert kwargs["voice"] == "sage"
        assert kwargs["instructions"] == "warm and unhurried"
        db.resolve_catalog_model.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_instructions_kwarg_when_unset(self):
        """tts-1 / Kokoro reject an `instructions` param, so it must be omitted
        entirely (not sent as None) when no catalog instructions are set."""
        from orchestrator.services.tts import generate_message_tts

        cls, client = _mock_openai(speech=b"AUDIO")
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(return_value=_credentials("tts-1", None, "sk-key")),
            ),
        ):
            await generate_message_tts(
                content="short clean text",
                language="en",
                reformulate=False,
                user_id="u1",
                postgres_db=_mock_db(),
            )
        assert "instructions" not in client.audio.speech.create.call_args.kwargs


# ---------------------------------------------------------------------------
# Endpoint: POST /api/persistent/threads/{thread_id}/tts
# ---------------------------------------------------------------------------


class TestTtsEndpoint:
    def test_route_is_registered(self):
        from orchestrator.main import app
        from tests._route_inventory import mounted_routes

        routes = mounted_routes(app)
        assert ("POST", "/api/persistent/threads/{thread_id}/tts") in routes

    @pytest.mark.asyncio
    async def test_returns_json_text_and_audio(self):
        from orchestrator.routers import voice as voice_routes

        with patch(
            "orchestrator.services.tts.generate_message_tts",
            AsyncMock(return_value=("spoken words", b"\x00\x01\x02")),
        ):
            resp = await voice_routes.synthesize_thread_message_tts(
                thread_id="t1",
                request=MagicMock(),
                body={"content": "hello"},
                dependencies=_voice_route_deps(),
            )
        assert resp.status_code == 200
        payload = json.loads(resp.body)
        assert payload["text"] == "spoken words"
        assert base64.b64decode(payload["audio"]) == b"\x00\x01\x02"

    @pytest.mark.asyncio
    async def test_204_when_not_configured(self):
        from orchestrator.routers import voice as voice_routes

        with patch(
            "orchestrator.services.tts.generate_message_tts",
            AsyncMock(return_value=None),
        ):
            resp = await voice_routes.synthesize_thread_message_tts(
                thread_id="t1",
                request=MagicMock(),
                body={"content": "hello"},
                dependencies=_voice_route_deps(),
            )
        assert resp.status_code == 204

    @pytest.mark.asyncio
    async def test_502_on_synthesis_failure(self):
        from fastapi import HTTPException

        from orchestrator.routers import voice as voice_routes
        from orchestrator.services.tts import TtsSynthesisError

        with patch(
            "orchestrator.services.tts.generate_message_tts",
            AsyncMock(side_effect=TtsSynthesisError("down")),
        ):
            with pytest.raises(HTTPException) as exc:
                await voice_routes.synthesize_thread_message_tts(
                    thread_id="t1",
                    request=MagicMock(),
                    body={"content": "hello"},
                    dependencies=_voice_route_deps(),
                )
        assert exc.value.status_code == 502

    @pytest.mark.asyncio
    async def test_400_on_empty_content(self):
        from fastapi import HTTPException

        from orchestrator.routers import voice as voice_routes

        with pytest.raises(HTTPException) as exc:
            await voice_routes.synthesize_thread_message_tts(
                thread_id="t1",
                request=MagicMock(),
                body={"content": "   "},
                dependencies=_voice_route_deps(),
            )
        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_thread_ownership_is_checked_before_synthesis(self):
        """The gate runs first: a refused owner never reaches the TTS service."""
        from fastapi import HTTPException

        from orchestrator.routers import voice as voice_routes
        from orchestrator.routers.voice import VoiceDependencies
        from orchestrator.services.voice import VoiceDependencies as OpDeps

        async def deny(_request, _store, _thread_id):
            raise HTTPException(status_code=403, detail="Not your session")

        generate = AsyncMock()
        deps = VoiceDependencies(
            store=MagicMock(),
            operations=OpDeps(store=MagicMock(), logger=MagicMock()),
            require_thread_owner=deny,
        )
        with patch("orchestrator.services.tts.generate_message_tts", generate):
            with pytest.raises(HTTPException) as exc:
                await voice_routes.synthesize_thread_message_tts(
                    thread_id="t1",
                    request=MagicMock(),
                    body={"content": "hello"},
                    dependencies=deps,
                )
        assert exc.value.status_code == 403
        generate.assert_not_awaited()


# ---------------------------------------------------------------------------
# Chunk planning: splitter / gate / parser (pure) + plan_tts_chunks
# ---------------------------------------------------------------------------


class TestChunkSplitting:
    def test_short_text_is_one_chunk(self):
        from orchestrator.services.tts import _split_text_into_chunks

        assert _split_text_into_chunks("Just a short line.") == ["Just a short line."]

    def test_long_text_splits_under_limit(self):
        from orchestrator.services.tts import TTS_CHUNK_LIMIT, _split_text_into_chunks

        text = "\n\n".join(f"Paragraph number {i}. " * 30 for i in range(40))
        chunks = _split_text_into_chunks(text)
        assert len(chunks) > 1
        assert all(len(c) <= TTS_CHUNK_LIMIT for c in chunks)

    def test_enforce_limit_resplits_oversized(self):
        from orchestrator.services.tts import TTS_CHUNK_LIMIT, _enforce_chunk_limit

        out = _enforce_chunk_limit(["word " * 2000])  # ~10k, no breaks
        assert len(out) > 1
        assert all(len(c) <= TTS_CHUNK_LIMIT for c in out)

    def test_parse_plain_array(self):
        from orchestrator.services.tts import _parse_chunk_array

        assert _parse_chunk_array('["a", "b"]') == ["a", "b"]

    def test_parse_fenced_array(self):
        from orchestrator.services.tts import _parse_chunk_array

        assert _parse_chunk_array('```json\n["a", "b"]\n```') == ["a", "b"]

    def test_parse_array_with_prose(self):
        from orchestrator.services.tts import _parse_chunk_array

        assert _parse_chunk_array('Sure! ["a", "b"] hope that helps') == ["a", "b"]

    def test_parse_non_array_is_none(self):
        from orchestrator.services.tts import _parse_chunk_array

        assert _parse_chunk_array("I cannot do that") is None
        assert _parse_chunk_array("") is None


class TestAuxReasoningControl:
    """The rewrite/chunk stages force the aux model's thinking OFF, but only for
    families with a binary thinking toggle. Otherwise TTS pays a full reasoning
    pass before the first token (the dominant time-to-first-audio cost), and a
    non-vLLM endpoint would 400 on an unsupported ``chat_template_kwargs``."""

    def test_toggle_family_disables_thinking(self):
        from orchestrator.services.tts import _aux_reasoning_off_body

        # gemma is a hybrid-thinking family (chat_template_kwargs.enable_thinking).
        assert _aux_reasoning_off_body("gemma-4-moe") == {
            "chat_template_kwargs": {"enable_thinking": False}
        }

    def test_non_toggle_families_send_nothing(self):
        from orchestrator.services.tts import _aux_reasoning_off_body

        # No binary toggle → empty body, so we never send chat_template_kwargs to
        # a plain-OpenAI / effort-enum / no-reasoning endpoint that would reject it.
        for model in (
            "gpt-4o-mini",
            "gpt-4o-mini-tts",
            "MiniMax-M2",
            "gpt-oss-120b",
            "",
        ):
            assert _aux_reasoning_off_body(model) == {}, model

    @pytest.mark.asyncio
    async def test_plan_passes_thinking_off_for_gemma_aux(self):
        from orchestrator.services.tts import plan_tts_chunks

        cls, client = _mock_openai_chat('["First chunk.", "Second chunk."]')
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials", _caps()
            ),  # aux=gemma
        ):
            await plan_tts_chunks(
                content="some markdown **message**",
                user_id="u1",
                postgres_db=_mock_db(),
            )
        kwargs = client.chat.completions.create.call_args.kwargs
        assert kwargs["extra_body"] == {
            "chat_template_kwargs": {"enable_thinking": False}
        }

    @pytest.mark.asyncio
    async def test_plan_sends_no_toggle_for_openai_aux(self):
        from orchestrator.services.tts import plan_tts_chunks

        cls, client = _mock_openai_chat('["First chunk.", "Second chunk."]')
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                _caps(aux=("gpt-4o-mini", None, "sk-key")),
            ),
        ):
            await plan_tts_chunks(
                content="some markdown **message**",
                user_id="u1",
                postgres_db=_mock_db(),
            )
        kwargs = client.chat.completions.create.call_args.kwargs
        assert kwargs["extra_body"] == {}


class TestReadAloudRewritePrefs:
    """User read-aloud prefs: a reasoning-level knob (off by default) + a custom
    rewrite prompt that can override the default 'don't summarize' rule."""

    # ── reasoning level → aux extra_body ──────────────────────────────
    def test_toggle_family_off_disables_thinking(self):
        from orchestrator.services.tts import _aux_reasoning_body

        for level in (None, "off", "none"):
            assert _aux_reasoning_body("gemma-4-moe", level) == {
                "chat_template_kwargs": {"enable_thinking": False}
            }, level

    def test_toggle_family_level_enables_thinking(self):
        from orchestrator.services.tts import _aux_reasoning_body

        # gemma has no low/medium/high — any requested level just flips it ON.
        for level in ("low", "medium", "high"):
            assert _aux_reasoning_body("gemma-4-moe", level) == {
                "chat_template_kwargs": {"enable_thinking": True}
            }, level

    def test_effort_enum_family_maps_level_to_effort(self):
        from orchestrator.services.tts import _aux_reasoning_body

        fam = MagicMock()
        fam.family = "synthfam"
        matrix = {
            "synthfam": {
                "reasoning": {
                    "method": "effort_enum",
                    "delivery": "native",
                    "options": ["none", "low", "medium", "high"],
                }
            }
        }
        with (
            patch("orchestrator.services.tts._model_config_matrix", lambda: matrix),
            patch("orchestrator.services.tts.detect_family", lambda m: fam),
        ):
            assert _aux_reasoning_body("x", "low") == {"reasoning_effort": "low"}
            assert _aux_reasoning_body("x", "high") == {"reasoning_effort": "high"}
            # off → the family's explicit low-reasoning option ('none').
            assert _aux_reasoning_body("x", "off") == {"reasoning_effort": "none"}

    def test_effort_enum_without_off_option_inherits_default(self):
        """A [low,medium,high]-only family has no real 'off' — the off default must
        inject nothing (inherit the endpoint default), never an unsupported value."""
        from orchestrator.services.tts import _aux_reasoning_body

        fam = MagicMock()
        fam.family = "synthfam"
        matrix = {
            "synthfam": {
                "reasoning": {
                    "method": "effort_enum",
                    "delivery": "native",
                    "options": ["low", "medium", "high"],
                }
            }
        }
        with (
            patch("orchestrator.services.tts._model_config_matrix", lambda: matrix),
            patch("orchestrator.services.tts.detect_family", lambda m: fam),
        ):
            assert _aux_reasoning_body("x", "off") == {}
            assert _aux_reasoning_body("x", "medium") == {"reasoning_effort": "medium"}

    # ── custom prompt injection ───────────────────────────────────────
    def test_augment_prompt_noop_when_empty(self):
        from orchestrator.services.tts import (
            FORMULATION_SYSTEM_PROMPT,
            _augment_rewrite_prompt,
        )

        assert (
            _augment_rewrite_prompt(FORMULATION_SYSTEM_PROMPT, None)
            == FORMULATION_SYSTEM_PROMPT
        )
        assert (
            _augment_rewrite_prompt(FORMULATION_SYSTEM_PROMPT, "   ")
            == FORMULATION_SYSTEM_PROMPT
        )

    def test_augment_prompt_appends_pref_with_override_and_floor(self):
        from orchestrator.services.tts import _augment_rewrite_prompt

        out = _augment_rewrite_prompt("BASE RULES", "Give me a TLDR, skip tables")
        assert "BASE RULES" in out
        assert "Give me a TLDR, skip tables" in out
        # The user's preference must be granted authority to override 'don't
        # summarize'…
        assert "PREFERENCES WIN" in out
        # …but the no-fabrication / no-altered-figures floor must remain.
        assert "never invent facts" in out.lower()
        assert "never change numbers" in out.lower()

    # ── prefs extraction + cache-variant key ──────────────────────────
    def test_read_aloud_prefs_defaults_and_parse(self):
        from orchestrator.services.tts import _read_aloud_prefs

        assert _read_aloud_prefs({}) == (None, "off")
        assert _read_aloud_prefs({"read_aloud": {}}) == (None, "off")
        assert _read_aloud_prefs(
            {
                "read_aloud": {
                    "reasoning_level": "HIGH",
                    "custom_prompt": " skip tables ",
                }
            }
        ) == ("skip tables", "high")
        # Unknown level degrades to off (defense-in-depth alongside the API validator).
        assert _read_aloud_prefs({"read_aloud": {"reasoning_level": "bogus"}}) == (
            None,
            "off",
        )

    def test_variant_key_differs_by_prompt_and_level(self):
        from orchestrator.services.tts import _rewrite_variant_key

        base = _rewrite_variant_key(None, "off")
        by_prompt = _rewrite_variant_key("skip tables", "off")
        by_level = _rewrite_variant_key(None, "high")
        assert len({base, by_prompt, by_level}) == 3
        # Stable for the same inputs (so the cache actually hits when unchanged).
        assert _rewrite_variant_key("skip tables", "off") == by_prompt

    # ── end-to-end wiring through plan_tts_chunks ─────────────────────
    @pytest.mark.asyncio
    async def test_plan_applies_custom_prompt_and_reasoning(self):
        from orchestrator.services.tts import plan_tts_chunks

        cls, client = _mock_openai_chat('["A."]')
        db = _mock_db()
        db.get_user_settings = AsyncMock(
            return_value={
                "read_aloud": {
                    "reasoning_level": "high",
                    "custom_prompt": "Give me a TLDR",
                }
            }
        )
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials", _caps()
            ),  # aux=gemma
        ):
            await plan_tts_chunks(
                content="some **markdown** message", user_id="u1", postgres_db=db
            )
        kwargs = client.chat.completions.create.call_args.kwargs
        # Reasoning turned ON for gemma because a level was requested.
        assert kwargs["extra_body"] == {
            "chat_template_kwargs": {"enable_thinking": True}
        }
        # Custom instructions injected into the rewrite system prompt.
        sys_msg = kwargs["messages"][0]["content"]
        assert "Give me a TLDR" in sys_msg
        assert "PREFERENCES WIN" in sys_msg

    @pytest.mark.asyncio
    async def test_different_custom_prompt_busts_plan_cache(self):
        """Two different custom prompts over the SAME content must both hit the aux
        — the rewrite variant is part of the cache key, so no stale replay."""
        from orchestrator.services import tts
        from orchestrator.services.tts import plan_tts_chunks

        tts._plan_cache.clear()
        cls, client = _mock_openai_chat('["A."]')

        def _db_with(prompt):
            db = _mock_db()
            db.get_user_settings = AsyncMock(
                return_value={"read_aloud": {"custom_prompt": prompt}}
            )
            return db

        try:
            with (
                patch("orchestrator.services.tts.AsyncOpenAI", cls),
                patch(
                    "orchestrator.services.tts._resolve_capability_credentials", _caps()
                ),
            ):
                await plan_tts_chunks(
                    content="same content here",
                    user_id="u1",
                    postgres_db=_db_with("skip tables"),
                )
                await plan_tts_chunks(
                    content="same content here",
                    user_id="u1",
                    postgres_db=_db_with("give me a tldr"),
                )
            assert client.chat.completions.create.await_count == 2
        finally:
            tts._plan_cache.clear()


class TestReadAloudSettingsValidation:
    """The PATCH /api/settings/preferences body validator guards the read_aloud
    sub-object (level enum + prompt length) — 422 before it reaches the DB."""

    def test_valid_read_aloud_accepted_and_normalized(self):
        from orchestrator.routers.preferences import UserSettingsUpdate

        m = UserSettingsUpdate(
            read_aloud={"reasoning_level": "HIGH", "custom_prompt": "skip tables"}
        )
        assert m.read_aloud["reasoning_level"] == "high"  # lowercased

    def test_bad_level_rejected(self):
        from orchestrator.routers.preferences import UserSettingsUpdate

        with pytest.raises(Exception):
            UserSettingsUpdate(read_aloud={"reasoning_level": "ultra"})

    def test_overlong_prompt_rejected(self):
        from orchestrator.routers.preferences import UserSettingsUpdate

        with pytest.raises(Exception):
            UserSettingsUpdate(read_aloud={"custom_prompt": "z" * 1001})

    def test_prompt_at_cap_accepted(self):
        from orchestrator.routers.preferences import UserSettingsUpdate

        UserSettingsUpdate(read_aloud={"custom_prompt": "z" * 1000})


class TestPlanTtsChunks:
    @pytest.mark.asyncio
    async def test_none_when_no_tts_model(self):
        from orchestrator.services.tts import plan_tts_chunks

        with patch(
            "orchestrator.services.tts._resolve_capability_credentials", _caps(tts=None)
        ):
            result = await plan_tts_chunks(
                content="anything", user_id="u1", postgres_db=_mock_db()
            )
        assert result is None

    @pytest.mark.asyncio
    async def test_uses_llm_chunks(self):
        from orchestrator.services.tts import plan_tts_chunks

        cls, _ = _mock_openai_chat('["First chunk.", "Second chunk."]')
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch("orchestrator.services.tts._resolve_capability_credentials", _caps()),
        ):
            result = await plan_tts_chunks(
                content="some markdown **message**",
                user_id="u1",
                postgres_db=_mock_db(),
            )
        assert result["chunks"] == ["First chunk.", "Second chunk."]
        assert result["rewritten"] is True

    @pytest.mark.asyncio
    async def test_falls_back_to_deterministic_without_aux(self):
        from orchestrator.services.tts import TTS_CHUNK_LIMIT, plan_tts_chunks

        long_text = "\n\n".join(f"Paragraph {i}. " * 40 for i in range(40))
        with patch(
            "orchestrator.services.tts._resolve_capability_credentials", _caps(aux=None)
        ):
            result = await plan_tts_chunks(
                content=long_text, user_id="u1", postgres_db=_mock_db()
            )
        assert result is not None and len(result["chunks"]) > 1
        assert all(len(c) <= TTS_CHUNK_LIMIT for c in result["chunks"])
        # No aux model ran → deterministic split of raw markdown.
        assert result["rewritten"] is False

    @pytest.mark.asyncio
    async def test_falls_back_when_llm_unparseable(self):
        from orchestrator.services.tts import plan_tts_chunks

        cls, _ = _mock_openai_chat("Sorry, I can't help with that")
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch("orchestrator.services.tts._resolve_capability_credentials", _caps()),
        ):
            result = await plan_tts_chunks(
                content="short message", user_id="u1", postgres_db=_mock_db()
            )
        assert result["chunks"] == ["short message"]
        # LLM ran but couldn't deliver → fell back → not a real rewrite.
        assert result["rewritten"] is False

    @pytest.mark.asyncio
    async def test_truncated_llm_output_falls_back(self):
        from orchestrator.services.tts import plan_tts_chunks

        cls, _ = _mock_openai_chat('["partial', finish_reason="length")
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch("orchestrator.services.tts._resolve_capability_credentials", _caps()),
        ):
            result = await plan_tts_chunks(
                content="short message", user_id="u1", postgres_db=_mock_db()
            )
        assert result["chunks"] == ["short message"]
        assert result["rewritten"] is False

    @pytest.mark.asyncio
    async def test_first_chunk_is_shortened_for_fast_ttfa(self):
        from orchestrator.services.tts import TTS_FIRST_CHUNK_TARGET, plan_tts_chunks

        # A single long paragraph of many sentences the LLM returns as one chunk;
        # the planner must split off a short first chunk for fast time-to-first-audio.
        one_big = " ".join(f"This is sentence number {i}." for i in range(80))
        cls, _ = _mock_openai_chat(json.dumps([one_big]))
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch("orchestrator.services.tts._resolve_capability_credentials", _caps()),
        ):
            result = await plan_tts_chunks(
                content=one_big, user_id="u1", postgres_db=_mock_db()
            )
        assert len(result["chunks"]) >= 2
        assert len(result["chunks"][0]) <= TTS_FIRST_CHUNK_TARGET
        # Split at a sentence boundary — no mid-sentence cut.
        assert result["chunks"][0].endswith(".")


# ---------------------------------------------------------------------------
# Endpoint: POST /api/persistent/threads/{thread_id}/tts/plan
# ---------------------------------------------------------------------------


class TestTtsSynthesisHttpError:
    """The endpoints map a synthesis code → HTTP status. Critically, "auth" must
    NOT become 401 — the cockpit auth interceptor redirects to login on 401, so a
    provider-key problem must not look like the user's own session expiring."""

    def test_status_by_code(self):
        from orchestrator.services.tts import TtsSynthesisError
        from orchestrator.services.voice import (
            tts_synthesis_http_error as _tts_synthesis_http_error,
        )

        assert (
            _tts_synthesis_http_error(
                TtsSynthesisError("x", code="payment_required")
            ).status_code
            == 402
        )
        assert (
            _tts_synthesis_http_error(
                TtsSynthesisError("x", code="rate_limit")
            ).status_code
            == 429
        )
        # auth + generic → 502 (never 401/403)
        assert (
            _tts_synthesis_http_error(TtsSynthesisError("x", code="auth")).status_code
            == 502
        )
        assert (
            _tts_synthesis_http_error(
                TtsSynthesisError("x", code="generic")
            ).status_code
            == 502
        )

    def test_detail_is_machine_readable(self):
        from orchestrator.services.tts import TtsSynthesisError
        from orchestrator.services.voice import (
            tts_synthesis_http_error as _tts_synthesis_http_error,
        )

        exc = _tts_synthesis_http_error(
            TtsSynthesisError("needs a paid plan", code="payment_required")
        )
        assert exc.detail == {
            "code": "payment_required",
            "message": "needs a paid plan",
        }


class TestTtsPlanEndpoint:
    def test_route_is_registered(self):
        from orchestrator.main import app
        from tests._route_inventory import mounted_routes

        routes = mounted_routes(app)
        assert ("POST", "/api/persistent/threads/{thread_id}/tts/plan") in routes

    @pytest.mark.asyncio
    async def test_returns_chunks(self):
        from orchestrator.routers import voice as voice_routes

        with patch(
            "orchestrator.services.tts.plan_tts_chunks",
            AsyncMock(
                return_value={
                    "chunks": ["chunk one", "chunk two"],
                    "rewritten": True,
                }
            ),
        ):
            resp = await voice_routes.plan_thread_message_tts(
                thread_id="t1",
                request=MagicMock(),
                body={"content": "long message"},
                dependencies=_voice_route_deps(),
            )
        assert resp.status_code == 200
        assert json.loads(resp.body) == {
            "chunks": ["chunk one", "chunk two"],
            "rewritten": True,
        }

    @pytest.mark.asyncio
    async def test_204_when_not_configured(self):
        from orchestrator.routers import voice as voice_routes

        with patch(
            "orchestrator.services.tts.plan_tts_chunks",
            AsyncMock(return_value=None),
        ):
            resp = await voice_routes.plan_thread_message_tts(
                thread_id="t1",
                request=MagicMock(),
                body={"content": "hello"},
                dependencies=_voice_route_deps(),
            )
        assert resp.status_code == 204

    @pytest.mark.asyncio
    async def test_400_on_empty_content(self):
        from fastapi import HTTPException

        from orchestrator.routers import voice as voice_routes

        with pytest.raises(HTTPException) as exc:
            await voice_routes.plan_thread_message_tts(
                thread_id="t1",
                request=MagicMock(),
                body={"content": ""},
                dependencies=_voice_route_deps(),
            )
        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_502_on_unexpected_planner_error(self):
        from fastapi import HTTPException

        from orchestrator.routers import voice as voice_routes

        deps = _voice_route_deps()
        with patch(
            "orchestrator.services.tts.plan_tts_chunks",
            AsyncMock(side_effect=RuntimeError("planner blew up")),
        ):
            with pytest.raises(HTTPException) as exc:
                await voice_routes.plan_thread_message_tts(
                    thread_id="t1",
                    request=MagicMock(),
                    body={"content": "hello"},
                    dependencies=deps,
                )
        assert exc.value.status_code == 502
        assert exc.value.detail == "TTS planning failed"
        deps.operations.logger.exception.assert_called_once()


# ---------------------------------------------------------------------------
# Usage metering (rate-limiting v2): the direct-SDK path must feed usage_events.
# ---------------------------------------------------------------------------


def _mock_ledger(*, available=True):
    """A UsageLedger double that records events into an AsyncMock."""
    led = MagicMock()
    led.is_available = available
    led.record_events = AsyncMock(return_value=1)
    return led


class TestTtsMetering:
    @pytest.mark.asyncio
    async def test_records_tts_character_event(self):
        from orchestrator.services.tts import generate_message_tts

        cls, _ = _mock_openai(speech=b"AUDIO")
        led = _mock_ledger()
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(return_value=_credentials("kokoro", None, "sk-key")),
            ),
        ):
            result = await generate_message_tts(
                content="hello there",
                language="en",
                reformulate=False,
                user_id="u1",
                postgres_db=_mock_db(),
                ledger=led,
                ref_id="t1",
            )
        assert result is not None
        led.record_events.assert_awaited_once()
        (events,), _ = led.record_events.call_args
        assert len(events) == 1
        ev = events[0]
        assert ev.category == "tts"
        assert ev.unit == "tts-character"
        assert ev.quantity == len("hello there")
        assert ev.resource == "kokoro"
        assert ev.user_id == "u1"
        assert ev.ref_id == "t1"
        assert ev.ref_kind == "thread"

    @pytest.mark.asyncio
    async def test_no_write_when_ledger_unavailable(self):
        from orchestrator.services.tts import generate_message_tts

        cls, _ = _mock_openai(speech=b"AUDIO")
        led = _mock_ledger(available=False)
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(return_value=_credentials("kokoro", None, "sk-key")),
            ),
        ):
            result = await generate_message_tts(
                content="hello there",
                language="en",
                reformulate=False,
                user_id="u1",
                postgres_db=_mock_db(),
                ledger=led,
                ref_id="t1",
            )
        assert result is not None
        led.record_events.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_metering_failure_is_non_fatal(self):
        from orchestrator.services.tts import generate_message_tts

        cls, _ = _mock_openai(speech=b"AUDIO")
        led = _mock_ledger()
        led.record_events = AsyncMock(side_effect=RuntimeError("audit down"))
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch(
                "orchestrator.services.tts._resolve_capability_credentials",
                AsyncMock(return_value=_credentials("kokoro", None, "sk-key")),
            ),
        ):
            result = await generate_message_tts(
                content="hello there",
                language="en",
                reformulate=False,
                user_id="u1",
                postgres_db=_mock_db(),
                ledger=led,
                ref_id="t1",
            )
        # A ledger hiccup must never break playback.
        assert result == ("hello there", b"AUDIO")


# ---------------------------------------------------------------------------
# Endpoint: GET /api/voice/capabilities (kills the silent 204 up front)
# ---------------------------------------------------------------------------


class TestVoiceCapabilitiesEndpoint:
    def test_route_is_registered(self):
        from orchestrator.main import app
        from tests._route_inventory import mounted_routes

        routes = mounted_routes(app)
        assert ("GET", "/api/voice/capabilities") in routes

    @pytest.mark.asyncio
    async def test_available_from_user_setting(self):
        from orchestrator.routers import voice as voice_routes

        db = MagicMock()
        db.get_user_settings = AsyncMock(return_value={"default_tts_model": "kokoro"})
        db.resolve_default_for_capability = AsyncMock(return_value=None)
        result = await voice_routes.voice_capabilities(
            MagicMock(), dependencies=_voice_route_deps(db=db)
        )
        assert result == {"tts": True, "stt": False}

    @pytest.mark.asyncio
    async def test_available_from_system_default(self):
        from orchestrator.routers import voice as voice_routes

        db = MagicMock()
        db.get_user_settings = AsyncMock(return_value={})

        async def _resolve(cap):
            return "whisper-1" if cap == "whisper" else None

        db.resolve_default_for_capability = AsyncMock(side_effect=_resolve)
        result = await voice_routes.voice_capabilities(
            MagicMock(), dependencies=_voice_route_deps(db=db)
        )
        assert result == {"tts": False, "stt": True}


# ---------------------------------------------------------------------------
# Voice-preview endpoint: custom sample text passthrough + length cap.
# ---------------------------------------------------------------------------


class TestPreviewEndpoint:
    @pytest.mark.asyncio
    async def test_passes_custom_text_and_returns_audio(self):
        from orchestrator.routers import voice as voice_routes

        preview = AsyncMock(return_value=b"AUD")
        with patch("orchestrator.services.tts.synthesize_voice_preview", preview):
            resp = await voice_routes.preview_tts_voice(
                request=MagicMock(),
                body={"voice": "af_nova", "text": "hello there"},
                dependencies=_voice_route_deps(),
            )
        assert resp.status_code == 200
        assert base64.b64decode(json.loads(resp.body)["audio"]) == b"AUD"
        assert preview.call_args.kwargs["text"] == "hello there"

    @pytest.mark.asyncio
    async def test_422_on_overlength_text(self):
        from fastapi import HTTPException

        from orchestrator.routers import voice as voice_routes
        from orchestrator.services.tts import _PREVIEW_TEXT_MAX

        preview = AsyncMock()
        with patch("orchestrator.services.tts.synthesize_voice_preview", preview):
            with pytest.raises(HTTPException) as exc:
                await voice_routes.preview_tts_voice(
                    request=MagicMock(),
                    body={"text": "x" * (_PREVIEW_TEXT_MAX + 1)},
                    dependencies=_voice_route_deps(),
                )
        assert exc.value.status_code == 422
        preview.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_204_when_no_tts_model_is_configured(self):
        from orchestrator.routers import voice as voice_routes

        with patch(
            "orchestrator.services.tts.synthesize_voice_preview",
            AsyncMock(return_value=None),
        ):
            resp = await voice_routes.preview_tts_voice(
                request=MagicMock(),
                body={},
                dependencies=_voice_route_deps(),
            )
        assert resp.status_code == 204


# ---------------------------------------------------------------------------
# ElevenLabs adapter: provider routing + REST synthesis (not OpenAI-compatible).
# ---------------------------------------------------------------------------


def _mock_httpx(*, content=b"EL_AUDIO", status_code=200, status_error=None):
    """Fake httpx.AsyncClient usable as `async with ... as client: client.post`.
    ``status_code`` drives the adapter's success/error branch (it now inspects
    the status directly rather than calling ``raise_for_status``)."""
    resp = MagicMock()
    resp.content = content
    resp.status_code = status_code
    resp.json = MagicMock(return_value={})
    resp.raise_for_status = MagicMock(side_effect=status_error)
    client = MagicMock()
    client.post = AsyncMock(return_value=resp)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=cm), client


class TestResolveTtsProvider:
    def test_explicit_provider_wins(self):
        from orchestrator.services.tts import _resolve_tts_provider

        assert _resolve_tts_provider("anything", "ElevenLabs") == "elevenlabs"

    def test_sniffs_eleven_model_ids(self):
        from orchestrator.services.tts import _resolve_tts_provider

        assert _resolve_tts_provider("eleven_multilingual_v2", None) == "elevenlabs"
        assert _resolve_tts_provider("eleven_flash_v2_5", "") == "elevenlabs"

    def test_defaults_to_openai(self):
        from orchestrator.services.tts import _resolve_tts_provider

        assert _resolve_tts_provider("kokoro", None) == "openai"
        assert _resolve_tts_provider("gpt-4o-mini-tts", None) == "openai"


class TestElevenLabsAdapter:
    @pytest.mark.asyncio
    async def test_success_builds_correct_request(self):
        from orchestrator.services.tts import _synthesize_elevenlabs

        factory, client = _mock_httpx(content=b"MP3")
        with patch("orchestrator.services.tts.httpx.AsyncClient", factory):
            audio = await _synthesize_elevenlabs(
                "hello world",
                model="eleven_multilingual_v2",
                voice="voice_abc",
                api_key="xi-key",
            )
        assert audio == b"MP3"
        args, kwargs = client.post.call_args
        assert args[0].endswith("/v1/text-to-speech/voice_abc")
        assert kwargs["headers"]["xi-api-key"] == "xi-key"
        assert kwargs["json"] == {
            "text": "hello world",
            "model_id": "eleven_multilingual_v2",
        }
        assert kwargs["params"]["output_format"] == "mp3_44100_128"

    @pytest.mark.asyncio
    async def test_falls_back_to_env_key(self):
        from orchestrator.services.tts import _synthesize_elevenlabs

        factory, client = _mock_httpx()
        with (
            patch("orchestrator.services.tts.httpx.AsyncClient", factory),
            patch.dict(os.environ, {"ELEVENLABS_API_KEY": "env-key"}),
        ):
            audio = await _synthesize_elevenlabs(
                "hi", model="eleven_multilingual_v2", voice="v", api_key=None
            )
        assert audio == b"EL_AUDIO"
        assert client.post.call_args.kwargs["headers"]["xi-api-key"] == "env-key"

    @pytest.mark.asyncio
    async def test_none_when_no_key(self):
        from orchestrator.services.tts import _synthesize_elevenlabs

        with patch.dict(os.environ, {}, clear=True):
            audio = await _synthesize_elevenlabs(
                "hi", model="eleven_multilingual_v2", voice="v", api_key=None
            )
        assert audio is None

    @pytest.mark.asyncio
    async def test_none_when_no_voice(self):
        from orchestrator.services.tts import _synthesize_elevenlabs

        audio = await _synthesize_elevenlabs(
            "hi", model="eleven_multilingual_v2", voice="", api_key="xi-key"
        )
        assert audio is None

    @pytest.mark.asyncio
    async def test_none_on_generic_http_error(self):
        """A non-actionable upstream status (5xx) → None (the caller maps that to
        a generic 502); actionable codes raise instead — see TestTtsErrorSurfacing."""
        from orchestrator.services.tts import _synthesize_elevenlabs

        factory, _ = _mock_httpx(status_code=500)
        with patch("orchestrator.services.tts.httpx.AsyncClient", factory):
            audio = await _synthesize_elevenlabs(
                "hi", model="eleven_multilingual_v2", voice="v", api_key="xi-key"
            )
        assert audio is None

    @pytest.mark.asyncio
    async def test_synthesize_speech_routes_to_elevenlabs_not_openai(self):
        """The choke point forks to ElevenLabs on an eleven_* model and never
        constructs an OpenAI client (the ElevenLabs API isn't OpenAI-compatible)."""
        from orchestrator.services.tts import _synthesize_speech

        openai_cls = MagicMock()
        el = AsyncMock(return_value=b"MP3")
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", openai_cls),
            patch("orchestrator.services.tts._synthesize_elevenlabs", el),
        ):
            audio = await _synthesize_speech(
                "hi",
                model="eleven_multilingual_v2",
                voice="v",
                base_url=None,
                api_key="xi-key",
            )
        assert audio == b"MP3"
        el.assert_awaited_once()
        openai_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_synthesize_speech_explicit_provider_overrides_openai_model_id(self):
        """An explicit params_json.provider forces the ElevenLabs path even when
        the model id doesn't contain 'eleven'."""
        from orchestrator.services.tts import _synthesize_speech

        openai_cls = MagicMock()
        el = AsyncMock(return_value=b"MP3")
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", openai_cls),
            patch("orchestrator.services.tts._synthesize_elevenlabs", el),
        ):
            await _synthesize_speech(
                "hi",
                model="custom-voice-model",
                voice="v",
                base_url=None,
                api_key="xi-key",
                provider="elevenlabs",
            )
        el.assert_awaited_once()
        openai_cls.assert_not_called()


# ---------------------------------------------------------------------------
# Markdown-strip fallback: a timed-out/absent rewrite must still read cleanly
# (no "asterisk asterisk", no table pipes).
# ---------------------------------------------------------------------------


class TestStripMarkdownForSpeech:
    def test_strips_emphasis_and_headers(self):
        from orchestrator.services.tts import _strip_markdown_for_speech

        out = _strip_markdown_for_speech("# Title\n\nThis is **bold** and *italic*.")
        assert "*" not in out
        assert "#" not in out
        assert "bold" in out and "italic" in out

    def test_links_become_text_images_dropped(self):
        from orchestrator.services.tts import _strip_markdown_for_speech

        out = _strip_markdown_for_speech("See [the docs](http://x) and ![alt](y.png).")
        assert "the docs" in out
        assert "http://x" not in out and "y.png" not in out and "alt" not in out

    def test_table_becomes_sentences_no_pipes(self):
        from orchestrator.services.tts import _strip_markdown_for_speech

        md = "| Metal | Price |\n| --- | --- |\n| Neodymium | 155 |\n| Terbium | 1103 |"
        out = _strip_markdown_for_speech(md)
        assert "|" not in out
        assert "Neodymium, 155." in out
        assert "Terbium, 1103." in out
        assert "---" not in out

    def test_code_fence_becomes_placeholder(self):
        from orchestrator.services.tts import _strip_markdown_for_speech

        out = _strip_markdown_for_speech(
            "Before\n\n```python\nprint('hi')\n```\n\nAfter"
        )
        assert "```" not in out and "print" not in out
        assert "code snippet" in out
        assert "Before" in out and "After" in out


# ---------------------------------------------------------------------------
# Phase 5 — account voice listing for the Settings picker.
# ---------------------------------------------------------------------------


def _mock_httpx_get(*, json_body, status_error=None):
    """Fake httpx.AsyncClient usable as `async with ... as c: c.get`."""
    resp = MagicMock()
    resp.json = MagicMock(return_value=json_body)
    resp.raise_for_status = MagicMock(side_effect=status_error)
    client = MagicMock()
    client.get = AsyncMock(return_value=resp)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=cm), client


def _voices_db(*, tts_model, params_json=None, api_keys=None):
    """A postgres_db double resolving to ``tts_model`` as the user's TTS model,
    with an optional catalog ``params_json`` (provider/voice)."""
    db = MagicMock()
    db.get_user_settings = AsyncMock(return_value={"default_tts_model": tts_model})
    db.resolve_api_keys_for_job = AsyncMock(return_value=api_keys or {})
    db.resolve_default_for_capability = AsyncMock(return_value=None)
    db.resolve_catalog_model = AsyncMock(
        return_value={"params_json": params_json} if params_json is not None else None
    )
    return db


_EL_VOICES_BODY = {
    "voices": [
        {
            "voice_id": "v_sarah",
            "name": "Sarah",
            "labels": {"accent": "american", "gender": "female"},
            "preview_url": "https://cdn.elevenlabs.io/sarah.mp3",
        },
        {
            "voice_id": "v_george",
            "name": "George",
            "labels": {"accent": "british", "gender": "male"},
            "preview_url": "https://cdn.elevenlabs.io/george.mp3",
        },
    ]
}


class TestMapElevenLabsVoice:
    def test_maps_fields(self):
        from orchestrator.services.tts import _map_elevenlabs_voice

        out = _map_elevenlabs_voice(_EL_VOICES_BODY["voices"][0])
        assert out == {
            "id": "v_sarah",
            "name": "Sarah",
            "labels": {"accent": "american", "gender": "female"},
            "preview_url": "https://cdn.elevenlabs.io/sarah.mp3",
        }

    def test_missing_fields_degrade_gracefully(self):
        from orchestrator.services.tts import _map_elevenlabs_voice

        out = _map_elevenlabs_voice({"voice_id": "v1"})
        # name falls back to the id; labels default to {}, preview to None.
        assert out == {"id": "v1", "name": "v1", "labels": {}, "preview_url": None}


class TestListAccountVoices:
    @pytest.fixture(autouse=True)
    def _clear_voice_cache(self):
        from orchestrator.services import tts

        tts._voices_cache.clear()
        yield
        tts._voices_cache.clear()

    @pytest.mark.asyncio
    async def test_no_model_configured_returns_backend_none(self):
        from orchestrator.services.tts import list_account_voices

        db = MagicMock()
        db.get_user_settings = AsyncMock(return_value={})
        db.resolve_api_keys_for_job = AsyncMock(return_value={})
        db.resolve_default_for_capability = AsyncMock(return_value=None)
        out = await list_account_voices(user_id="u1", postgres_db=db)
        assert out == {"backend": None, "voices": []}

    @pytest.mark.asyncio
    async def test_non_elevenlabs_backend_returns_empty_list(self):
        """Kokoro/OpenAI keep their static catalogs in the cockpit — the server
        returns the backend name but no voices, and never calls ElevenLabs."""
        from orchestrator.services.tts import list_account_voices

        db = _voices_db(tts_model="kokoro")
        factory, _ = _mock_httpx_get(json_body=_EL_VOICES_BODY)
        with patch("orchestrator.services.tts.httpx.AsyncClient", factory):
            out = await list_account_voices(user_id="u1", postgres_db=db)
        assert out == {"backend": "openai", "voices": []}
        factory.assert_not_called()

    @pytest.mark.asyncio
    async def test_elevenlabs_lists_account_voices(self):
        from orchestrator.services.tts import list_account_voices

        db = _voices_db(
            tts_model="eleven_multilingual_v2",
            params_json={"provider": "elevenlabs", "voice": "v_sarah"},
        )
        factory, client = _mock_httpx_get(json_body=_EL_VOICES_BODY)
        with (
            patch("orchestrator.services.tts.httpx.AsyncClient", factory),
            patch.dict(os.environ, {"ELEVENLABS_API_KEY": "env-key"}),
        ):
            out = await list_account_voices(user_id="u1", postgres_db=db)
        assert out["backend"] == "elevenlabs"
        assert [v["id"] for v in out["voices"]] == ["v_sarah", "v_george"]
        assert out["voices"][1]["name"] == "George"
        # Auth header attached server-side; voices endpoint hit.
        assert client.get.call_args.args[0].endswith("/v2/voices")
        assert client.get.call_args.kwargs["headers"]["xi-api-key"] == "env-key"

    @pytest.mark.asyncio
    async def test_elevenlabs_no_key_returns_empty(self):
        from orchestrator.services.tts import list_account_voices

        db = _voices_db(
            tts_model="eleven_multilingual_v2",
            params_json={"provider": "elevenlabs"},
        )
        with patch.dict(os.environ, {}, clear=True):
            out = await list_account_voices(user_id="u1", postgres_db=db)
        assert out == {"backend": "elevenlabs", "voices": []}

    @pytest.mark.asyncio
    async def test_fetch_failure_degrades_to_empty(self):
        """A 5xx/timeout from ElevenLabs must not 500 the Settings page — the
        picker falls back to free-text, so an empty list is the contract."""
        from orchestrator.services.tts import list_account_voices

        db = _voices_db(
            tts_model="eleven_multilingual_v2",
            params_json={"provider": "elevenlabs"},
        )
        factory, _ = _mock_httpx_get(
            json_body={}, status_error=RuntimeError("503 upstream")
        )
        with (
            patch("orchestrator.services.tts.httpx.AsyncClient", factory),
            patch.dict(os.environ, {"ELEVENLABS_API_KEY": "env-key"}),
        ):
            out = await list_account_voices(user_id="u1", postgres_db=db)
        assert out == {"backend": "elevenlabs", "voices": []}

    @pytest.mark.asyncio
    async def test_second_call_served_from_cache(self):
        """The ~5 min in-process cache means opening Settings twice hits
        ElevenLabs once."""
        from orchestrator.services.tts import list_account_voices

        db = _voices_db(
            tts_model="eleven_multilingual_v2",
            params_json={"provider": "elevenlabs"},
        )
        factory, client = _mock_httpx_get(json_body=_EL_VOICES_BODY)
        with (
            patch("orchestrator.services.tts.httpx.AsyncClient", factory),
            patch.dict(os.environ, {"ELEVENLABS_API_KEY": "env-key"}),
        ):
            first = await list_account_voices(user_id="u1", postgres_db=db)
            second = await list_account_voices(user_id="u1", postgres_db=db)
        assert first == second
        assert client.get.await_count == 1

    @pytest.mark.asyncio
    async def test_invalidate_cache_forces_refetch(self):
        from orchestrator.services.tts import (
            invalidate_account_voices_cache,
            list_account_voices,
        )

        db = _voices_db(
            tts_model="eleven_multilingual_v2",
            params_json={"provider": "elevenlabs"},
        )
        factory, client = _mock_httpx_get(json_body=_EL_VOICES_BODY)
        with (
            patch("orchestrator.services.tts.httpx.AsyncClient", factory),
            patch.dict(os.environ, {"ELEVENLABS_API_KEY": "env-key"}),
        ):
            await list_account_voices(user_id="u1", postgres_db=db)
            await invalidate_account_voices_cache()
            await list_account_voices(user_id="u1", postgres_db=db)
        assert client.get.await_count == 2

    def test_preserves_snake_case_identifiers(self):
        from orchestrator.services.tts import _strip_markdown_for_speech

        # Single underscores (identifiers) must survive — only emphasis is stripped.
        assert "user_id" in _strip_markdown_for_speech("The user_id column is a key.")


# ---------------------------------------------------------------------------
# Phase 6 — ElevenLabs Voice Library: search proxy + add-to-account.
# ---------------------------------------------------------------------------


def _mock_httpx_post(*, json_body=None, status_code=200):
    """Fake httpx.AsyncClient whose `.post` returns a response with the given
    status + JSON body. Usable as `async with ... as c: await c.post(...)`."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json = MagicMock(return_value=json_body if json_body is not None else {})
    client = MagicMock()
    client.post = AsyncMock(return_value=resp)
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=client)
    cm.__aexit__ = AsyncMock(return_value=False)
    return MagicMock(return_value=cm), client


_SHARED_VOICES_BODY = {
    "has_more": True,
    "voices": [
        {
            "voice_id": "pub_amelie",
            "public_owner_id": "owner_1",
            "name": "Amélie",
            "accent": "french",
            "gender": "female",
            "age": "young",
            "language": "en",
            "descriptive": "warm",
            "preview_url": "https://cdn.elevenlabs.io/amelie.mp3",
            "free_users_allowed": True,
        }
    ],
}


class TestMapSharedVoice:
    def test_maps_fields(self):
        from orchestrator.services.tts import _map_shared_voice

        out = _map_shared_voice(_SHARED_VOICES_BODY["voices"][0])
        assert out["id"] == "pub_amelie"
        assert out["public_owner_id"] == "owner_1"
        assert out["name"] == "Amélie"
        assert out["accent"] == "french"
        # `description` falls back to the library's `descriptive` field.
        assert out["description"] == "warm"
        assert out["free"] is True

    def test_missing_fields_degrade(self):
        from orchestrator.services.tts import _map_shared_voice

        out = _map_shared_voice({"voice_id": "v1", "public_owner_id": "o1"})
        assert out == {
            "id": "v1",
            "public_owner_id": "o1",
            "name": "v1",
            "accent": None,
            "gender": None,
            "age": None,
            "language": None,
            "description": None,
            "preview_url": None,
            "free": False,
        }


class TestSearchVoiceLibrary:
    @pytest.fixture(autouse=True)
    def _clear_voice_cache(self):
        from orchestrator.services import tts

        tts._voices_cache.clear()
        yield
        tts._voices_cache.clear()

    @pytest.mark.asyncio
    async def test_non_elevenlabs_backend_returns_empty(self):
        """The library is ElevenLabs-only; other backends short-circuit without
        an HTTP call."""
        from orchestrator.services.tts import search_voice_library

        db = _voices_db(tts_model="kokoro")
        out = await search_voice_library(user_id="u1", postgres_db=db, filters={})
        assert out == {
            "backend": "openai",
            "voices": [],
            "has_more": False,
            "error": None,
        }

    @pytest.mark.asyncio
    async def test_search_passes_filters_and_maps(self):
        from orchestrator.services.tts import search_voice_library

        db = _voices_db(
            tts_model="eleven_multilingual_v2",
            params_json={"provider": "elevenlabs"},
        )
        factory, client = _mock_httpx_get(json_body=_SHARED_VOICES_BODY)
        with (
            patch("orchestrator.services.tts.httpx.AsyncClient", factory),
            patch.dict(os.environ, {"ELEVENLABS_API_KEY": "env-key"}),
        ):
            out = await search_voice_library(
                user_id="u1",
                postgres_db=db,
                filters={"search": "french english", "gender": "female", "page": "0"},
            )
        assert out["backend"] == "elevenlabs"
        assert out["has_more"] is True
        assert out["voices"][0]["accent"] == "french"
        # search + gender forwarded; page 0 omitted; page_size defaulted; key server-side.
        params = client.get.call_args.kwargs["params"]
        assert params["search"] == "french english"
        assert params["gender"] == "female"
        assert "page" not in params
        assert params["page_size"] == 30
        assert client.get.call_args.args[0].endswith("/v1/shared-voices")
        assert client.get.call_args.kwargs["headers"]["xi-api-key"] == "env-key"

    @pytest.mark.asyncio
    async def test_no_key_returns_error(self):
        from orchestrator.services.tts import search_voice_library

        db = _voices_db(
            tts_model="eleven_multilingual_v2",
            params_json={"provider": "elevenlabs"},
        )
        with patch.dict(os.environ, {}, clear=True):
            out = await search_voice_library(user_id="u1", postgres_db=db, filters={})
        assert out["backend"] == "elevenlabs"
        assert out["voices"] == []
        assert out["error"]

    @pytest.mark.asyncio
    async def test_fetch_failure_degrades_with_error(self):
        """A 5xx/timeout from ElevenLabs must not 500 the Settings page — the
        browser shows a banner, so an empty list + readable error is the
        contract."""
        from orchestrator.services.tts import search_voice_library

        db = _voices_db(
            tts_model="eleven_multilingual_v2",
            params_json={"provider": "elevenlabs"},
        )
        factory, _ = _mock_httpx_get(json_body={}, status_error=RuntimeError("503"))
        with (
            patch("orchestrator.services.tts.httpx.AsyncClient", factory),
            patch.dict(os.environ, {"ELEVENLABS_API_KEY": "env-key"}),
        ):
            out = await search_voice_library(
                user_id="u1", postgres_db=db, filters={"search": "x"}
            )
        assert out["voices"] == []
        assert out["error"]


class TestAddLibraryVoice:
    @pytest.fixture(autouse=True)
    def _clear_voice_cache(self):
        from orchestrator.services import tts

        tts._voices_cache.clear()
        yield
        tts._voices_cache.clear()

    @pytest.mark.asyncio
    async def test_add_success_returns_id_and_invalidates_cache(self):
        from orchestrator.services import tts
        from orchestrator.services.tts import add_library_voice

        db = _voices_db(
            tts_model="eleven_multilingual_v2",
            params_json={"provider": "elevenlabs"},
        )
        # Seed the account-voice cache so we can prove the add invalidates it —
        # the new voice must show up in the Settings picker immediately.
        tts._voices_cache["stale"] = (tts._now(), [{"id": "old"}])
        factory, client = _mock_httpx_post(json_body={"voice_id": "acct_new"})
        with (
            patch("orchestrator.services.tts.httpx.AsyncClient", factory),
            patch.dict(os.environ, {"ELEVENLABS_API_KEY": "env-key"}),
        ):
            out = await add_library_voice(
                user_id="u1",
                postgres_db=db,
                public_owner_id="owner_1",
                voice_id="pub_amelie",
                new_name="Amélie",
            )
        assert out == {"voice_id": "acct_new", "name": "Amélie"}
        assert tts._voices_cache == {}  # invalidated
        assert client.post.call_args.args[0].endswith(
            "/v1/voices/add/owner_1/pub_amelie"
        )
        assert client.post.call_args.kwargs["json"] == {"new_name": "Amélie"}
        assert client.post.call_args.kwargs["headers"]["xi-api-key"] == "env-key"

    @pytest.mark.asyncio
    async def test_slot_limit_raises_readable_error(self):
        """ElevenLabs' voice-slot-limit 400 becomes a readable TtsLibraryError
        carrying its status — never a bare 500."""
        from orchestrator.services.tts import TtsLibraryError, add_library_voice

        db = _voices_db(
            tts_model="eleven_multilingual_v2",
            params_json={"provider": "elevenlabs"},
        )
        factory, _ = _mock_httpx_post(
            json_body={
                "detail": {
                    "status": "voice_limit_reached",
                    "message": "You have reached your voice limit.",
                }
            },
            status_code=400,
        )
        with (
            patch("orchestrator.services.tts.httpx.AsyncClient", factory),
            patch.dict(os.environ, {"ELEVENLABS_API_KEY": "env-key"}),
        ):
            with pytest.raises(TtsLibraryError) as exc:
                await add_library_voice(
                    user_id="u1",
                    postgres_db=db,
                    public_owner_id="o",
                    voice_id="v",
                    new_name="X",
                )
        assert exc.value.status_code == 400
        assert "voice limit" in exc.value.message.lower()

    @pytest.mark.asyncio
    async def test_non_elevenlabs_backend_raises(self):
        from orchestrator.services.tts import TtsLibraryError, add_library_voice

        db = _voices_db(tts_model="kokoro")
        with pytest.raises(TtsLibraryError) as exc:
            await add_library_voice(
                user_id="u1",
                postgres_db=db,
                public_owner_id="o",
                voice_id="v",
                new_name="X",
            )
        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_network_failure_raises_502(self):
        from orchestrator.services.tts import TtsLibraryError, add_library_voice

        db = _voices_db(
            tts_model="eleven_multilingual_v2",
            params_json={"provider": "elevenlabs"},
        )
        client = MagicMock()
        client.post = AsyncMock(side_effect=RuntimeError("boom"))
        cm = MagicMock()
        cm.__aenter__ = AsyncMock(return_value=client)
        cm.__aexit__ = AsyncMock(return_value=False)
        factory = MagicMock(return_value=cm)
        with (
            patch("orchestrator.services.tts.httpx.AsyncClient", factory),
            patch.dict(os.environ, {"ELEVENLABS_API_KEY": "env-key"}),
        ):
            with pytest.raises(TtsLibraryError) as exc:
                await add_library_voice(
                    user_id="u1",
                    postgres_db=db,
                    public_owner_id="o",
                    voice_id="v",
                    new_name="X",
                )
        assert exc.value.status_code == 502


class TestTtsErrorSurfacing:
    """Actionable synthesis failures (402 needs-paid-plan, 401 auth, 429 rate)
    carry a code so the UI can say what's wrong instead of a generic failure —
    the ElevenLabs free-tier "can't use library voices" 402 is the motivating
    case."""

    def test_error_code_mapping(self):
        from orchestrator.services.tts import _tts_error_code

        assert _tts_error_code(402) == "payment_required"
        assert _tts_error_code(401) == "auth"
        assert _tts_error_code(403) == "auth"
        assert _tts_error_code(429) == "rate_limit"
        assert _tts_error_code(500) is None
        assert _tts_error_code(None) is None

    @pytest.mark.asyncio
    async def test_elevenlabs_402_raises_payment_required(self):
        from orchestrator.services.tts import TtsSynthesisError, _synthesize_elevenlabs

        factory, _ = _mock_httpx_post(
            json_body={
                "detail": {
                    "status": "payment_required",
                    "message": "Free users cannot use library voices via the API.",
                }
            },
            status_code=402,
        )
        with patch("orchestrator.services.tts.httpx.AsyncClient", factory):
            with pytest.raises(TtsSynthesisError) as exc:
                await _synthesize_elevenlabs(
                    "hi", model="eleven_multilingual_v2", voice="v1", api_key="k"
                )
        assert exc.value.code == "payment_required"
        assert "library voices" in str(exc.value).lower()

    @pytest.mark.asyncio
    async def test_elevenlabs_500_returns_none(self):
        """A transient 5xx has no actionable code → return None (generic 502, the
        caller may retry), not a raise."""
        from orchestrator.services.tts import _synthesize_elevenlabs

        factory, _ = _mock_httpx_post(json_body={}, status_code=500)
        with patch("orchestrator.services.tts.httpx.AsyncClient", factory):
            out = await _synthesize_elevenlabs(
                "hi", model="eleven_x", voice="v1", api_key="k"
            )
        assert out is None

    @pytest.mark.asyncio
    async def test_synthesize_speech_propagates_coded_error(self):
        from orchestrator.services.tts import TtsSynthesisError, _synthesize_speech

        factory, _ = _mock_httpx_post(
            json_body={"detail": {"message": "pay up"}}, status_code=402
        )
        with patch("orchestrator.services.tts.httpx.AsyncClient", factory):
            with pytest.raises(TtsSynthesisError) as exc:
                await _synthesize_speech(
                    "hi",
                    model="eleven_multilingual_v2",
                    voice="v1",
                    base_url=None,
                    api_key="k",
                    provider="elevenlabs",
                )
        assert exc.value.code == "payment_required"


class TestAuxThinkLeakControl:
    """The rewrite path must apply the aux family's matrix settings.extra_body
    (MiniMax reasoning_split) so <think> never lands in content — the main agent
    did this via create_llm; the TTS lane historically didn't ("extra_body layer
    2")."""

    def test_family_extra_body_from_matrix_minimax(self):
        from orchestrator.services.tts import _aux_family_extra_body

        # Real matrix: minimax-m3 carries reasoning_split to keep reasoning out of
        # `content` (returned as a separate reasoning_content field instead).
        assert _aux_family_extra_body("MiniMax-M3") == {"reasoning_split": True}

    def test_family_extra_body_empty_for_gemma(self):
        from orchestrator.services.tts import _aux_family_extra_body

        assert _aux_family_extra_body("gemma-4-moe") == {}

    def test_aux_extra_body_merges_reasoning_and_family(self):
        from orchestrator.services.tts import _aux_extra_body

        # gemma: reasoning toggle only (no family extra_body).
        assert _aux_extra_body("gemma-4-moe", "off") == {
            "chat_template_kwargs": {"enable_thinking": False}
        }
        # minimax-m3: family reasoning_split merges with the thinking toggle
        # (binary_toggle since the native-API promotion) — read-aloud "off" now
        # genuinely disables M3 thinking, a time-to-first-audio win.
        assert _aux_extra_body("MiniMax-M3", "off") == {
            "reasoning_split": True,
            "thinking": {"type": "disabled"},
        }


class TestThinkStrip:
    """Belt-and-suspenders: <think>…</think> reasoning must never be spoken, even
    if a provider leaks it into content despite reasoning_split."""

    def test_strip_complete_block(self):
        from orchestrator.services.tts import _strip_think_tags

        assert (
            _strip_think_tags("<think>reasoning here</think>Hello world.")
            == "Hello world."
        )

    def test_strip_multiline_block(self):
        from orchestrator.services.tts import _strip_think_tags

        t = "<think>\nline1\nline2\n</think>\n\nActual answer."
        assert _strip_think_tags(t) == "Actual answer."

    def test_unclosed_think_dropped_to_end(self):
        from orchestrator.services.tts import _strip_think_tags

        assert _strip_think_tags("Answer.<think>truncated reasoning") == "Answer."

    def test_no_think_unchanged(self):
        from orchestrator.services.tts import _strip_think_tags

        assert _strip_think_tags("Just plain text.") == "Just plain text."

    def test_case_insensitive(self):
        from orchestrator.services.tts import _strip_think_tags

        assert _strip_think_tags("<THINK>x</THINK>ok") == "ok"


class TestThinkStreamStrip:
    def test_withholds_while_open_then_releases(self):
        from orchestrator.services.tts import _strip_think_stream

        assert _strip_think_stream("<think>reasoning so f") == ""
        assert _strip_think_stream("<think>reasoning</think>Hello") == "Hello"

    def test_withholds_partial_open_tag(self):
        from orchestrator.services.tts import _strip_think_stream

        assert _strip_think_stream("Hello <thi") == "Hello "

    def test_final_releases_partial_tail(self):
        from orchestrator.services.tts import _strip_think_stream

        assert _strip_think_stream("done <") == "done "  # withheld mid-stream
        assert _strip_think_stream("done <", final=True) == "done <"

    def test_incremental_diff_matches_stream(self):
        """Simulate the streaming loop: accumulate deltas (tags split across
        them), feed only newly-clean text — the think block must vanish and only
        the answer survive."""
        from orchestrator.services.tts import _strip_think_stream

        deltas = ["<thi", "nk>let me ", "reason</thi", "nk>The ", "answer is 42."]
        raw, emitted, out = "", 0, ""
        for d in deltas:
            raw += d
            clean = _strip_think_stream(raw)
            if len(clean) > emitted:
                out += clean[emitted:]
                emitted = len(clean)
        final = _strip_think_stream(raw, final=True)
        if len(final) > emitted:
            out += final[emitted:]
        assert out == "The answer is 42."


# ---------------------------------------------------------------------------
# Streaming chunk plan: sentinel parsing + stream_tts_chunks generator.
# ---------------------------------------------------------------------------


class TestDrainSentinels:
    def test_no_sentinel_all_remainder(self):
        from orchestrator.services.tts import _drain_sentinels

        assert _drain_sentinels("partial text") == ([], "partial text")

    def test_splits_complete_chunks_keeps_tail(self):
        from orchestrator.services.tts import _drain_sentinels

        chunks, rem = _drain_sentinels("First.[[BREAK]]Second.[[BREAK]]Third")
        assert chunks == ["First.", "Second."]
        assert rem == "Third"

    def test_partial_sentinel_stays_in_remainder(self):
        from orchestrator.services.tts import _drain_sentinels

        chunks, rem = _drain_sentinels("First.[[BR")
        assert chunks == []
        assert rem == "First.[[BR"


class _FakeStream:
    """Async-iterable OpenAI streaming double: yields content-delta events for
    each string in ``pieces``, then a final usage-only event."""

    def __init__(self, pieces, usage=None):
        self._events = [
            MagicMock(choices=[MagicMock(delta=MagicMock(content=p))], usage=None)
            for p in pieces
        ]
        self._events.append(MagicMock(choices=[], usage=usage))

    def __aiter__(self):
        self._it = iter(self._events)
        return self

    async def __anext__(self):
        try:
            return next(self._it)
        except StopIteration:
            raise StopAsyncIteration


def _mock_openai_stream(pieces, usage=None):
    """(class, client) whose chat.completions.create(stream=True) yields ``pieces``."""
    client = MagicMock()
    client.chat.completions.create = AsyncMock(return_value=_FakeStream(pieces, usage))
    client.close = AsyncMock()
    return MagicMock(return_value=client), client


async def _collect(agen):
    return [ev async for ev in agen]


class TestStreamTtsChunks:
    @pytest.mark.asyncio
    async def test_unavailable_without_tts_model(self):
        from orchestrator.services.tts import stream_tts_chunks

        with patch(
            "orchestrator.services.tts._resolve_capability_credentials", _caps(tts=None)
        ):
            events = await _collect(
                stream_tts_chunks(content="hello", user_id="u1", postgres_db=_mock_db())
            )
        assert events == [{"type": "unavailable"}]

    @pytest.mark.asyncio
    async def test_streams_chunks_split_on_sentinel(self):
        from orchestrator.services.tts import stream_tts_chunks

        # Deltas deliberately split a sentinel across the boundary.
        cls, _ = _mock_openai_stream(
            ["First chunk.[[BR", "EAK]]Second chunk.[[BREAK]]Third chunk."]
        )
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch("orchestrator.services.tts._resolve_capability_credentials", _caps()),
        ):
            events = await _collect(
                stream_tts_chunks(
                    content="some **markdown** message",
                    user_id="u1",
                    postgres_db=_mock_db(),
                )
            )
        chunks = [e for e in events if e["type"] == "chunk"]
        assert [c["text"] for c in chunks] == [
            "First chunk.",
            "Second chunk.",
            "Third chunk.",
        ]
        assert all(c["rewritten"] is True for c in chunks)
        assert events[-1] == {"type": "done", "total": 3, "rewritten": True}

    @pytest.mark.asyncio
    async def test_size_flush_without_sentinel(self):
        """The real-world case: the model rewrites well but ignores the sentinel
        and streams one blob. The parser must still chunk incrementally by size at
        sentence boundaries — a short first chunk, then target-sized chunks."""
        from orchestrator.services.tts import TTS_FIRST_CHUNK_TARGET, stream_tts_chunks

        pieces = [f"This is sentence number {i}. " for i in range(120)]  # ~3.3k chars
        cls, _ = _mock_openai_stream(pieces)
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch("orchestrator.services.tts._resolve_capability_credentials", _caps()),
        ):
            events = await _collect(
                stream_tts_chunks(content="long", user_id="u1", postgres_db=_mock_db())
            )
        chunks = [e for e in events if e["type"] == "chunk"]
        assert len(chunks) >= 2  # chunked despite no sentinel
        assert len(chunks[0]["text"]) <= TTS_FIRST_CHUNK_TARGET  # short first chunk
        assert chunks[0]["text"].endswith(".")  # cut at a sentence boundary
        assert all(c["rewritten"] is True for c in chunks)

    @pytest.mark.asyncio
    async def test_fallback_without_aux_strips_markdown(self):
        from orchestrator.services.tts import stream_tts_chunks

        with patch(
            "orchestrator.services.tts._resolve_capability_credentials", _caps(aux=None)
        ):
            events = await _collect(
                stream_tts_chunks(
                    content="Here is **bold** text with a | pipe | table.",
                    user_id="u1",
                    postgres_db=_mock_db(),
                )
            )
        chunks = [e for e in events if e["type"] == "chunk"]
        assert chunks and all(c["rewritten"] is False for c in chunks)
        joined = " ".join(c["text"] for c in chunks)
        assert "*" not in joined and "|" not in joined
        assert events[-1]["type"] == "done" and events[-1]["rewritten"] is False

    @pytest.mark.asyncio
    async def test_empty_stream_falls_back(self):
        from orchestrator.services.tts import stream_tts_chunks

        cls, _ = _mock_openai_stream([""])  # model produced nothing usable
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch("orchestrator.services.tts._resolve_capability_credentials", _caps()),
        ):
            events = await _collect(
                stream_tts_chunks(
                    content="short message", user_id="u1", postgres_db=_mock_db()
                )
            )
        chunks = [e for e in events if e["type"] == "chunk"]
        assert [c["text"] for c in chunks] == ["short message"]
        assert all(c["rewritten"] is False for c in chunks)

    @pytest.mark.asyncio
    async def test_meters_stream_usage(self):
        from orchestrator.services.tts import stream_tts_chunks

        usage = MagicMock(prompt_tokens=11, completion_tokens=22)
        cls, _ = _mock_openai_stream(["One.[[BREAK]]Two."], usage=usage)
        led = _mock_ledger()
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch("orchestrator.services.tts._resolve_capability_credentials", _caps()),
        ):
            await _collect(
                stream_tts_chunks(
                    content="hi",
                    user_id="u1",
                    postgres_db=_mock_db(),
                    ledger=led,
                    ref_id="t1",
                )
            )
        led.record_events.assert_awaited()
        (events,), _ = led.record_events.call_args
        assert {e.unit for e in events} == {"prompt-token", "completion-token"}
        assert all(e.details["stage"] == "chunk-stream" for e in events)


class TestPlanRawMode:
    @pytest.mark.asyncio
    async def test_reformulate_false_skips_aux_and_strips_markdown(self):
        from orchestrator.services.tts import plan_tts_chunks

        cls, _ = _mock_openai_chat('["should not be used"]')
        with (
            patch("orchestrator.services.tts.AsyncOpenAI", cls),
            patch("orchestrator.services.tts._resolve_capability_credentials", _caps()),
        ):
            result = await plan_tts_chunks(
                content="This is **bold** and has a | table | row.",
                user_id="u1",
                postgres_db=_mock_db(),
                reformulate=False,
            )
        cls.assert_not_called()  # aux LLM never invoked in raw mode
        assert result["rewritten"] is False
        joined = " ".join(result["chunks"])
        assert "*" not in joined and "|" not in joined


class TestTtsPlanStreamEndpoint:
    def test_route_is_registered(self):
        from orchestrator.main import app
        from tests._route_inventory import mounted_routes

        routes = mounted_routes(app)
        assert (
            "POST",
            "/api/persistent/threads/{thread_id}/tts/plan/stream",
        ) in routes

    @pytest.mark.asyncio
    async def test_emits_sse_frames(self):
        """The handler must wrap the service generator into the house SSE wire
        format: a kickstart comment, `event: chunk` frames, then `event: done`."""
        from orchestrator.routers import voice as voice_routes

        async def _fake_stream(**_):
            yield {"type": "chunk", "index": 0, "text": "Hello.", "rewritten": True}
            yield {"type": "chunk", "index": 1, "text": "World.", "rewritten": True}
            yield {"type": "done", "total": 2, "rewritten": True}

        with patch("orchestrator.services.tts.stream_tts_chunks", _fake_stream):
            resp = await voice_routes.stream_thread_message_tts_plan(
                thread_id="t1",
                request=MagicMock(),
                body={"content": "hi"},
                dependencies=_voice_route_deps(),
            )
            assert resp.media_type == "text/event-stream"
            assert resp.headers["cache-control"] == "no-cache"
            assert resp.headers["x-accel-buffering"] == "no"
            frames = "".join([chunk async for chunk in resp.body_iterator])

        assert frames.startswith(": open")  # kickstart comment
        assert frames.count("event: chunk") == 2
        assert '"index": 0' in frames and '"text": "Hello."' in frames
        assert "event: done" in frames and '"total": 2' in frames

    @pytest.mark.asyncio
    async def test_maps_unavailable_event(self):
        from orchestrator.routers import voice as voice_routes

        async def _fake_stream(**_):
            yield {"type": "unavailable"}

        with patch("orchestrator.services.tts.stream_tts_chunks", _fake_stream):
            resp = await voice_routes.stream_thread_message_tts_plan(
                thread_id="t1",
                request=MagicMock(),
                body={"content": "hi"},
                dependencies=_voice_route_deps(),
            )
            frames = "".join([chunk async for chunk in resp.body_iterator])
        assert "event: unavailable" in frames
        assert "event: chunk" not in frames

    @pytest.mark.asyncio
    async def test_stream_failure_becomes_an_error_frame_not_a_5xx(self):
        """The response has already started; a mid-stream failure can only be
        reported in-band."""
        from orchestrator.routers import voice as voice_routes

        async def _fake_stream(**_):
            yield {"type": "chunk", "index": 0, "text": "Hello.", "rewritten": True}
            raise RuntimeError("upstream died")

        deps = _voice_route_deps()
        with patch("orchestrator.services.tts.stream_tts_chunks", _fake_stream):
            resp = await voice_routes.stream_thread_message_tts_plan(
                thread_id="t1",
                request=MagicMock(),
                body={"content": "hi"},
                dependencies=deps,
            )
            frames = "".join([chunk async for chunk in resp.body_iterator])
        assert "event: error" in frames
        assert '"message": "stream failed"' in frames
        assert "upstream died" not in frames
        deps.operations.logger.exception.assert_called_once()

    @pytest.mark.asyncio
    async def test_400_on_empty_content(self):
        from fastapi import HTTPException

        from orchestrator.routers import voice as voice_routes

        with pytest.raises(HTTPException) as exc:
            await voice_routes.stream_thread_message_tts_plan(
                thread_id="t1",
                request=MagicMock(),
                body={"content": "  "},
                dependencies=_voice_route_deps(),
            )
        assert exc.value.status_code == 400
