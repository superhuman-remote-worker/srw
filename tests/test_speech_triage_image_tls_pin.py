"""Speech, triage and image calls verify an endpoint's pinned certificate.

The orchestrator's own model calls (transcription, TTS and its rewrite/chunk
calls, message triage) and the agent's image-generation adapters build their
HTTP client from the pin the endpoint row stores. A pin that can't be read
never becomes "no pin".
"""

# ruff: noqa: F811
from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

from agent.tools.image_generation import adapters  # noqa: E402
from orchestrator.services import message_triage, transcribe, tts  # noqa: E402
from orchestrator.services import dispatch_credentials as dc  # noqa: E402
from orchestrator.services.capability_credentials import (  # noqa: E402
    CapabilityCredentials,
    resolve_capability_credentials,
)
from shared.tls_pinning import (  # noqa: E402
    CertificateChangedError,
    TlsPin,
    pin_failure_in,
)
from tests.tls_pin_fixtures import tls_certs, tls_server  # noqa: E402, F401

ROUTES = {
    ("POST", "/v1/audio/transcriptions"): {"text": "hello"},
    ("POST", "/v1/audio/speech"): {"audio": "bytes"},
    ("POST", "/v1/images/generations"): {"data": [{"b64_json": "aGk="}]},
}


@pytest.fixture
def pins(tls_certs):
    return {
        "leaf": TlsPin.from_pem(tls_certs.leaf.read_text()),
        "swap": TlsPin.from_pem(tls_certs.swap.read_text()),
        "leaf_pem": tls_certs.leaf.read_text(),
    }


# --- the resolver ---------------------------------------------------------------


def _catalog_row(**over):
    row = {
        "catalog_id": "c1",
        "provider_kind": "endpoint",
        "provider_ref": "ep",
        "params_json": {},
        "endpoint_base_url": "https://127.0.0.1:1/v1",
        "api_key": "k",
        "endpoint_tls_pin_cert": None,
        "endpoint_tls_pin_sha256": None,
    }
    row.update(over)
    return row


def _db(row=None, **extra):
    db = MagicMock()
    db.resolve_default_for_capability = AsyncMock(return_value="m")
    db.resolve_catalog_model = AsyncMock(return_value=row)
    for name, value in extra.items():
        setattr(db, name, AsyncMock(return_value=value))
    return db


async def _resolve(db):
    return await resolve_capability_credentials(
        capability="whisper",
        user_settings={},
        user_id="u",
        resolved_keys={},
        postgres_db=db,
    )


class TestResolver:
    @pytest.mark.asyncio
    async def test_an_endpoint_row_carries_its_pin(self, pins):
        creds = await _resolve(
            _db(_catalog_row(endpoint_tls_pin_cert=pins["leaf_pem"]))
        )
        assert creds.tls_pin == pins["leaf"]

    @pytest.mark.asyncio
    async def test_a_provider_key_row_has_no_pin(self):
        row = _catalog_row(provider_kind="system", endpoint_base_url=None)
        creds = await _resolve(_db(row))
        assert creds is not None and creds.tls_pin is None

    @pytest.mark.asyncio
    async def test_a_pin_on_a_plain_http_url_is_not_applied(self, pins):
        row = _catalog_row(
            endpoint_base_url="http://h/v1", endpoint_tls_pin_cert=pins["leaf_pem"]
        )
        creds = await _resolve(_db(row))
        assert creds is not None and creds.tls_pin is None

    @pytest.mark.asyncio
    async def test_an_unreadable_pin_resolves_nothing(self):
        row = _catalog_row(endpoint_tls_pin_cert="not a certificate")
        assert await _resolve(_db(row)) is None

    @pytest.mark.asyncio
    async def test_a_fingerprint_without_its_certificate_resolves_nothing(self):
        row = _catalog_row(endpoint_tls_pin_sha256="AA:BB")
        assert await _resolve(_db(row)) is None

    @pytest.mark.asyncio
    async def test_the_legacy_endpoint_path_carries_the_pin(self, pins):
        meta = SimpleNamespace(
            endpoint_id="ep", api_key_ref=None, provider="openai", served_model_id=None
        )
        endpoint = {
            "base_url": "https://127.0.0.1:1/v1",
            "api_key": "k",
            "tls_pin_cert": pins["leaf_pem"],
        }
        db = _db(None, get_user_llm_endpoint=endpoint)
        with patch(
            "orchestrator.services.capability_credentials._resolve_model",
            AsyncMock(return_value=meta),
        ):
            creds = await _resolve(db)
        assert creds.tls_pin == pins["leaf"]

    @pytest.mark.asyncio
    async def test_an_unreadable_legacy_pin_resolves_nothing(self):
        meta = SimpleNamespace(
            endpoint_id="ep", api_key_ref=None, provider="openai", served_model_id=None
        )
        endpoint = {
            "base_url": "https://127.0.0.1:1/v1",
            "api_key": "k",
            "tls_pin_cert": "garbage",
        }
        db = _db(None, get_user_llm_endpoint=endpoint)
        with patch(
            "orchestrator.services.capability_credentials._resolve_model",
            AsyncMock(return_value=meta),
        ):
            assert await _resolve(db) is None


# --- transcription ----------------------------------------------------------------


def _stt_db():
    db = MagicMock()
    db.get_user_settings = AsyncMock(return_value={})
    db.resolve_api_keys_for_job = AsyncMock(return_value={})
    return db


async def _transcribe(creds):
    with patch.object(
        transcribe, "resolve_capability_credentials", AsyncMock(return_value=creds)
    ):
        return await transcribe.transcribe_thread_audio(
            audio_bytes=b"abc", filename="v.webm", user_id="u", postgres_db=_stt_db()
        )


class TestTranscription:
    @pytest.mark.asyncio
    async def test_the_client_gets_the_pinned_transport(self, pins):
        client = MagicMock()
        client.audio.transcriptions.create = AsyncMock(return_value="hi")
        client.close = AsyncMock()
        cls = MagicMock(return_value=client)
        creds = CapabilityCredentials(
            model="w", base_url="https://127.0.0.1:1/v1", api_key="k",
            tls_pin=pins["leaf"],
        )  # fmt: skip
        with patch.object(transcribe, "AsyncOpenAI", cls):
            assert await _transcribe(creds) == "hi"
        assert cls.call_args.kwargs["http_client"] is not None

    @pytest.mark.asyncio
    async def test_an_unpinned_client_gets_no_http_client(self):
        client = MagicMock()
        client.audio.transcriptions.create = AsyncMock(return_value="hi")
        client.close = AsyncMock()
        cls = MagicMock(return_value=client)
        creds = CapabilityCredentials(model="w", api_key="k")
        with patch.object(transcribe, "AsyncOpenAI", cls):
            await _transcribe(creds)
        assert cls.call_args.kwargs.get("http_client") is None

    @pytest.mark.asyncio
    async def test_end_to_end_against_the_pinned_server(self, tls_certs, pins):
        with tls_server(tls_certs.leaf, tls_certs.leaf_key, ROUTES) as srv:
            creds = CapabilityCredentials(
                model="w", base_url=srv.url, api_key="k", tls_pin=pins["leaf"]
            )
            assert await _transcribe(creds) == "hello"
            assert [r["path"] for r in srv.requests] == ["/v1/audio/transcriptions"]

    @pytest.mark.asyncio
    async def test_a_changed_certificate_fails_and_sends_nothing(self, tls_certs, pins):
        with tls_server(tls_certs.swap, tls_certs.swap_key, ROUTES) as srv:
            creds = CapabilityCredentials(
                model="w", base_url=srv.url, api_key="k", tls_pin=pins["leaf"]
            )
            with pytest.raises(transcribe.TranscriptionError) as err:
                await _transcribe(creds)
            assert srv.requests == []
        assert isinstance(pin_failure_in(err.value), CertificateChangedError)
        assert "TLS certificate changed" in str(err.value)


# --- TTS --------------------------------------------------------------------------


def _recording_openai():
    client = MagicMock()
    client.close = AsyncMock()
    boom = AsyncMock(side_effect=RuntimeError("stop"))
    client.chat.completions.create = boom
    client.audio.speech.create = boom
    return MagicMock(return_value=client)


class TestTts:
    def test_one_client_builder(self, pins):
        with patch.object(tts, "AsyncOpenAI") as cls:
            tts._openai_client(
                base_url="https://127.0.0.1:1/v1", api_key="k",
                tls_pin=pins["leaf"], timeout=7.0,
            )  # fmt: skip
        kwargs = cls.call_args.kwargs
        assert kwargs["max_retries"] == 0 and kwargs["timeout"] == 7.0
        assert kwargs["http_client"] is not None

    def test_the_builder_without_a_pin_passes_none(self):
        with patch.object(tts, "AsyncOpenAI") as cls:
            tts._openai_client(base_url=None, api_key="k", tls_pin=None, timeout=7.0)
        assert cls.call_args.kwargs["http_client"] is None

    def test_a_pin_with_no_https_url_is_refused(self, pins):
        with pytest.raises(ValueError):
            tts._openai_client(
                base_url="http://h/v1", api_key="k", tls_pin=pins["leaf"], timeout=7.0
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "stage", ["formulate", "synthesize", "resplit", "clean", "stream"]
    )
    async def test_every_stage_builds_a_pinned_client(self, stage, pins):
        url = "https://127.0.0.1:1/v1"
        pin = pins["leaf"]
        cls = _recording_openai()
        with patch.object(tts, "AsyncOpenAI", cls):
            if stage == "formulate":
                await tts._formulate_for_speech(
                    "text " + stage, model="m", base_url=url, api_key="k", tls_pin=pin
                )
            elif stage == "synthesize":
                await tts._synthesize_speech(
                    "t", model="m", voice="v", base_url=url, api_key="k", tls_pin=pin
                )
            elif stage == "resplit":
                await tts._resplit_oversized(
                    ["x" * (tts.TTS_CHUNK_LIMIT + 1)],
                    model="m", base_url=url, api_key="k", tls_pin=pin,
                )  # fmt: skip
            elif stage == "clean":
                await tts._llm_clean_and_chunk(
                    "t", model="m", base_url=url, api_key="k", tls_pin=pin
                )
            else:
                aux = CapabilityCredentials(
                    model="a", base_url=url, api_key="k", tls_pin=pin
                )
                tts_creds = CapabilityCredentials(model="t", api_key="k")
                db = _stt_db()
                with patch.object(
                    tts,
                    "_resolve_capability_credentials",
                    AsyncMock(side_effect=[tts_creds, aux]),
                ):
                    async for _ in tts.stream_tts_chunks(
                        content="hello there", user_id="u", postgres_db=db,
                    ):  # fmt: skip
                        pass
        assert cls.call_args.kwargs["http_client"] is not None

    @pytest.mark.asyncio
    async def test_the_callers_pass_the_creds_pin_to_synthesis(self, pins):
        tts_creds = CapabilityCredentials(
            model="t", base_url="https://127.0.0.1:1/v1", api_key="k",
            tls_pin=pins["leaf"], params={},
        )  # fmt: skip
        seen = {}

        async def fake(text, **kw):
            seen.update(kw)
            return b"mp3"

        db = _stt_db()
        db.get_user_settings = AsyncMock(return_value={})
        with (
            patch.object(
                tts,
                "_resolve_capability_credentials",
                AsyncMock(return_value=tts_creds),
            ),
            patch.object(tts, "_synthesize_speech", fake),
        ):
            await tts.generate_message_tts(
                content="hi",
                user_id="u",
                postgres_db=db,
                reformulate=False,
                language="en",
            )
        assert seen["tls_pin"] == pins["leaf"]

    @pytest.mark.asyncio
    async def test_end_to_end_against_the_pinned_server(self, tls_certs, pins):
        with tls_server(tls_certs.leaf, tls_certs.leaf_key, ROUTES) as srv:
            audio = await tts._synthesize_speech(
                "hello", model="m", voice="v", base_url=srv.url, api_key="k",
                tls_pin=pins["leaf"],
            )  # fmt: skip
            assert audio
            assert [r["path"] for r in srv.requests] == ["/v1/audio/speech"]

    @pytest.mark.asyncio
    async def test_a_changed_certificate_says_so_and_sends_nothing(
        self, tls_certs, pins
    ):
        with tls_server(tls_certs.swap, tls_certs.swap_key, ROUTES) as srv:
            with pytest.raises(tts.TtsSynthesisError) as err:
                await tts._synthesize_speech(
                    "hello", model="m", voice="v", base_url=srv.url, api_key="k",
                    tls_pin=pins["leaf"],
                )  # fmt: skip
            assert srv.requests == []
        assert "TLS certificate changed" in str(err.value)


# --- message triage ---------------------------------------------------------------


def _triage_db(endpoint):
    return SimpleNamespace(
        get_default_llm_model=AsyncMock(return_value="gemma"),
        get_system_api_key=AsyncMock(return_value="sk"),
        list_models=AsyncMock(
            return_value=[
                {"model_id": "gemma", "provider_kind": "endpoint", "provider_ref": "e"}
            ]
        ),
        get_system_llm_endpoint=AsyncMock(return_value=endpoint),
    )


class TestTriage:
    @pytest.mark.asyncio
    async def test_the_endpoint_rows_pin_is_the_fourth_value(self, pins):
        db = _triage_db(
            {"base_url": "https://127.0.0.1:1/v1", "tls_pin_cert": pins["leaf_pem"]}
        )
        resolved = await message_triage._resolve_triage_config(db)
        assert resolved == ("gemma", "https://127.0.0.1:1/v1", "sk", pins["leaf"])

    @pytest.mark.asyncio
    async def test_no_pin_for_an_unpinned_or_default_route(self):
        db = _triage_db({"base_url": "https://127.0.0.1:1/v1"})
        assert (await message_triage._resolve_triage_config(db))[3] is None
        bare = SimpleNamespace(
            get_default_llm_model=AsyncMock(return_value="gpt-4o"),
            get_system_api_key=AsyncMock(return_value="sk"),
        )
        assert (await message_triage._resolve_triage_config(bare))[3] is None

    @pytest.mark.asyncio
    async def test_an_unreadable_pin_resolves_nothing(self):
        db = _triage_db({"base_url": "https://127.0.0.1:1/v1", "tls_pin_cert": "junk"})
        assert await message_triage._resolve_triage_config(db) is None

    @pytest.mark.asyncio
    async def test_the_triage_call_verifies_the_pin(self, tls_certs, pins):
        reply = {"choices": [{"message": {"content": '{"action":"interrupt"}'}}]}
        routes = {("POST", "/v1/chat/completions"): reply}
        db = _triage_db(None)
        with tls_server(tls_certs.leaf, tls_certs.leaf_key, routes) as srv:
            db.get_system_llm_endpoint = AsyncMock(
                return_value={"base_url": srv.url, "tls_pin_cert": pins["leaf_pem"]}
            )
            out = await message_triage.triage_message("m", "s", "d", db=db)
            assert out["action"] == "interrupt"
            assert len(srv.requests) == 1

    @pytest.mark.asyncio
    async def test_a_changed_certificate_queues_and_sends_nothing(
        self, tls_certs, pins
    ):
        db = _triage_db(None)
        with tls_server(tls_certs.swap, tls_certs.swap_key) as srv:
            db.get_system_llm_endpoint = AsyncMock(
                return_value={"base_url": srv.url, "tls_pin_cert": pins["leaf_pem"]}
            )
            out = await message_triage.triage_message("m", "s", "d", db=db)
            assert out["action"] == "queue"
            assert srv.requests == []


# --- image injection ----------------------------------------------------------------


async def _inject(creds, config=None):
    deps = SimpleNamespace(store=MagicMock(), logger=MagicMock())
    deps.store.get_system_setting = AsyncMock(return_value=None)
    with patch(
        "orchestrator.services.capability_credentials.resolve_capability_credentials",
        AsyncMock(return_value=creds),
    ):
        return await dc.inject_image_credentials(
            {} if config is None else config,
            user_settings={}, user_id="u", resolved_keys={}, dependencies=deps,
        )  # fmt: skip


def _img_creds(**over):
    base = dict(
        model="img", base_url="https://127.0.0.1:1/v1", api_key="k",
        provider="openai-images", params={"provider": "openai-images"},
    )  # fmt: skip
    base.update(over)
    return CapabilityCredentials(**base)


class TestImageInjection:
    @pytest.mark.asyncio
    async def test_a_pinned_endpoint_writes_the_wire_value(self, pins):
        out = await _inject(_img_creds(tls_pin=pins["leaf"]))
        assert out["image"]["tls_pinned_cert"] == pins["leaf"].wire

    @pytest.mark.asyncio
    async def test_an_unpinned_endpoint_writes_none(self):
        out = await _inject(_img_creds())
        assert out["image"]["tls_pinned_cert"] is None

    @pytest.mark.asyncio
    async def test_a_planted_pin_is_replaced_by_the_rows(self, pins):
        planted = {"image": {"tls_pinned_cert": "PLANTED"}}
        out = await _inject(_img_creds(tls_pin=pins["leaf"]), planted)
        assert out["image"]["tls_pinned_cert"] == pins["leaf"].wire
        out = await _inject(_img_creds(), {"image": {"tls_pinned_cert": "PLANTED"}})
        assert out["image"]["tls_pinned_cert"] is None

    @pytest.mark.asyncio
    async def test_a_planted_pin_is_gone_when_nothing_resolves(self):
        out = await _inject(None, {"image": {"tls_pinned_cert": "PLANTED"}})
        assert "image" not in out

    def test_redaction_strips_the_image_pin(self):
        from orchestrator.security.config_redaction import redact_config_override

        out = redact_config_override(
            {"image": {"base_url": "https://h/v1", "tls_pinned_cert": "PIN"}}
        )
        assert "tls_pinned_cert" not in out["image"]
        assert out["image"]["base_url"] == "https://h/v1"


# --- image adapters -----------------------------------------------------------------


def _section(url, pin):
    return {
        "provider": "openai-images", "base_url": url, "api_key": "k",
        "model": "gpt-image-2", "tls_pinned_cert": pin,
    }  # fmt: skip


REQ = adapters.ImageRequest(prompt="a cat")


class TestImageAdapters:
    @pytest.mark.asyncio
    async def test_the_pinned_server_is_reached(self, tls_certs, pins):
        with tls_server(tls_certs.leaf, tls_certs.leaf_key, ROUTES) as srv:
            result = await adapters.call_openai_images(
                _section(srv.url, pins["leaf"].wire), REQ
            )
            assert len(result.images) == 1
            assert [r["path"] for r in srv.requests] == ["/v1/images/generations"]

    @pytest.mark.asyncio
    async def test_a_changed_certificate_fails_with_the_pin_message(
        self, tls_certs, pins
    ):
        with tls_server(tls_certs.swap, tls_certs.swap_key, ROUTES) as srv:
            with pytest.raises(adapters.ImageProviderError) as err:
                await adapters.call_openai_images(
                    _section(srv.url, pins["leaf"].wire), REQ
                )
            assert srv.requests == []
        assert "TLS certificate changed" in str(err.value)
        assert isinstance(pin_failure_in(err.value), CertificateChangedError)

    @pytest.mark.asyncio
    async def test_gemini_verifies_the_pin_too(self, tls_certs, pins):
        with tls_server(tls_certs.swap, tls_certs.swap_key, ROUTES) as srv:
            section = {**_section(srv.url, pins["leaf"].wire), "provider": "gemini"}
            with pytest.raises(adapters.ImageProviderError) as err:
                await adapters.call_gemini(section, REQ)
            assert srv.requests == []
        assert "TLS certificate changed" in str(err.value)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("call", ["call_openai_images", "call_gemini"])
    @pytest.mark.parametrize("bad", ["garbage!", "unreadable"])
    async def test_an_unreadable_pin_fails_and_never_calls_unpinned(
        self, tls_certs, call, bad
    ):
        with tls_server(tls_certs.leaf, tls_certs.leaf_key, ROUTES) as srv:
            with pytest.raises(adapters.ImageProviderError) as err:
                await getattr(adapters, call)(_section(srv.url, bad), REQ)
            assert srv.requests == []
        assert "pinned certificate" in str(err.value)

    @pytest.mark.asyncio
    async def test_a_caller_supplied_client_is_used_as_is(self):
        handler = httpx.MockTransport(
            lambda request: httpx.Response(200, json={"data": [{"b64_json": "aGk="}]})
        )
        async with httpx.AsyncClient(transport=handler) as client:
            result = await adapters.call_openai_images(
                _section("https://h/v1", "garbage!"), REQ, client=client
            )
        assert len(result.images) == 1

    @pytest.mark.asyncio
    async def test_no_pin_keeps_the_plain_client(self):
        with patch.object(adapters.httpx, "AsyncClient") as cls:
            cls.return_value.post = AsyncMock(side_effect=httpx.ConnectError("down"))
            cls.return_value.aclose = AsyncMock()
            with pytest.raises(adapters.ImageProviderError):
                await adapters.call_openai_images(_section("https://h/v1", None), REQ)
        assert "transport" not in cls.call_args.kwargs


class TestImageAdapterEdges:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("call", ["call_openai_images", "call_gemini"])
    async def test_a_pin_that_openssl_cannot_load_has_the_certificate_kind(self, call):
        # base64 of one NUL byte-ish: not a DER certificate
        with pytest.raises(adapters.ImageProviderError) as err:
            await getattr(adapters, call)(_section("https://h/v1", "MAA="), REQ)
        assert err.value.kind == "certificate"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("call", ["call_openai_images", "call_gemini"])
    async def test_a_pin_with_a_plain_http_url_is_refused(self, pins, call):
        with pytest.raises(adapters.ImageProviderError) as err:
            await getattr(adapters, call)(
                _section("http://h/v1", pins["leaf"].wire), REQ
            )
        assert err.value.kind == "certificate"
        assert "https" in str(err.value)

    @pytest.mark.asyncio
    async def test_a_pin_failure_has_the_certificate_kind(self, tls_certs, pins):
        with tls_server(tls_certs.swap, tls_certs.swap_key, ROUTES) as srv:
            with pytest.raises(adapters.ImageProviderError) as err:
                await adapters.call_openai_images(
                    _section(srv.url, pins["leaf"].wire), REQ
                )
        assert err.value.kind == "certificate"


# --- the image tool ---------------------------------------------------------------


class TestImageTool:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("pin", ["garbage!", "MAA="])
    async def test_a_pin_failure_returns_the_paid_images_and_says_what_to_do(self, pin):
        from agent.tools.image_generation.tool import create_image_generation_tools

        store = MagicMock()
        store.reserve_image_generation = AsyncMock(return_value=(True, 0))
        store.release_image_generation = AsyncMock()
        context = MagicMock()
        context.config = {
            "image": {
                **_section("https://127.0.0.1:1/v1", pin),
                "billing": "api",
                "max_paid_images": 10,
            }
        }
        context.get_phase_multimodal.return_value = True
        context.has_workspace.return_value = True
        context.thread_id = "t1"
        context.postgres_db = store
        (generate_image,) = create_image_generation_tools(context)
        out = await generate_image.ainvoke({"prompt": "a cat", "n": 2})
        assert "(certificate)" in out
        assert "re-trust it in Admin" in out and "do not retry" in out.lower()
        assert ".." not in out
        store.release_image_generation.assert_awaited_once_with("thread", "t1", 2)


# --- the transcription route --------------------------------------------------------


class TestVoiceRoute:
    @pytest.mark.asyncio
    async def test_a_pin_failure_reaches_the_user(self):
        from fastapi import HTTPException

        from orchestrator.services import voice

        pin_error = CertificateChangedError("h", 443, "AA", "BB")
        err = transcribe.TranscriptionError(str(pin_error))
        err.__cause__ = pin_error
        deps = SimpleNamespace(store=MagicMock(), ledger=None)
        with patch.object(
            transcribe, "transcribe_thread_audio", AsyncMock(side_effect=err)
        ):
            with pytest.raises(HTTPException) as caught:
                await voice.transcribe_audio(
                    thread_id=None, user_id="u", data=b"abc", filename="v.webm",
                    dependencies=deps,
                )  # fmt: skip
        assert caught.value.status_code == 502
        assert "TLS certificate changed" in caught.value.detail

    @pytest.mark.asyncio
    async def test_other_failures_stay_generic(self):
        from fastapi import HTTPException

        from orchestrator.services import voice

        deps = SimpleNamespace(store=MagicMock(), ledger=None)
        with patch.object(
            transcribe,
            "transcribe_thread_audio",
            AsyncMock(side_effect=transcribe.TranscriptionError("x")),
        ):
            with pytest.raises(HTTPException) as caught:
                await voice.transcribe_audio(
                    thread_id=None, user_id="u", data=b"abc", filename="v.webm",
                    dependencies=deps,
                )  # fmt: skip
        assert caught.value.detail == "Transcription failed"


# --- search and fetch fail closed on a pinned endpoint ---------------------------------


async def _inject_search(by_capability, config=None, fallback=None):
    deps = SimpleNamespace(store=MagicMock(), logger=MagicMock())

    async def resolver(**kw):
        if kw.get("setting_key") == "default_search_fallback_model":
            return fallback
        return by_capability.get(kw["capability"])

    with patch(
        "orchestrator.services.capability_credentials.resolve_capability_credentials",
        resolver,
    ):
        out = await dc.inject_search_credentials(
            {} if config is None else config,
            user_settings={}, user_id="u", resolved_keys={}, dependencies=deps,
        )  # fmt: skip
    return out, deps


def _search_creds(catalog_id="c", **over):
    base = dict(
        model="s", base_url="https://127.0.0.1:1/v1", api_key="k",
        provider="tavily", params={"ops": ["search", "fetch"]}, catalog_id=catalog_id,
    )  # fmt: skip
    base.update(over)
    return CapabilityCredentials(**base)


class TestSearchFailsClosed:
    @pytest.mark.asyncio
    async def test_a_pinned_search_and_fetch_row_is_dropped_and_logged(self, pins):
        pinned = _search_creds(tls_pin=pins["leaf"])
        out, deps = await _inject_search(
            {"search": pinned, "fetch": pinned},
            {"research": {"search": {"api_key": "stale"}}},
        )
        assert "search" not in out.get("research", {}) and "fetch" not in out.get(
            "research", {}
        )
        assert deps.logger.error.call_count == 2
        assert "pinned" in deps.logger.error.call_args.args[0]

    @pytest.mark.asyncio
    async def test_an_unpinned_row_is_still_injected(self):
        plain = _search_creds()
        out, _ = await _inject_search({"search": plain, "fetch": plain})
        assert out["research"]["search"]["provider"] == "tavily"

    @pytest.mark.asyncio
    async def test_a_pinned_fallback_is_dropped(self, pins):
        out, deps = await _inject_search(
            {"search": _search_creds("a")},
            fallback=_search_creds("b", tls_pin=pins["leaf"]),
        )
        assert "search_fallback" not in out.get("research", {})
        deps.logger.error.assert_called_once()

    @pytest.mark.asyncio
    async def test_an_unreadable_pin_resolves_nothing_so_nothing_is_injected(self):
        out, _ = await _inject_search({"search": None, "fetch": None})
        assert "search" not in out.get("research", {})


class TestHelpersRequireThePin:
    @pytest.mark.asyncio
    async def test_a_missed_caller_fails_with_type_error(self):
        with pytest.raises(TypeError):
            await tts._formulate_for_speech("t", model="m", base_url=None, api_key="k")
