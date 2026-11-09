"""A chat endpoint whose stored key will not decrypt refuses the dispatch.

Regression for knowledge-base/knowledge/issues/
chat_endpoint_undecryptable_key_sent_keyless.md: when an endpoint's stored API
key failed to decrypt (the encryption key was rotated, or the row was copied
from another installation), the chat-model paths sent the endpoint's base URL
with no key and logged nothing. The provider answered 401 and the job failed
with an error that pointed nowhere near the cause.

The store's ``api_key_stored`` flag (whether the row holds a ciphertext at
all, set before it decrypts) tells that case from an endpoint registered
without a key; the decrypted ``api_key`` is ``None`` for both
(``6210d49d8`` drew the same line for the flat-env slots):

* a stored key that did not decrypt -> the slot is refused through the usual
  ``ModelUnavailable`` refusal, naming the endpoint, and an ERROR is logged;
* no stored key -> the endpoint keeps running keyless, exactly as before.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.services import dispatch_credentials as dc
from orchestrator.services import job_dispatch_credentials as jdc
from orchestrator.services import model_availability as ma
from orchestrator.services.config_resolver import inject_blob_credentials
from shared.runtime.core.model_registry import ModelMeta, UnknownModelError

LOGGER = logging.getLogger("test.undecryptable-chat-key")

BROKEN_ID = "33333333-3333-3333-3333-333333333333"
KEYLESS_ID = "22222222-2222-2222-2222-222222222222"
KEYED_ID = "55555555-5555-5555-5555-555555555555"
BROKEN_URL = "https://vllm.university.internal/v1"
KEYLESS_URL = "http://tei.internal:8080/v1"
KEYED_URL = "https://gateway.internal/v1"

MESSAGE = (
    "The model endpoint 'university-vllm' has a stored API key that can't be "
    "decrypted. Re-add the key in Admin → Models."
)

ENDPOINT_ROWS = {
    # A key is stored but did not decrypt (_decrypt_stored returned None).
    BROKEN_ID: {
        "id": BROKEN_ID,
        "label": "university-vllm",
        "base_url": BROKEN_URL,
        "api_key": None,
        "api_key_stored": True,
    },
    # Registered without a key: the column is NULL.
    KEYLESS_ID: {
        "id": KEYLESS_ID,
        "label": "tei",
        "base_url": KEYLESS_URL,
        "api_key": None,
        "api_key_stored": False,
    },
    KEYED_ID: {
        "id": KEYED_ID,
        "label": "gateway",
        "base_url": KEYED_URL,
        "api_key": "sk-gateway",
        "api_key_stored": True,
    },
}


def _meta(model_id: str, endpoint_id: str) -> ModelMeta:
    return ModelMeta(
        model_id=model_id,
        provider="openai",
        family="gpt",
        display_name=model_id,
        origin="catalog",
        endpoint_id=endpoint_id,
        api_key_ref="openai",
        capability="chat",
    )


METAS = {
    "broken-chat": _meta("broken-chat", BROKEN_ID),
    "broken-aux": _meta("broken-aux", BROKEN_ID),
    "keyless-chat": _meta("keyless-chat", KEYLESS_ID),
    "keyed-chat": _meta("keyed-chat", KEYED_ID),
}


async def _resolve(model_id, user_id=None, capability="chat"):
    try:
        return METAS[model_id]
    except KeyError:
        raise UnknownModelError(model_id) from None


def _store(defaults: dict[str, str] | None = None):
    defaults = defaults or {}

    async def get_user_llm_endpoint(endpoint_id):
        row = ENDPOINT_ROWS.get(endpoint_id)
        return dict(row) if row else None

    async def resolve_default_for_capability(capability):
        return defaults.get(capability)

    return SimpleNamespace(
        get_user_llm_endpoint=AsyncMock(side_effect=get_user_llm_endpoint),
        resolve_api_keys_for_job=AsyncMock(return_value={"openai": "sk-system"}),
        resolve_default_for_capability=AsyncMock(
            side_effect=resolve_default_for_capability
        ),
        get_user_settings=AsyncMock(return_value={}),
    )


def _service_deps(store=None) -> dc.DispatchCredentialDependencies:
    return dc.DispatchCredentialDependencies(
        store=store or _store(),
        logger=LOGGER,
        resolve_model=AsyncMock(side_effect=_resolve),
    )


def _job_deps(store) -> jdc.DispatchCredentialDependencies:
    service = _service_deps(store)

    async def model_creds(**kwargs):
        return await dc.inject_model_credentials(**kwargs, dependencies=service)

    async def env_creds(**kwargs):
        return await dc.inject_env_key_credentials(**kwargs, dependencies=service)

    return jdc.DispatchCredentialDependencies(
        store=store,
        logger=LOGGER,
        resolve_model=service.resolve_model,
        inject_model_credentials=model_creds,
        inject_env_key_credentials=env_creds,
        inject_search_credentials=AsyncMock(),
        inject_system_kb_embedding_profile=AsyncMock(return_value=None),
        dispatch_llm_provider_fallback=dc.dispatch_llm_provider_fallback,
        nested_model_slots=dc.nested_model_slots,
    )


JOB = {
    "id": "00000000-0000-0000-0000-000000000001",
    "user_id": "00000000-0000-0000-0000-0000000000aa",
    "project_id": None,
    "config_name": "worker_base",
}


def _errors(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]


@pytest.fixture(autouse=True)
def _no_search_credentials():
    with patch(
        "orchestrator.services.capability_credentials.resolve_capability_credentials",
        AsyncMock(return_value=None),
    ):
        yield


class TestModelSection:
    """``inject_model_credentials`` — every chat/auxiliary/roster section of a
    session, a model swap, and the job's nested and default slots."""

    @pytest.mark.asyncio
    async def test_a_key_that_did_not_decrypt_is_refused_and_logged(self, caplog):
        caplog.set_level(logging.DEBUG)
        section: dict = {"model": "broken-chat"}
        reason = await dc.inject_model_credentials(
            section=section,
            model_id="broken-chat",
            user_id="u",
            resolved_keys={"openai": "sk-system"},
            dependencies=_service_deps(),
        )
        assert reason == ma.REASON_ENDPOINT_KEY
        assert reason.endpoint == "university-vllm"
        # Never a half credential: no base_url without its key, and the
        # provider key never stands in for the endpoint's own.
        assert "base_url" not in section
        assert "api_key" not in section
        errors = _errors(caplog)
        assert len(errors) == 1
        assert "university-vllm" in errors[0]
        assert BROKEN_ID in errors[0]
        assert "decrypt" in errors[0]

    @pytest.mark.asyncio
    async def test_an_endpoint_registered_without_a_key_stays_keyless(self, caplog):
        caplog.set_level(logging.DEBUG)
        section: dict = {"model": "keyless-chat"}
        reason = await dc.inject_model_credentials(
            section=section,
            model_id="keyless-chat",
            user_id="u",
            resolved_keys={"openai": "sk-system"},
            dependencies=_service_deps(),
        )
        assert reason is None
        assert section["base_url"] == KEYLESS_URL
        assert "api_key" not in section
        assert not _errors(caplog)

    @pytest.mark.asyncio
    async def test_a_decrypted_key_is_injected(self):
        section: dict = {"model": "keyed-chat"}
        reason = await dc.inject_model_credentials(
            section=section,
            model_id="keyed-chat",
            user_id="u",
            resolved_keys={"openai": "sk-system"},
            dependencies=_service_deps(),
        )
        assert reason is None
        assert section["base_url"] == KEYED_URL
        assert section["api_key"] == "sk-gateway"


class TestMessage:
    def test_the_refusal_names_the_endpoint_and_where_to_fix_it(self):
        refusal = ma.ModelUnavailable(
            [
                ma.UnavailableModel(
                    "llm", "broken-chat", ma.EndpointKeyUnreadable("university-vllm")
                )
            ]
        )
        assert refusal.message(where=ma.WHERE_JOB) == MESSAGE
        assert refusal.message(where=ma.WHERE_SESSION, is_admin=True) == MESSAGE

    def test_the_detail_carries_the_reason_and_the_endpoint(self):
        detail = ma.ModelUnavailable(
            [
                ma.UnavailableModel(
                    "llm", "broken-chat", ma.EndpointKeyUnreadable("university-vllm")
                )
            ]
        ).detail(where=ma.WHERE_SESSION)
        assert detail["code"] == "model.unavailable"
        assert detail["message"] == MESSAGE
        assert detail["entries"] == [
            {
                "slot": "llm",
                "model": "broken-chat",
                "reason": "endpoint_key",
                "endpoint": "university-vllm",
            }
        ]

    def test_slots_on_one_endpoint_name_it_once(self):
        key = ma.EndpointKeyUnreadable("university-vllm")
        text = ma.render_unavailable_message(
            [
                ma.UnavailableModel("llm", "broken-chat", key),
                ma.UnavailableModel("auxiliary", "broken-aux", key),
            ],
            where=ma.WHERE_JOB,
        )
        assert text == MESSAGE

    def test_two_endpoints_are_both_named(self):
        text = ma.render_unavailable_message(
            [
                ma.UnavailableModel("llm", "a", ma.EndpointKeyUnreadable("one")),
                ma.UnavailableModel("auxiliary", "b", ma.EndpointKeyUnreadable("two")),
            ]
        )
        assert text == (
            "The model endpoints 'one', 'two' have stored API keys that can't be "
            "decrypted. Re-add the keys in Admin → Models."
        )

    def test_a_model_refusal_alongside_keeps_its_own_text(self):
        text = ma.render_unavailable_message(
            [
                ma.UnavailableModel(
                    "llm", "broken-chat", ma.EndpointKeyUnreadable("university-vllm")
                ),
                ma.UnavailableModel("auxiliary", "MiniMax-M3", "disabled"),
            ],
            where=ma.WHERE_JOB,
            is_admin=True,
        )
        assert text.startswith(MESSAGE + " The model `MiniMax-M3` (auxiliary model)")
        assert "the job's configuration" in text
        assert len(text) <= 240

    def test_the_reason_survives_a_copy(self):
        import copy
        import pickle

        reason = ma.EndpointKeyUnreadable("university-vllm")
        for clone in (copy.deepcopy(reason), pickle.loads(pickle.dumps(reason))):
            assert clone == ma.REASON_ENDPOINT_KEY
            assert clone.endpoint == "university-vllm"


class TestSession:
    """``inject_thread_dispatch_credentials`` behind ``inject_blob_credentials``
    — what every session attach (pinned and stateless) and model check runs."""

    @pytest.mark.asyncio
    async def test_the_session_delivery_is_refused_with_the_endpoint_named(
        self, caplog
    ):
        caplog.set_level(logging.DEBUG)
        deps = _service_deps()
        with pytest.raises(ma.ModelUnavailable) as raised:
            await inject_blob_credentials(
                {"agent": {"llm": {"model": "broken-chat", "base_url": None}}},
                lambda co: dc.inject_thread_dispatch_credentials(
                    co, user_id="u", dependencies=deps
                ),
            )
        assert raised.value.message(where=ma.WHERE_SESSION) == MESSAGE
        assert [e.slot for e in raised.value.entries] == ["llm"]
        assert any("university-vllm" in m for m in _errors(caplog))

    @pytest.mark.asyncio
    async def test_a_keyless_session_endpoint_still_delivers(self):
        deps = _service_deps()
        delivered = await inject_blob_credentials(
            {"agent": {"llm": {"model": "keyless-chat"}}},
            lambda co: dc.inject_thread_dispatch_credentials(
                co, user_id="u", dependencies=deps
            ),
        )
        assert delivered["agent"]["llm"]["base_url"] == KEYLESS_URL
        assert not delivered["agent"]["llm"].get("api_key")


class TestJob:
    """``job_dispatch_credentials.inject_dispatch_credentials`` — the start
    bundle of every worker job, pinned or stateless, and its resume."""

    @pytest.mark.asyncio
    async def test_the_jobs_own_model_is_refused_with_the_endpoint_named(self, caplog):
        caplog.set_level(logging.DEBUG)
        with pytest.raises(ma.ModelUnavailable) as raised:
            await jdc.inject_dispatch_credentials(
                JOB,
                {"llm": {"model": "broken-chat"}},
                dependencies=_job_deps(_store()),
            )
        assert raised.value.message(where=ma.WHERE_JOB) == MESSAGE
        assert [(e.slot, e.reason) for e in raised.value.entries] == [
            ("llm", "endpoint_key")
        ]
        errors = _errors(caplog)
        assert any("university-vllm" in m and JOB["id"] in m for m in errors)

    @pytest.mark.asyncio
    async def test_the_system_default_chat_model_is_refused_too(self):
        store = _store(defaults={"chat": "broken-chat"})
        with pytest.raises(ma.ModelUnavailable) as raised:
            await jdc.inject_dispatch_credentials(
                JOB, {}, dependencies=_job_deps(store)
            )
        assert raised.value.message(where=ma.WHERE_JOB) == MESSAGE

    @pytest.mark.asyncio
    async def test_an_auxiliary_default_on_that_endpoint_is_refused(self):
        store = _store(defaults={"auxiliary": "broken-aux"})
        with pytest.raises(ma.ModelUnavailable) as raised:
            await jdc.inject_dispatch_credentials(
                JOB,
                {"llm": {"model": "keyed-chat"}},
                dependencies=_job_deps(store),
            )
        assert [e.slot for e in raised.value.entries] == ["auxiliary"]
        assert raised.value.message(where=ma.WHERE_JOB) == MESSAGE

    @pytest.mark.asyncio
    async def test_a_flat_copy_beside_a_blob_only_logs_and_gets_no_half_credential(
        self, caplog
    ):
        caplog.set_level(logging.DEBUG)
        out = await jdc.inject_dispatch_credentials(
            JOB,
            {"llm": {"model": "broken-chat"}},
            strict=False,
            dependencies=_job_deps(_store()),
        )
        assert "base_url" not in out["llm"]
        assert "api_key" not in out["llm"]
        assert _errors(caplog)

    @pytest.mark.asyncio
    async def test_a_keyless_job_endpoint_still_dispatches(self, caplog):
        caplog.set_level(logging.DEBUG)
        out = await jdc.inject_dispatch_credentials(
            JOB,
            {"llm": {"model": "keyless-chat"}},
            dependencies=_job_deps(_store()),
        )
        assert out["llm"]["base_url"] == KEYLESS_URL
        assert "api_key" not in out["llm"]
        assert not _errors(caplog)
