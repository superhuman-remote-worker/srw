"""A keyless endpoint serves every slot; a key that failed is still refused.

Regression for knowledge-base/knowledge/issues/
keyless_endpoint_dropped_for_embedding_rerank_vision.md: an endpoint the
operator registered without an API key (a local TEI / vLLM over HTTP) reached
chat, but the flat-env slots (embedding, KB embedding, rerank, vision, ...)
were dropped by the half-credential guard, which read "no key" as "the stored
key failed to decrypt". The agent then failed the job: ``reranker needs a
base_url``.

The two cases are told apart by the store's ``api_key_stored`` flag (whether
the row holds a ciphertext at all), never by the decrypted value, which is
``None`` for both:

* no stored key -> the slot gets its base URL plus the codebase's keyless
  sentinel (``transport_resolution.NO_KEY_SENTINEL``), because the citation
  client refuses a custom endpoint with no key of its own;
* a stored key that did not decrypt, or a row that does not say -> refused,
  loudly, exactly as before.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from orchestrator.database.postgres import PostgresDB
from orchestrator.security.crypto import encrypt
from orchestrator.services import dispatch_credentials as dc
from orchestrator.services import job_dispatch_credentials as jdc
from shared.runtime.core.model_registry import ModelMeta, UnknownModelError
from shared.runtime.core.transport_resolution import (
    NO_KEY_SENTINEL,
    embedding_role_violation,
    env_endpoint_names,
    rerank_role_violation,
    resolve_citation_transport,
)

LOGGER = logging.getLogger("test.keyless-endpoint")

KEYLESS_ID = "22222222-2222-2222-2222-222222222222"
BROKEN_ID = "33333333-3333-3333-3333-333333333333"
UNFLAGGED_ID = "44444444-4444-4444-4444-444444444444"
KEYLESS_URL = "http://tei.internal:8080/v1"
BROKEN_URL = "https://broken.internal/v1"
SYSTEM_OPENAI_KEY = "sk-system-openai-SECRET"

ENDPOINT_ROWS = {
    # Registered without a key: the column is NULL.
    KEYLESS_ID: {
        "id": KEYLESS_ID,
        "label": "tei",
        "base_url": KEYLESS_URL,
        "api_key": None,
        "api_key_stored": False,
    },
    # A key is stored but did not decrypt (_decrypt_stored returned None).
    BROKEN_ID: {
        "id": BROKEN_ID,
        "label": "broken",
        "base_url": BROKEN_URL,
        "api_key": None,
        "api_key_stored": True,
    },
    # A row that does not say whether a key is stored: fail closed.
    UNFLAGGED_ID: {
        "id": UNFLAGGED_ID,
        "label": "unflagged",
        "base_url": BROKEN_URL,
        "api_key": None,
    },
}


def _meta(model_id: str, endpoint_id: str, capability: str = "chat") -> ModelMeta:
    return ModelMeta(
        model_id=model_id,
        provider="openai",
        family="gpt",
        display_name=model_id,
        origin="catalog",
        endpoint_id=endpoint_id,
        api_key_ref="openai",
        capability=capability,
    )


METAS = {
    "local-chat": _meta("local-chat", KEYLESS_ID),
    "local-embed": _meta("local-embed", KEYLESS_ID, "embedding"),
    "local-rerank": _meta("local-rerank", KEYLESS_ID, "rerank"),
    "local-vision": _meta("local-vision", KEYLESS_ID, "vision"),
    "broken-embed": _meta("broken-embed", BROKEN_ID, "embedding"),
    "unflagged-embed": _meta("unflagged-embed", UNFLAGGED_ID, "embedding"),
}


class _Store:
    def __init__(self, defaults: dict[str, str] | None = None):
        self.defaults = defaults or {}
        self.endpoint_reads: list[str] = []

    async def resolve_api_keys_for_job(self, *, user_id=None, project_id=None):
        return {"openai": SYSTEM_OPENAI_KEY}

    async def get_user_settings(self, user_id):
        return {}

    async def get_user_llm_endpoint(self, endpoint_id):
        self.endpoint_reads.append(endpoint_id)
        row = ENDPOINT_ROWS.get(endpoint_id)
        return dict(row) if row else None

    async def resolve_default_for_capability(self, capability):
        return self.defaults.get(capability)


async def _resolve(model_id, user_id=None, capability="chat"):
    try:
        return METAS[model_id]
    except KeyError:
        raise UnknownModelError(model_id) from None


def _service_deps(store: _Store) -> dc.DispatchCredentialDependencies:
    return dc.DispatchCredentialDependencies(
        store=store, logger=LOGGER, resolve_model=AsyncMock(side_effect=_resolve)
    )


async def _inject(prefix: str, model_id: str, capability: str, store: _Store):
    env: dict = {}
    await dc.inject_env_key_credentials(
        env_keys=env,
        prefix=prefix,
        model_id=model_id,
        user_id="u",
        resolved_keys={"openai": SYSTEM_OPENAI_KEY},
        capability=capability,
        dependencies=_service_deps(store),
    )
    return env


# Every flat-env slot that rides inject_env_key_credentials.
FLAT_ENV_SLOTS = [
    ("EMBEDDING", "local-embed", "embedding"),
    ("KB_EMBEDDING", "local-embed", "embedding"),
    ("RERANK", "local-rerank", "rerank"),
    ("VISION", "local-vision", "vision"),
    ("WHISPER", "local-chat", "chat"),
    ("TTS", "local-chat", "chat"),
    ("CITATION_LLM", "local-chat", "chat"),
]


class TestNoStoredKey:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(("prefix", "model_id", "capability"), FLAT_ENV_SLOTS)
    async def test_slot_gets_the_endpoint_and_the_keyless_sentinel(
        self, prefix, model_id, capability
    ):
        store = _Store()
        env = await _inject(prefix, model_id, capability, store)
        assert store.endpoint_reads == [KEYLESS_ID]
        assert env[env_endpoint_names(prefix)[0]] == KEYLESS_URL
        assert env[f"{prefix}_API_KEY"] == NO_KEY_SENTINEL
        # A stored provider key never rides along to the keyless host.
        assert SYSTEM_OPENAI_KEY not in env.values()

    @pytest.mark.asyncio
    async def test_agent_side_readers_accept_the_keyless_slots(self):
        store = _Store()
        env: dict = {}
        for prefix, model_id, capability in FLAT_ENV_SLOTS:
            env.update(await _inject(prefix, model_id, capability, store))
        # The checks the orchestrator runs before it spawns a pod.
        assert embedding_role_violation(env) is None
        assert rerank_role_violation(env) is None
        # The citation client refuses a custom endpoint with no key of its own.
        citation = resolve_citation_transport(env)
        assert citation.base_url == KEYLESS_URL
        assert citation.api_key == NO_KEY_SENTINEL

        from types import SimpleNamespace

        from agent.services.memory.plugins.reranker import resolve_reranker_transport

        cfg = SimpleNamespace(model=None, base_url=None, api_key=None)
        assert resolve_reranker_transport(cfg, env) == (
            "local-rerank",
            KEYLESS_URL,
            NO_KEY_SENTINEL,
        )


class TestStoredKeyFailed:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("model_id", ["broken-embed", "unflagged-embed"])
    async def test_half_credential_is_still_refused(self, model_id, caplog):
        caplog.set_level(logging.DEBUG)
        env = await _inject("EMBEDDING", model_id, "embedding", _Store())
        assert env == {"EMBEDDING_MODEL": model_id}
        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert errors
        assert METAS[model_id].endpoint_id in errors[0].getMessage()


class TestJobOnKeylessEndpoint:
    """The university repro: every default sits on one keyless endpoint."""

    def _deps(self, store: _Store) -> jdc.DispatchCredentialDependencies:
        service = _service_deps(store)

        async def model_creds(**kwargs):
            return await dc.inject_model_credentials(**kwargs, dependencies=service)

        async def env_creds(**kwargs):
            return await dc.inject_env_key_credentials(**kwargs, dependencies=service)

        async def kb_profile(env_keys):
            return await dc.inject_system_kb_embedding_profile(
                env_keys, dependencies=service
            )

        return jdc.DispatchCredentialDependencies(
            store=store,
            logger=LOGGER,
            resolve_model=service.resolve_model,
            inject_model_credentials=model_creds,
            inject_env_key_credentials=env_creds,
            inject_search_credentials=AsyncMock(),
            inject_system_kb_embedding_profile=kb_profile,
            dispatch_llm_provider_fallback=dc.dispatch_llm_provider_fallback,
            nested_model_slots=dc.nested_model_slots,
        )

    @pytest.mark.asyncio
    async def test_every_slot_reaches_the_keyless_endpoint(self, caplog):
        caplog.set_level(logging.DEBUG)
        store = _Store(
            defaults={
                "embedding": "local-embed",
                "rerank": "local-rerank",
                "vision": "local-vision",
            }
        )
        job = {
            "id": "00000000-0000-0000-0000-000000000001",
            "user_id": "00000000-0000-0000-0000-0000000000aa",
            "project_id": None,
            "config_name": "worker_base",
        }
        out = await jdc.inject_dispatch_credentials(
            job,
            {"llm": {"model": "local-chat"}},
            include_kb_profile=True,
            dependencies=self._deps(store),
        )
        assert out["llm"]["base_url"] == KEYLESS_URL
        env = out["env_keys"]
        for prefix in ("EMBEDDING", "KB_EMBEDDING", "RERANK", "VISION"):
            assert env[f"{prefix}_BASE_URL"] == KEYLESS_URL, prefix
            assert env[f"{prefix}_API_KEY"] == NO_KEY_SENTINEL, prefix
        # No "memory/KB will be unavailable" error for a slot that works.
        assert not [r for r in caplog.records if r.levelno >= logging.ERROR]


class TestStoreReportsWhetherAKeyIsStored:
    """``PostgresDB.get_user_llm_endpoint`` — the flag the dispatch relies on.

    The decrypted ``api_key`` is ``None`` both for a NULL column and for a
    ciphertext that did not decrypt; only ``api_key_stored`` tells them apart.
    """

    def _db(self, stored_api_key):
        conn = AsyncMock()
        conn.fetchrow = AsyncMock(
            return_value={
                "id": UUID(KEYLESS_ID),
                "user_id": None,
                "label": "probe",
                "base_url": KEYLESS_URL,
                "api_key": stored_api_key,
                "key_prefix": None,
                "transport_kind": None,
                "created_at": None,
                "updated_at": None,
            }
        )
        db = PostgresDB.__new__(PostgresDB)
        db._pool = MagicMock()
        db._connection_string = "test"
        db._queries = {}

        @asynccontextmanager
        async def _acquire():
            yield conn

        db.acquire = _acquire
        return db

    @pytest.mark.asyncio
    async def test_null_column_is_not_a_stored_key(self):
        row = await self._db(None).get_user_llm_endpoint(KEYLESS_ID)
        assert row["api_key"] is None
        assert row["api_key_stored"] is False

    @pytest.mark.asyncio
    async def test_decryptable_key_is_stored(self):
        row = await self._db(encrypt("sk-real")).get_user_llm_endpoint(KEYLESS_ID)
        assert row["api_key"] == "sk-real"
        assert row["api_key_stored"] is True

    @pytest.mark.asyncio
    async def test_key_that_fails_to_decrypt_is_still_stored(self, caplog):
        # A well-formed v1 ciphertext whose GCM tag does not verify: what a
        # rotated APP_ENCRYPTION_KEY or a tampered row looks like.
        _, nonce, _ = encrypt("other").split(":")
        _, _, body = encrypt("sk-real").split(":")
        caplog.set_level(logging.ERROR)
        row = await self._db(f"v1:{nonce}:{body}").get_user_llm_endpoint(KEYLESS_ID)
        assert row["api_key"] is None
        assert row["api_key_stored"] is True
        assert any("Failed to decrypt" in r.getMessage() for r in caplog.records)
