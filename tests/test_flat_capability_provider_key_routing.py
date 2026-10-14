"""Provider-key rows for the flat capabilities reach their provider's own API.

Regression for the 2026-10-14 university upgrade test (vault
``university-generator-upgrade-issues-2026-10-14.md``):

* APP-RR-1 — with OpenRouter provider-key rows as the embedding and rerank
  defaults, dispatch delivered ``RERANK_MODEL`` + ``RERANK_API_KEY`` but no
  base URL, so the agent's reranker raised ``reranker needs a base_url`` and
  every job failed 8 s after start.
* APP-EMB-1 — the same delivery left ``EMBEDDING_BASE_URL`` unset, so the
  embedding client defaulted to api.openai.com and sent the OpenRouter key
  there (``401 Incorrect API key provided: sk-or-v1****``).

The contract: a provider-key row's key travels only together with that
provider's own OpenAI-compatible base URL, for every capability that is
delivered as flat ``{PREFIX}_*`` env, on the job and the session path, and
again after the session copy is redacted and re-injected on resume. The agent
clients built from that delivery send the provider-native model id (OpenRouter
rejects the ``openrouter/`` routing prefix with 400) to that host and nowhere
else.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrator.security.config_redaction import redact_config_override
from orchestrator.services import dispatch_credentials as dc
from orchestrator.services import job_dispatch_credentials as jdc
from shared.runtime.core.model_registry import ModelMeta, UnknownModelError
from shared.runtime.core.transport_resolution import (
    embedding_role_violation,
    env_endpoint_names,
    rerank_role_violation,
    same_endpoint,
    wire_model_id,
)


LOGGER = logging.getLogger("test.flat-capability-routing")

OPENROUTER_URL = "https://openrouter.ai/api/v1"
OR_KEY = "sk-or-v1-university-SECRET"
ANTHROPIC_KEY = "sk-ant-SECRET"

CHAT = "openrouter/google/gemma-4-31b-it"
EMBED = "openrouter/qwen/qwen3-embedding-8b"
RERANK = "openrouter/qwen/qwen3-reranker-8b"
CLAUDE = "claude-sonnet-5-5"

# The university install's defaults: one OpenRouter key, every capability on
# a provider-key catalog row (no endpoint of its own).
DEFAULTS = {
    "chat": CHAT,
    "auxiliary": CHAT,
    "vision": CHAT,
    "embedding": EMBED,
    "rerank": RERANK,
}


def _provider_meta(model_id: str, provider: str, capability: str) -> ModelMeta:
    return ModelMeta(
        model_id=model_id,
        provider=provider,
        family="default",
        display_name=model_id,
        origin="catalog",
        api_key_ref=provider,
        capability=capability,
    )


METAS = {
    CHAT: _provider_meta(CHAT, "openrouter", "chat"),
    EMBED: _provider_meta(EMBED, "openrouter", "embedding"),
    RERANK: _provider_meta(RERANK, "openrouter", "rerank"),
    CLAUDE: _provider_meta(CLAUDE, "anthropic", "chat"),
}


class _Store:
    def __init__(self, defaults=None, keys=None):
        self.defaults = DEFAULTS if defaults is None else defaults
        self.keys = {"openrouter": OR_KEY} if keys is None else keys

    async def resolve_api_keys_for_job(self, *, user_id=None, project_id=None):
        return dict(self.keys)

    async def get_user_settings(self, user_id):
        return {}

    async def get_user_llm_endpoint(self, endpoint_id):
        return None

    async def resolve_default_for_capability(self, capability):
        return self.defaults.get(capability)


def _resolver():
    async def resolve(model_id, user_id=None, capability="chat"):
        try:
            return METAS[model_id]
        except KeyError:
            raise UnknownModelError(model_id) from None

    return AsyncMock(side_effect=resolve)


def _service_deps(store=None):
    return dc.DispatchCredentialDependencies(
        store=store or _Store(), logger=LOGGER, resolve_model=_resolver()
    )


def _job_deps(store=None):
    service = _service_deps(store)

    async def model_creds(**kwargs):
        return await dc.inject_model_credentials(**kwargs, dependencies=service)

    async def env_creds(**kwargs):
        return await dc.inject_env_key_credentials(**kwargs, dependencies=service)

    return jdc.DispatchCredentialDependencies(
        store=service.store,
        logger=LOGGER,
        resolve_model=service.resolve_model,
        inject_model_credentials=model_creds,
        inject_env_key_credentials=env_creds,
        inject_search_credentials=AsyncMock(),
        inject_system_kb_embedding_profile=AsyncMock(return_value=None),
        dispatch_llm_provider_fallback=dc.dispatch_llm_provider_fallback,
        nested_model_slots=dc.nested_model_slots,
    )


def _job():
    return {
        "id": "dc52e6b5-0c4c-4a60-90fb-f4f3c2b1733c",
        "user_id": "00000000-0000-0000-0000-0000000000aa",
        "project_id": None,
        "config_name": "worker_base",
    }


def _assert_key_follows_its_endpoint(env_keys: dict, prefix: str) -> None:
    """``{prefix}_API_KEY`` is the OpenRouter key and rides OpenRouter only."""
    assert env_keys[f"{prefix}_API_KEY"] == OR_KEY
    endpoint = env_keys[env_endpoint_names(prefix)[0]]
    assert same_endpoint(endpoint, OPENROUTER_URL), (prefix, endpoint)


# ---------------------------------------------------------------------------
# Dispatch: one routing rule for every flat capability
# ---------------------------------------------------------------------------


class TestEnvKeyInjectorPerCapability:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("prefix", "model_id", "capability"),
        [
            ("EMBEDDING", EMBED, "embedding"),
            ("KB_EMBEDDING", EMBED, "embedding"),
            ("RERANK", RERANK, "rerank"),
            ("VISION", CHAT, "vision"),
            ("WHISPER", CHAT, "whisper"),
            ("TTS", CHAT, "tts"),
            ("CITATION_LLM", CHAT, "chat"),
        ],
    )
    async def test_provider_key_row_gets_its_providers_base_url(
        self, prefix, model_id, capability
    ):
        env_keys: dict = {}
        await dc.inject_env_key_credentials(
            env_keys=env_keys,
            prefix=prefix,
            model_id=model_id,
            user_id="u",
            resolved_keys={"openrouter": OR_KEY},
            capability=capability,
            dependencies=_service_deps(),
        )
        assert env_keys[f"{prefix}_MODEL"] == model_id
        _assert_key_follows_its_endpoint(env_keys, prefix)

    @pytest.mark.asyncio
    async def test_reinjection_over_its_own_canonical_url_restores_the_key(self):
        # The session copy is redacted before persistence: the URL survives,
        # the key does not. Resume must put the key back next to it.
        env_keys = {"RERANK_MODEL": RERANK, "RERANK_BASE_URL": OPENROUTER_URL + "/"}
        await dc.inject_env_key_credentials(
            env_keys=env_keys,
            prefix="RERANK",
            model_id=RERANK,
            user_id="u",
            resolved_keys={"openrouter": OR_KEY},
            capability="rerank",
            dependencies=_service_deps(),
        )
        assert env_keys["RERANK_API_KEY"] == OR_KEY
        assert env_keys["RERANK_BASE_URL"] == OPENROUTER_URL + "/"

    @pytest.mark.asyncio
    async def test_canonical_citation_alias_is_not_foreign(self):
        env_keys = {"CITATION_LLM_URL": OPENROUTER_URL}
        await dc.inject_env_key_credentials(
            env_keys=env_keys,
            prefix="CITATION_LLM",
            model_id=CHAT,
            user_id="u",
            resolved_keys={"openrouter": OR_KEY},
            capability="chat",
            dependencies=_service_deps(),
        )
        assert env_keys["CITATION_LLM_API_KEY"] == OR_KEY
        assert env_keys["CITATION_LLM_BASE_URL"] == OPENROUTER_URL

    @pytest.mark.asyncio
    async def test_foreign_url_still_withholds_the_key(self):
        env_keys = {"EMBEDDING_BASE_URL": "https://evil.example/api/v1"}
        await dc.inject_env_key_credentials(
            env_keys=env_keys,
            prefix="EMBEDDING",
            model_id=EMBED,
            user_id="u",
            resolved_keys={"openrouter": OR_KEY},
            capability="embedding",
            dependencies=_service_deps(),
        )
        assert "EMBEDDING_API_KEY" not in env_keys
        assert env_keys["EMBEDDING_BASE_URL"] == "https://evil.example/api/v1"

    @pytest.mark.asyncio
    async def test_provider_without_openai_compatible_api_gets_no_key(self, caplog):
        # Anthropic serves no OpenAI-compatible embeddings/vision API for a
        # flat client to use; its key must not fall back to api.openai.com.
        env_keys: dict = {}
        with caplog.at_level(logging.ERROR, logger=LOGGER.name):
            await dc.inject_env_key_credentials(
                env_keys=env_keys,
                prefix="VISION",
                model_id=CLAUDE,
                user_id="u",
                resolved_keys={"anthropic": ANTHROPIC_KEY},
                capability="vision",
                dependencies=_service_deps(),
            )
        assert env_keys == {"VISION_MODEL": CLAUDE}
        assert "no OpenAI-compatible API" in caplog.text
        assert ANTHROPIC_KEY not in caplog.text

    @pytest.mark.asyncio
    async def test_no_key_means_no_half_credential(self):
        env_keys: dict = {}
        await dc.inject_env_key_credentials(
            env_keys=env_keys,
            prefix="RERANK",
            model_id=RERANK,
            user_id="u",
            resolved_keys={},
            capability="rerank",
            dependencies=_service_deps(),
        )
        assert env_keys == {"RERANK_MODEL": RERANK}


# ---------------------------------------------------------------------------
# Dispatch: the university repro, job and session composition
# ---------------------------------------------------------------------------


class TestUniversityRepro:
    @pytest.mark.asyncio
    async def test_job_delivery_routes_every_flat_capability_to_openrouter(self):
        out = await jdc.inject_dispatch_credentials(
            _job(), {}, dependencies=_job_deps()
        )
        env_keys = out["env_keys"]
        for prefix in ("EMBEDDING", "RERANK", "VISION"):
            _assert_key_follows_its_endpoint(env_keys, prefix)
        # The delivery the agent's pre-flight checks no longer leaves the
        # reranker without an endpoint (APP-RR-1).
        assert rerank_role_violation(env_keys) is None
        assert embedding_role_violation(env_keys) is None
        # Nothing in the delivery points a client at OpenAI (APP-EMB-1).
        assert not any("api.openai.com" in str(value) for value in env_keys.values()), (
            env_keys
        )

    @pytest.mark.asyncio
    async def test_session_delivery_survives_redaction_and_resume(self):
        deps = _service_deps()
        first = await dc.inject_thread_dispatch_credentials(
            {}, user_id="u", dependencies=deps
        )
        for prefix in ("EMBEDDING", "RERANK"):
            _assert_key_follows_its_endpoint(first["env_keys"], prefix)

        stored = redact_config_override(first)
        assert "RERANK_API_KEY" not in stored["env_keys"]
        resumed = await dc.inject_thread_dispatch_credentials(
            stored, user_id="u", dependencies=deps
        )
        for prefix in ("EMBEDDING", "RERANK"):
            _assert_key_follows_its_endpoint(resumed["env_keys"], prefix)
        assert resumed["env_keys"]["RERANK_MODEL"] == RERANK
        assert resumed["env_keys"]["EMBEDDING_MODEL"] == EMBED


# ---------------------------------------------------------------------------
# Agent: the clients built from that delivery
# ---------------------------------------------------------------------------


@pytest.fixture
def delivered_env(monkeypatch):
    """Export the job delivery the way the agent does (``os.environ``)."""

    async def build():
        out = await jdc.inject_dispatch_credentials(
            _job(), {}, dependencies=_job_deps()
        )
        for name in (
            "EMBEDDING_PROVIDER",
            "EMBEDDING_BASE_URL",
            "EMBEDDING_API_KEY",
            "VISION_BASE_URL",
            "VISION_API_KEY",
            "OPENAI_API_KEY",
        ):
            monkeypatch.delenv(name, raising=False)
        for name, value in out["env_keys"].items():
            monkeypatch.setenv(name, value)
        return out["env_keys"]

    return build


class TestAgentClients:
    @pytest.mark.asyncio
    async def test_embedding_client_targets_openrouter_with_the_native_id(
        self, delivered_env
    ):
        from shared.runtime.services.embedding_service import EmbeddingService

        await delivered_env()
        service = EmbeddingService()
        assert same_endpoint(str(service._client.base_url), OPENROUTER_URL)
        assert service.api_key == OR_KEY
        assert service.model == "qwen/qwen3-embedding-8b"

    @pytest.mark.asyncio
    async def test_reranker_binds_and_posts_the_native_id_to_openrouter(
        self, delivered_env
    ):
        from agent.services.memory.plugins.reranker import (
            RerankerScorer,
            resolve_reranker_transport,
        )

        env_keys = await delivered_env()
        cfg = SimpleNamespace(model=None, base_url=None, api_key=None)
        model, base_url, api_key = resolve_reranker_transport(cfg, env=env_keys)

        posted = {}

        class _Response:
            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "results": [
                        {"index": 1, "relevance_score": 0.9},
                        {"index": 0, "relevance_score": 0.1},
                    ]
                }

        class _Client:
            async def post(self, url, json):
                posted["url"] = url
                posted["body"] = json
                return _Response()

        scorer = RerankerScorer(
            model=model, base_url=base_url, api_key=api_key, client=_Client()
        )
        assert scorer.api_key == OR_KEY
        await scorer._rerank("cat", ["a car", "a cat"])
        assert posted["url"] == OPENROUTER_URL + "/rerank"
        assert posted["body"]["model"] == "qwen/qwen3-reranker-8b"

    @pytest.mark.asyncio
    async def test_vision_client_targets_openrouter_with_the_native_id(
        self, delivered_env
    ):
        from agent.services.vision_helper import VisionHelper

        await delivered_env()
        helper = VisionHelper()
        assert same_endpoint(helper.api_base, OPENROUTER_URL)
        assert helper.api_key == OR_KEY
        assert helper.model == "google/gemma-4-31b-it"


# ---------------------------------------------------------------------------
# wire_model_id: the prefix is dropped only for the provider's own API
# ---------------------------------------------------------------------------


class TestWireModelId:
    @pytest.mark.parametrize(
        ("model", "base_url", "expected"),
        [
            (RERANK, OPENROUTER_URL, "qwen/qwen3-reranker-8b"),
            (RERANK, "https://OpenRouter.ai/api/v1/", "qwen/qwen3-reranker-8b"),
            (
                "groq/whisper-large-v3",
                "https://api.groq.com/openai/v1",
                "whisper-large-v3",
            ),
            # Another host receives the catalog id unchanged.
            (RERANK, "https://router.university.example/v1", RERANK),
            (RERANK, None, RERANK),
            # On OpenRouter a vendor prefix is part of the model's real id.
            ("openai/gpt-5.5", OPENROUTER_URL, "openai/gpt-5.5"),
            ("qwen/qwen3-reranker-8b", OPENROUTER_URL, "qwen/qwen3-reranker-8b"),
            ("qwen3-reranker-8b", OPENROUTER_URL, "qwen3-reranker-8b"),
        ],
    )
    def test_wire_model_id(self, model, base_url, expected):
        assert wire_model_id(model, base_url) == expected
