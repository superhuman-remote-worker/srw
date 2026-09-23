"""A stored credential is never sent to a caller-chosen endpoint.

Regression for the credential-exfiltration finding: a job (or session) whose
``config_override`` pins a ``base_url`` next to a *provider-key* model made the
dispatch injector write the deployment's system/project/user key beside that
caller-chosen host, so the agent shipped the key to it.

The contract asserted here (one property, three sinks):

* An endpoint-backed catalog row keeps working — its ``base_url`` + ``api_key``
  come from the endpoint row, not the caller.
* A provider-key row with no caller ``base_url`` keeps working — the resolved
  key rides the provider's canonical endpoint.
* A provider-key row *with* a caller ``base_url`` (or ``{PREFIX}_BASE_URL``)
  dispatches WITHOUT the resolved key: a stored credential must never be paired
  with an endpoint the caller chose.
* A bring-your-own endpoint (caller supplies both ``base_url`` AND ``api_key``)
  keeps its own key and never has a stored one injected over it.

The loud front-door refusal for the same input lives in
``tests/test_job_create_wire.py`` (the ``POST /api/jobs`` admission fence);
this file pins the sink so resume / blob / legacy paths hold the line too.
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock

import pytest

from orchestrator.services import dispatch_credentials as dc
from orchestrator.services import job_dispatch_credentials as jdc
from shared.runtime.core.model_registry import ModelMeta, UnknownModelError


LOGGER = logging.getLogger("test.endpoint-binding")

ENDPOINT_ID = "11111111-1111-1111-1111-111111111111"
ENDPOINT_BASE_URL = "https://selfhosted.internal/v1"
ENDPOINT_KEY = "sk-endpoint-SECRET"
SYSTEM_OR_KEY = "sk-or-v1-system-SECRET"
CALLER_HOST = "https://evil.example/v1"

# A self-hosted catalog row that owns its transport via an endpoint id.
ENDPOINT_META = ModelMeta(
    model_id="selfhosted-chat",
    provider="openai",
    family="gpt",
    display_name="Self-hosted",
    origin="catalog",
    endpoint_id=ENDPOINT_ID,
    api_key_ref="openai",
)
# A system-anchored catalog row that routes on a provider key (no endpoint).
ROUTER_META = ModelMeta(
    model_id="router-chat",
    provider="openrouter",
    family="minimax",
    display_name="Router",
    origin="catalog",
    api_key_ref="openrouter",
)


class _Store:
    """Minimal dispatch store: system provider keys + one endpoint row."""

    async def resolve_api_keys_for_job(self, *, user_id=None, project_id=None):
        return {"openrouter": SYSTEM_OR_KEY, "openai": ENDPOINT_KEY}

    async def get_user_settings(self, user_id):
        return {}

    async def get_user_llm_endpoint(self, endpoint_id):
        if endpoint_id == ENDPOINT_ID:
            return {"base_url": ENDPOINT_BASE_URL, "api_key": ENDPOINT_KEY}
        return None

    async def resolve_default_for_capability(self, capability):
        return None


def _resolver(metas):
    async def resolve(model_id, user_id=None, capability="chat"):
        try:
            return metas[model_id]
        except KeyError:
            raise UnknownModelError(model_id) from None

    return AsyncMock(side_effect=resolve)


def _service_deps(metas):
    return dc.DispatchCredentialDependencies(
        store=_Store(), logger=LOGGER, resolve_model=_resolver(metas)
    )


# ---------------------------------------------------------------------------
# inject_model_credentials — the structured llm/auxiliary/roster sections
# ---------------------------------------------------------------------------


class TestModelSectionBinding:
    @pytest.mark.asyncio
    async def test_provider_key_not_injected_over_caller_base_url(self):
        section = {"model": "router-chat", "base_url": CALLER_HOST}
        await dc.inject_model_credentials(
            section=section,
            model_id="router-chat",
            user_id="u",
            resolved_keys={"openrouter": SYSTEM_OR_KEY},
            dependencies=_service_deps({"router-chat": ROUTER_META}),
        )
        assert section["base_url"] == CALLER_HOST
        assert "api_key" not in section

    @pytest.mark.asyncio
    async def test_provider_key_injected_without_caller_base_url(self):
        section = {"model": "router-chat"}
        await dc.inject_model_credentials(
            section=section,
            model_id="router-chat",
            user_id="u",
            resolved_keys={"openrouter": SYSTEM_OR_KEY},
            dependencies=_service_deps({"router-chat": ROUTER_META}),
        )
        assert section["api_key"] == SYSTEM_OR_KEY
        assert "base_url" not in section

    @pytest.mark.asyncio
    async def test_endpoint_row_still_supplies_transport(self):
        section = {"model": "selfhosted-chat"}
        await dc.inject_model_credentials(
            section=section,
            model_id="selfhosted-chat",
            user_id="u",
            resolved_keys={"openai": ENDPOINT_KEY},
            dependencies=_service_deps({"selfhosted-chat": ENDPOINT_META}),
        )
        assert section["base_url"] == ENDPOINT_BASE_URL
        assert section["api_key"] == ENDPOINT_KEY

    @pytest.mark.asyncio
    async def test_byo_endpoint_keeps_caller_key(self):
        section = {"model": "router-chat", "base_url": CALLER_HOST, "api_key": "byo"}
        await dc.inject_model_credentials(
            section=section,
            model_id="router-chat",
            user_id="u",
            resolved_keys={"openrouter": SYSTEM_OR_KEY},
            dependencies=_service_deps({"router-chat": ROUTER_META}),
        )
        assert section["api_key"] == "byo"


# ---------------------------------------------------------------------------
# inject_env_key_credentials — flat env vars (embedding / rerank / vision ...)
# ---------------------------------------------------------------------------


class TestEnvKeyBinding:
    @pytest.mark.asyncio
    async def test_key_not_injected_over_caller_base_url(self):
        env_keys = {"EMBEDDING_BASE_URL": CALLER_HOST}
        await dc.inject_env_key_credentials(
            env_keys=env_keys,
            prefix="EMBEDDING",
            model_id="router-chat",
            user_id="u",
            resolved_keys={"openrouter": SYSTEM_OR_KEY},
            capability="embedding",
            dependencies=_service_deps({"router-chat": ROUTER_META}),
        )
        assert env_keys["EMBEDDING_BASE_URL"] == CALLER_HOST
        assert "EMBEDDING_API_KEY" not in env_keys

    @pytest.mark.asyncio
    async def test_key_injected_without_caller_base_url(self):
        env_keys = {}
        await dc.inject_env_key_credentials(
            env_keys=env_keys,
            prefix="EMBEDDING",
            model_id="router-chat",
            user_id="u",
            resolved_keys={"openrouter": SYSTEM_OR_KEY},
            capability="embedding",
            dependencies=_service_deps({"router-chat": ROUTER_META}),
        )
        assert env_keys["EMBEDDING_API_KEY"] == SYSTEM_OR_KEY

    @pytest.mark.asyncio
    async def test_key_not_injected_over_citation_url_alias(self):
        # The guard checks every endpoint name a reader consults, not just the
        # canonical one: CITATION_LLM_URL is an alias for CITATION_LLM_BASE_URL.
        env_keys = {"CITATION_LLM_URL": CALLER_HOST}
        await dc.inject_env_key_credentials(
            env_keys=env_keys,
            prefix="CITATION_LLM",
            model_id="router-chat",
            user_id="u",
            resolved_keys={"openrouter": SYSTEM_OR_KEY},
            capability="chat",
            dependencies=_service_deps({"router-chat": ROUTER_META}),
        )
        assert "CITATION_LLM_API_KEY" not in env_keys

    @pytest.mark.asyncio
    async def test_endpoint_row_overwrites_preset_alias(self):
        # An endpoint row is authoritative: a pre-set alias cannot survive next
        # to the row's key (setdefault would have left the stale foreign host).
        emb_meta = ModelMeta(
            model_id="sys-embed",
            provider="openai",
            family="embedding",
            display_name="Embed",
            origin="catalog",
            endpoint_id=ENDPOINT_ID,
            api_key_ref="openai",
            capability="embedding",
        )
        env_keys = {"EMBEDDING_BASE_URL": CALLER_HOST}
        await dc.inject_env_key_credentials(
            env_keys=env_keys,
            prefix="EMBEDDING",
            model_id="sys-embed",
            user_id="u",
            resolved_keys={"openai": ENDPOINT_KEY},
            capability="embedding",
            dependencies=_service_deps({"sys-embed": emb_meta}),
        )
        assert env_keys["EMBEDDING_BASE_URL"] == ENDPOINT_BASE_URL
        assert env_keys["EMBEDDING_API_KEY"] == ENDPOINT_KEY


# ---------------------------------------------------------------------------
# inject_dispatch_credentials — the top-level composition (the review's repro)
# ---------------------------------------------------------------------------


def _composition_deps(metas):
    """Wire the real service injectors, stub the search/KB seams."""
    service = _service_deps(metas)

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
        "id": "00000000-0000-0000-0000-000000000001",
        "user_id": "00000000-0000-0000-0000-0000000000aa",
        "project_id": None,
        "config_name": "worker_base",
    }


class TestCompositionBinding:
    @pytest.mark.asyncio
    async def test_repro_withholds_system_key_from_caller_base_url(self):
        # The reviewer's repro: unknown model + caller provider/base_url.
        override = {
            "llm": {
                "model": "anything",
                "provider": "openrouter",
                "base_url": CALLER_HOST,
            }
        }
        out = await jdc.inject_dispatch_credentials(
            _job(), override, dependencies=_composition_deps({})
        )
        assert out["llm"]["base_url"] == CALLER_HOST
        assert "api_key" not in out["llm"]

    @pytest.mark.asyncio
    async def test_provider_key_model_without_base_url_is_credentialed(self):
        override = {"llm": {"model": "router-chat"}}
        out = await jdc.inject_dispatch_credentials(
            _job(),
            override,
            dependencies=_composition_deps({"router-chat": ROUTER_META}),
        )
        assert out["llm"]["api_key"] == SYSTEM_OR_KEY
        assert "base_url" not in out["llm"]

    @pytest.mark.asyncio
    async def test_selfhosted_catalog_endpoint_still_routes(self):
        override = {"llm": {"model": "selfhosted-chat"}}
        out = await jdc.inject_dispatch_credentials(
            _job(),
            override,
            dependencies=_composition_deps({"selfhosted-chat": ENDPOINT_META}),
        )
        assert out["llm"]["base_url"] == ENDPOINT_BASE_URL
        assert out["llm"]["api_key"] == ENDPOINT_KEY

    @pytest.mark.asyncio
    async def test_roster_entry_base_url_does_not_capture_system_key(self):
        override = {
            "subagents": {
                "roster": {
                    "child": {"llm": {"model": "router-chat", "base_url": CALLER_HOST}}
                }
            },
            "llm": {"model": "selfhosted-chat"},
        }
        out = await jdc.inject_dispatch_credentials(
            _job(),
            override,
            dependencies=_composition_deps(
                {"router-chat": ROUTER_META, "selfhosted-chat": ENDPOINT_META}
            ),
        )
        child = out["subagents"]["roster"]["child"]["llm"]
        assert child["base_url"] == CALLER_HOST
        assert "api_key" not in child


# ---------------------------------------------------------------------------
# Agent-side sink: a key follows its endpoint at the point of use
# ---------------------------------------------------------------------------


class TestWithOverrideKeyFollowsEndpoint:
    """``LLMConfig.with_override`` — summarization / subagents.llm overlay."""

    def _base(self):
        from shared.runtime.core.loader import LLMConfig

        return LLMConfig(model="m", base_url="https://parent/v1", api_key="PARENT-KEY")

    def test_new_base_url_without_key_drops_parent_key(self):
        from shared.runtime.core.loader import PhaseLLMOverride

        out = self._base().with_override(PhaseLLMOverride(base_url=CALLER_HOST))
        assert out.base_url == CALLER_HOST
        assert out.api_key is None

    def test_new_base_url_with_own_key_keeps_it(self):
        from shared.runtime.core.loader import PhaseLLMOverride

        out = self._base().with_override(
            PhaseLLMOverride(base_url=CALLER_HOST, api_key="BYO")
        )
        assert out.base_url == CALLER_HOST
        assert out.api_key == "BYO"

    def test_same_base_url_keeps_parent_key(self):
        from shared.runtime.core.loader import PhaseLLMOverride

        out = self._base().with_override(PhaseLLMOverride(temperature=0.5))
        assert out.base_url == "https://parent/v1"
        assert out.api_key == "PARENT-KEY"


class TestFactoryEnvFallbackBinding:
    """``_create_*_llm`` env fallbacks bind to the provider's own endpoint."""

    def test_openrouter_env_key_not_sent_to_foreign_host(self, monkeypatch):
        from shared.runtime.core.loader import LLMConfig, _create_openrouter_llm

        monkeypatch.setenv("OPENROUTER_API_KEY", "sk-or-system")
        cfg = LLMConfig(model="openrouter/x/y", base_url=CALLER_HOST)
        with pytest.raises(ValueError, match="OPENROUTER_API_KEY"):
            _create_openrouter_llm(cfg, limits=None)


class TestRerankerTransportBinding:
    def _cfg(self, **kw):
        from types import SimpleNamespace

        kw.setdefault("model", None)
        kw.setdefault("base_url", None)
        kw.setdefault("api_key", None)
        return SimpleNamespace(**kw)

    def test_explicit_base_url_no_key_gets_no_foreign_env_key(self):
        from agent.services.memory.plugins.reranker import resolve_reranker_transport

        _, base_url, api_key = resolve_reranker_transport(
            self._cfg(base_url=CALLER_HOST),
            env={
                "RERANK_BASE_URL": "https://rr.internal/v1",
                "RERANK_API_KEY": "rr-key",
                "EMBEDDING_API_KEY": "emb-key",
            },
        )
        assert base_url == CALLER_HOST
        assert api_key is None  # neither RERANK_API_KEY nor EMBEDDING_API_KEY

    def test_explicit_base_url_matching_env_host_uses_that_key(self):
        from agent.services.memory.plugins.reranker import resolve_reranker_transport

        _, base_url, api_key = resolve_reranker_transport(
            self._cfg(base_url="https://rr.internal/v1"),
            env={
                "RERANK_BASE_URL": "https://rr.internal/v1",
                "RERANK_API_KEY": "rr-key",
            },
        )
        assert base_url == "https://rr.internal/v1"
        assert api_key == "rr-key"

    def test_explicit_key_wins(self):
        from agent.services.memory.plugins.reranker import resolve_reranker_transport

        _, _, api_key = resolve_reranker_transport(
            self._cfg(base_url=CALLER_HOST, api_key="cfg-key"),
            env={"EMBEDDING_API_KEY": "emb-key"},
        )
        assert api_key == "cfg-key"


class TestExportableEnvKeys:
    def test_only_dispatch_names_export(self):
        from shared.runtime.core.transport_resolution import exportable_env_keys

        kept, dropped = exportable_env_keys(
            {
                "EMBEDDING_API_KEY": "k1",
                "VISION_BASE_URL": "https://v/v1",
                "OPENAI_BASE_URL": "https://evil/v1",
                "OPENAI_API_BASE": "https://evil/v1",
                "OPENAI_API_KEY": "leak",
                "PATH": "/usr/bin",
            }
        )
        assert kept == {
            "EMBEDDING_API_KEY": "k1",
            "VISION_BASE_URL": "https://v/v1",
        }
        assert "OPENAI_BASE_URL" in dropped
        assert "OPENAI_API_BASE" in dropped
        assert "OPENAI_API_KEY" in dropped
        assert "PATH" in dropped
