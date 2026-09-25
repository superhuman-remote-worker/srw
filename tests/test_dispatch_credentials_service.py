"""Credential and model-slot resolution: privacy, precedence, and the move.

R1.B05 lane C. Three things are proven here, in this order:

1. **Characterization** — B05 proved each extracted node byte-identical through
   its ``orchestrator.main`` bridge and the service. R1.B12 removed the last
   bridges, so those comparisons (which had nothing left to compare) are gone;
   the call-site guard below drives the application's own binding.
2. **Privacy** — a resolved credential lands only in the section it was
   resolved for, survives no logging statement, and is removed in full by
   ``redact_config_override`` (the persistence + response boundary). Asserted
   against a set of sentinel secrets rather than assumed from code reading.
3. **Precedence** — caller pin > endpoint row > resolved key map for a
   *transport*; section pin > user setting > system capability default for a
   *model*; and the provider heuristics are consulted only where registry
   resolution produced nothing.

Every collaborator patch in this module proves it was reached (an await count,
a tripwire, or a value only the stub could produce) — a patch that quietly does
nothing while the test still passes is the failure mode R1.B05 §P3 names.
"""

from __future__ import annotations

import asyncio
import copy
import logging
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from orchestrator.application import preparation as preparation_composition
from orchestrator.services import dispatch_credentials as dispatch_credentials_module
from shared.runtime.core import model_registry as model_registry_module

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

import orchestrator.main  # noqa: E402
from orchestrator.security.access import redact_config_override  # noqa: E402
from orchestrator.services import audit_usage  # noqa: E402
from orchestrator.services import dispatch_credentials as dc  # noqa: E402
from orchestrator.services.capability_credentials import (  # noqa: E402
    CapabilityCredentials,
)
from shared.runtime.core.model_registry import (  # noqa: E402
    ModelMeta,
    UnknownModelError,
)


# --------------------------------------------------------------------------
# Sentinels. Every one of these is a *secret*; no log line and no redacted
# payload may contain any of them.
# --------------------------------------------------------------------------

ENDPOINT_ID = "11111111-1111-1111-1111-111111111111"
ENDPOINT_BASE_URL = "https://endpoint.example/v1"

ENDPOINT_KEY = "sk-endpoint-SECRET-aaaa"
USER_OPENAI_KEY = "sk-user-openai-SECRET-bbbb"
USER_OPENROUTER_KEY = "sk-or-v1-user-SECRET-cccc"
SYSTEM_OPENAI_KEY = "sk-system-openai-SECRET-dddd"
SEARCH_KEY = "srch-SECRET-eeee"
FETCH_KEY = "ftch-SECRET-ffff"

SECRETS = (
    ENDPOINT_KEY,
    USER_OPENAI_KEY,
    USER_OPENROUTER_KEY,
    SYSTEM_OPENAI_KEY,
    SEARCH_KEY,
    FETCH_KEY,
)

USER_KEYS = {"openai": USER_OPENAI_KEY, "openrouter": USER_OPENROUTER_KEY}
SYSTEM_KEYS = {"openai": SYSTEM_OPENAI_KEY}


_METAS = {
    "endpoint-chat": ModelMeta(
        model_id="endpoint-chat",
        provider="openai",
        family="gpt",
        display_name="Endpoint Chat",
        origin="custom",
        endpoint_id=ENDPOINT_ID,
        api_key_ref="openai",
        context_window=131071,
        max_output_tokens=8191,
    ),
    "builtin-chat": ModelMeta(
        model_id="builtin-chat",
        provider="openai",
        family="gpt",
        display_name="Builtin Chat",
        origin="catalog",
        api_key_ref="openai",
        context_window=64000,
    ),
    "router-chat": ModelMeta(
        model_id="router-chat",
        provider="openrouter",
        family="minimax",
        display_name="Router Chat",
        origin="catalog",
        api_key_ref="openrouter",
    ),
    "sys-embed": ModelMeta(
        model_id="sys-embed",
        provider="openai",
        family="embedding",
        display_name="System Embedding",
        origin="catalog",
        api_key_ref="openai",
        capability="embedding",
    ),
}


def _job(config_name: str = "worker_base") -> dict:
    return {
        "id": "00000000-0000-0000-0000-000000000001",
        "user_id": "00000000-0000-0000-0000-0000000000aa",
        "project_id": "00000000-0000-0000-0000-0000000000bb",
        "config_name": config_name,
    }


@pytest.fixture
def patched_main(monkeypatch):
    """Patch the registry + store collaborators the injectors consult.

    Unknown model ids **raise** ``UnknownModelError`` here (what the real
    resolver does) rather than returning ``None``, so the except-branch of every
    injector is the one under test.
    """

    async def fake_resolve(model_id, user_id=None, capability="chat"):
        try:
            return _METAS[model_id]
        except KeyError:
            raise UnknownModelError(model_id) from None

    resolver = AsyncMock(side_effect=fake_resolve)
    monkeypatch.setattr(model_registry_module, "resolve_model", resolver, raising=True)

    async def fake_get_endpoint(endpoint_id):
        if endpoint_id == ENDPOINT_ID:
            return {
                "id": ENDPOINT_ID,
                "label": "endpoint",
                "base_url": ENDPOINT_BASE_URL,
                "api_key": ENDPOINT_KEY,
            }
        return None

    async def fake_resolve_keys(*, user_id=None, project_id=None):
        # The system profile deliberately resolves with user_id/project_id None
        # and must NOT see the per-user map.
        if user_id is None and project_id is None:
            return dict(SYSTEM_KEYS)
        return dict(USER_KEYS)

    endpoints = AsyncMock(side_effect=fake_get_endpoint)
    keys = AsyncMock(side_effect=fake_resolve_keys)
    defaults = AsyncMock(return_value=None)
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "get_user_llm_endpoint",
        endpoints,
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "resolve_api_keys_for_job",
        keys,
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "resolve_default_for_capability",
        defaults,
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "get_user_settings",
        AsyncMock(return_value={}),
    )
    return SimpleNamespace(
        resolver=resolver, endpoints=endpoints, keys=keys, defaults=defaults
    )


def _deps() -> dc.DispatchCredentialDependencies:
    """Build the dependency object the way a main factory must build it.

    Read from ``orchestrator.main`` at *call* time, so a monkeypatch applied by
    the fixture above is what the service actually uses. Every test that uses
    this also asserts the patched collaborator was awaited — that is the proof
    the patch was reached and not silently bypassed (R1.B05 §P3).
    """
    return dc.DispatchCredentialDependencies(
        store=orchestrator.main.app.state.resources.postgres_db,
        logger=preparation_composition.logger,
        resolve_model=model_registry_module.resolve_model,
    )


def _no_secret_in_logs(caplog) -> None:
    for record in caplog.records:
        rendered = record.getMessage() + " " + repr(record.args)
        for secret in SECRETS:
            assert secret not in rendered, (
                f"secret leaked into log record from {record.name}: {record.getMessage()!r}"
            )


def _collect_strings(value, out=None):
    out = [] if out is None else out
    if isinstance(value, dict):
        for k, v in value.items():
            out.append(str(k))
            _collect_strings(v, out)
    elif isinstance(value, (list, tuple)):
        for v in value:
            _collect_strings(v, out)
    else:
        out.append(str(value))
    return out


# ==========================================================================
# 1. Characterization: main and the service agree, node by node
# ==========================================================================


class TestPureHelpers:
    """The pure nodes' own contracts (§P4)."""

    def test_provider_of_model_is_a_miss_not_a_guess(self):
        """A miss returns None so the caller falls through to its own heuristic."""
        assert dc.provider_of_model("Qwen/Qwen3-32B") is None
        assert dc.provider_of_model("") is None


class TestInjectors:
    @pytest.mark.asyncio
    async def test_search_fallback_needs_a_DIFFERENT_catalog_row(self, patched_main):
        """A fallback resolving to the primary's own row is not a fallback.

        Without the ``different_row`` guard the same provider would be shipped
        twice and a dead primary would "fail over" to itself.
        """

        async def same_row(*, capability, setting_key=None, **kwargs):
            del kwargs, setting_key
            if capability != "search":
                return None
            return CapabilityCredentials(
                model="searxng",
                base_url="http://searxng.svc:8080",
                api_key=SEARCH_KEY,
                provider="searxng",
                params={"provider": "searxng", "ops": ["search"]},
                catalog_id="search-row",
            )

        with patch(
            "orchestrator.services.capability_credentials.resolve_capability_credentials",
            AsyncMock(side_effect=same_row),
        ) as resolver:
            out = await dc.inject_search_credentials(
                {"research": {"search_fallback": {"provider": "stale"}}},
                user_settings={},
                user_id="u",
                resolved_keys=dict(USER_KEYS),
                dependencies=_deps(),
            )
        assert resolver.await_count == 3
        assert out["research"]["search"]["provider"] == "searxng"
        assert "search_fallback" not in out["research"]

    @pytest.mark.asyncio
    async def test_search_fallback_ships_when_the_row_really_differs(
        self, patched_main
    ):
        async def other_row(*, capability, setting_key=None, **kwargs):
            del kwargs
            if setting_key == "default_search_fallback_model":
                return CapabilityCredentials(
                    model="brave",
                    base_url="https://api.search.brave.com",
                    api_key=FETCH_KEY,
                    provider="brave",
                    params={"provider": "brave", "ops": ["search"]},
                    catalog_id="fallback-row",
                )
            if capability != "search":
                return None
            return CapabilityCredentials(
                model="searxng",
                base_url="http://searxng.svc:8080",
                api_key=SEARCH_KEY,
                provider="searxng",
                params={"provider": "searxng", "ops": ["search"]},
                catalog_id="search-row",
            )

        with patch(
            "orchestrator.services.capability_credentials.resolve_capability_credentials",
            AsyncMock(side_effect=other_row),
        ):
            out = await dc.inject_search_credentials(
                {},
                user_settings={},
                user_id="u",
                resolved_keys=dict(USER_KEYS),
                dependencies=_deps(),
            )
        assert out["research"]["search_fallback"]["provider"] == "brave"


async def _capability_default(capability):
    return {"embedding": "sys-embed"}.get(capability)


async def _search_resolver(*, capability, setting_key=None, **kwargs):
    del kwargs
    if setting_key == "default_search_fallback_model":
        return None
    if capability == "search":
        return CapabilityCredentials(
            model="searxng",
            base_url="http://searxng.svc:8080",
            api_key=SEARCH_KEY,
            provider="searxng",
            params={"provider": "searxng", "ops": ["search"]},
            catalog_id="search-row",
        )
    return CapabilityCredentials(
        model="tavily",
        base_url="https://api.tavily.com",
        api_key=FETCH_KEY,
        provider="tavily",
        params={"provider": "tavily", "ops": ["extract"]},
        catalog_id="fetch-row",
    )


# ==========================================================================
# 2. Privacy: a credential reaches only the slot it was resolved for
# ==========================================================================


class TestCredentialPrivacy:
    @pytest.mark.asyncio
    async def test_secrets_land_only_in_their_own_slot(self, patched_main, caplog):
        """Every sentinel appears at exactly the paths it was resolved for.

        Not "a key is present somewhere" — the *whole* enriched payload is
        walked, and any occurrence at an unexpected path fails.
        """
        caplog.set_level(logging.DEBUG)
        patched_main.defaults.side_effect = _capability_default
        override = {
            "llm": {"model": "endpoint-chat"},
            "auxiliary": {"model": "router-chat"},
            "subagents": {"roster": {"critic": {"llm": {"model": "builtin-chat"}}}},
        }
        with patch(
            "orchestrator.services.capability_credentials.resolve_capability_credentials",
            AsyncMock(side_effect=_search_resolver),
        ) as resolver:
            out = await dc.inject_thread_dispatch_credentials(
                override,
                user_id="u",
                project_id="p",
                user_settings={"embedding_provider": "openrouter"},
                include_kb_profile=True,
                dependencies=_deps(),
            )
        assert resolver.await_count == 3

        found: dict[str, list[str]] = {s: [] for s in SECRETS}

        def walk(node, path):
            if isinstance(node, dict):
                for k, v in node.items():
                    walk(v, f"{path}.{k}")
            elif isinstance(node, list):
                for i, v in enumerate(node):
                    walk(v, f"{path}[{i}]")
            elif isinstance(node, str) and node in found:
                found[node].append(path)

        walk(out, "")

        assert found[ENDPOINT_KEY] == [".llm.api_key"]
        assert found[USER_OPENROUTER_KEY] == [
            ".auxiliary.api_key",
            ".env_keys.OPENROUTER_API_KEY",
        ]
        assert found[USER_OPENAI_KEY] == [
            ".subagents.roster.critic.llm.api_key",
            ".env_keys.EMBEDDING_API_KEY",
        ]
        # The KB profile is SYSTEM-scoped: it carries the system key, never the
        # user's, and never the reverse.
        assert found[SYSTEM_OPENAI_KEY] == [".env_keys.KB_EMBEDDING_API_KEY"]
        assert found[SEARCH_KEY] == [".research.search.api_key"]
        assert found[FETCH_KEY] == [".research.fetch.api_key"]

        _no_secret_in_logs(caplog)

    @pytest.mark.asyncio
    async def test_no_secret_reaches_any_log_record(self, patched_main, caplog):
        """Capture the logger and assert. Names, providers and kinds only."""
        caplog.set_level(logging.DEBUG)
        patched_main.defaults.side_effect = _capability_default
        with patch(
            "orchestrator.services.capability_credentials.resolve_capability_credentials",
            AsyncMock(side_effect=_search_resolver),
        ):
            await dc.inject_thread_dispatch_credentials(
                {
                    "subagents": {
                        "roster": {"critic": {"llm": {"model": "router-chat"}}}
                    }
                },
                user_id="u",
                project_id="p",
                user_settings={},
                include_kb_profile=True,
                dependencies=_deps(),
            )
        assert caplog.records, "expected the injectors to log at all"
        _no_secret_in_logs(caplog)
        # Positive control: the informative content really is there.
        joined = " ".join(r.getMessage() for r in caplog.records)
        assert "router-chat" in joined
        assert "subagents.roster.critic.llm" in joined

    @pytest.mark.asyncio
    async def test_half_credential_is_refused_and_logged_without_the_key(
        self, patched_main, caplog
    ):
        """base_url with no usable api_key must inject NEITHER, and say so."""
        caplog.set_level(logging.DEBUG)
        patched_main.endpoints.side_effect = None
        patched_main.endpoints.return_value = {
            "id": ENDPOINT_ID,
            "base_url": ENDPOINT_BASE_URL,
            "api_key": "",
        }
        env: dict = {}
        await dc.inject_env_key_credentials(
            env_keys=env,
            prefix="EMBEDDING",
            model_id="endpoint-chat",
            user_id="u",
            resolved_keys=dict(USER_KEYS),
            capability="embedding",
            dependencies=_deps(),
        )
        assert env == {"EMBEDDING_MODEL": "endpoint-chat"}
        assert "EMBEDDING_BASE_URL" not in env
        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert errors and ENDPOINT_ID in errors[0].getMessage()
        _no_secret_in_logs(caplog)

    @pytest.mark.asyncio
    async def test_redaction_removes_every_injected_secret(self, patched_main):
        """The persistence/response boundary sees none of them."""
        patched_main.defaults.side_effect = _capability_default
        with patch(
            "orchestrator.services.capability_credentials.resolve_capability_credentials",
            AsyncMock(side_effect=_search_resolver),
        ):
            out = await dc.inject_thread_dispatch_credentials(
                {
                    "llm": {"model": "endpoint-chat"},
                    "auxiliary": {"model": "router-chat"},
                },
                user_id="u",
                project_id="p",
                user_settings={"embedding_provider": "openrouter"},
                include_kb_profile=True,
                dependencies=_deps(),
            )
        redacted = redact_config_override(out)
        blob = " ".join(_collect_strings(redacted))
        for secret in SECRETS:
            assert secret not in blob, secret
        # Non-secret routing survives redaction — that is why re-injection works.
        assert redacted["llm"]["base_url"] == ENDPOINT_BASE_URL
        assert redacted["llm"]["model"] == "endpoint-chat"
        assert redacted["env_keys"]["KB_EMBEDDING_PROFILE_ID"] == "catalog:openai"

    @pytest.mark.asyncio
    async def test_kb_profile_never_carries_the_users_key(self, patched_main):
        """``_inject_system_kb_embedding_profile`` is system-scoped, not per-user.

        It must resolve its own key map with ``user_id=None, project_id=None``;
        reusing the job's already-resolved map would reintroduce exactly the
        per-user profile skew the separate prefix exists to prevent.
        """
        patched_main.defaults.side_effect = _capability_default
        env: dict = {}
        await dc.inject_system_kb_embedding_profile(env, dependencies=_deps())
        assert env["KB_EMBEDDING_API_KEY"] == SYSTEM_OPENAI_KEY
        assert env["KB_EMBEDDING_API_KEY"] != USER_OPENAI_KEY
        patched_main.keys.assert_awaited_once_with(user_id=None, project_id=None)

    @pytest.mark.asyncio
    async def test_kb_profile_is_dropped_when_not_requested(self, patched_main):
        """A stale KB_* block from a previous enrichment must not survive."""
        patched_main.defaults.side_effect = _capability_default
        with patch(
            "orchestrator.services.capability_credentials.resolve_capability_credentials",
            AsyncMock(return_value=None),
        ):
            out = await dc.inject_thread_dispatch_credentials(
                {"env_keys": {"KB_EMBEDDING_API_KEY": SYSTEM_OPENAI_KEY}},
                user_id="u",
                project_id="p",
                include_kb_profile=False,
                dependencies=_deps(),
            )
        assert not any(k.startswith("KB_EMBEDDING_") for k in out.get("env_keys", {}))

    @pytest.mark.asyncio
    async def test_kb_profile_is_authoritative_over_a_caller_pin(self, patched_main):
        """A caller cannot pin a different KB profile through config_override."""
        patched_main.defaults.side_effect = _capability_default
        env = {
            "KB_EMBEDDING_MODEL": "attacker-model",
            "KB_EMBEDDING_API_KEY": "sk-attacker",
            "KB_EMBEDDING_BASE_URL": "https://attacker.example",
        }
        await dc.inject_system_kb_embedding_profile(env, dependencies=_deps())
        assert env["KB_EMBEDDING_MODEL"] == "sys-embed"
        assert env["KB_EMBEDDING_API_KEY"] == SYSTEM_OPENAI_KEY
        assert "attacker" not in "".join(_collect_strings(env))


# ==========================================================================
# 3. Precedence
# ==========================================================================


class TestResolutionPrecedence:
    @pytest.mark.asyncio
    async def test_caller_pin_beats_the_resolved_key_map(self, patched_main):
        """Provider-key models: an explicit api_key is never overwritten."""
        section = {"api_key": "sk-caller-pinned", "base_url": "https://caller/v1"}
        await dc.inject_model_credentials(
            section=section,
            model_id="builtin-chat",
            user_id="u",
            resolved_keys=dict(USER_KEYS),
            dependencies=_deps(),
        )
        assert section["api_key"] == "sk-caller-pinned"
        assert section["base_url"] == "https://caller/v1"

    @pytest.mark.asyncio
    async def test_caller_pinned_key_wins_where_the_transport_is_incomplete(
        self, patched_main
    ):
        """The branch that actually reaches the key map: no base_url pinned.

        With BOTH base_url and api_key present the injector short-circuits on
        ``transport_complete``, so a test that pins both never exercises the
        ``"api_key" not in section`` guard at all. This one does.
        """
        section = {"api_key": "sk-caller-pinned"}
        await dc.inject_model_credentials(
            section=section,
            model_id="builtin-chat",
            user_id="u",
            resolved_keys=dict(USER_KEYS),
            dependencies=_deps(),
        )
        assert section["api_key"] == "sk-caller-pinned"
        assert USER_OPENAI_KEY not in _collect_strings(section)

    @pytest.mark.asyncio
    async def test_caller_pinned_env_key_wins_over_the_key_map(self, patched_main):
        """The env-var sibling: every write there is setdefault too."""
        env = {"VISION_API_KEY": "sk-caller-pinned", "VISION_MODEL": "pinned-model"}
        await dc.inject_env_key_credentials(
            env_keys=env,
            prefix="VISION",
            model_id="builtin-chat",
            user_id="u",
            resolved_keys=dict(USER_KEYS),
            capability="vision",
            dependencies=_deps(),
        )
        assert env["VISION_API_KEY"] == "sk-caller-pinned"
        assert env["VISION_MODEL"] == "pinned-model"

    @pytest.mark.asyncio
    async def test_endpoint_row_beats_a_stale_caller_transport(self, patched_main):
        """Endpoint-backed models: the row IS the transport authority.

        Deliberately different from the provider-key branch above — a rehydrated
        session must not keep pointing at a retired base_url.
        """
        section = {"api_key": "sk-stale", "base_url": "https://stale/v1"}
        await dc.inject_model_credentials(
            section=section,
            model_id="endpoint-chat",
            user_id="u",
            resolved_keys=dict(USER_KEYS),
            dependencies=_deps(),
        )
        assert section["api_key"] == ENDPOINT_KEY
        assert section["base_url"] == ENDPOINT_BASE_URL

    @pytest.mark.asyncio
    async def test_resolved_key_map_is_the_last_resort(self, patched_main):
        section: dict = {}
        await dc.inject_model_credentials(
            section=section,
            model_id="router-chat",
            user_id="u",
            resolved_keys=dict(USER_KEYS),
            dependencies=_deps(),
        )
        assert section["api_key"] == USER_OPENROUTER_KEY
        assert section["provider"] == "openrouter"

    @pytest.mark.asyncio
    async def test_no_key_map_entry_means_no_key(self, patched_main):
        """A provider absent from the map leaves the section keyless, not guessed."""
        section: dict = {}
        await dc.inject_model_credentials(
            section=section,
            model_id="router-chat",
            user_id="u",
            resolved_keys={"anthropic": "sk-not-mine"},
            dependencies=_deps(),
        )
        assert "api_key" not in section

    @pytest.mark.asyncio
    async def test_chat_model_precedence_section_then_system_default(
        self, patched_main
    ):
        """Section pin wins; only an empty slot takes the system default."""
        patched_main.defaults.side_effect = _defaults(chat="builtin-chat")
        with patch(
            "orchestrator.services.capability_credentials.resolve_capability_credentials",
            AsyncMock(return_value=None),
        ):
            pinned = await dc.inject_thread_dispatch_credentials(
                {"llm": {"model": "router-chat"}},
                user_id="u",
                project_id="p",
                dependencies=_deps(),
            )
            empty = await dc.inject_thread_dispatch_credentials(
                {}, user_id="u", project_id="p", dependencies=_deps()
            )
        assert pinned["llm"]["model"] == "router-chat"
        assert empty["llm"]["model"] == "builtin-chat"

    @pytest.mark.asyncio
    async def test_auxiliary_precedence_section_then_user_setting_then_system(
        self, patched_main
    ):
        patched_main.defaults.side_effect = _defaults(auxiliary="builtin-chat")
        with patch(
            "orchestrator.services.capability_credentials.resolve_capability_credentials",
            AsyncMock(return_value=None),
        ):
            pinned = await dc.inject_thread_dispatch_credentials(
                {"auxiliary": {"model": "endpoint-chat"}},
                user_id="u",
                project_id="p",
                user_settings={"default_auxiliary_model": "router-chat"},
                dependencies=_deps(),
            )
            from_setting = await dc.inject_thread_dispatch_credentials(
                {},
                user_id="u",
                project_id="p",
                user_settings={"default_auxiliary_model": "router-chat"},
                dependencies=_deps(),
            )
            from_system = await dc.inject_thread_dispatch_credentials(
                {}, user_id="u", project_id="p", user_settings={}, dependencies=_deps()
            )
        assert pinned["auxiliary"]["model"] == "endpoint-chat"
        assert from_setting["auxiliary"]["model"] == "router-chat"
        assert from_system["auxiliary"]["model"] == "builtin-chat"

    @pytest.mark.asyncio
    async def test_embedding_precedence_persisted_then_setting_then_system(
        self, patched_main
    ):
        patched_main.defaults.side_effect = _defaults(embedding="sys-embed")
        with patch(
            "orchestrator.services.capability_credentials.resolve_capability_credentials",
            AsyncMock(return_value=None),
        ):
            persisted = await dc.inject_thread_dispatch_credentials(
                {"env_keys": {"EMBEDDING_MODEL": "router-chat"}},
                user_id="u",
                project_id="p",
                user_settings={"default_embedding_model": "builtin-chat"},
                dependencies=_deps(),
            )
            from_setting = await dc.inject_thread_dispatch_credentials(
                {},
                user_id="u",
                project_id="p",
                user_settings={"default_embedding_model": "builtin-chat"},
                dependencies=_deps(),
            )
            from_system = await dc.inject_thread_dispatch_credentials(
                {}, user_id="u", project_id="p", user_settings={}, dependencies=_deps()
            )
        assert persisted["env_keys"]["EMBEDDING_MODEL"] == "router-chat"
        assert from_setting["env_keys"]["EMBEDDING_MODEL"] == "builtin-chat"
        assert from_system["env_keys"]["EMBEDDING_MODEL"] == "sys-embed"

    @pytest.mark.asyncio
    async def test_rerank_precedence_persisted_then_setting_then_system(
        self, patched_main
    ):
        # The `rerank` catalog slot travels like embedding: persisted block,
        # then the user's pin, then the admin default — and it is additive
        # (no RERANK_* when nothing resolves; the agent then rides EMBEDDING_*).
        patched_main.defaults.side_effect = _defaults(rerank="sys-rerank")
        with patch(
            "orchestrator.services.capability_credentials.resolve_capability_credentials",
            AsyncMock(return_value=None),
        ):
            persisted = await dc.inject_thread_dispatch_credentials(
                {"env_keys": {"RERANK_MODEL": "endpoint-chat"}},
                user_id="u",
                project_id="p",
                user_settings={"default_rerank_model": "builtin-chat"},
                dependencies=_deps(),
            )
            from_setting = await dc.inject_thread_dispatch_credentials(
                {},
                user_id="u",
                project_id="p",
                user_settings={"default_rerank_model": "builtin-chat"},
                dependencies=_deps(),
            )
            from_system = await dc.inject_thread_dispatch_credentials(
                {}, user_id="u", project_id="p", user_settings={}, dependencies=_deps()
            )
            patched_main.defaults.side_effect = _defaults()
            unpinned = await dc.inject_thread_dispatch_credentials(
                {}, user_id="u", project_id="p", user_settings={}, dependencies=_deps()
            )
        assert persisted["env_keys"]["RERANK_MODEL"] == "endpoint-chat"
        assert persisted["env_keys"]["RERANK_BASE_URL"] == ENDPOINT_BASE_URL
        assert persisted["env_keys"]["RERANK_API_KEY"] == ENDPOINT_KEY
        assert from_setting["env_keys"]["RERANK_MODEL"] == "builtin-chat"
        assert from_system["env_keys"]["RERANK_MODEL"] == "sys-rerank"
        assert "RERANK_MODEL" not in (unpinned.get("env_keys") or {})

    @pytest.mark.asyncio
    async def test_nested_roster_slots_are_credentialed_and_inherit_is_skipped(
        self, patched_main
    ):
        override = {
            "llm": {"model": "builtin-chat", "summarization": {"model": "router-chat"}},
            "subagents": {
                "llm": {"model": "endpoint-chat"},
                "roster": {
                    "critic": {"llm": {"model": "router-chat"}},
                    "clone": {"llm": {"model": "inherit"}},
                    "empty": {"llm": {}},
                },
            },
        }
        with patch(
            "orchestrator.services.capability_credentials.resolve_capability_credentials",
            AsyncMock(return_value=None),
        ):
            out = await dc.inject_thread_dispatch_credentials(
                override, user_id="u", project_id="p", dependencies=_deps()
            )
        assert out["llm"]["summarization"]["api_key"] == USER_OPENROUTER_KEY
        assert out["subagents"]["llm"]["api_key"] == ENDPOINT_KEY
        assert out["subagents"]["roster"]["critic"]["llm"]["api_key"] == (
            USER_OPENROUTER_KEY
        )
        # `inherit` is a sentinel, not a model: it is never looked up at all.
        # Asserting only "no api_key landed" would pass for an injector that DID
        # look it up and merely failed to resolve it — which would also stamp
        # `provider`/`extra_headers` and burn a registry call per dispatch.
        looked_up = [c.args[0] for c in patched_main.resolver.await_args_list]
        assert "inherit" not in looked_up
        assert out["subagents"]["roster"]["clone"]["llm"] == {"model": "inherit"}
        assert out["subagents"]["roster"]["empty"]["llm"] == {}

    @pytest.mark.asyncio
    async def test_none_sentinels_are_treated_as_absent_on_reinjection(
        self, patched_main
    ):
        """A hot-swap's explicit ``None`` must not block setdefault re-injection."""
        with patch(
            "orchestrator.services.capability_credentials.resolve_capability_credentials",
            AsyncMock(return_value=None),
        ):
            out = await dc.inject_thread_dispatch_credentials(
                {
                    "llm": {
                        "model": "endpoint-chat",
                        "provider": None,
                        "base_url": None,
                        "api_key": None,
                        "summarization": {"model": "router-chat", "api_key": None},
                    }
                },
                user_id="u",
                project_id="p",
                dependencies=_deps(),
            )
        assert out["llm"]["api_key"] == ENDPOINT_KEY
        assert out["llm"]["summarization"]["api_key"] == USER_OPENROUTER_KEY

    @pytest.mark.asyncio
    async def test_seed_setdefault_leaves_a_caller_pin_and_never_mutates_input(
        self, patched_main
    ):
        original = {"llm": {"model": "endpoint-chat", "model_max_context_tokens": 42}}
        snapshot = copy.deepcopy(original)
        out = await dc.seed_registry_model_overrides(
            original, user_id="u", dependencies=_deps()
        )
        assert out["llm"]["model_max_context_tokens"] == 42
        assert out["llm"]["max_output_tokens"] == 8191
        assert original == snapshot, "the persisted config_override was mutated"
        assert out is not original


def _defaults(**pins):
    """An async ``resolve_default_for_capability`` stub pinned per capability.

    A plain lambda would make ``AsyncMock`` hand back the *coroutine* instead of
    awaiting it, and the injector would then route on a coroutine object — the
    kind of silently-inert stub R1.B05 §P3 warns about.
    """

    async def _resolve(capability):
        return pins.get(capability)

    return _resolve


# ==========================================================================
# 4. The fallback is a fallback, not a default
# ==========================================================================


class TestProviderFallbackIsNotADefault:
    def test_explicit_provider_wins_over_the_model_heuristic(self):
        assert (
            dc.dispatch_llm_provider_fallback(
                _job(), {"llm": {"provider": "Groq", "model": "claude-opus-4-6"}}
            )
            == "groq"
        )

    def test_model_heuristic_wins_over_the_config_name_heuristic(self):
        assert (
            dc.dispatch_llm_provider_fallback(
                _job("anthropic_worker"), {"llm": {"model": "gpt-5.5"}}
            )
            == "openai"
        )

    def test_config_name_heuristic_is_below_the_model_heuristic(self):
        assert (
            dc.dispatch_llm_provider_fallback(
                _job("anthropic_worker"), {"llm": {"model": "Qwen/Qwen3-32B"}}
            )
            == "anthropic"
        )

    def test_last_resort_is_openai(self):
        assert dc.dispatch_llm_provider_fallback(_job(), {}) == "openai"

    @pytest.mark.asyncio
    async def test_registry_hit_never_consults_the_prefix_heuristic(
        self, patched_main, monkeypatch
    ):
        """A resolvable model routes off ``meta.api_key_ref``, not the prefix map.

        Tripwire, not a mock assertion: if the heuristic is reached at all the
        test fails loudly rather than passing on an unobserved call.
        """

        def tripwire(model):  # pragma: no cover - must never run
            raise AssertionError(f"prefix heuristic consulted for {model!r}")

        monkeypatch.setattr(dc, "provider_of_model", tripwire)
        section: dict = {}
        await dc.inject_model_credentials(
            section=section,
            model_id="router-chat",
            user_id="u",
            resolved_keys=dict(USER_KEYS),
            dependencies=_deps(),
        )
        assert section["api_key"] == USER_OPENROUTER_KEY

    @pytest.mark.asyncio
    async def test_registry_miss_does_consult_the_prefix_heuristic(
        self, patched_main, monkeypatch
    ):
        """Proves the tripwire above is watching a path that really exists."""
        seen: list[str] = []

        def spy(model):
            seen.append(model)
            return "openai"

        monkeypatch.setattr(dc, "provider_of_model", spy)
        section: dict = {}
        await dc.inject_model_credentials(
            section=section,
            model_id="unknown-model",
            user_id="u",
            resolved_keys=dict(USER_KEYS),
            dependencies=_deps(),
        )
        assert seen == ["unknown-model"]
        assert section["api_key"] == USER_OPENAI_KEY
        assert section["provider"] == "openai"

    @pytest.mark.asyncio
    async def test_job_dispatch_does_not_fall_back_when_the_registry_resolved(
        self, patched_main, monkeypatch
    ):
        """Characterization of the *call site* (``inject_dispatch_credentials`` as the
        job-start bundle binds it).

        The fallback lives behind ``meta.api_key_ref`` there. Pinned here so the
        guard cannot be dropped while the helper keeps passing its own tests.
        """

        def tripwire(job, config_override):  # pragma: no cover - must never run
            raise AssertionError("provider fallback fired on a resolved model")

        monkeypatch.setattr(
            dispatch_credentials_module, "dispatch_llm_provider_fallback", tripwire
        )
        with patch(
            "orchestrator.services.capability_credentials.resolve_capability_credentials",
            AsyncMock(return_value=None),
        ):
            inject = preparation_composition.job_start_bundle_dependencies(
                orchestrator.main.app.state.resources
            ).inject_dispatch_credentials
            out = await inject(_job(), {"llm": {"model": "router-chat"}})
        assert out["llm"]["api_key"] == USER_OPENROUTER_KEY


# ==========================================================================
# 5. The usage poll loop
# ==========================================================================


class _Conn:
    async def fetchval(self, _sql):
        return None


class _Acquire:
    async def __aenter__(self):
        return _Conn()

    async def __aexit__(self, *exc):
        return False


class _Pool:
    def acquire(self):
        return _Acquire()


def _ledger(available: bool = True):
    return SimpleNamespace(is_available=available)


class TestLlmUsagePollLoop:
    @pytest.mark.asyncio
    async def test_disabled_without_a_ledger(self, caplog):
        caplog.set_level(logging.INFO)
        await audit_usage.llm_usage_poll_loop(
            asyncio.Event(),
            audit_db=SimpleNamespace(pool=_Pool()),
            app_store=SimpleNamespace(pool=_Pool()),
            usage_ledger=None,
        )
        assert "usage ledger unavailable" in caplog.text

    @pytest.mark.asyncio
    async def test_disabled_without_pools(self, caplog):
        caplog.set_level(logging.INFO)
        await audit_usage.llm_usage_poll_loop(
            asyncio.Event(),
            audit_db=SimpleNamespace(pool=None),
            app_store=SimpleNamespace(pool=_Pool()),
            usage_ledger=_ledger(),
        )
        assert "audit/app pool unavailable" in caplog.text

    @pytest.mark.asyncio
    async def test_a_failing_tick_is_non_fatal_and_the_loop_keeps_its_cursor(
        self, monkeypatch
    ):
        calls: list[object] = []

        async def boom(*args, **kwargs):
            calls.append(kwargs.get("since_ts"))
            if len(calls) == 1:
                raise RuntimeError("tick blew up")
            return {"cursor": "advanced"}

        monkeypatch.setattr(audit_usage, "materialize_llm_usage_from_audit", boom)
        stop = asyncio.Event()
        task = asyncio.create_task(
            audit_usage.llm_usage_poll_loop(
                stop,
                audit_db=SimpleNamespace(pool=_Pool()),
                app_store=SimpleNamespace(pool=_Pool()),
                usage_ledger=_ledger(),
                interval=0.001,
            )
        )
        for _ in range(400):
            await asyncio.sleep(0.005)
            if len(calls) >= 2:
                break
        stop.set()
        await asyncio.wait_for(task, timeout=2)
        assert len(calls) >= 2, "the loop did not survive the failing tick"
        assert calls[1] == calls[0], "a failed tick must not advance the cursor"

    @pytest.mark.asyncio
    async def test_cancellation_is_not_swallowed(self, monkeypatch, caplog):
        """A poll loop that eats CancelledError blocks shutdown."""
        caplog.set_level(logging.INFO)
        started = asyncio.Event()

        async def tick(*args, **kwargs):
            started.set()
            return {"cursor": None}

        monkeypatch.setattr(audit_usage, "materialize_llm_usage_from_audit", tick)
        stop_all = asyncio.Event()
        task = asyncio.create_task(
            audit_usage.llm_usage_poll_loop(
                stop_all,
                audit_db=SimpleNamespace(pool=_Pool()),
                app_store=SimpleNamespace(pool=_Pool()),
                usage_ledger=_ledger(),
                interval=30.0,
            )
        )
        await asyncio.wait_for(started.wait(), timeout=5)
        task.cancel()
        # asyncio.wait rather than `await task`: a loop that SWALLOWS the
        # cancellation would keep looping forever and hang the suite instead of
        # failing it. This turns that into a named assertion.
        _done, pending = await asyncio.wait({task}, timeout=5)
        if pending:
            stop_all.set()
            task.cancel()
            await asyncio.wait({task}, timeout=5)
            pytest.fail("poll loop swallowed CancelledError — shutdown would hang")
        assert task.cancelled()
        assert "LLM usage poll loop stopped" in caplog.text
