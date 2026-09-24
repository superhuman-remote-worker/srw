"""Tests for the persistent-session credential at-rest fix.

Two guarantees:
1. ``_inject_thread_dispatch_credentials`` is idempotent + re-injection-safe —
   running it on a *stripped* config_override (the at-rest representation)
   repopulates exactly the removed secrets. This is what lets session
   attach/resume work after secrets stop being persisted.
2. ``redact_config_override`` round-trips with the injector (strip → re-inject
   restores the keys), and ``backfill_strip_thread_config_secrets`` removes
   legacy plaintext idempotently.

Harness mirrors tests/test_dispatch_phase_credentials.py.
"""

from __future__ import annotations

from tests._expert_catalog import patch_service_method
from orchestrator.services import expert_catalog as expert_catalog_module
from orchestrator.services import session_config_resolution


import json
import os
from unittest.mock import AsyncMock, MagicMock

import pytest
from orchestrator.application import preparation as preparation_composition
from orchestrator.services import config_resolver as config_resolver_module
from orchestrator.services import deployment_gates as deployment_gates_module
from orchestrator.services import dispatch_credentials as dispatch_credentials_module
from orchestrator.services import grant_enforcement as grant_enforcement_module
from orchestrator.services import (
    session_config_resolution as session_config_resolution_module,
)
from orchestrator.services import thread_mount_rows as thread_mount_rows_module
from shared.runtime.core import model_registry as model_registry_module

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

import orchestrator.main  # noqa: E402
from orchestrator.security.access import redact_config_override  # noqa: E402
from shared.runtime.core.model_registry import ModelMeta  # noqa: E402


ENDPOINT_ID = "11111111-1111-1111-1111-111111111111"
BASE_URL = "https://ai.h4ll.app/v1"
API_KEY = "sk-endpoint-SECRET"


@pytest.fixture
def patched_main(monkeypatch):
    """Patch the DB + registry collaborators so the injector exercises only its
    branching. ``custom-model`` is an endpoint-backed model (inlines
    base_url+api_key); everything else is unknown."""

    async def fake_resolve(model_id, user_id=None, capability="chat"):
        if model_id == "custom-model":
            return ModelMeta(
                model_id="custom-model",
                provider="openai",
                family="gpt",
                display_name="Custom Model",
                origin="custom",
                endpoint_id=ENDPOINT_ID,
                api_key_ref="openai",
            )
        return None

    async def fake_get_endpoint(endpoint_id):
        if endpoint_id == ENDPOINT_ID:
            return {
                "id": ENDPOINT_ID,
                "label": "endpoint",
                "base_url": BASE_URL,
                "api_key": API_KEY,
            }
        return None

    monkeypatch.setattr(
        model_registry_module,
        "resolve_model",
        AsyncMock(side_effect=fake_resolve),
        raising=True,
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "get_user_llm_endpoint",
        AsyncMock(side_effect=fake_get_endpoint),
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "resolve_api_keys_for_job",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "resolve_default_for_capability",
        AsyncMock(return_value=None),
    )


class TestInjectThreadCredentials:
    @pytest.mark.asyncio
    async def test_fresh_injection(self, patched_main):
        co = {"llm": {"model": "custom-model"}}
        out = await dispatch_credentials_module.inject_thread_dispatch_credentials(
            co,
            user_id="u",
            project_id="p",
            dependencies=preparation_composition.dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert out["llm"]["base_url"] == BASE_URL
        assert out["llm"]["api_key"] == API_KEY

    @pytest.mark.asyncio
    async def test_reinjection_after_strip(self, patched_main):
        """Keystone: a stripped copy (model + base_url survive, api_key removed)
        gets api_key re-injected. This is the resume/workspace-endpoint path."""
        stripped = {
            "llm": {"model": "custom-model", "provider": "openai", "base_url": BASE_URL}
        }
        out = await dispatch_credentials_module.inject_thread_dispatch_credentials(
            stripped,
            user_id="u",
            project_id="p",
            dependencies=preparation_composition.dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert out["llm"]["api_key"] == API_KEY
        assert out["llm"]["base_url"] == BASE_URL

    @pytest.mark.asyncio
    async def test_none_transport_sentinels_are_repopulated(self, patched_main):
        """A prior hot-swap leaves provider/base_url=None sentinels in the stored
        copy; re-injection must treat them as absent and repopulate."""
        stored = {"llm": {"model": "custom-model", "provider": None, "base_url": None}}
        out = await dispatch_credentials_module.inject_thread_dispatch_credentials(
            stored,
            user_id="u",
            project_id="p",
            dependencies=preparation_composition.dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert out["llm"]["base_url"] == BASE_URL
        assert out["llm"]["api_key"] == API_KEY

    @pytest.mark.asyncio
    async def test_redact_then_reinject_round_trip(self, patched_main):
        enriched = await dispatch_credentials_module.inject_thread_dispatch_credentials(
            {"llm": {"model": "custom-model"}},
            user_id="u",
            project_id="p",
            dependencies=preparation_composition.dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        stripped = redact_config_override(enriched)
        # The at-rest copy keeps model/base_url, drops the key.
        assert "api_key" not in stripped["llm"]
        assert stripped["llm"]["base_url"] == BASE_URL
        # Re-injection restores it.
        restored = await dispatch_credentials_module.inject_thread_dispatch_credentials(
            stripped,
            user_id="u",
            project_id="p",
            dependencies=preparation_composition.dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert restored["llm"]["api_key"] == API_KEY

    @pytest.mark.asyncio
    async def test_user_memory_profile_does_not_change_system_kb_profile(
        self, monkeypatch
    ):
        kb_model = "system-kb-model"

        async def fake_default(capability):
            return kb_model if capability == "embedding" else None

        async def fake_resolve(model_id, user_id=None, capability="chat"):
            if model_id == kb_model:
                return ModelMeta(
                    model_id=kb_model,
                    provider="openai",
                    family="default",
                    display_name="System KB",
                    origin="system",
                    endpoint_id=ENDPOINT_ID,
                    capability="embedding",
                )
            return None

        monkeypatch.setattr(
            orchestrator.main.app.state.resources.postgres_db,
            "resolve_default_for_capability",
            AsyncMock(side_effect=fake_default),
        )
        monkeypatch.setattr(
            orchestrator.main.app.state.resources.postgres_db,
            "resolve_api_keys_for_job",
            AsyncMock(return_value={}),
        )
        monkeypatch.setattr(
            orchestrator.main.app.state.resources.postgres_db,
            "get_user_llm_endpoint",
            AsyncMock(
                return_value={
                    "base_url": BASE_URL,
                    "api_key": API_KEY,
                }
            ),
        )
        monkeypatch.setattr(
            model_registry_module,
            "resolve_model",
            AsyncMock(side_effect=fake_resolve),
            raising=True,
        )
        co = {
            "env_keys": {
                "EMBEDDING_PROVIDER": "openrouter",
                "EMBEDDING_MODEL": "user-memory-model",
                "EMBEDDING_BASE_URL": "https://user.example/v1",
                "EMBEDDING_API_KEY": "user-key",
            }
        }

        out = await dispatch_credentials_module.inject_thread_dispatch_credentials(
            co,
            user_id="u",
            project_id="p",
            include_kb_profile=True,
            dependencies=preparation_composition.dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

        env = out["env_keys"]
        assert env["EMBEDDING_MODEL"] == "user-memory-model"
        assert env["EMBEDDING_API_KEY"] == "user-key"
        assert env["KB_EMBEDDING_MODEL"] == kb_model
        assert env["KB_EMBEDDING_BASE_URL"] == BASE_URL
        assert env["KB_EMBEDDING_API_KEY"] == API_KEY
        assert env["KB_EMBEDDING_PROVIDER"] == "openai"


@pytest.mark.asyncio
async def test_resolved_session_uses_canonical_mount_projects_for_kb_gate(monkeypatch):
    """A mount-only project thread still receives KB_* in the preferred blob."""
    thread_id = "aaaaaaaa-1111-2222-3333-444444444444"
    mounted_project = "bbbbbbbb-1111-2222-3333-444444444444"
    monkeypatch.setattr(deployment_gates_module, "is_experts_db_enabled", lambda: True)
    monkeypatch.setattr(
        grant_enforcement_module,
        "user_experts_enabled",
        AsyncMock(return_value=True),
        raising=True,
    )
    # R1.B05 lane P: ``_resolve_default_models``, ``resolve_config`` and
    # ``inject_blob_credentials`` are resolved in
    # ``services.session_config_resolution``'s own namespace once main
    # delegates, so each is patched on BOTH modules — the main entry is the
    # transitional half. Reachedness is proved below by
    # ``project_lookup.assert_awaited_once_with`` and by the result equalling
    # the value only ``fake_resolve_config`` can produce.
    monkeypatch.setattr(
        session_config_resolution_module,
        "resolve_default_models",
        AsyncMock(return_value={}),
        raising=True,
    )
    monkeypatch.setattr(
        session_config_resolution,
        "resolve_default_models",
        AsyncMock(return_value={}),
        raising=True,
    )
    patch_service_method(
        monkeypatch,
        expert_catalog_module.ExpertCatalogService,
        "gather_in_scope_skills",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        dispatch_credentials_module,
        "seed_registry_model_overrides",
        AsyncMock(side_effect=lambda override, **_kwargs: override),
        raising=True,
    )
    project_lookup = AsyncMock(return_value=[mounted_project])
    monkeypatch.setattr(
        thread_mount_rows_module, "thread_project_ids", project_lookup, raising=True
    )
    monkeypatch.setattr(
        grant_enforcement_module,
        "enforce_dispatch_grants",
        AsyncMock(return_value=None),
        raising=True,
    )
    injector = AsyncMock(side_effect=lambda co, **_kwargs: co)
    monkeypatch.setattr(
        dispatch_credentials_module,
        "inject_thread_dispatch_credentials",
        injector,
        raising=True,
    )

    def fake_resolve_config(*, capture, **_kwargs):
        capture["merged_fragment"] = {}
        return {"agent": {}}

    monkeypatch.setattr(
        config_resolver_module, "resolve_config", fake_resolve_config, raising=True
    )
    monkeypatch.setattr(
        session_config_resolution, "resolve_config", fake_resolve_config, raising=True
    )

    async def fake_inject_blob(blob, callback):
        await callback({})
        return blob

    monkeypatch.setattr(
        config_resolver_module,
        "inject_blob_credentials",
        fake_inject_blob,
        raising=True,
    )
    monkeypatch.setattr(
        session_config_resolution,
        "inject_blob_credentials",
        fake_inject_blob,
        raising=True,
    )
    # The helper imports this function inside the call; patch the source module.
    monkeypatch.setattr(
        "shared.runtime.core.skill_resolution.filter_bound_skills", MagicMock()
    )

    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "fetchrow",
        AsyncMock(return_value=None),
    )
    result = await session_config_resolution_module.resolve_session_config(
        {"id": thread_id, "project_id": None, "config_name": "persistent_defaults"},
        {},
        dependencies=preparation_composition.session_config_dependencies(
            orchestrator.main.app.state.resources
        ),
    )

    assert result == {"agent": {}}
    # R1.B12: the composition binds the owner with its dependency object, so
    # the call carries ``dependencies=`` for this application's store.
    project_lookup.assert_awaited_once()
    assert project_lookup.await_args.args == (thread_id,)
    assert set(project_lookup.await_args.kwargs) == {"dependencies"}
    assert (
        project_lookup.await_args.kwargs["dependencies"].store
        is orchestrator.main.app.state.resources.postgres_db
    )
    assert injector.await_args.kwargs["include_kb_profile"] is True


CODEX_ENDPOINT_ID = "44444444-4444-4444-4444-444444444444"
CODEX_BASE_URL = "http://srw-codex-proxy:8317/v1"
CODEX_API_KEY = "sk-codex-endpoint-test"
STALE_BASE_URL = "https://stale.example/v1"


@pytest.fixture
def patched_main_codex_endpoint(monkeypatch):
    """A codex model (`gpt-5.5`) on the codex-proxy endpoint."""

    async def fake_resolve(model_id, user_id=None, capability="chat"):
        if model_id == "gpt-5.5":
            return ModelMeta(
                model_id="gpt-5.5",
                provider="codex",
                family="gpt-5",
                display_name="GPT-5.5",
                origin="catalog",
                endpoint_id=CODEX_ENDPOINT_ID,
            )
        return None

    async def fake_get_endpoint(endpoint_id):
        if endpoint_id == CODEX_ENDPOINT_ID:
            return {
                "id": CODEX_ENDPOINT_ID,
                "label": "codex-proxy",
                "base_url": CODEX_BASE_URL,
                "api_key": CODEX_API_KEY,
            }
        return None

    monkeypatch.setattr(
        model_registry_module,
        "resolve_model",
        AsyncMock(side_effect=fake_resolve),
        raising=True,
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "get_user_llm_endpoint",
        AsyncMock(side_effect=fake_get_endpoint),
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "resolve_api_keys_for_job",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        orchestrator.main.app.state.resources.postgres_db,
        "resolve_default_for_capability",
        AsyncMock(return_value=None),
    )


class TestCodexSessionStaleTransport:
    """Endpoint-backed sessions refresh stale persisted transport on reattach."""

    @pytest.mark.asyncio
    async def test_stale_base_url_replaced_with_codex_endpoint(
        self, patched_main_codex_endpoint
    ):
        """A stored llm override can carry transport from a previous model.
        Re-injection on resume must route to the codex proxy with its own key."""
        stored = {
            "llm": {
                "model": "gpt-5.5",
                "base_url": STALE_BASE_URL,
                "api_key": "sk-stale",
                "provider": "openai",  # stale factory
            }
        }
        out = await dispatch_credentials_module.inject_thread_dispatch_credentials(
            stored,
            user_id="u",
            dependencies=preparation_composition.dispatch_credential_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert out["llm"]["base_url"] == CODEX_BASE_URL
        assert out["llm"]["api_key"] == CODEX_API_KEY
        assert out["llm"]["provider"] == "codex"
        assert out["llm"]["base_url"] != STALE_BASE_URL


class _FakeAcquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *a):
        return False


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows
        self.updates: list = []

    async def fetch(self, *_a, **_k):
        return self._rows

    async def execute(self, _q, *args):
        self.updates.append(args)


class TestStripBackfill:
    @pytest.mark.asyncio
    async def test_strips_plaintext_and_skips_clean(self, monkeypatch):
        rows = [
            {
                "id": "t-dirty",
                "metadata": json.dumps(
                    {"config_override": {"llm": {"model": "m", "api_key": "SECRET"}}}
                ),
            },
            {
                "id": "t-clean",
                "metadata": json.dumps({"config_override": {"llm": {"model": "m"}}}),
            },
        ]
        conn = _FakeConn(rows)
        monkeypatch.setattr(
            orchestrator.main.app.state.resources.postgres_db,
            "acquire",
            lambda: _FakeAcquire(conn),
        )

        counts = await orchestrator.main.app.state.resources.postgres_db.backfill_strip_thread_config_secrets()

        assert counts == {"stripped": 1, "skipped": 1, "errors": 0}
        # Exactly one UPDATE, and its payload carries no secret.
        assert len(conn.updates) == 1
        payload_json = conn.updates[0][0]
        assert "SECRET" not in payload_json
        assert json.loads(payload_json) == {"llm": {"model": "m"}}

    @pytest.mark.asyncio
    async def test_idempotent_second_pass_is_noop(self, monkeypatch):
        rows = [
            {
                "id": "t-clean",
                "metadata": json.dumps({"config_override": {"llm": {"model": "m"}}}),
            }
        ]
        conn = _FakeConn(rows)
        monkeypatch.setattr(
            orchestrator.main.app.state.resources.postgres_db,
            "acquire",
            lambda: _FakeAcquire(conn),
        )

        counts = await orchestrator.main.app.state.resources.postgres_db.backfill_strip_thread_config_secrets()

        assert counts == {"stripped": 0, "skipped": 1, "errors": 0}
        assert conn.updates == []
