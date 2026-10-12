"""Dispatch-time delivery of catalog-resolved search/fetch providers.

R1.B05 lane C re-pointed these from ``orchestrator.main._inject_search_credentials``
to ``orchestrator.services.dispatch_credentials.inject_search_credentials``. The
injector's only main-local callers were the two dispatch-credential composers,
both of which move in this batch, so the main name has no caller left and these
cases must exercise the service directly rather than a bridge that is about to
be deleted.

The ``resolve_capability_credentials`` patch still targets the attribute on
``orchestrator.services.capability_credentials`` — the module that owns it — and
the injector still resolves it through a function-local import, so the patch is
reached from the new home exactly as it was from the old one. Each case asserts
the stub actually ran (R1.B05 §P3: a patch that silently does nothing while the
test passes is the dangerous failure).
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, patch

import pytest
from orchestrator.application import preparation as preparation_composition
from shared.runtime.core import model_registry as model_registry_module

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

import orchestrator.main  # noqa: E402
from orchestrator.services import dispatch_credentials  # noqa: E402
from orchestrator.services.capability_credentials import CapabilityCredentials  # noqa: E402


def _deps() -> dispatch_credentials.DispatchCredentialDependencies:
    """The dependency object a main factory builds, read at call time.

    ``postgres_db`` and ``logger`` are rebound during ``lifespan``, so they are
    resolved per invocation rather than captured at import (R1.B05 §P1).
    """
    return dispatch_credentials.DispatchCredentialDependencies(
        store=orchestrator.main.app.state.resources.postgres_db,
        logger=preparation_composition.logger,
        resolve_model=model_registry_module.resolve_model,
    )


@pytest.mark.asyncio
async def test_injects_primary_search_and_fetch_sections():
    async def resolve(*, capability, **kwargs):
        del kwargs
        if capability == "search":
            return CapabilityCredentials(
                model="searxng",
                base_url="http://searxng.svc:8080",
                api_key=None,
                provider="searxng",
                params={"provider": "searxng", "ops": ["search"]},
                catalog_id="search-row",
            )
        return CapabilityCredentials(
            model="tavily",
            base_url="https://api.tavily.com",
            api_key="tvly-secret",
            provider="tavily",
            params={
                "provider": "tavily",
                "ops": ["extract", "crawl", "map"],
            },
            catalog_id="fetch-row",
        )

    config = {}
    with patch(
        "orchestrator.services.capability_credentials.resolve_capability_credentials",
        AsyncMock(side_effect=resolve),
    ) as resolver:
        result = await dispatch_credentials.inject_search_credentials(
            config,
            user_settings={"default_search_model": "searxng"},
            user_id="user-1",
            resolved_keys={},
            dependencies=_deps(),
        )

    assert resolver.await_count == 3  # search, fetch, fallback — the patch was reached
    assert result["research"] == {
        "search": {
            "provider": "searxng",
            "base_url": "http://searxng.svc:8080",
            "api_key": None,
            "ops": ["search"],
        },
        "fetch": {
            "provider": "tavily",
            "base_url": "https://api.tavily.com",
            "api_key": "tvly-secret",
            "ops": ["extract", "crawl", "map"],
        },
    }


@pytest.mark.asyncio
async def test_removes_stale_sections_when_nothing_resolves():
    config = {
        "research": {
            "search": {"provider": "stale"},
            "fetch": {"provider": "stale"},
        }
    }
    with patch(
        "orchestrator.services.capability_credentials.resolve_capability_credentials",
        AsyncMock(return_value=None),
    ) as resolver:
        await dispatch_credentials.inject_search_credentials(
            config,
            user_settings={},
            user_id="user-1",
            resolved_keys={},
            dependencies=_deps(),
        )

    assert resolver.await_count == 3
    assert "research" not in config


@pytest.mark.asyncio
async def test_malformed_provider_params_degrade_without_credentials():
    creds = CapabilityCredentials(
        model="broken-row",
        base_url="https://provider.invalid",
        api_key="must-not-survive",
        provider=None,
        params={"ops": ["search"]},
    )
    config = {}
    with patch(
        "orchestrator.services.capability_credentials.resolve_capability_credentials",
        AsyncMock(return_value=creds),
    ) as resolver:
        await dispatch_credentials.inject_search_credentials(
            config,
            user_settings={},
            user_id="user-1",
            resolved_keys={},
            dependencies=_deps(),
        )

    assert resolver.await_count == 3
    assert "research" not in config


@pytest.mark.asyncio
async def test_different_catalog_row_is_injected_as_search_fallback():
    primary = CapabilityCredentials(
        model="tavily",
        base_url="https://api.tavily.com",
        api_key="primary-key",
        provider="tavily",
        params={"provider": "tavily", "ops": ["search"]},
        catalog_id="primary-row",
    )
    fallback = CapabilityCredentials(
        model="searxng",
        base_url="http://searxng.svc:8080",
        provider="searxng",
        params={"provider": "searxng", "ops": ["search"]},
        catalog_id="fallback-row",
    )

    async def resolve(*, capability, setting_key=None, **kwargs):
        del kwargs
        if setting_key == "default_search_fallback_model":
            return fallback
        return primary if capability == "search" else None

    config = {}
    with patch(
        "orchestrator.services.capability_credentials.resolve_capability_credentials",
        AsyncMock(side_effect=resolve),
    ) as resolver:
        await dispatch_credentials.inject_search_credentials(
            config,
            user_settings={},
            user_id="user-1",
            resolved_keys={},
            dependencies=_deps(),
        )

    assert resolver.await_count == 3
    assert config["research"]["search_fallback"] == {
        "provider": "searxng",
        "base_url": "http://searxng.svc:8080",
        "api_key": None,
        "ops": ["search"],
    }


@pytest.mark.asyncio
async def test_same_catalog_row_is_not_injected_as_its_own_fallback():
    same = CapabilityCredentials(
        model="tavily",
        base_url="https://api.tavily.com",
        api_key="key",
        provider="tavily",
        params={"provider": "tavily", "ops": ["search"]},
        catalog_id="same-row",
    )

    async def resolve(*, capability, **kwargs):
        del kwargs
        return same if capability == "search" else None

    config = {}
    with patch(
        "orchestrator.services.capability_credentials.resolve_capability_credentials",
        AsyncMock(side_effect=resolve),
    ) as resolver:
        await dispatch_credentials.inject_search_credentials(
            config,
            user_settings={},
            user_id="user-1",
            resolved_keys={},
            dependencies=_deps(),
        )

    assert resolver.await_count == 3
    assert "search_fallback" not in config["research"]
