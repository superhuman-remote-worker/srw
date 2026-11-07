"""HTTP adapters for the provider catalog application owner."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, Request

from orchestrator.schemas.provider_catalog import (
    AdminDefaultModelSet,
    ApiKeySet,
    LlmEndpointCreate,
    LlmEndpointUpdate,
    SubscriptionModelImport,
)
from orchestrator.services.provider_catalog import ProviderCatalogService

router = APIRouter()


@dataclass(frozen=True)
class ProviderCatalogDependencies:
    service: ProviderCatalogService
    require_admin: Callable[[Request], Awaitable[dict[str, Any]]]


def get_provider_catalog_dependencies(request: Request) -> ProviderCatalogDependencies:
    return request.app.state.provider_catalog_dependencies_factory()


@router.get("/api/admin/providers/keys")
async def admin_list_provider_keys(
    request: Request,
    *,
    dependencies: ProviderCatalogDependencies = Depends(
        get_provider_catalog_dependencies
    ),
) -> list[dict[str, Any]]:
    """List system-scoped provider API keys (prefix only, no full keys)."""
    await dependencies.require_admin(request)
    return await dependencies.service.list_provider_keys()


@router.put("/api/admin/providers/keys/{provider}")
async def admin_set_provider_key(
    request: Request,
    provider: str,
    body: ApiKeySet,
    *,
    dependencies: ProviderCatalogDependencies = Depends(
        get_provider_catalog_dependencies
    ),
) -> dict[str, Any]:
    """Set or rotate the system-level API key for a provider.

    On success, schedules a non-blocking discovery probe for providers
    we know how to enumerate (see ``discovery_service.DISCOVERABLE_PROVIDERS``).
    The discovery cache is cleared inline before the probe fires so the
    cockpit never shows stale candidates from a previous key. When the
    ``admin.discovery_enabled`` flag is set to ``false``, no probe runs.
    """
    await dependencies.require_admin(request)
    return await dependencies.service.set_provider_key(provider=provider, body=body)


@router.delete("/api/admin/providers/keys/{provider}")
async def admin_delete_provider_key(
    request: Request,
    provider: str,
    *,
    dependencies: ProviderCatalogDependencies = Depends(
        get_provider_catalog_dependencies
    ),
) -> dict[str, str]:
    """Remove the system-level key for a provider."""
    await dependencies.require_admin(request)
    return await dependencies.service.delete_provider_key(provider=provider)


@router.get("/api/admin/providers/keys/{provider}/discovery")
async def admin_get_provider_discovery(
    request: Request,
    provider: str,
    *,
    dependencies: ProviderCatalogDependencies = Depends(
        get_provider_catalog_dependencies
    ),
) -> dict[str, Any]:
    """Return the cached discovery payload for a provider key.

    Powers the post-save confirmation dialog on Admin → Providers. The
    response is ``{ready, fresh, payload, cached_at}``:

    - ``ready=False`` when no probe has completed yet (e.g. the async
      probe scheduled by the PUT side-effect is still running).
    - ``fresh`` reflects the 24h TTL — the cockpit can prompt for an
      explicit rediscover when stale.
    - ``payload`` is the cockpit-ready candidate list shaped by
      :func:`discovery_service.build_cache_payload`.
    """
    await dependencies.require_admin(request)
    return await dependencies.service.get_provider_discovery(provider=provider)


@router.post("/api/admin/providers/keys/{provider}/rediscover")
async def admin_rediscover_provider_models(
    request: Request,
    provider: str,
    *,
    dependencies: ProviderCatalogDependencies = Depends(
        get_provider_catalog_dependencies
    ),
) -> dict[str, Any]:
    """Force-refresh the discovery cache for a provider key.

    Useful when the provider released new models since the cache was
    populated, or when the admin wants to retry after a transient probe
    failure. Returns the freshly-cached payload (synchronous probe — the
    button blocks until results come back, like an explicit "test" click).
    """
    await dependencies.require_admin(request)
    return await dependencies.service.rediscover_provider_models(provider=provider)


@router.get("/api/admin/providers/endpoints")
async def admin_list_provider_endpoints(
    request: Request,
    *,
    dependencies: ProviderCatalogDependencies = Depends(
        get_provider_catalog_dependencies
    ),
) -> list[dict[str, Any]]:
    """List system-scoped LLM endpoints with their models."""
    await dependencies.require_admin(request)
    return await dependencies.service.list_provider_endpoints()


@router.post("/api/admin/providers/endpoints")
async def admin_create_provider_endpoint(
    request: Request,
    body: LlmEndpointCreate,
    *,
    dependencies: ProviderCatalogDependencies = Depends(
        get_provider_catalog_dependencies
    ),
) -> dict[str, Any]:
    """Create a new system-scoped LLM endpoint (visible to every user)."""
    await dependencies.require_admin(request)
    return await dependencies.service.create_provider_endpoint(body=body)


@router.patch("/api/admin/providers/endpoints/{endpoint_id}")
async def admin_update_provider_endpoint(
    request: Request,
    endpoint_id: str,
    body: LlmEndpointUpdate,
    *,
    dependencies: ProviderCatalogDependencies = Depends(
        get_provider_catalog_dependencies
    ),
) -> dict[str, Any]:
    await dependencies.require_admin(request)
    return await dependencies.service.update_provider_endpoint(
        endpoint_id=endpoint_id, body=body
    )


@router.delete("/api/admin/providers/endpoints/{endpoint_id}")
async def admin_delete_provider_endpoint(
    request: Request,
    endpoint_id: str,
    *,
    dependencies: ProviderCatalogDependencies = Depends(
        get_provider_catalog_dependencies
    ),
) -> dict[str, str]:
    await dependencies.require_admin(request)
    return await dependencies.service.delete_provider_endpoint(endpoint_id=endpoint_id)


@router.post("/api/admin/providers/endpoints/{endpoint_id}/test")
async def admin_test_provider_endpoint(
    request: Request,
    endpoint_id: str,
    *,
    dependencies: ProviderCatalogDependencies = Depends(
        get_provider_catalog_dependencies
    ),
) -> dict[str, Any]:
    """Probe a system endpoint server-side: ``GET {base_url}/models``, or on a
    404 there one minimal embedding/rerank call when its rows declare one."""
    await dependencies.require_admin(request)
    return await dependencies.service.test_provider_endpoint(endpoint_id=endpoint_id)


@router.post("/api/admin/providers/endpoints/{endpoint_id}/discover")
async def admin_discover_provider_endpoint_models(
    request: Request,
    endpoint_id: str,
    *,
    dependencies: ProviderCatalogDependencies = Depends(
        get_provider_catalog_dependencies
    ),
) -> dict[str, Any]:
    """Return the model list served by ``GET {base_url}/models`` (admin).

    Discovery is read-only — admins author catalog rows via Admin → Models
    using the endpoint as the transport reference.

    For the subscription proxy the same probe is enriched server-side with
    per-credential attribution and the proxy's static model definitions, so the
    response can say *which connected account* serves each model, which are
    already registered, and which are advertised but unsupported (image/video
    generation) or need review. ``subscription: false`` marks the plain
    endpoint shape, which is unchanged.
    """
    await dependencies.require_admin(request)
    return await dependencies.service.discover_provider_endpoint_models(
        endpoint_id=endpoint_id
    )


@router.post("/api/admin/providers/endpoints/{endpoint_id}/models/import")
async def admin_import_provider_endpoint_models(
    request: Request,
    endpoint_id: str,
    body: SubscriptionModelImport,
    *,
    dependencies: ProviderCatalogDependencies = Depends(
        get_provider_catalog_dependencies
    ),
) -> dict[str, Any]:
    """Register discovered subscription models as catalog rows (admin).

    ``model_ids`` selects specific candidates; omitting it means "add all
    supported models". Idempotent in both directions: an already-registered
    model is skipped rather than re-inserted or rewritten, which is what
    preserves admin labels, capability choices, limits and enabled state
    across a rediscovery. Unsupported modalities are refused even when
    explicitly named.
    """
    await dependencies.require_admin(request)
    return await dependencies.service.import_provider_endpoint_models(
        endpoint_id=endpoint_id, body=body
    )


@router.get("/api/admin/providers/defaults")
async def admin_list_provider_defaults(
    request: Request,
    *,
    dependencies: ProviderCatalogDependencies = Depends(
        get_provider_catalog_dependencies
    ),
) -> dict[str, str | None]:
    """Return the currently-configured default model IDs for each workload kind."""
    await dependencies.require_admin(request)
    return await dependencies.service.list_provider_defaults()


@router.put("/api/admin/providers/defaults/{kind}")
async def admin_set_provider_default(
    request: Request,
    kind: str,
    body: AdminDefaultModelSet,
    *,
    dependencies: ProviderCatalogDependencies = Depends(
        get_provider_catalog_dependencies
    ),
) -> dict[str, str | None]:
    """Set or clear (empty string) the default model for a workload kind."""
    admin = await dependencies.require_admin(request)
    return await dependencies.service.set_provider_default(
        kind=kind, body=body, admin=admin
    )


@router.get("/api/admin/helm-managed")
async def admin_helm_managed(
    request: Request,
    *,
    dependencies: ProviderCatalogDependencies = Depends(
        get_provider_catalog_dependencies
    ),
) -> dict[str, Any]:
    """Which provider rows Helm reconciles on every upgrade, and which of
    them an admin has overridden since (``helm_drift``)."""
    await dependencies.require_admin(request)
    return await dependencies.service.helm_managed_overview()
