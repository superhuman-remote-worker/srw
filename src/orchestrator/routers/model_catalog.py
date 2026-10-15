"""HTTP adapters for the model catalog application owner."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, Request

from orchestrator.schemas.model_catalog import CatalogModelCreate, CatalogModelUpdate
from orchestrator.services.model_catalog import ModelCatalogService

router = APIRouter()


@dataclass(frozen=True)
class ModelCatalogDependencies:
    service: ModelCatalogService
    require_admin: Callable[[Request], Awaitable[dict[str, Any]]]
    require_approved_user: Callable[[Request], Awaitable[dict[str, Any]]]


def get_model_catalog_dependencies(request: Request) -> ModelCatalogDependencies:
    return request.app.state.model_catalog_dependencies_factory()


@router.get("/api/admin/providers/models")
async def admin_list_catalog_models(
    request: Request,
    capability: str | None = None,
    provider_kind: str | None = None,
    provider_ref: str | None = None,
    enabled_only: bool = False,
    *,
    dependencies: ModelCatalogDependencies = Depends(get_model_catalog_dependencies),
) -> list[dict[str, Any]]:
    """List catalog rows with optional filters.

    The ``capability`` query param narrows by membership — a row matches
    iff its ``capabilities[]`` contains the requested value. Returns full
    row shape.
    """
    await dependencies.require_admin(request)
    return await dependencies.service.list_catalog_models(
        capability=capability,
        provider_kind=provider_kind,
        provider_ref=provider_ref,
        enabled_only=enabled_only,
    )


@router.post("/api/admin/providers/models")
async def admin_create_catalog_model(
    request: Request,
    body: CatalogModelCreate,
    *,
    dependencies: ModelCatalogDependencies = Depends(get_model_catalog_dependencies),
) -> dict[str, Any]:
    """Insert a new catalog row.

    Validates that ``provider_ref`` resolves to an existing transport before
    insert. Returns the created row; raises 409 on
    ``(provider_kind, provider_ref, model_id, capability)`` collision.
    """
    await dependencies.require_admin(request)
    return await dependencies.service.create_catalog_model(body=body)


@router.patch("/api/admin/providers/models/{catalog_id}")
async def admin_update_catalog_model(
    request: Request,
    catalog_id: str,
    body: CatalogModelUpdate,
    *,
    dependencies: ModelCatalogDependencies = Depends(get_model_catalog_dependencies),
) -> dict[str, Any]:
    """Patch a catalog row. Only fields present in the body are written.

    Pass ``null`` to clear an optional column. The validator re-checks the
    transport when ``provider_kind`` or ``provider_ref`` changes.
    """
    await dependencies.require_admin(request)
    return await dependencies.service.update_catalog_model(
        catalog_id=catalog_id, body=body
    )


@router.delete("/api/admin/providers/models/{catalog_id}")
async def admin_delete_catalog_model(
    request: Request,
    catalog_id: str,
    *,
    dependencies: ModelCatalogDependencies = Depends(get_model_catalog_dependencies),
) -> dict[str, Any]:
    """Hard-delete a catalog row. Returns a warning when the row's model_id
    is currently referenced by a ``default_llm_models`` pointer (the pin
    becomes a dangling reference; the resolver's first-enabled-alphabetical
    fallback handles it gracefully).
    """
    await dependencies.require_admin(request)
    return await dependencies.service.delete_catalog_model(catalog_id=catalog_id)


@router.post("/api/admin/providers/models/{catalog_id}/test")
async def admin_test_catalog_model(
    request: Request,
    catalog_id: str,
    *,
    dependencies: ModelCatalogDependencies = Depends(get_model_catalog_dependencies),
) -> dict[str, Any]:
    """Probe a catalog row's transport.

    For ``provider_kind='endpoint'``, calls ``GET {endpoint.base_url}/models``
    via the existing endpoint-probe helper. For ``provider_kind='system'``,
    confirms the ``system_api_keys`` row exists and returns ``ok=True``
    without round-tripping the provider — vendor-specific health probes
    are out of scope for v1.
    """
    await dependencies.require_admin(request)
    return await dependencies.service.test_catalog_model(catalog_id=catalog_id)


@router.get("/api/admin/families")
async def admin_list_families(
    request: Request,
    *,
    dependencies: ModelCatalogDependencies = Depends(get_model_catalog_dependencies),
) -> dict[str, Any]:
    """Return the family keys defined in ``model_config_matrix.yaml`` plus each
    family's default context window.

    Powers the family dropdown on the *Admin → Models* form (so adding a family
    in the YAML doesn't require a frontend rebuild) and the context-window
    field's "family default" placeholder.
    """
    await dependencies.require_admin(request)
    return await dependencies.service.list_families()


@router.get("/api/admin/families/detect")
async def admin_detect_family(
    request: Request,
    model_id: str,
    *,
    dependencies: ModelCatalogDependencies = Depends(get_model_catalog_dependencies),
) -> dict[str, str]:
    """Suggest a family for ``model_id`` via the regex matcher.

    Pre-fills the family dropdown on the *Admin → Models* add form and the
    discovery confirmation dialog so admins don't have to memorize the
    mapping. ``source`` is ``"matched"`` for a regex hit and ``"fallback"``
    when no rule matched (the result is ``default`` — works, but quality is
    on the model). Admin can override before saving either way.
    """
    await dependencies.require_admin(request)
    return await dependencies.service.detect_family(model_id=model_id)


@router.get("/api/models")
async def list_available_models(
    request: Request,
    project_id: str | None = None,
    *,
    dependencies: ModelCatalogDependencies = Depends(get_model_catalog_dependencies),
) -> dict[str, Any]:
    """List all models from the admin-curated catalog.

    Returns catalog rows grouped by provider/capability:

    - ``groups`` (chat-capability rows, grouped by provider)
    - ``auxiliary_models`` / ``vision_models`` / ``embedding_models`` /
      ``whisper_models`` / ``tts_models`` / ``search_models`` /
      ``fetch_models`` / ``rerank_models`` / ``live_voice_models`` (one helper
      list per capability)

    Every row carries ``configured: true`` because the catalog only
    contains rows whose transport (system_api_keys row or system endpoint)
    is admin-managed. The legacy strategic+tactical preset bundle was
    removed in chunk 7 of the models_yaml_removal work — the job-create
    UX picks strategic and tactical models individually now.

    Query params:
        project_id: kept for backward compatibility — no longer affects the
            response shape now that the catalog is the source of truth.
    """
    await dependencies.require_approved_user(request)
    return await dependencies.service.list_available_models(project_id=project_id)


@router.post("/api/models/reload")
async def reload_model_catalog(
    request: Request,
    *,
    dependencies: ModelCatalogDependencies = Depends(get_model_catalog_dependencies),
) -> dict[str, str]:
    """No-op kept for backward compat with cockpit clients that still POST.

    Catalog rows live in the DB and ``/api/models`` queries them fresh on
    every call — there is no cache to invalidate. The YAML fallback
    registry that this endpoint used to bounce was deleted in chunk 6;
    the legacy YAML projection cache it then bounced was deleted in
    chunk 7.
    """
    await dependencies.require_admin(request)
    return await dependencies.service.reload_model_catalog()
