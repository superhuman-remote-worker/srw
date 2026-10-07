"""HTTP adapters for the connector (datasource) surface.

Route order is part of the contract: ``/api/datasources/catalog`` and
``/api/datasources/eligible`` are declared before
``/api/datasources/{datasource_id}`` so the literal segments win, and
``/api/projects/linkable-datasource-targets`` is declared here rather than on
the projects router for the same reason.

Every gate is called from the handler that needs it, never from the service.
Two shapes appear, because the original code has two:

* a **leading gate** — the common case, awaited first and its ``(user, row)``
  result handed to the operation;
* a **bound gate** — where the original fires the gate part-way through
  (create/update validate the body before authenticating and authorize each
  newly added project link mid-flow; ``test`` resolves the connector *inside*
  the try/except that turns an unexpected failure into a 500). The handler
  closes over ``request`` and passes the callable down, so the operation
  decides when it fires while the router still owns what it is.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, Query, Request

from orchestrator.schemas.datasources import (
    DatasourceCreate,
    DatasourceTestRequest,
    DatasourceUpdate,
    SSHKeyGenerateRequest,
    SSHKeyGenerateResponse,
)
from orchestrator.security.access import (
    require_datasource_access,
    require_datasource_owner,
    require_job_access,
    require_project_member,
    require_project_owner,
)
from orchestrator.security.auth import require_approved_user
from orchestrator.services import datasources

router = APIRouter()


@dataclass(frozen=True)
class DatasourcesDependencies:
    store: Any
    operations: datasources.DatasourceDependencies
    require_approved_user: Callable[..., Awaitable[Any]] = require_approved_user
    require_project_member: Callable[..., Awaitable[Any]] = require_project_member
    require_project_owner: Callable[..., Awaitable[Any]] = require_project_owner
    require_datasource_access: Callable[..., Awaitable[Any]] = require_datasource_access
    require_datasource_owner: Callable[..., Awaitable[Any]] = require_datasource_owner
    require_job_access: Callable[..., Awaitable[Any]] = require_job_access


def get_datasources_dependencies(request: Request) -> DatasourcesDependencies:
    return request.app.state.datasources_dependencies_factory()


# =============================================================================
# Datasource Endpoints
# =============================================================================


@router.post(
    "/api/datasources/ssh-keys/generate",
    response_model=SSHKeyGenerateResponse,
)
async def generate_datasource_ssh_key(
    request: Request,
    body: SSHKeyGenerateRequest | None = None,
    *,
    dependencies: DatasourcesDependencies = Depends(get_datasources_dependencies),
) -> SSHKeyGenerateResponse:
    """Generate a fresh ed25519 SSH keypair for the user to paste into the form."""
    await dependencies.require_approved_user(request, dependencies.store)
    return datasources.generate_datasource_ssh_key(body=body)


@router.get("/api/datasources")
async def list_datasources(
    request: Request,
    job_id: str | None = Query(
        default=None, description="Filter by job ID (use 'global' for global-only)"
    ),
    type: str | None = Query(
        default=None, description="Filter by type (postgresql, neo4j, mongodb)"
    ),
    limit: int = Query(default=100, ge=1, le=500),
    *,
    dependencies: DatasourcesDependencies = Depends(get_datasources_dependencies),
) -> list[dict[str, Any]]:
    """List connectors visible to the caller."""
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await datasources.list_datasources(
        user=user,
        job_id=job_id,
        ds_type=type,
        limit=limit,
        dependencies=dependencies.operations,
    )


@router.get("/api/datasources/catalog")
async def list_datasource_catalog(
    request: Request,
    q: str | None = Query(default=None, max_length=200),
    type: str | None = Query(default=None),
    project_id: str | None = Query(default=None),
    scope_mode: str | None = Query(default=None),
    auto_attach: bool | None = Query(default=None),
    visibility: str | None = Query(default=None),
    ownership: str | None = Query(default=None),
    availability: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=100),
    cursor: str | None = Query(default=None),
    *,
    dependencies: DatasourcesDependencies = Depends(get_datasources_dependencies),
) -> dict[str, Any]:
    """Cursor-paginated connector management catalog."""
    user = await dependencies.require_approved_user(request, dependencies.store)
    if project_id:
        await dependencies.require_project_member(
            request, dependencies.store, project_id
        )
    return await datasources.list_datasource_catalog(
        user=user,
        q=q,
        ds_type=type,
        project_id=project_id,
        scope_mode=scope_mode,
        auto_attach=auto_attach,
        visibility=visibility,
        ownership=ownership,
        availability=availability,
        limit=limit,
        cursor=cursor,
        dependencies=dependencies.operations,
    )


@router.get("/api/projects/linkable-datasource-targets")
async def list_linkable_datasource_targets(
    request: Request,
    datasource_id: str | None = Query(default=None),
    q: str | None = Query(default=None, max_length=200),
    limit: int = Query(default=50, ge=1, le=100),
    cursor: str | None = Query(default=None),
    *,
    dependencies: DatasourcesDependencies = Depends(get_datasources_dependencies),
) -> dict[str, Any]:
    """Projects addable to a connector policy, plus retained current links."""
    user = await dependencies.require_approved_user(request, dependencies.store)
    if datasource_id:
        await dependencies.require_datasource_owner(
            request, dependencies.store, datasource_id
        )
    return await datasources.list_linkable_datasource_targets(
        user=user,
        datasource_id=datasource_id,
        q=q,
        limit=limit,
        cursor=cursor,
        dependencies=dependencies.operations,
    )


@router.get("/api/datasources/eligible")
async def list_eligible_datasources(
    request: Request,
    project_id: list[str] | None = Query(
        default=None,
        description="Project(s) to include linked connectors for (repeatable)",
    ),
    *,
    dependencies: DatasourcesDependencies = Depends(get_datasources_dependencies),
) -> list[dict[str, Any]]:
    """Connectors the caller may pre-select for a job/session (the picker
    source of truth)."""
    user = await dependencies.require_approved_user(request, dependencies.store)

    async def member_of(pid: str) -> Any:
        return await dependencies.require_project_member(
            request, dependencies.store, pid
        )

    return await datasources.list_eligible_datasources(
        user=user,
        project_id=project_id,
        require_project_member=member_of,
        dependencies=dependencies.operations,
    )


@router.get("/api/datasources/{datasource_id}")
async def get_datasource(
    request: Request,
    datasource_id: str,
    *,
    dependencies: DatasourcesDependencies = Depends(get_datasources_dependencies),
) -> dict[str, Any]:
    """Get a single connector by ID. F3: gated + credentials redacted."""
    user, ds = await dependencies.require_datasource_access(
        request, dependencies.store, datasource_id
    )
    return await datasources.get_datasource(
        user=user,
        ds=ds,
        datasource_id=datasource_id,
        dependencies=dependencies.operations,
    )


@router.post("/api/datasources")
async def create_datasource(
    body: DatasourceCreate,
    request: Request,
    *,
    dependencies: DatasourcesDependencies = Depends(get_datasources_dependencies),
) -> dict[str, Any]:
    """Create a new connector owned by the current user.

    The body is validated before the caller is authenticated, and each
    selected project is owner-checked after — both gates are therefore bound
    here and fired by the operation.
    """

    async def approve_caller() -> dict[str, Any]:
        return await dependencies.require_approved_user(request, dependencies.store)

    async def owner_of(project_id: str) -> Any:
        return await dependencies.require_project_owner(
            request, dependencies.store, project_id, allow_archived=False
        )

    return await datasources.create_datasource(
        body=body,
        require_approved_user=approve_caller,
        require_project_owner=owner_of,
        dependencies=dependencies.operations,
    )


@router.put("/api/datasources/{datasource_id}")
async def update_datasource(
    request: Request,
    datasource_id: str,
    body: DatasourceUpdate,
    *,
    dependencies: DatasourcesDependencies = Depends(get_datasources_dependencies),
) -> dict[str, Any]:
    """Update a connector. F3: creator/admin only; credentials are preserved."""
    user, existing_ds = await dependencies.require_datasource_owner(
        request, dependencies.store, datasource_id
    )

    async def owner_of(project_id: str) -> Any:
        return await dependencies.require_project_owner(
            request, dependencies.store, project_id, allow_archived=False
        )

    return await datasources.update_datasource(
        request=request,
        datasource_id=datasource_id,
        body=body,
        user=user,
        existing_ds=existing_ds,
        require_project_owner=owner_of,
        dependencies=dependencies.operations,
    )


@router.delete("/api/datasources/{datasource_id}")
async def delete_datasource(
    request: Request,
    datasource_id: str,
    *,
    dependencies: DatasourcesDependencies = Depends(get_datasources_dependencies),
) -> dict[str, str]:
    """Delete a connector. F3: creator/admin only."""
    user, datasource = await dependencies.require_datasource_owner(
        request, dependencies.store, datasource_id
    )
    return await datasources.delete_datasource(
        user=user,
        datasource=datasource,
        datasource_id=datasource_id,
        dependencies=dependencies.operations,
    )


@router.get("/api/jobs/{job_id}/datasources")
async def get_job_datasources(
    request: Request,
    job_id: str,
    *,
    dependencies: DatasourcesDependencies = Depends(get_datasources_dependencies),
) -> list[dict[str, Any]]:
    """Get resolved connectors for a job."""
    await dependencies.require_job_access(request, dependencies.store, job_id)
    return await datasources.get_job_datasources(
        job_id=job_id, dependencies=dependencies.operations
    )


@router.get("/api/datasources/{datasource_id}/index-status")
async def get_datasource_index_status(
    request: Request,
    datasource_id: str,
    *,
    dependencies: DatasourcesDependencies = Depends(get_datasources_dependencies),
) -> dict[str, Any]:
    """Return credential-free indexing state for an OKF KB connector."""
    _, datasource = await dependencies.require_datasource_access(
        request, dependencies.store, datasource_id
    )
    return await datasources.get_datasource_index_status(
        datasource=datasource,
        datasource_id=datasource_id,
        dependencies=dependencies.operations,
    )


@router.post("/api/datasources/{datasource_id}/reindex")
async def reindex_datasource_knowledge(
    request: Request,
    datasource_id: str,
    full: bool = False,
    *,
    dependencies: DatasourcesDependencies = Depends(get_datasources_dependencies),
) -> dict[str, Any]:
    """Incrementally refresh an external OKF KB; owner/admin only."""
    _, datasource = await dependencies.require_datasource_owner(
        request, dependencies.store, datasource_id
    )
    return await datasources.reindex_datasource_knowledge(
        datasource=datasource, full=full, dependencies=dependencies.operations
    )


@router.post("/api/datasources/{datasource_id}/test")
async def test_datasource(
    request: Request,
    datasource_id: str,
    body: DatasourceTestRequest | None = None,
    *,
    dependencies: DatasourcesDependencies = Depends(get_datasources_dependencies),
) -> dict[str, Any]:
    """Test connectivity to a connector.

    The gate is bound rather than awaited here so it stays inside the
    operation's try/except: an unexpected (non-``HTTPException``) failure
    while resolving the connector must still surface as a 500 with its
    message, exactly as it did in ``main``. ``body`` carries the SSH endpoint
    a connector form is editing (see :class:`DatasourceTestRequest`).
    """

    async def owned_datasource() -> tuple[dict[str, Any], dict[str, Any]]:
        return await dependencies.require_datasource_owner(
            request, dependencies.store, datasource_id
        )

    return await datasources.test_datasource(
        resolve_datasource=owned_datasource,
        overrides=body.model_dump(exclude_unset=True) if body is not None else None,
        dependencies=dependencies.operations,
    )
