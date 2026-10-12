"""HTTP adapters for registered connector drivers (connector drivers D6).

``/api/connector-drivers`` lists, registers, reads, disables, enables and
deletes driver images someone registered in their Account, a Project or the
shared Catalog (``orchestrator.services.connector_driver_registrations``),
and imports an
MCP Registry ``server.json`` as a managed MCP driver
(``orchestrator.services.connector_driver_imports``). A connector of a
registered driver is created through the datasource API with type
``image_driver`` and the registration it runs.

Every route authenticates first; who may register where, and who may see a
registration, is the service's (the manifest scopes).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, ConfigDict, Field

from orchestrator.security.auth import require_approved_user
from orchestrator.services import (
    connector_driver_imports,
    connector_driver_registrations,
)
from orchestrator.services.connector_driver_registrations import (
    DriverTrustPolicy,
    ResolveImage,
    RunSpec,
)
from orchestrator.services.connector_drivers.matrix import (
    HostingStatus,
    registered_driver_entry,
)

router = APIRouter()


@dataclass(frozen=True)
class ConnectorDriversDependencies:
    store: Any
    #: Resolves an image reference in its registry (digest, labels).
    resolve_image: ResolveImage
    #: Runs an unlabelled image's ``spec`` in a driver pod; ``None`` where
    #: the installation runs none.
    run_spec: RunSpec | None
    trust: DriverTrustPolicy = DriverTrustPolicy()
    hosting: HostingStatus = HostingStatus()
    require_approved_user: Callable[..., Awaitable[Any]] = require_approved_user


def get_connector_drivers_dependencies(
    request: Request,
) -> ConnectorDriversDependencies:
    return request.app.state.connector_drivers_dependencies_factory()


class Scope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["Account", "Project", "Catalog"]
    name: str = Field(..., min_length=1, max_length=64)


class RegisterDriverBody(BaseModel):
    """Register a driver image. Its spec comes from the image itself."""

    model_config = ConfigDict(extra="forbid")

    image: str = Field(..., min_length=1, max_length=512)
    scope: Scope | None = Field(
        None, description="Where to register; your Account when omitted"
    )
    name: str | None = Field(
        None,
        max_length=128,
        description="The driver name you expect the image to declare",
    )
    title: str | None = Field(None, max_length=200)
    description: str | None = Field(None, max_length=2000)


class ImportServerJsonBody(BaseModel):
    """Import an MCP Registry ``server.json`` as a managed MCP driver."""

    model_config = ConfigDict(extra="forbid")

    server: dict[str, Any] = Field(..., description="The server.json document")
    scope: Scope | None = None
    package: int | None = Field(
        None,
        ge=0,
        description="Which entry of packages[] to import, when several are oci",
    )


def _scope(scope: Scope | None) -> dict[str, str] | None:
    return scope.model_dump() if scope is not None else None


@router.get("/api/connector-drivers")
async def list_connector_drivers(
    request: Request,
    *,
    dependencies: ConnectorDriversDependencies = Depends(
        get_connector_drivers_dependencies
    ),
) -> dict[str, Any]:
    """The registrations the caller can see: the shared Catalog, their
    Account and their Projects."""
    user = await dependencies.require_approved_user(request, dependencies.store)
    registrations = await connector_driver_registrations.list_visible_registrations(
        dependencies.store, user
    )
    return {
        "registrations": await connector_driver_registrations.management_views(
            dependencies.store, user, registrations, dependencies.trust
        )
    }


@router.post("/api/connector-drivers", status_code=201)
async def register_connector_driver(
    body: RegisterDriverBody,
    request: Request,
    *,
    dependencies: ConnectorDriversDependencies = Depends(
        get_connector_drivers_dependencies
    ),
) -> dict[str, Any]:
    """Register a driver image in an Account, a Project or the Catalog."""
    user = await dependencies.require_approved_user(request, dependencies.store)
    registration = await connector_driver_registrations.register_driver(
        dependencies.store,
        user,
        scope=_scope(body.scope),
        image_reference=body.image,
        policy=dependencies.trust,
        resolve_image=dependencies.resolve_image,
        run_spec=dependencies.run_spec,
        name=body.name,
        title=body.title,
        description=body.description,
        request=request,
    )
    return registration.view(dependencies.trust)


@router.post("/api/connector-drivers/import", status_code=201)
async def import_connector_driver(
    body: ImportServerJsonBody,
    request: Request,
    *,
    dependencies: ConnectorDriversDependencies = Depends(
        get_connector_drivers_dependencies
    ),
) -> dict[str, Any]:
    """Register the ``oci`` package of an MCP Registry ``server.json`` as a
    managed MCP driver (npm, pypi and mcpb packages are unsupported)."""
    user = await dependencies.require_approved_user(request, dependencies.store)
    registration = await connector_driver_imports.import_server_json(
        dependencies.store,
        user,
        server=body.server,
        scope=_scope(body.scope),
        package=body.package,
        policy=dependencies.trust,
        resolve_image=dependencies.resolve_image,
        request=request,
    )
    return registration.view(dependencies.trust)


@router.get("/api/connector-drivers/{registration_id}")
async def get_connector_driver(
    registration_id: str,
    request: Request,
    *,
    dependencies: ConnectorDriversDependencies = Depends(
        get_connector_drivers_dependencies
    ),
) -> dict[str, Any]:
    """One registration, with its spec as the capability matrix shows it."""
    user = await dependencies.require_approved_user(request, dependencies.store)
    registration = await connector_driver_registrations.get_visible_registration(
        dependencies.store, user, registration_id
    )
    (view,) = await connector_driver_registrations.management_views(
        dependencies.store, user, [registration], dependencies.trust
    )
    view["driver"] = registered_driver_entry(
        registration,
        trust=dependencies.trust.trust(registration.image_reference),
        hosting=dependencies.hosting,
    )
    return view


@router.delete("/api/connector-drivers/{registration_id}")
async def delete_connector_driver(
    registration_id: str,
    request: Request,
    *,
    dependencies: ConnectorDriversDependencies = Depends(
        get_connector_drivers_dependencies
    ),
) -> dict[str, str]:
    """Delete a registration whose bindings are all revoked, and which no
    connector uses or which is disabled."""
    user = await dependencies.require_approved_user(request, dependencies.store)
    await connector_driver_registrations.delete_registration(
        dependencies.store, user, registration_id, request=request
    )
    return {"status": "deleted"}


@router.post("/api/connector-drivers/{registration_id}/disable")
async def disable_connector_driver(
    registration_id: str,
    request: Request,
    *,
    dependencies: ConnectorDriversDependencies = Depends(
        get_connector_drivers_dependencies
    ),
) -> dict[str, Any]:
    """Disable a registration (the kill switch): it binds nothing new and
    every live binding of it is revoked. Its connectors stay, shown as
    "registration disabled"; it may then be deleted once its bindings are
    revoked."""
    user = await dependencies.require_approved_user(request, dependencies.store)
    registration = await connector_driver_registrations.set_registration_disabled(
        dependencies.store, user, registration_id, disabled=True, request=request
    )
    (view,) = await connector_driver_registrations.management_views(
        dependencies.store, user, [registration], dependencies.trust
    )
    return view


@router.post("/api/connector-drivers/{registration_id}/enable")
async def enable_connector_driver(
    registration_id: str,
    request: Request,
    *,
    dependencies: ConnectorDriversDependencies = Depends(
        get_connector_drivers_dependencies
    ),
) -> dict[str, Any]:
    """Enable a disabled registration again; its connectors bind afresh."""
    user = await dependencies.require_approved_user(request, dependencies.store)
    registration = await connector_driver_registrations.set_registration_disabled(
        dependencies.store, user, registration_id, disabled=False, request=request
    )
    (view,) = await connector_driver_registrations.management_views(
        dependencies.store, user, [registration], dependencies.trust
    )
    return view
