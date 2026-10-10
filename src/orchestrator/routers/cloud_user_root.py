"""HTTP adapters for a user's own cloud root (main cloud slice 4).

Every route acts on the caller's own root and nobody else's: there is no
user id in any path, and an administrator has no route to another user's
root. No response carries the credential.

* ``GET /api/me/cloud-root``: the root's state (the row is ensured first);
* ``POST /api/me/cloud-root/connect``: start "Connect your cloud" and
  answer the page the user opens (``login_url``); an earlier flow of the
  caller's is cancelled;
* ``DELETE /api/me/cloud-root/connect``: cancel the caller's flow;
* ``DELETE /api/me/cloud-root``: disconnect (revoke and clear).

Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md
(slice 4).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from orchestrator.security.auth import require_approved_user
from orchestrator.services import cloud_user_root
from orchestrator.services.cloud.errors import CloudBackendError

router = APIRouter(prefix="/api/me/cloud-root")


@dataclass(frozen=True)
class CloudUserRootDependencies:
    """The application's store, main cloud router and connect flows."""

    store: Any
    cloud_router: Any
    flows: cloud_user_root.CloudRootConnectFlows
    require_approved_user: Callable[[Request, Any], Awaitable[dict[str, Any]]] = (
        require_approved_user
    )


def get_cloud_user_root_dependencies(request: Request) -> CloudUserRootDependencies:
    return request.app.state.cloud_user_root_dependencies_factory()


async def _caller(request: Request, dependencies: CloudUserRootDependencies) -> dict:
    return await dependencies.require_approved_user(request, dependencies.store)


@router.get("")
async def get_cloud_root(
    request: Request,
    *,
    dependencies: CloudUserRootDependencies = Depends(get_cloud_user_root_dependencies),
) -> dict[str, Any]:
    """The caller's cloud root: whether their main cloud has one, and its
    state (``unsupported``, ``not_connected``, ``connecting``,
    ``connected``, ``revoked``, ``failed``)."""
    user = await _caller(request, dependencies)
    return await cloud_user_root.status(
        dependencies.store, dependencies.cloud_router, dependencies.flows, user
    )


@router.post("/connect")
async def connect_cloud_root(
    request: Request,
    *,
    dependencies: CloudUserRootDependencies = Depends(get_cloud_user_root_dependencies),
) -> dict[str, Any]:
    """Start "Connect your cloud" for the caller. The cockpit opens
    ``login_url`` in a new tab and reads ``GET`` until the state leaves
    ``connecting``. 409 when the caller has no root to connect (``detail``
    is the reason), 502 when the cloud could not start the flow."""
    user = await _caller(request, dependencies)
    try:
        flow = await dependencies.flows.start(
            dependencies.store, dependencies.cloud_router, user
        )
    except LookupError as exc:
        raise HTTPException(status_code=409, detail=str(exc.args[0])) from None
    except CloudBackendError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"The cloud could not start the connection ({exc.kind.value})",
        ) from None
    return {
        "state": "connecting",
        "connector_id": flow.connector_id,
        "login_url": flow.login_url,
        "expires_at": flow.expires_at.isoformat(),
    }


@router.delete("/connect")
async def cancel_cloud_root_connect(
    request: Request,
    *,
    dependencies: CloudUserRootDependencies = Depends(get_cloud_user_root_dependencies),
) -> dict[str, Any]:
    """Cancel the caller's connect flow (nothing is stored)."""
    user = await _caller(request, dependencies)
    cancelled = await cloud_user_root.cancel_connect(
        dependencies.store, dependencies.cloud_router, dependencies.flows, user
    )
    body = await cloud_user_root.status(
        dependencies.store, dependencies.cloud_router, dependencies.flows, user
    )
    return {**body, "cancelled": cancelled}


@router.delete("")
async def disconnect_cloud_root(
    request: Request,
    *,
    dependencies: CloudUserRootDependencies = Depends(get_cloud_user_root_dependencies),
) -> dict[str, Any]:
    """Disconnect the caller's root: the cloud is asked to revoke the
    credential (``provider_revoked``: true, false when it could not be
    asked, null when there was none) and SRW forgets it."""
    user = await _caller(request, dependencies)
    return await cloud_user_root.disconnect(
        dependencies.store, dependencies.cloud_router, dependencies.flows, user
    )
