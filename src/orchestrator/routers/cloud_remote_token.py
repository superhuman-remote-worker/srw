"""The executor's bearer for one of a Virtual Session's cloud folders.

One internal route. Transport lives here and policy in
``orchestrator.services.cloud_remote_token``: the service's live-lease proof
and plan check decide whether a token may cross this boundary at all, and that
decision is testable without a request.
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from orchestrator.services import cloud_remote_token

router = APIRouter()


def get_cloud_remote_token_dependencies(
    request: Request,
) -> cloud_remote_token.CloudRemoteTokenDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.cloud_remote_token_dependencies_factory()


@router.get("/internal/units/{unit_id}/cloud-token")
async def internal_unit_cloud_token(
    unit_id: str,
    request: Request,
    lease_token: int,
    pod_name: str,
    pod_uid: str,
    mount_id: str,
    fingerprint: str,
    incarnation: str,
) -> JSONResponse:
    """A short-lived bearer for the cloud folder ``mount_id`` of a leased
    stateless session unit — internal (agent executor).

    Authenticated like the claim bundle: the ``X-Internal-Key`` transport
    guard plus proof of the LIVE lease ``(unit_id, lease_token)``, and the
    plan the executor serves (``fingerprint``, ``incarnation``) must be the
    thread's recorded one. The body is ``{"mount_id", "token",
    "expires_in"}``, never cached; errors in
    ``services/cloud_remote_token``.
    """
    dependencies = get_cloud_remote_token_dependencies(request)
    await dependencies.require_internal(request)
    body = await cloud_remote_token.mint_cloud_remote_token(
        unit_id,
        lease_token=lease_token,
        pod_name=pod_name,
        pod_uid=pod_uid,
        mount_id=mount_id,
        fingerprint=fingerprint,
        incarnation=incarnation,
        dependencies=dependencies,
    )
    return JSONResponse(content=body, headers={"Cache-Control": "no-store"})
