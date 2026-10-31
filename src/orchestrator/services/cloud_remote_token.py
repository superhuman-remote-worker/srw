"""A Virtual Session's cloud folder bearer, minted per call under the claim lease.

A remote entry of the ``oauth_token`` auth kind (``cloud_remote_plan``) carries
no credential: the executor fetches the folder's short-lived bearer here
(``GET /internal/units/{unit_id}/cloud-token``) and hands it to rclone per
spawn (``features/cloud_folders_on_virtual_sessions.md`` § Provider
neutrality; ruling 9: OpenCloud never gets the client secret in the
executor).

Auth is the claim bundle's two layers (``stateless_agents.md`` § 5.6): the
``X-Internal-Key`` transport guard, plus proof of a LIVE lease: ``(unit_id,
lease_token)`` must match ``state='leased'`` with the exact current token,
held by the claimant Pod. On top, the request names the plan it serves (the
recorded remote plan's fingerprint and incarnation, as the probe report
does), so a token is minted only for a folder this claim's delivery bound.
The folder is bound again on the ``virtual`` tier (eligibility, grants and
levels as at the delivery) and its provider mints the token; nothing is
persisted and the token is never logged.

Errors: 401 bad internal key; 403 lease mismatch or not leased (one generic
detail, no enumeration oracle); 404 unknown unit, or no such folder bound for
this session; 409 not a session unit, not the stateless lane, another plan, an
auth kind the route does not mint, or a provider that mints none; 503 the
provider could not mint now.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from fastapi import HTTPException

from orchestrator.services import agent_cloud_mounts, cloud_mount_status
from orchestrator.services.cloud import SupportsDataPlaneToken
from orchestrator.services.cloud.errors import CloudBackendError, CloudBackendErrorKind
from orchestrator.services.cloud_remote_plan import TOKEN_ROUTE_AUTH_KINDS
from orchestrator.services.stateless_workspace_gate import thread_metadata_object

logger = logging.getLogger(__name__)

_LEASE_FAILED = "Lease validation failed"
_NO_TOKEN = "No token is minted for this folder"


@dataclass(frozen=True)
class CloudRemoteTokenDependencies:
    """Collaborators for one token, resolved per request.

    ``cloud_mount_dependencies`` is the planner's dependency factory
    (``application.workspace.agent_cloud_mount_dependencies``): the token
    route binds the folder exactly as the delivery did.
    """

    db: Any
    #: The router's transport guard, resolved from the handling app.
    require_internal: Callable[..., Awaitable[Any]]
    cloud_mount_dependencies: Callable[
        [], agent_cloud_mounts.AgentCloudMountDependencies
    ]


async def mint_cloud_remote_token(
    unit_id: str,
    *,
    lease_token: int,
    pod_name: str,
    pod_uid: str,
    mount_id: str,
    fingerprint: str,
    incarnation: str,
    dependencies: CloudRemoteTokenDependencies,
) -> dict[str, Any]:
    """The bearer for the cloud folder ``mount_id`` of the session unit
    ``unit_id``, for the executor that holds its live lease and serves the
    plan ``(fingerprint, incarnation)``: ``{"mount_id", "token",
    "expires_in"}``. ``pod_uid`` names the claimant for the log only."""
    from shared.run_queue import LANE_STATELESS, UNIT_KIND_SESSION_TURN

    try:
        UUID(str(unit_id))
    except (ValueError, TypeError):
        raise HTTPException(status_code=404, detail="Unknown unit") from None

    async with dependencies.db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT queue.unit_kind, queue.state, queue.lease_token, "
            "queue.leased_by, thread.execution_lane AS thread_lane "
            "FROM run_queue AS queue LEFT JOIN threads AS thread "
            "ON thread.id = queue.unit_id WHERE queue.unit_id = $1::uuid",
            unit_id,
        )
    if row is None:
        raise HTTPException(status_code=404, detail="Unknown unit")
    leased_by = row["leased_by"]
    if (
        row["state"] != "leased"
        or int(row["lease_token"]) != int(lease_token)
        or (leased_by and pod_name and str(leased_by) != str(pod_name))
    ):
        # ONE generic detail: a stale token, another claimant and not-leased
        # are indistinguishable to the caller.
        raise HTTPException(status_code=403, detail=_LEASE_FAILED)
    if (
        row["unit_kind"] != UNIT_KIND_SESSION_TURN
        or row["thread_lane"] != LANE_STATELESS
    ):
        raise HTTPException(status_code=409, detail="Not a stateless session unit")

    thread = await dependencies.db.get_thread(unit_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Unknown unit")
    recorded = cloud_mount_status.recorded_remote_plan(thread_metadata_object(thread))
    if (
        recorded is None
        or recorded.get("fingerprint") != fingerprint
        or recorded.get("runtime_incarnation") != incarnation
    ):
        raise HTTPException(
            status_code=409, detail="The token is not for this thread's plan"
        )

    cloud = dependencies.cloud_mount_dependencies()
    rows = await dependencies.db.list_thread_mounts(unit_id)
    live = await agent_cloud_mounts._resolve_live_mount_set(
        thread,
        mount_rows=rows,
        runtime_is_vm=False,
        dependencies=cloud,
        workspace_backend="virtual",
    )
    built = next(
        (
            mount
            for mount in live.mounts
            if str(mount.get("mount_id") or "") == str(mount_id)
        ),
        None,
    )
    if built is None:
        raise HTTPException(status_code=404, detail="No such cloud folder")
    auth = built.get("auth") or {}
    if auth.get("type") not in TOKEN_ROUTE_AUTH_KINDS:
        raise HTTPException(status_code=409, detail=_NO_TOKEN)
    backend = cloud.cloud_router.for_thread_optional(thread)
    if (
        backend is None
        or not getattr(backend, "is_initialized", False)
        or not isinstance(backend, SupportsDataPlaneToken)
        or getattr(backend, "backend_id", None) != built.get("backend")
    ):
        raise HTTPException(status_code=409, detail=_NO_TOKEN)
    try:
        minted = await backend.mint_data_plane_token(auth=auth)
    except CloudBackendError as exc:
        if exc.kind in (
            CloudBackendErrorKind.NOT_SUPPORTED,
            CloudBackendErrorKind.INVALID_REQUEST,
        ):
            raise HTTPException(status_code=409, detail=_NO_TOKEN) from None
        logger.warning(
            "Thread %s: the token of its cloud folder %s was not minted (%s)",
            unit_id,
            mount_id,
            exc.kind.value,
        )
        raise HTTPException(
            status_code=503, detail="The cloud folder's token is not available now"
        ) from None
    except Exception:
        logger.warning(
            "Thread %s: the token of its cloud folder %s was not minted",
            unit_id,
            mount_id,
            exc_info=True,
        )
        raise HTTPException(
            status_code=503, detail="The cloud folder's token is not available now"
        ) from None
    # The token itself is never logged.
    logger.info(
        "Thread %s: cloud folder %s bearer minted for claim %s (pod %s, %.0fs)",
        unit_id,
        mount_id,
        incarnation,
        pod_uid,
        minted.expires_in,
    )
    return {
        "mount_id": str(mount_id),
        "token": minted.token,
        "expires_in": float(minted.expires_in),
    }


__all__ = ["CloudRemoteTokenDependencies", "mint_cloud_remote_token"]
