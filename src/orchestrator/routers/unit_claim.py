"""The stateless executor's claim bundle.

Extracted from ``orchestrator.main`` (R1.B06, root lane). One internal route.
Transport lives here and policy lives in
``orchestrator.services.unit_claim_bundle``; the split matters because the
service's live-lease proof is what decides whether credentials may cross this
boundary at all, and that decision must be testable without a request.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from uuid import UUID

from orchestrator.services import unit_claim_bundle
from orchestrator.schemas.agent_runtime import WorkspaceRecoveryReport

# No `tags=`: the declaration this replaces carried none, and a tag would
# change the published OpenAPI operation for a route whose identity this batch
# is required to leave untouched.
router = APIRouter()


def get_unit_claim_bundle_dependencies(
    request: Request,
) -> unit_claim_bundle.UnitClaimBundleDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.unit_claim_bundle_dependencies_factory()


@router.get("/internal/units/{unit_id}/claim-bundle")
async def internal_unit_claim_bundle(
    unit_id: str,
    request: Request,
    lease_token: int,
    pod_name: str,
    pod_uid: str,
) -> dict[str, Any]:
    """Claim bundle for a leased stateless unit — internal (agent executor).

    The stateless turn executor calls this right after ``claim_unit`` to get
    everything a turn needs: the queue watermarks (skip-if-answered) and the
    full session-attach payload (config resolution, credentials in-flight,
    reauthorized datasources) — assembled by the SAME
    ``_assemble_session_attach_payload`` the pinned-lane sender uses, under
    the same fail-closed rules.

    Auth is two-layer (stateless_agents.md §5.6): the ``X-Internal-Key``
    transport guard every agent→orchestrator call carries, PLUS proof of a
    LIVE lease — (unit_id, lease_token) must match ``state='leased'`` with
    the exact current token, checked server-side in one SELECT that also
    reads the watermarks. Credentials therefore flow only to the executor
    that currently holds the unit; a zombie with a stale token gets the same
    generic 403 as a guess (no enumeration oracle).

    Errors: 401 bad internal key; 403 token mismatch / not leased (single
    generic detail); 404 unit row absent; 409 not a session unit, thread not
    on the stateless lane, or attach assembly refused (generic reason).
    """
    dependencies = get_unit_claim_bundle_dependencies(request)
    await dependencies.require_internal(request)
    try:
        return await unit_claim_bundle.claim_bundle_for_unit(
            unit_id,
            lease_token=lease_token,
            pod_name=pod_name,
            pod_uid=pod_uid,
            dependencies=dependencies,
        )
    except HTTPException as exc:
        if (
            exc.status_code == 409
            and isinstance(exc.detail, dict)
            and "recovery" in exc.detail
        ):
            return JSONResponse(status_code=409, content=exc.detail)
        raise


@router.post("/internal/units/{unit_id}/workspace-recovery")
async def internal_workspace_recovery(
    unit_id: UUID,
    body: WorkspaceRecoveryReport,
    request: Request,
) -> dict[str, Any]:
    dependencies = get_unit_claim_bundle_dependencies(request)
    await dependencies.require_internal(request)
    return (
        await unit_claim_bundle.report_workspace_recovery(
            unit_id=str(unit_id),
            report=body,
            dependencies=dependencies,
        )
    ).as_error_detail()


@router.get("/internal/units/{unit_id}/workspace-recovery-disposition")
async def internal_workspace_recovery_disposition(
    unit_id: UUID,
    request: Request,
    lease_token: int = Query(gt=0),
) -> dict[str, Any]:
    dependencies = get_unit_claim_bundle_dependencies(request)
    await dependencies.require_internal(request)
    receipt = await unit_claim_bundle.get_workspace_recovery_disposition(
        unit_id=str(unit_id),
        lease_token=lease_token,
        dependencies=dependencies,
    )
    if receipt is None:
        raise HTTPException(404, "No workspace recovery disposition")
    return receipt.as_error_detail()
