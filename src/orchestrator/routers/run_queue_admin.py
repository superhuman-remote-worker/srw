"""Operator routes over the run queue and completion commands.

Extracted from ``orchestrator.main`` (R1.B06, root lane), plus the claimant
attestation verb. The guard runs here and returns the acting administrator,
whose id the service records as the actor on every completion-command
disposition and claimant attestation — so the audit trail keeps naming a
person, not the process.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from orchestrator.schemas.run_queue_admin import (
    ClaimantGoneAttestationRequest,
    CompletionCommandForceResolveRequest,
    ExecutorPodGoneAttestationRequest,
)
from orchestrator.services import run_queue_admin

# No `tags=`: the declarations this replaces carried none, and a tag would
# change the published OpenAPI operations for routes whose identity this batch
# is required to leave untouched.
router = APIRouter()


def get_run_queue_admin_dependencies(
    request: Request,
) -> run_queue_admin.RunQueueAdminDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.run_queue_admin_dependencies_factory()


@router.get("/api/admin/run-queue")
async def admin_run_queue_read_model(request: Request) -> dict[str, Any]:
    """Operator read model for the stateless run_queue (admin only).

    ``src/shared/run_queue.list_active`` passthrough: current leases (with
    ``lease_remaining_seconds`` — negative means expired, awaiting the
    reaper) and parked units (the unpark worklist). Diagnostics only; never
    an input to correctness decisions.
    """
    dependencies = get_run_queue_admin_dependencies(request)
    await dependencies.require_admin(request)
    return await run_queue_admin.read_run_queue_model(dependencies=dependencies)


@router.post("/api/admin/run-queue/{unit_id}/unpark")
async def admin_run_queue_unpark(unit_id: str, request: Request) -> dict[str, Any]:
    """Operator verb: parked → queued, attempts reset, runnable now (admin
    only). The ONLY path out of 'parked' — neither enqueue nor input recording
    revives a parked unit (§5.1). 404 when the unit is not currently parked.
    """
    dependencies = get_run_queue_admin_dependencies(request)
    await dependencies.require_admin(request)
    return await run_queue_admin.unpark_run_queue_unit(
        unit_id, dependencies=dependencies
    )


@router.post("/api/admin/run-queue/{unit_id}/attest-claimant-gone")
async def admin_run_queue_attest_claimant_gone(
    unit_id: str,
    body: ClaimantGoneAttestationRequest,
    request: Request,
) -> dict[str, Any]:
    """Operator verb: settle a claim-loss hold whose exact claimant pod the
    administrator confirmed gone (admin only, audited receipt). 404 when no
    unresolved debt names that pod+UID; 409 while Kubernetes still shows it
    running or inside its termination grace.
    """
    dependencies = get_run_queue_admin_dependencies(request)
    admin = await dependencies.require_admin(request)
    return await run_queue_admin.attest_claimant_gone(
        unit_id,
        pod=body.pod,
        pod_uid=body.pod_uid,
        reason=body.reason,
        admin=admin,
        dependencies=dependencies,
    )


@router.post("/api/admin/run-queue/executor-pods/{pod_name}/attest-gone")
async def admin_run_queue_attest_executor_pod_gone(
    pod_name: str,
    body: ExecutorPodGoneAttestationRequest,
    request: Request,
) -> dict[str, Any]:
    """Operator verb: release a retained stateless executor Pod that no claim
    owes, on the administrator's audited assertion that its process is gone
    (admin only). 404 unless that exact UID is retained by the executor
    finalizer; 409 while it runs, is inside its grace, or a claim names it.
    """
    dependencies = get_run_queue_admin_dependencies(request)
    admin = await dependencies.require_admin(request)
    return await run_queue_admin.attest_executor_pod_gone(
        pod_name,
        pod_uid=body.pod_uid,
        reason=body.reason,
        admin=admin,
        dependencies=dependencies,
    )


@router.post("/api/admin/completion-commands/{command_id}/unpark")
async def admin_completion_command_unpark(
    command_id: str,
    request: Request,
) -> dict[str, Any]:
    """Rearm one exact parked completion command and its pending effects."""

    dependencies = get_run_queue_admin_dependencies(request)
    admin = await dependencies.require_admin(request)
    return await run_queue_admin.unpark_completion_command(
        command_id, admin=admin, dependencies=dependencies
    )


@router.post("/api/admin/completion-commands/{command_id}/force-resolve")
async def admin_completion_command_force_resolve(
    command_id: str,
    body: CompletionCommandForceResolveRequest,
    request: Request,
) -> dict[str, Any]:
    """Abandon a quiescent tail and write an operator-selected terminal state."""

    dependencies = get_run_queue_admin_dependencies(request)
    admin = await dependencies.require_admin(request)
    return await run_queue_admin.force_resolve_completion_command(
        command_id,
        expected_state=body.expected_state,
        terminal_status=body.terminal_status,
        reason=body.reason,
        admin=admin,
        dependencies=dependencies,
    )
