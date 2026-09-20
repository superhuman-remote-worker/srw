"""HTTP authorization and transport adapters for job, VM, and sudo controls."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
from typing import Any, Awaitable, Callable

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from orchestrator.schemas.job_controls import (
    JobApproveRequest,
    JobResumeRequest,
    SudoApproveRequest,
    SudoDenyRequest,
    SudoRuleCreateRequest,
    WorkspaceRecoveryRetryRequest,
)
from orchestrator.schemas.workspaces import VMCreateRequest
from orchestrator.services.job_controls import JobControlOperations
from shared.workspace_contract import WorkspaceContractError


router = APIRouter()


@dataclass(frozen=True, slots=True)
class JobControlRouteDependencies:
    """Per-application operation owner and exact authorization gates."""

    operations: JobControlOperations
    store: Any
    require_admin: Callable[..., Awaitable[Any]]
    require_job_access: Callable[..., Awaitable[Any]]
    require_internal_or_job_access: Callable[..., Awaitable[Any]]
    require_approved_user: Callable[..., Awaitable[Any]]
    require_sudo_request_authority: Callable[..., Awaitable[Any]]
    user_can_access_job_or_thread: Callable[..., Awaitable[bool]]
    mcp_scope_project_id: Callable[[dict[str, Any]], Any]


def get_job_control_dependencies(request: Request) -> JobControlRouteDependencies:
    """Resolve collaborators from the application handling this request."""

    return request.app.state.job_control_dependencies_factory()


@router.post("/api/vms")
async def create_vm(
    request: Request,
    body: VMCreateRequest,
    *,
    dependencies: JobControlRouteDependencies = Depends(get_job_control_dependencies),
) -> dict[str, Any]:
    """Create a VM for a job.

    **P4f** — gated by `require_job_access` on ``body.job_id``. VM
    provisioning is job-scoped, so callers must already be able to see
    the job. Admins (and project members) inherit access via the gate.

    Uses NATS (cross-cluster) or direct Kubernetes API (same-cluster).
    Returns 503 if no VM provisioning backend is available.
    """
    _caller, job = await dependencies.require_job_access(
        request,
        dependencies.store,
        body.job_id,
    )
    try:
        return await dependencies.operations.create_vm(job, body)
    except WorkspaceContractError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": exc.detail},
        ) from exc


@router.get("/api/vms")
async def list_vms(
    request: Request,
    *,
    dependencies: JobControlRouteDependencies = Depends(get_job_control_dependencies),
) -> list[dict[str, Any]]:
    """List jobs with active VMs. **Admin only** (P4d) — lists VMs across
    all users; the per-job VM detail/lifecycle endpoints stay job-scoped
    under P4f.

    Works from the database (no NATS required) — reads the 'vm' key from
    each job's context JSONB column.
    """
    await dependencies.require_admin(request)
    return await dependencies.operations.list_vms()


@router.get("/api/vms/{job_id}")
async def get_vm_status(
    request: Request,
    job_id: str,
    live: bool = Query(False, description="Query live status via NATS request/reply"),
    *,
    dependencies: JobControlRouteDependencies = Depends(get_job_control_dependencies),
) -> dict[str, Any]:
    """Get VM status for a job.

    **P4f** — gated by `require_job_access`. VM status leaks pod/IP/host
    details, so caller must already be able to see the job.

    By default reads from the database. With ?live=true, also queries the VM
    controller via NATS request/reply for real-time status.
    """
    _, job = await dependencies.require_job_access(
        request,
        dependencies.store,
        job_id,
    )
    return await dependencies.operations.get_vm_status(job_id, job, live=live)


@router.delete("/api/vms/{job_id}")
async def delete_vm(
    request: Request,
    job_id: str,
    *,
    dependencies: JobControlRouteDependencies = Depends(get_job_control_dependencies),
) -> dict[str, str]:
    """Delete a VM for a job.

    **P4f** — destructive. Caller must own the job OR be project-owner OR
    admin (mirrors `DELETE /api/jobs/{job_id}` from P4c). Plain project
    membership isn't enough.

    Uses NATS (cross-cluster) or direct Kubernetes API (same-cluster).
    Returns 503 if no VM provisioning backend is available.
    """
    caller, job = await dependencies.require_job_access(
        request,
        dependencies.store,
        job_id,
    )
    if not caller.get("is_admin"):
        is_job_owner = str(job.get("user_id") or "") == str(caller["id"])
        is_project_owner = False
        if not is_job_owner and job.get("project_id"):
            role = await dependencies.store.get_user_role_in_project(
                str(job["project_id"]),
                str(caller["id"]),
            )
            is_project_owner = role == "owner"
        if not (is_job_owner or is_project_owner):
            raise HTTPException(
                status_code=403,
                detail=(
                    "Only the job owner, the project owner, or an admin may "
                    "delete this VM"
                ),
            )
    return await dependencies.operations.delete_vm(job_id)


@router.get("/api/sudo/events")
async def sudo_sse_events(
    request: Request,
    *,
    dependencies: JobControlRouteDependencies = Depends(get_job_control_dependencies),
) -> StreamingResponse:
    """SSE stream of sudo approval events.

    Pushes events:
      - new_request: a new sudo request is pending
      - request_decided: a request was approved/denied/expired

    F6: per-user filtering. Admins see every event; non-admins see only
    events for jobs they can access or threads they own. Orphan events with
    neither ``job_id`` nor ``thread_id`` are admin-only. Filtering is applied per
    event inside the stream rather than at connect time so a member who
    later gains access doesn't have to reconnect.
    """
    user = await dependencies.require_approved_user(request, dependencies.store)
    queue = dependencies.operations.subscribe_sudo_events()

    async def event_stream():
        try:
            yield ": open\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event_type, data = await asyncio.wait_for(
                        queue.get(),
                        timeout=30.0,
                    )
                    entity_id = None
                    if isinstance(data, dict):
                        entity_id = data.get("thread_id") or data.get("job_id")
                    if not await dependencies.user_can_access_job_or_thread(
                        user,
                        dependencies.store,
                        entity_id,
                    ):
                        continue
                    yield (
                        f"event: {event_type}\n"
                        f"data: {json.dumps(data, default=str)}\n\n"
                    )
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            dependencies.operations.unsubscribe_sudo_events(queue)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/api/sudo/requests")
async def list_sudo_requests(
    request: Request,
    job_id: str | None = Query(None, description="Filter by job ID"),
    status: str | None = Query(None, description="Filter by status"),
    request_type: str | None = Query(
        None,
        description="Filter by type (sudo_command, vm_upgrade)",
    ),
    limit: int = Query(50, ge=1, le=200),
    *,
    dependencies: JobControlRouteDependencies = Depends(get_job_control_dependencies),
) -> list[dict]:
    """List sudo approval requests visible to the caller (G3).

    With ``?job_id=``: gate on ``require_job_access``. Without: admins
    see the full feed; non-admins receive only requests whose underlying
    job they can access (post-fetch filter).
    """
    if job_id:
        await dependencies.require_job_access(
            request,
            dependencies.store,
            job_id,
        )
        return await dependencies.operations.list_sudo_requests(
            job_id=job_id,
            status=status,
            request_type=request_type,
            limit=limit,
        )

    caller = await dependencies.require_approved_user(request, dependencies.store)
    rows = await dependencies.operations.list_sudo_requests(
        job_id=None,
        status=status,
        request_type=request_type,
        limit=limit,
    )
    if caller.get("is_admin") and dependencies.mcp_scope_project_id(caller) is None:
        return rows
    visible: list[dict] = []
    for row in rows:
        entity_id = row.get("thread_id") or row.get("job_id")
        if await dependencies.user_can_access_job_or_thread(
            caller,
            dependencies.store,
            entity_id,
        ):
            visible.append(row)
    return visible


@router.get("/api/sudo/requests/{request_id}")
async def get_sudo_request(
    request: Request,
    request_id: str,
    *,
    dependencies: JobControlRouteDependencies = Depends(get_job_control_dependencies),
) -> dict:
    """Get a single sudo approval request (G3: caller must access the underlying job)."""
    caller = await dependencies.require_approved_user(request, dependencies.store)
    result = await dependencies.operations.get_sudo_request(request_id)
    if not result:
        raise HTTPException(
            status_code=404,
            detail=f"Sudo request '{request_id}' not found",
        )
    entity_id = result.get("thread_id") or result.get("job_id")
    if not await dependencies.user_can_access_job_or_thread(
        caller,
        dependencies.store,
        entity_id,
    ):
        raise HTTPException(
            status_code=403,
            detail="Not authorized to access this sudo request",
        )
    return result


@router.post("/api/sudo/requests/{request_id}/approve")
async def approve_sudo_request(
    request_id: str,
    request: Request,
    body: SudoApproveRequest | None = None,
    *,
    dependencies: JobControlRouteDependencies = Depends(get_job_control_dependencies),
) -> dict:
    """Approve a pending sudo request. Caller must be project owner of the related job, or admin.

    For ``request_type='vm_upgrade'`` rows the approval also provisions a VM
    and re-dispatches the job — a bare row flip would leave ``freeze_data``
    set and park the job forever.
    """
    caller = await dependencies.require_approved_user(request, dependencies.store)
    row = await dependencies.require_sudo_request_authority(
        request,
        dependencies.store,
        request_id,
    )
    return await dependencies.operations.approve_sudo_request(
        request_id,
        row,
        caller=caller,
        reason=body.reason if body else "",
    )


@router.post("/api/sudo/requests/{request_id}/deny")
async def deny_sudo_request(
    request_id: str,
    body: SudoDenyRequest,
    request: Request,
    *,
    dependencies: JobControlRouteDependencies = Depends(get_job_control_dependencies),
) -> dict:
    """Deny a pending sudo request. Caller must be project owner of the related job, or admin.

    For ``request_type='vm_upgrade'`` rows the denial also re-dispatches the
    job on its original tier with a sticky, reasoned denial — previously no
    deny path acted on the job at all (row flip only → wedged forever).
    """
    caller = await dependencies.require_approved_user(request, dependencies.store)
    row = await dependencies.require_sudo_request_authority(
        request,
        dependencies.store,
        request_id,
    )
    return await dependencies.operations.deny_sudo_request(
        request_id,
        row,
        caller=caller,
        reason=body.reason,
    )


@router.post("/api/sudo/requests/{request_id}/approve-upgrade")
async def approve_sudo_vm_upgrade(
    request_id: str,
    request: Request,
    body: SudoApproveRequest | None = None,
    *,
    dependencies: JobControlRouteDependencies = Depends(get_job_control_dependencies),
) -> dict:
    """Approve a vm_upgrade sudo request — provisions a VM and resumes the job. Caller must be project owner of the related job, or admin."""
    caller = await dependencies.require_approved_user(request, dependencies.store)
    row = await dependencies.require_sudo_request_authority(
        request,
        dependencies.store,
        request_id,
    )
    return await dependencies.operations.approve_sudo_vm_upgrade(
        request_id,
        row,
        caller=caller,
        reason=body.reason if body else "",
    )


@router.post("/api/sudo/requests/{request_id}/resume-without-vm")
async def resume_sudo_without_vm(
    request_id: str,
    request: Request,
    body: SudoApproveRequest | None = None,
    *,
    dependencies: JobControlRouteDependencies = Depends(get_job_control_dependencies),
) -> dict:
    """Approve a vm_upgrade request but resume without provisioning a VM. Caller must be project owner of the related job, or admin.

    The job Continues-as-New on its original workspace tier with the sudo
    gate flipped to a reasoned block (previously this route called the
    pending_review-only job-approve path, which 400s on a paused job and
    never re-provisioned anything).
    """
    caller = await dependencies.require_approved_user(request, dependencies.store)
    row = await dependencies.require_sudo_request_authority(
        request,
        dependencies.store,
        request_id,
    )
    return await dependencies.operations.resume_sudo_without_vm(
        request_id,
        row,
        caller=caller,
        reason=body.reason if body else "",
    )


@router.get("/api/sudo/rules")
async def list_sudo_rules(
    request: Request,
    *,
    dependencies: JobControlRouteDependencies = Depends(get_job_control_dependencies),
) -> list[dict]:
    """List auto-approval rules. **Admin only** (P4d) — global pattern rules."""
    await dependencies.require_admin(request)
    return await dependencies.operations.list_sudo_rules()


@router.post("/api/sudo/rules")
async def create_sudo_rule(
    request: Request,
    body: SudoRuleCreateRequest,
    *,
    dependencies: JobControlRouteDependencies = Depends(get_job_control_dependencies),
) -> dict:
    """Create an auto-approval rule. **Admin only** (P4d) — global pattern rules."""
    await dependencies.require_admin(request)
    return await dependencies.operations.create_sudo_rule(body)


@router.delete("/api/sudo/rules/{rule_id}")
async def delete_sudo_rule(
    request: Request,
    rule_id: str,
    *,
    dependencies: JobControlRouteDependencies = Depends(get_job_control_dependencies),
) -> dict:
    """Delete an auto-approval rule. **Admin only** (P4d)."""
    await dependencies.require_admin(request)
    return await dependencies.operations.delete_sudo_rule(rule_id)


@router.post("/api/jobs/{job_id}/resume")
async def resume_job(
    req: Request,
    job_id: str,
    request: JobResumeRequest | None = None,
    *,
    dependencies: JobControlRouteDependencies = Depends(get_job_control_dependencies),
) -> dict[str, str]:
    """Resume a failed or paused job from its checkpoint. **Dual-callable**
    (P4b): cockpit user with job access OR agent with ``X-Internal-Key``
    (autoresume + ``resume_job_with_feedback`` tool). The Pydantic body keeps the
    historical ``request`` name; the FastAPI Request handle is ``req``.

    This endpoint:
    1. Validates the job exists and is not 'completed'
    2. Gets the assigned agent (or uses override agent_id from request)
    3. Validates the agent is ready or completed (not offline/working)
    4. Delegates delivery to ``_resume_job_on_agent`` — the dispatcher's
       resume path — so the job receives the full dispatch-time injection
       (credentials/env_keys, workspace config, queued feedback); falls back
       to queue-for-dispatch when the agent declines

    Returns:
        Status message indicating resume result
    """
    user, job = await dependencies.require_internal_or_job_access(
        req,
        dependencies.store,
        job_id,
    )
    return await dependencies.operations.resume_job(
        job_id,
        user=user,
        job=job,
        request=request,
        req=req,
    )


@router.post("/api/jobs/{job_id}/workspace-recovery/retry")
async def retry_workspace_recovery(
    req: Request,
    job_id: str,
    request: WorkspaceRecoveryRetryRequest,
    *,
    dependencies: JobControlRouteDependencies = Depends(get_job_control_dependencies),
) -> dict[str, Any]:
    """Retry a paused recovery after the ordinary job access gate."""

    user, _job = await dependencies.require_job_access(req, dependencies.store, job_id)
    return await dependencies.operations.retry_workspace_recovery(
        job_id,
        user=user,
        request=request,
    )


@router.post("/api/jobs/{job_id}/approve")
async def approve_job(
    req: Request,
    job_id: str,
    request: JobApproveRequest | None = None,
    *,
    dependencies: JobControlRouteDependencies = Depends(get_job_control_dependencies),
) -> dict[str, Any]:
    """Approve a frozen job, marking it as completed. **Dual-callable** (P4b):
    cockpit user with job access OR agent with ``X-Internal-Key`` (autonomous
    approve flow + ``approve_job`` tool). Body keeps the historical
    ``request`` name; FastAPI Request handle is ``req``.

    This endpoint mirrors the logic from agent.py:approve_frozen_job but runs
    entirely on the orchestrator side — no agent pod needs to be running.

    Steps:
    1. Validates job exists and is in 'pending_review' status
    2. Reads job_frozen.json from the Gitea repo
    3. Writes job_completion.json to the Gitea repo
    4. Removes job_frozen.json from the Gitea repo
    5. Updates DB status to 'completed' with completed_at timestamp
    """
    user, job = await dependencies.require_internal_or_job_access(
        req,
        dependencies.store,
        job_id,
    )
    return await dependencies.operations.approve_job(
        job_id,
        user=user,
        job=job,
        request=request,
    )


@router.post("/api/jobs/{job_id}/upgrade-to-vm")
async def upgrade_job_to_vm(
    request: Request,
    job_id: str,
    *,
    dependencies: JobControlRouteDependencies = Depends(get_job_control_dependencies),
) -> dict[str, Any]:
    """Upgrade a frozen job from container workspace to a VM.

    This endpoint is used when a job freezes with ``freeze_type: vm_upgrade_required``
    (i.e. the agent attempted a sudo command in a hardened container). See
    :func:`_upgrade_job_to_vm_internal` for the mechanics.
    """
    await dependencies.require_job_access(request, dependencies.store, job_id)
    return await dependencies.operations.upgrade_job_to_vm_internal(job_id)


__all__ = [
    "JobControlRouteDependencies",
    "approve_job",
    "approve_sudo_request",
    "approve_sudo_vm_upgrade",
    "create_sudo_rule",
    "create_vm",
    "delete_sudo_rule",
    "delete_vm",
    "deny_sudo_request",
    "get_job_control_dependencies",
    "get_sudo_request",
    "get_vm_status",
    "list_sudo_requests",
    "list_sudo_rules",
    "list_vms",
    "resume_job",
    "resume_sudo_without_vm",
    "router",
    "sudo_sse_events",
    "upgrade_job_to_vm",
]
