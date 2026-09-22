"""Thin job-admission and subjob-output HTTP composition adapters."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from fastapi import APIRouter, Depends, HTTPException, Request

from orchestrator.schemas.job_create import PublicJobCreateBody
from orchestrator.services.job_admission import JobAdmissionActor
from orchestrator.services.job_mutation_controls import JobControlOperations

router = APIRouter()


@dataclass(frozen=True, slots=True)
class JobLifecycleRouteDependencies:
    store: Any
    logger: logging.Logger
    is_internal_call: Callable[[Request], bool]
    require_approved_user: Callable[..., Awaitable[dict[str, Any]]]
    require_project_member: Callable[..., Awaitable[Any]]
    require_internal: Callable[[Request], Awaitable[Any]]
    strip_raw_officer_claim_context: Callable[[Any], None]
    strip_public_job_reserved_markers: Callable[[Any], None]
    admit_job: Callable[..., Awaitable[dict[str, Any]]]
    job_admission_dependencies: Callable[[Request], Any]
    graft_subjob_output: Callable[[str], Awaitable[dict[str, Any] | None]]


@dataclass(frozen=True, slots=True)
class JobControlRouteDependencies:
    operations: JobControlOperations
    store: Any
    require_job_access: Callable[..., Awaitable[tuple[dict[str, Any], dict[str, Any]]]]
    require_internal_or_job_access: Callable[
        ..., Awaitable[tuple[dict[str, Any], dict[str, Any]]]
    ]
    require_internal: Callable[[Request], Awaitable[Any]]


def get_job_lifecycle_route_dependencies(
    request: Request,
) -> JobLifecycleRouteDependencies:
    return request.app.state.job_lifecycle_route_dependencies_factory()


def get_job_control_route_dependencies(
    request: Request,
) -> JobControlRouteDependencies:
    return request.app.state.job_control_route_dependencies_factory()


@router.post("/api/jobs", operation_id="create_job_api_jobs_post")
async def create_job(
    request: Request,
    job: PublicJobCreateBody,
    *,
    dependencies: JobLifecycleRouteDependencies = Depends(
        get_job_lifecycle_route_dependencies
    ),
) -> dict[str, Any]:
    """Create a new job. **Dual-callable** (P4b):

    * Cockpit / user path → ``require_approved_user``; if ``body.project_id``
      is set, the caller must be at least an editor of that project; the
      submitted ``user_id`` is forced to ``caller.id`` so a malicious body
      can't create jobs attributed to someone else.
    * Agent path (delegation/session child jobs) → transport authentication via
      ``X-Internal-Key`` plus server-side user/project derivation from
      ``parent_job_id`` or ``thread_id``. Body identity/scope is never trusted.
      Originless internal HTTP calls are rejected; userless system children
      must still derive scope from an authoritative parent/thread.

    Creates a job with status 'created'. The automatic dispatcher provisions
    its workspace and assigns a ready agent; callers should monitor the job
    rather than manually assigning it.

    If ``project_id`` is set (directly or via the user's default project),
    the project's config and resources are inherited, while the root job still
    receives its own isolated repository. Subjobs branch within that root repo.
    """
    return await admit_job_request(request, job, dependencies=dependencies)


async def admit_job_request(
    request: Request,
    job: PublicJobCreateBody,
    *,
    dependencies: JobLifecycleRouteDependencies,
) -> dict[str, Any]:
    """Run the shared REST admission adapter for either mounted create route."""

    internal_call = dependencies.is_internal_call(request)
    dependencies.strip_raw_officer_claim_context(job)
    caller: dict[str, Any] | None = None
    if not internal_call:
        caller = await dependencies.require_approved_user(request, dependencies.store)
        job.user_id = str(caller["id"])
        dependencies.strip_public_job_reserved_markers(job)
        if job.project_id:
            await dependencies.require_project_member(
                request,
                dependencies.store,
                str(job.project_id),
                min_role="editor",
            )
    return await dependencies.admit_job(
        command=job,
        actor=JobAdmissionActor(
            principal=caller,
            forwarded_user_id=request.headers.get("X-MCP-User-Id"),
        ),
        origin="internal_rest" if internal_call else "user_rest",
        dependencies=dependencies.job_admission_dependencies(request),
    )


@router.delete("/api/jobs/{job_id}")
async def delete_job(
    request: Request,
    job_id: str,
    *,
    dependencies: JobControlRouteDependencies = Depends(
        get_job_control_route_dependencies
    ),
) -> dict[str, Any]:
    """Delete a job and its requirements.

    P4c: destructive. Caller must own the job OR be project-owner OR admin.
    Plain project membership is not enough — mirrors G3 sudo-authority gate.
    """
    caller, job = await dependencies.require_job_access(
        request, dependencies.store, job_id
    )
    return await dependencies.operations.delete(job_id, caller=caller, job=job)


@router.post("/api/jobs/{job_id}/subjob-merge")
async def subjob_merge(
    request: Request,
    job_id: str,
    *,
    dependencies: JobLifecycleRouteDependencies = Depends(
        get_job_lifecycle_route_dependencies
    ),
) -> dict[str, Any]:
    """Graft a completed subjob's output/ onto its parent's branch.
    **Internal** (P4b) — requires ``X-Internal-Key``. Ingress strips this
    path.

    Called by the agent after a subjob completes (autonomy=full auto-completion).
    Grafts the subjob's output/ onto the parent as a namespaced outputs/ folder.
    """
    await dependencies.require_internal(request)
    try:
        job = await dependencies.store.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        if not job.get("parent_job_id"):
            raise HTTPException(
                status_code=400,
                detail="Only subjobs (with parent_job_id) can be grafted",
            )
        result = await dependencies.graft_subjob_output(job_id)
        if result is None:
            return {"status": "skipped", "reason": "no branch/repo configured"}
        return {"job_id": job_id, **result}
    except HTTPException:
        raise
    except Exception as exc:
        dependencies.logger.error(
            "Squash merge failed for subjob %s: %s", job_id, exc, exc_info=True
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.put("/api/jobs/{job_id}/cancel")
async def cancel_job(
    request: Request,
    job_id: str,
    *,
    dependencies: JobControlRouteDependencies = Depends(
        get_job_control_route_dependencies
    ),
) -> dict[str, str]:
    """Cancel a running job. **Dual-callable** (P4b): cockpit user with job
    access (``require_job_access``) OR agent with valid ``X-Internal-Key``
    (agent's `cancel_job` tool path).

    If the job is assigned to an agent, this will also send a cancel request
    to the agent pod.
    """
    _, job = await dependencies.require_internal_or_job_access(
        request, dependencies.store, job_id
    )
    return await dependencies.operations.cancel(job_id, job=job)


@router.put("/api/jobs/{job_id}/pause")
async def pause_job(
    request: Request,
    job_id: str,
    *,
    dependencies: JobControlRouteDependencies = Depends(
        get_job_control_route_dependencies
    ),
) -> dict[str, str]:
    """Pause a running job. **Dual-callable** (P4b): cockpit user with
    job access OR agent with ``X-Internal-Key`` (`pause_job` tool).

    If the job is assigned to an agent, sends a graceful pause request
    to the agent pod. The agent finishes its current graph node, saves
    the checkpoint, and becomes available for new work.

    The paused job stays paused on either lane: it carries a durable operator
    pause hold that neither the dispatcher, the stateless admission and worker
    claim, nor an internal resume will cross, so it runs again only after an
    explicit ``POST /api/jobs/{job_id}/resume`` (or an admin assignment).
    """
    user, job = await dependencies.require_internal_or_job_access(
        request, dependencies.store, job_id
    )
    return await dependencies.operations.pause(
        job_id,
        job=job,
        paused_by=str(user["id"]) if user and user.get("id") else None,
    )


@router.put("/api/jobs/{job_id}/agent-release")
async def agent_release_job(
    request: Request,
    job_id: str,
    agent_id: str | None = None,
    lease_token: int | None = None,
    *,
    dependencies: JobControlRouteDependencies = Depends(
        get_job_control_route_dependencies
    ),
) -> dict[str, str]:
    """Agent-initiated job release (no agent callback). **Internal** (P4b)
    — requires ``X-Internal-Key``. Ingress strips this path.

    Called by an agent that is shutting down or otherwise releasing a job
    it was working on. Unlike the regular pause endpoint, this does NOT try to
    contact the agent pod (since the caller *is* the agent). A leased pinned
    job is handed to the authoritative expiry sweep, which advances durable
    redispatch accounting; only a genuine pre-lease NULL-lease row is paused
    here directly. Stateless work retains its queue-token release contract.
    """
    await dependencies.require_internal(request)
    return await dependencies.operations.release(
        job_id, agent_id=agent_id, lease_token=lease_token
    )
