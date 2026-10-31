"""HTTP adapters for job inspection with per-app dependencies."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, Query, Request

from orchestrator.security.access import (
    require_job_access,
    require_thread_owner,
    require_internal,
)
from orchestrator.security.auth import require_approved_user
from orchestrator.services import job_inspection

router = APIRouter()


@dataclass(frozen=True)
class JobInspectionDependencies:
    store: Any
    inspections: job_inspection.JobInspectionDependencies
    require_approved_user: Callable[..., Awaitable[Any]] = require_approved_user
    require_job_access: Callable[..., Awaitable[Any]] = require_job_access
    require_thread_owner: Callable[..., Awaitable[Any]] = require_thread_owner
    require_internal: Callable[..., Awaitable[Any]] = require_internal


def get_job_inspection_dependencies(request: Request) -> JobInspectionDependencies:
    return request.app.state.job_inspection_dependencies_factory()


@router.get("/api/jobs/{job_id}/progress")
async def get_job_progress(
    request: Request,
    job_id: str,
    *,
    dependencies: JobInspectionDependencies = Depends(get_job_inspection_dependencies),
) -> dict[str, Any]:
    """Get honest job liveness (E1/E3, officer_supervision_surface §4–§5).

    Composes the control-row basis with the one shared liveness computation
    (audit movement → agent heartbeat, ``updated_at`` never consulted).
    ``progress_percent``/``eta_seconds`` are kept in the payload for shape
    compatibility but are honest ``null`` — no percentage telemetry exists
    and none is fabricated.
    """
    _, job = await dependencies.require_job_access(request, dependencies.store, job_id)
    return await job_inspection.get_job_progress(
        job_id=job_id, authorized_job=job, dependencies=dependencies.inspections
    )


@router.get("/api/jobs/{job_id}/subjobs")
async def get_job_subjobs(
    request: Request,
    job_id: str,
    *,
    dependencies: JobInspectionDependencies = Depends(get_job_inspection_dependencies),
) -> dict[str, Any]:
    """The subjob roster for one job — what it spawned, and where each stands.

    Exists because a parent's own status is not self-explanatory. ``waiting``
    means *blocked on a child* (``_spawn_scholar_job`` holds the parent there
    while the scholar runs), so a reader looking at a ``waiting`` row is looking
    at the one status that cannot be understood without its children — and the
    jobs list is exactly where they are missing. Paging is over display roots
    and children ride along only if they *also* match the filter, so under the
    default ``origin IN ('user','session')`` a subjob never does. The row shows
    a parked-looking parent and no children whatsoever.

    So this deliberately does **not** reuse the list's query. It walks the tree
    (``get_job_subjob_roster``), which makes the answer independent of the
    caller's filters: the roster of a job is a property of the job, not of the
    view someone is looking at it through.

    Authorization is the parent's. A subjob inherits its parent's project and
    owner at creation, so seeing the parent is seeing the family, and every
    field returned is one ``GET /api/jobs`` already publishes for a child that
    the filter happened to let through.

    ``count`` is the honest size of the tree — the number the list's own
    ``childCount`` cannot give, because that one counts what survived filtering.
    """
    await dependencies.require_job_access(request, dependencies.store, job_id)
    return await job_inspection.get_job_subjobs(
        job_id=job_id, dependencies=dependencies.inspections
    )


@router.get("/api/jobs/{job_id}/subagents")
async def get_job_subagents(
    request: Request,
    job_id: str,
    *,
    dependencies: JobInspectionDependencies = Depends(get_job_inspection_dependencies),
) -> dict[str, Any]:
    """The subagent roster of one job — every child it delegated to, and
    where each stands (U3 B.10).

    The sibling of ``GET /api/jobs/{job_id}/subjobs`` for the in-process
    children: a ``delegate_agent`` call runs a child session inside the
    parent's pod, and the only durable trace is its ``threads`` row of
    ``kind='subagent'`` (0206) plus the transcript in ``thread_messages``.
    Those rows are deliberately kept off the sessions page (``list_threads``
    filters on kind), so this is the one place a reader sees them — with
    ``thread_id`` linking to ``/sessions/<thread_id>``, where the ordinary
    thread endpoints render the transcript read-only.

    Like the subjob roster it takes no filter parameters: the children of a
    job are a property of the job, not of the view someone reads it through,
    and terminal children are the ones that explain the parent's diff.

    Authorization is the parent's (``require_job_access``): a child row
    inherits the job's owner and project at creation, so seeing the job is
    seeing its children — the same rule that lets the job owner open the
    child's transcript through ``require_thread_owner``.
    """
    await dependencies.require_job_access(request, dependencies.store, job_id)
    return await job_inspection.get_job_subagents(
        job_id=job_id, dependencies=dependencies.inspections
    )


@router.get("/api/persistent/threads/{thread_id}/subagents")
async def get_session_subagents(
    request: Request,
    thread_id: str,
    *,
    dependencies: JobInspectionDependencies = Depends(get_job_inspection_dependencies),
) -> dict[str, Any]:
    """Owner-visible roster of children delegated by one session root."""
    _user, parent = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )
    return await job_inspection.get_session_subagents(
        thread_id=thread_id,
        authorized_parent=parent,
        dependencies=dependencies.inspections,
    )


@router.get("/api/jobs/{job_id}/brief")
async def get_job_brief(
    request: Request,
    job_id: str,
    *,
    dependencies: JobInspectionDependencies = Depends(get_job_inspection_dependencies),
) -> dict[str, Any]:
    """Brief fields for the agent's virtual ``task_brief.md``. **Internal** —
    requires ``X-Internal-Key``.

    ``JobResumeRequest`` carries no description/deliverables/kickoff, so a
    resumed job would serve an empty brief for the rest of its life; the agent
    backfills from here on resume
    (knowledge-base/knowledge/issues/fresh_job_dispatched_as_resume_skips_seeding.md).
    """
    await dependencies.require_internal(request)
    return await job_inspection.get_job_brief(
        job_id=job_id, dependencies=dependencies.inspections
    )


@router.get("/api/me/active-jobs")
async def list_my_active_jobs(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
    *,
    dependencies: JobInspectionDependencies = Depends(get_job_inspection_dependencies),
) -> list[dict[str, Any]]:
    """Caller's in-flight jobs — the non-admin replacement for `/api/admin/agents`.

    Returns jobs visible to the caller (G1 visibility OR — own jobs OR
    project-member jobs) in any of the active statuses (created,
    processing, paused, pending_review). The underlying ``query_jobs``
    SELECT already excludes pod IPs and hostnames, so this is safe to
    expose to non-admins. Admins still get the full fleet via
    `/api/admin/agents`; they can use this endpoint too if they want a personal
    in-flight summary.

    Respects MCP ``project:<uuid>`` scope narrowing.
    """
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await job_inspection.list_my_active_jobs(
        limit=limit, user=user, dependencies=dependencies.inspections
    )
