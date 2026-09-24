"""Job browsing HTTP adapters with app-owned authorization and read operations.

The application owns stores and lifecycle. Each request resolves its factory
from the app handling it; importing this router never imports application
startup. Main retains forwarding functions only for existing direct callers.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable

from fastapi import APIRouter, Depends, Query, Request

from orchestrator.schemas.job_list import JOB_LIST_RESPONSES
from orchestrator.security.access import require_job_access, require_project_member
from orchestrator.security.auth import require_approved_user
from orchestrator.services import job_queries, job_reads
from orchestrator.services.vm_idle_public import read_vm_idle_states

router = APIRouter()


@dataclass(frozen=True)
class JobReadsDependencies:
    """Per-app auth store and operations; no store startup or ownership here."""

    store: Any
    queries: job_queries.JobQueryDependencies
    reads: job_reads.JobReadDependencies
    require_approved_user: Callable[..., Awaitable[Any]] = require_approved_user
    require_job_access: Callable[..., Awaitable[Any]] = require_job_access
    require_project_member: Callable[..., Awaitable[Any]] = require_project_member


def get_job_reads_dependencies(request: Request) -> JobReadsDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.job_reads_dependencies_factory()


@router.get(
    "/api/jobs",
    operation_id="list_jobs_api_jobs_get",
    responses=JOB_LIST_RESPONSES,
)
async def list_jobs(
    request: Request,
    status: list[str] | None = Query(
        default=None,
        description="Lifecycle status(es) to keep (repeatable)",
    ),
    origin: list[str] | None = Query(
        default=None,
        description=(
            "Where the job came from (repeatable): user, session, automation, "
            "loop, officer, subjob, lifecycle, bench. No server-side default — "
            "omit it and every origin is returned."
        ),
    ),
    project_id: list[str] | None = Query(
        default=None,
        description=(
            "Project(s) to keep (repeatable). Pass 'none' for jobs with no project."
        ),
    ),
    has_project: bool | None = Query(
        default=None,
        description="true keeps only jobs with a project, false only those without",
    ),
    include_archived_projects: bool = Query(
        default=False,
        description="Include jobs belonging to archived projects",
    ),
    search: str | None = Query(
        default=None,
        max_length=200,
        description="Match against job description (substring) or id (prefix)",
    ),
    as_of: datetime | None = Query(
        default=None,
        description=(
            "Freeze the window at this creation-time watermark so paging is "
            "not shifted by concurrent inserts. Echoed in the response; pass "
            "it back on subsequent pages."
        ),
    ),
    user_id: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    include_total: bool = Query(
        default=True,
        description="Compute the capped total. Pass false when paging.",
    ),
    *,
    dependencies: JobReadsDependencies = Depends(get_job_reads_dependencies),
) -> dict[str, Any]:
    """List jobs visible to the caller.

    Visibility model (G1):
        * Admins see the full fleet, optionally narrowed by ``?user_id=`` or
          by an MCP ``project:<uuid>`` token scope.
        * Non-admins see jobs they own OR jobs in projects they're a member
          of, additionally narrowed by any MCP project scope.
        * A non-admin passing ``?user_id=`` for anyone other than themselves
          is rejected (403). Self-query (``?user_id=<self>``) is accepted and
          then ignored — AND-ing it onto the OR-clause would narrow the view
          to own-jobs-only and silently drop the caller's project rows.

    Returns an envelope, not a bare array::

        {"jobs": [...], "total": 806, "total_is_capped": false,
         "has_more": true, "limit": 100, "offset": 0, "as_of": "...",
         "filters": {...}}

    ``total`` is exact up to 10,000 and reported as capped beyond it; an
    exact filtered count cannot be made cheap, and bounding it is what keeps
    page 1 fast at a million rows. ``has_more`` is exact regardless.

    The ``filters`` echo exists for MCP callers: it makes a server-side
    default like ``include_archived_projects: false`` visible to a model that
    never read the docs, so an agent that finds nothing knows to widen its
    query rather than concluding the job does not exist.
    """
    user = await dependencies.require_approved_user(request, dependencies.store)
    result = await job_queries.list_jobs(
        user=user,
        dependencies=dependencies.queries,
        status=status,
        origin=origin,
        project_id=project_id,
        has_project=has_project,
        include_archived_projects=include_archived_projects,
        search=search,
        as_of=as_of,
        user_id=user_id,
        limit=limit,
        offset=offset,
        include_total=include_total,
    )
    states = await read_vm_idle_states(
        dependencies.store,
        owner_kind="job",
        owner_ids=[str(job["id"]) for job in result["jobs"]],
    )
    for job in result["jobs"]:
        job["workspace_lifecycle"] = states.get(str(job["id"]))
    return result


@router.get("/api/jobs/{job_id}")
async def get_job(
    request: Request,
    job_id: str,
    *,
    dependencies: JobReadsDependencies = Depends(get_job_reads_dependencies),
) -> dict[str, Any]:
    """Get a single job by ID."""
    _, job = await dependencies.require_job_access(request, dependencies.store, job_id)
    result = await job_reads.read_job(
        job_id=job_id, authorized_job=job, dependencies=dependencies.reads
    )
    if job.get("status") not in {"completed", "failed", "cancelled"}:
        states = await read_vm_idle_states(
            dependencies.store,
            owner_kind="job",
            owner_ids=[job_id],
        )
        result["workspace_lifecycle"] = states.get(job_id)
    else:
        result["workspace_lifecycle"] = None
    return result


@router.get("/api/stats/jobs")
async def get_job_statistics(
    request: Request,
    origin: list[str] | None = Query(
        default=None,
        description="Origin(s) to count within (repeatable).",
    ),
    project_id: list[str] | None = Query(
        default=None,
        description="Project(s) to count within (repeatable). 'none' for no project.",
    ),
    has_project: bool | None = Query(default=None),
    include_archived_projects: bool = Query(default=False),
    search: str | None = Query(default=None, max_length=200),
    as_of: datetime | None = Query(
        default=None,
        description="Count against the same watermark the list is paging.",
    ),
    *,
    dependencies: JobReadsDependencies = Depends(get_job_reads_dependencies),
) -> dict[str, Any]:
    """Per-status job counts scoped to the caller's visibility (G5).

    Admins see the full fleet (optionally narrowed by an MCP
    ``project:<uuid>`` scope). Non-admins see only jobs they own or are
    project members of.

    Takes the same filters as ``GET /api/jobs`` **except ``status``**. That
    is deliberate: these are disjunctive facet counts, so the status
    selection must not narrow them, or selecting one status drops every
    other chip to zero. Pass the rest of the list's filters — including
    ``as_of`` — and the chips will agree with the list's ``total``.

    Returns ``total_jobs`` (the "All" chip), one key per known status, and
    ``by_status`` for anything outside that vocabulary.
    """
    user = await dependencies.require_approved_user(request, dependencies.store)
    return await job_queries.get_job_statistics(
        user=user,
        dependencies=dependencies.queries,
        origin=origin,
        project_id=project_id,
        has_project=has_project,
        include_archived_projects=include_archived_projects,
        search=search,
        as_of=as_of,
    )


@router.get("/api/projects/{project_id}/jobs")
async def list_project_jobs(
    request: Request,
    project_id: str,
    status: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    *,
    dependencies: JobReadsDependencies = Depends(get_job_reads_dependencies),
) -> list[dict[str, Any]]:
    """List jobs belonging to a project."""
    await dependencies.require_project_member(request, dependencies.store, project_id)
    return await job_reads.read_project_jobs(
        project_id=project_id,
        status=status,
        limit=limit,
        dependencies=dependencies.reads,
    )
