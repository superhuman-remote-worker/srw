"""HTTP adapters for job delivery review — outbox re-sync, PR status, review session.

All three routes are gated by ``require_job_access``, which enforces both user
visibility AND the MCP/officer ``project:<uuid>`` scope. The export and the
review session need the authenticated *user* as well as the row (the export
acts in a user's name; the review session is the caller's, and attaches only
the job's connectors a session of theirs may), so they keep both halves of the
gate's result; the PR status needs only the row.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, Request

from orchestrator.security.access import require_job_access
from orchestrator.services import job_export, job_review_session

router = APIRouter()


@dataclass(frozen=True)
class JobReviewDependencies:
    """Per-app auth store and the two operation bundles; no store ownership."""

    store: Any
    export: job_export.JobExportDependencies
    review_session: job_review_session.JobReviewSessionDependencies
    require_job_access: Callable[..., Awaitable[Any]] = require_job_access


def get_job_review_dependencies(request: Request) -> JobReviewDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.job_review_dependencies_factory()


@router.post("/api/jobs/{job_id}/export-to-shared-folder")
async def export_job_to_shared_folder(
    request: Request,
    job_id: str,
    *,
    dependencies: JobReviewDependencies = Depends(get_job_review_dependencies),
) -> dict[str, Any]:
    """Re-sync the job's outbox now: copy its deliverables into a shared
    cloud folder ("Open cloud folder") and return its browser URL.

    Valid for any ``completed``, ``pending_review`` or ``reviewing`` job,
    whether or not its project has a cloud folder: the outbox is the job's
    deliverables, while the project folder's copy has its own diff review.
    A job whose owner's outbox is attached is copied at completion already
    (the ``job_outbox_delivery`` effect); this repeats the copy for the
    calling user, overwriting the same folder, re-stamping ``exported_at``
    as "last synced at", and sharing a folder the completion copy could not
    share yet (``unshared``). Workspace-relative paths are kept **except**
    for the leading directories every deliverable shares, which are
    collapsed (``_common_dir_prefix``).

    See knowledge-history/done/job_cloud_export.md §3.2 and
    main_cloud_as_connectors.md, slice 6.2.
    """
    user, job = await dependencies.require_job_access(
        request, dependencies.store, job_id
    )
    return await job_export.export_job_to_shared_folder(
        job_id=job_id,
        user=user,
        authorized_job=job,
        dependencies=dependencies.export,
    )


@router.get("/api/jobs/{job_id}/pull-request")
async def get_job_pull_request_status(
    request: Request,
    job_id: str,
    *,
    dependencies: JobReviewDependencies = Depends(get_job_review_dependencies),
) -> dict[str, Any]:
    """Read the live state of the PR recorded by ``repo_open_pr``.

    The caller supplies only a job id. The server resolves both the persisted
    PR identity and its credential-bearing repository connector after the
    ordinary job-access check; connector credentials never cross REST.
    """
    _, job = await dependencies.require_job_access(request, dependencies.store, job_id)
    return await job_review_session.get_job_pull_request_status(
        job_id=job_id,
        authorized_job=job,
        dependencies=dependencies.review_session,
    )


@router.post("/api/jobs/{job_id}/review-session")
async def create_job_review_session(
    request: Request,
    job_id: str,
    *,
    dependencies: JobReviewDependencies = Depends(get_job_review_dependencies),
) -> dict[str, Any]:
    """Create a fresh interactive review from an access-checked job id.

    There is intentionally no request body. Model, scope, connectors and the
    delivered branch are all derived from stored server state; the MCP session
    tool remains unchanged and has no route to ``config_override``.
    """
    user, job = await dependencies.require_job_access(
        request, dependencies.store, job_id
    )
    return await job_review_session.create_job_review_session(
        request=request,
        job_id=job_id,
        user=user,
        authorized_job=job,
        dependencies=dependencies.review_session,
    )
