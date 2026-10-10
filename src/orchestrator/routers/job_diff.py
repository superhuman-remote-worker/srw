"""HTTP adapters for the Mode A job diff review surface.

Route order is part of the contract: ``/api/jobs/{job_id}/diff`` is declared
before ``/api/jobs/{job_id}/diff/{file_path:path}`` so the summary keeps
winning over the greedy per-file path.

Every route is gated by ``require_job_access``, which enforces both user
visibility AND the MCP/officer ``project:<uuid>`` scope. The gate returns the
row and the operation works on *that* row — the accept/reject paths mutate it
in place before running the terminal side effects, so re-reading the job here
would break the effect chain. Accept and reject also take the caller: seeing
a job is not deciding its write-back (``job_diff_review``).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, Request

from orchestrator.security.access import require_job_access
from orchestrator.services import job_diff_review

router = APIRouter()


@dataclass(frozen=True)
class JobDiffDependencies:
    """Per-app auth store and diff-review operations; no store ownership here."""

    store: Any
    diff_review: job_diff_review.JobDiffReviewDependencies
    require_job_access: Callable[..., Awaitable[Any]] = require_job_access


def get_job_diff_dependencies(request: Request) -> JobDiffDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.job_diff_dependencies_factory()


@router.get("/api/jobs/{job_id}/diff")
async def get_job_diff(
    request: Request,
    job_id: str,
    *,
    dependencies: JobDiffDependencies = Depends(get_job_diff_dependencies),
) -> dict[str, Any]:
    """Mode A diff summary for a project-attached job.

    Returns ``{baseline_commit, head_commit, files: [{path, status}]}``
    where each ``status`` is ``added`` / ``modified`` / ``deleted``.
    Per-file diff content is served separately via the sibling
    ``/diff/{path}`` endpoint.

    Returns 404 when the job has no baseline (loose job, or a pre-Mode-A
    project job). Empty ``files`` list means no changes under
    ``projects/<slug>/`` — the agent didn't touch the mounted folder.

    See knowledge-history/done/job_cloud_export.md §5.
    """
    _, job = await dependencies.require_job_access(request, dependencies.store, job_id)
    return await job_diff_review.get_job_diff(
        job_id=job_id, authorized_job=job, dependencies=dependencies.diff_review
    )


@router.get("/api/jobs/{job_id}/diff/{file_path:path}")
async def get_job_diff_file(
    request: Request,
    job_id: str,
    file_path: str,
    *,
    dependencies: JobDiffDependencies = Depends(get_job_diff_dependencies),
) -> dict[str, Any]:
    """Mode A per-file diff content.

    Returns ``{path, status, old_content, new_content}`` for one file in
    the diff. ``old_content`` is read from the baseline commit;
    ``new_content`` from the head of the job's branch. Either side can
    be ``None`` (added → no old, deleted → no new).

    Only files under ``projects/`` are accepted — the Mode A diff is
    scoped to the project-folder mount.
    """
    _, job = await dependencies.require_job_access(request, dependencies.store, job_id)
    return await job_diff_review.get_job_diff_file(
        job_id=job_id,
        file_path=file_path,
        authorized_job=job,
        dependencies=dependencies.diff_review,
    )


@router.post("/api/jobs/{job_id}/accept")
async def accept_job_diff(
    request: Request,
    job_id: str,
    *,
    dependencies: JobDiffDependencies = Depends(get_job_diff_dependencies),
) -> dict[str, Any]:
    """Mode A accept: apply the job's diff back to the project's cloud folder.

    Gates:

    * The caller is an administrator or a current owner or editor of the
      job's project (403).
    * Job is ``pending_review`` and project-attached.
    * ``diff_status`` is ``pending`` (the diff capture flagged changes).
    * The job's checkout sends its diff for review (a read-only copy's
      changes are never written back).
    * Backend + Gitea are reachable.
    * No external modifications to the cloud folder since seed
      (etag map captured at seed time vs. fresh PROPFIND at accept). On
      divergence, returns 409 with the diverging path list — user must
      resolve manually and re-accept.

    On success, writes/deletes each diff path back via the cloud
    backend, then transitions ``diff_status='accepted'`` and
    ``status='completed'``.

    See knowledge-history/done/job_cloud_export.md §3.5.
    """
    user, job = await dependencies.require_job_access(
        request, dependencies.store, job_id
    )
    return await job_diff_review.accept_job_diff(
        job_id=job_id,
        user=user,
        authorized_job=job,
        dependencies=dependencies.diff_review,
    )


@router.post("/api/jobs/{job_id}/reject")
async def reject_job_diff(
    request: Request,
    job_id: str,
    *,
    dependencies: JobDiffDependencies = Depends(get_job_diff_dependencies),
) -> dict[str, Any]:
    """Mode A reject: discard the job's diff, no cloud write.

    Stamps ``diff_status='rejected'`` and ``status='completed'``. The
    Gitea commits stay around as the audit trail of what the agent
    tried to do (cheap; see §3.6). The caller must be an administrator or a
    current owner or editor of the job's project, as for an accept.

    See knowledge-history/done/job_cloud_export.md §3.6.
    """
    user, job = await dependencies.require_job_access(
        request, dependencies.store, job_id
    )
    return await job_diff_review.reject_job_diff(
        job_id=job_id,
        user=user,
        authorized_job=job,
        dependencies=dependencies.diff_review,
    )
