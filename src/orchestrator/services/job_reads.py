"""Read and enrich authorized job rows without owning transport or lifecycle.

Callers perform job access or project membership checks before entering these
operations. The project read deliberately retains its distinct job_summary SQL
and bare-array result; it is not the display-root query used by the jobs list.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Any, Callable, Protocol

from fastapi import HTTPException


class JobReadConnection(Protocol):
    async def fetch(self, query: str, *args: Any) -> Sequence[Mapping[str, Any]]: ...


class JobReadStore(Protocol):
    def acquire(self) -> AbstractAsyncContextManager[JobReadConnection]: ...

    async def get_project(self, project_id: str) -> dict[str, Any] | None: ...


class JobReadAuditReader(Protocol):
    @property
    def is_available(self) -> bool: ...

    async def get_audit_count(self, job_id: str) -> int: ...

    async def get_audit_counts(self, job_ids: list[str]) -> dict[str, int]: ...


@dataclass(frozen=True)
class JobReadDependencies:
    store: JobReadStore
    audit_reader: JobReadAuditReader
    redact_job: Callable[[dict[str, Any]], dict[str, Any]]
    with_cloud_review_mode: Callable[[dict[str, Any]], dict[str, Any]]
    status_filter_values: Sequence[str]


async def read_job(
    *,
    job_id: str,
    authorized_job: dict[str, Any],
    dependencies: JobReadDependencies,
) -> dict[str, Any]:
    """Enrich the exact row returned by the caller's completed access check."""
    job = authorized_job
    try:
        if dependencies.audit_reader.is_available:
            job["audit_count"] = await dependencies.audit_reader.get_audit_count(job_id)
        else:
            job["audit_count"] = None
        return dependencies.with_cloud_review_mode(dependencies.redact_job(job))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


async def read_project_jobs(
    *,
    project_id: str,
    status: str | None,
    limit: int,
    dependencies: JobReadDependencies,
) -> list[dict[str, Any]]:
    """Read a project's jobs after the caller has authorized membership."""
    # Direct service-level callers in the existing test/control seam omit the
    # FastAPI-injected value and therefore receive the Query marker itself.
    # HTTP requests are always a string or None.
    status = status if isinstance(status, str) else None
    if status is not None and status not in dependencies.status_filter_values:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Unknown job status/outcome: {status}. Valid values: "
                f"{', '.join(dependencies.status_filter_values)}"
            ),
        )
    try:
        async with dependencies.store.acquire() as conn:
            # The review mode reads the diff, the baseline and the checkout's
            # level (job_projection.cloud_review_mode); the summary view has
            # none of them.
            query = (
                "SELECT js.*, jcr.delivery_status, jcr.delivery_ref, "
                "jcr.delivery_sha, jcr.record_type AS change_record_type, "
                "j.diff_status, j.cloud_diff_baseline_commit, "
                "j.context->'cloud_checkout' AS cloud_checkout "
                "FROM job_summary js "
                "JOIN jobs j ON j.id = js.id "
                "LEFT JOIN job_change_records jcr ON jcr.job_id = js.id "
                "WHERE js.project_id = $1"
            )
            params: list = [project_id]
            if status:
                params.append(status)
                if status == "blocked_undelivered":
                    query += " AND js.completion_outcome_kind = $2"
                elif status == "cancelled":
                    query += (
                        " AND js.status = $2 AND "
                        "js.completion_outcome_kind IS DISTINCT FROM "
                        "'blocked_undelivered'"
                    )
                else:
                    query += " AND js.status = $2"
            query += " ORDER BY js.created_at DESC LIMIT $" + str(len(params) + 1)
            params.append(limit)
            rows = await conn.fetch(query, *params)

        jobs = [dict(r) for r in rows]

        # cloud_review_mode: all rows share one project, so resolve its
        # cloud-folder state once (the job_summary view has no projects JOIN).
        project = await dependencies.store.get_project(project_id)
        has_cloud_folder = bool(project and project.get("main_cloud_folder_handle"))

        # Enrich with audit counts (single batched query, not N+1)
        if dependencies.audit_reader.is_available:
            counts = await dependencies.audit_reader.get_audit_counts(
                [str(job["id"]) for job in jobs]
            )
            for job in jobs:
                job["audit_count"] = counts.get(str(job["id"]), 0)
        else:
            for job in jobs:
                job["audit_count"] = None

        result = []
        for job in jobs:
            job = dependencies.redact_job(job)
            job["project_has_cloud_folder"] = has_cloud_folder
            result.append(dependencies.with_cloud_review_mode(job))
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
