"""HTTP adapters for per-job and per-thread operational diagnostics."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from orchestrator.security.access import require_job_access, require_thread_owner
from orchestrator.services import job_diagnostics

router = APIRouter()


@dataclass(frozen=True)
class JobDiagnosticsDependencies:
    store: Any
    operations: job_diagnostics.JobDiagnosticsDependencies
    require_job_access: Callable[..., Awaitable[Any]] = require_job_access
    require_thread_owner: Callable[..., Awaitable[Any]] = require_thread_owner


def get_job_diagnostics_dependencies(request: Request) -> JobDiagnosticsDependencies:
    return request.app.state.job_diagnostics_dependencies_factory()


@router.get("/api/jobs/{job_id}/logs")
async def get_job_logs(
    request: Request,
    job_id: str,
    lines: int = Query(default=100, ge=1, le=1000),
    grep: str | None = Query(default=None),
    level: str | None = Query(default=None),
    raw: bool = Query(default=False),
    *,
    dependencies: JobDiagnosticsDependencies = Depends(
        get_job_diagnostics_dependencies
    ),
) -> Any:
    """Read the tail of a job's log file with optional filtering.

    Serves the live per-job file when it exists (compose/dev shared volume),
    else falls back to the S3 log archive written at agent-pod deletion — logs
    stay readable after the reap.

    Args:
        job_id: Job UUID
        lines: Number of tail lines to return (1-1000, default 100)
        grep: Case-insensitive substring filter
        level: Log level filter (DEBUG, INFO, WARNING, ERROR)
        raw: Return the whole log as text/plain, no filtering/tailing —
            for reading in an IDE or handing to an agent. For archived
            logs this is the full pod log (may span multiple jobs).
    """
    # Design: knowledge-history/done/2026-07-15-job-log-archive.md.
    # Validate job_id format
    try:
        UUID(job_id)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid job_id format: {job_id}")

    _, job = await dependencies.require_job_access(request, dependencies.store, job_id)

    return await job_diagnostics.get_job_logs(
        job_id=job_id,
        job=job,
        lines=lines,
        grep=grep,
        level=level,
        raw=raw,
        dependencies=dependencies.operations,
    )


@router.get("/api/persistent/threads/{thread_id}/logs")
async def get_thread_logs(
    request: Request,
    thread_id: str,
    lines: int = Query(default=100, ge=1, le=1000),
    grep: str | None = Query(default=None),
    level: str | None = Query(default=None),
    raw: bool = Query(default=False),
    *,
    dependencies: JobDiagnosticsDependencies = Depends(
        get_job_diagnostics_dependencies
    ),
) -> Any:
    """Read the archived agent-pod log for a session (auth: owner only).

    Post-mortem debugging for sessions whose agent pod is gone: serves the S3
    archive written at pod deletion. 404s until the pod has been deleted at
    least once — while it is alive, the log lives on the pod
    (``kubectl logs``).
    """
    # Design: knowledge-history/done/2026-07-15-job-log-archive.md.
    try:
        UUID(thread_id)
    except ValueError:
        raise HTTPException(
            status_code=400, detail=f"Invalid thread_id format: {thread_id}"
        )

    _, thread = await dependencies.require_thread_owner(
        request, dependencies.store, thread_id
    )

    return await job_diagnostics.get_thread_logs(
        thread_id=thread_id,
        thread=thread,
        lines=lines,
        grep=grep,
        level=level,
        raw=raw,
        dependencies=dependencies.operations,
    )


@router.get("/api/jobs/{job_id}/llm-requests")
async def get_job_llm_requests(
    request: Request,
    job_id: str,
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    call_type: str | None = Query(default=None),
    status: str | None = Query(default=None),
    *,
    dependencies: JobDiagnosticsDependencies = Depends(
        get_job_diagnostics_dependencies
    ),
) -> dict[str, Any]:
    """List LLM requests for a job with summary fields.

    Returns model, timestamp, token usage, tool call names, call_type, and
    iteration for each request. Use the _id with GET /api/requests/{doc_id} to
    get the full request/response.

    Query params:
        call_type: filter by call type; ``all``/omitted returns main +
            auxiliary calls, or pass an exact type (e.g. ``memory_extraction``).
        status: pass ``error`` to return only failed calls (auxiliary failures
            carry ``status="error"``).
    """
    await dependencies.require_job_access(request, dependencies.store, job_id)
    return await job_diagnostics.get_job_llm_requests(
        job_id=job_id,
        limit=limit,
        offset=offset,
        call_type=call_type,
        status=status,
        dependencies=dependencies.operations,
    )


@router.get("/api/jobs/{job_id}/shell-state")
async def get_job_shell_state(
    request: Request,
    job_id: str,
    *,
    dependencies: JobDiagnosticsDependencies = Depends(
        get_job_diagnostics_dependencies
    ),
) -> dict[str, Any]:
    """Proxy shell state request to the agent processing a job.

    The stored Pod IP is only a coordinate. Freshly attest the exact registered
    process and require its recipient envelope before returning shell output.
    """
    _, job = await dependencies.require_job_access(request, dependencies.store, job_id)
    return await job_diagnostics.get_job_shell_state(
        job_id=job_id, job=job, dependencies=dependencies.operations
    )
