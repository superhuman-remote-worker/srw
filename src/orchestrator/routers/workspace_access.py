"""HTTP adapters for frozen-job reads, snapshots and workspace access.

Extracted verbatim from ``orchestrator.main`` (R1.B04 lane W).

The snapshot reads keep their bodies here because they are the adapter: each
is a gate plus one call into the snapshot service, wrapped in the
``except Exception -> 500`` shape the cockpit's polling depends on.
``get_frozen_job_data`` keeps its three-source fallback (DB freeze data, then
Gitea, then the local workspace) for the same reason — the order is the
contract, and a 404 only fires once all three have missed.

The three workspace operations live in
:mod:`orchestrator.services.workspace_access`. Two of them
(``provision-workspace`` and ``workspace-status``) fire the internal-key gate
here, before the operation runs; ``ensure-workspace-access`` binds its gate
instead, because in ``main`` it fires *inside* the try/except that turns an
unexpected failure into a logged 500.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request

from orchestrator.schemas.workspace_access import JobWorkspaceUpgradeRequest
from orchestrator.security.access import (
    require_internal,
    require_job_access,
)
from orchestrator.security.auth import require_approved_user
from orchestrator.services import workspace_access

router = APIRouter()


@dataclass(frozen=True)
class WorkspaceAccessDependencies:
    """Per-app stores, singletons and gates; no store startup or ownership."""

    store: Any
    forge: Any
    workspace: Any
    snapshots: Any
    operations: workspace_access.WorkspaceOperationDependencies
    resolve_job_repo: Callable[..., Awaitable[Any]]
    require_admin: Callable[..., Awaitable[Any]]
    require_approved_user: Callable[..., Awaitable[Any]] = require_approved_user
    require_job_access: Callable[..., Awaitable[Any]] = require_job_access
    require_internal: Callable[..., Awaitable[Any]] = require_internal


def get_workspace_access_dependencies(request: Request) -> WorkspaceAccessDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.workspace_access_dependencies_factory()


@router.get("/api/jobs/{job_id}/frozen")
async def get_frozen_job_data(
    request: Request,
    job_id: str,
    *,
    dependencies: WorkspaceAccessDependencies = Depends(
        get_workspace_access_dependencies
    ),
) -> dict[str, Any]:
    """Get the frozen job data (job_frozen.json) for a pending_review job.

    Tries Gitea first, falls back to local workspace.

    Returns:
        Contents of job_frozen.json (summary, deliverables, confidence, notes, etc.)
    """
    _, job = await dependencies.require_job_access(request, dependencies.store, job_id)
    try:
        frozen_data = None

        # Primary: read freeze_data from DB
        if job.get("freeze_data"):
            frozen_data = job["freeze_data"]
            if isinstance(frozen_data, str):
                frozen_data = json.loads(frozen_data)

        # Fallback: Gitea
        if frozen_data is None and dependencies.forge.is_initialized:
            repo_name, job_branch = await dependencies.resolve_job_repo(job_id)
            frozen_data = await dependencies.forge.get_file(
                repo_name, "output/job_frozen.json", ref=job_branch
            )

        # Fallback: local workspace
        if frozen_data is None:
            workspace_path = (
                dependencies.workspace.base_path / "output" / "job_frozen.json"
            )
            if workspace_path.exists():
                frozen_data = json.loads(workspace_path.read_text())

        if frozen_data is None:
            raise HTTPException(
                status_code=404,
                detail=f"No frozen job data found for job '{job_id}'",
            )

        return frozen_data

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/api/jobs/{job_id}/snapshot")
async def get_job_snapshot(
    request: Request,
    job_id: str,
    *,
    dependencies: WorkspaceAccessDependencies = Depends(
        get_workspace_access_dependencies
    ),
) -> dict[str, Any]:
    """Get snapshot metadata for a job.

    Returns status, source type, size, and environment summary.
    Used by the cockpit to show snapshot availability indicators.
    """
    await dependencies.require_job_access(request, dependencies.store, job_id)
    try:
        result = await dependencies.snapshots.get_snapshot_status(job_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.delete("/api/jobs/{job_id}/snapshot")
async def delete_job_snapshot(
    request: Request,
    job_id: str,
    *,
    dependencies: WorkspaceAccessDependencies = Depends(
        get_workspace_access_dependencies
    ),
) -> dict[str, Any]:
    """Delete all snapshots for a job from S3."""
    await dependencies.require_job_access(request, dependencies.store, job_id)
    try:
        success = await dependencies.snapshots.delete_snapshot(job_id)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to delete snapshot")
        return {"status": "deleted", "job_id": job_id}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.put("/api/jobs/{job_id}/snapshot/pin")
async def toggle_snapshot_pin(
    request: Request,
    job_id: str,
    *,
    dependencies: WorkspaceAccessDependencies = Depends(
        get_workspace_access_dependencies
    ),
) -> dict[str, Any]:
    """Toggle pin state on a snapshot (GC exemption).

    Pinned snapshots are exempt from automatic garbage collection.
    """
    await dependencies.require_job_access(request, dependencies.store, job_id)
    try:
        new_value = await dependencies.snapshots.toggle_pin(job_id)
        return {"job_id": job_id, "pinned": new_value}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/api/snapshots/stats")
async def get_snapshot_stats(
    request: Request,
    *,
    dependencies: WorkspaceAccessDependencies = Depends(
        get_workspace_access_dependencies
    ),
) -> dict[str, Any]:
    """Get aggregate snapshot storage statistics. **Admin only** (G5) —
    storage-level metric with no per-user shape.

    Returns total snapshot count, total size, GC pending info.
    """
    await dependencies.require_admin(request)
    try:
        return await dependencies.snapshots.get_storage_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/api/jobs/{job_id}/ensure-workspace-access")
async def ensure_workspace_access(
    request: Request,
    job_id: str,
    *,
    dependencies: WorkspaceAccessDependencies = Depends(
        get_workspace_access_dependencies
    ),
) -> dict[str, Any]:
    """Ensure the current user has Gitea access to the job's workspace repo.

    Called by the cockpit before navigating to the Gitea workspace URL.
    Re-attempts the access grant that may have been skipped at job creation
    time (if the user hadn't logged into Gitea yet via OIDC).
    """

    # The gate is bound rather than awaited here so it stays inside the
    # operation's try/except: an unexpected (non-``HTTPException``) failure
    # while authenticating must still surface as a logged 500, exactly as it
    # did in ``main``. Kept as a comment, not docstring text — FastAPI
    # publishes a route's docstring as its OpenAPI description.
    # The grant makes the caller a Gitea collaborator on the job's repo, so
    # the gate is job visibility (owner, project member or admin), not just
    # an approved account (N36).
    async def approve_caller() -> dict[str, Any]:
        user, _job = await dependencies.require_job_access(
            request, dependencies.store, job_id
        )
        return user

    return await workspace_access.ensure_workspace_access(
        job_id=job_id,
        require_approved_user=approve_caller,
        dependencies=dependencies.operations,
    )


@router.post("/api/jobs/{job_id}/provision-workspace")
async def provision_job_workspace(
    request: Request,
    job_id: str,
    body: JobWorkspaceUpgradeRequest | None = None,
    *,
    dependencies: WorkspaceAccessDependencies = Depends(
        get_workspace_access_dependencies
    ),
) -> dict[str, Any]:
    """Provision a real workspace container for a RUNNING lite (``virtual``/
    ``none``) worker job, upgrading it to the ``sandbox`` tier IN PLACE.
    **Internal** (P4b) — requires ``X-Internal-Key``.

    The worker-side counterpart to the session ``upgrade-to-workspace`` endpoint
    (workspace_tier_upgrade.md §4.3 W2). Unlike the operator-gated
    ``/api/jobs/{id}/upgrade-to-vm`` (which freezes → re-dispatches), this never
    pauses or re-dispatches: the job stays ``processing`` and the SAME running
    agent provisions, polls ``/workspace-status`` for readiness, seeds the
    virtual files into the new pod, and hot-swaps its ``WorkspaceManager``
    backend — re-``ainvoke``-ing from the local checkpoint. That sidesteps the
    non-portable pod-local LangGraph checkpoint entirely (§2.3). Idempotent: a
    second call while a container is already provisioning/ready is a no-op.

    ``vm`` is intentionally NOT accepted here: VM is operator-gated and must
    pause for approval (it can't stay in-process), so it keeps the existing
    ``/upgrade-to-vm`` freeze→approve→re-dispatch path (§4.3 W3).
    """
    await dependencies.require_internal(request)
    return await workspace_access.provision_job_workspace(
        job_id=job_id,
        body=body,
        dependencies=dependencies.operations,
    )


@router.get("/api/jobs/{job_id}/workspace-status")
async def get_job_workspace_status(
    request: Request,
    job_id: str,
    *,
    dependencies: WorkspaceAccessDependencies = Depends(
        get_workspace_access_dependencies
    ),
) -> dict[str, Any]:
    """Return a running job's workspace-container connection details for the
    agent's in-process upgrade poller. **Internal** — requires ``X-Internal-Key``.

    The job-side analogue of ``GET /api/agents/threads/{id}/workspace``: surfaces
    ``context.workspace_container`` (status + pod IP/port) so the agent's
    ``_poll_job_workspace_ready`` can build the upgraded ``RemoteBackend``
    (workspace_tier_upgrade.md §4.3 W1). The provisioner writes ``pod_ip``/
    ``port``; map ``port`` → ``pod_port`` to match the session poller's shape.
    """
    await dependencies.require_internal(request)
    return await workspace_access.get_job_workspace_status(
        job_id=job_id,
        dependencies=dependencies.operations,
    )
