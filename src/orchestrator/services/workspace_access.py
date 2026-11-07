"""Gitea workspace access grants and the in-place job workspace upgrade.

Extracted verbatim from ``orchestrator.main`` (R1.B04 lane W). Three
properties are load-bearing and move unchanged:

* **The grant route degrades, it does not fail.** Every missing precondition
  (no repo, no Gitea, no email) is a ``200`` with a ``reason``; only an
  unexpected exception becomes a 500, and the approved-user gate fires *inside*
  that try/except so its own non-HTTP failure is logged and reported the same
  way. The gate therefore arrives as a callable the router binds to
  ``request``.
* **The workspace upgrade is fail-closed and ordered.** ``target_tier``
  validation, contract resolution and the owner-grant check (Sec-1) all run
  before any provisioning, and idempotency is only claimed once the assignment
  itself is durable — an opposite-tier readiness marker is never authority to
  select sandbox.
* **The tier transition is atomic before the background task.** The DB
  transition is what serializes concurrent callers;
  ``create_workspace`` is only scheduled after it commits, and a lost race
  re-reads the row before deciding between "already accepted" and a 409.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException

from orchestrator.schemas.workspace_access import JobWorkspaceUpgradeRequest
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from shared.workspace_contract import (
    WorkspaceContractError,
    resolve_workspace_contract,
)

logger = logging.getLogger(__name__)

#: Authenticate the caller. Bound by the router to ``require_approved_user``.
ApproveCaller = Callable[[], Awaitable[dict[str, Any]]]

#: How many of a user's projects one login re-grants. ``get_projects_for_user``
#: lists the default project first, then the most recently updated, so the
#: Gitea work a login starts stays bounded however many projects the account
#: belongs to; opening any other project in Cockpit re-grants that one.
LOGIN_REGRANT_PROJECT_LIMIT = 25


@dataclass(frozen=True)
class WorkspaceOperationDependencies:
    """Collaborators for one workspace-access operation, resolved per call.

    ``store``, ``forge`` and ``container_provisioner`` are rebound during
    ``lifespan``; the application rebuilds this dataclass per request rather
    than capturing it at import.
    """

    store: Any
    forge: Any
    container_provisioner: Any
    enforce_job_workspace_upgrade_grants: Callable[..., Awaitable[None]]


# =============================================================================
# Gitea workspace access
# =============================================================================


async def ensure_workspace_access(
    *,
    job_id: str,
    require_approved_user: ApproveCaller,
    dependencies: WorkspaceOperationDependencies,
) -> dict[str, Any]:
    """Ensure the current user has Gitea access to the job's workspace repo.

    Called by the cockpit before navigating to the Gitea workspace URL.
    Re-attempts the access grant that may have been skipped at job creation
    time (if the user hadn't logged into Gitea yet via OIDC).
    """
    try:
        user = await require_approved_user()
        job = await dependencies.store.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")

        repo_name = job.get("repo_name")
        if not repo_name:
            return {"granted": False, "reason": "no_repo"}

        if not dependencies.forge.is_initialized:
            return {"granted": False, "reason": "gitea_unavailable"}

        email = user.get("email")
        if not email:
            return {"granted": False, "reason": "no_email"}

        granted = await dependencies.forge.grant_user_repo_access(email, repo_name)
        return {"granted": granted, "reason": "ok" if granted else "user_not_in_gitea"}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Failed to ensure workspace access for job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e)) from e


async def grant_project_repo_access(
    *, store: Any, forge: Any, project_id: str, email: str
) -> dict[str, bool]:
    """Grant one user every managed repository of one project.

    The one place that decides a project member's Gitea access: adding a
    member, Cockpit opening the project and the login re-grant all call it, so
    they cannot drift apart. Each managed repository gets the forge's default
    grant, whatever the member's role; an attached (unmanaged) repository is
    not SRW's to share. Returns each managed repository's name and whether it
    was granted.
    """
    results: dict[str, bool] = {}
    for repo in await store.get_project_repositories(project_id):
        if repo.get("is_managed"):
            results[repo["name"]] = await forge.grant_user_repo_access(
                email, repo["name"]
            )
    return results


async def regrant_user_project_repos(
    *,
    store: Any,
    forge: Any,
    user_id: str,
    email: str,
    limit: int = LOGIN_REGRANT_PROJECT_LIMIT,
) -> int:
    """Re-grant a user the managed repositories of their projects.

    A creation-time grant that failed (no Gitea account yet) is otherwise never
    retried. Runs from the login's background task; capped at ``limit``
    projects, and one failing project does not stop the rest. Returns how many
    repositories were granted.
    """
    granted = 0
    for project in await store.get_projects_for_user(user_id, limit=limit):
        project_id = str(project["id"])
        try:
            results = await grant_project_repo_access(
                store=store, forge=forge, project_id=project_id, email=email
            )
        except Exception as e:
            logger.warning(
                "Gitea access to project %s not re-granted to user %s: %s",
                project_id,
                user_id,
                e,
            )
            continue
        granted += sum(results.values())
    return granted


async def ensure_project_repo_access(
    *,
    project_id: str,
    require_project_member: ApproveCaller,
    dependencies: WorkspaceOperationDependencies,
) -> dict[str, Any]:
    """Ensure the current member has Gitea access to the project's managed repos.

    Called by the cockpit when it opens a project, the counterpart of
    :func:`ensure_workspace_access` for the project's knowledge vault and jobs
    repo. Degrades like it: every missing precondition is a ``200`` with a
    ``reason``. Only an owner or member is granted; the gate's admin bypass is
    not membership.
    """
    try:
        user = await require_project_member()

        if not dependencies.forge.is_initialized:
            return {"granted": False, "reason": "gitea_unavailable"}

        email = user.get("email")
        if not email:
            return {"granted": False, "reason": "no_email"}

        role = await dependencies.store.get_user_role_in_project(
            project_id, str(user["id"])
        )
        if role is None:
            return {"granted": False, "reason": "not_a_member"}

        results = await grant_project_repo_access(
            store=dependencies.store,
            forge=dependencies.forge,
            project_id=project_id,
            email=email,
        )
        if not results:
            return {"granted": False, "reason": "no_repo"}
        granted = all(results.values())
        return {
            "granted": granted,
            "reason": "ok" if granted else "user_not_in_gitea",
            "repos": [name for name, ok in results.items() if ok],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            f"Failed to ensure repository access for project {project_id}: {e}"
        )
        raise HTTPException(status_code=500, detail=str(e)) from e


# =============================================================================
# In-place job workspace upgrade (worker side of workspace_tier_upgrade.md W2)
# =============================================================================


async def provision_job_workspace(
    *,
    job_id: str,
    body: JobWorkspaceUpgradeRequest | None,
    dependencies: WorkspaceOperationDependencies,
) -> dict[str, Any]:
    """Provision a real workspace container for a RUNNING lite (``virtual``/
    ``none``) worker job, upgrading it to the ``sandbox`` tier IN PLACE.

    The caller has already fired the internal-key gate; everything below is the
    operation as it stood in ``main``.
    """
    target_tier = (body.target_tier if body else "sandbox") or "sandbox"
    if target_tier != "sandbox":
        raise HTTPException(
            status_code=400,
            detail=(
                f"provision-workspace supports target_tier 'sandbox' only for a "
                f"running job; vm upgrades go through /upgrade-to-vm "
                f"(operator-gated). Got {target_tier!r}"
            ),
        )

    job = await dependencies.store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    try:
        workspace_contract = resolve_workspace_contract(job)
    except WorkspaceContractError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": exc.detail},
        ) from exc
    if workspace_contract.assigned_backend not in {"virtual", "none", "sandbox"}:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "workspace_backend_conflict",
                "message": (
                    "This job is assigned to the VM tier; an in-process "
                    "sandbox upgrade would violate its workspace contract"
                ),
                "assigned_backend": workspace_contract.assigned_backend,
            },
        )

    # Sec-1 — authorize against the owner's grants BEFORE provisioning
    # (fail-closed), via the shared gate. sandbox passes by default; a
    # shell-restricted owner is refused 403.
    await dependencies.enforce_job_workspace_upgrade_grants(
        job, target_tier=target_tier
    )

    if not (
        dependencies.container_provisioner.is_available
        and dependencies.container_provisioner.in_cluster
    ):
        raise HTTPException(
            status_code=503,
            detail="Workspace container provisioning not available (no in-cluster K8s)",
        )

    # Idempotency: short-circuit only after this route has durably changed the
    # assignment. Opposite-tier readiness is never authority to select sandbox.
    context = job.get("context") or {}
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (json.JSONDecodeError, TypeError):
            context = {}
    wc = context.get("workspace_container") or {}
    if workspace_contract.assigned_backend == "sandbox" and wc.get("status") in (
        "pending",
        "creating",
        "created",
        "ready",
    ):
        return {
            "status": wc["status"],
            "job_id": job_id,
            "target_tier": "sandbox",
            "message": "Workspace container already provisioned or in progress",
        }

    if workspace_contract.assigned_backend == "sandbox":
        raise HTTPException(
            status_code=409,
            detail={
                "code": "workspace_runtime_unavailable",
                "message": (
                    "The sandbox assignment exists without a current "
                    "provisioning generation; use normal workspace recovery"
                ),
            },
        )

    # Change the assignment and mark pending atomically, then provision in the
    # background: create_workspace blocks up
    # to ~120s waiting for the pod IP and updates context.workspace_container to
    # ready/failed itself. The agent polls /workspace-status
    # (-> _poll_job_workspace_ready) for the ready connection block, then swaps
    # in place. No status change, no _trigger_dispatch — the running agent owns
    # the swap (the whole point of the in-process design, §4.3 W1).
    # The container provisions the defaults chain's container template, read
    # as the Job's owner and recorded with the pending marker.
    from orchestrator.services.workspace_defaults_resolution import upgrade_record

    _, upgrade = await upgrade_record(
        dependencies.store,
        job.get("user_id"),
        role="worker",
        project_id=str(job["project_id"]) if job.get("project_id") else None,
        current_backend=workspace_contract.assigned_backend,
        requested_backend="sandbox",
    )
    transitioned = await dependencies.store.begin_job_workspace_tier_transition(
        job_id,
        expected_backend=workspace_contract.assigned_backend,
        target_backend="sandbox",
        requested_backend=workspace_contract.requested_backend,
        assignment_source="runtime_workspace_upgrade",
        expected_status=str(job.get("status") or ""),
        upgrade_config=upgrade["upgrade_config"],
        upgrade_sources=upgrade["upgrade_sources"],
    )
    if not transitioned:
        refreshed = await dependencies.store.get_job(job_id)
        if refreshed:
            try:
                refreshed_contract = resolve_workspace_contract(refreshed)
            except WorkspaceContractError:
                refreshed_contract = None
            refreshed_context = refreshed.get("context") or {}
            if isinstance(refreshed_context, str):
                try:
                    refreshed_context = json.loads(refreshed_context)
                except (json.JSONDecodeError, TypeError):
                    refreshed_context = {}
            refreshed_workspace = (
                refreshed_context.get("workspace_container")
                if isinstance(refreshed_context, dict)
                else None
            ) or {}
            if (
                refreshed_contract is not None
                and refreshed_contract.assigned_backend == "sandbox"
                and refreshed_workspace.get("status")
                in ("pending", "creating", "created", "ready")
            ):
                return {
                    "status": refreshed_workspace["status"],
                    "job_id": job_id,
                    "target_tier": "sandbox",
                    "message": "Workspace transition already accepted",
                }
        raise HTTPException(
            status_code=409,
            detail={
                "code": "workspace_contract_changed",
                "message": "Job workspace assignment changed before provisioning",
            },
        )
    asyncio.create_task(
        dependencies.container_provisioner.create_workspace(WorkspaceOwner.job(job_id))
    )

    return {
        "status": "provisioning",
        "job_id": job_id,
        "target_tier": "sandbox",
    }


async def get_job_workspace_status(
    *,
    job_id: str,
    dependencies: WorkspaceOperationDependencies,
) -> dict[str, Any]:
    """Return a running job's workspace-container connection details.

    The caller has already fired the internal-key gate.
    """
    job = await dependencies.store.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    try:
        contract = resolve_workspace_contract(job)
    except WorkspaceContractError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": exc.detail},
        ) from exc
    if contract.assigned_backend != "sandbox":
        raise HTTPException(
            status_code=409,
            detail={
                "code": "workspace_backend_conflict",
                "message": (
                    "Sandbox workspace status is unavailable for this job's "
                    "assigned workspace tier"
                ),
                "assigned_backend": contract.assigned_backend,
            },
        )

    context = job.get("context") or {}
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (json.JSONDecodeError, TypeError):
            context = {}
    wc = context.get("workspace_container") or {}

    return {
        "status": wc.get("status", "none"),
        "pod_ip": wc.get("pod_ip") or wc.get("host"),
        "pod_port": wc.get("pod_port") or wc.get("port"),
        "pod_name": wc.get("pod_name"),
        "namespace": wc.get("namespace"),
        "ssh_key_path": os.environ.get("SSH_KEY_PATH"),
        "git_remote_url": wc.get("git_remote_url"),
    }
