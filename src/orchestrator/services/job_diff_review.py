"""Mode A job diff review — summary, per-file content, accept and reject.

Extracted verbatim from ``orchestrator.main`` (R1.B04 lane C). Four
properties are load-bearing and moved unchanged:

* **Completion control is an authority, not a policy this module owns.**
  ``guard`` / ``claim`` / ``finish`` / ``abort`` arrive as injected callables
  (port contract §3.2); the *ordering* around them is the contract: the guard
  fires before any gate, the claim is taken after every cheap refusal and
  before the first slow external call, and every failure between the claim
  and the commit aborts it. When the claim is ``None`` (commands disabled)
  the slow calls run without a deadline and the transition degrades to four
  separate store writes — deliberately, because there is no fence to keep.
* **A partial cloud write does not transition the job.** Errors from
  ``apply_diff_to_cloud`` release the claim and 502 with the counts so the
  user can retry; the job stays ``pending_review``.
* **Refusal shape.** The divergence and partial-write refusals carry a dict
  ``detail`` with a ``code``; every other refusal carries a plain string.
  A caller distinguishes them on that shape.
* **The in-memory job row is mutated to match the committed row** before the
  terminal side effects run, because ``_advance_project_loop`` and
  ``apply_terminal_job_side_effects`` read the dict, not the database.

Seeing a job is not deciding its write-back: an accept writes the project
folder with the main cloud's own credential, so only an administrator or a
current owner or editor of the job's project may accept or reject its diff
(the job's owner too only while they are one; a viewer never writes the
folder through an accept), and only a diff the job's checkout sends for
review is ever accepted (``job_cloud_checkout.capture_applies``).
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import uuid4

from fastapi import HTTPException

logger = logging.getLogger(__name__)


class DiffStore(Protocol):
    """The store surface the diff review needs."""

    async def get_project(self, project_id: str) -> dict[str, Any] | None: ...

    async def get_user_role_in_project(
        self, project_id: str, user_id: str
    ) -> str | None: ...

    async def update_job_cloud_diff(self, job_id: str, *, diff_status: str) -> Any: ...

    async def update_job_merge_status(
        self, job_id: str, *, merge_status: str
    ) -> Any: ...

    async def update_job_status(self, job_id: str, *, status: str) -> Any: ...

    async def merge_job_context(self, job_id: str, patch: dict[str, Any]) -> Any: ...


class DiffForge(Protocol):
    @property
    def is_initialized(self) -> bool: ...


@dataclass(frozen=True)
class JobDiffReviewDependencies:
    """Collaborators for one diff-review operation, resolved per invocation.

    ``store``, ``vector_store``, ``forge`` and ``cloud_router`` are all
    rebound during ``lifespan``; the four completion-control callables are the
    application's B08-owned authority, injected rather than re-derived so this
    module never holds a second copy of that policy.
    """

    store: DiffStore
    vector_store: Any
    forge: DiffForge
    cloud_router: Any
    get_completion_control: Callable[[], Any]
    guard_completion_control: Callable[..., Awaitable[None]]
    claim_completion_control: Callable[..., Awaitable[Any]]
    abort_completion_control_claim: Callable[[Any], Awaitable[None]]
    advance_project_loop: Callable[..., Awaitable[Any]]


#: The project roles whose members decide a job's write-back diff.
DIFF_REVIEWER_ROLES = frozenset({"owner", "editor"})


async def _require_diff_reviewer(
    store: DiffStore, user: dict[str, Any], job: dict[str, Any], *, action: str
) -> None:
    """403 unless ``user`` may decide the job's diff: an administrator, or a
    current owner or editor of the job's project. A project-less job has
    no diff to decide; the gates below refuse it with their own 409."""
    if user.get("is_admin"):
        return
    project_id = job.get("project_id")
    if not project_id:
        return
    role = await store.get_user_role_in_project(str(project_id), str(user["id"]))
    if role in DIFF_REVIEWER_ROLES:
        return
    raise HTTPException(
        status_code=403,
        detail=(
            f"Only an owner or editor of the job's project, or an "
            f"administrator, can {action} its project-folder diff."
        ),
    )


async def get_job_diff(
    *,
    job_id: str,
    authorized_job: dict[str, Any],
    dependencies: JobDiffReviewDependencies,
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
    job = authorized_job
    if not job.get("cloud_diff_baseline_commit"):
        raise HTTPException(
            status_code=404,
            detail="Job has no Mode A diff baseline.",
        )

    if not dependencies.forge.is_initialized:
        raise HTTPException(status_code=503, detail="Gitea not available.")
    from orchestrator.services.diff_source import GiteaDiffSource

    diff_source = GiteaDiffSource(job=job, gitea_client=dependencies.forge)
    summary = await diff_source.summary()
    if summary is None:
        raise HTTPException(
            status_code=404,
            detail="Diff unavailable (no repo or no head).",
        )
    return {
        "job_id": job_id,
        "diff_status": job.get("diff_status"),
        "baseline_commit": summary.meta["baseline_commit"],
        "head_commit": summary.meta["head_commit"],
        "files": [{"path": f.path, "status": f.status} for f in summary.files],
    }


async def get_job_diff_file(
    *,
    job_id: str,
    file_path: str,
    authorized_job: dict[str, Any],
    dependencies: JobDiffReviewDependencies,
) -> dict[str, Any]:
    """Mode A per-file diff content.

    Returns ``{path, status, old_content, new_content}`` for one file in
    the diff. ``old_content`` is read from the baseline commit;
    ``new_content`` from the head of the job's branch. Either side can
    be ``None`` (added → no old, deleted → no new).

    Only files under ``projects/`` are accepted — the Mode A diff is
    scoped to the project-folder mount.
    """
    job = authorized_job
    baseline = job.get("cloud_diff_baseline_commit")
    if not baseline:
        raise HTTPException(
            status_code=404,
            detail="Job has no Mode A diff baseline.",
        )
    if not file_path.startswith("projects/"):
        raise HTTPException(
            status_code=400,
            detail="Per-file diff is scoped to projects/<slug>/* paths.",
        )
    if not dependencies.forge.is_initialized:
        raise HTTPException(status_code=503, detail="Gitea not available.")
    repo_name = job.get("repo_name")
    if not repo_name:
        raise HTTPException(status_code=404, detail="Job repo not found.")

    from orchestrator.services.diff_source import GiteaDiffSource

    diff_source = GiteaDiffSource(job=job, gitea_client=dependencies.forge)

    # Pull the diff summary to learn the file's status (added/modified/deleted).
    summary = await diff_source.summary()
    if summary is None:
        raise HTTPException(status_code=404, detail="Diff unavailable for this job.")
    file_entry = next(
        (f for f in summary.files if f.path == file_path),
        None,
    )
    if file_entry is None:
        raise HTTPException(
            status_code=404,
            detail=f"Path '{file_path}' is not in the diff.",
        )

    content = await diff_source.file(file_path)
    if content is None:
        raise HTTPException(
            status_code=404,
            detail=f"Path '{file_path}' is not in the diff.",
        )
    return {
        "job_id": job_id,
        "path": file_path,
        "status": content.status,
        "old_content": content.old_content,
        "new_content": content.new_content,
    }


async def accept_job_diff(
    *,
    job_id: str,
    user: dict[str, Any],
    authorized_job: dict[str, Any],
    dependencies: JobDiffReviewDependencies,
) -> dict[str, Any]:
    """Mode A accept: apply the job's diff back to the project's cloud folder.

    Gates:

    * ``user`` is an administrator or a current owner or editor of the
      job's project (403).
    * Job is ``pending_review`` and project-attached.
    * ``diff_status`` is ``pending`` (the diff capture flagged changes).
    * The job's checkout sends its diff for review (``capture_applies``): a
      read-only copy's changes are never written back, even when an older
      orchestrator captured them; reject discards them.
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
    job = authorized_job
    await dependencies.guard_completion_control(job_id, source="mode_a_accept")
    await _require_diff_reviewer(dependencies.store, user, job, action="accept")

    # --- Gates -------------------------------------------------------
    if job.get("status") != "pending_review":
        raise HTTPException(
            status_code=409,
            detail=(
                f"Job is in status '{job.get('status')}'; "
                "only pending_review jobs can be accepted."
            ),
        )
    if not job.get("project_id"):
        raise HTTPException(
            status_code=409,
            detail="Job has no project attached; nothing to write back to.",
        )
    if job.get("diff_status") != "pending":
        raise HTTPException(
            status_code=409,
            detail=(
                f"Job diff_status is '{job.get('diff_status')}'; "
                "only pending diffs can be accepted."
            ),
        )

    if not job.get("cloud_diff_baseline_commit"):
        raise HTTPException(
            status_code=409,
            detail="Job has no Mode A baseline; nothing to compare against.",
        )
    from orchestrator.services.job_cloud_checkout import capture_applies

    if not capture_applies(job):
        raise HTTPException(
            status_code=409,
            detail=(
                "This job's project folder copy is read-only: SRW never writes "
                "its changes back. Reject the diff to discard it."
            ),
        )

    if not dependencies.forge.is_initialized:
        raise HTTPException(status_code=503, detail="Gitea not available.")

    project = await dependencies.store.get_project(str(job["project_id"]))
    if not project:
        raise HTTPException(status_code=404, detail="Project not found.")
    if not project.get("main_cloud_folder_handle"):
        raise HTTPException(
            status_code=409,
            detail="Project has no cloud folder; cannot apply diff.",
        )
    backend_id = project.get("main_cloud_backend")
    if not backend_id:
        raise HTTPException(
            status_code=409,
            detail="Project has no cloud backend; cannot apply diff.",
        )
    try:
        backend = dependencies.cloud_router.for_project(project)
    except Exception as e:
        error_ref = uuid4().hex[:12]
        logger.exception(
            "Mode A: job %s — cloud backend %r unavailable (error_ref=%s)",
            job_id,
            backend_id,
            error_ref,
        )
        raise HTTPException(
            status_code=503,
            detail=(
                f"Cloud backend '{backend_id}' unavailable (error_ref={error_ref})"
            ),
        ) from e
    if not backend.is_initialized:
        raise HTTPException(status_code=503, detail="Cloud backend not initialized.")

    control_claim = await dependencies.claim_completion_control(
        {**job, "id": job_id}, source="mode_a_accept"
    )

    # --- External-modification gate ---------------------------------
    from orchestrator.services.job_cloud_baseline import (
        apply_diff_to_cloud,
        detect_external_mods,
        get_diff_summary,
        project_folder_slug,
    )

    try:
        diff_summary = await (
            asyncio.wait_for(
                get_diff_summary(job=job, gitea_client=dependencies.forge),
                timeout=120.0,
            )
            if control_claim is not None
            else get_diff_summary(job=job, gitea_client=dependencies.forge)
        )
    except Exception:
        await dependencies.abort_completion_control_claim(control_claim)
        raise
    slug = project_folder_slug(job, project)
    prefix = f"projects/{slug}/"
    affected_paths = {
        str(entry.get("path"))[len(prefix) :]
        for entry in ((diff_summary or {}).get("files") or [])
        if str(entry.get("path") or "").startswith(prefix)
    }
    try:
        detect_call = detect_external_mods(
            job=job,
            project=project,
            main_cloud_router=dependencies.cloud_router,
            scope_paths=affected_paths,
        )
        diverged = await (
            asyncio.wait_for(detect_call, timeout=180.0)
            if control_claim is not None
            else detect_call
        )
    except Exception:
        await dependencies.abort_completion_control_claim(control_claim)
        raise
    if diverged:
        await dependencies.abort_completion_control_claim(control_claim)
        raise HTTPException(
            status_code=409,
            detail={
                "code": "external_modifications_detected",
                "message": (
                    "Cloud folder was modified externally since the job "
                    "started. Resolve manually before accepting."
                ),
                "diverged": diverged,
            },
        )

    # --- Apply -------------------------------------------------------
    try:
        apply_call = apply_diff_to_cloud(
            job=job,
            project=project,
            gitea_client=dependencies.forge,
            main_cloud_router=dependencies.cloud_router,
        )
        result = await (
            asyncio.wait_for(apply_call, timeout=20 * 60.0)
            if control_claim is not None
            else apply_call
        )
    except Exception:
        await dependencies.abort_completion_control_claim(control_claim)
        raise
    if result.get("errors"):
        # Partial failure: cloud is now in a mixed state. Surface the
        # errors so the user can see what missed; don't transition the
        # job — user can retry.
        await dependencies.abort_completion_control_claim(control_claim)
        raise HTTPException(
            status_code=502,
            detail={
                "code": "partial_write_failure",
                "applied": result.get("applied", 0),
                "deleted": result.get("deleted", 0),
                "errors": result.get("errors"),
            },
        )

    # --- Status transition ------------------------------------------
    delivery = {
        "delivery_status": "cloud-applied",
        "needs_review": False,
        "delivery_sha": (diff_summary or {}).get("head_commit"),
        "notes": [],
        "applied": int(result.get("applied") or 0),
        "deleted": int(result.get("deleted") or 0),
    }
    if control_claim is not None:
        from orchestrator.services.completion_control import (
            CompletionControlClaimConflict,
        )

        try:
            async with dependencies.get_completion_control().finish_claim(
                control_claim
            ) as (
                conn,
                _locked_job,
            ):
                updated = await conn.fetchrow(
                    """
                    UPDATE jobs
                    SET diff_status='accepted', merge_status='cloud-applied',
                        status='completed', assigned_agent_id=NULL,
                        completed_at=COALESCE(completed_at, CURRENT_TIMESTAMP),
                        context=COALESCE(context, '{}'::jsonb) || $2::jsonb,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=$1::uuid
                      AND status='pending_review'
                      AND diff_status='pending'
                      AND execution_lane=$3::text
                    RETURNING id
                    """,
                    job_id,
                    json.dumps({"loop_cloud_delivery": delivery}),
                    str(job.get("execution_lane") or "pinned"),
                )
                if updated is None:
                    raise CompletionControlClaimConflict(
                        "job changed while Mode A accept was being committed"
                    )
        except CompletionControlClaimConflict as exc:
            await dependencies.abort_completion_control_claim(control_claim)
            raise HTTPException(status_code=409, detail=str(exc)) from exc
    else:
        await dependencies.store.update_job_cloud_diff(job_id, diff_status="accepted")
        await dependencies.store.update_job_merge_status(
            job_id, merge_status="cloud-applied"
        )
        await dependencies.store.update_job_status(job_id, status="completed")
        await dependencies.store.merge_job_context(
            job_id, {"loop_cloud_delivery": delivery}
        )
    job["status"] = "completed"
    job["diff_status"] = "accepted"
    job["merge_status"] = "cloud-applied"
    _accept_ctx = job.get("context") or {}
    if isinstance(_accept_ctx, str):
        try:
            _accept_ctx = json.loads(_accept_ctx)
        except (json.JSONDecodeError, TypeError):
            _accept_ctx = {}
    if not isinstance(_accept_ctx, dict):
        _accept_ctx = {}
    _accept_ctx["loop_cloud_delivery"] = delivery
    job["context"] = _accept_ctx

    terminal_actions: list[str] = []
    from orchestrator.services.project_loops import job_loop_id

    if job_loop_id(job):
        await dependencies.advance_project_loop(job, {}, terminal_actions)
    else:
        from orchestrator.services.completion import apply_terminal_job_side_effects

        side_effects = await apply_terminal_job_side_effects(
            job,
            "completed",
            gitea=dependencies.forge,
            db=dependencies.store,
            vector_db=dependencies.vector_store,
        )
        terminal_actions.extend(side_effects["actions"])
    logger.info(
        "Mode A: job %s — diff accepted (%d applied, %d deleted)",
        job_id,
        result.get("applied", 0),
        result.get("deleted", 0),
    )
    return {
        "job_id": job_id,
        "diff_status": "accepted",
        "status": "completed",
        "applied": result.get("applied", 0),
        "deleted": result.get("deleted", 0),
        "actions": terminal_actions,
    }


async def reject_job_diff(
    *,
    job_id: str,
    user: dict[str, Any],
    authorized_job: dict[str, Any],
    dependencies: JobDiffReviewDependencies,
) -> dict[str, Any]:
    """Mode A reject: discard the job's diff, no cloud write.

    Stamps ``diff_status='rejected'`` and ``status='completed'``. The
    Gitea commits stay around as the audit trail of what the agent
    tried to do (cheap; see §3.6). Decided by the same people as an
    accept; a read-only copy's stray diff is rejected like any other, since
    a pending diff blocks every other way to complete the job.

    See knowledge-history/done/job_cloud_export.md §3.6.
    """
    job = authorized_job
    await dependencies.guard_completion_control(job_id, source="mode_a_reject")
    await _require_diff_reviewer(dependencies.store, user, job, action="reject")

    if job.get("status") != "pending_review":
        raise HTTPException(
            status_code=409,
            detail=(
                f"Job is in status '{job.get('status')}'; "
                "only pending_review jobs can be rejected."
            ),
        )
    if not job.get("project_id"):
        raise HTTPException(
            status_code=409,
            detail="Job has no project attached; no diff to reject.",
        )
    if job.get("diff_status") != "pending":
        raise HTTPException(
            status_code=409,
            detail=(
                f"Job diff_status is '{job.get('diff_status')}'; "
                "only pending diffs can be rejected."
            ),
        )

    control_claim = await dependencies.claim_completion_control(
        {**job, "id": job_id}, source="mode_a_reject"
    )
    delivery = {
        "delivery_status": "cloud-rejected",
        "needs_review": False,
        "delivery_sha": None,
        "notes": ["project-file diff rejected; cloud folder left unchanged"],
    }
    if control_claim is not None:
        from orchestrator.services.completion_control import (
            CompletionControlClaimConflict,
        )

        try:
            async with dependencies.get_completion_control().finish_claim(
                control_claim
            ) as (
                conn,
                _locked_job,
            ):
                updated = await conn.fetchrow(
                    """
                    UPDATE jobs
                    SET diff_status='rejected', merge_status='cloud-rejected',
                        status='completed', assigned_agent_id=NULL,
                        completed_at=COALESCE(completed_at, CURRENT_TIMESTAMP),
                        context=COALESCE(context, '{}'::jsonb) || $2::jsonb,
                        updated_at=CURRENT_TIMESTAMP
                    WHERE id=$1::uuid
                      AND status='pending_review'
                      AND diff_status='pending'
                      AND execution_lane=$3::text
                    RETURNING id
                    """,
                    job_id,
                    json.dumps({"loop_cloud_delivery": delivery}),
                    str(job.get("execution_lane") or "pinned"),
                )
                if updated is None:
                    raise CompletionControlClaimConflict(
                        "job changed while Mode A reject was being committed"
                    )
        except CompletionControlClaimConflict as exc:
            await dependencies.abort_completion_control_claim(control_claim)
            raise HTTPException(status_code=409, detail=str(exc)) from exc
    else:
        await dependencies.store.update_job_cloud_diff(job_id, diff_status="rejected")
        await dependencies.store.update_job_merge_status(
            job_id, merge_status="cloud-rejected"
        )
        await dependencies.store.update_job_status(job_id, status="completed")
        await dependencies.store.merge_job_context(
            job_id, {"loop_cloud_delivery": delivery}
        )
    job["status"] = "completed"
    job["diff_status"] = "rejected"
    job["merge_status"] = "cloud-rejected"
    _reject_ctx = job.get("context") or {}
    if isinstance(_reject_ctx, str):
        try:
            _reject_ctx = json.loads(_reject_ctx)
        except (json.JSONDecodeError, TypeError):
            _reject_ctx = {}
    if not isinstance(_reject_ctx, dict):
        _reject_ctx = {}
    _reject_ctx["loop_cloud_delivery"] = delivery
    job["context"] = _reject_ctx

    terminal_actions: list[str] = []
    from orchestrator.services.project_loops import job_loop_id

    if job_loop_id(job):
        await dependencies.advance_project_loop(job, {}, terminal_actions)
    else:
        from orchestrator.services.completion import apply_terminal_job_side_effects

        side_effects = await apply_terminal_job_side_effects(
            job,
            "completed",
            gitea=dependencies.forge,
            db=dependencies.store,
            vector_db=dependencies.vector_store,
        )
        terminal_actions.extend(side_effects["actions"])
    logger.info("Mode A: job %s — diff rejected", job_id)
    return {
        "job_id": job_id,
        "diff_status": "rejected",
        "status": "completed",
        "actions": terminal_actions,
    }


__all__ = [
    "DIFF_REVIEWER_ROLES",
    "DiffForge",
    "DiffStore",
    "JobDiffReviewDependencies",
    "accept_job_diff",
    "get_job_diff",
    "get_job_diff_file",
    "reject_job_diff",
]
