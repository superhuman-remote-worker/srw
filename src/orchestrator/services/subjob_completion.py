"""Scholar and compatibility-delegation completion workflows.

The delegation-child producer is retired.  Its persisted children still need
one release of completion and resume handling, so that compatibility consumer
is kept explicit here without exposing any creation API for delegation jobs.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import json
import logging
from typing import Any, Protocol

from shared.job_mode import JOB_MODE_KEY, JOB_MODE_WORKER
from shared.runtime.core.loader import canonical_config_name

logger = logging.getLogger(__name__)


class SubjobCompletionStore(Protocol):
    async def get_job(self, job_id: str) -> dict[str, Any] | None: ...

    async def get_user(self, user_id: str) -> dict[str, Any] | None: ...

    async def update_job_status(
        self, job_id: str, *, status: str, **updates: Any
    ) -> Any: ...

    async def merge_job_context(self, job_id: str, updates: dict[str, Any]) -> Any: ...

    async def create_job(self, **values: Any) -> dict[str, Any]: ...

    async def bind_job_managed_repository(
        self, job_id: str, *, repo_name: str, clean_url: str
    ) -> bool: ...

    def acquire(self) -> Any: ...

    async def queue_stateless_job_for_resume(
        self, job_id: str, context: dict[str, Any], **options: Any
    ) -> bool: ...

    async def all_delegation_children_terminal(self, parent_job_id: str) -> bool: ...

    async def get_delegation_children(
        self, parent_job_id: str
    ) -> list[dict[str, Any]]: ...

    async def claim_delegation_resume(self, parent_job_id: str) -> bool: ...


class ScholarForge(Protocol):
    @property
    def is_initialized(self) -> bool: ...

    async def create_branch(
        self, repo_name: str, branch_name: str, *, from_branch: str
    ) -> bool: ...


ResumeGuardProvider = Callable[[], dict[str, Any]]


@dataclass(frozen=True)
class ScholarCompletionDependencies:
    store: SubjobCompletionStore
    forge: ScholarForge
    trigger_dispatch: Callable[[], None]
    resolve_workspace_backend: Callable[[dict[str, Any]], str]
    is_lite_config_override: Callable[[dict[str, Any] | None], bool]
    should_provision_parent_container: Callable[[dict[str, Any] | None], bool]
    revalidate_datasource_selection: Callable[
        [dict[str, Any]], Awaitable[tuple[Any, Any]]
    ]
    datasource_selection_provenance: Callable[..., Awaitable[dict[str, Any]]]
    prepare_primary_repository_authority: Callable[
        [dict[str, Any]], Awaitable[dict[str, Any] | None]
    ]
    completion_resume_guard_kwargs: ResumeGuardProvider
    maybe_wake_session: Callable[[str, str], Awaitable[Any]]
    kick_session_wake_drain: Callable[[], None]
    notify_review_returned: Callable[..., Awaitable[Any]]


@dataclass(frozen=True)
class DelegationCompletionDependencies:
    store: SubjobCompletionStore
    trigger_dispatch: Callable[[], None]
    completion_resume_guard_kwargs: ResumeGuardProvider


async def set_target_to_autonomy_status(
    target_job_id: str, *, dependencies: ScholarCompletionDependencies
) -> str:
    """Set a target job's status from its configured autonomy level."""
    from orchestrator.services.completion import get_autonomy_level

    store = dependencies.store
    job = await store.get_job(target_job_id)
    if not job:
        logger.warning("set_target_to_autonomy_status: job %s not found", target_job_id)
        return "unknown"

    autonomy = get_autonomy_level(job)
    if autonomy == "full":
        async with store.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET status = 'completed', completed_at = NOW() WHERE id = $1::uuid",
                target_job_id,
            )
        logger.info("Set target job %s to 'completed' (autonomy=full)", target_job_id)
        new_status = "completed"
    else:
        await store.update_job_status(target_job_id, status="pending_review")
        logger.info(
            "Set target job %s to 'pending_review' (autonomy=%s)",
            target_job_id,
            autonomy,
        )
        new_status = "pending_review"

    await dependencies.maybe_wake_session(target_job_id, new_status)
    dependencies.kick_session_wake_drain()
    return new_status


async def escalate_target(
    job_id: str,
    job: dict[str, Any],
    reason: str,
    *,
    dependencies: ScholarCompletionDependencies,
) -> str:
    """Hand a verification target to a human without approving it."""
    from orchestrator.services.project_loops import job_loop_id
    from orchestrator.services.verification_ledger import escalation_status

    is_loop_job = bool(job_loop_id(job))
    status = escalation_status(is_loop_job=is_loop_job)
    await dependencies.store.update_job_status(
        job_id, status=status, error_message=reason
    )
    logger.warning("Verification escalated target %s to %s: %s", job_id, status, reason)

    try:
        await dependencies.maybe_wake_session(job_id, status)
        dependencies.kick_session_wake_drain()
    except Exception:
        logger.exception(
            "Session wake for escalated target %s failed (non-fatal)", job_id
        )

    user_id = job.get("user_id")
    if not is_loop_job and user_id:
        try:
            await dependencies.notify_review_returned(
                user_id=str(user_id),
                job_id=job_id,
                config_name=job.get("config_name") or "",
                reason=reason,
            )
        except Exception:
            logger.exception(
                "Failed to notify owner of escalated target %s (non-fatal)", job_id
            )

    return status


async def spawn_scholar_subjob(
    job: dict[str, Any],
    config_name: str,
    config_override: dict[str, Any] | None,
    context: dict[str, Any] | None,
    *,
    dependencies: ScholarCompletionDependencies,
) -> dict[str, Any] | None:
    """Spawn the optional scholar research job before its parent starts."""
    from orchestrator.services.completion import (
        format_scholar_instructions,
        resolve_scholar_config_from_disk,
    )

    store = dependencies.store
    forge = dependencies.forge
    job_id = str(job["id"])

    if job.get("parent_job_id"):
        return None
    if dependencies.is_lite_config_override(config_override):
        logger.info(
            "Scholar skipped for job %s: lite workspace backend has no git "
            "workspace for the research-phase graft handoff",
            job_id,
        )
        return None

    scholar_config = resolve_scholar_config_from_disk(config_name, config_override)
    if not scholar_config.get("enabled", False):
        logger.debug("Scholar not enabled for job %s (config=%s)", job_id, config_name)
        return None

    scholar_config_name = scholar_config.get("scholar_config", "scholar")
    description = job.get("description", "")
    parent_instructions = (context or {}).get("instructions")
    instructions = format_scholar_instructions(
        parent_job_id=job_id,
        description=description,
        config_name=config_name,
        instructions=parent_instructions,
    )
    scholar_description = f"Research phase for: {description[:200]}"
    scholar_context: dict[str, Any] = {
        "scholar_target": job_id,
        "original_description": description,
        "instructions": instructions,
    }
    if parent_instructions:
        scholar_context["parent_instructions"] = parent_instructions

    parent_ctx = job.get("context") or {}
    if isinstance(parent_ctx, str):
        try:
            parent_ctx = json.loads(parent_ctx)
        except (json.JSONDecodeError, ValueError):
            parent_ctx = {}
    parent_workspace_backend = dependencies.resolve_workspace_backend(job)
    if parent_workspace_backend == "vm" and parent_ctx.get("vm"):
        scholar_context["inherits_parent_workspace"] = True
    elif parent_workspace_backend == "sandbox" and parent_ctx.get(
        "workspace_container"
    ):
        scholar_context["inherits_parent_workspace"] = True
    elif dependencies.should_provision_parent_container(config_override):
        scholar_context["provisions_parent_workspace"] = job_id

    # A sub-job never inherits its parent's mode: the research pass is the
    # phased procedure even when the parent job or a default runs agent mode.
    scholar_override: dict[str, Any] = {
        "scholar": {"enabled": False},
        "verification": {"enabled": False},
        "curator": {"enabled": False},
        "autonomy": "full",
        JOB_MODE_KEY: JOB_MODE_WORKER,
        "workspace": {"backend": parent_workspace_backend},
    }
    if config_override and isinstance(config_override.get("llm"), dict):
        scholar_override["llm"] = config_override["llm"]

    project_id = str(job["project_id"]) if job.get("project_id") else None
    logger.info(
        "Creating scholar subjob for job %s (scholar_config=%s)",
        job_id,
        scholar_config_name,
    )

    (
        scholar_datasource_ids,
        scholar_datasource_revisions,
    ) = await dependencies.revalidate_datasource_selection(job)
    scholar_owner_id = str(job["user_id"]) if job.get("user_id") else None
    scholar_actor = await store.get_user(scholar_owner_id) if scholar_owner_id else None
    scholar_datasource_provenance = await dependencies.datasource_selection_provenance(
        datasource_ids=scholar_datasource_ids,
        policy_revisions=scholar_datasource_revisions,
        origin="inherited",
        effective_work_owner_id=scholar_owner_id,
        actor=scholar_actor,
        project_ids=[project_id] if project_id else [],
        creation_path="scholar_lifecycle",
    )

    await store.update_job_status(job_id, status="waiting")
    try:
        scholar_job = await store.create_job(
            origin="subjob",
            description=scholar_description,
            config_name=scholar_config_name,
            config_override=scholar_override,
            context=scholar_context,
            parent_job_id=job_id,
            project_id=project_id,
            priority=10,
            user_id=str(job["user_id"]) if job.get("user_id") else None,
            runner_kind="lifecycle",
            datasource_ids=scholar_datasource_ids,
            datasource_selection_provenance=scholar_datasource_provenance,
            datasource_policy_revisions=scholar_datasource_revisions,
            authority_user_id=scholar_owner_id,
            authority_project_ids=(
                [project_id] if scholar_owner_id and project_id else []
            ),
            requested_workspace_backend=None,
            workspace_assignment_source="parent_inheritance",
        )
    except Exception:
        logger.exception(
            "Scholar materialization failed for parent %s; releasing hold", job_id
        )
        try:
            await store.merge_job_context(job_id, {"scholar_failed": True})
        except Exception:
            logger.exception(
                "Failed to record scholar materialization failure for parent %s", job_id
            )
        try:
            await store.update_job_status(job_id, status="created")
        except Exception:
            logger.exception("Failed to release scholar hold for parent %s", job_id)
        dependencies.trigger_dispatch()
        raise

    scholar_job_id = str(scholar_job["id"])
    short_id = scholar_job_id[:8]
    if forge.is_initialized:
        from_branch = job.get("branch_name") or "main"
        branch_name = f"subjob/{short_id}/{scholar_config_name}"
        try:
            parent_authority = await dependencies.prepare_primary_repository_authority(
                job
            )
            if parent_authority is None:
                raise RuntimeError("Parent repository authority is unavailable")
            parent_repo_name = str(parent_authority["repo_name"])
            branch_ok = await forge.create_branch(
                parent_repo_name, branch_name, from_branch=from_branch
            )
            if not branch_ok:
                logger.error(
                    "Failed to create branch %r from %r in %r for scholar %s",
                    branch_name,
                    from_branch,
                    parent_repo_name,
                    scholar_job_id,
                )
            if not await store.bind_job_managed_repository(
                scholar_job_id,
                repo_name=parent_repo_name,
                clean_url=str(parent_authority["clean_repo_url"]),
            ):
                raise RuntimeError("Scholar repository binding was refused")

            worktree_path = None
            if scholar_context.get("inherits_parent_workspace"):
                worktree_path = (
                    f"/home/agent-host/workspace/worktrees/"
                    f"{short_id}-{scholar_config_name}"
                )
            async with store.acquire() as conn:
                await conn.execute(
                    "UPDATE jobs SET branch_name = $1, worktree_path = $2 "
                    "WHERE id = $3::uuid",
                    branch_name,
                    worktree_path,
                    scholar_job_id,
                )
        except Exception as exc:
            logger.warning(
                "Failed to create Gitea branch for scholar %s: %s",
                scholar_job_id,
                exc,
            )

    dependencies.trigger_dispatch()
    logger.info("Scholar job %s created for parent %s", scholar_job_id, job_id)
    return scholar_job


async def handle_scholar_completion(
    job: dict[str, Any],
    actions: list[str],
    *,
    dependencies: ScholarCompletionDependencies,
) -> None:
    """Unblock a scholar's parent after a terminal scholar result."""
    parent_job_id = job.get("parent_job_id")
    if parent_job_id is None:
        return

    ctx_raw = job.get("context")
    if isinstance(ctx_raw, str):
        try:
            ctx = json.loads(ctx_raw)
        except (json.JSONDecodeError, ValueError):
            ctx = {}
    else:
        ctx = ctx_raw or {}
    if not ctx.get("scholar_target"):
        return

    store = dependencies.store
    job_id = str(job["id"])
    target_id = str(parent_job_id)
    job_status = job.get("status", "")
    if job_status not in ("completed", "failed", "cancelled", "pending_review"):
        logger.debug(
            "Scholar %s reported non-terminal status %r — parent %s keeps waiting",
            job_id,
            job_status,
            target_id,
        )
        return

    is_failure = job_status in ("failed", "cancelled")
    parent = await store.get_job(target_id)
    if not parent:
        logger.warning("Scholar %s parent %s not found", job_id, target_id)
        return
    if parent.get("status") != "waiting":
        logger.debug(
            "Scholar %s parent %s not in 'waiting' (status=%s) — skipping unblock",
            job_id,
            target_id,
            parent.get("status"),
        )
        return

    if is_failure:
        ctx_delta: dict[str, Any] = {"scholar_failed": True}
        logger.warning(
            "Scholar %s %s — unblocking parent %s without research",
            job_id,
            job_status,
            target_id,
        )
        actions.append(
            f"scholar {job_id} {job_status}, parent {target_id} unblocked (no research)"
        )
    else:
        fresh = await store.get_job(job_id)
        fresh_ctx = (fresh or {}).get("context") or {}
        if isinstance(fresh_ctx, str):
            try:
                fresh_ctx = json.loads(fresh_ctx)
            except (json.JSONDecodeError, ValueError):
                fresh_ctx = {}
        ctx_delta = {
            "scholar_completed": True,
            "scholar_output_dir": (fresh_ctx or {}).get("graft_output_path"),
        }
        logger.info("Scholar %s completed — unblocking parent %s", job_id, target_id)
        actions.append(f"scholar {job_id} completed, parent {target_id} unblocked")

    if parent.get("execution_lane") == "stateless":
        resumed = await store.queue_stateless_job_for_resume(
            target_id,
            ctx_delta,
            priority=int(parent.get("priority") or 0),
            fair_key=(str(parent["user_id"]) if parent.get("user_id") else None),
            expected_status="waiting",
            **dependencies.completion_resume_guard_kwargs(),
        )
        if not resumed:
            logger.debug(
                "Scholar %s parent %s changed before stateless unblock",
                job_id,
                target_id,
            )
            return
    else:
        await store.merge_job_context(target_id, ctx_delta)
        await store.update_job_status(target_id, status="created", assigned_agent_id="")
    dependencies.trigger_dispatch()


async def handle_delegation_child_completion(
    job: dict[str, Any],
    actions: list[str],
    *,
    dependencies: DelegationCompletionDependencies,
) -> None:
    """Resume a compatibility delegation parent after all children settle."""
    parent_job_id = job.get("parent_job_id")
    if parent_job_id is None or job.get("creation_order") is None:
        return

    store = dependencies.store
    job_id = str(job["id"])
    target_id = str(parent_job_id)
    if not await store.all_delegation_children_terminal(target_id):
        logger.debug(
            "Delegation child %s done, but not all siblings terminal yet (parent %s)",
            job_id,
            target_id,
        )
        return

    parent = await store.get_job(target_id)
    if not parent:
        logger.warning("Delegation child %s: parent %s not found", job_id, target_id)
        return
    if parent.get("status") != "waiting":
        logger.debug(
            "Delegation child %s: parent %s not in 'waiting' (status=%s) — "
            "skipping unblock",
            job_id,
            target_id,
            parent.get("status"),
        )
        return

    children = await store.get_delegation_children(target_id)
    child_results = []
    for child in children:
        child_id = str(child["id"])
        child_status = child.get("status", "unknown")
        freeze = child.get("freeze_data")
        if isinstance(freeze, str):
            try:
                freeze = json.loads(freeze)
            except (json.JSONDecodeError, ValueError):
                freeze = {}
        freeze = freeze or {}
        child_ctx = child.get("context") or {}
        if isinstance(child_ctx, str):
            try:
                child_ctx = json.loads(child_ctx)
            except (json.JSONDecodeError, ValueError):
                child_ctx = {}
        child_results.append(
            {
                "job_id": child_id,
                "description": child.get("description", ""),
                "status": child_status,
                "config_name": canonical_config_name(
                    child.get("config_name") or "worker_base"
                ),
                "output_path": (child_ctx or {}).get("graft_output_path"),
                "creation_order": child.get("creation_order"),
                "branch_name": child.get("branch_name"),
                "worktree_path": child.get("worktree_path"),
                "merge_status": child.get("merge_status"),
                "summary": freeze.get("summary", ""),
                "confidence": freeze.get("confidence", 0.0),
                "deliverables": freeze.get("deliverables", []),
            }
        )

    delegation_context = {"delegation_results": child_results}
    if parent.get("execution_lane") == "stateless":
        resumed = await store.queue_stateless_job_for_resume(
            target_id,
            delegation_context,
            priority=int(parent.get("priority") or 0),
            fair_key=(str(parent["user_id"]) if parent.get("user_id") else None),
            expected_status="waiting",
            **dependencies.completion_resume_guard_kwargs(),
        )
    else:
        await store.merge_job_context(target_id, delegation_context)
        resumed = await store.claim_delegation_resume(target_id)
        if resumed:
            dependencies.trigger_dispatch()
    if not resumed:
        logger.debug(
            "Delegation child %s: parent %s already re-queued by a concurrent "
            "writer — skipping duplicate unblock",
            job_id,
            target_id,
        )
        return

    completed_count = sum(
        1 for child in child_results if child["status"] == "completed"
    )
    total_count = len(child_results)
    logger.info(
        "All %d delegation children done for parent %s (%d completed) — parent "
        "re-queued for resume",
        total_count,
        target_id,
        completed_count,
    )
    actions.append(
        f"delegation: all {total_count} children done, parent {target_id} "
        f"re-queued ({completed_count} completed)"
    )
