"""Application-owned job control and retirement operations.

Authentication belongs to HTTP/caller adapters.  This owner accepts already
authorized job rows and shares the application's single completion-control
boundary, mutation targeting service, dispatch trigger, and lifecycle owners.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol
from uuid import UUID

from fastapi import HTTPException

from shared.operator_pause_hold import operator_pause_hold_present
from orchestrator.services.manifest_runtime_ownership import (
    require_srw_runtime,
    uses_srw_runtime,
)
from orchestrator.services.vm_workspace_policy import vm_needs_release


def _terminal_vm_needs_release(job: dict[str, Any]) -> bool:
    context = job.get("context") or {}
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (TypeError, ValueError):
            return False
    if (
        isinstance(context, dict)
        and job.get("parent_job_id")
        and context.get("inherits_parent_workspace") is True
    ):
        return False
    return bool(
        isinstance(context, dict)
        and vm_needs_release(context.get("vm") if isinstance(context.get("vm"), dict) else None)
    )


def _terminal_vm_cleanup_marked(job: dict[str, Any]) -> bool:
    context = job.get("context") or {}
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (TypeError, ValueError):
            return False
    return bool(
        isinstance(context, dict)
        and (
            context.get("_stateless_cancel_cleanup_pending") is True
            or isinstance(context.get("_job_terminal_vm_cleanup"), dict)
        )
    )


class CompletionControlPort(Protocol):
    async def claim_pause(
        self,
        job_id: str,
        *,
        source: str,
        expected_agent_id: str | None,
        operator_hold: bool = False,
        paused_by: str | None = None,
    ) -> Any: ...

    async def abort(self, claim: Any) -> None: ...

    def active_claim(self, job: dict[str, Any] | None) -> Any: ...

    def claim_detail(self, job: dict[str, Any] | None) -> Any: ...

    def dispatch_guard_kwargs(self) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class JobControlDependencies:
    """Explicit application ports used by destructive job controls."""

    store: Any
    logger: logging.Logger
    completion_commands_enabled: Callable[[], bool]
    completion_control: CompletionControlPort
    manifest_cancel: Callable[[str], Awaitable[bool]]
    prepare_pinned_mutation_target: Callable[..., Awaitable[Any | None]]
    archive_and_cleanup_workspace: Callable[[str], Awaitable[None]]
    http_client_factory: Callable[..., Any]
    handle_scholar_completion: Callable[[dict[str, Any]], Awaitable[None]]
    maybe_wake_session: Callable[[str, str], Awaitable[None]]
    kick_session_wake_drain: Callable[[], None]
    trigger_dispatch: Callable[[], None]
    resolve_job_notifications: Callable[..., Awaitable[None]]
    snapshot_service: Any
    gitea_client: Any
    revoke_and_delete_managed_repository: Callable[..., Awaitable[bool]]
    vector_db: Any


class JobControlOperations:
    """Own cancel, pause, release, and permanent job deletion policy."""

    def __init__(self, dependencies: JobControlDependencies) -> None:
        self.dependencies = dependencies
        self._terminal_vm_cleanup_cursor: str | None = None

    @property
    def commands_enabled(self) -> bool:
        return self.dependencies.completion_commands_enabled()

    async def reconcile_terminal_vm_cleanups(self, *, limit: int = 4) -> int:
        """Replay terminal Job teardown from the durable row after caller loss.

        The existing stateless cleanup marker and advisory lock own worker
        quiescence. Other terminal jobs use that same per-Job session lock to
        keep snapshot and exact admission replay single-flight across replicas.
        """
        d = self.dependencies
        candidates = await d.store.list_terminal_vm_cleanup_jobs(
            limit=limit, after_id=self._terminal_vm_cleanup_cursor,
        )
        if not candidates and self._terminal_vm_cleanup_cursor is not None:
            self._terminal_vm_cleanup_cursor = None
            candidates = await d.store.list_terminal_vm_cleanup_jobs(
                limit=limit, after_id=None,
            )
        if not candidates:
            return 0
        self._terminal_vm_cleanup_cursor = str(candidates[-1]["id"])
        # Distinct rows have distinct per-Job locks. A slow snapshot must not
        # delay another candidate in this bounded batch.
        return sum(await asyncio.gather(*(
            self._reconcile_terminal_vm_cleanup(candidate)
            for candidate in candidates
        )))

    async def _reconcile_terminal_vm_cleanup(self, candidate: dict[str, Any]) -> int:
        d = self.dependencies
        job_id = str(candidate["id"])
        try:
            job = await d.store.get_job(job_id)
            if not job or job.get("status") not in {"completed", "failed", "cancelled"}:
                return 0
            context = job.get("context") or {}
            if isinstance(context, str):
                context = json.loads(context)
            if not isinstance(context, dict):
                return 0
            marker = context.get("_job_terminal_vm_cleanup")
            if marker is not None:
                vm = context.get("vm")
                if (
                    not isinstance(marker, dict)
                    or marker.get("version") != 1
                    or marker.get("source") != (
                        "cancel" if job["status"] == "cancelled" else "approve"
                    )
                    or not isinstance(vm, dict)
                    or marker.get("provision_generation")
                        != vm.get("provision_generation")
                ):
                    d.logger.warning(
                        "Terminal Job VM cleanup marker changed for %s", job_id,
                    )
                    return 0
            if (
                job.get("status") == "cancelled"
                and job.get("execution_lane") == "stateless"
                and context.get("_stateless_cancel_cleanup_pending") is True
            ):
                if not await self.cascade_cancel_to_children(job_id):
                    return 0
                if await asyncio.wait_for(
                    self.wait_for_stateless_cancel_settle(
                        job_id, timeout_seconds=0,
                    ), timeout=900,
                ):
                    return 1
                return 0
            async with d.store.stateless_cancel_cleanup_lock(job_id) as owner:
                if not owner:
                    return 0
                fresh = await d.store.get_job(job_id)
                if not fresh or fresh.get("status") not in {
                    "completed", "failed", "cancelled",
                }:
                    return 0
                fresh_context = fresh.get("context") or {}
                if isinstance(fresh_context, str):
                    fresh_context = json.loads(fresh_context)
                if not isinstance(fresh_context, dict):
                    return 0
                if marker is not None and fresh_context.get(
                    "_job_terminal_vm_cleanup"
                ) != marker:
                    return 0
                if fresh["status"] == "cancelled":
                    if not await self.cascade_cancel_to_children(job_id):
                        return 0
                    if fresh.get("execution_lane") == "pinned":
                        # A terminal fence already detached the old agent.
                        # Checkpoint pruning precedes workspace retirement.
                        await d.store.delete_checkpoint_thread(job_id)
                await asyncio.wait_for(
                    d.archive_and_cleanup_workspace(job_id), timeout=900,
                )
                if marker is not None and not await d.store.complete_terminal_vm_cleanup_marker(
                    job_id, expected_generation=marker["provision_generation"],
                ):
                    return 0
                return 1
        except Exception:
            d.logger.exception("Terminal Job VM cleanup remains pending for %s", job_id)
        return 0

    async def authorize_delete(
        self, caller: dict[str, Any], job: dict[str, Any]
    ) -> None:
        """Enforce the destructive owner/project-owner/admin gate."""
        if caller.get("is_admin"):
            return
        is_job_owner = str(job.get("user_id") or "") == str(caller["id"])
        is_project_owner = False
        if not is_job_owner and job.get("project_id"):
            role = await self.dependencies.store.get_user_role_in_project(
                str(job["project_id"]), str(caller["id"])
            )
            is_project_owner = role == "owner"
        if not (is_job_owner or is_project_owner):
            raise HTTPException(
                status_code=403,
                detail=(
                    "Only the job owner, the project owner, or an admin may delete "
                    "this job"
                ),
            )

    async def delete(
        self,
        job_id: str,
        *,
        caller: dict[str, Any],
        job: dict[str, Any],
    ) -> dict[str, Any]:
        """Permanently delete authorized work after retiring its authorities."""
        d = self.dependencies
        store = d.store
        require_srw_runtime(job)
        await self.authorize_delete(caller, job)
        try:
            if await store.has_child_jobs(job_id):
                raise HTTPException(
                    status_code=409, detail="Job has child jobs; delete them first"
                )

            stateless = job.get("execution_lane") == "stateless"
            if stateless and not await store.prepare_stateless_job_for_delete(job_id):
                raise HTTPException(
                    status_code=409,
                    detail="Stateless job changed while deletion was being prepared",
                )

            try:
                await d.archive_and_cleanup_workspace(job_id)
            except Exception as exc:
                d.logger.warning(
                    "Workspace cleanup is incomplete for job %s: %s", job_id, exc
                )
                raise HTTPException(
                    status_code=503,
                    detail="Workspace authority retirement is incomplete",
                ) from exc

            if d.snapshot_service.is_available:
                try:
                    await d.snapshot_service.delete_snapshot(job_id)
                except Exception as exc:
                    d.logger.warning(
                        "Snapshot cleanup failed for deleted job %s: %s", job_id, exc
                    )
            if d.snapshot_service.is_available:
                try:
                    fresh = await store.get_job(job_id) or {}
                    context = fresh.get("context") or {}
                    if isinstance(context, str):
                        context = json.loads(context)
                    for key in context.get("log_archive_keys") or []:
                        await d.snapshot_service.delete_blob(str(key))
                except Exception as exc:
                    d.logger.warning(
                        "Log archive cleanup failed for deleted job %s: %s",
                        job_id,
                        exc,
                    )

            repo_name = job.get("repo_name")
            branch_name = job.get("branch_name")
            isolated_repo_name = f"job-{str(job.get('id') or job_id)[:8]}"
            if repo_name or branch_name:
                if job.get("parent_job_id") and branch_name and repo_name:
                    if d.gitea_client.is_initialized:
                        await d.gitea_client.delete_branch(repo_name, branch_name)
                elif repo_name == isolated_repo_name:
                    if not await d.revoke_and_delete_managed_repository(
                        store, d.gitea_client, repo_name
                    ):
                        raise HTTPException(
                            status_code=503,
                            detail="Repository credential revocation is retryable",
                        )
                elif job.get("project_id") and branch_name:
                    legacy_repo_name = repo_name
                    if not legacy_repo_name:
                        repos = await store.get_project_repositories(
                            str(job["project_id"]), role="jobs"
                        )
                        legacy_repo_name = repos[0]["name"] if repos else None
                    if legacy_repo_name and branch_name != "main":
                        await d.gitea_client.delete_branch(
                            legacy_repo_name, branch_name
                        )
                elif repo_name and not job.get("project_id"):
                    if not await d.revoke_and_delete_managed_repository(
                        store, d.gitea_client, repo_name
                    ):
                        raise HTTPException(
                            status_code=503,
                            detail="Repository credential revocation is retryable",
                        )

            try:
                async with d.vector_db.acquire() as conn:
                    await conn.execute(
                        "SELECT retire_job_vector_scope($1::uuid)", UUID(job_id)
                    )
            except Exception as exc:
                d.logger.warning(
                    "Vector retirement is incomplete for job %s: %s", job_id, exc
                )
                raise HTTPException(
                    status_code=503,
                    detail="Job vector cleanup is incomplete; retry deletion",
                ) from exc

            delete_kwargs = {
                "deletion_actor_user_id": str(caller["id"]),
                "deletion_reason": "authorized_api_delete",
                "return_claim_state": True,
            }
            if stateless:
                delete_kwargs["prepared_stateless"] = True
            delete_result = await store.delete_job(job_id, **delete_kwargs)
            if isinstance(delete_result, dict):
                success = bool(delete_result.get("deleted"))
                claim_retained = bool(delete_result.get("ticket_claim_retained"))
            else:
                success = bool(delete_result)
                claim_retained = False
            if not success:
                raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
            await d.resolve_job_notifications(job_id, user=caller, hook="delete")
            return {
                "status": "deleted",
                "ticket_claim_retained": claim_retained,
                "ticket_rearmed": False,
                "message": (
                    "Job deleted. Its backlog-ticket claim remains durable; "
                    "deletion does not re-arm the ticket."
                    if claim_retained
                    else "Job deleted. This job had no durable backlog-ticket claim."
                ),
            }
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    async def _signal(
        self,
        job_id: str,
        agent_id: str,
        action: str,
        *,
        reason: str | None = None,
        timeout_seconds: float = 130.0,
    ) -> tuple[bool, int | None]:
        d = self.dependencies
        target = await d.prepare_pinned_mutation_target(
            agent_id=agent_id, job_id=job_id, require_idle=False
        )
        if target is None:
            return False, -1
        payload: dict[str, Any] = {
            "recipient": target.recipient.model_dump(mode="json")
        }
        if reason is not None:
            payload["reason"] = reason
        url = f"http://{target.agent['pod_ip']}:{target.agent['pod_port']}/job/{action}"
        try:
            async with d.http_client_factory(timeout=timeout_seconds) as client:
                response = await client.post(url, json=payload)
            return response.status_code == 200, response.status_code
        except Exception as exc:
            d.logger.warning(
                "Could not reach agent to %s job %s: %s", action, job_id, exc
            )
            return False, None

    async def cascade_cancel_to_children(self, job_id: str) -> bool:
        d = self.dependencies
        children = await d.store.get_descendant_jobs(job_id, include_cancelled=True)
        if not children:
            return True
        native_settled = True
        for child in children:
            if not uses_srw_runtime(child):
                native_settled = (
                    await d.manifest_cancel(str(child["id"])) and native_settled
                )
        children = [child for child in children if uses_srw_runtime(child)]
        if not children:
            return native_settled

        async def cleanup(child: dict[str, Any]) -> bool:
            try:
                await d.archive_and_cleanup_workspace(str(child["id"]))
                return True
            except Exception as exc:
                d.logger.warning(
                    "Workspace cleanup failed for child %s: %s", child["id"], exc
                )
                return False

        async def cancel_pinned(child: dict[str, Any]) -> bool:
            child_id = str(child["id"])
            won = await d.store.linearize_pinned_cancel(
                child_id,
                expected_status=str(child.get("status") or ""),
                completion_commands_enabled=self.commands_enabled,
            )
            if not won:
                refreshed = await d.store.get_job(child_id)
                if not refreshed or refreshed.get("status") in ("completed", "failed"):
                    return True
                if refreshed.get("status") != "cancelled":
                    return False
                return await cleanup(refreshed)
            agent_id = child.get("assigned_agent_id")

            async def signal() -> bool:
                if child.get("status") != "processing":
                    return True
                if not agent_id:
                    return False
                result, _ = await self._signal(
                    child_id,
                    str(agent_id),
                    "cancel",
                    reason=f"Parent job {job_id} cancelled",
                )
                return result

            signal_result, cleanup_result = await asyncio.gather(
                signal(), cleanup(child), return_exceptions=True
            )
            return signal_result is True and cleanup_result is True

        pinned = [c for c in children if c.get("execution_lane") != "stateless"]
        stateless = [c for c in children if c.get("execution_lane") == "stateless"]
        pinned_results = await asyncio.gather(
            *(cancel_pinned(c) for c in pinned), return_exceptions=True
        )
        pinned_settled = all(result is True for result in pinned_results)

        async def cancel_stateless(child: dict[str, Any]) -> bool:
            child_id = str(child["id"])
            cancelled, _ = await d.store.cancel_stateless_job(
                child_id, **d.completion_control.dispatch_guard_kwargs()
            )
            if cancelled:
                return await self.wait_for_stateless_cancel_settle(child_id)
            refreshed = await d.store.get_job(child_id)
            if not refreshed or refreshed.get("status") in ("completed", "failed"):
                return True
            if refreshed.get("status") != "cancelled":
                return False
            context = refreshed.get("context") or {}
            if isinstance(context, str):
                try:
                    context = json.loads(context)
                except (TypeError, ValueError):
                    context = {}
            if context.get("_stateless_cancel_cleanup_pending") is True:
                return await self.wait_for_stateless_cancel_settle(child_id)
            return True

        stateless_results = await asyncio.gather(
            *(cancel_stateless(c) for c in stateless), return_exceptions=True
        )
        stateless_settled = all(result is True for result in stateless_results)
        d.logger.info(
            "Cascade-cancelled %s descendant(s) of job %s", len(children), job_id
        )
        return pinned_settled and stateless_settled and native_settled

    async def cascade_pause_to_children(self, job_id: str) -> None:
        d = self.dependencies
        children = await d.store.get_descendant_jobs(job_id)
        processing = [
            child
            for child in children
            if child["status"] == "processing" and uses_srw_runtime(child)
        ]
        pinned = [c for c in processing if c.get("execution_lane") != "stateless"]
        stateless = [c for c in processing if c.get("execution_lane") == "stateless"]

        async def signal_pause(
            child: dict[str, Any], *, require_positive_quiescence: bool
        ) -> bool:
            agent_id = child.get("assigned_agent_id")
            if not agent_id:
                return not require_positive_quiescence
            acknowledged, status = await self._signal(
                str(child["id"]), str(agent_id), "pause"
            )
            if status == -1:
                return False
            if not acknowledged and require_positive_quiescence:
                return False
            if not self.commands_enabled:
                await d.store.pause_job(str(child["id"]))
            return True

        if self.commands_enabled:

            async def claim_and_signal(child: dict[str, Any]) -> None:
                child_id = str(child["id"])
                try:
                    claim = await d.completion_control.claim_pause(
                        child_id,
                        source="cascade_pause",
                        expected_agent_id=(
                            str(child["assigned_agent_id"])
                            if child.get("assigned_agent_id")
                            else None
                        ),
                    )
                except HTTPException:
                    return
                if await signal_pause(child, require_positive_quiescence=True):
                    await d.completion_control.abort(claim)

            await asyncio.gather(
                *(claim_and_signal(child) for child in pinned),
                return_exceptions=True,
            )
        else:
            await asyncio.gather(
                *(
                    signal_pause(child, require_positive_quiescence=False)
                    for child in pinned
                ),
                return_exceptions=True,
            )
        for child in stateless:
            await d.store.pause_stateless_job(
                str(child["id"]), **d.completion_control.dispatch_guard_kwargs()
            )
        if processing:
            d.logger.info(
                "Cascade-paused %s processing descendant(s) of job %s",
                len(processing),
                job_id,
            )

    async def wait_for_stateless_cancel_settle(
        self,
        job_id: str,
        *,
        timeout_seconds: float = 130.0,
        poll_seconds: float = 0.5,
    ) -> bool:
        d = self.dependencies
        deadline = asyncio.get_running_loop().time() + max(0.0, timeout_seconds)
        while True:
            try:
                async with d.store.stateless_cancel_cleanup_lock(job_id) as owner:
                    settled = False
                    if owner:
                        pending = await d.store.stateless_cancel_cleanup_pending(job_id)
                        if pending is False:
                            return True
                        if pending is not None:
                            settled = await d.store.finalize_cancelled_stateless_job(
                                job_id
                            )
                        if settled:
                            try:
                                await d.archive_and_cleanup_workspace(job_id)
                            except Exception as exc:
                                d.logger.warning(
                                    "Workspace cleanup failed for cancelled stateless job %s: %s",
                                    job_id,
                                    exc,
                                )
                                settled = False
                            else:
                                if await d.store.complete_stateless_cancel_cleanup(
                                    job_id
                                ):
                                    return True
                                settled = False
            except Exception:
                d.logger.warning(
                    "Stateless cancel settle check failed for job %s; retrying",
                    job_id,
                    exc_info=True,
                )
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(max(0.01, poll_seconds), remaining))

    async def cancel(
        self, job_id: str, *, job: dict[str, Any], expected_execution_deadline=None
    ) -> dict[str, str]:
        """Cancel already-authorized work through runtime and cleanup fences."""
        d = self.dependencies
        deadline_guard = (
            {"expected_execution_deadline": expected_execution_deadline}
            if expected_execution_deadline is not None
            else {}
        )
        if deadline_guard and job.get("execution_lane") not in {"pinned", "stateless"}:
            return {"status": "unchanged"}
        if not deadline_guard and getattr(d.store, "manifests_ready", False) is True:
            if await d.manifest_cancel(job_id):
                return {"status": "cancelled"}
        pinned_cancel_committed = False
        try:
            if job.get("execution_lane") == "stateless":
                success, _ = await d.store.cancel_stateless_job(
                    job_id,
                    **d.completion_control.dispatch_guard_kwargs(),
                    **deadline_guard,
                )
                if not success:
                    if deadline_guard:
                        return {"status": "unchanged"}
                    refreshed = await d.store.get_job(job_id)
                    if refreshed and refreshed.get("execution_lane") == "pinned":
                        job = refreshed
                    elif (
                        refreshed
                        and (refreshed.get("context") or {}).get(
                            "_stateless_delete_pending"
                        )
                        is True
                    ):
                        raise HTTPException(
                            status_code=409,
                            detail="Job deletion is already in progress",
                        )
                    elif (
                        refreshed
                        and refreshed.get("status") not in ("completed", "cancelled")
                        and d.completion_control.active_claim(refreshed)
                    ):
                        raise HTTPException(
                            status_code=409,
                            detail=d.completion_control.claim_detail(refreshed),
                        )
                    elif not refreshed or refreshed.get("status") != "cancelled":
                        raise HTTPException(
                            status_code=400,
                            detail="Job cannot be cancelled (already completed or cancelled)",
                        )
                    else:
                        job = refreshed
                if job.get("execution_lane") == "stateless":
                    if _terminal_vm_needs_release(job) or _terminal_vm_cleanup_marked(job):
                        job["status"] = "cancelled"
                        await self._finish_cancel(job_id, job)
                        return {"status": "cancelled", "cleanup_pending": True}
                    if not await self.cascade_cancel_to_children(job_id):
                        raise HTTPException(
                            status_code=503,
                            detail="Descendant workspace authority retirement is incomplete",
                        )
                    if not await self.wait_for_stateless_cancel_settle(job_id):
                        d.logger.error(
                            "Stateless cancel cleanup timed out for job %s", job_id
                        )
                    job["status"] = "cancelled"
                    await self._finish_cancel(job_id, job)
                    return {"status": "cancelled"}

            if job.get("execution_lane") == "pinned":
                pinned_cancel_committed = await d.store.linearize_pinned_cancel(
                    job_id,
                    expected_status=str(job.get("status") or ""),
                    completion_commands_enabled=self.commands_enabled,
                    **deadline_guard,
                )
                if not pinned_cancel_committed:
                    if deadline_guard:
                        return {"status": "unchanged"}
                    refreshed = await d.store.get_job(job_id)
                    if (
                        self.commands_enabled
                        and refreshed
                        and refreshed.get("status") not in ("completed", "cancelled")
                        and d.completion_control.active_claim(refreshed)
                    ):
                        raise HTTPException(
                            status_code=409,
                            detail=d.completion_control.claim_detail(refreshed),
                        )
                    if not refreshed or refreshed.get("status") != "cancelled":
                        raise HTTPException(
                            status_code=400,
                            detail="Job cannot be cancelled (already completed or cancelled)",
                        )
                    pinned_cancel_committed = True
                    job = refreshed

            if _terminal_vm_needs_release(job) or _terminal_vm_cleanup_marked(job):
                assigned_agent_id = job.get("assigned_agent_id")
                if assigned_agent_id:
                    await self._signal(
                        job_id, str(assigned_agent_id), "cancel",
                        reason="Cancelled via cockpit", timeout_seconds=5,
                    )
                job["status"] = "cancelled"
                await self._finish_cancel(job_id, job)
                return {"status": "cancelled", "cleanup_pending": True}

            assigned_agent_id = job.get("assigned_agent_id")
            if assigned_agent_id:
                acknowledged, status = await self._signal(
                    job_id,
                    str(assigned_agent_id),
                    "cancel",
                    reason="Cancelled via cockpit",
                )
                if acknowledged:
                    d.logger.info("Agent confirmed cancel for job %s", job_id)
                elif status == 408:
                    d.logger.warning("Agent cancel timed out for job %s", job_id)

            if not await self.cascade_cancel_to_children(job_id):
                raise HTTPException(
                    status_code=503,
                    detail="Descendant workspace authority retirement is incomplete",
                )
            try:
                await d.archive_and_cleanup_workspace(job_id)
            except Exception as exc:
                d.logger.warning(
                    "Workspace cleanup remains pending for cancelled job %s: %s",
                    job_id,
                    exc,
                )
                raise HTTPException(
                    status_code=503,
                    detail="Workspace authority retirement is incomplete",
                ) from exc

            success = await d.store.cancel_job(job_id)
            if not success:
                refreshed = await d.store.get_job(job_id)
                if not refreshed or refreshed.get("status") != "cancelled":
                    raise HTTPException(
                        status_code=400,
                        detail="Job cannot be cancelled (already completed or cancelled)",
                    )
            if pinned_cancel_committed:
                try:
                    await d.store.delete_checkpoint_thread(job_id)
                except Exception as exc:
                    d.logger.debug("checkpoint prune skipped for %s: %s", job_id, exc)
            job["status"] = "cancelled"
            await self._finish_cancel(job_id, job)
            return {"status": "cancelled"}
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    async def _finish_cancel(self, job_id: str, job: dict[str, Any]) -> None:
        d = self.dependencies
        try:
            await d.handle_scholar_completion(job)
        except Exception as exc:
            d.logger.warning(
                "Error handling scholar cancellation for %s: %s", job_id, exc
            )
        await d.maybe_wake_session(job_id, "cancelled")
        d.kick_session_wake_drain()
        d.trigger_dispatch()
        await d.resolve_job_notifications(job_id, user=None, hook="cancel")

    async def pause(
        self,
        job_id: str,
        *,
        job: dict[str, Any],
        paused_by: str | None = None,
    ) -> dict[str, str]:
        """Pause already-authorized work, retaining ambiguous control holds.

        Both lanes also write the durable operator pause hold, so the parked
        row is not redispatched or re-admitted until an explicit resume lifts
        it. Cascaded children and system pauses never write one.
        """
        d = self.dependencies
        require_srw_runtime(job)
        try:
            if job["status"] == "paused" and not operator_pause_hold_present(
                job.get("context")
            ):
                # A system pause (agent release, lease recovery, preemption)
                # beat this operator pause to the row; it is still the
                # operator's pause, so hold the job where it is instead of
                # refusing and letting it redispatch.
                if await d.store.hold_paused_job(job_id, paused_by=paused_by):
                    d.logger.info(
                        "Operator pause hold set on already-paused job %s "
                        "(paused_by=%s)",
                        job_id,
                        paused_by or "internal",
                    )
                    await self.cascade_pause_to_children(job_id)
                    return {"status": "paused", "job_id": job_id}
                job = await d.store.get_job(job_id) or job
            if job["status"] != "processing":
                raise HTTPException(
                    status_code=400,
                    detail=f"Job cannot be paused (status: {job['status']})",
                )
            if job.get("execution_lane") == "stateless":
                success = await d.store.pause_stateless_job(
                    job_id,
                    operator_hold=True,
                    paused_by=paused_by,
                    **d.completion_control.dispatch_guard_kwargs(),
                )
                if not success:
                    refreshed = await d.store.get_job(job_id)
                    if d.completion_control.active_claim(refreshed):
                        raise HTTPException(
                            status_code=409,
                            detail=d.completion_control.claim_detail(refreshed),
                        )
                    raise HTTPException(
                        status_code=400,
                        detail="Job cannot be paused (status may have changed)",
                    )
                await d.maybe_wake_session(job_id, "paused")
                await self.cascade_pause_to_children(job_id)
                return {"status": "paused", "job_id": job_id}

            claim = (
                await d.completion_control.claim_pause(
                    job_id,
                    source="public_pause",
                    expected_agent_id=(
                        str(job["assigned_agent_id"])
                        if job.get("assigned_agent_id")
                        else None
                    ),
                    operator_hold=True,
                    paused_by=paused_by,
                )
                if self.commands_enabled
                else None
            )
            quiescent = not job.get("assigned_agent_id")
            try:
                if job.get("assigned_agent_id"):
                    quiescent, status = await self._signal(
                        job_id, str(job["assigned_agent_id"]), "pause"
                    )
                    if status == 408:
                        d.logger.warning(
                            "Pause timed out for job %s; retaining control hold", job_id
                        )
                if claim is None and not await d.store.pause_job(
                    job_id, operator_hold=True, paused_by=paused_by
                ):
                    raise HTTPException(
                        status_code=400,
                        detail="Job cannot be paused (status may have changed)",
                    )
                d.logger.info(
                    "Operator pause hold set for job %s (paused_by=%s); held until "
                    "an explicit resume",
                    job_id,
                    paused_by or "internal",
                )
            finally:
                if claim is not None and quiescent:
                    await d.completion_control.abort(claim)
                elif claim is not None:
                    d.logger.warning(
                        "Pause control hold retained for job %s after ambiguous quiescence",
                        job_id,
                    )
            await d.maybe_wake_session(job_id, "paused")
            await self.cascade_pause_to_children(job_id)
            return {"status": "paused", "job_id": job_id}
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    async def release(
        self,
        job_id: str,
        *,
        agent_id: str | None = None,
        lease_token: int | None = None,
    ) -> dict[str, str]:
        """Release work for an already-authenticated internal agent."""
        d = self.dependencies
        try:
            job = await d.store.get_job(job_id)
            if job is not None:
                require_srw_runtime(job)
            lease_recovery_pending = False
            if not job:
                success = False
            elif (
                job.get("execution_lane", "pinned") == "pinned"
                and job.get("lease_expires_at") is not None
            ):
                if self.commands_enabled and agent_id is None:
                    raise HTTPException(
                        status_code=409,
                        detail="Agent release does not identify the assigned agent",
                    )
                success = await d.store.route_pinned_agent_release_to_lease_recovery(
                    job_id,
                    completion_commands_enabled=self.commands_enabled,
                    expected_agent_id=agent_id,
                )
                lease_recovery_pending = success
            elif not self.commands_enabled:
                success = await d.store.pause_job(job_id)
            elif job.get("execution_lane") == "stateless":
                if lease_token is None:
                    raise HTTPException(
                        status_code=409,
                        detail="Agent release does not hold the worker lease",
                    )
                success = await d.store.pause_stateless_job(
                    job_id,
                    completion_commands_enabled=True,
                    expected_lease_token=lease_token,
                )
            else:
                if agent_id is None:
                    raise HTTPException(
                        status_code=409,
                        detail="Agent release does not identify the assigned agent",
                    )
                success = await d.store.pause_job(
                    job_id,
                    completion_commands_enabled=True,
                    expected_agent_id=agent_id,
                )
            if not success:
                refreshed = (
                    await d.store.get_job(job_id) if self.commands_enabled else None
                )
                if d.completion_control.active_claim(refreshed):
                    raise HTTPException(
                        status_code=409,
                        detail=d.completion_control.claim_detail(refreshed),
                    )
                if (
                    self.commands_enabled
                    and refreshed
                    and refreshed.get("status") == "processing"
                ):
                    raise HTTPException(
                        status_code=409,
                        detail="Job ownership changed before agent release",
                    )
                raise HTTPException(
                    status_code=400,
                    detail="Job cannot be paused (not found or status changed)",
                )
            if lease_recovery_pending:
                return {"status": "lease_recovery_pending", "job_id": job_id}
            d.trigger_dispatch()
            return {"status": "paused", "job_id": job_id}
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
