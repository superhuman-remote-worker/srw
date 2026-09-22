"""Administrative job-assignment policy without HTTP/request ownership."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Protocol

from fastapi import HTTPException

from orchestrator.services.dispatch_guards import resume_lane_applies
from orchestrator.services.job_workspace_runtime import WORKSPACE_CONTEXT_KEYS
from orchestrator.services.manifest_runtime_ownership import require_srw_runtime
from shared.operator_pause_hold import operator_pause_lift_token
from shared.workspace_contract import resolve_workspace_runtime


class JobAssignmentStore(Protocol):
    async def get_job(self, job_id: str) -> dict[str, Any] | None: ...

    async def get_agent(self, agent_id: str) -> dict[str, Any] | None: ...

    async def job_has_checkpoint(self, job_id: str) -> bool: ...

    async def claim_job_for_agent(
        self, job_id: str, agent_id: str, **kwargs: Any
    ) -> Any: ...

    async def shed_workspace_context(self, job_id: str, context_key: str) -> Any: ...

    async def queue_job_for_resume(self, job_id: str, **kwargs: Any) -> Any: ...

    async def prepare_pinned_job_for_workspace_resume(
        self, job_id: str, context_key: str, **kwargs: Any
    ) -> Any: ...


@dataclass(frozen=True, slots=True)
class JobAssignmentDependencies:
    """Application-owned ports for the manual scheduling override."""

    store: JobAssignmentStore
    logger: logging.Logger
    # HTTP-only adapter port retained here until the application factory is
    # migrated; the operation deliberately never calls it.
    require_admin: Callable[[Any], Awaitable[Any]]
    vm_mode: Callable[[], Any]
    completion_commands_enabled: Callable[[], bool]
    prepare_job_workspace_runtime: Callable[
        [dict[str, Any]], Awaitable[tuple[str, dict[str, Any], str | None]]
    ]
    prepare_job_repository_before_claim: Callable[[dict[str, Any]], Awaitable[bool]]
    resume_missing_workspace: Callable[[dict[str, Any]], str | None]
    guard_completion_control: Callable[..., Awaitable[None]]
    claim_completion_control: Callable[..., Awaitable[Any]]
    abort_completion_control_claim: Callable[[Any], Awaitable[None]]
    completion_resume_guard_kwargs: Callable[[], dict[str, Any]]
    dispatch_job_to_agent: Callable[[dict, dict], Awaitable[bool]]
    resume_job_on_agent: Callable[[dict, dict], Awaitable[bool]]
    trigger_dispatch: Callable[[], None]


class JobAssignmentOperations:
    """Own manual assignment after an adapter has authenticated an admin."""

    def __init__(self, dependencies: JobAssignmentDependencies) -> None:
        self.dependencies = dependencies

    async def assign(self, job_id: str, agent_id: str) -> dict[str, str]:
        dependencies = self.dependencies
        store = dependencies.store
        logger = dependencies.logger
        try:
            job = await store.get_job(job_id)
            if not job:
                raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
            require_srw_runtime(job)

            if job.get("execution_lane", "pinned") != "pinned":
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Stateless jobs are claimed from the run queue and cannot "
                        "be assigned directly to a registered agent"
                    ),
                )
            if job["status"] not in ("created", "failed", "paused"):
                raise HTTPException(
                    status_code=400,
                    detail=f"Job cannot be assigned (status: {job['status']})",
                )

            await dependencies.guard_completion_control(job_id, source="manual_assign")
            # An admin assignment is an explicit resume: it may lift the
            # operator pause hold it observed here, and only that one.
            operator_pause_lift = operator_pause_lift_token(job)
            (
                workspace_action,
                job,
                workspace_reason,
            ) = await dependencies.prepare_job_workspace_runtime(job)
            if workspace_action != "proceed":
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "workspace_runtime_adoption_pending",
                        "message": (
                            "Live Kubernetes workspace authority is not yet available; "
                            "no agent was reserved"
                        ),
                        "retryable": workspace_action == "wait",
                        "failure": workspace_reason,
                    },
                )

            workspace_decision = resolve_workspace_runtime(
                job, vm_mode=dependencies.vm_mode()
            )
            if (
                workspace_decision.contract is None
                or workspace_decision.state == "invalid"
            ):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "workspace_contract_invalid",
                        "message": (
                            "Job workspace authority is ambiguous; no agent was reserved"
                        ),
                        "state": workspace_decision.state,
                        "failure": workspace_decision.reason,
                    },
                )

            missing_workspace = dependencies.resume_missing_workspace(job)
            if missing_workspace:
                if dependencies.completion_commands_enabled():
                    control_claim = await dependencies.claim_completion_control(
                        {**job, "id": job_id}, source="manual_assign_workspace"
                    )
                    try:
                        queued = await store.prepare_pinned_job_for_workspace_resume(
                            job_id,
                            WORKSPACE_CONTEXT_KEYS[missing_workspace],
                            expected_status=str(job["status"]),
                            completion_control_claim_id=str(control_claim.claim_id),
                            lift_operator_pause_hold=operator_pause_lift,
                        )
                    except Exception:
                        await dependencies.abort_completion_control_claim(control_claim)
                        raise
                    if not queued:
                        await dependencies.abort_completion_control_claim(control_claim)
                        raise HTTPException(
                            status_code=409,
                            detail=(
                                "Job changed while it was being queued for provisioning"
                            ),
                        )
                else:
                    await store.shed_workspace_context(
                        job_id, WORKSPACE_CONTEXT_KEYS[missing_workspace]
                    )
                if (
                    job["status"] != "created"
                    and not dependencies.completion_commands_enabled()
                ):
                    queued = await store.queue_job_for_resume(
                        job_id,
                        lift_operator_pause_hold=operator_pause_lift,
                        **dependencies.completion_resume_guard_kwargs(),
                    )
                    if not queued:
                        raise HTTPException(
                            status_code=409,
                            detail=(
                                "Job changed while it was being queued for provisioning"
                            ),
                        )
                dependencies.trigger_dispatch()
                return {
                    "status": "queued",
                    "job_id": job_id,
                    "message": (
                        f"No live {missing_workspace} workspace; queued for automatic "
                        "provisioning and assignment. The requested agent was not reserved."
                    ),
                }

            agent = await store.get_agent(agent_id)
            if not agent:
                raise HTTPException(
                    status_code=404, detail=f"Agent '{agent_id}' not found"
                )
            if agent["status"] != "ready":
                raise HTTPException(
                    status_code=400,
                    detail=f"Agent is not ready (status: {agent['status']})",
                )
            if not agent.get("pod_ip"):
                raise HTTPException(
                    status_code=400, detail="Agent has no pod IP configured"
                )

            if not await dependencies.prepare_job_repository_before_claim(job):
                raise HTTPException(
                    status_code=409, detail="Job repository authority is not ready"
                )
            if not await store.claim_job_for_agent(
                job_id,
                agent_id,
                completion_commands_enabled=dependencies.completion_commands_enabled(),
                allow_failed=True,
                lift_operator_pause_hold=operator_pause_lift,
            ):
                raise HTTPException(
                    status_code=409, detail="Job changed while it was being assigned"
                )

            if resume_lane_applies(
                job, has_checkpoint=await store.job_has_checkpoint(job_id)
            ):
                success = await dependencies.resume_job_on_agent(job, agent)
            else:
                if job["status"] == "paused":
                    logger.info(
                        "Assign: job %s is paused with no checkpoint to resume from "
                        "(never started, or pruned at a terminal state) — dispatching "
                        "via the fresh /job/start lane",
                        job_id,
                    )
                success = await dependencies.dispatch_job_to_agent(job, agent)
            if not success:
                raise HTTPException(
                    status_code=502, detail="Failed to dispatch job to agent"
                )

            return {"status": "assigned", "agent_id": agent_id, "job_id": job_id}
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
