"""Application-owned job, VM, and sudo control operations.

HTTP authentication and route registration live in ``orchestrator.routers``.
The operation owner receives every application collaborator explicitly and
shares the app-owned completion-control boundary with job completion.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import httpx
from fastapi import HTTPException, Request

from orchestrator.schemas.job_controls import (
    JobApproveRequest,
    JobResumeRequest,
    SudoRuleCreateRequest,
    WorkspaceRecoveryRetryRequest,
)
from orchestrator.schemas.workspaces import VMCreateRequest
from orchestrator.services.grant_enforcement import GrantDenied
from orchestrator.services.manifest_runtime_ownership import (
    require_srw_runtime,
    uses_srw_runtime,
)
from shared.operator_pause_hold import (
    operator_pause_lift_already_consumed,
    operator_pause_lift_token,
)
from orchestrator.services.vm_workspace_recovery_store import (
    acquire_vm_cleanup_permit,
    vm_cleanup_kwargs,
    completed_cleanup_outcome,
    complete_vm_cleanup_permit,
)
from shared.runtime.core.tool_policy import ToolPolicyError
from shared.workspace_contract import resolve_workspace_contract


def resume_reject_should_requeue(status_code: int) -> bool:
    """Return whether a stale-ready agent rejection should re-enter dispatch."""

    return status_code == 409


@dataclass(frozen=True, slots=True)
class JobControlDependencies:
    """Stateful application collaborators used by job controls."""

    store: Any
    logger: logging.Logger
    completion_control: Any
    completion_commands_enabled: Callable[[], bool]
    completion_control_active_sql: Callable[[str], str]
    completion_control_owned_active_sql: Callable[[str, str], str]
    sudo_gate: Any
    workspace: Any
    snapshots: Any
    ide_sessions: Any
    vm_provisioner: Any
    forge: Any
    vector_store: Any
    subjob_output: Any
    subjob_output_dependencies: Callable[[], Any]
    authorize_runtime_actor_request: Callable[..., Awaitable[Any]]
    redispatch_livelock_trip: Callable[[Mapping[str, Any]], Any]
    user_experts_enabled: Callable[..., Awaitable[Any]]
    resolve_default_models: Callable[..., Awaitable[Any]]
    prefetch_roster_refs: Callable[..., Awaitable[Any]]
    resolve_config: Callable[..., Any]
    canonical_config_name: Callable[[str], str]
    enforce_dispatch_grants: Callable[..., Awaitable[Any]]
    grant_violations_detail: Callable[[Any], Any]
    prepare_job_workspace_runtime: Callable[..., Awaitable[Any]]
    resume_missing_workspace: Callable[[Mapping[str, Any]], Any]
    workspace_context_keys: Mapping[str, str]
    prepare_job_repository_before_claim: Callable[..., Awaitable[Any]]
    resume_job_on_agent: Callable[..., Awaitable[bool]]
    trigger_dispatch: Callable[[], Any]
    resolve_job_notifications: Callable[..., Awaitable[Any]]
    maybe_wake_session: Callable[..., Awaitable[Any]]
    kick_session_wake_drain: Callable[..., Any]
    get_container_context: Callable[[Mapping[str, Any]], Mapping[str, Any]]
    get_vm_context: Callable[[Mapping[str, Any]], Mapping[str, Any]]
    recovery_store: Any


@dataclass(frozen=True, slots=True)
class JobControlOperations:
    """Authorized operation owner for VM, sudo, resume, and approval controls."""

    dependencies: JobControlDependencies

    async def create_vm(
        self, job: Mapping[str, Any], body: VMCreateRequest
    ) -> dict[str, Any]:
        """Provision a VM after the router has authorized job access."""

        assigned_backend = resolve_workspace_contract(job).assigned_backend
        if assigned_backend != "vm":
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "workspace_backend_conflict",
                    "message": "Job is not assigned to the VM workspace tier",
                    "assigned_backend": assigned_backend,
                },
            )
        if not self.dependencies.vm_provisioner.is_available:
            raise HTTPException(
                status_code=503,
                detail="VM provisioning not available (no NATS or K8s)",
            )
        success = await self.dependencies.vm_provisioner.create_vm(
            job_id=body.job_id,
            agent_config=body.agent_config,
            vm_image=body.vm_image,
            cpu_cores=body.cpu_cores,
            memory=body.memory,
            description=body.description,
        )
        if not success:
            raise HTTPException(status_code=500, detail="Failed to create VM")
        return {
            "status": "provisioning",
            "job_id": body.job_id,
            "mode": self.dependencies.vm_provisioner.mode,
        }

    async def list_vms(self) -> list[dict[str, Any]]:
        """Read VM projections after the router has enforced fleet admin."""

        try:
            async with self.dependencies.store.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT id, description, status, context->'vm' as vm_context "
                    "FROM jobs WHERE context ? 'vm' "
                    "ORDER BY updated_at DESC"
                )
            return [
                {
                    "job_id": str(row["id"]),
                    "description": row["description"],
                    "job_status": row["status"],
                    "vm": json.loads(row["vm_context"]) if row["vm_context"] else {},
                }
                for row in rows
            ]
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    async def get_vm_status(
        self,
        job_id: str,
        job: Mapping[str, Any],
        *,
        live: bool,
    ) -> dict[str, Any]:
        """Read one authorized job's captured and optional live VM status."""

        context = job.get("context") or {}
        vm_context = context.get("vm") if isinstance(context, dict) else None
        if not vm_context:
            raise HTTPException(
                status_code=404,
                detail=f"No VM context for job '{job_id}'",
            )
        result: dict[str, Any] = {"job_id": job_id, "vm": vm_context}
        if live:
            if not self.dependencies.vm_provisioner.lifecycle_available:
                result["live_error"] = "VM provisioning not available"
            else:
                live_status = await self.dependencies.vm_provisioner.query_status(
                    job_id
                )
                if live_status:
                    result["live"] = live_status
                else:
                    result["live_error"] = "No response from VM controller"
        return result

    async def delete_vm(self, job_id: str) -> dict[str, str]:
        """Delete one authorized job's VM."""

        if not self.dependencies.vm_provisioner.lifecycle_available:
            raise HTTPException(
                status_code=503,
                detail="VM provisioning not available (no NATS or K8s)",
            )
        try:
            identity = (
                await self.dependencies.vm_provisioner.capture_vm_teardown_identity(
                    job_id, entity_type="job"
                )
            )
            permit = await acquire_vm_cleanup_permit(
                self.dependencies.recovery_store,
                owner_kind="job",
                owner_id=job_id,
                identity=identity,
                source="public_vm_delete",
                purge_disk=True,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=409,
                detail="VM cleanup authority is temporarily unavailable",
            ) from exc
        if not permit.allowed:
            raise HTTPException(
                status_code=409,
                detail="VM cleanup is held for workspace recovery",
            )
        disposition = completed_cleanup_outcome(permit)
        if disposition is None:
            outcome = await self.dependencies.vm_provisioner.release_vm_captured(
                job_id,
                identity,
                entity_type="job",
                purge_disk=True,
                capture_snapshot=False,
                **vm_cleanup_kwargs(permit),
            )
            disposition = outcome.disposition
            if disposition in {"completed", "identity_superseded"}:
                await complete_vm_cleanup_permit(
                    self.dependencies.recovery_store,
                    permit,
                    outcome=disposition,
                )
        if disposition != "completed":
            raise HTTPException(status_code=500, detail="Failed to delete VM")
        return {"status": "deleting", "job_id": job_id}

    async def resume_job(
        self,
        job_id: str,
        *,
        user: dict[str, Any] | None,
        job: dict[str, Any],
        request: JobResumeRequest | None,
        req: Request,
    ) -> dict[str, str]:
        result = await self._resume_job_internal(
            job_id,
            user=user,
            job=job,
            request=request,
            req=req,
        )
        await self.dependencies.resolve_job_notifications(
            job_id,
            user=user,
            hook="resume",
        )
        return result

    async def retry_workspace_recovery(
        self,
        job_id: str,
        *,
        user: Mapping[str, Any],
        request: WorkspaceRecoveryRetryRequest,
    ) -> dict[str, Any]:
        from orchestrator.services.vm_workspace_recovery_store import (
            WorkspaceRecoveryControlConflict,
        )

        try:
            return await self.dependencies.recovery_store.retry_paused(
                job_id=UUID(job_id),
                operation_id=request.operation_id,
                request_id=request.request_id,
                actor_kind="user",
                actor_id=str(user.get("id") or "unknown"),
            )
        except WorkspaceRecoveryControlConflict as exc:
            raise HTTPException(
                status_code=409,
                detail={"code": exc.code, "message": exc.message},
            ) from exc

    async def approve_job(
        self,
        job_id: str,
        *,
        user: dict[str, Any] | None,
        job: dict[str, Any],
        request: JobApproveRequest | None,
    ) -> dict[str, Any]:
        result = await self._approve_job_internal(
            job_id,
            user=user,
            job=job,
            request=request,
        )
        await self.dependencies.resolve_job_notifications(
            job_id,
            user=user,
            hook="approve",
        )
        return result

    def subscribe_sudo_events(self):
        return self.dependencies.sudo_gate.subscribe_sse()

    def unsubscribe_sudo_events(self, queue: Any) -> None:
        self.dependencies.sudo_gate.unsubscribe_sse(queue)

    async def list_sudo_requests(self, **filters: Any) -> list[dict[str, Any]]:
        return await self.dependencies.sudo_gate.list_requests(**filters)

    async def get_sudo_request(self, request_id: str) -> dict[str, Any] | None:
        return await self.dependencies.sudo_gate.get_request(request_id)

    async def create_sudo_rule(self, body: SudoRuleCreateRequest) -> dict[str, Any]:
        if body.action not in ("approve", "deny", "review"):
            raise HTTPException(
                status_code=400,
                detail="action must be 'approve', 'deny', or 'review'",
            )
        result = await self.dependencies.sudo_gate.create_rule(
            pattern=body.pattern,
            action=body.action,
            priority=body.priority,
            description=body.description,
        )
        if not result:
            raise HTTPException(status_code=500, detail="Failed to create rule")
        return result

    async def list_sudo_rules(self) -> list[dict[str, Any]]:
        return await self.dependencies.sudo_gate.list_rules()

    async def delete_sudo_rule(self, rule_id: str) -> dict[str, str]:
        if not await self.dependencies.sudo_gate.delete_rule(rule_id):
            raise HTTPException(
                status_code=404,
                detail=f"Rule '{rule_id}' not found",
            )
        return {"status": "deleted", "id": rule_id}

    async def approve_sudo_request(
        self,
        request_id: str,
        row: dict[str, Any],
        *,
        caller: Mapping[str, Any],
        reason: str = "",
    ) -> dict[str, Any]:
        """Apply an authorized generic sudo approval."""

        if row.get("request_type") == "vm_upgrade":
            return await self._apply_vm_upgrade_decision(
                request_id,
                row,
                approve=True,
                upgrade=True,
                reason=reason or "VM upgrade approved",
                decided_by=self._decider_name(dict(caller)),
            )
        result = await self.dependencies.sudo_gate.approve_request(
            request_id,
            reason=reason,
            decided_by=self._decider_name(dict(caller)),
        )
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Sudo request '{request_id}' not found",
            )
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        return result

    async def deny_sudo_request(
        self,
        request_id: str,
        row: dict[str, Any],
        *,
        caller: Mapping[str, Any],
        reason: str,
    ) -> dict[str, Any]:
        """Apply an authorized generic sudo denial."""

        if row.get("request_type") == "vm_upgrade":
            return await self._apply_vm_upgrade_decision(
                request_id,
                row,
                approve=False,
                upgrade=False,
                reason=reason,
                decided_by=self._decider_name(dict(caller)),
            )
        result = await self.dependencies.sudo_gate.deny_request(
            request_id,
            reason=reason,
            decided_by=self._decider_name(dict(caller)),
        )
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Sudo request '{request_id}' not found",
            )
        if "error" in result:
            raise HTTPException(status_code=400, detail=result["error"])
        return result

    async def approve_sudo_vm_upgrade(
        self,
        request_id: str,
        row: dict[str, Any],
        *,
        caller: Mapping[str, Any],
        reason: str = "",
    ) -> dict[str, Any]:
        """Apply an authorized VM-upgrade approval."""

        if row.get("request_type") != "vm_upgrade":
            raise HTTPException(
                status_code=400,
                detail=f"Request is '{row.get('request_type')}', not a vm_upgrade "
                "request. Use POST /approve instead.",
            )
        return await self._apply_vm_upgrade_decision(
            request_id,
            row,
            approve=True,
            upgrade=True,
            reason=reason or "VM upgrade approved",
            decided_by=self._decider_name(dict(caller)),
        )

    async def resume_sudo_without_vm(
        self,
        request_id: str,
        row: dict[str, Any],
        *,
        caller: Mapping[str, Any],
        reason: str = "",
    ) -> dict[str, Any]:
        """Apply an authorized resume-without-VM decision."""

        if row.get("request_type") != "vm_upgrade":
            raise HTTPException(
                status_code=400,
                detail=f"Request is '{row.get('request_type')}', not a vm_upgrade "
                "request. Use POST /approve instead.",
            )
        return await self._apply_vm_upgrade_decision(
            request_id,
            row,
            approve=True,
            upgrade=False,
            reason=reason or "Resume without VM",
            decided_by=self._decider_name(dict(caller)),
        )

    async def _fail_expired_vm_upgrade_jobs(self) -> int:
        """Fail-loud arm for vm_upgrade freezes whose approval window closed.

        A ``vm_upgrade_required`` freeze parks the job invisible to the dispatcher
        (``freeze_data`` set) while its 24 h approval request is open. When the
        request expires undecided, nothing else ever touches the job — pre-fix it
        stayed wedged forever behind an expired request. Fail it with an explicit
        ``vm_upgrade_expired`` message (never the generic ``workspace_unavailable``
        recovery arm) and clear ``freeze_data`` so a manual Resume is viable again
        (the replayed sudo command then raises a FRESH approval request).

        Also heals historical wedges: any paused vm_upgrade-frozen job whose
        vm_upgrade requests are all decided/expired (none pending) is picked up
        regardless of when the window closed. Returns the number of jobs failed.
        """
        completion_exclusion = (
            ""
            if not self.dependencies.completion_commands_enabled()
            else (
                " AND NOT EXISTS ("
                "SELECT 1 FROM job_completion_sweep_exclusions AS completion_route "
                "WHERE completion_route.job_id = j.id)"
            )
        )
        control_guard = (
            ""
            if not self.dependencies.completion_commands_enabled()
            else f" AND NOT ({self.dependencies.completion_control_active_sql('j.context')})"
        )
        async with self.dependencies.store.acquire() as conn:
            rows = await conn.fetch(
                f"""
                SELECT j.id
                FROM jobs j
                WHERE j.status = 'paused'
                  AND j.freeze_data->>'freeze_type' = 'vm_upgrade_required'
                  AND EXISTS (
                      SELECT 1 FROM sudo_approval_requests s
                      WHERE s.job_id = j.id AND s.request_type = 'vm_upgrade'
                        AND s.status = 'expired'
                  )
                  AND NOT EXISTS (
                      SELECT 1 FROM sudo_approval_requests s
                      WHERE s.job_id = j.id AND s.request_type = 'vm_upgrade'
                        AND s.status = 'pending'
                  )
                  {completion_exclusion}
                  {control_guard}
                """
            )
            for row in rows:
                update_exclusion = (
                    ""
                    if not self.dependencies.completion_commands_enabled()
                    else (
                        " AND NOT EXISTS ("
                        "SELECT 1 FROM job_completion_sweep_exclusions "
                        "AS completion_route "
                        "WHERE completion_route.job_id = jobs.id)"
                    )
                )
                update_control_guard = (
                    ""
                    if not self.dependencies.completion_commands_enabled()
                    else f" AND NOT ({self.dependencies.completion_control_active_sql('context')})"
                )
                await conn.execute(
                    f"UPDATE jobs SET status = 'failed', freeze_data = NULL, "
                    "error_message = $2, updated_at = CURRENT_TIMESTAMP "
                    f"WHERE id = $1 AND status = 'paused'{update_exclusion}"
                    f"{update_control_guard}",
                    row["id"],
                    "vm_upgrade_expired: the VM-upgrade approval window (24h) "
                    "closed with no decision. Re-run or resume the job — the sudo "
                    "command will raise a fresh approval request.",
                )
                self.dependencies.logger.warning(
                    "Job %s failed: vm_upgrade approval window expired undecided",
                    row["id"],
                )
        return len(rows)

    def _job_frozen_for_vm_upgrade(self, job: dict | None) -> bool:
        """Whether a job is still parked on a ``vm_upgrade_required`` freeze."""
        if not job:
            return False
        fd = job.get("freeze_data")
        if isinstance(fd, str):
            try:
                fd = json.loads(fd)
            except (TypeError, ValueError):
                return False
        return bool(fd) and fd.get("freeze_type") == "vm_upgrade_required"

    def _decider_name(self, user: dict | None) -> str:
        """Human-readable decider identity for audit rows and agent feedback."""
        if not user:
            return "operator"
        return user.get("username") or user.get("email") or "operator"

    async def _apply_vm_upgrade_decision(
        self,
        request_id: str,
        row: dict,
        *,
        approve: bool,
        upgrade: bool,
        reason: str,
        decided_by: str = "operator",
    ) -> dict[str, Any]:
        """Decide a ``request_type='vm_upgrade'`` approval request AND drive the job.

        vm_upgrade rows have no NATS reply subject — flipping the row alone does
        nothing to the job: ``freeze_data`` stays set, the job stays invisible to
        the dispatcher, wedged forever. Every decision surface (generic
        approve/deny, approve-upgrade, resume-without-vm, MCP tools via REST)
        routes here so the decision always drives the job:

          - ``approve=True, upgrade=True``  → provision a VM, Continue-as-New
          - ``approve=True, upgrade=False`` → Continue-as-New on the original tier
            (operator chose "resume without VM")
          - ``approve=False``               → Continue-as-New on the original tier
            with a sticky, reasoned denial (the agent's sudo gate flips to a
            reasoned block, so the replayed command can't re-freeze)

        Decision hygiene: bound to the immutable request id, first-decider-wins
        (the ``status='pending'`` row flip is the claim), expired requests reject
        late decisions, and a repeat of the SAME decision re-drives the job only
        while it is still frozen (recovers jobs wedged by the historical
        row-flip-only endpoints, and jobs whose upgrade failed transiently after
        the flip) — otherwise it is a visible no-op.
        """
        job_id = str(row["job_id"]) if row.get("job_id") else None
        if not job_id:
            raise HTTPException(status_code=400, detail="No job_id in sudo request")

        status = row.get("status")
        target_status = "approved" if approve else "denied"
        # A deny may also re-drive a row the system already auto-denied (covers
        # the fallback where the auto-deny resume failed and left the job frozen).
        redrivable_statuses = {target_status} | (
            {"auto_denied"} if not approve else set()
        )
        control_claim = None

        try:
            if status == "pending":
                await self.dependencies.completion_control.guard(
                    job_id, source="sudo_vm_decision"
                )
                expires_at = row.get("expires_at")
                if expires_at is not None and expires_at < datetime.now(timezone.utc):
                    # The sweeper will flip it to 'expired' shortly; reject the late
                    # decision now (stale-token model) rather than racing the sweep.
                    raise HTTPException(
                        status_code=409,
                        detail="Approval window has expired — re-run the job to raise "
                        "a new request",
                    )
                if self.dependencies.completion_commands_enabled():
                    job = await self.dependencies.store.get_job(job_id)
                    if not job:
                        raise HTTPException(
                            status_code=404, detail=f"Job '{job_id}' not found"
                        )
                    control_claim = await self.dependencies.completion_control.claim(
                        {**job, "id": job_id}, source="sudo_vm_decision"
                    )
                decide = (
                    self.dependencies.sudo_gate.approve_request
                    if approve
                    else self.dependencies.sudo_gate.deny_request
                )
                result = await decide(request_id, reason=reason, decided_by=decided_by)
                if not result:
                    raise HTTPException(
                        status_code=404, detail=f"Sudo request '{request_id}' not found"
                    )
                if "error" in result:
                    # Lost the first-decider race between our read and the flip.
                    raise HTTPException(status_code=409, detail=result["error"])
            elif status in redrivable_statuses:
                job = await self.dependencies.store.get_job(job_id)
                if not self._job_frozen_for_vm_upgrade(job):
                    return {
                        "id": request_id,
                        "status": status,
                        "job_id": job_id,
                        "note": "already decided and job already driven — no-op",
                    }
                control_claim = await self.dependencies.completion_control.claim(
                    {**job, "id": job_id}, source="sudo_vm_redrive"
                )
                # Same decision repeated while the job is still frozen → re-drive it.
            else:
                raise HTTPException(
                    status_code=409,
                    detail=f"Request already '{status}' — decisions are "
                    "first-decider-wins and bound to the request id",
                )

            if approve and upgrade:
                if control_claim is None:
                    job_action = await self._upgrade_job_to_vm_internal(job_id)
                else:
                    job_action = await self._upgrade_job_to_vm_internal(
                        job_id, control_claim=control_claim
                    )
            elif control_claim is None:
                job_action = await self._resume_job_without_vm_internal(
                    job_id, decided_by=decided_by, reason=reason, denied=not approve
                )
            else:
                job_action = await self._resume_job_without_vm_internal(
                    job_id,
                    decided_by=decided_by,
                    reason=reason,
                    denied=not approve,
                    control_claim=control_claim,
                )
        finally:
            await self.dependencies.completion_control.abort(control_claim)

        return {
            "id": request_id,
            "status": target_status,
            "job_id": job_id,
            "job_action": job_action,
        }

    def _resume_reject_should_requeue(self, status_code: int) -> bool:
        """Whether an agent's rejection of a resume POST should re-queue the job for
        auto-dispatch instead of surfacing a 502.

        A 409 means the agent's DB ``status='ready'`` was stale — its pod is
        actually non-idle (a zombie that leaked ``_pod_state=WORKING`` on a prior
        cancel/pause, or an agent still finishing post-completion work) and refused
        the resume. Re-queuing lets a genuinely-ready agent pick the job up. Any
        other non-2xx is a real failure → 502. See
        knowledge-history/done/worker_pod_state_zombie_on_cancel.md.
        """
        return resume_reject_should_requeue(status_code)

    async def _resume_job_internal(
        self,
        job_id: str,
        *,
        user: dict[str, Any] | None,
        job: dict[str, Any],
        request: JobResumeRequest | None = None,
        req: Request | None = None,
    ) -> dict[str, str]:
        """Core of :func:`resume_job` after the access gate — request-free so the
        notification ``review_queue.resume`` / ``budget_exceeded.resume`` handlers
        can call it directly. ``req`` is only needed on the internal-actor branch
        (no ``user``), which a notification action never takes."""
        require_srw_runtime(job)
        # An explicit resume is the authority that lifts an operator pause
        # hold, but only the hold on the row this request was authorized
        # against: every pinned re-queue/claim below CASes on it, so a pause
        # that lands after this read makes the resume lose instead of being
        # crossed.
        operator_pause_lift = operator_pause_lift_token(job)
        recovery = await self.dependencies.recovery_store.unresolved_participation(
            UUID(job_id)
        )
        if recovery is not None:
            from orchestrator.services.vm_workspace_recovery_store import (
                WorkspaceRecoveryControlConflict,
            )

            operation_id = UUID(str(recovery["operation_id"]))
            actor_id = str(user.get("id") if user is not None else "internal")
            request_id = uuid5(
                NAMESPACE_URL,
                f"generic-resume:{operation_id}:{actor_id}",
            )
            try:
                result = await self.dependencies.recovery_store.retry_paused(
                    job_id=UUID(job_id),
                    operation_id=operation_id,
                    request_id=request_id,
                    actor_kind="user" if user is not None else "internal",
                    actor_id=actor_id,
                )
            except WorkspaceRecoveryControlConflict as exc:
                raise HTTPException(
                    status_code=409,
                    detail={"code": exc.code, "message": exc.message},
                ) from exc
            return {
                "status": str(result["status"]),
                "job_id": job_id,
                "agent_id": "",
            }
        canonical_policy = None
        if job.get("execution_harness_adapter") == "srw/v1":
            from orchestrator.services.manifest_execution_snapshot import (
                read_execution,
                srw_snapshot_config,
            )

            snapshot = await read_execution(self.dependencies.store, "Job", job_id)
            if snapshot is None:
                raise HTTPException(
                    409, "The admitted execution configuration is unavailable."
                )
            _, canonical_policy = srw_snapshot_config(snapshot)
        if request is None:
            request = JobResumeRequest()
        if job.get("completion_outcome_kind") == "blocked_undelivered":
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "blocked_undelivered_is_terminal",
                    "message": (
                        "This job ended blocked/undelivered and cannot be resumed. "
                        "A newer Officer-ready ticket generation may create a new job."
                    ),
                },
            )
        recovery_trip = self.dependencies.redispatch_livelock_trip(job)
        trip_ack_actor: dict[str, Any] | None = None
        if recovery_trip is not None:
            if user is not None:
                # The existing job-access guard remains the human authorization
                # policy. This payload is server-derived audit context only.
                trip_ack_actor = {
                    "caller_kind": "human",
                    "user_id": str(user.get("id")) if user.get("id") else None,
                }
            else:
                project_id = str(job["project_id"]) if job.get("project_id") else None
                if project_id is None or req is None:
                    raise HTTPException(
                        status_code=403,
                        detail=(
                            "A project-less redispatch circuit may only be "
                            "acknowledged by an authorized human user."
                        ),
                    )
                actor = await self.dependencies.authorize_runtime_actor_request(
                    self.dependencies.store,
                    req,
                    action="redispatch_livelock_ack",
                    project_id=project_id,
                )
                trip_ack_actor = actor.audit_payload()
        await self.dependencies.completion_control.guard(job_id, source="public_resume")

        # Resume PEP (decision 9, B3): re-check the runner's CURRENT grants against the
        # job's stored config before replaying it. Placed before the resume try so a 403
        # is not downgraded by the broad handler below (fail closed on denial). The line
        # that matters is "the stored configuration is unusable" vs "we could not reach
        # storage" — NOT "known exception type" vs "unknown": a TRANSIENT failure (a DB
        # blip reading the expert/grant rows) still proceeds, because the dispatch-time
        # check already passed minutes ago and tolerating a hiccup is cheaper than
        # refusing a resume over it; a PERMANENTLY UNUSABLE stored config fails closed
        # instead, because the same row raises the same way on every future resume of
        # this job, so nothing is "standing in" for a check that can never run. See the
        # except clause below for exactly which exceptions land in which bucket.
        if await self.dependencies.user_experts_enabled():
            try:
                _rcap: dict = {"merged_fragment": canonical_policy}
                if canonical_policy is None:
                    _rco = job.get("config_override")
                    if isinstance(_rco, str):
                        _rco = json.loads(_rco)
                    _rbase = self.dependencies.canonical_config_name(
                        job.get("config_name") or "worker_base"
                    )
                    _rexpert_row = (
                        await self.dependencies.store.get_expert_by_id(
                            str(job["expert_id"])
                        )
                        if job.get("expert_id")
                        else None
                    )
                    self.dependencies.resolve_config(
                        base_config_name=_rbase,
                        base_defaults=await self.dependencies.resolve_default_models(
                            job.get("user_id")
                        ),
                        expert_row=_rexpert_row,
                        request_override=_rco,
                        expert_type="worker",
                        capture=_rcap,
                        db_refs=await self.dependencies.prefetch_roster_refs(
                            expert_row=_rexpert_row,
                            overrides=(_rco,),
                            user_id=str(job["user_id"]) if job.get("user_id") else None,
                            project_ids=[str(job["project_id"])]
                            if job.get("project_id")
                            else [],
                        ),
                    )
                await self.dependencies.enforce_dispatch_grants(
                    _rcap["merged_fragment"],
                    runner_user_id=str(job["user_id"]) if job.get("user_id") else None,
                    project_ids=[str(job["project_id"])]
                    if job.get("project_id")
                    else [],
                    runner_kind=str(job.get("runner_kind") or "user"),
                )
            except GrantDenied as gd:
                self.dependencies.logger.warning(
                    "Resume denied for job %s: %s", job_id, gd
                )
                raise HTTPException(
                    status_code=403,
                    detail=self.dependencies.grant_violations_detail(gd.violations),
                )
            except (ToolPolicyError, ValueError, FileNotFoundError) as exc:
                # The STORED CONFIGURATION IS UNUSABLE — a different failure class
                # from "we could not reach storage" below, not a different set of
                # exception types to keep in sync by hand as resolve_config grows
                # new callees. Concretely, today, this bucket is: a malformed
                # `tools` policy (ToolPolicyError); a stored config_override /
                # expert-row config or prompts blob that is a string but not
                # valid JSON (json.JSONDecodeError, a ValueError subclass); a
                # merged config missing a field load_agent_config_from_dict
                # requires (ValueError); or an unresolvable config file / $extends
                # target (FileNotFoundError, raised deliberately by
                # load_and_merge_config, not from a flaky mount). Every one of
                # these raises identically on every future resume of this job —
                # tolerating it would be a latch, not a flakiness allowance,
                # because the check never ran at all. Fail closed. This is not a
                # grants denial (403) — the caller's grants were never consulted —
                # so say plainly that the stored config could not be resolved.
                #
                # Bare `ValueError` is deliberately broad and has ONE known
                # overlap: `_enforce_dispatch_grants` reaches
                # `list_grants_for_scopes`, which `json.loads` the
                # `capability_grants.value_json` column — so a corrupted GLOBAL
                # grant row would 409 every resume in that scope while blaming
                # this job's own config. Accepted, not overlooked: the column is
                # `jsonb NOT NULL` written only via `json.dumps(...)::jsonb`, so
                # Postgres validates syntax on every write and reaching it needs
                # DB-level corruption rather than any application path.
                # If you ADD a call to this try block, check it cannot raise
                # ValueError for a TRANSIENT reason — that would silently convert
                # a tolerated blip into a refused resume. Scoping this handler to
                # the config work alone (a second try around the grant check) is
                # the structural fix if that ever becomes a real risk.
                self.dependencies.logger.warning(
                    "Resume PEP: stored config for job %s cannot be resolved (%s); "
                    "failing closed — the grant check could not run.",
                    job_id,
                    exc,
                )
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "This job's stored configuration cannot be resolved, so "
                        "its capability grants could not be re-checked: " + str(exc)
                    ),
                )
            except Exception:
                self.dependencies.logger.exception(
                    "Resume PEP: grant re-check failed for job %s; proceeding "
                    "(dispatch-time check stands)",
                    job_id,
                )

        try:
            resume_context = job.get("context") or {}
            if isinstance(resume_context, str):
                try:
                    resume_context = json.loads(resume_context)
                except (TypeError, ValueError):
                    resume_context = {}
            if (
                job.get("execution_lane") == "stateless"
                and isinstance(resume_context, dict)
                and (
                    resume_context.get("_stateless_delete_pending") is True
                    or resume_context.get("_stateless_cancel_cleanup_pending") is True
                )
            ):
                raise HTTPException(
                    status_code=409,
                    detail="Job lifecycle cleanup is already in progress",
                )

            # Allow resuming jobs in any status except completed
            # This handles cancelled jobs (user wants to retry) and cases where
            # agents disappear without marking jobs as failed
            if job["status"] == "completed":
                raise HTTPException(
                    status_code=400,
                    detail=f"Job cannot be resumed (status: {job['status']}).",
                )

            # Honest [FEEDBACK_RESUME] banner cause for this explicit resume
            # path, derived from the status the job is actually leaving.
            feedback_reason = (
                "This job was frozen for review; a reviewer resumed it with the "
                "feedback below."
                if job["status"] in ("pending_review", "reviewing")
                else "An operator explicitly resumed this job with the feedback below."
            )

            if recovery_trip is not None:
                generation = str(recovery_trip.get("generation") or "")
                if not generation:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            "This redispatch circuit predates supported "
                            "acknowledgement; an operator must inspect it before retry."
                        ),
                    )
                feedback = request.feedback if request else None
                context_merge = (
                    {
                        "queued_feedback": feedback,
                        "queued_feedback_reason": feedback_reason,
                    }
                    if feedback
                    else None
                )
                if trip_ack_actor is None:  # pragma: no cover - authorization invariant
                    raise RuntimeError("redispatch circuit actor was not authorized")
                acknowledged = await self.dependencies.store.acknowledge_lease_recovery_circuit(
                    job_id,
                    expected_status=str(job["status"]),
                    expected_generation=generation,
                    acknowledged_by=trip_ack_actor,
                    context_merge=context_merge,
                    completion_commands_enabled=self.dependencies.completion_commands_enabled(),
                )
                if not acknowledged:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            "The redispatch circuit changed or is under completion "
                            "control; no acknowledgement was applied."
                        ),
                    )
                self.dependencies.logger.warning(
                    "Redispatch-livelock circuit acknowledged for job %s by %s",
                    job_id,
                    trip_ack_actor,
                )
                self.dependencies.trigger_dispatch()
                return {
                    "status": "acknowledged",
                    "message": "Redispatch circuit acknowledged; job queued for dispatch",
                    "job_id": job_id,
                }

            if (
                getattr(self.dependencies.store, "supports_vm_creation_retry", False)
                is True
            ):
                from orchestrator.services.vm_creation_resume import (
                    resume_pending_creation,
                )
                from orchestrator.services.vm_creation_retry_store import (
                    VMCreationRetryConflict,
                )

                try:
                    creation = await resume_pending_creation(
                        self.dependencies.store,
                        job_id=job_id,
                        feedback=request.feedback,
                        feedback_reason=feedback_reason,
                    )
                except VMCreationRetryConflict as exc:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "code": exc.reason,
                            "message": {
                                "creation_request_unproven": "The original VM creation record could not be verified. This attempt needs operator inspection.",
                                "vm_creation_retry_disabled": "VM creation retry is disabled on this deployment.",
                                "job_admission_expired": "The original job deadline elapsed; Resume cannot extend it.",
                            }.get(
                                exc.reason,
                                "VM creation cannot be resumed under its current workspace or execution authority.",
                            ),
                        },
                    ) from exc
                if creation is not None:
                    self.dependencies.trigger_dispatch()
                    return creation

            async def _queue_for_dispatch(
                message: str,
                *,
                workspace_preflight_required: bool = False,
                workspace_context_key: str | None = None,
                control_claim: Any | None = None,
                expected_status_override: str | None = None,
            ) -> dict[str, str]:
                """Park the job as 'paused' (dispatchable, unassigned) and kick the
                auto-dispatcher. Used both when no agent is ready and when the
                picked agent rejects the resume (its DB 'ready' was stale). Stashes
                feedback into context so it survives until a real agent picks it up,
                and sheds the row-level freeze — an explicit Resume on a frozen job
                must not leave it paused-but-invisible to the dispatcher
                (``get_dispatchable_jobs`` requires ``freeze_data IS NULL``).
                """
                feedback = request.feedback if request else None
                context_merge = (
                    {
                        "queued_feedback": feedback,
                        "queued_feedback_reason": feedback_reason,
                    }
                    if feedback
                    else None
                )
                expected_status = expected_status_override or str(job["status"])
                if (
                    job.get("execution_lane") == "stateless"
                    and workspace_preflight_required
                ):
                    if workspace_context_key is None:
                        raise RuntimeError(
                            "stateless workspace preflight requires a context key"
                        )
                    queued = await self.dependencies.store.prepare_stateless_job_for_workspace_resume(
                        job_id,
                        workspace_context_key,
                        context_merge,
                        expected_status=expected_status,
                        lift_operator_pause_hold=operator_pause_lift,
                        **self.dependencies.completion_control.resume_guard_kwargs(),
                    )
                elif job.get("execution_lane") == "stateless":
                    queued = await self.dependencies.store.queue_stateless_job_for_resume(
                        job_id,
                        context_merge,
                        priority=int(job.get("priority") or 0),
                        fair_key=(str(job["user_id"]) if job.get("user_id") else None),
                        expected_status=expected_status,
                        lift_operator_pause_hold=operator_pause_lift,
                        **self.dependencies.completion_control.resume_guard_kwargs(),
                    )
                elif workspace_preflight_required and control_claim is not None:
                    if workspace_context_key is None:
                        raise RuntimeError("workspace preflight requires a context key")
                    try:
                        queued = await self.dependencies.store.prepare_pinned_job_for_workspace_resume(
                            job_id,
                            workspace_context_key,
                            context_merge,
                            expected_status=expected_status,
                            completion_control_claim_id=str(control_claim.claim_id),
                            lift_operator_pause_hold=operator_pause_lift,
                        )
                    except Exception:
                        await self.dependencies.completion_control.abort(control_claim)
                        raise
                else:
                    # Missing stateless workspaces deliberately return through the
                    # leader preflight before any queue row becomes runnable.
                    queued = await self.dependencies.store.queue_job_for_resume(
                        job_id,
                        context_merge,
                        expected_status=expected_status,
                        lift_operator_pause_hold=operator_pause_lift,
                        **self.dependencies.completion_control.resume_guard_kwargs(),
                    )
                if not queued and job.get("execution_lane") == "stateless":
                    # A legacy/operator-created VM row may be repaired to the
                    # pinned lane while this request waits for the queue lock.
                    # Refresh exactly once and retry the historical pinned verb;
                    # its status CAS still prevents a concurrent terminal control
                    # from being resurrected.
                    refreshed = await self.dependencies.store.get_job(job_id)
                    if (
                        refreshed
                        and refreshed.get("execution_lane") == "pinned"
                        and str(refreshed.get("status") or "") == expected_status
                    ):
                        if (
                            workspace_preflight_required
                            and self.dependencies.completion_commands_enabled()
                        ):
                            if workspace_context_key is None:
                                raise RuntimeError(
                                    "workspace preflight requires a context key"
                                )
                            fallback_claim = (
                                await self.dependencies.completion_control.claim(
                                    {**refreshed, "id": job_id},
                                    source="missing_workspace_resume",
                                )
                            )
                            try:
                                queued = await self.dependencies.store.prepare_pinned_job_for_workspace_resume(
                                    job_id,
                                    workspace_context_key,
                                    context_merge,
                                    expected_status=expected_status,
                                    completion_control_claim_id=str(
                                        fallback_claim.claim_id
                                    ),
                                    lift_operator_pause_hold=operator_pause_lift,
                                )
                            except Exception:
                                await self.dependencies.completion_control.abort(
                                    fallback_claim
                                )
                                raise
                            if not queued:
                                await self.dependencies.completion_control.abort(
                                    fallback_claim
                                )
                        else:
                            if workspace_preflight_required:
                                if workspace_context_key is None:
                                    raise RuntimeError(
                                        "workspace preflight requires a context key"
                                    )
                                await self.dependencies.store.shed_workspace_context(
                                    job_id, workspace_context_key
                                )
                            queued = await self.dependencies.store.queue_job_for_resume(
                                job_id,
                                context_merge,
                                expected_status=expected_status,
                                lift_operator_pause_hold=operator_pause_lift,
                                **self.dependencies.completion_control.resume_guard_kwargs(),
                            )
                        if queued:
                            job.update(refreshed)
                if not queued:
                    await self.dependencies.completion_control.abort(control_claim)
                    refreshed = await self.dependencies.store.get_job(job_id)
                    refreshed_context = (refreshed or {}).get("context") or {}
                    if isinstance(refreshed_context, str):
                        try:
                            refreshed_context = json.loads(refreshed_context)
                        except (TypeError, ValueError):
                            refreshed_context = {}
                    if (
                        refreshed
                        and refreshed.get("execution_lane") == "stateless"
                        and isinstance(refreshed_context, dict)
                        and (
                            refreshed_context.get("_stateless_delete_pending") is True
                            or refreshed_context.get(
                                "_stateless_cancel_cleanup_pending"
                            )
                            is True
                        )
                    ):
                        raise HTTPException(
                            status_code=409,
                            detail="Job lifecycle cleanup is already in progress",
                        )
                    if operator_pause_lift_already_consumed(
                        refreshed, operator_pause_lift, feedback=feedback
                    ):
                        # A concurrent explicit resume (a double-click) lifted
                        # this exact hold first, carrying the same feedback.
                        # The job IS resumed; answer as the winner did.
                        self.dependencies.logger.info(
                            "Resume of job %s joined a concurrent resume of "
                            "operator pause hold %s",
                            job_id,
                            operator_pause_lift,
                        )
                        return {
                            "status": "queued",
                            "message": "Operator pause hold already lifted by a "
                            "concurrent resume; job queued for auto-dispatch",
                            "job_id": job_id,
                        }
                    raise HTTPException(
                        status_code=409,
                        detail="Job changed while it was being queued for resume",
                    )
                self.dependencies.logger.info(
                    f"Queued job {job_id} for auto-dispatch (previous status: "
                    f"{job['status']}, feedback: {bool(feedback)})"
                )
                self.dependencies.trigger_dispatch()
                return {"status": "queued", "message": message, "job_id": job_id}

            (
                workspace_action,
                job,
                workspace_reason,
            ) = await self.dependencies.prepare_job_workspace_runtime(job)
            if workspace_action == "wait":
                return await _queue_for_dispatch(
                    "Workspace authority recovery is pending live Kubernetes attestation"
                )
            if workspace_action == "fail":
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "workspace_authority_unavailable",
                        "message": workspace_reason
                        or "Inherited workspace authority is unavailable",
                    },
                )

            # A workspace-backed job with no live workspace cannot be resumed onto an
            # agent — nothing in this path provisions. Shed the stale context and hand
            # it to the dispatcher, the only thing that rebuilds a workspace. This is
            # the "clear context.vm and re-queue the job" that the VM park error tells
            # operators to do by hand.
            #
            # Must run BEFORE agent selection: the "no agents available" branch below
            # returns early, so a pre-flight shed is the only way that path re-queues
            # a job the dispatcher will actually re-provision rather than re-park.
            missing_workspace = self.dependencies.resume_missing_workspace(job)
            if missing_workspace:
                workspace_context_key = self.dependencies.workspace_context_keys[
                    missing_workspace
                ]
                control_claim = None
                if (
                    self.dependencies.completion_commands_enabled()
                    and job.get("execution_lane") != "stateless"
                ):
                    control_claim = await self.dependencies.completion_control.claim(
                        {**job, "id": job_id}, source="missing_workspace_resume"
                    )
                elif job.get("execution_lane") != "stateless":
                    await self.dependencies.store.shed_workspace_context(
                        job_id, workspace_context_key
                    )
                return await _queue_for_dispatch(
                    f"No live {missing_workspace} workspace — queued for re-provisioning",
                    workspace_preflight_required=True,
                    workspace_context_key=workspace_context_key,
                    control_claim=control_claim,
                )

            # Stateless jobs never select or POST to a registered agent. A live
            # k8s workspace is already proven above; the Class-A resume write and
            # worker_batch enqueue commit together in queue-first order.
            if job.get("execution_lane") == "stateless":
                return await _queue_for_dispatch(
                    "Stateless job queued for worker claim"
                )

            if self.dependencies.completion_commands_enabled():
                # The command-aware path linearizes the resume in the guarded jobs
                # mutation before any pod POST.  The dispatcher then performs the
                # external delivery from that durable state. Flag-off retains the
                # direct-resume optimization, but it now takes the same atomic
                # workspace-contract claim before any pod POST.
                return await _queue_for_dispatch(
                    "Completion-safe resume queued for auto-dispatch"
                )
            if operator_pause_lift:
                # Lifting a hold always re-queues through the guarded write:
                # it appends this feedback to whatever queued behind the hold
                # (the direct path's plain context merge would replace it),
                # and the dispatcher's resume lane keeps the same workspace.
                return await _queue_for_dispatch(
                    "Operator pause hold lifted; job queued for auto-dispatch"
                )

            # Determine which agent to use
            # Convert to string since DB returns asyncpg UUID objects
            assigned_agent_id = job.get("assigned_agent_id")
            agent_id = request.agent_id or (
                str(assigned_agent_id) if assigned_agent_id else None
            )
            agent = None

            # Try to get the specified/assigned agent
            if agent_id:
                agent = await self.dependencies.store.get_agent(agent_id)

            # If no agent or agent is unavailable, find a ready one.
            # Includes "working" because critic verdict handlers call resume while the
            # same agent is still finishing post-completion work (agent clears its job
            # status after the graph loop but before the heartbeat propagates).
            if not agent or agent["status"] in ("offline", "failed", "working"):
                ready_agents = await self.dependencies.store.list_agents(
                    status="ready", limit=1
                )
                if not ready_agents:
                    # No agent available right now — queue for auto-dispatch and let
                    # the dispatcher pick the job up when an agent becomes free.
                    return await _queue_for_dispatch(
                        "No agents available, job queued for auto-dispatch"
                    )
                agent = ready_agents[0]
                agent_id = str(agent["id"])
                self.dependencies.logger.info(
                    f"Auto-selected agent {agent_id} for job resume"
                )

            if agent["status"] not in ("ready", "completed"):
                raise HTTPException(
                    status_code=400,
                    detail=f"Agent is not ready (status: {agent['status']})",
                )

            if not agent.get("pod_ip"):
                raise HTTPException(
                    status_code=400,
                    detail="Agent has no pod IP configured",
                )

            # Handle context - might be dict or JSON string depending on DB driver
            job_context = job.get("context") or {}
            if isinstance(job_context, str):
                try:
                    job_context = json.loads(job_context)
                except json.JSONDecodeError:
                    job_context = {}

            # Feedback travels via context so the shared resume path delivers it
            # and clears it only after the agent accepts. The in-memory job row
            # was fetched before this merge, so stamp the local copy too —
            # _resume_job_on_agent reads job["context"], not the DB. Idempotent
            # with the queue fallback below, which merges the same value.
            if request.feedback:
                await self.dependencies.store.merge_job_context(
                    job_id,
                    {
                        "queued_feedback": request.feedback,
                        "queued_feedback_reason": feedback_reason,
                    },
                )
                job_context["queued_feedback"] = request.feedback
                job_context["queued_feedback_reason"] = feedback_reason
                job = {**job, "context": job_context}

            # Restore S3 environment snapshot into the VM before resuming.
            # This gives true "pick up where you left off" (environment + state).
            # Non-blocking: if restore fails, resume proceeds without it.
            if self.dependencies.snapshots.is_available:
                vm_ctx = job_context.get("vm", {}) if job_context else {}
                ssh_host = vm_ctx.get("ssh_host") or vm_ctx.get("pod_ip")
                ssh_port = vm_ctx.get("ssh_port")
                if ssh_host and ssh_port:
                    try:
                        if await self.dependencies.ide_sessions.restore_snapshot_for_resume(
                            job_id, ssh_host, int(ssh_port)
                        ):
                            self.dependencies.logger.info(
                                f"Snapshot restored for job {job_id} resume"
                            )
                    except Exception as e:
                        self.dependencies.logger.warning(
                            f"Snapshot restore failed for job {job_id} resume (non-blocking): {e}"
                        )

            # Claim before the external POST. This is the same atomic workspace-
            # authority/agent/lease boundary used by the normal dispatcher. Rows
            # outside the claimable created/paused/failed set (or rows won by a
            # concurrent actor) return through the durable queue path below.
            if not await self.dependencies.prepare_job_repository_before_claim(job):
                return await _queue_for_dispatch(
                    "Repository authority is not ready; job remains queued"
                )
            if not await self.dependencies.store.claim_job_for_agent(
                job_id,
                str(agent_id),
                allow_failed=True,
                lift_operator_pause_hold=operator_pause_lift,
            ):
                return await _queue_for_dispatch(
                    "Job queued for authoritative resume dispatch"
                )

            # Delegate payload build + delivery to the dispatcher's resume path so
            # a user-triggered resume ships exactly what an auto re-dispatch ships:
            # dispatch-time credentials (llm api_key/base_url, env_keys incl.
            # EMBEDDING_*), VM/container workspace config, sticky sudo denial, lite
            # mounts, queued feedback/delegation results. The fast-path used to
            # hand-build a bare payload here, which killed jobs landing on fresh
            # (clean-env) agent pods.
            # knowledge-base/knowledge/issues/job_resume_direct_path_skips_credential_injection.md
            if not await self.dependencies.resume_job_on_agent(job, agent):
                return await _queue_for_dispatch(
                    "Agent did not accept the direct resume; job queued for auto-dispatch",
                    expected_status_override="processing",
                )

            # Parent resumed — trigger dispatch so paused children become dispatchable
            self.dependencies.trigger_dispatch()

            return {"status": "resumed", "job_id": job_id, "agent_id": str(agent_id)}

        except HTTPException:
            raise
        except httpx.RequestError as e:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to connect to agent: {str(e)}. Agent may be offline.",
            ) from e
        except Exception as e:
            self.dependencies.logger.exception(f"Failed to resume job {job_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e)) from e

    async def _unmerged_pr_gate_reason(
        self, job: dict[str, Any], *, user: dict[str, Any] | None
    ) -> str | None:
        """Why this job may not be sealed yet, or ``None`` if nothing blocks it.

        The single gate behind BOTH terminal paths — ``approve_job`` (a human
        clicking approve) and the autonomous seal — so the two can never drift.

        Ordered by cost. A job that never opened a pull request is outside this
        feature entirely and must cost no I/O at all; a principal holding
        ``complete_unmerged_pr`` short-circuits before the forge is contacted.

        The principal is the calling user, or — on the internal/agent path, where
        ``require_internal_or_job_access`` yields ``None`` — the job's owner. An
        unresolvable principal holds no grant and therefore does not lift the
        block. Spec: knowledge-base/knowledge/features/merged_pr_completion_grant.md §5.
        """
        from orchestrator.services.job_delivery import (
            parse_job_pull_request,
            unmerged_pr_block_reason,
        )

        if parse_job_pull_request(job.get("context")) is None:
            return None

        principal = user
        if principal is None:
            owner_id = job.get("user_id")
            principal = (
                await self.dependencies.store.get_user(str(owner_id))
                if owner_id
                else None
            )

        project_id = job.get("project_id")
        if (
            principal is not None
            and await self.dependencies.store.user_can_complete_unmerged_pr(
                principal, project_id
            )
        ):
            return None

        datasources = await self.dependencies.store.resolve_datasources_for_job(
            str(job.get("id"))
        )
        return await unmerged_pr_block_reason(job, datasources=datasources)

    async def _approve_job_internal(
        self,
        job_id: str,
        *,
        user: dict[str, Any] | None,
        job: dict[str, Any],
        request: JobApproveRequest | None = None,
    ) -> dict[str, Any]:
        """Core of :func:`approve_job` after the access gate — request-free so the
        notification ``review_queue.approve`` handler can call it directly."""
        require_srw_runtime(job)
        if request is None:
            request = JobApproveRequest()
        await self.dependencies.completion_control.guard(
            job_id, source="public_approve"
        )
        control_claim = None
        control_claim_finished = False

        try:
            # 1. Validate status (gate already loaded the job and raised 404 if missing)
            if job["status"] not in ("pending_review", "reviewing"):
                raise HTTPException(
                    status_code=400,
                    detail=f"Job cannot be approved (status: {job['status']}). "
                    f"Only jobs in 'pending_review' or 'reviewing' status can be approved.",
                )
            if job.get("diff_status") == "pending":
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "This job has a pending project-cloud diff. Use the diff "
                        "accept or reject action so cloud delivery is resolved "
                        "before the job becomes terminal."
                    ),
                )
            from orchestrator.services.deliverable_gate import (
                explicit_pr_delivery_block_reason,
            )

            pr_delivery_block = await explicit_pr_delivery_block_reason(
                {**job, "id": job_id}, db=self.dependencies.store
            )
            if pr_delivery_block is not None:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "pr_deliverable_unverified",
                        "message": (
                            "This job cannot be completed because its explicit PR "
                            f"deliverable is unverified: {pr_delivery_block}."
                        ),
                    },
                )
            unmerged_pr = await self._unmerged_pr_gate_reason(
                {**job, "id": job_id}, user=user
            )
            if unmerged_pr is not None:
                raise HTTPException(
                    status_code=403,
                    detail=(
                        f"This job cannot be completed yet: {unmerged_pr}. Merge the "
                        "pull request first, or fail the job if the work was "
                        "rejected."
                    ),
                )
            control_claim = await self.dependencies.completion_control.claim(
                {**job, "id": job_id}, source="public_approve"
            )

            # 2. Read freeze data — DB first, Gitea fallback, local fallback
            frozen_data = None
            (
                repo_name,
                job_branch,
            ) = await self.dependencies.subjob_output.resolve_job_repo(
                job_id, dependencies=self.dependencies.subjob_output_dependencies()
            )

            # Primary: read freeze_data from DB
            if job.get("freeze_data"):
                frozen_data = job["freeze_data"]
                if isinstance(frozen_data, str):
                    frozen_data = json.loads(frozen_data)

            # Fallback: Gitea
            if frozen_data is None and self.dependencies.forge.is_initialized:
                frozen_data = await self.dependencies.forge.get_file(
                    repo_name, "output/job_frozen.json", ref=job_branch
                )

            # Fallback: local workspace
            if frozen_data is None:
                workspace_path = (
                    self.dependencies.workspace.base_path / "output" / "job_frozen.json"
                )
                if workspace_path.exists():
                    frozen_data = json.loads(workspace_path.read_text())
                else:
                    raise HTTPException(
                        status_code=404,
                        detail=f"No freeze data found for job '{job_id}' "
                        f"(checked DB, Gitea repo, and local workspace)",
                    )

            # 3. Determine freeze type (backward compat: missing = job_complete)
            freeze_type = frozen_data.get("freeze_type", "job_complete")

            if freeze_type in ("phase_boundary", "vm_upgrade_required"):
                # Phase boundary or VM upgrade freeze: approve to continue execution
                # (not complete). For vm_upgrade_required, this is the "resume without
                # VM" path — the agent continues in the container and adapts.
                local_frozen = (
                    self.dependencies.workspace.base_path / "output" / "job_frozen.json"
                )
                if job.get("execution_lane") == "stateless":
                    queued = (
                        await self.dependencies.store.queue_stateless_job_for_resume(
                            job_id,
                            priority=int(job.get("priority") or 0),
                            fair_key=(
                                str(job["user_id"]) if job.get("user_id") else None
                            ),
                            expected_status=str(job["status"]),
                            **self.dependencies.completion_control.resume_guard_kwargs(
                                control_claim=control_claim
                            ),
                        )
                    )
                    if not queued:
                        raise HTTPException(
                            status_code=409,
                            detail="Job changed while approval was re-enqueuing it",
                        )
                    control_claim_finished = control_claim is not None
                elif control_claim is not None:
                    # The claim cleared the predecessor's assigned-agent fence.
                    # Re-enter through dispatcher ownership instead of reviving
                    # that now-stale in-process agent.
                    queued = await self.dependencies.store.queue_job_for_resume(
                        job_id,
                        expected_status=str(job["status"]),
                        **self.dependencies.completion_control.resume_guard_kwargs(
                            control_claim=control_claim
                        ),
                    )
                    if not queued:
                        raise HTTPException(
                            status_code=409,
                            detail="Job changed while approval was re-enqueuing it",
                        )
                    control_claim_finished = True
                    self.dependencies.trigger_dispatch()
                else:
                    if local_frozen.exists():
                        local_frozen.unlink()
                    # Pinned agent is still parked in-process and resumes directly.
                    await self.dependencies.store.resume_pinned_job_in_process(job_id)
                if (
                    job.get("execution_lane") == "stateless"
                    or control_claim is not None
                ) and local_frozen.exists():
                    local_frozen.unlink()

                msg = (
                    f"Job {job_id} phase boundary approved (resume execution)"
                    if freeze_type == "phase_boundary"
                    else f"Job {job_id} vm_upgrade_required approved without VM (resume in container)"
                )
                self.dependencies.logger.info(msg)

                return {
                    "status": "approved_continue",
                    "job_id": job_id,
                    "freeze_type": freeze_type,
                    "phase_type": frozen_data.get("phase_type"),
                    "phase_number": frozen_data.get("phase_number"),
                    "command": frozen_data.get("command"),
                }

            # job_complete freeze (or backward compat): mark as truly completed
            completion_data = {
                **frozen_data,
                "status": "job_completed",
                "approved_at": datetime.now(timezone.utc).isoformat(),
                "approved_by": "human_operator",
            }
            if request.notes:
                completion_data["reviewer_notes"] = request.notes

            completion_json = json.dumps(completion_data, indent=2, ensure_ascii=False)

            stateless_approval_committed = False
            if job.get("execution_lane") == "stateless" and control_claim is None:
                # Linearize the stateless approval before any durable artifact
                # advertises completion. Cancel/resume take the same queue->jobs
                # lock order, so a stale verb fails without deleting the frozen
                # evidence or writing job_completion.json.
                async with self.dependencies.store.acquire() as conn:
                    async with conn.transaction():
                        await conn.fetchrow(
                            "SELECT state FROM run_queue WHERE unit_id = $1::uuid "
                            "AND unit_kind = 'worker_batch' FOR UPDATE",
                            job_id,
                        )
                        updated = await conn.fetchrow(
                            "UPDATE jobs SET status = 'completed', freeze_data = NULL, "
                            "completed_at = CURRENT_TIMESTAMP, "
                            "updated_at = CURRENT_TIMESTAMP "
                            "WHERE id = $1::uuid AND execution_lane = 'stateless' "
                            "AND status::text = $2::text RETURNING id",
                            job_id,
                            str(job["status"]),
                        )
                        if updated is None:
                            raise HTTPException(
                                status_code=409,
                                detail="Job changed while approval was being committed",
                            )
                stateless_approval_committed = True

            # 4. Write job_completion.json and remove job_frozen.json
            wrote_to_gitea = False
            if self.dependencies.forge.is_initialized:
                wrote_completion = await self.dependencies.forge.create_or_update_file(
                    repo_name,
                    "output/job_completion.json",
                    completion_json,
                    "Approve job: write job_completion.json",
                )
                if wrote_completion:
                    await self.dependencies.forge.delete_file(
                        repo_name,
                        "output/job_frozen.json",
                        "Approve job: remove job_frozen.json",
                    )
                    wrote_to_gitea = True

            # Also write to local workspace if it exists
            local_output = self.dependencies.workspace.base_path / "output"
            if local_output.exists():
                completion_path = local_output / "job_completion.json"
                completion_path.write_text(completion_json)
                frozen_path = local_output / "job_frozen.json"
                if frozen_path.exists():
                    frozen_path.unlink()

            # 5. Update DB: status → completed, clear freeze_data, set completed_at
            if control_claim is not None:
                from orchestrator.services.completion_control import (
                    CompletionControlClaimConflict,
                )

                try:
                    async with self.dependencies.completion_control.finish_claim(
                        control_claim
                    ) as (
                        conn,
                        _locked_job,
                    ):
                        updated = await conn.fetchrow(
                            "UPDATE jobs SET status = 'completed', "
                            "freeze_data = NULL, assigned_agent_id = NULL, "
                            "completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP), "
                            "updated_at = CURRENT_TIMESTAMP "
                            "WHERE id = $1::uuid AND status::text = $2::text "
                            "AND execution_lane = $3::text RETURNING id",
                            job_id,
                            str(job["status"]),
                            str(job.get("execution_lane") or "pinned"),
                        )
                        if updated is None:
                            raise CompletionControlClaimConflict(
                                "job changed while approval was being committed"
                            )
                except CompletionControlClaimConflict as exc:
                    raise HTTPException(status_code=409, detail=str(exc)) from exc
                control_claim_finished = True
            elif not stateless_approval_committed:
                async with self.dependencies.store.acquire() as conn:
                    await conn.execute(
                        "UPDATE jobs SET status = 'completed', freeze_data = NULL, "
                        "completed_at = CURRENT_TIMESTAMP, "
                        "updated_at = CURRENT_TIMESTAMP WHERE id = $1::uuid",
                        job_id,
                    )

            self.dependencies.logger.info(
                f"Job {job_id} approved (gitea={wrote_to_gitea})"
            )

            # 5b. Terminal-transition side effects (§6.6) — the SAME call the
            # /complete handler makes, because approval IS this job's transition
            # to `completed` (a `review`-autonomy job never reaches /complete's
            # terminal branch). Without it an approved job got neither the merge
            # of its contracted deliverables nor a change record, and its work sat
            # on `job/<short_id>` forever. Best-effort: never fails the approval.
            try:
                from orchestrator.services.completion import (
                    apply_terminal_job_side_effects,
                )

                await apply_terminal_job_side_effects(
                    job,
                    "completed",
                    gitea=self.dependencies.forge,
                    db=self.dependencies.store,
                    vector_db=self.dependencies.vector_store,
                )
            except Exception:
                self.dependencies.logger.warning(
                    f"Job {job_id}: terminal side effects failed (non-fatal)",
                    exc_info=True,
                )

            # Graft subjob output onto parent branch if applicable
            merge_result = None
            if job.get("parent_job_id"):
                merge_result = (
                    await self.dependencies.subjob_output.graft_subjob_output(
                        job_id,
                        dependencies=self.dependencies.subjob_output_dependencies(),
                    )
                )

            # Approval is a SECOND legitimate wake: the session was already told the
            # job froze for review, and "it was approved" is new information. The
            # dedup key is (job_id, terminal_status), so this fires exactly because
            # the status changed from pending_review to completed.
            await self.dependencies.maybe_wake_session(
                self.dependencies.store, job_id, "completed"
            )
            self.dependencies.kick_session_wake_drain(self.dependencies.store)

            # Agent is freed after completion — trigger dispatcher
            self.dependencies.trigger_dispatch()

            result = {
                "status": "approved",
                "job_id": job_id,
                "summary": completion_data.get("summary", ""),
                "deliverables": completion_data.get("deliverables", []),
                "approved_at": completion_data["approved_at"],
            }
            if merge_result:
                result["merge"] = merge_result
            return result

        except HTTPException:
            raise
        except Exception as e:
            self.dependencies.logger.exception(f"Failed to approve job {job_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e)) from e
        finally:
            if control_claim is not None and not control_claim_finished:
                await self.dependencies.completion_control.abort(control_claim)

    async def _upgrade_job_to_vm_internal(
        self,
        job_id: str,
        *,
        control_claim: Any | None = None,
    ) -> dict[str, Any]:
        """Core of the VM upgrade — request-free so the sudo decision paths can
        call it directly (the endpoint wrapper handles auth). It:

        1. Validates the job is frozen with the correct freeze type
        2. Sets ``context.vm.requested = true`` so the dispatcher provisions a VM
        3. Clears the freeze data
        4. Sets status to ``paused`` (dispatchable)
        5. Triggers the auto-dispatcher

        The dispatcher then provisions a VM via ``vm_provisioner``, waits for it to
        become ready, and dispatches the job to an agent with ``RemoteBackend`` pointed
        at the VM. The agent resumes from its checkpoint with full sudo access (gated
        by the VM's sudo approval system).

        The original workspace container is NOT deleted immediately — it is cleaned up
        when the job eventually completes or is cancelled (existing cleanup logic).
        """
        try:
            # 1. Validate job
            job = await self.dependencies.store.get_job(job_id)
            if not job:
                raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
            require_srw_runtime(job)
            if self.dependencies.redispatch_livelock_trip(job) is not None:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "This job is parked by the redispatch-livelock circuit; "
                        "use the explicit Resume action to acknowledge it first."
                    ),
                )
            await self.dependencies.completion_control.guard(
                job_id, source="upgrade_to_vm"
            )

            if job["status"] not in ("pending_review", "reviewing", "paused"):
                raise HTTPException(
                    status_code=400,
                    detail=f"Job cannot be upgraded (status: {job['status']}). "
                    f"Only frozen jobs can be upgraded to VMs.",
                )
            if control_claim is None:
                control_claim = await self.dependencies.completion_control.claim(
                    {**job, "id": job_id}, source="upgrade_to_vm"
                )

            # 2. Read freeze data to validate freeze type
            frozen_data = None
            if job.get("freeze_data"):
                frozen_data = job["freeze_data"]
                if isinstance(frozen_data, str):
                    frozen_data = json.loads(frozen_data)

            if frozen_data is None:
                (
                    repo_name,
                    job_branch,
                ) = await self.dependencies.subjob_output.resolve_job_repo(
                    job_id, dependencies=self.dependencies.subjob_output_dependencies()
                )
                if self.dependencies.forge.is_initialized:
                    frozen_data = await self.dependencies.forge.get_file(
                        repo_name, "output/job_frozen.json", ref=job_branch
                    )
                if frozen_data is None:
                    workspace_path = (
                        self.dependencies.workspace.base_path
                        / "output"
                        / "job_frozen.json"
                    )
                    if workspace_path.exists():
                        frozen_data = json.loads(workspace_path.read_text())

            if frozen_data is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"No freeze data found for job '{job_id}'",
                )

            freeze_type = frozen_data.get("freeze_type")
            if freeze_type != "vm_upgrade_required":
                raise HTTPException(
                    status_code=400,
                    detail=f"Job freeze type is '{freeze_type}', not 'vm_upgrade_required'. "
                    f"Use POST /api/jobs/{job_id}/approve instead.",
                )

            # 3. Check VM provisioner is available
            if not self.dependencies.vm_provisioner.is_available:
                raise HTTPException(
                    status_code=503,
                    detail="VM provisioner is not available. Cannot upgrade to VM.",
                )

            # 4. Build the VM-request delta — merged into context.vm below so any
            #    existing vm siblings are preserved.
            vm_updates = {
                "requested": True,
                "upgrade_from": "container",
                "upgrade_command": frozen_data.get("command", ""),
            }
            upgraded_workspace_contract = {
                "version": 1,
                "requested_backend": "vm",
                "assigned_backend": "vm",
                "assignment_source": "operator_vm_upgrade",
            }

            # 5. Update DB in ONE statement: merge the VM keys into context.vm, clear
            #    freeze, set status to paused (dispatchable), unassign agent. Fused so
            #    the context is visible the instant the status flips — a split would
            #    open a dispatch window on half-written context.
            async with self.dependencies.store.acquire() as conn:
                async with conn.transaction():
                    if job.get("execution_lane") == "stateless":
                        # Queue-first handoff: the VM lane belongs to registered
                        # agents in this slice. Closing the durable worker row and
                        # flipping execution_lane commit together, so neither
                        # execution plane can observe a runnable half-transition.
                        await conn.fetchrow(
                            "SELECT state FROM run_queue "
                            "WHERE unit_id = $1::uuid "
                            "AND unit_kind = 'worker_batch' FOR UPDATE",
                            job_id,
                        )
                    if self.dependencies.completion_commands_enabled():
                        completion_blocked = await conn.fetchval(
                            "SELECT EXISTS (SELECT 1 FROM "
                            "job_completion_sweep_exclusions AS completion_route "
                            "WHERE completion_route.job_id=$1::uuid)",
                            job_id,
                        )
                        if completion_blocked:
                            raise HTTPException(
                                status_code=409, detail="completion finalizing"
                            )
                    control_drop = (
                        " - '_completion_control_claim'"
                        if control_claim is not None
                        else ""
                    )
                    control_guard = (
                        " AND ("
                        + self.dependencies.completion_control_owned_active_sql(
                            "context", "$6"
                        )
                        + ")"
                        if control_claim is not None
                        else ""
                    )
                    update_args: tuple[Any, ...] = (
                        json.dumps(vm_updates),
                        job_id,
                        str(job["status"]),
                        str(job.get("execution_lane") or "pinned"),
                        json.dumps(upgraded_workspace_contract),
                    )
                    if control_claim is not None:
                        update_args += (str(control_claim.claim_id),)
                    updated = await conn.fetchrow(
                        f"UPDATE jobs SET context = jsonb_set(jsonb_set("
                        f"        COALESCE(context, '{{}}'::jsonb){control_drop}, '{{vm}}', "
                        "        COALESCE(context->'vm', '{}'::jsonb) || $1::jsonb"
                        "    ), '{_workspace_contract}', $5::jsonb), "
                        "    config_override = jsonb_set("
                        "        COALESCE(config_override, '{}'::jsonb), '{workspace}', "
                        "        COALESCE(config_override->'workspace', '{}'::jsonb) "
                        '            || \'{"backend":"vm"}\'::jsonb), '
                        "    status = 'paused', freeze_data = NULL, "
                        "    assigned_agent_id = NULL, execution_lane = 'pinned', "
                        "    updated_at = CURRENT_TIMESTAMP "
                        "WHERE id = $2::uuid AND status::text = $3::text "
                        f"AND execution_lane = $4::text{control_guard} RETURNING id",
                        *update_args,
                    )
                    if updated is None:
                        raise HTTPException(
                            status_code=409,
                            detail="Job changed while it was being upgraded to a VM",
                        )
                    if job.get("execution_lane") == "stateless":
                        await conn.execute(
                            "UPDATE run_queue SET state = 'done', "
                            "lease_token = lease_token + 1, leased_by = NULL, "
                            "last_leased_by = NULL, leased_until = NULL, "
                            "run_after = now(), queued_at = now() "
                            "WHERE unit_id = $1::uuid "
                            "AND unit_kind = 'worker_batch'",
                            job_id,
                        )

            # Only remove the local freeze after the queue-first status/lane CAS
            # commits. A concurrent cancel must not lose its operator evidence to
            # an upgrade request that no longer owns the job transition.
            local_frozen = (
                self.dependencies.workspace.base_path / "output" / "job_frozen.json"
            )
            if local_frozen.exists():
                local_frozen.unlink()

            self.dependencies.logger.info(
                f"Job {job_id} approved for VM upgrade "
                f"(command={frozen_data.get('command', 'N/A')!r})"
            )

            # 7. Trigger dispatcher — it will provision a VM and dispatch
            self.dependencies.trigger_dispatch()
            await self.dependencies.resolve_job_notifications(
                job_id, user=None, hook="vm_upgrade"
            )

            return {
                "status": "approved_vm_upgrade",
                "job_id": job_id,
                "freeze_type": freeze_type,
                "command": frozen_data.get("command"),
                "vm_provisioner_mode": self.dependencies.vm_provisioner.mode,
            }

        except HTTPException:
            raise
        except Exception as e:
            self.dependencies.logger.exception(
                f"Failed to upgrade job {job_id} to VM: {e}"
            )
            raise HTTPException(status_code=500, detail=str(e)) from e
        finally:
            await self.dependencies.completion_control.abort(control_claim)

    async def _capture_workspace_snapshot_for_freeze(
        self, job: dict, job_id: str
    ) -> bool:
        """Force a durable capture when a job parks on a vm_upgrade approval.

        The pause that follows is a human-wait (24 h approval TTL) while the
        workspace may be legitimately reclaimed after the warm grace — the S3
        archive (plus the cross-pod checkpointer) is what makes that reclaim
        non-destructive. The legacy caller runs this as a background task; the
        durable completion runner awaits and journals the result so a process death
        cannot lose the Class-D attempt. Unreachable targets (tailnet VMs) skip
        visibly inside the snapshot service; the reconciler's snapshot-before-reap
        remains the backstop.
        """
        if not getattr(self.dependencies.snapshots, "is_available", False):
            return False
        container_ctx = self.dependencies.get_container_context(job)
        vm_ctx = self.dependencies.get_vm_context(job)
        assigned_backend = resolve_workspace_contract(job).assigned_backend
        container_host = container_ctx.get("host") or container_ctx.get("pod_ip")
        if (
            assigned_backend == "vm"
            and vm_ctx.get("status") == "ready"
            and vm_ctx.get("ssh_host")
        ):
            source = "vm"
        elif container_ctx.get("status") == "ready" and container_host:
            if str(container_ctx.get("provisioner") or "").strip().lower() != "docker":
                # A Ready Pod endpoint—even a freshly attested one—does not provide
                # crash-safe authority across the whole snapshot/S3 operation.
                self.dependencies.logger.warning(
                    "Freeze capture refused for %s: Kubernetes capture authority "
                    "is unavailable",
                    job_id,
                )
                return False
            ssh_host = container_host
            ssh_port, source = int(container_ctx.get("port") or 30022), "pod"
        elif vm_ctx.get("status") == "ready" and vm_ctx.get("ssh_host"):
            # Legacy residue is never trusted directly: the exact VM claim below
            # re-reads the authoritative selected workspace contract and refuses a
            # stale VM projection when another tier is selected.
            source = "vm"
        else:
            self.dependencies.logger.info(
                f"Freeze capture skipped for {job_id}: no live workspace endpoint"
            )
            return True
        try:
            if source == "vm":
                from orchestrator.services.vm_remote_operation import (
                    VMRemoteOperationLeaseLost,
                    VMRemoteOperationUnavailable,
                    claim_vm_remote_operation,
                )

                try:
                    lease = await claim_vm_remote_operation(
                        db=self.dependencies.store,
                        provisioner=self.dependencies.vm_provisioner,
                        owner_id=job_id,
                        owner_kind="job",
                        operation_kind="snapshot_capture",
                    )
                    async with lease:

                        async def capture_authority() -> bool:
                            return await lease.revalidate() is not None

                        identity = lease.identity
                        ssh_host = identity.ssh_host
                        ssh_port = identity.ssh_port
                        ok = await self.dependencies.snapshots.capture_vm_snapshot(
                            job_id=job_id,
                            ssh_host=ssh_host,
                            ssh_port=ssh_port,
                            source_type="vm",
                            agent_config=self.dependencies.canonical_config_name(
                                job.get("config_name") or "worker_base"
                            ),
                            expected_host_key_fingerprint=(
                                identity.ssh_host_key_fingerprint
                            ),
                            capture_authority=capture_authority,
                        )
                except (VMRemoteOperationUnavailable, VMRemoteOperationLeaseLost):
                    self.dependencies.logger.warning(
                        "Freeze capture refused for %s: exact VM authority is unavailable",
                        job_id,
                    )
                    return False
            else:
                ok = await self.dependencies.snapshots.capture_vm_snapshot(
                    job_id=job_id,
                    ssh_host=ssh_host,
                    ssh_port=ssh_port,
                    source_type=source,
                    agent_config=self.dependencies.canonical_config_name(
                        job.get("config_name") or "worker_base"
                    ),
                )
            self.dependencies.logger.info(
                f"Freeze capture for {job_id} ({source} {ssh_host}:{ssh_port}): "
                f"{'ok' if ok else 'failed/skipped'}"
            )
            return bool(ok)
        except Exception:
            self.dependencies.logger.exception(f"Freeze capture failed for {job_id}")
            return False

    async def _resume_job_without_vm_internal(
        self,
        job_id: str,
        *,
        decided_by: str = "operator",
        reason: str = "",
        denied: bool = True,
        completion_owner_command_id: str | None = None,
        completion_owner: str | None = None,
        control_claim: Any | None = None,
    ) -> dict[str, Any]:
        """Continue-as-New on the original workspace tier after a vm_upgrade
        decision that does NOT provision a VM (operator deny, resume-without-vm,
        or the auto-deny for owners who can never be granted one).

        Mirrors :func:`_upgrade_job_to_vm_internal`'s skeleton — clear
        ``freeze_data``, unassign, ``status='paused'`` (dispatchable), trigger
        dispatch — but instead of ``context.vm.requested`` it records a STICKY
        denial in ``context.sudo_denial`` (dispatch flips the agent's sudo gate
        from "freeze" to a reasoned "block", so the replayed command returns an
        explanation instead of re-freezing into a new approval loop) and queues
        reasoned feedback so the agent knows who decided, why, and what to do
        instead. Reason-less denials demonstrably cause agent retry loops.
        """
        job = await self.dependencies.store.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        require_srw_runtime(job)
        if self.dependencies.redispatch_livelock_trip(job) is not None:
            raise HTTPException(
                status_code=409,
                detail=(
                    "This job is parked by the redispatch-livelock circuit; "
                    "use the explicit Resume action to acknowledge it first."
                ),
            )
        if (completion_owner_command_id is None) != (completion_owner is None):
            raise ValueError(
                "completion owner command id and owner must be supplied together"
            )
        if completion_owner_command_id is None:
            await self.dependencies.completion_control.guard(
                job_id, source="sudo_resume_without_vm"
            )

        if job["status"] not in ("pending_review", "reviewing", "paused"):
            raise HTTPException(
                status_code=400,
                detail=f"Job cannot be resumed without VM (status: {job['status']}).",
            )

        frozen_data = job.get("freeze_data")
        if isinstance(frozen_data, str):
            try:
                frozen_data = json.loads(frozen_data)
            except (TypeError, ValueError):
                frozen_data = None
        command = (frozen_data or {}).get("command", "")

        reason_suffix = f": {reason}" if reason else "."
        if denied:
            feedback = (
                f"Your VM-upgrade request for `{command or 'a sudo command'}` was "
                f"DENIED by {decided_by}{reason_suffix} Continue the job WITHOUT "
                "elevated privileges. Do not re-attempt sudo commands — they will "
                "be blocked. Use a rootless alternative, or record the limitation "
                "in your results and move on."
            )
        else:
            feedback = (
                f"The operator ({decided_by}) chose to resume this job WITHOUT a "
                f"VM{reason_suffix} `{command or 'sudo'}` will not run with "
                "elevated privileges, and further sudo commands will be blocked. "
                "Use a rootless alternative, or record the limitation in your "
                "results and move on."
            )

        sudo_denial = {
            "denied": denied,
            "decided_by": decided_by,
            "reason": reason,
            "command": command,
            "decided_at": datetime.now(timezone.utc).isoformat(),
        }
        if completion_owner_command_id is None and control_claim is None:
            control_claim = await self.dependencies.completion_control.claim(
                {**job, "id": job_id}, source="sudo_resume_without_vm"
            )

        # Remove local freeze artifact (parity with the upgrade arm). Stateless
        # waits until its queue/status CAS succeeds below; a stale control request
        # must leave the operator evidence intact.
        local_frozen = (
            self.dependencies.workspace.base_path / "output" / "job_frozen.json"
        )
        if job.get("execution_lane") != "stateless" and local_frozen.exists():
            try:
                local_frozen.unlink()
            except Exception:
                await self.dependencies.completion_control.abort(control_claim)
                raise

        # ONE statement: sticky denial + queued feedback + clear freeze + unassign
        # + paused (dispatchable) — fused so the dispatcher can never observe a
        # half-written decision.
        resume_context = {"sudo_denial": sudo_denial, "queued_feedback": feedback}
        if job.get("execution_lane") == "stateless":
            try:
                queued = await self.dependencies.store.queue_stateless_job_for_resume(
                    job_id,
                    resume_context,
                    priority=int(job.get("priority") or 0),
                    fair_key=(str(job["user_id"]) if job.get("user_id") else None),
                    expected_status=str(job["status"]),
                    **self.dependencies.completion_control.resume_guard_kwargs(
                        completion_owner_command_id,
                        completion_owner,
                        control_claim,
                    ),
                )
            except Exception:
                await self.dependencies.completion_control.abort(control_claim)
                raise
            if not queued:
                await self.dependencies.completion_control.abort(control_claim)
                raise HTTPException(
                    status_code=409,
                    detail="Job changed while it was being re-enqueued without a VM",
                )
            if local_frozen.exists():
                local_frozen.unlink()
        else:
            try:
                async with self.dependencies.store.acquire() as conn:
                    if self.dependencies.completion_commands_enabled():
                        async with conn.transaction():
                            completion_blocked = await self.dependencies.store._completion_resume_blocked_on_conn(
                                conn,
                                UUID(job_id),
                                completion_owner_command_id=(
                                    completion_owner_command_id
                                ),
                                completion_owner=completion_owner,
                            )
                            if completion_blocked:
                                raise HTTPException(
                                    status_code=409, detail="completion finalizing"
                                )
                            control_drop = (
                                " - '_completion_control_claim'"
                                if control_claim is not None
                                else ""
                            )
                            control_guard = (
                                " AND ("
                                + self.dependencies.completion_control_owned_active_sql(
                                    "context", "$4"
                                )
                                + ")"
                                if control_claim is not None
                                else ""
                            )
                            args: tuple[Any, ...] = (
                                json.dumps(resume_context),
                                job_id,
                                str(job["status"]),
                            )
                            if control_claim is not None:
                                args += (str(control_claim.claim_id),)
                            updated = await conn.execute(
                                "UPDATE jobs SET context = "
                                f"(COALESCE(context, '{{}}'::jsonb){control_drop}) "
                                "|| $1::jsonb, status = 'paused', freeze_data = NULL, "
                                "assigned_agent_id = NULL, updated_at = CURRENT_TIMESTAMP "
                                "WHERE id = $2::uuid AND status::text = $3::text"
                                f"{control_guard}",
                                *args,
                            )
                            if control_claim is not None and updated != "UPDATE 1":
                                raise HTTPException(
                                    status_code=409,
                                    detail=(
                                        "Job changed while it was being resumed "
                                        "without a VM"
                                    ),
                                )
                    else:
                        await conn.execute(
                            "UPDATE jobs SET context = COALESCE(context, '{}'::jsonb) || $1::jsonb, "
                            "status = 'paused', freeze_data = NULL, "
                            "assigned_agent_id = NULL, updated_at = CURRENT_TIMESTAMP "
                            "WHERE id = $2::uuid",
                            json.dumps(resume_context),
                            job_id,
                        )
            except Exception:
                await self.dependencies.completion_control.abort(control_claim)
                raise

        self.dependencies.logger.info(
            f"Job {job_id} resumed without VM "
            f"({'denied' if denied else 'no-vm approve'} by {decided_by}, "
            f"command={command!r})"
        )
        self.dependencies.trigger_dispatch()
        await self.dependencies.resolve_job_notifications(
            job_id, user=None, hook="vm_denied" if denied else "resumed_without_vm"
        )

        return {
            "status": "denied_vm_upgrade" if denied else "resumed_without_vm",
            "job_id": job_id,
            "command": command,
            "decided_by": decided_by,
            "reason": reason,
        }

    async def _internal_resume_job(
        self,
        job_id: str,
        feedback: str,
        reason: str | None = None,
        *,
        expected_status: str | None = None,
        expected_route_id: str | None = None,
        additional_context: Mapping[str, Any] | None = None,
        completion_owner_command_id: str | None = None,
        completion_owner: str | None = None,
    ) -> bool:
        """Queue a job for resume via the auto-dispatcher.

        THE escalation verb — this is a destructive resume: the worker
        force-compacts its context, archives its in-flight todos, and re-plans
        from scratch against the feedback. Use it when the plan itself is wrong;
        a mid-run course correction belongs on the guidance lane
        (``services.inbound_reply.queue_supervisor_guidance``) instead.

        Stores feedback in the job's context as ``queued_feedback`` (plus
        ``queued_feedback_reason`` when given — rendered verbatim in the worker's
        [FEEDBACK_RESUME] banner so the stated cause is the actual cause), sets
        status to ``paused`` (dispatchable), and triggers the dispatcher.  Avoids
        HTTP self-calls — the dispatcher will pick it up and send it to an agent.

        The single write also sheds the row-level freeze. This path's dominant
        caller is a human reply to a ``blocking_message`` freeze, so the job ALWAYS
        arrives here frozen — and ``get_dispatchable_jobs`` requires
        ``freeze_data IS NULL``, so keeping the blob parked the job as
        paused-but-invisible forever.
        See knowledge-base/knowledge/issues/blocking_message_reply_keeps_freeze_data.md.
        """
        updates: dict[str, Any] = {
            **dict(additional_context or {}),
            "queued_feedback": feedback,
        }
        if reason:
            updates["queued_feedback_reason"] = reason
        job = await self.dependencies.store.get_job(job_id)
        if not job:
            self.dependencies.logger.warning(
                f"_internal_resume_job: job {job_id} not found"
            )
            return False
        if not uses_srw_runtime(job):
            return False
        observed_status = str(job.get("status") or "")
        if expected_status is not None and observed_status != expected_status:
            self.dependencies.logger.warning(
                "_internal_resume_job: expected status %s but job %s is %s",
                expected_status,
                job_id,
                observed_status,
            )
            return False
        if observed_status in ("completed", "failed", "cancelled"):
            self.dependencies.logger.warning(
                "_internal_resume_job: refusing to resurrect terminal job %s (%s)",
                job_id,
                observed_status,
            )
            return False

        if completion_owner_command_id is None:
            await self.dependencies.completion_control.guard(
                job_id, source="internal_resume"
            )

        if job.get("execution_lane") == "stateless":
            queued = await self.dependencies.store.queue_stateless_job_for_resume(
                job_id,
                updates,
                priority=int(job.get("priority") or 0),
                fair_key=(str(job["user_id"]) if job.get("user_id") else None),
                expected_status=observed_status,
                **(
                    {"expected_route_id": expected_route_id}
                    if expected_route_id is not None
                    else {}
                ),
                **self.dependencies.completion_control.resume_guard_kwargs(
                    completion_owner_command_id, completion_owner
                ),
            )
            if not queued:
                refreshed = await self.dependencies.store.get_job(job_id)
                if (
                    refreshed
                    and refreshed.get("execution_lane") == "pinned"
                    and str(refreshed.get("status") or "") == observed_status
                ):
                    queued = await self.dependencies.store.queue_job_for_resume(
                        job_id,
                        updates,
                        expected_status=observed_status,
                        expected_route_id=expected_route_id,
                        **self.dependencies.completion_control.resume_guard_kwargs(
                            completion_owner_command_id, completion_owner
                        ),
                    )
                    if queued:
                        job = refreshed
        else:
            queued = await self.dependencies.store.queue_job_for_resume(
                job_id,
                updates,
                expected_status=observed_status,
                expected_route_id=expected_route_id,
                **self.dependencies.completion_control.resume_guard_kwargs(
                    completion_owner_command_id, completion_owner
                ),
            )
        if not queued:
            self.dependencies.logger.warning(
                "_internal_resume_job: queue CAS lost for job %s", job_id
            )
            return False

        self.dependencies.logger.info(
            "Queued job %s for %s resume with feedback",
            job_id,
            job.get("execution_lane", "pinned"),
        )
        if operator_pause_lift_token(job):
            # The write merged around the hold: the feedback waits for the
            # operator's explicit resume instead of redispatching the job.
            self.dependencies.logger.info(
                "Job %s is held by an operator pause; queued feedback waits for "
                "an explicit resume",
                job_id,
            )
        self.dependencies.trigger_dispatch()
        return True

    fail_expired_vm_upgrade_jobs = _fail_expired_vm_upgrade_jobs
    job_frozen_for_vm_upgrade = _job_frozen_for_vm_upgrade
    apply_vm_upgrade_decision = _apply_vm_upgrade_decision
    resume_reject_should_requeue = _resume_reject_should_requeue
    resume_job_internal = _resume_job_internal
    unmerged_pr_gate_reason = _unmerged_pr_gate_reason
    approve_job_internal = _approve_job_internal
    upgrade_job_to_vm_internal = _upgrade_job_to_vm_internal
    capture_workspace_snapshot_for_freeze = _capture_workspace_snapshot_for_freeze
    resume_job_without_vm_internal = _resume_job_without_vm_internal
    internal_resume_job = _internal_resume_job


__all__ = [
    "JobControlDependencies",
    "JobControlOperations",
    "resume_reject_should_requeue",
]
