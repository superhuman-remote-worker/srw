"""Pending-job dispatch scheduling: the R1.B11 owner of the dispatcher.

Extracted verbatim from ``orchestrator.main`` (R1.B11 lane A). This module is
the home of dispatch scheduling: the core matcher that turns dispatchable and
admittable jobs into workspace preflight, stateless ``run_queue`` admission,
registered-agent claims, agent-pod provisioning and priority preemption
(:func:`dispatch_pending_jobs`), the 30-second catch-all loop that ticks it
(:func:`auto_assign_dispatcher`), and the event-driven fire-and-forget trigger
(:func:`trigger_dispatch`).

The application owns exactly one :class:`JobDispatchState` — the dispatch lock
that serializes every pass (no double-assignment inside one replica) and the
pause-pending set it shares with job-control delivery, which discards a job id
once its pause settles. The application rebuilds :class:`JobDispatchDependencies`
from its composition on every call rather than handing this module a captured
snapshot, so late-bound collaborators (the store, the completion-control
boundary, patched provisioners) are read at the moment a pass runs, exactly as
the former module globals were.

Task creation and leader gating stay in the application lifecycle: the
application starts :func:`auto_assign_dispatcher` as its background task, and
:func:`trigger_dispatch` keeps its leadership gate. The passes it starts and
the pauses preemption initiates are tracked on the application's
:class:`JobDispatchState` and drained at shutdown (R1.B11 correction). Nothing here imports the
application module or looks collaborators up from a global.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Coroutine

from fastapi import HTTPException

from orchestrator.security.access import vm_workspaces_on_pod_network
from orchestrator.services.dispatch_guards import (
    VM_CAPACITY_POLL,
    VM_GOLDEN_POLL,
    VM_HEADSCALE_POLL,
    VM_PARK_EXHAUSTED,
    VM_PARK_GOLDEN,
    VM_PARK_HEADSCALE,
    VM_PARK_INITIALIZATION,
    VM_PARK_PREPARATION,
    VM_PARKED,
    VM_PREPARATION_POLL,
    VM_PROVISION,
    VM_READY,
    preemption_blocked_reason,
    resume_lane_applies,
    vm_provisioning_decision,
)
from orchestrator.services.job_workspace_runtime import (
    get_container_context as _get_container_context,
)
from orchestrator.services.job_workspace_runtime import (
    get_vm_context as _get_vm_context,
)
from orchestrator.services.job_workspace_runtime import job_needs_vm as _job_needs_vm
from orchestrator.services.job_workspace_runtime import (
    scholar_provision_parent_id as _scholar_provision_parent_id,
)
from orchestrator.services.session_runtime_identity import (
    agent_sha_is_current as _agent_sha_is_current,
)
from orchestrator.services.vm_creation_dispatch import handle_creation_pending
from orchestrator.services.vm_provisioning_cleanup import handle_provisioning_wait
from orchestrator.services.vm_workspace_config import vm_provisioning_options
from orchestrator.services.vm_workspace_recovery_store import (
    VMWorkspaceRecoveryStore,
)
from orchestrator.services.workspace_lifecycle import (
    EnsureOutcome,
    WorkspaceOwner,
    ensure_workspace,
)
from shared.runtime.core.loader import canonical_config_name
from shared.workspace_contract import resolve_workspace_runtime

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class JobDispatchState:
    """Serialization state one application's dispatcher owns.

    ``tasks`` holds the dispatch passes :func:`trigger_dispatch` starts and the
    preemption pauses a pass initiates. They are the application's tasks, not
    anonymous ones: a strong reference keeps each alive until it finishes, and
    the application drains them on shutdown before its pools close.
    """

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    pause_pending_job_ids: set[str] = field(default_factory=set)
    tasks: set[asyncio.Task[Any]] = field(default_factory=set)

    def spawn(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        """Run ``coro`` as one of this dispatcher's tracked tasks."""

        task = asyncio.create_task(coro)
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return task

    async def drain(self) -> None:
        """Cancel and await every tracked dispatch pass and preemption.

        Called by the application after its background loops stopped (so
        leadership, which gates new triggers, is already released) and before
        its stores close. A pass cancelled after its claim leaves the job to
        the existing orphan/lease recovery, exactly like a pod dying there.
        """

        pending = list(self.tasks)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


@dataclass(frozen=True, slots=True)
class JobDispatchDependencies:
    """Per-invocation collaborators for one dispatch pass.

    ``state`` is the application's single :class:`JobDispatchState`; its
    ``pause_pending_job_ids`` must be the same set object job-control delivery
    discards from. ``job_delivery_operations`` and
    ``manifest_execution_service`` are providers, called where ``main`` called
    its composition functions, so each call composes fresh operations.
    """

    state: JobDispatchState
    store: Any
    completion_control_boundary: Any
    agent_provisioner: Any
    vm_provisioner: Any
    container_provisioner: Any
    docker_provisioner: Any
    workspace_suspension: Any
    auto_assign_enabled: bool
    stateless_worker_enabled: bool
    manifest_execution_service: Callable[[], Any]
    prepare_job_workspace_runtime: Callable[..., Awaitable[Any]]
    fail_subjob_and_unblock_parent: Callable[..., Awaitable[Any]]
    check_vm_permission: Callable[..., Awaitable[Any]]
    fail_vm_parked_job: Callable[..., Awaitable[Any]]
    job_needs_sandbox: Callable[[Any], bool]
    provision_parent_workspace_for_scholar: Callable[..., Awaitable[Any]]
    prepare_job_repository_before_claim: Callable[..., Awaitable[bool]]
    job_delivery_operations: Callable[[], Any]


async def dispatch_pending_jobs(*, dependencies: JobDispatchDependencies) -> None:
    """Core dispatcher: match pending jobs to available agents.

    Phase 1: Direct assignment (free agents → highest priority pending jobs)
    Phase 2: Preemption (remaining high-priority jobs → lowest-priority running jobs)

    VM-aware: jobs needing a VM are auto-provisioned and held until the VM
    registers as ready. Jobs with a ready VM get workspace config injected
    into config_override before dispatch.
    """
    if (
        getattr(dependencies.store, "manifests_ready", False) is True
        and dependencies.agent_provisioner._k8s_available
    ):
        await dependencies.manifest_execution_service().reconcile()
    if (
        not dependencies.auto_assign_enabled
        and not dependencies.stateless_worker_enabled
    ):
        return

    async with dependencies.state.lock:
        try:
            # Both lanes retain the same leader-owned workspace preflight. The
            # pinned set proceeds to registered-agent matching; a ready
            # stateless set is admitted to run_queue and never reaches that
            # half of this function.
            pending_jobs = (
                await dependencies.store.get_dispatchable_jobs(
                    limit=50,
                    **dependencies.completion_control_boundary.dispatch_guard_kwargs(),
                )
                if dependencies.auto_assign_enabled
                else []
            )
            if dependencies.stateless_worker_enabled:
                pending_jobs.extend(
                    await dependencies.store.get_admittable_stateless_jobs(
                        limit=50,
                        **dependencies.completion_control_boundary.dispatch_guard_kwargs(),
                    )
                )
            if not pending_jobs:
                return

            # Pre-filter: auto-provision VMs/containers for jobs that need one
            dispatchable_jobs = []
            for job in pending_jobs:
                job_id = str(job["id"])
                stateless_worker = job.get("execution_lane") == "stateless"
                if await handle_creation_pending(job, db=dependencies.store):
                    continue
                (
                    workspace_action,
                    job,
                    workspace_recovery_reason,
                ) = await dependencies.prepare_job_workspace_runtime(job)
                if workspace_action == "wait":
                    logger.warning(
                        "Dispatcher: job %s waiting for live workspace authority "
                        "recovery (%s)",
                        job_id,
                        workspace_recovery_reason or "retryable",
                    )
                    continue
                if workspace_action == "fail":
                    await dependencies.fail_subjob_and_unblock_parent(
                        job,
                        workspace_recovery_reason
                        or "Inherited workspace authority is unavailable.",
                    )
                    continue
                workspace_decision = resolve_workspace_runtime(
                    job, vm_mode=dependencies.vm_provisioner.mode
                )
                if (
                    workspace_decision.contract is None
                    or workspace_decision.state == "invalid"
                ):
                    logger.error(
                        "Dispatcher: refusing job %s with invalid workspace "
                        "authority (%s)",
                        job_id,
                        workspace_decision.reason or workspace_decision.state,
                    )
                    await dependencies.store.update_job_status(
                        job_id,
                        status="failed",
                        error_message=(
                            "Workspace contract is ambiguous or invalid; "
                            "refusing dispatch"
                        ),
                        expected_status=str(job.get("status")),
                    )
                    continue

                job_needs_vm = _job_needs_vm(job)
                stateless_same_cluster_vm = bool(
                    stateless_worker and job_needs_vm and vm_workspaces_on_pod_network()
                )

                # Defense for inherited/operator-created rows. External VMs
                # still belong to the mesh-enabled registered-agent plane; a
                # same-cluster VM remains on the pool lane below.
                if (
                    stateless_worker
                    and job_needs_vm
                    and not vm_workspaces_on_pod_network()
                ):
                    moved_to_pinned = False
                    async with dependencies.store.acquire() as conn:
                        async with conn.transaction():
                            await conn.fetchrow(
                                "SELECT state FROM run_queue "
                                "WHERE unit_id = $1::uuid "
                                "AND unit_kind = 'worker_batch' FOR UPDATE",
                                job_id,
                            )
                            moved = await conn.fetchrow(
                                "UPDATE jobs SET execution_lane = 'pinned', "
                                "updated_at = CURRENT_TIMESTAMP "
                                "WHERE id = $1::uuid "
                                "AND execution_lane = 'stateless' "
                                "AND status::text = $2::text "
                                "RETURNING id",
                                job_id,
                                str(job.get("status") or ""),
                            )
                            if moved is not None:
                                moved_to_pinned = True
                                await conn.execute(
                                    "UPDATE run_queue SET state = 'done', "
                                    "lease_token = lease_token + 1, "
                                    "leased_by = NULL, last_leased_by = NULL, "
                                    "leased_until = NULL, run_after = now(), "
                                    "queued_at = now() "
                                    "WHERE unit_id = $1::uuid "
                                    "AND unit_kind = 'worker_batch'",
                                    job_id,
                                )
                    if moved_to_pinned:
                        logger.info(
                            "Dispatcher: moved VM job %s from stateless to pinned lane",
                            job_id,
                        )
                    else:
                        logger.debug(
                            "Dispatcher: VM lane repair lost the status CAS for job %s",
                            job_id,
                        )
                    continue
                _stateless_container = _get_container_context(job)
                _stateless_has_k8s_workspace = (
                    _stateless_container.get("status") == "ready"
                    and _stateless_container.get("provisioner") == "k8s"
                    and bool(
                        _stateless_container.get("host")
                        or _stateless_container.get("pod_ip")
                    )
                )
                if stateless_worker and not (
                    stateless_same_cluster_vm
                    or dependencies.job_needs_sandbox(job)
                    or _stateless_has_k8s_workspace
                ):
                    logger.error(
                        "Dispatcher: refusing stateless job %s without a compatible "
                        "workspace",
                        job_id,
                    )
                    await dependencies.store.update_job_status(
                        job_id,
                        status="failed",
                        error_message=(
                            "Stateless workers currently require a Kubernetes "
                            "sandbox or same-cluster VM workspace"
                        ),
                        expected_status=str(job.get("status")),
                    )
                    continue
                if (
                    stateless_worker
                    and not stateless_same_cluster_vm
                    and not (
                        dependencies.container_provisioner.is_available
                        and dependencies.container_provisioner.in_cluster
                    )
                ):
                    logger.warning(
                        "Dispatcher: stateless job %s waiting for the in-cluster "
                        "Kubernetes workspace provisioner",
                        job_id,
                    )
                    continue

                if job_needs_vm:
                    # Admin-gated permission check (kill-switch + per-user grant).
                    # Re-verified here in case a grant was revoked or the
                    # kill-switch flipped after the job was submitted. Already
                    # running VMs aren't torn down by this check — the gate
                    # only blocks jobs that haven't been dispatched yet.
                    creator = None
                    creator_id = job.get("user_id")
                    if creator_id:
                        try:
                            creator = await dependencies.store.get_user(str(creator_id))
                        except Exception:
                            creator = None
                    try:
                        await dependencies.check_vm_permission(
                            creator, job_needs_vm=True
                        )
                    except HTTPException as permission_error:
                        logger.error(
                            "Dispatcher: job %s denied VM workspace: %s",
                            job_id,
                            permission_error.detail,
                        )
                        await dependencies.store.update_job_status(
                            job_id,
                            status="failed",
                            error_message=str(permission_error.detail),
                        )
                        continue
                    vm_ctx = _get_vm_context(job)
                    # Bounded provisioning retries. A VM that never reaches 'ready'
                    # (real infra failure) must park after N attempts instead of
                    # re-provisioning forever against the shared VM cluster. The
                    # counter records one exact admitted VM per generation and
                    # resets atomically with verified Ready, so it survives async
                    # status callbacks that a status-based park cannot. Decision
                    # logic is extracted + unit-tested in dispatch_guards.
                    provision_attempts = int(vm_ctx.get("provision_attempts") or 0)
                    max_provision_attempts = int(
                        os.environ.get("VM_PROVISION_MAX_ATTEMPTS", "3")
                    )
                    timeout_s = int(os.environ.get("VM_PROVISION_TIMEOUT_S", "600"))
                    rootdisk_stall_timeout_s = int(
                        os.environ.get("VM_ROOTDISK_STALL_TIMEOUT_S", "2700")
                    )
                    golden_timeout_s = int(
                        os.environ.get("VM_GOLDEN_WAIT_TIMEOUT_S", "2700")
                    )
                    headscale_timeout_s = int(
                        os.environ.get("VM_HEADSCALE_WAIT_TIMEOUT_S", "900")
                    )
                    vm_decision = vm_provisioning_decision(
                        vm_ctx,
                        provision_attempts=provision_attempts,
                        max_provision_attempts=max_provision_attempts,
                        now=time.time(),
                        timeout_s=timeout_s,
                        rootdisk_stall_timeout_s=rootdisk_stall_timeout_s,
                        golden_timeout_s=golden_timeout_s,
                        headscale_timeout_s=headscale_timeout_s,
                    )
                    if vm_decision == VM_PARK_EXHAUSTED:
                        # Retries used up — park the VM context AND fail the job.
                        # 'failed' is terminal for the dispatcher (VM_PARKED) and
                        # skipped by the reconciler (_PARKED_VM_STATUSES), so the
                        # park holds. The job itself must go terminal too: leaving
                        # it 'created' with nothing scheduled to change its state
                        # is an invisible wedge (a loop's current_job never turns
                        # terminal → the loop stalls forever). See knowledge-base/knowledge/issues/
                        # vm_ssh_readiness_probe_unroutable_from_orchestrator.md.
                        park_error = (
                            f"provisioning exhausted after "
                            f"{provision_attempts} attempts "
                            f"(never reached 'ready')"
                        )
                        logger.warning(
                            "Dispatcher: job %s VM provisioning exhausted "
                            "(%d/%d attempts) — failing job",
                            job_id,
                            provision_attempts,
                            max_provision_attempts,
                        )
                        await dependencies.store.merge_vm_context(
                            job_id,
                            {"status": "failed", "error": park_error},
                        )
                        await dependencies.fail_vm_parked_job(job_id, park_error)
                        continue
                    if vm_decision == VM_PROVISION:
                        # VM needed but absent — never provisioned, torn down while
                        # parked ('deleted': deploy-drain / crash recovery release
                        # it; work survives in the pushed branch + checkpoint), or
                        # recycled by the timeout. Without re-provisioning here a
                        # paused VM job waits forever on a VM nothing will create.
                        if not dependencies.vm_provisioner.is_available:
                            # VM explicitly requested but no provisioner — fail
                            logger.error(
                                "Dispatcher: job %s requires VM workspace but VM "
                                "provisioner is unavailable for VM_MODE=%s. "
                                "Failing job.",
                                job_id,
                                dependencies.vm_provisioner.mode,
                            )
                            await dependencies.store.update_job_status(
                                job_id,
                                status="failed",
                                error_message=(
                                    "VM workspace requested but VM provisioner is not "
                                    f"available for VM_MODE={dependencies.vm_provisioner.mode!r}. "
                                    "Configure VM_MODE and its required controller, use "
                                    "workspace.backend='container', or "
                                    "remove the explicit backend override."
                                ),
                            )
                            continue
                        config_override = job.get("config_override") or {}
                        if isinstance(config_override, str):
                            config_override = json.loads(config_override)
                        vm_options = await vm_provisioning_options(
                            dependencies.store, "Job", job, fallback=config_override
                        )
                        ok = await dependencies.vm_provisioner.create_vm(
                            job_id=job_id,
                            agent_config=canonical_config_name(
                                job.get("config_name", "worker_base")
                            ),
                            **vm_options,
                            description=job.get("description", ""),
                        )
                        protocol_pending = (
                            isinstance(ok, dict)
                            and type(ok.get("creation_retry_protocol")) is int
                            and ok["creation_retry_protocol"] == 1
                        )
                        if protocol_pending:
                            # Exact VM adoption counts a boot in the ledger;
                            # scheduling and dependency waits count no attempt.
                            continue
                        if not ok:
                            logger.warning(
                                "Dispatcher: VM provisioning failed for job %s", job_id
                            )
                        # Authenticated phase observation accounts admission;
                        # a create acknowledgement or dependency wait does not.
                        continue  # Skip this job — wait for VM to register
                    if vm_decision == VM_PARKED:
                        # Provisioning failed terminally — do NOT hot-retry every
                        # tick (shared VM cluster). The job is still non-terminal
                        # (it's in the dispatchable list), which means something
                        # left it parked-but-alive: an older build's park, or a
                        # controller-callback race with PARK_EXHAUSTED. Heal it
                        # to 'failed' so it stops wedging its loop.
                        vm_error = vm_ctx.get("error") or "VM provisioning failed"
                        logger.warning(
                            "Dispatcher: job %s VM parked (%s) — failing job",
                            job_id,
                            vm_error,
                        )
                        await dependencies.fail_vm_parked_job(job_id, vm_error)
                        continue
                    if vm_decision == VM_PARK_PREPARATION:
                        park_error = (
                            "Workspace preparation did not complete within its deadline"
                        )
                        await dependencies.store.merge_vm_context(
                            job_id, {"status": "failed", "error": park_error}
                        )
                        await dependencies.fail_vm_parked_job(job_id, park_error)
                        continue
                    if vm_decision in (
                        VM_GOLDEN_POLL,
                        VM_CAPACITY_POLL,
                        VM_PREPARATION_POLL,
                    ):
                        # No VM exists yet — the controller is waiting on a
                        # shared golden-image import (cold import after an
                        # agent-vm-base bump: ~30 min, longer than timeout_s).
                        # Re-issue create as the poll: the controller answers
                        # waiting_golden (cheap DV GET) until the golden is
                        # Succeeded, then actually builds the VM. fresh=False
                        # keeps the golden budget anchor + counters and does
                        # NOT consume a provision attempt — the attempt budget
                        # bounds VM boots, and no boot is happening. See
                        # knowledge-history/done/
                        # golden_image_cold_import_fails_inflight_vm_jobs.md.
                        wait_anchor = (
                            "preparation_wait_started_at"
                            if vm_decision == VM_PREPARATION_POLL
                            else "capacity_wait_started_at"
                            if vm_decision == VM_CAPACITY_POLL
                            else "golden_wait_started_at"
                        )
                        if not vm_ctx.get(wait_anchor):
                            await dependencies.store.merge_vm_context(
                                job_id,
                                {wait_anchor: time.time()},
                            )
                        config_override = job.get("config_override") or {}
                        if isinstance(config_override, str):
                            config_override = json.loads(config_override)
                        vm_options = await vm_provisioning_options(
                            dependencies.store, "Job", job, fallback=config_override
                        )
                        await dependencies.vm_provisioner.create_vm(
                            job_id=job_id,
                            agent_config=canonical_config_name(
                                job.get("config_name", "worker_base")
                            ),
                            **vm_options,
                            description=job.get("description", ""),
                            fresh=False,
                        )
                        if vm_decision == VM_PREPARATION_POLL:
                            logger.info(
                                "Dispatcher: job %s waiting on workspace preparation",
                                job_id,
                            )
                        elif vm_decision == VM_CAPACITY_POLL:
                            logger.info(
                                "Dispatcher: job %s waiting on VM capacity (%s/%s) "
                                "— polling",
                                job_id,
                                vm_ctx.get("running_vms") or "?",
                                vm_ctx.get("max_concurrent_vms") or "?",
                            )
                        else:
                            logger.info(
                                "Dispatcher: job %s waiting on golden image %s "
                                "(%s) — polling",
                                job_id,
                                vm_ctx.get("golden") or "?",
                                vm_ctx.get("golden_progress")
                                or vm_ctx.get("golden_phase")
                                or "importing",
                            )
                        continue
                    if vm_decision == VM_PARK_GOLDEN:
                        # The golden import outlived even the golden budget —
                        # CDI is wedged or the registry is unreachable. No VM
                        # was ever created, so there is nothing to recycle;
                        # fail the job with the truth (not the misleading
                        # "provisioning exhausted after N attempts").
                        elapsed = int(
                            time.time()
                            - float(vm_ctx.get("golden_wait_started_at") or 0)
                        )
                        park_error = (
                            f"golden image import did not complete within "
                            f"{golden_timeout_s}s (golden "
                            f"{vm_ctx.get('golden') or 'unknown'}, last progress "
                            f"{vm_ctx.get('golden_progress') or 'unknown'}, "
                            f"waited {elapsed}s) — VM never created"
                        )
                        logger.warning(
                            "Dispatcher: job %s golden wait exhausted — "
                            "failing job (%s)",
                            job_id,
                            park_error,
                        )
                        await dependencies.store.merge_vm_context(
                            job_id,
                            {"status": "failed", "error": park_error},
                        )
                        await dependencies.fail_vm_parked_job(job_id, park_error)
                        continue
                    if vm_decision == VM_HEADSCALE_POLL:
                        # No VM exists yet — the controller refused to build one
                        # while Headscale is unreachable, because a VM with no
                        # tailnet pre-auth key boots and heartbeats but is never
                        # reachable over SSH. Poll create (fresh=False, no
                        # attempt consumed) until the mesh recovers; the
                        # controller then builds the VM on the very next poll.
                        if not vm_ctx.get("headscale_wait_started_at"):
                            await dependencies.store.merge_vm_context(
                                job_id,
                                {"headscale_wait_started_at": time.time()},
                            )
                        config_override = job.get("config_override") or {}
                        if isinstance(config_override, str):
                            config_override = json.loads(config_override)
                        vm_options = await vm_provisioning_options(
                            dependencies.store, "Job", job, fallback=config_override
                        )
                        await dependencies.vm_provisioner.create_vm(
                            job_id=job_id,
                            agent_config=canonical_config_name(
                                job.get("config_name", "worker_base")
                            ),
                            **vm_options,
                            description=job.get("description", ""),
                            fresh=False,
                        )
                        logger.info(
                            "Dispatcher: job %s waiting on Headscale (%s) — polling",
                            job_id,
                            vm_ctx.get("headscale_error") or "mesh VPN unavailable",
                        )
                        continue
                    if vm_decision == VM_PARK_HEADSCALE:
                        # Headscale never came back inside its budget. No VM was
                        # ever created, so there is nothing to recycle — fail
                        # with the real cause rather than the misleading
                        # "provisioning exhausted after N attempts".
                        elapsed = int(
                            time.time()
                            - float(vm_ctx.get("headscale_wait_started_at") or 0)
                        )
                        park_error = (
                            f"Headscale (mesh VPN) unavailable for {elapsed}s "
                            f"(budget {headscale_timeout_s}s, last error: "
                            f"{vm_ctx.get('headscale_error') or 'unknown'}) — "
                            f"VM never created"
                        )
                        logger.warning(
                            "Dispatcher: job %s Headscale wait exhausted — "
                            "failing job (%s)",
                            job_id,
                            park_error,
                        )
                        await dependencies.store.merge_vm_context(
                            job_id,
                            {"status": "failed", "error": park_error},
                        )
                        await dependencies.fail_vm_parked_job(job_id, park_error)
                        continue
                    if vm_decision == VM_PARK_INITIALIZATION:
                        park_error = "Workspace initialization did not complete within its deadline"
                        await dependencies.store.merge_vm_context(
                            job_id, {"status": "failed", "error": park_error}
                        )
                        await dependencies.fail_vm_parked_job(job_id, park_error)
                        continue
                    if await handle_provisioning_wait(
                        vm_decision,
                        job_id,
                        vm_ctx,
                        db=dependencies.store,
                        provisioner=dependencies.vm_provisioner,
                        recovery_store=VMWorkspaceRecoveryStore(dependencies.store),
                        now=time.time(),
                        boot_timeout_s=timeout_s,
                        rootdisk_stall_timeout_s=rootdisk_stall_timeout_s,
                    ):
                        continue
                    if vm_decision != VM_READY:
                        logger.warning(
                            "Dispatcher: job %s has an unhandled VM decision", job_id
                        )
                        continue
                    # VM_READY: proceed with dispatch.
                    logger.info("Dispatcher: job %s using VM workspace", job_id)
                elif dependencies.job_needs_sandbox(job):
                    # Phase 1: a pre-agent scholar spawned before its parent had a
                    # workspace provisions the parent's ONE shared pod under the
                    # parent's identity and rides it, instead of self-provisioning
                    # a throwaway pod. k8s only — VM/docker parents fall through to
                    # the normal self-provision path below.
                    provision_parent_id = _scholar_provision_parent_id(job)
                    if (
                        provision_parent_id
                        and dependencies.container_provisioner.is_available
                        and dependencies.container_provisioner.in_cluster
                    ):
                        await dependencies.provision_parent_workspace_for_scholar(
                            job, provision_parent_id
                        )
                        # wait → retry next tick; promoted → dispatches next tick
                        # via the inherit path; fail → already failed + unblocked.
                        continue
                    container_ctx = _get_container_context(job)
                    container_status = container_ctx.get("status")
                    # K8s in-cluster takes priority; a local kubeconfig must not
                    # shadow Docker Compose when running outside the cluster.
                    use_k8s = dependencies.container_provisioner.is_available and (
                        dependencies.container_provisioner.in_cluster
                        or not dependencies.docker_provisioner.is_available
                    )
                    # States that mean "no live workspace yet" → (re)create.
                    needs_create = container_status in (None, "", "deleted", "none")
                    if needs_create and not use_k8s:
                        # Docker Compose pool / no-provisioner CREATE path (unchanged).
                        if dependencies.docker_provisioner.is_available:
                            logger.info(
                                "Dispatcher: job %s assigning workspace from "
                                "Docker Compose pool",
                                job_id,
                            )
                            result = (
                                await dependencies.docker_provisioner.assign_workspace(
                                    job_id
                                )
                            )
                            if not result:
                                logger.warning(
                                    "Dispatcher: no free workspace for job %s "
                                    "— all containers occupied, will retry",
                                    job_id,
                                )
                        else:
                            logger.error(
                                "Dispatcher: job %s needs workspace but no "
                                "provisioner available. Failing job.",
                                job_id,
                            )
                            await dependencies.store.update_job_status(
                                job_id,
                                status="failed",
                                error_message=(
                                    "No workspace provisioner available. "
                                    "Neither Kubernetes API nor WORKSPACE_HOSTS "
                                    "configured."
                                ),
                            )
                        continue  # Skip — wait for container to become ready
                    # K8s create (when status absent) + all lifecycle states route
                    # through the shared, owner-agnostic state machine.
                    config_override = job.get("config_override") or {}
                    if isinstance(config_override, str):
                        config_override = json.loads(config_override)
                    ws_cfg = config_override.get("workspace", {}).get("container", {})
                    res = await ensure_workspace(
                        WorkspaceOwner.job(job_id),
                        provisioner=dependencies.container_provisioner,
                        suspension=dependencies.workspace_suspension,
                        current_status=container_status,
                        ws_config={
                            k: ws_cfg[k]
                            for k in (
                                "cpu",
                                "memory",
                                "cpu_limit",
                                "memory_limit",
                                "image",
                            )
                            if k in ws_cfg
                        },
                    )
                    if res.outcome is EnsureOutcome.FAILED:
                        failed_ctx = container_ctx
                        if container_status != "failed":
                            # create_workspace records the concrete failure in
                            # context before returning False. Refresh once so a
                            # first-attempt auth/RBAC/image failure reaches the
                            # job error instead of being replaced by the generic
                            # "could not be created" wrapper.
                            try:
                                refreshed_job = await dependencies.store.get_job(job_id)
                                if refreshed_job:
                                    failed_ctx = _get_container_context(refreshed_job)
                            except Exception:
                                logger.warning(
                                    "Dispatcher: could not refresh failed workspace "
                                    "context for job %s",
                                    job_id,
                                    exc_info=True,
                                )
                        error = failed_ctx.get("error")
                        if error:
                            msg = f"Workspace container failed: {error}"
                        else:
                            msg = (
                                "Workspace container could not be created. Check "
                                "orchestrator logs for details (image pull failures, "
                                "insufficient resources, RBAC issues)."
                            )
                        logger.error(
                            "Dispatcher: workspace ensure failed for job %s: %s. "
                            "Failing job.",
                            job_id,
                            msg,
                        )
                        await dependencies.store.update_job_status(
                            job_id,
                            status="failed",
                            error_message=msg,
                            expected_status=(
                                str(job.get("status")) if stateless_worker else None
                            ),
                        )
                        continue
                    if res.outcome is EnsureOutcome.PENDING:
                        if container_status not in (
                            None,
                            "",
                            "deleted",
                            "none",
                            "created",
                            "creating",
                            "restoring",
                            "suspending",
                            "pending",
                        ):
                            logger.warning(
                                "Dispatcher: job %s has unexpected workspace "
                                "container status %r — waiting",
                                job_id,
                                container_status,
                            )
                        continue  # in progress — wait for next cycle
                    # READY → proceed with dispatch
                    logger.info("Dispatcher: job %s using workspace container", job_id)
                else:
                    # No VM or container provisioning needed — check if a workspace
                    # was already assigned (e.g. Docker provisioner assigned it on a
                    # previous cycle and the job is now ready for dispatch).
                    existing_ctx = _get_container_context(job)
                    if existing_ctx.get("status") == "ready":
                        logger.info(
                            "Dispatcher: job %s using pre-assigned workspace",
                            job_id,
                        )
                    else:
                        logger.debug(
                            "Dispatcher: job %s — no workspace provisioner needed",
                            job_id,
                        )
                if not await dependencies.prepare_job_repository_before_claim(job):
                    # Retry on the next dispatcher tick. A Gitea/SSH outage is
                    # not a worker failure and must not make the job cross the
                    # processing boundary with unproven repository authority.
                    continue
                if stateless_worker:
                    (
                        admitted,
                        queue_result,
                    ) = await dependencies.store.admit_stateless_worker_job(
                        job_id,
                        fair_key=(str(job["user_id"]) if job.get("user_id") else None),
                        priority=int(job.get("priority") or 0),
                        allow_vm_workspace=vm_workspaces_on_pod_network(),
                        **dependencies.completion_control_boundary.dispatch_guard_kwargs(),
                    )
                    if not admitted:
                        logger.warning(
                            "Dispatcher: stateless admission CAS lost for job %s; "
                            "workspace/lane/status changed after preflight",
                            job_id,
                        )
                        continue
                    logger.info(
                        "Dispatcher: admitted stateless worker job %s "
                        "(queue=%s, workspace=k8s-ready)",
                        job_id,
                        queue_result,
                    )
                    continue
                dispatchable_jobs.append(job)

            if not dispatchable_jobs:
                return

            # Get available agents (ready, cooldown passed), skip stale images
            all_agents = await dependencies.store.get_available_agents(limit=50)
            available_agents = []
            for ag in all_agents:
                meta = ag.get("metadata") or {}
                if isinstance(meta, str):
                    try:
                        meta = json.loads(meta)
                    except (json.JSONDecodeError, ValueError):
                        meta = {}
                if _agent_sha_is_current(meta):
                    available_agents.append(ag)
                else:
                    # Stale-SHA agents are skipped here; the lifecycle
                    # reconciler is responsible for draining them.
                    logger.debug(
                        "Skipping stale worker agent %s (build_sha=%s)",
                        ag["id"],
                        meta.get("build_sha", ""),
                    )

            # Phase 1: Direct assignment
            matched_job_ids = set()
            matched_agent_ids = set()

            agents_iter = iter(available_agents)
            for job in dispatchable_jobs:
                agent = next(agents_iter, None)
                if agent is None:
                    break  # No more free agents

                job_id = str(job["id"])
                # Atomically claim the job for this agent BEFORE notifying the
                # pod. Closes the dual-leader double-assign that leader election
                # cannot fence (M1): two transient leaders may both scan the same
                # candidate, but only one CAS wins — the loser skips. The claim
                # sets status='processing'+assigned_agent_id; a failed
                # dispatch/resume below self-heals via recover_orphaned_jobs.
                if not await dependencies.store.claim_job_for_agent(
                    job_id,
                    str(agent["id"]),
                    **dependencies.completion_control_boundary.dispatch_guard_kwargs(),
                ):
                    logger.debug(
                        "Dispatcher: job %s already claimed by another replica; skipping",
                        job_id,
                    )
                    continue
                if resume_lane_applies(
                    job,
                    has_checkpoint=await dependencies.store.job_has_checkpoint(job_id),
                ):
                    success = await dependencies.job_delivery_operations().resume(
                        job, agent
                    )
                else:
                    if job["status"] == "paused":
                        logger.info(
                            "Dispatcher: job %s is paused with no checkpoint "
                            "to resume from (never started, or pruned at a "
                            "terminal state) — dispatching via the fresh "
                            "/job/start lane",
                            job_id,
                        )
                    success = await dependencies.job_delivery_operations().dispatch(
                        job, agent
                    )

                if success:
                    matched_job_ids.add(job_id)
                    matched_agent_ids.add(str(agent["id"]))

            # Phase 1.5: Provision agent pods for unmatched jobs (K8s only)
            remaining = [
                j for j in dispatchable_jobs if str(j["id"]) not in matched_job_ids
            ]
            if remaining and dependencies.agent_provisioner.is_available:
                for job in remaining:
                    if (
                        await dependencies.agent_provisioner.active_count()
                        >= dependencies.agent_provisioner.max_agents
                    ):
                        break
                    pod_name = await dependencies.agent_provisioner.provision_agent(
                        purpose="job"
                    )
                    if pod_name:
                        logger.info(
                            "Provisioned agent %s for pending job %s",
                            pod_name,
                            str(job["id"]),
                        )
                    else:
                        break  # At capacity or error
                    # Don't assign yet — pod needs to register first.
                    # Agent heartbeats "ready" → _trigger_dispatch() → next
                    # cycle matches it.

            # Phase 2: Preemption (non-blocking). D1 placeability guard: only
            # workspace-ready jobs (dispatchable_jobs, not the full pending set)
            # may drive preemption — pausing a running job to free an agent is
            # pointless for a job that has no workspace to run in.
            remaining = [
                j for j in dispatchable_jobs if str(j["id"]) not in matched_job_ids
            ]
            if not remaining:
                return

            candidates = await dependencies.store.get_preemption_candidates()
            if not candidates:
                return

            for pending_job in remaining:
                pending_priority = pending_job.get("priority", 5)
                pending_job_id = str(pending_job["id"])

                # D1 Guard 2: a verification/critic subjob whose parent is
                # already terminal can never run, so it must not preempt — even
                # if it slipped past Guard 1 by inheriting a (now-dead) parent
                # workspace. Costs one lookup, only for sub-jobs reaching Phase 2.
                parent_status = None
                parent_id = pending_job.get("parent_job_id")
                if parent_id:
                    parent = await dependencies.store.get_job(str(parent_id))
                    parent_status = parent.get("status") if parent else None
                block_reason = preemption_blocked_reason(pending_job, parent_status)
                if block_reason:
                    logger.warning(
                        "Preempt: skipping pending job %s — %s",
                        pending_job_id,
                        block_reason,
                    )
                    continue

                # Find lowest-priority running job that can be preempted
                for candidate in candidates:
                    candidate_id = str(candidate["id"])
                    candidate_priority = candidate.get("priority", 5)

                    # Only preempt if strictly higher priority
                    if pending_priority <= candidate_priority:
                        continue

                    # Skip if already being paused
                    if candidate_id in dependencies.state.pause_pending_job_ids:
                        continue

                    # Skip if already matched (agent taken)
                    if str(candidate.get("assigned_agent_id", "")) in matched_agent_ids:
                        continue

                    # Initiate preemption (fire-and-forget)
                    dependencies.state.pause_pending_job_ids.add(candidate_id)
                    dependencies.state.spawn(
                        dependencies.job_delivery_operations().initiate_pause(candidate)
                    )
                    logger.info(
                        f"Preempt: pausing job {candidate_id} (priority={candidate_priority}) "
                        f"for pending job {pending_job_id} (priority={pending_priority})"
                    )
                    # Remove this candidate so it's not preempted again in this cycle
                    candidates.remove(candidate)
                    break  # One preemption per pending job per cycle

        except Exception as e:
            logger.error(f"Dispatcher error: {e}", exc_info=True)


async def auto_assign_dispatcher(
    shutdown_event: asyncio.Event, *, dependencies: JobDispatchDependencies
) -> None:
    """Background task that periodically dispatches pending jobs to available agents.

    Runs every 30 seconds as a catch-all. Event-driven triggers (job creation,
    agent heartbeat) also call dispatch_pending_jobs() for faster response.
    """
    logger.info(
        "Auto-assign dispatcher started (pinned=%s, stateless_workers=%s)",
        dependencies.auto_assign_enabled,
        dependencies.stateless_worker_enabled,
    )
    while not shutdown_event.is_set():
        try:
            await dispatch_pending_jobs(dependencies=dependencies)
        except Exception as e:
            logger.error(f"Error in auto-assign dispatcher: {e}")

        # Wait 30 seconds or until shutdown
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=30.0)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("Auto-assign dispatcher stopped")


def trigger_dispatch(*, dependencies: JobDispatchDependencies) -> None:
    """Fire-and-forget trigger for the dispatcher. Safe to call from any endpoint.

    Gated on leadership (M1): only the elected leader dispatches, so a job
    created via a REST handler on a non-leader replica is picked up by the
    leader's periodic dispatcher loop rather than dispatched here.
    """
    from orchestrator.services.leader_election import is_leader

    if (
        dependencies.auto_assign_enabled or dependencies.stateless_worker_enabled
    ) and is_leader.is_set():
        dependencies.state.spawn(dispatch_pending_jobs(dependencies=dependencies))
