"""Application-owned pending Job preflight and assignment.

Workspace preparation runs in a bounded, Job/owner-keyed set: two mutations
and two independent ready checks. Discovery and assignment retain the shared
application lock; neither waits for a workspace create or mutation guard.
Completed preparation is only a candidate for current-row checks and the
existing database claim/admission and delivery authority.

Triggers coalesce into one runner. The leader loop drains its preflights when
it stops; application shutdown drains every owned task before closing stores.
No in-memory scheduler state grants continuation of a pending creation after
restart. Existing durable reservation and runtime fences remain authoritative.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Coroutine

from fastapi import HTTPException

from orchestrator.database.dispatch_discovery import JobDiscoveryCursor, discovery_order
from orchestrator.security.access import vm_workspaces_on_pod_network
from orchestrator.services.container_provisioner import WorkspaceContainerExitedError
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
from orchestrator.services.job_workspace_authority import project_inherited_workspace
from orchestrator.services.job_workspace_runtime import (
    get_container_context as _get_container_context,
)
from orchestrator.services.job_workspace_runtime import (
    get_vm_context as _get_vm_context,
)
from orchestrator.services.job_workspace_runtime import job_needs_vm as _job_needs_vm
from orchestrator.services.job_workspace_runtime import stateless_worker_workspace_owner
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
from shared.workspace_contract import workspace_runtime_authority_digest

logger = logging.getLogger(__name__)

MUTATION_PREFLIGHT_LIMIT = 2
READY_PREFLIGHT_LIMIT = 2
PENDING_PREFLIGHT_LIMIT = 100
DISCOVERY_HEAD_LIMIT = 10
DISCOVERY_PROGRESS_LIMIT = 40


class _MutationRequired(Exception):
    """Reschedule through the mutation lane before any external effect."""


class _OwnerChanged(Exception):
    def __init__(self, job: dict[str, Any]):
        self.job = job


@dataclass(frozen=True, slots=True)
class _PendingPreflight:
    job: dict[str, Any]
    mutation: bool
    # Only an explicit missing-runtime deferral is sticky, for this raw row.
    forced_snapshot: str | None = None


@dataclass(slots=True)
class _DiscoverySweep:
    cutoff: datetime
    after: JobDiscoveryCursor | None = None


@dataclass(frozen=True, slots=True)
class _PreparedCandidate:
    job: dict[str, Any]
    durable_snapshot: str
    inherited_parent_id: str | None


@dataclass(slots=True)
class JobDispatchState:
    """Serialization state one application's dispatcher owns.

    ``tasks`` owns coalesced passes, workspace preflights and preemption pauses.
    ``preflight_tasks`` is the subset drained on leadership loss. Pending work
    is bounded data, not a task per waiting Job. The application drains all
    tasks on shutdown before its pools close.
    """

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    pause_pending_job_ids: set[str] = field(default_factory=set)
    tasks: set[asyncio.Task[Any]] = field(default_factory=set)
    preflight_tasks: set[asyncio.Task[Any]] = field(default_factory=set)
    pending: dict[str, _PendingPreflight] = field(default_factory=dict)
    discovery: dict[str, _DiscoverySweep] = field(default_factory=dict)
    active: dict[str, tuple[str, bool]] = field(default_factory=dict)
    completed: dict[str, _PreparedCandidate] = field(default_factory=dict)
    runner: asyncio.Task[Any] | None = None
    requested: JobDispatchDependencies | None = None
    discover_requested: bool = False
    closing: bool = False
    scheduling_paused: bool = False
    leader_bound: bool = False
    tick_started_at: float | None = None
    tick_finished_at: float | None = None

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

        self.closing = True
        self.requested = None
        self.discovery.clear()
        self.pending.clear()
        self.completed.clear()
        pending = list(self.tasks)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        # A task cancelled before its coroutine first runs has no finally
        # block to remove its reserved owner slot. Every worker is now joined.
        self.active.clear()
        self.pending.clear()
        self.completed.clear()

    async def pause_preflights(self) -> None:
        """Stop this leader's workspace work, retaining normal SDK joins."""
        self.scheduling_paused = True
        self.requested = None
        self.discovery.clear()
        self.pending.clear()
        self.completed.clear()
        pending = set(self.preflight_tasks)
        if self.runner is not None:
            pending.add(self.runner)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self.active.clear()
        self.pending.clear()
        self.completed.clear()


async def _dispatch_without_binds(_job: Any) -> tuple[str, str | None]:
    return "dispatch", None


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
    #: A job's registered driver binds before its claim (connector drivers
    #: D6, ``connector_bind_time.job_bind_gate``): ``("dispatch", None)``,
    #: ``("wait", None)`` or ``("fail", reason)``.
    job_bind_gate: Callable[[Any], Awaitable[tuple[str, str | None]]] = (
        _dispatch_without_binds
    )
    #: A job's provider-minted credentials exist before its claim (connector
    #: drivers C5, ``connector_minted_credentials.job_mint_gate``), the same
    #: three answers.
    job_mint_gate: Callable[[Any], Awaitable[tuple[str, str | None]]] = (
        _dispatch_without_binds
    )


def _needs_mutation(job: dict[str, Any], dependencies: JobDispatchDependencies) -> bool:
    if _scholar_provision_parent_id(job):
        return True
    if stateless_worker_workspace_owner(job).id != str(job["id"]):
        # An inheritor's stored snapshot is intentionally stale. Its resolver
        # waits for the parent; it cannot create before Ready is established.
        # A missing live runtime still takes the explicit mutation deferral.
        return False
    if _job_needs_vm(job):
        return _get_vm_context(job).get("status") != "ready"
    if dependencies.job_needs_sandbox(job):
        return _get_container_context(job).get("status") != "ready"
    return False


def _may_schedule(state: JobDispatchState) -> bool:
    if state.closing or state.scheduling_paused:
        return False
    if state.leader_bound:
        from orchestrator.services.leader_election import is_leader

        return is_leader.is_set()
    return True


def _owner_key(job: dict[str, Any]) -> str:
    parent = _scholar_provision_parent_id(job)
    if parent:
        return parent
    return stateless_worker_workspace_owner(job).id


def _eligible(
    job: dict[str, Any] | None, dependencies: JobDispatchDependencies
) -> bool:
    return bool(
        job
        and job.get("status") in {"created", "paused"}
        and job.get("assigned_agent_id") is None
        and job.get("freeze_data") is None
        and (
            (job.get("execution_lane") == "pinned" and dependencies.auto_assign_enabled)
            or (
                job.get("execution_lane") == "stateless"
                and dependencies.stateless_worker_enabled
            )
        )
    )


def _snapshot(job: dict[str, Any]) -> str:
    # This is comparison material, never a persisted grant or a logged payload.
    return json.dumps(
        {
            key: job.get(key)
            for key in (
                "status",
                "parent_job_id",
                "execution_lane",
                "assigned_agent_id",
                "freeze_data",
                "config_name",
                "config_override",
                "context",
            )
        },
        sort_keys=True,
        default=str,
    )


async def _current_candidate(
    candidate: _PreparedCandidate, dependencies: JobDispatchDependencies
) -> bool:
    job = candidate.job
    current = await dependencies.store.get_job(str(job["id"]))
    if not (
        _may_schedule(dependencies.state)
        and _eligible(current, dependencies)
        and _snapshot(current) == candidate.durable_snapshot
    ):
        return False
    expected = workspace_runtime_authority_digest(
        job, vm_mode=dependencies.vm_provisioner.mode
    )
    if expected is None:
        return False
    if candidate.inherited_parent_id is not None:
        parent = await dependencies.store.get_job(candidate.inherited_parent_id)
        current = copy.deepcopy(current)
        action, _ = project_inherited_workspace(current, parent)
        if action != "proceed":
            return False
    return bool(
        _may_schedule(dependencies.state)
        and workspace_runtime_authority_digest(
            current, vm_mode=dependencies.vm_provisioner.mode
        )
        == expected
    )


def _enqueue(
    job: dict[str, Any],
    dependencies: JobDispatchDependencies,
    *,
    mutation: bool | None = None,
    forced_snapshot: str | None = None,
) -> None:
    state = dependencies.state
    job_id = str(job["id"])
    if not _may_schedule(state) or job_id in state.active or job_id in state.completed:
        return
    previous = state.pending.get(job_id)
    if previous is not None and previous.forced_snapshot == _snapshot(job):
        forced_snapshot = previous.forced_snapshot
    candidate = _PendingPreflight(
        job,
        forced_snapshot is not None
        or (_needs_mutation(job, dependencies) if mutation is None else mutation),
        forced_snapshot,
    )
    if (
        job_id not in state.pending
        and len(state.pending) + len(state.active) + len(state.completed)
        >= PENDING_PREFLIGHT_LIMIT
    ):
        # Replace only a queued discovery hint. Active and completed work retain
        # their authority and ownership; an evicted row returns on a later sweep.
        replaceable = [
            (other_id, other)
            for other_id, other in state.pending.items()
            if (not candidate.mutation and other.mutation)
            or (
                candidate.mutation == other.mutation
                and int(job.get("priority") or 0) > int(other.job.get("priority") or 0)
            )
        ]
        if not replaceable:
            return
        evicted_id, _ = max(
            replaceable,
            key=lambda item: (item[1].mutation, discovery_order(item[1].job)),
        )
        state.pending.pop(evicted_id)
    state.pending[job_id] = candidate


async def _run_preflight(
    job_id: str, *, dependencies: JobDispatchDependencies, mutation: bool
) -> None:
    state = dependencies.state
    try:
        job = await dependencies.store.get_job(job_id)
        if not _may_schedule(state) or not _eligible(job, dependencies):
            return
        if _owner_key(job) != state.active[job_id][0]:
            raise _OwnerChanged(job)
        raw_snapshot = _snapshot(job)
        inherited_owner = stateless_worker_workspace_owner(job).id
        inherited_parent_id = (
            inherited_owner if inherited_owner != str(job["id"]) else None
        )
        before = workspace_runtime_authority_digest(
            job, vm_mode=dependencies.vm_provisioner.mode
        )
        candidate = await _preflight_job(
            job, dependencies=dependencies, mutation=mutation
        )
        if candidate is not None:
            state.completed[job_id] = _PreparedCandidate(
                copy.deepcopy(candidate),
                raw_snapshot
                if inherited_parent_id is not None
                else _snapshot(candidate),
                inherited_parent_id,
            )
        elif mutation:
            # A creator may finish Ready but deliberately return PENDING. Only
            # that newly durable authority wakes a fresh ready verification;
            # unchanged waits/polls are retried by the ordinary next tick.
            current = await dependencies.store.get_job(job_id)
            if _eligible(current, dependencies):
                after = workspace_runtime_authority_digest(
                    current, vm_mode=dependencies.vm_provisioner.mode
                )
                if after is not None and after != before:
                    state.pending[job_id] = _PendingPreflight(current, False)
    except _MutationRequired:
        if not state.closing:
            state.pending[job_id] = _PendingPreflight(job, True, raw_snapshot)
    except _OwnerChanged as changed:
        if not state.closing:
            state.pending[job_id] = _PendingPreflight(
                changed.job, _needs_mutation(changed.job, dependencies)
            )
    except Exception:
        logger.exception(
            "Dispatcher error: workspace preflight failed for job %s", job_id
        )
    finally:
        state.active.pop(job_id, None)
        if not state.closing:
            _request_dispatch(dependencies, discover=False)


def _pump_preflights(dependencies: JobDispatchDependencies) -> None:
    state = dependencies.state
    if not _may_schedule(state):
        return
    active_owners = {owner for owner, _ in state.active.values()}
    counts = {True: 0, False: 0}
    for _, mutation in state.active.values():
        counts[mutation] += 1
    for job_id, candidate in sorted(
        state.pending.items(), key=lambda item: discovery_order(item[1].job)
    ):
        job, mutation = candidate.job, candidate.mutation
        owner = _owner_key(job)
        limit = MUTATION_PREFLIGHT_LIMIT if mutation else READY_PREFLIGHT_LIMIT
        if owner in active_owners or counts[mutation] >= limit:
            continue
        state.pending.pop(job_id)
        state.active[job_id] = (owner, mutation)
        active_owners.add(owner)
        counts[mutation] += 1
        task = state.spawn(
            _run_preflight(job_id, dependencies=dependencies, mutation=mutation)
        )
        state.preflight_tasks.add(task)
        task.add_done_callback(state.preflight_tasks.discard)


def _request_dispatch(dependencies: JobDispatchDependencies, *, discover: bool) -> None:
    state = dependencies.state
    if not _may_schedule(state):
        return
    state.requested = dependencies
    state.discover_requested |= discover
    if state.runner is None or state.runner.done():
        state.runner = state.spawn(_run_requested_dispatches(state))


async def _run_requested_dispatches(state: JobDispatchState) -> None:
    while state.requested is not None and _may_schedule(state):
        dependencies, discover = state.requested, state.discover_requested
        state.requested, state.discover_requested = None, False
        await _dispatch_pass(dependencies=dependencies, discover=discover)


async def dispatch_pending_jobs(*, dependencies: JobDispatchDependencies) -> None:
    """Discover bounded owned preflights without waiting for workspace effects."""
    await _dispatch_pass(dependencies=dependencies, discover=True)


async def _discover_jobs(dependencies: JobDispatchDependencies) -> list[dict[str, Any]]:
    """Visit a fresh priority head and a finite forward page per enabled lane."""
    state = dependencies.state
    lanes = []
    if dependencies.auto_assign_enabled:
        lanes.append(("pinned", dependencies.store.get_dispatchable_jobs))
    if dependencies.stateless_worker_enabled:
        lanes.append(("stateless", dependencies.store.get_admittable_stateless_jobs))
    cutoff = None
    observed: dict[str, dict[str, Any]] = {}
    for lane, read_page in lanes:
        if lane not in state.discovery:
            if cutoff is None:
                cutoff = await dependencies.store.get_job_discovery_cutoff()
            state.discovery[lane] = _DiscoverySweep(cutoff)
        sweep = state.discovery[lane]
        guard = dependencies.completion_control_boundary.dispatch_guard_kwargs()
        head = await read_page(limit=DISCOVERY_HEAD_LIMIT, **guard)
        page = await read_page(
            limit=DISCOVERY_PROGRESS_LIMIT,
            discovery_after=sweep.after,
            discovery_cutoff=sweep.cutoff,
            **guard,
        )
        # Advance over every observed row even if it is already owned or the
        # local set is full. Otherwise queue pressure pins global discovery.
        if len(page) == DISCOVERY_PROGRESS_LIMIT:
            sweep.after = JobDiscoveryCursor.from_job(page[-1])
        else:
            state.discovery.pop(lane)
        for job in head + page:
            observed[str(job["id"])] = job
    return list(observed.values())


async def _dispatch_pass(
    *, dependencies: JobDispatchDependencies, discover: bool
) -> None:
    state = dependencies.state
    if (
        not state.closing
        and discover
        and getattr(dependencies.store, "manifests_ready", False) is True
        and dependencies.agent_provisioner._k8s_available
    ):
        await dependencies.manifest_execution_service().reconcile()
    if not _may_schedule(state) or not (
        dependencies.auto_assign_enabled or dependencies.stateless_worker_enabled
    ):
        return
    async with state.lock:
        if not _may_schedule(state):
            return
        state.tick_started_at = time.monotonic()
        try:
            if discover:
                jobs = await _discover_jobs(dependencies)
                for job in jobs:
                    _enqueue(job, dependencies)
            candidates = list(state.completed.values())
            state.completed.clear()
            dispatchable = []
            for candidate in candidates:
                job = candidate.job
                if not await _current_candidate(candidate, dependencies):
                    continue
                if job.get("execution_lane") == "stateless":
                    (
                        admitted,
                        queue_result,
                    ) = await dependencies.store.admit_stateless_worker_job(
                        str(job["id"]),
                        fair_key=str(job["user_id"]) if job.get("user_id") else None,
                        priority=int(job.get("priority") or 0),
                        allow_vm_workspace=vm_workspaces_on_pod_network(),
                        **dependencies.completion_control_boundary.dispatch_guard_kwargs(),
                    )
                    if admitted:
                        logger.info(
                            "Dispatcher: admitted stateless worker job %s (queue=%s)",
                            job["id"],
                            queue_result,
                        )
                    else:
                        logger.warning(
                            "Dispatcher: stateless admission CAS lost for job %s; "
                            "workspace/lane/status changed after preflight",
                            job["id"],
                        )
                else:
                    dispatchable.append(candidate)
            await _match_jobs(dispatchable, dependencies=dependencies)
            _pump_preflights(dependencies)
        except Exception:
            logger.exception("Dispatcher error: scheduling pass failed")
        finally:
            state.tick_finished_at = time.monotonic()


async def _preflight_job(
    job: dict[str, Any], *, dependencies: JobDispatchDependencies, mutation: bool
) -> dict[str, Any] | None:
    job_id = str(job["id"])
    stateless_worker = job.get("execution_lane") == "stateless"
    if await handle_creation_pending(job, db=dependencies.store):
        return None
    (
        workspace_action,
        job,
        workspace_recovery_reason,
    ) = await dependencies.prepare_job_workspace_runtime(job)
    if not _may_schedule(dependencies.state):
        return None
    if workspace_action == "wait":
        logger.warning(
            "Dispatcher: job %s waiting for live workspace authority recovery (%s)",
            job_id,
            workspace_recovery_reason or "retryable",
        )
        return None
    if workspace_action == "fail":
        await dependencies.fail_subjob_and_unblock_parent(
            job,
            workspace_recovery_reason
            or "Inherited workspace authority is unavailable.",
        )
        return None
    if _owner_key(job) != dependencies.state.active[job_id][0]:
        raise _OwnerChanged(job)
    if not mutation and _needs_mutation(job, dependencies):
        raise _MutationRequired
    workspace_decision = resolve_workspace_runtime(
        job, vm_mode=dependencies.vm_provisioner.mode
    )
    if workspace_decision.contract is None or workspace_decision.state == "invalid":
        logger.error(
            "Dispatcher: refusing job %s with invalid workspace authority (%s)",
            job_id,
            workspace_decision.reason or workspace_decision.state,
        )
        await dependencies.store.update_job_status(
            job_id,
            status="failed",
            error_message=(
                "Workspace contract is ambiguous or invalid; refusing dispatch"
            ),
            expected_status=str(job.get("status")),
        )
        return None

    job_needs_vm = _job_needs_vm(job)
    stateless_same_cluster_vm = bool(
        stateless_worker and job_needs_vm and vm_workspaces_on_pod_network()
    )

    # Defense for inherited/operator-created rows. External VMs
    # still belong to the mesh-enabled registered-agent plane; a
    # same-cluster VM remains on the pool lane below.
    if stateless_worker and job_needs_vm and not vm_workspaces_on_pod_network():
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
        return None
    _stateless_container = _get_container_context(job)
    _stateless_has_k8s_workspace = (
        _stateless_container.get("status") == "ready"
        and _stateless_container.get("provisioner") == "k8s"
        and bool(_stateless_container.get("host") or _stateless_container.get("pod_ip"))
    )
    if stateless_worker and not (
        stateless_same_cluster_vm
        or dependencies.job_needs_sandbox(job)
        or _stateless_has_k8s_workspace
    ):
        logger.error(
            "Dispatcher: refusing stateless job %s without a compatible workspace",
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
        return None
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
        return None

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
            await dependencies.check_vm_permission(creator, job_needs_vm=True)
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
            return None
        vm_ctx = _get_vm_context(job)
        # Bounded provisioning retries. A VM that never reaches 'ready'
        # (real infra failure) must park after N attempts instead of
        # re-provisioning forever against the shared VM cluster. The
        # counter records one exact admitted VM per generation and
        # resets atomically with verified Ready, so it survives async
        # status callbacks that a status-based park cannot. Decision
        # logic is extracted + unit-tested in dispatch_guards.
        provision_attempts = int(vm_ctx.get("provision_attempts") or 0)
        max_provision_attempts = int(os.environ.get("VM_PROVISION_MAX_ATTEMPTS", "3"))
        timeout_s = int(os.environ.get("VM_PROVISION_TIMEOUT_S", "600"))
        rootdisk_stall_timeout_s = int(
            os.environ.get("VM_ROOTDISK_STALL_TIMEOUT_S", "2700")
        )
        golden_timeout_s = int(os.environ.get("VM_GOLDEN_WAIT_TIMEOUT_S", "2700"))
        headscale_timeout_s = int(os.environ.get("VM_HEADSCALE_WAIT_TIMEOUT_S", "900"))
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
            return None
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
                return None
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
                return None
            if not ok:
                logger.warning("Dispatcher: VM provisioning failed for job %s", job_id)
            # Authenticated phase observation accounts admission;
            # a create acknowledgement or dependency wait does not.
            return None  # Skip this job — wait for VM to register
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
            return None
        if vm_decision == VM_PARK_PREPARATION:
            park_error = "Workspace preparation did not complete within its deadline"
            await dependencies.store.merge_vm_context(
                job_id, {"status": "failed", "error": park_error}
            )
            await dependencies.fail_vm_parked_job(job_id, park_error)
            return None
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
                    "Dispatcher: job %s waiting on VM capacity (%s/%s) — polling",
                    job_id,
                    vm_ctx.get("running_vms") or "?",
                    vm_ctx.get("max_concurrent_vms") or "?",
                )
            else:
                logger.info(
                    "Dispatcher: job %s waiting on golden image %s (%s) — polling",
                    job_id,
                    vm_ctx.get("golden") or "?",
                    vm_ctx.get("golden_progress")
                    or vm_ctx.get("golden_phase")
                    or "importing",
                )
            return None
        if vm_decision == VM_PARK_GOLDEN:
            # The golden import outlived even the golden budget —
            # CDI is wedged or the registry is unreachable. No VM
            # was ever created, so there is nothing to recycle;
            # fail the job with the truth (not the misleading
            # "provisioning exhausted after N attempts").
            elapsed = int(
                time.time() - float(vm_ctx.get("golden_wait_started_at") or 0)
            )
            park_error = (
                f"golden image import did not complete within "
                f"{golden_timeout_s}s (golden "
                f"{vm_ctx.get('golden') or 'unknown'}, last progress "
                f"{vm_ctx.get('golden_progress') or 'unknown'}, "
                f"waited {elapsed}s) — VM never created"
            )
            logger.warning(
                "Dispatcher: job %s golden wait exhausted — failing job (%s)",
                job_id,
                park_error,
            )
            await dependencies.store.merge_vm_context(
                job_id,
                {"status": "failed", "error": park_error},
            )
            await dependencies.fail_vm_parked_job(job_id, park_error)
            return None
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
            return None
        if vm_decision == VM_PARK_HEADSCALE:
            # Headscale never came back inside its budget. No VM was
            # ever created, so there is nothing to recycle — fail
            # with the real cause rather than the misleading
            # "provisioning exhausted after N attempts".
            elapsed = int(
                time.time() - float(vm_ctx.get("headscale_wait_started_at") or 0)
            )
            park_error = (
                f"Headscale (mesh VPN) unavailable for {elapsed}s "
                f"(budget {headscale_timeout_s}s, last error: "
                f"{vm_ctx.get('headscale_error') or 'unknown'}) — "
                f"VM never created"
            )
            logger.warning(
                "Dispatcher: job %s Headscale wait exhausted — failing job (%s)",
                job_id,
                park_error,
            )
            await dependencies.store.merge_vm_context(
                job_id,
                {"status": "failed", "error": park_error},
            )
            await dependencies.fail_vm_parked_job(job_id, park_error)
            return None
        if vm_decision == VM_PARK_INITIALIZATION:
            park_error = "Workspace initialization did not complete within its deadline"
            await dependencies.store.merge_vm_context(
                job_id, {"status": "failed", "error": park_error}
            )
            await dependencies.fail_vm_parked_job(job_id, park_error)
            return None
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
            return None
        if vm_decision != VM_READY:
            logger.warning("Dispatcher: job %s has an unhandled VM decision", job_id)
            return None
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
            return None
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
                    "Dispatcher: job %s assigning workspace from Docker Compose pool",
                    job_id,
                )
                result = await dependencies.docker_provisioner.assign_workspace(job_id)
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
            return None  # Skip — wait for container to become ready
        # K8s create (when status absent) + all lifecycle states route
        # through the shared, owner-agnostic state machine.
        if container_status == "suspended":
            # Restore belongs to this application's mutation task and drains
            # with it; the generic lifecycle's historical background path is
            # not used by the dispatcher.
            await dependencies.workspace_suspension.restore(WorkspaceOwner.job(job_id))
            return None
        res = await ensure_workspace(
            WorkspaceOwner.job(job_id),
            provisioner=dependencies.container_provisioner,
            suspension=dependencies.workspace_suspension,
            current_status=container_status,
            recreate_missing=mutation,
            # This caller fails the Job on FAILED, so a Job create whose
            # container exits before Ready may fail at once and leave its
            # creation open for the Job's terminal cleanup.
            fail_on_exited_container=True,
        )
        if res.mutation_required:
            raise _MutationRequired
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
            if error and str(error).startswith(
                WorkspaceContainerExitedError.MESSAGE_PREFIX
            ):
                # It already names the workspace container; the shared
                # prefix would only repeat it.
                msg = str(error)
            elif error:
                msg = f"Workspace container failed: {error}"
            else:
                msg = (
                    "Workspace container could not be created. Check "
                    "orchestrator logs for details (image pull failures, "
                    "insufficient resources, RBAC issues)."
                )
            logger.error(
                "Dispatcher: workspace ensure failed for job %s: %s. Failing job.",
                job_id,
                msg,
            )
            await dependencies.store.update_job_status(
                job_id,
                status="failed",
                error_message=msg,
                expected_status=str(job.get("status")),
            )
            return None
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
            return None  # in progress — wait for next cycle
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
        return None
    # A registered driver's connector binds in its own pod before the claim:
    # started here without waiting (this preflight never blocks a dispatch
    # loop), the job waits for later ticks while it runs, and one that fails
    # for good fails the job with the driver's reason (D6).
    action, reason = await dependencies.job_bind_gate(job)
    if action == "dispatch":
        # A provider-minted credential is minted before the claim too (C5):
        # started without waiting, the job waits for later ticks until its
        # delivery hands it out, and a provider's refusal fails the job.
        action, reason = await dependencies.job_mint_gate(job)
    if action == "wait":
        return None
    if action == "fail":
        logger.warning(
            "Dispatcher: job %s connector bind failed: %s. Failing job.",
            job_id,
            reason,
        )
        await dependencies.store.update_job_status(
            job_id,
            status="failed",
            error_message=reason,
            expected_status=str(job.get("status")),
        )
        return None
    return job


async def _match_jobs(
    prepared: list[_PreparedCandidate], *, dependencies: JobDispatchDependencies
) -> None:
    dispatchable_jobs = [candidate.job for candidate in prepared]
    candidates_by_id = {str(candidate.job["id"]): candidate for candidate in prepared}
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
        if not await _current_candidate(candidates_by_id[job_id], dependencies):
            continue
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
            success = await dependencies.job_delivery_operations().resume(job, agent)
        else:
            if job["status"] == "paused":
                logger.info(
                    "Dispatcher: job %s is paused with no checkpoint "
                    "to resume from (never started, or pruned at a "
                    "terminal state) — dispatching via the fresh "
                    "/job/start lane",
                    job_id,
                )
            success = await dependencies.job_delivery_operations().dispatch(job, agent)

        if success:
            matched_job_ids.add(job_id)
            matched_agent_ids.add(str(agent["id"]))

    # Phase 1.5: Provision agent pods for unmatched jobs (K8s only)
    remaining = [j for j in dispatchable_jobs if str(j["id"]) not in matched_job_ids]
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
    remaining = [j for j in dispatchable_jobs if str(j["id"]) not in matched_job_ids]
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
    if dependencies.state.closing:
        return
    dependencies.state.leader_bound = True
    dependencies.state.scheduling_paused = False
    try:
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
    finally:
        await dependencies.state.pause_preflights()

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
        dependencies.state.leader_bound = True
        _request_dispatch(dependencies, discover=True)
