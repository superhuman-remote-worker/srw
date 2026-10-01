"""Bind an agent to a thread for the create-thread fast path.

This is the K8s-mode binding path invoked by session admission as a
fire-and-forget asyncio task. It owns the lifecycle SSE emissions for that
path — without these the cockpit's startup card never sees
``provisioning``/``booting``/``ready`` because
``GET /api/sessions/{tid}/connection`` returns 200 immediately for the
warm-pool case (the idle-pool attach is synchronous inside session create)
and the cockpit's 425 fallback (which drives ``/prepare``'s lifecycle SSE)
never fires.

R1.B06 closed this module's late ``from orchestrator.main import ...``. Every
collaborator that needs a constructed dependency object now arrives as an
already-bound operation on :class:`ProvisionOrAssignDependencies`, built once
by the composition root and handed down by whichever caller schedules the
task. The genuinely pure helpers — the two violation-detail formatters, the
readiness budget and the lifecycle admission predicate — are imported from
their owning services directly, because routing them through a main wrapper
bought nothing but a second hop.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import os
from typing import Any, Awaitable, Callable

from orchestrator.services.grant_enforcement import grant_violations_detail
from orchestrator.services.session_config_resolution import (
    endpoint_violations_detail,
)
from orchestrator.services.session_runtime_identity import thread_accepts_runtime
from orchestrator.services.session_workspace_policy import session_ready_timeout_s
from orchestrator.services.vm_thread_initial import initial_thread_vm_blocks_runtime
from shared.pinned_session_identity import PinnedSessionBinding

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ProvisionOrAssignDependencies:
    """The collaborators this path needs, already constructed.

    Each callable is the owning service's operation with its own dependency
    object applied — never a lookup back into the composition root. ``store``
    and ``agent_provisioner`` are the composition bindings of the application
    that scheduled the task.
    """

    store: Any
    agent_provisioner: Any
    await_protected_cloud_runtime_ready: Callable[[str], Awaitable[bool]]
    session_grant_violations: Callable[..., Awaitable[list[str]]]
    session_endpoint_violations: Callable[..., Awaitable[list[str]]]
    find_idle_persistent_agent: Callable[[], Awaitable[dict[str, Any] | None]]
    send_session_attach: Callable[..., Awaitable[bool]]


async def provision_or_assign(
    uid: str,
    tid: str,
    cfg: str,
    co: dict,
    pids: list,
    ds_ids: list[str] | None,
    *,
    runtime_generation: str | None = None,
    dependencies: ProvisionOrAssignDependencies,
) -> None:
    """Bind an agent to a thread and emit ``session.lifecycle`` events.

    Serialises concurrent provisioning attempts for the same thread via
    Postgres advisory lock
    (knowledge-base/knowledge/issues/persistent_thread_double_provisioning_race.md).
    Idle-pool fast-path first; falls back to a dedicated pod on miss.

    Locking strategy: the advisory lock is held ONLY across the
    decide-who-provisions critical section (pool check + attach OR pod
    creation kickoff). It is released before ``wait_for_binding`` /
    ``wait_for_ready`` because the fresh-pod path's agent registers via
    ``POST /api/agents/register``, which acquires the SAME advisory lock
    for its duplicate-rejection check — holding it across the wait
    deadlocks both for the asyncpg query timeout (~60s).
    """
    from orchestrator.services.workspace_tier_policy import (
        backend_from_override as _backend_from_override,
    )

    postgres_db = dependencies.store
    agent_provisioner = dependencies.agent_provisioner
    from orchestrator.services.session_lifecycle import (
        emit as lifecycle_emit,
        wait_for_binding,
        wait_for_ready,
    )
    from orchestrator.services.session_provisioning_state import (
        agent_pod_provisioning_in_progress,
    )
    from orchestrator.services.session_runtime_admission import (
        ThreadRuntimeAuthority,
        same_thread_runtime_authority,
        thread_requests_protected_cloud,
        thread_runtime_authority,
    )
    from shared.run_queue import LANE_PINNED, LANE_STATELESS

    # A VM-backed session pays a cold KubeVirt boot (minutes). Tag the lifecycle
    # events so the cockpit renders the "Booting VM (this can take a few minutes)"
    # copy, and size the readiness wait from the backend (VM budget vs the fast
    # sandbox default) — see knowledge-base/knowledge/features/session_create_on_vm.md.
    _is_vm = _backend_from_override(co) == "vm"
    _vm_tag: dict[str, str] = {"backend": "vm"} if _is_vm else {}

    warm_attached = False
    needs_binding_wait = False  # True iff fresh-pod path took over

    if runtime_generation is None:
        # Compatibility for direct callers/tests. Production create admission
        # passes the generation captured immediately after INSERT, so a task
        # delayed until after End->Resume can never recapture G2 here.
        expected_runtime = thread_runtime_authority(await postgres_db.get_thread(tid))
    else:
        expected_runtime = ThreadRuntimeAuthority(
            thread_id=tid,
            generation=runtime_generation,
        )

    async def _same_runtime(current: dict | None = None) -> bool:
        if current is None:
            current = await postgres_db.get_thread(tid)
        if initial_thread_vm_blocks_runtime(current):
            return False
        if expected_runtime is None:
            # Direct mixed-version/test callers retain the historical status
            # gate. The production scheduler always passes a generation.
            return thread_accepts_runtime(current)
        return same_thread_runtime_authority(current, expected_runtime)

    async def _safe_emit(state: str, **extra: str) -> bool:
        """Emit only while the same thread may still own a live runtime."""

        current = await postgres_db.get_thread(tid)
        if not await _same_runtime(current):
            return False
        if expected_runtime is not None:
            extra["session_runtime_generation"] = expected_runtime.generation
        lifecycle_emit(uid, tid, state, **extra)
        return True

    try:
        if not await _same_runtime():
            return
        # Create-time protected engagement runs concurrently with repository
        # setup.  It must converge before either a warm reservation or a pod
        # spawn; ordinary sessions return immediately.
        if not await dependencies.await_protected_cloud_runtime_ready(tid):
            return
        async with postgres_db.thread_advisory_lock(tid):
            cur = await postgres_db.get_thread(tid)
            if not await _same_runtime(cur):
                return
            execution_lane = cur.get("execution_lane") if cur else None
            if execution_lane == LANE_STATELESS:
                # The create-thread task is fire-and-forget, so the lane may
                # have changed between scheduling and this authoritative
                # refetch. A stateless row is already admission-ready; emitting
                # provisioning -> failed here would contradict /connection and
                # surface a false startup error in the cockpit.
                logger.info(
                    "Thread %s: create-path provisioning no longer applies "
                    "after transition to the stateless lane",
                    tid,
                )
                return
            if execution_lane != LANE_PINNED:
                # Missing, corrupt, and future lanes are not healthy state.
                # Fail closed and retain the lifecycle failure for operators.
                logger.warning(
                    "Thread %s: refusing create-path provisioning for "
                    "execution lane %r",
                    tid,
                    execution_lane,
                )
                await _safe_emit(
                    "failed",
                    reason="Session execution lane does not use pinned provisioning",
                )
                return
            await _safe_emit("provisioning", **_vm_tag)
            if cur and cur.get("agent_id"):
                logger.info(
                    "Thread %s: already bound to agent %s — "
                    "skipping duplicate provision.",
                    tid,
                    cur["agent_id"],
                )
                # Another path (e.g. concurrent /prepare) owns the
                # remaining lifecycle emissions — exit silently rather
                # than double-emitting booting/ready.
                return

            if agent_pod_provisioning_in_progress(cur):
                logger.info(
                    "Thread %s: agent pod already provisioning — waiting for binding.",
                    tid,
                )
                needs_binding_wait = True
            else:
                # Pre-flight the capability grants BEFORE pool-attach or pod
                # spawn: a never-startable config (e.g. permission_mode above the
                # user's ceiling) would otherwise boot a dedicated pod that 403s
                # at the workspace endpoint and exits, leaving the cockpit to poll
                # /connection until its ~5m40s ready timeout. Fail fast with the
                # real reason instead.
                # knowledge-base/knowledge/issues/session_permission_mode_grant_denied_ready_timeout.md
                violations = await dependencies.session_grant_violations(cur)
                cur = await postgres_db.get_thread(tid)
                if not await _same_runtime(cur):
                    return
                if violations:
                    logger.warning(
                        "Thread %s: provisioning denied by capability grants: %s",
                        tid,
                        "; ".join(violations),
                    )
                    await _safe_emit(
                        "failed",
                        reason=grant_violations_detail(violations),
                    )
                    return
                # Pre-flight the model-role transports too: a configured role
                # with no reachable endpoint (e.g. the memory reranker riding an
                # unresolvable embedding endpoint, or a raising-provider chat
                # model with no key) crashes the agent at startup, releases the
                # workspace, and hangs the cockpit exactly like a grant denial.
                # Fail fast with the real reason instead of spawning a doomed pod.
                # knowledge-base/knowledge/issues/openrouter_auxiliary_crashes_session_via_memory_reranker.md
                endpoint_violations = await dependencies.session_endpoint_violations(
                    cur
                )
                cur = await postgres_db.get_thread(tid)
                if not await _same_runtime(cur):
                    return
                if endpoint_violations:
                    logger.warning(
                        "Thread %s: provisioning denied by unusable transport: %s",
                        tid,
                        "; ".join(endpoint_violations),
                    )
                    await _safe_emit(
                        "failed",
                        reason=endpoint_violations_detail(endpoint_violations),
                    )
                    return
                # Try to attach an idle dual-mode agent from the warm pool
                # first — this is instant (no image pull or pod boot needed).
                # _send_session_attach writes threads.agent_id via the
                # orchestrator's own DB connection, so the binding is visible
                # to other lock-takers immediately on release.
                idle_agent = await dependencies.find_idle_persistent_agent()
                cur = await postgres_db.get_thread(tid)
                if not await _same_runtime(cur):
                    return
                if idle_agent:
                    # The attach boundary re-fetches the committed thread,
                    # reauthorizes its current connector selection, and resolves
                    # credentials. A payload resolved here can go stale while a
                    # concurrent live settings update changes A -> B/[].
                    attach_identity = (
                        {"expected_runtime_generation": expected_runtime.generation}
                        if expected_runtime is not None
                        else {}
                    )
                    ok = await dependencies.send_session_attach(
                        idle_agent,
                        tid,
                        co,
                        pids,
                        datasources=None,
                        config_name=cfg,
                        **attach_identity,
                    )
                    if ok:
                        logger.info(
                            "Thread %s: attached to idle pool agent %s",
                            tid,
                            idle_agent["hostname"],
                        )
                        warm_attached = True
                    else:
                        # The attach attempt awaited DB and HTTP boundaries.
                        # Its entry snapshot is no longer authority: a lane
                        # transition or sibling binding must suppress the
                        # fresh-pod fallback.
                        cur = await postgres_db.get_thread(tid)
                        if not await _same_runtime(cur):
                            return
                        lane_after_attach = cur.get("execution_lane") if cur else None
                        if lane_after_attach == LANE_STATELESS:
                            logger.info(
                                "Thread %s: suppressing pod fallback after "
                                "transition to the stateless lane",
                                tid,
                            )
                            return
                        if lane_after_attach != LANE_PINNED:
                            await _safe_emit(
                                "failed",
                                reason=(
                                    "Session execution lane does not use "
                                    "pinned provisioning"
                                ),
                            )
                            return
                        if cur.get("agent_id") or agent_pod_provisioning_in_progress(
                            cur
                        ):
                            needs_binding_wait = True

                if not warm_attached and not needs_binding_wait:
                    # No idle agent (or pool attach failed) — create a
                    # dedicated session pod. Only kick off the creation here;
                    # the binding wait happens AFTER the lock is released, so
                    # the new pod's /register call can grab the same lock.
                    cur = await postgres_db.get_thread(tid)
                    if not await _same_runtime(cur):
                        return
                    provision_identity = (
                        {"expected_runtime_generation": expected_runtime.generation}
                        if expected_runtime is not None
                        else {}
                    )
                    pod_name = await agent_provisioner.provision_agent(
                        purpose="session",
                        thread_id=tid,
                        config_name=cfg,
                        **provision_identity,
                    )
                    cur = await postgres_db.get_thread(tid)
                    if not await _same_runtime(cur):
                        return
                    if not pod_name:
                        logger.error(
                            "Thread %s: no idle agents and pod "
                            "provisioning failed. Check image "
                            "availability, RBAC, node resources, "
                            "or increase MAX_AGENTS.",
                            tid,
                        )
                        await _safe_emit(
                            "failed",
                            reason="no idle agents and pod provisioning failed",
                        )
                        return
                    needs_binding_wait = True

        # Lock released. The fresh-pod path now waits for the agent's
        # /register handler to write threads.agent_id (which needs the
        # advisory lock we just dropped).
        if needs_binding_wait:
            bind_timeout_s = int(os.environ.get("AGENT_BIND_TIMEOUT_S", "300"))
            if not await wait_for_binding(tid, bind_timeout_s, store=postgres_db):
                cur = await postgres_db.get_thread(tid)
                if await _same_runtime(cur):
                    await _safe_emit("failed", reason="agent failed to register")
                return
        # Block on the actual session-ready signal so the cockpit's
        # startup card flips from "booting" to "ready" exactly when the
        # agent's _attach_session finishes (the agent's /ready is gated
        # on _session_ready()'s 3-way check). Mirrors the readiness-probe
        # tail of _do_prepare.
        if not await _safe_emit("booting", **_vm_tag):
            return
        if expected_runtime is None:
            await _safe_emit("failed", reason="session runtime identity is unavailable")
            return
        ready_timeout_s = session_ready_timeout_s(
            _backend_from_override(co),
            preparation=bool(
                ((co or {}).get("workspace") or {}).get("vm", {}).get("preparation")
            ),
        )
        cur = await postgres_db.get_thread(tid)
        if not await _same_runtime(cur):
            return
        binding: (
            PinnedSessionBinding | None
        ) = await postgres_db.get_pinned_session_binding(
            tid,
            expected_runtime_generation=expected_runtime.generation,
        )
        startup_statuses = {"booting", "ready", "working", "session"}
        if binding is None or binding.agent_status not in startup_statuses:
            await _safe_emit("failed", reason="session binding is not authoritative")
            return
        if not await wait_for_ready(
            binding.pod_ip,
            binding.pod_port,
            ready_timeout_s,
            require_protected_cloud=thread_requests_protected_cloud(cur),
            expected_session_identity_fingerprint=(
                binding.session_identity_fingerprint
            ),
            **(
                {
                    "vm_store": postgres_db,
                    "vm_thread_id": tid,
                    "vm_runtime_generation": expected_runtime.generation,
                    "vm_binding": binding,
                }
                if _is_vm
                else {}
            ),
        ):
            cur = await postgres_db.get_thread(tid)
            current_binding = await postgres_db.get_pinned_session_binding(
                tid,
                expected_runtime_generation=expected_runtime.generation,
            )
            if (
                await _same_runtime(cur)
                and current_binding is not None
                and current_binding.target_key == binding.target_key
                and current_binding.agent_status in startup_statuses
            ):
                await _safe_emit("failed", reason="agent /ready timeout")
            return

        if not await _same_runtime():
            return
        current_binding: (
            PinnedSessionBinding | None
        ) = await postgres_db.get_pinned_session_binding(
            tid,
            expected_runtime_generation=expected_runtime.generation,
        )
        if (
            current_binding is None
            or current_binding.target_key != binding.target_key
            or current_binding.agent_status not in startup_statuses
        ):
            return
        # The joined reread above is the final await before publication. The
        # event is synchronous, so no stale physical endpoint can be labeled
        # ready after a same-G attach/Pod rotation.
        lifecycle_emit(
            uid,
            tid,
            "ready",
            session_runtime_generation=expected_runtime.generation,
        )
    except Exception as e:
        logger.exception("provision_or_assign failed for thread %s: %s", tid, e)
        await _safe_emit("failed", reason=str(e))
