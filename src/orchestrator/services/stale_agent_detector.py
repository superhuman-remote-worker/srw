"""Agent-state reconciliation sweep and durable pinned-retirement retries.

R1.B11 home of the stale-agent detector that used to live in
``orchestrator.main``. The application owns task creation, leader gating
(``run_when_leader``), cadence ownership and shutdown; the bodies here receive
every stateful collaborator explicitly through
:class:`StaleAgentDetectorDependencies` and never reach back into application
globals.

Two bodies travelled with the detector because only its step 3/3b/3d call
them: :func:`retire_orphaned_pinned_runtime` (an offline incarnation through
the normal pinned End funnel) and :func:`retry_pending_pinned_retirement` (an
exact durable retirement whose local actor disappeared).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

from fastapi import HTTPException

from orchestrator.services.session_wake import (
    kick_event_drain as _kick_officer_event_drain,
    notify_all_officers,
    notify_owning_officers,
)

logger = logging.getLogger(__name__)

__all__ = [
    "PINNED_RETIREMENT_PREFLIGHT_GRACE_SECONDS",
    "PINNED_RETIREMENT_PROVEN_RETRY_GRACE_SECONDS",
    "PINNED_RETIREMENT_RETRY_GRACE_SECONDS",
    "StaleAgentDetectorDependencies",
    "retire_orphaned_pinned_runtime",
    "retry_pending_pinned_retirement",
    "stale_agent_detector",
]

# Durable pinned retirement is retried only after the exact local agent is
# absent/offline and this grace has elapsed.  This is deliberately longer than
# ordinary local teardown: a live runtime owns memory/git/event-writer drain
# after Begin and before its final settlement request.
PINNED_RETIREMENT_RETRY_GRACE_SECONDS = max(
    0, int(os.environ.get("PINNED_RETIREMENT_RETRY_GRACE_SECONDS", "900"))
)
PINNED_RETIREMENT_PREFLIGHT_GRACE_SECONDS = max(
    1, int(os.environ.get("PINNED_RETIREMENT_PREFLIGHT_GRACE_SECONDS", "300"))
)
# Once the exact agent's local-quiescence receipt exists, or a permanent
# delete follows a same-generation soft settlement, no live drain remains for
# the grace above to wait out. Such a row is only the orchestrator-side
# "owner/reconciler retry" of the exit handoff; this short grace keeps the
# sweep from racing the request that is still finishing it.
PINNED_RETIREMENT_PROVEN_RETRY_GRACE_SECONDS = max(
    0, int(os.environ.get("PINNED_RETIREMENT_PROVEN_RETRY_GRACE_SECONDS", "60"))
)


@dataclass(frozen=True, slots=True)
class StaleAgentDetectorDependencies:
    """Collaborators the application hands the detector for one task tenure."""

    #: The application's Postgres store (``main.postgres_db``).
    store: Any
    agent_provisioner: Any
    docker_provisioner: Any
    #: Read backend for the strict audit fingerprint used by lease recovery.
    audit_reader: Any
    completion_commands_enabled: bool
    #: Fire-and-forget dispatcher kick (leader-gated by the application).
    trigger_dispatch: Callable[[], None]
    #: Request-independent attach-abort successor scheduler.
    schedule_attach_abort_successor: Callable[..., Any]
    #: Provider of the composed thread retirement operations
    #: (``end_thread_flow``); called at each use, as main's composer was.
    thread_retirement_operations: Callable[[], Any]
    #: Provider of the composed ``PinnedRetirementOperations``
    #: (``retirement_context_runtime_exposed``,
    #: ``retirement_has_exact_local_quiescence``,
    #: ``recover_captured_process_zero``); called at each use.
    pinned_retirement_operations: Callable[[], Any]


async def retire_orphaned_pinned_runtime(
    candidate: Mapping[str, Any], *, dependencies: StaleAgentDetectorDependencies
) -> bool:
    """Retire one offline incarnation through the normal pinned End funnel.

    The candidate is a read-only hint. ``begin_pinned_thread_retirement``
    rechecks the exact generation, agent, attach attempt and offline state in
    its row transaction before closing admission. A recovered/rebound agent is
    therefore preserved even when it changes immediately after the sweep.
    """

    thread_id = str(candidate.get("id") or "")
    generation = str(candidate.get("runtime_generation") or "")
    agent_id = str(candidate.get("agent_id") or "")
    attach_token = (
        str(candidate.get("runtime_attach_token"))
        if candidate.get("runtime_attach_token") is not None
        else None
    )
    if not thread_id or not generation or not agent_id:
        return False
    settle_status: Literal["ended", "suspended"] = (
        "suspended"
        if str(candidate.get("status") or "") in {"awaiting_user", "suspended"}
        else "ended"
    )

    thread = await dependencies.store.get_thread(thread_id)
    if not isinstance(thread, Mapping):
        return False
    try:
        await dependencies.thread_retirement_operations().end_thread_flow(
            thread_id,
            dict(thread),
            permanent=False,
            force=True,
            expected_runtime_generation=generation,
            expected_agent_id=agent_id,
            expected_attach_token=attach_token,
            require_expected_agent_offline=True,
            settle_status=settle_status,
        )
    except HTTPException as exc:
        if exc.status_code == 409:
            logger.info(
                "Offline-runtime retirement lost authority for thread %s; "
                "preserving the current runtime",
                thread_id,
            )
            return False
        raise
    return True


async def retry_pending_pinned_retirement(
    candidate: Mapping[str, Any], *, dependencies: StaleAgentDetectorDependencies
) -> bool:
    """Retry one exact durable retirement after its local actor disappeared."""

    context = candidate.get("runtime_retirement_context") or {}
    if isinstance(context, str):
        try:
            context = json.loads(context)
        except (TypeError, ValueError):
            return False
    if not isinstance(context, Mapping):
        return False
    thread_id = str(candidate.get("id") or "")
    generation = str(candidate.get("runtime_generation") or "")
    token = str(candidate.get("runtime_retirement_token") or "")
    settle_status = str(context.get("settle_status") or "")
    if (
        not thread_id
        or not generation
        or not token
        or settle_status
        not in {
            "ended",
            "suspended",
        }
    ):
        return False
    if str(context.get("generation") or "") != generation:
        return False
    permanent = bool(candidate.get("runtime_retirement_permanent"))
    if permanent and settle_status != "ended":
        return False
    expected_agent_id = (
        str(context.get("agent_id")) if context.get("agent_id") is not None else None
    )
    expected_attach_token = (
        str(context.get("runtime_attach_token"))
        if context.get("runtime_attach_token") is not None
        else None
    )
    thread = await dependencies.store.get_thread(thread_id)
    if not isinstance(thread, Mapping):
        return False
    runtime_exposed = (
        dependencies.pinned_retirement_operations().retirement_context_runtime_exposed(
            {
                "generation": generation,
                "token": token,
                "permanent": permanent,
                "context": context,
            }
        )
    )
    if (
        runtime_exposed
        and not dependencies.pinned_retirement_operations().retirement_has_exact_local_quiescence(
            {
                "generation": generation,
                "token": token,
                "permanent": permanent,
                "context": context,
            },
            thread,
        )
        # A same-generation soft settlement already proved this life reached
        # process zero, and admission has stayed closed since. The End funnel
        # accepts that proof for a permanent delete (it rechecks it under the
        # lifecycle lock), so the retry must not demand a second, fresh one:
        # a soft-Ended session has no captured actor left to stop and would
        # otherwise stay pending until an owner retried by hand.
        and not (
            permanent
            and await dependencies.store.pinned_thread_has_prior_soft_settlement(
                thread_id,
                runtime_generation=generation,
                retirement_token=token,
            )
        )
    ):
        if candidate.get("nominated_before_grace"):
            # Nominated early for a proof it no longer shows exactly. Crash
            # recovery stays behind the full live-drain grace.
            return False
        recovered = await dependencies.pinned_retirement_operations().recover_captured_process_zero(
            {
                "generation": generation,
                "token": token,
                "permanent": permanent,
                "context": context,
            }
        )
        if not recovered:
            logger.warning(
                "Pinned retirement crash recovery could not prove process zero "
                "for thread %s (backend %r); the durable marker stays pending",
                thread_id,
                context.get("workspace_backend"),
            )
            return False
        thread = await dependencies.store.get_thread(thread_id)
        if thread is None:
            return True
        if str(thread.get("runtime_retirement_token") or "") != token:
            return str(thread.get("status") or "") in {"ended", "suspended"}
    try:
        result = await dependencies.thread_retirement_operations().end_thread_flow(
            thread_id,
            dict(thread),
            permanent=permanent,
            force=True,
            expected_runtime_generation=generation,
            expected_agent_id=expected_agent_id,
            expected_attach_token=expected_attach_token,
            # Exact physical process zero above supersedes the lossy `offline`
            # status hint.  A token itself rejects heartbeats, so offline can
            # never be used as a quiescence proof.
            require_expected_agent_offline=False,
            settle_status=settle_status,
            local_runtime_quiesced=runtime_exposed,
        )
    except HTTPException as exc:
        # Exact authority loss is a successful refusal; a retryable cleanup
        # failure remains represented by the durable marker for a later pass.
        # Either way it is worth a line — an all-refusing sweep used to be
        # indistinguishable from an idle one.
        if exc.status_code in {409, 503}:
            logger.warning(
                "Durable pinned retirement retry refused for thread %s (HTTP %s): %s",
                thread_id,
                exc.status_code,
                exc.detail,
            )
            return False
        raise
    return str(result.get("status") or "") in {
        "deleted" if permanent else settle_status
    }


async def stale_agent_detector(
    shutdown_event: asyncio.Event, *, dependencies: StaleAgentDetectorDependencies
) -> None:
    """Background task that reconciles agent state every 60 seconds.

    Two dimensions of reconciliation:

    1. Heartbeat freshness — agents that stopped reporting get marked offline,
       which in turn flips their threads to 'ended' and pauses their jobs.
    2. Self-reported consistency — agents that *are* heartbeating but report
       internally inconsistent state (working with no job, session bound to
       an ended thread) get flipped back to 'ready' so the dispatcher can
       reuse the slot. These zombies pass the heartbeat check and would
       otherwise hold pool slots indefinitely.

    Finally, offline agents older than 24h are GC'd to keep the table small.
    """
    logger.info("Stale agent detector started")

    async def _step(name: str, coro) -> Any:
        """Run one reconciliation step isolated from its siblings.

        Every step here repairs an INDEPENDENT inconsistency; a bug in one
        must degrade only that dimension. The 2026-07-11 incident proved the
        alternative: a bind-type bug in the graph-progress sweep silently
        disabled orphan-job recovery (and everything else after it) for ~36h
        because all steps shared one try block. See
        knowledge-history/done/stale_agent_detector_sql_crash_disables_recovery_sweeps.md.
        Returns None on failure — callers treat that as "no rows".
        """
        try:
            return await coro
        except Exception as e:
            logger.error(f"Stale agent detector step '{name}' failed: {e}")
            return None

    while not shutdown_event.is_set():
        try:
            # 1. Heartbeat-based: mark non-responsive agents offline
            offline_agents = await _step(
                "offline_marking",
                dependencies.store.mark_stale_agents_offline(timeout_minutes=3),
            )
            if offline_agents:
                logger.info(
                    f"Marked {len(offline_agents)} agent(s) as offline due to "
                    "missed heartbeats"
                )
                # Officer wake (centurion S4), scoped to the project each dead
                # agent was serving (derived from its assigned/last job): a
                # failing agent in one project is that officer's news, not the
                # whole roster's (owner ruling, 2026-08). Agents with no
                # derivable project keep the historical fleet-wide fan-out —
                # a warm-pool agent dying genuinely is capacity news for every
                # officer. 10-min debounce on 'fleet' keeps a flapping node
                # from spamming.
                offline_by_project: dict[str, int] = {}
                unattributed_offline = 0
                for agent_row in offline_agents:
                    agent_project = agent_row.get("project_id")
                    if agent_project:
                        offline_by_project[str(agent_project)] = (
                            offline_by_project.get(str(agent_project), 0) + 1
                        )
                    else:
                        unattributed_offline += 1
                if offline_by_project:
                    await _step(
                        "officer_fleet_offline",
                        notify_owning_officers(
                            dependencies.store,
                            {
                                project_id: {
                                    "summary": (
                                        f"{n} agent(s) marked offline "
                                        "(missed heartbeats)"
                                    )
                                }
                                for project_id, n in offline_by_project.items()
                            },
                            source="fleet",
                            dedup_key="fleet:agents_offline",
                        ),
                    )
                if unattributed_offline:
                    await _step(
                        "officer_fleet_offline",
                        notify_all_officers(
                            dependencies.store,
                            source="fleet",
                            dedup_key="fleet:agents_offline",
                            payload={
                                "summary": (
                                    f"{unattributed_offline} agent(s) marked "
                                    "offline (missed heartbeats)"
                                )
                            },
                        ),
                    )
                _kick_officer_event_drain(dependencies.store)

            # 2. Consistency-based: release slots held by zombie agents
            stuck_working = await _step(
                "stuck_working", dependencies.store.mark_stuck_working_agents_ready()
            )
            if stuck_working:
                logger.info(
                    f"Released {stuck_working} agent(s) stuck in 'working' with no job"
                )
                dependencies.trigger_dispatch()
            stalled_working = await _step(
                "graph_progress_stall",
                dependencies.store.mark_stalled_working_agents_by_graph_progress(
                    stall_minutes=10
                ),
            )
            if stalled_working:
                logger.info(
                    "Released %d working agent(s) with no graph-progress "
                    "for the stall interval",
                    stalled_working,
                )
                dependencies.trigger_dispatch()
            stuck_session = await _step(
                "stuck_session", dependencies.store.mark_stuck_session_agents_ready()
            )
            if stuck_session:
                logger.info(
                    f"Released {stuck_session} agent(s) stuck in 'session' "
                    f"on ended thread"
                )

            # 2b. STOPGAP — reap session agents wedged with NO bound thread/job.
            # mark_stuck_session_agents_ready (above) can't reach these: its
            # predicate needs thread_id IS NOT NULL, and a *live* agent
            # re-asserts 'session' on every 5s heartbeat so a flip-to-ready
            # never sticks — deleting the pod is the only actuation that does.
            # Scoped to thread_id + current_job_id both NULL (holds nothing
            # user-visible), so it never touches a thread-bound live session
            # (the 2026-06-10 incident). Proper fix = the intent/observed split
            # in knowledge-base/knowledge/features/unified_instance_lifecycle.md. Tracking:
            # knowledge-base/knowledge/issues/lifecycle_session_agents_without_thread_never_drain.md
            orphaned_sessions = await _step(
                "orphaned_session_reap",
                dependencies.store.reap_orphaned_session_agents(grace_minutes=5),
            )
            for orphan in orphaned_sessions or []:
                deleted = await _step(
                    "orphaned_session_pod_delete",
                    dependencies.agent_provisioner.delete_agent_pod(
                        orphan["hostname"],
                        expected_pod_uid=str(orphan.get("pod_uid") or ""),
                    ),
                )
                logger.warning(
                    "Reaped orphaned session agent %s (pod=%s, deleted=%s): "
                    "'session' with no thread/job past grace",
                    orphan["id"],
                    orphan["hostname"],
                    deleted,
                )

            # 2c. A failed warm attach rotates G1 -> unbound G2 and records an
            # append-only outcome. The request-local scheduler is only the
            # latency fast path; this durable scan is the restart/transient-
            # failure owner for headless sessions. Every task remains keyed to
            # the exact retired tuple and may provision only the named G2.
            attach_abort_successors = await _step(
                "attach_abort_successors",
                dependencies.store.list_retryable_thread_attach_abort_successors(
                    limit=25
                ),
            )
            for successor in attach_abort_successors or []:
                if not isinstance(successor, Mapping):
                    continue
                dependencies.schedule_attach_abort_successor(
                    str(successor.get("thread_id") or ""),
                    retired_runtime_generation=str(
                        successor.get("retired_runtime_generation") or ""
                    ),
                    retired_attach_token=str(
                        successor.get("retired_attach_token") or ""
                    ),
                    retired_agent_id=str(successor.get("retired_agent_id") or ""),
                )

            # 3. Propagate: exact pinned runtimes bound to offline agents go
            # through begin -> exact cleanup -> settle. The old set-based
            # status write made Resume visible before cleanup and let stale
            # name deletes destroy its successor.
            ended_candidates = await _step(
                "orphaned_threads_ended",
                dependencies.store.mark_orphaned_threads_ended(),
            )
            if ended_candidates:
                retired = 0
                for candidate in ended_candidates:
                    if isinstance(candidate, Mapping) and await _step(
                        "retire_orphaned_pinned_runtime",
                        retire_orphaned_pinned_runtime(
                            candidate, dependencies=dependencies
                        ),
                    ):
                        retired += 1
                if retired:
                    logger.info(
                        "Retired %d offline pinned runtime(s) through exact End",
                        retired,
                    )

            # 3b. Paused offline runtimes use the same safe funnel. They settle
            # as resumable ended sessions rather than exposing an automatic
            # suspended wake before exact cleanup has completed.
            suspended_candidates = await _step(
                "orphaned_threads_suspended",
                dependencies.store.mark_orphaned_threads_suspended(),
            )
            if suspended_candidates:
                retired = 0
                for candidate in suspended_candidates:
                    if isinstance(candidate, Mapping) and await _step(
                        "retire_orphaned_paused_runtime",
                        retire_orphaned_pinned_runtime(
                            candidate, dependencies=dependencies
                        ),
                    ):
                        retired += 1
                if retired:
                    logger.info(
                        "Retired %d paused offline pinned runtime(s) through exact End",
                        retired,
                    )

            # 3c. A hidden Begin is a short-lived admission preflight, not an
            # End instruction. If its owner dies before the append-only
            # authorization edge, exact expiry reopens the same runtime. The
            # row lock makes authorize-vs-expire choose exactly one outcome.
            expired_preflights = await _step(
                "stale_pinned_retirement_preflights",
                dependencies.store.abort_stale_pinned_retirement_preflights(
                    grace_seconds=PINNED_RETIREMENT_PREFLIGHT_GRACE_SECONDS,
                    limit=25,
                ),
            )
            if expired_preflights:
                logger.warning(
                    "Reopened %d abandoned pinned retirement preflight(s)",
                    len(expired_preflights),
                )

            # 3d. Authorized Begin is durable.  If an orchestrator/agent dies after it
            # closes admission, Resume must remain blocked until another
            # replica finishes the immutable captured disposition.  Only
            # sufficiently old markers whose exact actor is absent/offline
            # are nominated; the shared advisory lock serializes replicas.
            pending_retirements = await _step(
                "pending_pinned_retirements",
                dependencies.store.list_retryable_pinned_retirements(
                    grace_seconds=PINNED_RETIREMENT_RETRY_GRACE_SECONDS,
                    limit=25,
                    proven_grace_seconds=PINNED_RETIREMENT_PROVEN_RETRY_GRACE_SECONDS,
                ),
            )
            if pending_retirements:
                retired = 0
                for candidate in pending_retirements:
                    if isinstance(candidate, Mapping) and await _step(
                        "retry_pending_pinned_retirement",
                        retry_pending_pinned_retirement(
                            candidate, dependencies=dependencies
                        ),
                    ):
                        retired += 1
                if retired:
                    logger.info(
                        "Completed %d durable pinned retirement retry(s)", retired
                    )
                unresolved = len(pending_retirements) - retired
                if unresolved:
                    logger.warning(
                        "%d durable pinned retirement(s) remain unresolved after "
                        "this pass; each refusal is logged above",
                        unresolved,
                    )

            # Static Docker containers survive owner termination.  Their
            # exact inventory lease plus the terminal job/thread row is the
            # durable retry owner for managed-repository ssh-agent process
            # retirement.  This sweep closes crashes between a terminal DB
            # transition and cleanup, retries typed retirement failures, and
            # reclaims an external operation whose bounded deadline elapsed.
            docker_retirement_claims = await _step(
                "terminal_docker_workspace_retirement_claim",
                dependencies.store.claim_terminal_docker_workspace_retirements(),
            )
            for claim in docker_retirement_claims or []:
                await _step(
                    "terminal_docker_workspace_retirement_settle",
                    dependencies.docker_provisioner.settle_claimed_terminal_workspace_retirement(
                        claim
                    ),
                )

            # 4. Legacy compatibility: pre-lease pinned jobs assigned to
            # offline/non-working agents -> paused. The database predicate
            # excludes every non-NULL lease; ordering cannot steal a leased
            # row from the authoritative expiry circuit below.
            recovered = await _step(
                "orphaned_job_recovery",
                dependencies.store.recover_orphaned_jobs(
                    completion_commands_enabled=dependencies.completion_commands_enabled
                ),
            )
            if recovered:
                logger.info(
                    f"Recovered {recovered.count} orphaned job(s) from offline agents"
                )
                # Scoped to each job's owning project officer (owner ruling,
                # 2026-08): a recovered job is not fleet news. Jobs with no
                # project — or projects with no commissioned officer — notify
                # nobody.
                orphans_by_project: dict[str, list[str]] = {}
                for job in recovered.recovered_jobs:
                    if job.project_id:
                        orphans_by_project.setdefault(job.project_id, []).append(
                            job.job_id
                        )
                if orphans_by_project:
                    await _step(
                        "officer_fleet_orphans",
                        notify_owning_officers(
                            dependencies.store,
                            {
                                project_id: {
                                    "summary": (
                                        f"{len(job_ids)} orphaned job(s) "
                                        "auto-paused for re-dispatch "
                                        "(agent offline): "
                                        + ", ".join(
                                            str(job_id)[:8] for job_id in job_ids[:5]
                                        )
                                    )
                                }
                                for project_id, job_ids in orphans_by_project.items()
                            },
                            source="fleet",
                            dedup_key="fleet:orphans_recovered",
                        ),
                    )
                    _kick_officer_event_drain(dependencies.store)
                dependencies.trigger_dispatch()

            # 4b. Job execution lease: expired lease == orphaned, decided
            # purely by the DB clock — no agents-table join, no dependency on
            # step 1 having run. This is the sole automatic
            # infrastructure-loss authority for leased pinned rows; step 4 is
            # constrained to genuine pre-lease NULL-lease compatibility rows.
            lease_recovery_kwargs: dict[str, Any] = {
                "completion_commands_enabled": dependencies.completion_commands_enabled,
            }
            if getattr(dependencies.audit_reader, "is_available", False):
                lease_recovery_kwargs["audit_fingerprint_provider"] = (
                    dependencies.audit_reader.get_audit_counts_strict
                )
            lease_recovery = await _step(
                "lease_expiry_recovery",
                dependencies.store.recover_expired_lease_jobs(**lease_recovery_kwargs),
            )
            recovered_lease_ids = (
                lease_recovery.recovered_job_ids if lease_recovery is not None else ()
            )
            circuit_trips = (
                lease_recovery.circuit_trips if lease_recovery is not None else ()
            )
            for _job_id in recovered_lease_ids:
                logger.warning(
                    "Job %s recovered by lease expiry — its agent stopped "
                    "renewing (pod died, wedged, or a failed dispatch handoff); "
                    "re-queued for dispatch",
                    _job_id,
                )
            if recovered_lease_ids:
                # Recoveries below the containment threshold notify only each
                # job's owning project officer (owner ruling, 2026-08: one
                # livelocked job must not wake every officer each sweep —
                # ~10-min all night, in one observed case). Jobs with no
                # project, or projects with no commissioned officer, notify
                # nobody. The circuit-trip event below is unchanged: it is
                # inserted transactionally at the owning project's post.
                leases_by_project: dict[str, list[str]] = {}
                for job in lease_recovery.recovered_jobs:
                    if job.project_id:
                        leases_by_project.setdefault(job.project_id, []).append(
                            job.job_id
                        )
                if leases_by_project:
                    await _step(
                        "officer_fleet_leases",
                        notify_owning_officers(
                            dependencies.store,
                            {
                                project_id: {
                                    "summary": (
                                        f"{len(job_ids)} job(s) recovered by "
                                        "lease expiry: "
                                        + ", ".join(
                                            str(job_id)[:8] for job_id in job_ids[:5]
                                        )
                                    )
                                }
                                for project_id, job_ids in leases_by_project.items()
                            },
                            source="fleet",
                            dedup_key="fleet:lease_recovered",
                        ),
                    )
                    _kick_officer_event_drain(dependencies.store)
            for trip in circuit_trips:
                logger.error(
                    "Job %s parked by redispatch circuit after %s unchanged "
                    "lease recoveries (project=%s, officer_route=%s, queued=%s)",
                    trip.job_id,
                    trip.unchanged_recoveries,
                    trip.project_id,
                    trip.officer_destination,
                    trip.notification_queued,
                )
            if circuit_trips:
                # The recovery transaction already inserted the owning
                # project's durable outbox row. This is only a fast drain kick;
                # vacant posts retain the same incident in their durable ledger.
                _kick_officer_event_drain(dependencies.store)
            if recovered_lease_ids:
                dependencies.trigger_dispatch()

            # 5. GC: drop offline agent rows older than 24h
            gc_count = await _step(
                "offline_gc", dependencies.store.gc_offline_agents(retention_hours=24)
            )
            if gc_count:
                logger.info(f"GC'd {gc_count} offline agent record(s) > 24h old")
        except Exception as e:
            # Last resort — individual steps are isolated above, so anything
            # landing here is a bug in the loop scaffolding itself.
            logger.error(f"Error in stale agent detector: {e}")

        # Wait 60 seconds or until shutdown
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=60.0)
            break  # Shutdown signaled
        except asyncio.TimeoutError:
            pass  # Continue loop

    logger.info("Stale agent detector stopped")
