"""The Officer watchdog — dumb-code guardian of commissioned sessions.

Extracted verbatim from ``orchestrator.main`` (R1.B07 lane O, census group
``S_OFFICER``; centurion.md §4). Three duties, none requiring judgment: file
the implicit ``sleep_max`` timer when an unheld officer has none pending; treat
a pending timer overdue past ``fire_at + grace`` **with a live pod** as a
delivery failure and kick the drain; submit a missing runtime to the shared
durable lifecycle owner. Leader-gated by the application; the recycler also
carries a durable generation/claim.

Two orderings are load-bearing and neither is re-derived here:

* **Runtime authority is maintained before hold/timer decisions.** A held
  Officer is still commissioned and may sleep for days; letting a conference
  hold skip credential maintenance would just move the 24 h cliff to another
  branch.
* **The implicit filing is gated on LAST ENGAGEMENT**, not on ai/tool row age:
  a turn's ai row only lands at turn end, so age alone re-files mid-turn.

This task outlives every request, so it carries its collaborators explicitly on
:class:`OfficerWatchdogDependencies` rather than looking anything up later.
``persistent_thread_recycler`` is a **callable** because the application
assigns that global during startup and the watchdog re-reads it each tick.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

from orchestrator.services.officer_metadata import thread_officer_meta
from orchestrator.services.runtime_actor import (
    maintain_current_officer_runtime,
    settle_officer_runtime_incident_notification,
)
from orchestrator.services.session_runtime_identity import thread_uses_pinned_execution

logger = logging.getLogger(__name__)


@dataclass
class OfficerWatchdogDependencies:
    """Collaborators the watchdog carries for its whole lifetime."""

    store: Any
    persistent_provisioner: Any
    persistent_thread_recycler: Callable[[], Any]
    kick_officer_event_drain: Callable[[Any], None]
    dispatch_officer_page: Callable[..., Awaitable[str | None]]
    conclude_conference_if_any: Callable[[dict[str, Any]], Awaitable[None]]
    officer_runtime_verification_enabled: Callable[[], bool]
    persistent_agent_reconciliation_enabled: Callable[[], bool]
    # (thread_id, thread) -> the decommission stand-down result. The same
    # end_thread_flow funnel as POST .../officer/decommission with force.
    stand_down_officer: Callable[[str, dict[str, Any]], Awaitable[Any]] | None = None


OFFICER_WATCHDOG_INTERVAL_S = int(os.getenv("OFFICER_WATCHDOG_INTERVAL_S", "60"))
OFFICER_WAKE_GRACE_MINUTES = int(os.getenv("OFFICER_WAKE_GRACE_MINUTES", "10"))


async def maintain_officer_runtime_authorization(
    officer_row: dict[str, Any], *, dependencies: OfficerWatchdogDependencies
) -> Any:
    """Run the credential-independent Officer liveness check and page once."""

    project_id = officer_row.get("project_id")
    thread_id = officer_row.get("id")
    if not project_id or not thread_id:
        return None
    outcome = await maintain_current_officer_runtime(
        dependencies.store,
        project_id=str(project_id),
        thread_id=str(thread_id),
        verification_enabled=dependencies.officer_runtime_verification_enabled(),
    )
    if outcome.incident_changed and outcome.authorized:
        dependencies.kick_officer_event_drain(dependencies.store)
    if (
        not outcome.notification_due
        or outcome.officer_incarnation is None
        or outcome.notification_claim_id is None
    ):
        return outcome
    delivered = False
    failure_class = "delivery"
    try:
        delivered = await dependencies.dispatch_officer_page(
            officer_row,
            str(thread_id),
            category="officer_runtime",
            dedup_key=f"officer_runtime_auth:{outcome.notification_claim_id}",
            subject="Officer authorization unavailable",
            message_md=(
                "The commissioned Officer cannot maintain its server-derived "
                "runtime authorization. Autonomous planning is paused to "
                "prevent unusable model spend; the server will retry with "
                "bounded backoff."
            ),
        )
        failure_class = "notifier_rejected" if not delivered else ""
    except Exception as exc:
        failure_class = type(exc).__name__[:128]
        logger.warning(
            "officer runtime authorization page failed for project %s",
            str(project_id)[:8],
        )
    await settle_officer_runtime_incident_notification(
        dependencies.store,
        project_id=str(project_id),
        thread_id=str(thread_id),
        officer_incarnation=outcome.officer_incarnation,
        notification_claim_id=outcome.notification_claim_id,
        delivered=delivered,
        failure_class=failure_class or None,
    )
    return outcome


SELF_RETIRED_OFFICER_REASON = "self_retired_runtime"


async def stand_down_self_retired_officer(
    officer_row: dict[str, Any], *, dependencies: OfficerWatchdogDependencies
) -> bool:
    """Decommission an officer whose own pod retired him, and tell the owner.

    Nothing in the orchestrator suspends a commissioned officer: the attention
    sweeper exempts him and the recycler replaces his pod under an unchanged
    thread. A 'suspended' officer is therefore a pod that retired itself, which
    an agent image from before 2026-11-06 does on every drain and SIGTERM. Its
    first recycle (the upgrade itself) left the officer suspended behind a
    hold nothing could release, waking every two hours to an authorization
    error (Better Resavio on main dev, 2026-08-30 to 2026-11-06). No path
    revives a suspended pinned officer, so the post is stood down the way
    decommission does it, his state kept for a recommission, and the owner is
    paged to recommission him. Returns True when the stand-down ran.
    """
    thread_id = str(officer_row["id"])
    if dependencies.stand_down_officer is None:
        return False
    thread = await dependencies.store.get_thread(thread_id)
    metadata = (thread or {}).get("metadata") or {}
    if (
        thread is None
        or thread.get("status") != "suspended"
        or thread.get("runtime_retirement_token") is not None
        or thread.get("agent_id") is not None
        # A suspended thread that still names its pod is the recycler's to
        # replace; only a settled self-retirement clears the marker.
        or (isinstance(metadata, dict) and metadata.get("agent_pod"))
        or not thread_uses_pinned_execution(thread)
        or not await dependencies.store.fetchval(
            "SELECT EXISTS (SELECT 1 FROM thread_runtime_retirement_outcomes "
            "WHERE thread_id=$1::uuid AND disposition='suspended' "
            "AND outcome='settled' AND NOT permanent)",
            thread_id,
        )
    ):
        return False
    logger.warning(
        "officer watchdog: officer thread %s retired itself (suspended); "
        "standing the post down for a recommission",
        thread_id[:8],
    )
    await dependencies.stand_down_officer(thread_id, thread)
    try:
        await dependencies.dispatch_officer_page(
            officer_row,
            thread_id,
            category="officer_runtime",
            dedup_key=f"officer_self_retired:{thread_id}",
            subject="Your officer was stood down and needs recommissioning",
            message_md=(
                "His pod ran an agent version from before 2026-11-06, which "
                "retires itself when it is replaced instead of handing over. "
                "That happened during an upgrade, so the post was stood down "
                "the way a decommission does it. His state is kept: "
                "recommission him from the project's Centurion tab to continue. "
                "Jobs he had dispatched keep running."
            ),
        )
    except Exception:
        logger.warning(
            "officer watchdog: stand-down page failed for thread %s",
            thread_id[:8],
        )
    return True


async def officer_watchdog_check_one(
    officer_row: dict, session_wake_svc, *, dependencies: OfficerWatchdogDependencies
) -> None:
    """One officer thread's watchdog pass — see officer_watchdog below."""
    thread_id = str(officer_row["id"])
    if officer_row.get("status") == "suspended" and (
        await stand_down_self_retired_officer(officer_row, dependencies=dependencies)
    ):
        # Before the authorization check: a self-retired officer has no
        # runtime to authorize, and that check would page the owner about it
        # every two hours instead of fixing it.
        return
    # P0 runtime authority is maintained before hold/timer decisions. A held
    # Officer is still commissioned and may remain asleep for days; letting a
    # conference hold skip credential maintenance would simply move the 24 h
    # cliff to another lifecycle branch.
    await maintain_officer_runtime_authorization(officer_row, dependencies=dependencies)
    officer_meta = thread_officer_meta(officer_row)
    hold = officer_meta.get("hold")
    if hold:
        # Conference hold (centurion.md §4): stand down entirely — no
        # implicit-timer filing, no overdue kicks, no respawn. But first,
        # self-heal a STALE hold: if the conference thread is gone, ended, or
        # idle-suspended (Legate walked away; the attention sweeper parked
        # it), the meeting is over — a missed end-hook must not hold the
        # officer forever. Concluding here releases the hold and enqueues the
        # brief wake (idempotent with the end-hook via insert-dedup on the
        # conference thread id).
        conf_tid = hold.get("thread_id") if isinstance(hold, dict) else None
        if conf_tid:
            conf = await dependencies.store.get_thread(str(conf_tid))
            if conf is None or conf.get("status") in ("ended", "suspended"):
                logger.warning(
                    "officer watchdog: stale conference hold on %s "
                    "(conference %s is %s) — concluding",
                    thread_id[:8],
                    str(conf_tid)[:8],
                    conf.get("status") if conf else "gone",
                )
                if conf is not None:
                    await dependencies.conclude_conference_if_any(conf)
                else:
                    project_id = officer_row.get("project_id")
                    if project_id:
                        await dependencies.store.set_project_officer_hold(
                            str(project_id),
                            expected_thread_id=thread_id,
                            hold=None,
                        )
                return  # next tick resumes normal duties, unheld
        return
    try:
        sleep_max = int(officer_meta.get("sleep_max_minutes") or 60)
    except (TypeError, ValueError):
        sleep_max = 60

    thread = await dependencies.store.get_thread(thread_id)
    if not thread_uses_pinned_execution(thread) or thread.get("status") == "ended":
        return
    agent = await session_wake_svc._resolve_live_agent(dependencies.store, thread)
    timer = await dependencies.store.get_pending_officer_timer(thread_id)
    now = datetime.now(timezone.utc)

    if agent is not None and thread.get("status") == "active":
        if timer is None:
            # Duty 1: implicit sleep_max. The transport files explicit
            # sleeps; when a turn ended without one (or the filing POST was
            # lost), the watchdog files the default on the officer's behalf
            # (centurion.md §4 — "absent a filing, the system is lazy for
            # him too"). Gated on LAST ENGAGEMENT (any transcript row or the
            # last delivered timer) — ai/tool age alone re-files mid-turn,
            # since a turn's ai row only lands at turn end (k3d smoke).
            last = await dependencies.store.get_officer_last_engagement(thread_id)
            if last is None or (now - last) > timedelta(minutes=sleep_max):
                await dependencies.store.enqueue_session_wake_event(
                    thread_id,
                    source="timer",
                    dedup_key="timer",
                    payload={
                        "minutes": sleep_max,
                        "reason": "implicit sleep_max (watchdog-filed)",
                    },
                    fire_at=now,
                )
                session_wake_svc.kick_event_drain(dependencies.store)
        else:
            fire_at = timer.get("fire_at")
            if fire_at is not None and (now - fire_at) > timedelta(
                minutes=OFFICER_WAKE_GRACE_MINUTES
            ):
                # Duty 2: overdue pending timer with a live pod = delivery
                # failure somewhere in the drain path. Kick it; the drain's
                # own release/retry handles a refusing pod.
                logger.warning(
                    "officer watchdog: timer overdue %.0fs for thread %s — "
                    "kicking drain",
                    (now - fire_at).total_seconds(),
                    thread_id[:8],
                )
                session_wake_svc.kick_event_drain(dependencies.store)
        return

    # Duty 3: a missing pod is another observation for the same durable
    # lifecycle owner used by image drift and the supported operator action.
    # The watchdog no longer clears bindings or creates pods on its own.
    recycler = dependencies.persistent_thread_recycler()
    if recycler is None or not dependencies.persistent_provisioner.is_available:
        logger.warning(
            "officer watchdog: lifecycle owner unavailable for thread %s",
            thread_id[:8],
        )
        return
    if not dependencies.persistent_agent_reconciliation_enabled():
        logger.info(
            "officer watchdog: automatic persistent reconciliation disabled "
            "for thread %s",
            thread_id[:8],
        )
        return
    observation = await recycler.observe(thread_id)
    if observation is not None:
        # The session-wake probe is intentionally stricter than Kubernetes
        # liveness and can bounce during attach or a transient network fault.
        # A real pod observation is not "missing" authority. The registered
        # lifecycle manager owns build/UID/agent reciprocity decisions and
        # will submit an explicit drift/mismatch reason when appropriate.
        logger.info(
            "officer watchdog: live probe missed thread %s but pod UID %s "
            "still exists; deferring to persistent lifecycle reconciliation",
            thread_id[:8],
            observation.pod_uid[:8],
        )
        return
    await recycler.request_and_reconcile(
        thread_id=thread_id,
        reason="missing_pod",
        expected_build_sha=dependencies.persistent_provisioner.expected_build_sha,
        observation=None,
        expected_project_id=str(officer_row.get("project_id") or ""),
    )


async def officer_watchdog(
    shutdown_event: asyncio.Event, *, dependencies: OfficerWatchdogDependencies
) -> None:
    """Dumb-code guardian of officer (centurion) sessions — centurion.md §4.

    Three duties, none requiring judgment: file the implicit ``sleep_max``
    timer when an unheld officer has none pending; treat a pending timer
    overdue past ``fire_at + grace`` with a live pod as a delivery failure
    and kick the drain; submit missing runtimes to the shared durable lifecycle
    owner. Leader-gated; the recycler also carries a durable generation/claim.
    """
    from orchestrator.services import session_wake as session_wake_svc

    logger.info(
        "Officer watchdog started (tick=%ds, grace=%dm)",
        OFFICER_WATCHDOG_INTERVAL_S,
        OFFICER_WAKE_GRACE_MINUTES,
    )
    while not shutdown_event.is_set():
        try:
            for officer_row in await dependencies.store.list_officer_threads():
                try:
                    await officer_watchdog_check_one(
                        officer_row, session_wake_svc, dependencies=dependencies
                    )
                except Exception:
                    logger.exception(
                        "officer watchdog: check failed for thread %s "
                        "(continuing with the rest)",
                        str(officer_row.get("id"))[:8],
                    )
        except Exception:
            logger.exception("officer watchdog tick raised; will retry next tick")
        try:
            await asyncio.wait_for(
                shutdown_event.wait(), timeout=OFFICER_WATCHDOG_INTERVAL_S
            )
            break
        except asyncio.TimeoutError:
            pass
    logger.info("Officer watchdog stopped")


__all__ = [
    "OFFICER_WAKE_GRACE_MINUTES",
    "SELF_RETIRED_OFFICER_REASON",
    "OFFICER_WATCHDOG_INTERVAL_S",
    "OfficerWatchdogDependencies",
    "maintain_officer_runtime_authorization",
    "officer_watchdog",
    "officer_watchdog_check_one",
    "stand_down_self_retired_officer",
]
