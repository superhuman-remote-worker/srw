"""Session attention: permission reminders, attention sleep and decision wake.

R1.B10 moved these bodies out of the application module. The application still
owns task creation, leader gating, cadence and shutdown (B11); it hands each
body a :class:`SessionAttentionDependencies` whose late-bound collaborators are
providers read per call.

State machine (headless_persistent_sessions.md Phase 5):

    active ─→ awaiting_user (agent: natural pause + no WS subscriber)
    awaiting_user ─→ suspended (attention sleep after TTL)
    awaiting_user ─→ active (agent: subscriber reattach, clears timer)
    suspended ─→ active (magic-link wake or REST reattach restores workspace)

Magic-link "extend window" POSTs bump ``awaiting_user_since`` forward so the
watchdog re-arms; ``threads.extend_count`` caps the bumps at 4 (4h total).

Today's "tethered" signal for pinned sessions is WS-only. SSE-only consumers
(MCP, curl) do not block pinned suspension; they rely on magic-link wake to
bring the session back. Stateless sessions use durable client presence.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException

from orchestrator.services import headless_notifications
from orchestrator.services.persistent_recycler import read_recycle_record
from orchestrator.services.session_class_policy import require_stateless_workspace
from orchestrator.services.session_provisioner import ensure_session_workspace
from orchestrator.services.session_runtime_admission import (
    same_thread_runtime_authority,
    thread_runtime_authority,
)
from orchestrator.services.session_runtime_identity import (
    thread_uses_pinned_execution,
)
from orchestrator.services.workspace_lifecycle import EnsureOutcome
from shared.runtime.core.loader import canonical_config_name
from shared.thread_presence import promote_expired_stateless_pauses

logger = logging.getLogger(__name__)


ATTENTION_SLEEP_INTERVAL_S: int = int(
    os.environ.get("HEADLESS_ATTENTION_SLEEP_INTERVAL_S", "60")
)
ATTENTION_SLEEP_MINUTES: int = int(
    os.environ.get("HEADLESS_ATTENTION_SLEEP_MINUTES", "60")
)


@dataclass(frozen=True, slots=True)
class SessionAttentionDependencies:
    """Application-owned collaborators for attention sleep, reminders and wake.

    ``persistent_thread_recycler`` and ``thread_retirement_operations`` are
    providers: startup binds the recycler after the provisioners, and the
    retirement operations are recomposed per call from current application
    state.
    """

    store: Any
    container_provisioner: Any
    workspace_suspension: Any
    persistent_provisioner: Any
    persistent_thread_recycler: Callable[[], Any]
    emit_session_provisioning_failure: Callable[..., Awaitable[None]]
    thread_retirement_operations: Callable[[], Any]
    notification_service: Any
    cockpit_url: Callable[[], str]


# ---------------------------------------------------------------------------
# Permission-decision wake (magic link)
# ---------------------------------------------------------------------------


async def wake_stateless_after_permission_decision(
    thread_id: str,
    *,
    permission_request_id: str | None,
    dependencies: SessionAttentionDependencies,
) -> None:
    """Wake one queue-served permission continuation without binding a pod.

    A magic-link task can run well after its originating request was resolved.
    Revalidate every authority under the global ``threads -> run_queue`` lock
    order: exact stateless lane/class/tier, no pinned-agent binding, the exact
    terminal permission row, and a queued/leased session turn whose human input
    is still unconsumed.  ``done`` is deliberately not revived: no durable
    permission-continuation watermark exists yet, so a done row would hit the
    executor's skip-if-answered edge and falsely claim the tool resumed.

    The queue row itself is left untouched.  A live lease keeps ownership; a
    queued retry keeps its token/fairness/affinity.  Workspace convergence uses
    the owner-keyed session provisioner, which restores a Kubernetes sandbox,
    refreshes a virtual binding, and is a no-op for ``none``.  It never creates
    a persistent agent pod.
    """
    from shared.run_queue import (
        LANE_STATELESS,
        STATE_LEASED,
        STATE_QUEUED,
        UNIT_KIND_SESSION_TURN,
    )

    if permission_request_id is None:
        logger.warning(
            "magic-link wake: refusing unfenced stateless wake for thread %s",
            thread_id,
        )
        return

    store = dependencies.store
    should_ensure_workspace = False
    async with store.acquire() as conn:
        async with conn.transaction():
            locked_thread = await conn.fetchrow(
                "SELECT id, execution_lane, agent_id, status, metadata "
                "FROM threads WHERE id = $1::uuid FOR UPDATE",
                thread_id,
            )
            if locked_thread is None:
                return
            thread = dict(locked_thread)
            if (
                thread.get("execution_lane") != LANE_STATELESS
                or thread.get("agent_id") is not None
            ):
                logger.warning(
                    "magic-link wake: stateless authority moved for thread %s "
                    "(lane=%r agent_id=%r)",
                    thread_id,
                    thread.get("execution_lane"),
                    thread.get("agent_id"),
                )
                return
            try:
                require_stateless_workspace(thread)
            except HTTPException as exc:
                logger.warning(
                    "magic-link wake: refusing stateless workspace/class for "
                    "thread %s: %s",
                    thread_id,
                    exc.detail,
                )
                return

            # Keep the repository-wide threads -> run_queue lock order.  The
            # lock makes the pending-input test atomic with a concurrent claim,
            # completion, release or reaper steal.
            queue = await conn.fetchrow(
                "SELECT state, input_seq, consumed_seq "
                "FROM run_queue "
                "WHERE unit_id = $1::uuid AND unit_kind = $2 "
                "FOR UPDATE",
                thread_id,
                UNIT_KIND_SESSION_TURN,
            )
            if queue is None:
                logger.warning(
                    "magic-link wake: no session queue authority for thread %s",
                    thread_id,
                )
                return
            queue_state = str(queue["state"] or "")
            input_seq = queue["input_seq"]
            consumed_seq = queue["consumed_seq"]
            has_unconsumed_input = input_seq is not None and (
                consumed_seq is None or int(input_seq) > int(consumed_seq)
            )
            if (
                queue_state not in {STATE_QUEUED, STATE_LEASED}
                or not has_unconsumed_input
            ):
                logger.warning(
                    "magic-link wake: refusing stale stateless continuation for "
                    "thread %s (queue_state=%s input_seq=%r consumed_seq=%r)",
                    thread_id,
                    queue_state,
                    input_seq,
                    consumed_seq,
                )
                return

            decision = await conn.fetchval(
                "SELECT status FROM thread_permission_requests "
                "WHERE id = $2::uuid AND thread_id = $1::uuid "
                "  AND status IN ('approved', 'denied')",
                thread_id,
                permission_request_id,
            )
            if decision not in {"approved", "denied"}:
                logger.warning(
                    "magic-link wake: exact permission fence rejected thread %s "
                    "request %s",
                    thread_id,
                    permission_request_id,
                )
                return

            thread_status = str(thread.get("status") or "")
            if thread_status not in {"active", "awaiting_user", "suspended"}:
                logger.warning(
                    "magic-link wake: thread %s is not resumable (status=%r)",
                    thread_id,
                    thread_status,
                )
                return

            if thread_status in {"awaiting_user", "suspended"}:
                updated = await conn.fetchval(
                    "UPDATE threads "
                    "SET status = 'active', "
                    "    awaiting_user_since = NULL, "
                    "    extend_count = 0, "
                    "    control_admission_agent_id = NULL "
                    "WHERE id = $1::uuid "
                    "  AND execution_lane = $2 "
                    "  AND agent_id IS NULL "
                    "  AND status IN ('suspended', 'awaiting_user') "
                    "RETURNING id",
                    thread_id,
                    LANE_STATELESS,
                )
                if updated is None:
                    return
            should_ensure_workspace = True

    if not should_ensure_workspace:
        return
    # Queue/lifecycle admission commits before this potentially slow side
    # effect.  A claimant may arrive first, but its attach path polls the same
    # durable workspace lifecycle until it is ready.
    await ensure_session_workspace(
        thread_id,
        db=store,
        provisioner=dependencies.container_provisioner,
        suspension=dependencies.workspace_suspension,
    )
    logger.info(
        "magic-link wake: stateless permission continuation admitted for "
        "thread %s request %s",
        thread_id,
        permission_request_id,
    )


async def wake_after_permission_decision(
    thread_id: str,
    *,
    permission_request_id: str | None = None,
    dependencies: SessionAttentionDependencies,
) -> None:
    """Wake a suspended thread after a magic-link decision.

    Fire-and-forget — the HTTP response has already returned. Stateless
    sessions delegate to the queue-fenced, topology-neutral helper above.
    Pinned sessions preserve the historical resume pattern: restore from S3,
    then spawn the agent pod if the persistent provisioner is wired.
    """
    store = dependencies.store
    try:
        thread = await store.get_thread(thread_id)
        if not thread:
            return
        if thread.get("execution_lane") == "stateless":
            await wake_stateless_after_permission_decision(
                thread_id,
                permission_request_id=permission_request_id,
                dependencies=dependencies,
            )
            return
        if not thread_uses_pinned_execution(thread):
            logger.warning(
                "magic-link wake: refusing pinned wake for thread %s on "
                "execution lane %r",
                thread_id,
                thread.get("execution_lane"),
            )
            return
        wake_authority = thread_runtime_authority(thread)
        if wake_authority is None:
            return
        metadata = thread.get("metadata") or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (json.JSONDecodeError, TypeError):
                metadata = {}
        persistent_provisioner = dependencies.persistent_provisioner
        recycle = read_recycle_record(metadata)
        if isinstance(recycle, dict) and recycle.get("phase") not in {
            None,
            "",
            "complete",
            "cancelled",
        }:
            recycler = dependencies.persistent_thread_recycler()
            if recycler is not None:
                await recycler.request_and_reconcile(
                    thread_id=thread_id,
                    reason="resume_during_recycle",
                    expected_build_sha=persistent_provisioner.expected_build_sha,
                    expected_project_id=(
                        str(thread.get("project_id"))
                        if thread.get("project_id")
                        else None
                    ),
                )
            return
        suspension = dependencies.workspace_suspension
        ws_ctx = metadata.get("workspace_container") or {}
        ws_status = ws_ctx.get("status")
        if ws_status == "suspended" and suspension.is_enabled:
            logger.info(
                "magic-link wake: restoring suspended workspace for thread %s",
                thread_id,
            )
            restored = await ensure_session_workspace(
                thread_id,
                db=store,
                provisioner=dependencies.container_provisioner,
                suspension=suspension,
                expected_runtime_generation=wake_authority.generation,
            )
            if restored is None or restored.outcome is EnsureOutcome.FAILED:
                logger.warning(
                    "magic-link wake: workspace restore failed or lost authority "
                    "for thread %s",
                    thread_id,
                )
                return

        # Publish wake only to the exact post-suspension generation. A G2
        # restore delayed across another End/Resume cannot wake G3.
        async with store.acquire() as conn:
            woke = await conn.fetchval(
                "UPDATE threads "
                "SET status = 'active', "
                "    awaiting_user_since = NULL, "
                "    extend_count = 0, "
                "    control_admission_agent_id = NULL "
                "WHERE id = $1::uuid "
                "  AND execution_lane='pinned' "
                "  AND runtime_generation=$2::uuid "
                "  AND runtime_retirement_token IS NULL "
                "  AND status IN ('suspended', 'awaiting_user') "
                "RETURNING id",
                thread_id,
                wake_authority.generation,
            )
        if woke is None and not same_thread_runtime_authority(
            await store.get_thread(thread_id), wake_authority
        ):
            return

        # Agent pod may also have been deleted on suspension
        # (workspace_suspension.py:502-504). Re-provision if a persistent
        # provisioner is configured. fire-and-forget — the agent's boot
        # will restore the LangGraph checkpoint and re-enter permission_check
        # for the same tool_call_id, where the select-first guard picks up
        # the decision we just UPDATEd.
        current = await store.get_thread(thread_id)
        if not same_thread_runtime_authority(current, wake_authority):
            return
        if persistent_provisioner is not None and not current.get("agent_id"):
            config_name = canonical_config_name(
                thread.get("config_name", "session_base")
            )
            emit_failure = dependencies.emit_session_provisioning_failure

            async def _create_after_magic_link() -> None:
                # This closure sits lexically inside the wake handler's
                # try/except, but it is scheduled as its own task — so that
                # handler NEVER sees anything raised here. Its own guard is the
                # only thing between a raise and a silently vanished wake.
                try:
                    result = await persistent_provisioner.create_agent_pod(
                        thread_id,
                        config_name=config_name,
                        expected_runtime_generation=wake_authority.generation,
                    )
                    if not result.usable:
                        logger.warning(
                            "magic-link persistent provisioning for thread %s "
                            "is %s (%s)",
                            thread_id,
                            result.status.value,
                            result.failure_class or "no-detail",
                        )
                        await emit_failure(
                            thread_id,
                            str(thread.get("user_id") or "") or None,
                            wake_authority,
                            f"magic-link wake provisioning {result.status.value}"
                            f" ({result.failure_class or 'no-detail'})",
                        )
                except Exception as exc:
                    logger.exception(
                        "magic-link persistent provisioning for thread %s raised: %s",
                        thread_id,
                        exc,
                    )
                    await emit_failure(
                        thread_id,
                        str(thread.get("user_id") or "") or None,
                        wake_authority,
                        str(exc),
                    )

            asyncio.create_task(
                _create_after_magic_link(),
                name=f"phase5-create-agent-{thread_id[:8]}",
            )
    except Exception as e:
        logger.warning(
            "magic-link wake task failed for thread %s: %s",
            thread_id,
            e,
        )


# ---------------------------------------------------------------------------
# Sweep bodies (the application owns their tasks, leader gates and shutdown)
# ---------------------------------------------------------------------------


async def thread_permission_notify_sweeper(
    shutdown_event: asyncio.Event,
    *,
    dependencies: SessionAttentionDependencies,
) -> None:
    """Background task: a permission request that has waited longer than
    HEADLESS_NOTIFY_AGE_S without a decision becomes a ``session_permission``
    feed row for the thread owner — ``high``, so the mail (with the two magic
    links) goes out now, and the row resolves when the gate is decided by any
    path. In-session gates are answered within seconds through the agent's
    LISTEN, so only abandoned ones ever get here.

    Runs every HEADLESS_NOTIFY_INTERVAL_S (default 30s). Idempotent: the
    feed row is keyed on the request id, and rows already recorded are
    filtered out so the magic-link tokens are minted once.

    Best-effort. Survives transient errors by logging and continuing.
    """
    interval_s = int(os.environ.get("HEADLESS_NOTIFY_INTERVAL_S", "30"))
    age_threshold_s = int(os.environ.get("HEADLESS_NOTIFY_AGE_S", "30"))
    logger.info(
        "Headless permission-notify sweeper started (interval=%ds, age_threshold=%ds)",
        interval_s,
        age_threshold_s,
    )
    store = dependencies.store
    cockpit_external_url = dependencies.cockpit_url()

    while not shutdown_event.is_set():
        try:
            async with store.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT r.id, r.thread_id, r.tool_name, r.tool_args, "
                    "       r.requested_at, t.user_id, t.title "
                    "FROM thread_permission_requests r "
                    "JOIN threads t ON t.id = r.thread_id "
                    "WHERE r.status = 'pending' "
                    "  AND r.requested_at < now() - ($1::int * interval '1 second') "
                    "  AND NOT EXISTS ("
                    "    SELECT 1 FROM notifications n "
                    "    WHERE n.source_kind = 'permission_request' "
                    "      AND n.source_id = r.id::text"
                    "  ) "
                    "ORDER BY r.requested_at ASC "
                    "LIMIT 50",
                    age_threshold_s,
                )
            for row in rows:
                try:
                    result = await headless_notifications.record_permission_pending(
                        store,
                        dependencies.notification_service,
                        row=dict(row),
                        cockpit_external_url=cockpit_external_url,
                    )
                    if result.get("status") == "recorded":
                        logger.info(
                            "Recorded permission-pending notification "
                            "(thread=%s req=%s)",
                            str(row["thread_id"])[:8],
                            str(row["id"])[:8],
                        )
                except Exception as e:
                    logger.warning(
                        "Permission-pending notification failed (req=%s): %s",
                        str(row["id"])[:8],
                        e,
                    )
        except Exception as e:
            logger.warning("headless permission-notify sweep error: %s", e)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=float(interval_s))
            break
        except asyncio.TimeoutError:
            pass
    logger.info("Headless permission-notify sweeper stopped")


async def attention_sleep_sweeper(
    shutdown_event: asyncio.Event,
    *,
    dependencies: SessionAttentionDependencies,
) -> None:
    """Background task: suspend threads stuck in awaiting_user past their TTL.

    Runs every HEADLESS_ATTENTION_SLEEP_INTERVAL_S (default 60s). Each
    qualifying pinned generation enters the same durable retirement funnel as
    owner End, settling to ``suspended`` only after generation-fenced staging
    and exact resource cleanup. Resume stays closed for the entire operation.

    Best-effort: a transient failure (DB unavailable, suspend service
    error) is logged and retried on the next tick.
    """
    interval_s = ATTENTION_SLEEP_INTERVAL_S
    ttl_minutes = ATTENTION_SLEEP_MINUTES
    logger.info(
        "Attention-sleep sweeper started (interval=%ds, ttl=%dmin)",
        interval_s,
        ttl_minutes,
    )
    store = dependencies.store

    while not shutdown_event.is_set():
        try:
            # A disconnect intentionally leaves a short TTL grace so reloads
            # and multi-tab handoffs never flicker. If a turn reached its
            # natural pause inside that grace, converge it once the queue is
            # durably done and the final client TTL has expired. This is
            # independent of workspace suspension being enabled.
            try:
                promoted = await promote_expired_stateless_pauses(store, limit=50)
                if promoted:
                    logger.info(
                        "presence expiry promoted %d stateless thread(s) "
                        "to awaiting_user",
                        len(promoted),
                    )
            except Exception as exc:
                # Presence convergence is additive. It must never suppress the
                # pre-existing awaiting_user suspension sweep on the same tick.
                logger.warning("presence expiry promotion failed: %s", exc)
            if dependencies.workspace_suspension.is_enabled:
                async with store.acquire() as conn:
                    # Phase 6: per-thread TTL resolution. Priority order is
                    # (1) thread.metadata.config_override.headless overrides,
                    # (2) users.settings.persistent_agent overrides,
                    # (3) the global HEADLESS_ATTENTION_SLEEP_MINUTES default.
                    # ttl <= 0 disables the watchdog for that thread, matching
                    # the cockpit UX of "Never auto-suspend".
                    rows = await conn.fetch(
                        "SELECT t.id, t.status, t.execution_lane, "
                        "       t.runtime_generation, t.agent_id, "
                        "       t.runtime_attach_token "
                        "FROM threads t "
                        "LEFT JOIN users u ON u.id = t.user_id "
                        "WHERE t.status = 'awaiting_user' "
                        "  AND t.execution_lane <> 'stateless' "
                        "  AND t.awaiting_user_since IS NOT NULL "
                        # Officer sessions never sleep via attention-sleep —
                        # their lifecycle belongs to the officer watchdog
                        # (centurion.md §4). Belt-and-suspenders: the agent
                        # side already skips the awaiting_user flip for them.
                        "  AND COALESCE(t.metadata->'config_override'->'officer'"
                        "->>'enabled','false') <> 'true' "
                        "  AND COALESCE("
                        "    NULLIF(t.metadata->'config_override'->'headless'->>'attention_sleep_minutes', '')::int, "
                        "    NULLIF(u.settings->'persistent_agent'->>'headless_attention_sleep_minutes', '')::int, "
                        "    $1::int"
                        "  ) > 0 "
                        "  AND t.awaiting_user_since < now() - make_interval(mins => COALESCE("
                        "    NULLIF(t.metadata->'config_override'->'headless'->>'attention_sleep_minutes', '')::int, "
                        "    NULLIF(u.settings->'persistent_agent'->>'headless_attention_sleep_minutes', '')::int, "
                        "    $1::int"
                        "  )) "
                        "ORDER BY t.awaiting_user_since ASC "
                        "LIMIT 50",
                        int(ttl_minutes),
                    )

                for row in rows:
                    thread_id = str(row["id"])
                    try:
                        result = await dependencies.thread_retirement_operations().end_thread_flow(
                            thread_id,
                            dict(row),
                            permanent=False,
                            force=False,
                            expected_runtime_generation=str(row["runtime_generation"]),
                            expected_agent_id=(
                                str(row["agent_id"])
                                if row["agent_id"] is not None
                                else None
                            ),
                            expected_attach_token=(
                                str(row["runtime_attach_token"])
                                if row["runtime_attach_token"] is not None
                                else None
                            ),
                            settle_status="suspended",
                        )
                        if result.get("status") == "suspended":
                            logger.info(
                                "attention-sleep: thread %s suspended (was "
                                "awaiting_user >%dm)",
                                thread_id,
                                ttl_minutes,
                            )
                        else:
                            logger.info(
                                "attention-sleep: exact retirement declined "
                                "for thread %s (%s)",
                                thread_id,
                                result.get("status"),
                            )
                    except Exception as e:
                        logger.warning(
                            "attention-sleep: suspend failed for thread %s: %s",
                            thread_id,
                            e,
                        )
        except Exception as e:
            logger.warning("attention-sleep sweep error: %s", e)

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=float(interval_s))
            break
        except asyncio.TimeoutError:
            pass

    logger.info("Attention-sleep sweeper stopped")


__all__ = [
    "ATTENTION_SLEEP_INTERVAL_S",
    "ATTENTION_SLEEP_MINUTES",
    "SessionAttentionDependencies",
    "attention_sleep_sweeper",
    "thread_permission_notify_sweeper",
    "wake_after_permission_decision",
    "wake_stateless_after_permission_decision",
]
