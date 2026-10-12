"""Delete-by-age retention loops for three append-only orchestrator logs.

R1.B11 home of the three retention sweepers that prune ``thread_events``,
``security_events`` and ``ssh_attachments`` on age. None of them is
leader-gated: a delete-by-age is idempotent, so two replicas racing one is
harmless -- the second finds nothing. The application owns task creation and
shutdown; each loop only takes the store it prunes and the shutdown event it
waits on.
"""

import asyncio
import logging
import os

logger = logging.getLogger(__name__)


async def thread_events_prune_sweeper(shutdown_event: asyncio.Event, *, store) -> None:
    """Background task that prunes the thread_events log on retention.

    Runs every THREAD_EVENTS_PRUNE_INTERVAL_S (default 300s). Two queries:
      - DELETE rows for threads in 'ended' status older than 24h.
      - DELETE rows for threads NOT in 'ended' older than 7 days.
      - Preserve any event that is still the only durable receipt for a
        pending session-control or exact-turn interrupt request; crash
        recovery terminalizes it first.

    Best-effort. Survives transient DB errors by logging and continuing.
    """
    interval_s = int(os.environ.get("THREAD_EVENTS_PRUNE_INTERVAL_S", "300"))
    logger.info("Thread-events prune sweeper started (interval=%ds)", interval_s)
    while not shutdown_event.is_set():
        try:
            async with store.acquire() as conn:
                ended_deleted = await conn.fetchval(
                    "WITH deleted AS ("
                    "  DELETE FROM thread_events "
                    "  WHERE thread_id IN ("
                    "    SELECT id FROM threads WHERE status = 'ended'"
                    "  ) "
                    "  AND created_at < now() - interval '24 hours' "
                    "  AND NOT EXISTS ("
                    "    SELECT 1 FROM thread_control_requests request "
                    "    WHERE request.id = thread_events.control_request_id "
                    "      AND request.outcome IS NULL"
                    "  ) "
                    "  AND NOT EXISTS ("
                    "    SELECT 1 FROM thread_interrupt_requests request "
                    "    WHERE request.id = thread_events.interrupt_request_id "
                    "      AND (request.outcome IS NULL "
                    "           OR (request.outcome = 'applied' "
                    "               AND NOT (COALESCE(request.result, '{}'::jsonb) "
                    "                        ? 'consumed_input_seq')))"
                    "  ) "
                    "  RETURNING 1"
                    ") SELECT COUNT(*) FROM deleted"
                )
                active_deleted = await conn.fetchval(
                    "WITH deleted AS ("
                    "  DELETE FROM thread_events "
                    "  WHERE thread_id IN ("
                    "    SELECT id FROM threads WHERE status <> 'ended'"
                    "  ) "
                    "  AND created_at < now() - interval '7 days' "
                    "  AND NOT EXISTS ("
                    "    SELECT 1 FROM thread_control_requests request "
                    "    WHERE request.id = thread_events.control_request_id "
                    "      AND request.outcome IS NULL"
                    "  ) "
                    "  AND NOT EXISTS ("
                    "    SELECT 1 FROM thread_interrupt_requests request "
                    "    WHERE request.id = thread_events.interrupt_request_id "
                    "      AND (request.outcome IS NULL "
                    "           OR (request.outcome = 'applied' "
                    "               AND NOT (COALESCE(request.result, '{}'::jsonb) "
                    "                        ? 'consumed_input_seq')))"
                    "  ) "
                    "  RETURNING 1"
                    ") SELECT COUNT(*) FROM deleted"
                )
            if (ended_deleted or 0) + (active_deleted or 0) > 0:
                logger.info(
                    "thread_events prune: ended=%d active=%d",
                    int(ended_deleted or 0),
                    int(active_deleted or 0),
                )
        except Exception as e:
            logger.warning("thread_events prune error (non-fatal): %s", e)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=float(interval_s))
            break
        except asyncio.TimeoutError:
            pass
    logger.info("Thread-events prune sweeper stopped")


async def security_events_prune_sweeper(
    shutdown_event: asyncio.Event, *, store
) -> None:
    """Background task that prunes the security_events audit log on retention.

    Runs hourly (SECURITY_EVENTS_PRUNE_INTERVAL_S, default 3600). Deletes
    rows older than SECURITY_EVENTS_RETENTION_DAYS (default 90). Bounds
    table growth — writes happen on the post-auth 403 path, so any flood
    is tied to a real account, but retention still caps the worst case.
    Best-effort: survives transient DB errors by logging and continuing.
    """
    interval_s = int(os.environ.get("SECURITY_EVENTS_PRUNE_INTERVAL_S", "3600"))
    retention_days = int(os.environ.get("SECURITY_EVENTS_RETENTION_DAYS", "90"))
    logger.info(
        "Security-events prune sweeper started (interval=%ds, retention=%dd)",
        interval_s,
        retention_days,
    )
    while not shutdown_event.is_set():
        try:
            deleted = await store.prune_security_events(retention_days)
            if deleted:
                logger.info("security_events prune: deleted=%d", deleted)
        except Exception as e:
            logger.warning("security_events prune error (non-fatal): %s", e)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=float(interval_s))
            break
        except asyncio.TimeoutError:
            pass
    logger.info("Security-events prune sweeper stopped")


async def ssh_attachments_prune_sweeper(
    shutdown_event: asyncio.Event, *, store
) -> None:
    """Background task that prunes the ssh_attachments audit log on retention.

    Runs hourly (SSH_ATTACHMENTS_PRUNE_INTERVAL_S, default 3600). Deletes
    rows older than SSH_ATTACHMENTS_RETENTION_DAYS (default 90). thread_id
    on this table is ON DELETE SET NULL rather than CASCADE (see 0204's
    header), so ending a session no longer prunes its attach history —
    this sweeper is what bounds the table's growth instead.

    Not leader-gated, matching security_events_prune_task: a delete-by-age
    is idempotent, so two replicas racing it is harmless — the second finds
    nothing. Best-effort: survives transient DB errors by logging and
    continuing.
    """
    interval_s = int(os.environ.get("SSH_ATTACHMENTS_PRUNE_INTERVAL_S", "3600"))
    retention_days = int(os.environ.get("SSH_ATTACHMENTS_RETENTION_DAYS", "90"))
    logger.info(
        "SSH-attachments prune sweeper started (interval=%ds, retention=%dd)",
        interval_s,
        retention_days,
    )
    while not shutdown_event.is_set():
        try:
            deleted = await store.prune_ssh_attachments(retention_days)
            if deleted:
                logger.info("ssh_attachments prune: deleted=%d", deleted)
        except Exception as e:
            logger.warning("ssh_attachments prune error (non-fatal): %s", e)
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=float(interval_s))
            break
        except asyncio.TimeoutError:
            pass
    logger.info("SSH-attachments prune sweeper stopped")
