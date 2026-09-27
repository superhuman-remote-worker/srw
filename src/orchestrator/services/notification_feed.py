"""SSE notification feed for real-time cockpit updates.

Provides per-user Server-Sent Events broadcasting for live notification
delivery to the cockpit UI. Follows the SudoGateService SSE pattern.

Event types:
    new_message      — Agent sent a new message to the user
    reply_delivered   — User's reply was routed to the agent
"""

import asyncio
import json
import logging
from uuid import UUID, uuid4
from typing import Any

logger = logging.getLogger(__name__)

_LIFECYCLE_CHANNEL = "srw_session_lifecycle"
_OUTBOUND_LIMIT = 256
_MAX_WIRE_BYTES = 1024
_LISTENER_CHECK_SECONDS = 10.0
_RECONNECT_MIN_SECONDS = 1.0
_RECONNECT_MAX_SECONDS = 30.0
_LIFECYCLE_STATES = frozenset({"provisioning", "booting", "ready", "failed"})


def _uuid(value: Any) -> str | None:
    try:
        parsed = str(UUID(str(value)))
        return parsed if parsed == value else None
    except (TypeError, ValueError, AttributeError):
        return None


class NotificationFeedService:
    """Per-user SSE broadcast service for notification events."""

    def __init__(self) -> None:
        # user_id -> list of asyncio.Queue
        self._user_queues: dict[str, list[asyncio.Queue]] = {}
        self._origin = str(uuid4())
        self._outbound: asyncio.Queue[str] = asyncio.Queue(maxsize=_OUTBOUND_LIMIT)
        self._needs_resync = False
        self._bridge_running = False
        self._bridge_started = False
        self.lifecycle_bridge_ready = asyncio.Event()

    def subscribe_sse(self, user_id: str) -> asyncio.Queue:
        """Create a new SSE subscription queue for a user.

        Returns a Queue that receives event dicts.
        """
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        if user_id not in self._user_queues:
            self._user_queues[user_id] = []
        self._user_queues[user_id].append(q)
        # A stream admitted while this replica cannot hear PostgreSQL may
        # miss a remote Resume forever. Force EventSource to retry; its onopen
        # edge makes the ended Cockpit re-read authoritative owner metadata.
        if self._bridge_started and not self.lifecycle_bridge_ready.is_set():
            q.put_nowait(None)
        logger.debug(
            "SSE subscriber added for user %s (total: %d)",
            user_id,
            len(self._user_queues[user_id]),
        )
        return q

    def unsubscribe_sse(self, user_id: str, q: asyncio.Queue) -> None:
        """Remove an SSE subscription queue."""
        queues = self._user_queues.get(user_id, [])
        try:
            queues.remove(q)
        except ValueError:
            pass
        if not queues:
            self._user_queues.pop(user_id, None)

    def broadcast(
        self,
        user_id: str,
        event_type: str,
        data: dict[str, Any],
    ) -> None:
        """Push an event to all SSE clients for a specific user."""
        queues = self._user_queues.get(user_id, [])
        if not queues:
            return

        event = {"type": event_type, **data}
        dead: list[asyncio.Queue] = []

        for q in queues:
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                dead.append(q)

        for q in dead:
            self._close_queue(q)
            queues.remove(q)
        if not queues:
            self._user_queues.pop(user_id, None)

    @staticmethod
    def _close_queue(q: asyncio.Queue) -> None:
        # Queue removal alone leaves the SSE generator blocked on queue.get().
        # Make a slow client actually reconnect and reconcile its owner view.
        if q.full():
            q.get_nowait()
        q.put_nowait(None)

    def _close_sse_streams(self) -> None:
        for queues in self._user_queues.values():
            for q in queues:
                self._close_queue(q)
        self._user_queues.clear()

    def publish_lifecycle(
        self, user_id: str, thread_id: str, state: str, **extra: Any
    ) -> None:
        """Deliver locally now; queue only a non-secret, generation-scoped hint.

        Legacy emitters without an exact generation retain their local event.
        The recipient always re-reads the owner row before reopening controls.
        """
        self.broadcast(
            user_id,
            "session.lifecycle",
            {"thread_id": thread_id, "state": state, **extra},
        )
        owner = _uuid(user_id)
        thread = _uuid(thread_id)
        generation = _uuid(extra.get("session_runtime_generation"))
        if not (
            self._bridge_running
            and owner
            and thread
            and generation
            and state in _LIFECYCLE_STATES
        ):
            return
        event: dict[str, Any] = {
            "v": 1,
            "origin": self._origin,
            "kind": "lifecycle",
            "user_id": owner,
            "thread_id": thread,
            "state": state,
            "session_runtime_generation": generation,
        }
        if extra.get("backend") == "vm":
            event["backend"] = "vm"
        payload = json.dumps(event, separators=(",", ":"))
        try:
            self._outbound.put_nowait(payload)
        except asyncio.QueueFull:
            # One payload-free marker heals every dropped hint on remote
            # replicas without an unbounded per-owner overflow map.
            self._needs_resync = True
            self._close_sse_streams()
            logger.error("Session lifecycle publisher full; forcing feed resync")

    def _receive_lifecycle_wire(self, payload: str) -> None:
        if len(payload.encode("utf-8")) > _MAX_WIRE_BYTES:
            return
        try:
            event = json.loads(payload)
        except (TypeError, ValueError):
            return
        if (
            not isinstance(event, dict)
            or type(event.get("v")) is not int
            or event["v"] != 1
        ):
            return
        if not _uuid(event.get("origin")) or event["origin"] == self._origin:
            return
        if event.get("kind") == "resync":
            self._close_sse_streams()
            return
        if event.get("kind") != "lifecycle":
            return
        if "backend" in event and event["backend"] != "vm":
            return
        owner = _uuid(event.get("user_id"))
        thread = _uuid(event.get("thread_id"))
        generation = _uuid(event.get("session_runtime_generation"))
        state = event.get("state")
        if not (
            owner
            and thread
            and generation
            and isinstance(state, str)
            and state in _LIFECYCLE_STATES
        ):
            return
        data: dict[str, str] = {
            "thread_id": thread,
            "state": state,
            "session_runtime_generation": generation,
        }
        if event.get("backend") == "vm":
            data["backend"] = "vm"
        self.broadcast(owner, "session.lifecycle", data)

    async def _publish_lifecycle(self, db: Any, shutdown: asyncio.Event) -> None:
        pending: str | None = None
        pending_resync = False
        backoff = _RECONNECT_MIN_SECONDS
        while (
            not shutdown.is_set()
            or pending is not None
            or not self._outbound.empty()
            or self._needs_resync
        ):
            if pending is None:
                # A saturated publisher must not defer its recovery edge
                # behind a queue that a busy producer keeps refilling.
                if self._needs_resync:
                    pending = json.dumps(
                        {
                            "v": 1,
                            "origin": self._origin,
                            "kind": "resync",
                        },
                        separators=(",", ":"),
                    )
                    pending_resync = True
                    self._needs_resync = False
                elif not self._outbound.empty():
                    pending = self._outbound.get_nowait()
                else:
                    try:
                        pending = await asyncio.wait_for(
                            self._outbound.get(), timeout=0.2
                        )
                    except asyncio.TimeoutError:
                        continue
            try:
                await asyncio.wait_for(
                    db.notify_channel(_LIFECYCLE_CHANNEL, pending), timeout=10
                )
                if not pending_resync:
                    self._outbound.task_done()
                pending = None
                pending_resync = False
                backoff = _RECONNECT_MIN_SECONDS
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Session lifecycle NOTIFY failed; retrying: %s", exc)
                if shutdown.is_set():
                    return
                await self._sleep_or_shutdown(backoff, shutdown)
                backoff = min(backoff * 2, _RECONNECT_MAX_SECONDS)

    @staticmethod
    async def _sleep_or_shutdown(seconds: float, shutdown: asyncio.Event) -> None:
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def run_lifecycle_bridge(self, db: Any, shutdown: asyncio.Event) -> None:
        """Own one per-replica LISTEN connection and one FIFO publisher.

        LISTEN loss closes browser streams; their automatic reconnect gives
        ended views an authoritative metadata read even if a hint was missed.
        """
        if self._bridge_running:
            raise RuntimeError("session lifecycle bridge already running")
        self._bridge_started = True
        self._bridge_running = True
        publisher = asyncio.create_task(self._publish_lifecycle(db, shutdown))
        backoff = _RECONNECT_MIN_SECONDS
        try:
            while not shutdown.is_set():
                conn = None
                callback = None
                try:
                    pool = getattr(db, "_pool", None)
                    if pool is None:
                        await self._sleep_or_shutdown(backoff, shutdown)
                        backoff = min(backoff * 2, _RECONNECT_MAX_SECONDS)
                        continue
                    conn = await pool.acquire()

                    def _on_notify(
                        _connection: Any, _pid: int, _channel: str, payload: str
                    ) -> None:
                        self._receive_lifecycle_wire(payload)

                    callback = _on_notify
                    await asyncio.wait_for(
                        conn.add_listener(_LIFECYCLE_CHANNEL, callback), timeout=10
                    )
                    self.lifecycle_bridge_ready.set()
                    backoff = _RECONNECT_MIN_SECONDS
                    while not shutdown.is_set():
                        await self._sleep_or_shutdown(_LISTENER_CHECK_SECONDS, shutdown)
                        if not shutdown.is_set():
                            await asyncio.wait_for(
                                conn.fetchval("SELECT 1"), timeout=10
                            )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "Session lifecycle LISTEN failed; reconnecting: %s", exc
                    )
                finally:
                    self.lifecycle_bridge_ready.clear()
                    self._close_sse_streams()
                    if conn is not None:
                        if callback is not None:
                            try:
                                await conn.remove_listener(_LIFECYCLE_CHANNEL, callback)
                            except Exception:
                                pass
                        try:
                            await db._pool.release(conn)
                        except Exception:
                            pass
                if not shutdown.is_set():
                    await self._sleep_or_shutdown(backoff, shutdown)
                    backoff = min(backoff * 2, _RECONNECT_MAX_SECONDS)
        finally:
            self._bridge_running = False
            self.lifecycle_bridge_ready.clear()
            self._close_sse_streams()
            try:
                await asyncio.wait_for(publisher, timeout=2)
            except asyncio.TimeoutError:
                logger.warning(
                    "Session lifecycle publisher did not drain before shutdown"
                )
                publisher.cancel()
                await asyncio.gather(publisher, return_exceptions=True)

    @property
    def active_connections(self) -> int:
        """Total number of active SSE connections across all users."""
        return sum(len(qs) for qs in self._user_queues.values())


# Module-level singleton
notification_feed = NotificationFeedService()
