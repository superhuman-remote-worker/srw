"""The chat-pool stall rule and the admin notification it drives.

The chat pool is *stalled* when runnable ``session_turn`` rows exist, no live
lease of any kind is held, and the oldest runnable turn has waited at least
``STATELESS_CHAT_STALL_ALARM_S`` (default 300 s, floor 60 s). A busy pool is
never stalled, however long its turns run or however large a ``worker_batch``
backlog is.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import timezone
from typing import Any, Callable

from orchestrator.services.stateless_capacity import (
    QueueDemand,
    _connection,
    read_demand,
)

logger = logging.getLogger(__name__)

STALL_THRESHOLD_ENV = "STATELESS_CHAT_STALL_ALARM_S"
DEFAULT_STALL_THRESHOLD_S = 300.0
MIN_STALL_THRESHOLD_S = 60.0
SOURCE_KIND = "chat_pool"
SOURCE_ID = "stateless"
CACHE_TTL_S = 10.0


def stall_threshold_s() -> float:
    raw = os.getenv(STALL_THRESHOLD_ENV)
    if raw is None or not raw.strip():
        return DEFAULT_STALL_THRESHOLD_S
    try:
        return max(MIN_STALL_THRESHOLD_S, float(raw.strip()))
    except ValueError:
        logger.warning(
            "%s=%r is not a number; using %s",
            STALL_THRESHOLD_ENV,
            raw,
            DEFAULT_STALL_THRESHOLD_S,
        )
        return DEFAULT_STALL_THRESHOLD_S


def chat_pool_stalled(demand: QueueDemand, *, threshold_s: float) -> bool:
    age = demand.oldest_session_turn_age_s
    return (
        demand.runnable_session_turn >= 1
        and demand.busy == 0
        and age is not None
        and age >= threshold_s
    )


def stall_incident_key(demand: QueueDemand) -> str | None:
    at = demand.oldest_session_turn_runnable_at
    if at is None:
        return None
    if at.tzinfo is None:
        at = at.replace(tzinfo=timezone.utc)
    return at.astimezone(timezone.utc).isoformat(timespec="seconds")


_cache: tuple[float, bool] | None = None


def _reset_cache() -> None:
    global _cache
    _cache = None


async def read_chat_pool_stalled(
    conn: Any, *, clock: Callable[[], float] = time.monotonic
) -> bool:
    """The stall verdict, cached for ``CACHE_TTL_S`` so UI polls stay cheap."""
    global _cache
    now = clock()
    if _cache is not None and now - _cache[0] < CACHE_TTL_S:
        return _cache[1]
    stalled = chat_pool_stalled(
        await read_demand(conn), threshold_s=stall_threshold_s()
    )
    _cache = (now, stalled)
    return stalled


class ChatPoolStallWatch:
    """Notifies admins once per stall incident and resolves it when it clears."""

    def __init__(
        self,
        db: Any,
        notifications: Any,
        *,
        threshold_s: float | None = None,
        poll_seconds: float = 30.0,
    ) -> None:
        self._db = db
        self._notifications = notifications
        self._threshold_s = threshold_s
        self.poll_seconds = poll_seconds
        self._notified_incident: str | None = None
        self._may_have_open = True

    async def run_once(self) -> bool:
        async with _connection(self._db) as conn:
            demand = await read_demand(conn)
        threshold = (
            self._threshold_s if self._threshold_s is not None else stall_threshold_s()
        )
        stalled = chat_pool_stalled(demand, threshold_s=threshold)
        if stalled:
            incident = stall_incident_key(demand)
            if incident is not None and incident != self._notified_incident:
                try:
                    await self._notifications.record_chat_executors_unavailable(
                        incident=incident,
                        runnable=demand.runnable_session_turn,
                        oldest_age_s=float(demand.oldest_session_turn_age_s or 0.0),
                    )
                except Exception:
                    logger.exception("chat pool stall notification failed")
                self._notified_incident = incident
                self._may_have_open = True
        elif self._may_have_open:
            try:
                await self._notifications.resolve_source(
                    SOURCE_KIND, SOURCE_ID, resolved_by="system"
                )
            except Exception:
                logger.exception("chat pool stall resolve failed")
            self._notified_incident = None
            self._may_have_open = False
        return stalled

    async def run(self, shutdown_event: asyncio.Event) -> None:
        while not shutdown_event.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("chat pool stall watch tick failed")
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=self.poll_seconds)
            except TimeoutError:
                pass
