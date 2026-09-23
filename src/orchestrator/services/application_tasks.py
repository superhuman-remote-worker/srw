"""Ownership of the background tasks one application lifecycle starts.

R1.B11. The application lifespan creates one :class:`ApplicationTaskSet` per
lifecycle, together with that lifecycle's shutdown event. Loop bodies live
with their domains; this object only owns *which* tasks the lifecycle
started, how leader-only loops are gated, and the order shutdown awaits them.

Leader gating is the existing contract in
:mod:`orchestrator.services.leader_election`: a gated loop is started through
``run_when_leader``, which runs it only while this replica holds leadership
and cancels it when leadership is lost or shutdown begins. Loops with their
own advisory lock, row lease or claim are started plainly.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Coroutine, Sequence
from typing import Any

from orchestrator.services import leader_election

logger = logging.getLogger(__name__)


class ApplicationTaskSet:
    """The background tasks one application lifecycle started and must stop."""

    def __init__(self, shutdown_event: asyncio.Event) -> None:
        self._shutdown_event = shutdown_event
        self._tasks: dict[str, asyncio.Task[Any]] = {}

    @property
    def shutdown_event(self) -> asyncio.Event:
        return self._shutdown_event

    @property
    def keys(self) -> tuple[str, ...]:
        """Started task keys, in start order."""

        return tuple(self._tasks)

    def start(
        self,
        key: str,
        coro: Coroutine[Any, Any, Any],
        *,
        name: str | None = None,
    ) -> asyncio.Task[Any]:
        """Run ``coro`` as the lifecycle's task ``key`` (at most once)."""

        if key in self._tasks:
            coro.close()
            raise RuntimeError(f"background task {key!r} was already started")
        task = asyncio.create_task(coro, name=name)
        self._tasks[key] = task
        return task

    def start_leader_gated(
        self,
        key: str,
        make_coro: Callable[[asyncio.Event], Awaitable[None]],
        *,
        name: str | None = None,
    ) -> asyncio.Task[Any]:
        """Run the loop ``make_coro`` builds only while this replica leads."""

        return self.start(
            key,
            leader_election.run_when_leader(make_coro, self._shutdown_event),
            name=name,
        )

    async def stop(self, order: Sequence[str]) -> Exception | None:
        """Signal shutdown, then await every started task.

        Tasks are awaited in ``order`` (keys that were never started, such as
        feature-gated ones, are skipped), then any started task ``order`` did
        not name, in start order. A task that ended with an error is logged
        and the remaining tasks are still awaited; the first such error is
        returned so the caller can finish its own shutdown before surfacing
        it. Cancellation is never absorbed: it propagates at once.
        """

        self._shutdown_event.set()
        named = list(order)
        remaining = [key for key in self._tasks if key not in set(named)]
        first_failure: Exception | None = None
        for key in (*named, *remaining):
            task = self._tasks.get(key)
            if task is None:
                continue
            try:
                await task
            except Exception as exc:
                logger.error(
                    "Background task %r ended with an error; shutdown continues",
                    key,
                    exc_info=exc,
                )
                if first_failure is None:
                    first_failure = exc
        return first_failure


__all__ = ["ApplicationTaskSet"]
