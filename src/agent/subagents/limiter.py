"""A concurrency limit whose size is read at every admission.

``SubagentRuntime`` bounds its running children with this instead of an
``asyncio.Semaphore`` fixed at construction: the parent's cap is a zero-argument
callable over its live context (``agent.tools.delegation.fanout``), so a live
config update reaches the semaphore as well as the tool description
(parallel_subagents.md §6.4).

Semantics:

* FIFO admission. A waiter is handed its slot on release, so a caller that
  arrives later never overtakes a queued one.
* Lowering the limit never preempts a running child; the surplus drains as
  children finish. Raising it admits queued callers at ``wake()`` (the live
  config path calls it), at the next release, or at the next admission.
* ``async with limiter:`` like a semaphore.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from typing import Any, Callable, Deque, Union

logger = logging.getLogger(__name__)

LimitSource = Union[int, Callable[[], Any]]


class LiveConcurrencyLimit:
    """An asyncio semaphore whose size comes from ``limit`` at every check."""

    def __init__(self, limit: LimitSource, *, fallback: int = 1) -> None:
        self._source: Callable[[], Any] = (
            limit if callable(limit) else (lambda value=limit: value)
        )
        self._fallback = max(1, int(fallback))
        self._held = 0
        self._waiters: Deque[asyncio.Future] = deque()

    @property
    def limit(self) -> int:
        """The current size, never below 1. A failing source keeps the
        fallback instead of wedging every admission."""
        try:
            value = int(self._source())
        except Exception:  # noqa: BLE001 - a bad config must not wedge children
            logger.warning(
                "subagent concurrency limit unreadable — using %d",
                self._fallback,
                exc_info=True,
            )
            value = self._fallback
        return max(1, value)

    @property
    def held(self) -> int:
        """How many admitted callers have not released yet."""
        return self._held

    @property
    def waiting(self) -> int:
        return sum(1 for fut in self._waiters if not fut.done())

    def locked(self) -> bool:
        return self._held >= self.limit

    async def acquire(self) -> bool:
        # A limit raised since the last release admits queued callers first.
        self._wake()
        if not self.waiting and self._held < self.limit:
            self._held += 1
            return True
        future = asyncio.get_running_loop().create_future()
        self._waiters.append(future)
        try:
            await future
        except asyncio.CancelledError:
            if future.done() and not future.cancelled():
                # The slot was handed over before the cancel landed: give it
                # back so the next waiter is not stranded.
                self._held -= 1
                self._wake()
            raise
        finally:
            try:
                self._waiters.remove(future)
            except ValueError:
                pass
        return True

    def release(self) -> None:
        if self._held <= 0:
            raise ValueError("LiveConcurrencyLimit released too many times")
        self._held -= 1
        self._wake()

    def wake(self) -> int:
        """Re-read the limit and admit queued callers it now has room for.

        Call after the source may have changed (a live config update): a
        raised limit otherwise admits nobody until the next release or
        admission. Returns how many callers it admitted.
        """
        before = self._held
        self._wake()
        return self._held - before

    def _wake(self) -> None:
        limit = self.limit
        while self._waiters and self._held < limit:
            future = self._waiters.popleft()
            if future.done():
                continue
            self._held += 1
            future.set_result(True)

    async def __aenter__(self) -> None:
        await self.acquire()

    async def __aexit__(self, *exc_info: Any) -> None:
        self.release()


__all__ = ["LiveConcurrencyLimit"]
