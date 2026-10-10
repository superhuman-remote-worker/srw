"""Retry and rate-limit primitives shared across main-cloud backends.

Two things live here:

* ``retryable_policy()`` — a ``tenacity.AsyncRetrying`` factory for the
  transient-failure retry policy described in §4.9 of the design doc.
  Honors ``CloudBackendError.retryable`` and retries on the common
  ``httpx`` transport errors.
* ``LeakyBucket`` — a minimal token-bucket rate limiter used by the
  Nextcloud adapter for the NC 30.0.10+ 20-shares-per-10-min server
  limit. Client-side throttling keeps the adapter under the server
  ceiling instead of taking 429s and backing off.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Deque

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)

from orchestrator.services.cloud.errors import CloudBackendError

RETRYABLE_HTTPX: tuple[type[Exception], ...] = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.WriteTimeout,
    httpx.PoolTimeout,
    httpx.RemoteProtocolError,
)


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, CloudBackendError):
        return exc.retryable
    return isinstance(exc, RETRYABLE_HTTPX)


def retryable_policy(*, max_attempts: int = 4) -> AsyncRetrying:
    """Shared retry policy for cloud-backend HTTP calls.

    Exponential backoff with jitter. Retries transport-level httpx failures
    and ``CloudBackendError`` instances whose ``.retryable`` flag is True
    (which the adapter sets for 429 / 5xx / timeouts).
    """
    return AsyncRetrying(
        retry=retry_if_exception(_is_retryable),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=8) + wait_random(0, 0.5),
        stop=stop_after_attempt(max_attempts),
        reraise=True,
    )


#: The deadline (``time.monotonic()``) a caller's :func:`bounded_wait` puts
#: on every bucket it waits on, or ``None``.
_WAIT_DEADLINE: ContextVar[float | None] = ContextVar(
    "leaky_bucket_wait_deadline", default=None
)


class BucketFull(Exception):
    """A bucket has no capacity before the caller's bounded wait runs out
    (:func:`bounded_wait`, ``acquire(timeout=...)``). Nothing was consumed;
    ``retry_after`` is how long until it has capacity again, in seconds."""

    def __init__(self, retry_after: float) -> None:
        super().__init__(f"rate limited; capacity again in {retry_after:.0f}s")
        self.retry_after = retry_after


@contextmanager
def bounded_wait(seconds: float) -> Iterator[None]:
    """Bound every bucket wait inside the block to ``seconds`` in all.

    For a caller that cannot block for a whole window (a session's outbox
    share retry, a completion effect) around an adapter call that acquires
    its own bucket (``NextcloudBackend.share_session_folder``): an acquire
    that would wait past the deadline raises :class:`BucketFull` instead.
    """
    token = _WAIT_DEADLINE.set(time.monotonic() + max(0.0, seconds))
    try:
        yield
    finally:
        _WAIT_DEADLINE.reset(token)


class LeakyBucket:
    """Async-safe fixed-window leaky bucket rate limiter.

    Used by NextcloudBackend.share_session_folder to stay under the
    server's 20-shares-per-10-min ceiling (NC 30.0.10+). Not a general
    rate limiter — the window semantics are simple "sliding count of
    calls in the last N seconds". Callers ``await`` ``acquire()`` before
    issuing a rate-limited operation; the call blocks until the bucket
    has capacity, or until a bounded wait runs out (``timeout``,
    :func:`bounded_wait`), which raises :class:`BucketFull`.
    :meth:`try_acquire` never waits.

    The bucket is conservative by design: set ``max_events`` below the
    server's actual limit so that even under clock-skew the client stays
    below the ceiling.
    """

    def __init__(self, *, max_events: int, window_seconds: float) -> None:
        self._max_events = max_events
        self._window_seconds = window_seconds
        self._events: Deque[float] = deque()
        self._lock = asyncio.Lock()

    def _take(self, now: float) -> float:
        """Take one event now (0.0), or how long until there is room."""
        cutoff = now - self._window_seconds
        while self._events and self._events[0] < cutoff:
            self._events.popleft()
        if len(self._events) < self._max_events:
            self._events.append(now)
            return 0.0
        return max(0.0, (self._events[0] + self._window_seconds) - now) or 1e-3

    async def try_acquire(self) -> bool:
        """Take one event if the bucket has room now; never waits."""
        async with self._lock:
            return self._take(time.monotonic()) == 0.0

    async def acquire(self, *, timeout: float | None = None) -> None:
        """Block until the bucket has capacity for one more event.

        With ``timeout`` (or inside :func:`bounded_wait`, whichever ends
        first) a wait that would run past it raises :class:`BucketFull`
        without consuming anything."""
        deadline = _WAIT_DEADLINE.get()
        if timeout is not None:
            own = time.monotonic() + max(0.0, timeout)
            deadline = own if deadline is None else min(deadline, own)
        while True:
            async with self._lock:
                now = time.monotonic()
                wait_for = self._take(now)
                if wait_for == 0.0:
                    return
            if deadline is not None and now + wait_for > deadline:
                raise BucketFull(wait_for)
            await asyncio.sleep(wait_for)
