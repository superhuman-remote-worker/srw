"""Validation against a registered driver's schemas, off the event loop (D6).

A registered schema is cheap by construction
(``shared.connectors.registration.schema_problems``: an allowlist of
keywords and a node budget) and so is what is validated against it
(``instance_problem``). The validation still runs in a thread, so the
event loop never waits on it, and in a pool of its own: at most
:data:`SLOTS` validations run at once, and none shares the default
executor every other ``asyncio.to_thread`` uses. A slot is held until its
thread ends (a thread cannot be stopped: a caller that gave up at the
deadline still holds the slot until the work is done), so no more than
:data:`SLOTS` validations ever run. A caller that finds no free slot within
:data:`SLOT_WAIT_SECONDS` is told to retry (:class:`ValidationBusy`); one
whose validation outlasts its deadline is too (:class:`ValidationTimeout`).
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

#: Validations that run at once.
SLOTS = 2
#: How long a validation waits for a free slot before SRW answers busy.
SLOT_WAIT_SECONDS = 1.0
#: How often a waiting validation looks for a free slot.
_POLL_SECONDS = 0.02

_slots = threading.BoundedSemaphore(SLOTS)
_executor = ThreadPoolExecutor(
    max_workers=SLOTS, thread_name_prefix="srw-schema-validation"
)

T = TypeVar("T")


class ValidationUnavailable(RuntimeError):
    """SRW could not validate now; the caller retries."""


class ValidationBusy(ValidationUnavailable):
    """Every validation slot is taken."""


class ValidationTimeout(ValidationUnavailable):
    """The validation outlasted its deadline (its thread runs on, holding
    its slot, until it ends)."""


async def _take_slot() -> None:
    deadline = time.monotonic() + SLOT_WAIT_SECONDS
    while not _slots.acquire(blocking=False):
        if time.monotonic() >= deadline:
            raise ValidationBusy(
                "SRW is busy validating other connectors; retry shortly"
            )
        await asyncio.sleep(_POLL_SECONDS)


async def run_validation(validate: Callable[..., T], *args: Any, timeout: float) -> T:
    """``validate(*args)`` in a validation thread, waiting at most
    ``timeout`` seconds for it. Raises :class:`ValidationBusy` or
    :class:`ValidationTimeout`; whatever ``validate`` raises otherwise."""
    await _take_slot()
    try:
        future = _executor.submit(validate, *args)
    except BaseException:
        _slots.release()
        raise
    # Released when the thread ends, never when the caller stops waiting.
    future.add_done_callback(lambda _done: _slots.release())
    try:
        return await asyncio.wait_for(asyncio.wrap_future(future), timeout)
    except (TimeoutError, asyncio.TimeoutError):
        raise ValidationTimeout(
            f"validating took longer than {timeout:g} s; retry shortly"
        ) from None


__all__ = [
    "SLOTS",
    "SLOT_WAIT_SECONDS",
    "ValidationBusy",
    "ValidationTimeout",
    "ValidationUnavailable",
    "run_validation",
]
