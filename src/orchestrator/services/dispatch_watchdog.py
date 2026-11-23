"""Notice a dispatcher that has stopped, and optionally restart the replica.

The leader's dispatcher makes a catch-all pass every
``job_dispatcher.DISPATCH_INTERVAL_SECONDS``. On 2026-09-12 it stopped for
seven minutes on main dev: ready jobs waited, nothing in the product said so,
and only deleting the leader pod brought dispatch back
(knowledge-base/knowledge/issues/dispatcher_stalls_on_workspace_mutation_lock.md).

This loop reads the two timestamps the dispatcher's loop keeps
(``JobDispatchState.loop_pass_started_at`` / ``loop_pass_finished_at``):

- a pass that started and has not finished is stalled for as long as it runs;
- between passes, the dispatcher is stalled for the time since the last pass
  finished beyond the catch-all interval.

It also reads when each workspace preflight started
(``JobDispatchState.active_since``). The dispatcher cancels a preflight at its
time limit. One that is still there afterwards ignored the cancel (a blocking
call that never returns) and keeps one of the two creation slots; it counts as
a stall for as long as it is overdue.

The thresholds are deployment settings (``DISPATCH_STALL_ALARM_SECONDS``,
``DISPATCH_STALL_RESTART_SECONDS``). From ``alarm_seconds`` on it logs an
ERROR (once, then every ``REPEAT_ALARM_SECONDS``) that names what is still
running. From ``restart_seconds`` on, when that is set, it ends the process so
Kubernetes starts a fresh replica or another one takes the lead: the remedy an
operator applied by hand. It only watches; it never touches a job.

It runs on the leader only, next to the dispatcher. A pass that is blocked in
synchronous code blocks this loop too; that case stops the health endpoint as
well and is the liveness probe's.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

logger = logging.getLogger(__name__)

#: Seconds between two looks at the dispatcher.
CHECK_INTERVAL_SECONDS = 15.0
#: Seconds between two ERROR lines for the same stall.
REPEAT_ALARM_SECONDS = 300.0
#: After asking the process to stop, seconds until it is ended the hard way.
HARD_EXIT_GRACE_SECONDS = 30.0


@dataclass(frozen=True)
class DispatchStall:
    """A dispatcher that is not making passes: for how long, and why."""

    seconds: float
    in_pass: bool
    active_preflights: int
    pending_preflights: int
    #: Set when the stall is a preflight that outlived its time limit.
    overdue_job_id: str | None = None

    def describe(self) -> str:
        if self.overdue_job_id is not None:
            what = (
                f"the workspace preparation for job {self.overdue_job_id} was "
                "cancelled at its time limit and has not returned"
            )
        elif self.in_pass:
            what = "a scheduling pass has been running"
        else:
            what = "no scheduling pass has finished"
        return (
            f"{what} for {self.seconds:.0f} s "
            f"({self.active_preflights} workspace preparation(s) running, "
            f"{self.pending_preflights} job(s) waiting for one)"
        )


def dispatch_stall(
    state: Any, *, now: float, watching_since: float, interval: float
) -> DispatchStall | None:
    """How long ``state``'s dispatcher has been without a pass, or ``None``
    while it is on time.

    ``watching_since`` stands in for a first pass that never started. Times
    are ``time.monotonic()`` readings.
    """
    started = getattr(state, "loop_pass_started_at", None)
    finished = getattr(state, "loop_pass_finished_at", None)
    in_pass = started is not None and (finished is None or finished < started)
    if in_pass:
        seconds = now - started
    else:
        last = max(
            value for value in (finished, started, watching_since) if value is not None
        )
        seconds = now - last - interval
    if seconds <= 0:
        return None
    return DispatchStall(
        seconds=seconds,
        in_pass=in_pass,
        active_preflights=len(getattr(state, "active", ()) or ()),
        pending_preflights=len(getattr(state, "pending", ()) or ()),
    )


def overdue_preflight(
    state: Any, *, now: float, preflight_timeout: float
) -> DispatchStall | None:
    """The preflight that has outlived ``preflight_timeout`` the longest, as
    a stall of the time it is overdue, or ``None``. Without a time limit
    (``preflight_timeout`` 0) no preflight is ever overdue."""
    if preflight_timeout <= 0:
        return None
    started = getattr(state, "active_since", None) or {}
    overdue = [
        (now - since - preflight_timeout, str(job_id))
        for job_id, since in started.items()
        if isinstance(since, (int, float))
    ]
    overdue = [item for item in overdue if item[0] > 0]
    if not overdue:
        return None
    seconds, job_id = max(overdue)
    return DispatchStall(
        seconds=seconds,
        in_pass=False,
        active_preflights=len(getattr(state, "active", ()) or ()),
        pending_preflights=len(getattr(state, "pending", ()) or ()),
        overdue_job_id=job_id,
    )


def end_process() -> None:
    """Ask this process to stop, and end it if it has not after a grace.

    SIGTERM takes the ordinary shutdown path (the server drains and the
    application's lifecycle stops its tasks). A shutdown that hangs on the
    same thing the dispatcher hung on is cut short by the timer.
    """
    timer = threading.Timer(HARD_EXIT_GRACE_SECONDS, os._exit, args=(70,))
    timer.daemon = True
    timer.start()
    os.kill(os.getpid(), signal.SIGTERM)


async def dispatch_stall_watchdog(
    shutdown_event: asyncio.Event,
    *,
    state: Any,
    may_schedule: Callable[[Any], bool],
    interval: float,
    alarm_seconds: float,
    restart_seconds: float,
    preflight_timeout: float = 0.0,
    terminate: Callable[[], None] = end_process,
    check_interval: float = CHECK_INTERVAL_SECONDS,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Watch ``state``'s dispatcher until ``shutdown_event`` is set.

    ``may_schedule`` is the dispatcher's own test for "this replica may
    schedule now" (leader, not paused, not closing); while it is false there
    is nothing to expect and the watch starts over.
    """
    if alarm_seconds <= 0 and restart_seconds <= 0:
        logger.info("Dispatcher stall watchdog is off")
        # A leader-gated loop that returns is started again every second.
        await shutdown_event.wait()
        return
    logger.info(
        "Dispatcher stall watchdog started (alarm after %.0f s, restart %s)",
        alarm_seconds,
        f"after {restart_seconds:.0f} s" if restart_seconds > 0 else "never",
    )
    watching_since = clock()
    last_alarm: float | None = None
    longest = 0.0
    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=check_interval)
            return
        except asyncio.TimeoutError:
            pass
        now = clock()
        if not may_schedule(state):
            watching_since, last_alarm, longest = now, None, 0.0
            continue
        stalls = [
            dispatch_stall(
                state, now=now, watching_since=watching_since, interval=interval
            ),
            overdue_preflight(state, now=now, preflight_timeout=preflight_timeout),
        ]
        stall = max(
            (found for found in stalls if found is not None),
            key=lambda found: found.seconds,
            default=None,
        )
        if stall is None:
            if last_alarm is not None:
                logger.warning(
                    "Dispatcher is making passes again after a stall of %.0f s",
                    longest,
                )
            last_alarm, longest = None, 0.0
            continue
        longest = max(longest, stall.seconds)
        if (
            alarm_seconds > 0
            and stall.seconds >= alarm_seconds
            and (last_alarm is None or now - last_alarm >= REPEAT_ALARM_SECONDS)
        ):
            logger.error("Dispatcher stalled: %s", stall.describe())
            last_alarm = now
        if restart_seconds > 0 and stall.seconds >= restart_seconds:
            logger.critical(
                "Dispatcher stalled: %s. Ending this replica so that dispatch "
                "starts again (DISPATCH_STALL_RESTART_SECONDS=%.0f)",
                stall.describe(),
                restart_seconds,
            )
            terminate()
            # Returning would have the leader gate start this loop again in
            # the process that is ending.
            await shutdown_event.wait()
            return


__all__ = [
    "CHECK_INTERVAL_SECONDS",
    "DispatchStall",
    "dispatch_stall",
    "dispatch_stall_watchdog",
    "end_process",
    "overdue_preflight",
]
