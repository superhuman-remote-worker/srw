"""Request-path single-flight for the stateless workspace reconcile.

Extracted verbatim from ``orchestrator.main`` (R1.B05, root lane). It was a
module-level ``dict`` plus one function; it is now an application-owned
registry, for the same reason B04 gave when it replaced main's two cloud task
dicts: a module global is shared by every application built in one process, and
a fire-and-forget task nothing holds a reference to can be garbage-collected
mid-flight.

This is deliberately **not** folded into
:class:`~orchestrator.services.cloud_task_registry.CloudTaskRegistry`. That
registry owns cloud work — protected-cloud engage and turn-end staging — and
its two halves already answer different questions with different eviction
mechanisms. A workspace reconcile is neither, and merging them would mean one
class whose behaviour depends on which key shape it was handed.

The eviction mechanism is the one ``main`` had, and it matters: the slot is
cleared through an identity-checked ``add_done_callback``, so a newer schedule
for the same thread is never cleared by the previous task's stale callback.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from orchestrator.services.application_tasks import drain_task_mapping

logger = logging.getLogger(__name__)


class StatelessWorkspaceEnsureRegistry:
    """The in-flight workspace reconciles one application instance owns."""

    def __init__(self) -> None:
        self._tasks: dict[str, "asyncio.Task[None]"] = {}

    def get(self, thread_id: str) -> "asyncio.Task[None] | None":
        return self._tasks.get(thread_id)

    def register(self, thread_id: str, task: "asyncio.Task[None]") -> None:
        """Publish ``task`` for ``thread_id`` and clear the slot when it ends.

        The slot is cleared only if it still holds this exact task, so a
        schedule that lands while an older one is finishing survives.
        """
        self._tasks[thread_id] = task

        def _forget(finished: "asyncio.Task[None]") -> None:
            if self._tasks.get(thread_id) is finished:
                self._tasks.pop(thread_id, None)

        task.add_done_callback(_forget)

    def discard(self, thread_id: str) -> "asyncio.Task[None] | None":
        """Drop and return the slot for ``thread_id``, if it holds one.

        The registry evicts on completion by itself; this is for a caller that
        needs the slot gone *now* — a teardown that will not wait for the
        callback, or a test resetting between cases.
        """
        return self._tasks.pop(thread_id, None)

    def in_flight(self) -> dict[str, "asyncio.Task[None]"]:
        """A snapshot, for shutdown drain and for tests."""
        return dict(self._tasks)

    async def drain(self) -> None:
        """Cancel and await every in-flight reconcile, then empty the registry.

        R1.B12: the application calls this at shutdown, before its stores
        close. A cancelled reconcile is not handed over: the next input,
        resume or internal workspace poll schedules one again (see
        :func:`schedule_stateless_workspace_ensure`). Safe to call again; the
        registry stays usable.
        """
        await drain_task_mapping(self._tasks)


@dataclass(frozen=True)
class StatelessWorkspaceScheduleDependencies:
    """Collaborators for one schedule, resolved per invocation.

    ``ensure_session_workspace`` is the existing service operation and owns the
    cross-replica advisory lock; nothing here re-derives that. ``store``,
    ``provisioner`` and ``suspension`` are main singletons rebound during
    ``lifespan``. ``registry`` is the one instance the application owns — it is
    deliberately the *same* object across builds, unlike every other field.
    """

    store: Any
    provisioner: Any
    suspension: Any
    registry: StatelessWorkspaceEnsureRegistry
    ensure_session_workspace: Callable[..., Awaitable[Any]]


def schedule_stateless_workspace_ensure(
    thread_id: str, *, dependencies: StatelessWorkspaceScheduleDependencies
) -> "asyncio.Task[None]":
    """Single-flight a stateless workspace reconcile.

    Input and resume must stay durable/fast, while the internal workspace poll
    is allowed to observe provisioning over the already-heartbeating queue
    lease. Repeated user input and two-second agent polls therefore converge on
    one background create/adopt operation instead of starting task storms.
    """
    current = dependencies.registry.get(thread_id)
    if current is not None and not current.done():
        return current

    async def _ensure() -> None:
        try:
            # The service owns the cross-replica advisory lock because direct
            # resume/prepare and periodic-reconcile callers share this path.
            await dependencies.ensure_session_workspace(
                thread_id,
                db=dependencies.store,
                provisioner=dependencies.provisioner,
                suspension=dependencies.suspension,
            )
        except Exception:
            logger.exception(
                "Stateless workspace reconcile failed for thread %s", thread_id
            )

    task = asyncio.create_task(
        _ensure(), name=f"stateless-workspace-ensure-{thread_id[:8]}"
    )
    dependencies.registry.register(thread_id, task)
    return task


__all__ = [
    "StatelessWorkspaceEnsureRegistry",
    "StatelessWorkspaceScheduleDependencies",
    "schedule_stateless_workspace_ensure",
]
