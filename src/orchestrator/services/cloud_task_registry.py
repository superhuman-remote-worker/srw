"""One application-owned home for the two background cloud task registries.

Both were module globals in ``main``. They survive as separate internals here
because they answer different questions and are keyed differently:

``protected_engage``
    ``(thread_id, runtime_generation) -> Task``. Callers racing the attach path
    or a re-engage on resume **await** the registered task instead of falling
    through to a bare poll, so the slot must be published *before* the task
    runs and must only be cleared by the registration that owns it.

``stage``
    ``task_key -> Task``, a de-dupe: a second trigger for the same key while one
    is in flight is a no-op rather than a second stage.

Each keeps the eviction mechanism it had in ``main``, because the two are
observably different and both are relied on:

* ``protected_engage`` clears through ``add_done_callback``, guarded on task
  identity so a newer registration for the same thread (a resume re-engage
  firing right after a create engage) is never clobbered by a stale callback.
* ``stage`` clears in the running coroutine's own ``finally``, so awaiting the
  task is enough to observe the slot free. A done-callback would move that
  eviction one event-loop tick later and silently break callers — and tests —
  that await and then look.

Owned by the application: ``main`` constructs exactly one instance and injects
it. Two application instances in one process therefore do not share task state.
"""

from __future__ import annotations

import asyncio
from typing import Callable, Coroutine

from orchestrator.services.application_tasks import drain_task_mapping

ProtectedEngageKey = tuple[str, str]


class CloudTaskRegistry:
    """The two background-task registries one application instance owns."""

    def __init__(self) -> None:
        self._protected_engage: dict[ProtectedEngageKey, "asyncio.Task[None]"] = {}
        self._stage: dict[str, "asyncio.Task[None]"] = {}

    # -- protected-cloud engage ------------------------------------------
    def protected_engage_get(
        self, key: ProtectedEngageKey
    ) -> "asyncio.Task[None] | None":
        """The task a caller may await, or ``None`` if nothing is in flight."""
        return self._protected_engage.get(key)

    def protected_engage_register(
        self, key: ProtectedEngageKey, task: "asyncio.Task[None]"
    ) -> None:
        """Publish ``task`` under ``key`` and clear the slot when it finishes.

        The slot is cleared only if it is still this exact task. A newer
        registration for the same thread must survive a stale callback.
        """
        self._protected_engage[key] = task

        def _done(finished: "asyncio.Task[None]") -> None:
            if self._protected_engage.get(key) is finished:
                self._protected_engage.pop(key, None)

        task.add_done_callback(_done)

    # -- cloud stage ------------------------------------------------------
    def stage_has(self, key: str) -> bool:
        """Whether a stage is already in flight for this exact key."""
        return key in self._stage

    def stage_start(
        self, key: str, factory: Callable[[], Coroutine[object, object, None]]
    ) -> None:
        """Start one stage for ``key``, or do nothing if one is in flight.

        The slot is released inside the coroutine's ``finally``, so a caller
        that awaits the task observes it free — the behaviour ``main`` had.
        """
        if key in self._stage:
            return

        async def _run() -> None:
            try:
                await factory()
            finally:
                self._stage.pop(key, None)

        self._stage[key] = asyncio.create_task(_run())

    # -- shutdown ---------------------------------------------------------
    async def drain(self) -> None:
        """Cancel and await every in-flight engage and stage (R1.B12).

        The application calls this at shutdown, before its stores close. Both
        halves are cancelled before either is awaited; each body runs its own
        cancellation path (the thread advisory lock it holds is released by
        its context manager). Afterwards both registries are empty — including
        a stage cancelled before it started, whose own ``finally`` never ran —
        and usable.
        """
        await drain_task_mapping(self._protected_engage, self._stage)

    # -- inspection -------------------------------------------------------
    # The live mappings, not copies: existing suites reach into these to seed a
    # sentinel task or assert eviction, and a copy would make the identity they
    # mutate different from the identity the code reads.
    @property
    def protected_engage_tasks(self) -> dict[ProtectedEngageKey, "asyncio.Task[None]"]:
        return self._protected_engage

    @property
    def cloud_stage_tasks(self) -> dict[str, "asyncio.Task[None]"]:
        return self._stage
