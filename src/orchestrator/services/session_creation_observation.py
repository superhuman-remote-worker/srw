"""One scheduling quantum, separate from durable workspace readiness clocks."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from orchestrator.services.workspace_lifecycle import SessionWorkspaceObservationYielded


class SessionCreationObservationBudget:
    """Preparation checks stop signals; the first observation arms one quantum.

    Starting again cannot extend the deadline. This budget grants no lifecycle
    authority and cannot release a begun SDK call or subprocess from ownership.
    """

    def __init__(self, *, quantum_s: float, should_stop: Callable[[], bool]):
        self.quantum_s = quantum_s
        self.should_stop = should_stop
        self._deadline: float | None = None

    def start(self) -> None:
        if self._deadline is None:
            self._deadline = asyncio.get_running_loop().time() + self.quantum_s
        self()

    def __call__(self) -> None:
        if self.should_stop() or (
            self._deadline is not None
            and asyncio.get_running_loop().time() >= self._deadline
        ):
            raise SessionWorkspaceObservationYielded(
                "background workspace observation yielded"
            )
