"""Per-turn input locks for pinned session input (R1.B10).

The per-turn input lock guards against duplicate POSTs from concurrent Cockpit
tabs racing on the same pinned turn. It is in-process state, so each
application owns exactly one :class:`ThreadTurnLocks`; entries auto-clean five
minutes after release. The stateless lane never uses it: its run queue
serializes turns durably across replicas.
"""

from __future__ import annotations

import asyncio


class ThreadTurnLocks:
    """One application's ``(thread_id, turn_id)`` locks and in-flight turns."""

    CLEANUP_DELAY_S = 300

    def __init__(self) -> None:
        self.locks: dict[tuple[str, int], asyncio.Lock] = {}
        self.inflight: dict[str, int] = {}

    def ensure(self, thread_id: str, turn_id: int) -> asyncio.Lock:
        """Get or create the lock for (thread_id, turn_id). Concurrent callers
        landing on the same tuple share the same Lock object."""
        key = (thread_id, turn_id)
        lock = self.locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self.locks[key] = lock
        return lock

    def schedule_cleanup(self, thread_id: str, turn_id: int) -> None:
        """Remove the lock entry 5 minutes after release. Memory-leak guard
        for long-lived sessions accumulating per-turn locks."""

        async def _later() -> None:
            await asyncio.sleep(self.CLEANUP_DELAY_S)
            self.locks.pop((thread_id, turn_id), None)
            if self.inflight.get(thread_id) == turn_id:
                self.inflight.pop(thread_id, None)

        asyncio.create_task(_later(), name=f"turn-lock-cleanup-{thread_id[:8]}")


__all__ = ["ThreadTurnLocks"]
