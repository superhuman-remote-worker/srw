"""The pinned runtime life that this process's transcript writes belong to.

The stateless lane fences every ``thread_messages`` write on its run_queue
lease (:mod:`agent.api.lease_context`). The pinned lane's life is the R3.3b
identity instead: the registered agent id, the durable runtime generation and
the attach token minted for this attach. :class:`SessionIdentityRuntime` arms
the one process cell here when it adopts that identity, and the DB layer
(``postgres_db``) locks the thread row on it in the same transaction as each
write, so a replaced or retiring life can no longer add rows after its
successor settled them (parallel_subagents.md §14.2, P2).

States of the cell:

- *unarmed* (``None``): no exact generation was adopted (an orchestrator that
  predates the runtime-generation contract), or the process is stateless.
  Writes keep the historical unfenced behaviour, the same rolling-deploy
  exception as the pinned event journal.
- *armed*: the adopted life. Clearing the session identity does not disarm it:
  a late write of a life that ended is still fenced on that life, and refused.
  The next adoption replaces it.

Like ``lease_context`` this module imports nothing from the session app, the
loop or the DB layer.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from agent.api.lease_context import LeaseLostError


class PinnedWriteRefused(LeaseLostError):
    """A pinned transcript write found its runtime life no longer current.

    A :class:`LeaseLostError` so every caller treats it as the stateless lane
    treats a lost lease: the transaction rolled back and nothing landed, it is
    never a transient failure to retry, and the turn it came from belongs to a
    successor (or to no one) now.
    """


@dataclass(frozen=True, slots=True)
class PinnedWriteIdentity:
    """One adopted pinned life, as the thread and agent rows record it."""

    agent_id: Optional[str]
    runtime_generation: str
    attach_token: Optional[str]


class PinnedWriteFence:
    """Mutable process cell holding the armed pinned life (or ``None``)."""

    __slots__ = ("_identity",)

    def __init__(self) -> None:
        self._identity: Optional[PinnedWriteIdentity] = None

    @property
    def identity(self) -> Optional[PinnedWriteIdentity]:
        return self._identity

    def arm(self, identity: Optional[PinnedWriteIdentity]) -> None:
        """Install the adopted life; ``None`` restores unfenced writes."""

        self._identity = identity

    def reset(self) -> None:
        """Disarm (process reset and test isolation only)."""

        self._identity = None


# The one cell per process. ``SessionIdentityRuntime`` arms it through its
# ports; the DB layer reads it at write time.
PROCESS_PINNED_WRITE_FENCE = PinnedWriteFence()


def current_pinned_write_identity() -> Optional[PinnedWriteIdentity]:
    """The armed pinned life, or ``None`` when writes are unfenced."""

    return PROCESS_PINNED_WRITE_FENCE.identity
