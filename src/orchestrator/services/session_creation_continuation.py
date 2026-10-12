"""Bounded, owned observation of already-issued initial Session creations."""

from __future__ import annotations

import asyncio
import logging
from collections import deque

from orchestrator.database.session_creation_candidates import SessionCreationCandidate
from orchestrator.services.blocking_effect import joined_async_call
from orchestrator.services.session_creation_observation import (
    SessionCreationObservationBudget,
)
from orchestrator.services.stateless_workspace_gate import (
    stateless_session_workspace_check,
)
from orchestrator.services.workspace_lifecycle import (
    SessionWorkspaceObservationYielded,
    WorkspaceOwner,
)

logger = logging.getLogger(__name__)


class SessionCreationContinuationRunner:
    """Discovery is scheduling only; existing owner/source gates admit effects.

    At most two owner tasks exist per process. The quantum yields only between
    joined effects; a begun SDK call or SSH probe always retains its guard.
    """

    def __init__(
        self,
        *,
        db,
        provisioner,
        shutdown_event: asyncio.Event,
        quantum_s: float = 2.0,
        page_size: int = 32,
        round_delay_s: float = 5.0,
    ):
        self.db = db
        self.provisioner = provisioner
        self.shutdown_event = shutdown_event
        self.quantum_s = quantum_s
        self.page_size = page_size
        self.round_delay_s = round_delay_s
        self._stopping = asyncio.Event()
        self._workers: dict[asyncio.Task, SessionCreationCandidate] = {}

    def stop(self) -> None:
        """Ask observations to yield; never cancel a creator's accepted effect."""
        self._stopping.set()

    async def _continue(self, candidate: SessionCreationCandidate) -> bool:
        checkpoint = SessionCreationObservationBudget(
            quantum_s=self.quantum_s,
            should_stop=lambda: self._stopping.is_set() or self.shutdown_event.is_set(),
        )

        try:
            checkpoint()
            if candidate.namespace != self.provisioner._namespace:
                return False
            lock = (
                self.db.try_thread_advisory_lock(candidate.thread_id)
                if candidate.lane == "pinned"
                else self.db.stateless_session_workspace_ensure_lock(
                    candidate.thread_id
                )
            )
            async with lock as owned:
                if (
                    not owned
                    or not await self.db.current_session_creation_candidate_is_exact(
                        candidate
                    )
                ):
                    return False
                checkpoint()
                if candidate.lane == "pinned":
                    return await self.provisioner.create_pinned_thread_workspace(
                        candidate.thread_id,
                        runtime_lock_held=True,
                        expected_creation=candidate,
                        observation_check=checkpoint,
                    )
                _, refusal = stateless_session_workspace_check(
                    await self.db.get_thread(candidate.thread_id)
                )
                if refusal is not None:
                    return False
                return await self.provisioner.continue_stateless_workspace_creation(
                    WorkspaceOwner.session(candidate.thread_id),
                    generation=candidate.runtime_generation,
                    expected_runtime_incarnation=candidate.pod_uid,
                    expected_creation=candidate,
                    observation_check=checkpoint,
                )
        except SessionWorkspaceObservationYielded:
            return False
        except Exception:
            logger.exception(
                "Session creation continuation held for %s", candidate.thread_id
            )
            return False

    def _reap(self) -> None:
        for task in tuple(self._workers):
            if task.done():
                del self._workers[task]
                try:
                    task.result()
                except asyncio.CancelledError:
                    # Workers are never cancelled by this runner. Retrieve an
                    # exceptional external cancellation without losing peers.
                    logger.warning("Session creation observer was externally cancelled")
                except Exception:
                    logger.exception("Session creation observer failed")

    async def _drain(self) -> None:
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._reap()

    async def run(self) -> None:
        cursor = None
        pending = deque()
        exhausted = False
        next_scan_at = 0.0
        loop = asyncio.get_running_loop()
        shutdown_wait = asyncio.create_task(self.shutdown_event.wait())
        stop_wait = asyncio.create_task(self._stopping.wait())
        try:
            while not self.shutdown_event.is_set() and not self._stopping.is_set():
                self._reap()
                if (
                    len(self._workers) < 2
                    and not pending
                    and loop.time() >= next_scan_at
                ):
                    if exhausted:
                        cursor, exhausted = None, False
                    try:
                        page = await self.db.list_current_session_creation_candidates(
                            after=cursor,
                            limit=self.page_size,
                        )
                    except Exception:
                        logger.exception("Session creation discovery held")
                        next_scan_at = loop.time() + self.round_delay_s
                    else:
                        cursor, exhausted = page.cursor, page.exhausted
                        pending.extend(page.candidates)
                        if exhausted:
                            next_scan_at = loop.time() + self.round_delay_s
                active_owners = {c.thread_id for c in self._workers.values()}
                while pending and len(self._workers) < 2:
                    candidate = pending.popleft()
                    if candidate.thread_id in active_owners:
                        continue
                    task = asyncio.create_task(
                        self._continue(candidate), name="session-creation-continuation"
                    )
                    self._workers[task] = candidate
                    active_owners.add(candidate.thread_id)
                if (
                    len(self._workers) < 2
                    and not pending
                    and not exhausted
                    and loop.time() >= next_scan_at
                ):
                    continue  # A full held page still advances to the next page.
                timeout = (
                    max(0.01, next_scan_at - loop.time())
                    if len(self._workers) < 2 and not pending
                    else None
                )
                await asyncio.wait(
                    (*self._workers, shutdown_wait, stop_wait),
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=timeout,
                )
        finally:
            self.stop()
            try:
                await joined_async_call(self._drain())
            finally:
                for task in (shutdown_wait, stop_wait):
                    task.cancel()
                await asyncio.gather(shutdown_wait, stop_wait, return_exceptions=True)
