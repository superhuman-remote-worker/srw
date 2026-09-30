"""Bounded observation of already-issued Job workspace Pods."""

from __future__ import annotations

import asyncio
import logging
from collections import deque

from orchestrator.services.blocking_effect import joined_async_call
from orchestrator.services.session_creation_observation import (
    SessionCreationObservationBudget,
)
from orchestrator.services.workspace_lifecycle import SessionWorkspaceObservationYielded

logger = logging.getLogger(__name__)


class JobCreationContinuationRunner:
    """Discovery supplies hints; exact owner/receipt/Pod guards admit observations."""

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
        self._workers: dict[asyncio.Task, dict] = {}

    def stop(self) -> None:
        self._stopping.set()

    async def _continue(self, candidate: dict) -> bool:
        checkpoint = SessionCreationObservationBudget(
            quantum_s=self.quantum_s,
            should_stop=lambda: self._stopping.is_set() or self.shutdown_event.is_set(),
        )
        try:
            checkpoint()
            return await self.provisioner.continue_job_workspace_creation(
                str(candidate["job_id"]),
                str(candidate["reservation_id"]),
                int(candidate["claim_token"]),
                str(candidate["pod_uid"]),
                observation_check=checkpoint,
            )
        except SessionWorkspaceObservationYielded:
            return False
        except Exception:
            logger.exception(
                "Job workspace creation observation held for %s", candidate["job_id"]
            )
            return False

    def _reap(self) -> None:
        for task in tuple(self._workers):
            if task.done():
                del self._workers[task]
                try:
                    task.result()
                except asyncio.CancelledError:
                    logger.warning("Job creation observer was externally cancelled")
                except Exception:
                    logger.exception("Job creation observer failed")

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
                        page = await self.db.list_current_job_creation_candidates(
                            after=cursor, limit=self.page_size
                        )
                    except Exception:
                        logger.exception("Job creation discovery held")
                        next_scan_at = loop.time() + self.round_delay_s
                    else:
                        cursor, exhausted = page["cursor"], page["exhausted"]
                        pending.extend(page["candidates"])
                        if exhausted:
                            next_scan_at = loop.time() + self.round_delay_s
                active_owners = {str(c["job_id"]) for c in self._workers.values()}
                while pending and len(self._workers) < 2:
                    candidate = pending.popleft()
                    if str(candidate["job_id"]) in active_owners:
                        continue
                    task = asyncio.create_task(
                        self._continue(candidate), name="job-creation-continuation"
                    )
                    self._workers[task] = candidate
                    active_owners.add(str(candidate["job_id"]))
                if (
                    len(self._workers) < 2
                    and not pending
                    and not exhausted
                    and loop.time() >= next_scan_at
                ):
                    continue
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
