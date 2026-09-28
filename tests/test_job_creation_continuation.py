"""Job continuation scheduling does not let waiting Pods monopolize the scan."""

import asyncio
from uuid import uuid4

import pytest

from orchestrator.services.job_creation_continuation import (
    JobCreationContinuationRunner,
)


@pytest.mark.asyncio
async def test_two_slow_observers_yield_before_unrelated_ready_job():
    candidates = [
        {
            "job_id": str(uuid4()),
            "reservation_id": str(uuid4()),
            "claim_token": 1,
            "pod_uid": str(uuid4()),
        }
        for _ in range(3)
    ]
    ready_seen = asyncio.Event()
    shutdown = asyncio.Event()

    class Store:
        async def list_current_job_creation_candidates(self, *, after, limit):
            return {
                "candidates": candidates if after is None else (),
                "cursor": (None, "last") if after is None else after,
                "exhausted": True,
            }

    class Provider:
        async def continue_job_workspace_creation(
            self, job_id, reservation_id, claim_token, pod_uid, observation_check
        ):
            if job_id == candidates[2]["job_id"]:
                ready_seen.set()
                shutdown.set()
                return True
            observation_check.start()
            await asyncio.sleep(0.04)
            observation_check()

    runner = JobCreationContinuationRunner(
        db=Store(),
        provisioner=Provider(),
        shutdown_event=shutdown,
        quantum_s=0.01,
        round_delay_s=0.1,
    )
    await asyncio.wait_for(runner.run(), timeout=1)
    assert ready_seen.is_set()
