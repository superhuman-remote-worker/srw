"""Job continuation scheduling does not let waiting Pods monopolize the scan."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
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


@pytest.mark.asyncio
async def test_unobserved_creation_is_named_once_per_reservation(caplog):
    """A creation nothing observes (dev's cca716a8) shows up in the log once."""

    receipt = {
        "reservation_id": "b8d74c39-0000-4000-8000-000000000000",
        "job_id": "cca716a8-0000-4000-8000-000000000000",
        "job_status": "paused",
        "startup_state": "starting",
        "expires_at": "2026-11-07T17:14:58Z",
        "startup_first_ready_at": None,
    }
    db = SimpleNamespace(
        list_unobserved_job_creation_receipts=AsyncMock(return_value=(receipt,))
    )
    runner = JobCreationContinuationRunner(
        db=db, provisioner=SimpleNamespace(), shutdown_event=asyncio.Event()
    )
    with caplog.at_level("WARNING"):
        await runner._warn_unobserved()
        await runner._warn_unobserved()
    lines = [r for r in caplog.records if "not being observed" in r.getMessage()]
    assert len(lines) == 1
    assert "b8d74c39" in lines[0].getMessage()
    assert "never recorded" in lines[0].getMessage()


@pytest.mark.asyncio
async def test_unobserved_creation_check_never_breaks_the_runner():
    db = SimpleNamespace(
        list_unobserved_job_creation_receipts=AsyncMock(side_effect=RuntimeError)
    )
    runner = JobCreationContinuationRunner(
        db=db, provisioner=SimpleNamespace(), shutdown_event=asyncio.Event()
    )
    await runner._warn_unobserved()
