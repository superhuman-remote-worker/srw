"""Acknowledged End retries are bounded and do not starve other owners."""

import asyncio
from datetime import datetime, timezone

import pytest

from orchestrator.services import stale_agent_detector as detector
from tests.test_stale_agent_detector import _detector_dependencies, _mock_db


class ImmediateCadence(asyncio.Event):
    async def wait(self):
        if not self.is_set():
            raise asyncio.TimeoutError()
        return True


@pytest.mark.asyncio
@pytest.mark.parametrize("held", ["refused", "error", "timeout"])
async def test_settlement_cursor_passes_held_owners_and_preserves_other_sweeps(
    monkeypatch, held
):
    shutdown = ImmediateCadence()
    store = _mock_db(shutdown)
    rows = [
        {
            "id": f"00000000-0000-4000-8000-{index:012d}",
            "ended_at": datetime.now(timezone.utc),
        }
        for index in range(26)
    ]
    sweep = 0
    visits = []
    pages = []

    async def mark_stale(**_):
        nonlocal sweep
        sweep += 1
        if sweep == 3:
            shutdown.set()
        return []

    async def discover(*, limit, after):
        pages.append(after)
        return [
            row for row in rows if after is None or (row["ended_at"], row["id"]) > after
        ][:limit]

    async def retry(row, **_):
        visits.append((sweep, row["id"]))
        if row is rows[0]:
            if held == "error":
                raise RuntimeError("a single owner is unavailable")
            if held == "timeout":
                await asyncio.Event().wait()
        return row is rows[-1]

    store.mark_stale_agents_offline = mark_stale
    store.list_retryable_stateless_end_settlements = discover
    monkeypatch.setattr(detector, "retry_stateless_end_settlement", retry)
    monkeypatch.setattr(detector, "STATELESS_END_SETTLEMENT_SWEEP_SECONDS", 0.03)
    await asyncio.wait_for(
        detector.stale_agent_detector(
            shutdown, dependencies=_detector_dependencies(store)
        ),
        timeout=1,
    )
    assert (2, rows[-1]["id"]) in visits
    assert (3, rows[0]["id"]) in visits or pages[2] == (
        rows[-1]["ended_at"],
        rows[-1]["id"],
    )
    assert store.gc_offline_agents.await_count == 3
    assert store.recover_expired_lease_jobs.await_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["error", "timeout"])
async def test_discovery_failure_does_not_advance_cursor_or_block_other_sweeps(
    monkeypatch, failure
):
    shutdown = ImmediateCadence()
    store = _mock_db(shutdown)
    sweep = 0
    cursors = []

    async def mark_stale(**_):
        nonlocal sweep
        sweep += 1
        if sweep == 2:
            shutdown.set()
        return []

    async def discover(*, limit, after):
        cursors.append(after)
        if sweep == 1:
            if failure == "error":
                raise RuntimeError("discovery unavailable")
            await asyncio.Event().wait()
        return []

    store.mark_stale_agents_offline = mark_stale
    store.list_retryable_stateless_end_settlements = discover
    monkeypatch.setattr(detector, "STATELESS_END_SETTLEMENT_SWEEP_SECONDS", 0.03)
    await asyncio.wait_for(
        detector.stale_agent_detector(
            shutdown, dependencies=_detector_dependencies(store)
        ),
        timeout=1,
    )
    assert cursors == [None, None]
    assert store.gc_offline_agents.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["discovery", "replay"])
async def test_detector_cancellation_joins_active_settlement_work(monkeypatch, stage):
    """Cancellation must escape the sweep and finish its current owner inline."""
    shutdown = asyncio.Event()
    store = _mock_db(shutdown)
    started = asyncio.Event()
    stopped = asyncio.Event()
    candidate = {"id": "ended-owner", "ended_at": datetime.now(timezone.utc)}

    async def held_work():
        try:
            started.set()
            await asyncio.Event().wait()
        finally:
            stopped.set()

    async def discover(**_):
        if stage == "discovery":
            await held_work()
        return [candidate]

    async def retry(*_, **__):
        await held_work()

    store.list_retryable_stateless_end_settlements = discover
    monkeypatch.setattr(detector, "retry_stateless_end_settlement", retry)
    task = asyncio.create_task(
        detector.stale_agent_detector(
            shutdown, dependencies=_detector_dependencies(store)
        )
    )
    try:
        await asyncio.wait_for(started.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1)
        assert stopped.is_set()
        assert task.cancelled()
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
