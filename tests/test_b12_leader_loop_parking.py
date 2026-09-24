"""R1.B12 T3: a leader-gated loop whose dependency is absent parks, not returns.

``run_when_leader`` re-creates a gated loop that returns, on its next poll
(about once a second in the lifecycle). ``workspace_metering_loop`` and
``usage_rollup_loop`` return at once when the audit tier is absent, so the
leader restarted them — and logged "... disabled ..." — every second for its
whole tenure (observed live on 2026-09-23 with the audit database down). A
disabled loop must log once and wait for the stop event it was handed, as
``imap_poll_loop`` does.

The loops are driven through the lifecycle's own path,
``ApplicationTaskSet.start_leader_gated``, with the wrapper's poll shortened so
a few seconds of tenure fit into a fraction of one.
"""

from __future__ import annotations

import asyncio
import functools
import logging
from types import SimpleNamespace

import pytest

from orchestrator.services import (
    application_tasks,
    leader_election,
    usage_rollup,
    workspace_metering,
)

_XFAIL = pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "R1.B12 T3: a disabled metering/rollup loop returns, so run_when_leader "
        "restarts it every poll"
    ),
)

_POLL_SECONDS = 0.01


def _attribution(_owner_kind, _owner_id):  # pragma: no cover - never reached
    raise AssertionError("a disabled metering loop must not attribute anything")


_DISABLED = {
    "workspace_metering_without_ledger": (
        lambda stop: workspace_metering.workspace_metering_loop(
            stop, object(), None, _attribution
        ),
        workspace_metering.logger,
        "workspace metering loop disabled (no db/ledger)",
    ),
    "workspace_metering_with_unavailable_ledger": (
        lambda stop: workspace_metering.workspace_metering_loop(
            stop, object(), SimpleNamespace(is_available=False), _attribution
        ),
        workspace_metering.logger,
        "workspace metering loop disabled (no db/ledger)",
    ),
    "usage_rollup_without_rollup": (
        lambda stop: usage_rollup.usage_rollup_loop(stop, None),
        usage_rollup.logger,
        "usage rollup loop disabled (rollup unavailable)",
    ),
    "usage_rollup_with_unavailable_rollup": (
        lambda stop: usage_rollup.usage_rollup_loop(
            stop, SimpleNamespace(is_available=False)
        ),
        usage_rollup.logger,
        "usage rollup loop disabled (rollup unavailable)",
    ),
}


@pytest.fixture(autouse=True)
def _reset_leadership():
    leader_election.is_leader.clear()
    yield
    leader_election.is_leader.clear()


def _disabled_lines(caplog, logger: logging.Logger, message: str) -> int:
    return sum(
        1
        for record in caplog.records
        if record.name == logger.name and record.getMessage() == message
    )


@_XFAIL
@pytest.mark.asyncio
@pytest.mark.parametrize("case", sorted(_DISABLED))
async def test_a_disabled_loop_starts_once_per_leadership_tenure(
    monkeypatch, caplog, case
):
    make_loop, logger, message = _DISABLED[case]
    caplog.set_level(logging.INFO, logger=logger.name)
    monkeypatch.setattr(
        leader_election,
        "run_when_leader",
        functools.partial(leader_election.run_when_leader, poll_seconds=_POLL_SECONDS),
    )
    starts: list[asyncio.Event] = []

    def make(stop: asyncio.Event):
        starts.append(stop)
        return make_loop(stop)

    shutdown = asyncio.Event()
    tasks = application_tasks.ApplicationTaskSet(shutdown)
    leader_election.is_leader.set()
    tasks.start_leader_gated(case, make)
    try:
        # About 25 wrapper polls while this replica leads.
        await asyncio.sleep(25 * _POLL_SECONDS)
        assert len(starts) == 1, f"started {len(starts)} times in one tenure"
        assert _disabled_lines(caplog, logger, message) == 1
        # The loop was handed the lifecycle's own shutdown event.
        assert starts[0] is shutdown

        # Losing and regaining leadership is a new tenure: one more start.
        leader_election.is_leader.clear()
        await asyncio.sleep(10 * _POLL_SECONDS)
        leader_election.is_leader.set()
        await asyncio.sleep(25 * _POLL_SECONDS)
        assert len(starts) == 2
        assert _disabled_lines(caplog, logger, message) == 2
    finally:
        await asyncio.wait_for(tasks.stop([case]), timeout=5)


@_XFAIL
@pytest.mark.asyncio
@pytest.mark.parametrize("case", sorted(_DISABLED))
async def test_a_parked_loop_returns_promptly_once_stop_is_set(case):
    make_loop, _logger, _message = _DISABLED[case]
    stop = asyncio.Event()
    task = asyncio.create_task(make_loop(stop))
    try:
        await asyncio.sleep(0.05)
        assert not task.done(), "the disabled loop returned instead of parking"
        stop.set()
        await asyncio.wait_for(task, timeout=1)
        assert task.exception() is None
    finally:
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
