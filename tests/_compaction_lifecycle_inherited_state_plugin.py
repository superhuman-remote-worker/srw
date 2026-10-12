"""Subprocess-only prior-file ownership for legacy compaction regressions."""

import asyncio
import os
import tempfile
from pathlib import Path

import pytest

_prior = None


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    from agent.api import persistent_app as pa

    state = os.environ["COMPACTION_INHERITED_STATE"]
    identity, termination, inputs = (
        pa._session_identity,
        pa._session_termination,
        pa._session_input,
    )
    identity._thread_id = "prior-compaction-fixture-thread"
    identity._session_generation = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
    identity._attach_token = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb1"
    queue = asyncio.Queue()
    inputs._queue = queue
    temporary = tempfile.TemporaryDirectory(prefix="srw-compact-owner-")
    termination.termination_sentinel_path = Path(temporary.name) / "terminating"
    if state in {"termination", "pending-task"}:
        termination.termination_admission_fenced = True
    if state == "retirement":
        termination.retirement_admission_identity = identity.retirement_identity()
    if state == "sentinel":
        termination.termination_sentinel_path.touch()
    loop, task = None, None
    if state == "pending-task":
        loop = asyncio.new_event_loop()

        async def previous_cleanup():
            await loop.create_future()

        task = loop.create_task(previous_cleanup(), name="prior-compaction-owner")
        loop.run_until_complete(asyncio.sleep(0))
        termination.termination_task = task
    global _prior
    _prior = (
        identity,
        termination,
        inputs,
        [dict(vars(owner)) for owner in (identity, termination, inputs)],
        loop,
        task,
        temporary,
    )


def pytest_sessionfinish(session, exitstatus):
    from agent.api import persistent_app as pa

    identity, termination, inputs, states, loop, task, temporary = _prior
    try:
        assert (pa._session_identity, pa._session_termination, pa._session_input) == (
            identity,
            termination,
            inputs,
        )
        assert [vars(owner) for owner in (identity, termination, inputs)] == states
        if task is not None:
            assert termination.termination_task is task and not task.done()
    finally:
        if task is not None:
            task.cancel()
            loop.run_until_complete(asyncio.gather(task, return_exceptions=True))
            loop.close()
        temporary.cleanup()
