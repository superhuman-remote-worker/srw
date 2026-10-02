"""Subprocess-only prior-file owners for the Officer fixture regression."""

import asyncio
import os

import pytest

_prior = None


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    if os.environ.get("OFFICER_INHERITED_FENCE") != "1":
        return
    from agent.api import persistent_app as pa

    identity, termination, inputs = (
        pa._session_identity,
        pa._session_termination,
        pa._session_input,
    )
    identity._thread_id = "prior-fixture-thread"
    identity._runtime_contract = True
    identity._session_generation = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
    termination.termination_admission_fenced = True
    loop, task = None, None
    if os.environ.get("OFFICER_INHERITED_TASK") == "1":
        loop = asyncio.new_event_loop()

        async def prior_cleanup():
            await loop.create_future()

        task = loop.create_task(prior_cleanup(), name="prior-officer-fixture-owner")
        loop.run_until_complete(asyncio.sleep(0))
        termination.termination_task = task
    global _prior
    _prior = (identity, termination, inputs, loop, task)


def pytest_sessionfinish(session, exitstatus):
    if _prior is None:
        return
    from agent.api import persistent_app as pa

    identity, termination, inputs, loop, task = _prior
    try:
        assert pa._session_identity is identity
        assert pa._session_termination is termination
        assert pa._session_input is inputs
        assert identity.thread_id == "prior-fixture-thread"
        assert identity.session_generation == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
        assert termination.termination_admission_fenced
        if task is not None:
            assert termination.termination_task is task and not task.done()
    finally:
        if task is not None:
            task.cancel()
            loop.run_until_complete(asyncio.gather(task, return_exceptions=True))
            loop.close()
