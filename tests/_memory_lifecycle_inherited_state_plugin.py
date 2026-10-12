"""Subprocess-only prior-file lifecycle state for the memory fixture regression."""

import asyncio
import os

import pytest

_prior = None


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    if item.cls is None or item.cls.__name__ != "TestTeardownWiring":
        return
    from agent.api import persistent_app as pa

    pa._session_identity._runtime_contract = True
    pa._session_identity._status_contract = True
    pa._session_identity._session_generation = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
    pa._session_identity._attach_token = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb1"
    pa._orchestrator_client = None
    global _prior
    if os.environ.get("MEMORY_INHERITED_TERMINATION_TASK") == "1":
        loop = asyncio.new_event_loop()

        async def prior_cleanup():
            await loop.create_future()

        task = loop.create_task(prior_cleanup(), name="previous-fixture-termination")
        loop.run_until_complete(asyncio.sleep(0))
        pa._session_termination.termination_task = task
        _prior = (loop, task)


@pytest.hookimpl(trylast=True)
def pytest_runtest_teardown(item):
    global _prior
    if _prior is not None:
        loop, task = _prior
        task.cancel()
        loop.run_until_complete(asyncio.gather(task, return_exceptions=True))
        loop.close()
        _prior = None
