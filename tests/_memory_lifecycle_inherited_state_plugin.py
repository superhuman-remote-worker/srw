"""Subprocess-only prior-file lifecycle state for the memory fixture regression."""

import pytest


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
