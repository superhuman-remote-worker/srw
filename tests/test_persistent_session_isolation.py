"""A test never inherits the agent's session state from the test before it.

``tests/conftest.py::_isolate_persistent_session_runtime`` restores
``persistent_app``'s attached session, identity and retirement admission after
every test. The two tests below run in file order in one process: the first
leaves the state attach and retirement tests used to leak, the second must
start without it. A leaked runtime contract once made a later unproven
settlement retry forever with zero delay and grew an xdist worker to ~40 GiB.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from agent.api import persistent_app as pa

THREAD = "10000000-0000-4000-8000-00000000000a"
GENERATION = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
ATTACH = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb1"


def test_leaves_attached_identity_and_admission_behind(monkeypatch):
    # What a failed attach leaves: an adopted life with its runtime contract.
    pa._session = MagicMock()
    pa._session_identity._thread_id = THREAD
    pa._session_identity._session_generation = GENERATION
    pa._session_identity._attach_token = ATTACH
    pa._session_identity._runtime_contract = True
    pa._session_termination.retirement_admission_identity = (
        THREAD,
        GENERATION,
        ATTACH,
    )
    pa._session_termination.terminating = True
    # A later monkeypatch records the leaked thread as the value to put back.
    monkeypatch.setattr(pa._session_identity, "_thread_id", "successor-thread")

    assert pa._session_identity.runtime_contract is True


def test_next_test_starts_without_the_leaked_state():
    assert pa._session is None
    assert pa._session_identity.thread_id is None
    assert pa._session_identity.retirement_identity() is None
    assert pa._session_identity.runtime_contract is False
    assert pa._session_termination.retirement_admission_identity is None
    assert pa._session_termination.terminating is False
