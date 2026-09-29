"""Component tests of the R3.3b owners with fake ports.

``SessionIdentityRuntime`` and ``SessionAttachCoordinator`` are exercised
without ``persistent_app``: exact adoption and clearing, call-time reads of
the lease and of the other owners, receipt and pool-claim fences, and two
independent instances of each that share nothing.
"""

from __future__ import annotations

import asyncio
import dataclasses
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from agent.api.lease_context import LeaseHandle
from agent.api.session_attach import (
    PoolAttachAdmission,
    SessionAttachCoordinator,
    SessionAttachPorts,
)
from agent.api.session_contract import ProtectedCloudUnavailable, WorkspaceNotReady
from agent.api.session_identity import (
    SessionIdentityPorts,
    SessionIdentityRuntime,
)
from shared.session_subagent_authority import SessionParentAuthorityRefused

G1 = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
T1 = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb1"
G2 = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2"
T2 = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb2"
THREAD = "10000000-0000-4000-8000-00000000000a"
AGENT = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"


class _Client:
    def __init__(self, *, accept: bool = True) -> None:
        self.accept = accept
        self.adopted: list[tuple] = []
        self.cleared: list[dict] = []

    def adopt_session_runtime_identity(self, generation, token, *, contract_advertised):
        self.adopted.append((generation, token, contract_advertised))
        return self.accept

    def clear_session_runtime_identity(self, **kwargs):
        self.cleared.append(kwargs)
        return True


def _identity(*, client=None, lease=None, stateless=False, replaced=None):
    state = SimpleNamespace(client=client, lease=lease, replaced=replaced or [])
    ports = SessionIdentityPorts(
        agent_id=lambda: AGENT,
        pod_uid=lambda: "pod-a",
        lease=lambda: state.lease,
        stateless_mode=lambda: stateless,
        orchestrator_client=lambda: state.client,
        identity_replaced=lambda: state.replaced.append(True),
    )
    return SessionIdentityRuntime(ports), state


# ---------------------------------------------------------------------------
# SessionIdentityRuntime
# ---------------------------------------------------------------------------


def test_two_identity_owners_share_nothing():
    a, a_state = _identity(client=_Client())
    b, b_state = _identity(client=_Client())
    a.bind_thread(THREAD)
    a.adopt(G1, T1, contract_advertised=True)
    a.begin_attach()
    a.mint_process_generation()
    a.set_status_contract(True)

    assert (b.thread_id, b.session_generation, b.attach_token) == (None, None, None)
    assert (b.runtime_contract, b.status_contract) == (False, False)
    assert (b.process_generation, b.attach_generation) == (None, 0)
    assert b_state.client.adopted == [] and b_state.replaced == []
    b.bind_thread("other")
    b.clear()
    assert (a.thread_id, a.session_generation, a.attach_token) == (THREAD, G1, T1)
    assert a.attach_generation == 1


@pytest.mark.parametrize(
    ("generation", "token", "contract"),
    (
        ("not-a-uuid", T2, False),
        (G2, "not-a-uuid", False),
        (G2, None, True),
        (None, T2, True),
    ),
)
def test_adopt_validates_before_it_mutates(generation, token, contract):
    owner, state = _identity(client=_Client())
    owner.adopt(G1, T1, contract_advertised=True)
    state.replaced.clear()

    with pytest.raises(WorkspaceNotReady):
        owner.adopt(generation, token, contract_advertised=contract)

    assert (owner.session_generation, owner.attach_token) == (G1, T1)
    assert owner.runtime_contract is True
    assert state.replaced == []
    assert state.client.adopted == [(G1, T1, True)]


def test_adopt_mirrors_the_client_and_refuses_when_it_cannot():
    owner, state = _identity(client=_Client(accept=False))
    with pytest.raises(WorkspaceNotReady, match="could not be adopted"):
        owner.adopt(G1, T1, contract_advertised=True)
    # Same order as before the move: local values and the retirement reset
    # happen before the client adoption that refused.
    assert (owner.session_generation, owner.attach_token) == (G1, T1)
    assert state.replaced == [True]


def test_clear_with_an_expected_identity_never_clears_a_successor():
    owner, state = _identity(client=_Client())
    owner.adopt(G2, T2, contract_advertised=True)
    state.replaced.clear()

    assert owner.clear(expected_generation=G1, expected_attach_token=T1) is False
    assert owner.clear(expected_generation=G2, expected_attach_token=T1) is False
    assert (owner.session_generation, owner.attach_token) == (G2, T2)
    assert state.client.cleared == [] and state.replaced == []

    assert owner.clear(expected_generation=G2, expected_attach_token=T2) is True
    assert (owner.session_generation, owner.attach_token) == (None, None)
    assert owner.runtime_contract is False
    assert state.client.cleared == [
        {"expected_generation": G2, "expected_attach_token": T2}
    ]
    assert state.replaced == [True]


def test_release_thread_is_exact_when_asked():
    owner, _ = _identity()
    owner.bind_thread(THREAD)
    assert owner.release_thread(expected="another-thread") is False
    assert owner.thread_id == THREAD
    assert owner.release_thread(expected=THREAD) is True
    assert owner.thread_id is None


def test_snapshot_reads_the_current_lease_every_time():
    handle = LeaseHandle()
    handle.update(THREAD, 7, executor_id="ex", pod_uid="pod-a")
    owner, state = _identity(lease=handle, stateless=True)
    owner.bind_thread(THREAD)

    assert owner.snapshot().lease is handle
    assert owner.stateless_lease_token() == 7
    handle.lease_token = 9
    assert owner.snapshot().lease.lease_token == 9
    assert owner.stateless_lease_token() == 9
    replacement = LeaseHandle()
    replacement.update(THREAD, 11, executor_id="ex", pod_uid="pod-a")
    state.lease = replacement
    assert owner.snapshot().lease is replacement
    replacement.mark_lost()
    assert owner.stateless_lease_token() is None


def test_workspace_payload_fences_the_adopted_generation():
    owner, _ = _identity(client=_Client())
    owner.adopt(G1, T1, contract_advertised=True)
    advertised = {"pinned_runtime_generation_contract": 1}

    with pytest.raises(WorkspaceNotReady, match="changed during attach"):
        owner.adopt_workspace_payload(
            {**advertised, "session_runtime_generation": G2},
            protected_required=False,
        )
    with pytest.raises(WorkspaceNotReady, match="contract disappeared"):
        owner.adopt_workspace_payload(
            {"session_runtime_generation": G1}, protected_required=False
        )
    assert (
        owner.adopt_workspace_payload(
            {**advertised, "session_runtime_generation": G1},
            protected_required=True,
        )
        == G1
    )

    legacy, _ = _identity(client=_Client())
    with pytest.raises(ProtectedCloudUnavailable):
        legacy.adopt_workspace_payload(
            {"session_runtime_generation": G2}, protected_required=True
        )
    # The unadvertised generation was still adopted before the refusal.
    assert legacy.session_generation == G2


def test_parent_authority_requires_the_exact_current_life():
    owner, state = _identity(client=_Client())
    with pytest.raises(SessionParentAuthorityRefused):
        owner.parent_authority()
    owner.bind_thread(THREAD)
    with pytest.raises(SessionParentAuthorityRefused):
        owner.parent_authority()
    owner.adopt(G1, T1, contract_advertised=True)
    pinned = owner.parent_authority()
    assert pinned.execution_lane == "pinned"
    assert str(pinned.session_runtime_generation) == G1

    handle = LeaseHandle()
    handle.update(THREAD, 3, executor_id="ex", pod_uid="pod-a")
    state.lease = handle
    assert owner.parent_authority().execution_lane == "stateless"
    handle.mark_lost()
    with pytest.raises(SessionParentAuthorityRefused):
        owner.parent_authority()


# ---------------------------------------------------------------------------
# SessionAttachCoordinator
# ---------------------------------------------------------------------------


def _ports(owner, **overrides: Any) -> SessionAttachPorts:
    """Fake ports: every field a mock unless the test names it."""

    values: dict[str, Any] = {}
    for field in dataclasses.fields(SessionAttachPorts):
        values[field.name] = MagicMock(name=field.name)
    values.update(
        identity=lambda: owner,
        session=lambda: None,
        pending_drain_suspend=lambda: None,
        stateless_mode=lambda: False,
    )
    values.update(overrides)
    return SessionAttachPorts(**values)


def _receipt(generation=G1, token=T1):
    return {
        "thread_id": THREAD,
        "session_runtime_generation": generation,
        "session_runtime_attach_token": token,
        "agent_pod_uid": "pod-a",
        "local_runtime_quiesced": True,
        "local_quiescence_protocol": "agent_runtime_zero_v1",
        "workspace_generation": None,
        "workspace_runtime_incarnation": None,
    }


def test_two_coordinators_share_no_claim_receipt_or_context():
    a_identity, _ = _identity(client=_Client())
    b_identity, _ = _identity(client=_Client())
    a = SessionAttachCoordinator(_ports(a_identity))
    b = SessionAttachCoordinator(_ports(b_identity))

    assert a.retain_release_receipt(_receipt()) is True
    a._pool_claim = THREAD
    a._cleanup_context = {"thread_id": THREAD}

    assert b.release_receipt is None
    assert b.pool_claim == (None, None, None)
    assert b.cleanup_context is None
    assert a.pool_heartbeat_status() == "session"
    assert b.pool_heartbeat_status() == "ready"
    assert b.retain_release_receipt(_receipt(G2, T2)) is True
    assert a.release_receipt == _receipt()


@pytest.mark.asyncio
async def test_pool_admission_is_per_instance_and_adopts_before_its_task():
    started = asyncio.Event()
    hold = asyncio.Event()

    a_identity, _ = _identity(client=_Client())
    b_identity, _ = _identity(client=_Client())
    a = SessionAttachCoordinator(_ports(a_identity))
    b = SessionAttachCoordinator(_ports(b_identity))

    async def blocked_attach(*_args, **_kwargs):
        started.set()
        await hold.wait()

    a.attach = blocked_attach
    request = {
        "pinned_runtime_generation_contract": 1,
        "session_runtime_generation": G1,
        "session_runtime_attach_token": T1,
    }
    admitted = await a.admit_pool_attach(THREAD, request)
    assert admitted == PoolAttachAdmission(
        200, {"status": "attaching", "thread_id": THREAD}
    )
    assert (a_identity.session_generation, a_identity.attach_token) == (G1, T1)
    await started.wait()
    duplicate = await a.admit_pool_attach(THREAD, request)
    assert duplicate.status_code == 409
    # The other runtime has its own claim.
    assert b.pool_claim == (None, None, None)
    assert b_identity.session_generation is None
    hold.set()
    await a.pool_task
    assert a.pool_claim == (None, None, None)


@pytest.mark.asyncio
async def test_release_stops_when_a_successor_replaces_the_receipt(monkeypatch):
    import agent.api.session_attach as session_attach

    monkeypatch.setattr(session_attach, "EXACT_SETTLEMENT_RETRY_DELAYS", (0.0,))
    owner, _ = _identity()
    successor = _receipt(G2, T2)
    coordinator = None

    async def refuse(*_args, **_kwargs):
        coordinator._release_receipt = successor
        return False

    client = SimpleNamespace(release_thread_agent=AsyncMock(side_effect=refuse))
    coordinator = SessionAttachCoordinator(
        _ports(owner, orchestrator_client=lambda: client)
    )
    coordinator.retain_release_receipt(_receipt())

    assert (
        await coordinator.release_receipt_until_confirmed(
            THREAD, runtime_generation=G1, runtime_attach_token=T1
        )
        is False
    )
    assert client.release_thread_agent.await_count == 1
    assert coordinator.release_receipt == successor


@pytest.mark.asyncio
async def test_coordinator_reads_the_identity_owner_at_call_time():
    first, _ = _identity(client=_Client())
    second, _ = _identity(client=_Client())
    current = {"owner": first}
    coordinator = SessionAttachCoordinator(
        _ports(None, identity=lambda: current["owner"])
    )
    request = {
        "pinned_runtime_generation_contract": 1,
        "session_runtime_generation": G1,
        "session_runtime_attach_token": T1,
    }
    coordinator.attach = AsyncMock()
    current["owner"] = second
    await coordinator.admit_pool_attach(THREAD, request)
    await coordinator.pool_task
    assert second.session_generation == G1
    assert first.session_generation is None
