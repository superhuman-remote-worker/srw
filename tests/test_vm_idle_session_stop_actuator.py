"""A VM Session's idle release stops its agent and VM through the actuator.

The retirement funnel settles a suspend only after process zero of the
captured runtime. A VM Session's agent never proves that itself and hands only
an End to the orchestrator's VM actuator, so the idle release drives that
actuator, and the actuator admits the VM stop under the idle-release permit
(the End cleanup accepts only an ``ended`` retirement and releases the VM's
resources, which the idle lifecycle accounts itself).
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services import pinned_retirement as retirement_module
from orchestrator.services.pinned_retirement import (
    PinnedRetirementDependencies,
    PinnedRetirementOperations,
)
from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleService
from orchestrator.services.vm_workspace_recovery_store import CleanupPermit

THREAD = "11111111-1111-4111-8111-111111111111"
GENERATION = "22222222-2222-4222-8222-222222222222"
TOKEN = "33333333-3333-4333-8333-333333333333"
OPERATION = "44444444-4444-4444-8444-444444444444"
POD = {
    "pod_name": "srw-agent-s-1",
    "pod_uid": "55555555-5555-4555-8555-555555555555",
    "namespace": "srw",
    "protection_protocol": "finalizer_v1",
}


def _operations(store: MagicMock) -> PinnedRetirementOperations:
    return PinnedRetirementOperations(
        PinnedRetirementDependencies(
            store=store,
            agent_provisioner=MagicMock(),
            persistent_provisioner=MagicMock(),
            container_provisioner=MagicMock(),
            docker_provisioner=MagicMock(),
            vm_provisioner=MagicMock(),
            recovery_store=MagicMock(),
            session_router=MagicMock(),
            resolve_protected_reader_backend=AsyncMock(),
            resolve_ssh_key_path=lambda: None,
            logger=logging.getLogger(__name__),
        )
    )


def _retirement(*, settle_status: str = "suspended", permanent: bool = False):
    return {
        "generation": GENERATION,
        "token": TOKEN,
        "permanent": permanent,
        "context": {"thread_id": THREAD, "settle_status": settle_status},
    }


@pytest.fixture
def permits(monkeypatch):
    calls: dict[str, list] = {"idle": [], "end": []}
    idle = CleanupPermit(allowed=True)
    end = CleanupPermit(allowed=True)

    async def idle_permit(recovery_store, **kwargs):
        calls["idle"].append(kwargs)
        return idle

    async def end_permit(recovery_store, **kwargs):
        calls["end"].append(kwargs)
        return end

    monkeypatch.setattr(retirement_module, "acquire_vm_cleanup_permit", idle_permit)
    monkeypatch.setattr(
        retirement_module,
        "acquire_pinned_thread_retirement_cleanup_permit",
        end_permit,
    )
    return SimpleNamespace(calls=calls, idle=idle, end=end)


@pytest.mark.asyncio
async def test_idle_owned_suspend_stops_the_vm_under_the_idle_permit(permits):
    store = MagicMock()
    store.fetchval = AsyncMock(return_value=True)
    identity = object()

    permit = await _operations(store)._admit_vm_cleanup(
        THREAD, identity, purge_disk=False, retirement=_retirement()
    )

    assert permit is permits.idle
    assert permits.calls["end"] == []
    assert permits.calls["idle"] == [
        {
            "owner_kind": "thread",
            "owner_id": THREAD,
            "identity": identity,
            "source": "vm_idle_release",
            "purge_disk": False,
        }
    ]
    query, *args = store.fetchval.await_args.args
    assert "vm_idle_operations" in query and "pinned_thread" in query
    assert args == [THREAD, GENERATION, TOKEN]


@pytest.mark.asyncio
async def test_a_suspend_without_an_open_idle_operation_keeps_the_end_permit(
    permits,
):
    store = MagicMock()
    store.fetchval = AsyncMock(return_value=False)

    permit = await _operations(store)._admit_vm_cleanup(
        THREAD, object(), purge_disk=False, retirement=_retirement()
    )

    assert permit is permits.end
    assert permits.calls["idle"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "retirement",
    [
        _retirement(settle_status="ended"),
        _retirement(settle_status="ended", permanent=True),
    ],
)
async def test_an_end_never_asks_for_the_idle_permit(permits, retirement, monkeypatch):
    from orchestrator.services import vm_thread_retained_disk_purge as retained

    monkeypatch.setattr(
        retained, "acquire_retained_disk_purge", AsyncMock(return_value=None)
    )
    store = MagicMock()
    store.fetchval = AsyncMock(return_value=True)

    await _operations(store)._admit_vm_cleanup(
        THREAD,
        object(),
        purge_disk=bool(retirement["permanent"]),
        retirement=retirement,
    )

    store.fetchval.assert_not_awaited()
    assert permits.calls["idle"] == []


@pytest.mark.asyncio
async def test_a_refused_idle_permit_holds_the_stop(permits, monkeypatch):
    async def refused(recovery_store, **kwargs):
        return CleanupPermit(allowed=False, reason="cleanup_authority_held")

    monkeypatch.setattr(retirement_module, "acquire_vm_cleanup_permit", refused)
    store = MagicMock()
    store.fetchval = AsyncMock(return_value=True)

    assert (
        await _operations(store)._admit_vm_cleanup(
            THREAD, object(), purge_disk=False, retirement=_retirement()
        )
        is None
    )
    assert permits.calls["end"] == []


def _operation() -> dict:
    return {
        "id": OPERATION,
        "owner_id": THREAD,
        "release_kind": "pinned_thread",
        "thread_runtime_generation": GENERATION,
        "thread_retirement_token": TOKEN,
        "thread_agent_stop_verified_at": None,
        "thread_agent_pod_identity": dict(POD),
    }


def _thread(status: str) -> dict:
    return {
        "id": THREAD,
        "status": status,
        "runtime_generation": GENERATION,
        "runtime_retirement_token": TOKEN,
        "runtime_retirement_authorized_at": "2026-11-22T10:00:00+00:00",
        "runtime_retirement_context": {
            "thread_id": THREAD,
            "settle_status": "suspended",
            "agent_id": "agent-1",
            "runtime_attach_token": "attach-1",
        },
    }


def _service(*, funnel: list[str], actuator: bool, thread_after: str):
    db = MagicMock()
    db.get_thread = AsyncMock(
        side_effect=[_thread("awaiting_user"), _thread(thread_after)]
    )
    pinned = MagicMock()
    pinned.recover_captured_process_zero = AsyncMock(return_value=actuator)
    pinned.stop_captured_retirement_agent = AsyncMock()
    pinned.wait_for_captured_agent_pod_retired = AsyncMock(return_value="exact_absent")
    thread_retirement = MagicMock()
    thread_retirement.dependencies = SimpleNamespace(pinned_retirement=pinned)
    thread_retirement.end_thread_flow = AsyncMock(
        side_effect=[{"status": status} for status in funnel]
    )
    provisioner = MagicMock()
    provisioner.attest_vm_idle_stop = AsyncMock(return_value={"kind": "stop"})
    service = VMIdleLifecycleService(
        db, provisioner, MagicMock(), thread_retirement=thread_retirement
    )
    service.store = MagicMock()
    service.store.record_thread_agent_stop = AsyncMock(return_value=True)
    service.store.complete_release = AsyncMock(return_value=True)
    return service, pinned, thread_retirement


@pytest.mark.asyncio
async def test_release_drives_the_actuator_when_the_funnel_waits_for_zero():
    service, pinned, thread_retirement = _service(
        funnel=["ending", "suspended"], actuator=True, thread_after="awaiting_user"
    )

    assert await service._release_thread(_operation(), current=lambda: True)

    pinned.recover_captured_process_zero.assert_awaited_once()
    (retirement,) = pinned.recover_captured_process_zero.await_args.args
    assert retirement == {
        "generation": GENERATION,
        "token": TOKEN,
        "permanent": False,
        "context": _thread("awaiting_user")["runtime_retirement_context"],
    }
    assert thread_retirement.end_thread_flow.await_count == 2
    for call in thread_retirement.end_thread_flow.await_args_list:
        assert call.kwargs["settle_status"] == "suspended"
        assert call.kwargs["require_physical_agent_stop"] is True
        assert call.kwargs["permanent"] is False
    service.store.record_thread_agent_stop.assert_awaited_once()
    service.store.complete_release.assert_awaited_once_with(
        OPERATION, evidence={"kind": "stop"}
    )


@pytest.mark.asyncio
async def test_release_stops_when_the_actuator_cannot_prove_zero():
    service, pinned, thread_retirement = _service(
        funnel=["ending"], actuator=False, thread_after="awaiting_user"
    )

    assert not await service._release_thread(_operation(), current=lambda: True)

    assert thread_retirement.end_thread_flow.await_count == 1
    service.store.record_thread_agent_stop.assert_not_awaited()
    service.store.complete_release.assert_not_awaited()


@pytest.mark.asyncio
async def test_release_does_not_settle_twice_when_the_actuator_already_did():
    service, pinned, thread_retirement = _service(
        funnel=["ending"], actuator=True, thread_after="suspended"
    )

    assert await service._release_thread(_operation(), current=lambda: True)

    assert thread_retirement.end_thread_flow.await_count == 1
    service.store.complete_release.assert_awaited_once()


@pytest.mark.asyncio
async def test_release_without_a_wait_never_calls_the_actuator():
    service, pinned, thread_retirement = _service(
        funnel=["suspended"], actuator=True, thread_after="suspended"
    )

    assert await service._release_thread(_operation(), current=lambda: True)

    pinned.recover_captured_process_zero.assert_not_awaited()
    service.store.complete_release.assert_awaited_once()


@pytest.mark.asyncio
async def test_a_lost_claim_stops_before_the_actuator():
    service, pinned, thread_retirement = _service(
        funnel=["ending"], actuator=True, thread_after="awaiting_user"
    )
    state = iter([True, False])

    assert not await service._release_thread(
        _operation(), current=lambda: next(state, False)
    )

    pinned.recover_captured_process_zero.assert_not_awaited()


def _ready_wake() -> dict:
    return {
        "id": OPERATION,
        "owner_id": THREAD,
        "phase": "waking",
        "wake_requested": True,
        "wake_execution_requested": True,
        "thread_terminal_intent_at": None,
        "wake_generation": "66666666-6666-4666-8666-666666666666",
        "wake_id": "77777777-7777-4777-8777-777777777777",
        "wake_ready_at": "2026-11-22T12:00:00+00:00",
        "claim_token": 3,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["active", "awaiting_user"])
async def test_a_wake_closes_when_the_fresh_runtime_already_serves(status):
    # Opening the session (its ordinary prepare) can bring the fresh agent up
    # before the wake's own prepare runs; the wake must still close, on the
    # store's exact fresh-agent proof, or the session can never sleep again.
    db = MagicMock()
    db.get_thread = AsyncMock(return_value={"id": THREAD, "status": status})
    prepare = AsyncMock(return_value=True)
    service = VMIdleLifecycleService(
        db,
        MagicMock(),
        MagicMock(),
        thread_retirement=MagicMock(),
        thread_prepare=prepare,
    )
    service.store = MagicMock()
    service.store.finish_thread_wake = AsyncMock(return_value=True)

    assert await service._wake_thread(_ready_wake(), current=lambda: True)

    prepare.assert_not_awaited()
    service.store.finish_thread_wake.assert_awaited_once_with(OPERATION)


@pytest.mark.asyncio
async def test_a_created_session_still_gets_the_wakes_own_prepare():
    db = MagicMock()
    db.get_thread = AsyncMock(return_value={"id": THREAD, "status": "created"})
    prepare = AsyncMock(return_value=True)
    service = VMIdleLifecycleService(
        db,
        MagicMock(),
        MagicMock(),
        thread_retirement=MagicMock(),
        thread_prepare=prepare,
    )
    service.store = MagicMock()
    service.store.finish_thread_wake = AsyncMock(return_value=True)

    assert await service._wake_thread(_ready_wake(), current=lambda: True)

    prepare.assert_awaited_once_with(THREAD, OPERATION)
    service.store.finish_thread_wake.assert_awaited_once_with(OPERATION)
