from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from orchestrator.services.dispatch_guards import (
    VM_RECYCLE,
    VM_WAIT,
    vm_provisioning_decision,
)
from orchestrator.services.vm_provisioner import VMTeardownIdentity, VMTeardownResult
from orchestrator.services.vm_provisioning_cleanup import recycle_provisioning_vm
from orchestrator.services.vm_workspace_recovery_store import CleanupPermit

GENERATION = "00000000-0000-4000-8000-000000000001"


@pytest.fixture(autouse=True)
def legacy_resource_cleanup(monkeypatch):
    """These handoff fixtures do not install a v3 resource charge."""
    import orchestrator.services.vm_workspace_recovery_store as recovery

    monkeypatch.setattr(
        recovery, "prepare_vm_cleanup_resource", AsyncMock(return_value=None)
    )


def decision(vm, now):
    return vm_provisioning_decision(
        vm, provision_attempts=3, max_provision_attempts=3, now=now, timeout_s=600
    )


def test_deleted_vm_with_pending_cleanup_cannot_advance_to_reprovision():
    vm = {
        "status": "deleted",
        "retirement_cleanup_pending": True,
        "retirement_retry_after": 1100.0,
    }
    assert decision(vm, 1000.0) == VM_WAIT
    assert decision(vm, 1100.0) == VM_RECYCLE


@pytest.mark.asyncio
@pytest.mark.parametrize("replayed", [True, False])
async def test_cleanup_handoff_survives_delayed_delete_and_completed_replay(replayed):
    vm = {"status": "retiring_process_zero", "provision_generation": GENERATION}
    events = []

    async def merge(_job_id, _generation, updates):
        vm.update(updates)
        events.append(("merge", dict(updates)))
        return True

    async def release(*_args, **_kwargs):
        assert vm.get("retirement_cleanup_pending") is True
        vm["status"] = "deleted"  # HTTP delete accepted; VM is still terminating.
        return VMTeardownResult("retry_pending", False)

    async def complete(*_args, **_kwargs):
        assert vm["retirement_cleanup_pending"] is True
        events.append(("complete", None))

    db = SimpleNamespace(merge_vm_context_if_provision_generation=merge)
    store = SimpleNamespace(
        acquire_cleanup_permit=AsyncMock(return_value=CleanupPermit(True, UUID(int=2))),
        complete_cleanup_permit=complete,
    )
    provisioner = SimpleNamespace(
        capture_vm_teardown_identity=AsyncMock(
            return_value=VMTeardownIdentity(GENERATION, "vm-uid", "pvc-uid")
        ),
        release_vm_captured=AsyncMock(side_effect=release),
    )
    result = await recycle_provisioning_vm(
        "job-1",
        dict(vm),
        db=db,
        provisioner=provisioner,
        recovery_store=store,
        now=1000.0,
    )
    assert result == "retry_pending"
    assert decision(vm, 1000.0) == VM_WAIT
    assert not any(event[0] == "complete" for event in events)

    # A lost delete response may leave the retirement status behind instead.
    vm["status"] = "retiring_process_zero"
    provisioner.release_vm_captured.side_effect = None
    provisioner.release_vm_captured.return_value = VMTeardownResult("completed", True)
    if replayed:
        store.acquire_cleanup_permit.return_value = CleanupPermit(
            True, UUID(int=2), completed_outcome="completed"
        )
    result = await recycle_provisioning_vm(
        "job-1",
        dict(vm),
        db=db,
        provisioner=provisioner,
        recovery_store=store,
        now=1060.0,
    )
    assert result == "completed"
    assert vm["status"] == "deleted"
    assert vm["retirement_cleanup_pending"] is False
    assert vm["retirement_retry_after"] is None
    if replayed:
        assert provisioner.release_vm_captured.await_count == 1
    else:
        assert events[-2][0] == "complete"
        assert events[-1][1]["retirement_cleanup_pending"] is False


@pytest.mark.asyncio
async def test_pre_actuation_cas_failure_keeps_cleanup_admission_open():
    db = SimpleNamespace(
        merge_vm_context_if_provision_generation=AsyncMock(return_value=False)
    )
    store = SimpleNamespace(
        acquire_cleanup_permit=AsyncMock(return_value=CleanupPermit(True, UUID(int=2))),
        complete_cleanup_permit=AsyncMock(),
    )
    provisioner = SimpleNamespace(
        capture_vm_teardown_identity=AsyncMock(
            return_value=VMTeardownIdentity(GENERATION, "vm-uid", "pvc-uid")
        ),
        release_vm_captured=AsyncMock(return_value=VMTeardownResult("completed", True)),
    )
    await recycle_provisioning_vm(
        "job-1",
        {"provision_generation": GENERATION},
        db=db,
        provisioner=provisioner,
        recovery_store=store,
        now=1000.0,
    )
    provisioner.release_vm_captured.assert_not_awaited()
    store.acquire_cleanup_permit.assert_not_awaited()
    store.complete_cleanup_permit.assert_not_awaited()


@pytest.mark.asyncio
async def test_completion_failure_keeps_replacement_fenced_for_retry():
    vm = {"status": "deleted", "provision_generation": GENERATION}

    async def merge(_job_id, _generation, updates):
        vm.update(updates)
        return True

    db = SimpleNamespace(merge_vm_context_if_provision_generation=merge)
    store = SimpleNamespace(
        acquire_cleanup_permit=AsyncMock(return_value=CleanupPermit(True, UUID(int=2))),
        complete_cleanup_permit=AsyncMock(side_effect=TimeoutError),
    )
    provisioner = SimpleNamespace(
        capture_vm_teardown_identity=AsyncMock(
            return_value=VMTeardownIdentity(GENERATION, "vm-uid", "pvc-uid")
        ),
        release_vm_captured=AsyncMock(return_value=VMTeardownResult("completed", True)),
    )
    assert (
        await recycle_provisioning_vm(
            "job-1",
            dict(vm),
            db=db,
            provisioner=provisioner,
            recovery_store=store,
            now=1000.0,
        )
        == "cleanup_unavailable"
    )
    assert vm["retirement_cleanup_pending"] is True
    assert decision(vm, 1000.0) == VM_WAIT
    assert decision(vm, 1060.0) == VM_RECYCLE
