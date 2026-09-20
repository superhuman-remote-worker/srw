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
from orchestrator.services.vm_workspace_recovery_store import CleanupPermit


GENERATION = "00000000-0000-4000-8000-000000000001"


@pytest.mark.asyncio
async def test_cleanup_backoff_survives_ticks_and_does_not_spend_boot_attempts():
    from orchestrator.services.vm_provisioning_cleanup import recycle_provisioning_vm

    vm = {
        "status": "retiring_process_zero",
        "provisioned_at": 1.0,
        "provision_generation": GENERATION,
        "provision_attempts": 1,
    }

    async def merge(job_id, generation, updates):
        assert job_id == "job-1" and generation == GENERATION
        vm.update(updates)
        return True

    db = SimpleNamespace(merge_vm_context_if_provision_generation=merge)
    store = SimpleNamespace(
        acquire_cleanup_permit=AsyncMock(return_value=CleanupPermit(True, UUID(int=2))),
        complete_cleanup_permit=AsyncMock(),
    )
    provisioner = SimpleNamespace(
        capture_vm_teardown_identity=AsyncMock(
            return_value=VMTeardownIdentity(GENERATION, "vm-uid", "pvc-uid")
        ),
        release_vm_captured=AsyncMock(
            return_value=VMTeardownResult("process_zero_unproven", False)
        ),
    )
    now = 1000.0
    for delay in (60, 120, 240, 300, 300):
        result = await recycle_provisioning_vm(
            "job-1",
            dict(vm),
            db=db,
            provisioner=provisioner,
            recovery_store=store,
            now=now,
        )
        assert result == "process_zero_unproven"
        assert vm["retirement_last_result"] == "process_zero_unproven"
        assert vm["retirement_retry_after"] == now + delay
        assert vm["provision_attempts"] == 1
        assert (
            vm_provisioning_decision(
                vm,
                provision_attempts=1,
                max_provision_attempts=3,
                now=now + delay - 1,
                timeout_s=600,
            )
            == VM_WAIT
        )
        now += delay
        assert (
            vm_provisioning_decision(
                vm,
                provision_attempts=1,
                max_provision_attempts=3,
                now=now,
                timeout_s=600,
            )
            == VM_RECYCLE
        )
    assert vm["retirement_attempts"] == 5
    store.complete_cleanup_permit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("disposition", ["completed", "identity_superseded"])
async def test_cleanup_completes_only_definitive_outcomes(disposition):
    from orchestrator.services.vm_provisioning_cleanup import recycle_provisioning_vm

    db = SimpleNamespace(merge_vm_context_if_provision_generation=AsyncMock())
    store = SimpleNamespace(
        acquire_cleanup_permit=AsyncMock(return_value=CleanupPermit(True, UUID(int=2))),
        complete_cleanup_permit=AsyncMock(),
    )
    provisioner = SimpleNamespace(
        capture_vm_teardown_identity=AsyncMock(
            return_value=VMTeardownIdentity(GENERATION, "vm-uid", "pvc-uid")
        ),
        release_vm_captured=AsyncMock(
            return_value=VMTeardownResult(disposition, False)
        ),
    )
    result = await recycle_provisioning_vm(
        "job-1",
        {"provision_generation": GENERATION},
        db=db,
        provisioner=provisioner,
        recovery_store=store,
        now=1000.0,
    )
    assert result == disposition
    store.complete_cleanup_permit.assert_awaited_once_with(
        UUID(int=2), outcome=disposition
    )
    assert (
        db.merge_vm_context_if_provision_generation.await_args.args[2][
            "retirement_cleanup_pending"
        ]
        is False
    )
    assert provisioner.release_vm_captured.await_args.kwargs["purge_disk"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["held", "transport", "superseded"])
async def test_cleanup_hold_failure_or_generation_change_never_releases_vm(failure):
    from orchestrator.services.vm_provisioning_cleanup import recycle_provisioning_vm

    db = SimpleNamespace(
        merge_vm_context_if_provision_generation=AsyncMock(return_value=True)
    )
    store = SimpleNamespace(
        acquire_cleanup_permit=AsyncMock(return_value=CleanupPermit(False)),
        complete_cleanup_permit=AsyncMock(),
    )
    provisioner = SimpleNamespace(
        capture_vm_teardown_identity=AsyncMock(
            return_value=VMTeardownIdentity(
                GENERATION if failure != "superseded" else "new-generation",
                "vm-uid",
                "pvc-uid",
            )
        ),
        release_vm_captured=AsyncMock(),
    )
    if failure == "transport":
        provisioner.capture_vm_teardown_identity.side_effect = TimeoutError
    result = await recycle_provisioning_vm(
        "job-1",
        {"provision_generation": GENERATION},
        db=db,
        provisioner=provisioner,
        recovery_store=store,
        now=1000.0,
    )
    assert (
        result
        == {
            "held": "recovery_held",
            "transport": "cleanup_unavailable",
            "superseded": "identity_superseded",
        }[failure]
    )
    provisioner.release_vm_captured.assert_not_awaited()
    store.complete_cleanup_permit.assert_not_awaited()
    if failure == "superseded":
        db.merge_vm_context_if_provision_generation.assert_not_awaited()
    else:
        assert (
            db.merge_vm_context_if_provision_generation.await_args.args[1] == GENERATION
        )
