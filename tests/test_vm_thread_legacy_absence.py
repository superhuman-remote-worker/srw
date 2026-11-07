"""A legacy-failed Session VM is proven absent before End may retire it."""

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from orchestrator.services import vm_thread_legacy_absence as legacy
from orchestrator.services.vm_provisioner import (
    VMProvisioner,
    VMTeardownIdentity,
    _VMTeardownProbe,
)
from shared.pinned_vm_creation_retirement import legacy_failed_vm_generation


GENERATION = "11111111-1111-4111-8111-111111111111"
THREAD = "22222222-2222-4222-8222-222222222222"
RUNTIME = "33333333-3333-4333-8333-333333333333"
VM_UID = "44444444-4444-4444-8444-444444444444"
DISK_UID = "55555555-5555-4555-8555-555555555555"
DELAY = legacy.LEGACY_VM_ABSENCE_SETTLE_DELAY_SECONDS


def _marker(**overrides):
    marker = VMProvisioner._fresh_provision_ctx()
    marker.update(
        provision_generation=GENERATION,
        status="failed",
        error="VM create dispatch was not accepted",
        creation_observation={"outcome": "transport_unknown"},
    )
    marker.update(overrides)
    return marker


def _thread(vm=None, **overrides):
    thread = {
        "id": THREAD,
        "runtime_generation": RUNTIME,
        "execution_lane": "pinned",
        "runtime_retirement_token": None,
        "metadata": {"vm": _marker() if vm is None else vm},
    }
    thread.update(overrides)
    return thread


@pytest.fixture(autouse=True)
def _fresh_sightings():
    legacy._first_seen.clear()
    yield
    legacy._first_seen.clear()


class TestLegacyFailedVmShape:
    @pytest.mark.parametrize("status", ["failed", "deleted", "retiring_process_zero"])
    def test_recognizes_a_sourceless_marker(self, status):
        assert legacy_failed_vm_generation(_marker(status=status)) == GENERATION

    @pytest.mark.parametrize(
        "overrides",
        [
            {"status": "ready"},
            {"status": "provisioning"},
            {"vm_uid": VM_UID},
            {"rootdisk_pvc_uid": DISK_UID},
            {"_runtime_incarnation": VM_UID},
            {"creation_request_id": str(uuid4())},
            {"identity_authenticated": True},
            {"identity_provision_generation": GENERATION},
            {"ssh_host": "10.0.0.1"},
            {"workspace_storage": {"owner_id": THREAD}},
            {"rootdisk": "kept"},
            {"idle_wake_operation_id": str(uuid4())},
            {"provision_generation": None},
            {"provision_generation": "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"},
            {"provision_generation": "not-a-uuid"},
        ],
    )
    def test_refuses_any_vm_or_source_authority(self, overrides):
        assert legacy_failed_vm_generation(_marker(**overrides)) is None

    def test_refuses_a_non_mapping(self):
        assert legacy_failed_vm_generation(None) is None
        assert legacy_failed_vm_generation("failed") is None


def _store(*, claim=True, record=True, drop=True):
    return SimpleNamespace(
        claim_managed_repository_workspace_retirement=AsyncMock(return_value=claim),
        record_managed_repository_workspace_process_zero=AsyncMock(return_value=record),
        drop_legacy_failed_thread_vm_marker=AsyncMock(return_value=drop),
    )


def _provisioner(proven=True, *, lifecycle=True):
    return SimpleNamespace(
        lifecycle_available=lifecycle,
        prove_legacy_thread_vm_absent=AsyncMock(return_value=proven),
    )


async def _settle(thread, store, provisioner, now):
    return await legacy.settle_legacy_failed_vm_absence(
        thread,
        store=store,
        vm_provisioner=provisioner,
        logger=logging.getLogger("test"),
        clock=lambda: now,
    )


RECEIPT = {
    "owner_kind": "thread",
    "scope": "vm",
    "provisioner": "vm",
    "runtime_incarnation": GENERATION,
}


class TestSettleLegacyFailedVmAbsence:
    @pytest.mark.asyncio
    async def test_waits_out_the_replay_window_then_proves_and_drops(self):
        store, provisioner = _store(), _provisioner()

        assert await _settle(_thread(), store, provisioner, 100.0) == "pending"
        assert await _settle(_thread(), store, provisioner, 100.0 + DELAY - 1) == (
            "pending"
        )
        provisioner.prove_legacy_thread_vm_absent.assert_not_awaited()

        assert await _settle(_thread(), store, provisioner, 100.0 + DELAY) == "proven"
        store.claim_managed_repository_workspace_retirement.assert_awaited_once_with(
            THREAD, **RECEIPT
        )
        provisioner.prove_legacy_thread_vm_absent.assert_awaited_once_with(
            THREAD, provision_generation=GENERATION
        )
        store.record_managed_repository_workspace_process_zero.assert_awaited_once_with(
            THREAD, **RECEIPT
        )
        drop = store.drop_legacy_failed_thread_vm_marker.await_args
        assert drop.args == (THREAD,)
        assert drop.kwargs["runtime_generation"] == RUNTIME
        assert drop.kwargs["provision_generation"] == GENERATION
        assert drop.kwargs["proof"]["evidence"] == (
            "controller_delete_then_exact_absence_v1"
        )
        assert not legacy._first_seen

    def test_the_wait_exceeds_the_controller_replay_window(self):
        from shared.vm_lifecycle_auth import (
            MAX_FUTURE_SKEW_SECONDS,
            MAX_MESSAGE_AGE_SECONDS,
        )

        assert DELAY > MAX_MESSAGE_AGE_SECONDS + MAX_FUTURE_SKEW_SECONDS

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "thread",
        [
            _thread(vm={"status": "ready", "vm_uid": VM_UID}),
            _thread(vm={}),
            _thread(execution_lane="stateless"),
            _thread(runtime_retirement_token=str(uuid4())),
            _thread(metadata={}),
        ],
    )
    async def test_other_rows_are_not_applicable(self, thread):
        store, provisioner = _store(), _provisioner()
        assert await _settle(thread, store, provisioner, 0.0) == "not_applicable"
        assert not legacy._first_seen
        store.claim_managed_repository_workspace_retirement.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_metadata_may_arrive_as_json_text(self):
        import json

        thread = _thread()
        thread["metadata"] = json.dumps(thread["metadata"])
        assert await _settle(thread, _store(), _provisioner(), 0.0) == "pending"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "store,provisioner,reached",
        [
            (_store(), _provisioner(lifecycle=False), "none"),
            (_store(claim=False), _provisioner(), "claim"),
            (_store(), _provisioner(proven=False), "prove"),
            (_store(record=False), _provisioner(), "record"),
            (_store(drop=False), _provisioner(), "drop"),
        ],
    )
    async def test_any_unproven_step_stays_pending(self, store, provisioner, reached):
        await _settle(_thread(), store, provisioner, 0.0)
        assert await _settle(_thread(), store, provisioner, DELAY) == "pending"
        steps = [
            store.claim_managed_repository_workspace_retirement,
            provisioner.prove_legacy_thread_vm_absent,
            store.record_managed_repository_workspace_process_zero,
            store.drop_legacy_failed_thread_vm_marker,
        ]
        # Steps run in order and stop at the first one that is not proven.
        reached_steps = ["none", "claim", "prove", "record", "drop"].index(reached)
        assert [step.await_count for step in steps] == [
            1 if index < reached_steps else 0 for index in range(len(steps))
        ]
        # The sighting survives so the next attempt does not wait again.
        assert legacy._first_seen

    @pytest.mark.asyncio
    async def test_a_failed_virtual_upgrade_is_undone_not_dropped(self):
        from shared.workspace_upgrade import VM_UPGRADE_SOURCE_KEY

        thread = _thread(vm=_marker(**{VM_UPGRADE_SOURCE_KEY: {"backend": "virtual"}}))
        store = _store()
        store.undo_thread_vm_upgrade = AsyncMock(return_value=True)
        await _settle(thread, store, _provisioner(), 0.0)
        assert await _settle(thread, store, _provisioner(), DELAY) == "proven"
        store.record_managed_repository_workspace_process_zero.assert_awaited_once()
        store.undo_thread_vm_upgrade.assert_awaited_once_with(
            THREAD, provision_generation=GENERATION
        )
        store.drop_legacy_failed_thread_vm_marker.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_provisioner_stays_pending(self):
        await _settle(_thread(), _store(), None, 0.0)
        assert await _settle(_thread(), _store(), None, DELAY) == "pending"


def _identity(vm_uid=None, rootdisk=None):
    return VMTeardownIdentity(
        provision_generation=GENERATION, vm_uid=vm_uid, rootdisk_pvc_uid=rootdisk
    )


def _absent(*, rootdisk=None, runtime=True):
    return _VMTeardownProbe(
        "absent",
        _identity(rootdisk=rootdisk),
        rootdisk_identity_known=True,
        runtime_absence_known=runtime,
        vmi_absent=runtime,
        launcher_absent=runtime,
    )


def _proof_provisioner(*probes, deleted=True, generation=GENERATION):
    provisioner = VMProvisioner()
    provisioner._current_provision_generation = AsyncMock(return_value=generation)
    provisioner._probe_vm_teardown_identity = AsyncMock(side_effect=list(probes))
    provisioner._delete_vm_with_identity = AsyncMock(return_value=deleted)
    return provisioner


class TestProveLegacyThreadVmAbsent:
    @pytest.mark.asyncio
    async def test_absent_vm_is_still_deleted_by_name_before_the_proof(self):
        provisioner = _proof_provisioner(_absent(), _absent())
        assert await provisioner.prove_legacy_thread_vm_absent(
            THREAD, provision_generation=GENERATION
        )
        provisioner._delete_vm_with_identity.assert_awaited_once_with(
            THREAD,
            purge_disk=True,
            provision_generation=GENERATION,
            expected_vm_uid=None,
            expected_rootdisk_pvc_uid=None,
            entity_type="thread",
            record_deleted_status=False,
        )

    @pytest.mark.asyncio
    async def test_a_late_vm_is_deleted_by_its_observed_identity(self):
        present = _VMTeardownProbe(
            "present",
            _identity(vm_uid=VM_UID, rootdisk=DISK_UID),
            rootdisk_identity_known=True,
        )
        provisioner = _proof_provisioner(present, _absent())
        assert await provisioner.prove_legacy_thread_vm_absent(
            THREAD, provision_generation=GENERATION
        )
        kwargs = provisioner._delete_vm_with_identity.await_args.kwargs
        assert kwargs["expected_vm_uid"] == VM_UID
        assert kwargs["expected_rootdisk_pvc_uid"] == DISK_UID

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "after",
        [
            _absent(rootdisk=DISK_UID),
            _absent(runtime=False),
            _VMTeardownProbe("unknown"),
            _VMTeardownProbe(
                "present", _identity(vm_uid=VM_UID), rootdisk_identity_known=True
            ),
        ],
    )
    async def test_only_complete_absence_after_delete_is_proof(self, after):
        provisioner = _proof_provisioner(_absent(), after)
        assert not await provisioner.prove_legacy_thread_vm_absent(
            THREAD, provision_generation=GENERATION
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("before", ["unknown", "superseded"])
    async def test_an_unknown_first_observation_sends_no_delete(self, before):
        provisioner = _proof_provisioner(_VMTeardownProbe(before))
        assert not await provisioner.prove_legacy_thread_vm_absent(
            THREAD, provision_generation=GENERATION
        )
        provisioner._delete_vm_with_identity.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_refused_delete_is_not_proof(self):
        provisioner = _proof_provisioner(_absent(), _absent(), deleted=False)
        assert not await provisioner.prove_legacy_thread_vm_absent(
            THREAD, provision_generation=GENERATION
        )

    @pytest.mark.asyncio
    async def test_a_changed_generation_is_left_alone(self):
        provisioner = _proof_provisioner(generation=str(uuid4()))
        assert not await provisioner.prove_legacy_thread_vm_absent(
            THREAD, provision_generation=GENERATION
        )
        provisioner._probe_vm_teardown_identity.assert_not_awaited()
        provisioner._delete_vm_with_identity.assert_not_awaited()
