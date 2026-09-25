"""Terminal thread teardown must release the thread's VM.

``_archive_and_cleanup_workspace`` gated the thread-VM release on an
*allowlist* — ``status in ("provisioning", "created", "ready")`` — while the job
branch a few lines below used a *denylist*. Every other status therefore fell
through and ``release_thread_vm`` never ran, so the VM's rootdisk DataVolume
(20 GiB) and its Headscale node leaked permanently. ``suspended`` is the case
that matters: the rootdisk is kept on purpose while suspended, and terminal
teardown is the only thing that ever purges it. Kept-disk GC is jobs-only and
the controller's orphan backstop ships off, so nothing else reclaims them.

See knowledge-base/knowledge/issues/vm_reliability_assessment.md P1-7.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from tests import _b09_control_seams as control_seams

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

import orchestrator.main as orch_main


@pytest.fixture(autouse=True)
def legacy_resource_cleanup(monkeypatch):
    """These synthetic teardown owners have no v3 creation/retry charge."""
    import orchestrator.services.vm_workspace_recovery_store as recovery

    monkeypatch.setattr(
        recovery, "prepare_vm_cleanup_resource", AsyncMock(return_value=None)
    )


def _allow_cleanup_store():
    return SimpleNamespace(
        acquire_cleanup_permit=AsyncMock(
            return_value=SimpleNamespace(allowed=True, admission_id=None)
        )
    )


def _thread_with_vm(status):
    metadata = (
        {"vm": {"status": status, "ssh_host": "vm-thread", "ssh_port": 30022}}
        if status is not None
        else {}
    )
    return {"id": "t1", "metadata": metadata}


async def _cleanup_thread(status):
    """Run the real cleanup for a thread whose VM sits at ``status``."""
    identity = object()
    vm_provisioner = SimpleNamespace(
        is_available=True,
        lifecycle_available=True,
        capture_vm_teardown_identity=AsyncMock(return_value=identity),
        release_vm_captured=AsyncMock(
            return_value=SimpleNamespace(disposition="completed")
        ),
    )
    with (
        patch.object(
            orch_main,
            "postgres_db",
            SimpleNamespace(get_thread=AsyncMock(return_value=_thread_with_vm(status))),
        ),
        patch.object(orch_main, "vm_provisioner", vm_provisioner),
        patch.object(
            orch_main, "container_provisioner", SimpleNamespace(is_available=False)
        ),
        patch.object(
            orch_main,
            "VMWorkspaceRecoveryStore",
            return_value=_allow_cleanup_store(),
        ),
    ):
        await control_seams.archive_and_cleanup_workspace("t1", entity_type="threads")
    return vm_provisioner, identity


class TestThreadVmReleasedOnTeardown:
    @pytest.mark.asyncio
    async def test_suspended_thread_vm_is_released(self):
        # The leak: a suspended VM keeps its rootdisk, and this is the only
        # path that ever purges it.
        provisioner, identity = await _cleanup_thread("suspended")
        provisioner.capture_vm_teardown_identity.assert_awaited_once_with(
            "t1", entity_type="thread"
        )
        provisioner.release_vm_captured.assert_awaited_once_with(
            "t1",
            identity,
            ssh_host="vm-thread",
            ssh_port=30022,
            entity_type="thread",
            purge_disk=True,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status", ["ready", "created", "provisioning", "failed", "delete_failed"]
    )
    async def test_live_or_failed_thread_vm_is_released(self, status):
        provisioner, identity = await _cleanup_thread(status)
        provisioner.release_vm_captured.assert_awaited_once_with(
            "t1",
            identity,
            ssh_host="vm-thread",
            ssh_port=30022,
            entity_type="thread",
            purge_disk=True,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["deleted", "deleting"])
    async def test_already_torn_down_thread_vm_is_not_released_again(self, status):
        provisioner, _identity = await _cleanup_thread(status)
        provisioner.release_vm_captured.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_thread_that_never_had_a_vm_is_left_alone(self):
        # Guards the obvious mis-fix: swapping the allowlist for a bare denylist
        # makes a VM-less thread's absent status pass the check and fire a
        # spurious teardown.
        provisioner, _identity = await _cleanup_thread(None)
        provisioner.release_vm_captured.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_thread_teardown_refuses_transport_ack_without_exact_absence(self):
        identity = object()
        vm_provisioner = SimpleNamespace(
            is_available=True,
            lifecycle_available=True,
            capture_vm_teardown_identity=AsyncMock(return_value=identity),
            release_vm_captured=AsyncMock(
                return_value=SimpleNamespace(disposition="retry_pending")
            ),
        )
        with (
            patch.object(
                orch_main,
                "postgres_db",
                SimpleNamespace(
                    get_thread=AsyncMock(return_value=_thread_with_vm("ready"))
                ),
            ),
            patch.object(orch_main, "vm_provisioner", vm_provisioner),
            patch.object(
                orch_main, "container_provisioner", SimpleNamespace(is_available=False)
            ),
            patch.object(
                orch_main,
                "VMWorkspaceRecoveryStore",
                return_value=_allow_cleanup_store(),
            ),
        ):
            with pytest.raises(RuntimeError, match="retry_pending"):
                await control_seams.archive_and_cleanup_workspace(
                    "t1", entity_type="threads"
                )


async def _cleanup_job(status):
    """Run the real cleanup for a job whose VM sits at ``status``."""
    context = (
        {"vm": {"status": status, "ssh_host": "vm-job", "ssh_port": 30022}}
        if status is not None
        else {}
    )
    identity = SimpleNamespace(
        provision_generation="8d6f28f4-cc40-4adc-9ad3-343831f7874d",
        rootdisk_pvc_uid="8d6f28f4-cc40-4adc-9ad3-343831f7874e",
    )
    conn = SimpleNamespace(fetchval=AsyncMock(return_value=False))

    @asynccontextmanager
    async def acquire():
        yield conn

    vm_provisioner = SimpleNamespace(
        is_available=True,
        lifecycle_available=True,
        capture_vm_teardown_identity=AsyncMock(return_value=identity),
        release_vm_captured=AsyncMock(
            return_value=SimpleNamespace(disposition="completed")
        ),
    )
    with (
        patch.object(
            orch_main,
            "postgres_db",
            SimpleNamespace(
                get_job=AsyncMock(return_value={"id": "j1", "context": context}),
                acquire=acquire,
            ),
        ),
        patch.object(orch_main, "vm_provisioner", vm_provisioner),
        patch.object(
            orch_main, "container_provisioner", SimpleNamespace(is_available=False)
        ),
        patch.object(
            orch_main,
            "VMWorkspaceRecoveryStore",
            return_value=_allow_cleanup_store(),
        ),
    ):
        await control_seams.archive_and_cleanup_workspace("j1")
    return vm_provisioner, identity


class TestJobAndThreadTeardownAgree:
    """Both entity types must reclaim on the same statuses.

    They are separate branches of one function and drifted once already. Testing
    them behaviourally — rather than only asserting the shared predicate — is
    what catches a future edit that inlines one side again.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status",
        ["suspended", "ready", "created", "provisioning", "failed", "delete_failed"],
    )
    async def test_both_release_on_the_same_live_statuses(self, status):
        thread_provisioner, _ = await _cleanup_thread(status)
        job_provisioner, _ = await _cleanup_job(status)
        assert (
            thread_provisioner.release_vm_captured.await_count
            == job_provisioner.release_vm_captured.await_count
            == 1
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["deleted", "deleting", None])
    async def test_both_skip_on_the_same_finished_statuses(self, status):
        thread_provisioner, _ = await _cleanup_thread(status)
        job_provisioner, _ = await _cleanup_job(status)
        assert (
            thread_provisioner.release_vm_captured.await_count
            == job_provisioner.release_vm_captured.await_count
            == 0
        )
