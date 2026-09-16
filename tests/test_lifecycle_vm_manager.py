"""Tests for VMInstanceManager (Phase 3).

VMs are tracked in jobs.context.vm and threads.metadata.vm rather than
as a fleet K8s object, so list_instances reads from the DB. The manager
delegates dispatch to the multi-backend VMProvisioner — tests don't need
to know which backend is active.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services.lifecycle import (
    Instance,
    InstanceLifecycleReconciler,
    ReapableInstanceManager,
    StatefulInstanceManager,
    VMInstanceManager,
    expected_vm_shas,
)
from orchestrator.services.vm_provisioner import VMTeardownIdentity, VMTeardownResult


# =============================================================================
# Helpers
# =============================================================================


def _make_manager(
    job_rows: list[dict] | None = None,
    thread_rows: list[dict] | None = None,
    is_available: bool = True,
    lifecycle_available: bool | None = None,
    snapshot_available: bool = True,
    suspension_enabled: bool = True,
    shared_child_exists: bool = False,
    completion_commands_enabled: bool = False,
    completion_control_active: bool = False,
    completion_command_exists: bool = False,
    workspace_recovery_store=None,
):
    provisioner = MagicMock()
    provisioner.is_available = is_available
    provisioner.lifecycle_available = (
        is_available if lifecycle_available is None else lifecycle_available
    )
    provisioner.delete_vm = AsyncMock(return_value=True)
    provisioner.delete_thread_vm = AsyncMock(return_value=True)
    provisioner.capture_vm_teardown_identity = AsyncMock(
        return_value=VMTeardownIdentity(
            provision_generation="00000000-0000-4000-8000-000000000001",
            vm_uid="vm-uid-1",
            rootdisk_pvc_uid="rootdisk-uid-1",
            ssh_host="10.0.0.5",
            ssh_port=2222,
            ssh_host_key_fingerprint="SHA256:" + ("A" * 43),
        )
    )
    provisioner.revalidate_vm_teardown_identity = AsyncMock(return_value="matched")
    provisioner.delete_vm_captured = AsyncMock(
        return_value=VMTeardownResult("completed", True)
    )
    provisioner.release_vm_captured = AsyncMock(
        return_value=VMTeardownResult("completed", True)
    )
    provisioner.delete_orphan_vm_captured = AsyncMock(
        return_value=VMTeardownResult("completed", True)
    )

    suspension = MagicMock()
    suspension.is_enabled = suspension_enabled
    suspension.restore_workspace = AsyncMock(return_value=True)
    suspension.restore_thread_workspace = AsyncMock(return_value=True)

    snapshot = MagicMock()
    snapshot.is_available = snapshot_available
    snapshot.capture_vm_snapshot = AsyncMock(return_value=True)

    db = AsyncMock()
    db.acquire = MagicMock()
    conn = AsyncMock()

    # Two-call pattern: first fetch is jobs, second is threads.
    conn.fetch = AsyncMock(side_effect=[job_rows or [], thread_rows or []])

    # Backs the live-shared-child EXISTS query and the flag-gated control-marker
    # query.  The latter is never reached in default-off tests.
    async def _fetchval(sql, *_args):
        if "_completion_control_claim" in sql:
            return completion_control_active
        return shared_child_exists

    conn.fetchval = AsyncMock(side_effect=_fetchval)

    async def _fetchrow(sql, *args):
        identity = str(args[0]) if args else ""
        if (
            "SELECT command_id, route" in sql
            and "FROM job_completion_sweep_exclusions" in sql
        ):
            return (
                {
                    "command_id": "44444444-4444-4444-8444-444444444444",
                    "route": "stand_down",
                }
                if completion_command_exists
                else None
            )
        if "FROM jobs" in sql and "FOR UPDATE" in sql:
            row = next(
                (r for r in (job_rows or []) if str(r.get("id")) == identity),
                None,
            )
            if row is None:
                return None
            return {
                "status": row.get("status"),
                "execution_lane": row.get("execution_lane") or "pinned",
                "context": row.get("context") or {},
                "control_active": completion_control_active,
            }
        if "UPDATE jobs" in sql and "RETURNING" in sql:
            return {"id": identity, "context": {}}
        return None

    conn.fetchrow = AsyncMock(side_effect=_fetchrow)
    tx = AsyncMock()
    tx.__aenter__ = AsyncMock(return_value=None)
    tx.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=tx)
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    db.acquire.return_value = ctx

    router = MagicMock()
    router.enqueue_job = AsyncMock()
    if workspace_recovery_store is None:
        workspace_recovery_store = MagicMock()
        workspace_recovery_store.acquire_cleanup_permit = AsyncMock(
            return_value=SimpleNamespace(
                allowed=True, admission_id="cleanup-1", reason=None
            )
        )
        workspace_recovery_store.complete_cleanup_permit = AsyncMock(return_value=True)
    mgr = VMInstanceManager(
        vm_provisioner=provisioner,
        suspension_service=suspension,
        snapshot_service=snapshot,
        db=db,
        completion_commands_enabled=completion_commands_enabled,
        completion_router=router if completion_commands_enabled else None,
        workspace_recovery_store=workspace_recovery_store,
    )
    mgr._test_completion_router = router
    return mgr, provisioner, suspension, snapshot, db


# =============================================================================
# expected_vm_shas
# =============================================================================


class TestExpectedVMShas:
    def test_empty_when_no_env(self, monkeypatch):
        monkeypatch.delenv("DEFAULT_VM_IMAGE", raising=False)
        assert expected_vm_shas() == set()

    def test_extracts_sha(self, monkeypatch):
        monkeypatch.setenv("DEFAULT_VM_IMAGE", "registry/vm:sha-abc123")
        assert expected_vm_shas() == {"abc123"}

    def test_skips_latest(self, monkeypatch):
        monkeypatch.setenv("DEFAULT_VM_IMAGE", "registry/vm:latest")
        assert expected_vm_shas() == set()


# =============================================================================
# Protocol satisfaction
# =============================================================================


class TestProtocolSatisfaction:
    def test_implements_stateful_manager(self):
        mgr, *_ = _make_manager()
        assert isinstance(mgr, StatefulInstanceManager)


# =============================================================================
# list_instances
# =============================================================================


class TestListInstances:
    @pytest.mark.asyncio
    async def test_empty_when_provisioner_unavailable(self):
        mgr, *_ = _make_manager(is_available=False)
        assert await mgr.list_instances() == []

    @pytest.mark.asyncio
    async def test_external_create_closed_still_lists_existing_vm(self):
        job = {
            "id": "job-external",
            "status": "processing",
            "execution_lane": "pinned",
            "context": {"vm": {"status": "ready", "ssh_host": "100.64.0.8"}},
        }
        mgr, provisioner, *_ = _make_manager(
            job_rows=[job],
            is_available=False,
            lifecycle_available=True,
        )

        [instance] = await mgr.list_instances()

        assert instance.bound_to == "job-external"
        assert provisioner.is_available is False
        assert provisioner.lifecycle_available is True

    @pytest.mark.asyncio
    async def test_external_lifecycle_auth_unavailable_does_not_list_or_reap(self):
        job = {
            "id": "job-unsigned-external",
            "status": "completed",
            "execution_lane": "pinned",
            "context": {"vm": {"status": "ready", "ssh_host": "100.64.0.8"}},
        }
        mgr, provisioner, *_ = _make_manager(
            job_rows=[job],
            is_available=False,
            lifecycle_available=False,
        )

        assert await mgr.list_instances() == []
        assert await mgr.reap_orphans() == 0
        provisioner.list_vms.assert_not_called()
        provisioner.delete_orphan_vm_captured.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_flag_off_does_not_read_or_publish_control_marker(self):
        job = {
            "id": "job-legacy",
            "status": "processing",
            "execution_lane": "pinned",
            "context": {"vm": {"status": "ready", "ssh_host": "10.0.0.8"}},
        }
        mgr, _, _, _, db = _make_manager(job_rows=[job])

        [inst] = await mgr.list_instances()

        assert "completion_control_owned" not in inst.metadata
        conn = db.acquire.return_value.__aenter__.return_value
        conn.fetchval.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_live_or_malformed_control_marker_is_listed_as_owner(self):
        job = {
            "id": "job-controlled",
            "status": "completed",
            "execution_lane": "pinned",
            "context": {
                "_completion_control_claim": "malformed",
                "vm": {"status": "ready", "ssh_host": "10.0.0.9"},
            },
        }
        mgr, _, _, _, db = _make_manager(
            job_rows=[job],
            completion_commands_enabled=True,
            completion_control_active=True,
        )

        [inst] = await mgr.list_instances()

        assert inst.metadata["completion_control_owned"] is True
        assert await mgr.is_healthy(inst) is True
        assert await mgr.is_idle(inst) is False
        assert await mgr.is_reapable(inst) is False
        conn = db.acquire.return_value.__aenter__.return_value
        [sql] = [
            str(call.args[0])
            for call in conn.fetchrow.await_args_list
            if "FROM jobs" in str(call.args[0]) and "FOR UPDATE" in str(call.args[0])
        ]
        assert "jsonb_typeof" in sql
        assert "clock_timestamp()" in sql
        assert "expires_epoch" in sql

    @pytest.mark.asyncio
    async def test_returns_job_vm(self):
        job = {
            "id": "job-uuid-1",
            "status": "paused",
            "execution_lane": "pinned",
            "context": {
                "vm": {
                    "status": "ready",
                    "ssh_host": "10.0.0.42",
                    "ssh_port": 22,
                    "vm_image": "registry/vm:sha-abc123",
                    "provisioner": "kubevirt",
                }
            },
        }
        mgr, *_ = _make_manager(job_rows=[job])
        instances = await mgr.list_instances()
        assert len(instances) == 1
        inst = instances[0]
        assert inst.kind == "vm"
        assert inst.bound_to == "job-uuid-1"
        assert inst.version == "abc123"
        assert inst.metadata["scope"] == "job"
        assert inst.metadata["execution_lane"] == "pinned"
        assert inst.metadata["job_status"] == "paused"
        assert inst.metadata["vm_status"] == "ready"
        assert inst.metadata["ssh_host"] == "10.0.0.42"
        assert inst.metadata["provisioner"] == "kubevirt"

    @pytest.mark.asyncio
    async def test_row_includes_ide_session_status(self):
        job = {
            "id": "job-uuid-2",
            "status": "pending_review",
            "context": {
                "vm": {"status": "ready", "ssh_host": "10.0.0.42"},
                "ide_session": {"status": "active"},
            },
        }
        mgr, *_ = _make_manager(job_rows=[job])
        inst = (await mgr.list_instances())[0]
        assert inst.metadata["ide_session_status"] == "active"

    @pytest.mark.asyncio
    async def test_returns_thread_vm(self):
        thread = {
            "id": "thread-uuid-1",
            "status": "ended",
            "execution_lane": "stateless",
            "metadata": {
                "vm": {
                    "status": "ready",
                    "ssh_host": "10.0.0.7",
                    "vm_image": "registry/vm:sha-xyz789",
                }
            },
        }
        mgr, *_ = _make_manager(thread_rows=[thread])
        instances = await mgr.list_instances()
        assert len(instances) == 1
        inst = instances[0]
        assert inst.metadata["scope"] == "thread"
        assert inst.metadata["execution_lane"] == "stateless"
        assert inst.metadata["thread_status"] == "ended"
        assert inst.version == "xyz789"

    @pytest.mark.asyncio
    async def test_handles_jsonb_returned_as_string(self):
        # asyncpg sometimes surfaces JSONB as a string.
        import json

        job = {
            "id": "job-2",
            "status": "processing",
            "context": json.dumps(
                {"vm": {"status": "ready", "vm_image": "registry/vm:sha-aa"}}
            ),
        }
        mgr, *_ = _make_manager(job_rows=[job])
        instances = await mgr.list_instances()
        assert len(instances) == 1
        assert instances[0].version == "aa"

    @pytest.mark.asyncio
    async def test_synthesizes_id_when_no_native_name(self):
        job = {
            "id": "job-uuid-noname",
            "status": "paused",
            "context": {"vm": {"status": "ready"}},
        }
        mgr, *_ = _make_manager(job_rows=[job])
        instances = await mgr.list_instances()
        assert instances[0].id.startswith("vm-job-")

    @pytest.mark.asyncio
    async def test_skips_empty_vm_context(self):
        # Defensive: a row with vm = {} shouldn't produce an instance.
        # (The SQL filter excludes this, but the parser should be safe
        # against it too in case of stale rows.)
        job = {"id": "j", "status": "paused", "context": {"vm": {}}}
        mgr, *_ = _make_manager(job_rows=[job])
        instances = await mgr.list_instances()
        assert instances == []

    @pytest.mark.asyncio
    async def test_skips_torn_down_vms(self):
        # VMs are listed from DB context.vm (not live K8s pods), so a VM that
        # has been deleted/suspended still has a context row until the external
        # controller (or a later create) clears it. Those must NOT be surfaced
        # to the reconciler — otherwise reap/drift/crash re-fire every tick on
        # an already-torn-down VM (the force-delete loop seen on dev).
        jobs = [
            {
                "id": "j-deleting",
                "status": "paused",
                "context": {"vm": {"status": "deleting"}},
            },
            {
                "id": "j-deleted",
                "status": "paused",
                "context": {"vm": {"status": "deleted"}},
            },
            {
                "id": "j-suspended",
                "status": "paused",
                "context": {"vm": {"status": "suspended"}},
            },
            {
                "id": "j-ready",
                "status": "paused",
                "context": {"vm": {"status": "ready", "ssh_host": "10.0.0.9"}},
            },
        ]
        mgr, *_ = _make_manager(job_rows=jobs)
        instances = await mgr.list_instances()
        ids = [i.bound_to for i in instances]
        assert ids == ["j-ready"]

    @pytest.mark.asyncio
    async def test_skips_failed_provisioning_vm(self):
        # 'failed' is a provisioning failure the dispatcher parks (does NOT
        # hot-retry). It must not surface to the reconciler, else is_healthy=False
        # force-deletes it → 'deleted' → dispatcher re-provisions → churn.
        jobs = [
            {
                "id": "j-failed",
                "status": "paused",
                "context": {"vm": {"status": "failed", "error": "timeout"}},
            },
            {
                "id": "j-ready",
                "status": "paused",
                "context": {"vm": {"status": "ready", "ssh_host": "10.0.0.9"}},
            },
        ]
        mgr, *_ = _make_manager(job_rows=jobs)
        instances = await mgr.list_instances()
        assert [i.bound_to for i in instances] == ["j-ready"]

    @pytest.mark.asyncio
    async def test_dispatchable_flag_from_row(self):
        # A paused, unassigned, freeze-free job → the dispatcher owns its VM's
        # bring-up. _fetch_vm_rows mirrors get_dispatchable_jobs and stamps
        # job_dispatchable so is_reapable can hand off.
        job = {
            "id": "j-resuming",
            "status": "paused",
            "unassigned": True,
            "freeze_free": True,
            "context": {"vm": {"status": "created"}},
        }
        mgr, *_ = _make_manager(job_rows=[job])
        inst = (await mgr.list_instances())[0]
        assert inst.metadata["job_dispatchable"] is True

    @pytest.mark.asyncio
    async def test_not_dispatchable_when_assigned_or_frozen_or_wrong_status(self):
        jobs = [
            {  # assigned → dispatcher does not own it
                "id": "j-assigned",
                "status": "paused",
                "unassigned": False,
                "freeze_free": True,
                "context": {"vm": {"status": "created"}},
            },
            {  # still frozen → genuinely parked, reap allowed
                "id": "j-frozen",
                "status": "paused",
                "unassigned": True,
                "freeze_free": False,
                "context": {"vm": {"status": "created"}},
            },
            {  # pending_review is idle-suspendable, NOT dispatchable
                "id": "j-review",
                "status": "pending_review",
                "unassigned": True,
                "freeze_free": True,
                "context": {"vm": {"status": "ready", "ssh_host": "10.0.0.1"}},
            },
        ]
        mgr, *_ = _make_manager(job_rows=jobs)
        instances = await mgr.list_instances()
        assert all(i.metadata["job_dispatchable"] is False for i in instances)


# =============================================================================
# Stateless VM lifecycle refusal
# =============================================================================


class TestStatelessVMLifecycleRefusal:
    """Generic lifecycle cleanup cannot bypass stateless terminal/loss ACKs."""

    @staticmethod
    def _thread_row(*, lane="stateless", turns=0, snapshot_turns=None):
        return {
            "id": "thread-stateless",
            "status": "ended",
            "execution_lane": lane,
            "total_turns": turns,
            "metadata": {
                "vm": {
                    "status": "ready",
                    "ssh_host": "10.0.0.27",
                    "last_snapshot_turns": snapshot_turns,
                }
            },
        }

    @staticmethod
    def _job_row(*, lane="stateless"):
        return {
            "id": "job-stateless",
            "status": "completed",
            "execution_lane": lane,
            "context": {"vm": {"status": "ready", "ssh_host": "10.0.0.28"}},
        }

    @staticmethod
    def _finish_tick_wiring(provisioner, db, job_rows, thread_rows):
        # list_instances consumes the first two fetches. purge_kept_disks is the
        # third fetch in the optional orphan hook at the end of the same tick.
        conn = db.acquire.return_value.__aenter__.return_value
        conn.fetch.side_effect = [job_rows, thread_rows, []]
        provisioner.list_vms = AsyncMock(return_value=[])

    @staticmethod
    def _assert_no_effects(provisioner, snapshot, db):
        snapshot.capture_vm_snapshot.assert_not_called()
        provisioner.delete_vm.assert_not_called()
        provisioner.delete_thread_vm.assert_not_called()
        provisioner.release_vm_captured.assert_not_called()
        db.merge_vm_context.assert_not_called()
        db.merge_thread_vm_context.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("turns", "snapshot_turns"),
        [(8, 3), (0, None)],
        ids=["dirty", "clean"],
    )
    async def test_ended_stateless_thread_tick_has_no_reap_effects(
        self, monkeypatch, turns, snapshot_turns
    ):
        monkeypatch.delenv("DEFAULT_VM_IMAGE", raising=False)
        thread = self._thread_row(turns=turns, snapshot_turns=snapshot_turns)
        mgr, provisioner, _, snapshot, db = _make_manager(thread_rows=[thread])
        self._finish_tick_wiring(provisioner, db, [], [thread])
        mgr._tcp_probe = AsyncMock(return_value=True)

        report = await InstanceLifecycleReconciler([mgr]).tick()

        assert report["vm"]["listed"] == 1
        assert report["vm"]["unhealthy"] == 0
        assert report["vm"]["reaped"] == 0
        assert report["vm"]["reap_attempts"] == 0
        assert report["vm"]["reap_forced"] == 0
        mgr._tcp_probe.assert_not_called()
        self._assert_no_effects(provisioner, snapshot, db)

    @pytest.mark.asyncio
    async def test_completed_stateless_job_tick_has_no_reap_effects(self, monkeypatch):
        monkeypatch.delenv("DEFAULT_VM_IMAGE", raising=False)
        job = self._job_row()
        mgr, provisioner, _, snapshot, db = _make_manager(job_rows=[job])
        self._finish_tick_wiring(provisioner, db, [job], [])
        mgr._tcp_probe = AsyncMock(return_value=True)

        report = await InstanceLifecycleReconciler([mgr]).tick()

        assert report["vm"]["listed"] == 1
        assert report["vm"]["reaped"] == 0
        mgr._tcp_probe.assert_not_called()
        self._assert_no_effects(provisioner, snapshot, db)

    @pytest.mark.asyncio
    async def test_unhealthy_stateless_vm_cannot_take_force_delete(self, monkeypatch):
        monkeypatch.delenv("DEFAULT_VM_IMAGE", raising=False)
        mgr, provisioner, _, snapshot, db = _make_manager()
        inst = Instance(
            kind="vm",
            id="vm-stateless",
            bound_to="thread-stateless",
            metadata={
                "scope": "thread",
                "execution_lane": "stateless",
                "thread_status": "ended",
                "vm_status": "failed",
                "total_turns": 8,
                "last_snapshot_turns": 3,
            },
        )
        mgr.list_instances = AsyncMock(return_value=[inst])
        mgr.reap_orphans = AsyncMock(return_value=0)

        report = await InstanceLifecycleReconciler([mgr]).tick()

        assert report["vm"]["listed"] == 1
        assert report["vm"]["unhealthy"] == 0
        self._assert_no_effects(provisioner, snapshot, db)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("scope", ["thread", "job"])
    async def test_direct_legacy_mutators_are_belted_for_both_scopes(self, scope):
        mgr, provisioner, _, snapshot, db = _make_manager()
        inst = Instance(
            kind="vm",
            id=f"vm-{scope}-stateless",
            bound_to=f"{scope}-stateless",
            metadata={
                "scope": scope,
                "execution_lane": "stateless",
                "thread_status": "ended" if scope == "thread" else None,
                "job_status": "completed" if scope == "job" else None,
                "vm_status": "failed",
                "ssh_host": "10.0.0.29",
                "snapshot_attempts": 5,
            },
        )

        assert await mgr.is_healthy(inst) is True
        assert await mgr.is_idle(inst) is False
        assert await mgr.is_reapable(inst) is False
        await mgr.record_attempt(inst)
        assert await mgr.snapshot(inst) is None
        await mgr.give_up(inst, grace_s=0)
        await mgr.drain(inst, grace_s=0)
        await mgr.delete(inst, grace_s=0)

        self._assert_no_effects(provisioner, snapshot, db)

    @pytest.mark.asyncio
    async def test_pinned_ended_dirty_thread_keeps_legacy_reap_behavior(
        self, monkeypatch
    ):
        monkeypatch.delenv("DEFAULT_VM_IMAGE", raising=False)
        thread = self._thread_row(lane="pinned", turns=8, snapshot_turns=3)
        mgr, provisioner, _, snapshot, db = _make_manager(thread_rows=[thread])
        self._finish_tick_wiring(provisioner, db, [], [thread])
        mgr._tcp_probe = AsyncMock(return_value=True)

        report = await InstanceLifecycleReconciler([mgr]).tick()

        assert report["vm"]["reaped"] == 1
        snapshot.capture_vm_snapshot.assert_awaited_once()
        provisioner.release_vm_captured.assert_awaited_once_with(
            "thread-stateless",
            provisioner.capture_vm_teardown_identity.return_value,
            purge_disk=True,
            entity_type="thread",
            capture_snapshot=False,
        )

    @pytest.mark.asyncio
    async def test_pinned_completed_job_keeps_legacy_reap_behavior(self, monkeypatch):
        monkeypatch.delenv("DEFAULT_VM_IMAGE", raising=False)
        job = self._job_row(lane="pinned")
        mgr, provisioner, _, snapshot, db = _make_manager(job_rows=[job])
        self._finish_tick_wiring(provisioner, db, [job], [])
        mgr._tcp_probe = AsyncMock(return_value=True)

        report = await InstanceLifecycleReconciler([mgr]).tick()

        assert report["vm"]["reaped"] == 1
        snapshot.capture_vm_snapshot.assert_awaited_once()
        provisioner.release_vm_captured.assert_awaited_once_with(
            "job-stateless",
            provisioner.capture_vm_teardown_identity.return_value,
            purge_disk=True,
            entity_type="job",
            capture_snapshot=False,
        )


# =============================================================================
# Live-shared-child reap guard (durable fix, parity with WorkspaceInstanceManager)
# =============================================================================


class TestLiveSharedChildGuard:
    @staticmethod
    def _conn(db):
        return db.acquire.return_value.__aenter__.return_value

    @pytest.mark.asyncio
    async def test_reviewing_vm_with_shared_child_flagged_not_reapable(self):
        # A 'reviewing' parent whose live VM is shared by a critic: the EXISTS
        # query returns true → has_live_shared_child → not reapable.
        job = {
            "id": "parent1",
            "status": "reviewing",
            "context": {"vm": {"status": "ready", "ssh_host": "10.0.0.42"}},
        }
        mgr, *_ = _make_manager(job_rows=[job], shared_child_exists=True)
        (inst,) = await mgr.list_instances()
        assert inst.metadata["has_live_shared_child"] is True
        assert await mgr.is_reapable(inst) is False
        assert await mgr.is_idle(inst) is False

    @pytest.mark.asyncio
    async def test_reviewing_vm_without_shared_child_is_reapable(self):
        job = {
            "id": "parent1",
            "status": "reviewing",
            "context": {"vm": {"status": "ready", "ssh_host": "10.0.0.42"}},
        }
        mgr, *_ = _make_manager(job_rows=[job], shared_child_exists=False)
        (inst,) = await mgr.list_instances()
        assert inst.metadata["has_live_shared_child"] is False
        assert await mgr.is_reapable(inst) is True

    @pytest.mark.asyncio
    async def test_processing_vm_skips_shared_child_query(self):
        # 'processing' is not reapable → the guard is moot → don't spend a query.
        job = {
            "id": "run1",
            "status": "processing",
            "context": {"vm": {"status": "ready", "ssh_host": "10.0.0.42"}},
        }
        mgr, _, _, _, db = _make_manager(job_rows=[job])
        (inst,) = await mgr.list_instances()
        assert "has_live_shared_child" not in inst.metadata
        self._conn(db).fetchval.assert_not_called()

    @pytest.mark.asyncio
    async def test_thread_vm_skips_shared_child_query(self):
        # Threads have no critic subjobs — the guard only applies to job VMs.
        thread = {
            "id": "t1",
            "status": "ended",
            "metadata": {"vm": {"status": "ready", "ssh_host": "10.0.0.9"}},
        }
        mgr, _, _, _, db = _make_manager(thread_rows=[thread])
        (inst,) = await mgr.list_instances()
        assert "has_live_shared_child" not in inst.metadata
        self._conn(db).fetchval.assert_not_called()

    @pytest.mark.asyncio
    async def test_query_passes_parent_id_and_native_id(self):
        mgr, _, _, _, db = _make_manager(shared_child_exists=True)
        assert await mgr._live_shared_child_exists("parent1", "vm-abc") is True
        conn = self._conn(db)
        conn.fetchval.assert_awaited_once()
        args = conn.fetchval.await_args.args
        assert args[1] == "parent1"
        assert args[2] == "vm-abc"

    @pytest.mark.asyncio
    async def test_query_false_without_native_id(self):
        mgr, _, _, _, db = _make_manager(shared_child_exists=True)
        assert await mgr._live_shared_child_exists("parent1", None) is False
        self._conn(db).fetchval.assert_not_called()

    @pytest.mark.asyncio
    async def test_query_fails_safe_on_db_error(self):
        mgr, _, _, _, db = _make_manager()
        db.acquire = MagicMock(side_effect=RuntimeError("db down"))
        assert await mgr._live_shared_child_exists("parent1", "vm-abc") is True


# =============================================================================
# is_healthy / is_idle
# =============================================================================


class TestIsHealthy:
    @pytest.mark.asyncio
    async def test_failed_is_unhealthy(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="vm", id="x", metadata={"vm_status": "failed"})
        assert await mgr.is_healthy(inst) is False

    @pytest.mark.asyncio
    async def test_ready_is_healthy(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="vm", id="x", metadata={"vm_status": "ready"})
        assert await mgr.is_healthy(inst) is True

    @pytest.mark.asyncio
    async def test_creating_is_healthy(self):
        # Creating/restoring/etc. — anything not 'failed' is healthy.
        # Crash recovery only kicks in on the explicit failure signal.
        mgr, *_ = _make_manager()
        for status in ("creating", "restoring", "suspended", None):
            inst = Instance(kind="vm", id="x", metadata={"vm_status": status})
            assert await mgr.is_healthy(inst) is True


class TestIsIdle:
    @pytest.mark.asyncio
    async def test_paused_job_is_idle_after_grace(self):
        # 'paused' is idle only once the warm grace has passed — a fresh pause
        # is often a human-wait (sudo/VM approval), and VM reaps are
        # destructive. See vm_upgrade_pause_workspace_reaped_before_approval.md.
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="vm",
            id="x",
            metadata={
                "job_status": "paused",
                "job_updated_at": datetime.now(timezone.utc) - timedelta(hours=2),
            },
        )
        assert await mgr.is_idle(inst) is True

    @pytest.mark.asyncio
    async def test_freshly_paused_job_is_not_idle(self):
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="vm",
            id="x",
            metadata={
                "job_status": "paused",
                "job_updated_at": datetime.now(timezone.utc),
            },
        )
        assert await mgr.is_idle(inst) is False

    @pytest.mark.asyncio
    async def test_reviewing_without_live_child_is_idle(self):
        # Parity with the workspace manager: 'reviewing' is back in the idle set,
        # guarded by has_live_shared_child. With no live critic sharing the VM,
        # a review-state parent is idle.
        mgr, *_ = _make_manager()
        inst = Instance(kind="vm", id="x", metadata={"job_status": "reviewing"})
        assert await mgr.is_idle(inst) is True

    @pytest.mark.asyncio
    async def test_status_with_live_shared_child_is_not_idle(self):
        # The guard: a VM shared by a live critic must not be drained.
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="vm",
            id="x",
            metadata={"job_status": "paused", "has_live_shared_child": True},
        )
        assert await mgr.is_idle(inst) is False

    @pytest.mark.asyncio
    async def test_active_ide_session_is_not_idle(self):
        for status in ("restoring", "active", "idle"):
            mgr, *_ = _make_manager()
            inst = Instance(
                kind="vm",
                id="x",
                metadata={
                    "job_status": "pending_review",
                    "ide_session_status": status,
                },
            )
            assert await mgr.is_idle(inst) is False, status

    @pytest.mark.asyncio
    async def test_processing_job_is_not_idle(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="vm", id="x", metadata={"job_status": "processing"})
        assert await mgr.is_idle(inst) is False

    @pytest.mark.asyncio
    async def test_ended_thread_is_idle(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="vm", id="x", metadata={"thread_status": "ended"})
        assert await mgr.is_idle(inst) is True


# =============================================================================
# snapshot / restore / drain / delete
# =============================================================================


class TestSnapshot:
    @pytest.mark.asyncio
    async def test_captures_via_snapshot_service(self):
        mgr, _, _, snapshot, _ = _make_manager()
        inst = Instance(
            kind="vm",
            id="vm-foo",
            bound_to="job-1",
            metadata={"ssh_host": "10.0.0.5", "ssh_port": 2222},
        )
        ref = await mgr.snapshot(inst)
        assert ref == "job-1"
        snapshot.capture_vm_snapshot.assert_awaited_once()
        # source_type=vm distinguishes from container snapshots.
        call_kwargs = snapshot.capture_vm_snapshot.call_args.kwargs
        assert call_kwargs["source_type"] == "vm"
        assert call_kwargs["ssh_port"] == 2222
        assert call_kwargs["ssh_host"] == "10.0.0.5"
        assert call_kwargs["expected_host_key_fingerprint"] == "SHA256:" + ("A" * 43)

    @pytest.mark.asyncio
    async def test_returns_none_without_ssh_host(self):
        mgr, _, _, snapshot, _ = _make_manager()
        inst = Instance(kind="vm", id="x", bound_to="job-1", metadata={})
        assert await mgr.snapshot(inst) is None
        snapshot.capture_vm_snapshot.assert_not_called()


class TestAttemptsExhausted:
    """Bounded snapshot retries — except when the target is provably
    unroutable.

    A tailnet-addressed VM (100.64.0.0/10) cannot be snapshotted from the
    orchestrator's vantage (no tailnet route — see
    vm_ssh_readiness_probe_unroutable_from_orchestrator.md), so waiting out
    max_attempts × tick (~5 min observed) only delays the force-delete while
    a dead VM holds shared cluster capacity. See
    knowledge-history/done/golden_image_cold_import_fails_inflight_vm_jobs.md §C.
    """

    @pytest.mark.asyncio
    async def test_counter_below_max_not_exhausted(self, monkeypatch):
        monkeypatch.delenv("ORCHESTRATOR_HAS_TAILNET_ROUTE", raising=False)
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="vm",
            id="x",
            bound_to="job-1",
            metadata={"ssh_host": "10.0.0.5", "snapshot_attempts": 2},
        )
        assert await mgr.attempts_exhausted(inst) is False

    @pytest.mark.asyncio
    async def test_counter_at_max_exhausted(self):
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="vm",
            id="x",
            bound_to="job-1",
            metadata={"ssh_host": "10.0.0.5", "snapshot_attempts": 5},
        )
        assert await mgr.attempts_exhausted(inst) is True

    @pytest.mark.asyncio
    async def test_unroutable_tailnet_host_instantly_exhausted(self, monkeypatch):
        monkeypatch.delenv("ORCHESTRATOR_HAS_TAILNET_ROUTE", raising=False)
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="vm",
            id="x",
            bound_to="job-1",
            metadata={"ssh_host": "100.64.23.194", "snapshot_attempts": 0},
        )
        assert await mgr.attempts_exhausted(inst) is True

    @pytest.mark.asyncio
    async def test_tailnet_host_with_route_uses_counter(self, monkeypatch):
        # ORCHESTRATOR_HAS_TAILNET_ROUTE escape hatch: snapshot CAN succeed,
        # so the bounded retry applies as usual.
        monkeypatch.setenv("ORCHESTRATOR_HAS_TAILNET_ROUTE", "true")
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="vm",
            id="x",
            bound_to="job-1",
            metadata={"ssh_host": "100.64.23.194", "snapshot_attempts": 0},
        )
        assert await mgr.attempts_exhausted(inst) is False

    @pytest.mark.asyncio
    async def test_missing_host_uses_counter(self):
        # No endpoint at all → keep the bounded-attempt behaviour (unknown
        # ≠ provably unroutable).
        mgr, *_ = _make_manager()
        inst = Instance(kind="vm", id="x", bound_to="job-1", metadata={})
        assert await mgr.attempts_exhausted(inst) is False


class TestRestore:
    @pytest.mark.asyncio
    async def test_job_scope_restores_via_workspace(self):
        mgr, _, suspension, _, _ = _make_manager()
        inst = Instance(kind="vm", id="x", metadata={"scope": "job"})
        await mgr.restore(inst, "job-1")
        suspension.restore_workspace.assert_awaited_once_with("job-1")

    @pytest.mark.asyncio
    async def test_thread_scope_restores_via_thread(self):
        mgr, _, suspension, _, _ = _make_manager()
        inst = Instance(kind="vm", id="x", metadata={"scope": "thread"})
        await mgr.restore(inst, "thread-1")
        suspension.restore_thread_workspace.assert_awaited_once_with("thread-1")


class TestSignalDrainPending:
    @pytest.mark.asyncio
    async def test_is_noop(self):
        # No in-pod drain hook for VMs (the management daemon doesn't
        # poll intents). Drift drain fires only when bound work pauses.
        mgr, provisioner, suspension, snapshot, db = _make_manager()
        inst = Instance(kind="vm", id="x", bound_to="job-1", metadata={"scope": "job"})
        await mgr.signal_drain_pending(inst)
        provisioner.delete_vm.assert_not_called()
        provisioner.delete_thread_vm.assert_not_called()
        snapshot.capture_vm_snapshot.assert_not_called()


class TestDelete:
    @pytest.mark.asyncio
    async def test_job_vm_uses_exact_retirement_release(self):
        mgr, provisioner, *_ = _make_manager()
        inst = Instance(kind="vm", id="x", bound_to="job-1", metadata={"scope": "job"})
        await mgr.delete(inst, grace_s=0)
        provisioner.capture_vm_teardown_identity.assert_awaited_once_with(
            "job-1", entity_type="job"
        )
        provisioner.release_vm_captured.assert_awaited_once_with(
            "job-1",
            provisioner.capture_vm_teardown_identity.return_value,
            purge_disk=True,
            entity_type="job",
            capture_snapshot=False,
        )

    @pytest.mark.asyncio
    async def test_thread_vm_uses_exact_retirement_release(self):
        mgr, provisioner, *_ = _make_manager()
        inst = Instance(
            kind="vm",
            id="x",
            bound_to="thread-1",
            metadata={"scope": "thread"},
        )
        await mgr.delete(inst, grace_s=0)
        provisioner.capture_vm_teardown_identity.assert_awaited_once_with(
            "thread-1", entity_type="thread"
        )
        provisioner.release_vm_captured.assert_awaited_once_with(
            "thread-1",
            provisioner.capture_vm_teardown_identity.return_value,
            purge_disk=True,
            entity_type="thread",
            capture_snapshot=False,
        )

    @pytest.mark.asyncio
    async def test_drain_dispatches_through_delete(self):
        mgr, provisioner, *_ = _make_manager()
        inst = Instance(kind="vm", id="x", bound_to="job-1", metadata={"scope": "job"})
        await mgr.drain(inst, grace_s=0)
        provisioner.release_vm_captured.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_recovery_hold_blocks_external_delete_boundary(self):
        recovery_store = MagicMock()
        recovery_store.acquire_cleanup_permit = AsyncMock(
            return_value=SimpleNamespace(
                allowed=False, reason="workspace_recovery_unresolved"
            )
        )
        mgr, provisioner, *_ = _make_manager(workspace_recovery_store=recovery_store)
        inst = Instance(kind="vm", id="x", bound_to="job-1", metadata={"scope": "job"})

        await mgr.delete(inst, grace_s=0)

        recovery_store.acquire_cleanup_permit.assert_awaited_once()
        provisioner.release_vm_captured.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_noop_when_provisioner_unavailable(self):
        mgr, provisioner, *_ = _make_manager(is_available=False)
        inst = Instance(kind="vm", id="x", bound_to="job-1", metadata={"scope": "job"})
        await mgr.delete(inst, grace_s=0)
        provisioner.release_vm_captured.assert_not_called()

    @pytest.mark.asyncio
    async def test_skipped_without_bound(self):
        mgr, provisioner, *_ = _make_manager()
        inst = Instance(kind="vm", id="x", bound_to=None, metadata={})
        await mgr.delete(inst, grace_s=0)
        provisioner.release_vm_captured.assert_not_called()


class TestCompletionControlLifecycleOwnership:
    @pytest.mark.asyncio
    async def test_retry_pending_delete_releases_claim_for_next_attempt(self):
        """A known incomplete delete is retryable, not ambiguous ownership."""

        job = {
            "id": "job-1",
            "status": "completed",
            "execution_lane": "pinned",
            "context": {},
        }
        mgr, provisioner, _, _, db = _make_manager(
            job_rows=[job], completion_commands_enabled=True
        )
        provisioner.release_vm_captured.return_value = VMTeardownResult(
            "retry_pending", False
        )
        inst = Instance(
            kind="vm",
            id="agent-vm-job-1",
            bound_to="job-1",
            metadata={
                "scope": "job",
                "job_status": "completed",
                "execution_lane": "pinned",
                "provision_generation": "00000000-0000-4000-8000-000000000001",
                "vm_uid": "vm-uid-1",
                "rootdisk_pvc_uid": "rootdisk-uid-1",
            },
        )

        await mgr.delete(inst, grace_s=0)

        provisioner.release_vm_captured.assert_awaited_once()
        conn = db.acquire.return_value.__aenter__.return_value
        assert any(
            "- '_completion_control_claim'" in str(call.args[0])
            for call in conn.fetchrow.await_args_list
        )

    @pytest.mark.asyncio
    async def test_unknown_delete_retains_claim_as_ambiguous(self):
        job = {
            "id": "job-1",
            "status": "completed",
            "execution_lane": "pinned",
            "context": {},
        }
        mgr, provisioner, _, _, db = _make_manager(
            job_rows=[job], completion_commands_enabled=True
        )
        provisioner.release_vm_captured.return_value = VMTeardownResult(
            "identity_unknown", False
        )
        inst = Instance(
            kind="vm",
            id="agent-vm-job-1",
            bound_to="job-1",
            metadata={
                "scope": "job",
                "job_status": "completed",
                "execution_lane": "pinned",
                "provision_generation": "00000000-0000-4000-8000-000000000001",
                "vm_uid": "vm-uid-1",
                "rootdisk_pvc_uid": "rootdisk-uid-1",
            },
        )

        await mgr.delete(inst, grace_s=0)

        provisioner.release_vm_captured.assert_awaited_once()
        conn = db.acquire.return_value.__aenter__.return_value
        assert not any(
            "- '_completion_control_claim'" in str(call.args[0])
            for call in conn.fetchrow.await_args_list
        )

    @pytest.mark.asyncio
    async def test_action_time_recheck_blocks_snapshot_delete_and_give_up(self):
        mgr, provisioner, _, snapshot, db = _make_manager(
            job_rows=[
                {
                    "id": "job-1",
                    "status": "paused",
                    "execution_lane": "pinned",
                    "context": {},
                }
            ],
            completion_commands_enabled=True,
            completion_control_active=True,
        )
        inst = Instance(
            kind="vm",
            id="vm-job-1",
            bound_to="job-1",
            metadata={
                "scope": "job",
                "job_status": "paused",
                "execution_lane": "pinned",
                "ssh_host": "10.0.0.7",
            },
        )

        assert await mgr.snapshot(inst) is None
        await mgr.delete(inst, grace_s=0)
        await mgr.give_up(inst, grace_s=0)

        snapshot.capture_vm_snapshot.assert_not_awaited()
        provisioner.release_vm_captured.assert_not_awaited()
        db.merge_vm_context.assert_not_awaited()
        conn = db.acquire.return_value.__aenter__.return_value
        marker_queries = [
            call
            for call in conn.fetchrow.await_args_list
            if "FROM jobs" in str(call.args[0]) and "FOR UPDATE" in str(call.args[0])
        ]
        assert len(marker_queries) == 3

    @pytest.mark.asyncio
    async def test_lookup_error_fails_closed_before_delete(self):
        mgr, provisioner, _, _, db = _make_manager(
            job_rows=[
                {
                    "id": "job-1",
                    "status": "completed",
                    "execution_lane": "pinned",
                    "context": {},
                }
            ],
            completion_commands_enabled=True,
        )
        conn = db.acquire.return_value.__aenter__.return_value
        base_fetchrow = conn.fetchrow.side_effect

        async def _fetchrow(sql, *args):
            if "FROM jobs" in sql and "FOR UPDATE" in sql:
                raise RuntimeError("db clock unavailable")
            return await base_fetchrow(sql, *args)

        conn.fetchrow.side_effect = _fetchrow
        inst = Instance(
            kind="vm",
            id="vm-job-1",
            bound_to="job-1",
            metadata={
                "scope": "job",
                "job_status": "completed",
                "execution_lane": "pinned",
            },
        )

        await mgr.delete(inst, grace_s=0)

        provisioner.release_vm_captured.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_replacement_after_claim_blocks_vm_snapshot_and_context_write(self):
        job = {
            "id": "job-1",
            "status": "completed",
            "execution_lane": "pinned",
            "context": {},
        }
        mgr, provisioner, _, snapshot, db = _make_manager(
            job_rows=[job], completion_commands_enabled=True
        )
        provisioner.revalidate_vm_teardown_identity.return_value = "superseded"
        inst = Instance(
            kind="vm",
            id="agent-vm-job-1",
            bound_to="job-1",
            metadata={
                "scope": "job",
                "job_status": "completed",
                "execution_lane": "pinned",
                "ssh_host": "10.0.0.7",
                "ssh_port": 22,
                "provision_generation": ("00000000-0000-4000-8000-000000000001"),
                "vm_uid": "vm-uid-1",
                "rootdisk_pvc_uid": "rootdisk-uid-1",
            },
        )

        assert await mgr.snapshot(inst) is None

        snapshot.capture_vm_snapshot.assert_not_awaited()
        provisioner.release_vm_captured.assert_not_awaited()
        db.merge_vm_context.assert_not_awaited()


# =============================================================================
# Reap predicates (ReapableInstanceManager) — mirror workspace, VM-adjusted
# =============================================================================


class TestReapableProtocol:
    def test_vm_manager_is_reapable_instance_manager(self):
        mgr, *_ = _make_manager()
        assert isinstance(mgr, ReapableInstanceManager)


class TestIsReapable:
    @pytest.mark.asyncio
    async def test_completed_job_is_reapable(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="vm", id="x", metadata={"job_status": "completed"})
        assert await mgr.is_reapable(inst) is True

    @pytest.mark.asyncio
    async def test_paused_job_is_reapable_after_grace(self):
        # A paused job with no dispatchable hint (e.g. still frozen / assigned)
        # is genuinely parked → its VM is reclaimable (idle-suspension) once
        # the warm grace has passed.
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="vm",
            id="x",
            metadata={
                "job_status": "paused",
                "job_updated_at": datetime.now(timezone.utc) - timedelta(hours=2),
            },
        )
        assert await mgr.is_reapable(inst) is True

    @pytest.mark.asyncio
    async def test_freshly_paused_job_is_not_reapable(self):
        # The incident: a job froze on a vm_upgrade approval (24h TTL) and the
        # reaper destroyed its VM on the next tick. Within the grace the VM
        # must survive.
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="vm",
            id="x",
            metadata={
                "job_status": "paused",
                "job_updated_at": datetime.now(timezone.utc),
            },
        )
        assert await mgr.is_reapable(inst) is False

    @pytest.mark.asyncio
    async def test_dispatchable_paused_job_not_reapable(self):
        # The fix: a paused job the dispatcher is resuming (dispatchable) must
        # NOT have its VM reaped, or the reaper fights the dispatcher — the
        # create→reap→create churn against the shared VM cluster.
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="vm",
            id="x",
            metadata={"job_status": "paused", "job_dispatchable": True},
        )
        assert await mgr.is_reapable(inst) is False

    @pytest.mark.asyncio
    async def test_dispatchable_created_job_not_reapable(self):
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="vm",
            id="x",
            metadata={"job_status": "created", "job_dispatchable": True},
        )
        assert await mgr.is_reapable(inst) is False

    @pytest.mark.asyncio
    async def test_non_dispatchable_paused_job_still_reapable(self):
        # Explicit false (assigned or frozen) → idle-suspension still applies
        # (after the warm grace).
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="vm",
            id="x",
            metadata={
                "job_status": "paused",
                "job_dispatchable": False,
                "job_updated_at": datetime.now(timezone.utc) - timedelta(hours=2),
            },
        )
        assert await mgr.is_reapable(inst) is True

    @pytest.mark.asyncio
    async def test_reviewing_without_live_child_is_reapable(self):
        # Parity: once no critic shares the VM, a review-state parent's VM is
        # reapable like 'pending_review'.
        mgr, *_ = _make_manager()
        inst = Instance(kind="vm", id="x", metadata={"job_status": "reviewing"})
        assert await mgr.is_reapable(inst) is True

    @pytest.mark.asyncio
    async def test_status_with_live_shared_child_is_not_reapable(self):
        # Regression (VM tier, the P0 bug): a VM shared by a non-terminal critic
        # must NOT be reaped, whatever the parent's own status — reaping strands
        # the critic's SSH. See knowledge-base/knowledge/issues/reviewing_parent_pod_reaped_under_critic.md.
        mgr, *_ = _make_manager()
        for status in ("reviewing", "pending_review", "paused"):
            inst = Instance(
                kind="vm",
                id="x",
                metadata={
                    "job_status": status,
                    "job_dispatchable": False,
                    "has_live_shared_child": True,
                },
            )
            assert await mgr.is_reapable(inst) is False, status

    @pytest.mark.asyncio
    async def test_active_ide_session_is_not_reapable(self):
        for status in ("restoring", "active", "idle"):
            mgr, *_ = _make_manager()
            inst = Instance(
                kind="vm",
                id="x",
                metadata={"job_status": "completed", "ide_session_status": status},
            )
            assert await mgr.is_reapable(inst) is False, status

    @pytest.mark.asyncio
    async def test_processing_job_not_reapable(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="vm", id="x", metadata={"job_status": "processing"})
        assert await mgr.is_reapable(inst) is False

    @pytest.mark.asyncio
    async def test_ended_thread_is_reapable(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="vm", id="x", metadata={"thread_status": "ended"})
        assert await mgr.is_reapable(inst) is True

    @pytest.mark.asyncio
    async def test_no_status_not_reapable(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="vm", id="x", metadata={})
        assert await mgr.is_reapable(inst) is False


class TestIsDirty:
    @pytest.mark.asyncio
    async def test_thread_zero_turns_is_clean(self):
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="vm",
            id="x",
            bound_to="t1",
            metadata={
                "thread_status": "ended",
                "total_turns": 0,
                "last_snapshot_turns": None,
            },
        )
        assert await mgr.is_dirty(inst) is False

    @pytest.mark.asyncio
    async def test_thread_turns_ahead_is_dirty(self):
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="vm",
            id="x",
            bound_to="t1",
            metadata={
                "thread_status": "ended",
                "total_turns": 5,
                "last_snapshot_turns": 2,
            },
        )
        assert await mgr.is_dirty(inst) is True

    @pytest.mark.asyncio
    async def test_suspended_vm_is_clean(self):
        # vm_status 'suspended' = already snapshotted to S3, nothing to lose.
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="vm",
            id="x",
            bound_to="t1",
            metadata={
                "thread_status": "ended",
                "total_turns": 9,
                "last_snapshot_turns": None,
                "vm_status": "suspended",
            },
        )
        assert await mgr.is_dirty(inst) is False

    @pytest.mark.asyncio
    async def test_terminal_job_with_snapshot_is_clean(self):
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="vm",
            id="x",
            bound_to="j1",
            metadata={"job_status": "completed", "snapshot_status": "available"},
        )
        assert await mgr.is_dirty(inst) is False

    @pytest.mark.asyncio
    async def test_job_without_snapshot_is_dirty(self):
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="vm",
            id="x",
            bound_to="j1",
            metadata={"job_status": "pending_review", "snapshot_status": None},
        )
        assert await mgr.is_dirty(inst) is True


class TestIsReachable:
    @pytest.mark.asyncio
    async def test_probes_vm_ssh_port_default_22(self):
        mgr, *_ = _make_manager()
        mgr._tcp_probe = AsyncMock(return_value=True)
        inst = Instance(kind="vm", id="x", metadata={"ssh_host": "10.0.0.5"})
        assert await mgr.is_reachable(inst) is True
        mgr._tcp_probe.assert_awaited_once_with("10.0.0.5", 22)

    @pytest.mark.asyncio
    async def test_probes_explicit_ssh_port(self):
        mgr, *_ = _make_manager()
        mgr._tcp_probe = AsyncMock(return_value=True)
        inst = Instance(
            kind="vm", id="x", metadata={"ssh_host": "10.0.0.5", "ssh_port": 2222}
        )
        assert await mgr.is_reachable(inst) is True
        mgr._tcp_probe.assert_awaited_once_with("10.0.0.5", 2222)

    @pytest.mark.asyncio
    async def test_unreachable_without_host(self):
        mgr, *_ = _make_manager()
        mgr._tcp_probe = AsyncMock(return_value=True)
        inst = Instance(kind="vm", id="x", metadata={})
        assert await mgr.is_reachable(inst) is False
        mgr._tcp_probe.assert_not_called()

    @pytest.mark.asyncio
    async def test_result_is_cached(self):
        mgr, *_ = _make_manager()
        mgr._clock = lambda: 1000.0
        mgr._tcp_probe = AsyncMock(return_value=True)
        inst = Instance(kind="vm", id="x", metadata={"ssh_host": "10.0.0.5"})
        await mgr.is_reachable(inst)
        await mgr.is_reachable(inst)
        mgr._tcp_probe.assert_awaited_once()


class TestAttemptCounter:
    @pytest.mark.asyncio
    async def test_record_attempt_increments_job_vm_context(self):
        mgr, _, _, _, db = _make_manager()
        db.merge_vm_context_if_provision_generation = AsyncMock(return_value=True)
        inst = Instance(
            kind="vm",
            id="x",
            bound_to="j1",
            metadata={"scope": "job", "snapshot_attempts": 2},
        )
        await mgr.record_attempt(inst)
        db.merge_vm_context.assert_awaited_once_with("j1", {"snapshot_attempts": 3})

    @pytest.mark.asyncio
    async def test_record_attempt_increments_thread_vm_context(self):
        mgr, _, _, _, db = _make_manager()
        db.merge_thread_vm_context = AsyncMock(return_value=True)
        inst = Instance(
            kind="vm",
            id="x",
            bound_to="t1",
            metadata={"scope": "thread", "snapshot_attempts": 0},
        )
        await mgr.record_attempt(inst)
        db.merge_thread_vm_context.assert_awaited_once_with(
            "t1", {"snapshot_attempts": 1}
        )

    @pytest.mark.asyncio
    async def test_exhausted_at_threshold(self, monkeypatch):
        monkeypatch.setenv("WORKSPACE_SNAPSHOT_MAX_ATTEMPTS", "5")
        mgr, *_ = _make_manager()
        inst = Instance(kind="vm", id="x", metadata={"snapshot_attempts": 5})
        assert await mgr.attempts_exhausted(inst) is True

    @pytest.mark.asyncio
    async def test_not_exhausted_below_threshold(self, monkeypatch):
        monkeypatch.setenv("WORKSPACE_SNAPSHOT_MAX_ATTEMPTS", "5")
        mgr, *_ = _make_manager()
        inst = Instance(kind="vm", id="x", metadata={"snapshot_attempts": 4})
        assert await mgr.attempts_exhausted(inst) is False


class TestGiveUp:
    """give_up fires on dirty + unreachable + snapshot-exhausted — exactly the
    state whose files we must not destroy. The kept rootdisk IS the recovery
    artifact. knowledge-base/knowledge/features/vm_persistent_rootdisk.md D2.
    """

    @pytest.mark.asyncio
    async def test_give_up_keeps_the_job_rootdisk(self):
        mgr, provisioner, *_ = _make_manager()
        inst = Instance(kind="vm", id="x", bound_to="j1", metadata={"scope": "job"})
        await mgr.give_up(inst, grace_s=0)
        provisioner.release_vm_captured.assert_awaited_once_with(
            "j1",
            provisioner.capture_vm_teardown_identity.return_value,
            purge_disk=False,
            entity_type="job",
            capture_snapshot=False,
        )

    @pytest.mark.asyncio
    async def test_give_up_keeps_the_thread_rootdisk(self):
        mgr, provisioner, *_ = _make_manager()
        inst = Instance(kind="vm", id="x", bound_to="t1", metadata={"scope": "thread"})
        await mgr.give_up(inst, grace_s=0)
        provisioner.release_vm_captured.assert_awaited_once_with(
            "t1",
            provisioner.capture_vm_teardown_identity.return_value,
            purge_disk=False,
            entity_type="thread",
            capture_snapshot=False,
        )

    @pytest.mark.asyncio
    async def test_give_up_records_the_kept_disk(self):
        """The GC sweep finds kept disks by this key, so a give_up that forgets
        to write it leaks 20 Gi until the controller's age backstop fires."""
        mgr, _, _, _, db = _make_manager()
        db.merge_vm_context = AsyncMock(return_value=True)
        inst = Instance(kind="vm", id="x", bound_to="j1", metadata={"scope": "job"})
        await mgr.give_up(inst, grace_s=0)
        db.merge_vm_context.assert_awaited_with("j1", {"rootdisk": "kept"})

    @pytest.mark.asyncio
    async def test_drain_still_purges(self):
        """Version drift wants a fresh image anyway — keeping the old disk
        would defeat the drain."""
        mgr, provisioner, *_ = _make_manager()
        inst = Instance(kind="vm", id="x", bound_to="j1", metadata={"scope": "job"})
        await mgr.drain(inst, grace_s=0)
        provisioner.release_vm_captured.assert_awaited_once()


class TestSnapshotResetsAttempts:
    @pytest.mark.asyncio
    async def test_snapshot_success_resets_attempt_counter(self):
        mgr, _, _, snapshot, db = _make_manager()
        db.merge_vm_context = AsyncMock(return_value=True)
        snapshot.capture_vm_snapshot = AsyncMock(return_value=True)
        inst = Instance(
            kind="vm",
            id="x",
            bound_to="j1",
            metadata={"scope": "job", "ssh_host": "10.0.0.5"},
        )
        ref = await mgr.snapshot(inst)
        assert ref == "j1"
        db.merge_vm_context_if_provision_generation.assert_awaited_with(
            "j1",
            "00000000-0000-4000-8000-000000000001",
            {"snapshot_attempts": 0},
        )


# =============================================================================
# End-to-end churn regression: the dispatcher-vs-reconciler fight
# =============================================================================


class TestChurnRegression:
    """A dispatchable-paused job's provisioning VM must survive a full
    reconciler tick.

    The deployed churn: a version_upgrade drain parks the job 'paused'; the
    dispatcher re-provisions its VM (status 'created', disk cloning ~3 min);
    the reconciler saw paused→reapable + dirty + unreachable + a stale
    snapshot_attempts already at the max → give_up force-deleted it on the very
    first tick → dispatcher re-provisions → create→reap→create against the
    shared VM cluster. The fix (is_reapable False for dispatchable jobs) hands
    the VM to the dispatcher.
    """

    @pytest.mark.asyncio
    async def test_dispatchable_provisioning_vm_survives_tick(self):
        job = {
            "id": "j-churn",
            "status": "paused",
            "unassigned": True,
            "freeze_free": True,
            # snapshot_attempts already exhausted from prior churn cycles — the
            # exact state that made give_up fire on the first tick pre-fix.
            "context": {"vm": {"status": "created", "snapshot_attempts": 5}},
        }
        mgr, provisioner, *_ = _make_manager(job_rows=[job])
        rec = InstanceLifecycleReconciler([mgr])
        report = await rec.tick()
        provisioner.release_vm_captured.assert_not_called()
        assert report["vm"]["reaped"] == 0
        assert report["vm"]["reap_forced"] == 0

    @pytest.mark.asyncio
    async def test_non_dispatchable_paused_vm_still_reaped(self, monkeypatch):
        # Control: a paused job still frozen (freeze_free=False) is genuinely
        # parked, NOT resuming — idle-suspension still reclaims its VM.
        monkeypatch.setenv("WORKSPACE_SNAPSHOT_MAX_ATTEMPTS", "5")
        job = {
            "id": "j-idle",
            "status": "paused",
            # Paused long past the warm grace → genuinely parked.
            "updated_at": datetime.now(timezone.utc) - timedelta(hours=2),
            "unassigned": True,
            "freeze_free": False,
            "context": {
                "vm": {
                    "status": "ready",
                    "ssh_host": "10.0.0.1",
                    "snapshot_attempts": 5,
                }
            },
        }
        mgr, provisioner, *_ = _make_manager(job_rows=[job])
        mgr._tcp_probe = AsyncMock(return_value=False)  # unreachable, no real socket
        rec = InstanceLifecycleReconciler([mgr])
        report = await rec.tick()
        # Reached via give_up (dirty + unreachable + attempts exhausted), which
        # keeps the rootdisk — it is the only surviving copy of the state the
        # snapshot could not capture.
        provisioner.release_vm_captured.assert_awaited_once_with(
            "j-idle",
            provisioner.capture_vm_teardown_identity.return_value,
            purge_disk=False,
            entity_type="job",
            capture_snapshot=False,
        )
        assert report["vm"]["reap_forced"] == 1


# =============================================================================
# reap_orphans() — backstop sweep for VMs whose owning row was deleted
# =============================================================================


class TestReapOrphans:
    """A VM whose jobs/threads row is gone never surfaces as an Instance
    (list_instances reads FROM the rows), so only the backend inventory
    (provisioner.list_vms) can find it. The sweep reaps it age-gated; a row
    of any status, a DB error, an unknown age, or a non-UUID name spares it.
    See knowledge-history/done/deleted_job_orphans_workspace_pod.md."""

    ORPHAN_ID = "deadbeef-dead-4bad-8bad-feedfacef00d"

    @classmethod
    def _vm(
        cls,
        entity_id=None,
        age_hours: float = 2.0,
        created_at="unset",
        *,
        with_identity: bool = True,
    ):
        if created_at == "unset":
            created_at = (
                datetime.now(timezone.utc) - timedelta(hours=age_hours)
            ).isoformat()
        vm = {
            "vm_name": f"agent-vm-{entity_id or cls.ORPHAN_ID}",
            "entity_id": entity_id or cls.ORPHAN_ID,
            "created_at": created_at,
            "phase": "Running",
        }
        if with_identity:
            vm.update(
                {
                    "provision_generation": ("00000000-0000-4000-8000-000000000001"),
                    "vm_uid": "vm-uid-1",
                    "rootdisk_pvc_uid": "rootdisk-uid-1",
                }
            )
        return vm

    @staticmethod
    def _conn(db):
        return db.acquire.return_value.__aenter__.return_value

    @pytest.mark.asyncio
    async def test_reaps_aged_orphan(self, monkeypatch):
        monkeypatch.setenv("WORKSPACE_ORPHAN_GRACE_SECONDS", "900")
        mgr, provisioner, _, _, db = _make_manager()
        provisioner.list_vms = AsyncMock(return_value=[self._vm()])
        self._conn(db).fetchval = AsyncMock(return_value=False)  # no row anywhere
        assert await mgr.reap_orphans() == 1
        provisioner.delete_orphan_vm_captured.assert_awaited_once()
        args = provisioner.delete_orphan_vm_captured.await_args
        assert args.args[0] == self.ORPHAN_ID
        assert args.args[1].provision_generation == (
            "00000000-0000-4000-8000-000000000001"
        )
        assert args.args[1].vm_uid == "vm-uid-1"
        assert args.args[1].rootdisk_pvc_uid == "rootdisk-uid-1"
        assert args.kwargs == {"purge_disk": True}

    @pytest.mark.asyncio
    async def test_orphan_without_authenticated_inventory_is_preserved(
        self, monkeypatch
    ):
        monkeypatch.setenv("WORKSPACE_ORPHAN_GRACE_SECONDS", "900")
        mgr, provisioner, _, _, db = _make_manager()
        provisioner.list_vms = AsyncMock(return_value=[self._vm(with_identity=False)])
        self._conn(db).fetchval = AsyncMock(return_value=False)

        assert await mgr.reap_orphans() == 0

        provisioner.delete_orphan_vm_captured.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_spares_young_orphan(self, monkeypatch):
        # Never reap an in-flight provision whose row hasn't landed yet.
        monkeypatch.setenv("WORKSPACE_ORPHAN_GRACE_SECONDS", "900")
        mgr, provisioner, _, _, db = _make_manager()
        provisioner.list_vms = AsyncMock(return_value=[self._vm(age_hours=0.01)])
        assert await mgr.reap_orphans() == 0
        provisioner.delete_vm.assert_not_called()
        # Age gate fires before the orphan decision's DB work. (db.acquire is
        # no longer untouched: the kept-disk sweep shares this tick.)
        self._conn(db).fetchval.assert_not_called()

    @pytest.mark.asyncio
    async def test_spares_vm_with_live_row(self, monkeypatch):
        # A row of ANY status → instance path / dispatcher owns the VM.
        monkeypatch.setenv("WORKSPACE_ORPHAN_GRACE_SECONDS", "900")
        mgr, provisioner, _, _, db = _make_manager()
        provisioner.list_vms = AsyncMock(return_value=[self._vm()])
        self._conn(db).fetchval = AsyncMock(return_value=True)
        assert await mgr.reap_orphans() == 0
        provisioner.delete_vm.assert_not_called()

    @pytest.mark.asyncio
    async def test_unavailable_inventory_is_not_empty(self):
        # None = "unknown" (old controller / docker pool) — never reap on it.
        mgr, provisioner, _, _, db = _make_manager()
        provisioner.list_vms = AsyncMock(return_value=None)
        assert await mgr.reap_orphans() == 0
        provisioner.delete_vm.assert_not_called()

    @pytest.mark.asyncio
    async def test_list_error_reaps_nothing(self):
        mgr, provisioner, *_ = _make_manager()
        provisioner.list_vms = AsyncMock(side_effect=RuntimeError("nats down"))
        assert await mgr.reap_orphans() == 0
        provisioner.delete_vm.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_non_uuid_names(self, monkeypatch):
        # Not one of ours (whatever it is) — never touch it.
        monkeypatch.setenv("WORKSPACE_ORPHAN_GRACE_SECONDS", "900")
        mgr, provisioner, _, _, db = _make_manager()
        provisioner.list_vms = AsyncMock(
            return_value=[self._vm(entity_id="golden-abc123")]
        )
        assert await mgr.reap_orphans() == 0
        provisioner.delete_vm.assert_not_called()

    @pytest.mark.asyncio
    async def test_db_error_spares_the_vm(self, monkeypatch):
        # Unknown ≠ gone — mirrors the workspace sweep's stance.
        monkeypatch.setenv("WORKSPACE_ORPHAN_GRACE_SECONDS", "900")
        mgr, provisioner, _, _, db = _make_manager()
        provisioner.list_vms = AsyncMock(return_value=[self._vm()])
        db.acquire = MagicMock(side_effect=RuntimeError("db down"))
        assert await mgr.reap_orphans() == 0
        provisioner.delete_vm.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_age_spares_the_vm(self, monkeypatch):
        monkeypatch.setenv("WORKSPACE_ORPHAN_GRACE_SECONDS", "900")
        mgr, provisioner, *_ = _make_manager()
        provisioner.list_vms = AsyncMock(return_value=[self._vm(created_at=None)])
        assert await mgr.reap_orphans() == 0
        provisioner.delete_vm.assert_not_called()

    @pytest.mark.asyncio
    async def test_noop_when_provisioner_unavailable(self):
        mgr, provisioner, *_ = _make_manager(is_available=False)
        provisioner.list_vms = AsyncMock(return_value=[self._vm()])
        assert await mgr.reap_orphans() == 0
        provisioner.delete_vm.assert_not_called()

    @pytest.mark.asyncio
    async def test_reconciler_tick_runs_the_sweep(self, monkeypatch):
        # End-to-end: the reconciler's optional-hook path picks up
        # reap_orphans and reports the count.
        monkeypatch.delenv("DEFAULT_VM_IMAGE", raising=False)
        monkeypatch.setenv("WORKSPACE_ORPHAN_GRACE_SECONDS", "900")
        mgr, provisioner, _, _, db = _make_manager()  # no rows → no instances
        provisioner.list_vms = AsyncMock(return_value=[self._vm()])
        self._conn(db).fetchval = AsyncMock(return_value=False)
        rec = InstanceLifecycleReconciler([mgr])
        report = await rec.tick()
        assert report["vm"]["orphans_reaped"] == 1
        provisioner.delete_orphan_vm_captured.assert_awaited_once()


class TestKeptDiskSweep:
    """Layer 2 of the rootdisk GC (D4): a job that was crash-recovered and then
    went terminal without a live VM still holds 20 Gi. The terminal delete
    covers the normal path; this covers the one where no delete ever ran.

    Jobs ONLY. A thread's terminal status is 'ended', which is also exactly the
    state a suspended-but-resumable session sits in — sweeping those would
    delete the disk of every idle session a user meant to come back to.
    """

    def _mgr_with_kept(self, rows):
        mgr, provisioner, _, _, db = _make_manager()
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=rows)
        db.acquire = MagicMock()
        db.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
        db.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        db.merge_vm_context = AsyncMock(return_value=True)
        provisioner.list_vms = AsyncMock(return_value=[])
        return mgr, provisioner, db

    @pytest.mark.asyncio
    async def test_terminal_job_kept_disk_is_purged(self):
        mgr, provisioner, db = self._mgr_with_kept([{"id": "job-1"}])

        purged = await mgr.purge_kept_disks()

        assert purged == 1
        provisioner.release_vm_captured.assert_awaited_once_with(
            "job-1",
            provisioner.capture_vm_teardown_identity.return_value,
            purge_disk=True,
            entity_type="job",
            capture_snapshot=False,
        )

    @pytest.mark.asyncio
    async def test_recovery_hold_blocks_kept_disk_external_prune_boundary(self):
        mgr, provisioner, _ = self._mgr_with_kept([{"id": "job-1"}])
        recovery_store = MagicMock()
        recovery_store.acquire_cleanup_permit = AsyncMock(
            return_value=SimpleNamespace(
                allowed=False, reason="workspace_recovery_unresolved"
            )
        )
        mgr._workspace_recovery_store = recovery_store

        assert await mgr.purge_kept_disks() == 0

        recovery_store.acquire_cleanup_permit.assert_awaited_once()
        provisioner.release_vm_captured.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_completed_cleanup_replay_finishes_later_state_write(self):
        mgr, provisioner, db = self._mgr_with_kept([{"id": "job-1"}])
        recovery_store = MagicMock()
        recovery_store.acquire_cleanup_permit = AsyncMock(
            return_value=SimpleNamespace(
                allowed=True,
                admission_id="cleanup-1",
                completed_outcome="completed",
                reason="cleanup_request_already_completed",
            )
        )
        recovery_store.complete_cleanup_permit = AsyncMock()
        mgr._workspace_recovery_store = recovery_store

        assert await mgr.purge_kept_disks() == 1

        recovery_store.acquire_cleanup_permit.assert_awaited_once()
        provisioner.release_vm_captured.assert_not_awaited()
        db.merge_vm_context.assert_awaited_once_with("job-1", {"rootdisk": None})
        recovery_store.complete_cleanup_permit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_control_marker_blocks_kept_disk_destructive_recheck(self):
        job = {
            "id": "job-1",
            "status": "completed",
            "execution_lane": "pinned",
            "context": {"vm": {"rootdisk": "kept"}},
        }
        mgr, provisioner, _, _, db = _make_manager(
            job_rows=[job],
            completion_commands_enabled=True,
            completion_control_active=True,
        )
        conn = db.acquire.return_value.__aenter__.return_value
        conn.fetch.side_effect = None
        conn.fetch.return_value = [{**job, "vm_context": job["context"]["vm"]}]

        assert await mgr.purge_kept_disks() == 0

        provisioner.capture_vm_teardown_identity.assert_not_awaited()
        provisioner.release_vm_captured.assert_not_awaited()
        db.merge_vm_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_purge_clears_the_marker(self):
        """Otherwise the sweep re-purges the same job on every tick forever."""
        mgr, _, db = self._mgr_with_kept([{"id": "job-1"}])

        await mgr.purge_kept_disks()

        db.merge_vm_context.assert_awaited_with("job-1", {"rootdisk": None})

    @pytest.mark.asyncio
    async def test_marker_survives_a_failed_delete(self):
        """A delete that did not happen must stay on the worklist."""
        mgr, provisioner, db = self._mgr_with_kept([{"id": "job-1"}])
        provisioner.release_vm_captured = AsyncMock(
            return_value=VMTeardownResult("retry_pending", False)
        )

        purged = await mgr.purge_kept_disks()

        assert purged == 0
        db.merge_vm_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_retry_pending_purge_releases_claim_for_next_attempt(self):
        vm_context = {
            "rootdisk": "kept",
            "provision_generation": "00000000-0000-4000-8000-000000000001",
            "vm_uid": "vm-uid-1",
            "rootdisk_pvc_uid": "rootdisk-uid-1",
        }
        job = {
            "id": "job-1",
            "status": "completed",
            "execution_lane": "pinned",
            "context": {"vm": vm_context},
            "vm_context": vm_context,
        }
        mgr, provisioner, _, _, db = _make_manager(
            job_rows=[job], completion_commands_enabled=True
        )
        provisioner.release_vm_captured.return_value = VMTeardownResult(
            "retry_pending", False
        )

        assert await mgr.purge_kept_disks() == 0

        db.merge_vm_context.assert_not_awaited()
        conn = db.acquire.return_value.__aenter__.return_value
        assert any(
            "- '_completion_control_claim'" in str(call.args[0])
            for call in conn.fetchrow.await_args_list
        )

    @pytest.mark.asyncio
    async def test_nothing_to_do_is_silent(self):
        mgr, provisioner, _ = self._mgr_with_kept([])
        assert await mgr.purge_kept_disks() == 0
        provisioner.release_vm_captured.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_exact_stateless_job_is_refused_even_if_reader_returns_it(self):
        # The SQL excludes this row; the application check is the second belt
        # against stale/mocked readers and future query refactors.
        mgr, provisioner, db = self._mgr_with_kept(
            [{"id": "job-stateless", "execution_lane": "stateless"}]
        )

        assert await mgr.purge_kept_disks() == 0
        provisioner.release_vm_captured.assert_not_awaited()
        db.merge_vm_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_db_error_purges_nothing(self):
        """Unknown is not the same as terminal — never delete on a failed read."""
        mgr, provisioner, db = self._mgr_with_kept([])
        db.acquire = MagicMock(side_effect=RuntimeError("db down"))

        assert await mgr.purge_kept_disks() == 0
        provisioner.release_vm_captured.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_orphan_sweep_runs_it(self):
        """It rides the lifecycle tick rather than adding a second scheduler."""
        mgr, provisioner, _ = self._mgr_with_kept([{"id": "job-1"}])

        await mgr.reap_orphans()

        provisioner.release_vm_captured.assert_awaited_once()


@pytest.mark.asyncio
async def test_preparing_dispatchable_job_never_takes_teardown_claim(monkeypatch):
    """An unavailable identity probe must not park unrelated VM preparation."""
    row = {
        "id": "c7d46b72-57f5-4dbd-af5a-255f8f981654",
        "status": "created",
        "execution_lane": "pinned",
        "unassigned": True,
        "freeze_free": True,
        "context": {"vm": {"status": "waiting_preparation"}},
    }
    manager, provisioner, _, _, db = _make_manager(
        job_rows=[row], completion_commands_enabled=True
    )
    provisioner.capture_vm_teardown_identity.side_effect = RuntimeError(
        "probe unavailable"
    )
    monkeypatch.setattr(manager, "reap_orphans", AsyncMock(return_value=0))
    await InstanceLifecycleReconciler([manager]).tick()
    provisioner.capture_vm_teardown_identity.assert_not_awaited()
    provisioner.delete_vm_captured.assert_not_awaited()
    conn = db.acquire.return_value.__aenter__.return_value
    assert not any(
        "UPDATE jobs" in call.args[0] and "_completion_control_claim" in call.args[0]
        for call in conn.fetchrow.await_args_list
    )
