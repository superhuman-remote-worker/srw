"""Reap-branch decision-flow tests for InstanceLifecycleReconciler.tick()."""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services.lifecycle import (
    Instance,
    InstanceLifecycleReconciler,
    ReapableInstanceManager,
)


class _StatefulNonReapable:
    """Shaped like VMInstanceManager: a StatefulInstanceManager (has
    snapshot/restore) but WITHOUT the reap predicates. The deployed
    regression ran _reap on exactly this shape and AttributeError'd every
    tick. A spec'd MagicMock can't catch this — it auto-stubs the missing
    methods — so this regression test uses a real object."""

    kind = "vm"

    def __init__(self, instances):
        self._instances = instances
        self.delete_calls: list[str] = []

    async def list_instances(self):
        return self._instances

    async def expected_versions(self):
        return set()

    async def is_healthy(self, inst):
        return True

    async def is_idle(self, inst):
        return False

    async def signal_drain_pending(self, inst): ...

    async def drain(self, inst, grace_s): ...

    async def delete(self, inst, grace_s):
        self.delete_calls.append(inst.id)

    async def snapshot(self, inst):
        return None

    async def restore(self, inst, snapshot_ref): ...


def _stateful_mgr(
    inst,
    *,
    healthy=True,
    reapable=True,
    dirty=False,
    reachable=False,
    exhausted=False,
    snapshot_ref=None,
):
    mgr = MagicMock(spec=ReapableInstanceManager)
    mgr.kind = "workspace"
    mgr.expected_versions = AsyncMock(return_value=set())
    mgr.list_instances = AsyncMock(return_value=[inst])
    mgr.is_healthy = AsyncMock(return_value=healthy)
    mgr.is_idle = AsyncMock(return_value=False)
    mgr.signal_drain_pending = AsyncMock()
    mgr.is_reapable = AsyncMock(return_value=reapable)
    mgr.is_dirty = AsyncMock(return_value=dirty)
    mgr.is_reachable = AsyncMock(return_value=reachable)
    mgr.attempts_exhausted = AsyncMock(return_value=exhausted)
    mgr.snapshot = AsyncMock(return_value=snapshot_ref)
    mgr.delete = AsyncMock()
    mgr.give_up = AsyncMock()
    mgr.record_attempt = AsyncMock()
    return mgr


def _inst():
    return Instance(kind="workspace", id="ws-1", bound_to="j1")


@pytest.mark.asyncio
async def test_clean_reapable_deletes_without_probe():
    mgr = _stateful_mgr(_inst(), dirty=False)
    rec = InstanceLifecycleReconciler([mgr])
    await rec.tick()
    mgr.delete.assert_awaited_once()
    mgr.is_reachable.assert_not_called()
    mgr.snapshot.assert_not_called()


@pytest.mark.asyncio
async def test_dirty_reachable_snapshots_then_deletes():
    mgr = _stateful_mgr(_inst(), dirty=True, reachable=True, snapshot_ref="j1")
    rec = InstanceLifecycleReconciler([mgr])
    await rec.tick()
    mgr.snapshot.assert_awaited_once()
    mgr.delete.assert_awaited_once()
    mgr.give_up.assert_not_called()


@pytest.mark.asyncio
async def test_dirty_reachable_snapshot_fails_records_attempt():
    mgr = _stateful_mgr(_inst(), dirty=True, reachable=True, snapshot_ref=None)
    rec = InstanceLifecycleReconciler([mgr])
    await rec.tick()
    mgr.snapshot.assert_awaited_once()
    mgr.record_attempt.assert_awaited_once()
    mgr.delete.assert_not_called()


@pytest.mark.asyncio
async def test_dirty_unreachable_not_exhausted_records_attempt():
    mgr = _stateful_mgr(_inst(), dirty=True, reachable=False, exhausted=False)
    rec = InstanceLifecycleReconciler([mgr])
    await rec.tick()
    mgr.record_attempt.assert_awaited_once()
    mgr.give_up.assert_not_called()
    mgr.delete.assert_not_called()


@pytest.mark.asyncio
async def test_dirty_unreachable_exhausted_gives_up():
    mgr = _stateful_mgr(_inst(), dirty=True, reachable=False, exhausted=True)
    rec = InstanceLifecycleReconciler([mgr])
    await rec.tick()
    mgr.give_up.assert_awaited_once()
    mgr.delete.assert_not_called()  # give_up owns the deletion


@pytest.mark.asyncio
async def test_not_reapable_is_untouched():
    mgr = _stateful_mgr(_inst(), reapable=False)
    rec = InstanceLifecycleReconciler([mgr])
    await rec.tick()
    mgr.delete.assert_not_called()
    mgr.give_up.assert_not_called()
    mgr.record_attempt.assert_not_called()


@pytest.mark.asyncio
async def test_unhealthy_still_crash_deletes_before_reap():
    mgr = _stateful_mgr(_inst(), healthy=False)
    rec = InstanceLifecycleReconciler([mgr])
    await rec.tick()
    mgr.delete.assert_awaited_once()  # crash path
    mgr.is_reapable.assert_not_called()


@pytest.mark.asyncio
async def test_stateful_non_reapable_manager_is_skipped(caplog):
    """Regression: a StatefulInstanceManager that is NOT reapable (VM) must
    be skipped by the reap branch, not crash it.

    Pre-fix, _reap gated on isinstance(StatefulInstanceManager), so it called
    is_reapable() on the VM manager and AttributeError'd every tick. The gate
    is now ReapableInstanceManager, which the VM-shaped manager does not
    satisfy."""
    mgr = _StatefulNonReapable([Instance(kind="vm", id="vm-job-1", bound_to="j1")])
    rec = InstanceLifecycleReconciler([mgr])
    with caplog.at_level(logging.ERROR):
        report = await rec.tick()
    # No crash surfaced as a logged "Reap failed" error.
    assert "Reap failed" not in caplog.text
    assert "AttributeError" not in caplog.text
    # The VM manager was left entirely alone by the reap path.
    assert mgr.delete_calls == []
    assert report["vm"]["reaped"] == 0


# =============================================================================
# Drift + idle stateful instances must snapshot before any teardown.
#
# A drifted instance that has gone idle used to take the bare drift-drain path
# (drain() == no-snapshot delete() for workspaces/VMs), losing un-snapshotted
# state. It must instead flow through the snapshot-aware reap path.
# =============================================================================


def _drifted_inst():
    # version != any expected SHA ⇒ is_drift True (expected set is non-empty).
    return Instance(kind="workspace", id="ws-1", version="old", bound_to="j1")


def _drifted_idle_mgr(inst, **kw):
    """Reapable manager whose single instance is drifted AND idle — the
    combination that previously took the no-snapshot bare-drain path."""
    mgr = _stateful_mgr(inst, **kw)
    mgr.is_idle = AsyncMock(return_value=True)
    mgr.expected_versions = AsyncMock(return_value={"new"})
    return mgr


@pytest.mark.asyncio
async def test_drifted_idle_dirty_reachable_snapshots_before_delete():
    """Core data-loss regression: drifted + idle + dirty + reachable reapable
    instance is snapshotted BEFORE delete, via the reap path — never bare-drained."""
    inst = _drifted_inst()
    mgr = _drifted_idle_mgr(inst, dirty=True, reachable=True, snapshot_ref="j1")
    order: list[str] = []
    mgr.snapshot = AsyncMock(side_effect=lambda i: order.append("snapshot") or "j1")
    mgr.delete = AsyncMock(side_effect=lambda i, grace_s: order.append("delete"))
    rec = InstanceLifecycleReconciler([mgr])
    report = await rec.tick()
    assert order == ["snapshot", "delete"]
    mgr.drain.assert_not_called()
    assert report["workspace"]["reaped"] == 1
    assert report["workspace"]["drained"] == 0


@pytest.mark.asyncio
async def test_drifted_idle_clean_reapable_deletes_without_snapshot():
    """Drifted + idle + clean: delete without a wasted snapshot/probe."""
    mgr = _drifted_idle_mgr(_drifted_inst(), dirty=False)
    rec = InstanceLifecycleReconciler([mgr])
    report = await rec.tick()
    mgr.snapshot.assert_not_called()
    mgr.is_reachable.assert_not_called()
    mgr.delete.assert_awaited_once()
    mgr.drain.assert_not_called()
    assert report["workspace"]["reaped"] == 1
    assert report["workspace"]["drained"] == 0


@pytest.mark.asyncio
async def test_drifted_idle_dirty_snapshot_fails_records_attempt_pod_survives():
    """If the snapshot fails, the pod must survive (record_attempt) — never a
    bare-drain delete. Strongest anti-data-loss guarantee."""
    mgr = _drifted_idle_mgr(
        _drifted_inst(), dirty=True, reachable=True, snapshot_ref=None
    )
    rec = InstanceLifecycleReconciler([mgr])
    report = await rec.tick()
    mgr.snapshot.assert_awaited_once()
    mgr.record_attempt.assert_awaited_once()
    mgr.delete.assert_not_called()
    mgr.drain.assert_not_called()
    assert report["workspace"]["reap_attempts"] == 1
    assert report["workspace"]["drained"] == 0


@pytest.mark.asyncio
async def test_drifted_idle_dirty_unreachable_not_exhausted_records_attempt():
    """Drifted + idle + dirty + unreachable + retries left: pod survives."""
    mgr = _drifted_idle_mgr(
        _drifted_inst(), dirty=True, reachable=False, exhausted=False
    )
    rec = InstanceLifecycleReconciler([mgr])
    await rec.tick()
    mgr.record_attempt.assert_awaited_once()
    mgr.give_up.assert_not_called()
    mgr.delete.assert_not_called()
    mgr.drain.assert_not_called()


@pytest.mark.asyncio
async def test_drifted_idle_dirty_unreachable_exhausted_gives_up():
    """Bounded escape hatch still fires under drift (cannot snapshot forever)."""
    mgr = _drifted_idle_mgr(
        _drifted_inst(), dirty=True, reachable=False, exhausted=True
    )
    rec = InstanceLifecycleReconciler([mgr])
    report = await rec.tick()
    mgr.give_up.assert_awaited_once()
    mgr.delete.assert_not_called()  # give_up owns the deletion
    mgr.drain.assert_not_called()
    assert report["workspace"]["reap_forced"] == 1


@pytest.mark.asyncio
async def test_drifted_idle_reapable_does_not_count_against_drain_cap():
    """Reapable teardown uses the uncapped reap path: the disruption cap
    (max(1, n//4)) must never leave a drifted dirty pod bare-drained."""
    insts = [
        Instance(kind="workspace", id=f"ws-{i}", version="old", bound_to=f"j{i}")
        for i in range(8)
    ]
    mgr = MagicMock(spec=ReapableInstanceManager)
    mgr.kind = "workspace"
    mgr.expected_versions = AsyncMock(return_value={"new"})  # all 8 drift
    mgr.list_instances = AsyncMock(return_value=insts)
    mgr.is_healthy = AsyncMock(return_value=True)
    mgr.is_idle = AsyncMock(return_value=True)
    mgr.signal_drain_pending = AsyncMock()
    mgr.is_reapable = AsyncMock(return_value=True)
    mgr.is_dirty = AsyncMock(return_value=False)  # clean → straight delete
    mgr.is_reachable = AsyncMock(return_value=False)
    mgr.attempts_exhausted = AsyncMock(return_value=False)
    mgr.snapshot = AsyncMock(return_value=None)
    mgr.delete = AsyncMock()
    mgr.give_up = AsyncMock()
    mgr.record_attempt = AsyncMock()
    rec = InstanceLifecycleReconciler([mgr])
    report = await rec.tick()
    assert mgr.delete.await_count == 8  # cap=2, but reap is uncapped → all 8
    assert report["workspace"]["reaped"] == 8
    assert report["workspace"]["drained"] == 0
    mgr.drain.assert_not_called()


class _IdleStatefulNonReapable(_StatefulNonReapable):
    """Stateful-but-not-reapable AND drifted + idle — exercises the
    snapshot-then-drain contract arm of the drift branch (no real manager
    ships this shape today, but StatefulInstanceManager promises it)."""

    def __init__(self, instances):
        super().__init__(instances)
        self.order: list[str] = []

    async def expected_versions(self):
        return {"new"}

    async def is_idle(self, inst):
        return True

    async def snapshot(self, inst):
        self.order.append("snapshot")
        return "snap-ref"

    async def drain(self, inst, grace_s):
        self.order.append("drain")


@pytest.mark.asyncio
async def test_stateful_non_reapable_drifted_idle_snapshots_then_drains():
    """A StatefulInstanceManager that is NOT reapable still must snapshot
    before the destructive drain on a drift teardown."""
    mgr = _IdleStatefulNonReapable(
        [Instance(kind="vm", id="vm-1", version="old", bound_to="j1")]
    )
    rec = InstanceLifecycleReconciler([mgr])
    report = await rec.tick()
    assert mgr.order == ["snapshot", "drain"]
    assert report["vm"]["drained"] == 1
    assert report["vm"]["reaped"] == 0


# =============================================================================
# Once-per-tick orphan sweep hook (Branch a: backstop PVC GC)
# =============================================================================


@pytest.mark.asyncio
async def test_reap_orphans_hook_called_once_and_recorded():
    """A manager exposing reap_orphans() is swept once per tick; its count
    lands in the per-kind stats."""
    mgr = _stateful_mgr(_inst(), dirty=False)
    mgr.reap_orphans = AsyncMock(return_value=2)
    rec = InstanceLifecycleReconciler([mgr])
    report = await rec.tick()
    mgr.reap_orphans.assert_awaited_once()
    assert report["workspace"]["orphans_reaped"] == 2


@pytest.mark.asyncio
async def test_reap_orphans_absent_is_skipped():
    """A manager without the optional method must not break the tick."""
    mgr = _stateful_mgr(_inst(), dirty=False)
    assert not hasattr(mgr, "reap_orphans")  # spec=ReapableInstanceManager
    rec = InstanceLifecycleReconciler([mgr])
    report = await rec.tick()
    assert report["workspace"]["orphans_reaped"] == 0


@pytest.mark.asyncio
async def test_reap_orphans_failure_does_not_break_tick():
    """A raising orphan sweep is swallowed; the rest of the tick still reports."""
    mgr = _stateful_mgr(_inst(), dirty=False)
    mgr.reap_orphans = AsyncMock(side_effect=RuntimeError("boom"))
    rec = InstanceLifecycleReconciler([mgr])
    report = await rec.tick()
    assert report["workspace"]["orphans_reaped"] == 0
    assert report["workspace"]["reaped"] == 1  # clean reapable still torn down


@pytest.mark.asyncio
async def test_give_up_that_deleted_nothing_is_not_reported_as_force_deleted(caplog):
    """Dev logged "force-deleted … state not captured" every 2 h for a Pod it
    never deleted (automatic cleanup admission refused it)."""

    mgr = _stateful_mgr(_inst(), dirty=True, reachable=False, exhausted=True)
    mgr.give_up = AsyncMock(return_value=False)
    rec = InstanceLifecycleReconciler([mgr])
    with caplog.at_level("WARNING"):
        report = await rec.tick()
    mgr.give_up.assert_awaited_once()
    assert report["workspace"].get("reap_forced", 0) == 0
    assert "force-deleted" not in caplog.text
    assert "deleted nothing" in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize("result", [True, None], ids=["deleted", "legacy"])
async def test_give_up_that_deleted_is_still_reported(caplog, result):
    mgr = _stateful_mgr(_inst(), dirty=True, reachable=False, exhausted=True)
    mgr.give_up = AsyncMock(return_value=result)
    rec = InstanceLifecycleReconciler([mgr])
    with caplog.at_level("WARNING"):
        report = await rec.tick()
    assert report["workspace"]["reap_forced"] == 1
    assert "force-deleted" in caplog.text
