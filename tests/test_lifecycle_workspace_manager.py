"""Tests for WorkspaceInstanceManager (Phase 2a).

Covers list_instances (job + thread bound), drift, idle predicates,
snapshot/restore delegation, drain → delete, and dispatch between
job and thread workspace deletion.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.services.lifecycle import (
    Instance,
    InstanceLifecycleReconciler,
    StatefulInstanceManager,
    WorkspaceInstanceManager,
    expected_workspace_shas,
)
from orchestrator.services.container_provisioner import (
    WorkspaceCleanupOutcome,
    WorkspaceRuntimeAttestation,
    WorkspaceTeardownIdentity,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner


# =============================================================================
# Helpers
# =============================================================================


def _make_pod(name: str, labels: dict | None = None, phase: str = "Running"):
    pod = MagicMock()
    pod.metadata.name = name
    pod.metadata.labels = labels or {}
    pod.metadata.uid = "11111111-1111-4111-8111-111111111111"
    pod.status.phase = phase
    return pod


def _make_manager(
    pods: list | None = None,
    job_rows: dict | None = None,
    thread_rows: dict | None = None,
    k8s_available: bool = True,
    snapshot_available: bool = True,
    suspension_enabled: bool = True,
    shared_child_exists: bool = False,
    completion_commands_enabled: bool = False,
    completion_command_exists: bool = False,
    completion_control_active: bool = False,
):
    """Build a WorkspaceInstanceManager wrapping mocked dependencies.

    ``shared_child_exists`` backs the ``_live_shared_child_exists`` EXISTS
    query (``conn.fetchval``) used by the durable live-child reap guard.
    """
    container = MagicMock()
    container._k8s_available = k8s_available
    container._namespace = "test-ns"
    container._core_api = MagicMock()
    pod_list = MagicMock()
    pod_list.items = pods or []
    container._core_api.list_namespaced_pod.return_value = pod_list
    container.delete_workspace = AsyncMock(return_value=False)
    container.delete_workspace_with_outcome = AsyncMock()

    async def prepare_cleanup(_owner, **kwargs):
        return {
            "intent_generation": 7,
            "resources_captured_at": "2026-08-27T00:00:00+00:00",
            "reclaim_shared_resources": kwargs["reclaim_shared_resources"],
        }

    container.prepare_workspace_cleanup_intent = AsyncMock(side_effect=prepare_cleanup)
    container.reconcile_workspace_cleanup_intent = AsyncMock(
        return_value=WorkspaceCleanupOutcome("settled", 7)
    )
    container.delete_workspace_pvc = AsyncMock(return_value=True)
    container._delete_service = AsyncMock(return_value=True)
    container.capture_workspace_teardown_identity = AsyncMock(
        return_value=WorkspaceTeardownIdentity(
            pod_uid="11111111-1111-4111-8111-111111111111",
            pvc_uid="22222222-2222-4222-8222-222222222222",
            service_uid="33333333-3333-4333-8333-333333333333",
        )
    )
    container.capture_terminal_workspace_identity = AsyncMock(
        return_value=WorkspaceTeardownIdentity(
            pod_uid="11111111-1111-4111-8111-111111111111",
            pvc_uid="22222222-2222-4222-8222-222222222222",
            service_uid="33333333-3333-4333-8333-333333333333",
            pod_ip="10.0.0.7",
            ssh_host_key_fingerprint="SHA256:lifecycle-test",
        )
    )
    container.attest_workspace_runtime = AsyncMock(
        return_value=WorkspaceRuntimeAttestation(
            backing_id="k8s-pod:test-ns:11111111-1111-4111-8111-111111111111",
            workspace_generation="44444444-4444-4444-8444-444444444444",
            runtime_incarnation="11111111-1111-4111-8111-111111111111",
            ssh_host_key_fingerprint="SHA256:lifecycle-test",
            host="agent-workspace-j1.test-ns.svc",
            pod_ip="10.0.0.7",
        )
    )

    suspension = MagicMock()
    suspension.is_enabled = suspension_enabled
    suspension.restore_workspace = AsyncMock(return_value=True)
    suspension.restore_thread_workspace = AsyncMock(return_value=True)

    snapshot = MagicMock()
    snapshot.is_available = snapshot_available
    snapshot.capture_vm_snapshot = AsyncMock(return_value=True)

    db = AsyncMock()
    db.get_job = AsyncMock(side_effect=lambda jid: (job_rows or {}).get(jid))
    db.merge_workspace_container_context_if_runtime = AsyncMock(return_value=True)
    db.merge_thread_workspace_context_if_runtime = AsyncMock(return_value=True)
    db.acquire = MagicMock()
    conn = AsyncMock()

    async def _fetchrow(sql, *args):
        identity = str(args[0]) if args else ""
        if "UPDATE jobs" in sql and "RETURNING" in sql:
            return {"id": identity, "context": {}}
        if "job_completion_sweep_exclusions" in sql:
            return (
                {
                    "command_id": "44444444-4444-4444-8444-444444444444",
                    "route": "stand_down",
                }
                if completion_command_exists
                else None
            )
        if "FROM jobs" in sql:
            row = (job_rows or {}).get(identity)
            if row is None:
                return None
            return {
                "status": row.get("status"),
                "execution_lane": row.get("execution_lane") or "pinned",
                "context": row.get("context") or {},
                "control_active": completion_control_active,
            }
        return (thread_rows or {}).get(identity)

    conn.fetchrow = AsyncMock(side_effect=_fetchrow)
    tx = AsyncMock()
    tx.__aenter__ = AsyncMock(return_value=None)
    tx.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=tx)

    async def _fetchval(sql, *_args):
        if "_completion_control_claim" in sql:
            return completion_control_active
        if "job_completion_commands" in sql:
            return completion_command_exists
        return shared_child_exists

    conn.fetchval = AsyncMock(side_effect=_fetchval)
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    db.acquire.return_value = ctx

    router = MagicMock()
    router.enqueue_job = AsyncMock()
    mgr = WorkspaceInstanceManager(
        container_provisioner=container,
        suspension_service=suspension,
        snapshot_service=snapshot,
        db=db,
        completion_commands_enabled=completion_commands_enabled,
        completion_router=router if completion_commands_enabled else None,
    )
    mgr._test_completion_router = router
    return mgr, container, suspension, snapshot, db


def _assert_exact_workspace_delete(
    container: MagicMock,
    owner: WorkspaceOwner,
    runtime_uid: str = "11111111-1111-4111-8111-111111111111",
) -> None:
    container.prepare_workspace_cleanup_intent.assert_awaited_once()
    prepare = container.prepare_workspace_cleanup_intent.await_args
    assert prepare.args == (owner,)
    assert prepare.kwargs["expected_runtime_incarnation"] == runtime_uid
    assert prepare.kwargs["target_disposition"] in {"deleted", "suspended"}
    assert type(prepare.kwargs["reclaim_shared_resources"]) is bool
    assert "identity" not in prepare.kwargs
    container.reconcile_workspace_cleanup_intent.assert_awaited_once_with(
        owner,
        expected_runtime_incarnation=runtime_uid,
        intent_generation=7,
    )
    container.delete_workspace.assert_not_awaited()
    container.delete_workspace_with_outcome.assert_not_awaited()
    container.capture_workspace_teardown_identity.assert_not_awaited()


async def _fake_to_thread(fn, *args, **kwargs):
    return fn(*args, **kwargs)


# =============================================================================
# expected_workspace_shas
# =============================================================================


class TestExpectedWorkspaceShas:
    def test_returns_empty_when_no_env(self, monkeypatch):
        monkeypatch.delenv("WORKSPACE_IMAGE", raising=False)
        assert expected_workspace_shas() == set()

    def test_returns_empty_when_tag_lacks_sha_prefix(self, monkeypatch):
        monkeypatch.setenv("WORKSPACE_IMAGE", "registry/workspace:latest")
        assert expected_workspace_shas() == set()

    def test_extracts_sha(self, monkeypatch):
        monkeypatch.setenv("WORKSPACE_IMAGE", "registry/workspace:sha-abc1234")
        assert expected_workspace_shas() == {"abc1234"}


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
    async def test_empty_when_k8s_unavailable(self):
        mgr, *_ = _make_manager(k8s_available=False)
        assert await mgr.list_instances() == []

    @pytest.mark.asyncio
    async def test_job_workspace_join(self):
        pod = _make_pod(
            "workspace-abc",
            labels={
                "srw/job-id": "job-uuid-1",
                "srw.io/component": "agent-workspace",
                "srw/build-sha": "sha1",
            },
        )
        job = {
            "id": "job-uuid-1",
            "status": "paused",
            "context": {
                "workspace_container": {
                    "status": "ready",
                    "pod_ip": "10.0.0.7",
                }
            },
        }
        mgr, *_ = _make_manager(pods=[pod], job_rows={"job-uuid-1": job})
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            instances = await mgr.list_instances()
        assert len(instances) == 1
        inst = instances[0]
        assert inst.kind == "workspace"
        assert inst.id == "workspace-abc"
        assert inst.version == "sha1"
        assert inst.bound_to == "job-uuid-1"
        assert inst.metadata["job_status"] == "paused"
        assert inst.metadata["workspace_status"] == "ready"
        assert inst.metadata["pod_ip"] == "10.0.0.7"

    @pytest.mark.asyncio
    async def test_thread_workspace_uses_thread_table(self):
        # Legacy pre-migration pods carried BOTH srw/job-id and srw/thread-id
        # (the old job-id-slot-reuse hack). New pods carry only srw/thread-id,
        # but during a rolling deploy old dual-label pods still exist — the
        # manager must route them to the threads table (via srw/thread-id).
        pod = _make_pod(
            "ws-thread-xyz",
            labels={
                "srw/job-id": "thread-uuid-1",
                "srw/thread-id": "thread-uuid-1",
                "srw/component": "thread-workspace",
                "srw.io/component": "agent-workspace",
                "srw/build-sha": "sha2",
            },
        )
        thread_row = {
            "id": "thread-uuid-1",
            "status": "ended",
            "execution_lane": "pinned",
        }
        mgr, _, _, _, db = _make_manager(
            pods=[pod], thread_rows={"thread-uuid-1": thread_row}
        )
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            instances = await mgr.list_instances()
        assert len(instances) == 1
        inst = instances[0]
        assert inst.bound_to == "thread-uuid-1"
        assert inst.metadata["thread_status"] == "ended"
        assert inst.metadata["execution_lane"] == "pinned"
        # Job table must NOT have been consulted for thread workspaces.
        db.get_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_pod_without_db_row_still_listed(self):
        # A new workspace pod whose context hasn't been written yet still
        # appears in the listing — the manager just won't claim it idle.
        pod = _make_pod(
            "workspace-fresh",
            labels={
                "srw/job-id": "job-fresh",
                "srw.io/component": "agent-workspace",
            },
        )
        mgr, *_ = _make_manager(pods=[pod])
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            instances = await mgr.list_instances()
        assert len(instances) == 1
        assert "job_status" not in instances[0].metadata


# =============================================================================
# Stateless thread lifecycle refusal
# =============================================================================


class TestStatelessThreadLifecycleRefusal:
    """The acknowledged terminal/loss protocol exclusively retires these pods."""

    @staticmethod
    def _pod(phase: str = "Running"):
        return _make_pod(
            "ws-thread-stateless",
            labels={
                "srw/thread-id": "thread-stateless",
                "srw.io/component": "agent-workspace",
            },
            phase=phase,
        )

    @staticmethod
    def _row(*, execution_lane: str = "stateless", turns: int = 0, snapshot_turns=None):
        return {
            "id": "thread-stateless",
            "status": "ended",
            "execution_lane": execution_lane,
            "total_turns": turns,
            "metadata": {
                "workspace_container": {
                    "status": "ready",
                    "pod_ip": "10.0.0.17",
                    "last_snapshot_turns": snapshot_turns,
                }
            },
        }

    @staticmethod
    def _empty_pvc_sweep(container):
        listed = MagicMock()
        listed.items = []
        container._core_api.list_namespaced_persistent_volume_claim.return_value = (
            listed
        )

    @staticmethod
    def _assert_no_effects(container, snapshot, db):
        snapshot.capture_vm_snapshot.assert_not_called()
        container.delete_workspace_with_outcome.assert_not_called()
        container.create_workspace.assert_not_called()
        container.delete_workspace_pvc.assert_not_called()
        container._delete_service.assert_not_called()
        db.merge_thread_workspace_context.assert_not_called()
        db.merge_workspace_container_context.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("turns", "snapshot_turns"),
        [(7, 2), (0, None)],
        ids=["dirty", "clean"],
    )
    async def test_ended_stateless_tick_has_no_reap_effects(
        self, monkeypatch, turns, snapshot_turns
    ):
        monkeypatch.delenv("WORKSPACE_IMAGE", raising=False)
        row = self._row(turns=turns, snapshot_turns=snapshot_turns)
        mgr, container, _, snapshot, db = _make_manager(
            pods=[self._pod()], thread_rows={"thread-stateless": row}
        )
        self._empty_pvc_sweep(container)
        mgr._tcp_probe = AsyncMock(return_value=True)

        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            report = await InstanceLifecycleReconciler([mgr]).tick()

        assert report["workspace"]["listed"] == 1
        assert report["workspace"]["unhealthy"] == 0
        assert report["workspace"]["reaped"] == 0
        assert report["workspace"]["reap_attempts"] == 0
        assert report["workspace"]["reap_forced"] == 0
        self._assert_no_effects(container, snapshot, db)

    @pytest.mark.asyncio
    async def test_unhealthy_stateless_tick_cannot_take_force_delete(self, monkeypatch):
        monkeypatch.delenv("WORKSPACE_IMAGE", raising=False)
        row = self._row(turns=3, snapshot_turns=1)
        mgr, container, _, snapshot, db = _make_manager(
            pods=[self._pod(phase="Failed")],
            thread_rows={"thread-stateless": row},
        )
        self._empty_pvc_sweep(container)

        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            report = await InstanceLifecycleReconciler([mgr]).tick()

        assert report["workspace"]["listed"] == 1
        assert report["workspace"]["unhealthy"] == 0
        self._assert_no_effects(container, snapshot, db)

    @pytest.mark.asyncio
    async def test_direct_legacy_mutators_are_belted(self):
        mgr, container, _, snapshot, db = _make_manager()
        inst = Instance(
            kind="workspace",
            id="ws-thread-stateless",
            bound_to="thread-stateless",
            metadata={
                "labels": {"srw/thread-id": "thread-stateless"},
                "execution_lane": "stateless",
                "thread_status": "ended",
                "pod_phase": "Failed",
                "pod_ip": "10.0.0.17",
                "total_turns": 7,
                "last_snapshot_turns": 2,
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

        self._assert_no_effects(container, snapshot, db)

    @pytest.mark.asyncio
    async def test_pinned_ended_dirty_thread_is_contained_without_capture_or_reap(
        self, monkeypatch
    ):
        monkeypatch.delenv("WORKSPACE_IMAGE", raising=False)
        row = self._row(execution_lane="pinned", turns=7, snapshot_turns=2)
        mgr, container, _, snapshot, _ = _make_manager(
            pods=[self._pod()], thread_rows={"thread-stateless": row}
        )
        self._empty_pvc_sweep(container)
        mgr._tcp_probe = AsyncMock(return_value=True)

        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            report = await InstanceLifecycleReconciler([mgr]).tick()

        assert report["workspace"]["reaped"] == 0
        snapshot.capture_vm_snapshot.assert_not_awaited()
        container.prepare_workspace_cleanup_intent.assert_not_awaited()
        container.delete_workspace_with_outcome.assert_not_awaited()


# =============================================================================
# Live-shared-child reap guard (durable fix)
# =============================================================================


class TestLiveSharedChildGuard:
    @staticmethod
    def _job_pod(job_id: str):
        return _make_pod(
            job_id if job_id.startswith("workspace-") else f"workspace-{job_id}",
            labels={
                "srw/job-id": job_id,
                "srw.io/component": "agent-workspace",
            },
        )

    @staticmethod
    def _conn(db):
        return db.acquire.return_value.__aenter__.return_value

    @pytest.mark.asyncio
    async def test_reviewing_pod_with_shared_child_flagged_not_reapable(self):
        # A 'reviewing' parent whose live pod is shared by a critic: the
        # EXISTS query returns true → has_live_shared_child → not reapable.
        pod = self._job_pod("parent1")
        job = {"id": "parent1", "status": "reviewing", "context": {}}
        mgr, *_ = _make_manager(
            pods=[pod], job_rows={"parent1": job}, shared_child_exists=True
        )
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            (inst,) = await mgr.list_instances()
        assert inst.metadata["has_live_shared_child"] is True
        assert await mgr.is_reapable(inst) is False
        assert await mgr.is_idle(inst) is False

    @pytest.mark.asyncio
    async def test_reviewing_pod_without_shared_child_is_reapable(self):
        pod = self._job_pod("parent1")
        job = {"id": "parent1", "status": "reviewing", "context": {}}
        mgr, *_ = _make_manager(
            pods=[pod], job_rows={"parent1": job}, shared_child_exists=False
        )
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            (inst,) = await mgr.list_instances()
        assert inst.metadata["has_live_shared_child"] is False
        assert await mgr.is_reapable(inst) is True

    @pytest.mark.asyncio
    async def test_processing_pod_skips_shared_child_query(self):
        # Not reapable by status → the guard is moot → don't spend a query.
        pod = self._job_pod("run1")
        job = {"id": "run1", "status": "processing", "context": {}}
        mgr, _, _, _, db = _make_manager(pods=[pod], job_rows={"run1": job})
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            (inst,) = await mgr.list_instances()
        assert "has_live_shared_child" not in inst.metadata
        self._conn(db).fetchval.assert_not_called()

    @pytest.mark.asyncio
    async def test_query_passes_parent_id_and_pod_name(self):
        mgr, _, _, _, db = _make_manager(shared_child_exists=True)
        assert (
            await mgr._live_shared_child_exists("parent1", "workspace-parent1") is True
        )
        conn = self._conn(db)
        conn.fetchval.assert_awaited_once()
        args = conn.fetchval.await_args.args
        assert args[1] == "parent1"
        assert args[2] == "workspace-parent1"

    @pytest.mark.asyncio
    async def test_query_false_without_pod_name(self):
        # No pod name → nothing to match on; don't even hit the DB.
        mgr, _, _, _, db = _make_manager(shared_child_exists=True)
        assert await mgr._live_shared_child_exists("parent1", None) is False
        self._conn(db).fetchval.assert_not_called()

    @pytest.mark.asyncio
    async def test_query_fails_safe_on_db_error(self):
        # A transient DB error must NOT let the reaper delete a maybe-shared pod
        # (mirrors reap_orphans' "db error → don't delete"): assume shared.
        mgr, _, _, _, db = _make_manager()
        db.acquire = MagicMock(side_effect=RuntimeError("db down"))
        assert (
            await mgr._live_shared_child_exists("parent1", "workspace-parent1") is True
        )


# =============================================================================
# is_idle / is_healthy
# =============================================================================


class TestIsIdle:
    @pytest.mark.asyncio
    async def test_paused_job_is_idle_after_grace(self):
        # 'paused' is idle only once the warm grace has passed — a fresh pause
        # is often a human-wait (sudo/VM approval) and must stay warm. See
        # knowledge-base/knowledge/issues/vm_upgrade_pause_workspace_reaped_before_approval.md.
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="workspace",
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
            kind="workspace",
            id="x",
            metadata={
                "job_status": "paused",
                "job_updated_at": datetime.now(timezone.utc),
            },
        )
        assert await mgr.is_idle(inst) is False

    @pytest.mark.asyncio
    async def test_paused_job_with_unknown_age_is_not_idle(self):
        # Conservative: no timestamp → treat as inside the grace (never
        # destroy on missing data).
        mgr, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", metadata={"job_status": "paused"})
        assert await mgr.is_idle(inst) is False

    @pytest.mark.asyncio
    async def test_pending_review_is_idle(self):
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="workspace", id="x", metadata={"job_status": "pending_review"}
        )
        assert await mgr.is_idle(inst) is True

    @pytest.mark.asyncio
    async def test_reviewing_job_without_live_child_is_idle(self):
        # With the durable live-child guard, 'reviewing' is back in the idle
        # set: a review-state parent with NO live critic sharing its pod is
        # quiescent and snapshot+free-able (the pre-bug optimization).
        mgr, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", metadata={"job_status": "reviewing"})
        assert await mgr.is_idle(inst) is True

    @pytest.mark.asyncio
    async def test_status_with_live_shared_child_is_not_idle(self):
        # The guard: an otherwise-idle parent (here 'paused') whose live pod is
        # shared by a non-terminal child (critic) must NOT be drained — doing so
        # kills the child's SSH. Keyed on the real dependency, not job status.
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="workspace",
            id="x",
            metadata={"job_status": "paused", "has_live_shared_child": True},
        )
        assert await mgr.is_idle(inst) is False

    @pytest.mark.asyncio
    async def test_processing_is_not_idle(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", metadata={"job_status": "processing"})
        assert await mgr.is_idle(inst) is False

    @pytest.mark.asyncio
    async def test_ended_thread_is_idle(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", metadata={"thread_status": "ended"})
        assert await mgr.is_idle(inst) is True

    @pytest.mark.asyncio
    async def test_active_thread_is_not_idle(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", metadata={"thread_status": "active"})
        assert await mgr.is_idle(inst) is False

    @pytest.mark.asyncio
    async def test_no_status_is_not_idle(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", metadata={})
        assert await mgr.is_idle(inst) is False


class TestIsHealthy:
    @pytest.mark.asyncio
    async def test_running_pod_is_healthy(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", metadata={"pod_phase": "Running"})
        assert await mgr.is_healthy(inst) is True

    @pytest.mark.asyncio
    async def test_failed_pod_is_unhealthy(self):
        # Phase 2b will use this signal to trigger the crash detector.
        mgr, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", metadata={"pod_phase": "Failed"})
        assert await mgr.is_healthy(inst) is False


class TestCompletionFinalizerTeardownOwnership:
    """Gate-3 S36 and the generic lifecycle reaper must never act in parallel."""

    @pytest.mark.asyncio
    async def test_flag_off_preserves_terminal_reap_without_command_table_read(self):
        pod = _make_pod(
            "workspace-jdone",
            labels={"srw/job-id": "jdone"},
            phase="Failed",
        )
        mgr, _, _, _, db = _make_manager(
            pods=[pod],
            job_rows={"jdone": {"status": "completed", "context": {}}},
        )

        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            [inst] = await mgr.list_instances()

        assert "completion_finalization_owned" not in inst.metadata
        assert "completion_control_owned" not in inst.metadata
        assert "completion_lifecycle_disposition" not in inst.metadata
        assert "execution_lane" not in inst.metadata
        assert inst.metadata["pod_uid"] == "11111111-1111-4111-8111-111111111111"
        assert await mgr.is_healthy(inst) is False
        assert await mgr.is_reapable(inst) is True
        conn = db.acquire.return_value.__aenter__.return_value
        assert all(
            "job_completion_commands" not in str(call.args[0])
            for call in conn.fetchval.await_args_list
        )
        assert all(
            "_completion_control_claim" not in str(call.args[0])
            for call in conn.fetchval.await_args_list
        )

    @pytest.mark.asyncio
    async def test_unfinished_command_blocks_failed_pod_and_terminal_reap(self):
        pod = _make_pod(
            "workspace-jdone",
            labels={"srw/job-id": "jdone"},
            phase="Failed",
        )
        mgr, *_ = _make_manager(
            pods=[pod],
            job_rows={"jdone": {"status": "completed", "context": {}}},
            completion_commands_enabled=True,
            completion_command_exists=True,
        )

        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            [inst] = await mgr.list_instances()

        assert inst.metadata["completion_finalization_owned"] is True
        assert await mgr.is_healthy(inst) is True
        assert await mgr.is_idle(inst) is False
        assert await mgr.is_reapable(inst) is False

    @pytest.mark.asyncio
    async def test_done_or_absent_command_keeps_legacy_terminal_classification(self):
        pod = _make_pod(
            "workspace-jdone",
            labels={"srw/job-id": "jdone"},
            phase="Failed",
        )
        mgr, *_ = _make_manager(
            pods=[pod],
            job_rows={"jdone": {"status": "completed", "context": {}}},
            completion_commands_enabled=True,
            completion_command_exists=False,
        )

        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            [inst] = await mgr.list_instances()

        assert inst.metadata["completion_finalization_owned"] is False
        assert await mgr.is_healthy(inst) is False
        assert await mgr.is_reapable(inst) is True

    @pytest.mark.asyncio
    async def test_command_lookup_error_fails_closed_only_when_flag_on(self):
        pod = _make_pod("workspace-jdone", labels={"srw/job-id": "jdone"})
        mgr, _, _, _, db = _make_manager(
            pods=[pod],
            job_rows={"jdone": {"status": "completed", "context": {}}},
            completion_commands_enabled=True,
        )
        conn = db.acquire.return_value.__aenter__.return_value

        base_fetchrow = conn.fetchrow.side_effect

        async def _fetchrow(sql, *args):
            if "job_completion_sweep_exclusions" in sql:
                raise RuntimeError("completion relation unavailable")
            return await base_fetchrow(sql, *args)

        conn.fetchrow.side_effect = _fetchrow

        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            [inst] = await mgr.list_instances()

        assert inst.metadata["completion_lifecycle_disposition"] == "unknown"
        assert inst.metadata["completion_lifecycle_deferred"] is True
        assert await mgr.is_reapable(inst) is False

    @pytest.mark.asyncio
    async def test_action_time_recheck_blocks_snapshot_and_delete_after_list(self):
        pod = _make_pod("workspace-jdone", labels={"srw/job-id": "jdone"})
        job = {
            "status": "completed",
            "context": {"workspace_container": {"pod_ip": "10.0.0.7"}},
        }
        mgr, container, _, snapshot, db = _make_manager(
            pods=[pod],
            job_rows={"jdone": job},
            completion_commands_enabled=True,
        )
        command = {"route": None}
        conn = db.acquire.return_value.__aenter__.return_value
        base_fetchrow = conn.fetchrow.side_effect

        async def _fetchrow(sql, *args):
            if "job_completion_sweep_exclusions" in sql and command["route"]:
                return {
                    "command_id": "44444444-4444-4444-8444-444444444444",
                    "route": command["route"],
                }
            return await base_fetchrow(sql, *args)

        conn.fetchrow.side_effect = _fetchrow

        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            [inst] = await mgr.list_instances()
        assert inst.metadata["completion_finalization_owned"] is False

        command["route"] = "resume_finalizer"
        assert await mgr.snapshot(inst) is None
        await mgr.delete(inst, grace_s=0)
        container.create_workspace = AsyncMock(return_value=True)
        give_up_inst = Instance(
            kind="workspace",
            id="workspace-jdone",
            bound_to="jdone",
            metadata={
                "labels": {"srw/job-id": "jdone"},
                "job_status": "paused",
                "volume_ephemeral": False,
            },
        )
        await mgr.give_up(give_up_inst, grace_s=0)

        snapshot.capture_vm_snapshot.assert_not_awaited()
        container.delete_workspace_with_outcome.assert_not_awaited()
        container.create_workspace.assert_not_awaited()
        route_queries = [
            str(call.args[0])
            for call in conn.fetchrow.await_args_list
            if "job_completion_sweep_exclusions" in str(call.args[0])
        ]
        # list + snapshot + delete. give_up's fixture has no jobs row and is
        # therefore a command-ineligible legacy orphan without a route read.
        assert len(route_queries) == 3
        assert mgr._test_completion_router.enqueue_job.await_count == 2


class TestCompletionControlLifecycleOwnership:
    """A claimed human control and the generic reaper never share a resource."""

    @pytest.mark.asyncio
    async def test_processing_job_reap_does_not_acquire_a_control_claim(
        self, monkeypatch
    ):
        """A busy workspace must not acquire teardown authority or block recovery."""

        monkeypatch.delenv("WORKSPACE_IMAGE", raising=False)
        pod = _make_pod(
            "workspace-j1",
            labels={"srw/job-id": "j1", "srw.io/component": "agent-workspace"},
        )
        mgr, container, _, snapshot, db = _make_manager(
            pods=[pod],
            job_rows={
                "j1": {
                    "status": "processing",
                    "execution_lane": "pinned",
                    "context": {
                        "workspace_container": {
                            "status": "ready",
                            "pod_ip": "10.0.0.7",
                        }
                    },
                }
            },
            completion_commands_enabled=True,
        )
        listed = MagicMock()
        listed.items = []
        container._core_api.list_namespaced_persistent_volume_claim.return_value = (
            listed
        )

        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            report = await InstanceLifecycleReconciler([mgr]).tick()

        assert report["workspace"]["reaped"] == 0
        snapshot.capture_vm_snapshot.assert_not_awaited()
        container.delete_workspace_with_outcome.assert_not_awaited()
        conn = db.acquire.return_value.__aenter__.return_value
        assert not any(
            "UPDATE jobs" in str(call.args[0])
            and "_completion_control_claim" in str(call.args[0])
            for call in conn.fetchrow.await_args_list
        )

    @pytest.mark.asyncio
    async def test_live_or_malformed_marker_preserves_failed_terminal_workspace(self):
        pod = _make_pod(
            "workspace-jdone",
            labels={"srw/job-id": "jdone"},
            phase="Failed",
        )
        mgr, _, _, _, db = _make_manager(
            pods=[pod],
            job_rows={
                "jdone": {
                    "status": "completed",
                    "context": {"_completion_control_claim": []},
                }
            },
            completion_commands_enabled=True,
            completion_control_active=True,
        )

        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
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
    async def test_action_time_recheck_blocks_snapshot_delete_and_give_up(self):
        mgr, container, _, snapshot, db = _make_manager(
            job_rows={"j1": {"status": "paused", "context": {}}},
            completion_commands_enabled=True,
            completion_control_active=True,
        )
        container.create_workspace = AsyncMock(return_value=True)
        inst = Instance(
            kind="workspace",
            id="workspace-j1",
            bound_to="j1",
            metadata={
                "labels": {"srw/job-id": "j1"},
                "job_status": "paused",
                "pod_ip": "10.0.0.7",
                "volume_ephemeral": False,
            },
        )

        assert await mgr.snapshot(inst) is None
        await mgr.delete(inst, grace_s=0)
        await mgr.give_up(inst, grace_s=0)

        snapshot.capture_vm_snapshot.assert_not_awaited()
        container.delete_workspace_with_outcome.assert_not_awaited()
        container.create_workspace.assert_not_awaited()
        conn = db.acquire.return_value.__aenter__.return_value
        marker_queries = [
            call
            for call in conn.fetchrow.await_args_list
            if "FROM jobs" in str(call.args[0]) and "FOR UPDATE" in str(call.args[0])
        ]
        assert len(marker_queries) == 3

    @pytest.mark.asyncio
    async def test_marker_lookup_error_fails_closed(self):
        pod = _make_pod("workspace-jdone", labels={"srw/job-id": "jdone"})
        mgr, _, _, _, db = _make_manager(
            pods=[pod],
            job_rows={"jdone": {"status": "completed", "context": {}}},
            completion_commands_enabled=True,
        )
        conn = db.acquire.return_value.__aenter__.return_value

        base_fetchrow = conn.fetchrow.side_effect

        async def _fetchrow(sql, *args):
            if "FROM jobs" in sql and "FOR UPDATE" in sql:
                raise RuntimeError("control lookup unavailable")
            return await base_fetchrow(sql, *args)

        conn.fetchrow.side_effect = _fetchrow
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            [inst] = await mgr.list_instances()

        assert inst.metadata["completion_lifecycle_disposition"] == "unknown"
        assert await mgr.is_reapable(inst) is False

    @pytest.mark.asyncio
    async def test_dirty_flip_requires_terminal_snapshot_attestation(self, monkeypatch):
        monkeypatch.delenv("WORKSPACE_IMAGE", raising=False)
        pod = _make_pod("workspace-j1", labels={"srw/job-id": "j1"})
        mgr, container, _, snapshot, db = _make_manager(
            pods=[pod],
            job_rows={
                "j1": {
                    "status": "completed",
                    "execution_lane": "pinned",
                    "context": {"workspace_container": {"pod_ip": "10.0.0.7"}},
                }
            },
            completion_commands_enabled=True,
        )
        mgr.is_dirty = AsyncMock(side_effect=[False, True])
        mgr.is_reachable = AsyncMock(return_value=True)
        mgr.reap_orphans = AsyncMock(return_value=0)

        report = await InstanceLifecycleReconciler([mgr]).tick()

        assert report["workspace"]["reaped"] == 0
        snapshot.capture_vm_snapshot.assert_not_awaited()
        container.delete_workspace_with_outcome.assert_not_awaited()
        db.merge_workspace_container_context.assert_not_awaited()
        container.capture_terminal_workspace_identity.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_captured_delete_waits_for_exact_absence_before_follow_on(self):
        mgr, container, _, _, _ = _make_manager(
            job_rows={
                "j1": {
                    "status": "completed",
                    "execution_lane": "pinned",
                    "context": {},
                }
            },
            completion_commands_enabled=True,
        )
        container.reconcile_workspace_cleanup_intent.return_value = (
            WorkspaceCleanupOutcome("retryable", 7)
        )
        inst = Instance(
            kind="workspace",
            id="workspace-j1",
            bound_to="j1",
            metadata={
                "labels": {"srw/job-id": "j1"},
                "job_status": "completed",
                "execution_lane": "pinned",
                "pod_uid": "11111111-1111-4111-8111-111111111111",
                "volume_ephemeral": False,
            },
        )

        await mgr.delete(inst, grace_s=0)

        _assert_exact_workspace_delete(container, WorkspaceOwner.job("j1"))
        container.delete_workspace_pvc.assert_not_awaited()
        container._delete_service.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_lost_term_after_exact_delete_has_no_pvc_or_service_follow_on(self):
        mgr, container, _, _, db = _make_manager(
            job_rows={
                "j1": {
                    "status": "completed",
                    "execution_lane": "pinned",
                    "context": {},
                }
            },
            completion_commands_enabled=True,
        )
        calls = 0

        async def _refresh(permit):
            nonlocal calls
            calls += 1
            if calls >= 3:
                permit.lost.set()
                return False
            return True

        mgr._completion_lifecycle.refresh = AsyncMock(side_effect=_refresh)
        inst = Instance(
            kind="workspace",
            id="workspace-j1",
            bound_to="j1",
            metadata={
                "labels": {"srw/job-id": "j1"},
                "job_status": "completed",
                "execution_lane": "pinned",
                "pod_uid": "11111111-1111-4111-8111-111111111111",
                "volume_ephemeral": False,
            },
        )

        await mgr.delete(inst, grace_s=0)

        container.prepare_workspace_cleanup_intent.assert_awaited_once()
        container.reconcile_workspace_cleanup_intent.assert_not_awaited()
        container.delete_workspace_pvc.assert_not_awaited()
        container._delete_service.assert_not_awaited()
        conn = db.acquire.return_value.__aenter__.return_value
        assert not any(
            "- '_completion_control_claim'" in str(call.args[0])
            for call in conn.fetchrow.await_args_list
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("failed_resource", ["pvc", "service"])
    async def test_ambiguous_residual_delete_retains_lifecycle_claim(
        self, failed_resource
    ):
        mgr, container, _, _, db = _make_manager(
            job_rows={
                "j1": {
                    "status": "completed",
                    "execution_lane": "pinned",
                    "context": {},
                }
            },
            completion_commands_enabled=True,
        )
        container.reconcile_workspace_cleanup_intent.return_value = (
            WorkspaceCleanupOutcome("retryable", 7)
        )
        inst = Instance(
            kind="workspace",
            id="workspace-j1",
            bound_to="j1",
            metadata={
                "labels": {"srw/job-id": "j1"},
                "job_status": "completed",
                "execution_lane": "pinned",
                "pod_uid": "11111111-1111-4111-8111-111111111111",
                "volume_ephemeral": False,
            },
        )

        await mgr.delete(inst, grace_s=0)

        _assert_exact_workspace_delete(container, WorkspaceOwner.job("j1"))
        assert (
            container.prepare_workspace_cleanup_intent.await_args.kwargs[
                "reclaim_shared_resources"
            ]
            is True
        )
        container.delete_workspace_pvc.assert_not_awaited()
        container._delete_service.assert_not_awaited()
        conn = db.acquire.return_value.__aenter__.return_value
        assert not any(
            "- '_completion_control_claim'" in str(call.args[0])
            for call in conn.fetchrow.await_args_list
        )

    @pytest.mark.asyncio
    async def test_captured_absent_residuals_are_not_name_deleted(self):
        mgr, container, _, _, db = _make_manager(
            job_rows={
                "j1": {
                    "status": "completed",
                    "execution_lane": "pinned",
                    "context": {},
                }
            },
            completion_commands_enabled=True,
        )
        container.capture_workspace_teardown_identity.return_value = (
            WorkspaceTeardownIdentity(
                pod_uid="11111111-1111-4111-8111-111111111111",
                pvc_uid=None,
                service_uid=None,
            )
        )
        inst = Instance(
            kind="workspace",
            id="workspace-j1",
            bound_to="j1",
            metadata={
                "labels": {"srw/job-id": "j1"},
                "job_status": "completed",
                "execution_lane": "pinned",
                "pod_uid": "11111111-1111-4111-8111-111111111111",
                "volume_ephemeral": False,
            },
        )

        await mgr.delete(inst, grace_s=0)

        _assert_exact_workspace_delete(container, WorkspaceOwner.job("j1"))
        container.delete_workspace_pvc.assert_not_awaited()
        container._delete_service.assert_not_awaited()
        conn = db.acquire.return_value.__aenter__.return_value
        assert any(
            "- '_completion_control_claim'" in str(call.args[0])
            for call in conn.fetchrow.await_args_list
        )


# =============================================================================
# is_reapable (teardown eligibility — superset of is_idle)
# =============================================================================


class TestIsReapable:
    @pytest.mark.asyncio
    async def test_completed_job_is_reapable(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", metadata={"job_status": "completed"})
        assert await mgr.is_reapable(inst) is True

    @pytest.mark.asyncio
    async def test_failed_job_is_reapable(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", metadata={"job_status": "failed"})
        assert await mgr.is_reapable(inst) is True

    @pytest.mark.asyncio
    async def test_paused_job_is_reapable_after_grace(self):
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="workspace",
            id="x",
            metadata={
                "job_status": "paused",
                "job_updated_at": datetime.now(timezone.utc) - timedelta(hours=2),
            },
        )
        assert await mgr.is_reapable(inst) is True

    @pytest.mark.asyncio
    async def test_freshly_paused_job_is_not_reapable(self):
        # The incident shape: a job pauses on a vm_upgrade approval (24h TTL)
        # and the reaper destroyed the workspace on the next tick. Within the
        # warm grace the pod must survive.
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="workspace",
            id="x",
            metadata={
                "job_status": "paused",
                "job_updated_at": datetime.now(timezone.utc),
            },
        )
        assert await mgr.is_reapable(inst) is False

    @pytest.mark.asyncio
    async def test_paused_grace_env_override(self, monkeypatch):
        # WORKSPACE_PAUSED_REAP_GRACE_S=0 disables the grace (old behavior).
        monkeypatch.setenv("WORKSPACE_PAUSED_REAP_GRACE_S", "0")
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="workspace",
            id="x",
            metadata={
                "job_status": "paused",
                "job_updated_at": datetime.now(timezone.utc),
            },
        )
        assert await mgr.is_reapable(inst) is True

    @pytest.mark.asyncio
    async def test_reviewing_job_without_live_child_is_reapable(self):
        # Durable guard restores 'reviewing' to the reapable set: once no live
        # critic shares the pod (critic terminated), the review-state parent's
        # pod can be snapshot+freed like 'pending_review'.
        mgr, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", metadata={"job_status": "reviewing"})
        assert await mgr.is_reapable(inst) is True

    @pytest.mark.asyncio
    async def test_status_with_live_shared_child_is_not_reapable(self):
        # Regression (the P0 bug): a parent whose live pod is shared by a
        # non-terminal critic must NOT be reaped, whatever its own status.
        # Reaping (snapshot→delete) strands the headless Service → the critic's
        # next SSH is NXDOMAIN → the whole review fails. Keyed on the live child,
        # not on job_status. See knowledge-base/knowledge/issues/reviewing_parent_pod_reaped_under_critic.md.
        mgr, *_ = _make_manager()
        for status in ("reviewing", "pending_review", "paused"):
            inst = Instance(
                kind="workspace",
                id="x",
                metadata={"job_status": status, "has_live_shared_child": True},
            )
            assert await mgr.is_reapable(inst) is False, status

    @pytest.mark.asyncio
    async def test_processing_job_not_reapable(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", metadata={"job_status": "processing"})
        assert await mgr.is_reapable(inst) is False

    @pytest.mark.asyncio
    async def test_ended_thread_is_reapable(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", metadata={"thread_status": "ended"})
        assert await mgr.is_reapable(inst) is True

    @pytest.mark.asyncio
    async def test_active_thread_not_reapable(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", metadata={"thread_status": "active"})
        assert await mgr.is_reapable(inst) is False

    @pytest.mark.asyncio
    async def test_no_status_not_reapable(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", metadata={})
        assert await mgr.is_reapable(inst) is False


# =============================================================================
# Missing-row orphan reap (job deleted while its pod lived)
# =============================================================================


class TestMissingRowOrphan:
    """A pod whose bound row is confirmed gone (job/thread deleted) is an
    orphan: reapable once past the grace age, clean (nothing to snapshot),
    terminal (PVC/Service reclaimed). A *failed* lookup must never be
    mistaken for a missing row. See
    knowledge-history/done/deleted_job_orphans_workspace_pod.md."""

    @staticmethod
    def _orphan_pod(job_id: str = "jgone", age_hours: float = 2.0):
        pod = _make_pod(
            f"workspace-{job_id}",
            labels={"srw/job-id": job_id, "srw.io/component": "agent-workspace"},
        )
        pod.metadata.creation_timestamp = datetime.now(timezone.utc) - timedelta(
            hours=age_hours
        )
        return pod

    @pytest.mark.asyncio
    async def test_list_instances_marks_missing_job_row(self):
        mgr, *_ = _make_manager(pods=[self._orphan_pod()])  # no job_rows → row gone
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            (inst,) = await mgr.list_instances()
        assert inst.metadata["bound_row_missing"] is True
        assert inst.metadata["pod_age_s"] == pytest.approx(7200, abs=60)

    @pytest.mark.asyncio
    async def test_list_instances_marks_missing_thread_row(self):
        pod = _make_pod(
            "ws-thread-gone",
            labels={
                "srw/thread-id": "tgone",
                "srw.io/component": "agent-workspace",
            },
        )
        mgr, *_ = _make_manager(pods=[pod])  # no thread_rows → row gone
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            (inst,) = await mgr.list_instances()
        assert inst.metadata["bound_row_missing"] is True

    @pytest.mark.asyncio
    async def test_fetch_failure_is_not_marked_missing(self):
        # DB error ≠ row gone: metadata stays bare → never reapable.
        mgr, _, _, _, db = _make_manager(pods=[self._orphan_pod()])
        db.get_job = AsyncMock(side_effect=RuntimeError("db down"))
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            (inst,) = await mgr.list_instances()
        assert "bound_row_missing" not in inst.metadata
        assert await mgr.is_reapable(inst) is False

    @pytest.mark.asyncio
    async def test_aged_orphan_is_reapable(self, monkeypatch):
        monkeypatch.setenv("WORKSPACE_ORPHAN_GRACE_SECONDS", "900")
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="workspace",
            id="x",
            bound_to="jgone",
            metadata={"bound_row_missing": True, "pod_age_s": 3600.0},
        )
        assert await mgr.is_reapable(inst) is True

    @pytest.mark.asyncio
    async def test_young_orphan_is_not_reapable(self, monkeypatch):
        # Protects the pod-created-but-row-not-yet-persisted window.
        monkeypatch.setenv("WORKSPACE_ORPHAN_GRACE_SECONDS", "900")
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="workspace",
            id="x",
            bound_to="jgone",
            metadata={"bound_row_missing": True, "pod_age_s": 30.0},
        )
        assert await mgr.is_reapable(inst) is False

    @pytest.mark.asyncio
    async def test_unknown_age_orphan_is_not_reapable(self):
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="workspace",
            id="x",
            bound_to="jgone",
            metadata={"bound_row_missing": True, "pod_age_s": None},
        )
        assert await mgr.is_reapable(inst) is False

    @pytest.mark.asyncio
    async def test_orphan_is_clean_and_terminal(self):
        # Clean: no entity can restore a snapshot, and record_attempt would
        # merge into a deleted row (silent no-op → infinite retry).
        # Terminal: delete() reclaims PVC + Service; give_up won't recreate.
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="workspace",
            id="x",
            bound_to="jgone",
            metadata={"bound_row_missing": True, "pod_age_s": 3600.0},
        )
        assert await mgr.is_dirty(inst) is False
        assert mgr._is_terminal(inst) is True

    @pytest.mark.asyncio
    async def test_pvc_backed_orphan_delete_reclaims_pvc_and_service(self):
        mgr, container, *_ = _make_manager()
        container.delete_workspace_pvc = AsyncMock(return_value=True)
        inst = Instance(
            kind="workspace",
            id="workspace-jgone",
            bound_to="jgone",
            metadata={
                "labels": {"srw/job-id": "jgone"},
                "bound_row_missing": True,
                "pod_uid": "11111111-1111-4111-8111-111111111111",
                "volume_ephemeral": False,
            },
        )
        await mgr.delete(inst, grace_s=0)
        _assert_exact_workspace_delete(container, WorkspaceOwner.job("jgone"))
        assert (
            container.prepare_workspace_cleanup_intent.await_args.kwargs[
                "reclaim_shared_resources"
            ]
            is True
        )
        container.delete_workspace_pvc.assert_not_awaited()
        container._delete_service.assert_not_awaited()

    @staticmethod
    def _wire_empty_pvcs(container):
        pvc_list = MagicMock()
        pvc_list.items = []
        container._core_api.list_namespaced_persistent_volume_claim.return_value = (
            pvc_list
        )

    @pytest.mark.asyncio
    async def test_reconciler_tick_reaps_aged_orphan_without_snapshot(
        self, monkeypatch
    ):
        # End-to-end through the reconciler: the aged orphan goes straight to
        # delete — no snapshot capture, no attempt bookkeeping.
        monkeypatch.delenv("WORKSPACE_IMAGE", raising=False)
        monkeypatch.setenv("WORKSPACE_ORPHAN_GRACE_SECONDS", "900")
        mgr, container, _, snapshot, _ = _make_manager(pods=[self._orphan_pod()])
        self._wire_empty_pvcs(container)
        reconciler = InstanceLifecycleReconciler(managers=[mgr])
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            report = await reconciler.tick()
        assert report["workspace"]["reaped"] == 1
        _assert_exact_workspace_delete(container, WorkspaceOwner.job("jgone"))
        snapshot.capture_vm_snapshot.assert_not_called()

    @pytest.mark.asyncio
    async def test_reconciler_tick_spares_young_orphan(self, monkeypatch):
        monkeypatch.delenv("WORKSPACE_IMAGE", raising=False)
        monkeypatch.setenv("WORKSPACE_ORPHAN_GRACE_SECONDS", "900")
        mgr, container, _, _, _ = _make_manager(pods=[self._orphan_pod(age_hours=0.01)])
        self._wire_empty_pvcs(container)
        reconciler = InstanceLifecycleReconciler(managers=[mgr])
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            report = await reconciler.tick()
        assert report["workspace"]["reaped"] == 0
        container.delete_workspace_with_outcome.assert_not_called()


# =============================================================================
# is_dirty (activity-based; threads total_turns, jobs conservative)
# =============================================================================


class TestIsDirty:
    @pytest.mark.asyncio
    async def test_thread_zero_turns_is_clean(self):
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="workspace",
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
    async def test_thread_turns_ahead_of_snapshot_is_dirty(self):
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="workspace",
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
    async def test_thread_turns_equal_snapshot_is_clean(self):
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="workspace",
            id="x",
            bound_to="t1",
            metadata={
                "thread_status": "ended",
                "total_turns": 3,
                "last_snapshot_turns": 3,
            },
        )
        assert await mgr.is_dirty(inst) is False

    @pytest.mark.asyncio
    async def test_thread_with_turns_never_snapshotted_is_dirty(self):
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="workspace",
            id="x",
            bound_to="t1",
            metadata={
                "thread_status": "ended",
                "total_turns": 4,
                "last_snapshot_turns": None,
            },
        )
        assert await mgr.is_dirty(inst) is True

    @pytest.mark.asyncio
    async def test_terminal_job_with_snapshot_is_clean(self):
        # Completed jobs get a completion snapshot — reap without re-capture.
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="workspace",
            id="x",
            bound_to="j1",
            metadata={"job_status": "completed", "snapshot_status": "available"},
        )
        assert await mgr.is_dirty(inst) is False

    @pytest.mark.asyncio
    async def test_job_without_snapshot_is_dirty(self):
        # No job turn-counter → conservative: attempt a snapshot.
        mgr, *_ = _make_manager()
        inst = Instance(
            kind="workspace",
            id="x",
            bound_to="j1",
            metadata={"job_status": "pending_review", "snapshot_status": None},
        )
        assert await mgr.is_dirty(inst) is True


# =============================================================================
# is_state_ephemeral (volume-mode branch)
# =============================================================================


class TestIsStateEphemeral:
    @pytest.mark.asyncio
    async def test_emptydir_is_ephemeral(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", metadata={"volume_ephemeral": True})
        assert await mgr.is_state_ephemeral(inst) is True

    @pytest.mark.asyncio
    async def test_pvc_is_not_ephemeral(self):
        mgr, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", metadata={"volume_ephemeral": False})
        assert await mgr.is_state_ephemeral(inst) is False

    @pytest.mark.asyncio
    async def test_unknown_defaults_to_ephemeral(self):
        # Default matches today's reality (emptyDir). Conservative for the
        # current fleet; the PVC migration spec flips the default explicitly.
        mgr, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", metadata={})
        assert await mgr.is_state_ephemeral(inst) is True


class TestIsReachable:
    @pytest.mark.asyncio
    async def test_reachable_when_connect_succeeds(self):
        mgr, *_ = _make_manager()
        mgr._tcp_probe = AsyncMock(return_value=True)
        inst = Instance(kind="workspace", id="x", metadata={"pod_ip": "10.0.0.5"})
        assert await mgr.is_reachable(inst) is True
        mgr._tcp_probe.assert_awaited_once_with("10.0.0.5", 30022)

    @pytest.mark.asyncio
    async def test_unreachable_when_connect_fails(self):
        mgr, *_ = _make_manager()
        mgr._tcp_probe = AsyncMock(return_value=False)
        inst = Instance(kind="workspace", id="x", metadata={"pod_ip": "10.0.0.5"})
        assert await mgr.is_reachable(inst) is False

    @pytest.mark.asyncio
    async def test_unreachable_without_pod_ip(self):
        mgr, *_ = _make_manager()
        mgr._tcp_probe = AsyncMock(return_value=True)
        inst = Instance(kind="workspace", id="x", metadata={})
        assert await mgr.is_reachable(inst) is False
        mgr._tcp_probe.assert_not_called()

    @pytest.mark.asyncio
    async def test_result_is_cached(self):
        mgr, *_ = _make_manager()
        mgr._clock = lambda: 1000.0
        mgr._tcp_probe = AsyncMock(return_value=True)
        inst = Instance(kind="workspace", id="x", metadata={"pod_ip": "10.0.0.5"})
        assert await mgr.is_reachable(inst) is True
        assert await mgr.is_reachable(inst) is True
        mgr._tcp_probe.assert_awaited_once()  # second call served from cache

    @pytest.mark.asyncio
    async def test_cache_expires(self):
        mgr, *_ = _make_manager()
        t = {"now": 1000.0}
        mgr._clock = lambda: t["now"]
        mgr._tcp_probe = AsyncMock(return_value=True)
        inst = Instance(kind="workspace", id="x", metadata={"pod_ip": "10.0.0.5"})
        await mgr.is_reachable(inst)
        t["now"] = 1040.0  # > 30s TTL
        await mgr.is_reachable(inst)
        assert mgr._tcp_probe.await_count == 2


class TestAttemptCounter:
    @pytest.mark.asyncio
    async def test_record_attempt_increments_job_context(self):
        mgr, _, _, _, db = _make_manager()
        db.merge_workspace_container_context = AsyncMock(return_value=True)
        inst = Instance(
            kind="workspace",
            id="workspace-a",
            bound_to="j1",
            metadata={"labels": {"srw/job-id": "j1"}, "snapshot_attempts": 2},
        )
        await mgr.record_attempt(inst)
        db.merge_workspace_container_context.assert_awaited_once_with(
            "j1", {"snapshot_attempts": 3}
        )

    @pytest.mark.asyncio
    async def test_record_attempt_increments_thread_context(self):
        mgr, _, _, _, db = _make_manager()
        db.merge_thread_workspace_context = AsyncMock(return_value=True)
        inst = Instance(
            kind="workspace",
            id="ws-thread-a",
            bound_to="t1",
            metadata={"labels": {"srw/thread-id": "t1"}, "snapshot_attempts": 0},
        )
        await mgr.record_attempt(inst)
        db.merge_thread_workspace_context.assert_awaited_once_with(
            "t1", {"snapshot_attempts": 1}
        )

    @pytest.mark.asyncio
    async def test_exhausted_true_at_threshold(self, monkeypatch):
        monkeypatch.setenv("WORKSPACE_SNAPSHOT_MAX_ATTEMPTS", "5")
        mgr, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", metadata={"snapshot_attempts": 5})
        assert await mgr.attempts_exhausted(inst) is True

    @pytest.mark.asyncio
    async def test_exhausted_false_below_threshold(self, monkeypatch):
        monkeypatch.setenv("WORKSPACE_SNAPSHOT_MAX_ATTEMPTS", "5")
        mgr, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", metadata={"snapshot_attempts": 4})
        assert await mgr.attempts_exhausted(inst) is False


class TestGiveUp:
    @pytest.mark.asyncio
    async def test_ephemeral_give_up_deletes(self):
        mgr, container, *_ = _make_manager()
        inst = Instance(
            kind="workspace",
            id="workspace-a",
            bound_to="j1",
            metadata={
                "labels": {"srw/job-id": "j1"},
                "pod_uid": "11111111-1111-4111-8111-111111111111",
                "volume_ephemeral": True,
            },
        )
        await mgr.give_up(inst, grace_s=0)
        _assert_exact_workspace_delete(container, WorkspaceOwner.job("j1"))

    @pytest.mark.asyncio
    async def test_pvc_give_up_recreates_keeps_pvc(self):
        mgr, container, *_ = _make_manager()
        container.create_workspace = AsyncMock(return_value=True)
        container.delete_workspace_pvc = AsyncMock(return_value=True)
        inst = Instance(
            kind="workspace",
            id="workspace-a",
            bound_to="j1",
            metadata={
                "labels": {"srw/job-id": "j1"},
                "pod_uid": "11111111-1111-4111-8111-111111111111",
                "volume_ephemeral": False,
            },
        )
        await mgr.give_up(inst, grace_s=0)
        _assert_exact_workspace_delete(container, WorkspaceOwner.job("j1"))
        container.create_workspace.assert_awaited_once_with(WorkspaceOwner.job("j1"))
        container.delete_workspace_pvc.assert_not_called()  # PVC must survive

    @pytest.mark.asyncio
    async def test_kubernetes_snapshot_refusal_does_not_reset_attempt_counter(self):
        mgr, _, _, snapshot, db = _make_manager()
        db.merge_workspace_container_context = AsyncMock(return_value=True)
        snapshot.capture_vm_snapshot = AsyncMock(return_value=True)
        inst = Instance(
            kind="workspace",
            id="workspace-a",
            bound_to="j1",
            metadata={"labels": {"srw/job-id": "j1"}, "pod_ip": "10.0.0.5"},
        )
        ref = await mgr.snapshot(inst)
        assert ref is None
        snapshot.capture_vm_snapshot.assert_not_awaited()
        db.merge_workspace_container_context.assert_not_awaited()


def _make_pvc(name: str, job_id: str | None = None, thread_id: str | None = None):
    """A PVC as the sweep sees it: name + owner label.

    ``thread_id`` stamps ``srw/thread-id`` the way both provisioners do for
    session claims (workspace pod ``pvc-ws-thread-*``, agent pod
    ``pvc-agent-s-*``); neither label set makes the claim foreign.
    """
    pvc = MagicMock()
    pvc.metadata.name = name
    labels: dict[str, str] = {}
    if job_id:
        labels["srw/job-id"] = job_id
    if thread_id:
        labels["srw/thread-id"] = thread_id
    pvc.metadata.labels = labels
    return pvc


def _terminal_job_row(status: str = "completed") -> dict:
    return {
        "status": status,
        "execution_lane": "pinned",
        "context": {
            "workspace_container": {
                "status": "deleted",
                "provisioner": "k8s",
                "_runtime_incarnation": ("11111111-1111-4111-8111-111111111111"),
            }
        },
    }


# =============================================================================
# delete() — terminal PVC reclaim (Branch a leak guard)
# =============================================================================


class TestDeleteTerminalPvc:
    @pytest.mark.asyncio
    async def test_terminal_job_pvc_backed_deletes_pvc(self):
        mgr, container, *_ = _make_manager()
        container.delete_workspace_pvc = AsyncMock(return_value=True)
        inst = Instance(
            kind="workspace",
            id="workspace-a",
            bound_to="j1",
            metadata={
                "labels": {"srw/job-id": "j1"},
                "job_status": "completed",
                "pod_uid": "11111111-1111-4111-8111-111111111111",
                "volume_ephemeral": False,
            },
        )
        await mgr.delete(inst, grace_s=0)
        _assert_exact_workspace_delete(container, WorkspaceOwner.job("j1"))
        assert (
            container.prepare_workspace_cleanup_intent.await_args.kwargs[
                "reclaim_shared_resources"
            ]
            is True
        )
        # Reconciliation owns exact captured PVC/Service cleanup and the final
        # owner projection in one durable protocol.
        container.delete_workspace_pvc.assert_not_awaited()
        container._delete_service.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_reaped_emptydir_with_snapshot_marked_suspended(self):
        # Reap-and-restore: a NON-terminal emptyDir pod whose state made it to
        # S3 flips to 'suspended' so the next dispatch restores instead of
        # re-creating a blank pod.
        mgr, container, _, _, db = _make_manager()
        inst = Instance(
            kind="workspace",
            id="workspace-a",
            bound_to="j1",
            metadata={
                "labels": {"srw/job-id": "j1"},
                "job_status": "paused",
                "pod_uid": "11111111-1111-4111-8111-111111111111",
                "volume_ephemeral": True,
                "snapshot_status": "available",
            },
        )
        await mgr.delete(inst, grace_s=0)
        _assert_exact_workspace_delete(container, WorkspaceOwner.job("j1"))
        prepare = container.prepare_workspace_cleanup_intent.await_args.kwargs
        assert prepare["target_disposition"] == "suspended"
        assert prepare["snapshot_restore_required"] is True
        assert prepare["reclaim_shared_resources"] is False
        db.merge_workspace_container_context_if_runtime.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_reaped_emptydir_without_snapshot_not_marked_suspended(self):
        mgr, container, _, _, db = _make_manager()
        inst = Instance(
            kind="workspace",
            id="workspace-a",
            bound_to="j1",
            metadata={
                "labels": {"srw/job-id": "j1"},
                "job_status": "paused",
                "pod_uid": "11111111-1111-4111-8111-111111111111",
                "volume_ephemeral": True,
            },
        )
        await mgr.delete(inst, grace_s=0)
        _assert_exact_workspace_delete(container, WorkspaceOwner.job("j1"))
        assert (
            container.prepare_workspace_cleanup_intent.await_args.kwargs[
                "target_disposition"
            ]
            == "deleted"
        )
        db.merge_workspace_container_context.assert_not_called()

    @pytest.mark.asyncio
    async def test_reaped_pvc_backed_not_marked_suspended(self):
        # PVC state survives on the volume; an S3 extract could roll newer
        # files back — the restore arm must not fire.
        mgr, container, _, _, db = _make_manager()
        inst = Instance(
            kind="workspace",
            id="workspace-a",
            bound_to="j1",
            metadata={
                "labels": {"srw/job-id": "j1"},
                "job_status": "paused",
                "pod_uid": "11111111-1111-4111-8111-111111111111",
                "volume_ephemeral": False,
                "snapshot_status": "available",
            },
        )
        await mgr.delete(inst, grace_s=0)
        _assert_exact_workspace_delete(container, WorkspaceOwner.job("j1"))
        assert (
            container.prepare_workspace_cleanup_intent.await_args.kwargs[
                "target_disposition"
            ]
            == "deleted"
        )
        db.merge_workspace_container_context.assert_not_called()

    @pytest.mark.asyncio
    async def test_terminal_job_not_marked_suspended(self):
        mgr, container, _, _, db = _make_manager()
        container.delete_workspace_pvc = AsyncMock(return_value=True)
        inst = Instance(
            kind="workspace",
            id="workspace-a",
            bound_to="j1",
            metadata={
                "labels": {"srw/job-id": "j1"},
                "job_status": "completed",
                "pod_uid": "11111111-1111-4111-8111-111111111111",
                "volume_ephemeral": True,
                "snapshot_status": "available",
            },
        )
        await mgr.delete(inst, grace_s=0)
        _assert_exact_workspace_delete(container, WorkspaceOwner.job("j1"))
        db.merge_workspace_container_context.assert_not_called()

    @pytest.mark.asyncio
    async def test_kubernetes_snapshot_refusal_does_not_stamp_instance_metadata(self):
        mgr, _, _, snapshot, db = _make_manager()
        inst = Instance(
            kind="workspace",
            id="workspace-a",
            bound_to="j1",
            metadata={"labels": {"srw/job-id": "j1"}, "pod_ip": "10.0.0.7"},
        )
        ref = await mgr.snapshot(inst)
        assert ref is None
        assert "snapshot_status" not in inst.metadata
        snapshot.capture_vm_snapshot.assert_not_awaited()
        db.merge_workspace_container_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_idle_job_pvc_backed_keeps_pvc(self):
        mgr, container, *_ = _make_manager()
        container.delete_workspace_pvc = AsyncMock(return_value=True)
        inst = Instance(
            kind="workspace",
            id="workspace-a",
            bound_to="j1",
            metadata={
                "labels": {"srw/job-id": "j1"},
                "job_status": "paused",  # idle, NOT terminal → reattach next dispatch
                "pod_uid": "11111111-1111-4111-8111-111111111111",
                "volume_ephemeral": False,
            },
        )
        await mgr.delete(inst, grace_s=0)
        _assert_exact_workspace_delete(container, WorkspaceOwner.job("j1"))
        container.delete_workspace_pvc.assert_not_called()
        # Idle keeps the Service too (stable DNS persists for the resume).
        container._delete_service.assert_not_called()

    @pytest.mark.asyncio
    async def test_terminal_job_emptydir_does_not_delete_pvc(self):
        mgr, container, *_ = _make_manager()
        container.delete_workspace_pvc = AsyncMock(return_value=True)
        inst = Instance(
            kind="workspace",
            id="workspace-a",
            bound_to="j1",
            metadata={
                "labels": {"srw/job-id": "j1"},
                "job_status": "completed",
                "volume_ephemeral": True,  # emptyDir → there is no PVC
            },
        )
        await mgr.delete(inst, grace_s=0)
        container.delete_workspace_pvc.assert_not_called()

    @pytest.mark.asyncio
    async def test_ended_thread_keeps_its_pvc_because_it_is_resumable(self):
        """THE session invariant: ending a session must not destroy its volume.

        'ended' is terminal for the POD and not for the VOLUME. The agent's
        idle-archive handler flips a thread to 'ended' after 30 idle minutes,
        and ``resume_thread`` requires exactly that status to bring the session
        back — so reclaiming here would delete a user's working tree on a coffee
        break, which is strictly worse than the emptyDir behavior PVCs replace.
        The pod still goes (that is how idle suspend saves money).
        """
        mgr, container, *_ = _make_manager()
        container.delete_workspace_pvc = AsyncMock(return_value=True)
        inst = Instance(
            kind="workspace",
            id="ws-thread-a",
            bound_to="t1",
            metadata={
                "labels": {"srw/thread-id": "t1"},
                "thread_status": "ended",
                "pod_uid": "11111111-1111-4111-8111-111111111111",
                "volume_ephemeral": False,
            },
        )
        await mgr.delete(inst, grace_s=0)
        _assert_exact_workspace_delete(container, WorkspaceOwner.session("t1"))
        container.delete_workspace_pvc.assert_not_called()
        # The Service follows the volume here, not the pod: a resume reattaches
        # the claim and the stable DNS should still point at it.
        container._delete_service.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_thread_status_licenses_a_volume_delete(self):
        """Not just 'ended' — no thread status at all licenses a volume delete.

        'suspended' and 'active' are equally resumable, and the day someone adds
        a new one it must default to keeping data. Only the row going away
        reclaims (see the sibling below).
        """
        for status in ("ended", "suspended", "active"):
            mgr, container, *_ = _make_manager()
            container.delete_workspace_pvc = AsyncMock(return_value=True)
            inst = Instance(
                kind="workspace",
                id="ws-thread-a",
                bound_to="t1",
                metadata={
                    "labels": {"srw/thread-id": "t1"},
                    "thread_status": status,
                    "pod_uid": "11111111-1111-4111-8111-111111111111",
                    "volume_ephemeral": False,
                },
            )
            assert mgr._is_volume_reclaimable(inst) is False, status
            await mgr.delete(inst, grace_s=0)
            _assert_exact_workspace_delete(container, WorkspaceOwner.session("t1"))
            container.delete_workspace_pvc.assert_not_called()

    @pytest.mark.asyncio
    async def test_deleted_thread_pvc_backed_reclaims_with_session_owner(self):
        """The one session case that DOES reclaim: the thread row is gone.

        Thread deletion is a hard ``DELETE FROM threads`` — there is no 'deleted'
        status to read — so ``bound_row_missing`` is the only deletion signal,
        and it is set only when the lookup succeeded and found nothing.
        """
        mgr, container, *_ = _make_manager()
        container.delete_workspace_pvc = AsyncMock(return_value=True)
        inst = Instance(
            kind="workspace",
            id="ws-thread-a",
            bound_to="t1",
            metadata={
                "labels": {"srw/thread-id": "t1"},
                "bound_row_missing": True,
                "pod_uid": "11111111-1111-4111-8111-111111111111",
                "volume_ephemeral": False,
            },
        )
        assert mgr._is_volume_reclaimable(inst) is True
        await mgr.delete(inst, grace_s=0)
        _assert_exact_workspace_delete(container, WorkspaceOwner.session("t1"))
        assert (
            container.prepare_workspace_cleanup_intent.await_args.kwargs[
                "reclaim_shared_resources"
            ]
            is True
        )
        container.delete_workspace_pvc.assert_not_awaited()
        container._delete_service.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unknown_binding_never_reclaims(self):
        """A failed DB lookup leaves the metadata bare — and bare must mean keep.

        The fall-through has to be "keep": a leaked 10Gi volume is an
        operational annoyance the backstop sweep collects later; a deleted one
        is unrecoverable.
        """
        mgr, container, *_ = _make_manager()
        container.delete_workspace_pvc = AsyncMock(return_value=True)
        inst = Instance(
            kind="workspace",
            id="ws-thread-a",
            bound_to="t1",
            metadata={"labels": {"srw/thread-id": "t1"}, "volume_ephemeral": False},
        )
        assert mgr._is_volume_reclaimable(inst) is False
        await mgr.delete(inst, grace_s=0)
        container.delete_workspace_pvc.assert_not_called()

    @pytest.mark.asyncio
    async def test_terminal_job_still_reclaims_its_volume(self):
        """Regression guard for the asymmetry: jobs are unchanged. A finished
        job is finished forever, so its volume goes with it — that is the
        "PVC dies when the job dies" guard Branch (a) shipped on.
        (test_terminal_job_pvc_backed_deletes_pvc covers the delete() wiring;
        this pins the predicate across the whole terminal set.)"""
        mgr, *_ = _make_manager()
        for status in ("completed", "failed", "cancelled"):
            inst = Instance(
                kind="workspace",
                id="workspace-a",
                bound_to="j1",
                metadata={
                    "labels": {"srw/job-id": "j1"},
                    "job_status": status,
                    "volume_ephemeral": False,
                },
            )
            assert mgr._is_volume_reclaimable(inst) is True, status

    @pytest.mark.asyncio
    async def test_pod_delete_failure_skips_pvc_delete(self):
        mgr, container, *_ = _make_manager()
        container.reconcile_workspace_cleanup_intent = AsyncMock(
            side_effect=RuntimeError("boom")
        )
        container.delete_workspace_pvc = AsyncMock(return_value=True)
        inst = Instance(
            kind="workspace",
            id="workspace-a",
            bound_to="j1",
            metadata={
                "labels": {"srw/job-id": "j1"},
                "job_status": "completed",
                "volume_ephemeral": False,
            },
        )
        await mgr.delete(inst, grace_s=0)
        # If we couldn't even delete the pod, don't delete the volume.
        container.delete_workspace_pvc.assert_not_called()


class TestGiveUpTerminal:
    @pytest.mark.asyncio
    async def test_terminal_pvc_give_up_reclaims_and_does_not_recreate(self):
        mgr, container, *_ = _make_manager()
        container.create_workspace = AsyncMock(return_value=True)
        container.delete_workspace_pvc = AsyncMock(return_value=True)
        inst = Instance(
            kind="workspace",
            id="workspace-a",
            bound_to="j1",
            metadata={
                "labels": {"srw/job-id": "j1"},
                "job_status": "failed",  # terminal
                "pod_uid": "11111111-1111-4111-8111-111111111111",
                "volume_ephemeral": False,
            },
        )
        await mgr.give_up(inst, grace_s=0)
        _assert_exact_workspace_delete(container, WorkspaceOwner.job("j1"))
        assert (
            container.prepare_workspace_cleanup_intent.await_args.kwargs[
                "reclaim_shared_resources"
            ]
            is True
        )
        # Cleanup is owned by the durable reconciler; give_up must not recreate.
        container.delete_workspace_pvc.assert_not_awaited()
        container.create_workspace.assert_not_called()


# =============================================================================
# reap_orphans() — backstop PVC GC
# =============================================================================


class TestReapOrphans:
    @staticmethod
    def _wire_pvcs(container, pvcs):
        pvc_list = MagicMock()
        pvc_list.items = pvcs
        container._core_api.list_namespaced_persistent_volume_claim.return_value = (
            pvc_list
        )
        container._delete_pvc = AsyncMock(return_value=True)

    @pytest.mark.asyncio
    async def test_reaps_terminal_job_pvc_with_no_live_pod(self):
        mgr, container, _, _, _ = _make_manager(
            pods=[], job_rows={"jdone": _terminal_job_row()}
        )
        self._wire_pvcs(container, [_make_pvc("pvc-workspace-jdone", "jdone")])
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            n = await mgr.reap_orphans()
        assert n == 1
        container.prepare_workspace_cleanup_intent.assert_awaited_once_with(
            WorkspaceOwner.job("jdone"),
            expected_runtime_incarnation=("11111111-1111-4111-8111-111111111111"),
            target_disposition="deleted",
            reclaim_shared_resources=True,
            admission_source="automatic",
        )
        container.reconcile_workspace_cleanup_intent.assert_awaited_once_with(
            WorkspaceOwner.job("jdone"),
            expected_runtime_incarnation=("11111111-1111-4111-8111-111111111111"),
            intent_generation=7,
        )
        container._delete_pvc.assert_not_awaited()
        container._delete_service.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_completion_owner_blocks_orphan_pvc_then_done_allows_reap(self):
        mgr, container, _, _, db = _make_manager(
            pods=[],
            job_rows={"jdone": _terminal_job_row()},
            completion_commands_enabled=True,
        )
        self._wire_pvcs(container, [_make_pvc("pvc-workspace-jdone", "jdone")])
        command = {"route": "stand_down"}
        conn = db.acquire.return_value.__aenter__.return_value
        base_fetchrow = conn.fetchrow.side_effect

        async def _fetchrow(sql, *args):
            if "job_completion_sweep_exclusions" in sql and command["route"]:
                return {
                    "command_id": "44444444-4444-4444-8444-444444444444",
                    "route": command["route"],
                }
            return await base_fetchrow(sql, *args)

        conn.fetchrow.side_effect = _fetchrow

        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            assert await mgr.reap_orphans() == 0
        container._delete_pvc.assert_not_awaited()
        container._delete_service.assert_not_awaited()

        command["route"] = None
        # Model the successful jobs-row marker write/clear used by the second
        # pass; the first pass stands down before reaching either statement.
        routed_fetchrow = conn.fetchrow.side_effect

        async def _claiming_fetchrow(sql, *args):
            if "UPDATE jobs" in sql and "RETURNING" in sql:
                return {"id": args[0], "context": {}}
            return await routed_fetchrow(sql, *args)

        conn.fetchrow.side_effect = _claiming_fetchrow
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            assert await mgr.reap_orphans() == 1
        container.prepare_workspace_cleanup_intent.assert_awaited_once_with(
            WorkspaceOwner.job("jdone"),
            expected_runtime_incarnation=("11111111-1111-4111-8111-111111111111"),
            target_disposition="deleted",
            reclaim_shared_resources=True,
            admission_source="automatic",
        )
        container._delete_pvc.assert_not_awaited()
        container._delete_service.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_failed_identity_probe_releases_the_orphan_pvc_claim(self):
        """lifecycle_reap_failure_parks_running_stateless_job: the incident's
        failure was a read-only identity capture (a ws_client handshake error)
        under a lifecycle claim. No teardown I/O happened, so the claim must be
        released rather than fencing the job's controls for the whole 2 h
        lifecycle lease — and one failing PVC must not abort the sweep."""
        from orchestrator.services.container_provisioner import (
            WorkspaceRuntimeAuthorityError,
        )

        mgr, container, _, _, db = _make_manager(
            pods=[],
            job_rows={
                "jdone": _terminal_job_row(),
                "jnext": _terminal_job_row("failed"),
            },
            completion_commands_enabled=True,
        )
        self._wire_pvcs(
            container,
            [
                _make_pvc("pvc-workspace-jdone", "jdone"),
                _make_pvc("pvc-workspace-jnext", "jnext"),
            ],
        )
        container.capture_workspace_teardown_identity.side_effect = (
            WorkspaceRuntimeAuthorityError("workspace Pod identity capture failed")
        )

        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            assert await mgr.reap_orphans() == 0

        assert container.capture_workspace_teardown_identity.await_count == 2
        container.prepare_workspace_cleanup_intent.assert_not_awaited()
        container.reconcile_workspace_cleanup_intent.assert_not_awaited()
        conn = db.acquire.return_value.__aenter__.return_value
        released = [
            call.args[1]  # (sql, job_id, claim_id)
            for call in conn.fetchrow.await_args_list
            if "- '_completion_control_claim'" in str(call.args[0])
        ]
        assert released == ["jdone", "jnext"]

    @pytest.mark.asyncio
    async def test_control_marker_blocks_orphan_pvc_destructive_recheck(self):
        mgr, container, _, _, db = _make_manager(
            pods=[],
            job_rows={"jdone": _terminal_job_row()},
            completion_commands_enabled=True,
            completion_control_active=True,
        )
        self._wire_pvcs(container, [_make_pvc("pvc-workspace-jdone", "jdone")])

        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            assert await mgr.reap_orphans() == 0

        container._delete_pvc.assert_not_awaited()
        container._delete_service.assert_not_awaited()
        conn = db.acquire.return_value.__aenter__.return_value
        assert any(
            "FROM jobs" in str(call.args[0]) and "FOR UPDATE" in str(call.args[0])
            for call in conn.fetchrow.await_args_list
        )

    @pytest.mark.asyncio
    async def test_retains_pvc_whose_job_row_is_gone_without_exact_authority(self):
        # A name and missing row do not prove the retired runtime/process zero.
        mgr, container, _, _, _ = _make_manager(pods=[], thread_rows={})
        self._wire_pvcs(container, [_make_pvc("pvc-workspace-jgone", "jgone")])
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            n = await mgr.reap_orphans()
        assert n == 0
        container.prepare_workspace_cleanup_intent.assert_not_awaited()
        container._delete_pvc.assert_not_awaited()
        container._delete_service.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_skips_active_job_pvc(self):
        mgr, container, _, _, _ = _make_manager(
            pods=[], job_rows={"jrun": {"status": "processing", "context": {}}}
        )
        self._wire_pvcs(container, [_make_pvc("pvc-workspace-jrun", "jrun")])
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            n = await mgr.reap_orphans()
        assert n == 0
        container._delete_pvc.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_pvc_with_live_pod(self):
        # Terminal job but a pod still exists → instance path owns teardown.
        pod = _make_pod("workspace-jlive", labels={"srw/job-id": "jlive"})
        mgr, container, _, _, _ = _make_manager(
            pods=[pod], job_rows={"jlive": _terminal_job_row()}
        )
        self._wire_pvcs(container, [_make_pvc("pvc-workspace-jlive", "jlive")])
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            n = await mgr.reap_orphans()
        assert n == 0
        container._delete_pvc.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_unlabeled_and_foreign_pvcs(self):
        mgr, container, _, _, _ = _make_manager(pods=[], thread_rows={})
        self._wire_pvcs(
            container,
            [
                _make_pvc("srw-workspace"),  # shared scratch PVC, no job-id label
                _make_pvc("some-other-pvc", "jx"),  # job-id but wrong name prefix
            ],
        )
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            n = await mgr.reap_orphans()
        assert n == 0
        container._delete_pvc.assert_not_called()

    @pytest.mark.asyncio
    async def test_db_lookup_error_does_not_delete(self):
        # A transient DB error must NOT masquerade as "job gone".
        mgr, container, _, _, db = _make_manager(pods=[])
        db.acquire = MagicMock(side_effect=RuntimeError("db down"))
        self._wire_pvcs(container, [_make_pvc("pvc-workspace-jerr", "jerr")])
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            n = await mgr.reap_orphans()
        assert n == 0
        container._delete_pvc.assert_not_called()

    @pytest.mark.asyncio
    async def test_noop_when_k8s_unavailable(self):
        mgr, container, _, _, _ = _make_manager(k8s_available=False)
        n = await mgr.reap_orphans()
        assert n == 0

    @pytest.mark.asyncio
    async def test_reconciles_pending_cleanup_intents_without_a_live_pod_or_pvc(self):
        mgr, container, _, _, _ = _make_manager(pods=[])
        self._wire_pvcs(container, [])
        reconcile_pending = AsyncMock(
            return_value={"settled": 1, "superseded": 1, "retryable": 2}
        )

        with patch.object(
            type(container),
            "reconcile_pending_workspace_cleanup_intents",
            reconcile_pending,
            create=True,
        ):
            n = await mgr.reap_orphans()

        assert n == 2
        reconcile_pending.assert_awaited_once_with(container, limit=25)
        container._delete_pvc.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_terminal_cleanup_lost_response_replays_same_intent(self):
        mgr, container, _, _, _ = _make_manager(
            pods=[], job_rows={"jdone": _terminal_job_row()}
        )
        self._wire_pvcs(container, [_make_pvc("pvc-workspace-jdone", "jdone")])
        container.reconcile_workspace_cleanup_intent.side_effect = [
            RuntimeError("response lost after settlement"),
            WorkspaceCleanupOutcome("settled", 7),
        ]

        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            assert await mgr.reap_orphans() == 0
            assert await mgr.reap_orphans() == 1

        assert container.prepare_workspace_cleanup_intent.await_count == 2
        for call in container.prepare_workspace_cleanup_intent.await_args_list:
            assert call.kwargs["expected_runtime_incarnation"] == (
                "11111111-1111-4111-8111-111111111111"
            )
            assert call.kwargs["target_disposition"] == "deleted"
            assert call.kwargs["reclaim_shared_resources"] is True
        assert [
            call.kwargs["intent_generation"]
            for call in container.reconcile_workspace_cleanup_intent.await_args_list
        ] == [7, 7]
        container._delete_pvc.assert_not_awaited()
        container._delete_service.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_terminal_row_without_runtime_receipt_is_retained(self):
        mgr, container, _, _, _ = _make_manager(
            pods=[],
            job_rows={
                "jlegacy": {
                    "status": "completed",
                    "execution_lane": "pinned",
                    "context": {"workspace_container": {"provisioner": "k8s"}},
                }
            },
        )
        self._wire_pvcs(container, [_make_pvc("pvc-workspace-jlegacy", "jlegacy")])

        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            assert await mgr.reap_orphans() == 0

        container.prepare_workspace_cleanup_intent.assert_not_awaited()
        container._delete_pvc.assert_not_awaited()
        container._delete_service.assert_not_awaited()


class TestReapOrphanSessionPvcs:
    """Missing session owners are retained until exact teardown proof exists."""

    @staticmethod
    def _wire_pvcs(container, pvcs):
        pvc_list = MagicMock()
        pvc_list.items = pvcs
        container._core_api.list_namespaced_persistent_volume_claim.return_value = (
            pvc_list
        )
        container._delete_pvc = AsyncMock(return_value=True)

    @pytest.mark.asyncio
    async def test_retains_workspace_pvc_whose_thread_row_is_gone(self):
        mgr, container, _, _, _ = _make_manager(pods=[], thread_rows={})
        self._wire_pvcs(
            container, [_make_pvc("pvc-ws-thread-tgone", thread_id="tgone")]
        )
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            n = await mgr.reap_orphans()
        assert n == 0
        container.prepare_workspace_cleanup_intent.assert_not_awaited()
        container._delete_pvc.assert_not_awaited()
        container._delete_service.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_retains_session_agent_pvc_whose_thread_row_is_gone(self):
        mgr, container, _, _, _ = _make_manager(pods=[], thread_rows={})
        self._wire_pvcs(container, [_make_pvc("pvc-agent-s-tgone", thread_id="tgone")])
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            n = await mgr.reap_orphans()
        assert n == 0
        container._delete_pvc.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_both_claims_of_one_missing_thread_remain_fail_closed(self):
        mgr, container, _, _, _ = _make_manager(pods=[], thread_rows={})
        self._wire_pvcs(
            container,
            [
                _make_pvc("pvc-ws-thread-tgone", thread_id="tgone"),
                _make_pvc("pvc-agent-s-tgone", thread_id="tgone"),
            ],
        )
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            n = await mgr.reap_orphans()
        assert n == 0
        container._delete_pvc.assert_not_awaited()
        container._delete_service.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_skips_pvc_whose_thread_still_exists(self):
        """An 'ended' thread is still a row, and a row means resumable.

        This is the sweep-side twin of
        TestDeleteTerminalPvc::test_ended_thread_keeps_its_pvc_because_it_is_resumable
        — status is not even read here, existence is the whole test.
        """
        mgr, container, _, _, _ = _make_manager(
            pods=[], thread_rows={"tended": {"id": "tended", "status": "ended"}}
        )
        self._wire_pvcs(
            container, [_make_pvc("pvc-ws-thread-tended", thread_id="tended")]
        )
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            n = await mgr.reap_orphans()
        assert n == 0
        container._delete_pvc.assert_not_called()
        container._delete_service.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_pvc_with_a_live_workspace_pod(self):
        """Never yank a volume out from under a running pod — the instance path
        owns that teardown."""
        pod = _make_pod(
            "ws-thread-tlive", labels={"srw/thread-id": "tlive-full-uuid-value"}
        )
        mgr, container, _, _, _ = _make_manager(pods=[pod], thread_rows={})
        self._wire_pvcs(
            container,
            [_make_pvc("pvc-ws-thread-tlive", thread_id="tlive-full-uuid-value")],
        )
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            n = await mgr.reap_orphans()
        assert n == 0
        container._delete_pvc.assert_not_called()

    @pytest.mark.asyncio
    async def test_live_pod_match_survives_the_12_char_truncation(self):
        """A pod label carries the full uuid; an unlabeled claim only has the
        12 chars in its name. The live check has to compare both spellings, or
        a legacy claim would be reaped while its pod is still mounting it."""
        pod = _make_pod(
            "ws-thread-tlive", labels={"srw/thread-id": "tlive-full-uuid-value"}
        )
        mgr, container, _, _, _ = _make_manager(pods=[pod], thread_rows={})
        self._wire_pvcs(container, [_make_pvc("pvc-ws-thread-tlive-full-u")])
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            n = await mgr.reap_orphans()
        assert n == 0
        container._delete_pvc.assert_not_called()

    @pytest.mark.asyncio
    async def test_db_lookup_failure_does_not_delete(self):
        """``_thread_row_exists`` → None (lookup failed) must read as "unknown",
        never as "gone". A transient DB blip that deleted every session volume
        in the namespace is the worst outcome this module can produce."""
        mgr, container, _, _, db = _make_manager(pods=[])
        db.acquire = MagicMock(side_effect=RuntimeError("db down"))
        self._wire_pvcs(container, [_make_pvc("pvc-ws-thread-terr", thread_id="terr")])
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            n = await mgr.reap_orphans()
        assert n == 0
        container._delete_pvc.assert_not_called()

    @pytest.mark.asyncio
    async def test_thread_row_exists_is_three_way(self):
        mgr, _, _, _, db = _make_manager(
            thread_rows={"tlive": {"id": "tlive", "status": "ended"}}
        )
        assert await mgr._thread_row_exists("tlive") is True
        assert await mgr._thread_row_exists("tgone") is False
        db.acquire = MagicMock(side_effect=RuntimeError("db down"))
        assert await mgr._thread_row_exists("tlive") is None

    @pytest.mark.asyncio
    async def test_unlabeled_session_claim_is_not_authority_to_delete(self):
        mgr, container, _, _, _ = _make_manager(pods=[], thread_rows={})
        self._wire_pvcs(container, [_make_pvc("pvc-ws-thread-tgone")])
        with patch(
            "orchestrator.services.lifecycle.workspace_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            n = await mgr.reap_orphans()
        assert n == 0
        container.prepare_workspace_cleanup_intent.assert_not_awaited()
        container._delete_pvc.assert_not_awaited()


def test_pod_volume_is_ephemeral_helper():
    from orchestrator.services.lifecycle.workspace_manager import (
        _pod_volume_is_ephemeral,
    )

    empty = _make_pod("w1")
    vol_e = MagicMock()
    vol_e.name = "workspace-data"
    vol_e.persistent_volume_claim = None
    vol_e.empty_dir = MagicMock()
    empty.spec.volumes = [vol_e]
    assert _pod_volume_is_ephemeral(empty) is True

    pvc = _make_pod("w2")
    vol_p = MagicMock()
    vol_p.name = "workspace-data"
    vol_p.persistent_volume_claim = MagicMock()
    pvc.spec.volumes = [vol_p]
    assert _pod_volume_is_ephemeral(pvc) is False


# =============================================================================
# snapshot / restore
# =============================================================================


class TestSnapshot:
    @pytest.mark.asyncio
    async def test_kubernetes_capture_refused_before_snapshot_service(self):
        mgr, _, _, snapshot, db = _make_manager()
        inst = Instance(
            kind="workspace",
            id="workspace-abc",
            bound_to="job-uuid-1",
            metadata={"pod_ip": "10.0.0.5"},
        )
        ref = await mgr.snapshot(inst)
        assert ref is None
        snapshot.capture_vm_snapshot.assert_not_awaited()
        db.merge_workspace_container_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_returns_none_without_pod_ip(self):
        mgr, _, _, snapshot, _ = _make_manager()
        inst = Instance(kind="workspace", id="x", bound_to="job1", metadata={})
        assert await mgr.snapshot(inst) is None
        snapshot.capture_vm_snapshot.assert_not_called()

    @pytest.mark.asyncio
    async def test_returns_none_when_snapshot_unavailable(self):
        mgr, *_ = _make_manager(snapshot_available=False)
        inst = Instance(
            kind="workspace",
            id="x",
            bound_to="job1",
            metadata={"pod_ip": "10.0.0.5"},
        )
        assert await mgr.snapshot(inst) is None


class TestRestore:
    @pytest.mark.asyncio
    async def test_restore_job_workspace(self):
        mgr, _, suspension, _, _ = _make_manager()
        inst = Instance(
            kind="workspace",
            id="workspace-abc",
            metadata={
                "labels": {"srw/job-id": "job1"},
                "pod_uid": "11111111-1111-4111-8111-111111111111",
            },
        )
        await mgr.restore(inst, "job1")
        suspension.restore_workspace.assert_awaited_once_with("job1")
        suspension.restore_thread_workspace.assert_not_called()

    @pytest.mark.asyncio
    async def test_restore_thread_workspace(self):
        mgr, _, suspension, _, _ = _make_manager()
        inst = Instance(
            kind="workspace",
            id="ws-thread-xyz",
            metadata={
                "labels": {"srw/thread-id": "t1"},
                "pod_uid": "11111111-1111-4111-8111-111111111111",
            },
        )
        await mgr.restore(inst, "t1")
        suspension.restore_thread_workspace.assert_awaited_once_with("t1")
        suspension.restore_workspace.assert_not_called()


# =============================================================================
# drain / delete
# =============================================================================


class TestSignalDrainPending:
    @pytest.mark.asyncio
    async def test_is_noop(self):
        # Workspaces have no in-pod drain hook. The soft signal is a
        # no-op; drift drain happens once the bound work goes idle.
        mgr, container, suspension, snapshot, db = _make_manager()
        inst = Instance(kind="workspace", id="x", bound_to="job1", metadata={})
        await mgr.signal_drain_pending(inst)
        container.delete_workspace_with_outcome.assert_not_called()
        suspension.restore_workspace.assert_not_called()
        snapshot.capture_vm_snapshot.assert_not_called()
        db.acquire.assert_not_called()


class TestDrain:
    @pytest.mark.asyncio
    async def test_job_drain_calls_delete_workspace(self):
        mgr, container, *_ = _make_manager()
        inst = Instance(
            kind="workspace",
            id="workspace-abc",
            bound_to="job1",
            metadata={
                "labels": {"srw/job-id": "job1"},
                "pod_uid": "11111111-1111-4111-8111-111111111111",
            },
        )
        await mgr.drain(inst, grace_s=10)
        _assert_exact_workspace_delete(container, WorkspaceOwner.job("job1"))

    @pytest.mark.asyncio
    async def test_thread_drain_calls_delete_workspace_with_session_owner(self):
        mgr, container, *_ = _make_manager()
        inst = Instance(
            kind="workspace",
            id="ws-thread-xyz",
            bound_to="t1",
            metadata={
                "labels": {"srw/thread-id": "t1"},
                "pod_uid": "11111111-1111-4111-8111-111111111111",
            },
        )
        await mgr.drain(inst, grace_s=10)
        _assert_exact_workspace_delete(container, WorkspaceOwner.session("t1"))

    @pytest.mark.asyncio
    async def test_drain_skipped_without_bound(self):
        mgr, container, *_ = _make_manager()
        inst = Instance(kind="workspace", id="x", bound_to=None, metadata={})
        await mgr.drain(inst, grace_s=10)
        container.delete_workspace_with_outcome.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("state", "expected_settled"),
        [("superseded", True), ("retryable", False)],
    )
    async def test_typed_delete_preserves_successor_runtime(
        self, state: str, expected_settled: bool
    ):
        mgr, container, _, _, db = _make_manager()
        container.reconcile_workspace_cleanup_intent.return_value = (
            WorkspaceCleanupOutcome(state, 7)
        )
        inst = Instance(
            kind="workspace",
            id="workspace-abc",
            bound_to="job1",
            metadata={
                "labels": {"srw/job-id": "job1"},
                "pod_uid": "11111111-1111-4111-8111-111111111111",
                "job_status": "paused",
                "volume_ephemeral": True,
            },
        )

        assert await mgr._delete_owned(inst, grace_s=0) is expected_settled

        _assert_exact_workspace_delete(container, WorkspaceOwner.job("job1"))
        container.delete_workspace_pvc.assert_not_awaited()
        container._delete_service.assert_not_awaited()
        db.merge_workspace_container_context_if_runtime.assert_not_awaited()
        db.merge_workspace_container_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_settled_cleanup_projects_only_inside_atomic_intent_settlement(self):
        mgr, container, _, _, db = _make_manager()
        inst = Instance(
            kind="workspace",
            id="workspace-abc",
            bound_to="job1",
            metadata={
                "labels": {"srw/job-id": "job1"},
                "pod_uid": "11111111-1111-4111-8111-111111111111",
                "job_status": "paused",
                "volume_ephemeral": True,
            },
        )

        assert await mgr._delete_owned(inst, grace_s=0) is True

        _assert_exact_workspace_delete(container, WorkspaceOwner.job("job1"))
        db.merge_workspace_container_context_if_runtime.assert_not_awaited()
        db.merge_workspace_container_context.assert_not_awaited()


class TestDeleteNoopWhenK8sUnavailable:
    @pytest.mark.asyncio
    async def test_noop(self):
        mgr, container, *_ = _make_manager(k8s_available=False)
        inst = Instance(
            kind="workspace",
            id="x",
            bound_to="job1",
            metadata={"labels": {"srw/job-id": "job1"}},
        )
        await mgr.delete(inst, grace_s=10)
        container.delete_workspace_with_outcome.assert_not_called()
