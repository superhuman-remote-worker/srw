"""Tests for AgentInstanceManager and the reconciler tick algorithm.

Covers Phase 1b: drift detection, drain writes, reconciliation tick,
disruption budget enforcement.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.services.lifecycle import (
    AgentInstanceManager,
    DisruptionBudget,
    Instance,
    InstanceLifecycleReconciler,
    expected_agent_shas,
)


# =============================================================================
# Helpers
# =============================================================================


def _make_pod(name: str, phase: str = "Running", labels: dict | None = None):
    pod = MagicMock()
    pod.metadata.name = name
    pod.metadata.labels = labels or {}
    pod.metadata.creation_timestamp = datetime.now(timezone.utc)
    pod.status.phase = phase
    return pod


def _make_manager(
    pods: list | None = None,
    agent_rows: list[dict] | None = None,
    k8s_available: bool = True,
):
    """Build an AgentInstanceManager wrapping mocked provisioner + db."""
    provisioner = MagicMock()
    provisioner._k8s_available = k8s_available
    provisioner._namespace = "test-ns"
    provisioner._core_api = MagicMock()
    pod_list = MagicMock()
    pod_list.items = pods or []
    provisioner._core_api.list_namespaced_pod.return_value = pod_list
    provisioner.delete_agent_pod = AsyncMock(return_value=True)

    db = AsyncMock()
    db.acquire = MagicMock()
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=agent_rows or [])
    conn.execute = AsyncMock(return_value="UPDATE 1")
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    db.acquire.return_value = ctx

    mgr = AgentInstanceManager(provisioner=provisioner, db=db)
    return mgr, provisioner, db, conn


async def _fake_to_thread(fn, *args, **kwargs):
    return fn(*args, **kwargs)


# =============================================================================
# expected_agent_shas (module-level helper)
# =============================================================================


class TestExpectedAgentShas:
    def test_returns_empty_when_no_env_vars(self, monkeypatch):
        monkeypatch.delenv("AGENT_IMAGE", raising=False)
        monkeypatch.delenv("PERSISTENT_AGENT_IMAGE", raising=False)
        assert expected_agent_shas() == set()

    def test_returns_empty_when_tags_lack_sha_prefix(self, monkeypatch):
        monkeypatch.setenv("AGENT_IMAGE", "registry/agent:latest")
        monkeypatch.delenv("PERSISTENT_AGENT_IMAGE", raising=False)
        assert expected_agent_shas() == set()

    def test_extracts_single_sha(self, monkeypatch):
        monkeypatch.setenv("AGENT_IMAGE", "registry/agent:sha-abc1234")
        monkeypatch.delenv("PERSISTENT_AGENT_IMAGE", raising=False)
        assert expected_agent_shas() == {"abc1234"}

    def test_extracts_both_when_distinct(self, monkeypatch):
        monkeypatch.setenv("AGENT_IMAGE", "registry/agent:sha-worker1")
        monkeypatch.setenv("PERSISTENT_AGENT_IMAGE", "registry/agent:sha-persistent2")
        assert expected_agent_shas() == {"worker1", "persistent2"}

    def test_dedupes_when_both_same(self, monkeypatch):
        monkeypatch.setenv("AGENT_IMAGE", "registry/agent:sha-shared")
        monkeypatch.setenv("PERSISTENT_AGENT_IMAGE", "registry/agent:sha-shared")
        assert expected_agent_shas() == {"shared"}


# =============================================================================
# AgentInstanceManager.list_instances
# =============================================================================


class TestListInstances:
    @pytest.mark.asyncio
    async def test_empty_when_k8s_unavailable(self):
        mgr, _, _, _ = _make_manager(k8s_available=False)
        assert await mgr.list_instances() == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("image", "approved", "build_sha", "expected_version"),
        [
            (None, (), "old", "old"),
            ("srw-agent:sha-current", (), "old", "old"),
            # previous release: same repository, old tag, stale build_sha
            ("srw-agent:sha-previous", (), "old", "old"),
            (
                "registry.example/custom:1",
                ({"image": "registry.example/custom:1"},),
                "old",
                None,
            ),
            ("registry.example/other:1", (), "old", "ineligible-image"),
        ],
    )
    async def test_version_follows_image_class(
        self, monkeypatch, image, approved, build_sha, expected_version
    ):
        monkeypatch.setenv("AGENT_IMAGE", "srw-agent:sha-current")
        rows = [
            {
                "id": "a1",
                "hostname": "p1",
                "status": "ready",
                "current_job_id": None,
                "thread_id": None,
                "metadata": {"build_sha": build_sha},
                "agent_mode": "worker",
                "last_heartbeat": None,
                "image": image,
            }
        ]
        mgr, _, db, _ = _make_manager(pods=[_make_pod("p1")], agent_rows=rows)
        db.manifest_approved_images = approved
        with patch(
            "orchestrator.services.lifecycle.agent_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            (inst,) = await mgr.list_instances()
        assert inst.version == expected_version
        from orchestrator.services.lifecycle.reconciler import (
            InstanceLifecycleReconciler,
        )

        drifts = InstanceLifecycleReconciler.is_drift(inst, {"current"})
        assert drifts is (expected_version is not None)

    @pytest.mark.asyncio
    async def test_joins_pod_with_db_row(self):
        pods = [_make_pod("srw-agent-j-x")]
        rows = [
            {
                "id": "11111111-1111-1111-1111-111111111111",
                "hostname": "srw-agent-j-x",
                "status": "ready",
                "current_job_id": None,
                "thread_id": None,
                "metadata": {"build_sha": "current"},
                "agent_mode": "worker",
                "last_heartbeat": datetime.now(timezone.utc),
            }
        ]
        mgr, _, _, _ = _make_manager(pods=pods, agent_rows=rows)
        with patch(
            "orchestrator.services.lifecycle.agent_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            instances = await mgr.list_instances()
        assert len(instances) == 1
        inst = instances[0]
        assert inst.kind == "agent"
        assert inst.id == "srw-agent-j-x"
        assert inst.version == "current"
        assert inst.bound_to is None
        assert inst.metadata["row_present"] is True
        assert inst.metadata["status"] == "ready"

    @pytest.mark.asyncio
    async def test_falls_back_to_label_sha_when_db_row_missing(self):
        # A pod that registered too recently for its agents row to exist
        # yet — version comes from the srw/build-sha pod label set by
        # _build_pod_manifest (Phase 1a addition).
        pods = [_make_pod("srw-agent-j-fresh", labels={"srw/build-sha": "from-label"})]
        mgr, _, _, _ = _make_manager(pods=pods, agent_rows=[])
        with patch(
            "orchestrator.services.lifecycle.agent_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            instances = await mgr.list_instances()
        assert len(instances) == 1
        assert instances[0].version == "from-label"
        assert instances[0].metadata["row_present"] is False

    @pytest.mark.asyncio
    async def test_metadata_pulls_thread_id_when_session(self):
        pods = [_make_pod("srw-agent-s-y")]
        rows = [
            {
                "id": "11111111-1111-1111-1111-111111111111",
                "hostname": "srw-agent-s-y",
                "status": "session",
                "current_job_id": None,
                "thread_id": "22222222-2222-2222-2222-222222222222",
                "metadata": {"build_sha": "v1"},
                "agent_mode": "persistent",
                "last_heartbeat": datetime.now(timezone.utc),
            }
        ]
        mgr, _, _, _ = _make_manager(pods=pods, agent_rows=rows)
        with patch(
            "orchestrator.services.lifecycle.agent_manager.asyncio.to_thread",
            side_effect=_fake_to_thread,
        ):
            instances = await mgr.list_instances()
        assert instances[0].bound_to == "22222222-2222-2222-2222-222222222222"


# =============================================================================
# Idle / health predicates
# =============================================================================


class TestIsIdle:
    @pytest.mark.asyncio
    async def test_ready_no_job_no_thread_is_idle(self):
        mgr, _, _, _ = _make_manager()
        inst = Instance(
            kind="agent",
            id="x",
            metadata={
                "row_present": True,
                "status": "ready",
                "current_job_id": None,
                "thread_id": None,
            },
        )
        assert await mgr.is_idle(inst) is True

    @pytest.mark.asyncio
    async def test_working_is_not_idle(self):
        mgr, _, _, _ = _make_manager()
        inst = Instance(
            kind="agent",
            id="x",
            metadata={
                "row_present": True,
                "status": "working",
                "current_job_id": "j",
                "thread_id": None,
            },
        )
        assert await mgr.is_idle(inst) is False

    @pytest.mark.asyncio
    async def test_session_is_not_idle(self):
        mgr, _, _, _ = _make_manager()
        inst = Instance(
            kind="agent",
            id="x",
            metadata={
                "row_present": True,
                "status": "session",
                "current_job_id": None,
                "thread_id": "t",
            },
        )
        assert await mgr.is_idle(inst) is False

    @pytest.mark.asyncio
    async def test_no_db_row_is_not_idle(self):
        # No row → can't safely claim idle; conservatively false.
        mgr, _, _, _ = _make_manager()
        inst = Instance(kind="agent", id="x", metadata={"row_present": False})
        assert await mgr.is_idle(inst) is False


# =============================================================================
# drain / delete
# =============================================================================


class TestSignalDrainPending:
    """Phase 1e: soft drain signal — intent only, no status flip."""

    @pytest.mark.asyncio
    async def test_writes_intent_only(self):
        mgr, _, _, conn = _make_manager()
        inst = Instance(
            kind="agent",
            id="srw-agent-j-x",
            version="old",
            metadata={"agent_id": "11111111-1111-1111-1111-111111111111"},
        )
        await mgr.signal_drain_pending(inst)
        assert conn.execute.call_count == 1
        sql = conn.execute.call_args[0][0]
        assert "intents = intents" in sql
        # Distinguishes from drain(): no status CASE expression.
        assert "CASE" not in sql
        assert "should_drain" in conn.execute.call_args[0][1]

    @pytest.mark.asyncio
    async def test_skipped_when_no_agent_id(self):
        mgr, _, _, conn = _make_manager()
        inst = Instance(kind="agent", id="x", metadata={})
        await mgr.signal_drain_pending(inst)
        assert conn.execute.call_count == 0


class TestDrain:
    @pytest.mark.asyncio
    async def test_writes_intent_and_status_for_idle(self):
        mgr, _, _, conn = _make_manager()
        inst = Instance(
            kind="agent",
            id="srw-agent-j-x",
            version="old",
            metadata={"agent_id": "11111111-1111-1111-1111-111111111111"},
        )
        await mgr.drain(inst, grace_s=0)
        # The single UPDATE writes both intents (always) and status
        # (CASE — only flips if currently 'ready').
        assert conn.execute.call_count == 1
        sql = conn.execute.call_args[0][0]
        assert "intents = intents" in sql
        assert "should_drain" in conn.execute.call_args[0][1]
        assert "CASE" in sql

    @pytest.mark.asyncio
    async def test_skipped_when_no_agent_id(self):
        mgr, _, _, conn = _make_manager()
        inst = Instance(kind="agent", id="x", metadata={})
        await mgr.drain(inst, grace_s=0)
        assert conn.execute.call_count == 0


class TestDelete:
    @pytest.mark.asyncio
    async def test_delegates_to_provisioner(self):
        mgr, provisioner, _, _ = _make_manager()
        inst = Instance(
            kind="agent",
            id="srw-agent-j-x",
            metadata={"pod_uid": "11111111-1111-4111-8111-111111111111"},
        )
        await mgr.delete(inst, grace_s=0)
        provisioner.delete_agent_pod.assert_awaited_once_with(
            "srw-agent-j-x",
            expected_pod_uid="11111111-1111-4111-8111-111111111111",
        )

    @pytest.mark.asyncio
    async def test_noop_when_k8s_unavailable(self):
        mgr, provisioner, _, _ = _make_manager(k8s_available=False)
        inst = Instance(kind="agent", id="srw-agent-j-x")
        await mgr.delete(inst, grace_s=0)
        provisioner.delete_agent_pod.assert_not_called()


# =============================================================================
# Reconciler tick
# =============================================================================


class _ScriptedManager:
    """Minimal manager double driven by canned values, for tick tests."""

    def __init__(
        self,
        *,
        kind: str = "agent",
        expected: set[str] | None = None,
        instances: list[Instance] | None = None,
        idle_ids: set[str] | None = None,
        unhealthy_ids: set[str] | None = None,
    ):
        self.kind = kind
        self._expected = expected or set()
        self._instances = instances or []
        self._idle_ids = idle_ids or set()
        self._unhealthy_ids = unhealthy_ids or set()
        self.drain_calls: list[str] = []
        self.delete_calls: list[str] = []
        self.signal_calls: list[str] = []

    async def expected_versions(self):
        return self._expected

    async def list_instances(self):
        return list(self._instances)

    async def is_healthy(self, inst):
        return inst.id not in self._unhealthy_ids

    async def is_idle(self, inst):
        return inst.id in self._idle_ids

    async def signal_drain_pending(self, inst):
        self.signal_calls.append(inst.id)

    async def drain(self, inst, grace_s):
        self.drain_calls.append(inst.id)

    async def delete(self, inst, grace_s):
        self.delete_calls.append(inst.id)


class TestReconcilerTick:
    @pytest.mark.asyncio
    async def test_drains_drifted_idle_instance(self):
        mgr = _ScriptedManager(
            expected={"new"},
            instances=[Instance(kind="agent", id="a", version="old")],
            idle_ids={"a"},
        )
        reconciler = InstanceLifecycleReconciler(managers=[mgr])
        report = await reconciler.tick()
        assert mgr.drain_calls == ["a"]
        assert report["agent"] == {
            "listed": 1,
            "unhealthy": 0,
            "drift": 1,
            "drained": 1,
            "skipped_busy": 0,
            "reaped": 0,
            "reap_attempts": 0,
            "reap_forced": 0,
            "orphans_reaped": 0,
        }

    @pytest.mark.asyncio
    async def test_drift_on_busy_records_skipped_busy(self):
        mgr = _ScriptedManager(
            expected={"new"},
            instances=[Instance(kind="agent", id="busy", version="old")],
            idle_ids=set(),
        )
        reconciler = InstanceLifecycleReconciler(managers=[mgr])
        report = await reconciler.tick()
        assert mgr.drain_calls == []
        # Phase 1e: busy drifted instances still get the soft signal
        # so the in-pod handler can react at the next safe boundary.
        assert mgr.signal_calls == ["busy"]
        assert report["agent"]["drift"] == 1
        assert report["agent"]["skipped_busy"] == 1

    @pytest.mark.asyncio
    async def test_signal_fires_on_idle_drift_too(self):
        # Both signal AND drain on idle drift — drain calls
        # signal_drain_pending implicitly via the agent manager's own
        # implementation, but the reconciler also fires the explicit
        # signal beforehand. Two writes in production; the second is a
        # no-op due to JSONB merge idempotency.
        mgr = _ScriptedManager(
            expected={"new"},
            instances=[Instance(kind="agent", id="x", version="old")],
            idle_ids={"x"},
        )
        reconciler = InstanceLifecycleReconciler(managers=[mgr])
        await reconciler.tick()
        assert mgr.signal_calls == ["x"]
        assert mgr.drain_calls == ["x"]

    @pytest.mark.asyncio
    async def test_signal_does_not_fire_without_drift(self):
        mgr = _ScriptedManager(
            expected={"v1"},
            instances=[Instance(kind="agent", id="ok", version="v1")],
            idle_ids={"ok"},
        )
        reconciler = InstanceLifecycleReconciler(managers=[mgr])
        await reconciler.tick()
        assert mgr.signal_calls == []
        assert mgr.drain_calls == []

    @pytest.mark.asyncio
    async def test_signal_exception_does_not_break_tick(self):
        mgr = _ScriptedManager(
            expected={"new"},
            instances=[Instance(kind="agent", id="boom", version="old")],
            idle_ids={"boom"},
        )

        async def explode(inst):
            raise RuntimeError("intent write blew up")

        mgr.signal_drain_pending = explode  # type: ignore[assignment]
        reconciler = InstanceLifecycleReconciler(managers=[mgr])
        report = await reconciler.tick()
        # Tick continued past the failure; drain still attempted on idle.
        assert mgr.drain_calls == ["boom"]
        assert report["agent"]["drained"] == 1

    @pytest.mark.asyncio
    async def test_no_drift_when_version_matches(self):
        mgr = _ScriptedManager(
            expected={"v1"},
            instances=[Instance(kind="agent", id="a", version="v1")],
            idle_ids={"a"},
        )
        reconciler = InstanceLifecycleReconciler(managers=[mgr])
        report = await reconciler.tick()
        assert mgr.drain_calls == []
        assert report["agent"]["drift"] == 0

    @pytest.mark.asyncio
    async def test_no_drift_when_expected_empty(self):
        # Local dev with :latest — expected_versions returns empty, so
        # the reconciler skips drift checks for the kind.
        mgr = _ScriptedManager(
            expected=set(),
            instances=[Instance(kind="agent", id="a", version="anything")],
            idle_ids={"a"},
        )
        reconciler = InstanceLifecycleReconciler(managers=[mgr])
        report = await reconciler.tick()
        assert mgr.drain_calls == []
        assert report["agent"]["drift"] == 0

    @pytest.mark.asyncio
    async def test_disruption_budget_caps_drains_per_tick(self):
        # 8 drifted idle instances. Default budget is max(1, total // 4)
        # = max(1, 2) = 2. So at most 2 drains per tick.
        instances = [
            Instance(kind="agent", id=f"a{i}", version="old") for i in range(8)
        ]
        mgr = _ScriptedManager(
            expected={"new"},
            instances=instances,
            idle_ids={i.id for i in instances},
        )
        reconciler = InstanceLifecycleReconciler(managers=[mgr])
        report = await reconciler.tick()
        assert len(mgr.drain_calls) == 2
        assert report["agent"]["drained"] == 2
        # All 8 still counted as drift — only the actuation is capped.
        assert report["agent"]["drift"] == 8

    @pytest.mark.asyncio
    async def test_per_kind_budget_override(self):
        instances = [
            Instance(kind="agent", id=f"a{i}", version="old") for i in range(8)
        ]
        mgr = _ScriptedManager(
            expected={"new"},
            instances=instances,
            idle_ids={i.id for i in instances},
        )
        budget = DisruptionBudget(overrides={"agent": 1})
        reconciler = InstanceLifecycleReconciler(managers=[mgr], budget=budget)
        report = await reconciler.tick()
        assert report["agent"]["drained"] == 1

    @pytest.mark.asyncio
    async def test_unhealthy_instance_is_deleted(self):
        # Phase 2b: unhealthy instances get force-deleted before drift
        # consideration. Closes the workspace crash-recovery gap.
        mgr = _ScriptedManager(
            expected={"v1"},
            instances=[Instance(kind="agent", id="dead", version="v1")],
            idle_ids=set(),
            unhealthy_ids={"dead"},
        )
        reconciler = InstanceLifecycleReconciler(managers=[mgr])
        report = await reconciler.tick()
        assert mgr.delete_calls == ["dead"]
        assert report["agent"]["unhealthy"] == 1
        # Unhealthy instance is deleted, not drained — drift stats untouched.
        assert report["agent"]["drift"] == 0
        assert report["agent"]["drained"] == 0

    @pytest.mark.asyncio
    async def test_unhealthy_supersedes_drift(self):
        # If both unhealthy AND drifted, only the unhealthy delete fires.
        mgr = _ScriptedManager(
            expected={"new"},
            instances=[Instance(kind="agent", id="d", version="old")],
            idle_ids={"d"},
            unhealthy_ids={"d"},
        )
        reconciler = InstanceLifecycleReconciler(managers=[mgr])
        report = await reconciler.tick()
        assert mgr.delete_calls == ["d"]
        assert mgr.drain_calls == []
        assert report["agent"]["unhealthy"] == 1
        assert report["agent"]["drift"] == 0

    @pytest.mark.asyncio
    async def test_unhealthy_delete_failure_does_not_break_tick(self):
        instances = [
            Instance(kind="agent", id="boom", version="v1"),
            Instance(kind="agent", id="ok", version="v1"),
        ]
        mgr = _ScriptedManager(
            expected={"v1"},
            instances=instances,
            unhealthy_ids={"boom"},
        )

        async def explode(inst, grace_s):
            mgr.delete_calls.append(inst.id)
            raise RuntimeError("k8s on fire")

        mgr.delete = explode  # type: ignore[assignment]
        reconciler = InstanceLifecycleReconciler(managers=[mgr])
        report = await reconciler.tick()
        # The exception didn't bring down the tick; both instances were
        # observed (one delete attempted, the other healthy).
        assert mgr.delete_calls == ["boom"]
        assert report["agent"]["unhealthy"] == 1
        assert report["agent"]["listed"] == 2

    @pytest.mark.asyncio
    async def test_is_healthy_exception_treats_as_healthy(self):
        # Defense in depth: if is_healthy raises, the reconciler must not
        # mass-delete by treating the failure as 'unhealthy'. Conservative.
        mgr = _ScriptedManager(
            expected={"new"},
            instances=[Instance(kind="agent", id="x", version="old")],
            idle_ids={"x"},
        )

        async def boom(inst):
            raise RuntimeError("flaky check")

        mgr.is_healthy = boom  # type: ignore[assignment]
        reconciler = InstanceLifecycleReconciler(managers=[mgr])
        report = await reconciler.tick()
        # Treated as healthy → falls through to drift, drains normally.
        assert mgr.delete_calls == []
        assert mgr.drain_calls == ["x"]
        assert report["agent"]["unhealthy"] == 0

    @pytest.mark.asyncio
    async def test_manager_exception_is_isolated(self):
        bad = _ScriptedManager(kind="bad")

        async def boom():
            raise RuntimeError("cluster on fire")

        bad.list_instances = boom  # type: ignore[assignment]

        good = _ScriptedManager(
            kind="agent",
            expected={"new"},
            instances=[Instance(kind="agent", id="a", version="old")],
            idle_ids={"a"},
        )
        reconciler = InstanceLifecycleReconciler(managers=[bad, good])
        report = await reconciler.tick()
        # bad kind reports zero stats but doesn't break the tick
        assert report["bad"]["listed"] == 0
        assert report["agent"]["drained"] == 1
