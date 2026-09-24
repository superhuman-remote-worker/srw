"""Stateless capacity read + pod-deletion-cost reconciler
(knowledge-base/knowledge/features/capacity_ux_and_queue_autoscaling.md §2).

The capacity SQL must carry the claim's own predicates so KEDA, the admin
endpoint, and the executor agree; the reconciler patches only pods whose
annotation differs, contains per-pod errors, is leader-gated on
STATELESS_DELETION_COST_ID, and idles without crashing when Kubernetes is
unreachable. The route is admin-only and actually mounted.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

os.environ.setdefault("VECTOR_DB_URL", "postgresql://localhost/test")

import pytest
from fastapi import HTTPException

from orchestrator.database.lock_ids import (
    LEADER_ID,
    RUN_QUEUE_REAPER_ID,
    STATELESS_DELETION_COST_ID,
)
from orchestrator.services import stateless_capacity as cap
from orchestrator.services import stateless_pod_deletion_cost as dc

NOW = datetime(2026, 9, 8, tzinfo=UTC)


# --------------------------------------------------------------------------- #
# Demand arithmetic + SQL predicates
# --------------------------------------------------------------------------- #


def _demand_row(**overrides):
    row = {
        "observed_at": NOW,
        "busy": 0,
        "runnable_session_turn": 0,
        "runnable_worker_batch": 0,
        "runnable_bg_task": 0,
        "runnable_total": 0,
        "oldest_queued_age_s": None,
    }
    row.update(overrides)
    return row


class _Conn:
    def __init__(self, row=None, busy_rows=(), parked_rows=()):
        self.row = row if row is not None else _demand_row()
        self.busy_rows = list(busy_rows)
        self.parked_rows = list(parked_rows)
        self.queries: list[str] = []

    async def fetchrow(self, query, *_args):
        self.queries.append(query)
        return self.row

    async def fetch(self, query, *_args):
        self.queries.append(query)
        # The snapshot reads two lists on one connection: live leases (busy
        # pods) and the parked worklist. Dispatch on the statement.
        if "state = 'parked'" in query:
            return self.parked_rows
        return self.busy_rows


@pytest.mark.asyncio
async def test_demand_sql_uses_the_claim_predicates_verbatim():
    conn = _Conn()
    await cap.read_demand(conn)
    (sql,) = conn.queries
    assert "FROM run_queue" in sql
    assert cap.BUSY_PREDICATE == "state = 'leased' AND leased_until > now()"
    assert cap.RUNNABLE_PREDICATE == "state = 'queued' AND run_after <= now()"
    assert sql.count(cap.BUSY_PREDICATE) == 1
    # Three kinds, total, and oldest age all use the same readiness predicate.
    assert sql.count(cap.RUNNABLE_PREDICATE) == 5
    assert "unit_kind = 'session_turn'" in sql
    assert "unit_kind = 'worker_batch'" in sql
    assert "unit_kind = 'bg_task'" in sql
    # Never a parked, done, or expired-lease row.
    assert "'parked'" not in sql and "'done'" not in sql


@pytest.mark.asyncio
async def test_busy_pod_names_reads_live_leases_only():
    conn = _Conn(
        busy_rows=[{"leased_by": "pod-a"}, {"leased_by": "pod-b"}, {"leased_by": None}]
    )
    names = await cap.busy_pod_names(conn)
    assert names == {"pod-a", "pod-b"}
    (sql,) = conn.queries
    assert cap.BUSY_PREDICATE in sql and "leased_by IS NOT NULL" in sql


def test_desired_is_floor_when_idle_and_busy_plus_runnable_plus_reserve_when_loaded():
    params = cap.CapacityParams(min_replicas=2, reserve=1)
    idle = cap.QueueDemand(NOW, 0, 0, 0, 0, None)
    assert cap.desired_replicas(idle, params) == 2
    one_busy = cap.QueueDemand(NOW, 1, 0, 0, 0, None)
    assert cap.desired_replicas(one_busy, params) == 2
    loaded = cap.QueueDemand(NOW, 2, 1, 1, 2, 12.5)
    assert cap.desired_replicas(loaded, params) == 5
    # Floor 0 (a scale-to-zero worker pool) still adds the reserve.
    assert cap.desired_replicas(idle, cap.CapacityParams(0, 1)) == 1


def test_params_from_env_defaults_and_clamps(monkeypatch):
    monkeypatch.delenv("STATELESS_AUTOSCALE_MIN_REPLICAS", raising=False)
    monkeypatch.delenv("STATELESS_AUTOSCALE_RESERVE", raising=False)
    assert cap.CapacityParams.from_env() == cap.CapacityParams(2, 1)
    monkeypatch.setenv("STATELESS_AUTOSCALE_MIN_REPLICAS", "4")
    monkeypatch.setenv("STATELESS_AUTOSCALE_RESERVE", "-3")
    assert cap.CapacityParams.from_env() == cap.CapacityParams(4, 0)
    monkeypatch.setenv("STATELESS_AUTOSCALE_MIN_REPLICAS", "junk")
    assert cap.CapacityParams.from_env().min_replicas == 2


# --------------------------------------------------------------------------- #
# Executor inventory + snapshot
# --------------------------------------------------------------------------- #


def _pod(name, *, ready=True, cost=None, terminating=False):
    annotations = {} if cost is None else {cap.DELETION_COST_ANNOTATION: cost}
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            annotations=annotations,
            deletion_timestamp=NOW if terminating else None,
        ),
        status=SimpleNamespace(
            conditions=[
                SimpleNamespace(type="Ready", status="True" if ready else "False")
            ]
        ),
    )


class _CoreApi:
    def __init__(self, pods):
        self.pods = pods
        self.list_calls = []
        self.patches = []
        self.fail_patch_for: set[str] = set()

    def list_namespaced_pod(self, namespace, **kwargs):
        self.list_calls.append((namespace, kwargs))
        return SimpleNamespace(items=list(self.pods))

    def patch_namespaced_pod(self, name, namespace, body, **kwargs):
        if name in self.fail_patch_for:
            raise RuntimeError(f"boom {name}")
        self.patches.append((name, namespace, body))
        return None


@pytest.mark.asyncio
async def test_inventory_counts_ready_and_skips_terminating():
    api = _CoreApi([_pod("a"), _pod("b", ready=False), _pod("c", terminating=True)])
    inv = await cap.read_inventory(api, namespace="ns")
    assert inv == cap.ExecutorInventory(total=2, ready=1)
    ((namespace, kwargs),) = api.list_calls
    assert namespace == "ns"
    assert kwargs["label_selector"] == "srw/class=agent-stateless"
    assert kwargs["_request_timeout"] == cap.K8S_READ_REQUEST_TIMEOUT


@pytest.mark.asyncio
async def test_snapshot_reports_null_inventory_without_kubernetes():
    class _Pool:
        def __init__(self, conn):
            self.conn = conn

        def acquire(self):
            pool = self

            class _Ctx:
                async def __aenter__(self_inner):
                    return pool.conn

                async def __aexit__(self_inner, *exc):
                    return False

            return _Ctx()

    conn = _Conn(
        _demand_row(
            busy=2, runnable_session_turn=1, runnable_total=1, oldest_queued_age_s=3.5
        )
    )
    payload = await cap.capacity_snapshot(
        _Pool(conn),
        core_api_factory=lambda: None,
        params=cap.CapacityParams(2, 1),
    )
    assert payload["executors"] == {"total": None, "ready": None, "busy": 2}
    assert payload["queued"] == {
        "session_turn": 1,
        "worker_batch": 0,
        "bg_task": 0,
        "total": 1,
    }
    assert payload["oldest_queued_age_s"] == 3.5
    assert payload["desired"] == 4
    assert payload["params"] == {"min_replicas": 2, "reserve": 1}
    assert payload["observed_at"] == NOW.isoformat()


@pytest.mark.asyncio
async def test_snapshot_survives_a_failing_kubernetes_read():
    class _Boom:
        def list_namespaced_pod(self, *a, **k):
            raise RuntimeError("api down")

    payload = await cap.capacity_snapshot(
        _Conn(), core_api_factory=lambda: _Boom(), params=cap.CapacityParams(2, 1)
    )
    assert payload["executors"]["total"] is None
    assert payload["desired"] == 2


def test_load_core_api_never_loads_ambient_kubeconfig_by_default(monkeypatch):
    """Off-cluster with no opt-in: None, even if a kubeconfig would have loaded."""
    monkeypatch.delenv("STATELESS_CAPACITY_KUBECONFIG_FALLBACK", raising=False)
    kube_calls = []
    try:
        from kubernetes import config as k8s_config
    except ImportError:
        pytest.skip("kubernetes client not installed")

    def _incluster():
        raise k8s_config.ConfigException("not in cluster")

    monkeypatch.setattr(k8s_config, "load_incluster_config", _incluster)
    monkeypatch.setattr(
        k8s_config, "load_kube_config", lambda *a, **k: kube_calls.append(1)
    )
    assert cap.load_core_api() is None
    assert kube_calls == []
    monkeypatch.setenv("STATELESS_CAPACITY_KUBECONFIG_FALLBACK", "1")
    assert cap.load_core_api() is not None
    assert kube_calls == [1]


# --------------------------------------------------------------------------- #
# Deletion-cost reconciler
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_reconcile_patches_only_where_the_annotation_differs():
    api = _CoreApi(
        [
            _pod("busy-unannotated"),
            _pod("busy-already", cost="10000"),
            _pod("idle-unannotated"),
            _pod("idle-already", cost="0"),
            _pod("idle-stale-busy", cost="10000"),
            _pod("busy-terminating", cost="0", terminating=True),
        ]
    )
    conn = _Conn(
        busy_rows=[
            {"leased_by": "busy-unannotated"},
            {"leased_by": "busy-already"},
            {"leased_by": "busy-terminating"},
            {"leased_by": "not-a-pool-pod"},
        ]
    )
    result = await dc.reconcile_once(conn, api, pod_namespace="ns")
    patched = {
        name: body["metadata"]["annotations"][cap.DELETION_COST_ANNOTATION]
        for name, _, body in api.patches
    }
    assert patched == {
        "busy-unannotated": dc.BUSY_COST,
        "idle-unannotated": dc.IDLE_COST,
        "idle-stale-busy": dc.IDLE_COST,
    }
    assert all(ns == "ns" for _, ns, _ in api.patches)
    assert result == dc.ReconcileResult(pods=6, busy=3, patched=3, failed=0)


@pytest.mark.asyncio
async def test_reconcile_contains_a_single_pod_failure(caplog):
    api = _CoreApi([_pod("first"), _pod("second")])
    api.fail_patch_for = {"first"}
    conn = _Conn(busy_rows=[{"leased_by": "first"}, {"leased_by": "second"}])
    with caplog.at_level("WARNING", logger=dc.logger.name):
        result = await dc.reconcile_once(conn, api, pod_namespace="ns")
    assert [name for name, _, _ in api.patches] == ["second"]
    assert result.patched == 1 and result.failed == 1
    assert any("deletion-cost patch failed" in r.getMessage() for r in caplog.records)


def test_lock_id_is_distinct_and_packed_ascii():
    assert STATELESS_DELETION_COST_ID not in {LEADER_ID, RUN_QUEUE_REAPER_ID}
    assert STATELESS_DELETION_COST_ID.to_bytes(8, "big") == b"SRW_DELC"


@pytest.mark.asyncio
async def test_loop_leader_gates_on_its_own_advisory_lock(monkeypatch):
    shutdown = asyncio.Event()
    conn = MagicMock()
    lock_calls = []

    async def _fetchval(q, *a):
        lock_calls.append((q, a))
        assert "pg_try_advisory_lock" in q
        assert a == (STATELESS_DELETION_COST_ID,)
        return True

    conn.fetchval = _fetchval
    conn.execute = AsyncMock()
    cycles = []

    async def _reconcile(c, api, *, pod_namespace):
        assert c is conn and pod_namespace == "ns"
        cycles.append(api)
        shutdown.set()
        return dc.ReconcileResult(0, 0, 0, 0)

    monkeypatch.setattr(dc, "reconcile_once", _reconcile)
    pool = MagicMock()
    pool.acquire = AsyncMock(return_value=conn)
    pool.release = AsyncMock()
    db = MagicMock()
    db._pool = pool
    api = object()

    await asyncio.wait_for(
        dc.stateless_pod_deletion_cost_loop(
            db,
            shutdown,
            interval=0.01,
            core_api_factory=lambda: api,
            pod_namespace="ns",
        ),
        timeout=5,
    )

    assert cycles == [api]
    assert len(lock_calls) == 1
    unlock = [
        c for c in conn.execute.await_args_list if "pg_advisory_unlock" in c.args[0]
    ]
    assert len(unlock) == 1 and unlock[0].args[1] == STATELESS_DELETION_COST_ID
    pool.release.assert_awaited_once_with(conn)


@pytest.mark.asyncio
async def test_loop_as_follower_never_reconciles(monkeypatch):
    shutdown = asyncio.Event()
    conn = MagicMock()
    polls = []

    async def _fetchval(q, *a):
        polls.append(q)
        if len(polls) >= 2:
            shutdown.set()
        return False

    conn.fetchval = _fetchval
    conn.execute = AsyncMock()
    reconcile = AsyncMock()
    monkeypatch.setattr(dc, "reconcile_once", reconcile)
    pool = MagicMock()
    pool.acquire = AsyncMock(return_value=conn)
    pool.release = AsyncMock()
    db = MagicMock()
    db._pool = pool

    await asyncio.wait_for(
        dc.stateless_pod_deletion_cost_loop(
            db,
            shutdown,
            interval=0.01,
            core_api_factory=lambda: object(),
            pod_namespace="ns",
        ),
        timeout=5,
    )
    reconcile.assert_not_awaited()
    assert len(polls) == 2


@pytest.mark.asyncio
async def test_loop_idles_without_kubernetes_and_logs_once(caplog):
    shutdown = asyncio.Event()
    factory_calls = []

    def _factory():
        factory_calls.append(1)
        if len(factory_calls) >= 3:
            shutdown.set()
        return None

    db = MagicMock()
    db._pool = MagicMock()  # must never be touched
    db._pool.acquire = AsyncMock()
    with caplog.at_level("INFO", logger=dc.logger.name):
        await asyncio.wait_for(
            dc.stateless_pod_deletion_cost_loop(
                db,
                shutdown,
                interval=0.01,
                core_api_factory=_factory,
                pod_namespace="ns",
            ),
            timeout=5,
        )
    db._pool.acquire.assert_not_awaited()
    unavailable = [
        r for r in caplog.records if "kubernetes unavailable" in r.getMessage()
    ]
    assert len(unavailable) == 1
    assert len(factory_calls) >= 3


@pytest.mark.asyncio
async def test_loop_survives_cycle_errors_and_recontends(monkeypatch):
    shutdown = asyncio.Event()
    conn = MagicMock()
    conn.fetchval = AsyncMock(return_value=True)
    conn.execute = AsyncMock()
    attempts = []

    async def _reconcile(c, api, *, pod_namespace):
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("api blip")
        shutdown.set()
        return dc.ReconcileResult(0, 0, 0, 0)

    monkeypatch.setattr(dc, "reconcile_once", _reconcile)
    pool = MagicMock()
    pool.acquire = AsyncMock(return_value=conn)
    pool.release = AsyncMock()
    db = MagicMock()
    db._pool = pool

    await asyncio.wait_for(
        dc.stateless_pod_deletion_cost_loop(
            db,
            shutdown,
            interval=0.01,
            core_api_factory=lambda: object(),
            pod_namespace="ns",
        ),
        timeout=5,
    )
    assert len(attempts) == 2
    assert pool.release.await_count == 2


def test_reconciler_env_knobs(monkeypatch):
    monkeypatch.delenv("STATELESS_DELETION_COST_RECONCILER_ENABLED", raising=False)
    assert dc.reconciler_enabled() is True
    monkeypatch.setenv("STATELESS_DELETION_COST_RECONCILER_ENABLED", "false")
    assert dc.reconciler_enabled() is False
    monkeypatch.delenv("STATELESS_DELETION_COST_INTERVAL_S", raising=False)
    assert dc.interval_seconds() == 10.0
    monkeypatch.setenv("STATELESS_DELETION_COST_INTERVAL_S", "0.2")
    assert dc.interval_seconds() == 1.0
    monkeypatch.setenv("STATELESS_DELETION_COST_INTERVAL_S", "junk")
    assert dc.interval_seconds() == 10.0


# --------------------------------------------------------------------------- #
# Route: mounted + admin-gated
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_capacity_route_requires_admin():
    from orchestrator.routers import capacity as routes

    async def _deny(_request):
        raise HTTPException(status_code=403, detail="Admin access required")

    snapshot = AsyncMock(return_value={"desired": 2})
    vm_snapshot = AsyncMock(return_value={"available": False, "clusters": []})
    deps = routes.CapacityDependencies(snapshot=snapshot, require_admin=_deny, vm_snapshot=vm_snapshot)
    with pytest.raises(HTTPException) as exc:
        await routes.get_capacity(MagicMock(), dependencies=deps)
    assert exc.value.status_code == 403
    snapshot.assert_not_awaited()
    vm_snapshot.assert_not_awaited()

    async def _allow(_request):
        return {"id": "admin", "real_is_admin": True}

    deps = routes.CapacityDependencies(snapshot=snapshot, require_admin=_allow)
    assert await routes.get_capacity(MagicMock(), dependencies=deps) == {"desired": 2}
    deps = routes.CapacityDependencies(snapshot=snapshot, require_admin=_allow, vm_snapshot=vm_snapshot)
    assert await routes.get_capacity(MagicMock(), dependencies=deps) == {
        "desired": 2, "vm": {"available": False, "clusters": []},
    }
    vm_snapshot.assert_awaited_once()


def test_capacity_route_is_mounted():
    # main is imported inside the body (heavy; CI-gated like the other admin
    # route tests) and inventoried through tests/_route_inventory so the
    # include_router wrapper cannot hide it.
    from tests._route_inventory import mounted_routes

    import orchestrator.main as main

    assert ("GET", "/api/admin/capacity") in mounted_routes(main.app)
    assert callable(main.app.state.capacity_dependencies_factory)


@pytest.mark.asyncio
async def test_snapshot_lists_parked_units_newest_first_with_owner():
    import uuid as _uuid

    unit = _uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    conn = _Conn(
        parked_rows=[
            {
                "unit_id": unit,
                "unit_kind": "session_turn",
                "park_reason": "attach_failed",
                "parked_at": NOW,
                "attempts_since_completion": 5,
                "max_attempts": 5,
                "attach_failures": 3,
                "last_error": "subagent session-terminalize failed (HTTP 400)",
                "queued_at": NOW,
                "pending_input": True,
                "thread_id": unit,
                "title": "Comparing Take-Home Pay",
                "user_id": _uuid.uuid4(),
                "owner": "knaeckebrothero",
            }
        ]
    )
    payload = await cap.capacity_snapshot(
        conn, core_api_factory=lambda: None, params=cap.CapacityParams(2, 1)
    )
    assert payload["parked"] == [
        {
            "unit_id": str(unit),
            "unit_kind": "session_turn",
            "thread_id": str(unit),
            "title": "Comparing Take-Home Pay",
            "owner": "knaeckebrothero",
            "park_reason": "attach_failed",
            "parked_at": NOW.isoformat(),
            "attempts": 5,
            "attach_failures": 3,
            "last_error": "subagent session-terminalize failed (HTTP 400)",
            "pending_input": True,
        }
    ]
    # The parked read is the LIST statement, bounded, on the same connection.
    parked_queries = [q for q in conn.queries if "state = 'parked'" in q]
    assert len(parked_queries) == 1 and "LIMIT $1::int" in parked_queries[0]


@pytest.mark.asyncio
async def test_snapshot_parked_is_empty_when_nothing_is_parked():
    payload = await cap.capacity_snapshot(
        _Conn(), core_api_factory=lambda: None, params=cap.CapacityParams(2, 1)
    )
    assert payload["parked"] == []
