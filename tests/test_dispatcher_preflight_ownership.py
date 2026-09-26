"""Bounded application ownership and current authority at dispatch completion."""

from __future__ import annotations

import asyncio
import copy
import dataclasses
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrator.services.container_provisioner import ContainerProvisioner
from orchestrator.services import job_dispatcher as scheduler
from tests import test_b11_job_dispatcher as cases

no_dispatcher_error = cases.no_dispatcher_error


class SnapshotStore(cases.FakeStore):
    async def get_job(self, job_id):
        return copy.deepcopy(await super().get_job(job_id))


def sandbox(job_id, status=None, **kwargs):
    return cases._job(
        job_id,
        config_override={"workspace": {"backend": "sandbox"}},
        context={"workspace_container": {"status": status, "provisioner": "k8s"}},
        **kwargs,
    )


async def flush(state):
    while state.tasks:
        await asyncio.wait_for(asyncio.gather(*list(state.tasks)), 2)
        await asyncio.sleep(0)


async def until(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0.001)


def dependencies(store, create):
    return dataclasses.replace(
        cases._deps(store),
        job_needs_sandbox=lambda job: job["config_override"]["workspace"]["backend"]
        == "sandbox",
        container_provisioner=SimpleNamespace(
            is_available=True,
            in_cluster=True,
            create_workspace=create,
            workspace_pod_live=AsyncMock(return_value=True),
        ),
    )


@pytest.mark.asyncio
async def test_two_mutations_saturated_ready_lane_progresses_and_queue_rotates(
    no_dispatcher_error,
):
    jobs = [sandbox(f"slow-{i}") for i in range(5)] + [cases._job("ready")]
    store = SnapshotStore(pinned=jobs, agents=[{"id": "agent", "metadata": {}}])
    gates = {job["id"]: asyncio.Event() for job in jobs[:5]}
    started = []

    async def create(owner):
        started.append(owner.id)
        await gates[owner.id].wait()
        return False

    deps = dependencies(store, create)
    try:
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        await until(lambda: bool(store.called("claim_job_for_agent")))
        assert started == ["slow-0", "slow-1"]
        assert sum(mutation for _, mutation in deps.state.active.values()) == 2
        for _ in range(5):
            await scheduler.dispatch_pending_jobs(dependencies=deps)
        assert started == ["slow-0", "slow-1"]
        gates["slow-0"].set()
        await until(lambda: len(started) == 3)
        assert started[-1] == "slow-2"
        gates["slow-1"].set()
        await until(lambda: len(started) == 4)
        assert started[-1] == "slow-3"
        gates["slow-2"].set()
        await until(lambda: len(started) == 5)
        assert started[-1] == "slow-4"
    finally:
        await deps.state.drain()


@pytest.mark.asyncio
async def test_ready_missing_pod_defers_creation_until_mutation_slot_is_free(
    no_dispatcher_error,
):
    jobs = [sandbox("first"), sandbox("second"), sandbox("missing", "ready")]
    store = SnapshotStore(pinned=jobs)
    gates = {job["id"]: asyncio.Event() for job in jobs}
    started = []

    async def create(owner):
        started.append(owner.id)
        await gates[owner.id].wait()
        return False

    deps = dependencies(store, create)
    deps.container_provisioner.workspace_pod_live = AsyncMock(return_value=False)
    try:
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        await until(lambda: "missing" in deps.state.pending)
        assert started == ["first", "second"]
        assert deps.state.pending["missing"][1] is True
        gates["first"].set()
        await until(lambda: "missing" in started)
        assert sum(mutation for _, mutation in deps.state.active.values()) <= 2
    finally:
        await deps.state.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["cancel", "config", "runtime", "lane"])
async def test_finished_preflight_cannot_claim_changed_authority(
    change, no_dispatcher_error
):
    job = cases._job("job")
    store = SnapshotStore(pinned=[job], agents=[{"id": "agent", "metadata": {}}])
    entered, release = asyncio.Event(), asyncio.Event()

    async def prepare(_):
        entered.set()
        await release.wait()
        return True

    deps = dataclasses.replace(
        cases._deps(store), prepare_job_repository_before_claim=prepare
    )
    try:
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        await entered.wait()
        changed = copy.deepcopy(job)
        if change == "cancel":
            changed["status"] = "cancelled"
        elif change == "config":
            changed["config_override"]["agent"] = {"model": "successor"}
        elif change == "runtime":
            changed["context"]["_workspace_binding"] = {"generation": "successor"}
        else:
            changed["execution_lane"] = "stateless"
        store.jobs["job"] = changed
        release.set()
        await flush(deps.state)
        assert store.called("claim_job_for_agent") == []
        assert store.called("admit_stateless_worker_job") == []
    finally:
        await deps.state.drain()


@pytest.mark.asyncio
async def test_trigger_storm_coalesces_and_shutdown_joins_started_sdk_call(monkeypatch):
    from orchestrator.services import leader_election

    leader = asyncio.Event()
    leader.set()
    monkeypatch.setattr(leader_election, "is_leader", leader)
    started, release = threading.Event(), threading.Event()
    lock = asyncio.Lock()
    store = SnapshotStore(pinned=[sandbox("slow")])

    def sdk_call(**kwargs):
        assert kwargs["_request_timeout"] == (5, 30)
        started.set()
        assert release.wait(5)

    async def create(owner):
        async with lock:
            await ContainerProvisioner._bounded_kubernetes_call(sdk_call)
        return False

    deps = dependencies(store, create)
    try:
        for _ in range(100):
            scheduler.trigger_dispatch(dependencies=deps)
        assert len(deps.state.tasks) == 1
        await until(started.is_set)
        assert len(store.called("get_dispatchable_jobs")) == 1
        drain = asyncio.create_task(deps.state.drain())
        await asyncio.sleep(0.02)
        assert not drain.done()
        assert lock.locked()
        scheduler.trigger_dispatch(dependencies=deps)
        release.set()
        await asyncio.wait_for(drain, 2)
        assert not lock.locked()
        assert not deps.state.tasks
        assert not deps.state.active
    finally:
        release.set()
        await deps.state.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["creating", "created", "restoring", "pending"])
async def test_restart_keeps_existing_pending_hold_without_fresh_create(
    status, no_dispatcher_error
):
    create = AsyncMock(side_effect=AssertionError("must not invent a continuation"))
    deps = dependencies(SnapshotStore(pinned=[sandbox("pending", status)]), create)
    try:
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        await flush(deps.state)
        create.assert_not_awaited()
    finally:
        await deps.state.drain()


@pytest.mark.asyncio
async def test_pending_memory_bound_includes_active_operations():
    entered, release = asyncio.Event(), asyncio.Event()
    jobs = [sandbox(str(i)) for i in range(150)]

    async def create(_):
        entered.set()
        await release.wait()
        return False

    deps = dependencies(SnapshotStore(pinned=jobs), create)
    try:
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        await entered.wait()
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        assert (
            len(deps.state.pending) + len(deps.state.active) + len(deps.state.completed)
            <= 100
        )
    finally:
        await deps.state.drain()


@pytest.mark.asyncio
async def test_scholar_siblings_share_one_active_parent_operation():
    jobs = [sandbox("child-1"), sandbox("child-2")]
    for job in jobs:
        job["context"]["provisions_parent_workspace"] = "parent"
    entered, release = asyncio.Event(), asyncio.Event()
    calls = []

    async def parent_create(job, parent):
        calls.append((job["id"], parent))
        entered.set()
        await release.wait()

    deps = dataclasses.replace(
        dependencies(SnapshotStore(pinned=jobs), AsyncMock()),
        provision_parent_workspace_for_scholar=parent_create,
    )
    try:
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        await entered.wait()
        assert calls == [("child-1", "parent")]
        assert len(deps.state.active) == 1
        release.set()
        await flush(deps.state)
        assert calls == [("child-1", "parent"), ("child-2", "parent")]
    finally:
        await deps.state.drain()


@pytest.mark.asyncio
async def test_suspended_restore_is_owned_and_awaited():
    entered, stopped = asyncio.Event(), asyncio.Event()

    async def restore(_):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    deps = dataclasses.replace(
        dependencies(
            SnapshotStore(pinned=[sandbox("suspended", "suspended")]), AsyncMock()
        ),
        workspace_suspension=SimpleNamespace(restore=restore),
    )
    await scheduler.dispatch_pending_jobs(dependencies=deps)
    await asyncio.wait_for(entered.wait(), 2)
    assert deps.state.active == {"suspended": ("suspended", True)}
    await deps.state.drain()
    assert stopped.is_set()


@pytest.mark.asyncio
async def test_owner_changed_since_discovery_requeues_before_parent_effect():
    first, second = sandbox("first"), sandbox("second")
    first["context"]["provisions_parent_workspace"] = "old-parent"
    second["context"]["provisions_parent_workspace"] = "shared-parent"
    current = copy.deepcopy(first)
    current["context"]["provisions_parent_workspace"] = "shared-parent"
    store = SnapshotStore(pinned=[first, second], jobs={"first": current})
    calls = []
    release = asyncio.Event()

    async def provision_parent(job, parent):
        calls.append((job["id"], parent))
        await release.wait()

    deps = dataclasses.replace(
        dependencies(store, AsyncMock()),
        provision_parent_workspace_for_scholar=provision_parent,
    )
    try:
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        await until(lambda: bool(calls))
        await asyncio.sleep(0.01)
        assert len(calls) == 1
        assert calls[0][1] == "shared-parent"
        assert len(deps.state.active) == 1
        release.set()
        await flush(deps.state)
        assert {job for job, _ in calls} == {"first", "second"}
    finally:
        await deps.state.drain()


@pytest.mark.asyncio
async def test_lost_leadership_refuses_completed_claim_and_new_mutations(monkeypatch):
    from orchestrator.services import leader_election

    leader = asyncio.Event()
    leader.set()
    monkeypatch.setattr(leader_election, "is_leader", leader)
    entered, release = asyncio.Event(), asyncio.Event()
    store = SnapshotStore(
        pinned=[cases._job("ready")], agents=[{"id": "agent", "metadata": {}}]
    )

    async def prepare(_):
        entered.set()
        await release.wait()
        return True

    deps = dataclasses.replace(
        cases._deps(store), prepare_job_repository_before_claim=prepare
    )
    try:
        scheduler.trigger_dispatch(dependencies=deps)
        await entered.wait()
        leader.clear()
        release.set()
        await flush(deps.state)
        assert not store.called("claim_job_for_agent")
    finally:
        await deps.state.drain()


@pytest.mark.asyncio
async def test_leader_loop_exit_drains_preflight_and_reacquisition_can_schedule(
    monkeypatch,
):
    from orchestrator.services import leader_election

    leader = asyncio.Event()
    leader.set()
    monkeypatch.setattr(leader_election, "is_leader", leader)
    entered, stopped = asyncio.Event(), asyncio.Event()

    async def create(_):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    deps = dependencies(SnapshotStore(pinned=[sandbox("slow")]), create)
    stop = asyncio.Event()
    loop = asyncio.create_task(
        scheduler.auto_assign_dispatcher(stop, dependencies=deps)
    )
    try:
        await entered.wait()
        loop.cancel()
        await asyncio.gather(loop, return_exceptions=True)
        assert stopped.is_set()
        assert not deps.state.active
        entered.clear()
        stopped.clear()
        loop = asyncio.create_task(
            scheduler.auto_assign_dispatcher(stop, dependencies=deps)
        )
        await asyncio.wait_for(entered.wait(), 2)
    finally:
        loop.cancel()
        await asyncio.gather(loop, return_exceptions=True)
        await deps.state.drain()


@pytest.mark.asyncio
async def test_leadership_loss_joins_sdk_before_reacquisition(monkeypatch):
    from orchestrator.services import leader_election

    leader = asyncio.Event()
    leader.set()
    monkeypatch.setattr(leader_election, "is_leader", leader)
    started, release = threading.Event(), threading.Event()
    lock = asyncio.Lock()
    slow, ready = sandbox("slow"), cases._job("ready")
    store = SnapshotStore(pinned=[slow], agents=[{"id": "agent", "metadata": {}}])

    def sdk_call(**_):
        started.set()
        assert release.wait(5)

    async def create(_):
        async with lock:
            await ContainerProvisioner._bounded_kubernetes_call(sdk_call)
        return False

    deps = dependencies(store, create)
    stop = asyncio.Event()
    loop = asyncio.create_task(
        scheduler.auto_assign_dispatcher(stop, dependencies=deps)
    )
    try:
        await until(started.is_set)
        leader.clear()
        loop.cancel()
        await asyncio.sleep(0.02)
        assert not loop.done()
        assert lock.locked()
        assert deps.state.scheduling_paused
        store.jobs["slow"] = {**slow, "status": "cancelled"}
        release.set()
        await asyncio.gather(loop, return_exceptions=True)
        assert not lock.locked()
        assert not deps.state.preflight_tasks
        assert not store.called("claim_job_for_agent")
        store.pinned = [ready]
        store.jobs["ready"] = ready
        leader.set()
        loop = asyncio.create_task(
            scheduler.auto_assign_dispatcher(stop, dependencies=deps)
        )
        await until(lambda: bool(store.called("claim_job_for_agent")))
        assert [args[0] for args, _ in store.called("claim_job_for_agent")] == ["ready"]
    finally:
        release.set()
        loop.cancel()
        await asyncio.gather(loop, return_exceptions=True)
        await deps.state.drain()


@pytest.mark.asyncio
async def test_preflight_exception_is_local_to_its_job(caplog):
    store = SnapshotStore(
        pinned=[sandbox("broken"), cases._job("ready")],
        agents=[{"id": "agent", "metadata": {}}],
    )
    deps = dependencies(
        store, AsyncMock(side_effect=RuntimeError("bounded API refusal"))
    )
    try:
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        await flush(deps.state)
        assert [args[0] for args, _ in store.called("claim_job_for_agent")] == ["ready"]
        assert any(
            "preflight failed for job broken" in record.message
            for record in caplog.records
        )
        assert not deps.state.active
    finally:
        await deps.state.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("shutdown", [False, True])
async def test_stop_before_scheduled_worker_starts_clears_owner_slots(shutdown):
    create = AsyncMock()
    deps = dependencies(SnapshotStore(pinned=[sandbox("queued")]), create)
    await scheduler.dispatch_pending_jobs(dependencies=deps)
    assert deps.state.active == {"queued": ("queued", True)}
    if shutdown:
        await deps.state.drain()
    else:
        await deps.state.pause_preflights()
    create.assert_not_awaited()
    assert not deps.state.active
    assert not deps.state.pending
    assert not deps.state.preflight_tasks
