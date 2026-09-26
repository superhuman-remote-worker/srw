"""Local queue fairness must not widen mutation or owner authority."""

from __future__ import annotations

import asyncio
import copy

import pytest

from orchestrator.services import job_dispatcher as scheduler
from tests import test_b11_job_dispatcher as cases
from tests.test_dispatcher_preflight_ownership import (
    SnapshotStore,
    dependencies,
    sandbox,
    until,
)
from tests.test_subjob_inherited_workspace import READY_CONTAINER

no_dispatcher_error = cases.no_dispatcher_error


@pytest.mark.asyncio
async def test_ready_job_displaces_only_queued_hint_at_100_owned_limit(
    no_dispatcher_error,
):
    jobs = [sandbox(f"slow-{i:03}") for i in range(150)]
    store = SnapshotStore(pinned=jobs, agents=[{"id": "agent", "metadata": {}}])
    started = []

    async def create(owner):
        started.append(owner.id)
        await asyncio.Event().wait()

    deps = dependencies(store, create)
    try:
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        await until(lambda: len(started) == 2)
        for _ in range(2):
            await scheduler.dispatch_pending_jobs(dependencies=deps)
        assert len(deps.state.pending) + len(deps.state.active) == 100
        active_before = dict(deps.state.active)
        ready = cases._job("ready", priority=1)
        store.pinned.append(ready)
        store.jobs["ready"] = ready
        for _ in range(5):
            await scheduler.dispatch_pending_jobs(dependencies=deps)
            await asyncio.sleep(0.01)
        assert store.called("claim_job_for_agent")
        assert deps.state.active == active_before
        assert len(started) == 2
        assert (
            len(deps.state.pending) + len(deps.state.active) + len(deps.state.completed)
            <= 100
        )
    finally:
        await deps.state.drain()


@pytest.mark.asyncio
async def test_queued_mutation_becoming_ready_uses_ready_lane(no_dispatcher_error):
    jobs = [sandbox("a-slow"), sandbox("b-slow"), sandbox("c-now-ready")]
    store = SnapshotStore(pinned=jobs, agents=[{"id": "agent", "metadata": {}}])
    started = []

    async def create(owner):
        started.append(owner.id)
        await asyncio.Event().wait()

    deps = dependencies(store, create)
    try:
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        await until(lambda: len(started) == 2)
        ready = copy.deepcopy(jobs[2])
        ready["context"]["workspace_container"] = copy.deepcopy(READY_CONTAINER)
        store.jobs[ready["id"]] = ready
        store.pinned[2] = ready
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        await asyncio.sleep(0.05)
        assert store.called("claim_job_for_agent")
        assert started == ["a-slow", "b-slow"]
    finally:
        await deps.state.drain()


@pytest.mark.asyncio
async def test_urgent_mutation_displaces_queued_hint_and_runs_at_next_slot(
    no_dispatcher_error,
):
    jobs = [sandbox(f"slow-{i:03}") for i in range(150)]
    store = SnapshotStore(pinned=jobs)
    gates = {job["id"]: asyncio.Event() for job in jobs}
    started = []

    async def create(owner):
        started.append(owner.id)
        await gates[owner.id].wait()
        return False

    deps = dependencies(store, create)
    try:
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        await until(lambda: len(started) == 2)
        for _ in range(2):
            await scheduler.dispatch_pending_jobs(dependencies=deps)
        assert len(deps.state.pending) + len(deps.state.active) == 100
        urgent = sandbox("urgent", priority=99)
        gates["urgent"] = asyncio.Event()
        store.jobs["urgent"] = urgent
        store.pinned.append(urgent)
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        assert len(deps.state.pending) + len(deps.state.active) == 100
        gates[started[0]].set()
        await until(lambda: len(started) == 3)
        assert started[-1] == "urgent"
    finally:
        await deps.state.drain()


@pytest.mark.asyncio
async def test_new_runtime_clears_exact_missing_pod_deferral(no_dispatcher_error):
    jobs = [sandbox("a-slow"), sandbox("b-slow"), sandbox("c-missing", "ready")]
    jobs[2]["context"]["workspace_container"] = copy.deepcopy(READY_CONTAINER)
    store = SnapshotStore(pinned=jobs, agents=[{"id": "agent", "metadata": {}}])
    started = []
    live = False

    async def create(owner):
        started.append(owner.id)
        await asyncio.Event().wait()

    async def pod_live(*args, **kwargs):
        return live

    deps = dependencies(store, create)
    deps.container_provisioner.workspace_pod_live = pod_live
    try:
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        await until(lambda: "c-missing" in deps.state.pending)
        await until(lambda: deps.state.pending["c-missing"].forced_snapshot is not None)
        for _ in range(2):
            await scheduler.dispatch_pending_jobs(dependencies=deps)
        assert not store.called("claim_job_for_agent")
        assert started == ["a-slow", "b-slow"]
        current = copy.deepcopy(jobs[2])
        current["context"]["workspace_container"]["_runtime_incarnation"] = (
            "33333333-3333-4333-8333-333333333333"
        )
        store.jobs["c-missing"] = current
        store.pinned[2] = current
        live = True
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        await until(lambda: bool(store.called("claim_job_for_agent")))
        assert started == ["a-slow", "b-slow"]
    finally:
        await deps.state.drain()


@pytest.mark.asyncio
async def test_ready_discovery_becoming_missing_before_preflight_cannot_create_outside_slot(
    no_dispatcher_error,
):
    ready = sandbox("c-ready", "ready")
    ready["context"]["workspace_container"] = copy.deepcopy(READY_CONTAINER)
    jobs = [sandbox("a-slow"), sandbox("b-slow"), ready]
    missing = sandbox("c-ready")
    store = SnapshotStore(pinned=jobs, jobs={"c-ready": missing})
    gates = {job["id"]: asyncio.Event() for job in jobs}
    started = []

    async def create(owner):
        started.append(owner.id)
        await gates[owner.id].wait()
        return False

    deps = dependencies(store, create)
    try:
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        await until(lambda: "c-ready" in deps.state.pending)
        assert started == ["a-slow", "b-slow"]
        gates["a-slow"].set()
        await until(lambda: len(started) == 3)
        assert started[-1] == "c-ready"
        assert sum(mutation for _, mutation in deps.state.active.values()) == 2
    finally:
        await deps.state.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["sandbox", "vm"])
async def test_ready_inherited_child_progresses_with_two_mutations_saturated(
    backend, no_dispatcher_error
):
    import dataclasses
    from tests.test_dispatcher_inherited_candidates import scenario

    child, parent, store, deps, prepared, release, deliveries = scenario(backend)
    slow = [sandbox("a-slow", priority=9), sandbox("b-slow", priority=9)]
    store.pinned = slow + store.pinned
    store.jobs.update({job["id"]: job for job in slow})
    started = []

    async def create(owner):
        started.append(owner.id)
        await asyncio.Event().wait()

    deps = dataclasses.replace(
        deps,
        job_needs_sandbox=lambda job: job["config_override"]["workspace"]["backend"]
        == "sandbox",
    )
    deps.container_provisioner.create_workspace = create
    release.set()
    try:
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        await until(lambda: len(started) == 2)
        await asyncio.sleep(0.05)
        assert len(deliveries) == 1
        assert set(deps.state.active) == {"a-slow", "b-slow"}
        assert (
            store.jobs["child"]["context"][
                "workspace_container" if backend == "sandbox" else "vm"
            ]["status"]
            == "created"
        )
    finally:
        await deps.state.drain()
