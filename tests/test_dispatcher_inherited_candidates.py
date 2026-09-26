"""Inherited runtime overlays retain independent child and parent authority."""

from __future__ import annotations

import asyncio
import copy
import dataclasses
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrator.services import job_dispatcher as scheduler
from orchestrator.services import job_workspace_authority as authority
from tests import test_b11_job_dispatcher as cases
from tests import test_subjob_inherited_workspace as inherited
from tests.test_dispatcher_preflight_ownership import SnapshotStore, flush

no_dispatcher_error = cases.no_dispatcher_error


def scenario(backend, lane="pinned"):
    key = "workspace_container" if backend == "sandbox" else "vm"
    runtime = inherited.READY_CONTAINER if backend == "sandbox" else inherited.READY_VM
    parent = inherited._stamp_workspace(
        {"id": "parent", "status": "waiting", "context": {key: copy.deepcopy(runtime)}}
    )
    child = cases._job(
        "child",
        **{
            k: v
            for k, v in inherited._subjob(
                {"inherits_parent_workspace": True, key: {"status": "created"}},
                parent_id="parent",
            ).items()
            if k != "id"
        },
        execution_lane=lane,
    )
    store = SnapshotStore(
        pinned=[child] if lane == "pinned" else [],
        stateless=[child] if lane == "stateless" else [],
        agents=[{"id": "agent", "metadata": {}}],
        jobs={"parent": parent},
    )
    prepared = asyncio.Event()
    release = asyncio.Event()
    deliveries = []
    authority_deps = SimpleNamespace(
        store=store, workspace_provisioner=object(), logger=logging.getLogger(__name__)
    )

    async def resolve(job):
        assert not deps.state.lock.locked()
        return await authority.resolve_subjob_inherited_workspace(
            job, dependencies=authority_deps
        )

    authority_deps.resolve_inherited_workspace = resolve

    async def prepare(job):
        return await authority.prepare_job_workspace_runtime(
            job, dependencies=authority_deps
        )

    async def repository(job):
        prepared.set()
        await release.wait()
        return True

    async def deliver(job, agent):
        deliveries.append(copy.deepcopy(job))
        return True

    deps = dataclasses.replace(
        cases._deps(store, stateless_worker_enabled=True),
        prepare_job_workspace_runtime=prepare,
        prepare_job_repository_before_claim=repository,
        check_vm_permission=AsyncMock(),
        job_needs_sandbox=lambda job: backend == "sandbox",
        container_provisioner=SimpleNamespace(
            is_available=True,
            in_cluster=True,
            workspace_pod_live=AsyncMock(return_value=True),
        ),
        job_delivery_operations=lambda: SimpleNamespace(dispatch=deliver),
    )
    return child, parent, store, deps, prepared, release, deliveries


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "backend,lane", [("sandbox", "pinned"), ("vm", "pinned"), ("sandbox", "stateless")]
)
async def test_unchanged_inherited_workspace_is_admitted_without_persisting_overlay(
    backend, lane, no_dispatcher_error
):
    child, parent, store, deps, prepared, release, deliveries = scenario(backend, lane)
    raw_child = copy.deepcopy(child)
    try:
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        await asyncio.wait_for(prepared.wait(), 2)
        release.set()
        await flush(deps.state)
        if lane == "pinned":
            assert len(deliveries) == 1
            key = "workspace_container" if backend == "sandbox" else "vm"
            assert deliveries[0]["context"][key] == parent["context"][key]
        else:
            assert len(store.called("admit_stateless_worker_job")) == 1
        assert store.jobs["child"] == raw_child
    finally:
        await deps.state.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["sandbox", "vm"])
@pytest.mark.parametrize(
    "change",
    [
        "parent_uid",
        "parent_endpoint",
        "parent_pending",
        "parent_contract",
        "child_parent",
        "child_config",
        "child_cancel",
    ],
)
async def test_inherited_preflight_refuses_changed_child_or_parent(
    backend, change, no_dispatcher_error
):
    child, parent, store, deps, prepared, release, deliveries = scenario(backend)
    try:
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        await asyncio.wait_for(prepared.wait(), 2)
        key = "workspace_container" if backend == "sandbox" else "vm"
        if change == "parent_uid":
            uid_key = (
                "_runtime_incarnation"
                if backend == "sandbox"
                else "provision_generation"
            )
            parent["context"][key][uid_key] = "33333333-3333-4333-8333-333333333333"
        elif change == "parent_endpoint":
            parent["context"][key]["host" if backend == "sandbox" else "ssh_host"] = (
                "successor"
            )
        elif change == "parent_pending":
            parent["context"][key]["status"] = "creating"
        elif change == "parent_contract":
            parent["config_override"]["workspace"]["backend"] = "none"
        elif change == "child_parent":
            child["parent_job_id"] = "successor-parent"
        elif change == "child_config":
            child["config_override"]["agent"] = {"model": "successor"}
        else:
            child["status"] = "cancelled"
        release.set()
        await flush(deps.state)
        assert not deliveries
        assert not store.called("claim_job_for_agent")
        assert not store.called("admit_stateless_worker_job")
    finally:
        await deps.state.drain()


@pytest.mark.asyncio
async def test_parent_replacement_after_completion_check_refused_before_claim(
    no_dispatcher_error,
):
    child, parent, store, deps, prepared, release, deliveries = scenario("sandbox")
    original_agents = store.get_available_agents

    async def agents_after_parent_replacement(**kwargs):
        parent["context"]["workspace_container"]["_runtime_incarnation"] = (
            "33333333-3333-4333-8333-333333333333"
        )
        return await original_agents(**kwargs)

    store.get_available_agents = agents_after_parent_replacement
    try:
        await scheduler.dispatch_pending_jobs(dependencies=deps)
        await asyncio.wait_for(prepared.wait(), 2)
        release.set()
        await flush(deps.state)
        assert store.called("get_available_agents")
        assert not deliveries
        assert not store.called("claim_job_for_agent")
    finally:
        await deps.state.drain()
