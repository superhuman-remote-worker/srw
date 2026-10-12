"""Dispatcher progress while a real workspace create or advisory lock waits."""

from __future__ import annotations

import asyncio
import dataclasses
import json
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from orchestrator.services import container_provisioner as provider_module
from orchestrator.services.job_dispatcher import dispatch_pending_jobs
from orchestrator.services.job_workspace_authority import prepare_job_workspace_runtime
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from tests import test_b11_job_dispatcher as dispatch_tests
from tests import test_workspace_pull_failure_real_postgres as pull

pg_dsn = pull.pg_dsn
db = pull.db
_schema_applied = pull._schema_applied
no_dispatcher_error = dispatch_tests.no_dispatcher_error

FINGERPRINT = "SHA256:" + "A" * 43


class WorkspaceCluster(pull.NeverPullingCluster):
    def __init__(self, *, ready, observed):
        super().__init__()
        self.ready = ready
        self.observed = observed
        self.loop = asyncio.get_running_loop()

    def create_namespaced_pod(self, **kwargs):
        pod = super().create_namespaced_pod(**kwargs)
        status = pod.status.container_statuses[0]
        if self.ready:
            pod.status.phase = "Running"
            status.ready = True
            status.started = True
            status.state = SimpleNamespace(
                waiting=None, running=SimpleNamespace(), terminated=None
            )
        else:
            pod.metadata.creation_timestamp = datetime.now(timezone.utc)
            status.state.waiting = SimpleNamespace(
                reason="ErrImagePull", message="registry retry"
            )
        return pod

    def read_namespaced_pod(self, **kwargs):
        pod = super().read_namespaced_pod(**kwargs)
        if not self.ready:
            self.loop.call_soon_threadsafe(self.observed.set)
        return pod


class OwnerProviders:
    is_available = True
    in_cluster = True

    def __init__(self, providers):
        self.providers = providers

    async def create_workspace(self, owner, **kwargs):
        return await self.providers[owner.id].create_workspace(owner, **kwargs)

    async def workspace_pod_live(self, owner, **kwargs):
        return await self.providers[owner.id].workspace_pod_live(owner, **kwargs)

    async def attest_workspace_runtime(self, owner, **kwargs):
        return await self.providers[owner.id].attest_workspace_runtime(owner, **kwargs)


class DispatchStore:
    """Scope discovery to this case; all authority and claims use real PostgreSQL."""

    manifests_ready = False

    def __init__(self, database, jobs, agents):
        self.database, self.jobs, self.agents = database, jobs, agents

    def __getattr__(self, name):
        return getattr(self.database, name)

    async def get_dispatchable_jobs(self, **_):
        rows = [await self.database.get_job(job) for job in self.jobs]
        return [row for row in rows if row["status"] == "created"]

    async def get_available_agents(self, **_):
        return [await self.database.get_agent(agent) for agent in self.agents]

    async def get_preemption_candidates(self):
        return []


async def scenario(db, monkeypatch):
    for variable in ("AGENT_IMAGE", "PERSISTENT_AGENT_IMAGE", "VM_MODE"):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setattr(
        provider_module, "wait_for_agent_ssh", AsyncMock(return_value=(True, 1, None))
    )
    monkeypatch.setattr(
        provider_module, "workspace_private_key_fingerprint", lambda _: FINGERPRINT
    )
    monkeypatch.setattr(
        provider_module,
        "_isolated_pod_exec",
        lambda *args, **kwargs: f"256 {FINGERPRINT} workspace (ED25519)",
    )
    providers, jobs = {}, []
    observed = asyncio.Event()
    for ready in (False, True, True):
        job_id = str(uuid4())
        await db.execute(
            "INSERT INTO jobs(id,description,status,execution_lane,config_override) "
            "VALUES($1::uuid,'dispatcher isolation','created','pinned',$2::jsonb)",
            job_id,
            json.dumps({"workspace": {"backend": "sandbox"}}),
        )
        provider = pull._provisioner(
            monkeypatch, db, WorkspaceCluster(ready=ready, observed=observed)
        )
        providers[job_id] = provider
        jobs.append(job_id)
        if ready:
            assert await provider.create_workspace(WorkspaceOwner.job(job_id))
            workspace = await pull._workspace(db, UUID(job_id))
            assert workspace["status"] == "ready"
    agents = []
    for index in range(2):
        agent = await db.register_agent(
            "worker_base", f"10.42.1.{index + 1}", hostname=f"dispatcher-{uuid4()}"
        )
        assert await db.heartbeat(str(agent["agent_id"]), "ready")
        agents.append(str(agent["agent_id"]))
    store = DispatchStore(db, jobs, agents)
    delivery = dispatch_tests.FakeDelivery()
    delivered = asyncio.Event()
    delivered_ids = []

    async def dispatch(job, agent):
        delivered_ids.append(str(job["id"]))
        if len(delivered_ids) == 2:
            delivered.set()
        return True

    delivery.dispatch = dispatch
    router = OwnerProviders(providers)
    authority = SimpleNamespace(store=db, workspace_provisioner=router)
    dependencies = dataclasses.replace(
        dispatch_tests._deps(store, delivery=delivery),
        completion_control_boundary=SimpleNamespace(dispatch_guard_kwargs=lambda: {}),
        container_provisioner=router,
        job_needs_sandbox=lambda job: True,
        prepare_job_workspace_runtime=lambda job: prepare_job_workspace_runtime(
            job, dependencies=authority
        ),
    )
    return SimpleNamespace(
        jobs=jobs,
        providers=providers,
        observed=observed,
        delivered=delivered,
        delivered_ids=delivered_ids,
        dependencies=dependencies,
    )


@pytest.mark.asyncio
async def test_two_ready_jobs_claim_without_slow_predecessor(
    db, monkeypatch, no_dispatcher_error
):
    case = await scenario(db, monkeypatch)
    case.dependencies.store.jobs = case.jobs[1:]
    try:
        await dispatch_pending_jobs(dependencies=case.dependencies)
        await asyncio.wait_for(case.delivered.wait(), 1)
        assert set(case.delivered_ids) == set(case.jobs[1:])
        for job_id in case.jobs[1:]:
            assert (await db.get_job(job_id))["status"] == "processing"
    finally:
        await case.dependencies.state.drain()


@pytest.mark.asyncio
@pytest.mark.parametrize("blocker", ["image_pull", "mutation_lock"])
async def test_slow_workspace_does_not_block_two_ready_job_claims(
    db, monkeypatch, blocker, no_dispatcher_error
):
    case = await scenario(db, monkeypatch)
    owner = WorkspaceOwner.job(case.jobs[0])
    entered = case.observed

    @asynccontextmanager
    async def held_lock():
        if blocker == "mutation_lock":
            entered.clear()
            original = case.providers[owner.id]._workspace_mutation_guard

            @asynccontextmanager
            async def observe_guard(*args, **kwargs):
                entered.set()
                async with original(*args, **kwargs) as acquired:
                    yield acquired

            monkeypatch.setattr(
                case.providers[owner.id], "_workspace_mutation_guard", observe_guard
            )
            async with db.workspace_runtime_mutation_lock(
                owner.id, owner_kind="job", scope="workspace_container", wait=False
            ) as acquired:
                assert acquired
                yield
        else:
            yield

    async with held_lock():
        task = asyncio.create_task(
            dispatch_pending_jobs(dependencies=case.dependencies)
        )
        try:
            await asyncio.wait_for(entered.wait(), 5)
            # Source boundary reached: the first Job has an exact open receipt.
            receipt = await db.fetchrow(
                "SELECT * FROM managed_repository_workspace_creation_reservations "
                "WHERE owner_kind='job' AND owner_id=$1::uuid AND settled_at IS NULL",
                owner.id,
            )
            assert receipt is not None
            if blocker == "image_pull":
                assert receipt["pod_uid"] is not None
            else:
                assert receipt["external_mutation_started_at"] is None
            await asyncio.wait_for(case.delivered.wait(), 1)
            assert set(case.delivered_ids) == set(case.jobs[1:])
            for job_id in case.jobs[1:]:
                assert (await db.get_job(job_id))["status"] == "processing"
            assert (await db.get_job(owner.id))["status"] == "created"
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await case.dependencies.state.drain()


@pytest.mark.asyncio
async def test_shutdown_joins_sdk_before_releasing_real_owner_lock(
    db, monkeypatch, no_dispatcher_error
):
    case = await scenario(db, monkeypatch)
    owner = WorkspaceOwner.job(case.jobs[0])
    cluster = case.providers[owner.id]._core_api
    create = cluster.create_namespaced_pod
    entered, release = threading.Event(), threading.Event()

    def blocked_create(**kwargs):
        entered.set()
        assert release.wait(10)
        return create(**kwargs)

    monkeypatch.setattr(cluster, "create_namespaced_pod", blocked_create)
    try:
        await dispatch_pending_jobs(dependencies=case.dependencies)
        async with asyncio.timeout(5):
            while not entered.is_set():
                await asyncio.sleep(0.01)
        await asyncio.wait_for(case.delivered.wait(), 1)
        drain = asyncio.create_task(case.dependencies.state.drain())
        await asyncio.sleep(0.02)
        assert not drain.done()
        async with db.workspace_runtime_mutation_lock(
            owner.id, owner_kind="job", scope="workspace_container", wait=False
        ) as acquired:
            assert not acquired
        release.set()
        await asyncio.wait_for(drain, 5)
        async with db.workspace_runtime_mutation_lock(
            owner.id, owner_kind="job", scope="workspace_container", wait=False
        ) as acquired:
            assert acquired
        assert not case.dependencies.state.tasks
        # Interruption is not evidence that the now-joined POST did not run.
        receipt = await db.fetchrow(
            "SELECT * FROM managed_repository_workspace_creation_reservations "
            "WHERE owner_kind='job' AND owner_id=$1::uuid AND settled_at IS NULL",
            owner.id,
        )
        assert receipt is not None
        assert receipt["external_mutation_started_at"] is not None
    finally:
        release.set()
        await case.dependencies.state.drain()
