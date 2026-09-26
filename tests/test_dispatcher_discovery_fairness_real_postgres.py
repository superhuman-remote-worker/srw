"""Production discovery must reach healthy Jobs beyond persistent first pages."""

from __future__ import annotations

import asyncio
import dataclasses
import json
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from orchestrator.database.dispatch_discovery import JobDiscoveryCursor
from orchestrator.services import job_dispatcher as scheduler
from orchestrator.services import container_provisioner as provider_module
from orchestrator.services.job_dispatcher import dispatch_pending_jobs
from orchestrator.services.job_workspace_authority import prepare_job_workspace_runtime
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from tests import test_b11_job_dispatcher as dispatch_cases
from tests import test_dispatcher_workspace_isolation_real_postgres as isolation
from tests import test_workspace_pull_failure_real_postgres as pull

pg_dsn = pull.pg_dsn
db = pull.db
_schema_applied = pull._schema_applied
no_dispatcher_error = dispatch_cases.no_dispatcher_error


class ObservedDiscovery:
    """Record the real SQL pages without filtering or replacing their rows."""

    manifests_ready = False

    def __init__(self, database):
        self.database = database
        self.pages = {"pinned": [], "stateless": []}

    def __getattr__(self, name):
        return getattr(self.database, name)

    async def get_dispatchable_jobs(self, **kwargs):
        rows = await self.database.get_dispatchable_jobs(**kwargs)
        self.pages["pinned"].append([str(row["id"]) for row in rows])
        return rows

    async def get_admittable_stateless_jobs(self, **kwargs):
        rows = await self.database.get_admittable_stateless_jobs(**kwargs)
        self.pages["stateless"].append([str(row["id"]) for row in rows])
        return rows


async def _case(db, monkeypatch):
    for variable in ("AGENT_IMAGE", "PERSISTENT_AGENT_IMAGE", "VM_MODE"):
        monkeypatch.delenv(variable, raising=False)
    monkeypatch.setattr(
        provider_module, "wait_for_agent_ssh", AsyncMock(return_value=(True, 1, None))
    )
    monkeypatch.setattr(
        provider_module,
        "workspace_private_key_fingerprint",
        lambda _: isolation.FINGERPRINT,
    )
    monkeypatch.setattr(
        provider_module,
        "_isolated_pod_exec",
        lambda *args, **kwargs: f"256 {isolation.FINGERPRINT} workspace (ED25519)",
    )
    providers = {}
    store = ObservedDiscovery(db)
    router = isolation.OwnerProviders(providers)
    authority = SimpleNamespace(store=db, workspace_provisioner=router)
    delivery = dispatch_cases.FakeDelivery()
    deps = dataclasses.replace(
        dispatch_cases._deps(store, delivery=delivery, stateless_worker_enabled=True),
        completion_control_boundary=SimpleNamespace(dispatch_guard_kwargs=lambda: {}),
        container_provisioner=router,
        job_needs_sandbox=lambda job: True,
        prepare_job_workspace_runtime=lambda job: prepare_job_workspace_runtime(
            job, dependencies=authority
        ),
    )
    agent = await db.register_agent(
        "worker_base", "10.42.1.1", hostname=f"fairness-{uuid4()}"
    )
    assert await db.heartbeat(agent["agent_id"], "ready")
    return SimpleNamespace(providers=providers, store=store, deps=deps)


async def _workspace(db, monkeypatch, case, *, lane, ready):
    job_id = str(uuid4())
    await db.execute(
        "INSERT INTO jobs(id,description,status,execution_lane,config_override,priority) "
        "VALUES($1::uuid,'discovery fairness','created',$2,$3::jsonb,5)",
        job_id,
        lane,
        json.dumps({"workspace": {"backend": "sandbox"}}),
    )
    observed = asyncio.Event()
    cluster = isolation.WorkspaceCluster(ready=ready, observed=observed)
    provider = pull._provisioner(monkeypatch, db, cluster)
    case.providers[job_id] = provider
    if ready:
        assert await provider.create_workspace(WorkspaceOwner.job(job_id))
        assert (await pull._workspace(db, UUID(job_id)))["status"] == "ready"
    else:
        create = asyncio.create_task(
            provider.create_workspace(WorkspaceOwner.job(job_id))
        )
        try:
            await asyncio.wait_for(observed.wait(), 5)
            receipt = await db.fetchrow(
                "SELECT * FROM managed_repository_workspace_creation_reservations "
                "WHERE owner_kind='job' AND owner_id=$1::uuid AND settled_at IS NULL",
                job_id,
            )
            assert receipt is not None and receipt["pod_uid"] is not None
        finally:
            create.cancel()
            await asyncio.gather(create, return_exceptions=True)
        runtime = await pull._workspace(db, UUID(job_id))
        assert runtime["status"] in {"creating", "created"}
        assert (await db.get_job(job_id))["status"] == "created"
    return job_id


async def _tick(case):
    await dispatch_pending_jobs(dependencies=case.deps)
    async with asyncio.timeout(15):
        while case.deps.state.tasks:
            await asyncio.gather(*list(case.deps.state.tasks))
            await asyncio.sleep(0)


async def _progress(db, healthy):
    pinned = (await db.get_job(healthy["pinned"]))["status"] == "processing"
    stateless = bool(
        await db.fetchval(
            "SELECT EXISTS(SELECT 1 FROM run_queue WHERE unit_id=$1::uuid "
            "AND unit_kind='worker_batch' AND state IN ('queued','leased'))",
            healthy["stateless"],
        )
    )
    return {"pinned": pinned, "stateless": stateless}


@pytest.mark.asyncio
async def test_production_discovery_healthy_controls_admit_both_lanes(
    db, monkeypatch, no_dispatcher_error
):
    case = await _case(db, monkeypatch)
    healthy = {}
    try:
        for lane in ("pinned", "stateless"):
            healthy[lane] = await _workspace(
                db, monkeypatch, case, lane=lane, ready=True
            )
        await _tick(case)
        assert await _progress(db, healthy) == {"pinned": True, "stateless": True}
    finally:
        await case.deps.state.drain()
        # Ordinary terminal writes exclude this control's rows from the next
        # case's real, unfiltered discovery queries; no lifecycle proof changes.
        for job_id in healthy.values():
            await db.update_job_status(job_id, status="cancelled")


@pytest.mark.asyncio
async def test_later_healthy_jobs_progress_past_51_open_creations_per_lane(
    db, monkeypatch, no_dispatcher_error
):
    case = await _case(db, monkeypatch)
    pending = {"pinned": [], "stateless": []}
    healthy = {}
    try:
        for lane in ("pinned", "stateless"):
            for _ in range(51):
                pending[lane].append(
                    await _workspace(db, monkeypatch, case, lane=lane, ready=False)
                )
            healthy[lane] = await _workspace(
                db, monkeypatch, case, lane=lane, ready=True
            )
        for _ in range(4):
            await _tick(case)
        assert all(
            len(page) <= 50 for pages in case.store.pages.values() for page in pages
        )
        # Both physical Ready publications and the first-page pending holds
        # came from production helpers. Repeated ticks must discover later work.
        observed = await _progress(db, healthy)
        assert observed == {"pinned": True, "stateless": True}, {
            "progress": observed,
            "distinct_discovered": {
                lane: len({job for page in pages for job in page})
                for lane, pages in case.store.pages.items()
            },
            "healthy_discovered": {
                lane: healthy[lane] in {job for page in pages for job in page}
                for lane, pages in case.store.pages.items()
            },
        }
    finally:
        await case.deps.state.drain()
        for job_id in [*pending["pinned"], *pending["stateless"], *healthy.values()]:
            await db.update_job_status(job_id, status="cancelled")


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_keyset_pages_tied_and_null_timestamps_without_skips(db, lane):
    cutoff = await db.get_job_discovery_cutoff()
    base = (uuid4().int >> 16) << 16
    ids = [UUID(int=base + i) for i in range(1, 7)]
    created = cutoff - timedelta(seconds=1)
    try:
        for job_id, priority, stamp in zip(
            ids, [8, 8, 8, 7, 7, 7], [created, created, None, created, None, None]
        ):
            await db.execute(
                "INSERT INTO jobs(id,description,status,execution_lane,priority,created_at) "
                "VALUES($1,'keyset','created',$2,$3,$4)",
                job_id,
                lane,
                priority,
                stamp,
            )
        read = (
            db.get_dispatchable_jobs
            if lane == "pinned"
            else db.get_admittable_stateless_jobs
        )
        seen, after = [], None
        for _ in range(4):
            page = await read(limit=2, discovery_after=after, discovery_cutoff=cutoff)
            seen.extend(row["id"] for row in page)
            if not page:
                break
            after = JobDiscoveryCursor.from_job(page[-1])
        assert seen == ids
    finally:
        await db.execute("DELETE FROM jobs WHERE id=ANY($1::uuid[])", ids)


@pytest.mark.asyncio
async def test_priority_arrivals_and_new_tail_cannot_extend_frozen_sweep(db):
    ids = []
    store = ObservedDiscovery(db)
    deps = dataclasses.replace(
        dispatch_cases._deps(store),
        completion_control_boundary=SimpleNamespace(dispatch_guard_kwargs=lambda: {}),
    )

    async def insert(count, priority):
        new = [uuid4() for _ in range(count)]
        ids.extend(new)
        async with db.acquire() as conn:
            await conn.executemany(
                "INSERT INTO jobs(id,description,status,priority) VALUES($1,'sweep','created',$2)",
                [(job_id, priority) for job_id in new],
            )
        return {str(job_id) for job_id in new}

    try:
        old = await insert(80, 5)
        observed = {str(job["id"]) for job in await scheduler._discover_jobs(deps)}
        urgent = await insert(15, 9)
        tail = await insert(60, 1)
        next_page = await scheduler._discover_jobs(deps)
        observed.update(str(job["id"]) for job in next_page)
        assert len(urgent & {str(job["id"]) for job in next_page}) == 10
        for _ in range(8):
            await insert(5, 1)
            observed.update(
                str(job["id"]) for job in await scheduler._discover_jobs(deps)
            )
        assert old | urgent | tail <= observed
        assert all(len(page) <= 40 for page in store.pages["pinned"])
        assert len(store.pages["pinned"]) == 20
    finally:
        await deps.state.drain()
        await db.execute("DELETE FROM jobs WHERE id=ANY($1::uuid[])", ids)


@pytest.mark.asyncio
async def test_external_creator_ready_reclassifies_queued_job_during_two_live_pulls(
    db, monkeypatch, no_dispatcher_error
):
    case = await _case(db, monkeypatch)
    owners, clusters, observations = [], [], []
    external = None
    try:
        for _ in range(3):
            job_id = str(uuid4())
            owners.append(job_id)
            await db.execute(
                "INSERT INTO jobs(id,description,status,priority,config_override) "
                "VALUES($1::uuid,'external completion','created',5,$2::jsonb)",
                job_id,
                json.dumps({"workspace": {"backend": "sandbox"}}),
            )
            observed = asyncio.Event()
            cluster = isolation.WorkspaceCluster(ready=False, observed=observed)
            observations.append(observed)
            clusters.append(cluster)
            case.providers[job_id] = pull._provisioner(monkeypatch, db, cluster)
        external = asyncio.create_task(
            case.providers[owners[2]].create_workspace(WorkspaceOwner.job(owners[2]))
        )
        await asyncio.wait_for(observations[2].wait(), 5)
        await dispatch_pending_jobs(dependencies=case.deps)
        await asyncio.wait_for(
            asyncio.gather(*(event.wait() for event in observations[:2])), 10
        )
        assert owners[2] in case.deps.state.pending
        assert len(case.deps.state.active) == 2
        # The kubelet completes the same actual creator's pull. Production
        # create_workspace publishes Ready; no row/receipt/trigger is bypassed.
        pod = clusters[2].objects["pod"]
        pod.status.phase = "Running"
        status = pod.status.container_statuses[0]
        status.ready = status.started = True
        status.state = SimpleNamespace(
            waiting=None, running=SimpleNamespace(), terminated=None
        )
        clusters[2].ready = True
        assert await asyncio.wait_for(external, 10)
        assert (await pull._workspace(db, UUID(owners[2])))["status"] == "ready"
        await dispatch_pending_jobs(dependencies=case.deps)
        async with asyncio.timeout(10):
            while (await db.get_job(owners[2]))["status"] != "processing":
                await asyncio.sleep(0.01)
        assert set(case.deps.state.active) == set(owners[:2])
        assert all(cluster.pod_deletes == 0 for cluster in clusters)
    finally:
        if external is not None and not external.done():
            external.cancel()
            await asyncio.gather(external, return_exceptions=True)
        await case.deps.state.drain()
        for job_id in owners:
            await db.update_job_status(job_id, status="cancelled")


@pytest.mark.asyncio
async def test_keyset_query_explain_on_representative_backlog(db, monkeypatch):
    ids = [uuid4() for _ in range(1600)]
    queries = []
    original_acquire = db.acquire
    try:
        async with original_acquire() as conn:
            await conn.executemany(
                "INSERT INTO jobs(id,description,status,execution_lane,priority) VALUES($1,'explain',$2,$3,$4)",
                [
                    (
                        job_id,
                        "created" if i < 800 else "completed",
                        "pinned" if i % 2 else "stateless",
                        i % 7,
                    )
                    for i, job_id in enumerate(ids)
                ],
            )
            await conn.execute("ANALYZE jobs")
        cutoff = await db.get_job_discovery_cutoff()

        @asynccontextmanager
        async def observed_acquire():
            async with original_acquire() as conn:

                async def fetch(query, *args):
                    queries.append((query, args))
                    return await conn.fetch(query, *args)

                yield SimpleNamespace(fetch=fetch)

        monkeypatch.setattr(db, "acquire", observed_acquire)
        for read in (db.get_dispatchable_jobs, db.get_admittable_stateless_jobs):
            first = await read(limit=40, discovery_cutoff=cutoff)
            assert len(first) == 40
            later = await read(
                limit=40,
                discovery_cutoff=cutoff,
                discovery_after=JobDiscoveryCursor.from_job(first[-1]),
            )
            assert len(later) == 40
            assert {row["id"] for row in first}.isdisjoint(row["id"] for row in later)
        monkeypatch.setattr(db, "acquire", original_acquire)
        plans = []
        async with original_acquire() as conn:
            for query, args in queries:
                raw = await conn.fetchval(
                    "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query, *args
                )
                plan = json.loads(raw) if isinstance(raw, str) else raw
                plans.append(plan)
        target = Path(
            ".superpowers/sdd/2026-09-26-workspace-reliability-acceptance-plan/dispatcher-discovery-explain.json"
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(plans, indent=2))
    finally:
        monkeypatch.setattr(db, "acquire", original_acquire)
        await db.execute("DELETE FROM jobs WHERE id=ANY($1::uuid[])", ids)
