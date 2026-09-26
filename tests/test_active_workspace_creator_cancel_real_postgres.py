"""Owner Cancel must not depend on a creator finishing an image readiness wait."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException, Request

from orchestrator import main
from orchestrator.application import controls
from orchestrator.routers import job_lifecycle
from orchestrator.services import container_provisioner as provider_module
from orchestrator.services.job_mutation_controls import JobControlOperations
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from tests import test_workspace_pull_failure_real_postgres as fixtures

db = fixtures.db
pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied


class PullingCluster(fixtures.NeverPullingCluster):
    def create_namespaced_pod(self, *, body, **kwargs):
        pod = super().create_namespaced_pod(body=body, **kwargs)
        pod.status.container_statuses[0].state.waiting.reason = "ImagePullBackOff"
        pod.status.container_statuses[0].state.waiting.message = "Pulling custom image"
        return pod


def _route_dependencies(db, provider, monkeypatch):
    resources = main.app.state.resources
    monkeypatch.setattr(resources, "postgres_db", db)
    monkeypatch.setattr(provider_module, "container_provisioner", provider)
    dependencies = controls.job_mutation_route_dependencies(resources)
    # Only authentication and unrelated completion notifications are modeled.
    # The route, cancellation owner, store, physical guard and cleanup are real.
    operations = JobControlOperations(
        replace(
            dependencies.operations.dependencies,
            handle_scholar_completion=AsyncMock(),
            maybe_wake_session=AsyncMock(),
            kick_session_wake_drain=lambda: None,
            trigger_dispatch=lambda: None,
            resolve_job_notifications=AsyncMock(),
        )
    )

    async def authorized_owner(request, store, job_id):
        assert store is db
        return {"id": "fixture-owner"}, await db.get_job(job_id)

    return replace(
        dependencies,
        operations=operations,
        require_internal_or_job_access=authorized_owner,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("creator_still_running", [False, True])
@pytest.mark.parametrize("execution_lane", ["pinned", "stateless"])
async def test_owner_cancel_during_image_wait(
    db, monkeypatch, creator_still_running, execution_lane
):
    job = await fixtures._job(db)
    if execution_lane == "stateless":
        await db.execute("UPDATE jobs SET execution_lane='stateless' WHERE id=$1", job)
    owner = WorkspaceOwner.job(str(job))
    cluster = PullingCluster()
    provider = fixtures._provisioner(monkeypatch, db, cluster)
    entered_readiness = asyncio.Event()
    real_wait = provider._wait_for_ready

    async def observed_readiness(*args, **kwargs):
        entered_readiness.set()
        return await real_wait(*args, **kwargs)

    monkeypatch.setattr(provider, "_wait_for_ready", observed_readiness)
    creator = asyncio.create_task(provider.create_workspace(owner))
    try:
        await asyncio.wait_for(entered_readiness.wait(), timeout=10)
        assert not creator.done()
        assert (await fixtures._workspace(db, job))["_runtime_incarnation"] == (
            cluster.objects["pod"].metadata.uid
        )
        assert await fixtures._open_authority(db, job) == {
            "reservations": 1,
            "intents": 0,
        }
        if not creator_still_running:
            # Healthy control: the actual readiness loop observes the failure
            # and exits naturally. There is no mocked Ready or creator return.
            cluster.objects["pod"].status.container_statuses[
                0
            ].state.waiting.reason = "InvalidImageName"
            assert await asyncio.wait_for(creator, timeout=5) is False

        async with db.workspace_runtime_mutation_lock(
            str(job), owner_kind="job", scope="workspace_container", wait=False
        ) as acquired:
            mutation_busy = not acquired
        request = Request(
            {"type": "http", "method": "PUT", "path": f"/api/jobs/{job}/cancel"}
        )
        try:
            result = await asyncio.wait_for(
                job_lifecycle.cancel_job(
                    request,
                    str(job),
                    dependencies=_route_dependencies(db, provider, monkeypatch),
                ),
                timeout=5,
            )
        except HTTPException as exc:
            current = await db.get_job(str(job))
            reservation = await db.fetchrow(
                "SELECT phase,cancel_requested_at FROM "
                "managed_repository_workspace_creation_reservations "
                "WHERE owner_id=$1 AND settled_at IS NULL",
                job,
            )
            pytest.fail(
                f"Cancel refused while creator_running={creator_still_running}: "
                f"HTTP {exc.status_code} {exc.detail}; mutation_busy={mutation_busy}; "
                f"job_status={current['status']}; reservation={dict(reservation or {})}"
            )
        assert result["status"] == "cancelled"
        assert (await db.get_job(str(job)))["status"] == "cancelled"
        if creator_still_running:
            assert await asyncio.wait_for(asyncio.shield(creator), timeout=5) is False
        assert await fixtures._open_authority(db, job) == {
            "reservations": 0,
            "intents": 0,
        }
        assert cluster.objects == {}
    finally:
        # A failing red must not leave a creator or its session advisory lock
        # alive; cancellation is test cleanup only, never part of the assertion.
        if not creator.done():
            creator.cancel()
        await asyncio.gather(creator, return_exceptions=True)


async def _cancel_route(db, provider, monkeypatch, job):
    return await job_lifecycle.cancel_job(
        Request({"type": "http", "method": "PUT", "path": f"/api/jobs/{job}/cancel"}),
        str(job),
        dependencies=_route_dependencies(db, provider, monkeypatch),
    )


async def _wait_until(predicate):
    async with asyncio.timeout(10):
        while not predicate():
            await asyncio.sleep(0.01)


def _observe_wait(provider, monkeypatch):
    entered = asyncio.Event()
    wait = provider._wait_for_ready

    async def observe(*args, **kwargs):
        entered.set()
        return await wait(*args, **kwargs)

    monkeypatch.setattr(provider, "_wait_for_ready", observe)
    return entered


@pytest.mark.asyncio
@pytest.mark.parametrize("execution_lane", ["pinned", "stateless"])
async def test_started_sdk_retains_guard_until_uid_is_published(
    db, monkeypatch, execution_lane
):
    import threading

    job = await fixtures._job(db)
    if execution_lane == "stateless":
        await db.execute("UPDATE jobs SET execution_lane='stateless' WHERE id=$1", job)
    owner = WorkspaceOwner.job(str(job))
    cluster = PullingCluster()
    provider = fixtures._provisioner(monkeypatch, db, cluster)
    started, release = threading.Event(), threading.Event()
    create_pod = cluster.create_namespaced_pod
    waiting = _observe_wait(provider, monkeypatch)

    def blocked_sdk(**kwargs):
        pod = create_pod(**kwargs)
        started.set()
        assert release.wait(15)
        return pod

    monkeypatch.setattr(cluster, "create_namespaced_pod", blocked_sdk)
    creator = asyncio.create_task(provider.create_workspace(owner))
    try:
        await _wait_until(started.is_set)
        assert "pod" in cluster.objects
        reservation = await db.fetchrow(
            "SELECT pod_uid FROM managed_repository_workspace_creation_reservations "
            "WHERE owner_id=$1 AND settled_at IS NULL",
            job,
        )
        assert reservation["pod_uid"] is None
        with pytest.raises(
            HTTPException, match="Workspace mutation is still in progress"
        ) as refused:
            await _cancel_route(db, provider, monkeypatch, job)
        assert refused.value.status_code == 409
        assert refused.value.headers == {"Retry-After": "1"}
        current = await db.get_job(str(job))
        assert current["status"] == "created"
        assert (
            await db.fetchval(
                "SELECT COALESCE(context, '{}'::jsonb) ? '_stateless_cancel_cleanup_pending' "
                "FROM jobs WHERE id=$1",
                job,
            )
            is False
        )
        assert await fixtures._open_authority(db, job) == {
            "reservations": 1,
            "intents": 0,
        }
        assert not creator.done()
        release.set()
        await asyncio.wait_for(waiting.wait(), 10)
        assert (await fixtures._workspace(db, job))[
            "_runtime_incarnation"
        ] == cluster.objects["pod"].metadata.uid
        assert (await _cancel_route(db, provider, monkeypatch, job))[
            "status"
        ] == "cancelled"
        assert await asyncio.wait_for(creator, 5) is False
        assert cluster.objects == {}
    finally:
        release.set()
        if not creator.done():
            creator.cancel()
        await asyncio.gather(creator, return_exceptions=True)


def _make_ready(cluster, monkeypatch):
    import sys
    from types import SimpleNamespace
    from orchestrator.services import ssh_helpers

    # Model only the external kubelet/SSH transport. The production readiness
    # helper really starts and joins an owned subprocess; publication is real.
    pod = cluster.objects["pod"]
    pod.status.phase = "Running"
    for status in pod.status.container_statuses:
        status.ready = True
        status.state = SimpleNamespace(
            waiting=None, running=SimpleNamespace(), terminated=None
        )
    pod.spec.containers = [
        SimpleNamespace(**container) for container in pod.spec.containers
    ]
    monkeypatch.setattr(
        provider_module, "workspace_private_key_fingerprint", lambda _: "test-key"
    )
    monkeypatch.setattr(
        ssh_helpers,
        "build_agent_ssh_cmd",
        lambda *args, **kwargs: [sys.executable, "-c", "raise SystemExit(0)"],
    )


async def _wait_for_row_contender(conn, task):
    async with asyncio.timeout(10):
        while True:
            if task.done():
                await task
                raise AssertionError(
                    "Cancel completed before the owner lock was released"
                )
            await conn.execute("SELECT pg_stat_clear_snapshot()")
            if await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity "
                "WHERE datname=current_database() AND pid<>pg_backend_pid() "
                "AND wait_event_type='Lock' AND query LIKE '%SELECT id FROM jobs%FOR UPDATE%')"
            ):
                return
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_wins", [True, False])
async def test_cancel_and_ready_finalization_serialize_in_postgres(
    db, monkeypatch, cancel_wins
):
    job = await fixtures._job(db)
    owner = WorkspaceOwner.job(str(job))
    cluster = PullingCluster()
    provider = fixtures._provisioner(monkeypatch, db, cluster)
    waiting = _observe_wait(provider, monkeypatch)
    finalizing, release = asyncio.Event(), asyncio.Event()
    complete = provider._complete_prepared_workspace

    async def held_completion(*args, **kwargs):
        finalizing.set()
        await release.wait()
        return await complete(*args, **kwargs)

    monkeypatch.setattr(provider, "_complete_prepared_workspace", held_completion)
    creator = asyncio.create_task(provider.create_workspace(owner))
    cancelling = None
    try:
        await asyncio.wait_for(waiting.wait(), 10)
        if cancel_wins:
            async with db.acquire() as conn:
                async with conn.transaction():
                    await conn.fetchrow(
                        "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", job
                    )
                    cancelling = asyncio.create_task(
                        db.linearize_pinned_cancel(str(job), expected_status="created")
                    )
                    await _wait_for_row_contender(conn, cancelling)
                    _make_ready(cluster, monkeypatch)
            assert await asyncio.wait_for(cancelling, 5) is True
            assert await asyncio.wait_for(creator, 5) is False
            assert not finalizing.is_set()
            assert (await fixtures._workspace(db, job))[
                "status"
            ] == "retiring_process_zero"
        else:
            _make_ready(cluster, monkeypatch)
            await asyncio.wait_for(finalizing.wait(), 10)
            async with db.workspace_runtime_mutation_lock(
                str(job), owner_kind="job", scope="workspace_container", wait=False
            ) as acquired:
                assert not acquired
            with pytest.raises(
                HTTPException, match="Workspace mutation is still in progress"
            ):
                await _cancel_route(db, provider, monkeypatch, job)
            assert (await fixtures._workspace(db, job))["status"] == "created"
            release.set()
            assert await asyncio.wait_for(creator, 5) is True
            assert (await fixtures._workspace(db, job))["status"] == "ready"
            # The full Ready-workspace retirement attestation is a separate
            # protocol. This race proves its actual terminal SQL can win once
            # finalization has released the guard, preserving exact cleanup.
            assert await db.cancel_job(str(job)) is True
            assert await fixtures._open_authority(db, job) == {
                "reservations": 0,
                "intents": 1,
            }
        assert (await db.get_job(str(job)))["status"] == "cancelled"
        if cancel_wins:
            assert await fixtures._open_authority(db, job) == {
                "reservations": 1,
                "intents": 1,
            }
    finally:
        release.set()
        for task in (creator, cancelling):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (creator, cancelling) if task is not None),
            return_exceptions=True,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["pod", "pvc", "service", "database"])
async def test_observation_never_publishes_replaced_or_unavailable_authority(
    db, monkeypatch, changed
):
    from uuid import uuid4

    job = await fixtures._job(db)
    owner = WorkspaceOwner.job(str(job))
    cluster = PullingCluster()
    provider = fixtures._provisioner(monkeypatch, db, cluster)
    waiting = _observe_wait(provider, monkeypatch)
    creator = asyncio.create_task(provider.create_workspace(owner))
    try:
        await asyncio.wait_for(waiting.wait(), 10)
        _make_ready(cluster, monkeypatch)
        if changed == "database":

            async def unavailable(*args, **kwargs):
                raise ConnectionError("test unavailable authority store")

            monkeypatch.setattr(
                type(db),
                "managed_repository_workspace_creation_claim_is_current",
                unavailable,
            )
        else:
            cluster.objects[changed].metadata.uid = str(uuid4())
        assert await asyncio.wait_for(creator, 5) is False
        assert (await fixtures._workspace(db, job))["status"] == "created"
        assert await fixtures._open_authority(db, job) == {
            "reservations": 1,
            "intents": 0,
        }
        assert cluster.pod_deletes == 0
    finally:
        if not creator.done():
            creator.cancel()
        await asyncio.gather(creator, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["ip", "readiness"])
async def test_ready_observation_revalidated_after_guard_reacquisition(
    db, monkeypatch, change
):
    job = await fixtures._job(db)
    owner = WorkspaceOwner.job(str(job))
    cluster = PullingCluster()
    provider = fixtures._provisioner(monkeypatch, db, cluster)
    waiting = _observe_wait(provider, monkeypatch)
    check = provider._prepared_workspace_authority_is_current

    async def changed_before_finalization(*args, **kwargs):
        pod = cluster.objects["pod"]
        if change == "ip":
            pod.status.pod_ip = "10.42.0.101"
        else:
            pod.status.container_statuses[0].ready = False
        return await check(*args, **kwargs)

    monkeypatch.setattr(
        provider,
        "_prepared_workspace_authority_is_current",
        changed_before_finalization,
    )
    creator = asyncio.create_task(provider.create_workspace(owner))
    try:
        await asyncio.wait_for(waiting.wait(), 10)
        _make_ready(cluster, monkeypatch)
        assert await asyncio.wait_for(creator, 5) is False
        assert (await fixtures._workspace(db, job))["status"] == "created"
        assert await fixtures._open_authority(db, job) == {
            "reservations": 1,
            "intents": 0,
        }
    finally:
        if not creator.done():
            creator.cancel()
        await asyncio.gather(creator, return_exceptions=True)


@pytest.mark.asyncio
async def test_owner_cancel_during_ssh_observation_joins_probe_before_exit(
    db, monkeypatch, tmp_path
):
    import sys
    from orchestrator.services import ssh_helpers

    job = await fixtures._job(db)
    owner = WorkspaceOwner.job(str(job))
    cluster = PullingCluster()
    provider = fixtures._provisioner(monkeypatch, db, cluster)
    waiting = _observe_wait(provider, monkeypatch)
    started = asyncio.Event()
    processes = []
    release = tmp_path / "release-probe"
    spawn = ssh_helpers.create_owned_subprocess_exec

    async def observed_spawn(*args, **kwargs):
        process = await spawn(*args, **kwargs)
        processes.append(process)
        started.set()
        return process

    monkeypatch.setattr(ssh_helpers, "create_owned_subprocess_exec", observed_spawn)
    creator = asyncio.create_task(provider.create_workspace(owner))
    try:
        await asyncio.wait_for(waiting.wait(), 10)
        _make_ready(cluster, monkeypatch)
        monkeypatch.setattr(
            ssh_helpers,
            "build_agent_ssh_cmd",
            lambda *args, **kwargs: [
                sys.executable,
                "-c",
                "import pathlib,sys,time\np=pathlib.Path(sys.argv[1])\nwhile not p.exists(): time.sleep(0.01)",
                str(release),
            ],
        )
        await asyncio.wait_for(started.wait(), 10)
        assert (
            await db.linearize_pinned_cancel(str(job), expected_status="created")
            is True
        )
        assert processes[0].returncode is None
        assert not creator.done()
        release.touch()
        assert await asyncio.wait_for(creator, 5) is False
        assert processes[0].returncode == 0
        assert (await fixtures._workspace(db, job))["status"] == "retiring_process_zero"
        assert await fixtures._open_authority(db, job) == {
            "reservations": 1,
            "intents": 1,
        }
    finally:
        release.touch()
        if not creator.done():
            creator.cancel()
        await asyncio.gather(creator, return_exceptions=True)


@pytest.mark.asyncio
async def test_dispatcher_does_not_overwrite_owner_cancel_after_creator_returns(
    db, monkeypatch
):
    from types import SimpleNamespace
    from orchestrator.services import job_dispatcher
    from tests import test_b11_job_dispatcher as dispatcher_cases

    job = await fixtures._job(db)
    cluster = PullingCluster()
    provider = fixtures._provisioner(monkeypatch, db, cluster)
    waiting = _observe_wait(provider, monkeypatch)

    class SingleOwnerStore:
        manifests_ready = False

        def __getattr__(self, name):
            return getattr(db, name)

        async def get_dispatchable_jobs(self, **kwargs):
            return [await db.get_job(str(job))]

    dependencies = replace(
        dispatcher_cases._deps(SingleOwnerStore()),
        completion_control_boundary=SimpleNamespace(dispatch_guard_kwargs=lambda: {}),
        container_provisioner=provider,
        job_needs_sandbox=lambda _: True,
    )
    dispatcher = asyncio.create_task(
        job_dispatcher.dispatch_pending_jobs(dependencies=dependencies)
    )
    try:
        await asyncio.wait_for(waiting.wait(), 10)
        assert (await _cancel_route(db, provider, monkeypatch, job))[
            "status"
        ] == "cancelled"
        await asyncio.wait_for(dispatcher, 5)
        while dependencies.state.tasks:
            await asyncio.wait_for(asyncio.gather(*list(dependencies.state.tasks)), 5)
        assert (await db.get_job(str(job)))["status"] == "cancelled"
    finally:
        if not dispatcher.done():
            dispatcher.cancel()
        await asyncio.gather(dispatcher, return_exceptions=True)
        await dependencies.state.drain()
