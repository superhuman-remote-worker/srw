"""Pinned cancellation publishes terminal authority before destructive retirement."""

from __future__ import annotations

from tests import _b09_control_seams as control_seams

from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID, uuid4

import asyncpg
from fastapi import FastAPI
import httpx
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer

from orchestrator.database.migrate import run_migrations
from orchestrator.database.postgres import PostgresDB
from orchestrator.routers import job_lifecycle as job_lifecycle_routes
from orchestrator.services.completion_control import CompletionControl
from tests.test_non_pinned_workspace_lifecycle_real_postgres import (
    _create_settled_authoritative_runtime,
)
from orchestrator.application import controls as controls_composition
from orchestrator.security import access as access_module
from orchestrator.services import job_dispatcher as job_dispatcher_module
from orchestrator.services import (
    job_freeze_notifications as job_freeze_notifications_module,
)
from orchestrator.services import job_mutation_controls as job_mutation_controls_module
from orchestrator.services import session_wake as session_wake_module
from orchestrator.services import subjob_completion as subjob_completion_module
from orchestrator.services import thread_retirement as thread_retirement_module
import functools


JOB_ID = "11111111-1111-4111-8111-111111111111"
CHILD_ID = "22222222-2222-4222-8222-222222222222"


def _bind_runtime(monkeypatch, store, enabled):
    from orchestrator import main

    monkeypatch.setattr(main.app.state.resources, "postgres_db", store)
    monkeypatch.setattr(
        main.app.state.resources.settings, "completion_commands_enabled", enabled
    )
    monkeypatch.setattr(
        subjob_completion_module,
        "handle_scholar_completion",
        AsyncMock(),
    )
    monkeypatch.setattr(session_wake_module, "maybe_wake_session", AsyncMock())
    monkeypatch.setattr(session_wake_module, "kick_drain", Mock())
    monkeypatch.setattr(job_dispatcher_module, "trigger_dispatch", Mock())
    monkeypatch.setattr(
        job_freeze_notifications_module,
        "resolve_job_notifications",
        AsyncMock(),
    )
    return main


@asynccontextmanager
async def _cancel_client(main, monkeypatch, store):
    async def access(_request, _store, job_id):
        return None, await store.get_job(job_id)

    monkeypatch.setattr(access_module, "require_internal_or_job_access", access)
    app = FastAPI()
    app.state.job_control_route_dependencies_factory = functools.partial(
        controls_composition.job_mutation_route_dependencies, main.app.state.resources
    )
    app.add_api_route(
        "/api/jobs/{job_id}/cancel", job_lifecycle_routes.cancel_job, methods=["PUT"]
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


@asynccontextmanager
async def _legacy_bulk_connection():
    yield SimpleNamespace(execute=AsyncMock(return_value="UPDATE 1"))


def _job(status="pending_review", *, job_id=JOB_ID, lane="pinned"):
    return {
        "id": job_id,
        "status": status,
        "execution_lane": lane,
        "assigned_agent_id": None,
        "context": {},
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_root_cancel_publishes_terminal_authority_before_retirement(
    monkeypatch, enabled
):
    row = _job()
    order = []

    async def publish(*_args, **_kwargs):
        order.append("cancel")
        row["status"] = "cancelled"
        return True

    store = SimpleNamespace(
        get_job=AsyncMock(side_effect=lambda _: dict(row)),
        linearize_pinned_cancel=AsyncMock(side_effect=publish),
        cancel_job=AsyncMock(return_value=False),
        delete_checkpoint_thread=AsyncMock(side_effect=lambda _: order.append("prune")),
    )
    main = _bind_runtime(monkeypatch, store, enabled)

    async def cleanup(_operations, _job_id):
        assert row["status"] == "cancelled", "retirement needs terminal authority"
        order.append("retire")

    monkeypatch.setattr(
        thread_retirement_module.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        cleanup,
    )
    monkeypatch.setattr(
        job_mutation_controls_module.JobControlOperations,
        "cascade_cancel_to_children",
        AsyncMock(return_value=True),
    )
    async with _cancel_client(main, monkeypatch, store) as client:
        response = await client.put(f"/api/jobs/{JOB_ID}/cancel")
    assert response.status_code == 200, response.json()
    assert response.json() == {"status": "cancelled"}
    assert order == ["cancel", "retire", "prune"]
    store.linearize_pinned_cancel.assert_awaited_once_with(
        JOB_ID, expected_status="pending_review", completion_commands_enabled=enabled
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("winner", ["completed", "processing"])
async def test_root_cancel_cas_loser_does_not_retire_or_prune(
    monkeypatch, enabled, winner
):
    store = SimpleNamespace(
        get_job=AsyncMock(side_effect=[_job(), _job(winner)]),
        linearize_pinned_cancel=AsyncMock(return_value=False),
        cancel_job=AsyncMock(return_value=False),
        delete_checkpoint_thread=AsyncMock(),
    )
    main = _bind_runtime(monkeypatch, store, enabled)
    cleanup = AsyncMock()
    cascade = AsyncMock(return_value=True)
    monkeypatch.setattr(
        thread_retirement_module.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        cleanup,
    )
    monkeypatch.setattr(
        job_mutation_controls_module.JobControlOperations,
        "cascade_cancel_to_children",
        cascade,
    )
    async with _cancel_client(main, monkeypatch, store) as client:
        response = await client.put(f"/api/jobs/{JOB_ID}/cancel")
    assert response.status_code == 400
    cleanup.assert_not_awaited()
    cascade.assert_not_awaited()
    store.cancel_job.assert_not_awaited()
    store.delete_checkpoint_thread.assert_not_awaited()
    store.linearize_pinned_cancel.assert_awaited_once_with(
        JOB_ID, expected_status="pending_review", completion_commands_enabled=enabled
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_descendant_cancellation_publishes_before_cleanup(monkeypatch, enabled):
    row = _job(job_id=CHILD_ID)
    order = []

    async def publish(*_args, **_kwargs):
        order.append("cancel")
        row["status"] = "cancelled"
        return True

    store = SimpleNamespace(
        get_descendant_jobs=AsyncMock(side_effect=lambda *_a, **_k: [dict(row)]),
        linearize_pinned_cancel=AsyncMock(side_effect=publish),
        get_job=AsyncMock(side_effect=lambda _: dict(row)),
    )
    _bind_runtime(monkeypatch, store, enabled)

    async def cleanup(_operations, _job_id):
        assert row["status"] == "cancelled", "child retirement needs terminal authority"
        order.append("retire")

    monkeypatch.setattr(
        thread_retirement_module.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        cleanup,
    )
    assert await control_seams.cascade_cancel_to_children(JOB_ID)
    assert order == ["cancel", "retire"]
    store.linearize_pinned_cancel.assert_awaited_once_with(
        CHILD_ID, expected_status="pending_review", completion_commands_enabled=enabled
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("winner,settled", [("completed", True), ("processing", False)])
async def test_descendant_cas_loser_preserves_winner_workspace(
    monkeypatch, enabled, winner, settled
):
    store = SimpleNamespace(
        get_descendant_jobs=AsyncMock(return_value=[_job(job_id=CHILD_ID)]),
        linearize_pinned_cancel=AsyncMock(return_value=False),
        get_job=AsyncMock(return_value=_job(winner, job_id=CHILD_ID)),
        acquire=_legacy_bulk_connection,
    )
    _bind_runtime(monkeypatch, store, enabled)
    cleanup = AsyncMock()
    target = AsyncMock()
    monkeypatch.setattr(
        thread_retirement_module.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        cleanup,
    )
    monkeypatch.setattr(
        controls_composition, "prepare_pinned_job_mutation_target", target
    )
    assert await control_seams.cascade_cancel_to_children(JOB_ID) is settled
    cleanup.assert_not_awaited()
    target.assert_not_awaited()
    store.linearize_pinned_cancel.assert_awaited_once_with(
        CHILD_ID, expected_status="pending_review", completion_commands_enabled=enabled
    )


@pytest.fixture(scope="module")
def cancellation_pg_dsn():
    with PostgresContainer("postgres:15") as container:
        yield container.get_connection_url().replace(
            "postgresql+psycopg2", "postgresql"
        )


@pytest_asyncio.fixture(scope="module")
async def cancellation_schema(cancellation_pg_dsn):
    migrations = (
        Path(__file__).resolve().parents[1] / "src/orchestrator/database/migrations/app"
    )
    async with asyncpg.create_pool(cancellation_pg_dsn, min_size=1, max_size=2) as pool:
        await run_migrations(pool, migrations)
    async with AsyncPostgresSaver.from_conn_string(cancellation_pg_dsn) as saver:
        await saver.setup()


@pytest_asyncio.fixture
async def cancellation_db(cancellation_pg_dsn, cancellation_schema, monkeypatch):
    monkeypatch.setenv("CHECKPOINTER_BACKEND", "postgres")
    store = PostgresDB(
        connection_string=cancellation_pg_dsn, min_connections=1, max_connections=4
    )
    await store.connect()
    try:
        yield store
    finally:
        await store.close()


async def _runtime_and_checkpoint(db, status):
    job, runtime, _, _ = await _create_settled_authoritative_runtime(
        db, owner_kind="job", scope="workspace_container"
    )
    async with db.acquire() as conn:
        await conn.execute("UPDATE jobs SET status=$2 WHERE id=$1", job, status)
        await conn.execute(
            "INSERT INTO checkpoints (thread_id,checkpoint_ns,checkpoint_id,checkpoint) "
            "VALUES ($1,'','cancel-retirement','{}'::jsonb)",
            str(job),
        )
    return str(job), runtime


async def _checkpoint_count(db, job_id):
    async with db.acquire() as conn:
        return await conn.fetchval(
            "SELECT count(*) FROM checkpoints WHERE thread_id=$1", job_id
        )


def _capture_args(runtime):
    return dict(
        owner_kind="job",
        scope="workspace_container",
        runtime_incarnation=runtime,
        target_disposition="deleted",
        reclaim_shared_resources=True,
        pod_uid=runtime,
        pvc_uid=str(uuid4()),
        service_uid=str(uuid4()),
        resources_captured=True,
    )


async def _capture_resources(db, intent, args):
    claimed = await db.claim_managed_repository_workspace_cleanup_intent(
        str(intent["id"]), claimant="cancel-retirement-proof"
    )
    assert claimed is not None
    captured = await db.record_managed_repository_workspace_cleanup_resources(
        str(intent["id"]),
        claimant=claimed["claimed_by"],
        claim_token=claimed["claim_token"],
        pod_uid=args["pod_uid"],
        seed_configmap_uid=None,
        pvc_uid=args["pvc_uid"],
        service_uid=args["service_uid"],
    )
    assert captured is not None
    return captured


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_real_postgres_terminal_intent_requires_cancel_but_preserves_checkpoint(
    cancellation_db, enabled
):
    db = cancellation_db
    job_id, runtime = await _runtime_and_checkpoint(db, "pending_review")
    args = _capture_args(runtime)
    assert (
        await db.prepare_managed_repository_workspace_cleanup_intent(job_id, **args)
        is None
    )
    assert await _checkpoint_count(db, job_id) == 1
    assert await db.linearize_pinned_cancel(
        job_id, expected_status="pending_review", completion_commands_enabled=enabled
    )
    assert await _checkpoint_count(db, job_id) == 1
    intent = await db.prepare_managed_repository_workspace_cleanup_intent(
        job_id, **args
    )
    assert intent is not None
    assert intent["resource_policy"] == "terminal_reclaim"
    captured = await _capture_resources(db, intent, args)
    assert captured["capture_complete"] is True
    assert str(captured["pod_uid"]) == runtime
    assert await _checkpoint_count(db, job_id) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("initial_status", ["pending_review", "cancelled"])
async def test_real_postgres_cancel_retry_keeps_checkpoint_until_retirement_succeeds(
    cancellation_db, monkeypatch, enabled, initial_status
):
    db = cancellation_db
    job_id, runtime = await _runtime_and_checkpoint(db, initial_status)
    args = _capture_args(runtime)
    main = _bind_runtime(monkeypatch, db, enabled)
    monkeypatch.setattr(
        job_mutation_controls_module.JobControlOperations,
        "cascade_cancel_to_children",
        AsyncMock(return_value=True),
    )
    retirement_ready = False
    attempts = []

    async def cleanup(_operations, owner_id):
        # Use the real immutable cleanup intent guard, not a permissive mock.
        intent = await db.prepare_managed_repository_workspace_cleanup_intent(
            owner_id, **args
        )
        assert intent is not None, "pending_review must not authorize terminal reclaim"
        captured = await _capture_resources(db, intent, args)
        assert captured["capture_complete"] is True
        assert await _checkpoint_count(db, owner_id) == 1
        attempts.append(str(intent["id"]))
        if not retirement_ready:
            raise RuntimeError("controlled exact-runtime cleanup failure")

    monkeypatch.setattr(
        thread_retirement_module.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        cleanup,
    )
    async with _cancel_client(main, monkeypatch, db) as client:
        response = await client.put(f"/api/jobs/{job_id}/cancel")
        assert response.status_code == 503
        assert response.json() == {
            "detail": "Workspace authority retirement is incomplete"
        }
        assert (await db.get_job(job_id))["status"] == "cancelled"
        assert len(attempts) == 1
        assert await _checkpoint_count(db, job_id) == 1
        retirement_ready = True
        response = await client.put(f"/api/jobs/{job_id}/cancel")
    assert response.status_code == 200, response.json()
    assert response.json() == {"status": "cancelled"}
    assert attempts == [attempts[0], attempts[0]], (
        "retry must retain the captured cleanup owner"
    )
    assert await _checkpoint_count(db, job_id) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_stateless_root_keeps_queue_owned_cancellation(monkeypatch, enabled):
    store = SimpleNamespace(
        get_job=AsyncMock(return_value=_job("processing", lane="stateless")),
        cancel_stateless_job=AsyncMock(return_value=(True, True)),
        linearize_pinned_cancel=AsyncMock(),
        cancel_job=AsyncMock(),
        delete_checkpoint_thread=AsyncMock(),
    )
    main = _bind_runtime(monkeypatch, store, enabled)
    cleanup = AsyncMock()
    settle = AsyncMock(return_value=True)
    monkeypatch.setattr(
        thread_retirement_module.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        cleanup,
    )
    monkeypatch.setattr(
        job_mutation_controls_module.JobControlOperations,
        "cascade_cancel_to_children",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        job_mutation_controls_module.JobControlOperations,
        "wait_for_stateless_cancel_settle",
        settle,
    )
    async with _cancel_client(main, monkeypatch, store) as client:
        response = await client.put(f"/api/jobs/{JOB_ID}/cancel")
    assert response.status_code == 200, response.json()
    store.cancel_stateless_job.assert_awaited_once()
    settle.assert_awaited_once_with(JOB_ID)
    store.linearize_pinned_cancel.assert_not_awaited()
    store.cancel_job.assert_not_awaited()
    store.delete_checkpoint_thread.assert_not_awaited()
    cleanup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_real_postgres_root_claim_honors_completion_flag(
    cancellation_db, monkeypatch, enabled
):
    db = cancellation_db
    job_id, _ = await _runtime_and_checkpoint(db, "pending_review")
    control = CompletionControl(db, AsyncMock())
    await control.claim_job(
        job_id,
        source="mode_a_accept",
        expected_status="pending_review",
        expected_lane="pinned",
    )
    main = _bind_runtime(monkeypatch, db, enabled)
    cleanup = AsyncMock()
    cascade = AsyncMock(return_value=True)
    monkeypatch.setattr(
        thread_retirement_module.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        cleanup,
    )
    monkeypatch.setattr(
        job_mutation_controls_module.JobControlOperations,
        "cascade_cancel_to_children",
        cascade,
    )
    async with _cancel_client(main, monkeypatch, db) as client:
        response = await client.put(f"/api/jobs/{job_id}/cancel")
    if enabled:
        assert response.status_code == 409, response.json()
        assert (await db.get_job(job_id))["status"] == "pending_review"
        assert await _checkpoint_count(db, job_id) == 1
        cleanup.assert_not_awaited()
        cascade.assert_not_awaited()
    else:
        assert response.status_code == 200, response.json()
        assert (await db.get_job(job_id))["status"] == "cancelled"
        assert await _checkpoint_count(db, job_id) == 0
        cleanup.assert_awaited_once_with(job_id)
        cascade.assert_awaited_once_with(job_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("descendant", [False, True])
async def test_pinned_cancel_signals_original_assignment_after_cas(
    monkeypatch, enabled, descendant
):
    row = _job("processing", job_id=CHILD_ID if descendant else JOB_ID)
    original_agent = "33333333-3333-4333-8333-333333333333"
    row["assigned_agent_id"] = original_agent
    order = []

    async def publish(*_args, **_kwargs):
        row.update(status="cancelled", assigned_agent_id=None)
        order.append("cancel")
        return True

    store = SimpleNamespace(
        get_job=AsyncMock(side_effect=lambda _: dict(row)),
        get_descendant_jobs=AsyncMock(side_effect=lambda *_a, **_k: [dict(row)]),
        linearize_pinned_cancel=AsyncMock(side_effect=publish),
        cancel_job=AsyncMock(return_value=False),
        delete_checkpoint_thread=AsyncMock(),
    )
    main = _bind_runtime(monkeypatch, store, enabled)
    recipient = {"job_id": row["id"], "agent_id": original_agent}
    target = AsyncMock(
        return_value=SimpleNamespace(
            agent={"pod_ip": "127.0.0.1", "pod_port": 8000},
            recipient=SimpleNamespace(model_dump=Mock(return_value=recipient)),
        )
    )
    monkeypatch.setattr(
        controls_composition, "prepare_pinned_job_mutation_target", target
    )
    client = Mock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)

    async def signal(*_args, **_kwargs):
        assert row["status"] == "cancelled"
        assert row["assigned_agent_id"] is None
        order.append("signal")
        return httpx.Response(200, json={"graceful": True})

    client.post = AsyncMock(side_effect=signal)
    monkeypatch.setattr(httpx, "AsyncClient", Mock(return_value=client))
    monkeypatch.setattr(
        thread_retirement_module.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        AsyncMock(),
    )
    if descendant:
        assert await control_seams.cascade_cancel_to_children(JOB_ID)
    else:
        monkeypatch.setattr(
            access_module,
            "require_internal_or_job_access",
            AsyncMock(return_value=(None, dict(row))),
        )
        monkeypatch.setattr(
            job_mutation_controls_module.JobControlOperations,
            "cascade_cancel_to_children",
            AsyncMock(return_value=True),
        )
        assert await control_seams.cancel_job(Mock(), JOB_ID) == {"status": "cancelled"}
    assert order == ["cancel", "signal"]
    # The composition function takes the application's resources first.
    target.assert_awaited_once_with(
        main.app.state.resources,
        agent_id=original_agent,
        job_id=row["id"],
        require_idle=False,
    )
    assert client.post.await_args.kwargs["json"]["recipient"] == recipient
    store.linearize_pinned_cancel.assert_awaited_once_with(
        row["id"], expected_status="processing", completion_commands_enabled=enabled
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_real_postgres_descendant_claim_honors_completion_flag(
    cancellation_db, monkeypatch, enabled
):
    db = cancellation_db
    root_id, _ = await _runtime_and_checkpoint(db, "pending_review")
    child_id, _ = await _runtime_and_checkpoint(db, "pending_review")
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET parent_job_id=$2 WHERE id=$1",
            UUID(child_id),
            UUID(root_id),
        )
    await CompletionControl(db, AsyncMock()).claim_job(
        child_id,
        source="mode_a_accept",
        expected_status="pending_review",
        expected_lane="pinned",
    )
    _bind_runtime(monkeypatch, db, enabled)
    cleanup = AsyncMock()
    monkeypatch.setattr(
        thread_retirement_module.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        cleanup,
    )
    assert await control_seams.cascade_cancel_to_children(root_id) is not enabled
    child = await db.get_job(child_id)
    if enabled:
        assert child["status"] == "pending_review"
        cleanup.assert_not_awaited()
    else:
        assert child["status"] == "cancelled"
        cleanup.assert_awaited_once_with(child_id)
    assert await _checkpoint_count(db, child_id) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_real_postgres_root_retry_revisits_cancelled_descendant(
    cancellation_db, monkeypatch, enabled
):
    db = cancellation_db
    root_id, root_runtime = await _runtime_and_checkpoint(db, "pending_review")
    child_id, child_runtime = await _runtime_and_checkpoint(db, "pending_review")
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET parent_job_id=$2 WHERE id=$1",
            UUID(child_id),
            UUID(root_id),
        )
    main = _bind_runtime(monkeypatch, db, enabled)
    args_by_owner = {
        root_id: _capture_args(root_runtime),
        child_id: _capture_args(child_runtime),
    }
    attempts = []
    child_retirement_ready = False

    async def cleanup(_operations, owner_id):
        intent = await db.prepare_managed_repository_workspace_cleanup_intent(
            owner_id, **args_by_owner[owner_id]
        )
        assert intent is not None, "each owner must be terminal before cleanup"
        captured = await _capture_resources(db, intent, args_by_owner[owner_id])
        assert captured["capture_complete"] is True
        assert await _checkpoint_count(db, owner_id) == 1
        attempts.append((owner_id, str(intent["id"])))
        if owner_id == child_id and not child_retirement_ready:
            raise RuntimeError("controlled descendant retirement failure")

    monkeypatch.setattr(
        thread_retirement_module.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        cleanup,
    )
    async with _cancel_client(main, monkeypatch, db) as client:
        first = await client.put(f"/api/jobs/{root_id}/cancel")
        assert first.status_code == 503, first.json()
        assert first.json() == {
            "detail": "Descendant workspace authority retirement is incomplete"
        }
        assert (await db.get_job(root_id))["status"] == "cancelled"
        assert (await db.get_job(child_id))["status"] == "cancelled"
        assert [owner for owner, _ in attempts] == [child_id]
        assert await _checkpoint_count(db, root_id) == 1
        # The ordinary active-only view must remain unchanged for pause callers.
        assert await db.get_descendant_jobs(root_id) == []
        child_retirement_ready = True
        second = await client.put(f"/api/jobs/{root_id}/cancel")
    assert second.status_code == 200, second.json()
    assert [owner for owner, _ in attempts] == [child_id, child_id, root_id]
    assert attempts[0] == attempts[1], "child retry must reuse exact captured intent"
    assert await _checkpoint_count(db, root_id) == 0
