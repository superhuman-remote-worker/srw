"""Active completion control must safely refuse pre-Ready Job Cancel."""

import asyncio
import json
import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException

from orchestrator.database.container_startup_stage import (
    ScheduledAt,
    StageBudgets,
    StartupAttention,
)
from orchestrator.routers.job_lifecycle import cancel_job
from orchestrator.services.completion_control import (
    completion_control_claim_active,
    completion_control_claim_detail,
)
from orchestrator.services.job_mutation_controls import JobControlOperations
from orchestrator.services.manifest_execution import ManifestExecutionService
from orchestrator.services.connector_drivers import builtin_connector_drivers
from tests import test_workspace_pull_failure_real_postgres as fixtures


db = fixtures.db
pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied


async def _pre_ready_exit17(db, monkeypatch, *, active_claim: bool):
    async def new_stateless_job(store):
        job_id = uuid4()
        async with store.acquire() as conn:
            await conn.execute(
                "INSERT INTO jobs (id, description, status, execution_lane) "
                "VALUES ($1, 'pre-Ready exit 17', 'created', 'stateless')",
                job_id,
            )
        return job_id

    monkeypatch.setattr(fixtures, "_job", new_stateless_job)
    job, _owner, cluster, _provider, pod_uid = await fixtures._interrupted_pull(
        db, monkeypatch
    )
    pod = cluster.objects["pod"]
    pod.status.phase = "Failed"
    (container,) = pod.status.container_statuses
    container.ready = False
    container.started = True
    container.restart_count = 0
    container.container_id = "containerd://exit17-workspace"
    container.image_id = "containerd://exit17-image"
    container.state = SimpleNamespace(
        waiting=None,
        running=None,
        terminated=SimpleNamespace(
            exit_code=17,
            reason="Error",
            started_at=datetime.now(timezone.utc),
            finished_at=datetime.now(timezone.utc),
        ),
    )

    receipt = await db.fetchrow(
        "SELECT id, claim_token, phase, pod_uid FROM "
        "managed_repository_workspace_creation_reservations "
        "WHERE owner_kind='job' AND owner_id=$1 AND scope='workspace_container' "
        "AND operation_kind='create' AND settled_at IS NULL",
        job,
    )
    assert receipt["phase"] == "runtime_bound"
    assert str(receipt["pod_uid"]) == pod_uid
    observe = dict(
        owner_kind="job",
        owner_id=str(job),
        reservation_id=str(receipt["id"]),
        claim_token=int(receipt["claim_token"]),
        pod_uid=pod_uid,
    )
    scheduled_at = datetime.now(timezone.utc)
    assert await db.observe_container_startup(
        **observe,
        observation=ScheduledAt(scheduled_at),
        budgets=StageBudgets(ready_seconds=0.05, pull_seconds=0.05, ssh_seconds=1),
        adopt_if_unmarked=True,
    )
    await asyncio.sleep(0.08)
    assert await db.observe_container_startup(
        **observe, observation=StartupAttention("readiness_deadline")
    )
    before = await db.fetchrow(
        "SELECT startup_protocol_version, startup_state, startup_reason_code, "
        "startup_first_ready_at, cancel_requested_at FROM "
        "managed_repository_workspace_creation_reservations WHERE id=$1",
        receipt["id"],
    )
    assert (
        before["startup_protocol_version"],
        before["startup_state"],
        before["startup_reason_code"],
        before["startup_first_ready_at"],
        before["cancel_requested_at"],
    ) == (1, "attention", "readiness_deadline", None, None)
    assert (
        await db.fetchval(
            "SELECT count(*) FROM run_queue WHERE unit_kind='worker_batch' "
            "AND unit_id=$1",
            job,
        )
        == 0
    )
    assert (await db.get_job(str(job)))["execution_lane"] == "stateless"
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO srw_execution_specs "
            "(work_kind,work_id,document,resolved,revision,harness_adapter) "
            "VALUES('Job',$1,$2::jsonb,$3::jsonb,$4,'srw/v1')",
            job,
            json.dumps({}),
            json.dumps({"spec": {}}),
            str(uuid4()),
        )
        if active_claim:
            await conn.execute(
                "UPDATE jobs SET context = COALESCE(context,'{}'::jsonb) || "
                "jsonb_build_object('_completion_control_claim', "
                "jsonb_build_object('version',1,'source','active-control',"
                "'expires_epoch',extract(epoch FROM clock_timestamp())+3600)) "
                "WHERE id=$1",
                job,
            )
    if active_claim:
        assert (
            await db.fetchval(
                "SELECT context->'_completion_control_claim'->>'version'='1' "
                "AND (context->'_completion_control_claim'->>'expires_epoch')::numeric "
                "> extract(epoch FROM clock_timestamp()) FROM jobs WHERE id=$1",
                job,
            )
            is True
        )
        assert isinstance((await db.get_job(str(job)))["context"], str)
    monkeypatch.setattr(db, "manifests_ready", True, raising=False)
    manifest = ManifestExecutionService(
        db,
        runtime=None,
        namespace="test",
        connector_drivers=builtin_connector_drivers(),
    )
    operations = JobControlOperations(
        SimpleNamespace(
            store=db,
            logger=logging.getLogger(__name__),
            manifest_cancel=manifest.cancel,
            completion_control=SimpleNamespace(
                dispatch_guard_kwargs=lambda: {"completion_commands_enabled": True},
                active_claim=lambda row: completion_control_claim_active(
                    row.get("context")
                ),
                claim_detail=lambda row: completion_control_claim_detail(
                    row.get("context")
                ),
            ),
        )
    )

    async def already_settled(_job_id):
        # Isolate the request/terminal transaction; external teardown is not
        # what this regression test exercises.
        return True

    async def no_completion_hooks(_job_id, _job):
        return None

    monkeypatch.setattr(operations, "wait_for_stateless_cancel_settle", already_settled)
    monkeypatch.setattr(operations, "_finish_cancel", no_completion_hooks)

    async def authorized(_request, store, job_id):
        assert store is db and job_id == str(job)
        return None, await db.get_job(job_id)

    route = SimpleNamespace(
        require_internal_or_job_access=authorized,
        store=db,
        operations=operations,
    )
    return SimpleNamespace(
        job=job, cluster=cluster, pod_uid=pod_uid, receipt=receipt, route=route
    )


@pytest.mark.asyncio
async def test_stateless_cancel_active_claim_returns_409_for_pre_ready_exit17(
    db, monkeypatch
):
    case = await _pre_ready_exit17(db, monkeypatch, active_claim=True)
    job, pod_uid, receipt = case.job, case.pod_uid, case.receipt
    with pytest.raises(HTTPException) as failure:
        await cancel_job(None, str(job), dependencies=case.route)
    assert (
        await db.fetchval(
            "SELECT count(*) FROM run_queue WHERE unit_kind='worker_batch' "
            "AND unit_id=$1",
            job,
        )
        == 0
    )
    assert (await db.get_job(str(job)))["status"] == "created"
    after = await db.fetchrow(
        "SELECT claim_token, cancel_requested_at, runtime_incarnation "
        "FROM managed_repository_workspace_creation_reservations WHERE id=$1",
        receipt["id"],
    )
    assert after["claim_token"] == receipt["claim_token"]
    assert after["cancel_requested_at"] is None
    assert str(after["runtime_incarnation"]) == pod_uid
    assert await fixtures._open_authority(db, job) == {
        "reservations": 1,
        "intents": 0,
    }
    assert case.cluster.objects["pod"].metadata.uid == pod_uid
    assert failure.value.status_code == 409
    assert failure.value.detail.startswith("job control is in progress")


@pytest.mark.asyncio
async def test_stateless_cancel_without_claim_hands_off_exact_pre_ready_exit17(
    db, monkeypatch
):
    case = await _pre_ready_exit17(db, monkeypatch, active_claim=False)
    job, pod_uid, receipt = case.job, case.pod_uid, case.receipt
    assert await cancel_job(None, str(job), dependencies=case.route) == {
        "status": "cancelled"
    }
    assert (await db.get_job(str(job)))["status"] == "cancelled"
    after = await db.fetchrow(
        "SELECT claim_token, cancel_requested_at, runtime_incarnation "
        "FROM managed_repository_workspace_creation_reservations WHERE id=$1",
        receipt["id"],
    )
    assert after["claim_token"] != receipt["claim_token"]
    assert after["cancel_requested_at"] is not None
    assert str(after["runtime_incarnation"]) == pod_uid
    assert await fixtures._open_authority(db, job) == {
        "reservations": 1,
        "intents": 1,
    }
