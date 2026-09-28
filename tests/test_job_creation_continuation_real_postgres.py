"""Exact Job startup continuation on the migrated PostgreSQL authority."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from orchestrator.database.container_startup_stage import (
    BoundPodObserved,
    ReadyObservedAt,
    ScheduledAt,
    StageBudgets,
)
from orchestrator.services.workspace_lifecycle import (
    EnsureOutcome,
    WorkspaceOwner,
    ensure_workspace,
)
from orchestrator.services import container_provisioner as provider_module
from orchestrator.services.container_provisioner import WorkspaceRuntimeAuthorityError
from tests import test_container_startup_stage_real_postgres as stage
from tests import test_workspace_pull_failure_real_postgres as fixtures


db = fixtures.db
pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied


@pytest.mark.asyncio
async def test_gate_off_bridge_accepts_existing_v1_job_as_pending(db, monkeypatch):
    job_id, receipt, pod_uid, _ = await stage._bound_job(db)
    assert await db.observe_container_startup(
        **stage._observe_kwargs(job_id, receipt, pod_uid),
        observation=BoundPodObserved(),
        adopt_if_unmarked=True,
    )
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "false")
    cluster = fixtures.NeverPullingCluster()
    provisioner = fixtures._provisioner(monkeypatch, db, cluster)
    result = await ensure_workspace(
        WorkspaceOwner.job(str(job_id)),
        provisioner=provisioner,
        suspension=SimpleNamespace(),
        current_status=None,
    )
    assert result.outcome is EnsureOutcome.PENDING
    assert cluster.objects == {}
    unchanged = await stage._reservation(db, receipt)
    assert unchanged["startup_protocol_version"] == 1
    assert unchanged["phase"] == "runtime_bound"
    assert unchanged["settled_at"] is None


@pytest.mark.asyncio
async def test_new_job_waiting_for_schedule_remains_pending_on_same_pod(
    db, monkeypatch
):
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    job_id = await fixtures._job(db)
    cluster = fixtures.NeverPullingCluster()
    provisioner = fixtures._provisioner(monkeypatch, db, cluster)
    result = await ensure_workspace(
        WorkspaceOwner.job(str(job_id)),
        provisioner=provisioner,
        suspension=SimpleNamespace(),
        current_status=None,
    )
    assert result.outcome is EnsureOutcome.PENDING
    pod = cluster.objects["pod"]
    current = await provisioner.get_current_workspace_creation_result(
        WorkspaceOwner.job(str(job_id)), operation_kind="create"
    )
    assert current["startup_protocol_version"] == 1
    assert current["phase"] == "runtime_bound"
    assert current["settled_at"] is None
    assert str(current["pod_uid"]) == pod.metadata.uid
    assert not await provisioner.continue_job_workspace_creation(
        str(job_id), str(current["id"]), int(current["claim_token"]), str(uuid4())
    )
    assert not await provisioner.continue_job_workspace_creation(
        str(job_id),
        str(current["id"]),
        int(current["claim_token"]) + 1,
        pod.metadata.uid,
    )
    assert (await stage._reservation(db, current))["settled_at"] is None


@pytest.mark.asyncio
async def test_job_ready_cas_refuses_expired_frozen_execution(db):
    job_id, receipt, pod_uid, _ = await stage._bound_job(db)
    kwargs = stage._observe_kwargs(job_id, receipt, pod_uid)
    assert await db.observe_container_startup(
        **kwargs, observation=BoundPodObserved(), adopt_if_unmarked=True
    )
    now = datetime.now(timezone.utc)
    assert await db.observe_container_startup(
        **kwargs,
        observation=ScheduledAt(now),
        budgets=StageBudgets(120, None, 30),
    )
    assert await db.observe_container_startup(
        **kwargs, observation=ReadyObservedAt(datetime.now(timezone.utc))
    )
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO srw_execution_specs "
            "(work_kind,work_id,document,resolved,revision,harness_adapter,created_at) "
            "VALUES('Job',$1,$2::jsonb,$3::jsonb,$4,'srw/v1',$5)",
            job_id,
            json.dumps({}),
            json.dumps({"spec": {"timeoutSeconds": 30}}),
            str(uuid4()),
            now - timedelta(seconds=60),
        )
    assert (
        await db.complete_job_workspace_creation(
            str(job_id),
            runtime_incarnation=pod_uid,
            backing_id=f"k8s-pod:agent-workspaces:{pod_uid}",
            ssh_host_key_fingerprint="SHA256:" + "A" * 43,
            pod_ip="10.42.0.50",
            port=30022,
            creation_reservation_id=str(receipt["id"]),
            creation_claim_token=int(receipt["claim_token"]),
        )
        is None
    )
    assert (await stage._reservation(db, receipt))["settled_at"] is None
    assert (await fixtures._workspace(db, job_id))["status"] == "creating"


@pytest.mark.asyncio
async def test_job_ready_cas_refuses_execution_expired_while_waiting_for_receipt_lock(
    db,
):
    job_id, receipt, pod_uid, _ = await stage._bound_job(db)
    kwargs = stage._observe_kwargs(job_id, receipt, pod_uid)
    assert await db.observe_container_startup(
        **kwargs, observation=BoundPodObserved(), adopt_if_unmarked=True
    )
    scheduled_at = datetime.now(timezone.utc)
    assert await db.observe_container_startup(
        **kwargs,
        observation=ScheduledAt(scheduled_at),
        budgets=StageBudgets(120, None, 30),
    )
    assert await db.observe_container_startup(
        **kwargs, observation=ReadyObservedAt(datetime.now(timezone.utc))
    )
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO srw_execution_specs "
            "(work_kind,work_id,document,resolved,revision,harness_adapter,created_at) "
            "VALUES('Job',$1,$2::jsonb,$3::jsonb,$4,'srw/v1',clock_timestamp())",
            job_id,
            json.dumps({}),
            json.dumps({"spec": {"timeoutSeconds": 2}}),
            str(uuid4()),
        )
    async with db.acquire() as blocker:
        async with blocker.transaction():
            await blocker.fetchval(
                "SELECT id FROM managed_repository_workspace_creation_reservations "
                "WHERE id=$1 FOR UPDATE",
                receipt["id"],
            )
            completion = asyncio.create_task(
                db.complete_job_workspace_creation(
                    str(job_id),
                    runtime_incarnation=pod_uid,
                    backing_id=f"k8s-pod:agent-workspaces:{pod_uid}",
                    ssh_host_key_fingerprint="SHA256:" + "A" * 43,
                    pod_ip="10.42.0.50",
                    port=30022,
                    creation_reservation_id=str(receipt["id"]),
                    creation_claim_token=int(receipt["claim_token"]),
                )
            )
            await asyncio.sleep(0.5)
            assert not completion.done(), "completion did not wait for receipt lock"
            await asyncio.sleep(2.4)
    assert await asyncio.wait_for(completion, 5) is None
    assert (await stage._reservation(db, receipt))["settled_at"] is None
    assert (await fixtures._workspace(db, job_id))["status"] == "creating"
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT context ? '_workspace_binding' FROM jobs WHERE id=$1", job_id
            )
            is False
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_during_probe", [False, True])
async def test_waiting_job_pod_schedules_then_publishes_ready_same_uid(
    db, monkeypatch, cancel_during_probe
):
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    job_id = await fixtures._job(db)
    cluster = fixtures.NeverPullingCluster()
    provisioner = fixtures._provisioner(monkeypatch, db, cluster)
    owner = WorkspaceOwner.job(str(job_id))
    assert (
        await ensure_workspace(
            owner,
            provisioner=provisioner,
            suspension=SimpleNamespace(),
            current_status=None,
        )
    ).outcome is EnsureOutcome.PENDING
    pod = cluster.objects["pod"]
    uid = pod.metadata.uid
    receipt = await provisioner.get_current_workspace_creation_result(
        owner, operation_kind="create"
    )
    scheduled_at = datetime.now(timezone.utc)
    pod.spec.node_name = "node-1"
    pod.status.conditions = [
        SimpleNamespace(
            type="PodScheduled", status="True", last_transition_time=scheduled_at
        ),
        SimpleNamespace(type="Ready", status="True", last_transition_time=scheduled_at),
    ]
    pod.status.phase = "Running"
    pod.status.pod_ip = "10.42.0.50"
    for container in pod.status.container_statuses:
        container.ready = True
        container.state = SimpleNamespace(waiting=None, running=SimpleNamespace())
    provisioner._trusted_pod_ssh_identity = AsyncMock(
        return_value=(
            f"k8s-pvc:agent-workspaces:{receipt['pvc_uid']}",
            "SHA256:" + "A" * 43,
            uid,
        )
    )

    async def probe(*args, authority_check=None, **kwargs):
        if cancel_during_probe:
            cancelled = (
                await db.request_managed_repository_workspace_creation_cancellation(
                    str(job_id),
                    owner_kind="job",
                    scope="workspace_container",
                    target_disposition="deleted",
                    reclaim_shared_resources=False,
                    claimant="job-startup-cancel",
                )
            )
            assert cancelled is not None
            await authority_check()
        return True, 1, ""

    monkeypatch.setattr(
        provider_module, "wait_for_agent_ssh", AsyncMock(side_effect=probe)
    )
    monkeypatch.setattr(
        provider_module, "resolve_ssh_key_path", lambda: "/run/test-key"
    )
    monkeypatch.setattr(
        provider_module, "workspace_private_key_fingerprint", lambda _: "SHA256:test"
    )
    if cancel_during_probe:
        with pytest.raises(WorkspaceRuntimeAuthorityError):
            await provisioner.continue_job_workspace_creation(
                str(job_id), str(receipt["id"]), int(receipt["claim_token"]), uid
            )
        assert (await fixtures._workspace(db, job_id))["status"] == "creating"
        assert (await stage._reservation(db, receipt))["settled_at"] is None
        return
    assert await provisioner.continue_job_workspace_creation(
        str(job_id), str(receipt["id"]), int(receipt["claim_token"]), uid
    )
    closed = await stage._reservation(db, receipt)
    assert closed["settled_at"] is not None
    assert closed["scheduled_at"] == scheduled_at
    assert closed["pod_uid"] == receipt["pod_uid"]
    assert (await fixtures._workspace(db, job_id))["status"] == "ready"


@pytest.mark.asyncio
async def test_scan_only_open_exact_v1_job_and_excludes_settled_legacy(db):
    live_job, live_receipt, live_uid, _ = await stage._bound_job(db)
    assert await db.observe_container_startup(
        **stage._observe_kwargs(live_job, live_receipt, live_uid),
        observation=BoundPodObserved(),
        adopt_if_unmarked=True,
    )
    legacy_job, legacy_receipt, legacy_uid, gate = await stage._bound_job(db)
    assert await db.settle_managed_repository_workspace_creation_reservation(
        str(legacy_job), **gate, runtime_incarnation=legacy_uid
    )
    page = await db.list_current_job_creation_candidates(limit=32)
    live_candidates = [
        candidate for candidate in page["candidates"] if candidate["job_id"] == live_job
    ]
    assert len(live_candidates) == 1
    assert str(live_candidates[0]["pod_uid"]) == live_uid
    assert all(
        candidate["reservation_id"] != legacy_receipt["id"]
        for candidate in page["candidates"]
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=jsonb_set(context, "
            "'{workspace_container,status}', '\"created\"'::jsonb) WHERE id=$1",
            live_job,
        )
    page = await db.list_current_job_creation_candidates(limit=32)
    assert sum(candidate["job_id"] == live_job for candidate in page["candidates"]) == 1


@pytest.mark.asyncio
async def test_expired_claim_rotates_without_new_pod_or_new_stage_clock(
    db, monkeypatch
):
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    job_id = await fixtures._job(db)
    cluster = fixtures.NeverPullingCluster()
    provisioner = fixtures._provisioner(monkeypatch, db, cluster)
    owner = WorkspaceOwner.job(str(job_id))
    assert await provisioner.create_workspace(owner)
    first = await provisioner.get_current_workspace_creation_result(
        owner, operation_kind="create"
    )
    pod = cluster.objects["pod"]
    pod.spec.node_name = "node-1"
    scheduled_at = datetime.now(timezone.utc)
    pod.status.conditions = [
        SimpleNamespace(
            type="PodScheduled", status="True", last_transition_time=scheduled_at
        )
    ]
    pod.status.container_statuses[0].state.waiting.reason = "ContainerCreating"
    assert await db.observe_container_startup(
        **stage._observe_kwargs(job_id, first, pod.metadata.uid),
        observation=ScheduledAt(scheduled_at),
        budgets=StageBudgets(120, 300, 30),
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET created_at=clock_timestamp()-interval '1800 seconds', "
            "expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
            first["id"],
        )
    assert await provisioner.continue_job_workspace_creation(
        str(job_id), str(first["id"]), int(first["claim_token"]), pod.metadata.uid
    )
    rotated = await stage._reservation(db, first)
    assert rotated["claim_token"] != first["claim_token"]
    assert rotated["attempts"] == first["attempts"] + 1
    assert rotated["scheduled_at"] == scheduled_at
    assert (
        rotated["ready_budget_seconds"],
        rotated["pull_budget_seconds"],
        rotated["ssh_budget_seconds"],
    ) == (120, 300, 30)
    assert rotated["settled_at"] is None
    assert len(cluster.objects) == 3
