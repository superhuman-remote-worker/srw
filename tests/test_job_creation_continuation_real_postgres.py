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
from tests.test_started_job_creation_cancel_real_postgres import (
    _exit_before_first_ready,
)


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("exit_code", "reason", "phase"),
    [(3, "Error", "Failed"), (0, "Completed", "Succeeded")],
    ids=["exit-3", "exit-0"],
)
async def test_exited_job_pod_reaches_container_exited_attention_in_one_pass(
    db, monkeypatch, exit_code, reason, phase
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
    scheduled_at = _exit_before_first_ready(
        pod, exit_code=exit_code, reason=reason, phase=phase
    )
    receipt = await provisioner.get_current_workspace_creation_result(
        owner, operation_kind="create"
    )
    gate = (
        str(job_id),
        str(receipt["id"]),
        int(receipt["claim_token"]),
        pod.metadata.uid,
    )

    assert await provisioner.continue_job_workspace_creation(*gate)

    row = await stage._reservation(db, receipt)
    assert (
        row["startup_stage"],
        row["startup_state"],
        row["startup_reason_code"],
        row["startup_exit_code"],
    ) == ("readiness", "attention", "container_exited", exit_code)
    assert row["scheduled_at"] == scheduled_at
    assert row["startup_first_ready_at"] is None
    # Within the tick, long before the frozen readiness boundary.
    assert row["startup_attention_at"] < scheduled_at + timedelta(
        seconds=row["ready_budget_seconds"]
    )
    assert (row["phase"], row["settled_at"]) == ("runtime_bound", None)
    assert (await fixtures._workspace(db, job_id))["status"] == "creating"
    assert str((await db.get_job(str(job_id)))["status"]) == "created"
    # Sticky: the continuation scan drops it, and a later pass changes nothing.
    page = await db.list_current_job_creation_candidates(limit=32)
    assert all(candidate["job_id"] != job_id for candidate in page["candidates"])
    assert not await provisioner.continue_job_workspace_creation(*gate)
    assert (await stage._reservation(db, receipt))["startup_exit_code"] == exit_code
    assert set(cluster.objects) == {"pvc", "service", "pod"}


def _make_job_pod_ready(monkeypatch, provisioner, pod, receipt, *, ready_at):
    """Turn the fake Pod Ready and stub the SSH attestation around it."""

    pod.spec.node_name = "node-1"
    pod.status.conditions = [
        SimpleNamespace(
            type="PodScheduled", status="True", last_transition_time=ready_at
        ),
        SimpleNamespace(type="Ready", status="True", last_transition_time=ready_at),
    ]
    pod.status.phase = "Running"
    pod.status.pod_ip = "10.42.0.51"
    for container in pod.status.container_statuses:
        container.ready = True
        container.state = SimpleNamespace(waiting=None, running=SimpleNamespace())
    provisioner._trusted_pod_ssh_identity = AsyncMock(
        return_value=(
            f"k8s-pvc:agent-workspaces:{receipt['pvc_uid']}",
            "SHA256:" + "B" * 43,
            pod.metadata.uid,
        )
    )
    monkeypatch.setattr(
        provider_module, "wait_for_agent_ssh", AsyncMock(return_value=(True, 1, ""))
    )
    monkeypatch.setattr(
        provider_module, "resolve_ssh_key_path", lambda: "/run/test-key"
    )
    monkeypatch.setattr(
        provider_module, "workspace_private_key_fingerprint", lambda _: "SHA256:test"
    )


async def _resumed_paused_stateless_job(db):
    """A stateless review Job resumed through the real queue-holding writer.

    ``prepare_stateless_job_for_workspace_resume`` closes the worker queue row
    until the replacement workspace is Ready, so no worker claim moves the Job
    to ``processing``: it stays ``paused`` for the whole creation.
    """

    job_id = await fixtures._job(db)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET execution_lane='stateless', status='pending_review' "
            "WHERE id=$1",
            job_id,
        )
    assert await db.prepare_stateless_job_for_workspace_resume(
        str(job_id),
        "workspace_container",
        {"queued_feedback": "Remove the two tracked .pyc files."},
        expected_status="pending_review",
    )
    job = await db.get_job(str(job_id))
    assert str(job["status"]) == "paused"
    return job_id


@pytest.mark.asyncio
@pytest.mark.parametrize("lease_expired", [False, True], ids=["live", "expired"])
async def test_paused_resume_creation_publishes_ready_and_is_admitted(
    db, monkeypatch, lease_expired
):
    """Kai's cca716a8: every gate on the observe-and-publish path accepts paused.

    Discovery, the continuation and the Ready writer each refused ``paused``,
    so a review Resume's new Pod was never published Ready and the Job never
    ran. This checks the whole path, not only the listing.
    """

    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    job_id = await _resumed_paused_stateless_job(db)
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
    receipt = await provisioner.get_current_workspace_creation_result(
        owner, operation_kind="create"
    )
    async with db.acquire() as conn:
        # The reaper's counter from the retired runtime (5 on dev).
        await conn.execute(
            "UPDATE jobs SET context=jsonb_set(context, "
            "'{workspace_container,snapshot_attempts}', '5'::jsonb) WHERE id=$1",
            job_id,
        )
        if lease_expired:
            await conn.execute(
                "UPDATE managed_repository_workspace_creation_reservations "
                "SET created_at=clock_timestamp()-interval '1800 seconds', "
                "expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
                receipt["id"],
            )
    assert not (
        await db.admit_stateless_worker_job(str(job_id), fair_key=None, priority=0)
    )[0]
    _make_job_pod_ready(
        monkeypatch, provisioner, pod, receipt, ready_at=datetime.now(timezone.utc)
    )

    page = await db.list_current_job_creation_candidates(limit=32)
    assert [c["job_id"] for c in page["candidates"]].count(job_id) == 1
    assert await provisioner.continue_job_workspace_creation(
        str(job_id), str(receipt["id"]), int(receipt["claim_token"]), pod.metadata.uid
    )

    closed = await stage._reservation(db, receipt)
    assert closed["settled_at"] is not None
    assert closed["startup_first_ready_at"] is not None
    assert closed["pod_uid"] == receipt["pod_uid"]
    if lease_expired:
        assert closed["claim_token"] != receipt["claim_token"]
    workspace = await fixtures._workspace(db, job_id)
    assert workspace["status"] == "ready"
    assert workspace["_runtime_incarnation"] == pod.metadata.uid
    assert workspace["snapshot_attempts"] == 0
    job = await db.get_job(str(job_id))
    assert str(job["status"]) == "paused"
    context = job["context"]
    if isinstance(context, str):
        context = json.loads(context)
    assert context["queued_feedback"] == "Remove the two tracked .pyc files."
    assert set(cluster.objects) == {"pvc", "service", "pod"}
    assert (
        await db.admit_stateless_worker_job(str(job_id), fair_key=None, priority=0)
    )[0]


@pytest.mark.asyncio
async def test_operator_held_paused_job_publishes_ready_but_is_not_admitted(
    db, monkeypatch
):
    """An operator hold placed mid-creation: Ready may settle, admission refuses."""

    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    job_id = await _resumed_paused_stateless_job(db)
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
    receipt = await provisioner.get_current_workspace_creation_result(
        owner, operation_kind="create"
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=context || "
            "jsonb_build_object('_operator_pause_hold', "
            "jsonb_build_object('token', $2::text)) WHERE id=$1",
            job_id,
            uuid4().hex,
        )
    _make_job_pod_ready(
        monkeypatch, provisioner, pod, receipt, ready_at=datetime.now(timezone.utc)
    )

    assert await provisioner.continue_job_workspace_creation(
        str(job_id), str(receipt["id"]), int(receipt["claim_token"]), pod.metadata.uid
    )
    assert (await stage._reservation(db, receipt))["settled_at"] is not None
    assert (await fixtures._workspace(db, job_id))["status"] == "ready"
    assert not (
        await db.admit_stateless_worker_job(str(job_id), fair_key=None, priority=0)
    )[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal", ["cancelled", "failed", "completed"])
async def test_terminal_owner_creation_is_neither_listed_nor_published(
    db, monkeypatch, terminal
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
    receipt = await provisioner.get_current_workspace_creation_result(
        owner, operation_kind="create"
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status=$2::text WHERE id=$1", job_id, terminal
        )
    _make_job_pod_ready(
        monkeypatch, provisioner, pod, receipt, ready_at=datetime.now(timezone.utc)
    )

    page = await db.list_current_job_creation_candidates(limit=32)
    assert all(c["job_id"] != job_id for c in page["candidates"])
    assert not await provisioner.continue_job_workspace_creation(
        str(job_id), str(receipt["id"]), int(receipt["claim_token"]), pod.metadata.uid
    )
    assert (await stage._reservation(db, receipt))["settled_at"] is None
    # A terminal transition may start the runtime's retirement; never Ready.
    assert (await fixtures._workspace(db, job_id))["status"] != "ready"


@pytest.mark.asyncio
async def test_pinned_paused_resume_creation_publishes_ready(db, monkeypatch):
    """The pinned lane stalls the same way: its dispatcher claims only after Ready."""

    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    job_id = await fixtures._job(db)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET execution_lane='pinned', status='pending_review' "
            "WHERE id=$1",
            job_id,
        )
    assert await db.queue_job_for_resume(
        str(job_id),
        {"queued_feedback": "Remove the two tracked .pyc files."},
        expected_status="pending_review",
    )
    assert str((await db.get_job(str(job_id)))["status"]) == "paused"
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
    receipt = await provisioner.get_current_workspace_creation_result(
        owner, operation_kind="create"
    )
    _make_job_pod_ready(
        monkeypatch, provisioner, pod, receipt, ready_at=datetime.now(timezone.utc)
    )

    assert await provisioner.continue_job_workspace_creation(
        str(job_id), str(receipt["id"]), int(receipt["claim_token"]), pod.metadata.uid
    )
    assert (await stage._reservation(db, receipt))["settled_at"] is not None
    assert (await fixtures._workspace(db, job_id))["status"] == "ready"
    assert str((await db.get_job(str(job_id)))["status"]) == "paused"


@pytest.mark.asyncio
async def test_unobserved_receipt_listing_names_expired_open_creations(db, monkeypatch):
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    job_id = await fixtures._job(db)
    cluster = fixtures.NeverPullingCluster()
    provisioner = fixtures._provisioner(monkeypatch, db, cluster)
    owner = WorkspaceOwner.job(str(job_id))
    assert await provisioner.create_workspace(owner)
    receipt = await provisioner.get_current_workspace_creation_result(
        owner, operation_kind="create"
    )
    listed = await db.list_unobserved_job_creation_receipts(limit=64)
    assert all(r["reservation_id"] != receipt["id"] for r in listed)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET created_at=clock_timestamp()-interval '3600 seconds', "
            "expires_at=clock_timestamp()-interval '1800 seconds' WHERE id=$1",
            receipt["id"],
        )
    listed = await db.list_unobserved_job_creation_receipts(limit=64)
    [row] = [r for r in listed if r["reservation_id"] == receipt["id"]]
    assert row["job_id"] == job_id
    assert row["job_status"] == "created"
    assert row["startup_first_ready_at"] is None
