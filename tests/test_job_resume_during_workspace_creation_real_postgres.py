"""A Resume that finds the replacement Pod of an earlier Resume still starting.

Dev Job cca716a8 (2026-11-07): a review Resume holds the worker queue and
starts a new Pod on the kept PVC. A second Resume during that creation shed
the live projection, the authority trigger refused it, and the route answered
500 with the trigger text. Owner decisions 2026-11-08:

- D-A: keep the creation and append the new feedback (identical repeats are
  idempotent);
- D-B: a creation whose startup window ran out (``ssh_deadline`` and the
  other deadlines) is retired, exact Pod only and PVC kept, and the
  dispatcher creates a new one; a deterministic reason answers 409.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from orchestrator.database.container_startup_stage import (
    ReadyObservedAt,
    ScheduledAt,
    StageBudgets,
    StartupAttention,
)
from orchestrator.services.job_controls import JobControlOperations
from orchestrator.services.workspace_lifecycle import (
    EnsureOutcome,
    WorkspaceOwner,
    ensure_workspace,
)
from tests import test_container_startup_stage_real_postgres as stage
from tests import test_workspace_pull_failure_real_postgres as fixtures
from tests.test_job_control_operations import _operations
from tests.test_job_creation_continuation_real_postgres import _make_job_pod_ready

db = fixtures.db
pg_dsn = fixtures.pg_dsn
_schema_applied = fixtures._schema_applied

FIRST = "Remove the two tracked .pyc files."
SECOND = "Also add *.pyc to .gitignore."


def _context(job):
    context = job.get("context") or {}
    return json.loads(context) if isinstance(context, str) else context


async def _job_row(db, job_id):
    return dict(await db.get_job(str(job_id)))


async def _queue_state(db, job_id):
    return await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", job_id)


async def _paused_with_creation_in_flight(db, monkeypatch):
    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
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
        {"queued_feedback": FIRST, "queued_feedback_reason": "review"},
        expected_status="pending_review",
    )
    cluster = fixtures.NeverPullingCluster()
    provider = fixtures._provisioner(monkeypatch, db, cluster)
    owner = WorkspaceOwner.job(str(job_id))
    assert (
        await ensure_workspace(
            owner,
            provisioner=provider,
            suspension=SimpleNamespace(),
            current_status=None,
        )
    ).outcome is EnsureOutcome.PENDING
    receipt = await provider.get_current_workspace_creation_result(
        owner, operation_kind="create"
    )
    assert receipt["phase"] == "runtime_bound"
    return job_id, owner, cluster, provider, receipt


def _ops(tmp_path, db, provider):
    base = _operations(tmp_path, store=db)
    return JobControlOperations(
        dataclasses.replace(
            base.dependencies,
            container_provisioner=provider,
            get_container_context=lambda job: _context(job).get("workspace_container")
            or {},
        )
    )


async def _resume(ops, db, job_id, feedback):
    return await ops._resume_during_workspace_creation(
        str(job_id),
        job=await _job_row(db, job_id),
        feedback=feedback,
        feedback_reason="An operator explicitly resumed this job.",
        lift_operator_pause_hold="",
    )


@pytest.mark.asyncio
async def test_second_resume_keeps_the_creation_and_appends_feedback(
    db, monkeypatch, tmp_path
):
    job_id, _owner, cluster, provider, receipt = await _paused_with_creation_in_flight(
        db, monkeypatch
    )
    ops = _ops(tmp_path, db, provider)
    before = _context(await _job_row(db, job_id))
    reservation_before = await stage._reservation(db, receipt)
    queue_before = await _queue_state(db, job_id)

    answer = await _resume(ops, db, job_id, SECOND)

    assert answer["status"] == "queued"
    assert "added" in answer["message"]
    after = _context(await _job_row(db, job_id))
    assert after["queued_feedback"] == f"{FIRST}\n\n---\n\n{SECOND}"
    assert after["queued_feedback_reason"] == before["queued_feedback_reason"]
    assert after["queued_feedback_delivery_id"] == before["queued_feedback_delivery_id"]
    assert after["workspace_container"] == before["workspace_container"]
    assert after["worker_resume_id"] == before["worker_resume_id"]
    assert await stage._reservation(db, receipt) == reservation_before
    assert await _queue_state(db, job_id) == queue_before
    assert str((await _job_row(db, job_id))["status"]) == "paused"
    assert set(cluster.objects) == {"pvc", "service", "pod"}
    ops.dependencies.trigger_dispatch.assert_called()

    # Identical repeats (a double-click, or no new feedback) write nothing.
    repeat = await _resume(ops, db, job_id, SECOND)
    assert repeat["status"] == "queued"
    bare = await _resume(ops, db, job_id, None)
    assert bare["status"] == "queued"
    assert _context(await _job_row(db, job_id)) == after


@pytest.mark.asyncio
async def test_the_shed_refuses_an_in_flight_creation_instead_of_tripping_23514(
    db, monkeypatch
):
    """Backstop below the route: every shed path loses its CAS cleanly."""

    (
        job_id,
        _owner,
        _cluster,
        _provider,
        _receipt,
    ) = await _paused_with_creation_in_flight(db, monkeypatch)
    before = _context(await _job_row(db, job_id))

    assert not await db.prepare_stateless_job_for_workspace_resume(
        str(job_id),
        "workspace_container",
        {"queued_feedback": SECOND},
        expected_status="paused",
    )
    assert not await db.shed_workspace_context(str(job_id), "workspace_container")
    assert _context(await _job_row(db, job_id)) == before


async def _drive_to_attention(db, receipt, reason):
    observe = dict(
        owner_kind="job",
        owner_id=str(receipt["owner_id"]),
        reservation_id=str(receipt["id"]),
        claim_token=int(receipt["claim_token"]),
        pod_uid=str(receipt["pod_uid"]),
    )
    assert await db.observe_container_startup(
        **observe,
        observation=ScheduledAt(datetime.now(timezone.utc)),
        budgets=StageBudgets(ready_seconds=180, pull_seconds=None, ssh_seconds=1),
        adopt_if_unmarked=True,
    )
    if reason == "ssh_deadline":
        assert await db.observe_container_startup(
            **observe, observation=ReadyObservedAt(datetime.now(timezone.utc))
        )
        await asyncio.sleep(1.1)
    assert await db.observe_container_startup(
        **observe, observation=StartupAttention(reason)
    )
    row = await stage._reservation(db, receipt)
    assert (row["startup_state"], row["startup_reason_code"]) == ("attention", reason)


@pytest.mark.asyncio
async def test_resume_retires_a_timed_out_creation_and_the_new_pod_runs_on_the_same_pvc(
    db, monkeypatch, tmp_path
):
    """Kai's Job after the deploy: ssh_deadline attention, one Resume, recovered."""

    job_id, owner, cluster, provider, receipt = await _paused_with_creation_in_flight(
        db, monkeypatch
    )
    await _drive_to_attention(db, receipt, "ssh_deadline")
    pvc = cluster.objects["pvc"]
    pvc_uid = pvc.metadata.uid
    old_pod_uid = cluster.objects["pod"].metadata.uid
    delete_pod = cluster.delete_namespaced_pod

    def terminate_pod(**kwargs):
        delete_pod(**kwargs)
        if "pod" in cluster.objects:
            status = cluster.objects["pod"].status.container_statuses[0]
            status.ready = False
            status.started = False

    monkeypatch.setattr(cluster, "delete_namespaced_pod", terminate_pod)
    ops = _ops(tmp_path, db, provider)

    answer = await _resume(ops, db, job_id, None)

    assert answer["status"] == "queued"
    assert "replaced" in answer["message"]
    assert "pod" not in cluster.objects
    assert cluster.objects["pvc"] is pvc
    assert await fixtures._open_authority(db, job_id) == {
        "reservations": 0,
        "intents": 0,
    }
    job = await _job_row(db, job_id)
    context = _context(job)
    assert str(job["status"]) == "paused"
    assert context["queued_feedback"] == FIRST
    workspace = context["workspace_container"]
    assert workspace["status"] == "deleted"
    assert workspace["_runtime_incarnation"] == old_pod_uid
    closed = await stage._reservation(db, receipt)
    assert closed["settled_at"] is not None
    assert closed["result_kind"] != "settled"

    # The dispatcher recreates on the kept claim, and the continuation
    # publishes the new Pod Ready while the Job is still paused (WP1).
    assert (
        await ensure_workspace(
            owner,
            provisioner=provider,
            suspension=SimpleNamespace(),
            current_status="deleted",
        )
    ).outcome is EnsureOutcome.PENDING
    new_pod = cluster.objects["pod"]
    assert new_pod.metadata.uid != old_pod_uid
    assert cluster.objects["pvc"].metadata.uid == pvc_uid
    second = await provider.get_current_workspace_creation_result(
        owner, operation_kind="create"
    )
    assert str(second["id"]) != str(receipt["id"])
    _make_job_pod_ready(
        monkeypatch, provider, new_pod, second, ready_at=datetime.now(timezone.utc)
    )
    assert await provider.continue_job_workspace_creation(
        str(job_id), str(second["id"]), int(second["claim_token"]), new_pod.metadata.uid
    )
    assert (await fixtures._workspace(db, job_id))["status"] == "ready"
    assert _context(await _job_row(db, job_id))["queued_feedback"] == FIRST
    assert (
        await db.admit_stateless_worker_job(str(job_id), fair_key=None, priority=0)
    )[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["invalid_image", "invalid_configuration"])
async def test_resume_refuses_a_deterministic_start_failure_without_writing(
    db, monkeypatch, tmp_path, reason
):
    job_id, _owner, cluster, provider, receipt = await _paused_with_creation_in_flight(
        db, monkeypatch
    )
    await _drive_to_attention(db, receipt, reason)
    before = _context(await _job_row(db, job_id))
    ops = _ops(tmp_path, db, provider)

    with pytest.raises(HTTPException) as refused:
        await _resume(ops, db, job_id, SECOND)

    assert refused.value.status_code == 409
    assert refused.value.detail["code"] == "workspace_start_failed"
    assert reason in refused.value.detail["message"]
    assert _context(await _job_row(db, job_id)) == before
    assert "pod" in cluster.objects


@pytest.mark.asyncio
async def test_a_newer_operator_hold_makes_the_join_a_conflict(
    db, monkeypatch, tmp_path
):
    (
        job_id,
        _owner,
        _cluster,
        provider,
        _receipt,
    ) = await _paused_with_creation_in_flight(db, monkeypatch)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=context || "
            "jsonb_build_object('_operator_pause_hold', "
            "jsonb_build_object('hold_id', 'newer-hold')) WHERE id=$1",
            job_id,
        )
    before = _context(await _job_row(db, job_id))
    ops = _ops(tmp_path, db, provider)

    with pytest.raises(HTTPException) as refused:
        await _resume(ops, db, job_id, SECOND)

    assert refused.value.status_code == 409
    assert refused.value.detail["code"] == "job_state_conflict"
    assert _context(await _job_row(db, job_id)) == before


@pytest.mark.asyncio
async def test_the_observed_operator_hold_is_lifted_with_the_join(
    db, monkeypatch, tmp_path
):
    (
        job_id,
        _owner,
        _cluster,
        provider,
        _receipt,
    ) = await _paused_with_creation_in_flight(db, monkeypatch)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=context || "
            "jsonb_build_object('_operator_pause_hold', "
            "jsonb_build_object('hold_id', 'seen-hold')) WHERE id=$1",
            job_id,
        )
    ops = _ops(tmp_path, db, provider)

    answer = await ops._resume_during_workspace_creation(
        str(job_id),
        job=await _job_row(db, job_id),
        feedback=SECOND,
        feedback_reason="An operator explicitly resumed this job.",
        lift_operator_pause_hold="seen-hold",
    )

    assert answer["status"] == "queued"
    context = _context(await _job_row(db, job_id))
    assert "_operator_pause_hold" not in context
    assert context["last_operator_pause_hold"]["hold_id"] == "seen-hold"
    assert context["queued_feedback"].endswith(SECOND)


@pytest.mark.asyncio
async def test_kais_exact_shape_released_volume_resumed_timed_out_then_recovered(
    db, monkeypatch, tmp_path
):
    """cca716a8 end to end: a settled first runtime, an idle release keeping
    the PVC, a review Resume whose new Pod missed its SSH window, then one
    Resume that retires it and runs on the same claim."""

    monkeypatch.setenv("CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", "true")
    job_id = await fixtures._job(db)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET execution_lane='stateless' WHERE id=$1", job_id
        )
    cluster = fixtures.NeverPullingCluster()
    provider = fixtures._provisioner(monkeypatch, db, cluster)
    owner = WorkspaceOwner.job(str(job_id))
    delete_pod = cluster.delete_namespaced_pod

    def terminate_pod(**kwargs):
        delete_pod(**kwargs)
        if "pod" in cluster.objects:
            pod = cluster.objects["pod"]
            status = pod.status.container_statuses[0]
            status.ready = False
            status.started = False
            # The kubelet drops Ready and the Pod IP once the process stops.
            pod.status.pod_ip = None
            for condition in getattr(pod.status, "conditions", None) or ():
                if condition.type == "Ready":
                    condition.status = "False"
            # The fake keeps manifest dicts; a real V1Pod names its containers,
            # which the exact process-zero proof for a scheduled Pod compares.
            pod.spec.containers = [
                SimpleNamespace(name=c["name"]) if isinstance(c, dict) else c
                for c in pod.spec.containers
            ]
            stopped = SimpleNamespace(
                waiting=None,
                running=None,
                terminated=SimpleNamespace(exit_code=0, reason="Completed"),
            )
            pod.spec.init_containers = [
                SimpleNamespace(name=c["name"]) if isinstance(c, dict) else c
                for c in pod.spec.init_containers
            ]
            pod.status.init_container_statuses = [
                SimpleNamespace(name=c.name, state=stopped)
                for c in pod.spec.init_containers
            ]

    monkeypatch.setattr(cluster, "delete_namespaced_pod", terminate_pod)

    async def create_and_publish():
        assert (
            await ensure_workspace(
                owner,
                provisioner=provider,
                suspension=SimpleNamespace(),
                current_status=(await fixtures._workspace(db, job_id) or {}).get(
                    "status"
                ),
            )
        ).outcome is EnsureOutcome.PENDING
        pod = cluster.objects["pod"]
        receipt = await provider.get_current_workspace_creation_result(
            owner, operation_kind="create"
        )
        return pod, receipt

    # 1. First runtime settles Ready; the Job freezes for review.
    first_pod, first = await create_and_publish()
    _make_job_pod_ready(
        monkeypatch, provider, first_pod, first, ready_at=datetime.now(timezone.utc)
    )
    assert await provider.continue_job_workspace_creation(
        str(job_id), str(first["id"]), int(first["claim_token"]), first_pod.metadata.uid
    )
    pvc_uid = cluster.objects["pvc"].metadata.uid
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status='pending_review' WHERE id=$1", job_id
        )

    # 2. Idle release: exact Pod retired, PVC kept, projection `deleted`.
    released = await provider.prepare_workspace_cleanup_intent(
        owner,
        expected_runtime_incarnation=first_pod.metadata.uid,
        target_disposition="deleted",
        reclaim_shared_resources=False,
        admission_source="explicit",
    )
    assert (
        await provider.reconcile_workspace_cleanup_intent(
            owner,
            expected_runtime_incarnation=first_pod.metadata.uid,
            intent_generation=int(released["intent_generation"]),
        )
    ).settled
    assert (await fixtures._workspace(db, job_id))["status"] == "deleted"

    # 3. Review Resume: queue held, Job paused, a new Pod on the kept claim.
    assert await db.prepare_stateless_job_for_workspace_resume(
        str(job_id),
        "workspace_container",
        {"queued_feedback": FIRST},
        expected_status="pending_review",
    )
    second_pod, second = await create_and_publish()
    assert cluster.objects["pvc"].metadata.uid == pvc_uid

    # 4. Its SSH window ran out before anything observed it (dev, 11-07).
    await _drive_to_attention(db, second, "ssh_deadline")

    # 5. One Resume, no new feedback (D-C (a)).
    ops = _ops(tmp_path, db, provider)
    answer = await _resume(ops, db, job_id, None)
    assert answer["status"] == "queued"
    assert "pod" not in cluster.objects
    assert cluster.objects["pvc"].metadata.uid == pvc_uid
    assert await fixtures._open_authority(db, job_id) == {
        "reservations": 0,
        "intents": 0,
    }

    # 6. The dispatcher's next creation is observed Ready and admitted.
    third_pod, third = await create_and_publish()
    assert third_pod.metadata.uid not in {
        first_pod.metadata.uid,
        second_pod.metadata.uid,
    }
    assert cluster.objects["pvc"].metadata.uid == pvc_uid
    _make_job_pod_ready(
        monkeypatch, provider, third_pod, third, ready_at=datetime.now(timezone.utc)
    )
    assert await provider.continue_job_workspace_creation(
        str(job_id), str(third["id"]), int(third["claim_token"]), third_pod.metadata.uid
    )
    job = await _job_row(db, job_id)
    assert str(job["status"]) == "paused"
    assert _context(job)["queued_feedback"] == FIRST
    assert _context(job)["workspace_container"]["status"] == "ready"
    assert (
        await db.admit_stateless_worker_job(str(job_id), fair_key=None, priority=0)
    )[0]
