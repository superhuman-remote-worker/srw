"""A workspace-loss report settles the same whichever detector came first.

knowledge-base/knowledge/issues/workspace_report_after_automatic_pod_cleanup_parks_job_in_processing.md:
on a stateless job the lifecycle reconciler and the worker both notice a dead
workspace Pod. When the reconciler's ``unhealthy_delete`` retires the Pod
first (automatic ``preserve`` intent, row ``deleted``), the worker's later
``workspace_unavailable`` report must still pause the job behind a hold for
explicit Resume, keep the volume, and let Resume start a new Pod on it.

Real PostgreSQL with the full migration chain; only Kubernetes is faked. The
retirement runs through the production reconciler tick and workspace manager
with cleanup reconciliation switched on, the report through acceptance, the
completion claim and the recovery arm.
"""

from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock
from uuid import UUID, uuid4

import pytest

from orchestrator.services.completion import handle_pod_workspace_recovery
from orchestrator.services.completion_control import (
    COMPLETION_CONTROL_CLAIM_KEY,
    CompletionControl,
)
from orchestrator.services.completion_finalizer import CompletionFinalizer
from orchestrator.services.completion_runtime import CompletionControlBoundary
from orchestrator.services.job_completion_commands import accept_completion_command
from orchestrator.services.lifecycle.reconciler import InstanceLifecycleReconciler
from orchestrator.services.lifecycle.workspace_manager import WorkspaceInstanceManager
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from shared.operator_pause_hold import operator_pause_lift_token
from shared.worker_queue import claim_worker_batch
from tests import test_container_recovery_retention_real_postgres as retention
from tests.test_completion_finalizer_real_postgres import _claimed_runner
from tests.test_container_recovery_cleanup_real_postgres import terminal_pod_api
from tests.test_job_control_operations import _operations
from tests.test_non_pinned_workspace_lifecycle_real_postgres import (
    _create_settled_authoritative_runtime,
)

pg_dsn = retention.pg_dsn
_schema_applied = retention._schema_applied
db = retention.db

current_job = retention.current_job

REPORT = {
    "type": "workspace_unavailable",
    "recoverable": True,
    "message": "Remote tmux observe failed: timed out",
}
DEAD_ARM_ACTION = (
    "workspace transport lost; interrupted command outcome is unknown; "
    "workspace storage and checkpoints retained; explicit Resume required"
)


def _failed_pod_cluster(db, monkeypatch, owner, runtime):
    """The job's Pod has died (phase Failed); its PVC and Service remain."""

    # The rollout flag is read when the provisioner is built.
    monkeypatch.setenv("WORKSPACE_CLEANUP_RECONCILIATION_ENABLED", "true")
    provisioner, resources = terminal_pod_api(db, owner, runtime)
    assert provisioner._workspace_cleanup_reconciliation_enabled is True
    provisioner._core_api.list_namespaced_pod.side_effect = lambda **_: SimpleNamespace(
        items=[resources["pod"]] if "pod" in resources else []
    )
    return provisioner, resources


async def _drain_queue(db):
    """Earlier tests in this module may leave a resumed job queued."""

    for unit_id in await db.fetch("SELECT unit_id FROM run_queue WHERE state='queued'"):
        cancelled, _ = await db.cancel_stateless_job(
            str(unit_id["unit_id"]), completion_commands_enabled=True
        )
        assert cancelled


async def _stateless_job(db, monkeypatch):
    """A processing stateless job on a settled Ready runtime, queue leased."""

    await _drain_queue(db)
    job_id, runtime, creation, state = await _create_settled_authoritative_runtime(
        db, owner_kind="job", scope="workspace_container", settle=False
    )
    owner = WorkspaceOwner.job(str(job_id))
    state["workspace_container"]["pod_name"] = owner.pod_name
    await db.execute(
        "UPDATE jobs SET context=$2::jsonb, execution_lane='stateless', "
        'config_override=\'{"workspace":{"backend":"sandbox"}}\' WHERE id=$1',
        job_id,
        json.dumps(state),
    )
    assert await db.settle_managed_repository_workspace_creation_reservation(
        str(job_id),
        owner_kind="job",
        scope="workspace_container",
        reservation_generation=creation["reservation_generation"],
        claimant="authority-envelope-creator",
        claim_token=creation["claim_token"],
        runtime_incarnation=runtime,
    )
    admitted, _ = await db.admit_stateless_worker_job(
        str(job_id), fair_key=None, priority=5, completion_commands_enabled=True
    )
    assert admitted
    claim = await claim_worker_batch(
        db,
        pod_name="report-worker",
        affinity_grace_seconds=0,
        completion_commands_enabled=True,
    )
    assert claim is not None and claim.unit_id == job_id

    provisioner, resources = _failed_pod_cluster(db, monkeypatch, owner, runtime)
    job = await current_job(db, job_id)
    assert job["status"] == "processing"
    assert job["context"]["workspace_container"]["status"] == "ready"
    return job, owner, runtime, provisioner, resources


def _reconciler(db, provisioner):
    """The leader's lifecycle reconciler with only the workspace manager."""

    return InstanceLifecycleReconciler(
        [
            WorkspaceInstanceManager(
                provisioner,
                MagicMock(),
                MagicMock(),
                db,
                completion_commands_enabled=True,
                completion_router=AsyncMock(),
            )
        ]
    )


async def _accept_report(db, job_id):
    accepted = await accept_completion_command(
        db._pool,
        job_id=str(job_id),
        payload={
            "should_stop": True,
            "goal_achieved": False,
            "error": REPORT,
            "freeze_data": None,
        },
        lease_token=await db.fetchval(
            "SELECT lease_token FROM run_queue WHERE unit_id=$1", job_id
        ),
        agent_id=None,
        client_report_id=str(uuid4()),
        requested_by="report-after-cleanup-proof",
    )
    return await _claimed_runner(db, accepted.command_id)


async def _intents(db, job_id):
    return await db.fetch(
        "SELECT admission_source, resource_policy, target_disposition, "
        "runtime_incarnation::text AS runtime, result_kind, settled_at "
        "FROM managed_repository_workspace_cleanup_intents "
        "WHERE owner_id=$1 ORDER BY created_at",
        job_id,
    )


def _recover(db, job, runner, provisioner, *, probe, dispatch):
    return handle_pod_workspace_recovery(
        job,
        str(job["id"]),
        REPORT,
        db=db,
        delete_workspace=AsyncMock(side_effect=AssertionError("untyped deletion")),
        cleanup_service=provisioner,
        trigger_dispatch=dispatch,
        probe=probe,
        completion_command_id=runner.command_id,
        completion_finalizing_by=runner.owner,
    )


def _job_controls(db, tmp_path, provider):
    """The public job controls with completion commands on."""

    operations = _operations(tmp_path, store=db, completion_commands_enabled=True)
    runtime_boundary = SimpleNamespace(
        dependencies=SimpleNamespace(
            commands_enabled=lambda: True, logger=operations.dependencies.logger
        ),
        control=lambda: CompletionControl(db, AsyncMock()),
    )
    return replace(
        operations,
        dependencies=replace(
            operations.dependencies,
            completion_control=CompletionControlBoundary(runtime_boundary),
            get_container_context=lambda job: job["context"]["workspace_container"],
            validate_workspace_recovery_storage=provider.validate_workspace_recovery_storage,
            resume_missing_workspace=lambda job: "container",
        ),
    )


async def _resume_on_a_new_pod(db, monkeypatch, tmp_path, owner, held, pvc):
    """Public Resume, then the dispatcher's creator, on a cluster that works."""

    from tests import test_workspace_pull_failure_real_postgres as creator_cases
    from tests.test_active_workspace_creator_cancel_real_postgres import _make_ready

    class ReadyCluster(creator_cases.NeverPullingCluster):
        def __init__(self):
            super().__init__()
            self.create_calls = []

        def create_namespaced_persistent_volume_claim(self, **kwargs):
            self.create_calls.append("pvc")
            return super().create_namespaced_persistent_volume_claim(**kwargs)

        def create_namespaced_pod(self, **kwargs):
            self.create_calls.append("pod")
            result = super().create_namespaced_pod(**kwargs)
            _make_ready(self, monkeypatch)
            return result

        def create_namespaced_service(self, **kwargs):
            self.create_calls.append("service")
            return super().create_namespaced_service(**kwargs)

    cluster = ReadyCluster()
    cluster.objects["pvc"] = pvc
    provider = creator_cases._provisioner(monkeypatch, db, cluster)
    operations = _job_controls(db, tmp_path, provider)
    await operations.resume_job_internal(owner.id, user={"id": str(uuid4())}, job=held)
    queued = await current_job(db, held["id"])
    assert "_operator_pause_hold" not in queued["context"]
    assert await provider.create_workspace(owner)
    return await current_job(db, held["id"]), cluster


@pytest.mark.asyncio
async def test_t1_report_after_automatic_cleanup_holds_and_resumes_on_the_volume(
    db, monkeypatch, tmp_path
):
    """T1: reconciler first, then the report. The order is forced."""

    original, owner, runtime, provisioner, resources = await _stateless_job(
        db, monkeypatch
    )
    pvc = resources["pvc"]
    pvc_uid = pvc.metadata.uid

    # 1. The Pod dies (OOMKilled, phase Failed) and the reconciler's tick
    #    retires it before the worker reports: an automatic preserve intent,
    #    Pod and Service gone, PVC kept, row ``deleted``, claim cleared.
    stats = await _reconciler(db, provisioner).tick()
    assert stats["workspace"]["unhealthy"] == 1
    assert "pod" not in resources and "service" not in resources
    assert resources["pvc"] is pvc
    provisioner._core_api.delete_namespaced_persistent_volume_claim.assert_not_called()
    (automatic,) = await _intents(db, original["id"])
    assert automatic["admission_source"] == "automatic"
    assert automatic["resource_policy"] == "preserve"
    assert automatic["target_disposition"] == "deleted"
    assert automatic["runtime"] == runtime
    assert automatic["result_kind"] == "settled"
    retired = await current_job(db, original["id"])
    assert retired["status"] == "processing"
    assert COMPLETION_CONTROL_CLAIM_KEY not in retired["context"]
    workspace = retired["context"]["workspace_container"]
    assert workspace["status"] == "deleted"
    assert workspace["_runtime_incarnation"] == runtime

    # 2. The worker's report arrives and is accepted (the queue term is still
    #    leased at its token; acceptance closes the row to ``done``).
    runner = await _accept_report(db, original["id"])
    assert (
        await db.fetchval(
            "SELECT state FROM run_queue WHERE unit_id=$1", original["id"]
        )
        == "done"
    )

    # 3. The recovery arm settles it into the hold. The retired runtime is
    #    never probed and nothing is deleted again.
    probe = AsyncMock(return_value=False)
    dispatch = Mock()
    outcome = await _recover(
        db, retired, runner, provisioner, probe=probe, dispatch=dispatch
    )

    assert outcome["held_for_resume"] is True
    assert outcome["paused"] is True
    assert outcome["cleanup_pending"] is False
    assert outcome["actions"] == [DEAD_ARM_ACTION]
    probe.assert_not_awaited()
    dispatch.assert_not_called()
    await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, outcome)
    assert (
        await db.fetchval(
            "SELECT state FROM job_completion_commands WHERE id=$1",
            UUID(runner.command_id),
        )
        == "done"
    )

    held = await current_job(db, original["id"])
    assert held["status"] == "paused"
    assert held["assigned_agent_id"] is None
    assert held["freeze_data"] is None
    assert held["error_message"] == DEAD_ARM_ACTION
    hold = held["context"]["_operator_pause_hold"]
    assert hold["source"] == "workspace_recovery_unavailable"
    assert hold["attention"]["reason"] == "interrupted_command_outcome_unknown"
    held_workspace = held["context"]["workspace_container"]
    assert "recovery_cleanup" not in held_workspace
    assert held_workspace["recovery_attempts"] == 1
    assert held_workspace["recovery_completion_command_id"] == runner.command_id
    assert {
        key: value
        for key, value in held_workspace.items()
        if not key.startswith("recovery_")
    } == {
        key: value
        for key, value in workspace.items()
        if not key.startswith("recovery_")
    }
    assert "last_workspace_container" not in held["context"]
    # One retirement only: the arm admitted no cleanup of its own.
    assert len(await _intents(db, original["id"])) == 1

    # 4. Resume lifts the hold; the dispatcher's creator starts a new Pod on
    #    the same claim.
    assert operator_pause_lift_token(held)
    successor, cluster = await _resume_on_a_new_pod(
        db, monkeypatch, tmp_path, owner, held, pvc
    )
    new_workspace = successor["context"]["workspace_container"]
    assert new_workspace["status"] == "ready"
    assert new_workspace["_runtime_incarnation"] != runtime
    assert new_workspace["_runtime_incarnation"] == cluster.objects["pod"].metadata.uid
    assert cluster.objects["pvc"] is pvc
    assert cluster.objects["pvc"].metadata.uid == pvc_uid
    # Without a recovery receipt the creator meets AlreadyExists on the claim
    # and reattaches the kept one by name (F4 in the note), as after an idle
    # Pod release.
    assert cluster.create_calls == ["pvc", "pod", "service"]


@pytest.mark.asyncio
async def test_t2_report_before_cleanup_still_holds_through_the_dead_arm(
    db, monkeypatch
):
    """T2: the order that already works. The arm retires the Pod itself."""

    original, owner, runtime, provisioner, resources = await _stateless_job(
        db, monkeypatch
    )
    pvc = resources["pvc"]

    runner = await _accept_report(db, original["id"])
    probe = AsyncMock(return_value=False)
    dispatch = Mock()
    outcome = await _recover(
        db, original, runner, provisioner, probe=probe, dispatch=dispatch
    )

    assert outcome["held_for_resume"] is True
    assert outcome["cleanup_pending"] is False
    assert outcome["actions"] == [DEAD_ARM_ACTION]
    probe.assert_awaited_once()
    dispatch.assert_not_called()
    await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, outcome)
    held = await current_job(db, original["id"])
    assert held["status"] == "paused"
    assert held["context"]["_operator_pause_hold"]["source"] == (
        "workspace_recovery_unavailable"
    )
    assert held["context"]["workspace_container"]["status"] == "deleted"
    assert held["context"]["workspace_container"]["recovery_attempts"] == 1
    assert resources["pvc"] is pvc and "pod" not in resources
    (explicit,) = await _intents(db, original["id"])
    assert explicit["admission_source"] == "explicit"
    assert explicit["resource_policy"] == "preserve"

    # The reconciler's next tick finds no Pod and changes nothing.
    stats = await _reconciler(db, provisioner).tick()
    assert stats["workspace"]["listed"] == 0
    assert await current_job(db, original["id"]) == held


async def _retired_report(db, monkeypatch, *, retire=True):
    """A stateless job whose report was accepted, optionally after retirement."""

    original, owner, runtime, provisioner, resources = await _stateless_job(
        db, monkeypatch
    )
    if retire:
        stats = await _reconciler(db, provisioner).tick()
        assert stats["workspace"]["unhealthy"] == 1
    runner = await _accept_report(db, original["id"])
    job = await current_job(db, original["id"])
    return job, owner, runtime, provisioner, runner


async def _hold_retired(db, job, runner):
    return await db.hold_retired_workspace_recovery(
        str(job["id"]),
        expected_workspace=job["context"]["workspace_container"],
        expected_agent_id=job["assigned_agent_id"],
        error_detail=REPORT["message"],
        completion_command_id=runner.command_id if runner else None,
        completion_finalizing_by=runner.owner if runner else None,
    )


async def _observed(db, job_id):
    job = await current_job(db, job_id)
    return (
        job,
        [dict(row) for row in await _intents(db, job_id)],
        await db.fetchrow(
            "SELECT state, lease_token FROM run_queue WHERE unit_id=$1", job_id
        ),
    )


async def _raw(db, query, *args):
    """A write the production triggers would refuse: a hand-made bad state."""

    from tests.test_non_pinned_workspace_lifecycle_real_postgres import (
        _execute_pre_0195,
    )

    async with db.acquire() as conn:
        await _execute_pre_0195(conn, query, *args)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fence",
    [
        "ready",
        "retiring",
        "deleted_without_settled_intent",
        "other_runtime",
        "moved_queue_token",
        "open_reservation",
        "existing_hold",
        "active_claim",
        "terminal_reclaim",
    ],
)
async def test_t6_retired_hold_refuses_without_its_proof(db, monkeypatch, fence):
    """T6: every fence of the retired-runtime mode refuses and writes nothing."""

    job, owner, runtime, provisioner, runner = await _retired_report(
        db,
        monkeypatch,
        retire=fence not in {"ready", "retiring", "deleted_without_settled_intent"},
    )
    job_id = job["id"]
    if fence == "retiring":
        # Admitted, not settled: no proof yet that the Pod is gone.
        intent = await provisioner.prepare_workspace_cleanup_intent(
            owner,
            expected_runtime_incarnation=runtime,
            target_disposition="deleted",
            reclaim_shared_resources=False,
            admission_source="automatic",
        )
        assert isinstance(intent, dict) and intent["settled_at"] is None
    elif fence == "deleted_without_settled_intent":
        await _raw(
            db,
            "UPDATE jobs SET context=jsonb_set(context,"
            "'{workspace_container,status}','\"deleted\"') WHERE id=$1",
            job_id,
        )
    elif fence == "moved_queue_token":
        await _raw(
            db,
            "UPDATE run_queue SET lease_token=lease_token+1 WHERE unit_id=$1",
            job_id,
        )
    elif fence == "open_reservation":
        assert await db.reserve_managed_repository_workspace_creation(
            str(job_id),
            owner_kind="job",
            scope="workspace_container",
            claimant="successor-creator",
            desired_manifest_digest="0" * 64,
        )
    elif fence == "existing_hold":
        await db.execute(
            "UPDATE jobs SET context=context||jsonb_build_object("
            "'_operator_pause_hold',jsonb_build_object('hold_id','earlier',"
            "'source','public_pause')) WHERE id=$1",
            job_id,
        )
    elif fence == "active_claim":
        await db.execute(
            "UPDATE jobs SET context=context||jsonb_build_object("
            "'_completion_control_claim',jsonb_build_object('version',1,"
            "'expires_epoch',extract(epoch FROM now())+3600)) WHERE id=$1",
            job_id,
        )
    elif fence == "terminal_reclaim":
        await _raw(
            db,
            "UPDATE managed_repository_workspace_cleanup_intents SET "
            "resource_policy='terminal_reclaim', reclaim_shared_resources=TRUE "
            "WHERE owner_id=$1",
            job_id,
        )
    expected = await current_job(db, job_id)
    if fence == "other_runtime":
        expected["context"]["workspace_container"]["_runtime_incarnation"] = str(
            uuid4()
        )
    before = await _observed(db, job_id)

    assert await _hold_retired(db, expected, runner) is None
    assert await _observed(db, job_id) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("after", ["resume", "cancel"])
async def test_t7_lost_reply_replays_the_stored_hold_after_owner_action(
    db, monkeypatch, after
):
    """T7: a replayed report returns its stored outcome and restamps nothing."""

    job, owner, runtime, provisioner, runner = await _retired_report(db, monkeypatch)
    first = await _hold_retired(db, job, runner)
    assert first["held_for_resume"] is True
    await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, first)
    held = await current_job(db, job["id"])
    if after == "resume":
        assert await db.queue_stateless_job_for_resume(
            str(job["id"]),
            expected_status="paused",
            lift_operator_pause_hold=operator_pause_lift_token(held),
            completion_commands_enabled=True,
        )
    else:
        cancelled, _ = await db.cancel_stateless_job(
            str(job["id"]), completion_commands_enabled=True
        )
        assert cancelled
    before = await current_job(db, job["id"])
    assert before["status"] == ("paused" if after == "resume" else "cancelled")

    # The original processing snapshot models a reply lost before the effect
    # journal recorded completion: the store and the arm both replay.
    assert await _hold_retired(db, job, runner) == first
    probe = AsyncMock(side_effect=AssertionError("a replay is not probed"))
    dispatch = Mock()
    assert (
        await _recover(db, job, runner, provisioner, probe=probe, dispatch=dispatch)
        == first
    )
    dispatch.assert_not_called()
    assert await current_job(db, job["id"]) == before
    assert before["context"]["workspace_container"]["recovery_attempts"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("durable", [True, False])
async def test_pinned_lane_report_after_automatic_cleanup_holds(
    db, monkeypatch, durable
):
    """The pinned lane: an accepted (durable) or a legacy report."""

    from tests.test_container_recovery_cleanup_real_postgres import cleanup_job

    original, owner, runtime = await cleanup_job(db)
    provisioner, resources = _failed_pod_cluster(db, monkeypatch, owner, runtime)
    stats = await _reconciler(db, provisioner).tick()
    assert stats["workspace"]["unhealthy"] == 1
    retired = await current_job(db, original["id"])
    assert retired["status"] == "processing"
    assert retired["context"]["workspace_container"]["status"] == "deleted"
    (automatic,) = await _intents(db, original["id"])
    assert automatic["admission_source"] == "automatic"

    runner = await retention.accepted_recovery(db, retired) if durable else None
    probe = AsyncMock(side_effect=AssertionError("a retired runtime is not probed"))
    outcome = await handle_pod_workspace_recovery(
        retired,
        str(retired["id"]),
        REPORT,
        db=db,
        delete_workspace=AsyncMock(side_effect=AssertionError("untyped deletion")),
        cleanup_service=provisioner,
        trigger_dispatch=Mock(),
        probe=probe,
        completion_command_id=runner.command_id if runner else None,
        completion_finalizing_by=runner.owner if runner else None,
    )
    assert outcome["held_for_resume"] is True
    assert outcome["actions"] == [DEAD_ARM_ACTION]
    if runner:
        await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, outcome)
    held = await current_job(db, original["id"])
    assert held["status"] == "paused"
    assert held["assigned_agent_id"] is None
    assert held["context"]["_operator_pause_hold"]["source"] == (
        "workspace_recovery_unavailable"
    )
    assert held["context"]["workspace_container"]["recovery_attempts"] == 1
    assert len(await _intents(db, original["id"])) == 1
    assert "pvc" in resources


OLD_ARM_ERROR = "workspace recovery lost its cleanup disposition term"


async def _old_arm(runner):
    """The recovery effect as it failed before the fix (T1's RuntimeError)."""

    async def effect():
        raise RuntimeError(OLD_ARM_ERROR)

    return await runner.run(
        name="pod_workspace_recovery", group="recovery", callback=effect
    )


def _fixed_arm(db, job_id, provisioner):
    """The same effect, now running the real recovery arm."""

    async def workflow(runner):
        job = await current_job(db, job_id)

        async def effect():
            return await handle_pod_workspace_recovery(
                job,
                str(job_id),
                REPORT,
                db=db,
                delete_workspace=AsyncMock(side_effect=AssertionError("no deletion")),
                cleanup_service=provisioner,
                trigger_dispatch=Mock(),
                probe=AsyncMock(side_effect=AssertionError("no probe")),
                completion_command_id=runner.command_id,
                completion_finalizing_by=runner.owner,
            )

        return await runner.run(
            name="pod_workspace_recovery", group="recovery", callback=effect
        )

    return workflow


async def _command(db, command_id):
    return await db.fetchrow(
        "SELECT state, attempts, error_code, deadline_at < now() AS past_deadline, "
        "outcome FROM job_completion_commands WHERE id=$1",
        UUID(command_id),
    )


async def _effect(db, command_id):
    return await db.fetchrow(
        "SELECT state, attempts, error_code FROM completion_effects "
        "WHERE producer_kind='job_completion' AND producer_id=$1 "
        "AND effect_name='pod_workspace_recovery'",
        UUID(command_id),
    )


async def _parked_like_3677b309(db, monkeypatch):
    """The stuck job's shape: retired by the reconciler, report parked 5/5.

    The seed runs the real finalizer five times over the effect as it failed
    before the fix, then lets the command's deadline pass, as on dev.
    """

    job, owner, runtime, provisioner, resources = await _stateless_job(db, monkeypatch)
    assert (await _reconciler(db, provisioner).tick())["workspace"]["unhealthy"] == 1
    accepted = await accept_completion_command(
        db._pool,
        job_id=str(job["id"]),
        payload={
            "should_stop": True,
            "goal_achieved": False,
            "error": REPORT,
            "freeze_data": None,
        },
        lease_token=await db.fetchval(
            "SELECT lease_token FROM run_queue WHERE unit_id=$1", job["id"]
        ),
        agent_id=None,
        client_report_id=str(uuid4()),
        requested_by="parked-shape-proof",
    )
    command_id = accepted.command_id
    finalizer = CompletionFinalizer(db, workflow=_old_arm, random_source=lambda: 1.0)
    for attempt in range(1, 6):
        result = await finalizer.finalize_command(command_id, inline=attempt == 1)
        if attempt < 5:
            assert result.disposition == "retry"
            await db.execute(
                "UPDATE job_completion_commands SET run_after=now()-interval '1 second' "
                "WHERE id=$1",
                UUID(command_id),
            )
            await db.execute(
                "UPDATE completion_effects SET complete_by=now()-interval '1 second' "
                "WHERE producer_kind='job_completion' AND producer_id=$1",
                UUID(command_id),
            )
    assert result.disposition == "parked"
    # Age the seed: on dev the command had been parked for days, so its
    # deadline and the last effect's complete_by window had long passed.
    # (Within that 90 s window unpark answers 409 effect_owner_live.)
    await db.execute(
        "UPDATE job_completion_commands SET deadline_at=now()-interval '1 hour' "
        "WHERE id=$1",
        UUID(command_id),
    )
    await db.execute(
        "UPDATE completion_effects SET complete_by=now()-interval '1 hour' "
        "WHERE producer_kind='job_completion' AND producer_id=$1",
        UUID(command_id),
    )

    command = await _command(db, command_id)
    assert (command["state"], command["attempts"], command["error_code"]) == (
        "parked",
        5,
        "RuntimeError",
    )
    assert command["past_deadline"] is True
    effect = await _effect(db, command_id)
    assert (effect["state"], effect["attempts"]) == ("pending", 5)
    stuck = await current_job(db, job["id"])
    assert stuck["status"] == "processing"
    assert stuck["context"]["workspace_container"]["status"] == "deleted"
    assert (
        await db.fetchval("SELECT state FROM run_queue WHERE unit_id=$1", job["id"])
        == "done"
    )
    (automatic,) = await _intents(db, job["id"])
    assert (automatic["admission_source"], automatic["result_kind"]) == (
        "automatic",
        "settled",
    )
    return stuck, owner, runtime, provisioner, resources, command_id


async def _unpark_and_finalize(db, command_id, job_id, provisioner):
    from orchestrator.services.completion_command_resolution import (
        CompletionCommandResolution,
    )

    unparked = await CompletionCommandResolution(db).unpark(
        command_id, actor="admin-proof"
    )
    assert unparked.state == "pending"
    assert unparked.reset_effects == ("pod_workspace_recovery",)
    return await CompletionFinalizer(
        db, workflow=_fixed_arm(db, job_id, provisioner)
    ).finalize_command(command_id, inline=False)


@pytest.mark.asyncio
async def test_t5_unpark_settles_the_parked_report_into_the_hold(
    db, monkeypatch, tmp_path
):
    """T5: after the fix, one unpark turns the parked shape into the hold."""

    from fastapi import HTTPException

    (
        stuck,
        owner,
        runtime,
        provisioner,
        resources,
        command_id,
    ) = await _parked_like_3677b309(db, monkeypatch)
    controls = _job_controls(db, tmp_path, provisioner)
    with pytest.raises(HTTPException) as refused:
        await controls.resume_job_internal(
            owner.id, user={"id": str(uuid4())}, job=stuck
        )
    assert refused.value.status_code == 409

    result = await _unpark_and_finalize(db, command_id, stuck["id"], provisioner)

    assert result.state == "done"
    assert (await _command(db, command_id))["state"] == "done"
    assert (await _effect(db, command_id))["state"] == "done"
    held = await current_job(db, stuck["id"])
    assert held["status"] == "paused"
    assert held["assigned_agent_id"] is None
    assert held["error_message"] == DEAD_ARM_ACTION
    assert held["context"]["_operator_pause_hold"]["source"] == (
        "workspace_recovery_unavailable"
    )
    assert held["context"]["workspace_container"]["recovery_attempts"] == 1
    assert len(await _intents(db, stuck["id"])) == 1
    assert "pod" not in resources and "pvc" in resources

    successor, cluster = await _resume_on_a_new_pod(
        db, monkeypatch, tmp_path, owner, held, resources["pvc"]
    )
    assert successor["context"]["workspace_container"]["status"] == "ready"
    assert successor["context"]["workspace_container"]["_runtime_incarnation"] != (
        runtime
    )
    assert cluster.objects["pvc"] is resources["pvc"]


@pytest.mark.asyncio
async def test_t13_pause_then_unpark(db, monkeypatch, tmp_path):
    """T13: the owner pressed Pause on the parked job first (as on 11-15)."""

    (
        stuck,
        owner,
        runtime,
        provisioner,
        resources,
        command_id,
    ) = await _parked_like_3677b309(db, monkeypatch)
    # Pause is not blocked by the parked command: the stateless store path
    # the public Pause uses is fenced by the control-claim marker only.
    assert await db.pause_stateless_job(
        str(stuck["id"]),
        operator_hold=True,
        paused_by=str(uuid4()),
        completion_commands_enabled=True,
    )
    paused = await current_job(db, stuck["id"])
    assert paused["status"] == "paused"
    assert paused["context"]["_operator_pause_hold"]["source"] == "public_pause"
    assert (await _command(db, command_id))["state"] == "parked"

    result = await _unpark_and_finalize(db, command_id, stuck["id"], provisioner)

    command = await _command(db, command_id)
    assert result.state == command["state"] == "superseded"
    assert command["error_code"] == "entry_status_superseded"
    outcome = json.loads(command["outcome"])
    assert outcome["observed_status"] == "paused"
    assert outcome["abandoned_effects"] == ["pod_workspace_recovery"]
    after = await current_job(db, stuck["id"])
    # The pause hold stays as the owner left it; the arm never ran.
    assert after["status"] == "paused"
    assert (
        after["context"]["_operator_pause_hold"]
        == paused["context"]["_operator_pause_hold"]
    )
    assert (
        after["context"]["workspace_container"]
        == paused["context"]["workspace_container"]
    )
    assert len(await _intents(db, stuck["id"])) == 1

    successor, cluster = await _resume_on_a_new_pod(
        db, monkeypatch, tmp_path, owner, after, resources["pvc"]
    )
    assert successor["context"]["workspace_container"]["status"] == "ready"
    assert cluster.objects["pvc"] is resources["pvc"]
