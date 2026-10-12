"""Production cleanup adapter + SQL authority, with Kubernetes transport models."""

import json
import asyncio
import asyncpg
from types import SimpleNamespace
from uuid import UUID
from dataclasses import replace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from kubernetes import client
from kubernetes.client.exceptions import ApiException
from fastapi import HTTPException

from orchestrator.services.completion import handle_pod_workspace_recovery
from orchestrator.services.completion_finalizer import (
    CompletionFinalizer,
    CompletionEffectRunner,
)
from orchestrator.services.completion_control import CompletionControl
from orchestrator.services.completion_runtime import CompletionControlBoundary
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from shared.operator_pause_hold import operator_pause_lift_token
from tests.test_container_dead_probe_recovery_real_postgres import prepare, intent_for
from tests.test_container_recovery_retention_real_postgres import (
    _schema_applied,  # noqa: F401
    accepted_recovery,
    current_job,
    db as recovery_db,
    pg_dsn,  # noqa: F401
)
from tests.test_non_pinned_workspace_lifecycle_real_postgres import (
    _create_settled_authoritative_runtime,
)
from tests.test_workspace_cleanup_retry_real_postgres import _absent_pod_provisioner
from tests.test_job_control_operations import _operations
from tests.test_vm_workspace_recovery_real_postgres import (
    prepare_checkpoint_rows,
    checkpoint_row_counts,
)


# Reuse the same real PostgreSQL fixture and migrations as the retention suite.
db = recovery_db


async def cleanup_job(db):
    job_id, runtime, creation, state = await _create_settled_authoritative_runtime(
        db,
        owner_kind="job",
        scope="workspace_container",
        settle=False,
    )
    owner = WorkspaceOwner.job(str(job_id))
    state["workspace_container"]["pod_name"] = owner.pod_name
    await db.execute(
        'UPDATE jobs SET context=$2::jsonb,config_override=\'{"workspace":{"backend":"sandbox"}}\' WHERE id=$1',
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
    agent = await db.fetchval(
        "INSERT INTO agents(config_name,hostname,status) VALUES('worker_base',$1,'ready') RETURNING id",
        "recovery-cleanup-" + uuid4().hex,
    )
    assert await db.claim_job_for_agent(str(job_id), str(agent))
    return await current_job(db, job_id), owner, runtime


def terminal_pod_api(db, owner, runtime):
    provisioner, resources = _absent_pod_provisioner(
        db, owner, {"pvc_uid": uuid4(), "service_uid": uuid4()}
    )
    resources["checkpoint_bytes"] = b"retained source data"
    pod = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=owner.pod_name,
            namespace=provisioner._namespace,
            uid=runtime,
            labels={
                "app": "srw-workspace",
                owner.label_key: owner.id,
                "srw/component": owner.component_label,
                "srw.io/component": "agent-workspace",
            },
        ),
        spec=client.V1PodSpec(
            containers=[
                client.V1Container(
                    name="workspace",
                    image="fixture",
                    volume_mounts=[
                        client.V1VolumeMount(
                            name="workspace-data",
                            mount_path="/home/agent-host",
                            read_only=False,
                        ),
                    ],
                )
            ],
            volumes=[
                client.V1Volume(
                    name="workspace-data",
                    persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                        claim_name="pvc-" + owner.pod_name, read_only=False
                    ),
                )
            ],
        ),
        status=client.V1PodStatus(
            phase="Failed",
            container_statuses=[
                client.V1ContainerStatus(
                    name="workspace",
                    image="fixture",
                    image_id="fixture",
                    ready=False,
                    restart_count=0,
                    state=client.V1ContainerState(
                        terminated=client.V1ContainerStateTerminated(exit_code=1)
                    ),
                )
            ],
        ),
    )
    resources["pod"] = pod

    def read(**kwargs):
        assert kwargs["name"] == owner.pod_name
        if "pod" not in resources:
            raise ApiException(status=404)
        return resources["pod"]

    def delete(**kwargs):
        assert kwargs["body"]["preconditions"]["uid"] == runtime
        resources.pop("pod")

    provisioner._core_api.read_namespaced_pod.side_effect = read
    provisioner._core_api.delete_namespaced_pod.side_effect = delete
    return provisioner, resources


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_reply", [False, True])
async def test_cleanup_settles_exact_uid_preserves_pvc_then_explicit_resume(
    db, lost_reply
):
    original, owner, runtime = await cleanup_job(db)
    runner = await accepted_recovery(db, original)
    p, resources = terminal_pod_api(db, owner, runtime)
    pvc = resources["pvc"]
    await prepare_checkpoint_rows(db._pool, original["id"], checkpoint_count=1)
    dispatch = Mock()
    delete = p._core_api.delete_namespaced_pod.side_effect
    if lost_reply:

        def deleted_then_timeout(**kwargs):
            delete(**kwargs)
            raise TimeoutError("DELETE committed; response lost")

        p._core_api.delete_namespaced_pod.side_effect = deleted_then_timeout

    async def recover(job):
        return await handle_pod_workspace_recovery(
            job,
            owner.id,
            {"type": "workspace_unavailable"},
            db=db,
            delete_workspace=AsyncMock(side_effect=AssertionError("untyped deletion")),
            cleanup_service=p,
            trigger_dispatch=dispatch,
            probe=AsyncMock(return_value=False),
            completion_command_id=runner.command_id,
            completion_finalizing_by=runner.owner,
        )

    if lost_reply:
        with pytest.raises(RuntimeError, match="cleanup.*pending"):
            await recover(original)
        held = await current_job(db, original["id"])
        assert (
            held["context"]["workspace_container"]["status"] == "retiring_process_zero"
        )
        assert (await intent_for(db, original))["result_kind"] is None
    outcome = (
        await recover(await current_job(db, original["id"]))
        if lost_reply
        else await recover(original)
    )
    assert outcome["held_for_resume"] is True and outcome["cleanup_pending"] is False
    assert "pod" not in resources and "service" not in resources
    assert (
        resources["pvc"] is pvc
        and resources["checkpoint_bytes"] == b"retained source data"
    )
    assert p._core_api.delete_namespaced_pod.call_count == 1
    p._core_api.delete_namespaced_persistent_volume_claim.assert_not_called()
    dispatch.assert_not_called()
    held = await current_job(db, original["id"])
    assert held["context"]["workspace_container"]["status"] == "deleted"
    assert held["context"]["workspace_container"]["recovery_attempts"] == 1
    assert await recover(held) == outcome
    assert await current_job(db, original["id"]) == held
    await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, outcome)
    token = operator_pause_lift_token(held)
    assert await db.shed_workspace_context(owner.id, "workspace_container")
    assert await db.queue_job_for_resume(
        owner.id, expected_status="paused", lift_operator_pause_hold=token
    )
    resumed = await current_job(db, original["id"])
    assert "_operator_pause_hold" not in resumed["context"]
    assert resumed["context"]["workspace_container"]["_runtime_incarnation"] == runtime
    assert resumed["context"]["workspace_container"]["status"] == "deleted"
    assert resources["pvc"] is pvc
    assert await checkpoint_row_counts(db._pool, original["id"]) == (1, 1, 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["entry", "capture", "claim"])
async def test_recovery_cannot_follow_cancel_policy_promotion(db, monkeypatch, stage):
    original, owner, runtime = await cleanup_job(db)
    runner = await accepted_recovery(db, original)
    admitted = await prepare(db, original, runner)
    receipt = admitted["cleanup_receipt"]
    assert await db.workspace_recovery_cleanup_is_current(receipt)
    p, resources = terminal_pod_api(db, owner, runtime)
    await p.capture_workspace_teardown_identity(
        owner, expected_runtime_incarnation=runtime
    )
    if stage == "entry":
        assert await db.cancel_job(owner.id)
    elif stage == "capture":
        capture = p.capture_workspace_teardown_identity

        async def capture_then_cancel(*args, **kwargs):
            await capture(*args, **kwargs)
            with pytest.raises(
                asyncpg.SerializationError,
                match="Workspace mutation is still in progress",
            ):
                await db.cancel_job(owner.id)
            raise RuntimeError("interrupted during capture")

        p.capture_workspace_teardown_identity = capture_then_cancel
    else:
        claim = type(db).claim_managed_repository_workspace_cleanup_intent
        calls = 0

        async def claim_then_cancel(self, *args, **kwargs):
            nonlocal calls
            result = await claim(self, *args, **kwargs)
            calls += 1
            if calls == 2:
                with pytest.raises(
                    asyncpg.SerializationError,
                    match="Workspace mutation is still in progress",
                ):
                    await db.cancel_job(owner.id)
                raise RuntimeError("interrupted during claim")
            return result

        monkeypatch.setattr(
            type(db),
            "claim_managed_repository_workspace_cleanup_intent",
            claim_then_cancel,
        )
    if stage != "entry":
        with pytest.raises(RuntimeError, match="interrupted during"):
            await p.reconcile_workspace_recovery_cleanup(receipt)
        assert await db.cancel_job(owner.id)
    cleanup = await p.reconcile_workspace_recovery_cleanup(receipt)
    assert cleanup.settled is False
    assert {"pod", "pvc", "service"} <= set(resources)
    p._core_api.delete_namespaced_pod.assert_not_called()
    p._core_api.delete_namespaced_persistent_volume_claim.assert_not_called()
    assert (await intent_for(db, original))["resource_policy"] == "terminal_reclaim"
    assert (await current_job(db, original["id"]))["status"] == "cancelled"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "storage,lane,commands",
    [
        ("present", "pinned", False),
        ("present", "pinned", True),
        ("present", "stateless", True),
        ("missing", "pinned", False),
        ("replaced", "pinned", False),
        ("missing_after_lift", "pinned", True),
        ("replaced_after_lift", "stateless", True),
    ],
)
async def test_public_resume_then_real_successor_creator_keeps_exact_retained_pvc(
    db, monkeypatch, tmp_path, storage, lane, commands
):
    from tests.test_active_workspace_creator_cancel_real_postgres import _make_ready
    from tests import test_workspace_pull_failure_real_postgres as creator_cases

    original, owner, runtime = await cleanup_job(db)
    runner = await accepted_recovery(db, original)
    first, resources = terminal_pod_api(db, owner, runtime)
    admitted = await prepare(db, original, runner)
    assert (
        await first.reconcile_workspace_recovery_cleanup(admitted["cleanup_receipt"])
    ).settled
    outcome = await db.complete_workspace_recovery_cleanup(
        admitted["cleanup_receipt"], completion_finalizing_by=runner.owner
    )
    await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, outcome)
    if lane == "stateless":
        await db.execute(
            "UPDATE jobs SET execution_lane='stateless' WHERE id=$1", original["id"]
        )
    held = await current_job(db, original["id"])
    pvc = resources["pvc"]
    pvc.retained_bytes = b"original workspace files"

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
    if storage != "missing":
        cluster.objects["pvc"] = pvc
    if storage == "replaced":
        pvc.metadata.uid = str(uuid4())
    provider = creator_cases._provisioner(monkeypatch, db, cluster)
    operations = _operations(tmp_path, store=db, completion_commands_enabled=commands)
    runtime_boundary = SimpleNamespace(
        dependencies=SimpleNamespace(
            commands_enabled=lambda: commands, logger=operations.dependencies.logger
        ),
        control=lambda: CompletionControl(db, AsyncMock()),
    )
    operations = replace(
        operations,
        dependencies=replace(
            operations.dependencies,
            completion_control=CompletionControlBoundary(runtime_boundary),
            get_container_context=lambda job: job["context"]["workspace_container"],
            validate_workspace_recovery_storage=provider.validate_workspace_recovery_storage,
            resume_missing_workspace=lambda job: "container",
        ),
    )
    if storage in {"missing", "replaced"}:
        with pytest.raises(HTTPException) as refused:
            await operations.resume_job_internal(
                owner.id, user={"id": str(uuid4())}, job=held
            )
        assert refused.value.status_code == 409
        assert await current_job(db, original["id"]) == held
        assert not await provider.create_workspace(owner)
        assert cluster.create_calls == []
        return
    await operations.resume_job_internal(owner.id, user={"id": str(uuid4())}, job=held)
    queued = await current_job(db, original["id"])
    assert "_operator_pause_hold" not in queued["context"]
    assert queued["context"]["workspace_container"]["_runtime_incarnation"] == runtime
    if storage.endswith("after_lift"):
        if storage.startswith("missing"):
            cluster.objects.pop("pvc")
        else:
            pvc.metadata.uid = str(uuid4())
        assert not await provider.create_workspace(owner)
        assert cluster.create_calls == []
        current = await current_job(db, original["id"])
        assert (
            current["context"]["workspace_container"]["_runtime_incarnation"] == runtime
        )
        return
    assert await provider.create_workspace(owner)
    successor = await current_job(db, original["id"])
    assert successor["context"]["workspace_container"]["status"] == "ready"
    assert (
        successor["context"]["workspace_container"]["_runtime_incarnation"] != runtime
    )
    assert "recovery_cleanup" not in successor["context"]["workspace_container"]
    assert successor["context"]["workspace_container"]["recovery_attempts"] == 1
    assert (
        successor["context"]["last_workspace_container"]["_runtime_incarnation"]
        == runtime
    )
    assert (
        cluster.objects["pvc"] is pvc
        and pvc.retained_bytes == b"original workspace files"
    )
    assert cluster.create_calls == ["pod", "service"]
    assert (
        await db.complete_workspace_recovery_cleanup(
            admitted["cleanup_receipt"],
            completion_finalizing_by=runner.owner,
        )
        is None
    )
    assert not await db.workspace_recovery_cleanup_is_current(
        admitted["cleanup_receipt"]
    )
    assert (
        await first.reconcile_workspace_recovery_cleanup(admitted["cleanup_receipt"])
    ).settled
    assert await current_job(db, original["id"]) == successor
    assert cluster.create_calls == ["pod", "service"]


@pytest.mark.asyncio
async def test_effect_journal_restart_replays_pending_cleanup_not_pure_hold(db):
    original, owner, runtime = await cleanup_job(db)
    runner = await accepted_recovery(db, original)
    provisioner, resources = terminal_pod_api(db, owner, runtime)
    delete = provisioner._core_api.delete_namespaced_pod.side_effect

    def lose_reply(**kwargs):
        delete(**kwargs)
        raise TimeoutError("DELETE committed; reply lost")

    provisioner._core_api.delete_namespaced_pod.side_effect = lose_reply
    dispatch = Mock()

    async def callback(current_runner):
        return await handle_pod_workspace_recovery(
            await current_job(db, original["id"]),
            owner.id,
            {"type": "workspace_unavailable"},
            db=db,
            cleanup_service=provisioner,
            delete_workspace=AsyncMock(side_effect=AssertionError("untyped delete")),
            trigger_dispatch=dispatch,
            probe=AsyncMock(return_value=False),
            completion_command_id=current_runner.command_id,
            completion_finalizing_by=current_runner.owner,
        )

    with pytest.raises(RuntimeError, match="cleanup.*pending") as interrupted:
        await runner.run(
            name="pod_workspace_recovery",
            group="recovery",
            callback=lambda: callback(runner),
            effect_timeout_seconds=6,
        )
    effect = await db.fetchrow(
        "SELECT state FROM completion_effects WHERE producer_id=$1 AND effect_name='pod_workspace_recovery'",
        UUID(runner.command_id),
    )
    assert effect["state"] != "done"
    first_intent = await intent_for(db, original)
    assert first_intent["settled_at"] is None
    finalizer = CompletionFinalizer(db)
    await finalizer._retry_or_park(runner.command_id, runner.owner, interrupted.value)
    async with asyncio.timeout(10):
        while await db.fetchval(
            "SELECT complete_by > clock_timestamp() FROM completion_effects WHERE producer_id=$1 AND effect_name='pod_workspace_recovery'",
            UUID(runner.command_id),
        ):
            await asyncio.sleep(0.1)
    command, new_owner = await finalizer._claim(runner.command_id, inline=True)
    assert command is not None and new_owner is not None and new_owner != runner.owner
    restarted = CompletionEffectRunner(db, command=command, owner=new_owner)
    result = await restarted.run(
        name="pod_workspace_recovery",
        group="recovery",
        callback=lambda: callback(restarted),
    )
    assert result["cleanup_pending"] is False
    assert (await intent_for(db, original))["id"] == first_intent["id"]
    assert provisioner._core_api.delete_namespaced_pod.call_count == 1
    assert resources["checkpoint_bytes"] == b"retained source data"
    assert (
        await restarted.run(
            name="pod_workspace_recovery",
            group="recovery",
            callback=AsyncMock(side_effect=AssertionError("done effect replayed")),
        )
        == result
    )
    dispatch.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["storage_source", "explicit_lift"])
async def test_native_cleanup_settlement_does_not_consume_pending_completion_receipt(
    db, path
):
    original, owner, runtime = await cleanup_job(db)
    runner = await accepted_recovery(db, original)
    admitted = await prepare(db, original, runner)
    provider, _ = terminal_pod_api(db, owner, runtime)
    assert (
        await provider.reconcile_workspace_recovery_cleanup(admitted["cleanup_receipt"])
    ).settled
    # An interruption here leaves physical cleanup settled but its domain
    # receipt pending. A flag-off control must not consume that continuation.
    held = await current_job(db, original["id"])
    assert (
        held["context"]["workspace_container"]["recovery_cleanup"]["phase"] == "pending"
    )
    if path == "storage_source":
        assert await db.get_workspace_recovery_storage(owner.id) is None
    else:
        assert not await db.queue_job_for_resume(
            owner.id,
            expected_status="paused",
            lift_operator_pause_hold=operator_pause_lift_token(held),
        )
    assert await current_job(db, original["id"]) == held


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("phase", True),
        ("phase", "pending"),
        ("version", "1"),
        ("version", True),
        ("version", 1.0),
        ("intent_generation", "1"),
        ("intent_generation", True),
        ("command_id", "not-a-uuid"),
        ("command_id", 1),
        ("hold_id", "not-a-uuid"),
        ("extra", "unexpected"),
        ("job_id", None),
        ("__replace__", None),
        ("__replace__", False),
        ("__replace__", []),
        ("__replace__", {}),
        ("__replace__", "not-a-receipt"),
    ],
)
async def test_malformed_settled_receipt_cannot_lift_or_claim(db, field, value):
    original, owner, runtime = await cleanup_job(db)
    runner = await accepted_recovery(db, original)
    admitted = await prepare(db, original, runner)
    provider, _ = terminal_pod_api(db, owner, runtime)
    assert (
        await provider.reconcile_workspace_recovery_cleanup(admitted["cleanup_receipt"])
    ).settled
    outcome = await db.complete_workspace_recovery_cleanup(
        admitted["cleanup_receipt"], completion_finalizing_by=runner.owner
    )
    await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, outcome)
    receipt = (
        value
        if field == "__replace__"
        else {**admitted["cleanup_receipt"], "phase": "settled", field: value}
    )
    await db.execute(
        "UPDATE jobs SET context=jsonb_set(context,'{workspace_container,recovery_cleanup}',$2::jsonb) WHERE id=$1",
        original["id"],
        json.dumps(receipt),
    )
    held = await current_job(db, original["id"])
    token = operator_pause_lift_token(held)
    assert await db.get_workspace_recovery_storage(owner.id) is None
    assert not await db.queue_job_for_resume(
        owner.id, expected_status="paused", lift_operator_pause_hold=token
    )
    assert not await db.claim_job_for_agent(
        owner.id, str(original["assigned_agent_id"]), lift_operator_pause_hold=token
    )
    assert await current_job(db, original["id"]) == held


@pytest.mark.asyncio
async def test_cancel_supersedes_pending_recovery_completion_without_reentering_effect(
    db,
):
    original, owner, runtime = await cleanup_job(db)
    runner = await accepted_recovery(db, original)
    provider, resources = terminal_pod_api(db, owner, runtime)

    async def recover():
        return await handle_pod_workspace_recovery(
            original,
            owner.id,
            {"type": "workspace_unavailable"},
            db=db,
            cleanup_service=None,
            delete_workspace=AsyncMock(side_effect=AssertionError("untyped cleanup")),
            trigger_dispatch=Mock(side_effect=AssertionError("automatic replay")),
            probe=AsyncMock(return_value=False),
            completion_command_id=runner.command_id,
            completion_finalizing_by=runner.owner,
        )

    with pytest.raises(RuntimeError, match="cleanup.*pending"):
        await runner.run(
            name="pod_workspace_recovery",
            group="recovery",
            callback=recover,
            effect_timeout_seconds=6,
        )
    finalizer = CompletionFinalizer(db)
    await finalizer._retry_or_park(
        runner.command_id,
        runner.owner,
        RuntimeError("interruption before command finish"),
    )
    assert await db.cancel_job(owner.id)
    async with asyncio.timeout(10):
        while await db.fetchval(
            "SELECT complete_by > clock_timestamp() FROM completion_effects WHERE producer_id=$1 AND effect_name='pod_workspace_recovery'",
            UUID(runner.command_id),
        ):
            await asyncio.sleep(0.1)
    callback = AsyncMock(
        side_effect=AssertionError("cancelled completion re-entered cleanup")
    )
    result = await finalizer.finalize_command(runner.command_id, callback=callback)
    assert result.disposition == "superseded"
    callback.assert_not_awaited()
    assert (await current_job(db, original["id"]))["status"] == "cancelled"
    provider._core_api.delete_namespaced_persistent_volume_claim.assert_not_called()
    assert resources["checkpoint_bytes"] == b"retained source data"
