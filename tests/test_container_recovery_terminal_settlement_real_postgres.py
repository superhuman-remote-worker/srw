"""0292: real PG terminal admission after modeled exact preserve cleanup."""

from uuid import UUID
from unittest.mock import AsyncMock, Mock

import pytest

from orchestrator.services.completion import handle_pod_workspace_recovery
from orchestrator.services.completion_finalizer import CompletionFinalizer
from tests import test_container_recovery_cleanup_real_postgres as recovery
from tests.test_non_pinned_workspace_lifecycle_real_postgres import (
    _create_settled_authoritative_runtime,
)

pg_dsn = recovery.pg_dsn
_schema_applied = recovery._schema_applied
db = recovery.db


@pytest.mark.asyncio
async def test_settled_recovery_cancel_supersedes_then_purges_exact_retained_pvc(db):
    original, owner, runtime = await recovery.cleanup_job(db)
    runner = await recovery.accepted_recovery(db, original)
    provider, resources = recovery.terminal_pod_api(db, owner, runtime)

    async def recover():
        return await handle_pod_workspace_recovery(
            original,
            owner.id,
            {"type": "workspace_unavailable"},
            db=db,
            cleanup_service=provider,
            delete_workspace=AsyncMock(side_effect=AssertionError("untyped cleanup")),
            trigger_dispatch=Mock(side_effect=AssertionError("automatic replay")),
            probe=AsyncMock(return_value=False),
            completion_command_id=runner.command_id,
            completion_finalizing_by=runner.owner,
        )

    outcome = await runner.run(
        name="pod_workspace_recovery", group="recovery", callback=recover
    )
    assert outcome["cleanup_pending"] is False
    preserve = dict(await recovery.intent_for(db, original))
    held = await recovery.current_job(db, original["id"])
    receipt = held["context"]["workspace_container"]["recovery_cleanup"]
    prior_receipts = await db.fetch(
        "SELECT * FROM managed_repository_process_zero_receipts WHERE owner_id=$1 ORDER BY id",
        original["id"],
    )
    assert prior_receipts
    finalizer = CompletionFinalizer(db)
    await finalizer._retry_or_park(
        runner.command_id, runner.owner, RuntimeError("lost command finish")
    )
    assert await db.cancel_job(owner.id)
    cancelled = await recovery.current_job(db, original["id"])
    assert cancelled["context"]["workspace_container"]["status"] == "deleted"
    assert (
        cancelled["context"]["workspace_container"]["_runtime_incarnation"] == runtime
    )
    terminal = await recovery.intent_for(db, original)
    assert terminal["id"] != preserve["id"]
    assert terminal["resource_policy"] == "terminal_reclaim"
    assert terminal["settled_at"] is None
    assert str(terminal["runtime_incarnation"]) == runtime

    # An old preserve receipt can acknowledge its own result only. It has no
    # authority to act on this new terminal storage intent.
    assert (await provider.reconcile_workspace_recovery_cleanup(receipt)).settled
    assert (
        await db.complete_workspace_recovery_cleanup(
            receipt, completion_finalizing_by=runner.owner
        )
        is None
    )
    provider._core_api.delete_namespaced_persistent_volume_claim.assert_not_called()
    callback = AsyncMock(
        side_effect=AssertionError("cancelled report re-entered cleanup")
    )
    result = await finalizer.finalize_command(runner.command_id, callback=callback)
    assert result.disposition == "superseded"
    callback.assert_not_awaited()

    assert (
        await provider.reconcile_workspace_cleanup_intent(
            owner,
            expected_runtime_incarnation=runtime,
            intent_generation=terminal["intent_generation"],
        )
    ).settled
    assert "pvc" not in resources
    assert provider._core_api.delete_namespaced_persistent_volume_claim.call_count == 1
    assert provider._core_api.delete_namespaced_pod.call_count == 1
    settled = await recovery.intent_for(db, original)
    assert settled["id"] == terminal["id"] and settled["result_kind"] == "settled"
    assert (
        dict(
            await db.fetchrow(
                "SELECT * FROM managed_repository_workspace_cleanup_intents WHERE id=$1",
                preserve["id"],
            )
        )
        == preserve
    )
    assert (
        await db.fetch(
            "SELECT * FROM managed_repository_process_zero_receipts WHERE owner_id=$1 ORDER BY id",
            original["id"],
        )
        == prior_receipts
    )
    assert (
        await provider.reconcile_workspace_cleanup_intent(
            owner,
            expected_runtime_incarnation=runtime,
            intent_generation=terminal["intent_generation"],
        )
    ).settled
    assert provider._core_api.delete_namespaced_persistent_volume_claim.call_count == 1
    assert (
        await db.fetchval(
            "SELECT state FROM job_completion_commands WHERE id=$1",
            UUID(runner.command_id),
        )
        == "superseded"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ["workspace_container", "ide"])
async def test_settled_receipt_of_another_owner_never_skips_live_runtime_retirement(
    db, scope
):
    original, owner, runtime = await recovery.cleanup_job(db)
    runner = await recovery.accepted_recovery(db, original)
    admitted = await recovery.prepare(db, original, runner)
    provider, _ = recovery.terminal_pod_api(db, owner, runtime)
    assert (
        await provider.reconcile_workspace_recovery_cleanup(admitted["cleanup_receipt"])
    ).settled
    other, other_runtime, _, _ = await _create_settled_authoritative_runtime(
        db, owner_kind="job", scope=scope
    )
    assert other_runtime != runtime
    assert not await db.fetchval(
        "SELECT EXISTS(SELECT 1 FROM managed_repository_process_zero_receipts WHERE owner_id=$1)",
        other,
    )
    assert await db.cancel_job(str(other))
    current = await recovery.current_job(db, other)
    key = "workspace_container" if scope == "workspace_container" else "ide_session"
    assert current["context"][key]["status"] == "retiring_process_zero"
    assert current["context"][key]["_runtime_incarnation"] == other_runtime
    terminal = await db.get_managed_repository_workspace_cleanup_intent(
        str(other), owner_kind="job", scope=scope, runtime_incarnation=other_runtime
    )
    assert terminal["settled_at"] is None
    assert terminal["resource_policy"] == "terminal_reclaim"
    assert terminal["reclaim_shared_resources"] is (scope == "workspace_container")


@pytest.mark.asyncio
async def test_predecessor_process_zero_does_not_skip_successor_runtime_retirement(
    db, monkeypatch
):
    from tests.test_active_workspace_creator_cancel_real_postgres import _make_ready
    from tests import test_workspace_pull_failure_real_postgres as creator_cases
    from shared.operator_pause_hold import operator_pause_lift_token

    original, owner, runtime = await recovery.cleanup_job(db)
    runner = await recovery.accepted_recovery(db, original)
    admitted = await recovery.prepare(db, original, runner)
    first, resources = recovery.terminal_pod_api(db, owner, runtime)
    assert (
        await first.reconcile_workspace_recovery_cleanup(admitted["cleanup_receipt"])
    ).settled
    outcome = await db.complete_workspace_recovery_cleanup(
        admitted["cleanup_receipt"], completion_finalizing_by=runner.owner
    )
    await CompletionFinalizer(db)._finish(runner.command_id, runner.owner, outcome)
    held = await recovery.current_job(db, original["id"])
    assert await first.validate_workspace_recovery_storage(owner.id)
    assert await db.queue_job_for_resume(
        owner.id,
        expected_status="paused",
        lift_operator_pause_hold=operator_pause_lift_token(held),
    )

    class ReadyCluster(creator_cases.NeverPullingCluster):
        def create_namespaced_pod(self, **kwargs):
            result = super().create_namespaced_pod(**kwargs)
            _make_ready(self, monkeypatch)
            return result

    cluster = ReadyCluster()
    cluster.objects["pvc"] = resources["pvc"]
    provider = creator_cases._provisioner(monkeypatch, db, cluster)
    assert await provider.create_workspace(owner)
    successor = await recovery.current_job(db, original["id"])
    successor_uid = successor["context"]["workspace_container"]["_runtime_incarnation"]
    assert successor_uid != runtime
    assert successor["context"]["workspace_container"]["status"] == "ready"
    assert await db.cancel_job(owner.id)
    cancelled = await recovery.current_job(db, original["id"])
    assert (
        cancelled["context"]["workspace_container"]["status"] == "retiring_process_zero"
    )
    assert (
        cancelled["context"]["workspace_container"]["_runtime_incarnation"]
        == successor_uid
    )
    latest = await db.get_managed_repository_workspace_cleanup_intent(
        owner.id,
        owner_kind="job",
        scope="workspace_container",
        runtime_incarnation=successor_uid,
    )
    assert latest["result_kind"] is None
    assert str(latest["runtime_incarnation"]) == successor_uid
    assert (await recovery.intent_for(db, original))["result_kind"] == "settled"
