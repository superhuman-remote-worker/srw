"""Restore clears only a proven suspended predecessor, preserving its volume."""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import pytest

from orchestrator.services.manifest_execution_snapshot import read_execution
from orchestrator.services.session_provisioner import ensure_session_workspace
from orchestrator.services.workspace_lifecycle import EnsureOutcome
from orchestrator.services.workspace_suspension import (
    WorkspaceSuspensionService,
    _settled_restore_runtime,
)
from tests import test_stateless_workspace_continuation_real_postgres as continuation

actor = continuation.actor
database = continuation.database
postgres_url = continuation.postgres_url


async def cleanup_rows(database, thread_id):
    return [
        dict(row)
        for row in await database.fetch(
            "SELECT * FROM managed_repository_workspace_cleanup_intents "
            "WHERE owner_kind='thread' AND owner_id=$1::uuid AND scope='workspace_container' "
            "ORDER BY intent_generation",
            thread_id,
        )
    ]


async def suspended_case(database, actor, monkeypatch, *, finish_cleanup=True):
    case = await continuation.workspace_attempt(
        database, actor, monkeypatch, first_wait="ready"
    )
    captured = await case.provisioner.prepare_workspace_cleanup_intent(
        case.owner,
        expected_runtime_incarnation=case.pod_uid,
        target_disposition="suspended",
        reclaim_shared_resources=False,
        suspended_at=datetime.now(timezone.utc).isoformat(),
        snapshot_restore_required=True,
    )
    assert captured is not None
    if finish_cleanup:
        result = await case.provisioner.reconcile_workspace_cleanup_intent(
            case.owner,
            expected_runtime_incarnation=case.pod_uid,
            intent_generation=captured["intent_generation"],
        )
        assert result.settled
        assert "pod" not in case.cluster.objects
        prepared = await database.prepare_stateless_thread_workspace_creation(
            case.thread_id,
            proposed_generation=str(uuid4()),
            mode="restore",
            expected_runtime_incarnation=case.pod_uid,
        )
        assert prepared["state"] == "prepared"
    case.generation = str(case.before["runtime_generation"])
    case.source = (await cleanup_rows(database, case.thread_id))[0]
    service = WorkspaceSuspensionService()
    service.connect(database, SimpleNamespace(is_available=True), case.provisioner)
    # A retained PVC must never be overwritten by an older snapshot. The
    # production restore-work lease, live attestation and completion still run.
    service._extract_snapshot = AsyncMock(
        side_effect=AssertionError("retained PVC extraction")
    )
    case.suspension = service
    wait = case.provisioner._wait_for_ready

    async def ready(*args, **kwargs):
        case.cluster.become_ready()
        return await wait(*args, **kwargs)

    monkeypatch.setattr(case.provisioner, "_wait_for_ready", ready)
    return case


async def ensure(case, database):
    return await ensure_session_workspace(
        case.thread_id,
        db=database,
        provisioner=case.provisioner,
        suspension=case.suspension,
    )


async def assert_cleared(case, database):
    rows = await cleanup_rows(database, case.thread_id)
    assert len(rows) == 2
    assert rows[0] == case.source
    derived = rows[1]
    assert derived["target_disposition"] == "deleted"
    assert derived["resource_policy"] == "preserve"
    assert derived["result_kind"] == "settled"
    assert derived["settled_at"] is not None
    for key in (
        "thread_runtime_generation",
        "runtime_incarnation",
        "pod_uid",
        "pvc_uid",
        "seed_configmap_uid",
        "service_uid",
        "resource_location",
    ):
        assert derived[key] == case.source[key]
    source = await database.get_stateless_thread_workspace_restore_predecessor(
        case.thread_id,
        generation=case.generation,
    )
    assert source["id"] == case.source["id"]
    current = await database.get_thread(case.thread_id)
    workspace = continuation.metadata(current)["workspace_container"]
    assert workspace["_runtime_incarnation"] is None
    assert workspace["status"] == "deleted"
    assert workspace["_snapshot_restore_required"] is True
    assert workspace["_runtime_creation"] == {
        "generation": case.generation,
        "mode": "restore",
        "attempted": False,
        "replaces_uid": case.pod_uid,
    }
    return rows


@pytest.mark.asyncio
async def test_normal_ensure_clears_then_restores_exact_retained_volume(
    database, actor, monkeypatch
):
    case = await suspended_case(database, actor, monkeypatch)
    first = await ensure(case, database)
    assert first.outcome == EnsureOutcome.PENDING
    current = await database.get_thread(case.thread_id)
    workspace = continuation.metadata(current)["workspace_container"]
    successor = case.cluster.objects["pod"].metadata.uid
    assert successor != case.pod_uid
    assert workspace["_runtime_incarnation"] == successor
    assert workspace["_snapshot_restore_required"] is False
    assert "_runtime_creation" not in workspace
    assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid
    rows = await cleanup_rows(database, case.thread_id)
    assert len(rows) == 2 and rows[0] == case.source
    restore = await case.provisioner.get_current_workspace_creation_result(
        case.owner, operation_kind="restore"
    )
    assert restore["claimed_by"] == f"container-restore:{case.source['id']}"
    assert restore["result_kind"] == "settled"
    assert restore["restore_work_completed_at"] is not None
    assert restore["restore_work_result_kind"] == "ready"
    assert (
        await read_execution(database, "Session", case.thread_id)
        == case.original_snapshot
    )
    assert (await ensure(case, database)).outcome == EnsureOutcome.READY
    # An old clear caller cannot drop B or mint another receipt after success.
    assert not await database.clear_stateless_thread_workspace_runtime_for_recreation(
        case.thread_id,
        generation=case.generation,
        expected_runtime_incarnation=case.pod_uid,
    )
    assert not await case.provisioner.finalize_stateless_workspace_recreation_deletion(
        case.owner,
        generation=case.generation,
        expected_runtime_incarnation=case.pod_uid,
    )
    assert await cleanup_rows(database, case.thread_id) == rows
    assert case.cluster.pod_create_calls == 2
    case.suspension._extract_snapshot.assert_not_called()


class LostClearReply(BaseException):
    """Process stops immediately after the predecessor transaction commits."""


@pytest.mark.asyncio
async def test_lost_clear_response_replays_one_receipt_then_normal_restore(
    database, actor, monkeypatch
):
    case = await suspended_case(database, actor, monkeypatch)
    clear = type(database).clear_stateless_thread_workspace_runtime_for_recreation

    async def lose(self, thread_id, **kwargs):
        result = await clear(self, thread_id, **kwargs)
        if result:
            raise LostClearReply
        return result

    with monkeypatch.context() as interruption:
        interruption.setattr(
            type(database),
            "clear_stateless_thread_workspace_runtime_for_recreation",
            lose,
        )
        with pytest.raises(LostClearReply):
            await ensure(case, database)
    rows = await assert_cleared(case, database)
    assert case.cluster.pod_create_calls == 1
    assert await case.provisioner.finalize_stateless_workspace_recreation_deletion(
        case.owner,
        generation=case.generation,
        expected_runtime_incarnation=case.pod_uid,
    )
    assert await cleanup_rows(database, case.thread_id) == rows
    await ensure(case, database)
    assert (await ensure(case, database)).outcome == EnsureOutcome.READY
    assert await cleanup_rows(database, case.thread_id) == rows
    assert case.cluster.pod_create_calls == 2
    assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid


@pytest.mark.asyncio
async def test_owner_projection_fault_rolls_back_derived_receipt(
    database, actor, monkeypatch
):
    case = await suspended_case(database, actor, monkeypatch)
    before = await database.get_thread(case.thread_id)
    await database.execute("""
        CREATE FUNCTION fail_test_restore_clear() RETURNS trigger AS $$
        BEGIN
            IF NEW.metadata->'workspace_container'->>'status' = 'deleted' THEN
                IF EXISTS (SELECT 1 FROM managed_repository_workspace_cleanup_intents
                    WHERE owner_id=NEW.id AND target_disposition='deleted' AND result_kind='settled') THEN
                    RAISE EXCEPTION 'test clear fault after derived settlement';
                END IF;
                RAISE EXCEPTION 'test clear fault before derived settlement';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        CREATE TRIGGER fail_test_restore_clear BEFORE UPDATE ON threads
        FOR EACH ROW EXECUTE FUNCTION fail_test_restore_clear();
    """)
    try:
        with pytest.raises(asyncpg.RaiseError, match="after derived settlement"):
            await case.provisioner.finalize_stateless_workspace_recreation_deletion(
                case.owner,
                generation=case.generation,
                expected_runtime_incarnation=case.pod_uid,
            )
        assert await database.get_thread(case.thread_id) == before
        assert await cleanup_rows(database, case.thread_id) == [case.source]
    finally:
        await database.execute(
            "DROP TRIGGER fail_test_restore_clear ON threads; DROP FUNCTION fail_test_restore_clear()"
        )
    assert await case.provisioner.finalize_stateless_workspace_recreation_deletion(
        case.owner,
        generation=case.generation,
        expected_runtime_incarnation=case.pod_uid,
    )
    await assert_cleared(case, database)


@pytest.mark.asyncio
@pytest.mark.parametrize("storage", ("missing", "replaced"))
async def test_restore_refuses_lost_retained_pvc_before_any_create(
    database, actor, monkeypatch, storage
):
    case = await suspended_case(database, actor, monkeypatch)
    if storage == "missing":
        del case.cluster.objects["pvc"]
    else:
        case.cluster.objects["pvc"].metadata.uid = str(uuid4())
    creates = []
    # Any Kubernetes CREATE after suspension is a failure for this refusal.
    for name in (
        "create_namespaced_persistent_volume_claim",
        "create_namespaced_pod",
        "create_namespaced_service",
        "create_namespaced_config_map",
    ):

        def forbidden(*args, _name=name, **kwargs):
            creates.append(_name)
            raise AssertionError("unexpected Kubernetes CREATE")

        monkeypatch.setattr(case.cluster, name, forbidden, raising=False)
    await ensure(case, database)
    await assert_cleared(case, database)
    assert creates == []
    assert case.cluster.pod_create_calls == 1
    assert "pod" not in case.cluster.objects
    case.suspension._extract_snapshot.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "refusal",
    (
        "generation",
        "runtime",
        "attempted",
        "process_zero",
        "unfinished_cleanup",
        "live_replacement",
    ),
)
async def test_predecessor_clear_refuses_missing_or_stale_authority(
    database, actor, monkeypatch, refusal
):
    case = await suspended_case(
        database, actor, monkeypatch, finish_cleanup=refusal != "unfinished_cleanup"
    )
    generation, runtime = case.generation, case.pod_uid
    if refusal == "generation":
        generation = str(uuid4())
    elif refusal == "runtime":
        runtime = str(uuid4())
    elif refusal == "attempted":
        await database.execute(
            "UPDATE threads SET metadata=jsonb_set(metadata,'{workspace_container,_runtime_creation,attempted}','true'::jsonb) WHERE id=$1::uuid",
            case.thread_id,
        )
    elif refusal == "process_zero":
        await database.execute(
            "DELETE FROM managed_repository_process_zero_receipts WHERE owner_id=$1::uuid",
            case.thread_id,
        )
    elif refusal == "live_replacement":
        # A same-name successor is an external observation, never absence.
        replacement = SimpleNamespace(
            metadata=SimpleNamespace(name=case.owner.pod_name, uid=str(uuid4()))
        )
        case.cluster.objects["pod"] = replacement
    before = await database.get_thread(case.thread_id)
    rows = await cleanup_rows(database, case.thread_id)
    assert not await case.provisioner.finalize_stateless_workspace_recreation_deletion(
        case.owner,
        generation=generation,
        expected_runtime_incarnation=runtime,
    )
    assert await database.get_thread(case.thread_id) == before
    assert await cleanup_rows(database, case.thread_id) == rows
    assert case.cluster.pod_create_calls == 1


@pytest.mark.asyncio
async def test_raw_uid_clear_cannot_replace_derived_cleanup_authority(
    database, actor, monkeypatch
):
    case = await suspended_case(database, actor, monkeypatch)
    before = await database.get_thread(case.thread_id)
    with pytest.raises(asyncpg.CheckViolationError):
        await database.execute(
            "UPDATE threads SET metadata=jsonb_set(metadata,'{workspace_container}', "
            'metadata->\'workspace_container\' || \'{"status":"deleted","_runtime_incarnation":null}\'::jsonb) '
            "WHERE id=$1::uuid",
            case.thread_id,
        )
    assert await database.get_thread(case.thread_id) == before
    assert (
        await database.get_stateless_thread_workspace_restore_predecessor(
            case.thread_id,
            generation=case.generation,
        )
        is None
    )
    assert await cleanup_rows(database, case.thread_id) == [case.source]


@pytest.mark.parametrize("kind", ("native", "text", "wrong_native", "noncanonical"))
def test_settled_restore_receipt_accepts_native_uuid_but_requires_exact_runtime(kind):
    runtime = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    receipt_runtime = {
        "native": UUID(runtime),
        "text": runtime,
        "wrong_native": uuid4(),
        "noncanonical": runtime.upper(),
    }[kind]
    reservation_id = uuid4()
    workspace = {
        "_runtime_incarnation": runtime,
        "_snapshot_restore_required": True,
        "_creation_reservation_id": str(reservation_id),
        "_creation_claim_token": "17",
    }
    creation = {
        "id": reservation_id,
        "operation_kind": "restore",
        "result_kind": "settled",
        "settled_at": datetime.now(timezone.utc),
        "claim_token": 17,
        "runtime_incarnation": receipt_runtime,
    }
    assert _settled_restore_runtime(workspace, creation) == (
        runtime if kind in {"native", "text"} else None
    )


@pytest.mark.asyncio
async def test_restore_create_refuses_another_suspension_operation(
    database, actor, monkeypatch
):
    case = await suspended_case(database, actor, monkeypatch)
    assert await case.provisioner.finalize_stateless_workspace_recreation_deletion(
        case.owner,
        generation=case.generation,
        expected_runtime_incarnation=case.pod_uid,
    )
    before = await database.get_thread(case.thread_id)
    assert not await case.provisioner.create_workspace(
        case.owner,
        stateless_creation_generation=case.generation,
        allow_stateless_create=True,
        operation_kind="restore",
        operation_id=str(uuid4()),
    )
    assert await database.get_thread(case.thread_id) == before
    assert case.cluster.pod_create_calls == 1
    assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid


@pytest.mark.asyncio
@pytest.mark.parametrize("lineage", ("missing_source", "extra_field"))
async def test_restore_refuses_unproven_derived_suspension_lineage(
    database, actor, monkeypatch, lineage
):
    case = await suspended_case(database, actor, monkeypatch)
    assert await case.provisioner.finalize_stateless_workspace_recreation_deletion(
        case.owner,
        generation=case.generation,
        expected_runtime_incarnation=case.pod_uid,
    )
    derived = (await cleanup_rows(database, case.thread_id))[-1]
    if lineage == "missing_source":
        await database.execute(
            "UPDATE managed_repository_workspace_cleanup_intents SET lifecycle_fingerprint="
            "jsonb_set(lifecycle_fingerprint,'{suspension_intent_id}',to_jsonb($2::text)) WHERE id=$1",
            derived["id"],
            str(uuid4()),
        )
    else:
        await database.execute(
            "UPDATE managed_repository_workspace_cleanup_intents SET lifecycle_fingerprint="
            "lifecycle_fingerprint || '{\"unproven\":true}'::jsonb WHERE id=$1",
            derived["id"],
        )
    before = await database.get_thread(case.thread_id)
    rows = await cleanup_rows(database, case.thread_id)
    assert (
        await database.get_stateless_thread_workspace_restore_predecessor(
            case.thread_id,
            generation=case.generation,
        )
        is None
    )
    assert not await case.suspension.restore(
        case.owner,
        stateless_creation_generation=case.generation,
        allow_stateless_create=True,
    )
    assert await database.get_thread(case.thread_id) == before
    assert await cleanup_rows(database, case.thread_id) == rows
    assert case.cluster.pod_create_calls == 1


@pytest.mark.asyncio
async def test_restore_endpoint_stays_quarantined_until_exact_work_completion(
    database, actor, monkeypatch
):
    from orchestrator.services.workspace_binding import (
        remote_canvas_presentation_available,
    )

    case = await suspended_case(database, actor, monkeypatch)
    with monkeypatch.context() as interruption:
        # Leave the real restore-work lease unfinished at its final boundary.
        interruption.setattr(
            case.provisioner,
            "complete_strict_thread_restore_work",
            AsyncMock(return_value=False),
        )
        assert (await ensure(case, database)).outcome == EnsureOutcome.PENDING
        current = await database.get_thread(case.thread_id)
        metadata = continuation.metadata(current)
        workspace = metadata["workspace_container"]
        assert workspace["status"] == "restoring"
        assert workspace["_snapshot_restore_required"] is True
        assert "_runtime_creation" not in workspace
        assert not remote_canvas_presentation_available(metadata, workspace)
        creation = await case.provisioner.get_current_workspace_creation_result(
            case.owner, operation_kind="restore"
        )
        assert creation["result_kind"] == "settled"
        assert creation["restore_work_completed_at"] is None
        assert await case.provisioner._stateless_workspace_creation_is_settled(
            case.owner, generation=case.generation, reservation=creation
        )
        # An attested live endpoint is still not executable Session readiness.
        assert (await ensure(case, database)).outcome != EnsureOutcome.READY
    # Advance only this receipt's retry timer; preserve the released lease,
    # incomplete work and all owner/runtime authority exactly as persisted.
    await database.execute(
        "UPDATE managed_repository_workspace_creation_reservations "
        "SET restore_work_next_attempt_at=now() WHERE id=$1",
        creation["id"],
    )
    await ensure(case, database)
    assert (await ensure(case, database)).outcome == EnsureOutcome.READY
    current = await database.get_thread(case.thread_id)
    metadata = continuation.metadata(current)
    workspace = metadata["workspace_container"]
    assert workspace["status"] == "ready"
    assert workspace["_snapshot_restore_required"] is False
    assert remote_canvas_presentation_available(metadata, workspace)
    # A late wrapper receipt check accepts the exact completed restore, too.
    assert await case.provisioner._stateless_workspace_creation_is_settled(
        case.owner, generation=case.generation, reservation=creation
    )
    assert case.cluster.pod_create_calls == 2


@pytest.mark.asyncio
async def test_fresh_create_still_publishes_ready_without_restore_debt(
    database, actor, monkeypatch
):
    case = await continuation.workspace_attempt(
        database, actor, monkeypatch, first_wait="ready"
    )
    current = await database.get_thread(case.thread_id)
    workspace = continuation.metadata(current)["workspace_container"]
    assert workspace["status"] == "ready"
    assert workspace.get("_snapshot_restore_required", False) is False
    assert "_runtime_creation" not in workspace
    assert (
        await ensure_session_workspace(
            case.thread_id,
            db=database,
            provisioner=case.provisioner,
            suspension=case.suspension,
        )
    ).outcome == EnsureOutcome.READY
    assert (await continuation.reservation(database, case.thread_id))[
        "result_kind"
    ] == "settled"
