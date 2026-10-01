"""An acknowledged running Session End survives loss of its request owner."""

import asyncio
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from orchestrator.services import stale_agent_detector as detector
from orchestrator.services import stateless_session_retirement as protocol
from orchestrator.services.thread_retirement import ThreadRetirementOperations
from tests import test_stateless_failed_start_end_real_postgres as initial
from tests.test_stale_agent_detector import _detector_dependencies, _mock_db

actor = initial.actor
database = initial.database
postgres_url = initial.postgres_url
metadata = initial.metadata


async def acknowledged_running_end(
    database, actor, monkeypatch, *, permanent, physical_cleanup=True
):
    case = await initial.workspace_attempt(
        database, actor, monkeypatch, first_wait="ready"
    )
    # A completed claimant used this ready runtime. Keep its real creation
    # reservation/binding and exercise all End/cleanup database authorities.
    await database.execute(
        "INSERT INTO run_queue(unit_id,unit_kind,state,lease_token) "
        "VALUES($1::uuid,'session_turn','done',2)",
        case.thread_id,
    )

    async def residents(thread, *, terminal_token, **_):
        return protocol.ResidentRetirementProof(
            authority=protocol.resolve_shell_retirement_authority(
                thread, terminal_token=terminal_token
            )
        )

    async def shell(thread, *, terminal_token, **_):
        return protocol.resolve_shell_retirement_authority(
            thread, terminal_token=terminal_token
        )

    # Only the remote SSH protocol is external; acknowledgement persistence,
    # finalizer/process-zero checks, exact cleanup and owner deletion are real.
    monkeypatch.setattr(protocol, "retire_stateless_workspace_residents", residents)
    monkeypatch.setattr(protocol, "retire_stateless_session_shell", shell)
    monkeypatch.setattr(protocol, "verify_stateless_workspace_residents_retired", shell)
    dependencies = replace(
        initial.retirement_dependencies(database, case),
        build_agent_cloud_mount=AsyncMock(return_value=None),
    )
    operations = ThreadRetirementOperations(dependencies)
    release = case.provisioner.release_workspace

    async def lose_response(*args, **kwargs):
        if physical_cleanup:
            assert await release(*args, **kwargs)
        raise asyncio.CancelledError()

    with monkeypatch.context() as patch:
        patch.setattr(case.provisioner, "release_workspace", lose_response)
        with pytest.raises(asyncio.CancelledError):
            await operations.end_thread_flow(
                case.thread_id, case.before, permanent=permanent, force=False
            )
    pending = await database.get_thread(case.thread_id)
    marker = metadata(pending)["_stateless_claim_retirement"]
    assert "initial_creation" not in marker
    assert marker["remote_retired"] and marker["residents_retired"]
    assert metadata(pending)["_stateless_workspace_retirement_pending"] is True
    intent = await database.fetchrow(
        "SELECT * FROM managed_repository_workspace_cleanup_intents "
        "WHERE owner_id=$1::uuid ORDER BY intent_generation DESC LIMIT 1",
        case.thread_id,
    )
    assert intent["result_kind"] == ("settled" if physical_cleanup else None)
    assert str(intent["runtime_incarnation"]) == case.pod_uid
    assert ("pod" not in case.cluster.objects) is physical_cleanup
    return case, pending, operations


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent", [False, True])
@pytest.mark.parametrize("physical_cleanup", [False, True])
async def test_detector_finishes_acknowledged_end_after_physical_cleanup(
    database, actor, monkeypatch, permanent, physical_cleanup
):
    """Physical cleanup must hand its still-pending business End to the sweep."""
    case, pending, operations = await acknowledged_running_end(
        database,
        actor,
        monkeypatch,
        permanent=permanent,
        physical_cleanup=physical_cleanup,
    )
    shutdown = asyncio.Event()
    store = _mock_db(shutdown)
    store.list_retryable_stateless_end_settlements = (
        database.list_retryable_stateless_end_settlements
    )
    store.get_thread = database.get_thread
    await detector.stale_agent_detector(
        shutdown,
        dependencies=_detector_dependencies(
            store, thread_retirement_operations=lambda: operations
        ),
    )
    after = await database.get_thread(case.thread_id)
    if permanent:
        assert after is None
        assert case.cluster.objects == {}
        assert await detector.retry_stateless_end_settlement(
            pending,
            dependencies=SimpleNamespace(
                store=database, thread_retirement_operations=lambda: operations
            ),
        )
    else:
        assert "_stateless_workspace_retirement_pending" not in metadata(after)
        assert (
            metadata(after)["_stateless_workspace_retirement_settled"][
                "cleanup_complete"
            ]
            is True
        )
        assert metadata(after)["workspace_container"]["volume_reclaimed"] is False
        assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid
    assert store.gc_offline_agents.await_count == 1


@pytest.mark.asyncio
async def test_physical_soft_end_keeps_retention_truth_until_permanent_reclaim(
    database, actor, monkeypatch
):
    case, pending, operations = await acknowledged_running_end(
        database, actor, monkeypatch, permanent=False, physical_cleanup=True
    )
    assert await operations.end_thread_flow(
        case.thread_id, pending, permanent=False, force=False
    ) == {"status": "ended"}
    retained = await database.get_thread(case.thread_id)
    retained_metadata = metadata(retained)
    assert retained_metadata["workspace_container"]["volume_reclaimed"] is False
    assert (
        retained_metadata["_stateless_workspace_retirement_settled"]["permanent"]
        is False
    )
    assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid
    assert await database.fetchval(
        "SELECT EXISTS(SELECT 1 FROM managed_repository_process_zero_receipts "
        "WHERE owner_kind='thread' AND owner_id=$1::uuid "
        "AND scope='workspace_container' AND provisioner='k8s' "
        "AND runtime_incarnation=$2)",
        case.thread_id,
        case.pod_uid,
    )

    async def refuse_reclaim(*args, **kwargs):
        return None

    with monkeypatch.context() as patch:
        patch.setattr(
            case.provisioner, "reconcile_workspace_cleanup_intent", refuse_reclaim
        )
        with pytest.raises(HTTPException) as exc:
            await operations.end_thread_flow(
                case.thread_id, retained, permanent=True, force=False
            )
    assert exc.value.status_code == 503
    retry = await database.get_thread(case.thread_id)
    retry_metadata = metadata(retry)
    assert (
        retry_metadata["_stateless_workspace_retirement_settled"]["permanent"] is True
    )
    assert retry_metadata["workspace_container"]["volume_reclaimed"] is False
    assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid

    assert await operations.end_thread_flow(
        case.thread_id, retry, permanent=True, force=False
    ) == {"status": "deleted"}
    assert await database.get_thread(case.thread_id) is None
    assert case.cluster.objects == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift",
    [
        "token",
        "queue",
        "queue_owner",
        "queue_state",
        "generation",
        "runtime",
        "permanent",
    ],
)
async def test_old_settlement_candidate_cannot_change_current_end(
    database, actor, monkeypatch, drift
):
    case, pending, operations = await acknowledged_running_end(
        database, actor, monkeypatch, permanent=True, physical_cleanup=False
    )
    stale = deepcopy(pending)
    stale["metadata"] = deepcopy(metadata(pending))
    marker = stale["metadata"]["_stateless_claim_retirement"]
    if drift == "queue_owner":
        await database.execute(
            "UPDATE run_queue SET leased_by='successor-worker' WHERE unit_id=$1::uuid",
            case.thread_id,
        )
        assert await database.list_retryable_stateless_end_settlements() == []
    elif drift == "queue_state":
        await database.execute(
            "UPDATE run_queue SET state='queued' WHERE unit_id=$1::uuid",
            case.thread_id,
        )
        assert await database.list_retryable_stateless_end_settlements() == []
    elif drift == "queue":
        await database.execute(
            "UPDATE run_queue SET lease_token=lease_token+1 WHERE unit_id=$1::uuid",
            case.thread_id,
        )
        assert await database.list_retryable_stateless_end_settlements() == []
    elif drift == "generation":
        stale["runtime_generation"] = uuid4()
    elif drift == "permanent":
        marker["permanent"] = False
    else:
        key = "terminal_token" if drift == "token" else "runtime_incarnation"
        value = marker[key] + 1 if drift == "token" else str(uuid4())
        marker[key] = value
        for ack in (
            "_stateless_shell_retirement_ack",
            "_stateless_resident_retirement_ack",
        ):
            stale["metadata"][ack][key] = value
        if drift == "runtime":
            stale["metadata"]["workspace_container"]["_runtime_incarnation"] = value
    assert not await detector.retry_stateless_end_settlement(
        stale,
        dependencies=SimpleNamespace(
            store=database, thread_retirement_operations=lambda: operations
        ),
    )
    assert await database.get_thread(case.thread_id) == pending
    assert case.cluster.objects["pod"].metadata.uid == case.pod_uid
    assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid


@pytest.mark.asyncio
async def test_duplicate_settlement_and_old_candidate_after_resume_preserve_successor(
    database, actor, monkeypatch
):
    from orchestrator.services.session_provisioner import ensure_session_workspace

    case, pending, operations = await acknowledged_running_end(
        database, actor, monkeypatch, permanent=False
    )
    dependencies = SimpleNamespace(
        store=database, thread_retirement_operations=lambda: operations
    )
    assert await detector.retry_stateless_end_settlement(
        pending, dependencies=dependencies
    )
    assert await detector.retry_stateless_end_settlement(
        pending, dependencies=dependencies
    )
    assert await initial.resume_case(database, case, actor)
    await ensure_session_workspace(
        case.thread_id,
        db=database,
        provisioner=case.provisioner,
        suspension=case.suspension,
    )
    successor = await database.get_thread(case.thread_id)
    pod_uid = case.cluster.objects["pod"].metadata.uid
    assert pod_uid != case.pod_uid
    assert not await detector.retry_stateless_end_settlement(
        pending, dependencies=dependencies
    )
    assert await database.get_thread(case.thread_id) == successor
    assert case.cluster.objects["pod"].metadata.uid == pod_uid
    assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_proof", ["ack", "receipt", "unknown_runtime"])
async def test_settlement_does_not_infer_proof_from_ended_and_absent(
    database, actor, monkeypatch, missing_proof
):
    case, pending, operations = await acknowledged_running_end(
        database,
        actor,
        monkeypatch,
        permanent=True,
        physical_cleanup=missing_proof != "receipt",
    )
    if missing_proof == "ack":
        await database.execute(
            "UPDATE threads SET metadata=metadata-'_stateless_shell_retirement_ack' "
            "WHERE id=$1::uuid",
            case.thread_id,
        )
    elif missing_proof == "receipt":
        # Resident retirement already wrote an authenticated generic receipt.
        # Remove every exact receipt in this disposable fixture: omitting only
        # the later finalizer receipt still leaves valid replay authority.
        await database.execute(
            "DELETE FROM managed_repository_process_zero_receipts "
            "WHERE owner_kind='thread' AND owner_id=$1::uuid "
            "AND runtime_incarnation=$2",
            case.thread_id,
            case.pod_uid,
        )
        case.cluster.objects.pop("pod")
        assert not await database.fetchval(
            "SELECT EXISTS(SELECT 1 FROM managed_repository_process_zero_receipts "
            "WHERE owner_kind='thread' AND owner_id=$1::uuid)",
            case.thread_id,
        )
    else:
        monkeypatch.setattr(
            case.provisioner,
            "workspace_pod_authority",
            AsyncMock(return_value="unknown"),
        )
    before = await database.get_thread(case.thread_id)
    assert not await detector.retry_stateless_end_settlement(
        pending,
        dependencies=SimpleNamespace(
            store=database, thread_retirement_operations=lambda: operations
        ),
    )
    assert await database.get_thread(case.thread_id) == before


@pytest.mark.asyncio
async def test_settlement_selector_pages_exact_authority_and_excludes_unacknowledged(
    database, actor, monkeypatch
):
    case, pending, _ = await acknowledged_running_end(
        database, actor, monkeypatch, permanent=False
    )
    rows = await database.list_retryable_stateless_end_settlements(limit=1)
    assert [str(row["id"]) for row in rows] == [case.thread_id]
    assert (
        await database.list_retryable_stateless_end_settlements(
            limit=1, after=(pending["ended_at"], case.thread_id)
        )
        == []
    )
    assert await database.list_retryable_initial_creation_retirements() == []
    await database.execute(
        "UPDATE threads SET metadata=jsonb_set(metadata, "
        "'{_stateless_claim_retirement,remote_retired}','false'::jsonb) "
        "WHERE id=$1::uuid",
        case.thread_id,
    )
    assert await database.list_retryable_stateless_end_settlements() == []


@pytest.mark.asyncio
async def test_disconnected_permanent_upgrade_is_rediscovered_and_deletes_owner(
    database, actor, monkeypatch
):
    case, pending, operations = await acknowledged_running_end(
        database, actor, monkeypatch, permanent=False
    )
    dependencies = SimpleNamespace(
        store=database, thread_retirement_operations=lambda: operations
    )
    assert await detector.retry_stateless_end_settlement(
        pending, dependencies=dependencies
    )
    begin = type(database).begin_stateless_thread_workspace_retirement

    async def disconnect_after_upgrade(store, *args, **kwargs):
        assert (await begin(store, *args, **kwargs))["state"] == "settled"
        raise asyncio.CancelledError()

    with monkeypatch.context() as patch:
        patch.setattr(
            type(database),
            "begin_stateless_thread_workspace_retirement",
            disconnect_after_upgrade,
        )
        with pytest.raises(asyncio.CancelledError):
            await operations.end_thread_flow(
                case.thread_id,
                await database.get_thread(case.thread_id),
                permanent=True,
                force=False,
            )
    candidates = await database.list_retryable_stateless_end_settlements()
    assert len(candidates) == 1
    assert (
        metadata(candidates[0])["_stateless_workspace_retirement_settled"]["permanent"]
        is True
    )
    assert case.cluster.objects["pvc"].metadata.uid == case.pvc_uid
    assert await detector.retry_stateless_end_settlement(
        candidates[0], dependencies=dependencies
    )
    assert await database.get_thread(case.thread_id) is None
    assert case.cluster.objects == {}
