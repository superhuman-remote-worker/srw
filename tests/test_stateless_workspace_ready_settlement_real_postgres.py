"""Ready publication cannot lose the exact creation reservation settlement."""

from uuid import uuid4

import pytest

from orchestrator.services.manifest_execution_snapshot import read_execution
from orchestrator.services.session_provisioner import ensure_session_workspace
from orchestrator.services.workspace_lifecycle import EnsureOutcome
from tests import test_stateless_workspace_continuation_real_postgres as continuation

actor = continuation.actor
database = continuation.database
postgres_url = continuation.postgres_url


class LostReadyReply(BaseException):
    """Simulate process loss after the Ready transaction commits."""


@pytest.mark.asyncio
async def test_lost_ready_publication_reply_does_not_strand_open_creation(
    database, actor, monkeypatch
):
    case = await continuation.workspace_attempt(
        database, actor, monkeypatch, first_wait="not_ready"
    )
    case.cluster.become_ready()
    complete = type(database).complete_stateless_thread_workspace_creation

    async def lose_reply(self, thread_id, **kwargs):
        result = await complete(self, thread_id, **kwargs)
        if thread_id == case.thread_id and result is not None:
            raise LostReadyReply
        return result

    with monkeypatch.context() as interruption:
        interruption.setattr(
            type(database), "complete_stateless_thread_workspace_creation", lose_reply
        )
        with pytest.raises(LostReadyReply):
            await ensure_session_workspace(
                case.thread_id,
                db=database,
                provisioner=case.provisioner,
                suspension=case.suspension,
            )

    current = await database.get_thread(case.thread_id)
    workspace = continuation.metadata(current)["workspace_container"]
    assert workspace["status"] == "ready"
    assert "_runtime_creation" not in workspace
    assert workspace["_runtime_incarnation"] == case.pod_uid
    replay = await ensure_session_workspace(
        case.thread_id,
        db=database,
        provisioner=case.provisioner,
        suspension=case.suspension,
    )
    assert replay.outcome == EnsureOutcome.READY
    settled = await continuation.reservation(database, case.thread_id)
    assert settled["phase"] == "settled"
    assert settled["settled_at"] is not None
    for key in (
        "id",
        "reservation_generation",
        "thread_runtime_generation",
        "claimed_by",
        "claim_token",
        "desired_manifest_digest",
        "operation_kind",
        "runtime_incarnation",
        "pod_uid",
        "pvc_uid",
        "service_uid",
    ):
        assert settled[key] == case.creation[key]
    assert case.cluster.pod_create_calls == 1
    assert case.cluster.pod_deletes == 0
    assert (
        await read_execution(database, "Session", case.thread_id)
        == case.original_snapshot
    )


@pytest.mark.asyncio
async def test_initial_ready_lost_commit_response_replays_without_new_runtime(
    database, actor, monkeypatch
):
    complete = type(database).complete_stateless_thread_workspace_creation
    committed = []

    async def lose_response(self, thread_id, **kwargs):
        result = await complete(self, thread_id, **kwargs)
        if result is not None:
            committed.append(thread_id)
            raise ConnectionError("lost committed Ready response")
        return result

    with monkeypatch.context() as interruption:
        interruption.setattr(
            type(database),
            "complete_stateless_thread_workspace_creation",
            lose_response,
        )
        case = await continuation.workspace_attempt(
            database, actor, monkeypatch, first_wait="ready"
        )
    assert committed == [case.thread_id]
    before_replay = await continuation.reservation(database, case.thread_id)
    replay = await ensure_session_workspace(
        case.thread_id,
        db=database,
        provisioner=case.provisioner,
        suspension=case.suspension,
    )
    assert replay.outcome == EnsureOutcome.READY
    assert await continuation.reservation(database, case.thread_id) == before_replay
    assert case.cluster.pod_create_calls == 1
    assert case.cluster.pod_deletes == 0
    assert (
        "error"
        not in continuation.metadata(await database.get_thread(case.thread_id))[
            "workspace_container"
        ]
    )


@pytest.mark.asyncio
async def test_fault_after_ready_write_rolls_back_marker_and_reservation_together(
    database, actor, monkeypatch, caplog
):
    case = await continuation.workspace_attempt(
        database, actor, monkeypatch, first_wait="not_ready"
    )
    case.cluster.become_ready()
    # This failing trigger leaves every production authority trigger enabled.
    # Its query sees the Ready UPDATE inside the real transaction, then aborts
    # the subsequent reservation write. No test seeds a Ready projection.
    await database.execute(
        """
        CREATE FUNCTION fail_test_ready_settlement() RETURNS trigger AS $$
        BEGIN
            IF NEW.phase = 'settled' THEN
                IF EXISTS (SELECT 1 FROM threads WHERE id = NEW.owner_id
                    AND metadata->'workspace_container'->>'status' = 'ready'
                    AND NOT (metadata->'workspace_container' ? '_runtime_creation')) THEN
                    RAISE EXCEPTION 'test settlement fault after Ready write';
                END IF;
                RAISE EXCEPTION 'test fault did not reach Ready write';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        CREATE TRIGGER fail_test_ready_settlement
        BEFORE UPDATE ON managed_repository_workspace_creation_reservations
        FOR EACH ROW EXECUTE FUNCTION fail_test_ready_settlement();
        """
    )
    try:
        assert not await case.provisioner.continue_stateless_workspace_creation(
            case.owner,
            generation=str(case.before["runtime_generation"]),
            expected_runtime_incarnation=case.pod_uid,
        )
        assert "test settlement fault after Ready write" in caplog.text
        current = await database.get_thread(case.thread_id)
        assert continuation.metadata(current) == continuation.metadata(case.before)
        open_creation = await continuation.reservation(database, case.thread_id)
        assert open_creation["phase"] == "runtime_bound"
        assert open_creation["settled_at"] is None
        assert open_creation["id"] == case.creation["id"]
        assert open_creation["claim_token"] == case.creation["claim_token"]
    finally:
        await database.execute(
            "DROP TRIGGER fail_test_ready_settlement ON "
            "managed_repository_workspace_creation_reservations; "
            "DROP FUNCTION fail_test_ready_settlement()"
        )
    assert await case.provisioner.continue_stateless_workspace_creation(
        case.owner,
        generation=str(case.before["runtime_generation"]),
        expected_runtime_incarnation=case.pod_uid,
    )
    replay = await ensure_session_workspace(
        case.thread_id,
        db=database,
        provisioner=case.provisioner,
        suspension=case.suspension,
    )
    assert replay.outcome == EnsureOutcome.READY
    assert (await continuation.reservation(database, case.thread_id))[
        "phase"
    ] == "settled"
    assert case.cluster.pod_create_calls == 1
    assert case.cluster.pod_deletes == 0
    assert (
        await read_execution(database, "Session", case.thread_id)
        == case.original_snapshot
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("flow", ("initial", "delayed"))
async def test_receipt_verification_refusal_cannot_undo_committed_ready(
    database, actor, monkeypatch, flow
):
    read = type(database).get_current_managed_repository_workspace_creation_result
    refused = []

    async def mismatched_receipt(self, thread_id, **kwargs):
        result = await read(self, thread_id, **kwargs)
        if result is not None and result["phase"] == "settled":
            refused.append(thread_id)
            return {**result, "desired_manifest_digest": "0" * 64}
        return result

    with monkeypatch.context() as refusal:
        refusal.setattr(
            type(database),
            "get_current_managed_repository_workspace_creation_result",
            mismatched_receipt,
        )
        case = await continuation.workspace_attempt(
            database,
            actor,
            monkeypatch,
            first_wait="ready" if flow == "initial" else "not_ready",
            expected_first_outcome=(
                EnsureOutcome.FAILED if flow == "initial" else None
            ),
        )
        if flow == "delayed":
            case.cluster.become_ready()
            assert not await case.provisioner.continue_stateless_workspace_creation(
                case.owner,
                generation=str(case.before["runtime_generation"]),
                expected_runtime_incarnation=case.pod_uid,
            )
    assert refused == [case.thread_id]
    before_replay = await continuation.reservation(database, case.thread_id)
    assert before_replay["phase"] == "settled"
    replay = await ensure_session_workspace(
        case.thread_id,
        db=database,
        provisioner=case.provisioner,
        suspension=case.suspension,
    )
    assert replay.outcome == EnsureOutcome.READY
    assert await continuation.reservation(database, case.thread_id) == before_replay
    workspace = continuation.metadata(await database.get_thread(case.thread_id))[
        "workspace_container"
    ]
    assert workspace["status"] == "ready"
    assert "_runtime_creation" not in workspace
    assert "error" not in workspace
    assert case.cluster.pod_create_calls == 1
    assert case.cluster.pod_deletes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "refusal",
    (
        "generation",
        "runtime",
        "reservation",
        "claim",
        "expired",
        "cancelled",
        "settled",
    ),
)
async def test_ready_transaction_refuses_inexact_or_closed_creation(
    database, actor, monkeypatch, refusal
):
    case = await continuation.workspace_attempt(
        database, actor, monkeypatch, first_wait="not_ready"
    )
    case.cluster.become_ready()
    captured = []

    async def capture_ready(self, thread_id, **kwargs):
        assert thread_id == case.thread_id
        captured.append(kwargs)
        return None

    # Obtain the exact production helper inputs after real identity/readiness
    # checks, without publishing Ready or changing production trigger behavior.
    with monkeypatch.context() as capture:
        capture.setattr(
            type(database),
            "complete_stateless_thread_workspace_creation",
            capture_ready,
        )
        assert not await case.provisioner.continue_stateless_workspace_creation(
            case.owner,
            generation=str(case.before["runtime_generation"]),
            expected_runtime_incarnation=case.pod_uid,
        )
    assert len(captured) == 1
    kwargs = captured[0]
    if refusal in {"generation", "runtime", "reservation", "claim"}:
        field = {
            "generation": "generation",
            "runtime": "runtime_incarnation",
            "reservation": "creation_reservation_id",
            "claim": "creation_claim_token",
        }[refusal]
        kwargs[field] = kwargs[field] + 1 if refusal == "claim" else str(uuid4())
    elif refusal == "expired":
        await database.execute(
            "UPDATE managed_repository_workspace_creation_reservations "
            "SET created_at=now()-interval '1 hour', expires_at=now()-interval '1 second' "
            "WHERE id=$1",
            case.creation["id"],
        )
    elif refusal == "cancelled":
        assert (
            await database.request_managed_repository_workspace_creation_cancellation(
                case.thread_id,
                owner_kind="thread",
                scope="workspace_container",
                target_disposition="deleted",
                reclaim_shared_resources=False,
                claimant="ready-settlement-test-cancellation",
            )
        )
    else:
        # Existing generic settlement can produce a historical non-Ready row.
        # It must never become authority to publish Ready through this helper.
        assert await database.settle_managed_repository_workspace_creation_reservation(
            case.thread_id,
            owner_kind="thread",
            scope="workspace_container",
            reservation_generation=case.creation["reservation_generation"],
            claimant=case.creation["claimed_by"],
            claim_token=case.creation["claim_token"],
            runtime_incarnation=case.pod_uid,
        )
    before = await database.get_thread(case.thread_id)
    creation_before = await continuation.reservation(database, case.thread_id)
    assert (
        await database.complete_stateless_thread_workspace_creation(
            case.thread_id, **kwargs
        )
        is None
    )
    assert continuation.metadata(
        await database.get_thread(case.thread_id)
    ) == continuation.metadata(before)
    assert await continuation.reservation(database, case.thread_id) == creation_before
    assert case.cluster.pod_create_calls == 1
    assert case.cluster.pod_deletes == 0
