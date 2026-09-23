"""Focused control-plane regressions for stateless worker admission/verbs."""

from orchestrator.services import job_dispatcher
from orchestrator.services.workspace_lifecycle import EnsureOutcome
from tests import _b09_control_seams as control_seams

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException

from orchestrator.database.postgres import PostgresDB


JOB_ID = "11111111-1111-1111-1111-111111111111"
PARENT_ID = "22222222-2222-2222-2222-222222222222"


class _AsyncCM:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False


@asynccontextmanager
async def _owned_cleanup_lock(_job_id):
    yield True


def _db_with_conn(conn):
    db = PostgresDB.__new__(PostgresDB)

    @asynccontextmanager
    async def acquire():
        yield conn

    db.acquire = acquire
    db.delete_checkpoint_thread = AsyncMock(return_value=0)
    return db


def test_omitted_lane_defaults_stateless_when_fully_capable(monkeypatch):
    from orchestrator import main

    monkeypatch.setattr(main, "STATELESS_WORKER_ENABLED", True)
    monkeypatch.setattr(main.container_provisioner, "_k8s_available", True)
    monkeypatch.setattr(main.container_provisioner, "_in_cluster", True)

    assert (
        main._resolve_requested_job_execution_lane(
            None,
            default_stateless=True,
            needs_vm=False,
            needs_sandbox=True,
        )
        == "stateless"
    )


@pytest.mark.parametrize("requested_lane", [None, "stateless", "pinned"])
def test_external_vm_jobs_stay_pinned_across_lane_arms(monkeypatch, requested_lane):
    from orchestrator import main

    monkeypatch.setenv("VM_MODE", "external")
    monkeypatch.setattr(main, "STATELESS_WORKER_ENABLED", True)
    monkeypatch.setattr(main.container_provisioner, "_k8s_available", True)
    monkeypatch.setattr(main.container_provisioner, "_in_cluster", True)

    assert (
        main._resolve_requested_job_execution_lane(
            requested_lane,
            default_stateless=True,
            needs_vm=True,
            needs_sandbox=False,
        )
        == "pinned"
    )


def test_same_cluster_vm_is_stateless_for_explicit_and_default_arms(monkeypatch):
    from orchestrator import main

    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setattr(main, "STATELESS_WORKER_ENABLED", True)
    # VM admission is backed by the authenticated VM controller, not the
    # container provisioner capability used by the sandbox arm.
    monkeypatch.setattr(main.container_provisioner, "_k8s_available", False)
    monkeypatch.setattr(main.container_provisioner, "_in_cluster", False)

    assert (
        main._resolve_requested_job_execution_lane(
            "stateless",
            default_stateless=True,
            needs_vm=True,
            needs_sandbox=False,
        )
        == "stateless"
    )
    assert (
        main._resolve_requested_job_execution_lane(
            None,
            default_stateless=True,
            needs_vm=True,
            needs_sandbox=False,
        )
        == "stateless"
    )
    assert (
        main._resolve_requested_job_execution_lane(
            "pinned",
            default_stateless=True,
            needs_vm=True,
            needs_sandbox=False,
        )
        == "pinned"
    )


def test_omitted_lane_default_falls_back_without_sandbox(monkeypatch):
    from orchestrator import main

    monkeypatch.setattr(main, "STATELESS_WORKER_ENABLED", True)
    monkeypatch.setattr(main.container_provisioner, "_k8s_available", True)
    monkeypatch.setattr(main.container_provisioner, "_in_cluster", True)

    assert (
        main._resolve_requested_job_execution_lane(
            None,
            default_stateless=True,
            needs_vm=False,
            needs_sandbox=False,
        )
        is None
    )


@pytest.mark.parametrize(
    ("available", "in_cluster"),
    [(False, True), (True, False)],
)
def test_omitted_lane_default_falls_back_without_in_cluster_provisioner(
    monkeypatch,
    available,
    in_cluster,
):
    from orchestrator import main

    monkeypatch.setattr(main, "STATELESS_WORKER_ENABLED", True)
    monkeypatch.setattr(main.container_provisioner, "_k8s_available", available)
    monkeypatch.setattr(main.container_provisioner, "_in_cluster", in_cluster)

    assert (
        main._resolve_requested_job_execution_lane(
            None,
            default_stateless=True,
            needs_vm=False,
            needs_sandbox=True,
        )
        is None
    )


def test_omitted_lane_default_is_subordinate_to_worker_admission(monkeypatch):
    from orchestrator import main

    monkeypatch.setattr(main, "STATELESS_WORKER_ENABLED", False)
    monkeypatch.setattr(main.container_provisioner, "_k8s_available", True)
    monkeypatch.setattr(main.container_provisioner, "_in_cluster", True)

    assert (
        main._resolve_requested_job_execution_lane(
            None,
            default_stateless=True,
            needs_vm=False,
            needs_sandbox=True,
        )
        is None
    )


def test_omitted_lane_stays_unchanged_when_default_is_off(monkeypatch):
    from orchestrator import main

    monkeypatch.setattr(main, "STATELESS_WORKER_ENABLED", True)
    monkeypatch.setattr(main.container_provisioner, "_k8s_available", True)
    monkeypatch.setattr(main.container_provisioner, "_in_cluster", True)

    assert (
        main._resolve_requested_job_execution_lane(
            None,
            default_stateless=False,
            needs_vm=False,
            needs_sandbox=True,
        )
        is None
    )


@pytest.mark.parametrize(
    (
        "admission_enabled",
        "available",
        "in_cluster",
        "needs_sandbox",
        "status_code",
        "detail",
    ),
    [
        (
            False,
            True,
            True,
            True,
            409,
            "Stateless worker admission is disabled",
        ),
        (
            True,
            False,
            True,
            True,
            503,
            "Stateless workers require an in-cluster Kubernetes workspace provisioner",
        ),
        (
            True,
            True,
            True,
            False,
            422,
            (
                "Stateless workers currently require a Kubernetes sandbox or "
                "same-cluster VM workspace"
            ),
        ),
    ],
)
def test_explicit_stateless_infeasibility_still_raises(
    monkeypatch,
    admission_enabled,
    available,
    in_cluster,
    needs_sandbox,
    status_code,
    detail,
):
    from orchestrator import main

    monkeypatch.setattr(main, "STATELESS_WORKER_ENABLED", admission_enabled)
    monkeypatch.setattr(main.container_provisioner, "_k8s_available", available)
    monkeypatch.setattr(main.container_provisioner, "_in_cluster", in_cluster)

    with pytest.raises(HTTPException) as exc:
        main._resolve_requested_job_execution_lane(
            "stateless",
            default_stateless=True,
            needs_vm=False,
            needs_sandbox=needs_sandbox,
        )
    assert exc.value.status_code == status_code
    assert exc.value.detail == detail


def test_explicit_lanes_are_unchanged_when_default_is_on(monkeypatch):
    from orchestrator import main

    monkeypatch.setattr(main, "STATELESS_WORKER_ENABLED", True)
    monkeypatch.setattr(main.container_provisioner, "_k8s_available", True)
    monkeypatch.setattr(main.container_provisioner, "_in_cluster", True)

    assert (
        main._resolve_requested_job_execution_lane(
            "pinned",
            default_stateless=True,
            needs_vm=False,
            needs_sandbox=False,
        )
        == "pinned"
    )
    assert (
        main._resolve_requested_job_execution_lane(
            "stateless",
            default_stateless=True,
            needs_vm=False,
            needs_sandbox=True,
        )
        == "stateless"
    )


@pytest.mark.asyncio
async def test_create_job_sql_inherits_omitted_parent_lane(monkeypatch):
    conn = AsyncMock()
    conn.transaction = MagicMock(return_value=_AsyncCM())
    monkeypatch.setattr(
        "orchestrator.services.manifest_execution_snapshot.capture_execution",
        AsyncMock(),
    )
    conn.fetchrow.return_value = {"id": UUID(JOB_ID), "execution_lane": "stateless"}
    db = _db_with_conn(conn)

    result = await db.create_job(description="child", parent_job_id=PARENT_ID)

    sql, *args = conn.fetchrow.await_args.args
    normalized = " ".join(sql.split())
    assert "SELECT parent.execution_lane FROM jobs parent" in normalized
    assert "FOR SHARE" in normalized
    assert "COALESCE(" in sql
    assert args[-2] is None
    assert isinstance(args[-1], UUID)
    assert result["execution_lane"] == "stateless"


@pytest.mark.asyncio
async def test_create_job_explicit_lane_is_bound_and_invalid_lane_rejected(monkeypatch):
    conn = AsyncMock()
    conn.transaction = MagicMock(return_value=_AsyncCM())
    monkeypatch.setattr(
        "orchestrator.services.manifest_execution_snapshot.capture_execution",
        AsyncMock(),
    )
    conn.fetchrow.return_value = {"id": UUID(JOB_ID), "execution_lane": "stateless"}
    db = _db_with_conn(conn)

    await db.create_job(description="root", execution_lane="stateless")
    assert conn.fetchrow.await_args.args[-2] == "stateless"
    assert isinstance(conn.fetchrow.await_args.args[-1], UUID)

    with pytest.raises(ValueError):
        await db.create_job(description="bad", execution_lane="future")


@pytest.mark.asyncio
async def test_leased_cancel_publishes_status_without_pruning_checkpoint():
    conn = MagicMock()
    conn.transaction.return_value = _AsyncCM()

    async def fetchrow(sql, *_args):
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT state FROM run_queue"):
            return {"state": "leased"}
        if normalized.startswith("SELECT id FROM jobs"):
            return {"id": UUID(JOB_ID)}
        if normalized.startswith("UPDATE run_queue"):
            return None
        if normalized.startswith("SELECT recovery_id FROM vm_workspace_recovery_jobs"):
            return None
        if normalized.startswith("UPDATE jobs"):
            return {"id": UUID(JOB_ID)}
        raise AssertionError(normalized)

    conn.fetchrow = AsyncMock(side_effect=fetchrow)
    conn.fetchval = AsyncMock(return_value=False)
    db = _db_with_conn(conn)

    assert await db.cancel_stateless_job(JOB_ID) == (True, False)
    db.delete_checkpoint_thread.assert_not_awaited()
    job_update = next(
        call
        for call in conn.fetchrow.await_args_list
        if call.args and "UPDATE jobs" in call.args[0]
    )
    assert "_stateless_cancel_cleanup_pending" in job_update.args[0]
    cancel_sql = " ".join(job_update.args[0].split())
    assert "status::text <> 'completed'" in cancel_sql
    assert "_stateless_delete_pending" in cancel_sql
    assert "_completion_control_claim" not in cancel_sql
    # A response-lost retry after cleanup cleared the marker must be able to
    # re-arm idempotent cleanup instead of waiting the full settle timeout.
    assert "status::text <> 'cancelled' OR NOT" in cancel_sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("queue_state", "queue_update_expected"),
    [("leased", False), ("queued", True)],
)
async def test_stateless_pause_closes_only_unowned_queue_rows(
    queue_state,
    queue_update_expected,
):
    conn = MagicMock()
    conn.transaction.return_value = _AsyncCM()

    async def fetchrow(sql, *_args):
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT state FROM run_queue"):
            return {"state": queue_state}
        if normalized.startswith("SELECT status::text AS status"):
            return {"status": "processing", "execution_lane": "stateless"}
        if normalized.startswith("UPDATE run_queue"):
            return {"state": "done"}
        if normalized.startswith("UPDATE jobs"):
            return {"id": UUID(JOB_ID)}
        raise AssertionError(normalized)

    conn.fetchrow = AsyncMock(side_effect=fetchrow)
    db = _db_with_conn(conn)

    assert await db.pause_stateless_job(JOB_ID)
    queue_updates = [
        call
        for call in conn.fetchrow.await_args_list
        if call.args and "UPDATE run_queue" in call.args[0]
    ]
    assert bool(queue_updates) is queue_update_expected


@pytest.mark.asyncio
async def test_cancel_finalizer_waits_for_lease_then_prunes_checkpoint():
    conn = MagicMock()
    conn.transaction.return_value = _AsyncCM()
    queue_states = iter(({"state": "leased"}, {"state": "done"}))

    async def fetchrow(sql, *_args):
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT state FROM run_queue"):
            return next(queue_states)
        if normalized.startswith("SELECT status::text AS status"):
            return {
                "status": "cancelled",
                "execution_lane": "stateless",
                "cleanup_pending": True,
            }
        raise AssertionError(normalized)

    conn.fetchrow = AsyncMock(side_effect=fetchrow)
    db = _db_with_conn(conn)

    assert await db.finalize_cancelled_stateless_job(JOB_ID) is False
    db.delete_checkpoint_thread.assert_not_awaited()
    assert await db.finalize_cancelled_stateless_job(JOB_ID) is True
    db.delete_checkpoint_thread.assert_awaited_once_with(JOB_ID, strict=True)


@pytest.mark.asyncio
async def test_cancel_cleanup_session_lock_is_single_owner_and_released(monkeypatch):
    from orchestrator.database import postgres as postgres_module

    conn = MagicMock()
    conn.fetchval = AsyncMock(side_effect=(True, True))
    conn.close = AsyncMock()
    conn.terminate = MagicMock()
    db = _db_with_conn(conn)
    db._connection_string = "postgresql://scratch/test"
    connect = AsyncMock(return_value=conn)
    monkeypatch.setattr(postgres_module.asyncpg, "connect", connect)

    async with db.stateless_cancel_cleanup_lock(JOB_ID) as owner:
        assert owner is True

    first_sql = " ".join(conn.fetchval.await_args_list[0].args[0].split())
    second_sql = " ".join(conn.fetchval.await_args_list[1].args[0].split())
    assert first_sql == "SELECT pg_try_advisory_lock($1)"
    assert second_sql == "SELECT pg_advisory_unlock($1)"
    assert (
        conn.fetchval.await_args_list[0].args[1]
        == conn.fetchval.await_args_list[1].args[1]
    )
    connect.assert_awaited_once()
    assert connect.await_args.kwargs["timeout"] == 5
    conn.close.assert_awaited_once_with(timeout=5)
    conn.terminate.assert_not_called()


@pytest.mark.asyncio
async def test_cancel_cleanup_marker_clears_only_after_queue_is_done():
    conn = MagicMock()
    conn.transaction.return_value = _AsyncCM()
    conn.fetchrow = AsyncMock(
        side_effect=[
            {"state": "done"},
            {"id": UUID(JOB_ID)},
        ]
    )
    db = _db_with_conn(conn)

    assert await db.complete_stateless_cancel_cleanup(JOB_ID)
    update_sql = " ".join(conn.fetchrow.await_args_list[1].args[0].split())
    assert "- '_stateless_cancel_cleanup_pending'" in update_sql


@pytest.mark.asyncio
async def test_cancel_cleanup_marker_stays_while_queue_is_live():
    conn = MagicMock()
    conn.transaction.return_value = _AsyncCM()
    conn.fetchrow = AsyncMock(return_value={"state": "leased"})
    db = _db_with_conn(conn)

    assert not await db.complete_stateless_cancel_cleanup(JOB_ID)
    assert conn.fetchrow.await_count == 1


@pytest.mark.asyncio
async def test_cancel_cleanup_finalizers_are_idempotent_after_marker_clear():
    conn = MagicMock()
    conn.transaction.return_value = _AsyncCM()
    cleanup_pending = True

    async def fetchrow(sql, *_args):
        nonlocal cleanup_pending
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT state FROM run_queue"):
            return {"state": "done"}
        if normalized.startswith("SELECT status::text AS status"):
            return {
                "status": "cancelled",
                "execution_lane": "stateless",
                "cleanup_pending": cleanup_pending,
            }
        if normalized.startswith("UPDATE jobs"):
            if cleanup_pending:
                cleanup_pending = False
                return {"id": UUID(JOB_ID)}
            return None
        raise AssertionError(normalized)

    conn.fetchrow = AsyncMock(side_effect=fetchrow)
    db = _db_with_conn(conn)

    assert await db.finalize_cancelled_stateless_job(JOB_ID)
    assert await db.complete_stateless_cancel_cleanup(JOB_ID)
    # A concurrent response-loss retry that reached the same lifecycle points
    # after the first owner cleared the marker must settle immediately.
    assert await db.finalize_cancelled_stateless_job(JOB_ID)
    assert await db.complete_stateless_cancel_cleanup(JOB_ID)
    db.delete_checkpoint_thread.assert_awaited_once_with(JOB_ID, strict=True)


@pytest.mark.asyncio
async def test_prepare_delete_fences_queue_before_job_and_strictly_prunes():
    conn = MagicMock()
    conn.transaction.return_value = _AsyncCM()
    calls = []

    async def fetchrow(sql, *_args):
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT unit_kind, state FROM run_queue"):
            calls.append("queue_lock")
            return {"unit_kind": "worker_batch", "state": "leased"}
        if normalized.startswith("UPDATE run_queue"):
            calls.append("queue_close")
            return {"state": "done", "lease_token": 8}
        if normalized.startswith("UPDATE jobs"):
            calls.append("job_cancel")
            return {"id": UUID(JOB_ID)}
        raise AssertionError(normalized)

    conn.fetchrow = AsyncMock(side_effect=fetchrow)
    db = _db_with_conn(conn)

    assert await db.prepare_stateless_job_for_delete(JOB_ID)
    assert calls == ["queue_lock", "queue_close", "job_cancel"]
    db.delete_checkpoint_thread.assert_awaited_once_with(JOB_ID, strict=True)


@pytest.mark.asyncio
async def test_prepare_delete_prune_failure_retains_fenced_job_for_retry():
    conn = MagicMock()
    conn.transaction.return_value = _AsyncCM()

    async def fetchrow(sql, *_args):
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT unit_kind, state FROM run_queue"):
            return {"unit_kind": "worker_batch", "state": "done"}
        if normalized.startswith("UPDATE run_queue"):
            return {"state": "done", "lease_token": 8}
        if normalized.startswith("UPDATE jobs"):
            return {"id": UUID(JOB_ID)}
        raise AssertionError(normalized)

    conn.fetchrow = AsyncMock(side_effect=fetchrow)
    db = _db_with_conn(conn)
    db.delete_checkpoint_thread.side_effect = RuntimeError("prune unavailable")

    with pytest.raises(RuntimeError, match="prune unavailable"):
        await db.prepare_stateless_job_for_delete(JOB_ID)

    assert any("UPDATE jobs" in call.args[0] for call in conn.fetchrow.await_args_list)


@pytest.mark.asyncio
async def test_final_prepared_delete_removes_queue_and_job_atomically():
    conn = MagicMock()
    conn.transaction.return_value = _AsyncCM()
    lock_order = []

    async def fetchrow(sql, *_args):
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT unit_kind, state FROM run_queue"):
            lock_order.append("queue")
            return {"unit_kind": "worker_batch", "state": "done"}
        if normalized.startswith("SELECT execution_lane"):
            lock_order.append("job")
            return {"execution_lane": "stateless", "delete_pending": True}
        if normalized.startswith("SELECT status, completion_outcome_kind,"):
            return {"status": "created", "completion_outcome_kind": None}
        raise AssertionError(normalized)

    conn.fetchrow = AsyncMock(side_effect=fetchrow)
    conn.fetchval = AsyncMock(return_value=None)

    async def execute(sql, *_args):
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT pg_advisory_xact_lock"):
            lock_order.append("docker_advisory")
            return "SELECT 1"
        if normalized.startswith("UPDATE docker_workspace_leases"):
            return "UPDATE 0"
        if normalized.startswith("UPDATE threads"):
            # 0206 subagent children are ended before the jobs DELETE so the
            # cascade never trips the pinned delete authority on a live child.
            assert "kind = 'subagent'" in normalized
            return "UPDATE 0"
        if normalized.startswith("DELETE FROM run_queue"):
            return "DELETE 1"
        if normalized.startswith("DELETE FROM jobs"):
            return "DELETE 1"
        raise AssertionError(normalized)

    conn.execute = AsyncMock(side_effect=execute)
    db = _db_with_conn(conn)

    assert await db.delete_job(JOB_ID, prepared_stateless=True)
    assert lock_order == ["queue", "docker_advisory", "job"]
    assert (
        conn.fetchrow.await_args_list[0]
        .args[0]
        .lstrip()
        .startswith("SELECT unit_kind, state")
    )
    deletes = [
        " ".join(call.args[0].split())
        for call in conn.execute.await_args_list
        if call.args and call.args[0].lstrip().startswith("DELETE")
    ]
    assert deletes[0].startswith("DELETE FROM run_queue")
    assert deletes[1].startswith("DELETE FROM jobs")


@pytest.mark.asyncio
async def test_final_prepared_delete_refuses_a_live_queue_row():
    conn = MagicMock()
    conn.transaction.return_value = _AsyncCM()
    conn.fetchrow = AsyncMock(
        return_value={"unit_kind": "worker_batch", "state": "leased"}
    )
    conn.execute = AsyncMock()
    db = _db_with_conn(conn)

    with pytest.raises(RuntimeError, match="closed queue tombstone"):
        await db.delete_job(JOB_ID, prepared_stateless=True)
    conn.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_stateless_resume_sql_rejects_durable_delete_intent():
    conn = AsyncMock()
    conn.fetchrow.return_value = None
    db = _db_with_conn(conn)

    row = await db._queue_job_for_resume_on_conn(
        conn,
        UUID(JOB_ID),
        {"queued_feedback": "continue"},
        void_completion_decision=True,
        stateless_only=True,
        expected_status="cancelled",
    )

    assert row is None
    sql = " ".join(conn.fetchrow.await_args.args[0].split())
    assert "_stateless_delete_pending" in sql
    assert "_stateless_cancel_cleanup_pending" in sql
    assert "execution_lane = 'stateless'" in sql


@pytest.mark.asyncio
async def test_resume_mutation_guard_names_shared_view_only_when_enabled():
    conn = AsyncMock()
    conn.fetchrow.return_value = None
    db = _db_with_conn(conn)

    await db._queue_job_for_resume_on_conn(
        conn,
        UUID(JOB_ID),
        None,
        void_completion_decision=True,
        completion_commands_enabled=True,
    )

    sql = " ".join(conn.fetchrow.await_args.args[0].split())
    assert "job_completion_sweep_exclusions" in sql
    assert "completion_route.job_id = jobs.id" in sql


@pytest.mark.asyncio
async def test_resume_owner_exemption_requires_live_exact_finalizer_term():
    conn = AsyncMock()
    conn.fetchrow.return_value = None
    db = _db_with_conn(conn)
    command_id = "22222222-2222-2222-2222-222222222222"

    await db._queue_job_for_resume_on_conn(
        conn,
        UUID(JOB_ID),
        None,
        void_completion_decision=True,
        completion_commands_enabled=True,
        completion_owner_command_id=command_id,
        completion_owner="finalizer:exact-term",
    )

    call = conn.fetchrow.await_args
    sql = " ".join(call.args[0].split())
    assert "owning.state = 'finalizing'" in sql
    assert "owning.finalizing_by" in sql
    assert "owning.lease_expires_at > now()" in sql
    assert UUID(command_id) in call.args
    assert "finalizer:exact-term" in call.args

    with pytest.raises(ValueError, match="must be supplied together"):
        await db._queue_job_for_resume_on_conn(
            conn,
            UUID(JOB_ID),
            None,
            void_completion_decision=True,
            completion_commands_enabled=True,
            completion_owner_command_id=command_id,
        )


@pytest.mark.asyncio
async def test_explicit_resume_unparks_then_updates_job_in_one_transaction():
    conn = MagicMock()
    conn.transaction.return_value = _AsyncCM()

    async def fetchrow(sql, *_args):
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT unit_kind, input_seq FROM run_queue"):
            return {"unit_kind": "worker_batch", "input_seq": 12}
        if normalized.startswith("WITH cur AS"):
            return {"old_state": "parked", "new_state": "parked"}
        if normalized.startswith("UPDATE run_queue SET attempts_since_completion"):
            return {"state": "parked"}
        if normalized.startswith("UPDATE jobs"):
            return {"id": UUID(JOB_ID), "priority": 5, "user_id": None}
        raise AssertionError(normalized)

    conn.fetchrow = AsyncMock(side_effect=fetchrow)
    conn.fetchval = AsyncMock(return_value=True)
    db = _db_with_conn(conn)

    assert await db.queue_stateless_job_for_resume(
        JOB_ID,
        {"queued_feedback": "continue"},
        priority=5,
    )
    enqueue_call = next(
        call
        for call in conn.fetchrow.await_args_list
        if call.args and "WITH cur AS" in call.args[0]
    )
    assert enqueue_call.args[6] == 13
    conn.fetchval.assert_awaited_once()
    assert "state = 'parked'" in conn.fetchval.await_args.args[0]
    assert any(
        "execution_lane = 'stateless'" in call.args[0]
        for call in conn.fetchrow.await_args_list
        if call.args
    )
    resume_update = next(
        call
        for call in conn.fetchrow.await_args_list
        if call.args and "UPDATE jobs" in call.args[0]
    )
    context_merge = json.loads(resume_update.args[2])
    assert context_merge["queued_feedback"] == "continue"
    assert context_merge["queued_feedback_delivery_id"]
    assert UUID(context_merge["worker_resume_id"])


@pytest.mark.asyncio
async def test_delegation_resume_keeps_waiting_status_cas_inside_queue_transaction():
    conn = MagicMock()
    conn.transaction.return_value = _AsyncCM()

    async def fetchrow(sql, *_args):
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT unit_kind, input_seq FROM run_queue"):
            return {"unit_kind": "worker_batch", "input_seq": 21}
        if normalized.startswith("WITH cur AS"):
            return {"old_state": "done", "new_state": "queued"}
        if normalized.startswith("UPDATE run_queue SET attempts_since_completion"):
            return {"state": "queued"}
        if normalized.startswith("UPDATE jobs"):
            return {"id": UUID(JOB_ID), "priority": 5, "user_id": None}
        raise AssertionError(normalized)

    conn.fetchrow = AsyncMock(side_effect=fetchrow)
    db = _db_with_conn(conn)

    assert await db.queue_stateless_job_for_resume(
        JOB_ID,
        {"delegation_results": [{"job_id": "child"}]},
        expected_status="waiting",
    )

    resume_update = next(
        call
        for call in conn.fetchrow.await_args_list
        if call.args and "UPDATE jobs" in call.args[0]
    )
    assert "AND status = $3" in resume_update.args[0]
    assert resume_update.args[3] == "waiting"
    context_merge = json.loads(resume_update.args[2])
    assert context_merge["delegation_results_delivery_id"]
    assert UUID(context_merge["worker_resume_id"])


@pytest.mark.asyncio
async def test_resume_during_old_terminal_lease_records_newer_wake_watermark():
    conn = MagicMock()
    conn.transaction.return_value = _AsyncCM()

    async def fetchrow(sql, *_args):
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT unit_kind, input_seq FROM run_queue"):
            return {"unit_kind": "worker_batch", "input_seq": 44}
        if normalized.startswith("WITH cur AS"):
            return {"old_state": "leased", "new_state": "leased"}
        if normalized.startswith("UPDATE run_queue SET attempts_since_completion"):
            return {"state": "leased"}
        if normalized.startswith("UPDATE jobs"):
            return {"id": UUID(JOB_ID), "priority": 0, "user_id": None}
        raise AssertionError(normalized)

    conn.fetchrow = AsyncMock(side_effect=fetchrow)
    db = _db_with_conn(conn)

    assert await db.queue_stateless_job_for_resume(
        JOB_ID,
        {"queued_feedback": "one more change"},
        expected_status="pending_review",
    )

    enqueue_call = next(
        call
        for call in conn.fetchrow.await_args_list
        if call.args and "WITH cur AS" in call.args[0]
    )
    assert enqueue_call.args[6] == 45
    assert enqueue_call.args[1] == UUID(JOB_ID)


@pytest.mark.asyncio
async def test_workspace_reprovision_resume_stamps_generation_without_runnable_unit():
    conn = MagicMock()
    conn.transaction.return_value = _AsyncCM()

    async def fetchrow(sql, *_args):
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT unit_kind, state FROM run_queue"):
            return {"unit_kind": "worker_batch", "state": "done", "input_seq": 9}
        if normalized.startswith("UPDATE run_queue SET state = 'done'"):
            return {"state": "done", "lease_token": 4}
        if normalized.startswith("UPDATE run_queue SET attempts_since_completion"):
            return {"state": "done"}
        if normalized.startswith("UPDATE jobs") and "last_' || $2::text" in sql:
            assert "execution_lane = 'stateless'" in normalized
            return {"id": UUID(JOB_ID)}
        if normalized.startswith("UPDATE jobs"):
            return {"id": UUID(JOB_ID), "priority": 0, "user_id": None}
        raise AssertionError(normalized)

    conn.fetchrow = AsyncMock(side_effect=fetchrow)
    db = _db_with_conn(conn)

    assert await db.prepare_stateless_job_for_workspace_resume(
        JOB_ID,
        "workspace_container",
        {"queued_feedback": "continue after rebuild"},
        expected_status="pending_review",
    )

    updates = [
        call
        for call in conn.fetchrow.await_args_list
        if call.args and "UPDATE jobs" in call.args[0]
    ]
    assert len(updates) == 2
    assert updates[0].args[2] == "workspace_container"
    assert updates[0].args[3] == "pending_review"
    merged = json.loads(updates[1].args[2])
    assert merged["queued_feedback"] == "continue after rebuild"
    assert UUID(merged["worker_resume_id"])
    assert UUID(merged["queued_feedback_delivery_id"])


@pytest.mark.asyncio
async def test_workspace_reprovision_command_guard_runs_before_context_shed():
    conn = MagicMock()
    conn.transaction.return_value = _AsyncCM()
    conn.fetchrow = AsyncMock(
        return_value={"unit_kind": "worker_batch", "state": "done", "input_seq": 9}
    )
    conn.fetchval = AsyncMock(side_effect=[UUID(JOB_ID), True])
    db = _db_with_conn(conn)

    assert not await db.prepare_stateless_job_for_workspace_resume(
        JOB_ID,
        "workspace_container",
        expected_status="pending_review",
        completion_commands_enabled=True,
    )

    assert conn.fetchval.await_count == 2
    assert "FOR UPDATE" in conn.fetchval.await_args_list[0].args[0]
    assert "job_completion_sweep_exclusions" in conn.fetchval.await_args_list[1].args[0]
    assert not any(
        call.args and "last_' || $2::text" in call.args[0]
        for call in conn.fetchrow.await_args_list
    )


@pytest.mark.asyncio
async def test_admission_rechecks_exact_k8s_ready_evidence_after_enqueue():
    conn = MagicMock()
    conn.transaction.return_value = _AsyncCM()

    async def fetchrow(sql, *_args):
        normalized = " ".join(sql.split())
        if normalized.startswith("WITH cur AS"):
            return {"old_state": None, "new_state": "queued"}
        if normalized.startswith("SELECT id FROM jobs"):
            assert "->>'status' = 'ready'" in normalized
            assert "->>'provisioner' = 'k8s'" in normalized
            assert "->>'pod_ip'" in normalized
            assert "_workspace_contract" in normalized
            assert "assigned_backend' = 'sandbox'" in normalized
            assert "workspace_backend" in normalized
            assert "context->'vm'->>'requested'" in normalized
            assert "inherits_parent_workspace" in normalized
            assert "parent.id = jobs.parent_job_id" in normalized
            assert "ssh_ready_source" in normalized
            assert "identity_authenticated" in normalized
            assert "active_pod_uid" in normalized
            assert "ssh_host_key_fingerprint" in normalized
            assert _args[1] is False
            return {"id": UUID(JOB_ID)}
        raise AssertionError(normalized)

    conn.fetchrow = AsyncMock(side_effect=fetchrow)
    db = _db_with_conn(conn)

    admitted, status = await db.admit_stateless_worker_job(
        JOB_ID, fair_key=None, priority=5
    )
    assert admitted is True
    assert status == "inserted"


@pytest.mark.asyncio
async def test_admission_vm_arm_is_explicitly_topology_capability_bound():
    conn = MagicMock()
    conn.transaction.return_value = _AsyncCM()

    async def fetchrow(sql, *_args):
        normalized = " ".join(sql.split())
        if normalized.startswith("WITH cur AS"):
            return {"old_state": None, "new_state": "queued"}
        if normalized.startswith("SELECT id FROM jobs"):
            assert "assigned_backend' = 'vm'" in normalized
            assert "identity_provision_generation" in normalized
            assert "provision_generation" in normalized
            assert _args[1] is True
            return {"id": UUID(JOB_ID)}
        raise AssertionError(normalized)

    conn.fetchrow = AsyncMock(side_effect=fetchrow)
    db = _db_with_conn(conn)

    admitted, status = await db.admit_stateless_worker_job(
        JOB_ID,
        fair_key=None,
        priority=5,
        allow_vm_workspace=True,
    )

    assert admitted is True
    assert status == "inserted"


@pytest.mark.asyncio
async def test_stateless_dispatch_refusal_cannot_overwrite_winning_control(
    monkeypatch,
):
    from orchestrator import main

    job = {
        "id": JOB_ID,
        "status": "created",
        "execution_lane": "stateless",
        "assigned_agent_id": None,
        "context": {},
        "config_override": {"workspace": {"backend": "virtual"}},
        "priority": 0,
        "user_id": None,
    }
    monkeypatch.setattr(main, "AUTO_ASSIGN_ENABLED", False)
    monkeypatch.setattr(main, "STATELESS_WORKER_ENABLED", True)
    monkeypatch.setattr(main.container_provisioner, "_k8s_available", True)
    monkeypatch.setattr(main.container_provisioner, "_in_cluster", True)
    monkeypatch.setattr(
        main.postgres_db,
        "get_admittable_stateless_jobs",
        AsyncMock(return_value=[job]),
    )
    update = AsyncMock(return_value=False)
    monkeypatch.setattr(main.postgres_db, "update_job_status", update)

    await job_dispatcher.dispatch_pending_jobs(
        dependencies=main._job_dispatch_dependencies()
    )

    update.assert_awaited_once_with(
        JOB_ID,
        status="failed",
        error_message=(
            "Stateless workers currently require a Kubernetes sandbox or "
            "same-cluster VM workspace"
        ),
        expected_status="created",
    )


@pytest.mark.asyncio
async def test_stateless_workspace_failure_uses_scanned_status_cas(monkeypatch):
    from orchestrator import main

    job = {
        "id": JOB_ID,
        "status": "created",
        "execution_lane": "stateless",
        "assigned_agent_id": None,
        "context": {
            "workspace_container": {
                "status": "failed",
                "error": "image pull failed",
                "provisioner": "k8s",
            }
        },
        "config_override": {"workspace": {"backend": "sandbox"}},
        "priority": 0,
        "user_id": None,
        "parent_job_id": None,
    }
    monkeypatch.setattr(main, "AUTO_ASSIGN_ENABLED", False)
    monkeypatch.setattr(main, "STATELESS_WORKER_ENABLED", True)
    monkeypatch.setattr(main.container_provisioner, "_k8s_available", True)
    monkeypatch.setattr(main.container_provisioner, "_in_cluster", True)
    monkeypatch.setattr(
        main.postgres_db,
        "get_admittable_stateless_jobs",
        AsyncMock(return_value=[job]),
    )
    monkeypatch.setattr(
        job_dispatcher,
        "ensure_workspace",
        AsyncMock(
            return_value=SimpleNamespace(
                outcome=EnsureOutcome.FAILED,
                status="failed",
            )
        ),
    )
    update = AsyncMock(return_value=False)
    monkeypatch.setattr(main.postgres_db, "update_job_status", update)

    await job_dispatcher.dispatch_pending_jobs(
        dependencies=main._job_dispatch_dependencies()
    )

    update.assert_awaited_once_with(
        JOB_ID,
        status="failed",
        error_message="Workspace container failed: image pull failed",
        expected_status="created",
    )


@pytest.mark.asyncio
async def test_dispatcher_waits_instead_of_failing_uidless_k8s_runtime(monkeypatch):
    from orchestrator import main

    job = {
        "id": JOB_ID,
        "status": "created",
        "execution_lane": "stateless",
        "assigned_agent_id": None,
        "context": {
            "workspace_container": {
                "status": "ready",
                "provisioner": "k8s",
                "pod_ip": "10.0.0.8",
                "port": 30022,
            }
        },
        "config_override": {"workspace": {"backend": "sandbox"}},
        "priority": 0,
        "user_id": None,
        "parent_job_id": None,
    }
    monkeypatch.setattr(main, "AUTO_ASSIGN_ENABLED", False)
    monkeypatch.setattr(main, "STATELESS_WORKER_ENABLED", True)
    monkeypatch.setattr(
        main.postgres_db,
        "get_admittable_stateless_jobs",
        AsyncMock(return_value=[job]),
    )
    prepare = AsyncMock(
        return_value=("wait", job, "kubernetes_attestation_unavailable")
    )
    monkeypatch.setattr(main, "_prepare_job_workspace_runtime", prepare)
    update = AsyncMock()
    monkeypatch.setattr(main.postgres_db, "update_job_status", update)
    provision = AsyncMock(side_effect=AssertionError("provisioning attempted"))
    monkeypatch.setattr(job_dispatcher, "ensure_workspace", provision)

    await job_dispatcher.dispatch_pending_jobs(
        dependencies=main._job_dispatch_dependencies()
    )

    prepare.assert_awaited_once_with(job)
    update.assert_not_awaited()
    provision.assert_not_awaited()


@pytest.mark.asyncio
async def test_vm_lane_repair_losing_status_cas_does_not_close_queue(monkeypatch):
    from orchestrator import main

    job = {
        "id": JOB_ID,
        "status": "paused",
        "execution_lane": "stateless",
        "context": {"vm": {"requested": True}},
        "config_override": {"workspace": {"backend": "vm"}},
    }
    conn = MagicMock()
    conn.transaction.return_value = _AsyncCM()

    async def fetchrow(sql, *_args):
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT state FROM run_queue"):
            return {"state": "queued"}
        if normalized.startswith("UPDATE jobs SET execution_lane = 'pinned'"):
            assert "status::text = $2::text" in normalized
            assert _args == (JOB_ID, "paused")
            return None
        raise AssertionError(normalized)

    conn.fetchrow = AsyncMock(side_effect=fetchrow)

    @asynccontextmanager
    async def acquire():
        yield conn

    monkeypatch.setattr(main, "AUTO_ASSIGN_ENABLED", False)
    monkeypatch.setattr(main, "STATELESS_WORKER_ENABLED", True)
    monkeypatch.setattr(
        main.postgres_db,
        "get_admittable_stateless_jobs",
        AsyncMock(return_value=[job]),
    )
    monkeypatch.setattr(main.postgres_db, "acquire", acquire)

    await job_dispatcher.dispatch_pending_jobs(
        dependencies=main._job_dispatch_dependencies()
    )

    conn.execute.assert_not_called()


@pytest.mark.asyncio
async def test_same_cluster_vm_dispatch_stays_stateless_and_reaches_admission(
    monkeypatch,
):
    from orchestrator import main

    generation = "33333333-3333-4333-8333-333333333333"
    launcher_uid = "44444444-4444-4444-8444-444444444444"
    job = {
        "id": JOB_ID,
        "status": "created",
        "execution_lane": "stateless",
        "assigned_agent_id": None,
        "priority": 3,
        "user_id": None,
        "config_override": {"workspace": {"backend": "vm"}},
        "context": {
            "_workspace_contract": {
                "version": 1,
                "requested_backend": "vm",
                "assigned_backend": "vm",
                "assignment_source": "test",
            },
            "vm": {
                "status": "ready",
                "provision_generation": generation,
                "identity_authenticated": True,
                "identity_provision_generation": generation,
                "active_pod_uid": launcher_uid,
                "ssh_host_key_fingerprint": "SHA256:" + ("A" * 43),
                "ssh_ready_source": "provisioner_probe",
                "ssh_host": "10.42.1.23",
                "pod_ip": "10.42.1.23",
                "ssh_port": 22,
            },
        },
    }
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setattr(main, "AUTO_ASSIGN_ENABLED", False)
    monkeypatch.setattr(main, "STATELESS_WORKER_ENABLED", True)
    monkeypatch.setattr(
        main.postgres_db,
        "get_admittable_stateless_jobs",
        AsyncMock(return_value=[job]),
    )
    monkeypatch.setattr(
        main,
        "_prepare_job_workspace_runtime",
        AsyncMock(return_value=("proceed", job, None)),
    )
    monkeypatch.setattr(main, "_check_vm_permission", AsyncMock())
    monkeypatch.setattr(
        job_dispatcher, "vm_provisioning_decision", MagicMock(return_value="ready")
    )
    monkeypatch.setattr(
        main,
        "_prepare_job_repository_before_claim",
        AsyncMock(return_value=True),
    )
    admitted = AsyncMock(return_value=(True, "inserted"))
    monkeypatch.setattr(main.postgres_db, "admit_stateless_worker_job", admitted)

    await job_dispatcher.dispatch_pending_jobs(
        dependencies=main._job_dispatch_dependencies()
    )

    admitted.assert_awaited_once_with(
        JOB_ID,
        fair_key=None,
        priority=3,
        allow_vm_workspace=True,
    )


@pytest.mark.asyncio
async def test_resume_retries_pinned_verb_after_vm_lane_repair(monkeypatch):
    monkeypatch.setattr(
        "orchestrator.services.vm_workspace_recovery_store.VMWorkspaceRecoveryStore.unresolved_participation",
        AsyncMock(return_value=None),
    )
    from orchestrator import main

    # This fixture exercises lane fallback with an unconnected mocked store.
    # Durable creation Resume has its own real-PostgreSQL authority tests.
    monkeypatch.setattr(main.postgres_db, "supports_vm_creation_retry", False)

    job = {
        "id": JOB_ID,
        "status": "paused",
        "execution_lane": "stateless",
        "priority": 4,
        "user_id": None,
        "context": {
            "workspace_container": {
                "status": "ready",
                "provisioner": "k8s",
                "pod_ip": "10.0.0.8",
            }
        },
        "config_override": {"workspace": {"backend": "sandbox"}},
    }
    pinned = {**job, "execution_lane": "pinned"}
    monkeypatch.setattr(
        main,
        "require_internal_or_job_access",
        AsyncMock(return_value=(None, job)),
    )
    monkeypatch.setattr(main, "_user_experts_enabled", AsyncMock(return_value=False))
    stateless_queue = AsyncMock(return_value=False)
    pinned_queue = AsyncMock(return_value=True)
    monkeypatch.setattr(
        main.postgres_db,
        "queue_stateless_job_for_resume",
        stateless_queue,
    )
    monkeypatch.setattr(main.postgres_db, "get_job", AsyncMock(return_value=pinned))
    monkeypatch.setattr(main.postgres_db, "queue_job_for_resume", pinned_queue)
    monkeypatch.setattr(main, "_trigger_dispatch", MagicMock())

    result = await control_seams.resume_job(
        MagicMock(), JOB_ID, main.JobResumeRequest()
    )

    assert result["status"] == "queued"
    stateless_queue.assert_awaited_once()
    pinned_queue.assert_awaited_once_with(
        JOB_ID,
        None,
        expected_status="paused",
        lift_operator_pause_hold="",
    )


@pytest.mark.asyncio
async def test_internal_resume_retries_pinned_verb_after_vm_lane_repair(monkeypatch):
    from orchestrator import main

    stateless = {
        "id": JOB_ID,
        "status": "waiting",
        "execution_lane": "stateless",
        "priority": 2,
        "user_id": None,
    }
    pinned = {**stateless, "execution_lane": "pinned"}
    monkeypatch.setattr(
        main.postgres_db,
        "get_job",
        AsyncMock(side_effect=(stateless, pinned)),
    )
    monkeypatch.setattr(
        main.postgres_db,
        "queue_stateless_job_for_resume",
        AsyncMock(return_value=False),
    )
    pinned_queue = AsyncMock(return_value=True)
    monkeypatch.setattr(main.postgres_db, "queue_job_for_resume", pinned_queue)
    monkeypatch.setattr(main, "_trigger_dispatch", MagicMock())

    await control_seams.internal_resume_job(
        JOB_ID,
        "continue",
        expected_status="waiting",
    )

    pinned_queue.assert_awaited_once_with(
        JOB_ID,
        {"queued_feedback": "continue"},
        expected_status="waiting",
        # OC-04 threads the route-generation token through every resume.
        # None here is the documented no-guard default: this verb is a lane
        # repair, not a route resolution, so it has no generation to prove.
        expected_route_id=None,
    )


@pytest.mark.asyncio
async def test_cancel_retries_pinned_verb_after_vm_lane_repair(monkeypatch):
    from orchestrator import main

    job = {
        "id": JOB_ID,
        "status": "created",
        "execution_lane": "stateless",
        "assigned_agent_id": None,
        "context": {},
    }
    pinned = {**job, "execution_lane": "pinned"}
    monkeypatch.setattr(
        main,
        "require_internal_or_job_access",
        AsyncMock(return_value=(None, job)),
    )
    monkeypatch.setattr(
        main.postgres_db,
        "cancel_stateless_job",
        AsyncMock(return_value=(False, False)),
    )
    monkeypatch.setattr(main.postgres_db, "get_job", AsyncMock(return_value=pinned))
    pinned_cancel = AsyncMock(return_value=True)
    monkeypatch.setattr(main.postgres_db, "cancel_job", pinned_cancel)
    linearize = AsyncMock(return_value=True)
    monkeypatch.setattr(main.postgres_db, "linearize_pinned_cancel", linearize)
    monkeypatch.setattr(main.postgres_db, "delete_checkpoint_thread", AsyncMock())
    monkeypatch.setattr(
        main.thread_retirement_operations, "archive_and_cleanup_workspace", AsyncMock()
    )
    monkeypatch.setattr(
        main.job_mutation_operations.JobControlOperations,
        "cascade_cancel_to_children",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        main.subjob_completion_operations,
        "handle_scholar_completion",
        AsyncMock(),
    )
    monkeypatch.setattr(main, "maybe_wake_session", AsyncMock())
    monkeypatch.setattr(main, "_kick_session_wake_drain", MagicMock())
    monkeypatch.setattr(main, "_trigger_dispatch", MagicMock())

    assert await control_seams.cancel_job(MagicMock(), JOB_ID) == {
        "status": "cancelled"
    }

    pinned_cancel.assert_awaited_once_with(JOB_ID)
    linearize.assert_awaited_once_with(
        JOB_ID,
        expected_status="created",
        completion_commands_enabled=main.COMPLETION_COMMANDS_ENABLED,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_pinned_cancel_linearizes_before_agent_post_and_prunes_after(
    monkeypatch,
    enabled,
):
    from orchestrator import main

    agent_id = "22222222-2222-2222-2222-222222222222"
    job = {
        "id": JOB_ID,
        "status": "processing",
        "execution_lane": "pinned",
        "assigned_agent_id": agent_id,
        "context": {},
    }
    order = []
    monkeypatch.setattr(main, "COMPLETION_COMMANDS_ENABLED", enabled)
    monkeypatch.setattr(
        main,
        "require_internal_or_job_access",
        AsyncMock(return_value=(None, job)),
    )
    linearize = AsyncMock(
        side_effect=lambda *_a, **_k: order.append("linearize") or True
    )
    monkeypatch.setattr(main.postgres_db, "linearize_pinned_cancel", linearize)
    monkeypatch.setattr(
        main.postgres_db,
        "get_agent",
        AsyncMock(
            return_value={
                "id": agent_id,
                "pod_ip": "127.0.0.1",
                "pod_port": 8000,
                "status": "working",
                "current_job_id": JOB_ID,
                "metadata": {
                    "dispatch_process_generation": (
                        "33333333-3333-4333-8333-333333333333"
                    )
                },
            }
        ),
    )
    ready_response = MagicMock(status_code=200)
    ready_response.json.return_value = {
        "ready": True,
        "capabilities": {"pinned_recipient_binding": True},
    }
    response = MagicMock(status_code=200)
    response.json.return_value = {"graceful": True}
    client = MagicMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    client.get = AsyncMock(return_value=ready_response)
    client.post = AsyncMock(
        side_effect=lambda *_a, **_k: order.append("post") or response
    )
    monkeypatch.setattr(main.httpx, "AsyncClient", MagicMock(return_value=client))
    monkeypatch.setattr(
        main.thread_retirement_operations, "archive_and_cleanup_workspace", AsyncMock()
    )
    monkeypatch.setattr(main.postgres_db, "cancel_job", AsyncMock(return_value=False))
    monkeypatch.setattr(
        main.postgres_db,
        "get_job",
        AsyncMock(return_value={**job, "status": "cancelled"}),
    )
    prune = AsyncMock(side_effect=lambda *_: order.append("prune"))
    monkeypatch.setattr(main.postgres_db, "delete_checkpoint_thread", prune)
    monkeypatch.setattr(
        main.job_mutation_operations.JobControlOperations,
        "cascade_cancel_to_children",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        main.subjob_completion_operations,
        "handle_scholar_completion",
        AsyncMock(),
    )
    monkeypatch.setattr(main, "maybe_wake_session", AsyncMock())
    monkeypatch.setattr(main, "_kick_session_wake_drain", MagicMock())
    monkeypatch.setattr(main, "_trigger_dispatch", MagicMock())

    assert await control_seams.cancel_job(MagicMock(), JOB_ID) == {
        "status": "cancelled"
    }

    assert order == ["linearize", "post", "prune"]


@pytest.mark.asyncio
async def test_flag_on_completed_pinned_cancel_stays_completed_without_cleanup(
    monkeypatch,
):
    from orchestrator import main

    job = {
        "id": JOB_ID,
        "status": "completed",
        "execution_lane": "pinned",
        "assigned_agent_id": None,
        "context": {},
    }
    monkeypatch.setattr(main, "COMPLETION_COMMANDS_ENABLED", True)
    monkeypatch.setattr(
        main,
        "require_internal_or_job_access",
        AsyncMock(return_value=(None, job)),
    )
    linearize = AsyncMock(return_value=False)
    monkeypatch.setattr(main.postgres_db, "linearize_pinned_cancel", linearize)
    monkeypatch.setattr(
        main.postgres_db,
        "get_job",
        AsyncMock(return_value={**job, "status": "completed"}),
    )
    cleanup = AsyncMock()
    monkeypatch.setattr(
        main.thread_retirement_operations.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        cleanup,
    )
    monkeypatch.setattr(main.postgres_db, "cancel_job", AsyncMock())
    monkeypatch.setattr(main.postgres_db, "delete_checkpoint_thread", AsyncMock())
    cascade = AsyncMock()
    monkeypatch.setattr(
        main.job_mutation_operations.JobControlOperations,
        "cascade_cancel_to_children",
        cascade,
    )

    with pytest.raises(HTTPException) as exc:
        await control_seams.cancel_job(MagicMock(), JOB_ID)

    assert exc.value.status_code == 400
    linearize.assert_awaited_once_with(
        JOB_ID,
        expected_status="completed",
        completion_commands_enabled=True,
    )
    cleanup.assert_not_awaited()
    main.postgres_db.cancel_job.assert_not_awaited()
    main.postgres_db.delete_checkpoint_thread.assert_not_awaited()
    cascade.assert_not_awaited()


@pytest.mark.asyncio
async def test_pinned_cancel_linearizer_has_terminal_status_parity_guard():
    conn = AsyncMock()
    conn.transaction = MagicMock(return_value=_AsyncCM())
    conn.fetchrow = AsyncMock(return_value={"id": UUID(JOB_ID)})
    conn.fetchval = AsyncMock(return_value=False)
    conn.execute.return_value = "UPDATE 0"
    db = _db_with_conn(conn)

    assert not await db.linearize_pinned_cancel(JOB_ID, expected_status="completed")

    sql = " ".join(conn.execute.await_args.args[0].split())
    assert "status::text = $2::text" in sql
    assert "status NOT IN ('completed', 'cancelled')" in sql
    assert "_completion_control_claim" not in sql
    assert conn.execute.await_args.args[2] == "completed"


@pytest.mark.asyncio
async def test_cancel_sql_names_control_marker_only_when_completion_flag_on():
    conn = MagicMock()
    conn.transaction.return_value = _AsyncCM()
    conn.execute = AsyncMock(return_value="UPDATE 0")

    async def fetchrow(sql, *_args):
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT state FROM run_queue"):
            return {"state": "queued"}
        if normalized.startswith("SELECT unit_id FROM run_queue"):
            return {"unit_id": UUID(JOB_ID)}
        if normalized.startswith("SELECT id FROM jobs"):
            return {"id": UUID(JOB_ID)}
        if normalized.startswith("UPDATE run_queue"):
            return {"state": "done"}
        if normalized.startswith("UPDATE jobs"):
            return None
        raise AssertionError(normalized)

    conn.fetchrow = AsyncMock(side_effect=fetchrow)
    conn.fetchval = AsyncMock(return_value=False)
    db = _db_with_conn(conn)

    assert not await db.linearize_pinned_cancel(
        JOB_ID,
        expected_status="processing",
        completion_commands_enabled=True,
    )
    pinned_sql = " ".join(conn.execute.await_args.args[0].split())
    assert "_completion_control_claim" in pinned_sql

    assert await db.cancel_stateless_job(JOB_ID, completion_commands_enabled=True) == (
        False,
        False,
    )
    stateless_sql = next(
        " ".join(call.args[0].split())
        for call in conn.fetchrow.await_args_list
        if call.args and "UPDATE jobs" in call.args[0]
    )
    assert "_completion_control_claim" in stateless_sql


@pytest.mark.asyncio
@pytest.mark.parametrize("lane", ["pinned", "stateless"])
async def test_flag_on_cancel_returns_exact_409_for_active_control_claim(
    monkeypatch, lane
):
    from orchestrator import main

    job = {
        "id": JOB_ID,
        "execution_lane": lane,
        "status": "processing",
        "assigned_agent_id": None,
        "context": {
            "_completion_control_claim": {
                "version": 1,
                "source": "public_pause",
                "expires_epoch": 4_102_444_800,
            }
        },
    }
    monkeypatch.setattr(main, "COMPLETION_COMMANDS_ENABLED", True)
    monkeypatch.setattr(
        main,
        "require_internal_or_job_access",
        AsyncMock(return_value=(None, job)),
    )
    monkeypatch.setattr(main.postgres_db, "get_job", AsyncMock(return_value=job))
    stateless_cancel = AsyncMock(return_value=(False, False))
    pinned_cancel = AsyncMock(return_value=False)
    monkeypatch.setattr(main.postgres_db, "cancel_stateless_job", stateless_cancel)
    monkeypatch.setattr(main.postgres_db, "linearize_pinned_cancel", pinned_cancel)
    cleanup = AsyncMock()
    monkeypatch.setattr(
        main.thread_retirement_operations, "archive_and_cleanup_workspace", cleanup
    )

    with pytest.raises(HTTPException) as exc:
        await control_seams.cancel_job(MagicMock(), JOB_ID)

    assert exc.value.status_code == 409
    assert exc.value.detail.startswith("job control is in progress")
    assert "source=public_pause" in exc.value.detail
    assert "expires_at=2100-01-01T00:00:00Z" in exc.value.detail
    cleanup.assert_not_awaited()
    if lane == "pinned":
        pinned_cancel.assert_awaited_once_with(
            JOB_ID,
            expected_status="processing",
            completion_commands_enabled=True,
        )
        stateless_cancel.assert_not_awaited()
    else:
        stateless_cancel.assert_awaited_once_with(
            JOB_ID, completion_commands_enabled=True
        )
        pinned_cancel.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_endpoint_closes_queued_stateless_unit_without_agent_post(
    monkeypatch,
):
    from orchestrator import main

    job = {
        "id": JOB_ID,
        "execution_lane": "stateless",
        "status": "created",
        "assigned_agent_id": None,
        "context": {},
    }
    monkeypatch.setattr(
        main,
        "require_internal_or_job_access",
        AsyncMock(return_value=(None, job)),
    )
    cancel = AsyncMock(return_value=(True, True))
    monkeypatch.setattr(main.postgres_db, "cancel_stateless_job", cancel)
    settle = AsyncMock(return_value=True)
    monkeypatch.setattr(
        main.job_mutation_operations.JobControlOperations,
        "wait_for_stateless_cancel_settle",
        settle,
    )
    monkeypatch.setattr(main.postgres_db, "get_agent", AsyncMock())
    cleanup = AsyncMock()
    monkeypatch.setattr(
        main.thread_retirement_operations, "archive_and_cleanup_workspace", cleanup
    )
    monkeypatch.setattr(
        main.job_mutation_operations.JobControlOperations,
        "cascade_cancel_to_children",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        main.subjob_completion_operations,
        "handle_scholar_completion",
        AsyncMock(),
    )
    monkeypatch.setattr(main, "maybe_wake_session", AsyncMock())
    monkeypatch.setattr(main, "_kick_session_wake_drain", MagicMock())
    monkeypatch.setattr(main, "_trigger_dispatch", MagicMock())

    assert await control_seams.cancel_job(MagicMock(), JOB_ID) == {
        "status": "cancelled"
    }

    cancel.assert_awaited_once_with(JOB_ID)
    settle.assert_awaited_once_with(JOB_ID)
    cleanup.assert_not_awaited()
    main.postgres_db.get_agent.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status_cas_wins", [True, False])
async def test_blocking_message_status_is_exact_worker_fenced(
    monkeypatch,
    status_cas_wins,
):
    from orchestrator import main
    from orchestrator.services import agent_messaging

    # OC-01: the stateless fence used to be inline SQL in send_agent_message.
    # It now lives inside create_routed_blocking_freeze, which commits the
    # message, the route and the freeze as one unit — and whose lane/lease
    # guards are exercised against real Postgres in the routing suite. What
    # this test still owns is the DELEGATION: the endpoint must hand the
    # helper the exact lane and lease, and must 409 rather than proceed when
    # the helper reports a lost guard.
    committed = AsyncMock(
        return_value=(
            {"route_id": str(uuid4()), "originating_message_id": str(uuid4())}
            if status_cas_wins
            else None
        )
    )

    @asynccontextmanager
    async def acquire():
        yield MagicMock()

    monkeypatch.setattr(
        main.postgres_db,
        "get_job",
        AsyncMock(
            return_value={
                "id": JOB_ID,
                "status": "processing",
                "execution_lane": "stateless",
                "user_id": "33333333-3333-3333-3333-333333333333",
                "description": "test job",
                "config_name": "worker_base",
                "project_id": None,
            }
        ),
    )
    monkeypatch.setattr(
        main.postgres_db,
        "get_user",
        AsyncMock(return_value={"email": "owner@example.com", "display_name": "Owner"}),
    )
    monkeypatch.setattr(
        main.postgres_db,
        "check_message_rate_limit",
        AsyncMock(return_value={"job_hourly": 0, "job_daily": 0, "user_daily": 0}),
    )
    monkeypatch.setattr(
        main.postgres_db,
        "reserve_message_delivery_intent",
        AsyncMock(
            return_value={
                "allowed": True,
                "intent_id": str(uuid4()),
                "accepted_at": None,
            }
        ),
    )
    monkeypatch.setattr(
        main.postgres_db,
        "begin_message_delivery_attempt",
        AsyncMock(return_value={"delivery_claimed": True, "attempt_number": 1}),
    )
    monkeypatch.setattr(
        main.postgres_db,
        "settle_message_delivery_attempt",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        main.postgres_db, "get_message_sequence", AsyncMock(return_value=1)
    )
    monkeypatch.setattr(main.postgres_db, "log_message", AsyncMock())
    monkeypatch.setattr(main.postgres_db, "set_message_email_id", AsyncMock())
    monkeypatch.setattr(main.postgres_db, "settle_outbound_message_log", AsyncMock())
    monkeypatch.setattr(main.postgres_db, "mark_route_user_delivery", AsyncMock())
    monkeypatch.setattr(main.postgres_db, "create_routed_blocking_freeze", committed)
    monkeypatch.setattr(main.postgres_db, "acquire", acquire)
    from orchestrator.services.notification_service import RecordResult

    monkeypatch.setattr(
        main.notification_service,
        "record_agent_message",
        AsyncMock(
            return_value=RecordResult(
                "n-1", True, {"in_app": True, "email": True, "email_message_id": "m-1"}
            )
        ),
    )
    body = main.MessageSendRequest(
        to="user",
        subject="Need input",
        message="Please answer",
        mode="blocking",
        lease_token=9,
    )

    # The send funnel moved to ``services.agent_messaging``; its collaborators
    # come from ``main._agent_messaging_dependencies()``, which binds
    # ``main.postgres_db`` and ``main.notification_service`` — the very objects
    # monkeypatched above — so those patches still steer the code under test.
    def _send():
        return agent_messaging.send_agent_message(
            MagicMock(),
            JOB_ID,
            body,
            dependencies=main._agent_messaging_dependencies(),
        )

    if status_cas_wins:
        result = await _send()
        assert result["status"] == "sent"
    else:
        with pytest.raises(HTTPException) as lost:
            await _send()
        assert lost.value.status_code == 409
        assert lost.value.detail == "Job changed before blocking message was committed"

    kwargs = committed.await_args.kwargs
    assert kwargs["expected_lane"] == "stateless"
    assert kwargs["lease_token"] == 9


@pytest.mark.asyncio
async def test_cancel_endpoint_waits_for_leased_owner_before_workspace_cleanup(
    monkeypatch,
):
    from orchestrator import main

    job = {
        "id": JOB_ID,
        "execution_lane": "stateless",
        "status": "processing",
        "assigned_agent_id": None,
        "context": {},
    }
    monkeypatch.setattr(
        main,
        "require_internal_or_job_access",
        AsyncMock(return_value=(None, job)),
    )
    monkeypatch.setattr(
        main.postgres_db,
        "cancel_stateless_job",
        AsyncMock(return_value=(True, False)),
    )
    settle = AsyncMock(return_value=True)
    monkeypatch.setattr(
        main.job_mutation_operations.JobControlOperations,
        "wait_for_stateless_cancel_settle",
        settle,
    )
    direct_cleanup = AsyncMock()
    monkeypatch.setattr(
        main.thread_retirement_operations,
        "archive_and_cleanup_workspace",
        direct_cleanup,
    )
    monkeypatch.setattr(
        main.job_mutation_operations.JobControlOperations,
        "cascade_cancel_to_children",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        main.subjob_completion_operations,
        "handle_scholar_completion",
        AsyncMock(),
    )
    monkeypatch.setattr(main, "maybe_wake_session", AsyncMock())
    monkeypatch.setattr(main, "_kick_session_wake_drain", MagicMock())
    monkeypatch.setattr(main, "_trigger_dispatch", MagicMock())

    assert await control_seams.cancel_job(MagicMock(), JOB_ID) == {
        "status": "cancelled"
    }

    settle.assert_awaited_once_with(JOB_ID)
    # Cleanup belongs to the settle helper after queue closure; the endpoint
    # must not run a second eager teardown against the live holder.
    direct_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_cancel_keeps_root_workspace_when_stateless_child_did_not_settle(
    monkeypatch,
):
    from orchestrator import main

    job = {
        "id": JOB_ID,
        "status": "processing",
        "execution_lane": "stateless",
        "assigned_agent_id": None,
    }
    monkeypatch.setattr(
        main,
        "require_internal_or_job_access",
        AsyncMock(return_value=(None, job)),
    )
    monkeypatch.setattr(
        main.postgres_db,
        "cancel_stateless_job",
        AsyncMock(return_value=(True, True)),
    )
    monkeypatch.setattr(
        main.job_mutation_operations.JobControlOperations,
        "cascade_cancel_to_children",
        AsyncMock(return_value=False),
    )
    cleanup = AsyncMock()
    monkeypatch.setattr(
        main.thread_retirement_operations, "archive_and_cleanup_workspace", cleanup
    )
    monkeypatch.setattr(
        main.subjob_completion_operations,
        "handle_scholar_completion",
        AsyncMock(),
    )
    monkeypatch.setattr(main, "maybe_wake_session", AsyncMock())
    monkeypatch.setattr(main, "_kick_session_wake_drain", MagicMock())
    monkeypatch.setattr(main, "_trigger_dispatch", MagicMock())

    with pytest.raises(HTTPException) as error:
        await control_seams.cancel_job(MagicMock(), JOB_ID)

    assert error.value.status_code == 503
    assert (
        error.value.detail == "Descendant workspace authority retirement is incomplete"
    )
    cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_cascade_cancel_reports_unsettled_stateless_child(monkeypatch):
    from orchestrator import main

    child_id = "22222222-2222-2222-2222-222222222222"
    child = {
        "id": child_id,
        "status": "processing",
        "execution_lane": "stateless",
        "assigned_agent_id": None,
    }
    monkeypatch.setattr(
        main.postgres_db,
        "get_descendant_jobs",
        AsyncMock(return_value=[child]),
    )
    monkeypatch.setattr(
        main.postgres_db,
        "cancel_stateless_job",
        AsyncMock(return_value=(True, False)),
    )
    monkeypatch.setattr(
        main.job_mutation_operations.JobControlOperations,
        "wait_for_stateless_cancel_settle",
        AsyncMock(return_value=False),
    )

    assert not await control_seams.cascade_cancel_to_children(JOB_ID)


@pytest.mark.asyncio
async def test_flag_on_cascade_pinned_control_loser_has_zero_external_io(monkeypatch):
    from orchestrator import main

    child = {
        "id": PARENT_ID,
        "status": "processing",
        "execution_lane": "pinned",
        "assigned_agent_id": None,
        "context": {
            "_completion_control_claim": {
                "version": 1,
                "expires_epoch": 4_102_444_800,
            }
        },
    }
    monkeypatch.setattr(main, "COMPLETION_COMMANDS_ENABLED", True)
    monkeypatch.setattr(
        main.postgres_db, "get_descendant_jobs", AsyncMock(return_value=[child])
    )
    linearize = AsyncMock(return_value=False)
    monkeypatch.setattr(main.postgres_db, "linearize_pinned_cancel", linearize)
    monkeypatch.setattr(main.postgres_db, "get_job", AsyncMock(return_value=child))
    cleanup = AsyncMock()
    monkeypatch.setattr(
        main.thread_retirement_operations, "archive_and_cleanup_workspace", cleanup
    )
    monkeypatch.setattr(main.postgres_db, "get_agent", AsyncMock())

    assert not await control_seams.cascade_cancel_to_children(JOB_ID)

    linearize.assert_awaited_once_with(
        PARENT_ID,
        expected_status="processing",
        completion_commands_enabled=True,
    )
    cleanup.assert_not_awaited()
    main.postgres_db.get_agent.assert_not_awaited()


@pytest.mark.asyncio
async def test_flag_on_cascade_pinned_cancel_linearizes_before_cleanup(monkeypatch):
    from orchestrator import main

    child = {
        "id": PARENT_ID,
        "status": "paused",
        "execution_lane": "pinned",
        "assigned_agent_id": None,
        "context": {},
    }
    order: list[str] = []
    monkeypatch.setattr(main, "COMPLETION_COMMANDS_ENABLED", True)
    monkeypatch.setattr(
        main.postgres_db, "get_descendant_jobs", AsyncMock(return_value=[child])
    )
    monkeypatch.setattr(
        main.postgres_db,
        "linearize_pinned_cancel",
        AsyncMock(side_effect=lambda *_a, **_k: order.append("linearize") or True),
    )
    monkeypatch.setattr(
        main.thread_retirement_operations,
        "archive_and_cleanup_workspace",
        AsyncMock(side_effect=lambda *_a, **_k: order.append("cleanup")),
    )

    assert await control_seams.cascade_cancel_to_children(JOB_ID)
    assert order == ["linearize", "cleanup"]


@pytest.mark.asyncio
async def test_cascade_cancel_retry_settles_existing_child_cleanup_marker(monkeypatch):
    from orchestrator import main

    child_id = "22222222-2222-2222-2222-222222222222"
    child = {
        "id": child_id,
        "status": "cancelled",
        "execution_lane": "stateless",
        "assigned_agent_id": None,
        "context": {"_stateless_cancel_cleanup_pending": True},
    }
    monkeypatch.setattr(
        main.postgres_db,
        "get_descendant_jobs",
        AsyncMock(return_value=[child]),
    )
    monkeypatch.setattr(
        main.postgres_db,
        "cancel_stateless_job",
        AsyncMock(return_value=(False, False)),
    )
    monkeypatch.setattr(main.postgres_db, "get_job", AsyncMock(return_value=child))
    settle = AsyncMock(return_value=True)
    monkeypatch.setattr(
        main.job_mutation_operations.JobControlOperations,
        "wait_for_stateless_cancel_settle",
        settle,
    )

    assert await control_seams.cascade_cancel_to_children(JOB_ID)
    settle.assert_awaited_once_with(child_id)


@pytest.mark.asyncio
async def test_cancel_settle_prunes_then_cleans_workspace(monkeypatch):
    from orchestrator import main

    finalize = AsyncMock(side_effect=(False, True))
    monkeypatch.setattr(main.postgres_db, "finalize_cancelled_stateless_job", finalize)
    monkeypatch.setattr(
        main.postgres_db,
        "stateless_cancel_cleanup_lock",
        _owned_cleanup_lock,
    )
    monkeypatch.setattr(
        main.postgres_db,
        "stateless_cancel_cleanup_pending",
        AsyncMock(return_value=True),
    )
    complete = AsyncMock(return_value=True)
    monkeypatch.setattr(
        main.postgres_db,
        "complete_stateless_cancel_cleanup",
        complete,
    )
    cleanup = AsyncMock()
    monkeypatch.setattr(
        main.thread_retirement_operations.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        cleanup,
    )

    settled = await control_seams.wait_for_stateless_cancel_settle(
        JOB_ID,
        timeout_seconds=0.2,
        poll_seconds=0.001,
    )

    assert settled is True
    assert finalize.await_count == 2
    cleanup.assert_awaited_once_with(JOB_ID)
    complete.assert_awaited_once_with(JOB_ID)


@pytest.mark.asyncio
async def test_cancel_settle_keeps_resume_block_until_workspace_cleanup_succeeds(
    monkeypatch,
):
    from orchestrator import main

    finalize = AsyncMock(return_value=True)
    monkeypatch.setattr(main.postgres_db, "finalize_cancelled_stateless_job", finalize)
    monkeypatch.setattr(
        main.postgres_db,
        "stateless_cancel_cleanup_lock",
        _owned_cleanup_lock,
    )
    monkeypatch.setattr(
        main.postgres_db,
        "stateless_cancel_cleanup_pending",
        AsyncMock(return_value=True),
    )
    complete = AsyncMock(return_value=True)
    monkeypatch.setattr(
        main.postgres_db,
        "complete_stateless_cancel_cleanup",
        complete,
    )
    cleanup = AsyncMock(side_effect=(RuntimeError("teardown busy"), None))
    monkeypatch.setattr(
        main.thread_retirement_operations.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        cleanup,
    )

    settled = await control_seams.wait_for_stateless_cancel_settle(
        JOB_ID,
        timeout_seconds=0.2,
        poll_seconds=0.001,
    )

    assert settled is True
    assert cleanup.await_count == 2
    # The first failed teardown must not clear the lifecycle marker.
    complete.assert_awaited_once_with(JOB_ID)


@pytest.mark.asyncio
async def test_concurrent_cancel_settlers_run_destructive_cleanup_once(monkeypatch):
    from orchestrator import main

    cleanup_lock = asyncio.Lock()
    marker_pending = True

    @asynccontextmanager
    async def try_cleanup_lock(_job_id):
        if cleanup_lock.locked():
            yield False
            return
        await cleanup_lock.acquire()
        try:
            yield True
        finally:
            cleanup_lock.release()

    async def pending(_job_id):
        return marker_pending

    async def complete(_job_id):
        nonlocal marker_pending
        marker_pending = False
        return True

    first_cleanup_entered = asyncio.Event()
    release_first_cleanup = asyncio.Event()

    async def cleanup(_job_id):
        first_cleanup_entered.set()
        await release_first_cleanup.wait()

    monkeypatch.setattr(
        main.postgres_db,
        "stateless_cancel_cleanup_lock",
        try_cleanup_lock,
    )
    monkeypatch.setattr(
        main.postgres_db,
        "stateless_cancel_cleanup_pending",
        AsyncMock(side_effect=pending),
    )
    monkeypatch.setattr(
        main.postgres_db,
        "finalize_cancelled_stateless_job",
        AsyncMock(return_value=True),
    )
    complete_mock = AsyncMock(side_effect=complete)
    monkeypatch.setattr(
        main.postgres_db,
        "complete_stateless_cancel_cleanup",
        complete_mock,
    )
    cleanup_mock = AsyncMock(side_effect=cleanup)
    monkeypatch.setattr(
        main.thread_retirement_operations.ThreadRetirementOperations,
        "archive_and_cleanup_workspace",
        cleanup_mock,
    )

    first = asyncio.create_task(
        control_seams.wait_for_stateless_cancel_settle(
            JOB_ID,
            timeout_seconds=0.5,
            poll_seconds=0.001,
        )
    )
    await first_cleanup_entered.wait()
    second = asyncio.create_task(
        control_seams.wait_for_stateless_cancel_settle(
            JOB_ID,
            timeout_seconds=0.5,
            poll_seconds=0.001,
        )
    )
    await asyncio.sleep(0.01)
    release_first_cleanup.set()

    assert await first
    assert await second
    cleanup_mock.assert_awaited_once_with(JOB_ID)
    complete_mock.assert_awaited_once_with(JOB_ID)


@pytest.mark.asyncio
async def test_phase_approval_reenqueues_stateless_job(monkeypatch, tmp_path):
    from orchestrator import main

    job = {
        "id": JOB_ID,
        "execution_lane": "stateless",
        "status": "pending_review",
        "freeze_data": {"freeze_type": "phase_boundary", "phase_number": 2},
        "diff_status": None,
        "priority": 7,
        "user_id": "33333333-3333-3333-3333-333333333333",
    }
    monkeypatch.setattr(
        main,
        "require_internal_or_job_access",
        AsyncMock(return_value=(None, job)),
    )
    monkeypatch.setattr(
        main.subjob_output_operations,
        "resolve_job_repo",
        AsyncMock(return_value=(None, None)),
    )
    monkeypatch.setattr(main, "gitea_client", SimpleNamespace(is_initialized=False))
    monkeypatch.setattr(main, "workspace_service", SimpleNamespace(base_path=tmp_path))
    # Model an available idle schema with no open operation for this offline
    # stateless fixture; approval then follows the ordinary queue path.
    monkeypatch.setattr(
        "orchestrator.services.vm_idle_lifecycle.VMIdleLifecycleStore.schema_available",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        "orchestrator.services.vm_idle_lifecycle.VMIdleLifecycleStore.get_open_for_owner",
        AsyncMock(return_value=None),
    )
    queued = AsyncMock(return_value=True)
    monkeypatch.setattr(main.postgres_db, "queue_stateless_job_for_resume", queued)

    result = await control_seams.approve_job(MagicMock(), JOB_ID)

    assert result["status"] == "approved_continue"
    queued.assert_awaited_once_with(
        JOB_ID,
        priority=7,
        fair_key="33333333-3333-3333-3333-333333333333",
        expected_status="pending_review",
        idle_phase_source_snapshot=None,
    )


@pytest.mark.asyncio
async def test_internal_stateless_reply_forwards_exact_route_fence(monkeypatch):
    from orchestrator import main

    route_id = str(uuid4())
    job = {
        "id": JOB_ID,
        "status": "waiting_for_reply",
        "execution_lane": "stateless",
        "priority": 2,
        "user_id": None,
    }
    monkeypatch.setattr(main.postgres_db, "get_job", AsyncMock(return_value=job))
    queued = AsyncMock(return_value=True)
    monkeypatch.setattr(main.postgres_db, "queue_stateless_job_for_resume", queued)
    monkeypatch.setattr(main, "_trigger_dispatch", MagicMock())
    assert await control_seams.internal_resume_job(
        JOB_ID,
        "answer",
        expected_status="waiting_for_reply",
        expected_route_id=route_id,
    )
    assert queued.await_args.kwargs["expected_route_id"] == route_id
