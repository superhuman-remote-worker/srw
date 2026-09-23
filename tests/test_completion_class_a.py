"""Atomic jobs-row disposition regressions for completion Class A.

The completion paths used to commit status, agent release, freeze shedding,
and ``completed_at`` separately. These tests pin both halves of the repair:
``PostgresDB.update_job_status`` emits one UPDATE containing the full
disposition, and the live endpoints select the right freeze behavior.
"""

from __future__ import annotations

from tests import _b09_control_seams as control_seams

from tests import b08_completion_helpers as b08_helpers

import dataclasses
import json
from contextlib import ExitStack, asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

from tests._workspace_recovery_fakes import idle_recovery_store

import pytest
from fastapi import HTTPException

import orchestrator.main
from orchestrator.database.postgres import PostgresDB
from orchestrator.routers import job_diff as job_diff_routes

JOB_ID = "11111111-1111-1111-1111-111111111111"
AGENT_ID = "22222222-2222-2222-2222-222222222222"
PROJECT_ID = "33333333-3333-3333-3333-333333333333"


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _db_with_connection(conn: AsyncMock) -> PostgresDB:
    db = PostgresDB.__new__(PostgresDB)

    @asynccontextmanager
    async def acquire():
        yield conn

    db.acquire = acquire
    db.delete_checkpoint_thread = AsyncMock(return_value=0)
    return db


class TestUpdateJobStatusClassA:
    @pytest.mark.asyncio
    async def test_completed_status_automatically_coalesces_timestamp(self):
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")
        db = _db_with_connection(conn)

        assert await db.update_job_status(
            JOB_ID,
            status="completed",
        )

        conn.execute.assert_awaited_once()
        sql = _normalized(conn.execute.await_args.args[0])
        assert sql.startswith("UPDATE jobs SET status = $1,")
        assert "completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP)" in sql
        assert "assigned_agent_id" not in sql
        assert "freeze_data" not in sql
        assert conn.execute.await_args.args[1:] == ("completed", UUID(JOB_ID))

    @pytest.mark.asyncio
    async def test_pause_stashes_exact_payload_and_clears_in_one_update(self):
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")
        db = _db_with_connection(conn)
        freeze = {
            "freeze_type": "version_upgrade",
            "phase_number": 7,
            "reason": "drain",
        }

        assert await db.update_job_status(
            JOB_ID,
            status="paused",
            assigned_agent_id="",
            freeze_data=freeze,
            stash_and_clear_freeze=True,
        )

        conn.execute.assert_awaited_once()
        args = conn.execute.await_args.args
        sql = _normalized(args[0])
        assert sql.startswith("UPDATE jobs SET status = $1, assigned_agent_id = $2,")
        assert (
            "context = COALESCE(context, '{}'::jsonb) || "
            "jsonb_build_object('last_freeze_data', $3::jsonb)"
        ) in sql
        assert "freeze_data = NULL" in sql
        assert "freeze_data = $3::jsonb" not in sql
        assert args[1:] == ("paused", None, json.dumps(freeze), UUID(JOB_ID))

    @pytest.mark.asyncio
    async def test_human_action_pause_clears_agent_but_keeps_freeze(self):
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")
        db = _db_with_connection(conn)

        assert await db.update_job_status(
            JOB_ID,
            status="paused",
            assigned_agent_id="",
        )

        conn.execute.assert_awaited_once()
        sql = _normalized(conn.execute.await_args.args[0])
        assert "assigned_agent_id = $2" in sql
        assert "last_freeze_data" not in sql
        assert "freeze_data = NULL" not in sql
        assert "completed_at" not in sql

    @pytest.mark.asyncio
    async def test_optional_status_cas_rides_the_same_disposition_update(self):
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")
        db = _db_with_connection(conn)

        assert await db.update_job_status(
            JOB_ID,
            status="completed",
            expected_status="processing",
        )

        args = conn.execute.await_args.args
        sql = _normalized(args[0])
        assert "WHERE id = $2 AND status::text = $3::text" in sql
        assert args[1:] == ("completed", UUID(JOB_ID), "processing")

    @pytest.mark.asyncio
    async def test_blocked_delivery_outcome_rides_the_terminal_status_update(self):
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")
        db = _db_with_connection(conn)

        assert await db.update_job_status(
            JOB_ID,
            status="cancelled",
            completion_outcome_kind="blocked_undelivered",
            expected_status="processing",
        )

        args = conn.execute.await_args.args
        sql = _normalized(args[0])
        assert "status = $1" in sql
        assert "completion_outcome_kind = $2" in sql
        assert "WHERE id = $3 AND status::text = $4::text" in sql
        assert args[1:] == (
            "cancelled",
            "blocked_undelivered",
            UUID(JOB_ID),
            "processing",
        )

    @pytest.mark.asyncio
    async def test_blocked_delivery_outcome_rejects_invalid_status_pairing(self):
        conn = AsyncMock()
        db = _db_with_connection(conn)

        with pytest.raises(ValueError, match="invalid completion outcome"):
            await db.update_job_status(
                JOB_ID,
                status="completed",
                completion_outcome_kind="blocked_undelivered",
            )

        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_completion_term_and_entry_status_are_one_atomic_cas(self):
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")
        db = _db_with_connection(conn)
        command_id = "44444444-4444-4444-8444-444444444444"

        assert await db.update_job_status(
            JOB_ID,
            status="completed",
            expected_status="processing",
            completion_command_id=command_id,
            completion_finalizing_by="attempt-owner",
        )

        args = conn.execute.await_args.args
        sql = _normalized(args[0])
        assert "WHERE id = $2 AND status::text = $3::text" in sql
        assert "command.id = $4::uuid" in sql
        assert "command.job_id = jobs.id" in sql
        assert "command.state = 'finalizing'" in sql
        assert "command.finalizing_by = $5::text" in sql
        assert "command.lease_expires_at > now()" in sql
        assert "command.deadline_at > now()" in sql
        assert args[1:] == (
            "completed",
            UUID(JOB_ID),
            "processing",
            UUID(command_id),
            "attempt-owner",
        )

    @pytest.mark.asyncio
    async def test_delivery_marker_is_strictly_consumed_in_the_status_cas(self):
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")
        db = _db_with_connection(conn)
        command_id = "44444444-4444-4444-8444-444444444444"

        assert await db.update_job_status(
            JOB_ID,
            status="completed",
            expected_status="processing",
            completion_command_id=command_id,
            completion_finalizing_by="attempt-owner",
            completion_control_claim_id=command_id,
        )

        args = conn.execute.await_args.args
        sql = _normalized(args[0])
        assert (
            "context = (COALESCE(context, '{}'::jsonb)) - '_completion_control_claim'"
            in sql
        )
        assert "->>'claim_id' = $6::text" in sql
        assert "->>'source' = 'completion_delivery'" in sql
        assert "->>'fence_kind' = 'completion_command'" in sql
        assert "->>'fence_value' = $6::text" in sql
        assert "extract(epoch FROM clock_timestamp())" in sql
        assert args[1:] == (
            "completed",
            UUID(JOB_ID),
            "processing",
            UUID(command_id),
            "attempt-owner",
            command_id,
        )

    @pytest.mark.asyncio
    async def test_completion_decision_is_consumed_by_exact_identity_in_status_cas(
        self,
    ):
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")
        db = _db_with_connection(conn)
        command_id = "44444444-4444-4444-8444-444444444444"

        assert await db.update_job_status(
            JOB_ID,
            status="completed",
            expected_status="processing",
            completion_command_id=command_id,
            completion_finalizing_by="attempt-owner",
            consume_completion_decision_tool_call_id="round-2-tool",
        )

        args = conn.execute.await_args.args
        sql = _normalized(args[0])
        assert "'{completion_decision,tool_call_id}' = $2::text" in sql
        assert "- 'completion_decision'" in sql
        assert "command.id = $5::uuid" in sql
        assert "command.finalizing_by = $6::text" in sql
        assert args[1:] == (
            "completed",
            "round-2-tool",
            UUID(JOB_ID),
            "processing",
            UUID(command_id),
            "attempt-owner",
        )

    @pytest.mark.asyncio
    async def test_completion_term_arguments_must_be_paired(self):
        conn = AsyncMock()
        db = _db_with_connection(conn)

        with pytest.raises(ValueError, match="must be paired"):
            await db.update_job_status(
                JOB_ID,
                status="completed",
                completion_command_id="44444444-4444-4444-8444-444444444444",
            )

        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_workspace_recovery_marker_shares_failed_disposition_cas(self):
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")
        conn.fetchval = AsyncMock(return_value="stateless")
        db = _db_with_connection(conn)
        command_id = "44444444-4444-4444-8444-444444444444"
        marker = {
            "recovery_completion_command_id": command_id,
            "recovery_completion_outcome": {"new_status": "failed"},
        }

        assert await db.update_job_status(
            JOB_ID,
            status="failed",
            workspace_context_updates=marker,
            expected_status="processing",
            completion_command_id=command_id,
            completion_finalizing_by="attempt-owner",
        )

        args = conn.execute.await_args.args
        sql = _normalized(args[0])
        assert "status = $1" in sql
        assert "context = jsonb_set(" in sql
        assert "'{workspace_container}'" in sql
        assert "|| $2::jsonb" in sql
        assert "WHERE id = $3 AND status::text = $4::text" in sql
        assert "command.id = $5::uuid" in sql
        assert "command.finalizing_by = $6::text" in sql
        assert args[1:] == (
            "failed",
            json.dumps(marker),
            UUID(JOB_ID),
            "processing",
            UUID(command_id),
            "attempt-owner",
        )

    @pytest.mark.asyncio
    async def test_workspace_recovery_pause_marker_and_term_are_one_update(self):
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value="UPDATE 1")
        db = _db_with_connection(conn)
        command_id = "44444444-4444-4444-8444-444444444444"
        marker = {
            "recovery_completion_command_id": command_id,
            "recovery_completion_outcome": {"new_status": "paused"},
        }

        assert await db.pause_job_shed_freeze(
            JOB_ID,
            completion_command_id=command_id,
            completion_finalizing_by="attempt-owner",
            workspace_context_updates=marker,
        )

        args = conn.execute.await_args.args
        sql = _normalized(args[0])
        assert "SET status = 'paused'" in sql
        assert "context = jsonb_set(" in sql
        assert "'{workspace_container}'" in sql
        assert "|| $2::jsonb" in sql
        assert "command.id = $3::uuid" in sql
        assert "command.finalizing_by = $4::text" in sql
        assert args[1:] == (
            UUID(JOB_ID),
            json.dumps(marker),
            UUID(command_id),
            "attempt-owner",
        )


class _EndpointDB(PostgresDB):
    """Small real-helper/fake-connection DB for driving ``complete_job``."""

    def __init__(
        self,
        job: dict,
        *,
        project: dict | None = None,
        fail_initial_freeze_write: bool = False,
    ) -> None:
        self.job = job
        self.project = project
        self.statements: list[tuple[str, tuple]] = []
        self.context_writes: list[dict] = []
        self.fail_initial_freeze_write = fail_initial_freeze_write

    @asynccontextmanager
    async def acquire(self):
        db = self

        class _Connection:
            async def execute(self, sql: str, *args):
                db.statements.append((sql, args))
                normalized = _normalized(sql)

                if normalized.startswith("UPDATE jobs SET freeze_data = $1::jsonb"):
                    if db.fail_initial_freeze_write:
                        db.fail_initial_freeze_write = False
                        raise RuntimeError("injected S3 freeze persist failure")
                    db.job["freeze_data"] = json.loads(args[0])
                elif normalized.startswith("UPDATE jobs SET status = $1"):
                    db.job["status"] = args[0]
                    if "assigned_agent_id = $2" in normalized:
                        db.job["assigned_agent_id"] = args[1]
                    if "last_freeze_data" in normalized:
                        freeze_json = next(
                            value
                            for value in args
                            if isinstance(value, str) and value.startswith("{")
                        )
                        frozen = json.loads(freeze_json)
                        db.job.setdefault("context", {})["last_freeze_data"] = frozen
                        db.job["freeze_data"] = None
                    if "completed_at = COALESCE" in normalized:
                        db.job["completed_at"] = "set"
                return "UPDATE 1"

            async def fetchval(self, _sql: str, *_args):
                return db.job.get("execution_lane", "pinned")

        yield _Connection()

    async def get_job(self, job_id: str) -> dict:
        return self.job

    async def get_project(self, project_id: str) -> dict | None:
        return self.project

    async def update_job_cloud_diff(self, job_id: str, **updates) -> bool:
        self.job.update(updates)
        return True

    async def update_job_merge_status(self, job_id: str, **updates) -> bool:
        self.job.update(updates)
        return True

    async def merge_job_context(self, job_id: str, updates: dict) -> bool:
        self.context_writes.append(updates)
        self.job.setdefault("context", {}).update(updates)
        return True

    async def delete_checkpoint_thread(self, job_id: str) -> int:
        return 0

    def class_a_statements(self) -> list[tuple[str, tuple]]:
        return [
            (sql, args)
            for sql, args in self.statements
            if _normalized(sql).startswith("UPDATE jobs SET status = $1")
        ]


def _job(*, freeze_data: dict | None = None, **overrides) -> dict:
    job = {
        "id": JOB_ID,
        "status": "processing",
        "assigned_agent_id": AGENT_ID,
        "freeze_data": freeze_data,
        "context": {},
        "parent_job_id": None,
        "project_id": None,
        "user_id": None,
        "config_name": "defaults",
        "config_override": None,
        "resolved_config": {
            "agent": {
                "autonomy": "full",
                "verification": {"enabled": False},
                "curator": {"enabled": False},
            }
        },
        "cloud_diff_baseline_commit": None,
        "merge_status": None,
        "repo_name": None,
        "branch_name": None,
    }
    job.update(overrides)
    return job


def _patch_completion(stack: ExitStack, db: _EndpointDB) -> None:
    stack.enter_context(
        patch(
            "orchestrator.main.VMWorkspaceRecoveryStore",
            return_value=idle_recovery_store(),
        )
    )
    stack.enter_context(patch("orchestrator.main.require_internal", AsyncMock()))
    stack.enter_context(patch("orchestrator.main.postgres_db", db))
    gitea = MagicMock()
    gitea.is_initialized = False
    stack.enter_context(patch("orchestrator.main.gitea_client", gitea))
    stack.enter_context(patch("orchestrator.main.vector_db", None))
    stack.enter_context(
        patch(
            "orchestrator.services.completion.apply_deliverable_gate",
            AsyncMock(
                side_effect=lambda job, result, status, **kw: (status, [], False)
            ),
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.services.completion.apply_terminal_job_side_effects",
            AsyncMock(return_value={"actions": []}),
        )
    )
    for target in (
        "orchestrator.main.verification_operations.handle_critic_verdict_on_complete",
        "orchestrator.main.subjob_completion_operations.handle_scholar_completion",
        "orchestrator.main.subjob_completion_operations.handle_delegation_child_completion",
        "orchestrator.main.verification_operations.trigger_verification_on_complete",
        "orchestrator.main.project_loop_advance_service.advance_project_loop",
        "orchestrator.main.maybe_wake_session",
    ):
        stack.enter_context(patch(target, AsyncMock(return_value=[])))
    stack.enter_context(
        patch.object(
            control_seams,
            "archive_and_cleanup_workspace",
            AsyncMock(return_value=[]),
        )
    )
    stack.enter_context(
        patch("orchestrator.main._kick_session_wake_drain", MagicMock())
    )
    stack.enter_context(patch("orchestrator.main._trigger_dispatch", MagicMock()))


class TestCompleteJobClassA:
    @pytest.mark.asyncio
    async def test_stale_stateless_token_rejected_before_late_callback_guard(self):
        job = _job(status="completed", execution_lane="stateless")
        db = _EndpointDB(job)
        body = orchestrator.main.JobCompleteRequest(
            should_stop=True,
            goal_achieved=True,
            lease_token=6,
        )

        with ExitStack() as stack:
            _patch_completion(stack, db)
            current = stack.enter_context(
                patch(
                    "shared.worker_queue.worker_lease_is_current",
                    AsyncMock(return_value=False),
                )
            )
            with pytest.raises(HTTPException) as exc:
                await b08_helpers.complete_job(MagicMock(), JOB_ID, body)

        assert exc.value.status_code == 409
        current.assert_awaited_once()
        assert db.statements == []

    @pytest.mark.asyncio
    async def test_exact_stateless_token_retry_reaches_benign_late_guard(self):
        job = _job(status="completed", execution_lane="stateless")
        db = _EndpointDB(job)
        body = orchestrator.main.JobCompleteRequest(
            should_stop=True,
            goal_achieved=True,
            lease_token=7,
        )

        with ExitStack() as stack:
            _patch_completion(stack, db)
            current = stack.enter_context(
                patch(
                    "shared.worker_queue.worker_lease_is_current",
                    AsyncMock(return_value=True),
                )
            )
            handled = await b08_helpers.complete_job(MagicMock(), JOB_ID, body)

        current.assert_awaited_once()
        assert handled["new_status"] == "completed"
        assert handled["actions"] == ["late callback ignored; job already completed"]
        assert db.statements == []

    @pytest.mark.asyncio
    async def test_pinned_job_ignores_optional_stateless_lease_token(self):
        job = _job(status="completed", execution_lane="pinned")
        db = _EndpointDB(job)
        body = orchestrator.main.JobCompleteRequest(
            should_stop=True,
            goal_achieved=False,
            lease_token=7,
        )

        with ExitStack() as stack:
            _patch_completion(stack, db)
            current = stack.enter_context(
                patch(
                    "shared.worker_queue.worker_lease_is_current",
                    AsyncMock(return_value=False),
                )
            )
            handled = await b08_helpers.complete_job(MagicMock(), JOB_ID, body)

        assert handled["new_status"] == "completed"
        current.assert_not_awaited()
        assert db.statements == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status",
        ["paused", "failed", "cancelled", "waiting", "waiting_for_reply"],
    )
    async def test_exact_stateless_terminal_retry_is_benign_for_published_status(
        self,
        status,
    ):
        job = _job(status=status, execution_lane="stateless")
        db = _EndpointDB(job)
        body = orchestrator.main.JobCompleteRequest(
            should_stop=True,
            goal_achieved=status == "failed",
            lease_token=7,
        )

        with ExitStack() as stack:
            _patch_completion(stack, db)
            stack.enter_context(
                patch(
                    "shared.worker_queue.worker_lease_is_current",
                    AsyncMock(return_value=True),
                )
            )
            handled = await b08_helpers.complete_job(MagicMock(), JOB_ID, body)

        assert handled["new_status"] == status
        assert handled["actions"] == [
            f"exact-token terminal retry; job already {status}"
        ]
        assert db.statements == []

    @pytest.mark.asyncio
    async def test_stateless_disposition_cas_cannot_overwrite_winning_cancel(self):
        job = _job(execution_lane="stateless")
        db = _EndpointDB(job)
        body = orchestrator.main.JobCompleteRequest(
            should_stop=True,
            goal_achieved=True,
            lease_token=7,
        )

        async def lose_to_cancel(_job_id, **_kwargs):
            job["status"] = "cancelled"
            return False

        db.update_job_status = AsyncMock(side_effect=lose_to_cancel)
        with ExitStack() as stack:
            _patch_completion(stack, db)
            stack.enter_context(
                patch(
                    "shared.worker_queue.worker_lease_is_current",
                    AsyncMock(return_value=True),
                )
            )
            with pytest.raises(HTTPException) as exc:
                await b08_helpers.complete_job(MagicMock(), JOB_ID, body)

        assert exc.value.status_code == 409
        assert exc.value.detail == (
            "Completion report lost an out-of-band job control race"
        )
        db.update_job_status.assert_awaited_once()
        assert db.update_job_status.await_args.kwargs["expected_status"] == "processing"

    @pytest.mark.asyncio
    async def test_stateless_stop_does_not_blanket_delete_checkpoint_acked_replies(
        self,
    ):
        job = _job(
            execution_lane="stateless",
            context={"queued_replies": [{"reply": "keep until checkpoint ack"}]},
        )
        db = _EndpointDB(job)
        body = orchestrator.main.JobCompleteRequest(
            should_stop=True,
            goal_achieved=True,
            lease_token=7,
        )

        with ExitStack() as stack:
            _patch_completion(stack, db)
            stack.enter_context(
                patch(
                    "shared.worker_queue.worker_lease_is_current",
                    AsyncMock(return_value=True),
                )
            )
            handled = await b08_helpers.complete_job(MagicMock(), JOB_ID, body)

        assert handled["new_status"] == "completed"
        assert not any("context - 'queued_replies'" in sql for sql, _ in db.statements)

    @pytest.mark.asyncio
    async def test_completed_disposition_is_one_jobs_update(self):
        job = _job()
        db = _EndpointDB(job)
        body = orchestrator.main.JobCompleteRequest(
            should_stop=True,
            goal_achieved=True,
            freeze_data={"status": "job_completed", "summary": "done"},
        )

        with ExitStack() as stack:
            _patch_completion(stack, db)
            handled = await b08_helpers.complete_job(MagicMock(), JOB_ID, body)

        [(sql, _args)] = db.class_a_statements()
        normalized = _normalized(sql)
        assert "completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP)" in normalized
        assert sum("completed_at" in sql for sql, _ in db.statements) == 1
        assert handled["new_status"] == "completed"
        # E4 (officer_supervision_surface §3.3): a sealed completion claim now
        # records its evidence manifest before the status write.
        assert handled["actions"] == [
            "evidence manifest recorded (1 entr(y/ies))",
            "status -> completed",
        ]
        assert job["completed_at"] == "set"

    @pytest.mark.asyncio
    async def test_strict_delivery_cap_terminalizes_blocked_in_one_class_a_write(
        self,
    ):
        from orchestrator.services.deliverable_gate import DeliverableGateResult

        job = _job(
            context={"required_deliverables": ["pr:acme/widget"]},
        )
        db = _EndpointDB(job)
        body = orchestrator.main.JobCompleteRequest(
            should_stop=True,
            goal_achieved=False,
            freeze_data={"status": "job_completed", "summary": "no PR produced"},
        )

        with ExitStack() as stack:
            _patch_completion(stack, db)
            stack.enter_context(
                patch(
                    "orchestrator.services.completion.apply_deliverable_gate",
                    AsyncMock(
                        return_value=DeliverableGateResult(
                            "cancelled",
                            ["delivery contract terminalized blocked/undelivered"],
                            False,
                            "blocked_undelivered",
                        )
                    ),
                )
            )
            verification = stack.enter_context(
                patch(
                    "orchestrator.main.verification_operations.trigger_verification_on_complete",
                    AsyncMock(),
                )
            )
            handled = await b08_helpers.complete_job(MagicMock(), JOB_ID, body)

        [(sql, args)] = db.class_a_statements()
        normalized = _normalized(sql)
        assert "status = $1" in normalized
        assert "completion_outcome_kind = $3" in normalized
        assert args[0] == "cancelled"
        assert "blocked_undelivered" in args
        assert handled["new_status"] == "cancelled"
        assert job["completion_outcome_kind"] == "blocked_undelivered"
        verification.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_held_full_autonomy_seal_is_neither_silent_nor_unexplained(
        self,
    ):
        """worker_git_versioning_stops_midjob_seal_pins_stale_revision.md.

        A full-autonomy completion carries no freeze_type, so a seal the gate
        held as delivery-unproven used to land in pending_review with no
        notification and no reason anywhere a human or officer reads. The
        reason now rides the same status write as error_message, and the
        owner gets a review-queue item that names it.
        """
        from orchestrator.services.deliverable_gate import DeliverableGateResult

        reason = (
            "the worker could not read its workspace HEAD when it sealed "
            "(head_commit is null)"
        )
        job = _job(context={"required_deliverables": ["output/report.md"]})
        db = _EndpointDB(job)
        body = orchestrator.main.JobCompleteRequest(
            should_stop=True,
            goal_achieved=True,
            freeze_data={
                "status": "job_completed",
                "summary": "done",
                "head_commit": None,
            },
        )
        notify = AsyncMock()

        with ExitStack() as stack:
            _patch_completion(stack, db)
            stack.enter_context(
                patch(
                    "orchestrator.services.completion.apply_deliverable_gate",
                    AsyncMock(
                        return_value=DeliverableGateResult(
                            "pending_review",
                            [f"deliverable gate: delivery unproven ({reason})"],
                            False,
                            hold_reason=reason,
                        )
                    ),
                )
            )
            stack.enter_context(
                patch(
                    "orchestrator.main.job_freeze_notification_service.notify_operator_freeze",
                    notify,
                )
            )
            handled = await b08_helpers.complete_job(MagicMock(), JOB_ID, body)

        assert handled["new_status"] == "pending_review"
        [(_sql, args)] = db.class_a_statements()
        assert args[0] == "pending_review"
        assert f"Delivery unproven: {reason}" in args
        notify.assert_awaited_once()
        _job_arg, _job_id, freeze_type, freeze_arg = notify.await_args.args[:4]
        assert freeze_type == "job_complete"
        assert freeze_arg["delivery_hold"] == reason

    @pytest.mark.asyncio
    async def test_evidence_record_failure_never_persists_private_coordinates(
        self, caplog
    ):
        private_detail = "victim-private-repo/private/report.png"
        job = _job()
        db = _EndpointDB(job)
        body = orchestrator.main.JobCompleteRequest(
            should_stop=True,
            goal_achieved=True,
            freeze_data={"status": "job_completed", "summary": "done"},
        )

        with ExitStack() as stack:
            _patch_completion(stack, db)
            stack.enter_context(
                patch(
                    "orchestrator.services.job_evidence.build_evidence_manifest",
                    AsyncMock(side_effect=RuntimeError(private_detail)),
                )
            )
            handled = await b08_helpers.complete_job(MagicMock(), JOB_ID, body)

        assert handled["new_status"] == "completed"
        assert private_detail not in caplog.text
        assert private_detail not in json.dumps(db.context_writes)

    @pytest.mark.asyncio
    async def test_auto_pause_uses_report_payload_after_initial_persist_failure(self):
        freeze = {
            "freeze_type": "version_upgrade",
            "phase_number": 5,
            "reason": "deploy drain",
        }
        job = _job()
        db = _EndpointDB(job, fail_initial_freeze_write=True)
        body = orchestrator.main.JobCompleteRequest(
            should_stop=True,
            goal_achieved=False,
            freeze_data=freeze,
        )

        with ExitStack() as stack:
            _patch_completion(stack, db)
            handled = await b08_helpers.complete_job(MagicMock(), JOB_ID, body)

        [(sql, args)] = db.class_a_statements()
        normalized = _normalized(sql)
        assert "assigned_agent_id = $2" in normalized
        assert "jsonb_build_object('last_freeze_data', $3::jsonb)" in normalized
        assert "freeze_data = NULL" in normalized
        assert json.loads(args[2]) == freeze
        assert handled["new_status"] == "paused"
        assert handled["actions"] == [
            "status -> paused",
            "cleared agent on paused job (re-dispatchable)",
            "freeze stashed to context (auto-redispatch)",
        ]
        assert job["assigned_agent_id"] is None
        assert job["freeze_data"] is None
        assert job["context"]["last_freeze_data"] == freeze
        assert all("last_freeze_data" not in update for update in db.context_writes)

    @pytest.mark.asyncio
    async def test_vm_upgrade_pause_keeps_freeze_in_the_same_row(self):
        freeze = {
            "freeze_type": "vm_upgrade_required",
            "command": "apt install example",
            "reason": "needs packages",
        }
        job = _job()
        db = _EndpointDB(job)
        body = orchestrator.main.JobCompleteRequest(
            should_stop=True,
            goal_achieved=False,
            freeze_data=freeze,
        )

        def close_capture(coroutine, **_kwargs):
            coroutine.close()
            return MagicMock()

        with ExitStack() as stack:
            _patch_completion(stack, db)
            stack.enter_context(
                patch("orchestrator.main._check_vm_permission", AsyncMock())
            )
            stack.enter_context(
                patch(
                    "orchestrator.main.sudo_gate.insert_vm_upgrade_request",
                    AsyncMock(return_value=None),
                )
            )
            stack.enter_context(
                patch(
                    "orchestrator.main.job_freeze_notification_service.notify_operator_freeze",
                    AsyncMock(),
                )
            )
            stack.enter_context(
                patch.object(
                    control_seams,
                    "capture_workspace_snapshot_for_freeze",
                    AsyncMock(),
                )
            )
            stack.enter_context(
                patch(
                    "orchestrator.main.asyncio.create_task",
                    MagicMock(side_effect=close_capture),
                )
            )
            handled = await b08_helpers.complete_job(MagicMock(), JOB_ID, body)

        [(sql, _args)] = db.class_a_statements()
        normalized = _normalized(sql)
        assert "assigned_agent_id = $2" in normalized
        assert "last_freeze_data" not in normalized
        assert "freeze_data = NULL" not in normalized
        assert "completed_at" not in normalized
        assert handled["new_status"] == "paused"
        assert handled["actions"][:2] == [
            "status -> paused",
            "cleared agent on paused job (re-dispatchable)",
        ]
        assert "freeze stashed to context (auto-redispatch)" not in handled["actions"]
        assert job["freeze_data"] == freeze


class TestDiffDecisionClassA:
    @pytest.mark.asyncio
    async def test_accept_uses_one_status_and_timestamp_update(self):
        job = _job(
            status="pending_review",
            project_id=PROJECT_ID,
            diff_status="pending",
            cloud_diff_baseline_commit="a" * 40,
        )
        project = {
            "id": PROJECT_ID,
            "name": "Cloud project",
            "main_cloud_backend": "opencloud",
            "main_cloud_folder_handle": "opaque-handle",
        }
        db = _EndpointDB(job, project=project)
        gitea = MagicMock()
        gitea.is_initialized = True
        router = MagicMock()
        backend = MagicMock()
        backend.is_initialized = True
        router.for_backend.return_value = backend

        with ExitStack() as stack:
            stack.enter_context(patch("orchestrator.main.postgres_db", db))
            stack.enter_context(patch("orchestrator.main.gitea_client", gitea))
            stack.enter_context(patch("orchestrator.main.main_cloud_router", router))
            stack.enter_context(patch("orchestrator.main.vector_db", None))
            stack.enter_context(
                patch(
                    "orchestrator.services.job_cloud_baseline.get_diff_summary",
                    AsyncMock(
                        return_value={
                            "files": [],
                            "head_commit": "b" * 40,
                        }
                    ),
                )
            )
            stack.enter_context(
                patch(
                    "orchestrator.services.job_cloud_baseline.detect_external_mods",
                    AsyncMock(return_value=[]),
                )
            )
            stack.enter_context(
                patch(
                    "orchestrator.services.job_cloud_baseline.apply_diff_to_cloud",
                    AsyncMock(return_value={"applied": 2, "deleted": 1, "errors": []}),
                )
            )
            stack.enter_context(
                patch(
                    "orchestrator.services.job_cloud_baseline.project_folder_slug",
                    return_value="p",
                )
            )
            stack.enter_context(
                patch(
                    "orchestrator.services.completion.apply_terminal_job_side_effects",
                    AsyncMock(return_value={"actions": []}),
                )
            )
            result = await job_diff_routes.accept_job_diff(
                MagicMock(),
                JOB_ID,
                dependencies=dataclasses.replace(
                    orchestrator.main._job_diff_dependencies(),
                    require_job_access=AsyncMock(return_value=({}, job)),
                ),
            )

        [(sql, _args)] = db.class_a_statements()
        assert (
            "completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP)"
            in _normalized(sql)
        )
        assert sum("completed_at" in sql for sql, _ in db.statements) == 1
        assert result["status"] == "completed"
        assert result["diff_status"] == "accepted"

    @pytest.mark.asyncio
    async def test_reject_uses_one_status_and_timestamp_update(self):
        job = _job(
            status="pending_review",
            project_id=PROJECT_ID,
            diff_status="pending",
        )
        db = _EndpointDB(job)
        gitea = MagicMock()

        with ExitStack() as stack:
            stack.enter_context(patch("orchestrator.main.postgres_db", db))
            stack.enter_context(patch("orchestrator.main.gitea_client", gitea))
            stack.enter_context(patch("orchestrator.main.vector_db", None))
            stack.enter_context(
                patch(
                    "orchestrator.services.completion.apply_terminal_job_side_effects",
                    AsyncMock(return_value={"actions": []}),
                )
            )
            result = await job_diff_routes.reject_job_diff(
                MagicMock(),
                JOB_ID,
                dependencies=dataclasses.replace(
                    orchestrator.main._job_diff_dependencies(),
                    require_job_access=AsyncMock(return_value=({}, job)),
                ),
            )

        [(sql, _args)] = db.class_a_statements()
        assert (
            "completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP)"
            in _normalized(sql)
        )
        assert sum("completed_at" in sql for sql, _ in db.statements) == 1
        assert result["status"] == "completed"
        assert result["diff_status"] == "rejected"
