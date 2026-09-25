"""Tests for the vm_upgrade decision paths (reap-and-restore fix spec).

Covers the orchestrator-side decision plumbing added by
knowledge-base/knowledge/issues/vm_upgrade_pause_workspace_reaped_before_approval.md:

- `_job_frozen_for_vm_upgrade` — freeze-shape predicate.
- `_apply_vm_upgrade_decision` — the single driver behind ALL four decision
  endpoints: first-decider-wins, expired-rejects-late-decisions, idempotent
  re-drive of wedged jobs, and the approve/deny job actions (pre-fix, the
  generic endpoints and the MCP tools flipped the row and wedged the job).
- `_resume_job_without_vm_internal` — the deny / resume-without-vm arm:
  sticky denial context + reasoned queued feedback + Continue-as-New.
- `_apply_sticky_sudo_denial` — dispatch-time sudo gate flip so the replayed
  command gets a reasoned block instead of re-freezing.
- `_fail_expired_vm_upgrade_jobs` — vm_upgrade_expired fail-loud arm.

Follows the `test_admin_vm_controls.py` harness: import `main` with the
module-level singletons mocked per-test; no TestClient / DB.
"""

from __future__ import annotations

from tests import _b09_control_seams as control_seams

import json
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
from fastapi import HTTPException
from orchestrator.services import job_controls as job_controls_module
from orchestrator.services import job_dispatcher as job_dispatcher_module
from orchestrator.services import job_workspace_runtime as job_workspace_runtime_module
from orchestrator.services import sudo_gate as sudo_gate_module
from orchestrator.services import workspace as workspace_module

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

import orchestrator.main as orch_main  # noqa: E402

MODULE = "orchestrator.main"

JOB_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
REQ_ID = "11111111-2222-3333-4444-555555555555"


def _vm_upgrade_row(
    *,
    status: str = "pending",
    expires_in_s: int = 3600,
    job_id: str = JOB_ID,
) -> dict:
    return {
        "id": REQ_ID,
        "job_id": job_id,
        "status": status,
        "request_type": "vm_upgrade",
        "expires_at": datetime.now(timezone.utc) + timedelta(seconds=expires_in_s),
        "command": "sudo docker --version",
    }


def _frozen_job(*, status: str = "paused") -> dict:
    return {
        "id": JOB_ID,
        "status": status,
        "context": {},
        "freeze_data": {
            "freeze_type": "vm_upgrade_required",
            "command": "sudo docker --version",
        },
    }


# =============================================================================
# _job_frozen_for_vm_upgrade
# =============================================================================


class TestJobFrozenPredicate:
    def test_dict_freeze(self):
        assert control_seams.job_frozen_for_vm_upgrade(_frozen_job()) is True

    def test_str_freeze(self):
        job = _frozen_job()
        job["freeze_data"] = json.dumps(job["freeze_data"])
        assert control_seams.job_frozen_for_vm_upgrade(job) is True

    def test_other_freeze_type(self):
        job = _frozen_job()
        job["freeze_data"] = {"freeze_type": "job_complete"}
        assert control_seams.job_frozen_for_vm_upgrade(job) is False

    def test_no_freeze(self):
        job = _frozen_job()
        job["freeze_data"] = None
        assert control_seams.job_frozen_for_vm_upgrade(job) is False

    def test_none_job(self):
        assert control_seams.job_frozen_for_vm_upgrade(None) is False


# =============================================================================
# _apply_vm_upgrade_decision
# =============================================================================


class TestApplyVmUpgradeDecision:
    @pytest.mark.asyncio
    async def test_completion_barrier_precedes_pending_decision_row_flip(self):
        gate = MagicMock()
        gate.approve_request = AsyncMock()
        blocked = AsyncMock(side_effect=HTTPException(409, "completion finalizing"))

        with (
            patch.object(sudo_gate_module, "sudo_gate", gate),
            patch.object(
                orch_main.app.state.resources.completion_control_boundary,
                "guard",
                blocked,
            ),
        ):
            with pytest.raises(HTTPException) as exc:
                await control_seams.apply_vm_upgrade_decision(
                    REQ_ID,
                    _vm_upgrade_row(),
                    approve=True,
                    upgrade=True,
                    reason="ok",
                    decided_by="op",
                )

        assert exc.value.status_code == 409
        blocked.assert_awaited_once_with(JOB_ID, source="sudo_vm_decision")
        gate.approve_request.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_pending_approve_upgrades_job(self):
        row = _vm_upgrade_row()
        gate = MagicMock()
        gate.approve_request = AsyncMock(
            return_value={"id": REQ_ID, "status": "approved", "job_id": JOB_ID}
        )
        upgrade = AsyncMock(return_value={"status": "approved_vm_upgrade"})
        with (
            patch.object(sudo_gate_module, "sudo_gate", gate),
            patch.object(
                job_controls_module.JobControlOperations,
                "_upgrade_job_to_vm_internal",
                upgrade,
            ),
        ):
            result = await control_seams.apply_vm_upgrade_decision(
                REQ_ID, row, approve=True, upgrade=True, reason="ok", decided_by="op"
            )
        gate.approve_request.assert_awaited_once_with(
            REQ_ID, reason="ok", decided_by="op"
        )
        upgrade.assert_awaited_once_with(JOB_ID)
        assert result["status"] == "approved"
        assert result["job_action"] == {"status": "approved_vm_upgrade"}

    @pytest.mark.asyncio
    async def test_pending_deny_resumes_without_vm(self):
        row = _vm_upgrade_row()
        gate = MagicMock()
        gate.deny_request = AsyncMock(
            return_value={"id": REQ_ID, "status": "denied", "job_id": JOB_ID}
        )
        no_vm = AsyncMock(return_value={"status": "denied_vm_upgrade"})
        with (
            patch.object(sudo_gate_module, "sudo_gate", gate),
            patch.object(
                job_controls_module.JobControlOperations,
                "_resume_job_without_vm_internal",
                no_vm,
            ),
        ):
            result = await control_seams.apply_vm_upgrade_decision(
                REQ_ID, row, approve=False, upgrade=False, reason="no", decided_by="op"
            )
        gate.deny_request.assert_awaited_once_with(REQ_ID, reason="no", decided_by="op")
        no_vm.assert_awaited_once_with(
            JOB_ID, decided_by="op", reason="no", denied=True
        )
        assert result["status"] == "denied"

    @pytest.mark.asyncio
    async def test_expired_pending_rejects_late_decision(self):
        # Stale-token model: TTL lapsed but the sweeper hasn't flipped the row
        # yet — the decision must be rejected, not raced through.
        row = _vm_upgrade_row(expires_in_s=-60)
        gate = MagicMock()
        gate.approve_request = AsyncMock()
        with (
            patch.object(sudo_gate_module, "sudo_gate", gate),
            pytest.raises(HTTPException) as exc,
        ):
            await control_seams.apply_vm_upgrade_decision(
                REQ_ID, row, approve=True, upgrade=True, reason="", decided_by="op"
            )
        assert exc.value.status_code == 409
        gate.approve_request.assert_not_called()

    @pytest.mark.asyncio
    async def test_already_decided_and_driven_is_noop(self):
        row = _vm_upgrade_row(status="approved")
        db = MagicMock()
        db.get_job = AsyncMock(return_value={"id": JOB_ID, "freeze_data": None})
        upgrade = AsyncMock()
        with (
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            patch.object(
                job_controls_module.JobControlOperations,
                "_upgrade_job_to_vm_internal",
                upgrade,
            ),
        ):
            result = await control_seams.apply_vm_upgrade_decision(
                REQ_ID, row, approve=True, upgrade=True, reason="", decided_by="op"
            )
        upgrade.assert_not_called()
        assert "no-op" in result["note"]

    @pytest.mark.asyncio
    async def test_already_approved_but_still_frozen_redrives(self):
        # Recovers jobs wedged by the historical row-flip-only endpoints (and
        # upgrades that failed transiently after the flip).
        row = _vm_upgrade_row(status="approved")
        db = MagicMock()
        db.get_job = AsyncMock(return_value=_frozen_job())
        upgrade = AsyncMock(return_value={"status": "approved_vm_upgrade"})
        with (
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            patch.object(
                job_controls_module.JobControlOperations,
                "_upgrade_job_to_vm_internal",
                upgrade,
            ),
        ):
            result = await control_seams.apply_vm_upgrade_decision(
                REQ_ID, row, approve=True, upgrade=True, reason="", decided_by="op"
            )
        upgrade.assert_awaited_once_with(JOB_ID)
        assert result["status"] == "approved"

    @pytest.mark.asyncio
    async def test_conflicting_second_decision_is_409(self):
        # First-decider-wins: a deny after an approve is a visible conflict.
        row = _vm_upgrade_row(status="approved")
        with pytest.raises(HTTPException) as exc:
            await control_seams.apply_vm_upgrade_decision(
                REQ_ID, row, approve=False, upgrade=False, reason="", decided_by="op"
            )
        assert exc.value.status_code == 409

    @pytest.mark.asyncio
    async def test_auto_denied_row_redrivable_by_deny(self):
        # The auto-deny fallback (resume failed after the auto_denied insert)
        # leaves an auto_denied row + a frozen job; a manual deny re-drives it.
        row = _vm_upgrade_row(status="auto_denied")
        db = MagicMock()
        db.get_job = AsyncMock(return_value=_frozen_job())
        no_vm = AsyncMock(return_value={"status": "denied_vm_upgrade"})
        with (
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            patch.object(
                job_controls_module.JobControlOperations,
                "_resume_job_without_vm_internal",
                no_vm,
            ),
        ):
            await control_seams.apply_vm_upgrade_decision(
                REQ_ID, row, approve=False, upgrade=False, reason="r", decided_by="op"
            )
        no_vm.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_lost_claim_race_is_409(self):
        row = _vm_upgrade_row()
        gate = MagicMock()
        gate.approve_request = AsyncMock(
            return_value={"error": "Request status is 'denied', not 'pending'"}
        )
        with (
            patch.object(sudo_gate_module, "sudo_gate", gate),
            pytest.raises(HTTPException) as exc,
        ):
            await control_seams.apply_vm_upgrade_decision(
                REQ_ID, row, approve=True, upgrade=True, reason="", decided_by="op"
            )
        assert exc.value.status_code == 409

    @pytest.mark.asyncio
    async def test_missing_job_id_is_400(self):
        row = _vm_upgrade_row()
        row["job_id"] = None
        with pytest.raises(HTTPException) as exc:
            await control_seams.apply_vm_upgrade_decision(
                REQ_ID, row, approve=True, upgrade=True, reason="", decided_by="op"
            )
        assert exc.value.status_code == 400


# =============================================================================
# _resume_job_without_vm_internal
# =============================================================================


def _db_with_execute(job: dict):
    db = MagicMock()
    db.get_job = AsyncMock(return_value=job)
    conn = AsyncMock()
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    db.acquire = MagicMock(return_value=ctx)
    return db, conn


class TestResumeWithoutVm:
    @pytest.mark.asyncio
    async def test_circuit_trip_refuses_before_any_resume_write(self, tmp_path):
        job = _frozen_job()
        job["context"] = {
            "_lease_recovery": {
                "state": "tripped",
                "generation": "incident-1",
            }
        }
        db, conn = _db_with_execute(job)
        ws = MagicMock()
        ws.base_path = tmp_path
        with (
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            patch.object(workspace_module, "workspace_service", ws),
            patch.object(
                job_dispatcher_module, "trigger_dispatch", MagicMock()
            ) as trigger,
            pytest.raises(HTTPException) as exc,
        ):
            await control_seams.resume_job_without_vm_internal(JOB_ID)

        assert exc.value.status_code == 409
        conn.execute.assert_not_awaited()
        trigger.assert_not_called()

    @pytest.mark.asyncio
    async def test_deny_writes_sticky_denial_and_requeues(self, tmp_path):
        db, conn = _db_with_execute(_frozen_job())
        ws = MagicMock()
        ws.base_path = tmp_path
        trigger = MagicMock()
        with (
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            patch.object(workspace_module, "workspace_service", ws),
            patch.object(job_dispatcher_module, "trigger_dispatch", trigger),
        ):
            result = await control_seams.resume_job_without_vm_internal(
                JOB_ID, decided_by="alice", reason="not needed", denied=True
            )
        sql, payload, job_id = conn.execute.await_args.args
        assert "freeze_data = NULL" in sql
        assert "status = 'paused'" in sql
        assert "assigned_agent_id = NULL" in sql
        assert job_id == JOB_ID
        merged = json.loads(payload)
        assert merged["sudo_denial"]["denied"] is True
        assert merged["sudo_denial"]["decided_by"] == "alice"
        assert merged["sudo_denial"]["command"] == "sudo docker --version"
        assert "DENIED by alice" in merged["queued_feedback"]
        assert "not needed" in merged["queued_feedback"]
        trigger.assert_called_once()
        assert result["status"] == "denied_vm_upgrade"

    @pytest.mark.asyncio
    async def test_no_vm_approve_flavour(self, tmp_path):
        db, conn = _db_with_execute(_frozen_job())
        ws = MagicMock()
        ws.base_path = tmp_path
        with (
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            patch.object(workspace_module, "workspace_service", ws),
            patch.object(job_dispatcher_module, "trigger_dispatch", MagicMock()),
        ):
            result = await control_seams.resume_job_without_vm_internal(
                JOB_ID, decided_by="alice", reason="", denied=False
            )
        merged = json.loads(conn.execute.await_args.args[1])
        assert merged["sudo_denial"]["denied"] is False
        assert "WITHOUT a VM" in merged["queued_feedback"]
        assert result["status"] == "resumed_without_vm"

    @pytest.mark.asyncio
    async def test_wrong_status_is_400(self, tmp_path):
        db, _ = _db_with_execute(_frozen_job(status="processing"))
        ws = MagicMock()
        ws.base_path = tmp_path
        with (
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            patch.object(workspace_module, "workspace_service", ws),
            pytest.raises(HTTPException) as exc,
        ):
            await control_seams.resume_job_without_vm_internal(JOB_ID)
        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_flag_on_finalizer_owner_bypasses_only_its_pinned_command(
        self, tmp_path
    ):
        job = {**_frozen_job(), "execution_lane": "pinned"}
        db, conn = _db_with_execute(job)
        transaction = MagicMock()
        transaction.__aenter__ = AsyncMock(return_value=None)
        transaction.__aexit__ = AsyncMock(return_value=False)
        conn.transaction = MagicMock(return_value=transaction)
        db._completion_resume_blocked_on_conn = AsyncMock(return_value=False)
        guard = AsyncMock(side_effect=AssertionError("owner used public guard"))
        ws = MagicMock()
        ws.base_path = tmp_path

        with (
            patch.object(
                orch_main.app.state.resources.settings,
                "completion_commands_enabled",
                True,
            ),
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            patch.object(workspace_module, "workspace_service", ws),
            patch.object(
                orch_main.app.state.resources.completion_control_boundary,
                "guard",
                guard,
            ),
            patch.object(job_dispatcher_module, "trigger_dispatch", MagicMock()),
        ):
            result = await control_seams.resume_job_without_vm_internal(
                JOB_ID,
                decided_by="system",
                reason="not allowed",
                completion_owner_command_id=REQ_ID,
                completion_owner="finalizer-owner",
            )

        guard.assert_not_awaited()
        db._completion_resume_blocked_on_conn.assert_awaited_once_with(
            conn,
            UUID(JOB_ID),
            completion_owner_command_id=REQ_ID,
            completion_owner="finalizer-owner",
        )
        assert "status = 'paused'" in conn.execute.await_args.args[0]
        assert result["status"] == "denied_vm_upgrade"

    @pytest.mark.asyncio
    async def test_flag_on_finalizer_owner_reaches_stateless_resume_cas(self, tmp_path):
        job = {
            **_frozen_job(),
            "execution_lane": "stateless",
            "priority": 4,
            "user_id": None,
        }
        db = MagicMock()
        db.get_job = AsyncMock(return_value=job)
        db.queue_stateless_job_for_resume = AsyncMock(return_value=True)
        guard = AsyncMock(side_effect=AssertionError("owner used public guard"))
        ws = MagicMock()
        ws.base_path = tmp_path

        with (
            patch.object(
                orch_main.app.state.resources.settings,
                "completion_commands_enabled",
                True,
            ),
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            patch.object(workspace_module, "workspace_service", ws),
            patch.object(
                orch_main.app.state.resources.completion_control_boundary,
                "guard",
                guard,
            ),
            patch.object(job_dispatcher_module, "trigger_dispatch", MagicMock()),
        ):
            await control_seams.resume_job_without_vm_internal(
                JOB_ID,
                completion_owner_command_id=REQ_ID,
                completion_owner="finalizer-owner",
            )

        guard.assert_not_awaited()
        db.queue_stateless_job_for_resume.assert_awaited_once()
        assert db.queue_stateless_job_for_resume.await_args.kwargs == {
            "priority": 4,
            "fair_key": None,
            "expected_status": "paused",
            "completion_commands_enabled": True,
            "completion_owner_command_id": REQ_ID,
            "completion_owner": "finalizer-owner",
        }


# =============================================================================
# _apply_sticky_sudo_denial (dispatch-time gate flip)
# =============================================================================


class TestApplyStickySudoDenial:
    def _job_with_denial(self) -> dict:
        return {
            "id": JOB_ID,
            "context": {
                "sudo_denial": {
                    "denied": True,
                    "decided_by": "alice",
                    "reason": "policy",
                    "command": "sudo apt install x",
                }
            },
        }

    def test_denial_flips_gate_to_reasoned_block(self):
        co = job_workspace_runtime_module.apply_sticky_sudo_denial(
            self._job_with_denial(), {}
        )
        assert co["shell"]["sudo_action"] == "block"
        assert "alice" in co["shell"]["sudo_block_message"]
        assert "policy" in co["shell"]["sudo_block_message"]

    def test_vm_backend_untouched(self):
        co = {"workspace": {"backend": "vm"}, "shell": {"sudo_action": "allow"}}
        out = job_workspace_runtime_module.apply_sticky_sudo_denial(
            self._job_with_denial(), co
        )
        assert out["shell"]["sudo_action"] == "allow"

    def test_no_denial_untouched(self):
        co = {"shell": {"sudo_action": "freeze"}}
        out = job_workspace_runtime_module.apply_sticky_sudo_denial(
            {"id": JOB_ID, "context": {}}, co
        )
        assert out["shell"]["sudo_action"] == "freeze"

    def test_none_override_created_when_denied(self):
        out = job_workspace_runtime_module.apply_sticky_sudo_denial(
            self._job_with_denial(), None
        )
        assert out["shell"]["sudo_action"] == "block"

    def test_str_context_parsed(self):
        job = self._job_with_denial()
        job["context"] = json.dumps(job["context"])
        out = job_workspace_runtime_module.apply_sticky_sudo_denial(job, {})
        assert out["shell"]["sudo_action"] == "block"


# =============================================================================
# user_can_use_vm admin guard (fix 5.1)
# =============================================================================


class TestUserCanUseVmAdminGuard:
    @pytest.mark.asyncio
    async def test_admin_short_circuits_before_any_db_access(self):
        from orchestrator.database.postgres import PostgresDB

        db = PostgresDB.__new__(PostgresDB)  # no pool — DB access would raise
        assert await db.user_can_use_vm({"is_admin": True, "can_use_vm": False})

    @pytest.mark.asyncio
    async def test_non_admin_falls_back_to_column(self):
        from orchestrator.database.postgres import PostgresDB

        db = PostgresDB.__new__(PostgresDB)
        db.list_grants_for_scopes = AsyncMock(side_effect=RuntimeError("no db"))
        assert (
            await db.user_can_use_vm(
                {"id": JOB_ID, "is_admin": False, "can_use_vm": True}
            )
            is True
        )
        assert (
            await db.user_can_use_vm(
                {"id": JOB_ID, "is_admin": False, "can_use_vm": False}
            )
            is False
        )


# =============================================================================
# _fail_expired_vm_upgrade_jobs
# =============================================================================


class TestFailExpiredVmUpgradeJobs:
    @pytest.mark.asyncio
    async def test_fails_wedged_jobs_loudly(self):
        db = MagicMock()
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[{"id": JOB_ID}])
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=conn)
        ctx.__aexit__ = AsyncMock(return_value=False)
        db.acquire = MagicMock(return_value=ctx)
        with patch.object(orch_main.app.state.resources, "postgres_db", db):
            count = await control_seams.fail_expired_vm_upgrade_jobs()
        assert count == 1
        sql = conn.execute.await_args.args[0]
        assert "status = 'failed'" in sql
        assert "freeze_data = NULL" in sql
        assert "vm_upgrade_expired" in conn.execute.await_args.args[2]

    @pytest.mark.asyncio
    async def test_no_wedged_jobs_is_zero(self):
        db = MagicMock()
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[])
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=conn)
        ctx.__aexit__ = AsyncMock(return_value=False)
        db.acquire = MagicMock(return_value=ctx)
        with patch.object(orch_main.app.state.resources, "postgres_db", db):
            assert await control_seams.fail_expired_vm_upgrade_jobs() == 0
        conn.execute.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("enabled", [False, True])
    async def test_completion_route_exclusion_is_flag_gated(self, enabled):
        db = MagicMock()
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[{"id": JOB_ID}])
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=conn)
        ctx.__aexit__ = AsyncMock(return_value=False)
        db.acquire = MagicMock(return_value=ctx)
        with (
            patch.object(orch_main.app.state.resources, "postgres_db", db),
            patch.object(
                orch_main.app.state.resources.settings,
                "completion_commands_enabled",
                enabled,
            ),
        ):
            assert await control_seams.fail_expired_vm_upgrade_jobs() == 1

        relation = "job_completion_sweep_exclusions"
        select_sql = conn.fetch.await_args.args[0]
        update_sql = conn.execute.await_args.args[0]
        assert (relation in select_sql) is enabled
        assert (relation in update_sql) is enabled
