"""Owner-seam tests for extracted job, VM, and sudo controls."""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI, HTTPException
import pytest

import orchestrator.main as main
from orchestrator.routers import job_controls as routes
from orchestrator.services.job_controls import (
    JobControlDependencies,
    JobControlOperations,
)


JOB_ID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
REQUEST_ID = "11111111-2222-4333-8444-555555555555"


def _completion_control() -> MagicMock:
    control = MagicMock()
    control.guard = AsyncMock()
    control.claim = AsyncMock(return_value=None)
    control.abort = AsyncMock()
    control.resume_guard_kwargs = MagicMock(return_value={})
    return control


def _operations(
    tmp_path: Path,
    *,
    store: object | None = None,
    sudo_gate: object | None = None,
    completion_control: object | None = None,
    completion_commands_enabled: bool = False,
) -> JobControlOperations:
    workspace = MagicMock()
    workspace.base_path = tmp_path
    snapshots = MagicMock()
    snapshots.is_available = False
    return JobControlOperations(
        JobControlDependencies(
            store=store or MagicMock(),
            logger=logging.getLogger(__name__),
            completion_control=completion_control or _completion_control(),
            completion_commands_enabled=lambda: completion_commands_enabled,
            completion_control_active_sql=lambda field: f"active({field})",
            completion_control_owned_active_sql=(
                lambda field, parameter: f"owned({field},{parameter})"
            ),
            sudo_gate=sudo_gate or MagicMock(),
            workspace=workspace,
            snapshots=snapshots,
            ide_sessions=MagicMock(),
            vm_provisioner=MagicMock(),
            forge=MagicMock(),
            vector_store=MagicMock(),
            subjob_output=MagicMock(),
            subjob_output_dependencies=MagicMock(return_value=MagicMock()),
            authorize_runtime_actor_request=AsyncMock(),
            redispatch_livelock_trip=MagicMock(return_value=None),
            user_experts_enabled=AsyncMock(return_value=False),
            resolve_default_models=AsyncMock(return_value={}),
            prefetch_roster_refs=AsyncMock(return_value={}),
            resolve_config=MagicMock(),
            canonical_config_name=lambda value: value,
            enforce_dispatch_grants=AsyncMock(),
            grant_violations_detail=MagicMock(),
            prepare_job_workspace_runtime=AsyncMock(
                side_effect=lambda job: ("ready", job, None)
            ),
            resume_missing_workspace=MagicMock(return_value=None),
            workspace_context_keys={"container": "workspace_container", "vm": "vm"},
            prepare_job_repository_before_claim=AsyncMock(return_value=True),
            resume_job_on_agent=AsyncMock(return_value=True),
            trigger_dispatch=MagicMock(),
            resolve_job_notifications=AsyncMock(),
            maybe_wake_session=AsyncMock(),
            kick_session_wake_drain=MagicMock(),
            get_container_context=MagicMock(return_value={}),
            get_vm_context=MagicMock(return_value={}),
            recovery_store=MagicMock(
                unresolved_participation=AsyncMock(return_value=None),
                retry_paused=AsyncMock(),
            ),
        )
    )


def _vm_upgrade_row(*, status: str = "pending", expires_in: int = 3600):
    return {
        "id": REQUEST_ID,
        "job_id": JOB_ID,
        "status": status,
        "request_type": "vm_upgrade",
        "expires_at": datetime.now(timezone.utc) + timedelta(seconds=expires_in),
    }


def _frozen_job(*, execution_lane: str = "pinned"):
    return {
        "id": JOB_ID,
        "status": "paused",
        "execution_lane": execution_lane,
        "context": {},
        "freeze_data": {
            "freeze_type": "vm_upgrade_required",
            "command": "sudo docker --version",
        },
    }


def test_vm_upgrade_freeze_predicate_accepts_dict_and_json(tmp_path: Path) -> None:
    operations = _operations(tmp_path)
    job = _frozen_job()
    assert operations.job_frozen_for_vm_upgrade(job)
    job["freeze_data"] = json.dumps(job["freeze_data"])
    assert operations.job_frozen_for_vm_upgrade(job)
    job["freeze_data"] = "not-json"
    assert not operations.job_frozen_for_vm_upgrade(job)


@pytest.mark.asyncio
async def test_completion_barrier_precedes_sudo_decision_row_flip(
    tmp_path: Path,
) -> None:
    gate = MagicMock()
    gate.approve_request = AsyncMock()
    control = _completion_control()
    control.guard = AsyncMock(side_effect=HTTPException(409, "completion finalizing"))
    operations = _operations(
        tmp_path,
        sudo_gate=gate,
        completion_control=control,
    )

    with pytest.raises(HTTPException) as exc:
        await operations.apply_vm_upgrade_decision(
            REQUEST_ID,
            _vm_upgrade_row(),
            approve=True,
            upgrade=True,
            reason="ok",
            decided_by="operator",
        )

    assert exc.value.status_code == 409
    control.guard.assert_awaited_once_with(JOB_ID, source="sudo_vm_decision")
    gate.approve_request.assert_not_awaited()
    control.abort.assert_awaited_once_with(None)


@pytest.mark.asyncio
async def test_pending_sudo_approval_drives_vm_upgrade(tmp_path: Path) -> None:
    gate = MagicMock()
    gate.approve_request = AsyncMock(return_value={"status": "approved"})
    operations = _operations(tmp_path, sudo_gate=gate)
    upgrade = AsyncMock(return_value={"status": "approved_vm_upgrade"})

    with patch.object(
        JobControlOperations,
        "_upgrade_job_to_vm_internal",
        upgrade,
    ):
        result = await operations.apply_vm_upgrade_decision(
            REQUEST_ID,
            _vm_upgrade_row(),
            approve=True,
            upgrade=True,
            reason="approved",
            decided_by="alice",
        )

    gate.approve_request.assert_awaited_once_with(
        REQUEST_ID,
        reason="approved",
        decided_by="alice",
    )
    upgrade.assert_awaited_once_with(JOB_ID)
    assert result["job_action"] == {"status": "approved_vm_upgrade"}


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_vm_upgrade_expiration_preserves_completion_guards(
    tmp_path: Path,
    enabled: bool,
) -> None:
    connection = AsyncMock()
    connection.fetch = AsyncMock(return_value=[{"id": JOB_ID}])

    @asynccontextmanager
    async def acquire():
        yield connection

    store = MagicMock()
    store.acquire = acquire
    operations = _operations(
        tmp_path,
        store=store,
        completion_commands_enabled=enabled,
    )

    assert await operations.fail_expired_vm_upgrade_jobs() == 1

    select_sql = connection.fetch.await_args.args[0]
    update_sql = connection.execute.await_args.args[0]
    assert ("job_completion_sweep_exclusions" in select_sql) is enabled
    assert ("job_completion_sweep_exclusions" in update_sql) is enabled
    assert ("active(j.context)" in select_sql) is enabled
    assert "vm_upgrade_expired" in connection.execute.await_args.args[2]


@pytest.mark.asyncio
async def test_resume_without_vm_records_sticky_denial_and_dispatches(
    tmp_path: Path,
) -> None:
    connection = AsyncMock()

    @asynccontextmanager
    async def acquire():
        yield connection

    store = MagicMock()
    store.get_job = AsyncMock(return_value=_frozen_job())
    store.acquire = acquire
    operations = _operations(tmp_path, store=store)

    result = await operations.resume_job_without_vm_internal(
        JOB_ID,
        decided_by="alice",
        reason="not needed",
        denied=True,
    )

    sql, payload, observed_job_id = connection.execute.await_args.args
    assert "freeze_data = NULL" in sql
    assert "status = 'paused'" in sql
    assert observed_job_id == JOB_ID
    merged = json.loads(payload)
    assert merged["sudo_denial"]["denied"] is True
    assert merged["sudo_denial"]["decided_by"] == "alice"
    assert "DENIED by alice" in merged["queued_feedback"]
    operations.dependencies.trigger_dispatch.assert_called_once_with()
    operations.dependencies.resolve_job_notifications.assert_awaited_once_with(
        JOB_ID,
        user=None,
        hook="vm_denied",
    )
    assert result["status"] == "denied_vm_upgrade"


@pytest.mark.asyncio
async def test_internal_resume_refuses_terminal_job_before_control_claim(
    tmp_path: Path,
) -> None:
    store = MagicMock()
    store.get_job = AsyncMock(
        return_value={"id": JOB_ID, "status": "completed", "execution_lane": "pinned"}
    )
    control = _completion_control()
    operations = _operations(tmp_path, store=store, completion_control=control)

    assert not await operations.internal_resume_job(JOB_ID, "try again")
    control.guard.assert_not_awaited()
    store.queue_job_for_resume.assert_not_called()


@pytest.mark.asyncio
async def test_command_mode_resume_queues_stateless_before_agent_delivery(
    tmp_path: Path,
) -> None:
    job = {
        **_frozen_job(execution_lane="stateless"),
        "priority": 7,
        "user_id": "user-a",
    }
    store = MagicMock()
    store.queue_stateless_job_for_resume = AsyncMock(return_value=True)
    operations = _operations(
        tmp_path,
        store=store,
        completion_commands_enabled=True,
    )

    result = await operations.resume_job_internal(
        JOB_ID,
        user={"id": "user-a"},
        job=job,
    )

    assert result == {
        "status": "queued",
        "message": "Stateless job queued for worker claim",
        "job_id": JOB_ID,
    }
    store.queue_stateless_job_for_resume.assert_awaited_once_with(
        JOB_ID,
        None,
        priority=7,
        fair_key="user-a",
        expected_status="paused",
        lift_operator_pause_hold="",
    )
    operations.dependencies.resume_job_on_agent.assert_not_awaited()
    operations.dependencies.trigger_dispatch.assert_called_once_with()


def _operator_paused_job() -> dict:
    return {
        "id": JOB_ID,
        "status": "paused",
        "execution_lane": "pinned",
        "assigned_agent_id": None,
        "context": {"_operator_pause_hold": {"version": 1, "hold_id": "hold-1"}},
    }


@pytest.mark.asyncio
async def test_command_mode_resume_lifts_only_the_observed_operator_pause_hold(
    tmp_path: Path,
) -> None:
    store = MagicMock()
    store.queue_job_for_resume = AsyncMock(return_value=True)
    operations = _operations(tmp_path, store=store, completion_commands_enabled=True)

    result = await operations.resume_job_internal(
        JOB_ID, user={"id": "user-a"}, job=_operator_paused_job()
    )

    assert result["status"] == "queued"
    store.queue_job_for_resume.assert_awaited_once_with(
        JOB_ID, None, expected_status="paused", lift_operator_pause_hold="hold-1"
    )


@pytest.mark.asyncio
async def test_direct_resume_lifts_the_hold_in_its_claim_then_requeues_unheld(
    tmp_path: Path,
) -> None:
    store = MagicMock()
    store.get_agent = AsyncMock(
        return_value={"id": "agent-a", "status": "ready", "pod_ip": "10.0.0.1"}
    )
    store.claim_job_for_agent = AsyncMock(return_value=True)
    store.queue_job_for_resume = AsyncMock(return_value=True)
    operations = _operations(tmp_path, store=store)
    operations.dependencies.resume_job_on_agent.return_value = False

    result = await operations.resume_job_internal(
        JOB_ID,
        user={"id": "user-a"},
        job=_operator_paused_job(),
        request=main.JobResumeRequest(agent_id="agent-a"),
    )

    assert result["status"] == "queued"
    store.claim_job_for_agent.assert_awaited_once_with(
        JOB_ID, "agent-a", allow_failed=True, lift_operator_pause_hold="hold-1"
    )
    # The claim consumed the hold; the fallback re-queue must find none, so
    # a pause that lands in between still wins that CAS.
    store.queue_job_for_resume.assert_awaited_once_with(
        JOB_ID, None, expected_status="processing", lift_operator_pause_hold=""
    )


@pytest.mark.asyncio
async def test_generic_resume_routes_unresolved_workspace_recovery_without_unparking(
    tmp_path: Path,
) -> None:
    operations = _operations(tmp_path)
    recovery_store = operations.dependencies.recovery_store
    recovery_store.unresolved_participation = AsyncMock(
        return_value={
            "operation_id": "11111111-2222-4333-8444-555555555555",
            "phase": "paused_attention",
        }
    )
    recovery_store.retry_paused = AsyncMock(
        return_value={"status": "recovering_workspace"}
    )
    job = _frozen_job(execution_lane="stateless")

    result = await operations.resume_job(
        JOB_ID,
        user={"id": "99999999-9999-4999-8999-999999999999"},
        job=job,
        request=None,
        req=MagicMock(),
    )

    assert result["status"] == "recovering_workspace"
    recovery_store.retry_paused.assert_awaited_once()
    operations.dependencies.completion_control.guard.assert_not_awaited()
    operations.dependencies.prepare_job_workspace_runtime.assert_not_awaited()


@pytest.mark.asyncio
async def test_public_vm_delete_stands_down_before_recovery_owned_cleanup(
    tmp_path: Path,
) -> None:
    operations = _operations(tmp_path)
    provisioner = operations.dependencies.vm_provisioner
    provisioner.lifecycle_available = True
    provisioner.capture_vm_teardown_identity = AsyncMock(
        return_value=SimpleNamespace(
            provision_generation="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            vm_uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
            rootdisk_pvc_uid="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        )
    )
    provisioner.release_vm_captured = AsyncMock()
    provisioner.delete_vm = AsyncMock(return_value=True)
    operations.dependencies.recovery_store.acquire_cleanup_permit = AsyncMock(
        return_value=SimpleNamespace(
            allowed=False,
            reason="workspace_recovery_unresolved",
        )
    )

    with pytest.raises(HTTPException) as refused:
        await operations.delete_vm(JOB_ID)

    assert refused.value.status_code == 409
    provisioner.capture_vm_teardown_identity.assert_awaited_once_with(
        JOB_ID, entity_type="job"
    )
    operations.dependencies.recovery_store.acquire_cleanup_permit.assert_awaited_once()
    provisioner.release_vm_captured.assert_not_awaited()
    provisioner.delete_vm.assert_not_awaited()


@pytest.mark.asyncio
async def test_public_vm_delete_retains_cleanup_permit_on_ambiguous_outcome(
    tmp_path: Path,
) -> None:
    operations = _operations(tmp_path)
    provisioner = operations.dependencies.vm_provisioner
    provisioner.lifecycle_available = True
    identity = SimpleNamespace(
        provision_generation="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        vm_uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        rootdisk_pvc_uid="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
    )
    provisioner.capture_vm_teardown_identity = AsyncMock(return_value=identity)
    provisioner.release_vm_captured = AsyncMock(
        return_value=SimpleNamespace(disposition="retry_pending")
    )
    recovery_store = operations.dependencies.recovery_store
    recovery_store.acquire_cleanup_permit = AsyncMock(
        return_value=SimpleNamespace(
            allowed=True,
            admission_id="dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        )
    )
    recovery_store.complete_cleanup_permit = AsyncMock()

    with pytest.raises(HTTPException) as pending:
        await operations.delete_vm(JOB_ID)

    assert pending.value.status_code == 500
    provisioner.release_vm_captured.assert_awaited_once()
    recovery_store.complete_cleanup_permit.assert_not_awaited()


@pytest.mark.asyncio
async def test_approve_phase_boundary_requeues_under_completion_claim(
    tmp_path: Path,
) -> None:
    job = {
        **_frozen_job(),
        "status": "pending_review",
        "freeze_data": {"freeze_type": "phase_boundary", "phase_number": 2},
    }
    store = MagicMock()
    store.queue_job_for_resume = AsyncMock(return_value=True)
    control = _completion_control()
    claim = MagicMock()
    claim.claim_id = "claim-a"
    control.claim = AsyncMock(return_value=claim)
    control.resume_guard_kwargs = MagicMock(return_value={"claim": "claim-a"})
    operations = _operations(tmp_path, store=store, completion_control=control)
    operations.dependencies.subjob_output.resolve_job_repo = AsyncMock(
        return_value=("repo", "job/branch")
    )

    result = await operations.approve_job_internal(
        JOB_ID,
        user={"id": "reviewer"},
        job=job,
    )

    assert result["status"] == "approved_continue"
    store.queue_job_for_resume.assert_awaited_once_with(
        JOB_ID,
        expected_status="pending_review",
        claim="claim-a",
    )
    operations.dependencies.trigger_dispatch.assert_called_once_with()
    control.abort.assert_not_awaited()


@pytest.mark.asyncio
async def test_upgrade_to_vm_commits_contract_before_dispatch(tmp_path: Path) -> None:
    job = _frozen_job()
    connection = AsyncMock()
    connection.fetchrow = AsyncMock(return_value={"id": JOB_ID})

    @asynccontextmanager
    async def transaction():
        yield

    connection.transaction = transaction

    @asynccontextmanager
    async def acquire():
        yield connection

    store = MagicMock()
    store.get_job = AsyncMock(return_value=job)
    store.acquire = acquire
    control = _completion_control()
    operations = _operations(tmp_path, store=store, completion_control=control)
    operations.dependencies.vm_provisioner.is_available = True
    operations.dependencies.vm_provisioner.mode = "same-cluster"

    result = await operations.upgrade_job_to_vm_internal(JOB_ID)

    sql = connection.fetchrow.await_args.args[0]
    assert "assignment_source" in connection.fetchrow.await_args.args[5]
    assert "status = 'paused', freeze_data = NULL" in sql
    assert "execution_lane = 'pinned'" in sql
    operations.dependencies.trigger_dispatch.assert_called_once_with()
    operations.dependencies.resolve_job_notifications.assert_awaited_once_with(
        JOB_ID,
        user=None,
        hook="vm_upgrade",
    )
    assert result["status"] == "approved_vm_upgrade"


def test_extracted_router_openapi_matches_original_surface() -> None:
    extracted = FastAPI()
    extracted.include_router(routes.router)
    expected_schema = main.app.openapi()
    actual_schema = extracted.openapi()
    paths = {
        "/api/vms",
        "/api/vms/{job_id}",
        "/api/sudo/events",
        "/api/sudo/requests",
        "/api/sudo/requests/{request_id}",
        "/api/sudo/requests/{request_id}/approve",
        "/api/sudo/requests/{request_id}/deny",
        "/api/sudo/requests/{request_id}/approve-upgrade",
        "/api/sudo/requests/{request_id}/resume-without-vm",
        "/api/sudo/rules",
        "/api/sudo/rules/{rule_id}",
        "/api/jobs/{job_id}/resume",
        "/api/jobs/{job_id}/workspace-recovery/retry",
        "/api/jobs/{job_id}/approve",
        "/api/jobs/{job_id}/upgrade-to-vm",
    }
    assert set(actual_schema["paths"]) == paths
    for path in paths:
        assert actual_schema["paths"][path] == expected_schema["paths"][path]


@pytest.mark.asyncio
async def test_vm_delete_router_preserves_owner_or_project_owner_gate(
    tmp_path: Path,
) -> None:
    operations = MagicMock(spec=JobControlOperations)
    operations.delete_vm = AsyncMock()
    store = MagicMock()
    store.get_user_role_in_project = AsyncMock(return_value="member")
    route_dependencies = routes.JobControlRouteDependencies(
        operations=operations,
        store=store,
        require_admin=AsyncMock(),
        require_job_access=AsyncMock(
            return_value=(
                {"id": "caller", "is_admin": False},
                {"id": JOB_ID, "user_id": "owner", "project_id": "project"},
            )
        ),
        require_internal_or_job_access=AsyncMock(),
        require_approved_user=AsyncMock(),
        require_sudo_request_authority=AsyncMock(),
        user_can_access_job_or_thread=AsyncMock(),
        mcp_scope_project_id=MagicMock(),
    )

    with pytest.raises(HTTPException) as exc:
        await routes.delete_vm(
            MagicMock(),
            JOB_ID,
            dependencies=route_dependencies,
        )

    assert exc.value.status_code == 403
    operations.delete_vm.assert_not_awaited()
