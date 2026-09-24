"""HTTP adapters for completion-command operator recovery verbs."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

# R1.B06: the four operator verbs moved to ``services/run_queue_admin`` with
# their routes in ``routers/run_queue_admin``. main's dependency factory still
# reads main's attributes at call time, so the monkeypatches below keep
# steering exactly what they steered before.
from orchestrator.schemas.run_queue_admin import (  # noqa: E402
    CompletionCommandForceResolveRequest,
)
from orchestrator.services import run_queue_admin  # noqa: E402
from fastapi import HTTPException

import orchestrator.main as main
from orchestrator.services.completion_command_resolution import (
    CompletionForceResolveResult,
    CompletionResolutionConflict,
    CompletionResolutionNotFound,
    CompletionUnparkResult,
)
from orchestrator.application import access as access_composition
from orchestrator.application import sessions as sessions_composition
import uuid as uuid_module


COMMAND_ID = "22222222-bbbb-4222-8222-222222222222"
JOB_ID = "11111111-aaaa-4111-8111-111111111111"
ADMIN_ID = "33333333-cccc-4333-8333-333333333333"


@pytest.fixture
def operator(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    service = MagicMock()
    service.unpark = AsyncMock()
    service.force_resolve = AsyncMock()
    monkeypatch.setattr(
        access_composition, "require_admin", AsyncMock(return_value={"id": ADMIN_ID})
    )
    monkeypatch.setattr(
        main.app.state.resources.settings, "completion_commands_enabled", True
    )
    monkeypatch.setattr(
        main.app.state.resources.completion_runtime,
        "command_resolution",
        lambda: service,
    )
    return service


@pytest.mark.asyncio
async def test_admin_unpark_delegates_exact_command_and_serializes_deadline(
    operator: MagicMock,
) -> None:
    deadline = datetime(2026, 8, 14, tzinfo=UTC)
    operator.unpark.return_value = CompletionUnparkResult(
        command_id=COMMAND_ID,
        job_id=JOB_ID,
        report_seq=7,
        state="pending",
        reset_effects=("subjob_output_graft",),
        deadline_at=deadline,
    )

    result = await run_queue_admin.unpark_completion_command(
        COMMAND_ID,
        admin={"id": ADMIN_ID},
        dependencies=sessions_composition.run_queue_admin_dependencies(
            main.app.state.resources
        ),
    )

    operator.unpark.assert_awaited_once_with(
        uuid_module.UUID(COMMAND_ID), actor=ADMIN_ID
    )
    assert result == {
        "command_id": COMMAND_ID,
        "job_id": JOB_ID,
        "report_seq": 7,
        "state": "pending",
        "reset_effects": ("subjob_output_graft",),
        "deadline_at": deadline,
    }


@pytest.mark.asyncio
async def test_admin_force_resolve_prunes_checkpoint_after_durable_commit(
    operator: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outcome = {
        "status": "force_resolved",
        "incident": True,
        "callbacks": False,
    }
    operator.force_resolve.return_value = CompletionForceResolveResult(
        command_id=COMMAND_ID,
        job_id=JOB_ID,
        report_seq=8,
        state="force_resolved",
        terminal_status="failed",
        prior_job_status="processing",
        abandoned_effects=("workspace_archive_teardown",),
        outcome=outcome,
    )
    prune = AsyncMock()
    monkeypatch.setattr(
        main.app.state.resources.postgres_db, "delete_checkpoint_thread", prune
    )
    body = CompletionCommandForceResolveRequest(
        expected_state="parked",
        terminal_status="failed",
        reason="operator confirmed delivery cannot converge",
    )

    result = await run_queue_admin.force_resolve_completion_command(
        COMMAND_ID,
        expected_state=(body).expected_state,
        terminal_status=(body).terminal_status,
        reason=(body).reason,
        admin={"id": ADMIN_ID},
        dependencies=sessions_composition.run_queue_admin_dependencies(
            main.app.state.resources
        ),
    )

    operator.force_resolve.assert_awaited_once_with(
        uuid_module.UUID(COMMAND_ID),
        expected_state="parked",
        terminal_status="failed",
        actor=ADMIN_ID,
        reason="operator confirmed delivery cannot converge",
    )
    prune.assert_awaited_once_with(JOB_ID)
    assert result["state"] == "force_resolved"
    assert result["abandoned_effects"] == ("workspace_archive_teardown",)
    assert result["outcome"] == outcome


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "error", "status", "detail"),
    [
        (
            "unpark",
            CompletionResolutionNotFound(COMMAND_ID),
            404,
            "Completion command not found",
        ),
        (
            "unpark",
            CompletionResolutionConflict("command_owner_live"),
            409,
            "command_owner_live",
        ),
        (
            "force_resolve",
            CompletionResolutionConflict("workspace_teardown_authorized"),
            409,
            "workspace_teardown_authorized",
        ),
    ],
)
async def test_admin_operator_errors_have_stable_http_status(
    operator: MagicMock,
    operation: str,
    error: Exception,
    status: int,
    detail: str,
) -> None:
    getattr(operator, operation).side_effect = error

    with pytest.raises(HTTPException) as exc:
        if operation == "unpark":
            await run_queue_admin.unpark_completion_command(
                COMMAND_ID,
                admin={"id": ADMIN_ID},
                dependencies=sessions_composition.run_queue_admin_dependencies(
                    main.app.state.resources
                ),
            )
        else:
            await run_queue_admin.force_resolve_completion_command(
                COMMAND_ID,
                expected_state=(
                    CompletionCommandForceResolveRequest(
                        expected_state="parked",
                        terminal_status="completed",
                        reason="incident resolution",
                    )
                ).expected_state,
                terminal_status=(
                    CompletionCommandForceResolveRequest(
                        expected_state="parked",
                        terminal_status="completed",
                        reason="incident resolution",
                    )
                ).terminal_status,
                reason=(
                    CompletionCommandForceResolveRequest(
                        expected_state="parked",
                        terminal_status="completed",
                        reason="incident resolution",
                    )
                ).reason,
                admin={"id": ADMIN_ID},
                dependencies=sessions_composition.run_queue_admin_dependencies(
                    main.app.state.resources
                ),
            )

    assert exc.value.status_code == status
    assert exc.value.detail == detail


@pytest.mark.asyncio
async def test_invalid_command_id_is_404_after_admin_authorization(
    operator: MagicMock,
) -> None:
    with pytest.raises(HTTPException) as exc:
        await run_queue_admin.unpark_completion_command(
            "not-a-uuid",
            admin={"id": ADMIN_ID},
            dependencies=sessions_composition.run_queue_admin_dependencies(
                main.app.state.resources
            ),
        )

    assert exc.value.status_code == 404
    operator.unpark.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["unpark", "force_resolve"])
async def test_commands_off_authorizes_then_stays_service_dark(
    operator: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    monkeypatch.setattr(
        main.app.state.resources.settings, "completion_commands_enabled", False
    )

    with pytest.raises(HTTPException) as exc:
        if operation == "unpark":
            await run_queue_admin.unpark_completion_command(
                COMMAND_ID,
                admin={"id": ADMIN_ID},
                dependencies=sessions_composition.run_queue_admin_dependencies(
                    main.app.state.resources
                ),
            )
        else:
            await run_queue_admin.force_resolve_completion_command(
                COMMAND_ID,
                expected_state=(
                    CompletionCommandForceResolveRequest(
                        expected_state="parked",
                        terminal_status="completed",
                        reason="must remain dark",
                    )
                ).expected_state,
                terminal_status=(
                    CompletionCommandForceResolveRequest(
                        expected_state="parked",
                        terminal_status="completed",
                        reason="must remain dark",
                    )
                ).terminal_status,
                reason=(
                    CompletionCommandForceResolveRequest(
                        expected_state="parked",
                        terminal_status="completed",
                        reason="must remain dark",
                    )
                ).reason,
                admin={"id": ADMIN_ID},
                dependencies=sessions_composition.run_queue_admin_dependencies(
                    main.app.state.resources
                ),
            )

    assert exc.value.status_code == 404
    assert exc.value.detail == "Completion commands are disabled"
    operator.unpark.assert_not_awaited()
    operator.force_resolve.assert_not_awaited()


@pytest.mark.parametrize("reorder_enabled", [False, True])
def test_safety_preclaim_and_router_reconciliation_follow_reorder_gate(
    monkeypatch: pytest.MonkeyPatch,
    reorder_enabled: bool,
) -> None:
    monkeypatch.setattr(
        main.app.state.resources.settings,
        "completion_status_reorder_enabled",
        reorder_enabled,
    )
    monkeypatch.setattr(main.app.state.resources.completion_runtime, "_finalizer", None)
    monkeypatch.setattr(
        main.app.state.resources.completion_runtime, "_sweep_router", None
    )
    monkeypatch.setattr(
        main.app.state.resources.completion_runtime, "_command_resolution", None
    )

    finalizer = main.app.state.resources.completion_runtime.finalizer()
    router = main.app.state.resources.completion_runtime.sweep_router()

    if reorder_enabled:
        resolution = main.app.state.resources.completion_runtime._command_resolution
        assert resolution is not None
        assert finalizer.preclaim.__self__ is resolution
        assert router.safety_net is resolution
    else:
        assert finalizer.preclaim is None
        assert router.safety_net is None
        assert main.app.state.resources.completion_runtime._command_resolution is None
