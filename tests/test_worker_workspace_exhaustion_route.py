"""The real completion workflow selects retention before terminal effects."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.schemas.job_runtime import JobCompleteRequest
from orchestrator.services import completion
from tests import b08_completion_helpers
from tests.test_job_completion_endpoint_wrapper import (
    JOB_ID,
    _RecordingRunner,
    _RouteDB,
    _isolate_workspace_cleanup_authority,  # noqa: F401
    _patch_normal_route_dependencies,
    _route_job,
)


@pytest.mark.asyncio
async def test_worker_workspace_budget_routes_to_hold_before_terminal_cleanup(
    monkeypatch,
):
    job = _route_job()
    job.update(
        execution_lane="stateless",
        assigned_agent_id=None,
        config_override={"workspace": {"backend": "sandbox"}},
        context={"workspace_container": {"provisioner": "k8s", "status": "ready"}},
    )
    database = _RouteDB(job)
    runner = _RecordingRunner()
    terminal = AsyncMock(side_effect=AssertionError("must not terminalize"))
    cleanup = AsyncMock(side_effect=AssertionError("must not reclaim workspace"))
    _patch_normal_route_dependencies(
        monkeypatch,
        database=database,
        terminal_effects=terminal,
        workspace_cleanup=cleanup,
    )
    held = {
        "status": "handled",
        "job_id": JOB_ID,
        "new_status": "paused",
        "paused": True,
        "held_for_resume": True,
        "actions": ["retained"],
    }
    recovery = AsyncMock(return_value=held)
    monkeypatch.setattr(completion, "handle_pod_workspace_recovery", recovery)
    error = {
        "type": "worker_retry_exhausted",
        "recoverable": False,
        "cause": {"type": "workspace_unavailable", "recoverable": True},
    }

    result = await b08_completion_helpers.complete_job_legacy(
        MagicMock(),
        JOB_ID,
        JobCompleteRequest(
            should_stop=True,
            goal_achieved=False,
            error=error,
            freeze_data={"freeze_type": "worker_retry_exhausted"},
            lease_token=17,
        ),
        _authorized=True,
        _effect_runner=runner,
    )

    assert result == held
    recovery.assert_awaited_once()
    assert recovery.await_args.args[2] == error
    assert recovery.await_args.kwargs["completion_command_id"] == runner.command_id
    assert "persist_reported_freeze" not in runner.started
    assert database.status_write_count == 0
    terminal.assert_not_awaited()
    cleanup.assert_not_awaited()
