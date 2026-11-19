from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException

from orchestrator.services.completion_control import CompletionControlClaimConflict
from orchestrator.services.completion_runtime import (
    CompletionControlBoundary,
    CompletionRuntime,
    CompletionRuntimeDependencies,
)


def _runtime(*, enabled: bool) -> CompletionRuntime:
    dependencies = CompletionRuntimeDependencies(
        store=object(),
        workflow=AsyncMock(),
        commands_enabled=lambda: enabled,
        status_reorder_enabled=lambda: False,
        sweep_alert=AsyncMock(),
        resolution_alert=AsyncMock(),
        monitor_alert=AsyncMock(),
        max_queued_session_age_seconds=lambda: 60.0,
        logger=logging.getLogger("test.completion_runtime"),
    )
    return CompletionRuntime(dependencies)


@pytest.mark.asyncio
async def test_disabled_boundary_never_constructs_durable_control() -> None:
    runtime = _runtime(enabled=False)
    runtime.control = MagicMock(side_effect=AssertionError("control constructed"))
    boundary = CompletionControlBoundary(runtime)

    assert await boundary.guard("job", source="test") is None
    assert await boundary.claim({"id": "job"}, source="test") is None
    assert (
        await boundary.claim_pause("job", source="test", expected_agent_id=None) is None
    )
    assert boundary.resume_guard_kwargs() == {}
    assert boundary.dispatch_guard_kwargs() == {}
    assert boundary.active_claim({"context": {}}) is False
    runtime.control.assert_not_called()


@pytest.mark.asyncio
async def test_boundary_reuses_one_control_and_preserves_refusal_detail() -> None:
    runtime = _runtime(enabled=True)
    control = SimpleNamespace(
        guard_job=AsyncMock(
            return_value=SimpleNamespace(
                blocked=True, route="resume_finalizer", command_id="command"
            )
        ),
        claim_job=AsyncMock(
            side_effect=CompletionControlClaimConflict("completion finalizing")
        ),
    )
    runtime._control = control
    boundary = CompletionControlBoundary(runtime)

    with pytest.raises(HTTPException) as guarded:
        await boundary.guard("job", source="resume")
    with pytest.raises(HTTPException) as claimed:
        await boundary.claim(
            {"id": "job", "status": "processing", "execution_lane": "pinned"},
            source="cancel",
        )

    assert guarded.value.status_code == 409
    assert guarded.value.detail == "completion finalizing"
    assert claimed.value.status_code == 409
    assert claimed.value.detail == "completion finalizing"
    control.guard_job.assert_awaited_once_with("job", source="resume")
    control.claim_job.assert_awaited_once_with(
        "job",
        source="cancel",
        expected_status="processing",
        expected_lane="pinned",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "route,typed",
    [
        ("alert_only", True),
        ("resume_finalizer", False),
        ("park_alert", False),
        ("stand_down", False),
    ],
)
async def test_guard_types_the_refusal_of_a_parked_command(route, typed) -> None:
    """A parked command waits for an operator; every other route passes."""

    runtime = _runtime(enabled=True)
    command_id = "03dc2497-cad4-4481-8af0-d94fccf04ec2"
    runtime._control = SimpleNamespace(
        guard_job=AsyncMock(
            return_value=SimpleNamespace(
                blocked=True, route=route, command_id=command_id
            )
        )
    )

    with pytest.raises(HTTPException) as guarded:
        await CompletionControlBoundary(runtime).guard("job", source="resume")

    assert guarded.value.status_code == 409
    if typed:
        assert guarded.value.detail["code"] == "completion_parked"
        assert guarded.value.detail["command_id"] == command_id
        assert "(command 03dc2497)" in guarded.value.detail["message"]
        assert "administrator" in guarded.value.detail["message"]
    else:
        assert guarded.value.detail == "completion finalizing"


def test_guard_kwargs_preserve_flag_on_storage_contract() -> None:
    boundary = CompletionControlBoundary(_runtime(enabled=True))
    claim = SimpleNamespace(claim_id="claim-id")

    assert boundary.resume_guard_kwargs("command-id", "owner", claim) == {
        "completion_commands_enabled": True,
        "completion_owner_command_id": "command-id",
        "completion_owner": "owner",
        "completion_control_claim_id": "claim-id",
    }
    assert boundary.dispatch_guard_kwargs() == {"completion_commands_enabled": True}
