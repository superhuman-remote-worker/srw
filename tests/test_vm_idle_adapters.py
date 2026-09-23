"""Public controls preserve execution/access separation at idle wake."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.routers.ide import IdeDependencies, start_ide_session


@pytest.mark.asyncio
async def test_authorized_ide_start_requests_access_only_idle_wake(monkeypatch):
    monkeypatch.setenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false")
    user_id = "11111111-1111-4111-8111-111111111111"
    job_id = "22222222-2222-4222-8222-222222222222"
    ide_sessions = SimpleNamespace(start_session=AsyncMock())
    dependencies = IdeDependencies(
        store=MagicMock(),
        ide_sessions=ide_sessions,
        ide_proxy=MagicMock(),
        require_job_access=AsyncMock(return_value=({"id": user_id}, {"id": job_id})),
    )
    with patch("orchestrator.services.vm_idle_lifecycle.VMIdleLifecycleStore") as idle_class:
        idle = idle_class.return_value
        idle.schema_available = AsyncMock(return_value=True)
        idle.get_open_for_owner = AsyncMock(return_value={"id": "wake"})
        idle.request_wake = AsyncMock(return_value={"phase": "waking"})
        result = await start_ide_session(
            MagicMock(), job_id, dependencies=dependencies,
        )

    assert result["status"] == "restoring"
    dependencies.require_job_access.assert_awaited_once()
    idle.request_wake.assert_awaited_once_with(
        job_id, execution_requested=False,
        access_kind="ide", access_claimant=user_id,
    )
    ide_sessions.start_session.assert_not_awaited()
