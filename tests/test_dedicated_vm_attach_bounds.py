"""A dedicated VM attach stays bounded while its Pod waits in startup.

With a startup allowance that covers the session's readiness budget
(``session_pod_startup_allowance_s``), nothing on the kubelet side ends a
slow attach any more, so the attach's own exits are what bound it:

* a genuinely failed VM ends the poll at once (``vm_status='failed'``), and
  a VM that never becomes ready ends it at the agent's budget, which is below
  the orchestrator's readiness budget and therefore below the allowance;
* an End or Delete while the VM still boots reaches the poll as the ended-
  session fence (409 ``session_ended``), which the dedicated lifespan turns into
  ``_exit_session_ended``: deregister and exit 0 without ending the thread a
  second time and without waiting for the VM.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from agent.api import persistent_app
from agent.api.orchestrator_client import OrchestratorClient, SessionEnded
from orchestrator.services import session_workspace_policy


def _client():
    return OrchestratorClient(
        orchestrator_url="http://localhost:8085",
        pod_ip="10.0.0.5",
        pod_port=8001,
        hostname="test-agent",
        config_name="creator",
        pid=12345,
    )


@pytest.mark.asyncio
async def test_workspace_poll_reports_the_ended_session_fence():
    client = _client()
    response = MagicMock()
    response.status_code = 409
    response.json.return_value = {
        "detail": {"code": "session_ended", "message": "Session has ended."}
    }
    client._client = MagicMock()
    client._client.get = AsyncMock(return_value=response)

    with pytest.raises(SessionEnded):
        await client.get_thread_workspace("tid", raise_on_denied=True)


@pytest.mark.asyncio
async def test_vm_attach_stops_when_the_session_ends_mid_boot():
    client = AsyncMock()
    client.get_thread_workspace.side_effect = [
        {"vm_status": "provisioning"},
        {"vm_status": "created"},
        SessionEnded("session ended before workspace attach"),
    ]

    with pytest.raises(SessionEnded):
        await persistent_app._poll_workspace_ready(
            client, "tid", timeout=120, poll_interval=0, require_vm=True
        )
    assert client.get_thread_workspace.call_count == 3


@pytest.mark.asyncio
async def test_vm_attach_that_never_becomes_ready_ends_at_the_agent_budget(
    monkeypatch,
):
    """The agent's own deadline, not the kubelet, ends a VM that never boots."""

    clock = {"now": 0.0}
    monkeypatch.setattr("time.monotonic", lambda: clock["now"])

    async def _sleep(seconds):
        clock["now"] += 30.0

    monkeypatch.setattr(persistent_app.asyncio, "sleep", _sleep)
    client = AsyncMock()
    client.get_thread_workspace.return_value = {"vm_status": "provisioning"}

    result = await persistent_app._poll_workspace_ready(
        client, "tid", timeout=120, poll_interval=30, require_vm=True, vm_timeout=900
    )

    assert result is None
    assert 900 <= clock["now"] < session_workspace_policy.session_ready_timeout_s("vm")
