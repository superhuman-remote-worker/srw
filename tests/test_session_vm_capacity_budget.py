"""Capacity waiting is part of VM readiness, before physical creation."""

from unittest.mock import AsyncMock

import pytest

from agent.api.session_workspace import poll_workspace_ready


@pytest.mark.asyncio
async def test_vm_capacity_wait_uses_the_vm_readiness_budget(monkeypatch):
    import time
    import agent.api.session_workspace as workspace

    ticks = iter((0, 0.5, 130))
    monkeypatch.setattr(time, "monotonic", lambda: next(ticks, 130))
    monkeypatch.setattr(workspace.asyncio, "sleep", AsyncMock())
    client = AsyncMock()
    client.get_thread_workspace.side_effect = [
        {"vm_status": "waiting_capacity"},
        {"vm_status": "ready", "vm_ssh_host": "192.0.2.10", "vm_ssh_port": 22},
    ]
    result = await poll_workspace_ready(client, "captured", require_vm=True, session_runtime_generation=None)
    assert result is not None
    assert result["backend"] == "vm"
    assert client.get_thread_workspace.await_count == 2


@pytest.mark.asyncio
async def test_non_vm_capacity_label_does_not_extend_the_vm_budget(monkeypatch):
    import time
    import agent.api.session_workspace as workspace

    ticks = iter((0, 0.5, 130))
    monkeypatch.setattr(time, "monotonic", lambda: next(ticks, 130))
    monkeypatch.setattr(workspace.asyncio, "sleep", AsyncMock())
    client = AsyncMock()
    client.get_thread_workspace.return_value = {"status": "waiting_capacity"}
    assert await poll_workspace_ready(client, "captured", session_runtime_generation=None) is None
    assert client.get_thread_workspace.await_count == 1
