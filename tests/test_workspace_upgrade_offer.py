"""The pinned agent's upgrade offer asks for a VM, and only when SRW would
accept one (stateless upgrade design, decision 1)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from agent.tools.context import ToolContext
from agent.tools.core.upgrade import create_workspace_upgrade_tools

URL = "http://localhost:8085/api/agents/threads/tid/upgrade-availability"


class _Client:
    def __init__(self, payload=None, error: Exception | None = None):
        self.payload = payload
        self.error = error
        self.gets: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, **kwargs):
        self.gets.append(url)
        if self.error:
            raise self.error
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json = MagicMock(return_value=self.payload)
        return resp


def _tool(monkeypatch, client):
    monkeypatch.delenv("ORCHESTRATOR_URL", raising=False)
    monkeypatch.setattr(
        "agent.tools.orchestrator.jobs._get_client", lambda **kw: client
    )
    context = ToolContext(user_id="u-1")
    context.thread_id = "tid"
    (tool,) = create_workspace_upgrade_tools(context)
    return tool, context


@pytest.mark.asyncio
async def test_available_vm_is_offered(monkeypatch):
    client = _Client({"vm": {"available": True, "reason": None}})
    tool, context = _tool(monkeypatch, client)
    result = await tool.ainvoke({"reason": "need to run pytest"})
    assert client.gets == [URL]
    freeze = context.consume_freeze_request()
    assert freeze["freeze_type"] == "workspace_upgrade_required"
    assert freeze["target_tier"] == "vm"
    assert freeze["reason"] == "need to run pytest"
    assert "VM workspace" in result


@pytest.mark.asyncio
async def test_unavailable_vm_offers_nothing_and_says_why(monkeypatch):
    client = _Client(
        {"vm": {"available": False, "reason": "vm_workspace grant denied"}}
    )
    tool, context = _tool(monkeypatch, client)
    result = await tool.ainvoke({"reason": "need a shell"})
    assert context.consume_freeze_request() is None
    assert "vm_workspace grant denied" in result
    assert "Nothing was offered" in result


@pytest.mark.asyncio
async def test_failed_check_offers_nothing(monkeypatch):
    client = _Client(error=RuntimeError("connection refused"))
    tool, context = _tool(monkeypatch, client)
    result = await tool.ainvoke({"reason": "need a shell"})
    assert context.consume_freeze_request() is None
    assert "/upgrade-workspace vm" in result


@pytest.mark.asyncio
async def test_no_thread_identity_offers_nothing(monkeypatch):
    client = _Client({"vm": {"available": True, "reason": None}})
    monkeypatch.setattr(
        "agent.tools.orchestrator.jobs._get_client", lambda **kw: client
    )
    (tool,) = create_workspace_upgrade_tools(ToolContext(user_id="u-1"))
    context_free = await tool.ainvoke({"reason": "need a shell"})
    assert client.gets == []
    assert "Nothing was offered" in context_free
