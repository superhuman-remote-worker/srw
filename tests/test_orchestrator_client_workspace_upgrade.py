"""The upgrade request carries a template and returns the refusal text."""

import json

import httpx
import pytest

from agent.api.orchestrator_client import OrchestratorClient


@pytest.mark.asyncio
async def test_the_template_and_the_refusal_detail_travel():
    seen = []

    def handler(request):
        seen.append(json.loads(request.content))
        if seen[-1].get("template") == "site":
            return httpx.Response(409, json={"detail": "X"})
        return httpx.Response(200, json={"status": "provisioning"})

    client = OrchestratorClient.__new__(OrchestratorClient)
    client.orchestrator_url = "http://orchestrator"
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    assert await client.request_thread_workspace_upgrade(
        "t", target_tier="vm", template="site"
    ) == (False, "X")
    assert await client.request_thread_workspace_upgrade("t", target_tier="vm") == (
        True,
        None,
    )
    assert seen == [{"target_tier": "vm", "template": "site"}, {"target_tier": "vm"}]
