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


def _client_answering(handler):
    client = OrchestratorClient.__new__(OrchestratorClient)
    client.orchestrator_url = "http://orchestrator"
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return client


@pytest.mark.asyncio
async def test_an_abort_the_orchestrator_finished_returns_true():
    client = _client_answering(
        lambda request: httpx.Response(200, json={"status": "aborted"})
    )
    assert await client.abort_thread_vm_upgrade("t") is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "answer",
    [
        httpx.Response(
            503,
            json={"detail": {"code": "vm_process_zero_unproven", "retryable": True}},
        ),
        httpx.Response(502, text="bad gateway"),
        httpx.ConnectError("refused"),
    ],
    ids=["unproven", "gateway", "no-answer"],
)
async def test_an_abort_not_finished_yet_says_so(answer):
    """E4 part 2: the caller asks again; a single False used to end it."""
    from agent.api.orchestrator_client import VmUpgradeAbortPending

    def handler(request):
        if isinstance(answer, Exception):
            raise answer
        return answer

    with pytest.raises(VmUpgradeAbortPending):
        await _client_answering(handler).abort_thread_vm_upgrade("t")


@pytest.mark.asyncio
async def test_an_abort_refusal_returns_false():
    client = _client_answering(
        lambda request: httpx.Response(404, json={"detail": "Thread not found"})
    )
    assert await client.abort_thread_vm_upgrade("t") is False
