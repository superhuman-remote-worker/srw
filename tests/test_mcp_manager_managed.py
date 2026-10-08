"""MCPManager with managed MCP servers (connector drivers D5a).

A managed server's entry carries the endpoint of its pods and a lease token,
which is the client's bearer: the upstream credential never reaches the
agent process. The connect waits for the pod's front to be ready, and a pod
replaced mid-session reconnects within a budget. The real front and server
are exercised end to end in tests/test_managed_mcp_front_integration.py.
"""

from __future__ import annotations

import asyncio
import builtins
from collections import deque

import httpx
import pytest

from agent.tools.mcp import manager as manager_module
from agent.tools.mcp.manager import (
    MANAGED_MCP_RECONNECT_WINDOW,
    MANAGED_MCP_RECONNECTS,
    MANAGED_MCP_START_TIMEOUT,
    MCP_CONNECT_TIMEOUT,
    MCPManager,
    parse_mcp_config,
)
from shared.connectors.leases import mint_token

CONNECTOR = "66666666-7777-4888-8999-aaaaaaaaaaaa"
URL = "http://srw-ep-x.srw-connectors.svc.cluster.local:8080/mcp"


def _entry(token: str | None = None, **over):
    entry = {
        "type": "mcp_test",
        "name": "Notes",
        "connection_url": URL,
        "credentials": {
            "lease": {
                "id": "lease-1",
                "connector_id": CONNECTOR,
                "token": token or mint_token("scl"),
            }
        },
        "datasource_id": CONNECTOR,
        "config": {},
    }
    entry.update(over)
    return entry


def test_a_managed_entry_connects_with_its_lease_token_as_the_bearer():
    token = mint_token("scl")
    config = parse_mcp_config(_entry(token))
    assert config.managed
    assert config.transport == "http" and config.url == URL
    assert config.headers == {"Authorization": f"Bearer {token}"}
    assert config.ready_url == (
        "http://srw-ep-x.srw-connectors.svc.cluster.local:8080/readyz"
    )
    remote = parse_mcp_config(
        {"type": "mcp", "name": "R", "connection_url": "https://r.example/mcp"}
    )
    assert not remote.managed


@pytest.mark.parametrize(
    "over",
    [
        {"credentials": {}},
        {"credentials": {"lease": {"token": "not-a-lease"}}},
        {"credentials": {"token": "a-raw-upstream-token"}},
        {"connection_url": None},
        {"connection_url": "file:///etc/passwd"},
    ],
)
def test_a_managed_entry_without_a_lease_or_an_endpoint_is_unavailable(over):
    with pytest.raises(ValueError) as refused:
        parse_mcp_config(_entry(**over))
    assert "a-raw-upstream-token" not in str(refused.value)
    manager = MCPManager([_entry(**over)])
    (status,) = manager.statuses.values()
    assert status.startswith("unavailable: invalid config")
    assert "a-raw-upstream-token" not in status


def test_a_managed_server_may_take_its_pods_start_to_connect():
    managed = MCPManager([_entry()])._handles[0]
    remote = MCPManager(
        [{"type": "mcp", "name": "R", "connection_url": "https://r.example/mcp"}]
    )._handles[0]
    assert managed.connect_timeout > MANAGED_MCP_START_TIMEOUT
    assert remote.connect_timeout == MCP_CONNECT_TIMEOUT


def _ready_after(monkeypatch, answers: list[int | Exception]):
    """The front answers /readyz with each of ``answers`` in turn."""
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        answer = answers[min(len(seen) - 1, len(answers) - 1)]
        if isinstance(answer, Exception):
            raise answer
        return httpx.Response(answer, json={})

    monkeypatch.setattr(
        manager_module,
        "_ready_client",
        lambda: httpx.AsyncClient(transport=httpx.MockTransport(handle)),
    )
    monkeypatch.setattr(manager_module, "_READY_POLL_SECONDS", (0.01,))
    return seen


@pytest.mark.asyncio
async def test_the_connect_waits_for_a_starting_pod(monkeypatch):
    seen = _ready_after(
        monkeypatch, [httpx.ConnectError("no endpoint yet"), 503, 503, 200]
    )
    manager = MCPManager([_entry()])
    await manager._wait_until_serving(manager._handles[0].config)
    assert len(seen) == 4
    assert all(str(r.url).endswith("/readyz") for r in seen)
    # The readiness route never sees the lease.
    assert all("authorization" not in r.headers for r in seen)


@pytest.mark.asyncio
async def test_a_pod_that_never_serves_leaves_the_server_unavailable(monkeypatch):
    _ready_after(monkeypatch, [503])
    monkeypatch.setattr(manager_module, "MANAGED_MCP_START_TIMEOUT", 0.05)
    manager = MCPManager([_entry()])
    await manager.connect_all()
    (status,) = manager.statuses.values()
    assert status.startswith("unavailable: not ready after")
    assert manager.get_langchain_tools() == []


def test_a_managed_server_reconnects_within_a_budget(monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(manager_module.time, "monotonic", lambda: now[0])
    handle = MCPManager([_entry()])._handles[0]
    for _ in range(MANAGED_MCP_RECONNECTS):
        assert MCPManager._may_reconnect(handle)
        handle.reconnects.append(now[0])
    assert not MCPManager._may_reconnect(handle)
    now[0] += MANAGED_MCP_RECONNECT_WINDOW + 1
    assert MCPManager._may_reconnect(handle)
    assert handle.reconnects == deque()
    # A remote server keeps its single reconnect.
    remote = MCPManager(
        [{"type": "mcp", "name": "R", "connection_url": "https://r.example/mcp"}]
    )._handles[0]
    assert MCPManager._may_reconnect(remote)
    remote.reconnected_once = True
    assert not MCPManager._may_reconnect(remote)


def test_a_tool_error_is_not_a_dead_session():
    from langchain_core.tools import ToolException
    from mcp.shared.exceptions import McpError
    from mcp.types import ErrorData

    def mcp_error(code: int) -> McpError:
        return McpError(ErrorData(code=code, message="x"))

    assert manager_module._tool_level(ToolException("the tool failed"))
    # The front refusing a tool this binding may not call.
    assert manager_module._tool_level(mcp_error(-32602))
    # A terminated session (404 from a replaced pod) or a closed connection.
    assert not manager_module._tool_level(mcp_error(32600))
    assert not manager_module._tool_level(mcp_error(-32000))
    assert not manager_module._tool_level(httpx.ConnectError("refused"))
    assert not manager_module._tool_level(ConnectionError("gone"))


@pytest.mark.asyncio
async def test_a_call_fails_at_once_when_its_session_ends():
    handle = MCPManager([_entry()])._handles[0]
    owner_done = asyncio.Event()

    async def owner():
        await owner_done.wait()

    handle.task = asyncio.create_task(owner())
    cancelled = asyncio.Event()

    async def hanging_call():
        try:
            await asyncio.sleep(3600)
        finally:
            cancelled.set()

    racing = asyncio.create_task(MCPManager._raced(handle, hanging_call()))
    await asyncio.sleep(0)
    owner_done.set()
    with pytest.raises(ConnectionError):
        await asyncio.wait_for(racing, timeout=5)
    assert cancelled.is_set()

    # A call that answers first returns its answer.
    handle.task = asyncio.create_task(asyncio.sleep(3600))

    async def answer():
        return "ok"

    assert await MCPManager._raced(handle, answer()) == "ok"
    # The caller's own timeout cancels the call too.
    cancelled.clear()
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(MCPManager._raced(handle, hanging_call()), 0.05)
    assert cancelled.is_set()
    handle.task.cancel()


@pytest.mark.asyncio
async def test_a_remote_servers_call_is_not_raced():
    remote = MCPManager(
        [{"type": "mcp", "name": "R", "connection_url": "https://r.example/mcp"}]
    )._handles[0]
    remote.task = asyncio.create_task(asyncio.sleep(0))
    await remote.task

    async def answer():
        return "ok"

    assert await MCPManager._raced(remote, answer()) == "ok"


def _lease_ended_error(message: str = "lease revoked: the lease is revoked"):
    from mcp.shared.exceptions import McpError
    from mcp.types import ErrorData

    return McpError(ErrorData(code=manager_module.LEASE_ENDED_CODE, message=message))


def _http_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", URL)
    return httpx.HTTPStatusError(
        "refused", request=request, response=httpx.Response(status, request=request)
    )


def test_a_refused_lease_is_found_wherever_the_transport_put_it():
    lease_ended = manager_module._lease_ended
    # A new request on a dead lease: 401, inside the transport's task group.
    assert lease_ended(_http_error(401))
    group = builtins.BaseExceptionGroup(
        "transport", [RuntimeError("x"), _http_error(401)]
    )
    assert lease_ended(group)
    try:
        raise ConnectionError("the server's session ended") from group
    except ConnectionError as wrapped:
        assert lease_ended(wrapped)
    # A call in flight when the lease ended: the front's own error.
    assert lease_ended(_lease_ended_error())
    # Anything else is not the lease: a server's 403, the same code from a
    # server with another message, a replaced pod.
    assert not lease_ended(_http_error(403))
    assert not lease_ended(_lease_ended_error("something else"))
    assert not lease_ended(httpx.ConnectError("refused"))
    assert not lease_ended(ConnectionError("gone"))


@pytest.mark.asyncio
async def test_a_call_told_its_lease_ended_says_so_and_never_reconnects():
    manager = MCPManager([_entry()])
    handle = manager._handles[0]
    handle.status = "connected"
    handle.session = object()
    handle.task = asyncio.create_task(asyncio.sleep(3600))
    restarts: list[bool] = []

    async def restart(_handle, *, force=False):
        restarts.append(force)
        return False

    manager._restart_server = restart

    class Tool:
        name = "whoami"

        @staticmethod
        async def coroutine(**_kwargs):
            raise _lease_ended_error()

    call = manager._guarded(handle, Tool())
    answer = await call()
    assert manager_module.LEASE_REFUSED in answer
    assert handle.status == f"unavailable: {manager_module.LEASE_REFUSED}"
    assert handle.refused_lease == handle.config.headers
    assert restarts == []
    # The next call is not live: the restart refuses the same lease, and the
    # agent is told why again.
    manager._restart_server = MCPManager._restart_server.__get__(manager)
    assert manager_module.LEASE_REFUSED in await call()
    assert len(handle.reconnects) == 0
    handle.task.cancel()


def test_only_another_lease_lifts_a_refusal():
    handle = MCPManager([_entry()])._handles[0]
    manager_module._refuse_lease(handle)
    assert not manager_module._lease_renewed(handle)
    handle.ds["credentials"]["lease"]["token"] = mint_token("scl")
    assert manager_module._lease_renewed(handle)
