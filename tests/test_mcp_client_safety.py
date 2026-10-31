"""Concurrency and retry-safety regressions for the orchestrator MCP client."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import httpx
import pytest

from shared.orch_surface.client import AsyncCockpitClient, MutationOutcomeUnknown
from shared.orch_surface import client as client_module
from shared.orch_surface.jobs import CallerCtx, get_descriptor, make_bound_handler


@pytest.mark.asyncio
async def test_bound_invocations_isolate_project_scope_and_reset_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One pooled client must never leak identity between concurrent tools."""
    monkeypatch.setenv("MCP_INTERNAL_KEY", "test-internal-key")
    observed: list[tuple[str, str]] = []
    both_arrived = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        observed.append(
            (
                request.headers.get("X-MCP-User-Id", ""),
                request.headers.get("X-MCP-Scope", ""),
            )
        )
        if len(observed) == 2:
            both_arrived.set()
        await asyncio.wait_for(both_arrived.wait(), timeout=1)
        return httpx.Response(200, json=[])

    client = AsyncCockpitClient(
        base_url="http://orchestrator.test",
        transport=httpx.MockTransport(handler),
    )
    descriptor = get_descriptor("list_jobs")
    # Only the MCP lane stamps X-MCP-Scope; a session caller with an identical
    # single-project binding must NOT (see make_bound_handler).
    mcp_call = make_bound_handler(
        descriptor,
        client_provider=lambda: client,
        caller_provider=lambda: CallerCtx(
            kind="mcp",
            user_id="user-mcp",
            project_ids=("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",),
        ),
    )
    session_call = make_bound_handler(
        descriptor,
        client_provider=lambda: client,
        caller_provider=lambda: CallerCtx(
            kind="session",
            user_id="user-session",
            project_ids=("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",),
        ),
    )

    try:
        await asyncio.gather(mcp_call(), session_call())
        # The descriptor wrapper has reset its ContextVar token. A direct read
        # therefore carries only the client's internal credential.
        direct = await client.list_jobs()
    finally:
        await client.close()

    assert set(observed[:2]) == {
        (
            "user-mcp",
            "project:aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        ),
        ("user-session", ""),
    }
    assert observed[2] == ("", "")
    assert direct["jobs"] == []


@pytest.mark.asyncio
async def test_request_scope_headers_are_isolated_between_users_and_health(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Delayed concurrent calls must retain their own identity and scope."""
    monkeypatch.setenv("MCP_INTERNAL_KEY", "test-internal-key")
    observed: list[tuple[str, str, str, str]] = []
    both_users_arrived = asyncio.Event()
    arrivals = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal arrivals
        user_id = request.headers.get("X-MCP-User-Id", "")
        scope = request.headers.get("X-MCP-Scope", "")
        internal_key = request.headers.get("X-Internal-Key", "")
        observed.append((request.url.path, user_id, scope, internal_key))

        if request.url.path == "/api/health":
            return httpx.Response(200, json={"status": "healthy"})

        arrivals += 1
        if arrivals == 2:
            both_users_arrived.set()
        await asyncio.wait_for(both_users_arrived.wait(), timeout=1)
        return httpx.Response(200, json=[{"user_id": user_id, "scope": scope}])

    client = AsyncCockpitClient(
        base_url="http://orchestrator.test",
        transport=httpx.MockTransport(handler),
    )

    # F15: the imperative set_scope_headers/clear_scope_headers pair is gone;
    # invocation_scope is the one binding mechanism and must isolate
    # concurrent tasks exactly as the removed methods did.
    async def call_as(user_id: str, scope: str) -> list[dict[str, str]]:
        with client.invocation_scope(user_id=user_id, scope=scope):
            return await client.list_jobs()

    async def run_health_probe() -> dict[str, str]:
        await asyncio.sleep(0)
        with client.invocation_scope(unauthenticated=True):
            return await client.health_check()

    try:
        first, second, health = await asyncio.gather(
            call_as("user-a", "project:project-a"),
            call_as("user-b", "project:project-b"),
            run_health_probe(),
        )
    finally:
        await client.close()

    assert first["jobs"] == [{"user_id": "user-a", "scope": "project:project-a"}]
    assert second["jobs"] == [{"user_id": "user-b", "scope": "project:project-b"}]
    assert health == {"status": "healthy"}
    assert set(observed) == {
        ("/api/jobs", "user-a", "project:project-a", "test-internal-key"),
        ("/api/jobs", "user-b", "project:project-b", "test-internal-key"),
        ("/api/health", "", "", ""),
    }
    assert "X-MCP-User-Id" not in client._client.headers
    assert "X-MCP-Scope" not in client._client.headers
    assert "X-Internal-Key" not in client._client.headers


@pytest.mark.asyncio
async def test_delayed_mutation_is_sent_once() -> None:
    attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        await asyncio.sleep(0.02)
        return httpx.Response(201, json={"id": "job-1", "status": "created"})

    client = AsyncCockpitClient(
        base_url="http://orchestrator.test",
        transport=httpx.MockTransport(handler),
    )
    try:
        result = await client.create_job("delayed job")
    finally:
        await client.close()

    assert result == {"id": "job-1", "status": "created"}
    assert attempts == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("permanent", [False, True])
async def test_session_end_waits_past_the_servers_end_budget(permanent) -> None:
    """A Session End takes ~30 s server-side (handoff join up to 30 s, agent
    detach up to 150 s): the client default of 30 s timed it out, so the tool
    reported a failure for an End that succeeded and the caller re-sent it."""
    seen: list[dict] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(dict(request.extensions["timeout"]))
        return httpx.Response(200, json={"status": "ended"})

    client = AsyncCockpitClient(
        base_url="http://orchestrator.test",
        transport=httpx.MockTransport(handler),
    )
    try:
        result = await client.end_persistent_thread("t-1", permanent=permanent)
        await client.create_job("ordinary mutation")
    finally:
        await client.close()

    assert result == {"status": "ended"}
    end_timeout, ordinary_timeout = seen
    assert end_timeout["read"] >= 30.0 + 150.0
    assert end_timeout["connect"] == 30.0
    # Only the End call is widened; other requests keep the client default.
    assert ordinary_timeout["read"] == 30.0


@pytest.mark.asyncio
async def test_ambiguous_mutation_timeout_is_not_retried_or_reported_failed() -> None:
    attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ReadTimeout("response arrived too late", request=request)

    client = AsyncCockpitClient(
        base_url="http://orchestrator.test",
        transport=httpx.MockTransport(handler),
    )
    try:
        with pytest.raises(MutationOutcomeUnknown) as exc_info:
            await client.create_job("ambiguous job")
    finally:
        await client.close()

    assert attempts == 1
    assert "Outcome unknown" in str(exc_info.value)
    assert "did not retry" in str(exc_info.value)
    assert "Verify current state" in str(exc_info.value)


@pytest.mark.asyncio
async def test_mutation_connect_failure_is_distinct_and_not_retried() -> None:
    attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ConnectError("orchestrator unavailable", request=request)

    client = AsyncCockpitClient(
        base_url="http://orchestrator.test",
        transport=httpx.MockTransport(handler),
    )
    try:
        with pytest.raises(httpx.ConnectError):
            await client.create_job("not sent")
    finally:
        await client.close()

    assert attempts == 1


@pytest.mark.asyncio
async def test_non_get_read_is_single_attempt_without_mutation_ambiguity() -> None:
    attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ReadTimeout("read timed out", request=request)

    client = AsyncCockpitClient(
        base_url="http://orchestrator.test",
        transport=httpx.MockTransport(handler),
    )
    try:
        with pytest.raises(httpx.ReadTimeout):
            await client.test_datasource("connector-1")
    finally:
        await client.close()

    assert attempts == 1


@pytest.fixture
def retry_delays(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Skip retry backoff without replacing the event loop's own sleep."""
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)
        await asyncio.sleep(0)

    monkeypatch.setattr(client_module, "asyncio", SimpleNamespace(sleep=sleep))
    return delays


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [httpx.ReadTimeout, httpx.ConnectError])
@pytest.mark.parametrize(
    "operation,arguments,method,path",
    [
        pytest.param(
            "create_job",
            {"description": "test job"},
            "POST",
            "/api/jobs",
            id="create-job",
        ),
        pytest.param(
            "delete_job",
            {"job_id": "job-1"},
            "DELETE",
            "/api/jobs/job-1",
            id="delete-job",
        ),
        pytest.param(
            "cancel_job",
            {"job_id": "job-1"},
            "PUT",
            "/api/jobs/job-1/cancel",
            id="cancel-job",
        ),
        pytest.param(
            "create_project",
            {"name": "test project", "user_id": "user-1"},
            "POST",
            "/api/projects",
            id="create-project",
        ),
        pytest.param(
            "update_project",
            {"project_id": "project-1", "name": "updated"},
            "PATCH",
            "/api/projects/project-1",
            id="update-project",
        ),
        pytest.param(
            "create_datasource",
            {"name": "test connector", "ds_type": "generic"},
            "POST",
            "/api/datasources",
            id="create-datasource",
        ),
        pytest.param(
            "create_persistent_thread",
            {},
            "POST",
            "/api/persistent/threads",
            id="create-thread",
        ),
        pytest.param(
            "end_persistent_thread",
            {"thread_id": "thread-1"},
            "DELETE",
            "/api/persistent/threads/thread-1",
            id="end-thread",
        ),
    ],
)
async def test_public_mutations_send_once_on_transport_failure(
    operation, arguments, method, path, failure, retry_delays
) -> None:
    """Public operations must not inherit the safe-read retry policy."""
    attempts: list[tuple[str, str]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        attempts.append((request.method, request.url.path))
        raise failure("transport failure", request=request)

    expected_error = (
        MutationOutcomeUnknown if failure is httpx.ReadTimeout else httpx.ConnectError
    )
    async with AsyncCockpitClient(
        base_url="http://orchestrator.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(expected_error) as exc_info:
            await asyncio.wait_for(getattr(client, operation)(**arguments), timeout=1)

    assert attempts == [(method, path)]
    assert retry_delays == []
    if failure is httpx.ReadTimeout:
        assert exc_info.value.method == method
        assert exc_info.value.path == path
        assert isinstance(exc_info.value.__cause__, httpx.ReadTimeout)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [httpx.ReadTimeout, httpx.ConnectError])
@pytest.mark.parametrize("recovers", [True, False], ids=["recovers", "exhausted"])
async def test_safe_get_retries_are_bounded(failure, recovers, retry_delays) -> None:
    attempts: list[tuple[str, str]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        attempts.append((request.method, request.url.path))
        if recovers and len(attempts) == 3:
            return httpx.Response(200, json={"id": "job-1"})
        raise failure("temporary failure", request=request)

    async with AsyncCockpitClient(
        base_url="http://orchestrator.test",
        transport=httpx.MockTransport(handler),
    ) as client:
        if recovers:
            result = await asyncio.wait_for(client.get_job("job-1"), timeout=1)
            assert result == {"id": "job-1"}
        else:
            with pytest.raises(failure):
                await asyncio.wait_for(client.get_job("job-1"), timeout=1)

    assert attempts == [("GET", "/api/jobs/job-1")] * 3
    assert len(retry_delays) == 2
