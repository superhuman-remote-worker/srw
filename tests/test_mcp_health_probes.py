"""MCP survives an upgrade's restart order instead of crash-looping.

Regression for APP-UPG-1 (university upgrade test 2026-10-14):

1. The new MCP pod exited on its first OIDC discovery error while Keycloak
   was still starting and crash-looped five times (4.5 minutes to Ready).
2. ``/health`` calls the orchestrator and was the liveness probe, so an
   orchestrator restart got the healthy old MCP pod killed as well.
3. ``/health`` answered 503 tens of thousands of times without logging why.
"""

from __future__ import annotations

import json
import logging
import os

import httpx
import pytest

# Skip auth setup in the server module, as the other MCP suites do: the module
# is imported once per process, and HTTP-mode auth would leak into them.
os.environ.setdefault("MCP_TRANSPORT", "stdio")

from mcp_server import server  # noqa: E402


class _Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class TestOidcDiscoveryRetry:
    def test_keycloak_starting_is_waited_for(self, caplog):
        clock = _Clock()
        failures = [
            httpx.ConnectError("Connection refused"),
            httpx.HTTPStatusError(
                "404 realm not imported yet",
                request=httpx.Request("GET", "http://keycloak/"),
                response=httpx.Response(404),
            ),
        ]

        def factory():
            if failures:
                raise failures.pop(0)
            return "proxy"

        with caplog.at_level(logging.WARNING, logger="mcp_server.server"):
            result = server.build_with_discovery_retry(
                factory, budget_s=240, sleep=clock.sleep, clock=clock.time
            )
        assert result == "proxy"
        assert clock.sleeps == [2.0, 4.0]
        assert caplog.text.count("OIDC discovery failed") == 2

    def test_the_budget_ends_the_wait(self):
        clock = _Clock()

        def factory():
            raise httpx.ConnectError("Connection refused")

        with pytest.raises(httpx.ConnectError):
            server.build_with_discovery_retry(
                factory, budget_s=30, sleep=clock.sleep, clock=clock.time
            )
        assert sum(clock.sleeps) == pytest.approx(30)
        assert max(clock.sleeps) <= 15.0

    def test_a_configuration_error_is_not_retried(self):
        clock = _Clock()

        def factory():
            raise ValueError("Missing required client id")

        with pytest.raises(ValueError):
            server.build_with_discovery_retry(
                factory, budget_s=240, sleep=clock.sleep, clock=clock.time
            )
        assert clock.sleeps == []


class _DownClient:
    async def health_check(self):
        raise httpx.ConnectError("orchestrator restarting")


@pytest.fixture
def orchestrator_down(monkeypatch):
    async def build_info():
        return {"schema_artifact_status": "not_configured"}

    monkeypatch.setattr(server, "_get_client", lambda: _DownClient())
    monkeypatch.setattr(server, "_mcp_build_info", build_info)


@pytest.mark.asyncio
async def test_liveness_does_not_depend_on_the_orchestrator(orchestrator_down):
    response = await server.liveness_check(None)
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_readiness_failure_logs_its_cause(orchestrator_down, caplog):
    with caplog.at_level(logging.WARNING, logger="mcp_server.server"):
        response = await server.health_check(None)
    assert response.status_code == 503
    assert json.loads(response.body)["error"] == "orchestrator restarting"
    assert "ConnectError: orchestrator restarting" in caplog.text
