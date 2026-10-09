"""The agent's question about a git swap binding's driver pod, over HTTP.

git_swap_refused_driver_still_costs_the_full_wait: the route a waiting first
clone asks whether the binding's pod was refused. Internal (the shared key,
as every agent route), answered for the lease token in the body and nothing
else, never cached. The answer itself (``binding_driver_state``) is proven
against PostgreSQL in ``test_connector_git_swap_delivery_real_postgres.py``.
"""

from __future__ import annotations

import contextlib
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from tests._route_inventory import mounted_routes

from orchestrator.application import connectors as connectors_composition
from orchestrator.application import routes as routes_composition
from orchestrator.routers import agent_git_swap as routes
from orchestrator.security import access
from shared.connectors.git_swap import DRIVER_STATE_PATH

KEY = "internal-key-for-the-test"
TOKEN = "scl_" + "A" * 49


class _Store:
    def __init__(self) -> None:
        self.conn = object()
        self.acquired = 0

    @contextlib.asynccontextmanager
    async def acquire(self):
        self.acquired += 1
        yield self.conn


async def _require_internal(request: Request) -> None:
    if request.headers.get("X-Internal-Key") != KEY:
        raise HTTPException(status_code=401, detail="Invalid internal key")


def _client(store: _Store) -> TestClient:
    app = FastAPI()
    app.state.agent_git_swap_dependencies_factory = lambda: (
        routes.AgentGitSwapDependencies(store=store, require_internal=_require_internal)
    )
    app.include_router(routes.router)
    return TestClient(app)


@pytest.fixture
def answered(monkeypatch):
    asked: list[tuple[Any, Any]] = []

    async def state(conn, token):
        asked.append((conn, token))
        return {"state": "refused", "reason": "its driver pod did not start"}

    monkeypatch.setattr(routes, "binding_driver_state", state)
    return asked


def test_the_route_is_the_one_the_agent_asks():
    [route] = routes.router.routes
    assert route.path == DRIVER_STATE_PATH
    assert route.methods == {"POST"}
    # Under /api/agents: the ingress strips it, a PAT is refused.
    assert DRIVER_STATE_PATH.startswith("/api/agents/")


def test_an_agent_learns_its_bindings_state_never_cached(answered):
    store = _Store()
    response = _client(store).post(
        DRIVER_STATE_PATH,
        json={"lease_token": TOKEN},
        headers={"X-Internal-Key": KEY},
    )
    assert response.status_code == 200
    assert response.json() == {
        "state": "refused",
        "reason": "its driver pod did not start",
    }
    assert "no-store" in response.headers["cache-control"]
    # The token from the body is the question, on the app's own store.
    assert answered == [(store.conn, TOKEN)]


def test_without_the_internal_key_nothing_is_read(answered):
    store = _Store()
    client = _client(store)
    for headers in ({}, {"X-Internal-Key": "wrong"}):
        response = client.post(
            DRIVER_STATE_PATH, json={"lease_token": TOKEN}, headers=headers
        )
        assert response.status_code == 401
    assert answered == [] and store.acquired == 0


@pytest.mark.parametrize(
    "body",
    [
        {},
        {"lease_token": TOKEN, "connector_id": "x"},
        {"lease_token": "s" * 129},
        {"token": TOKEN},
    ],
)
def test_only_a_lease_token_is_accepted(answered, body):
    response = _client(_Store()).post(
        DRIVER_STATE_PATH, json=body, headers={"X-Internal-Key": KEY}
    )
    # (FastAPI's 422 names the input back to its sender only; the agent
    # never sends any of these and reads no body but a 200's.)
    assert response.status_code == 422
    assert answered == []


def test_the_application_mounts_it_with_the_internal_guard_and_its_store():
    app = FastAPI()
    routes_composition.include_routers(app)
    assert ("POST", DRIVER_STATE_PATH) in mounted_routes(app)
    store = object()
    dependencies = connectors_composition.agent_git_swap_dependencies(
        SimpleNamespace(postgres_db=store)
    )
    assert dependencies.store is store
    assert dependencies.require_internal is access.require_internal
