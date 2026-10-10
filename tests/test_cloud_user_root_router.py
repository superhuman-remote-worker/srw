"""``/api/me/cloud-root``: a user's own cloud root over HTTP (main cloud
slice 4).

Every route acts on the caller's own root (no user id in any path); a
response never carries the credential or the poll token; a provider without
user roots answers ``unsupported`` and refuses a connect (409); a cancel
stops the flow and stores nothing; a disconnect revokes and forgets.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from orchestrator.routers import cloud_user_root as routes
from orchestrator.services import cloud_user_root
from tests._mounted_router import mount_router
from tests.test_cloud_user_root import (
    ANN,
    POLL_TOKEN,
    TOKEN,
    FakeCloud,
    FakeRouter,
    FakeStore,
    NoRoots,
)


def _app(store, cloud_router, flows, caller):
    async def require_approved_user(_request, _db):
        return dict(caller)

    deps = routes.CloudUserRootDependencies(
        store=store,
        cloud_router=cloud_router,
        flows=flows,
        require_approved_user=require_approved_user,
    )
    return mount_router(
        routes.router,
        factories={"cloud_user_root_dependencies_factory": lambda: deps},
    )


def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    )


def test_no_route_names_another_user():
    paths = {route.path for route in routes.router.routes}
    assert paths == {"/api/me/cloud-root", "/api/me/cloud-root/connect"}


@pytest.mark.asyncio
async def test_connect_poll_and_disconnect_never_show_a_secret():
    store, cloud = FakeStore(), FakeCloud()
    flows = cloud_user_root.CloudRootConnectFlows(poll_seconds=0, flow_seconds=5)
    app = _app(store, FakeRouter(cloud), flows, store.users[ANN])
    seen: list[str] = []
    async with _client(app) as client:
        first = await client.get("/api/me/cloud-root")
        assert first.status_code == 200
        assert first.json()["state"] == "not_connected"
        seen.append(first.text)

        cloud.polls = [None, cloud.issue(TOKEN, "uid-ann")]
        started = await client.post("/api/me/cloud-root/connect")
        assert started.status_code == 200
        body = started.json()
        assert body["state"] == "connecting"
        assert body["login_url"] == "https://cloud.example/login/v2/flow/abc"
        seen.append(started.text)
        await asyncio.wait_for(flows.active(ANN).task, 5)

        connected = await client.get("/api/me/cloud-root")
        assert connected.json()["state"] == "connected"
        assert connected.json()["account_id"] == "uid-ann"
        seen.append(connected.text)

        gone = await client.delete("/api/me/cloud-root")
        assert gone.status_code == 200
        assert gone.json()["state"] == "revoked"
        assert gone.json()["provider_revoked"] is True
        seen.append(gone.text)
    assert cloud.revoked == [TOKEN]
    for text in seen:
        assert TOKEN not in text and POLL_TOKEN not in text


@pytest.mark.asyncio
async def test_cancel_stops_the_flow_and_stores_nothing():
    store, cloud = FakeStore(), FakeCloud()
    flows = cloud_user_root.CloudRootConnectFlows(poll_seconds=0.01, flow_seconds=60)
    app = _app(store, FakeRouter(cloud), flows, store.users[ANN])
    async with _client(app) as client:
        await client.post("/api/me/cloud-root/connect")
        assert (await client.get("/api/me/cloud-root")).json()["state"] == (
            "connecting"
        )
        cancelled = await client.delete("/api/me/cloud-root/connect")
        assert cancelled.json()["cancelled"] is True
        assert cancelled.json()["state"] == "not_connected"
    assert store.root()["credentials"] == {}


@pytest.mark.asyncio
async def test_a_provider_without_roots_is_unsupported_and_refuses_a_connect():
    store = FakeStore()
    flows = cloud_user_root.CloudRootConnectFlows()
    app = _app(store, FakeRouter(NoRoots()), flows, store.users[ANN])
    async with _client(app) as client:
        body = (await client.get("/api/me/cloud-root")).json()
        assert (body["supported"], body["state"], body["reason"]) == (
            False,
            "unsupported",
            "provider_unsupported",
        )
        refused = await client.post("/api/me/cloud-root/connect")
        assert refused.status_code == 409
        assert refused.json()["detail"] == "provider_unsupported"
    assert store.rows == {}


@pytest.mark.asyncio
async def test_a_cloud_that_cannot_start_the_flow_answers_502():
    from orchestrator.services.cloud.errors import (
        CloudBackendError,
        CloudBackendErrorKind,
    )

    store, cloud = FakeStore(), FakeCloud()

    async def down():
        raise CloudBackendError(
            CloudBackendErrorKind.UNAVAILABLE, "down", backend="acme-cloud"
        )

    cloud.start_login_flow = down
    app = _app(
        store, FakeRouter(cloud), cloud_user_root.CloudRootConnectFlows(), {"id": ANN}
    )
    async with _client(app) as client:
        refused = await client.post("/api/me/cloud-root/connect")
    assert refused.status_code == 502
    assert "unavailable" in refused.json()["detail"]
