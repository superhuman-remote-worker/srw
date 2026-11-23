"""Admin API: trust, re-trust and remove an endpoint's pinned certificate.

PUT/DELETE ``/api/admin/providers/endpoints/{id}/tls-pin`` against a mounted
router, a mock store and an ``inspect_tls`` / handshake stub. Every refusal
asserts nothing was stored.
"""

# ruff: noqa: F811  (pytest fixtures are requested by parameter name)
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from fastapi import HTTPException

from orchestrator.routers import provider_catalog
from orchestrator.services import endpoint_tls
from shared.subscription_routing import SUBSCRIPTION_PROXY_TRANSPORT
from shared.tls_pinning import TlsInspection
from tests._mounted_router import mount_router
from tests.test_endpoint_tls_test import (
    ADMIN_ID,
    ENDPOINT_ID,
    NO_PIN,
    REFUSED,
    URL,
    details,
    endpoint_row,
    model_row,
    pin_columns,
    unverified,
    verified,
)
from tests.tls_pin_fixtures import tls_certs  # noqa: F401

REAL_ASYNC_CLIENT = httpx.AsyncClient
PIN_URL = f"/api/admin/providers/endpoints/{ENDPOINT_ID}/tls-pin"
MISMATCH = "IP address mismatch, certificate is not valid for '10.18.2.105'."


class Stub:
    def __init__(self, result):
        self.result = result
        self.calls = []

    async def __call__(self, base_url, *args, **kwargs):
        self.calls.append((base_url, args, kwargs))
        return self.result


@pytest.fixture
def inspect(monkeypatch):
    def install(result: TlsInspection) -> Stub:
        stub = Stub(result)
        monkeypatch.setattr(endpoint_tls, "inspect_tls", stub)
        return stub

    return install


@pytest.fixture
def handshake(monkeypatch):
    stub = Stub(None)
    monkeypatch.setattr(endpoint_tls, "pinned_handshake_error", stub)
    return stub


@pytest.fixture
def store(tls_certs):
    s = MagicMock()
    s.get_system_llm_endpoint = AsyncMock(return_value=endpoint_row())
    s.list_models = AsyncMock(return_value=[model_row()])
    s.set_llm_endpoint_tls_pin = AsyncMock(
        side_effect=lambda eid, *, details, **kw: endpoint_row(pin=tls_certs.leaf)
    )
    s.clear_llm_endpoint_tls_pin = AsyncMock(return_value=endpoint_row())
    s.list_system_llm_endpoints = AsyncMock(return_value=[endpoint_row()])
    s.update_system_llm_endpoint = AsyncMock()
    s.get_system_setting = AsyncMock(return_value=None)
    return s


def make_app(store, admin=None):
    from orchestrator.services.provider_catalog import ProviderCatalogService

    if admin is None:
        admin = AsyncMock(return_value={"id": ADMIN_ID})
    deps = provider_catalog.ProviderCatalogDependencies(
        service=ProviderCatalogService(store, MagicMock(), MagicMock(), MagicMock()),
        require_admin=admin,
    )
    return mount_router(
        provider_catalog.router,
        factories={"provider_catalog_dependencies_factory": lambda: deps},
    )


async def call(store, method, path=PIN_URL, admin=None, **kwargs):
    app = make_app(store, admin)
    async with REAL_ASYNC_CLIENT(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        return await client.request(method, path, **kwargs)


def trust(store, fingerprint):
    return call(store, "PUT", json={"sha256": fingerprint})


@pytest.mark.asyncio
async def test_trust_stores_and_returns_the_pin(tls_certs, store, inspect, handshake):
    d = details(tls_certs.leaf)
    stub = inspect(unverified(tls_certs.leaf))
    response = await trust(store, d.sha256.lower())

    assert response.status_code == 200
    body = response.json()
    assert body["tls_pin"]["sha256"] == d.sha256
    assert body["id"] == ENDPOINT_ID
    store.set_llm_endpoint_tls_pin.assert_awaited_once()
    args, kwargs = store.set_llm_endpoint_tls_pin.call_args
    assert args == (ENDPOINT_ID,)
    assert kwargs["details"].sha256 == d.sha256
    assert kwargs["admin_user_id"] == ADMIN_ID
    assert kwargs["method"] == "PUT"
    assert kwargs["path"] == PIN_URL
    assert stub.calls[0][0] == URL
    assert handshake.calls[0][0] == URL


@pytest.mark.asyncio
async def test_retrust_inspects_with_the_stored_pin_and_replaces(
    tls_certs, store, inspect, handshake
):
    store.get_system_llm_endpoint.return_value = endpoint_row(pin=tls_certs.leaf)
    new = details(tls_certs.swap)
    stub = inspect(unverified(tls_certs.swap, "unknown_issuer", "unable to get"))
    response = await trust(store, new.sha256)

    assert response.status_code == 200
    assert stub.calls[0][2]["pin"].sha256 == details(tls_certs.leaf).sha256
    assert (
        store.set_llm_endpoint_tls_pin.call_args.kwargs["details"].sha256 == new.sha256
    )


@pytest.mark.asyncio
async def test_other_fingerprint_is_409_and_stores_nothing(
    tls_certs, store, inspect, handshake
):
    presented = details(tls_certs.swap)
    inspect(unverified(tls_certs.swap))
    response = await trust(store, details(tls_certs.leaf).sha256)

    assert response.status_code == 409
    assert response.json()["detail"] == (
        f"The server now presents a different certificate (SHA-256 "
        f"{presented.sha256}). Test again and compare."
    )
    store.set_llm_endpoint_tls_pin.assert_not_awaited()
    assert handshake.calls == []


@pytest.mark.asyncio
async def test_malformed_fingerprint_is_422(store, inspect):
    response = await trust(store, "not-a-fingerprint")
    assert response.status_code == 422
    store.set_llm_endpoint_tls_pin.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_endpoint_is_404(tls_certs, store, inspect):
    store.get_system_llm_endpoint.return_value = None
    response = await trust(store, details(tls_certs.leaf).sha256)
    assert response.status_code == 404
    assert response.json()["detail"] == "System endpoint not found"
    store.set_llm_endpoint_tls_pin.assert_not_awaited()


@pytest.mark.asyncio
async def test_http_endpoint_is_400(tls_certs, store, inspect):
    store.get_system_llm_endpoint.return_value = endpoint_row(
        base_url="http://10.18.2.105:8090/v1"
    )
    response = await trust(store, details(tls_certs.leaf).sha256)
    assert response.status_code == 400
    assert response.json()["detail"] == "Pinning applies to https:// endpoints."
    store.set_llm_endpoint_tls_pin.assert_not_awaited()


@pytest.mark.asyncio
async def test_subscription_proxy_is_400(tls_certs, store, inspect):
    store.get_system_llm_endpoint.return_value = endpoint_row(
        transport_kind=SUBSCRIPTION_PROXY_TRANSPORT
    )
    response = await trust(store, details(tls_certs.leaf).sha256)
    assert response.status_code == 400
    assert response.json()["detail"] == "The subscription proxy can't be pinned."
    store.set_llm_endpoint_tls_pin.assert_not_awaited()


@pytest.mark.asyncio
async def test_adapter_only_endpoint_is_400(tls_certs, store, inspect):
    store.list_models.return_value = [
        model_row(
            provider_kind="endpoint",
            capabilities=["web_search"],
            params_json={"provider": "searxng"},
        )
    ]
    refusal = endpoint_tls.endpoint_trust_refusal(
        endpoint_row(), store.list_models.return_value
    )
    assert refusal is not None
    response = await trust(store, details(tls_certs.leaf).sha256)
    assert response.status_code == 400
    assert response.json()["detail"] == refusal
    store.set_llm_endpoint_tls_pin.assert_not_awaited()


@pytest.mark.asyncio
async def test_live_voice_is_400(tls_certs, store, inspect):
    store.list_models.return_value = [model_row(capabilities=["live_voice"])]
    response = await trust(store, details(tls_certs.leaf).sha256)
    assert response.status_code == 400
    assert response.json()["detail"] == endpoint_tls.LIVE_VOICE_REFUSAL
    store.set_llm_endpoint_tls_pin.assert_not_awaited()


@pytest.mark.asyncio
async def test_unreadable_certificate_is_502(tls_certs, store, inspect):
    inspect(REFUSED)
    response = await trust(store, details(tls_certs.leaf).sha256)
    assert response.status_code == 502
    assert response.json()["detail"] == (
        "Could not read the server's certificate: [Errno 111] Connect call failed"
    )
    store.set_llm_endpoint_tls_pin.assert_not_awaited()


@pytest.mark.asyncio
async def test_certificate_that_verifies_is_400(tls_certs, store, inspect):
    inspect(verified(tls_certs.good))
    response = await trust(store, details(tls_certs.good).sha256)
    assert response.status_code == 400
    assert response.json()["detail"] == (
        "This certificate verifies normally; no pin is needed."
    )
    store.set_llm_endpoint_tls_pin.assert_not_awaited()


@pytest.mark.asyncio
async def test_verifying_certificate_with_a_pin_offers_removal(
    tls_certs, store, inspect
):
    store.get_system_llm_endpoint.return_value = endpoint_row(pin=tls_certs.leaf)
    inspect(verified(tls_certs.good))
    response = await trust(store, details(tls_certs.good).sha256)
    assert response.status_code == 400
    assert response.json()["detail"] == (
        "This certificate verifies normally; no pin is needed. Remove the pin instead."
    )
    store.set_llm_endpoint_tls_pin.assert_not_awaited()


@pytest.mark.asyncio
async def test_certificate_openssl_rejects_even_pinned_is_400(
    tls_certs, store, inspect, handshake
):
    inspect(unverified(tls_certs.leaf))
    handshake.result = "certificate key too weak"
    response = await trust(store, details(tls_certs.leaf).sha256)
    assert response.status_code == 400
    assert response.json()["detail"] == (
        "SRW can't use this certificate even when pinned: certificate key too weak"
    )
    store.set_llm_endpoint_tls_pin.assert_not_awaited()


@pytest.mark.asyncio
async def test_non_admin_is_403_and_stores_nothing(tls_certs, store, inspect):
    denied = AsyncMock(side_effect=HTTPException(status_code=403, detail="no"))
    put = await call(
        store, "PUT", admin=denied, json={"sha256": details(tls_certs.leaf).sha256}
    )
    delete = await call(store, "DELETE", admin=denied)
    assert (put.status_code, delete.status_code) == (403, 403)
    store.set_llm_endpoint_tls_pin.assert_not_awaited()
    store.clear_llm_endpoint_tls_pin.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_removes_the_pin_and_list_shows_null(tls_certs, store):
    store.get_system_llm_endpoint.return_value = endpoint_row(pin=tls_certs.leaf)
    response = await call(store, "DELETE")
    assert response.status_code == 200
    assert response.json()["tls_pin"] is None
    store.clear_llm_endpoint_tls_pin.assert_awaited_once_with(
        ENDPOINT_ID, admin_user_id=ADMIN_ID, method="DELETE", path=PIN_URL
    )
    listing = await call(store, "GET", path="/api/admin/providers/endpoints")
    assert listing.status_code == 200
    assert listing.json()[0]["tls_pin"] is None


@pytest.mark.asyncio
async def test_delete_without_a_pin_is_404(store):
    store.clear_llm_endpoint_tls_pin.return_value = None
    response = await call(store, "DELETE")
    assert response.status_code == 404
    assert response.json()["detail"] == "This endpoint has no pinned certificate."


@pytest.mark.asyncio
async def test_patch_that_changes_the_port_returns_the_unchanged_pin(tls_certs, store):
    store.update_system_llm_endpoint.return_value = endpoint_row(
        pin=tls_certs.leaf, base_url="https://10.18.2.105:8443/v1"
    )
    response = await call(
        store,
        "PATCH",
        path=f"/api/admin/providers/endpoints/{ENDPOINT_ID}",
        json={"base_url": "https://10.18.2.105:8443/v1"},
    )
    assert response.status_code == 200
    assert response.json()["tls_pin"]["sha256"] == details(tls_certs.leaf).sha256


def test_no_pin_columns_serialize_as_null():
    from orchestrator.services.provider_catalog import _serialize_endpoint

    assert _serialize_endpoint(endpoint_row())["tls_pin"] is None
    assert NO_PIN and pin_columns
