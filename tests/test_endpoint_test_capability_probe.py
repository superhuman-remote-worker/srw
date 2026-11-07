"""Admin endpoint Test probes by the capability its catalog rows declare.

Hugging Face TEI serves ``POST /embeddings`` (or ``/rerank``) but has no
``GET /models``. The Test route must not report such an endpoint as failing
when the capability call it exists for succeeds. Requests go to a fake server
standing in for the endpoint's host; no network is used.
"""

import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import httpx
import pytest

from tests._mounted_router import mount_router

REAL_ASYNC_CLIENT = httpx.AsyncClient

ENDPOINT_ID = "33333333-3333-4333-8333-333333333333"
BASE_URL = "http://tei.invalid:8081/v1"
API_KEY = "tei-secret-key"
ROW_ID = "44444444-4444-4444-8444-444444444444"

EMBEDDINGS_OK = httpx.Response(
    200,
    json={
        "object": "list",
        "data": [{"object": "embedding", "index": 0, "embedding": [0.1, 0.2]}],
        "model": "Qwen/Qwen3-Embedding-8B",
    },
)
RERANK_OK = httpx.Response(
    200, json={"results": [{"index": 0, "relevance_score": 0.91}]}
)


def endpoint_row(**updates):
    return {
        "id": UUID(ENDPOINT_ID),
        "label": "TEI",
        "base_url": BASE_URL,
        "api_key": API_KEY,
        "key_prefix": "tei-secr",
        "transport_kind": None,
        "created_at": datetime(2026, 11, 3, tzinfo=timezone.utc),
        "updated_at": None,
        **updates,
    }


def model_row(**updates):
    return {
        "id": UUID(ROW_ID),
        "provider_kind": "endpoint",
        "provider_ref": ENDPOINT_ID,
        "model_id": "qwen3-embedding-8b",
        "served_model_id": None,
        "display_label": "Qwen3 Embedding 8B",
        "capabilities": ["embedding"],
        "family": "qwen",
        "context_window": None,
        "params_json": None,
        "enabled": True,
        **updates,
    }


class FakeServer:
    """The endpoint's host: canned responses keyed by (method, path)."""

    def __init__(self, routes):
        self.routes = routes
        self.requests: list[httpx.Request] = []
        self.client_kwargs: list[dict] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        route = self.routes.get((request.method, request.url.path))
        if route is None:
            return httpx.Response(404, json={"error": "Not Found"})
        return route

    def calls(self) -> list[tuple[str, str]]:
        return [(r.method, str(r.url)) for r in self.requests]


@pytest.fixture
def serve(monkeypatch):
    """Route every outbound probe client through a fake endpoint host."""

    def install(routes) -> FakeServer:
        server = FakeServer(routes)

        # A subclass, so modules imported later that subclass httpx.AsyncClient
        # (the openai SDK) still import.
        class Client(REAL_ASYNC_CLIENT):
            def __init__(self, **kwargs):
                server.client_kwargs.append(dict(kwargs))
                kwargs["transport"] = httpx.MockTransport(server.handle)
                super().__init__(**kwargs)

        monkeypatch.setattr(httpx, "AsyncClient", Client)
        return server

    return install


@pytest.fixture
def store():
    store = MagicMock()
    store.get_system_llm_endpoint = AsyncMock(return_value=endpoint_row())
    store.list_models = AsyncMock(return_value=[model_row()])
    return store


async def post_test(store) -> httpx.Response:
    from orchestrator.routers import provider_catalog
    from orchestrator.services import discovery, subscription_discovery
    from orchestrator.services.llm_endpoint_probe import probe_endpoint_models
    from orchestrator.services.provider_catalog import ProviderCatalogService

    deps = provider_catalog.ProviderCatalogDependencies(
        service=ProviderCatalogService(
            store, discovery, probe_endpoint_models, subscription_discovery
        ),
        require_admin=AsyncMock(return_value={"id": "admin"}),
    )
    app = mount_router(
        provider_catalog.router,
        factories={"provider_catalog_dependencies_factory": lambda: deps},
    )
    async with REAL_ASYNC_CLIENT(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        return await client.post(f"/api/admin/providers/endpoints/{ENDPOINT_ID}/test")


@pytest.mark.asyncio
async def test_tei_embedding_endpoint_without_model_list_tests_ok(serve, store):
    store.list_models.return_value = [
        model_row(served_model_id="Qwen/Qwen3-Embedding-8B")
    ]
    server = serve({("POST", "/v1/embeddings"): EMBEDDINGS_OK})

    response = await post_test(store)

    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "status": 200,
        "error": None,
        "probe_url": f"{BASE_URL}/embeddings",
    }
    assert server.calls() == [
        ("GET", f"{BASE_URL}/models"),
        ("POST", f"{BASE_URL}/embeddings"),
    ]
    embed = server.requests[1]
    # The upstream name the row serves, as the embedding client sends it.
    assert json.loads(embed.content) == {
        "model": "Qwen/Qwen3-Embedding-8B",
        "input": "ping",
    }
    assert embed.headers["authorization"] == f"Bearer {API_KEY}"
    # Same timeout as the model-list probe.
    assert [kwargs.get("timeout") for kwargs in server.client_kwargs] == [10.0, 10.0]
    store.list_models.assert_awaited_once_with(
        provider_kind="endpoint", provider_ref=ENDPOINT_ID
    )


@pytest.mark.asyncio
async def test_rerank_endpoint_is_probed_with_the_rerank_clients_shape(serve, store):
    store.list_models.return_value = [
        model_row(model_id="bge-reranker-v2-m3", capabilities=["rerank"])
    ]
    server = serve({("POST", "/v1/rerank"): RERANK_OK})

    response = await post_test(store)

    assert response.json() == {
        "ok": True,
        "status": 200,
        "error": None,
        "probe_url": f"{BASE_URL}/rerank",
    }
    rerank = server.requests[-1]
    assert (rerank.method, str(rerank.url)) == ("POST", f"{BASE_URL}/rerank")
    body = json.loads(rerank.content)
    assert set(body) == {"model", "query", "documents"}
    assert body["model"] == "bge-reranker-v2-m3"
    assert isinstance(body["query"], str) and body["query"]
    assert isinstance(body["documents"], list) and len(body["documents"]) == 1


@pytest.mark.asyncio
async def test_failing_capability_call_reports_its_own_failure(serve, store):
    server = serve(
        {("POST", "/v1/embeddings"): httpx.Response(401, json={"error": "bad key"})}
    )

    response = await post_test(store)

    assert response.json() == {
        "ok": False,
        "status": 401,
        "error": '{"error":"bad key"}',
        "probe_url": f"{BASE_URL}/embeddings",
    }
    assert len(server.requests) == 2


@pytest.mark.asyncio
async def test_capability_answer_without_its_payload_is_not_ok(serve, store):
    # A web UI behind the base URL answers 200 with a page, not vectors.
    serve(
        {
            ("POST", "/v1/embeddings"): httpx.Response(
                200, text="<html></html>", headers={"content-type": "text/html"}
            )
        }
    )

    body = (await post_test(store)).json()

    assert body["ok"] is False
    assert body["status"] == 200
    assert body["probe_url"] == f"{BASE_URL}/embeddings"
    assert "web page" in body["error"]


@pytest.mark.asyncio
async def test_chat_endpoint_keeps_the_model_list_result(serve, store):
    store.list_models.return_value = [
        model_row(model_id="qwen3-32b", capabilities=["chat", "auxiliary"])
    ]
    server = serve({})

    body = (await post_test(store)).json()

    assert body["ok"] is False
    assert body["status"] == 404
    assert body["probe_url"] == f"{BASE_URL}/models"
    # No completion is spent on a chat endpoint.
    assert server.calls() == [("GET", f"{BASE_URL}/models")]


@pytest.mark.asyncio
async def test_embedding_endpoint_with_a_model_list_costs_no_extra_call(serve, store):
    server = serve(
        {("GET", "/v1/models"): httpx.Response(200, json={"data": [{"id": "e5"}]})}
    )

    body = (await post_test(store)).json()

    assert body == {
        "ok": True,
        "status": 200,
        "error": None,
        "probe_url": f"{BASE_URL}/models",
    }
    assert server.calls() == [("GET", f"{BASE_URL}/models")]


@pytest.mark.asyncio
async def test_model_list_failure_other_than_404_is_not_masked(serve, store):
    server = serve(
        {("GET", "/v1/models"): httpx.Response(401, json={"error": "bad key"})}
    )

    body = (await post_test(store)).json()

    assert body["ok"] is False
    assert body["status"] == 401
    assert body["probe_url"] == f"{BASE_URL}/models"
    assert server.calls() == [("GET", f"{BASE_URL}/models")]


# --- The model row's Test: the row's own capability decides the probe -------


async def post_row_test(store) -> httpx.Response:
    from orchestrator.routers import model_catalog
    from orchestrator.services.llm_endpoint_probe import probe_endpoint_models
    from orchestrator.services.model_catalog import ModelCatalogService

    deps = model_catalog.ModelCatalogDependencies(
        service=ModelCatalogService(
            store=store,
            probe=probe_endpoint_models,
            get_config_dir=MagicMock(),
            load_settings_matrix=MagicMock(),
            settings_for_family=MagicMock(),
            family_detector=MagicMock(),
            reasoning_capability=MagicMock(),
        ),
        require_admin=AsyncMock(return_value={"id": "admin"}),
        require_approved_user=AsyncMock(),
    )
    app = mount_router(
        model_catalog.router,
        factories={"model_catalog_dependencies_factory": lambda: deps},
    )
    async with REAL_ASYNC_CLIENT(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        return await client.post(f"/api/admin/providers/models/{ROW_ID}/test")


@pytest.mark.asyncio
async def test_tei_embedding_row_without_model_list_tests_ok(serve, store):
    store.get_model = AsyncMock(
        return_value=model_row(served_model_id="Qwen/Qwen3-Embedding-8B")
    )
    server = serve({("POST", "/v1/embeddings"): EMBEDDINGS_OK})

    response = await post_row_test(store)

    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "status": 200,
        "error": None,
        "probe_url": f"{BASE_URL}/embeddings",
    }
    assert server.calls() == [
        ("GET", f"{BASE_URL}/models"),
        ("POST", f"{BASE_URL}/embeddings"),
    ]
    embed = server.requests[1]
    assert json.loads(embed.content) == {
        "model": "Qwen/Qwen3-Embedding-8B",
        "input": "ping",
    }
    assert embed.headers["authorization"] == f"Bearer {API_KEY}"
    assert [kwargs.get("timeout") for kwargs in server.client_kwargs] == [10.0, 10.0]


@pytest.mark.asyncio
async def test_rerank_row_is_probed_by_its_own_capability(serve, store):
    # The endpoint also carries an embedding row; the tested row decides.
    store.get_model = AsyncMock(
        return_value=model_row(model_id="bge-reranker-v2-m3", capabilities=["rerank"])
    )
    server = serve({("POST", "/v1/rerank"): RERANK_OK})

    body = (await post_row_test(store)).json()

    assert body == {
        "ok": True,
        "status": 200,
        "error": None,
        "probe_url": f"{BASE_URL}/rerank",
    }
    assert json.loads(server.requests[-1].content)["model"] == "bge-reranker-v2-m3"
    store.list_models.assert_not_awaited()


@pytest.mark.asyncio
async def test_chat_row_keeps_the_model_list_result(serve, store):
    store.get_model = AsyncMock(
        return_value=model_row(model_id="qwen3-32b", capabilities=["chat"])
    )
    server = serve({})

    body = (await post_row_test(store)).json()

    assert body["ok"] is False
    assert body["status"] == 404
    assert body["probe_url"] == f"{BASE_URL}/models"
    assert server.calls() == [("GET", f"{BASE_URL}/models")]


@pytest.mark.asyncio
async def test_row_probe_drops_the_routing_prefix_its_provider_rejects(serve, store):
    # Same wire_model_id rule as the embedding client: the provider's own API
    # names the model without the ``openrouter/`` routing prefix.
    store.get_system_llm_endpoint.return_value = endpoint_row(
        base_url="https://openrouter.ai/api/v1"
    )
    store.get_model = AsyncMock(
        return_value=model_row(model_id="openrouter/qwen/qwen3-embedding-8b")
    )
    server = serve({("POST", "/api/v1/embeddings"): EMBEDDINGS_OK})

    body = (await post_row_test(store)).json()

    assert body["ok"] is True
    assert json.loads(server.requests[-1].content)["model"] == (
        "qwen/qwen3-embedding-8b"
    )


@pytest.mark.asyncio
async def test_capability_probe_reports_a_transport_error_without_raising(serve):
    from orchestrator.services.llm_endpoint_probe import probe_endpoint_capability

    def refuse(request):
        raise httpx.ConnectError("connection refused", request=request)

    server = serve({})
    server.handle = refuse

    result = await probe_endpoint_capability(
        base_url=BASE_URL, api_key=None, capability="embedding", model="m"
    )

    assert result.ok is False
    assert result.status is None
    assert result.error == "ConnectError: connection refused"
    assert result.probe_url == f"{BASE_URL}/embeddings"
