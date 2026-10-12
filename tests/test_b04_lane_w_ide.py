"""Wire contracts for the extracted IDE session and code-server proxy.

R1.B04 lane W. These are characterization tests: they pin the refusal
*ordering*, the exact ``detail`` shapes, the two header allow-lists, the
secret-query redaction, the WebSocket close codes, and the stream cleanup that
must survive the move out of ``orchestrator.main``.

Nothing here imports ``orchestrator.main``; every collaborator arrives through
``app.state.ide_dependencies_factory``, which is also how the per-invocation
resolution is proved.
"""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from fastapi import FastAPI, HTTPException

from orchestrator.routers import ide as ide_routes
from orchestrator.routers.ide import IdeDependencies, router
from orchestrator.services.ide_proxy import IdeProxyUnavailable
from orchestrator.services.ide_proxy_gateway import (
    _IDE_PROXY_REQUEST_ALLOW_HEADERS,
    _IDE_PROXY_RESPONSE_ALLOW_HEADERS,
    _IDE_PROXY_SECRET_QUERY_FIELDS,
    _ide_proxy_query,
    _ide_ws_runtime_is_current,
    _IdeProxyAuthorityLost,
    _IdeProxyHttpResponse,
    _is_browser_navigation,
    _request_exact_ide_http,
    _require_stateless_ide_lifecycle,
)

JOB = "44444444-4444-4444-8444-444444444444"
USER = {"id": "u-1", "is_approved": True}
PROXY_PATH = f"/api/ide/{JOB}/proxy/workspace"


# =============================================================================
# Harness
# =============================================================================


@pytest.fixture
def wire():
    """One app whose IDE collaborators are rebuildable between requests."""

    events: list[str] = []

    async def require_job_access(request, store, job_id):
        events.append("job_access")
        if request.headers.get("x-test-user") != USER["id"]:
            raise HTTPException(status_code=401, detail="Authentication required")
        return USER, {"id": job_id}

    async def require_approved_user(request, store):
        events.append("approved")
        code = request.headers.get("x-test-refuse")
        if code:
            raise HTTPException(status_code=int(code), detail="refused")
        return USER

    async def user_can_access_ide_entity(user, store, entity_id):
        events.append("ide_entity")
        return entity_id == JOB

    async def log_security_event(store, **kwargs):
        events.append("audit")

    async def resolve_ws_user(ws, store):
        events.append("ws_user")
        return USER

    holder = SimpleNamespace(
        store=SimpleNamespace(name="store-1"),
        ide_sessions=SimpleNamespace(
            start_session=AsyncMock(return_value={"status": "active", "gen": 1}),
            get_session_status=AsyncMock(return_value={"status": "active", "gen": 1}),
            stop_session=AsyncMock(return_value={"status": "expired", "gen": 1}),
        ),
        ide_proxy=SimpleNamespace(
            resolve_target=AsyncMock(return_value=None),
            revalidate_target=AsyncMock(return_value=True),
            evict=MagicMock(),
        ),
        builds=0,
    )

    def factory():
        holder.builds += 1
        return IdeDependencies(
            store=holder.store,
            ide_sessions=holder.ide_sessions,
            ide_proxy=holder.ide_proxy,
            require_approved_user=require_approved_user,
            require_job_access=require_job_access,
            resolve_ws_user=resolve_ws_user,
            user_can_access_ide_entity=user_can_access_ide_entity,
            log_security_event=log_security_event,
        )

    app = FastAPI()
    app.state.ide_dependencies_factory = factory
    app.include_router(router)
    return SimpleNamespace(app=app, holder=holder, events=events, factory=factory)


_DEFAULT_HEADERS = object()


async def call(wire, method, path, *, headers=_DEFAULT_HEADERS, **kwargs):
    if headers is _DEFAULT_HEADERS:
        headers = {"x-test-user": USER["id"]}
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=wire.app), base_url="http://ide.test"
    ) as client:
        return await client.request(method, path, headers=headers, **kwargs)


def target(**overrides):
    base = {
        "backend": "k8s",
        "credential": "c0ffee",
        "host": "10.42.0.7",
        "port": 38080,
        "authority": "10.42.0.7:38080",
    }
    base.update(overrides)
    return SimpleNamespace(**base)


# =============================================================================
# Session routes
# =============================================================================


class TestIdeSessionRoutes:
    @pytest.mark.asyncio
    async def test_start_gate_precedes_the_service(self, wire):
        response = await call(wire, "POST", f"/api/jobs/{JOB}/ide", headers={})

        assert response.status_code == 401
        assert wire.events == ["job_access"]
        wire.holder.ide_sessions.start_session.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_defaults_the_body_when_absent(self, wire):
        response = await call(wire, "POST", f"/api/jobs/{JOB}/ide")

        assert response.status_code == 200
        assert response.json() == {"status": "active", "gen": 1}
        assert wire.holder.ide_sessions.start_session.await_args.kwargs == {
            "job_id": JOB,
            "cpu_cores": 8,
            "memory": "16Gi",
            "idle_timeout_minutes": None,
        }

    @pytest.mark.asyncio
    async def test_start_forwards_an_explicit_body(self, wire):
        response = await call(
            wire,
            "POST",
            f"/api/jobs/{JOB}/ide",
            json={"cpu_cores": 2, "memory": "4Gi", "idle_timeout_minutes": 5},
        )

        assert response.status_code == 200
        assert wire.holder.ide_sessions.start_session.await_args.kwargs == {
            "job_id": JOB,
            "cpu_cores": 2,
            "memory": "4Gi",
            "idle_timeout_minutes": 5,
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "method,path_suffix,attribute",
        [
            ("POST", "", "start_session"),
            ("GET", "", "get_session_status"),
            ("DELETE", "", "stop_session"),
        ],
    )
    async def test_service_failure_is_a_500_carrying_str_of_the_error(
        self, wire, method, path_suffix, attribute
    ):
        getattr(wire.holder.ide_sessions, attribute).side_effect = RuntimeError("boom")

        response = await call(wire, method, f"/api/jobs/{JOB}/ide{path_suffix}")

        assert response.status_code == 500
        assert response.json()["detail"] == "boom"

    @pytest.mark.asyncio
    async def test_each_request_rebuilds_the_dependencies(self, wire):
        await call(wire, "GET", f"/api/jobs/{JOB}/ide")
        first = wire.holder.builds

        replacement = SimpleNamespace(
            get_session_status=AsyncMock(return_value={"status": "swapped"})
        )
        wire.holder.ide_sessions = replacement

        response = await call(wire, "GET", f"/api/jobs/{JOB}/ide")

        assert wire.holder.builds > first
        assert response.json() == {"status": "swapped"}
        replacement.get_session_status.assert_awaited_once_with(JOB)


# =============================================================================
# HTTP proxy — admission ordering
# =============================================================================


class TestIdeProxyHttpAdmission:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name", ["serviceWorker.js", "service-worker.js", "out/serviceWorker.js"]
    )
    async def test_service_worker_stub_is_served_before_authentication(
        self, wire, name
    ):
        response = await call(wire, "GET", f"/api/ide/{JOB}/proxy/{name}", headers={})

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("application/javascript")
        assert response.headers["cache-control"] == "no-store"
        assert "skipWaiting" in response.text
        assert wire.events == []

    @pytest.mark.asyncio
    async def test_expired_session_on_a_navigation_redirects_to_login(self, wire):
        response = await call(
            wire,
            "GET",
            PROXY_PATH,
            headers={"x-test-refuse": "401", "sec-fetch-mode": "navigate"},
        )

        assert response.status_code == 302
        assert response.headers["location"] == "/auth/login?return_to=/"
        assert wire.events == ["approved"]

    @pytest.mark.asyncio
    async def test_expired_session_on_a_subresource_stays_a_401(self, wire):
        response = await call(wire, "GET", PROXY_PATH, headers={"x-test-refuse": "401"})

        assert response.status_code == 401

    @pytest.mark.asyncio
    async def test_a_403_never_loops_an_authenticated_user_through_login(self, wire):
        response = await call(
            wire,
            "GET",
            PROXY_PATH,
            headers={"x-test-refuse": "403", "sec-fetch-mode": "navigate"},
        )

        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_entity_refusal_audits_then_403s(self, wire, monkeypatch):
        lifecycle = AsyncMock()
        monkeypatch.setattr(ide_routes, "_require_stateless_ide_lifecycle", lifecycle)

        response = await call(wire, "GET", "/api/ide/other-job/proxy/workspace")

        assert response.status_code == 403
        assert response.json()["detail"] == "IDE access denied"
        assert wire.events == ["approved", "ide_entity", "audit"]
        lifecycle.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("method", ["POST", "PUT", "DELETE", "PATCH"])
    async def test_mutation_is_refused_before_the_lifecycle_fence(
        self, wire, monkeypatch, method
    ):
        lifecycle = AsyncMock()
        monkeypatch.setattr(ide_routes, "_require_stateless_ide_lifecycle", lifecycle)

        response = await call(wire, method, PROXY_PATH)

        # PROXY_PATH carries no ``_vm/<lease>`` prefix: only a VM access lease
        # admits a mutation (through a durable IDE operation row), so the
        # unleased path still refuses before any lifecycle or upstream work.
        assert response.status_code == 503
        assert response.json()["detail"] == {
            "code": "ide_mutation_operation_lease_unavailable",
            "message": "IDE mutation transport is unavailable",
        }
        lifecycle.assert_not_awaited()
        wire.holder.ide_proxy.resolve_target.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_lifecycle_fence_refusal_reaches_the_client(self, wire, monkeypatch):
        monkeypatch.setattr(
            ide_routes,
            "_require_stateless_ide_lifecycle",
            AsyncMock(
                side_effect=HTTPException(
                    status_code=409,
                    detail="Stateless workspace lifecycle is not admitting IDE traffic",
                )
            ),
        )

        response = await call(wire, "GET", PROXY_PATH)

        assert response.status_code == 409
        assert (
            response.json()["detail"]
            == "Stateless workspace lifecycle is not admitting IDE traffic"
        )
        wire.holder.ide_proxy.resolve_target.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_target_is_a_503(self, wire, monkeypatch):
        monkeypatch.setattr(ide_routes, "_require_stateless_ide_lifecycle", AsyncMock())

        response = await call(wire, "GET", PROXY_PATH)

        assert response.status_code == 503
        assert response.json()["detail"] == "IDE session not active"


# =============================================================================
# HTTP proxy — transport boundary
# =============================================================================


@pytest.fixture
def proxied(wire, monkeypatch):
    """Admission granted, one target resolved, the transport captured."""

    monkeypatch.setattr(ide_routes, "_require_stateless_ide_lifecycle", AsyncMock())
    monkeypatch.setattr(
        "orchestrator.services.ssh_helpers.orchestrator_can_reach", lambda _host: True
    )
    wire.holder.ide_proxy.resolve_target = AsyncMock(return_value=target())
    captured: dict = {}

    async def transport(**kwargs):
        captured.update(kwargs)
        return _IdeProxyHttpResponse(
            status_code=207,
            headers=(
                ("content-type", "text/plain"),
                ("etag", 'W/"a"'),
                ("set-cookie", "session=leak"),
                ("www-authenticate", "Basic realm=x"),
                ("x-frame-options", "DENY"),
            ),
            body=b"payload",
        )

    monkeypatch.setattr(ide_routes, "_request_exact_ide_http", transport)
    wire.captured = captured
    return wire


class TestIdeProxyHttpTransportBoundary:
    @pytest.mark.asyncio
    async def test_only_allow_listed_request_headers_reach_the_upstream(self, proxied):
        response = await call(
            proxied,
            "GET",
            PROXY_PATH,
            headers={
                "x-test-user": USER["id"],
                "accept": "text/html",
                "range": "bytes=0-1",
                "user-agent": "probe/1",
                "cookie": "session=leak",
                "authorization": "Bearer leak",
                "x-forwarded-user": "attacker",
                "sec-fetch-mode": "cors",
            },
        )

        assert response.status_code == 207
        sent = {k.lower(): v for k, v in proxied.captured["headers"].items()}
        assert sent["accept"] == "text/html"
        assert sent["range"] == "bytes=0-1"
        assert sent["user-agent"] == "probe/1"
        assert "cookie" not in sent
        assert "authorization" not in sent
        assert "x-forwarded-user" not in sent
        assert "sec-fetch-mode" not in sent
        # Server-derived forwarding fields, plus the identity encoding that
        # keeps the bounded raw-byte reader honest.
        assert sent["x-forwarded-proto"] == "https"
        assert sent["accept-encoding"] == "identity"
        assert sent["host"] == "10.42.0.7:38080"
        assert "x-forwarded-for" in sent

    def test_no_authentication_header_is_on_the_request_allow_list(self):
        for name in ("cookie", "authorization", "proxy-authorization", "x-mcp-token"):
            assert name not in _IDE_PROXY_REQUEST_ALLOW_HEADERS

    def test_no_identity_minting_header_is_on_the_response_allow_list(self):
        for name in ("set-cookie", "www-authenticate", "authorization"):
            assert name not in _IDE_PROXY_RESPONSE_ALLOW_HEADERS

    @pytest.mark.asyncio
    async def test_only_allow_listed_response_headers_reach_the_browser(self, proxied):
        response = await call(proxied, "GET", PROXY_PATH)

        assert response.status_code == 207
        assert response.content == b"payload"
        assert response.headers["etag"] == 'W/"a"'
        assert "set-cookie" not in response.headers
        assert "www-authenticate" not in response.headers
        assert "x-frame-options" not in response.headers

    @pytest.mark.asyncio
    async def test_secret_query_fields_never_reach_the_upstream_url(self, proxied):
        await call(
            proxied,
            "GET",
            PROXY_PATH
            + "?token=leak&reconnectionToken=keep&Access_Token=leak2&folder=/w",
        )

        url = proxied.captured["url"]
        assert "token=leak" not in url
        assert "Access_Token" not in url
        assert "reconnectionToken=keep" in url
        assert "folder=%2Fw" in url or "folder=/w" in url

    @pytest.mark.asyncio
    async def test_the_injected_proxy_is_what_the_transport_receives(self, proxied):
        await call(proxied, "GET", PROXY_PATH)

        assert proxied.captured["ide_proxy"] is proxied.holder.ide_proxy
        assert proxied.captured["content"] is None

    @pytest.mark.asyncio
    async def test_authority_loss_evicts_and_503s(self, proxied, monkeypatch):
        async def transport(**_kwargs):
            raise _IdeProxyAuthorityLost

        monkeypatch.setattr(ide_routes, "_request_exact_ide_http", transport)

        response = await call(proxied, "GET", PROXY_PATH)

        assert response.status_code == 503
        assert response.json()["detail"] == "IDE runtime authority changed"
        proxied.holder.ide_proxy.evict.assert_called_once_with(JOB)

    @pytest.mark.asyncio
    async def test_connect_error_evicts_and_502s(self, proxied, monkeypatch):
        async def transport(**_kwargs):
            raise httpx.ConnectError("refused")

        monkeypatch.setattr(ide_routes, "_request_exact_ide_http", transport)

        response = await call(proxied, "GET", PROXY_PATH)

        assert response.status_code == 502
        assert response.json()["detail"] == "code-server unreachable"
        proxied.holder.ide_proxy.evict.assert_called_once_with(JOB)

    @pytest.mark.asyncio
    async def test_timeout_504s_without_evicting(self, proxied, monkeypatch):
        async def transport(**_kwargs):
            raise httpx.TimeoutException("slow")

        monkeypatch.setattr(ide_routes, "_request_exact_ide_http", transport)

        response = await call(proxied, "GET", PROXY_PATH)

        assert response.status_code == 504
        assert response.json()["detail"] == "code-server timeout"
        proxied.holder.ide_proxy.evict.assert_not_called()

    @pytest.mark.asyncio
    async def test_typed_unavailability_keeps_its_code_and_message(
        self, proxied, monkeypatch
    ):
        async def transport(**_kwargs):
            raise IdeProxyUnavailable("ide_response_too_large", "too big")

        monkeypatch.setattr(ide_routes, "_request_exact_ide_http", transport)

        response = await call(proxied, "GET", PROXY_PATH)

        assert response.status_code == 503
        assert response.json()["detail"] == {
            "code": "ide_response_too_large",
            "message": "too big",
        }

    @pytest.mark.asyncio
    async def test_unreachable_vm_host_is_a_typed_503(self, proxied, monkeypatch):
        monkeypatch.setattr(
            "orchestrator.services.ssh_helpers.orchestrator_can_reach",
            lambda _host: False,
        )

        response = await call(proxied, "GET", PROXY_PATH)

        assert response.status_code == 503
        assert (
            response.json()["detail"]
            == "IDE is not yet available for VM-backed workspaces."
        )

    @pytest.mark.asyncio
    async def test_a_plain_httpx_response_still_yields_its_body(
        self, proxied, monkeypatch
    ):
        """Focused route tests replace the bounded transport with a Response."""

        async def transport(**_kwargs):
            return httpx.Response(
                200, headers={"content-type": "text/plain"}, content=b"legacy"
            )

        monkeypatch.setattr(ide_routes, "_request_exact_ide_http", transport)

        response = await call(proxied, "GET", PROXY_PATH)

        assert response.status_code == 200
        assert response.content == b"legacy"


# =============================================================================
# Gateway helpers
# =============================================================================


class TestIdeProxyQuery:
    def test_secret_fields_are_dropped_and_state_fields_survive(self):
        result = _ide_proxy_query(
            "token=a&reconnectionToken=b&folder=/w&password=c&blank="
        )

        assert "token=a" not in result
        assert "password=c" not in result
        assert "reconnectionToken=b" in result
        assert "blank=" in result

    def test_key_matching_is_case_and_whitespace_insensitive(self):
        assert _ide_proxy_query(" Access_Token =x") == ""
        assert _ide_proxy_query("SECRET=x") == ""

    def test_every_declared_secret_field_is_stripped(self):
        for field in _IDE_PROXY_SECRET_QUERY_FIELDS:
            assert _ide_proxy_query(f"{field}=value") == ""

    def test_empty_query_stays_empty(self):
        assert _ide_proxy_query("") == ""


class TestBrowserNavigationDetection:
    def _request(self, headers):
        return SimpleNamespace(headers=headers)

    def test_sec_fetch_mode_navigate_is_a_navigation(self):
        assert _is_browser_navigation(self._request({"sec-fetch-mode": "navigate"}))

    def test_cors_subresource_is_not(self):
        assert not _is_browser_navigation(
            self._request({"sec-fetch-mode": "cors", "accept": "*/*"})
        )

    def test_html_accept_is_the_older_browser_fallback(self):
        assert _is_browser_navigation(self._request({"accept": "text/html,*/*"}))

    def test_no_headers_at_all_is_not_a_navigation(self):
        assert not _is_browser_navigation(self._request({}))


class TestRequestExactIdeHttp:
    @pytest.mark.asyncio
    async def test_mutation_is_contained_before_any_client_is_constructed(
        self, monkeypatch
    ):
        constructor = MagicMock()
        monkeypatch.setattr(httpx, "AsyncClient", constructor)
        proxy = SimpleNamespace(revalidate_target=AsyncMock(return_value=True))

        with pytest.raises(IdeProxyUnavailable) as exc:
            await _request_exact_ide_http(
                target=SimpleNamespace(backend="docker"),
                method="POST",
                url="http://10.42.0.7:38080/effect",
                headers={},
                content=b"must-not-cross",
                ide_proxy=proxy,
            )

        assert exc.value.code == "ide_mutation_operation_lease_unavailable"
        constructor.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("backend", ["k8s", "vm"])
    async def test_remote_without_a_credential_is_contained(self, monkeypatch, backend):
        constructor = MagicMock()
        monkeypatch.setattr(httpx, "AsyncClient", constructor)

        with pytest.raises(IdeProxyUnavailable) as exc:
            await _request_exact_ide_http(
                target=SimpleNamespace(backend=backend, credential=None),
                method="GET",
                url="http://10.42.0.7:38080/data",
                headers={},
                content=None,
                ide_proxy=SimpleNamespace(),
            )

        assert exc.value.code == "ide_remote_transport_unavailable"
        constructor.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("limit", [0, -1, True, "32"])
    async def test_an_invalid_byte_limit_refuses_rather_than_defaulting(
        self, monkeypatch, limit
    ):
        constructor = MagicMock()
        monkeypatch.setattr(httpx, "AsyncClient", constructor)

        with pytest.raises(IdeProxyUnavailable) as exc:
            await _request_exact_ide_http(
                target=SimpleNamespace(backend="docker"),
                method="GET",
                url="http://127.0.0.1:1/data",
                headers={},
                content=None,
                max_response_body_bytes=limit,
                ide_proxy=SimpleNamespace(),
            )

        assert exc.value.code == "ide_response_limit_invalid"
        constructor.assert_not_called()

    def _client(self, *, headers, chunks, sent):
        class Response:
            def __init__(self):
                self.headers = httpx.Headers(headers)
                self.status_code = 200

            async def aiter_raw(self):
                for chunk in chunks:
                    yield chunk

        class Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            @asynccontextmanager
            async def stream(self, **kwargs):
                sent.append(kwargs)
                yield Response()

        return MagicMock(return_value=Client())

    @pytest.mark.asyncio
    async def test_declared_oversize_is_refused_before_the_body_is_read(
        self, monkeypatch
    ):
        sent: list[dict] = []
        monkeypatch.setattr(
            httpx,
            "AsyncClient",
            self._client(headers={"content-length": "99"}, chunks=[b"x"], sent=sent),
        )

        with pytest.raises(IdeProxyUnavailable) as exc:
            await _request_exact_ide_http(
                target=SimpleNamespace(backend="docker"),
                method="GET",
                url="http://127.0.0.1:1/data",
                headers={},
                content=None,
                max_response_body_bytes=8,
                ide_proxy=SimpleNamespace(revalidate_target=AsyncMock()),
            )

        assert exc.value.code == "ide_response_too_large"

    @pytest.mark.asyncio
    async def test_streamed_oversize_is_refused_mid_body(self, monkeypatch):
        sent: list[dict] = []
        monkeypatch.setattr(
            httpx,
            "AsyncClient",
            self._client(headers={}, chunks=[b"aaaa", b"bbbb", b"cccc"], sent=sent),
        )

        with pytest.raises(IdeProxyUnavailable) as exc:
            await _request_exact_ide_http(
                target=SimpleNamespace(backend="docker"),
                method="GET",
                url="http://127.0.0.1:1/data",
                headers={},
                content=None,
                max_response_body_bytes=6,
                ide_proxy=SimpleNamespace(revalidate_target=AsyncMock()),
            )

        assert exc.value.code == "ide_response_too_large"

    @pytest.mark.asyncio
    async def test_a_malformed_content_length_is_a_typed_refusal(self, monkeypatch):
        sent: list[dict] = []
        monkeypatch.setattr(
            httpx,
            "AsyncClient",
            self._client(
                headers={"content-length": "not-a-number"}, chunks=[], sent=sent
            ),
        )

        with pytest.raises(IdeProxyUnavailable) as exc:
            await _request_exact_ide_http(
                target=SimpleNamespace(backend="docker"),
                method="GET",
                url="http://127.0.0.1:1/data",
                headers={},
                content=None,
                ide_proxy=SimpleNamespace(revalidate_target=AsyncMock()),
            )

        assert exc.value.code == "ide_response_invalid"

    @pytest.mark.asyncio
    async def test_post_body_revalidation_failure_loses_the_response(self, monkeypatch):
        sent: list[dict] = []
        monkeypatch.setattr(
            httpx,
            "AsyncClient",
            self._client(headers={}, chunks=[b"ok"], sent=sent),
        )
        proxy = SimpleNamespace(revalidate_target=AsyncMock(return_value=False))

        with pytest.raises(_IdeProxyAuthorityLost):
            await _request_exact_ide_http(
                target=SimpleNamespace(backend="docker"),
                method="GET",
                url="http://127.0.0.1:1/data",
                headers={},
                content=None,
                ide_proxy=proxy,
            )

    @pytest.mark.asyncio
    async def test_a_credential_target_binds_the_cookie_without_mutating_the_caller(
        self, monkeypatch
    ):
        sent: list[dict] = []
        monkeypatch.setattr(
            httpx,
            "AsyncClient",
            self._client(headers={}, chunks=[b"ok"], sent=sent),
        )
        caller_headers = {"accept": "*/*"}
        proxy = SimpleNamespace(revalidate_target=AsyncMock(return_value=True))

        result = await _request_exact_ide_http(
            target=SimpleNamespace(backend="k8s", credential="c0ffee"),
            method="GET",
            url="http://127.0.0.1:1/data",
            headers=caller_headers,
            content=None,
            ide_proxy=proxy,
        )

        assert result.body == b"ok"
        assert sent[0]["headers"]["cookie"] == "code-server-session=c0ffee"
        assert "cookie" not in caller_headers


class TestStatelessIdeLifecycleFence:
    def _proxy(self):
        return SimpleNamespace(evict=MagicMock())

    @pytest.mark.asyncio
    async def test_a_missing_thread_is_not_fenced(self):
        proxy = self._proxy()
        store = SimpleNamespace(get_thread=AsyncMock(return_value=None))

        await _require_stateless_ide_lifecycle("x", store=store, ide_proxy=proxy)

        proxy.evict.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_pinned_thread_is_not_fenced(self):
        proxy = self._proxy()
        store = SimpleNamespace(
            get_thread=AsyncMock(return_value={"execution_lane": "pinned"})
        )

        await _require_stateless_ide_lifecycle("x", store=store, ide_proxy=proxy)

        proxy.evict.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_live_stateless_sandbox_evicts_the_stale_coordinate(
        self, monkeypatch
    ):
        proxy = self._proxy()
        store = SimpleNamespace(
            get_thread=AsyncMock(
                return_value={
                    "execution_lane": "stateless",
                    "status": "active",
                    "metadata": {},
                }
            )
        )
        monkeypatch.setattr(
            "orchestrator.services.ide_proxy_gateway.stateless_session_workspace_check",
            lambda _thread: ("sandbox", None),
        )

        await _require_stateless_ide_lifecycle("x", store=store, ide_proxy=proxy)

        proxy.evict.assert_called_once_with("x")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "row,check",
        [
            ({"status": "ended"}, ("sandbox", None)),
            ({"status": "active"}, ("virtual", None)),
            ({"status": "active"}, ("sandbox", "refused")),
        ],
    )
    async def test_a_fenced_stateless_thread_409s_after_evicting(
        self, monkeypatch, row, check
    ):
        proxy = self._proxy()
        store = SimpleNamespace(
            get_thread=AsyncMock(
                return_value={"execution_lane": "stateless", "metadata": {}, **row}
            )
        )
        monkeypatch.setattr(
            "orchestrator.services.ide_proxy_gateway.stateless_session_workspace_check",
            lambda _thread: check,
        )

        with pytest.raises(HTTPException) as exc:
            await _require_stateless_ide_lifecycle("x", store=store, ide_proxy=proxy)

        assert exc.value.status_code == 409
        assert (
            exc.value.detail
            == "Stateless workspace lifecycle is not admitting IDE traffic"
        )
        proxy.evict.assert_called_once_with("x")

    @pytest.mark.asyncio
    async def test_unreadable_stop_markers_fail_closed(self, monkeypatch):
        proxy = self._proxy()
        store = SimpleNamespace(
            get_thread=AsyncMock(
                return_value={
                    "execution_lane": "stateless",
                    "status": "active",
                    "metadata": "corrupt",
                }
            )
        )
        monkeypatch.setattr(
            "orchestrator.services.ide_proxy_gateway.stateless_stop_markers",
            MagicMock(side_effect=RuntimeError("bad json")),
        )
        monkeypatch.setattr(
            "orchestrator.services.ide_proxy_gateway.stateless_session_workspace_check",
            lambda _thread: ("sandbox", None),
        )

        with pytest.raises(HTTPException) as exc:
            await _require_stateless_ide_lifecycle("x", store=store, ide_proxy=proxy)

        assert exc.value.status_code == 409


class TestIdeWsRuntimeIsCurrent:
    TARGET = SimpleNamespace(
        backend="k8s", credential="c0ffee", host="10.42.0.7", port=38080
    )

    async def _current(self, *, resolve):
        store = SimpleNamespace(get_thread=AsyncMock(return_value=None))
        proxy = SimpleNamespace(resolve_target=resolve, evict=MagicMock())
        return await _ide_ws_runtime_is_current(
            "thread-a", self.TARGET, store=store, ide_proxy=proxy
        )

    @pytest.mark.asyncio
    async def test_same_runtime_keeps_the_stream(self):
        assert await self._current(resolve=AsyncMock(return_value=self.TARGET))

    @pytest.mark.asyncio
    async def test_status_churn_alone_does_not_drop_a_working_ide(self):
        churned = SimpleNamespace(
            backend="k8s",
            credential="c0ffee",
            host="10.42.0.7",
            port=38080,
            identity=("something", "else"),
        )

        assert await self._current(resolve=AsyncMock(return_value=churned))

    @pytest.mark.asyncio
    async def test_replacement_runtime_closes_the_stream(self):
        successor = SimpleNamespace(
            backend="k8s", credential="decafbad", host="10.42.0.9", port=38080
        )

        assert not await self._current(resolve=AsyncMock(return_value=successor))

    @pytest.mark.asyncio
    async def test_ended_owner_closes_the_stream(self):
        assert not await self._current(resolve=AsyncMock(return_value=None))

    @pytest.mark.asyncio
    async def test_inconclusive_check_closes_the_stream(self):
        assert not await self._current(
            resolve=AsyncMock(side_effect=RuntimeError("api down"))
        )

    @pytest.mark.asyncio
    async def test_typed_unavailability_closes_the_stream(self):
        assert not await self._current(
            resolve=AsyncMock(side_effect=IdeProxyUnavailable("code", "detail"))
        )


# =============================================================================
# WebSocket proxy
# =============================================================================


class _Upstream:
    """An upstream socket that never produces a frame until cancelled."""

    def __init__(self):
        self.exited = False
        self.iterator_cancelled = False
        self.sent: list = []

    async def send(self, message):
        self.sent.append(message)

    def __aiter__(self):
        async def stream():
            try:
                await asyncio.Event().wait()
                yield "unreachable"
            except asyncio.CancelledError:
                self.iterator_cancelled = True
                raise

        return stream()


def fake_ws(*, query="", receive=None):
    ws = MagicMock()
    ws.url.query = query
    ws.accept = AsyncMock()
    ws.close = AsyncMock()
    ws.receive = receive or AsyncMock(return_value={"type": "websocket.disconnect"})
    ws.send_text = AsyncMock()
    ws.send_bytes = AsyncMock()
    return ws


def ws_dependencies(**overrides):
    base = dict(
        store=SimpleNamespace(name="store-ws"),
        ide_sessions=SimpleNamespace(),
        ide_proxy=SimpleNamespace(
            resolve_target=AsyncMock(return_value=target()),
            revalidate_target=AsyncMock(return_value=True),
            evict=MagicMock(),
        ),
        resolve_ws_user=AsyncMock(return_value={"id": "u", "is_approved": True}),
        user_can_access_ide_entity=AsyncMock(return_value=True),
        log_security_event=AsyncMock(),
    )
    base.update(overrides)
    return IdeDependencies(**base)


class TestIdeProxyWebSocketAdmission:
    @pytest.mark.asyncio
    async def test_no_session_closes_4401(self):
        ws = fake_ws()

        await ide_routes.ide_proxy_ws(
            ws,
            JOB,
            "",
            dependencies=ws_dependencies(resolve_ws_user=AsyncMock(return_value=None)),
        )

        ws.close.assert_awaited_once_with(code=4401, reason="Authentication required")
        ws.accept.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unapproved_account_closes_4403(self):
        ws = fake_ws()

        await ide_routes.ide_proxy_ws(
            ws,
            JOB,
            "",
            dependencies=ws_dependencies(
                resolve_ws_user=AsyncMock(return_value={"id": "u"})
            ),
        )

        ws.close.assert_awaited_once_with(code=4403, reason="Account pending approval")

    @pytest.mark.asyncio
    async def test_entity_refusal_audits_then_closes_4403(self):
        ws = fake_ws()
        audit = AsyncMock()
        deps = ws_dependencies(
            user_can_access_ide_entity=AsyncMock(return_value=False),
            log_security_event=audit,
        )

        await ide_routes.ide_proxy_ws(ws, JOB, "", dependencies=deps)

        audit.assert_awaited_once()
        assert audit.await_args.args[0] is deps.store
        assert audit.await_args.kwargs["method"] == "WS"
        assert audit.await_args.kwargs["request"] is ws
        ws.close.assert_awaited_once_with(code=4403, reason="IDE access denied")

    @pytest.mark.asyncio
    async def test_lifecycle_fence_closes_4409(self, monkeypatch):
        ws = fake_ws()
        monkeypatch.setattr(
            ide_routes,
            "_require_stateless_ide_lifecycle",
            AsyncMock(side_effect=HTTPException(status_code=409, detail="fenced")),
        )

        await ide_routes.ide_proxy_ws(ws, JOB, "", dependencies=ws_dependencies())

        ws.close.assert_awaited_once_with(
            code=4409, reason="Workspace lifecycle fenced"
        )
        ws.accept.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_typed_unavailability_closes_with_its_code(self, monkeypatch):
        ws = fake_ws()
        monkeypatch.setattr(ide_routes, "_require_stateless_ide_lifecycle", AsyncMock())
        deps = ws_dependencies(
            ide_proxy=SimpleNamespace(
                resolve_target=AsyncMock(
                    side_effect=IdeProxyUnavailable(
                        "vm_ide_transport_unavailable", "needs a tunnel"
                    )
                ),
                evict=MagicMock(),
            )
        )

        await ide_routes.ide_proxy_ws(ws, JOB, "", dependencies=deps)

        ws.close.assert_awaited_once_with(
            code=4503, reason="vm_ide_transport_unavailable"
        )
        ws.accept.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_target_closes_4503(self, monkeypatch):
        ws = fake_ws()
        monkeypatch.setattr(ide_routes, "_require_stateless_ide_lifecycle", AsyncMock())
        deps = ws_dependencies(
            ide_proxy=SimpleNamespace(
                resolve_target=AsyncMock(return_value=None), evict=MagicMock()
            )
        )

        await ide_routes.ide_proxy_ws(ws, JOB, "", dependencies=deps)

        ws.close.assert_awaited_once_with(code=4503, reason="IDE session not active")

    @pytest.mark.asyncio
    async def test_remote_without_a_credential_closes_4503(self, monkeypatch):
        ws = fake_ws()
        monkeypatch.setattr(ide_routes, "_require_stateless_ide_lifecycle", AsyncMock())
        deps = ws_dependencies(
            ide_proxy=SimpleNamespace(
                resolve_target=AsyncMock(return_value=target(credential=None)),
                evict=MagicMock(),
            )
        )

        await ide_routes.ide_proxy_ws(ws, JOB, "", dependencies=deps)

        ws.close.assert_awaited_once_with(
            code=4503, reason="ide_remote_transport_unavailable"
        )
        ws.accept.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unreachable_host_closes_4503(self, monkeypatch):
        ws = fake_ws()
        monkeypatch.setattr(ide_routes, "_require_stateless_ide_lifecycle", AsyncMock())
        monkeypatch.setattr(
            "orchestrator.services.ssh_helpers.orchestrator_can_reach",
            lambda _host: False,
        )

        await ide_routes.ide_proxy_ws(ws, JOB, "", dependencies=ws_dependencies())

        ws.close.assert_awaited_once_with(
            code=4503, reason="IDE not available for VM workspaces"
        )


class TestIdeProxyWebSocketStream:
    @pytest.fixture
    def relay(self, monkeypatch):
        monkeypatch.setattr(ide_routes, "_require_stateless_ide_lifecycle", AsyncMock())
        monkeypatch.setattr(
            "orchestrator.services.ssh_helpers.orchestrator_can_reach",
            lambda _host: True,
        )
        upstream = _Upstream()
        calls: list = []

        @asynccontextmanager
        async def connect(url, **kwargs):
            calls.append((url, kwargs))
            try:
                yield upstream
            finally:
                upstream.exited = True

        import websockets

        monkeypatch.setattr(websockets, "connect", connect)
        return SimpleNamespace(upstream=upstream, calls=calls)

    @pytest.mark.asyncio
    async def test_handshake_carries_the_credential_and_strips_secrets(self, relay):
        ws = fake_ws(query="token=leak-me&reconnectionToken=keep-me")

        await ide_routes.ide_proxy_ws(
            ws, JOB, "stable/connection", dependencies=ws_dependencies()
        )

        ws.accept.assert_awaited_once()
        url, kwargs = relay.calls[0]
        assert kwargs["additional_headers"]["Cookie"] == "code-server-session=c0ffee"
        assert "token=leak-me" not in url
        assert "reconnectionToken=keep-me" in url
        assert url.startswith("ws://10.42.0.7:38080/stable/connection?")

    @pytest.mark.asyncio
    async def test_client_disconnect_cancels_every_relay_task_and_closes(self, relay):
        ws = fake_ws()
        before = {t for t in asyncio.all_tasks() if not t.done()}

        await ide_routes.ide_proxy_ws(ws, JOB, "", dependencies=ws_dependencies())

        after = {t for t in asyncio.all_tasks() if not t.done()}
        assert after - before == set()
        assert relay.upstream.iterator_cancelled is True
        assert relay.upstream.exited is True
        ws.close.assert_awaited()

    @pytest.mark.asyncio
    async def test_a_retired_runtime_closes_the_stream_within_the_bound(
        self, relay, monkeypatch
    ):
        monkeypatch.setattr(ide_routes, "_IDE_WS_LIFECYCLE_RECHECK_S", 0.01)
        monkeypatch.setattr(
            ide_routes, "_ide_ws_runtime_is_current", AsyncMock(return_value=False)
        )

        async def never_receives():
            await asyncio.Event().wait()

        ws = fake_ws(receive=never_receives)
        before = {t for t in asyncio.all_tasks() if not t.done()}

        await asyncio.wait_for(
            ide_routes.ide_proxy_ws(ws, JOB, "", dependencies=ws_dependencies()),
            timeout=5,
        )

        after = {t for t in asyncio.all_tasks() if not t.done()}
        assert after - before == set()
        assert relay.upstream.exited is True
        ws.close.assert_awaited()

    @pytest.mark.asyncio
    async def test_browser_frames_are_relayed_in_both_directions(
        self, relay, monkeypatch
    ):
        frames = [
            {"type": "websocket.receive", "text": "hello"},
            {"type": "websocket.receive", "bytes": b"raw"},
            {"type": "websocket.ping"},
            {"type": "websocket.disconnect"},
        ]
        ws = fake_ws(receive=AsyncMock(side_effect=frames))

        await ide_routes.ide_proxy_ws(ws, JOB, "", dependencies=ws_dependencies())

        assert relay.upstream.sent == ["hello", b"raw"]

    @pytest.mark.asyncio
    async def test_an_unreachable_upstream_evicts_and_closes_4502(self, monkeypatch):
        monkeypatch.setattr(ide_routes, "_require_stateless_ide_lifecycle", AsyncMock())
        monkeypatch.setattr(
            "orchestrator.services.ssh_helpers.orchestrator_can_reach",
            lambda _host: True,
        )

        import websockets

        def connect(_url, **_kwargs):
            raise OSError("refused")

        monkeypatch.setattr(websockets, "connect", connect)
        ws = fake_ws()
        deps = ws_dependencies()

        await ide_routes.ide_proxy_ws(ws, JOB, "", dependencies=deps)

        deps.ide_proxy.evict.assert_called_once_with(JOB)
        assert ws.close.await_args_list[0].kwargs == {
            "code": 4502,
            "reason": "code-server unreachable",
        }


class TestSharedBrowserStreamWs:
    @pytest.mark.asyncio
    async def test_the_relay_receives_the_injected_store(self, monkeypatch):
        seen: dict = {}

        async def relay_browser_stream(ws, thread_id, *, db):
            seen.update(ws=ws, thread_id=thread_id, db=db)

        monkeypatch.setattr(
            "orchestrator.services.browser_stream_broker.relay_browser_stream",
            relay_browser_stream,
        )
        ws = fake_ws()
        deps = ws_dependencies()

        await ide_routes.shared_browser_stream_ws(ws, "thread-a", dependencies=deps)

        assert seen["thread_id"] == "thread-a"
        assert seen["db"] is deps.store
        assert seen["ws"] is ws


class TestWebSocketDependencyResolution:
    def test_the_ws_provider_reads_the_same_per_app_factory(self):
        app = FastAPI()
        built: list[int] = []

        def factory():
            built.append(len(built))
            return ws_dependencies()

        app.state.ide_dependencies_factory = factory
        connection = SimpleNamespace(app=app)

        first = ide_routes.get_ide_dependencies_ws(connection)
        second = ide_routes.get_ide_dependencies_ws(connection)

        assert built == [0, 1]
        assert first is not second

    def test_the_http_provider_reads_the_same_per_app_factory(self):
        app = FastAPI()
        sentinel = ws_dependencies()
        app.state.ide_dependencies_factory = lambda: sentinel

        assert ide_routes.get_ide_dependencies(SimpleNamespace(app=app)) is sentinel
