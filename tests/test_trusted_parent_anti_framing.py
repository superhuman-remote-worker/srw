"""Trusted Cockpit/BFF response boundary for untrusted Canvas frames."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.testclient import TestClient

from shared.anti_framing import TrustedParentAntiFramingMiddleware
from orchestrator.application import http as http_composition


ROOT = Path(__file__).resolve().parents[1]
FRAME_ANCESTORS_NONE = "frame-ancestors 'none'"

# Only `add_header ... always;` lines, so a comment mentioning frame-ancestors
# cannot satisfy the assertions below.
_NGINX_ALWAYS_HEADER = re.compile(
    r'^\s*add_header\s+(\S+)\s+"([^"]*)"\s+always;', re.MULTILINE
)


def _csp_directives(policy: str) -> list[str]:
    return [directive.strip() for directive in policy.split(";") if directive.strip()]


def _test_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(TrustedParentAntiFramingMiddleware)

    @app.get("/plain")
    async def plain() -> HTMLResponse:
        return HTMLResponse("<h1>trusted</h1>")

    @app.get("/existing-policy")
    async def existing_policy() -> HTMLResponse:
        return HTMLResponse(
            "<h1>restricted</h1>",
            headers={
                "Content-Security-Policy": "default-src 'none'; sandbox",
                "X-Frame-Options": "SAMEORIGIN",
            },
        )

    @app.get("/redirect")
    async def redirect() -> RedirectResponse:
        return RedirectResponse("/plain", status_code=302)

    @app.get("/error")
    async def error() -> None:
        raise HTTPException(status_code=418, detail="teapot")

    @app.get("/unhandled")
    async def unhandled() -> None:
        raise RuntimeError("deliberate boundary test")

    @app.get("/duplicate-policies")
    async def duplicate_policies() -> HTMLResponse:
        response = HTMLResponse("<h1>duplicates</h1>")
        response.raw_headers.extend(
            [
                (b"Content-Security-Policy", b"default-src 'none'"),
                (b"content-security-policy", b"sandbox allow-scripts"),
                (b"X-Frame-Options", b"SAMEORIGIN"),
                (b"x-frame-options", b"ALLOW-FROM https://legacy.example"),
            ]
        )
        return response

    @app.get("/stream")
    async def stream() -> StreamingResponse:
        async def chunks():
            yield b"one"
            yield b"two"

        response = StreamingResponse(
            chunks(),
            media_type="text/plain",
            headers={"Content-Security-Policy": FRAME_ANCESTORS_NONE},
        )
        response.raw_headers.extend(
            [(b"set-cookie", b"first=1; Path=/"), (b"set-cookie", b"second=2; Path=/")]
        )
        return response

    return app


def _assert_denies_framing(response) -> None:
    assert response.headers.get_list("x-frame-options") == ["DENY"]
    assert FRAME_ANCESTORS_NONE in response.headers.get_list("content-security-policy")


def test_middleware_denies_html_redirect_and_error_framing() -> None:
    client = TestClient(_test_app())

    _assert_denies_framing(client.get("/plain"))
    _assert_denies_framing(client.get("/redirect", follow_redirects=False))
    _assert_denies_framing(client.get("/error"))
    _assert_denies_framing(client.get("/missing"))


def test_middleware_preserves_route_csp_and_replaces_conflicting_legacy_header() -> (
    None
):
    response = TestClient(_test_app()).get("/existing-policy")

    assert response.headers.get_list("content-security-policy") == [
        "default-src 'none'; sandbox",
        FRAME_ANCESTORS_NONE,
    ]
    assert response.headers.get_list("x-frame-options") == ["DENY"]


def test_middleware_protects_unhandled_500_outside_starlette_user_middleware() -> None:
    client = TestClient(_test_app(), raise_server_exceptions=False)

    response = client.get("/unhandled")

    assert response.status_code == 500
    assert response.json() == {"detail": "Internal Server Error"}
    _assert_denies_framing(response)


def test_middleware_preserves_duplicate_csp_and_replaces_every_legacy_xfo() -> None:
    response = TestClient(_test_app()).get("/duplicate-policies")

    assert response.headers.get_list("content-security-policy") == [
        "default-src 'none'",
        "sandbox allow-scripts",
        FRAME_ANCESTORS_NONE,
    ]
    assert response.headers.get_list("x-frame-options") == ["DENY"]


def test_middleware_keeps_stream_body_and_repeated_headers_without_duplicate_policy() -> (
    None
):
    response = TestClient(_test_app()).get("/stream")

    assert response.status_code == 200
    assert response.content == b"onetwo"
    assert response.headers.get_list("content-security-policy") == [
        FRAME_ANCESTORS_NONE
    ]
    assert response.headers.get_list("set-cookie") == [
        "first=1; Path=/",
        "second=2; Path=/",
    ]
    assert response.headers.get_list("x-frame-options") == ["DENY"]


@pytest.mark.asyncio
async def test_middleware_leaves_non_http_protocol_messages_untouched() -> None:
    sent = []

    async def downstream(scope, receive, send) -> None:
        del scope, receive
        await send({"type": "websocket.close", "code": 1000, "reason": "done"})

    async def receive():
        return {"type": "websocket.disconnect", "code": 1000}

    async def send(message) -> None:
        sent.append(message)

    middleware = TrustedParentAntiFramingMiddleware(downstream)
    await middleware(
        {"type": "websocket", "path": "/ws", "headers": []},
        receive,
        send,
    )
    assert sent == [{"type": "websocket.close", "code": 1000, "reason": "done"}]


def test_frameable_path_requires_the_exact_isolated_authority() -> None:
    app = FastAPI()
    app.add_middleware(
        TrustedParentAntiFramingMiddleware,
        same_origin_frameable_path_authorities={
            "/api/ide/": {"api.example.test", "api.example.test:443"}
        },
    )

    @app.get("/api/ide/session/proxy/webview/")
    async def webview() -> HTMLResponse:
        return HTMLResponse(
            "<h1>webview</h1>",
            headers={
                "Content-Security-Policy": "frame-ancestors 'self'",
                "X-Frame-Options": "SAMEORIGIN",
            },
        )

    @app.get("/api/ide/session/proxy/plain-webview/")
    async def plain_webview() -> HTMLResponse:
        return HTMLResponse("<h1>plain webview</h1>")

    @app.get("/api/ide/session/proxy/denied-webview/")
    async def denied_webview() -> HTMLResponse:
        return HTMLResponse(
            "<h1>upstream denial</h1>",
            headers={"X-Frame-Options": "DENY"},
        )

    client = TestClient(app, base_url="https://api.example.test")
    isolated = client.get("/api/ide/session/proxy/webview/")
    assert isolated.headers.get_list("content-security-policy") == [
        "frame-ancestors 'self'"
    ]
    assert isolated.headers.get_list("x-frame-options") == ["SAMEORIGIN"]

    no_upstream_policy = client.get("/api/ide/session/proxy/plain-webview/")
    assert no_upstream_policy.headers.get_list("content-security-policy") == [
        "frame-ancestors 'self'"
    ]
    assert no_upstream_policy.headers.get_list("x-frame-options") == ["SAMEORIGIN"]

    upstream_denial = client.get("/api/ide/session/proxy/denied-webview/")
    assert upstream_denial.headers.get_list("content-security-policy") == [
        "frame-ancestors 'self'"
    ]
    assert upstream_denial.headers.get_list("x-frame-options") == ["DENY"]

    explicit_default_port = client.get(
        "/api/ide/session/proxy/plain-webview/",
        headers={"Host": "api.example.test:443"},
    )
    assert explicit_default_port.headers.get_list("content-security-policy") == [
        "frame-ancestors 'self'"
    ]
    assert explicit_default_port.headers.get_list("x-frame-options") == ["SAMEORIGIN"]

    trailing_dot = client.get(
        "/api/ide/session/proxy/plain-webview/",
        headers={"Host": "api.example.test."},
    )
    assert trailing_dot.headers.get_list("content-security-policy") == [
        "frame-ancestors 'self'"
    ]

    cockpit = client.get(
        "/api/ide/session/proxy/webview/",
        headers={"Host": "cockpit.example.test"},
    )
    _assert_denies_framing(cockpit)
    assert cockpit.headers.get_list("content-security-policy") == [
        "frame-ancestors 'self'",
        FRAME_ANCESTORS_NONE,
    ]
    assert cockpit.headers.get_list("x-frame-options") == ["DENY"]

    missing_host = client.get(
        "/api/ide/session/proxy/plain-webview/",
        headers={"Host": ""},
    )
    _assert_denies_framing(missing_host)

    duplicate_host = client.get(
        "/api/ide/session/proxy/plain-webview/",
        headers=[("Host", "api.example.test"), ("Host", "api.example.test")],
    )
    _assert_denies_framing(duplicate_host)


@pytest.mark.parametrize("prefix", ["", "/", "api/ide/", "/api/ide"])
def test_same_origin_compatibility_prefix_must_be_narrow(prefix: str) -> None:
    with pytest.raises(ValueError, match="non-root absolute path"):
        TrustedParentAntiFramingMiddleware(
            _test_app(),
            same_origin_frameable_path_authorities={prefix: {"api.example.test"}},
        )


def test_ide_exception_fails_closed_when_api_and_cockpit_share_an_authority(
    monkeypatch,
) -> None:
    monkeypatch.setenv("IDE_PROXY_BASE_URL", "https://cockpit.example.test")
    monkeypatch.setenv("SRW_SPA_BASE_URL", "https://cockpit.example.test:443")
    monkeypatch.setenv("CORS_ORIGINS", "https://cockpit.example.test.")
    assert http_composition.isolated_ide_frame_authorities() == {}

    monkeypatch.setenv("IDE_PROXY_BASE_URL", "https://api.example.test:443")
    assert http_composition.isolated_ide_frame_authorities() == {
        "/api/ide/": ("api.example.test", "api.example.test:443")
    }


def test_orchestrator_registers_anti_framing_inside_request_correlation() -> None:
    import orchestrator.main

    middleware_classes = [item.cls for item in orchestrator.main.app.user_middleware]
    assert middleware_classes[0].__name__ == "CorrelationIdMiddleware"
    assert middleware_classes[1] is TrustedParentAntiFramingMiddleware

    response = TestClient(orchestrator.main.app).get("/openapi.json")
    assert response.status_code == 200
    _assert_denies_framing(response)


def test_cockpit_production_and_dev_servers_deny_framing() -> None:
    nginx_headers = dict(
        _NGINX_ALWAYS_HEADER.findall((ROOT / "docker/cockpit-nginx.conf").read_text())
    )
    nginx_csp = nginx_headers["Content-Security-Policy"]
    assert FRAME_ANCESTORS_NONE in _csp_directives(nginx_csp)
    assert nginx_headers["X-Frame-Options"] == "DENY"

    dockerfile = (ROOT / "docker/Dockerfile.cockpit").read_text()
    assert "COPY docker/cockpit-nginx.conf /etc/nginx/conf.d/default.conf" in dockerfile
    assert "RUN nginx -t" in dockerfile

    angular = json.loads((ROOT / "cockpit/angular.json").read_text())
    headers = angular["projects"]["cockpit"]["architect"]["serve"]["options"]["headers"]
    assert FRAME_ANCESTORS_NONE in _csp_directives(headers["Content-Security-Policy"])
    assert headers["X-Frame-Options"] == "DENY"

    # The dev server is the only place framing regressions get noticed early, so
    # it must serve the same policy the container does.
    assert headers["Content-Security-Policy"] == nginx_csp


def test_optional_mcp_documents_use_the_same_response_boundary() -> None:
    run_source = (ROOT / "src/mcp_server/run.py").read_text()
    assert "middleware=[Middleware(TrustedParentAntiFramingMiddleware)]" in run_source

    for relative_path in ("docker/Dockerfile.mcp", "docker/Dockerfile.mcp.dev"):
        dockerfile = (ROOT / relative_path).read_text()
        assert "COPY --chown=srw:srw src/shared/ ./src/shared/" in dockerfile
