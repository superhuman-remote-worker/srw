from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from email.message import Message
from pathlib import Path
from urllib.error import URLError
from urllib.parse import parse_qs, urlsplit

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "verify-canvas-hosted-edge.py"
SPEC = importlib.util.spec_from_file_location("canvas_hosted_edge_verifier", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
verifier = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = verifier
SPEC.loader.exec_module(verifier)


def _response(
    body: bytes = b"",
    *,
    status: int = 200,
    csp: str | None = None,
    xfo: str | None = None,
    content_type: str | None = None,
) -> verifier.ResponseSnapshot:
    headers: dict[str, tuple[str, ...]] = {}
    if csp is not None:
        headers["content-security-policy"] = (csp,)
    if xfo is not None:
        headers["x-frame-options"] = (xfo,)
    if content_type is not None:
        headers["content-type"] = (content_type,)
    return verifier.ResponseSnapshot(status=status, headers=headers, body=body)


def _strict(body: bytes = b"", *, status: int = 200):
    return verifier.ResponseSnapshot(
        status=status,
        headers={
            "content-security-policy": (
                "default-src 'self'",
                "frame-ancestors 'none'",
            ),
            "x-frame-options": ("DENY",),
        },
        body=body,
    )


class FakeFetcher:
    def __init__(self, responses: dict[str, verifier.ResponseSnapshot]) -> None:
        self.responses = responses
        self.requested: list[str] = []

    def get(self, url: str, *, label: str) -> verifier.ResponseSnapshot:
        del label
        self.requested.append(url)
        return self.responses[url]

    def get_follow_same_origin(
        self, url: str, *, label: str, max_redirects: int = 5
    ) -> verifier.ResponseSnapshot:
        del label, max_redirects
        self.requested.append(url)
        return self.responses["keycloak-final"]


def test_policy_interpretation_requires_exact_enforced_directives() -> None:
    strict = verifier.ResponseSnapshot(
        status=200,
        headers={
            "content-security-policy": (
                "default-src 'none'; sandbox",
                "frame-ancestors   'none'",
            ),
            "x-frame-options": ("DENY",),
        },
        body=b"",
    )
    same_origin = _response(csp="frame-ancestors 'self'", xfo="SAMEORIGIN")

    assert verifier._strict_anti_framing(strict) is True
    assert verifier._same_origin_anti_framing(strict) is False
    assert verifier._same_origin_anti_framing(same_origin) is True
    assert verifier._strict_anti_framing(same_origin) is False


def test_cockpit_and_pwa_check_protected_shell_and_worker_hash() -> None:
    origin = "https://cockpit.example.test"
    shell = b"<html>" + verifier.APP_SHELL_POLICY_MARKER + b"</html>"
    worker_manifest = json.dumps(
        {"hashTable": {"/index.html": hashlib.sha1(shell).hexdigest()}}
    ).encode()
    responses = {
        f"{origin}/index.html": _strict(shell),
        f"{origin}/": _strict(shell),
        f"{origin}/sessions/thread/canvas": _strict(shell),
        f"{origin}/manifest.webmanifest": _strict(b"{}"),
        f"{origin}/ngsw.json": _strict(worker_manifest),
        f"{origin}/ngsw-worker.js": _strict(b"worker"),
    }

    results = verifier.verify_cockpit_and_pwa(
        FakeFetcher(responses),
        cockpit_origin=origin,
        deep_path="/sessions/thread/canvas",
    )

    assert len(results) == 6
    assert all(result.passed for result in results)


def test_pwa_hash_fails_when_worker_manifest_references_an_old_shell() -> None:
    origin = "https://cockpit.example.test"
    shell = b"<html>" + verifier.APP_SHELL_POLICY_MARKER + b"</html>"
    responses = {
        f"{origin}/index.html": _strict(shell),
        f"{origin}/": _strict(shell),
        f"{origin}/sessions/thread/canvas": _strict(shell),
        f"{origin}/manifest.webmanifest": _strict(b"{}"),
        f"{origin}/ngsw.json": _strict(
            json.dumps({"hashTable": {"/index.html": "old"}}).encode()
        ),
        f"{origin}/ngsw-worker.js": _strict(b"worker"),
    }

    results = verifier.verify_cockpit_and_pwa(
        FakeFetcher(responses),
        cockpit_origin=origin,
        deep_path="/sessions/thread/canvas",
    )

    by_name = {result.name: result for result in results}
    assert by_name["pwa.protected-shell-hash"].passed is False


def test_api_and_ide_require_distinct_framing_policies() -> None:
    origin = "https://api.example.test"
    responses = {
        f"{origin}/api/openapi.json": _strict(b"{}"),
        f"{origin}/api/ide/probe/": _response(
            status=404,
            csp="frame-ancestors 'self'",
            xfo="SAMEORIGIN",
        ),
    }

    results = verifier.verify_api_and_ide(
        FakeFetcher(responses), api_origin=origin, ide_path="/api/ide/probe/"
    )

    assert all(result.passed for result in results)


def test_keycloak_probe_uses_pkce_and_checks_only_final_document_policy() -> None:
    fetcher = FakeFetcher(
        {
            "keycloak-final": _response(
                b"<html>login</html>",
                csp="frame-src 'self'; frame-ancestors 'self'; object-src 'none'",
                xfo="SAMEORIGIN",
                content_type="text/html; charset=utf-8",
            )
        }
    )

    results = verifier.verify_keycloak(
        fetcher,
        issuer="https://auth.example.test/realms/srw",
        client_id="cockpit-bff",
        redirect_uri="https://api.example.test/auth/callback",
    )

    assert results[0].passed is True
    parsed = urlsplit(fetcher.requested[0])
    query = parse_qs(parsed.query)
    assert query["code_challenge_method"] == ["S256"]
    assert len(query["code_challenge"][0]) == 43
    assert "code_verifier" not in query


def test_private_psl_check_requires_exact_rule_inside_private_section() -> None:
    url = "https://psl.example.test/list.dat"
    present = b"""// ===BEGIN ICANN DOMAINS===
works
// ===END ICANN DOMAINS===
// ===BEGIN PRIVATE DOMAINS===
srwcanvas.works
// ===END PRIVATE DOMAINS===
"""
    absent = present.replace(b"srwcanvas.works", b"other.example")

    success = verifier.verify_private_psl_rule(
        FakeFetcher({url: _response(present)}),
        psl_url=url,
        expected_rule="srwcanvas.works",
    )
    pending = verifier.verify_private_psl_rule(
        FakeFetcher({url: _response(absent)}),
        psl_url=url,
        expected_rule="srwcanvas.works",
    )

    assert success[0].passed is True
    assert "browser propagation remains a separate gate" in success[0].detail
    assert pending[0].passed is False
    assert "pending" in pending[0].detail


def test_raw_path_probe_requires_gateway_specific_rejection_without_rendering_body() -> (
    None
):
    sensitive_body = json.dumps(
        {
            "detail": {
                "code": "canvas_path_invalid",
                "message": "must-not-be-rendered",
            }
        }
    ).encode()
    calls: list[tuple[str, str]] = []

    def request(origin: str, target: str, *, timeout_seconds: float):
        assert timeout_seconds == 3
        calls.append((origin, target))
        return _response(sensitive_body, status=400)

    results = verifier.verify_raw_path(
        request,
        canvas_origin="https://00000000-0000-4000-8000-000000000000.example.test",
        timeout_seconds=3,
    )

    assert results[0].passed is True
    assert calls[0][1].startswith("//")
    assert "must-not-be-rendered" not in results[0].detail


def test_rate_probe_is_bounded_and_requires_admitted_and_limited_results() -> None:
    counter = iter([401, 401, 429, 429])

    def request(origin: str, target: str, *, timeout_seconds: float):
        del origin, target, timeout_seconds
        return _response(status=next(counter))

    results = verifier.verify_rate_limit(
        request,
        canvas_origin="https://00000000-0000-4000-8000-000000000000.example.test",
        timeout_seconds=3,
        requests=4,
        concurrency=1,
    )

    assert results[0].passed is True
    assert "2 bounded rejections across 4 requests" in results[0].detail


def test_http_fetcher_does_not_echo_query_or_network_error_text() -> None:
    fetcher = verifier.HttpFetcher()

    class FailingOpener:
        def open(self, request, timeout):  # noqa: ANN001
            del request, timeout
            raise URLError("https://example.test/?token=do-not-print")

    fetcher._opener = FailingOpener()
    with pytest.raises(verifier.VerificationError) as exc_info:
        fetcher.get("https://example.test/?token=do-not-print", label="redaction.probe")

    rendered = str(exc_info.value)
    assert rendered == "redaction.probe: request failed (URLError)"
    assert "token" not in rendered


def test_cookie_free_fetcher_builds_no_cookie_or_authorization_headers() -> None:
    fetcher = verifier.HttpFetcher()
    captured = None

    class Response:
        status = 200
        headers = Message()

        def read(self, amount):  # noqa: ANN001
            del amount
            return b"ok"

        def close(self):
            return None

    class CapturingOpener:
        def open(self, request, timeout):  # noqa: ANN001
            nonlocal captured
            del timeout
            captured = request
            return Response()

    fetcher._opener = CapturingOpener()
    fetcher.get("https://example.test/", label="header.probe")

    assert captured is not None
    names = {name.lower() for name, _ in captured.header_items()}
    assert "cookie" not in names
    assert "authorization" not in names


def test_cli_bounds_explicit_rate_probe() -> None:
    assert verifier.main(["--rate-requests", "501"]) == 1
    assert verifier.main(["--rate-requests", "2", "--rate-concurrency", "3"]) == 1
