#!/usr/bin/env python3
"""Read-only black-box checks for the hosted Dynamic Canvas security boundary.

The default probes send unauthenticated GET requests only. They verify the
trusted Cockpit/BFF anti-framing boundary, the narrow IDE compatibility policy,
the final Keycloak login document, and that the deployed Angular service-worker
manifest references the exact protected app shell. The script deliberately
does not print request URLs, query strings, response bodies, cookies, or
response headers other than a pass/fail interpretation of the framing policy.

Raw-path and rate-limit probes are opt-in. The rate probe is the only mode that
sends a burst of requests; it still uses GET without cookies or authorization.
This is network/PWA-asset evidence, not installed-PWA or shipping-Safari
evidence.
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import hashlib
import http.client
import json
import re
import secrets
import ssl
import sys
import uuid
from dataclasses import dataclass
from email.message import Message
from typing import Callable, Iterable, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


DEFAULT_COCKPIT_ORIGIN = "https://cockpit.srw.works"
DEFAULT_API_ORIGIN = "https://api.srw.works"
DEFAULT_KEYCLOAK_ISSUER = "https://auth.srw.works/realms/srw"
DEFAULT_CANVAS_DOMAIN = "srwcanvas.works"
DEFAULT_PSL_URL = "https://publicsuffix.org/list/public_suffix_list.dat"
DEFAULT_DEEP_PATH = "/sessions/00000000-0000-4000-8000-000000000000/canvas"
DEFAULT_IDE_PATH = "/api/ide/canvas-hosted-edge-probe/"
APP_SHELL_POLICY_MARKER = (
    b'<meta name="srw-app-shell-policy" content="anti-framing-v1">'
)
MAX_BODY_BYTES = 4 * 1024 * 1024
USER_AGENT = "srw-canvas-hosted-edge-verifier/1"
_DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")


class VerificationError(RuntimeError):
    """A redacted verification failure safe to show to an operator."""


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        return None


@dataclass(frozen=True)
class ResponseSnapshot:
    status: int
    headers: Mapping[str, tuple[str, ...]]
    body: bytes

    def header_values(self, name: str) -> tuple[str, ...]:
        return self.headers.get(name.lower(), ())


@dataclass(frozen=True)
class ProbeResult:
    name: str
    passed: bool
    detail: str


def _headers_snapshot(headers: Message) -> dict[str, tuple[str, ...]]:
    # Retain repeated fields for CSP composition, but never render this mapping.
    result: dict[str, tuple[str, ...]] = {}
    for name in {key.lower() for key in headers.keys()}:
        result[name] = tuple(headers.get_all(name, failobj=[]))
    return result


def _read_bounded(response, *, label: str) -> bytes:  # noqa: ANN001
    body = response.read(MAX_BODY_BYTES + 1)
    if len(body) > MAX_BODY_BYTES:
        raise VerificationError(f"{label}: response exceeded the verifier limit")
    return body


class HttpFetcher:
    """Small cookie-free HTTP client with redirects disabled by default."""

    def __init__(self, timeout_seconds: float = 10.0) -> None:
        self.timeout_seconds = timeout_seconds
        self._opener = build_opener(_NoRedirect())

    def get(self, url: str, *, label: str) -> ResponseSnapshot:
        request = Request(
            url,
            method="GET",
            headers={
                "Accept": "text/html,application/json;q=0.9,*/*;q=0.1",
                "Accept-Encoding": "identity",
                "User-Agent": USER_AGENT,
            },
        )
        try:
            response = self._opener.open(request, timeout=self.timeout_seconds)
        except HTTPError as exc:
            response = exc
        except (OSError, TimeoutError, URLError) as exc:
            # Exception messages commonly contain the complete URL. Never echo it.
            raise VerificationError(
                f"{label}: request failed ({type(exc).__name__})"
            ) from None
        try:
            return ResponseSnapshot(
                status=response.status,
                headers=_headers_snapshot(response.headers),
                body=_read_bounded(response, label=label),
            )
        finally:
            response.close()

    def get_follow_same_origin(
        self,
        url: str,
        *,
        label: str,
        max_redirects: int = 5,
    ) -> ResponseSnapshot:
        current = url
        allowed_origin = _origin_from_url(url)
        for _ in range(max_redirects + 1):
            response = self.get(current, label=label)
            if response.status not in {301, 302, 303, 307, 308}:
                return response
            locations = response.header_values("location")
            if len(locations) != 1:
                raise VerificationError(
                    f"{label}: redirect did not contain one Location field"
                )
            candidate = urljoin(current, locations[0])
            if _origin_from_url(candidate) != allowed_origin:
                raise VerificationError(f"{label}: cross-origin redirect refused")
            current = candidate
        raise VerificationError(f"{label}: redirect limit exceeded")


def _origin_from_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
        port = parsed.port
        hostname_value = parsed.hostname
        hostname = (
            hostname_value.rstrip(".").encode("idna").decode("ascii").lower()
            if hostname_value
            else ""
        )
    except (UnicodeError, ValueError):
        raise VerificationError("configured endpoint is not a valid URL") from None
    if (
        parsed.scheme not in {"http", "https"}
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise VerificationError("configured endpoint is not a valid HTTP origin")
    default_port = 443 if parsed.scheme == "https" else 80
    host = f"[{hostname}]" if ":" in hostname else hostname
    authority = host if port in {None, default_port} else f"{host}:{port}"
    return f"{parsed.scheme}://{authority}"


def _validated_origin(value: str, *, allow_http: bool) -> str:
    parsed = urlsplit(value)
    origin = _origin_from_url(value)
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise VerificationError("configured origin must not contain a path or query")
    if not allow_http and parsed.scheme != "https":
        raise VerificationError("hosted verification requires HTTPS")
    return origin


def _validated_issuer(value: str, *, allow_http: bool) -> str:
    parsed = urlsplit(value)
    _origin_from_url(value)
    if not allow_http and parsed.scheme != "https":
        raise VerificationError("hosted verification requires HTTPS")
    if parsed.query or parsed.fragment or not parsed.path.startswith("/"):
        raise VerificationError("Keycloak issuer must be a query-free realm URL")
    return value.rstrip("/")


def _join(origin: str, path: str) -> str:
    if not path.startswith("/") or path.startswith("//"):
        raise VerificationError("probe paths must be absolute canonical paths")
    return f"{origin}{path}"


def _frame_ancestor_directives(response: ResponseSnapshot) -> list[tuple[str, ...]]:
    directives: list[tuple[str, ...]] = []
    for policy in response.header_values("content-security-policy"):
        for raw_directive in policy.split(";"):
            fields = tuple(raw_directive.strip().lower().split())
            if fields and fields[0] == "frame-ancestors":
                directives.append(fields[1:])
    return directives


def _strict_anti_framing(response: ResponseSnapshot) -> bool:
    return ("'none'",) in _frame_ancestor_directives(response) and any(
        value.strip().upper() == "DENY"
        for value in response.header_values("x-frame-options")
    )


def _same_origin_anti_framing(response: ResponseSnapshot) -> bool:
    return ("'self'",) in _frame_ancestor_directives(response) and any(
        value.strip().upper() == "SAMEORIGIN"
        for value in response.header_values("x-frame-options")
    )


def _keycloak_anti_framing(response: ResponseSnapshot) -> bool:
    directives = _frame_ancestor_directives(response)
    xfo = {value.strip().upper() for value in response.header_values("x-frame-options")}
    return any(item in directives for item in (("'self'",), ("'none'",))) and bool(
        xfo.intersection({"SAMEORIGIN", "DENY"})
    )


def _result(name: str, condition: bool, success: str, failure: str) -> ProbeResult:
    return ProbeResult(
        name=name, passed=condition, detail=success if condition else failure
    )


def verify_cockpit_and_pwa(
    fetcher: HttpFetcher,
    *,
    cockpit_origin: str,
    deep_path: str,
) -> list[ProbeResult]:
    shell = fetcher.get(_join(cockpit_origin, "/index.html"), label="cockpit.shell")
    root = fetcher.get(_join(cockpit_origin, "/"), label="cockpit.root")
    deep = fetcher.get(_join(cockpit_origin, deep_path), label="cockpit.deep")
    manifest = fetcher.get(
        _join(cockpit_origin, "/manifest.webmanifest"), label="pwa.manifest"
    )
    worker_manifest = fetcher.get(
        _join(cockpit_origin, "/ngsw.json"), label="pwa.worker-manifest"
    )
    worker = fetcher.get(
        _join(cockpit_origin, "/ngsw-worker.js"), label="pwa.worker-script"
    )

    results = [
        _result(
            "cockpit.shell",
            shell.status == 200
            and _strict_anti_framing(shell)
            and APP_SHELL_POLICY_MARKER in shell.body,
            "protected policy-marked shell returned 200",
            "shell status, policy, or policy marker is invalid",
        ),
        _result(
            "cockpit.root",
            root.status == 200
            and _strict_anti_framing(root)
            and root.body == shell.body,
            "protected app shell returned at root",
            "root did not return the protected app shell",
        ),
        _result(
            "cockpit.deep-route",
            deep.status == 200
            and _strict_anti_framing(deep)
            and deep.body == shell.body,
            "protected app shell returned for a Canvas deep route",
            "Canvas deep route did not return the protected app shell",
        ),
        _result(
            "pwa.webmanifest",
            manifest.status == 200 and bool(manifest.body),
            "web manifest is reachable",
            "web manifest is missing or empty",
        ),
        _result(
            "pwa.worker-script",
            worker.status == 200 and bool(worker.body),
            "service-worker script is reachable",
            "service-worker script is missing or empty",
        ),
    ]

    pwa_manifest_valid = False
    if worker_manifest.status == 200:
        try:
            parsed = json.loads(worker_manifest.body)
            expected_hash = hashlib.sha1(shell.body).hexdigest()  # noqa: S324
            pwa_manifest_valid = (
                isinstance(parsed, dict)
                and isinstance(parsed.get("hashTable"), dict)
                and parsed["hashTable"].get("/index.html") == expected_hash
                and APP_SHELL_POLICY_MARKER in shell.body
                and _strict_anti_framing(shell)
            )
        except (UnicodeDecodeError, json.JSONDecodeError):
            pwa_manifest_valid = False
    results.append(
        _result(
            "pwa.protected-shell-hash",
            pwa_manifest_valid,
            "service-worker manifest pins the protected shell bytes",
            "service-worker manifest does not pin the protected shell bytes",
        )
    )
    return results


def verify_api_and_ide(
    fetcher: HttpFetcher,
    *,
    api_origin: str,
    ide_path: str,
) -> list[ProbeResult]:
    api = fetcher.get(_join(api_origin, "/api/openapi.json"), label="api.document")
    ide = fetcher.get(_join(api_origin, ide_path), label="api.ide-document")
    return [
        _result(
            "api.trusted-document",
            api.status == 200 and _strict_anti_framing(api),
            "API document has the trusted-origin deny policy",
            "API document status or trusted-origin policy is invalid",
        ),
        _result(
            "api.ide-compatibility",
            200 <= ide.status < 500 and _same_origin_anti_framing(ide),
            "IDE path is restricted to same-origin framing",
            "IDE path status or same-origin policy is invalid",
        ),
    ]


def _pkce_challenge() -> str:
    verifier = secrets.token_urlsafe(48)
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def build_keycloak_login_url(
    *,
    issuer: str,
    client_id: str,
    redirect_uri: str,
) -> str:
    query = urlencode(
        {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "response_mode": "query",
            "scope": "openid",
            "state": secrets.token_urlsafe(24),
            "nonce": secrets.token_urlsafe(24),
            "code_challenge": _pkce_challenge(),
            "code_challenge_method": "S256",
        }
    )
    return f"{issuer}/protocol/openid-connect/auth?{query}"


def verify_keycloak(
    fetcher: HttpFetcher,
    *,
    issuer: str,
    client_id: str,
    redirect_uri: str,
) -> list[ProbeResult]:
    login_url = build_keycloak_login_url(
        issuer=issuer, client_id=client_id, redirect_uri=redirect_uri
    )
    response = fetcher.get_follow_same_origin(login_url, label="keycloak.login")
    content_types = response.header_values("content-type")
    is_html = any("text/html" in value.lower() for value in content_types)
    return [
        _result(
            "keycloak.final-login-document",
            response.status == 200 and is_html and _keycloak_anti_framing(response),
            "final login document rejects cross-origin framing",
            "final login document status, type, or framing policy is invalid",
        )
    ]


def verify_private_psl_rule(
    fetcher: HttpFetcher,
    *,
    psl_url: str,
    expected_rule: str,
) -> list[ProbeResult]:
    """Check the authoritative text list, not browser propagation/cookie behavior."""

    response = fetcher.get(psl_url, label="psl.private-rule")
    found = False
    if response.status == 200:
        try:
            text = response.body.decode("utf-8")
            private = text.split("// ===BEGIN PRIVATE DOMAINS===", 1)[1].split(
                "// ===END PRIVATE DOMAINS===", 1
            )[0]
            found = expected_rule in {
                line.strip()
                for line in private.splitlines()
                if line.strip() and not line.lstrip().startswith("//")
            }
        except (UnicodeDecodeError, IndexError):
            found = False
    return [
        _result(
            "psl.authoritative-private-rule",
            found,
            "exact PRIVATE rule is present; browser propagation remains a separate gate",
            "exact PRIVATE rule is absent or unreadable; the launch gate remains pending",
        )
    ]


def _raw_https_get(
    origin: str,
    raw_target: str,
    *,
    timeout_seconds: float,
) -> ResponseSnapshot:
    parsed = urlsplit(origin)
    if parsed.scheme != "https" or not parsed.hostname:
        raise VerificationError("Canvas edge probes require an HTTPS origin")
    try:
        connection = http.client.HTTPSConnection(
            parsed.hostname,
            parsed.port or 443,
            timeout=timeout_seconds,
            context=ssl.create_default_context(),
        )
        connection.putrequest("GET", raw_target, skip_accept_encoding=True)
        connection.putheader("Accept", "application/json")
        connection.putheader("Accept-Encoding", "identity")
        connection.putheader("Connection", "close")
        connection.putheader("User-Agent", USER_AGENT)
        connection.endheaders()
        response = connection.getresponse()
        return ResponseSnapshot(
            status=response.status,
            headers=_headers_snapshot(response.headers),
            body=_read_bounded(response, label="canvas.edge"),
        )
    except (OSError, TimeoutError, http.client.HTTPException) as exc:
        raise VerificationError(
            f"canvas.edge: request failed ({type(exc).__name__})"
        ) from None
    finally:
        if "connection" in locals():
            connection.close()


def verify_raw_path(
    request: Callable[..., ResponseSnapshot],
    *,
    canvas_origin: str,
    timeout_seconds: float,
) -> list[ProbeResult]:
    response = request(
        canvas_origin,
        "//__canvas_raw_path_probe__",
        timeout_seconds=timeout_seconds,
    )
    code = None
    try:
        payload = json.loads(response.body)
        if isinstance(payload, dict) and isinstance(payload.get("detail"), dict):
            code = payload["detail"].get("code")
    except (UnicodeDecodeError, json.JSONDecodeError):
        pass
    return [
        _result(
            "canvas.raw-path",
            response.status == 400 and code == "canvas_path_invalid",
            "gateway observed and rejected the non-canonical raw target",
            "edge did not preserve the non-canonical raw target to the gateway",
        )
    ]


def verify_rate_limit(
    request: Callable[..., ResponseSnapshot],
    *,
    canvas_origin: str,
    timeout_seconds: float,
    requests: int,
    concurrency: int,
) -> list[ProbeResult]:
    def one_request(_: int) -> int:
        return request(
            canvas_origin,
            "/__canvas_edge_rate_probe__",
            timeout_seconds=timeout_seconds,
        ).status

    statuses: list[int] = []
    failures = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(one_request, index) for index in range(requests)]
        for future in concurrent.futures.as_completed(futures):
            try:
                statuses.append(future.result())
            except VerificationError:
                failures += 1
    limited = statuses.count(429)
    passed = failures == 0 and limited > 0 and limited < len(statuses)
    return [
        _result(
            "canvas.edge-rate-limit",
            passed,
            f"observed {limited} bounded rejections across {len(statuses)} requests",
            "bounded burst did not produce both admitted and rate-limited responses",
        )
    ]


def _canvas_probe_origin(domain: str) -> str:
    try:
        normalized = domain.strip().rstrip(".").encode("idna").decode("ascii").lower()
    except UnicodeError:
        raise VerificationError("Canvas domain is invalid") from None
    labels = normalized.split(".")
    if (
        len(normalized) > 253
        or len(labels) < 2
        or any(_DNS_LABEL.fullmatch(label) is None for label in labels)
    ):
        raise VerificationError("Canvas domain is invalid")
    return f"https://{uuid.uuid4()}.{normalized}"


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cockpit-origin", default=DEFAULT_COCKPIT_ORIGIN)
    parser.add_argument("--api-origin", default=DEFAULT_API_ORIGIN)
    parser.add_argument("--keycloak-issuer", default=DEFAULT_KEYCLOAK_ISSUER)
    parser.add_argument("--keycloak-client-id", default="cockpit-bff")
    parser.add_argument("--deep-path", default=DEFAULT_DEEP_PATH)
    parser.add_argument("--ide-path", default=DEFAULT_IDE_PATH)
    parser.add_argument("--canvas-domain", default=DEFAULT_CANVAS_DOMAIN)
    parser.add_argument("--psl-url", default=DEFAULT_PSL_URL)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument(
        "--allow-http",
        action="store_true",
        help="allow cleartext origins for a local fixture only",
    )
    parser.add_argument(
        "--probe-raw-path",
        action="store_true",
        help="send one non-canonical raw target to a random Canvas UUID host",
    )
    parser.add_argument(
        "--probe-rate-limit",
        action="store_true",
        help="explicitly send a bounded concurrent GET burst to the Canvas edge",
    )
    parser.add_argument("--rate-requests", type=_positive_int, default=120)
    parser.add_argument("--rate-concurrency", type=_positive_int, default=32)
    return parser


def _render(results: Iterable[ProbeResult]) -> None:
    for result in results:
        prefix = "PASS" if result.passed else "FAIL"
        print(f"{prefix} {result.name}: {result.detail}")


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.timeout <= 0 or args.timeout > 60:
            raise VerificationError("timeout must be greater than zero and at most 60")
        if args.rate_requests > 500:
            raise VerificationError("rate request count must not exceed 500")
        if args.rate_concurrency > 64 or args.rate_concurrency > args.rate_requests:
            raise VerificationError(
                "rate concurrency must not exceed 64 or the request count"
            )
        cockpit_origin = _validated_origin(
            args.cockpit_origin, allow_http=args.allow_http
        )
        api_origin = _validated_origin(args.api_origin, allow_http=args.allow_http)
        issuer = _validated_issuer(args.keycloak_issuer, allow_http=args.allow_http)
        psl_url = _validated_issuer(args.psl_url, allow_http=args.allow_http)
        fetcher = HttpFetcher(timeout_seconds=args.timeout)
        results = [
            *verify_cockpit_and_pwa(
                fetcher, cockpit_origin=cockpit_origin, deep_path=args.deep_path
            ),
            *verify_api_and_ide(fetcher, api_origin=api_origin, ide_path=args.ide_path),
            *verify_keycloak(
                fetcher,
                issuer=issuer,
                client_id=args.keycloak_client_id,
                redirect_uri=_join(api_origin, "/auth/callback"),
            ),
            *verify_private_psl_rule(
                fetcher,
                psl_url=psl_url,
                expected_rule=args.canvas_domain.strip().rstrip(".").lower(),
            ),
        ]
        if args.probe_raw_path or args.probe_rate_limit:
            canvas_origin = _canvas_probe_origin(args.canvas_domain)
            if args.probe_raw_path:
                results.extend(
                    verify_raw_path(
                        _raw_https_get,
                        canvas_origin=canvas_origin,
                        timeout_seconds=args.timeout,
                    )
                )
            if args.probe_rate_limit:
                results.extend(
                    verify_rate_limit(
                        _raw_https_get,
                        canvas_origin=canvas_origin,
                        timeout_seconds=args.timeout,
                        requests=args.rate_requests,
                        concurrency=args.rate_concurrency,
                    )
                )
        _render(results)
        return 0 if all(result.passed for result in results) else 1
    except VerificationError as exc:
        print(f"FAIL verifier: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
