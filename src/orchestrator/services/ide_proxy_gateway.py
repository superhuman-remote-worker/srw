"""Transport and lifecycle containment for the IDE reverse proxy.

Extracted verbatim from ``orchestrator.main`` (R1.B04 lane W). This module is
the boundary side of ``/api/ide/{id}/proxy/…``: the allow-lists that decide
which bytes may cross in either direction, the unpooled single-socket HTTP
transport that re-attests the runtime after TCP connect but before the request
is written, and the stateless-lifecycle fence that refuses admission once a
session starts retiring.

Three properties are load-bearing and move unchanged:

* **Positive allow-lists, never denylists.** code-server runs with
  ``auth: none``, so a header forwarded by accident becomes a credential at an
  unauthenticated upstream. Both directions enumerate what may pass.
* **Attestation happens at the network boundary, not before it.** A Pod-UID
  proof taken earlier in the request is stale by the time a socket exists;
  ``_request_exact_ide_http`` therefore revalidates inside httpcore's
  ``connection.connect_tcp.complete`` trace and again after the body is read.
* **The proxy cache is invalidated by every lifecycle check.** It is keyed only
  by entity ID, and a completed retirement can put a different runtime behind
  that same ID.

The collaborators (``store``, ``ide_proxy``) arrive as explicit arguments; this
module never reaches for application globals.
"""

from __future__ import annotations

import urllib.parse
from dataclasses import dataclass
from typing import Any

import httpx
from fastapi import HTTPException, Request

from orchestrator.services.ide_credentials import ide_credential_cookie_header
from orchestrator.services.ide_proxy import IdeProxyUnavailable
from orchestrator.services.stateless_workspace_gate import (
    stateless_session_workspace_check,
)
from shared.session_retirement import stateless_stop_markers

# code-server runs with ``auth: none`` behind this authenticated BFF. Browser
# headers are therefore data, never upstream identity. Forward only protocol
# fields that a read-only asset/document request actually needs; forwarding
# every header except a known denylist would turn future identity headers into
# credentials at the unauthenticated upstream.
_IDE_PROXY_REQUEST_ALLOW_HEADERS = frozenset(
    {
        "accept",
        "accept-language",
        "cache-control",
        "content-type",
        "if-match",
        "if-modified-since",
        "if-none-match",
        "if-range",
        "if-unmodified-since",
        "pragma",
        "range",
        "user-agent",
    }
)

_IDE_PROXY_RESPONSE_ALLOW_HEADERS = frozenset(
    {
        "accept-ranges",
        "cache-control",
        "content-disposition",
        "content-encoding",
        "content-language",
        "content-range",
        "content-type",
        "etag",
        "expires",
        "last-modified",
    }
)

_IDE_PROXY_SAFE_HTTP_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

# The IDE proxy is a shared control-plane process.  Browser assets are allowed
# to be moderately large, but one tenant-controlled code-server must never make
# a replica buffer an unbounded body.  The transport reads raw (not decoded)
# bytes and rejects byte max+1 before a response can be returned.
_IDE_PROXY_MAX_RESPONSE_BODY_BYTES = 32 * 1024 * 1024

_IDE_PROXY_SECRET_QUERY_FIELDS = frozenset(
    {
        "access_token",
        "assertion",
        "apikey",
        "api_key",
        "authorization",
        "client_secret",
        "code",
        "code_verifier",
        "id_token",
        "key",
        "password",
        "proxy-authorization",
        "refresh_token",
        "samlresponse",
        "secret",
        "session_token",
        "signature",
        "state",
        "token",
    }
)


class _IdeProxyAuthorityLost(Exception):
    """The exact runtime changed at an upstream network boundary."""


@dataclass(frozen=True, slots=True)
class _IdeProxyHttpResponse:
    status_code: int
    headers: tuple[tuple[str, str], ...]
    body: bytes


def _ide_proxy_query(raw_query: str) -> str:
    """Remove browser-auth material without altering code-server state fields."""

    if not raw_query:
        return ""
    pairs = urllib.parse.parse_qsl(raw_query, keep_blank_values=True)
    filtered = [
        (key, value)
        for key, value in pairs
        if key.strip().lower() not in _IDE_PROXY_SECRET_QUERY_FIELDS
    ]
    return urllib.parse.urlencode(filtered, doseq=True)


async def _request_exact_ide_http(
    *,
    target: Any,
    method: str,
    url: str,
    headers: dict[str, str],
    content: Any,
    max_response_body_bytes: int = _IDE_PROXY_MAX_RESPONSE_BODY_BYTES,
    ide_proxy: Any,
) -> _IdeProxyHttpResponse:
    """Open one unpooled socket, then re-attest before sending HTTP bytes.

    httpcore's async trace callback runs after TCP connection establishment and
    before the HTTP protocol writes the request.  A new client per request is
    intentional: a process-global pool keyed by a reusable Pod IP could hand a
    new owner a socket established to an old runtime.
    """

    if method.upper() not in _IDE_PROXY_SAFE_HTTP_METHODS or content is not None:
        raise IdeProxyUnavailable(
            "ide_mutation_operation_lease_unavailable",
            "IDE mutation transport requires a durable operation lease",
        )
    credential = getattr(target, "credential", None)
    if not credential and getattr(target, "backend", None) != "docker":
        # A fresh Pod-UID/owner attestation is only point-in-time authority:
        # Kubernetes may delete the Pod and reuse its IP after the proof, and
        # re-reading the API server cannot see that, because its Pod status
        # lags the CNI that assigns the address. A remote target is therefore
        # allowed only once the destination itself can refuse a connection it
        # should not have received — see services/ide_credentials.py. Local
        # Docker keeps its explicit single-host development trust contract; it
        # is not provisioned by us and has no credential to bind.
        raise IdeProxyUnavailable(
            "ide_remote_transport_unavailable",
            "Remote IDE transport requires a connection-level exact identity",
        )
    if (
        isinstance(max_response_body_bytes, bool)
        or not isinstance(max_response_body_bytes, int)
        or max_response_body_bytes < 1
    ):
        raise IdeProxyUnavailable(
            "ide_response_limit_invalid",
            "IDE response limit is unavailable",
        )

    async def trace(event_name: str, info: dict[str, Any]) -> None:
        if (
            event_name == "connection.connect_tcp.complete"
            and not await ide_proxy.revalidate_target(target)
        ):
            # A failure raised from a ``complete`` trace prevents pool
            # adoption, but httpcore does not promise prompt peer-visible EOF.
            # Close the just-opened stream before refusing the request.
            stream = info.get("return_value")
            close = getattr(stream, "aclose", None)
            if callable(close):
                await close()
            raise _IdeProxyAuthorityLost

    if credential:
        # Server-minted, like the x-forwarded-* fields above — never a browser
        # cookie, and never sent back to the browser. Copy rather than mutate:
        # the caller's dict is built per request and must not accumulate
        # another runtime's credential.
        headers = {**headers, "cookie": ide_credential_cookie_header(credential)}

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(connect=5.0, read=300.0, write=30.0, pool=5.0),
        limits=httpx.Limits(max_connections=1, max_keepalive_connections=0),
        follow_redirects=False,
        trust_env=False,
    ) as client:
        async with client.stream(
            method=method,
            url=url,
            headers=headers,
            content=content,
            extensions={"trace": trace},
        ) as response:
            raw_length = response.headers.get("content-length")
            if raw_length is not None:
                try:
                    declared_length = int(raw_length)
                except (TypeError, ValueError) as exc:
                    raise IdeProxyUnavailable(
                        "ide_response_invalid",
                        "IDE response metadata is invalid",
                    ) from exc
                if declared_length < 0 or declared_length > max_response_body_bytes:
                    raise IdeProxyUnavailable(
                        "ide_response_too_large",
                        "IDE response exceeds the proxy byte limit",
                    )

            body = bytearray()
            async for chunk in response.aiter_raw():
                if len(body) + len(chunk) > max_response_body_bytes:
                    raise IdeProxyUnavailable(
                        "ide_response_too_large",
                        "IDE response exceeds the proxy byte limit",
                    )
                body.extend(chunk)
            status_code = response.status_code
            response_headers = tuple(response.headers.multi_items())

        if not await ide_proxy.revalidate_target(target):
            raise _IdeProxyAuthorityLost
        return _IdeProxyHttpResponse(
            status_code=status_code,
            headers=response_headers,
            body=bytes(body),
        )


def _is_browser_navigation(request: Request) -> bool:
    """True for a top-level browser navigation (vs an XHR / sub-resource).

    Browsers send ``Sec-Fetch-Mode: navigate`` on top-level navigations;
    code-server's own asset/XHR sub-requests send ``cors``/``no-cors``.
    Fall back to an HTML-preferring Accept header for older browsers.
    """
    if request.headers.get("sec-fetch-mode") == "navigate":
        return True
    return "text/html" in request.headers.get("accept", "")


async def _require_stateless_ide_lifecycle(
    entity_id: str,
    *,
    store: Any,
    ide_proxy: Any,
) -> None:
    """Refuse IDE admission once stateless terminal/loss fencing begins."""

    thread = await store.get_thread(entity_id)
    if thread is None or str(thread.get("execution_lane") or "") != "stateless":
        return
    try:
        stopped = bool(stateless_stop_markers(thread.get("metadata")))
    except RuntimeError:
        stopped = True
    backend, workspace_refusal = stateless_session_workspace_check(thread)
    if (
        str(thread.get("status") or "") not in {"created", "active", "awaiting_user"}
        or stopped
        or backend != "sandbox"
        or workspace_refusal is not None
    ):
        ide_proxy.evict(entity_id)
        raise HTTPException(
            status_code=409,
            detail="Stateless workspace lifecycle is not admitting IDE traffic",
        )
    # The proxy cache is keyed only by entity ID. A completed retirement can
    # replace U1/IP1 with U2/IP2 under that same ID, so every fresh stateless
    # lifecycle check invalidates the old coordinate before resolution/use.
    ide_proxy.evict(entity_id)


async def _ide_ws_runtime_is_current(
    job_id: str,
    target: Any,
    *,
    store: Any,
    ide_proxy: Any,
) -> bool:
    """Whether an open IDE stream is still talking to the runtime it opened on.

    Deliberately *not* ``revalidate_target``. That compares the whole target,
    including the owner lifecycle projection — and a session's status moves
    between ``active`` and ``awaiting_user`` during ordinary use, so exact
    equality would drop a working IDE every time the user stopped typing. What
    matters here is narrower: the runtime behind the socket must still be the
    same one, and the owner must still admit IDE traffic.

    ``credential`` carries namespace, owner and Pod name, so comparing it plus
    the address covers "same runtime" without being brittle to status churn.
    """

    try:
        await _require_stateless_ide_lifecycle(job_id, store=store, ide_proxy=ide_proxy)
        current = await ide_proxy.resolve_target(job_id)
    except (HTTPException, IdeProxyUnavailable):
        return False
    except Exception:
        # Never hold a stream open on an inconclusive check.
        return False
    if current is None:
        return False
    return (
        current.credential == target.credential
        and current.host == target.host
        and current.port == target.port
        and current.backend == target.backend
        and (target.backend != "vm" or current.identity == target.identity)
    )


# How often an open stream re-checks that its runtime still exists and still
# admits traffic. A WebSocket outlives the proof that opened it, so the honest
# guarantee is a bound, not an instant: End closes the socket within this
# window rather than the moment it commits. The credential is what makes that
# bound safe — a stream can only ever have reached the runtime that accepted
# this owner's credential, so the residue is a user reading their own workspace
# a few seconds too long, not a stranger reading someone else's.
_IDE_WS_LIFECYCLE_RECHECK_S = 15.0
