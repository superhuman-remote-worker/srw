"""HTTP and WebSocket adapters for the IDE session and code-server proxy.

Extracted verbatim from ``orchestrator.main`` (R1.B04 lane W).

Three shapes in here are deliberate and must not be tidied:

* **The service-worker stub answers before authentication.** Its body is
  non-sensitive and browsers refetch it in the background after a cookie
  expires; a 401 there breaks every subsequent visit.
* **A 401 on a top-level browser navigation redirects; a 403 does not.** Only
  the no-session case can be fixed by logging in — sending an
  authenticated-but-unauthorized user through login would loop them.
* **Mutating methods are refused before the body is read.** An exact-target
  attestation is a point-in-time proof, not an operation lease, so no
  body-bearing request may begin: the refusal fires ahead of the lifecycle
  fence and ahead of any upstream socket.

The WebSocket half carries its own containment: the handshake sends the
server-derived per-workspace credential (never a browser cookie), a supervisor
task re-checks the runtime on a bounded interval, and the ``finally`` blocks
cancel every relay task and close the socket on every exit path.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import httpx
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import RedirectResponse

from orchestrator.schemas.workspace_access import IdeSessionRequest
from orchestrator.security.access import (
    log_security_event,
    require_job_access,
    user_can_access_ide_entity,
)
from orchestrator.security.auth import require_approved_user, resolve_ws_user
from orchestrator.services.ide_proxy import IdeProxyUnavailable
from orchestrator.services.ide_credentials import ide_credential_cookie_header
from orchestrator.services.ide_proxy_gateway import (
    _IDE_PROXY_REQUEST_ALLOW_HEADERS,
    _IDE_PROXY_RESPONSE_ALLOW_HEADERS,
    _IDE_PROXY_SAFE_HTTP_METHODS,
    _IDE_WS_LIFECYCLE_RECHECK_S,
    _ide_proxy_query,
    _ide_ws_runtime_is_current,
    _IdeProxyAuthorityLost,
    _is_browser_navigation,
    _request_exact_ide_http,
    _require_stateless_ide_lifecycle,
)

logger = logging.getLogger(__name__)

router = APIRouter()


@dataclass(frozen=True)
class IdeDependencies:
    """Per-app stores, IDE singletons and gates; no lifecycle ownership here."""

    store: Any
    ide_sessions: Any
    ide_proxy: Any
    require_approved_user: Callable[..., Awaitable[Any]] = require_approved_user
    require_job_access: Callable[..., Awaitable[Any]] = require_job_access
    resolve_ws_user: Callable[..., Awaitable[Any]] = resolve_ws_user
    user_can_access_ide_entity: Callable[..., Awaitable[Any]] = (
        user_can_access_ide_entity
    )
    log_security_event: Callable[..., Awaitable[Any]] = log_security_event


def get_ide_dependencies(request: Request) -> IdeDependencies:
    """Resolve collaborators only from the application handling this request."""
    return request.app.state.ide_dependencies_factory()


def get_ide_dependencies_ws(websocket: WebSocket) -> IdeDependencies:
    """WebSocket sibling of :func:`get_ide_dependencies`.

    FastAPI only fills a ``Request``-annotated dependency parameter for HTTP
    routes; a WebSocket route needs a ``WebSocket``-annotated one. Both read
    the same per-app factory.
    """
    return websocket.app.state.ide_dependencies_factory()


@router.post("/api/jobs/{job_id}/ide")
async def start_ide_session(
    request: Request,
    job_id: str,
    body: IdeSessionRequest | None = None,
    *,
    dependencies: IdeDependencies = Depends(get_ide_dependencies),
) -> dict[str, Any]:
    """Start or get an IDE session for a job.

    Idempotent: if a session is already active, returns it.
    If restoring, returns current progress status.
    """
    user, job = await dependencies.require_job_access(
        request, dependencies.store, job_id
    )
    if body is None:
        body = IdeSessionRequest()

    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    idle = VMIdleLifecycleStore(dependencies.store)
    if await idle.schema_available():
        if await idle.get_open_for_owner(job_id) is not None:
            wake = await idle.request_wake(
                job_id, execution_requested=False,
                access_kind="ide", access_claimant=str(user["id"]),
            )
            if wake is None:
                raise HTTPException(status_code=409, detail="VM wake authority changed")
            return {"status": "restoring", "estimated_seconds": 120}
        await idle.renew_access(job_id, kind="ide", claimant=str(user["id"]))

    try:
        result = await dependencies.ide_sessions.start_session(
            job_id=job_id,
            cpu_cores=body.cpu_cores,
            memory=body.memory,
            idle_timeout_minutes=body.idle_timeout_minutes,
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/api/jobs/{job_id}/ide")
async def get_ide_session(
    request: Request,
    job_id: str,
    *,
    dependencies: IdeDependencies = Depends(get_ide_dependencies),
) -> dict[str, Any]:
    """Get IDE session status and URL.

    Used by the cockpit to poll session state and determine
    IDE button visibility/behavior.
    """
    user, _job = await dependencies.require_job_access(
        request, dependencies.store, job_id
    )
    from orchestrator.services.vm_idle_lifecycle import VMIdleLifecycleStore

    idle = VMIdleLifecycleStore(dependencies.store)
    if await idle.schema_available():
        active = await idle.get_open_for_owner(job_id)
        if active is not None and active["wake_ready_at"] is None:
            return {"status": "restoring", "estimated_seconds": 120}
        await idle.renew_access(job_id, kind="ide", claimant=str(user["id"]))
    try:
        return await dependencies.ide_sessions.get_session_status(job_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.delete("/api/jobs/{job_id}/ide")
async def stop_ide_session(
    request: Request,
    job_id: str,
    *,
    dependencies: IdeDependencies = Depends(get_ide_dependencies),
) -> dict[str, Any]:
    """Tear down an active IDE session.

    Deletes the restored VM and marks the session as expired.
    The underlying S3 snapshot is preserved for future restores.
    """
    await dependencies.require_job_access(request, dependencies.store, job_id)
    try:
        result = await dependencies.ide_sessions.stop_session(job_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


# =============================================================================
# IDE Proxy — reverse proxy HTTP + WebSocket to code-server in workspace pods
# =============================================================================


@router.api_route(
    "/api/ide/{job_id}/proxy/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
)
async def ide_proxy_http(
    request: Request,
    job_id: str,
    path: str = "",
    *,
    dependencies: IdeDependencies = Depends(get_ide_dependencies),
):
    """Reverse proxy HTTP requests to code-server in a workspace pod."""
    # Neuter code-server's service worker — it caches aggressively behind the
    # reverse proxy and breaks subsequent visits (infinite loading screen).
    # Return a no-op worker so the browser doesn't intercept fetches.
    # Pre-auth: the script body is non-sensitive and browsers may fetch it
    # in the background even after a cookie expires.
    if path.endswith("serviceWorker.js") or path.endswith("service-worker.js"):
        return Response(
            content="self.addEventListener('install',()=>self.skipWaiting());"
            "self.addEventListener('activate',e=>e.waitUntil(self.clients.claim()));",
            media_type="application/javascript",
            headers={"cache-control": "no-store"},
        )

    # H1: close the zero-auth hole — pre-fix, any caller knowing (or guessing)
    # the job/thread UUID got full code-server access (file r/w, terminal).
    try:
        user = await dependencies.require_approved_user(request, dependencies.store)
    except HTTPException as exc:
        # A top-level browser navigation can only carry the BFF cookie. When
        # that session has idle-expired, send the browser through the cockpit
        # login instead of dumping raw 401 JSON. Only the no-session 401
        # redirects — 403 (pending approval / IDE access denied) stays an error
        # so an authenticated-but-unauthorized user never loops through login.
        if exc.status_code == 401 and _is_browser_navigation(request):
            return RedirectResponse("/auth/login?return_to=/", status_code=302)
        raise
    if not await dependencies.user_can_access_ide_entity(
        user, dependencies.store, job_id
    ):
        await dependencies.log_security_event(
            dependencies.store,
            user=user,
            resource_type="ide_entity",
            resource_id=job_id,
            detail="IDE access denied",
            request=request,
        )
        raise HTTPException(status_code=403, detail="IDE access denied")

    # An exact target attestation is a point-in-time proof, not a durable
    # operation lease serialized against End/recycle. Until that lease exists,
    # never begin a body-bearing or otherwise mutating HTTP operation: a
    # lifecycle transition could otherwise commit between the last proof and a
    # later upload chunk. The request body remains unread and no upstream
    # connection is opened.
    if str(request.method or "").upper() not in _IDE_PROXY_SAFE_HTTP_METHODS:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "ide_mutation_operation_lease_unavailable",
                "message": "IDE mutation transport requires a durable operation lease",
            },
        )

    await _require_stateless_ide_lifecycle(
        job_id, store=dependencies.store, ide_proxy=dependencies.ide_proxy
    )

    # Browser identity is never meaningful to auth-none code-server. Build the
    # request from a small positive protocol allowlist, then add only forwarding
    # fields derived by this server.
    upstream_headers = {}
    for key, value in request.headers.items():
        if key.lower() in _IDE_PROXY_REQUEST_ALLOW_HEADERS:
            upstream_headers[key] = value
    upstream_headers["x-forwarded-for"] = request.client.host if request.client else ""
    upstream_headers["x-forwarded-proto"] = "https"
    # Ask for identity so ordinary code-server responses avoid compression
    # entirely.  A non-compliant upstream is still safe: the helper buffers
    # bounded raw bytes and preserves Content-Encoding without decoding them.
    upstream_headers["accept-encoding"] = "identity"

    from orchestrator.services.ssh_helpers import orchestrator_can_reach

    try:
        # Recheck at the upstream credential/use boundary. A request that
        # passed authorization before End waited on unrelated work must never
        # reach the code-server after the terminal marker is durable. K8s
        # resolution itself performs the fresh Pod-UID control-plane proof;
        # keeping only this boundary lookup avoids doubling API-server traffic
        # for every code-server asset request.
        await _require_stateless_ide_lifecycle(
            job_id, store=dependencies.store, ide_proxy=dependencies.ide_proxy
        )
        target = await dependencies.ide_proxy.resolve_target(job_id)
        if target is None:
            raise HTTPException(status_code=503, detail="IDE session not active")
        if not orchestrator_can_reach(target.host):
            raise HTTPException(
                status_code=503,
                detail="IDE is not yet available for VM-backed workspaces.",
            )
        upstream_url = f"http://{target.authority}/{path}"
        safe_query = _ide_proxy_query(request.url.query)
        if safe_query:
            upstream_url += f"?{safe_query}"
        upstream_headers["host"] = target.authority
        upstream_resp = await _request_exact_ide_http(
            target=target,
            method=request.method,
            url=upstream_url,
            headers=upstream_headers,
            content=None,
            ide_proxy=dependencies.ide_proxy,
        )
    except IdeProxyUnavailable as exc:
        raise HTTPException(
            status_code=503,
            detail={"code": exc.code, "message": exc.detail},
        ) from exc
    except _IdeProxyAuthorityLost:
        dependencies.ide_proxy.evict(job_id)
        raise HTTPException(status_code=503, detail="IDE runtime authority changed")
    except httpx.ConnectError:
        dependencies.ide_proxy.evict(job_id)
        raise HTTPException(status_code=502, detail="code-server unreachable")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="code-server timeout")

    # The unauthenticated upstream cannot mint browser identity, cookies, or
    # authentication challenges. Forward only inert representation metadata.
    response_headers = {}
    upstream_header_items = upstream_resp.headers
    if callable(multi_items := getattr(upstream_header_items, "multi_items", None)):
        upstream_header_items = multi_items()
    for key, value in upstream_header_items:
        lower = key.lower()
        if lower in _IDE_PROXY_RESPONSE_ALLOW_HEADERS:
            response_headers[key] = value

    response_body = getattr(upstream_resp, "body", None)
    if response_body is None:
        # Compatibility for focused route tests which replace the bounded
        # transport helper with an ordinary in-memory httpx.Response.
        response_body = upstream_resp.content
    return Response(
        content=response_body,
        status_code=upstream_resp.status_code,
        headers=response_headers,
    )


@router.websocket("/api/persistent/threads/{thread_id}/browser/stream")
async def shared_browser_stream_ws(
    ws: WebSocket,
    thread_id: str,
    *,
    dependencies: IdeDependencies = Depends(get_ide_dependencies_ws),
):
    """Relay shared-browser frames/input through the pinned SSH transport."""

    from orchestrator.services.browser_stream_broker import relay_browser_stream

    await relay_browser_stream(ws, thread_id, db=dependencies.store)


@router.websocket("/api/ide/{job_id}/proxy/{path:path}")
async def ide_proxy_ws(
    ws: WebSocket,
    job_id: str,
    path: str = "",
    *,
    dependencies: IdeDependencies = Depends(get_ide_dependencies_ws),
):
    """Reverse proxy an IDE WebSocket to the exact attested workspace runtime.

    code-server cannot render a workbench without this socket, so the HTTP
    half is useless on its own. It was contained while an ``auth: none``
    upstream meant a mis-routed stream would be *served* by whatever answered
    the address. It no longer can be: the handshake carries the per-workspace
    credential (services/ide_credentials.py) and a runtime that did not accept
    it refuses the upgrade. Browser cookies, bearer tokens and proxy-auth
    material still never cross this boundary — the only credential sent is the
    one this server derived for this owner.
    """
    import websockets

    user = await dependencies.resolve_ws_user(ws, dependencies.store)
    if not user:
        await ws.close(code=4401, reason="Authentication required")
        return
    if not user.get("is_approved"):
        await ws.close(code=4403, reason="Account pending approval")
        return
    if not await dependencies.user_can_access_ide_entity(
        user, dependencies.store, job_id
    ):
        await dependencies.log_security_event(
            dependencies.store,
            user=user,
            resource_type="ide_entity",
            resource_id=job_id,
            detail="IDE access denied",
            request=ws,
            method="WS",
        )
        await ws.close(code=4403, reason="IDE access denied")
        return

    try:
        await _require_stateless_ide_lifecycle(
            job_id, store=dependencies.store, ide_proxy=dependencies.ide_proxy
        )
    except HTTPException:
        await ws.close(code=4409, reason="Workspace lifecycle fenced")
        return

    try:
        target = await dependencies.ide_proxy.resolve_target(job_id)
    except IdeProxyUnavailable as exc:
        await ws.close(code=4503, reason=exc.code)
        return
    if target is None:
        await ws.close(code=4503, reason="IDE session not active")
        return

    credential = getattr(target, "credential", None)
    if not credential and getattr(target, "backend", None) != "docker":
        # Same rule as the HTTP transport: a remote runtime is reachable only
        # once it can refuse a stream it should not have received.
        await ws.close(code=4503, reason="ide_remote_transport_unavailable")
        return

    from orchestrator.services.ssh_helpers import orchestrator_can_reach

    if not orchestrator_can_reach(target.host):
        await ws.close(code=4503, reason="IDE not available for VM workspaces")
        return

    upstream_url = f"ws://{target.authority}/{path}"
    safe_query = _ide_proxy_query(ws.url.query)
    if safe_query:
        upstream_url += f"?{safe_query}"
    upstream_headers = {}
    if credential:
        upstream_headers["Cookie"] = ide_credential_cookie_header(credential)

    try:
        await ws.accept()
        async with websockets.connect(
            upstream_url,
            additional_headers=upstream_headers,
            max_size=16 * 1024 * 1024,
            ping_interval=30,
            ping_timeout=10,
            close_timeout=5,
        ) as upstream_ws:

            async def browser_to_pod():
                while True:
                    message = await ws.receive()
                    if message["type"] == "websocket.disconnect":
                        break
                    if message["type"] != "websocket.receive":
                        continue
                    if message.get("text") is not None:
                        await upstream_ws.send(message["text"])
                    elif message.get("bytes") is not None:
                        await upstream_ws.send(message["bytes"])

            async def pod_to_browser():
                async for message in upstream_ws:
                    if isinstance(message, str):
                        await ws.send_text(message)
                    else:
                        await ws.send_bytes(message)

            async def lifecycle_supervisor():
                """Close the stream once its runtime stops admitting traffic."""
                while True:
                    await asyncio.sleep(_IDE_WS_LIFECYCLE_RECHECK_S)
                    if not await _ide_ws_runtime_is_current(
                        job_id,
                        target,
                        store=dependencies.store,
                        ide_proxy=dependencies.ide_proxy,
                    ):
                        return

            tasks = [
                asyncio.create_task(browser_to_pod()),
                asyncio.create_task(pod_to_browser()),
                asyncio.create_task(lifecycle_supervisor()),
            ]
            try:
                _done, pending = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    except WebSocketDisconnect:
        pass
    except (OSError, websockets.InvalidURI, websockets.InvalidHandshake) as exc:
        dependencies.ide_proxy.evict(job_id)
        logger.debug("IDE WS proxy failed for %s: %s", job_id, exc)
        with contextlib.suppress(Exception):
            await ws.close(code=4502, reason="code-server unreachable")
    except websockets.ConnectionClosed:
        pass
    finally:
        with contextlib.suppress(Exception):
            await ws.close()
