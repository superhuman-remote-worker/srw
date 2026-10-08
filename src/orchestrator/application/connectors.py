"""Connector credential leases: the exchange port and the sweeper (slice C2),
and service-plane driver hosting (D5): driver image resolution.

The exchange runs as a second, minimal ASGI application on its own port in
the orchestrator process (``orchestrator.connectorLeases.exchangePort``, off
unless set). It serves the exchange and introspection routes and nothing
else: no health route, no docs, no OpenAPI. The main application never
includes them, so the main port does not serve the exchange, and no ingress
routes this port. The chart's NetworkPolicy admits only the driver namespace
to it.

The port shares the orchestrator's process, so it is bounded: a request body
over ``MAX_BODY_BYTES`` is refused before it is buffered (by its
``Content-Length`` and by the bytes actually streamed), at most
``MAX_CONCURRENT_CONNECTIONS`` connections are served at once, and shutdown
waits at most ``GRACEFUL_SHUTDOWN_SECONDS`` for open requests.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
from collections.abc import Awaitable, Callable, Generator
from typing import Any

import uvicorn
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from orchestrator.application.resources import ApplicationResources
from orchestrator.routers import connector_lease_exchange as exchange_routes
from orchestrator.services.connector_lease_exchange import (
    NO_STORE,
    ConnectorLeaseExchange,
    DenialLimiter,
)
from orchestrator.services.connector_service_hosting import (
    ServiceHostingReconciler,
    ServiceHostingSettings,
    ServicePodRuntime,
)
from orchestrator.services.connector_service_images import ServiceImageSettings
from shared.oci_registry import DEFAULT_TOKEN_HOSTS, RegistryResolver

logger = logging.getLogger(__name__)

#: An exchange body is a lease token and an operation: a few hundred bytes.
MAX_BODY_BYTES = 4096
MAX_CONCURRENT_CONNECTIONS = 32
GRACEFUL_SHUTDOWN_SECONDS = 5

Scope = dict[str, Any]
Message = dict[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]


class BodyLimit:
    """ASGI wrapper refusing a request body over ``limit`` bytes with 413.

    A declared ``Content-Length`` over the limit is refused before anything
    is read. A body that streams past the limit (chunked, or lying about its
    length) is cut at the first chunk that crosses it: the application sees
    the client disconnect, so it never holds more than ``limit`` bytes, and
    whatever it answers to that is dropped. The 413 (no-store) is this
    wrapper's own, not the framework's 400 for an unreadable body.
    """

    def __init__(self, app: ASGIApp, *, limit: int = MAX_BODY_BYTES) -> None:
        self.app = app
        self.limit = int(limit)

    async def _refuse(self, send: Send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": 413,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"cache-control", b"no-store"),
                ],
            }
        )
        await send(
            {"type": "http.response.body", "body": b'{"error":"request_too_large"}'}
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        for name, value in scope.get("headers") or ():
            if name.lower() == b"content-length":
                try:
                    declared = int(value)
                except ValueError:
                    declared = self.limit + 1
                if declared > self.limit:
                    await self._refuse(send)
                    return
        received = 0
        overflow = False
        started = False

        async def limited_receive() -> Message:
            nonlocal received, overflow
            if overflow:
                return {"type": "http.disconnect"}
            message = await receive()
            if message.get("type") == "http.request":
                received += len(message.get("body") or b"")
                if received > self.limit:
                    overflow = True
                    return {"type": "http.disconnect"}
            return message

        async def guarded_send(message: Message) -> None:
            nonlocal started
            if overflow and not started:
                # The application's answer to a body it never fully got.
                return
            if message.get("type") == "http.response.start":
                started = True
            await send(message)

        try:
            await self.app(scope, limited_receive, guarded_send)
        except Exception:
            if not overflow or started:
                raise
        if overflow and not started:
            await self._refuse(send)


def service_image_settings(resources: ApplicationResources) -> ServiceImageSettings:
    """How this application resolves service driver images (D5).

    Each installed service-plane driver names its image reference. Any
    registry may serve a driver image (there is no allow-list), at a public
    address unless the chart names it private; plain HTTP only for the hosts
    the chart names, and a bearer-token challenge only to the registry's own
    host, Docker Hub's or a host the chart names.
    """
    settings = resources.settings
    references = {
        driver.spec.name: driver.image_reference
        for driver in resources.connector_drivers.drivers()
        if driver.spec.plane == "service" and getattr(driver, "image_reference", "")
    }
    return ServiceImageSettings(
        references=references,
        resolver=RegistryResolver(
            hosts=None,
            insecure_hosts=settings.connector_driver_registry_insecure_hosts,
            private_hosts=settings.connector_driver_registry_private_hosts,
            refused_networks=settings.connector_service_cluster_cidrs,
            token_hosts=DEFAULT_TOKEN_HOSTS
            | settings.connector_driver_registry_token_hosts,
            same_host_tokens=True,
            timeout=settings.connector_driver_resolve_timeout_seconds,
        ),
        cache_seconds=settings.connector_driver_resolve_cache_seconds,
        timeout_seconds=settings.connector_driver_resolve_timeout_seconds,
        # Image rows and refusal audits are written on this store's own
        # connections, never in a bind's caller's transaction.
        store=resources.postgres_db,
    )


def service_hosting_settings(
    resources: ApplicationResources,
) -> ServiceHostingSettings | None:
    """This installation's service-pod hosting, or ``None`` when it is off
    or incomplete (no shim image, no exchange)."""
    settings = resources.settings
    if not settings.connector_service_pods_enabled:
        return None
    missing = [
        name
        for name, value in (
            ("namespace", settings.connector_service_namespace),
            ("release namespace", settings.connector_service_release_namespace),
            ("shim image", settings.connector_driver_shim_image),
            ("exchange host", settings.connector_service_exchange_host),
            ("exchange port", settings.connector_lease_exchange_port),
            ("canary port", settings.connector_lease_canary_port),
            ("orchestrator labels", settings.connector_service_orchestrator_labels),
        )
        if not value
    ]
    if missing:
        logger.error(
            "Service-pod hosting is on but not configured (%s); no driver pod starts",
            ", ".join(missing),
        )
        return None
    return ServiceHostingSettings(
        namespace=settings.connector_service_namespace,
        release_namespace=settings.connector_service_release_namespace,
        shim_image=settings.connector_driver_shim_image,
        exchange_host=settings.connector_service_exchange_host,
        exchange_port=int(settings.connector_lease_exchange_port or 0),
        canary_port=int(settings.connector_lease_canary_port or 0),
        orchestrator_labels=dict(settings.connector_service_orchestrator_labels),
        max_installation=settings.connector_service_max_installation,
        idle_seconds=settings.connector_service_idle_seconds,
        start_timeout_seconds=settings.connector_service_start_timeout_seconds,
        cluster_cidrs=tuple(settings.connector_service_cluster_cidrs),
        private_tiers=frozenset(settings.connector_service_private_tiers),
        ipv6=settings.connector_service_ipv6,
        resources=dict(settings.connector_service_resources),
        refused_cidrs=tuple(settings.connector_service_refused_cidrs),
        pod_ip=settings.connector_service_pod_ip,
    )


def connector_service_reconciler_builder(
    resources: ApplicationResources, hosting: ServiceHostingSettings
) -> Callable[[], ServiceHostingReconciler | None]:
    """What the leader's loop calls each pass: one reconciler over the
    current stores, or ``None`` while the Kubernetes API is unavailable.

    The networking API client is created once, over the core API's own
    client (again only if the provisioner replaced its core API).
    """
    cached: dict[str, Any] = {}

    def build() -> ServiceHostingReconciler | None:
        from kubernetes.client import NetworkingV1Api

        from orchestrator.services import (
            agent_provisioner as agent_provisioner_module,
            container_provisioner as container_provisioner_module,
        )

        if not agent_provisioner_module.agent_provisioner._k8s_available:
            return None
        core_api = container_provisioner_module.container_provisioner._core_api
        if core_api is None:
            return None
        if cached.get("core") is not core_api:
            cached["core"] = core_api
            cached["networking"] = NetworkingV1Api(
                getattr(core_api, "api_client", None)
            )
        return ServiceHostingReconciler(
            store=resources.postgres_db,
            runtime=ServicePodRuntime(
                core_api, cached["networking"], namespace=hosting.namespace
            ),
            drivers=resources.connector_drivers,
            settings=hosting,
        )

    return build


def connector_lease_exchange(
    resources: ApplicationResources,
    limiter: DenialLimiter | None = None,
    *,
    service_hosting: bool = True,
) -> ConnectorLeaseExchange:
    """The exchange operations, bound to this application's store and drivers.

    ``service_hosting`` is whether this process hosts service pods; without,
    a service pod's identity is refused.
    """
    return ConnectorLeaseExchange(
        store=resources.postgres_db,
        drivers=resources.connector_drivers,
        limiter=limiter,
        service_hosting=service_hosting,
    )


def connector_lease_exchange_app(resources: ApplicationResources) -> FastAPI:
    """The dedicated exchange port's application.

    One denial limiter for the application's life, so coalescing survives
    across requests; the store and drivers are read per request.
    """
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    limiter = DenialLimiter()
    # Read once: the deployment's setting for this process's life. Without
    # settings (a bare composition) no service pod is hosted here.
    hosting = (
        getattr(resources, "settings", None) is not None
        and service_hosting_settings(resources) is not None
    )
    app.state.connector_lease_exchange_factory = lambda: connector_lease_exchange(
        resources, limiter, service_hosting=hosting
    )

    @app.exception_handler(RequestValidationError)
    async def _invalid(_request: Request, _exc: RequestValidationError) -> JSONResponse:
        # No echo of the body: it holds a lease token.
        return JSONResponse(
            {"error": "invalid_request"}, status_code=422, headers=NO_STORE
        )

    app.include_router(exchange_routes.router)
    return app


class _EmbeddedServer(uvicorn.Server):
    """A uvicorn server that leaves the process's signal handling alone.

    The main server owns SIGTERM/SIGINT; this one stops when the lifecycle
    sets ``should_exit``.
    """

    @contextlib.contextmanager
    def capture_signals(self) -> Generator[None, None, None]:
        yield


def exchange_server_config(app: Any) -> uvicorn.Config:
    """The exchange port's server: bounded, quiet, no websockets.

    ``proxy_headers`` is off: no proxy fronts this port, so the client
    address is always the socket peer, never an ``X-Forwarded-For`` value.
    """
    return uvicorn.Config(
        BodyLimit(app),
        ws="none",
        lifespan="off",
        proxy_headers=False,
        log_config=None,
        access_log=False,
        server_header=False,
        date_header=False,
        limit_concurrency=MAX_CONCURRENT_CONNECTIONS,
        timeout_graceful_shutdown=GRACEFUL_SHUTDOWN_SECONDS,
    )


async def serve_connector_lease_exchange(
    resources: ApplicationResources,
    *,
    port: int,
    shutdown_event: asyncio.Event,
    canary_port: int | None = None,
) -> None:
    """Serve the exchange on ``port`` (and its canary on ``canary_port``)
    until shutdown.

    The sockets are bound here, not by uvicorn, whose bind failure exits the
    process: a busy port is logged and leaves the exchange off, never the
    orchestrator down. Shutdown waits for open requests at most
    ``GRACEFUL_SHUTDOWN_SECONDS`` (and the server task a little longer
    before it is cancelled), so a slow client never holds the orchestrator's
    shutdown.

    The canary is a driver pod's start-up deny target: its policy admits the
    exchange and refuses the canary, and "exchange answers, canary refused"
    proves the policy is in force only if nothing else can make that so. One
    server serves both sockets: they start listening together, and its one
    shutdown stops both at once (the API port, by contrast, stops listening
    while the exchange still drains). Both bind or neither does.
    """
    sockets: list[socket.socket] = []
    for label, number in (("exchange", port), ("canary", canary_port)):
        if number is None:
            continue
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            # Pod network; the chart's NetworkPolicy bounds who reaches it.
            sock.bind(("0.0.0.0", number))  # nosec B104
        except OSError as exc:
            sock.close()
            for bound in sockets:
                bound.close()
            logger.error(
                "Connector lease exchange cannot bind its %s port %d: %s; the "
                "exchange is off",
                label,
                number,
                exc,
            )
            return
        sockets.append(sock)
    server = _EmbeddedServer(
        exchange_server_config(connector_lease_exchange_app(resources))
    )
    task = asyncio.create_task(
        server.serve(sockets=sockets), name="connector-lease-exchange"
    )
    logger.info(
        "Connector lease exchange listening on port %d%s",
        port,
        f" (canary {canary_port})" if canary_port is not None else "",
    )
    try:
        stop = asyncio.create_task(shutdown_event.wait())
        done, _ = await asyncio.wait({task, stop}, return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            stop.cancel()
            # A bind failure ends serve(); surface it rather than run without.
            task.result()
            logger.error("Connector lease exchange server stopped on its own")
            return
    finally:
        server.should_exit = True
        if not task.done():
            try:
                await asyncio.wait_for(task, timeout=GRACEFUL_SHUTDOWN_SECONDS + 5)
            except asyncio.TimeoutError:
                logger.warning("Connector lease exchange did not stop in time")
        for sock in sockets:
            sock.close()


__all__ = [
    "GRACEFUL_SHUTDOWN_SECONDS",
    "MAX_BODY_BYTES",
    "MAX_CONCURRENT_CONNECTIONS",
    "BodyLimit",
    "connector_lease_exchange",
    "connector_lease_exchange_app",
    "connector_service_reconciler_builder",
    "service_hosting_settings",
    "exchange_server_config",
    "serve_connector_lease_exchange",
    "service_image_settings",
]
