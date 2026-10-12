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
from orchestrator.routers import agent_git_swap as agent_git_swap_routes
from orchestrator.routers import connector_drivers as connector_drivers_routes
from orchestrator.routers import connector_lease_exchange as exchange_routes
from orchestrator.services.connector_driver_ca import driver_ca
from orchestrator.services.connector_git_swap_delivery import GitSwapDeliverySettings
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
from orchestrator.services.connector_bind_time import (
    MAX_RESULT_BYTES,
    BindTimePodRuntime,
    BindTimeRuntime,
    BindTimeSettings,
    DriverOperations,
)
from orchestrator.services.connector_bind_time_launch import RESULT_PATH
from orchestrator.services.connector_driver_registrations import DriverTrustPolicy
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

    ``path_limits`` gives one path its own limit: a bind-time driver pod
    posts its whole output to the result route (D6).
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        limit: int = MAX_BODY_BYTES,
        path_limits: dict[str, int] | None = None,
    ) -> None:
        self.app = app
        self.limit = int(limit)
        self.path_limits = {
            path: int(value) for path, value in (path_limits or {}).items()
        }

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
        limit = self.path_limits.get(scope.get("path") or "", self.limit)
        for name, value in scope.get("headers") or ():
            if name.lower() == b"content-length":
                try:
                    declared = int(value)
                except ValueError:
                    declared = limit + 1
                if declared > limit:
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
                if received > limit:
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


def git_swap_image(settings: Any, ca: Any) -> str | None:
    """The git swap driver's image when this installation can run it (C3).

    It needs service-pod hosting (its pods are service pods) and SRW's driver
    certificate authority ``ca`` (workspaces reach it over TLS). Without
    either it is not installed, which the operator is told: token
    repositories then take ``connectors.drivers.gitSwap.fallback``.
    """
    image = getattr(settings, "connector_git_swap_image", "")
    if not image:
        return None
    missing = [
        what
        for what, ok in (
            (
                "service-pod hosting (connectors.servicePods)",
                settings.connector_service_pods_enabled,
            ),
            (
                "the driver certificate authority (connectors.drivers.ca)",
                ca is not None,
            ),
        )
        if not ok
    ]
    if missing:
        logger.error(
            "The git swap driver is configured but not installed: it needs %s; "
            "token repositories take connectors.drivers.gitSwap.fallback=%s",
            " and ".join(missing),
            settings.connector_git_swap_fallback,
        )
        return None
    logger.info("The git swap driver is installed with image %s", image)
    return image


def git_swap_delivery_settings(
    resources: ApplicationResources,
) -> GitSwapDeliverySettings:
    """What the per-delivery git swap decision reads (C3): whether the
    driver is installed, the fallback, and the reconciler's own egress rule,
    cap and back-off."""
    from orchestrator.security.access import vm_workspaces_on_pod_network
    from shared.connectors.builtin import GIT_SWAP_SPEC

    settings = resources.settings
    return GitSwapDeliverySettings(
        installed=any(
            driver.spec.name == GIT_SWAP_SPEC.name
            for driver in resources.connector_drivers.drivers()
        ),
        fallback=settings.connector_git_swap_fallback,
        store=resources.postgres_db,
        max_installation=settings.connector_service_max_installation,
        cluster_cidrs=tuple(settings.connector_service_cluster_cidrs),
        refused_cidrs=tuple(settings.connector_service_refused_cidrs),
        private_tiers=frozenset(settings.connector_service_private_tiers),
        ipv6=settings.connector_service_ipv6,
        vm_on_pod_network=vm_workspaces_on_pod_network,
    )


def configure_provider_minting(resources: ApplicationResources) -> None:
    """Provider-minted credentials (C5): where the orchestrator's own
    provider calls may go (the service pods' cluster and refused ranges and
    private tiers, and ``connectors.providerMinting.privateHosts``), and
    whether minting is offered at all, process-wide like the git swap
    settings."""
    from orchestrator.services import connector_minted_credentials
    from orchestrator.services.connector_drivers.provider_http import (
        ProviderNetwork,
        configure_provider_network,
    )

    settings = resources.settings
    configure_provider_network(
        ProviderNetwork(
            cluster_cidrs=tuple(settings.connector_service_cluster_cidrs),
            refused_cidrs=tuple(settings.connector_service_refused_cidrs),
            private_tiers=frozenset(settings.connector_service_private_tiers),
            ipv6=settings.connector_service_ipv6,
            private_hosts=frozenset(settings.connector_provider_minting_private_hosts),
            # SRW's own Gitea, for a connector's Test only (never a mint).
            test_hosts=frozenset(settings.connector_test_gitea_endpoints),
        )
    )
    enabled = settings.connector_provider_minting_enabled
    connector_minted_credentials.configure_minted_credentials(
        connector_minted_credentials.MintedRuntime(store=resources.postgres_db)
        if enabled
        else None,
        enabled=enabled,
    )


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
        # Managed MCP bindings carry their endpoint Service's URL.
        service_namespace=(
            settings.connector_service_namespace
            if settings.connector_service_pods_enabled
            else ""
        ),
        # A git swap binding's first clone waits for a new pod at most this.
        service_start_seconds=(
            settings.connector_service_reconcile_seconds
            + settings.connector_service_start_timeout_seconds
        ),
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
        node_ip=settings.connector_service_node_ip,
        reresolve_seconds=settings.connector_service_reresolve_seconds,
        repin_drain_seconds=settings.connector_service_repin_drain_seconds,
        front_image=settings.connector_mcp_front_image,
        driver_ca=driver_ca(),
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
    strikes: dict[str, Any] = {}

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
            strikes=strikes,
        )

    return build


def driver_trust_policy(resources: ApplicationResources) -> DriverTrustPolicy:
    """The operator's trust in registered driver images (D6)."""
    settings = resources.settings
    return DriverTrustPolicy(
        trusted_repositories=tuple(settings.connector_driver_trusted_repositories),
        custom_drivers_privileged=settings.connector_custom_drivers_privileged,
    )


def bind_time_pod_runtime_builder(
    hosting: ServiceHostingSettings,
) -> Callable[[], BindTimePodRuntime | None]:
    """The driver namespace's API for bind-time pods (D6), or ``None`` while
    the Kubernetes API is unavailable; the networking client is created once
    over the core API's own client, as the service reconciler's is."""
    cached: dict[str, Any] = {}

    def build() -> BindTimePodRuntime | None:
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
        return BindTimePodRuntime(
            core_api, cached["networking"], namespace=hosting.namespace
        )

    return build


def bind_time_runtime(resources: ApplicationResources) -> BindTimeRuntime | None:
    """This application's bind-time driver hosting (D6), or ``None`` when the
    installation runs no driver pods (``connectors.servicePods``)."""
    hosting = service_hosting_settings(resources)
    if hosting is None:
        return None
    settings = resources.settings
    return BindTimeRuntime(
        store=resources.postgres_db,
        operations=DriverOperations(
            store=resources.postgres_db,
            settings=BindTimeSettings(
                hosting=hosting,
                max_pods=settings.connector_bind_time_max_pods,
                deadline_seconds=settings.connector_bind_time_deadline_seconds,
                wait_seconds=settings.connector_bind_time_wait_seconds,
                max_spec_pods_per_user=settings.connector_bind_time_spec_pods_per_user,
            ),
            runtime=bind_time_pod_runtime_builder(hosting),
        ),
        privileged=driver_trust_policy(resources).privileged,
    )


async def resolve_registration_image(lookup: str) -> Any:
    """Resolve an image a caller registers, with the bind's resolver and
    deadline (``service_image_settings``): any registry, the same SSRF
    guard."""
    from orchestrator.services import connector_service_images

    settings = connector_service_images.service_image_settings()
    if settings.resolver is None:
        raise RuntimeError("no image resolver is configured")
    return await asyncio.wait_for(
        settings.resolver.resolve_image(lookup), timeout=settings.timeout_seconds
    )


def connector_drivers_dependencies(
    resources: ApplicationResources,
) -> connector_drivers_routes.ConnectorDriversDependencies:
    """Compose the driver registration routes (D6). An unlabelled image's
    spec is asked in a driver pod; without hosting, that answers why not."""
    from orchestrator.security import auth
    from orchestrator.services.connector_bind_time import run_spec_operation
    from orchestrator.services.connector_drivers.matrix import HostingStatus

    settings = resources.settings
    return connector_drivers_routes.ConnectorDriversDependencies(
        store=resources.postgres_db,
        resolve_image=resolve_registration_image,
        run_spec=run_spec_operation,
        trust=driver_trust_policy(resources),
        hosting=HostingStatus(
            enabled=settings.connector_service_pods_enabled,
            enforcement_verified=settings.connector_service_enforcement_verified,
        ),
        require_approved_user=auth.require_approved_user,
    )


def agent_git_swap_dependencies(
    resources: ApplicationResources,
) -> agent_git_swap_routes.AgentGitSwapDependencies:
    """Compose the agent's question about a git swap binding's pod (C3)."""
    from orchestrator.security import access

    return agent_git_swap_routes.AgentGitSwapDependencies(
        store=resources.postgres_db,
        require_internal=access.require_internal,
    )


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
    # Bind-time driver pods post their outcome here (D6).
    app.state.driver_operations_store_factory = lambda: resources.postgres_db
    # Its denials are logged coalesced, as the exchange's are recorded.
    app.state.driver_result_limiter = DenialLimiter()

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
        BodyLimit(app, path_limits={RESULT_PATH: MAX_RESULT_BYTES}),
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
    "agent_git_swap_dependencies",
    "bind_time_pod_runtime_builder",
    "bind_time_runtime",
    "connector_lease_exchange",
    "driver_trust_policy",
    "connector_lease_exchange_app",
    "connector_service_reconciler_builder",
    "git_swap_delivery_settings",
    "git_swap_image",
    "service_hosting_settings",
    "exchange_server_config",
    "serve_connector_lease_exchange",
    "service_image_settings",
]
