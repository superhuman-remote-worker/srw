"""Connector credential leases: the exchange port and the sweeper (slice C2).

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


class _BodyTooLarge(Exception):
    pass


class BodyLimit:
    """ASGI wrapper refusing a request body over ``limit`` bytes with 413.

    A declared ``Content-Length`` over the limit is refused before anything
    is read; a body that streams past the limit (chunked, or lying about its
    length) is refused at the first chunk that crosses it, so the
    application never buffers more than ``limit`` bytes.
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
        started = False

        async def limited_receive() -> Message:
            nonlocal received
            message = await receive()
            if message.get("type") == "http.request":
                received += len(message.get("body") or b"")
                if received > self.limit:
                    raise _BodyTooLarge
            return message

        async def tracked_send(message: Message) -> None:
            nonlocal started
            if message.get("type") == "http.response.start":
                started = True
            await send(message)

        try:
            await self.app(scope, limited_receive, tracked_send)
        except _BodyTooLarge:
            if started:
                raise
            await self._refuse(send)


def connector_lease_exchange(
    resources: ApplicationResources, limiter: DenialLimiter | None = None
) -> ConnectorLeaseExchange:
    """The exchange operations, bound to this application's store and drivers."""
    return ConnectorLeaseExchange(
        store=resources.postgres_db,
        drivers=resources.connector_drivers,
        limiter=limiter,
    )


def connector_lease_exchange_app(resources: ApplicationResources) -> FastAPI:
    """The dedicated exchange port's application.

    One denial limiter for the application's life, so coalescing survives
    across requests; the store and drivers are read per request.
    """
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    limiter = DenialLimiter()
    app.state.connector_lease_exchange_factory = lambda: connector_lease_exchange(
        resources, limiter
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
    """The exchange port's server: bounded, quiet, no websockets."""
    return uvicorn.Config(
        BodyLimit(app),
        ws="none",
        lifespan="off",
        log_config=None,
        access_log=False,
        server_header=False,
        date_header=False,
        limit_concurrency=MAX_CONCURRENT_CONNECTIONS,
        timeout_graceful_shutdown=GRACEFUL_SHUTDOWN_SECONDS,
    )


async def serve_connector_lease_exchange(
    resources: ApplicationResources, *, port: int, shutdown_event: asyncio.Event
) -> None:
    """Serve the exchange on ``port`` until shutdown.

    The socket is bound here, not by uvicorn, whose bind failure exits the
    process: a busy port is logged and leaves the exchange off, never the
    orchestrator down. Shutdown waits for open requests at most
    ``GRACEFUL_SHUTDOWN_SECONDS`` (and the server task a little longer
    before it is cancelled), so a slow client never holds the orchestrator's
    shutdown.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        # Pod network; the chart's NetworkPolicy bounds who reaches it.
        sock.bind(("0.0.0.0", port))  # nosec B104
    except OSError as exc:
        sock.close()
        logger.error("Connector lease exchange cannot bind port %d: %s", port, exc)
        return
    server = _EmbeddedServer(
        exchange_server_config(connector_lease_exchange_app(resources))
    )
    task = asyncio.create_task(
        server.serve(sockets=[sock]), name="connector-lease-exchange"
    )
    logger.info("Connector lease exchange listening on port %d", port)
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
        sock.close()


__all__ = [
    "GRACEFUL_SHUTDOWN_SECONDS",
    "MAX_BODY_BYTES",
    "MAX_CONCURRENT_CONNECTIONS",
    "BodyLimit",
    "connector_lease_exchange",
    "connector_lease_exchange_app",
    "exchange_server_config",
    "serve_connector_lease_exchange",
]
