"""Connector credential leases: the exchange port and the sweeper (slice C2).

The exchange runs as a second, minimal ASGI application on its own port in
the orchestrator process (``orchestrator.connectorLeases.exchangePort``). It
serves the exchange and introspection routes and nothing else: no health
route, no docs, no OpenAPI. The main application never includes them, so
the main port does not serve the exchange, and no ingress routes this port.
The chart's NetworkPolicy admits only the driver namespace to it.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
from collections.abc import Generator

import uvicorn
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from orchestrator.application.resources import ApplicationResources
from orchestrator.routers import connector_lease_exchange as exchange_routes
from orchestrator.services.connector_lease_exchange import (
    NO_STORE,
    ConnectorLeaseExchange,
)

logger = logging.getLogger(__name__)


def connector_lease_exchange(resources: ApplicationResources) -> ConnectorLeaseExchange:
    """The exchange operations, bound to this application's store and drivers."""
    return ConnectorLeaseExchange(
        store=resources.postgres_db, drivers=resources.connector_drivers
    )


def connector_lease_exchange_app(resources: ApplicationResources) -> FastAPI:
    """The dedicated exchange port's application."""
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    app.state.connector_lease_exchange_factory = lambda: connector_lease_exchange(
        resources
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


async def serve_connector_lease_exchange(
    resources: ApplicationResources, *, port: int, shutdown_event: asyncio.Event
) -> None:
    """Serve the exchange on ``port`` until shutdown.

    The socket is bound here, not by uvicorn, whose bind failure exits the
    process: a busy port is logged and leaves the exchange off, never the
    orchestrator down.
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
        uvicorn.Config(
            connector_lease_exchange_app(resources),
            ws="none",
            lifespan="off",
            log_config=None,
            access_log=False,
            server_header=False,
            date_header=False,
        )
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
            await task
        sock.close()


__all__ = [
    "connector_lease_exchange",
    "connector_lease_exchange_app",
    "serve_connector_lease_exchange",
]
