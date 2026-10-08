"""The credential lease exchange routes (slice C2), and the result route
bind-time driver pods post their outcome to (D6).

These routes are mounted ONLY on the orchestrator's dedicated exchange port
(``application.connectors.connector_lease_exchange_app``), never on the main
application: the main port's ``/api/internal/*`` is reachable from the public
ingress by default, and drivers must not reach the rest of the API. The
driver NetworkPolicy opens only this port.

A caller authenticates with its driver identity, ``Authorization: Bearer
sdi_…``; the shared internal key is never accepted. Every answer, denial
included, carries ``Cache-Control: no-store``.
"""

from __future__ import annotations

import logging
from typing import Literal

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from orchestrator.services.connector_bind_time import (
    operation_identity,
    parse_result_body,
    record_operation_result,
)
from orchestrator.services.connector_bind_time_launch import RESULT_PATH
from orchestrator.services.connector_lease_exchange import (
    EXCHANGE_PATH,
    INTROSPECT_PATH,
    NO_STORE,
    ConnectorLeaseExchange,
    ExchangeOutcome,
)

logger = logging.getLogger(__name__)

router = APIRouter()


class LeaseExchangeBody(BaseModel):
    lease_token: str = Field(..., max_length=128)
    operation: Literal["read", "write"]


class LeaseIntrospectionBody(BaseModel):
    lease_token: str = Field(..., max_length=128)


def get_connector_lease_exchange(request: Request) -> ConnectorLeaseExchange:
    """Resolve the exchange only from the application serving this port."""
    return request.app.state.connector_lease_exchange_factory()


def _identity(request: Request) -> str:
    scheme, _, credential = (request.headers.get("authorization") or "").partition(" ")
    return credential.strip() if scheme.lower() == "bearer" else ""


def _respond(outcome: ExchangeOutcome) -> JSONResponse:
    return JSONResponse(outcome.body, status_code=outcome.status, headers=NO_STORE)


@router.post(EXCHANGE_PATH)
async def exchange_connector_lease(
    request: Request, body: LeaseExchangeBody
) -> JSONResponse:
    """Exchange a lease token for its connector's upstream credential."""
    outcome = await get_connector_lease_exchange(request).exchange(
        identity_token=_identity(request),
        lease_token=body.lease_token,
        operation=body.operation,
        request=request,
    )
    return _respond(outcome)


@router.post(INTROSPECT_PATH)
async def introspect_connector_lease(
    request: Request, body: LeaseIntrospectionBody
) -> JSONResponse:
    """Whether a lease is live, for which connector and access level."""
    outcome = await get_connector_lease_exchange(request).introspect(
        identity_token=_identity(request),
        lease_token=body.lease_token,
        request=request,
    )
    return _respond(outcome)


def _result_denied(request: Request, status: int, error: str) -> JSONResponse:
    """A refused post, logged once per reason a window (never a token)."""
    limiter = getattr(request.app.state, "driver_result_limiter", None)
    record, held = limiter.admit(("driver-result", error)) if limiter else (True, 0)
    if record:
        logger.warning("Driver result refused: %s (%d more held back)", error, held)
    return JSONResponse({"error": error}, status_code=status, headers=NO_STORE)


@router.post(RESULT_PATH)
async def record_driver_result(request: Request) -> JSONResponse:
    """A bind-time driver pod's outcome (D6). Its identity names the
    operation and is checked before the body is read; each identity posts
    once, while its operation runs. The body is the shim's exact keys, no
    duplicate key anywhere (drivers/shim/run.go)."""
    store = request.app.state.driver_operations_store_factory()
    identity = _identity(request)
    refused = await operation_identity(store, identity)
    if refused is not None:
        return _result_denied(request, *refused)
    posted = parse_result_body(await request.body())
    if posted is None:
        return _result_denied(request, 422, "invalid_outcome")
    status, answer = await record_operation_result(
        store, identity_token=identity, posted=posted
    )
    if status != 200:
        return _result_denied(request, status, str(answer.get("error")))
    return JSONResponse(answer, status_code=status, headers=NO_STORE)
