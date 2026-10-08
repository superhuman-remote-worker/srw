"""The credential lease exchange routes (slice C2).

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

from typing import Literal

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from orchestrator.services.connector_lease_exchange import (
    EXCHANGE_PATH,
    INTROSPECT_PATH,
    NO_STORE,
    ConnectorLeaseExchange,
    ExchangeOutcome,
)

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
