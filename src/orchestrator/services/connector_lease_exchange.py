"""The credential lease exchange and introspection (slice C2).

Served only on the orchestrator's dedicated exchange port
(``application.connectors``), which the public ingress never routes and the
driver NetworkPolicy names. ``/api/internal/*`` on the main port is public by
default and is never used for this.

A caller authenticates with its driver identity (``Authorization: Bearer
sdi_…``) and presents a lease token. One query reads both rows; the request
is allowed only when the identity is live and belongs to the lease's
connector and driver, the lease is live, and the requested operation fits
the lease's access level. The answer carries ``Cache-Control: no-store``; a
driver may cache it for ``max_cache_seconds`` (the revocation lag).

Audit: every denial and the first exchange of a lease go to
``security_events``; later exchanges update the lease's counters only.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from orchestrator.security.access import log_security_event
from orchestrator.services.connector_credential_leases import EXPIRED
from orchestrator.services.connector_drivers.base import SupportsCredentialLease
from shared.connectors.leases import (
    DRIVER_IDENTITY_PREFIX,
    LEASE_TOKEN_PREFIX,
    MAX_CACHE_SECONDS,
    OPERATION_ACCESS,
    last_four,
    operation_allowed,
    token_digest,
    token_shape_valid,
)

logger = logging.getLogger(__name__)

EXCHANGE_PATH = "/v1/leases/exchange"
INTROSPECT_PATH = "/v1/leases/introspect"
NO_STORE = {"Cache-Control": "no-store", "Pragma": "no-cache"}

#: Denials of the caller's identity (401) rather than of its request (403).
_IDENTITY_DENIALS = frozenset({"unknown_driver_identity", "driver_identity_revoked"})

_CHECK = """
SELECT ident.id AS identity_id,
       ident.connector_id AS identity_connector_id,
       ident.driver AS identity_driver,
       ident.revoked_at IS NOT NULL AS identity_revoked,
       lease.id AS lease_id,
       lease.connector_id,
       lease.driver,
       lease.access,
       lease.expires_at,
       lease.revoked_at IS NOT NULL AS lease_revoked,
       lease.revoke_reason,
       lease.expires_at > now() AS lease_unexpired
  FROM (SELECT 1) AS anchor
  LEFT JOIN connector_driver_identities AS ident ON ident.token_hash = $1
  LEFT JOIN connector_credential_leases AS lease ON lease.token_hash = $2
"""


@dataclass(frozen=True)
class ExchangeOutcome:
    """An HTTP status and a JSON body; never carries a token."""

    status: int
    body: dict[str, Any] = field(repr=False)


def check_denial(row: Any, *, operation: str | None) -> str | None:
    """Why the checked rows refuse the request, or ``None``.

    ``operation`` is ``None`` for introspection, which never refuses on the
    lease's state (it reports it) but does refuse an identity of another
    connector.
    """
    if row is None or row["identity_id"] is None:
        return "unknown_driver_identity"
    if row["identity_revoked"]:
        return "driver_identity_revoked"
    if row["lease_id"] is None:
        return "unknown_lease"
    if (
        row["identity_connector_id"] != row["connector_id"]
        or row["identity_driver"] != row["driver"]
    ):
        return "driver_identity_of_another_connector"
    if operation is None:
        return None
    if row["lease_revoked"]:
        return "lease_expired" if row["revoke_reason"] == EXPIRED else "lease_revoked"
    if not row["lease_unexpired"]:
        return "lease_expired"
    if not operation_allowed(operation, row["access"]):
        return "operation_not_allowed"
    return None


class ConnectorLeaseExchange:
    """The exchange port's operations, bound to one application's store."""

    def __init__(self, *, store: Any, drivers: Any) -> None:
        self._store = store
        self._drivers = drivers

    async def _checked_row(self, identity_token: str, lease_token: str) -> Any:
        identity_ok = token_shape_valid(identity_token, DRIVER_IDENTITY_PREFIX)
        lease_ok = token_shape_valid(lease_token, LEASE_TOKEN_PREFIX)
        if not identity_ok:
            return None
        async with self._store.acquire() as conn:
            return await conn.fetchrow(
                _CHECK,
                token_digest(identity_token),
                token_digest(lease_token) if lease_ok else b"\x00" * 32,
            )

    async def _deny(
        self,
        reason: str,
        row: Any,
        *,
        path: str,
        operation: str | None,
        lease_token: str,
        request: Any,
    ) -> ExchangeOutcome:
        lease_id = row["lease_id"] if row is not None else None
        detail = " ".join(
            part
            for part in (
                f"reason={reason}",
                f"operation={operation}" if operation else "",
                f"identity={row['identity_id']}" if row is not None else "",
                f"identity_connector={row['identity_connector_id']}"
                if row is not None and row["identity_connector_id"]
                else "",
                f"lease_connector={row['connector_id']}"
                if row is not None and row["connector_id"]
                else "",
                f"lease_token_last_four={last_four(lease_token)}"
                if isinstance(lease_token, str) and len(lease_token) >= 4
                else "",
            )
            if part
        )
        await log_security_event(
            self._store,
            resource_type="connector_lease",
            event_type=(
                "connector_lease_exchange_denied"
                if path == EXCHANGE_PATH
                else "connector_lease_introspection_denied"
            ),
            resource_id=str(lease_id) if lease_id else None,
            detail=detail,
            request=request,
            method="POST",
            path=path,
        )
        status = 401 if reason in _IDENTITY_DENIALS else 403
        return ExchangeOutcome(status, {"error": reason})

    async def exchange(
        self,
        *,
        identity_token: str,
        lease_token: str,
        operation: str,
        request: Any = None,
    ) -> ExchangeOutcome:
        """The upstream credential for a live lease, or a denial."""
        if operation not in OPERATION_ACCESS:
            return ExchangeOutcome(400, {"error": "unknown_operation"})
        row = await self._checked_row(identity_token, lease_token)
        reason = check_denial(row, operation=operation)
        if reason is None:
            driver = self._drivers.get(str(row["driver"]))
            if not isinstance(driver, SupportsCredentialLease):
                reason = "driver_not_installed"
        upstream: dict[str, Any] = {}
        if reason is None:
            connector = await self._store.get_datasource(str(row["connector_id"]))
            try:
                if connector is None:
                    raise ValueError("connector missing")
                upstream = driver.lease_upstream(connector)
            except ValueError:
                reason = "connector_holds_no_credential"
        if reason is not None:
            return await self._deny(
                reason,
                row,
                path=EXCHANGE_PATH,
                operation=operation,
                lease_token=lease_token,
                request=request,
            )
        async with self._store.acquire() as conn:
            counted = await conn.fetchrow(
                """
                UPDATE connector_credential_leases
                   SET exchange_count = exchange_count + 1,
                       last_exchanged_at = now()
                 WHERE id = $1 AND revoked_at IS NULL AND expires_at > now()
                RETURNING exchange_count, expires_at, access
                """,
                row["lease_id"],
            )
            if counted is not None:
                await conn.execute(
                    "UPDATE connector_driver_identities SET last_used_at = now() "
                    "WHERE id = $1",
                    row["identity_id"],
                )
        if counted is None:
            # Revoked or expired between the check and the count.
            return await self._deny(
                "lease_revoked",
                row,
                path=EXCHANGE_PATH,
                operation=operation,
                lease_token=lease_token,
                request=request,
            )
        if int(counted["exchange_count"]) == 1:
            await log_security_event(
                self._store,
                resource_type="connector_lease",
                event_type="connector_lease_first_exchange",
                resource_id=str(row["lease_id"]),
                detail=(
                    f"identity={row['identity_id']} connector={row['connector_id']} "
                    f"driver={row['driver']} operation={operation}"
                ),
                request=request,
                method="POST",
                path=EXCHANGE_PATH,
            )
        return ExchangeOutcome(
            200,
            {
                "credential": str(upstream.get("credential") or ""),
                "expires_at": counted["expires_at"].isoformat(),
                "access": str(counted["access"]),
                "allowed_upstream": [
                    str(item) for item in upstream.get("allowed_upstream") or ()
                ],
                "max_cache_seconds": MAX_CACHE_SECONDS,
            },
        )

    async def introspect(
        self, *, identity_token: str, lease_token: str, request: Any = None
    ) -> ExchangeOutcome:
        """Whether a lease is live, for which connector, at which level.

        Never returns a credential. An unknown, revoked or expired lease is
        ``{"active": false}``; only the caller's identity can be refused.
        """
        row = await self._checked_row(identity_token, lease_token)
        reason = check_denial(row, operation=None)
        if reason == "unknown_lease":
            return ExchangeOutcome(200, {"active": False})
        if reason is not None:
            return await self._deny(
                reason,
                row,
                path=INTROSPECT_PATH,
                operation=None,
                lease_token=lease_token,
                request=request,
            )
        if row["lease_revoked"] or not row["lease_unexpired"]:
            return ExchangeOutcome(200, {"active": False})
        return ExchangeOutcome(
            200,
            {
                "active": True,
                "lease_id": str(row["lease_id"]),
                "connector_id": str(UUID(str(row["connector_id"]))),
                "access": str(row["access"]),
                "expires_at": row["expires_at"].isoformat(),
            },
        )


__all__ = [
    "EXCHANGE_PATH",
    "INTROSPECT_PATH",
    "NO_STORE",
    "ConnectorLeaseExchange",
    "ExchangeOutcome",
    "check_denial",
]
