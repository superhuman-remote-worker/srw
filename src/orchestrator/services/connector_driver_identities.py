"""Connector driver identities: the ``sdi_`` tokens drivers exchange with.

A driver pod proves who it is to the lease exchange with an opaque ``sdi_``
token minted when SRW creates the pod, never with the shared internal key
every agent pod holds. SRW stores the SHA-256 digest only and reads the
binding (connector, driver, pod or image digest) from the row: a request can
never claim another connector. This copies the runtime-actor pod bootstrap.

The API service-plane hosting (D5) calls when it launches and stops a driver
pod:

* :func:`mint_driver_identity` before the pod is created; the token goes
  into the pod's own immutable Secret and is returned exactly once;
* :func:`revoke_driver_identity` when the pod stops (by id or by pod UID);
* :func:`revoke_connector_driver_identities` when the connector goes away.

Deleting the connector deletes its identities (foreign key cascade).

Design: knowledge-base/knowledge/features/connector_drivers.md, "Driver
identity".
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from orchestrator.services.connector_credential_leases import record_lease_event
from shared.connectors.contract import validate_driver_name
from shared.connectors.leases import (
    DRIVER_IDENTITY_PREFIX,
    last_four,
    mint_token,
    token_digest,
)


@dataclass(frozen=True, slots=True)
class MintedDriverIdentity:
    """A new identity. ``token`` is shown once and never stored."""

    id: str
    token: str
    connector_id: str
    driver: str

    def __repr__(self) -> str:  # never print the token
        return (
            f"MintedDriverIdentity(id={self.id!r}, connector_id="
            f"{self.connector_id!r}, driver={self.driver!r})"
        )


async def mint_driver_identity(
    conn: Any,
    *,
    connector_id: str,
    driver: str,
    image_digest: str | None = None,
    pod_namespace: str | None = None,
    pod_name: str | None = None,
    pod_uid: str | None = None,
) -> MintedDriverIdentity:
    """Mint an identity bound to one connector, driver and pod or digest."""
    problems = validate_driver_name(driver)
    if problems:
        raise ValueError(problems[0])
    connector_uuid = UUID(str(connector_id))
    token = mint_token(DRIVER_IDENTITY_PREFIX)
    row = await conn.fetchrow(
        """
        INSERT INTO connector_driver_identities
            (token_hash, token_last_four, connector_id, driver, image_digest,
             pod_namespace, pod_name, pod_uid)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id
        """,
        token_digest(token),
        last_four(token),
        connector_uuid,
        driver,
        image_digest,
        pod_namespace,
        pod_name,
        pod_uid,
    )
    identity_id = str(row["id"])
    await record_lease_event(
        conn,
        event_type="connector_driver_identity_minted",
        resource_type="connector_driver_identity",
        resource_id=identity_id,
        detail=(
            f"connector={connector_uuid} driver={driver} "
            f"pod={pod_namespace or '-'}/{pod_name or '-'} "
            f"digest={image_digest or '-'} token_last_four={last_four(token)}"
        ),
    )
    return MintedDriverIdentity(
        id=identity_id, token=token, connector_id=str(connector_uuid), driver=driver
    )


async def _revoke(conn: Any, where: str, arg: Any, reason: str) -> list[str]:
    rows = await conn.fetch(
        f"""
        UPDATE connector_driver_identities
           SET revoked_at = now(), revoke_reason = $2
         WHERE revoked_at IS NULL AND {where}
        RETURNING id, connector_id, driver
        """,
        arg,
        reason,
    )
    for row in rows:
        await record_lease_event(
            conn,
            event_type="connector_driver_identity_revoked",
            resource_type="connector_driver_identity",
            resource_id=str(row["id"]),
            detail=(
                f"connector={row['connector_id']} driver={row['driver']} "
                f"reason={reason}"
            ),
        )
    return [str(row["id"]) for row in rows]


async def revoke_driver_identity(
    conn: Any,
    *,
    identity_id: str | None = None,
    pod_uid: str | None = None,
    reason: str = "pod_stopped",
) -> list[str]:
    """Revoke one identity, by its id or by the pod UID it is bound to."""
    if (identity_id is None) == (pod_uid is None):
        raise ValueError("revoke_driver_identity needs an identity_id or a pod_uid")
    if identity_id is not None:
        return await _revoke(conn, "id = $1::uuid", UUID(str(identity_id)), reason)
    return await _revoke(conn, "pod_uid = $1::text", str(pod_uid), reason)


async def revoke_connector_driver_identities(
    conn: Any, *, connector_id: str, reason: str = "connector_removed"
) -> list[str]:
    """Revoke every live identity of one connector."""
    return await _revoke(
        conn, "connector_id = $1::uuid", UUID(str(connector_id)), reason
    )


__all__ = [
    "MintedDriverIdentity",
    "mint_driver_identity",
    "revoke_connector_driver_identities",
    "revoke_driver_identity",
]
