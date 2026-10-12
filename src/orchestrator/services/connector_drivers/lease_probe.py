"""``srw.lease-probe/v1``: a development driver behind a credential lease.

The first driver that delivers by lease (``credential_delivery = "lease"``).
It stores a fake upstream secret and an optional ``upstream`` it names as the
allowed destination. Its binding carries no secret: the lease service puts
the lease token into the entry, and the exchange hands the secret only to a
driver identity of this connector. With no driver pod of its own, a gate
mints an identity for it and calls the exchange directly.

Installed only when ``orchestrator.connectorLeases.probeDriver`` is on (the
k3d profile turns it on). The swap driver of C3 replaces it as the real
consumer of leases.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi import HTTPException

from orchestrator.services.connector_drivers.base import (
    BindContext,
    ConnectorDraft,
    DatasourceDriver,
    NormalizedConnector,
    ValidationContext,
    payload_entry,
)
from shared.connectors.builtin import LEASE_PROBE_SPEC

_MAX_SECRET = 4096


class LeaseProbeDriver(DatasourceDriver):
    def __init__(self) -> None:
        super().__init__(LEASE_PROBE_SPEC)

    async def validate(
        self,
        draft: ConnectorDraft,
        *,
        existing: Mapping[str, Any] | None,
        ctx: ValidationContext,
    ) -> NormalizedConnector:
        if draft.connection_url:
            raise HTTPException(
                status_code=400, detail="A lease probe connector has no URL"
            )
        credentials = self.stored_credentials(draft, existing)
        if credentials is not None:
            secret = credentials.get("secret")
            if set(credentials) != {"secret"} or not isinstance(secret, str):
                raise HTTPException(
                    status_code=400,
                    detail="A lease probe connector's credentials are {secret}",
                )
            if not secret or len(secret) > _MAX_SECRET:
                raise HTTPException(
                    status_code=400,
                    detail="The lease probe secret is empty or too long",
                )
        elif existing is None:
            raise HTTPException(
                status_code=400, detail="A lease probe connector needs a secret"
            )
        config = draft.config
        if config is not None:
            upstream = config.get("upstream")
            if set(config) - {"upstream"} or (
                upstream is not None
                and (not isinstance(upstream, str) or len(upstream) > 512)
            ):
                raise HTTPException(
                    status_code=400,
                    detail="A lease probe connector's config is {upstream}",
                )
            config = dict(config)
        elif existing is None:
            config = {}
        return NormalizedConnector(
            connection_url=None, config=config, credentials=credentials
        )

    def bind(
        self, row: Mapping[str, Any], credentials: Any, *, ctx: BindContext
    ) -> dict[str, Any] | None:
        # Never the stored secret: the lease service fills ``credentials``
        # with the lease token, keyed by this connector's id.
        return payload_entry(
            row,
            credentials={},
            read_only=row.get("project_read_only", False),
            connection_url=None,
            fields={"datasource_id": str(row["id"])},
        )

    def lease_upstream(self, row: Mapping[str, Any]) -> dict[str, Any]:
        credentials = row.get("credentials")
        secret = credentials.get("secret") if isinstance(credentials, Mapping) else None
        if not isinstance(secret, str) or not secret:
            raise ValueError("The lease probe connector holds no secret")
        config = row.get("config")
        upstream = config.get("upstream") if isinstance(config, Mapping) else None
        return {
            "credential": secret,
            "allowed_upstream": [upstream]
            if isinstance(upstream, str) and upstream
            else [],
        }
