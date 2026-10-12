"""``srw.echo-service/v1``: a development service-plane driver (D5).

The control-plane half of the echo driver. Its connector holds a fake
upstream secret behind a lease, as ``srw.lease-probe/v1`` does, plus the host
and port its pod may reach (its declared egress, pinned per pod). Its service
pod runs the ``image_reference`` the chart names (SRW's ``srw-driver-echo``,
built by Tilt): it answers with its request file's non-secret fields and calls
the lease exchange with its own identity, so a gate can prove the service
plane end to end.

Installed only when ``connectors.drivers.echo.enabled`` names an image (the
k3d profile does). Never in production.
"""

from __future__ import annotations

import ipaddress
import re
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
from shared.connectors.builtin import ECHO_SERVICE_SPEC

_MAX_SECRET = 4096
_HOSTNAME = re.compile(
    r"(?=.{1,253}\Z)[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    r"(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*\Z"
)


def _host(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        ipaddress.ip_address(value)
        return True
    except ValueError:
        return bool(_HOSTNAME.fullmatch(value))


class EchoServiceDriver(DatasourceDriver):
    def __init__(self, image_reference: str) -> None:
        super().__init__(ECHO_SERVICE_SPEC)
        #: The image every service pod of this driver runs (the registration's
        #: reference: a tag follows, a digest pins).
        self.image_reference = image_reference

    async def validate(
        self,
        draft: ConnectorDraft,
        *,
        existing: Mapping[str, Any] | None,
        ctx: ValidationContext,
    ) -> NormalizedConnector:
        if draft.connection_url:
            raise HTTPException(status_code=400, detail="An echo connector has no URL")
        credentials = self.stored_credentials(draft, existing)
        if credentials is not None:
            secret = credentials.get("secret")
            if set(credentials) != {"secret"} or not isinstance(secret, str):
                raise HTTPException(
                    status_code=400,
                    detail="An echo connector's credentials are {secret}",
                )
            if not secret or len(secret) > _MAX_SECRET:
                raise HTTPException(
                    status_code=400, detail="The echo secret is empty or too long"
                )
        elif existing is None:
            raise HTTPException(
                status_code=400, detail="An echo connector needs a secret"
            )
        config = draft.config
        if config is None and existing is None:
            raise HTTPException(
                status_code=400, detail="An echo connector needs a host and port"
            )
        if config is not None:
            config = dict(config)
            port = config.get("port")
            message = config.get("message")
            if (
                set(config) - {"host", "port", "message"}
                or not _host(config.get("host"))
                or isinstance(port, bool)
                or not isinstance(port, int)
                or not 1 <= port <= 65535
                or (
                    message is not None
                    and (not isinstance(message, str) or len(message) > 256)
                )
            ):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "An echo connector's config is {host, port, message?}: "
                        "a host name or address and a port"
                    ),
                )
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
            raise ValueError("The echo connector holds no secret")
        config = row.get("config")
        host = config.get("host") if isinstance(config, Mapping) else None
        port = config.get("port") if isinstance(config, Mapping) else None
        return {
            "credential": secret,
            "allowed_upstream": [f"{host}:{port}"] if host and port else [],
        }
