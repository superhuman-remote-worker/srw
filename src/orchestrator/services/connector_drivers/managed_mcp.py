"""Managed MCP servers: service drivers whose pod runs an MCP server image (D5a).

The control-plane half of every managed MCP driver (``srw.gitea-mcp/v1``,
the development ``srw.mcp-test/v1``): the connector's config and its one
secret, the ``token`` the server needs upstream. The token never leaves SRW
except to the server, per request: a binding receives a lease token (the
agent process's bearer) and the endpoint of the connector's pods, and the
pod's front exchanges the lease for the token on each call
(:meth:`lease_upstream`). The pod's Secret holds no credential either:
``credential_delivery`` is ``lease``.

Installed only when the chart names the driver's image
(``connectors.drivers.managedMcp``; ``connectors.drivers.mcpTest`` for the
development server), and only with service-pod hosting on.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

from fastapi import HTTPException

from orchestrator.services.connector_drivers.base import (
    BindContext,
    CheckContext,
    ConnectorDraft,
    DatasourceDriver,
    NormalizedConnector,
    SecretLeaf,
    ValidationContext,
    payload_entry,
    top_level_leaves,
)
from shared.connectors.contract import DriverSpec, effective_access
from shared.connectors.envelope import unsupported_check
from shared.connectors.mcp import managed_mcp

_MAX_TOKEN = 4096


class ManagedMcpDriver(DatasourceDriver):
    """A managed MCP server image behind SRW's front."""

    def __init__(self, spec: DriverSpec, image_reference: str) -> None:
        super().__init__(spec)
        mcp = managed_mcp(spec)
        if mcp is None:
            raise ValueError(f"{spec.name} has no managed MCP block")
        self.mcp = mcp
        #: The image every pod of this driver runs (a tag follows, a digest
        #: pins), as the chart names it.
        self.image_reference = image_reference

    # -- config and credentials ------------------------------------------

    def derived_config(self, config: dict[str, Any]) -> dict[str, Any]:
        """The config as stored: a ``url`` adds the ``host`` and ``port``
        the pod's egress pins."""
        url = config.get("url")
        if url is None:
            return config
        parts = urlsplit(str(url))
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise HTTPException(
                status_code=400, detail="The URL must be http(s)://host"
            )
        try:
            port = parts.port or (443 if parts.scheme == "https" else 80)
        except ValueError:
            raise HTTPException(
                status_code=400, detail="The URL's port is invalid"
            ) from None
        return {
            **config,
            "url": f"{parts.scheme}://{parts.netloc}",
            "host": parts.hostname.lower(),
            "port": port,
        }

    def _validated_config(self, config: Mapping[str, Any]) -> dict[str, Any]:
        from jsonschema import Draft202012Validator

        schema = self.spec.config_schema
        properties = schema.get("properties") or {}
        # Mirrors are SRW's to write: a draft's value for one is replaced.
        draft = {
            key: value
            for key, value in config.items()
            if not (properties.get(key) or {}).get("readOnly")
        }
        validator = Draft202012Validator(schema)
        # The draft as sent first (a URL with a path is refused, never cut),
        # then what SRW derives from it.
        self._refuse_invalid(validator, draft)
        derived = self.derived_config(draft)
        self._refuse_invalid(validator, derived)
        return derived

    def _refuse_invalid(self, validator: Any, config: Mapping[str, Any]) -> None:
        errors = sorted(validator.iter_errors(config), key=lambda e: list(e.path))
        if errors:
            error = errors[0]
            where = "/".join(str(part) for part in error.path)
            raise HTTPException(
                status_code=400,
                detail=f"{self.spec.title} config{f' {where}' if where else ''}: "
                f"{error.message}",
            )

    async def validate(
        self,
        draft: ConnectorDraft,
        *,
        existing: Mapping[str, Any] | None,
        ctx: ValidationContext,
    ) -> NormalizedConnector:
        if draft.connection_url:
            raise HTTPException(
                status_code=400,
                detail=f"A {self.spec.title} connector has no connection URL; "
                "its config names what it serves",
            )
        credentials = self.stored_credentials(draft, existing)
        if credentials is not None:
            token = credentials.get("token")
            if set(credentials) != {"token"} or not isinstance(token, str):
                raise HTTPException(
                    status_code=400,
                    detail=f"A {self.spec.title} connector's credentials are {{token}}",
                )
            if not token or len(token) > _MAX_TOKEN or token != token.strip():
                raise HTTPException(
                    status_code=400, detail="The token is empty, padded or too long"
                )
        elif existing is None:
            raise HTTPException(
                status_code=400,
                detail=f"A {self.spec.title} connector needs a token",
            )
        config = draft.config
        if config is None and existing is None:
            config = {}
        if config is not None:
            if not isinstance(config, Mapping):
                raise HTTPException(status_code=400, detail="config must be an object")
            config = self._validated_config(config)
        return NormalizedConnector(
            connection_url=None, config=config, credentials=credentials
        )

    async def check(
        self, row: Mapping[str, Any], credentials: dict[str, Any], *, ctx: CheckContext
    ) -> dict[str, Any]:
        return unsupported_check(
            f"{self.spec.title} is tested when a session attaches it: its "
            "server runs in a pod SRW starts for the binding"
        )

    def effective_access(self, row: Mapping[str, Any]) -> str | None:
        return effective_access(row, self.spec)

    # -- binding and the lease -------------------------------------------

    def bind(
        self, row: Mapping[str, Any], credentials: Any, *, ctx: BindContext
    ) -> dict[str, Any] | None:
        # Never the token: the lease service fills ``credentials`` with a
        # lease token and the endpoint URL once the binding's digest is
        # known (``deliver_connector_leases``).
        config = row.get("config")
        return payload_entry(
            row,
            credentials={},
            read_only=row.get("project_read_only", False),
            connection_url=None,
            fields={
                "datasource_id": str(row["id"]),
                "config": dict(config) if isinstance(config, Mapping) else {},
            },
        )

    def lease_upstream(self, row: Mapping[str, Any]) -> dict[str, Any]:
        credentials = row.get("credentials")
        token = credentials.get("token") if isinstance(credentials, Mapping) else None
        if not isinstance(token, str) or not token:
            raise ValueError(f"The {self.spec.name} connector holds no token")
        config = row.get("config")
        url = config.get("url") if isinstance(config, Mapping) else None
        return {"credential": token, "allowed_upstream": [url] if url else []}

    def secret_leaves(self, credentials: Mapping[str, Any]) -> list[SecretLeaf]:
        return top_level_leaves(credentials, ("token",))


__all__ = ["ManagedMcpDriver"]
