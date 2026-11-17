"""``srw.mcp/v1``: an MCP server the agent process connects to.

A remote server (http or sse) is a URL plus optional auth, gated by the
installation (``MCP_DATASOURCES_ENABLED``) and validated before the caller
is authenticated.  The server and its credentials are the access boundary:
SRW binds every tool the server lists, so a read-only project link changes
nothing.

Two specs, one implementation: ``srw.mcp/v1`` (stdio) owns the stored
``mcp`` type and ``srw.mcp-remote/v1`` (http and sse) serves its remote rows.
A row's Connector resource names the one its transport needs, so an edit of
the transport changes the resource's driver.  A stdio server no longer runs
in the agent pod (connector drivers D5b): a stdio row is refused on create,
never delivered and never tested here; it can be read, renamed, unpublished
(never published) and deleted, or moved to a remote transport with a URL.
A stdio image runs as a managed MCP server instead.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from typing import Any

from fastapi import HTTPException

from orchestrator.services.connector_drivers.base import (
    BindContext,
    CheckContext,
    ConnectorDraft,
    DatasourceDriver,
    DeploymentGates,
    DriverEnvironment,
    NormalizedConnector,
    SecretLeaf,
    ValidationContext,
    payload_entry,
    server_questions_config,
    server_questions_fields,
    string_leaves,
    top_level_leaves,
)
from shared.connectors.builtin import MCP_SPEC, MCP_STDIO_RETIRED, mcp_spec_for
from shared.connectors.contract import DriverSpec
from shared.connectors.envelope import unsupported_check

_CONFIG_REFUSED = (
    "An MCP connector's config holds only server_questions (decline or accept)"
)


def _mcp_config(config: Any) -> dict[str, Any]:
    """The one setting an MCP connector's config holds: what a question the
    server asks mid-call gets by itself (``shared.mcp_questions``)."""
    if not config:
        return {}
    if not isinstance(config, Mapping) or set(config) - {"server_questions"}:
        raise HTTPException(status_code=400, detail=_CONFIG_REFUSED)
    return server_questions_config(config)


class McpDriver(DatasourceDriver):
    def __init__(
        self, spec: DriverSpec = MCP_SPEC, *, serves_stored_type: bool = True
    ) -> None:
        super().__init__(spec, serves_stored_type=serves_stored_type)

    def disabled_detail(self) -> str:
        return "MCP connectors are disabled on this deployment"

    async def prevalidate(
        self, draft: ConnectorDraft, environment: DriverEnvironment
    ) -> None:
        self.require_enabled(environment.gates)
        environment.validate_mcp_datasource(
            draft.connection_url, draft.credentials or {}
        )

    async def validate(
        self,
        draft: ConnectorDraft,
        *,
        existing: Mapping[str, Any] | None,
        ctx: ValidationContext,
    ) -> NormalizedConnector:
        if existing is None:
            # prevalidate already checked the URL and credentials (and
            # refused a stdio server).
            config = _mcp_config(draft.config)
            return NormalizedConnector(
                draft.connection_url, config, self.stored_credentials(draft, existing)
            )

        credentials = self.stored_credentials(draft, existing)
        config = _mcp_config(draft.config) if draft.config is not None else None
        # Validate the connector as it will be after the edit: a stored stdio
        # server is refused unless the edit moves it to a remote transport.
        effective_credentials = (
            credentials
            if credentials is not None
            else (existing.get("credentials") or {})
        )
        if (
            credentials is None
            and not draft.connection_url
            and self._transport(effective_credentials) == "stdio"
        ):
            # An edit that leaves a stored stdio server's server alone: it
            # may still be renamed and unpublished (and deleted), never
            # published again.
            if draft.is_global is True and not existing.get("is_global"):
                raise HTTPException(status_code=400, detail=MCP_STDIO_RETIRED)
            return NormalizedConnector(None, None, None)
        url_was_supplied = "connection_url" in draft.supplied
        effective_url = (
            draft.connection_url if url_was_supplied else existing.get("connection_url")
        )
        ctx.environment.validate_mcp_datasource(effective_url, effective_credentials)
        return NormalizedConnector(draft.connection_url, config, credentials)

    async def check(
        self, row: Mapping[str, Any], credentials: dict[str, Any], *, ctx: CheckContext
    ) -> dict[str, Any]:
        if self._transport(credentials) == "stdio":
            # A stored stdio server: never run, here or in the agent pod.
            return unsupported_check(MCP_STDIO_RETIRED)
        url = row["connection_url"]
        ctx.environment.validate_mcp_datasource(url, credentials)
        return await test_mcp_datasource(url, credentials)

    def effective_access(self, row: Mapping[str, Any]) -> str | None:
        return "ReadWrite"

    @staticmethod
    def stored_credentials(
        draft: ConnectorDraft, existing: Mapping[str, Any] | None
    ) -> dict[str, Any] | None:
        """The credentials to store, a bearer token without a pasted
        ``Bearer`` scheme (SRW adds it when it sends the token)."""
        credentials = DatasourceDriver.stored_credentials(draft, existing)
        auth = credentials.get("auth") if isinstance(credentials, dict) else None
        if (
            isinstance(auth, Mapping)
            and auth.get("type") == "bearer"
            and isinstance(auth.get("token"), str)
        ):
            from shared.mcp_sdk import bare_bearer_token

            credentials = {
                **credentials,
                "auth": {**auth, "token": bare_bearer_token(auth["token"])},
            }
        return credentials

    def bind(
        self, row: Mapping[str, Any], credentials: Any, *, ctx: BindContext
    ) -> dict[str, Any] | None:
        """The server and its auth, the connector's id (a Session's "Always
        accept" is kept by it) and the server-questions setting it runs with."""
        return payload_entry(
            row,
            credentials=credentials,
            read_only=row.get("project_read_only", False),
            fields={
                "datasource_id": str(row["id"]) if row.get("id") else None,
                **server_questions_fields(row),
            },
        )

    def runtime_allowed(self, row: Mapping[str, Any], gates: DeploymentGates) -> bool:
        """Apply the deployment gate without exposing secrets; a stored
        stdio server is never delivered."""
        if not gates.mcp_datasources_enabled():
            return False
        credentials = row.get("credentials") or {}
        if isinstance(credentials, str):
            try:
                credentials = json.loads(credentials)
            except (json.JSONDecodeError, ValueError):
                credentials = {}
        transport = (
            credentials.get("transport", "http")
            if isinstance(credentials, dict)
            else "http"
        )
        return str(transport).lower() != "stdio"

    @staticmethod
    def _transport(credentials: Mapping[str, Any]) -> str:
        return str(credentials.get("transport") or "http").lower().strip()

    def resource_driver(self, credentials: Mapping[str, Any]) -> str:
        """``srw.mcp/v1`` for a stdio server, ``srw.mcp-remote/v1`` otherwise."""
        return mcp_spec_for(credentials).name

    def credential_config(self, credentials: Mapping[str, Any]) -> dict[str, Any]:
        """The transport, and a remote server's auth kind and header names.

        The stdio command and its arguments stay secret with the environment,
        the bearer token and the header values: a pasted command line often
        carries a key.
        """
        transport = self._transport(credentials)
        config: dict[str, Any] = {"transport": transport}
        if transport == "stdio":
            return config
        auth = credentials.get("auth")
        auth = auth if isinstance(auth, Mapping) else {}
        config["auth_type"] = str(auth.get("type") or "none")
        headers = auth.get("headers")
        if config["auth_type"] == "headers" and isinstance(headers, Mapping):
            config["header_names"] = sorted(str(name) for name in headers)
        return config

    def secret_leaves(self, credentials: Mapping[str, Any]) -> list[SecretLeaf]:
        """A stdio server's ``command``, each ``arg.<n>`` and ``env.<NAME>``;
        a remote server's bearer ``token`` and each ``header.<Name>``.  The
        transport and the auth type stay with the structure."""
        leaves = top_level_leaves(credentials, ("command",))
        leaves += string_leaves(
            credentials.get("args"), ("args",), lambda n: f"arg.{n}"
        )
        leaves += string_leaves(
            credentials.get("env"), ("env",), lambda n: f"env.{n}", named=True
        )
        auth = credentials.get("auth")
        if isinstance(auth, Mapping):
            if isinstance(auth.get("token"), str):
                leaves.append((("auth", "token"), "token"))
            # A header's name is already config (header_names).
            leaves += string_leaves(
                auth.get("headers"), ("auth", "headers"), lambda n: f"header.{n}"
            )
        return leaves


async def test_mcp_datasource(
    connection_url: str | None,
    credentials: dict[str, Any],
) -> dict[str, Any]:
    """Connect to a remote server and list its tools with a ten-second
    overall bound."""
    from contextlib import AsyncExitStack

    transport = str(credentials.get("transport") or "http").lower()

    async def _probe() -> dict[str, Any]:
        from shared.mcp_sdk import bearer_authorization, open_client

        async with AsyncExitStack() as stack:
            headers: dict[str, str] = {}
            auth = credentials.get("auth") or {}
            if auth.get("type") == "bearer":
                headers["Authorization"] = bearer_authorization(auth["token"])
            elif auth.get("type") == "headers":
                headers.update(auth.get("headers") or {})

            client = await open_client(
                stack,
                url=connection_url,
                transport="sse" if transport == "sse" else "http",
                headers=headers,
            )
            listing = await client.list_tools()
            names = [tool.name for tool in listing.tools]
            preview = ", ".join(names[:8])
            if len(names) > 8:
                preview += ", …"
            suffix = f" ({preview})" if preview else ""
            return {
                "status": "ok",
                "message": f"Connected: {len(names)} tools{suffix}",
            }

    try:
        return await asyncio.wait_for(_probe(), timeout=10)
    except TimeoutError:
        return {"status": "error", "message": "MCP connect timed out after 10s"}
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        # Transport exceptions can contain URLs/headers. Report only the
        # class, or the HTTP status the server refused the connection with.
        from shared.mcp_sdk import http_status

        status = http_status(exc)
        reason = f"HTTP {status}" if status is not None else _leaf_name(exc)
        return {"status": "error", "message": f"MCP connection failed ({reason})"}


def _leaf_name(exc: BaseException) -> str:
    """The type of the first exception an exception group wraps (the SDK's
    task groups wrap a transport's failure), else the exception's own."""
    grouped = getattr(exc, "exceptions", None)
    while isinstance(grouped, (list, tuple)) and grouped:
        exc = grouped[0]
        grouped = getattr(exc, "exceptions", None)
    return type(exc).__name__
