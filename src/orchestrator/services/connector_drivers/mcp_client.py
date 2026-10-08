"""``srw.mcp/v1``: an MCP server the agent process connects to.

A remote server (http or sse) is a URL plus optional auth; a stdio server is
a command the agent runs as its subprocess.  Both are gated by the
installation (``MCP_DATASOURCES_ENABLED``; stdio also needs
``MCP_STDIO_ENABLED``, which the validator and the payload gate read), and
both are validated before the caller is authenticated.  The server and its
credentials are the access boundary: SRW binds every tool the server lists,
so a read-only project link changes nothing.

Two specs, one implementation: ``srw.mcp/v1`` (stdio) owns the stored
``mcp`` type and ``srw.mcp-remote/v1`` (http and sse) serves its remote rows.
A row's Connector resource names the one its transport needs, so an edit of
the transport changes the resource's driver.  Managed MCP images replace the
stdio subprocess path in D5.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Mapping
from typing import Any

from fastapi import HTTPException

from orchestrator.services.connector_drivers.base import (
    CheckContext,
    ConnectorDraft,
    DatasourceDriver,
    DeploymentGates,
    DriverEnvironment,
    NormalizedConnector,
    ValidationContext,
)
from shared.connectors.builtin import MCP_SPEC, mcp_spec_for
from shared.connectors.contract import DriverSpec

_CONFIG_REFUSED = "Connector config is not supported for MCP connectors"


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
            # prevalidate already checked the URL and credentials.
            connection_url = draft.connection_url
            if (draft.credentials or {}).get("transport", "http").lower() == "stdio":
                connection_url = None
            if draft.config:
                raise HTTPException(status_code=400, detail=_CONFIG_REFUSED)
            return NormalizedConnector(
                connection_url, {}, self.stored_credentials(draft, existing)
            )

        credentials = self.stored_credentials(draft, existing)
        if draft.config:
            raise HTTPException(status_code=400, detail=_CONFIG_REFUSED)
        # Validate the connector as it will be after the edit.
        effective_credentials = (
            credentials
            if credentials is not None
            else (existing.get("credentials") or {})
        )
        url_was_supplied = "connection_url" in draft.supplied
        effective_url = (
            draft.connection_url if url_was_supplied else existing.get("connection_url")
        )
        ctx.environment.validate_mcp_datasource(effective_url, effective_credentials)
        connection_url = draft.connection_url
        connection_url_set = False
        if (effective_credentials.get("transport") or "http").lower() == "stdio":
            # A stdio server has no URL; clear a stored one.
            connection_url = None
            connection_url_set = bool(
                url_was_supplied or existing.get("connection_url") is not None
            )
        return NormalizedConnector(
            connection_url,
            draft.config,
            credentials,
            connection_url_set=connection_url_set,
        )

    async def check(
        self, row: Mapping[str, Any], credentials: dict[str, Any], *, ctx: CheckContext
    ) -> dict[str, Any]:
        url = row["connection_url"]
        ctx.environment.validate_mcp_datasource(url, credentials)
        return await test_mcp_datasource(url, credentials)

    def effective_access(self, row: Mapping[str, Any]) -> str | None:
        return "ReadWrite"

    def runtime_allowed(self, row: Mapping[str, Any], gates: DeploymentGates) -> bool:
        """Apply the deployment gates without exposing secrets."""
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
        return str(transport).lower() != "stdio" or gates.mcp_stdio_enabled()

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


async def test_mcp_datasource(
    connection_url: str | None,
    credentials: dict[str, Any],
) -> dict[str, Any]:
    """Connect and list MCP tools with a ten-second overall bound."""
    import shutil
    from contextlib import AsyncExitStack

    transport = str(credentials.get("transport") or "http").lower()
    if transport == "stdio" and not shutil.which(credentials.get("command") or ""):
        return {
            "status": "ok",
            "message": (
                "stdio server untested here (runtime not on the orchestrator); "
                "it will resolve on the agent at job start"
            ),
        }

    async def _probe() -> dict[str, Any]:
        from shared.mcp_sdk import ensure_mcp_sdk

        ensure_mcp_sdk()
        async with AsyncExitStack() as stack:
            if transport == "stdio":
                from mcp import StdioServerParameters
                from mcp.client.stdio import get_default_environment, stdio_client

                parameters = StdioServerParameters(
                    command=credentials["command"],
                    args=credentials.get("args") or [],
                    env={
                        **get_default_environment(),
                        **dict(credentials.get("env") or {}),
                    },
                )
                # Never forward third-party stderr: a server may print its
                # credential-bearing environment.
                error_sink = stack.enter_context(open(os.devnull, "w"))
                read, write = await stack.enter_async_context(
                    stdio_client(parameters, errlog=error_sink)
                )
            else:
                headers: dict[str, str] = {}
                auth = credentials.get("auth") or {}
                if auth.get("type") == "bearer":
                    headers["Authorization"] = f"Bearer {auth['token']}"
                elif auth.get("type") == "headers":
                    headers.update(auth.get("headers") or {})

                if transport == "sse":
                    from mcp.client.sse import sse_client

                    read, write = await stack.enter_async_context(
                        sse_client(connection_url, headers=headers or None)
                    )
                else:
                    from mcp.client import streamable_http

                    http_transport = getattr(
                        streamable_http,
                        "streamable_http_client",
                        None,
                    )
                    if http_transport is not None:
                        from mcp.shared._httpx_utils import create_mcp_http_client

                        http_client = await stack.enter_async_context(
                            create_mcp_http_client(headers=headers or None)
                        )
                        transport_context = http_transport(
                            connection_url,
                            http_client=http_client,
                        )
                    else:
                        transport_context = streamable_http.streamablehttp_client(
                            connection_url,
                            headers=headers or None,
                        )
                    read, write, _ = await stack.enter_async_context(transport_context)

            from mcp import ClientSession

            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
            listing = await session.list_tools()
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
        # Transport exceptions can contain URLs/headers. Report only the class.
        return {
            "status": "error",
            "message": f"MCP connection failed ({type(exc).__name__})",
        }
