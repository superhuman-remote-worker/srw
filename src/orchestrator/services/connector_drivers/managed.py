"""Managed connections: ``srw.postgresql/v1``, ``srw.mongodb/v1``,
``srw.neo4j/v1`` and ``srw.webdav/v1``.

The agent process opens the connection and SRW's own tools use it; the
workspace never sees the login.  These connectors take no config: Postgres
and MongoDB carry their login in the connection URL, Neo4j and WebDAV in
``credentials`` (username and password).

Test connection opens the connection from the orchestrator and reports
without the driver's exception text, which routinely carries the URL and
its password.  The Neo4j, MongoDB and WebDAV clients are synchronous and
block the event loop while they probe (L1 §6 #8, pinned by the goldens).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import asyncpg

from orchestrator.services.connector_drivers.base import (
    BindContext,
    CheckContext,
    DatasourceDriver,
    payload_entry,
    probe_failure,
)
from shared.connectors.builtin import (
    MONGODB_SPEC,
    NEO4J_SPEC,
    POSTGRESQL_SPEC,
    WEBDAV_SPEC,
)


class ManagedConnectionDriver(DatasourceDriver):
    #: What a failed probe reports, without the exception text.
    failure_message: str
    #: Whether the client logs in with ``credentials`` (rather than a login
    #: in the URL), so the agent needs them at every access level.
    logs_in_with_credentials = False

    async def check(
        self, row: Mapping[str, Any], credentials: dict[str, Any], *, ctx: CheckContext
    ) -> dict[str, Any]:
        try:
            return await self.probe(row["connection_url"], credentials)
        except Exception:
            return probe_failure(self.failure_message, self.type_id)

    async def probe(
        self, url: str | None, credentials: dict[str, Any]
    ) -> dict[str, Any]:
        raise NotImplementedError

    def bind(
        self, row: Mapping[str, Any], credentials: Any, *, ctx: BindContext
    ) -> dict[str, Any] | None:
        read_only = row.get("project_read_only", False)
        # Read-only is enforced by the tool surface, not by the login. A
        # read-only link still withholds the credentials object of a client
        # whose login is in its URL, as it always did; one that logs in with
        # it keeps it, or it could not connect at all.
        withhold = read_only and not self.logs_in_with_credentials
        return payload_entry(
            row, credentials={} if withhold else credentials, read_only=read_only
        )


class PostgresDriver(ManagedConnectionDriver):
    failure_message = "PostgreSQL connection failed"

    def __init__(self) -> None:
        super().__init__(POSTGRESQL_SPEC)

    async def probe(
        self, url: str | None, credentials: dict[str, Any]
    ) -> dict[str, Any]:
        conn = await asyncpg.connect(url, timeout=10)
        version = await conn.fetchval("SELECT version()")
        await conn.close()
        return {"status": "ok", "message": f"Connected: {version[:80]}"}


class Neo4jDriver(ManagedConnectionDriver):
    failure_message = "Neo4j connection failed"
    logs_in_with_credentials = True

    def __init__(self) -> None:
        super().__init__(NEO4J_SPEC)

    async def probe(
        self, url: str | None, credentials: dict[str, Any]
    ) -> dict[str, Any]:
        from neo4j import GraphDatabase

        username = credentials.get("username", "neo4j")
        password = credentials.get("password", "")
        driver = GraphDatabase.driver(url, auth=(username, password))
        driver.verify_connectivity()
        driver.close()
        return {"status": "ok", "message": "Connected to Neo4j"}


class MongoDriver(ManagedConnectionDriver):
    failure_message = "MongoDB connection failed"

    def __init__(self) -> None:
        super().__init__(MONGODB_SPEC)

    async def probe(
        self, url: str | None, credentials: dict[str, Any]
    ) -> dict[str, Any]:
        from pymongo import MongoClient

        client = MongoClient(url, serverSelectionTimeoutMS=5000)
        client.server_info()
        client.close()
        return {"status": "ok", "message": "Connected to MongoDB"}


class WebDavDriver(ManagedConnectionDriver):
    failure_message = "WebDAV connection failed"
    logs_in_with_credentials = True

    def __init__(self) -> None:
        super().__init__(WEBDAV_SPEC)

    async def probe(
        self, url: str | None, credentials: dict[str, Any]
    ) -> dict[str, Any]:
        from webdav3.client import Client as WebDAVClient

        client = WebDAVClient(
            {
                "webdav_hostname": url,
                "webdav_login": credentials.get("username"),
                "webdav_password": credentials.get("password"),
            }
        )
        client.list("/")
        return {"status": "ok", "message": "Connected to WebDAV"}


def drivers() -> tuple[ManagedConnectionDriver, ...]:
    return (PostgresDriver(), Neo4jDriver(), MongoDriver(), WebDavDriver())
