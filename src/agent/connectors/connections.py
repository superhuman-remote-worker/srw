"""``managed_connection``: connections the agent process holds for SRW's tools.

PostgreSQL, Neo4j, MongoDB, WebDAV and email connectors never reach the
workspace: the agent process opens the connection and the built-in tools
read it from its harness slot (``ToolContext.datasources[kind]``). The
factories are a closed set, keyed by the slot they fill, because only SRW's
own tools can use what they open; an image driver can never add one.

The slots are keyed by kind and the last connection wins, while the tool
categories bind write tools when ANY connector of a kind is read-write. So
read-only connections are opened first and read-write ones last: the
connection that takes a slot always matches the granted tool surface (write
tools never bind to a read-only link's connection).
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlparse

from agent.connectors.base import (
    ConnectionFactory,
    Delivery,
    FactsLines,
    RuntimeContext,
    declared_read_only_note,
)
from shared.datasource_policy import email_effective_access

logger = logging.getLogger(__name__)


def _database_line(delivery: Delivery, value: Mapping[str, Any]) -> list[str]:
    ds = delivery.entry
    name = ds.get("name", "Unnamed")
    kind = ds.get("type", "unknown")
    if value["read_only"]:
        return [f"- **{name}** ({kind}, read-only) — query tools"]
    return [
        f"- **{name}** ({kind}, read-write) — query + write tools"
        + declared_read_only_note(ds)
    ]


class _DatabaseFactory:
    section = "Databases"

    def facts(self, delivery: Delivery, value: Mapping[str, Any]) -> list[str]:
        return _database_line(delivery, value)


class Neo4jConnectionFactory(_DatabaseFactory):
    kind = "neo4j"

    def connect(self, value: Mapping[str, Any]) -> tuple[Any, Any | None]:
        from shared.runtime.database.neo4j_db import Neo4jDB

        credentials = value["credentials"]
        db = Neo4jDB(
            uri=value.get("url") or "",
            username=credentials.get("username", "neo4j"),
            password=credentials.get("password", ""),
            # A read-only link: every session reads, so the server refuses
            # writes even though the login could make them.
            read_only=bool(value["read_only"]),
        )
        db.connect()
        return db, None


class PostgresqlConnectionFactory(_DatabaseFactory):
    kind = "postgresql"

    def connect(self, value: Mapping[str, Any]) -> tuple[Any, Any | None]:
        import psycopg

        conn = psycopg.connect(value.get("url") or "", autocommit=False)
        conn.execute("SELECT 1")
        conn.rollback()
        return conn, None


class MongodbConnectionFactory(_DatabaseFactory):
    kind = "mongodb"

    def connect(self, value: Mapping[str, Any]) -> tuple[Any, Any | None]:
        from pymongo import MongoClient

        url = value.get("url") or ""
        client = MongoClient(url, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        parsed = urlparse(url)
        db_name = parsed.path.lstrip("/").split("?")[0] or "default"
        return client[db_name], client


class WebdavConnectionFactory:
    kind = "webdav"
    section = "Other"

    def connect(self, value: Mapping[str, Any]) -> tuple[Any, Any | None]:
        from webdav3.client import Client

        credentials = value["credentials"]
        client = Client(
            {
                "webdav_hostname": value.get("url") or "",
                "webdav_login": credentials.get("username"),
                "webdav_password": credentials.get("password"),
            }
        )
        client.list("/")
        return client, None

    def facts(self, delivery: Delivery, value: Mapping[str, Any]) -> list[str]:
        ds = delivery.entry
        access = "read-only tools" if value["read_only"] else "read-write tools"
        return [
            f"- **{ds.get('name', 'Unnamed')}** (webdav, {access})"
            f"{declared_read_only_note(ds)}"
        ]


def _email_access(value: Mapping[str, Any]) -> str:
    return email_effective_access(
        {"project_read_only": value["read_only"], "config": value["config"]}
    )


class EmailConnectionFactory:
    kind = "email"
    section = "Other"

    def connect(self, value: Mapping[str, Any]) -> tuple[Any, Any | None]:
        # Lazy import: the email tool module ships with the email tool
        # surface — keep this module importable without it.
        from agent.tools.email.connection import EmailConnection

        config = dict(value["config"])
        # The effective tier (default 'draft'); a read-only project link
        # clamps to 'read' but credentials stay intact — email needs a live
        # IMAP login at every tier.
        config["access"] = _email_access(value)
        return EmailConnection(dict(value["credentials"]), config), None

    def facts(self, delivery: Delivery, value: Mapping[str, Any]) -> list[str]:
        ds = delivery.entry
        config = ds.get("config") or {}
        account = (
            (ds.get("credentials") or {}).get("username")
            or config.get("from_address")
            or "unknown account"
        )
        folders = config.get("folders") or []
        scope = ", ".join(f"`{f}`" for f in folders) if folders else "entire mailbox"
        return [
            f"- **{ds.get('name', 'Unnamed')}** (email, {account}, tier "
            f"`{_email_access(value)}`) — folders: {scope}"
            f"{declared_read_only_note(ds)}"
        ]


#: Every managed connection SRW can open, by harness slot.
CONNECTION_FACTORIES: dict[str, ConnectionFactory] = {
    factory.kind: factory
    for factory in (
        Neo4jConnectionFactory(),
        PostgresqlConnectionFactory(),
        MongodbConnectionFactory(),
        WebdavConnectionFactory(),
        EmailConnectionFactory(),
    )
}


def open_connection(value: Mapping[str, Any]) -> tuple[Any, Any | None]:
    """Open one managed connection from its binding value.

    Returns ``(connection, parent_client)``; the parent client (MongoDB's
    ``MongoClient``) is what must be closed. Raises on failure.
    """
    factory = CONNECTION_FACTORIES.get(str(value.get("kind")))
    if factory is None:
        raise ValueError(f"No connection factory for {value.get('kind')!r}")
    return factory.connect(value)


def close_connections(connections: dict[str, Any], clients: dict[str, Any]) -> None:
    """Close every harness slot and parent client. Never raises.

    The slots include the MCP manager, which closes its servers.
    """
    for kind, conn in connections.items():
        try:
            if hasattr(conn, "close"):
                conn.close()
                logger.debug("Closed %s datasource connection", kind)
        except Exception as e:
            logger.warning("Error closing %s datasource: %s", kind, e)

    for kind, client in clients.items():
        try:
            if hasattr(client, "close"):
                client.close()
                logger.debug("Closed %s datasource client", kind)
        except Exception as e:
            logger.warning("Error closing %s datasource client: %s", kind, e)


class ManagedConnectionMaterializer:
    form = "managed_connection"

    def materialize(self, deliveries: Sequence[Delivery], rt: RuntimeContext) -> None:
        items = [
            (delivery, value)
            for delivery in deliveries
            for value in delivery.values(self.form)
        ]
        for delivery, value in sorted(items, key=lambda item: not item[1]["read_only"]):
            kind = str(value["kind"])
            try:
                conn, client = open_connection(value)
            except Exception as e:
                logger.warning("Failed to connect to %s datasource: %s", kind, e)
                continue
            # The slot holds one connection per kind: close the one this
            # replaces (a read-only link's, when a read-write one follows),
            # or it stays open with nothing left to close it.
            replaced = rt.connections.get(kind)
            if replaced is not None:
                replaced_client = rt.clients.pop(kind, None)
                close_connections(
                    {kind: replaced},
                    {kind: replaced_client} if replaced_client else {},
                )
            rt.connections[kind] = conn
            if client:
                rt.clients[kind] = client
            logger.info(
                "Connected to %s datasource: %s (%s)",
                kind,
                delivery.name,
                "read-only" if value["read_only"] else "read-write",
            )

    def release(self, rt: RuntimeContext) -> None:
        close_connections(rt.connections, rt.clients)

    def facts(
        self, deliveries: Sequence[Delivery], rt: RuntimeContext
    ) -> list[FactsLines]:
        out: list[FactsLines] = []
        for delivery in deliveries:
            for value in delivery.values(self.form):
                factory = CONNECTION_FACTORIES.get(str(value["kind"]))
                if factory is not None:
                    out.append(
                        FactsLines(
                            factory.section,
                            delivery.index,
                            factory.facts(delivery, value),
                        )
                    )
        return out
