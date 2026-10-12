"""Connector knowledge projection into Neo4j and the pgvector search index.

Every datasource a project can reach gets one derived knowledge note so an
agent can discover it by retrieval instead of by configuration. The note is
projected into two stores: the optional Neo4j graph and the ``knowledge_index``
table in the vector pool. Both legs degrade independently — a missing Neo4j is
a normal deployment, not a fault — and ``strict=True`` is the reconciler's way
of asking for the failure to be raised instead of swallowed.

``KnowledgeProjectionDependencies.store`` is the **vector** pool (main's
``vector_db``), not ``postgres_db``: the projection touches only
``knowledge_index``, and the graph leg goes through ``graph``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from orchestrator.services.connector_drivers import (
    ConnectorDriverRegistry,
)
from orchestrator.services.connector_drivers.knowledge_note import (
    bare_note,
    connection_phrases,
)


class KnowledgeGraphHandle:
    """App-owned lazy ``KnowledgeGraphDB`` singleton.

    Replaces main's ``_knowledge_graph_db`` module global and
    ``_get_knowledge_graph()``. Semantics are unchanged: the first successful
    connect is cached for the process, and a deployment without Neo4j keeps
    returning ``None`` while every caller degrades gracefully.
    """

    def __init__(self, *, logger: Any) -> None:
        self._logger = logger
        self._graph: Any | None = None

    def get(self) -> Any | None:
        """Lazily initialise and return the KnowledgeGraphDB singleton."""
        if self._graph is not None:
            return self._graph
        try:
            from shared.runtime.services.knowledge_graph import KnowledgeGraphDB

            self._graph = KnowledgeGraphDB()
            if not self._graph.connect():
                self._logger.warning("Could not connect to Neo4j for knowledge base")
                self._graph = None
        except Exception as e:
            self._logger.warning(f"KnowledgeGraphDB not available: {e}")
            self._graph = None
        return self._graph


@dataclass(frozen=True)
class KnowledgeProjectionDependencies:
    """Collaborators for the connector-knowledge projection.

    ``store`` is the vector pool (``vector_db``) — this projection has no
    ``postgres_db`` leg.
    """

    store: Any
    logger: Any
    graph: KnowledgeGraphHandle
    #: The drivers that write each connector's note: the application's
    #: registry, never a module-level fallback.
    connector_drivers: ConnectorDriverRegistry


def get_knowledge_graph(*, dependencies: KnowledgeProjectionDependencies) -> Any | None:
    """Lazily initialise and return the KnowledgeGraphDB singleton."""
    return dependencies.graph.get()


def build_datasource_note_content(
    ds: dict[str, Any], *, drivers: ConnectorDriverRegistry
) -> str:
    """Build markdown content for a connector knowledge entry.

    The connector's driver writes it (``DatasourceDriver.knowledge_note``):
    an environment connector lists its variable names, a repository where it
    is cloned, a knowledge base how to search it, and a managed connection
    the tools its access level binds, from the driver's spec. A type no
    driver serves gets the bare note.
    """
    driver = drivers.for_type(ds.get("type"))
    return driver.knowledge_note(ds) if driver is not None else bare_note(ds)


def datasource_retrieval_messages(
    ds: dict[str, Any], *, drivers: ConnectorDriverRegistry
) -> list[str]:
    """The phrases a connector's knowledge entry is retrieved by."""
    driver = drivers.for_type(ds.get("type"))
    return (
        driver.retrieval_messages(ds) if driver is not None else connection_phrases(ds)
    )


async def sync_datasource_knowledge(
    project_id: str,
    datasource: dict[str, Any],
    *,
    strict: bool = False,
    dependencies: KnowledgeProjectionDependencies,
) -> None:
    """Create or update a connector knowledge entry in a project."""
    failures: list[str] = []
    ds_id = str(datasource["id"]).replace("-", "")[:8]
    note_id = f"ds-{ds_id}"
    ds_name = datasource.get("name", "Unnamed")
    ds_type = datasource.get("type", "unknown")
    drivers = dependencies.connector_drivers
    content = build_datasource_note_content(datasource, drivers=drivers)
    retrieval_messages = datasource_retrieval_messages(datasource, drivers=drivers)

    # Write to Neo4j (upsert with deterministic note_id)
    kg = dependencies.graph.get()
    if kg:
        try:
            title = f"Connector: {ds_name} ({ds_type})"
            from datetime import datetime, timezone

            now = datetime.now(timezone.utc).isoformat()
            kg._db.execute_write(
                """
                MERGE (n:Note {project_id: $pid, id: $nid})
                ON CREATE SET
                    n.type = 'datasource',
                    n.title = $title,
                    n.content = $content,
                    n.status = 'active',
                    n.confidence = 'high',
                    n.retrieval_messages = $retrieval_messages,
                    n.created = datetime($now),
                    n.modified = datetime($now)
                ON MATCH SET
                    n.title = $title,
                    n.content = $content,
                    n.retrieval_messages = $retrieval_messages,
                    n.modified = datetime($now)
                """,
                {
                    "pid": project_id,
                    "nid": note_id,
                    "title": title,
                    "content": content,
                    "retrieval_messages": retrieval_messages,
                    "now": now,
                },
            )
            # Ensure tags exist
            for tag_name in ["datasource", ds_type]:
                kg._db.execute_write(
                    """
                    MATCH (n:Note {project_id: $pid, id: $nid})
                    MERGE (t:Tag {name: $tag, project_id: $pid})
                    MERGE (n)-[:TAGGED]->(t)
                    """,
                    {"pid": project_id, "nid": note_id, "tag": tag_name},
                )
        except Exception as exc:
            failures.append("neo4j")
            dependencies.logger.warning(
                "Neo4j datasource knowledge sync failed error_class=%s",
                type(exc).__name__,
            )

    # Write to pgvector search index
    try:
        async with dependencies.store.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO knowledge_index (
                    note_id, project_id, title, note_type, status,
                    confidence, tags, content, retrieval_messages, modified_at
                ) VALUES ($1, $2::uuid, $3, 'datasource', 'active', 'high',
                          $4::text[], $5, $6::text[], NOW())
                ON CONFLICT (project_id, note_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    content = EXCLUDED.content,
                    retrieval_messages = EXCLUDED.retrieval_messages,
                    tags = EXCLUDED.tags,
                    modified_at = NOW(),
                    content_hash = NULL
                """,
                note_id,
                project_id,
                f"Connector: {ds_name} ({ds_type})",
                ["datasource", ds_type],
                content,
                retrieval_messages,
            )
    except Exception as exc:
        failures.append("pgvector")
        dependencies.logger.warning(
            "pgvector datasource knowledge sync failed error_class=%s",
            type(exc).__name__,
        )

    if strict and failures:
        raise RuntimeError(
            "Datasource knowledge sync failed for " + ", ".join(failures)
        )


async def delete_datasource_knowledge(
    project_id: str,
    datasource_id: str,
    *,
    strict: bool = False,
    dependencies: KnowledgeProjectionDependencies,
) -> None:
    """Remove the knowledge entry for a datasource from a project."""
    failures: list[str] = []
    ds_id = datasource_id.replace("-", "")[:8]
    note_id = f"ds-{ds_id}"

    kg = dependencies.graph.get()
    if kg:
        try:
            kg._db.execute_write(
                "MATCH (n:Note {project_id: $pid, id: $nid}) DETACH DELETE n",
                {"pid": project_id, "nid": note_id},
            )
        except Exception as exc:
            failures.append("neo4j")
            dependencies.logger.warning(
                "Neo4j datasource knowledge delete failed error_class=%s",
                type(exc).__name__,
            )

    try:
        async with dependencies.store.acquire() as conn:
            await conn.execute(
                "DELETE FROM knowledge_index WHERE project_id = $1::uuid AND note_id = $2",
                project_id,
                note_id,
            )
    except Exception as exc:
        failures.append("pgvector")
        dependencies.logger.warning(
            "pgvector datasource knowledge delete failed error_class=%s",
            type(exc).__name__,
        )

    if strict and failures:
        raise RuntimeError(
            "Datasource knowledge delete failed for " + ", ".join(failures)
        )
