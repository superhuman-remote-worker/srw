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

import json
from dataclasses import dataclass
from typing import Any

from shared.connectors.builtin import spec_for_type


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


def get_knowledge_graph(*, dependencies: KnowledgeProjectionDependencies) -> Any | None:
    """Lazily initialise and return the KnowledgeGraphDB singleton."""
    return dependencies.graph.get()


def build_datasource_note_content(ds: dict[str, Any]) -> str:
    """Build markdown content for a connector knowledge entry.

    Content varies by type and access mode:
    - generic: lists env var names + CLI hint
    - repository: cloned path + git usage
    - managed connectors (read-write): read and write tools list
    - managed connectors (read-only): read tools list
    - webdav: always tools
    """
    ds_type = ds.get("type", "unknown")
    ds_name = ds.get("name", "Unnamed")
    desc = ds.get("description") or ""
    is_read_only = ds.get("project_read_only", False)

    if ds_type in {"generic", "credentials"}:
        return build_generic_note(ds_name, desc, ds)
    elif ds_type == "repository":
        return build_repository_note(ds_name, desc, ds)
    elif ds_type == "kb":
        root = str((ds.get("config") or {}).get("root_path") or "")
        lines = [f"## OKF Knowledge Base: {ds_name}"]
        if desc:
            lines.append(desc)
        lines.append("Centrally indexed and read-only to agents in this release.")
        if root:
            lines.append(f"OKF root: `{root}`")
        lines.append("Attach this connector explicitly, then use the `kb_*` tools.")
        return "\n\n".join(lines)
    elif ds_type == "webdav":
        return build_webdav_note(ds_name, desc, is_read_only)
    elif ds_type in ("postgresql", "neo4j", "mongodb"):
        if is_read_only:
            return build_managed_readonly_note(ds_name, desc, ds_type)
        else:
            return build_managed_readwrite_note(ds_name, desc, ds_type)
    else:
        return f"## Connector: {ds_name}\n{desc}"


def build_generic_note(name: str, desc: str, ds: dict) -> str:
    """KB entry for generic connectors."""
    lines = [f"## Connector: {name}"]
    if desc:
        lines.append(desc)

    url = ds.get("connection_url")
    cli_hint = ds.get("cli_hint")
    if url or cli_hint:
        lines.append("\n### Connection")
        if url:
            lines.append(f"- **URL:** {url} (credentials via env vars)")
        if cli_hint:
            lines.append(f"- **CLI:** `{cli_hint}`")

    creds = ds.get("credentials") or {}
    if isinstance(creds, str):
        try:
            creds = json.loads(creds)
        except (json.JSONDecodeError, ValueError):
            creds = {}
    env_vars = creds.get("env_vars", {})
    if env_vars:
        lines.append("\n### Environment Variables")
        for key in env_vars:
            lines.append(f"- `{key}` — available in workspace")

    return "\n".join(lines)


def build_repository_note(name: str, desc: str, ds: dict) -> str:
    """KB entry for repository connectors."""
    import re

    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    lines = [f"## Repository: {name}"]
    if desc:
        lines.append(desc)
    lines.append("\n### Location")
    lines.append(f"Cloned to `./repos/{slug}/` — git is pre-authenticated.")
    lines.append("\n### Usage")
    lines.append("Use standard git commands:")
    lines.append(f"- `cd repos/{slug} && git status`")
    lines.append("- `git pull`, `git commit`, `git push`")
    lines.append("- No login or credential setup required.")
    branch = ds.get("default_branch")
    if branch:
        lines.append(f"- Default branch: `{branch}`")
    return "\n".join(lines)


#: What each managed-connection tool does, for the read-write KB note.
_MANAGED_TOOL_HELP = {
    "sql_query": "execute SELECT queries",
    "sql_schema": "inspect tables, columns, types, constraints",
    "sql_execute": "execute write statements (INSERT, UPDATE, DELETE, DDL)",
    "cypher_query": "execute read-only Cypher queries",
    "cypher_execute": "execute write Cypher statements (CREATE, MERGE, DELETE, SET)",
    "get_database_schema": "inspect labels, relationships, properties",
    "mongo_query": "document queries with filters",
    "mongo_aggregate": "aggregation pipelines",
    "mongo_schema": "collections, fields, indexes",
    "mongo_insert": "insert documents",
    "mongo_update": "update documents",
}


def build_managed_readwrite_note(name: str, desc: str, ds_type: str) -> str:
    """KB entry for managed connectors in read-write mode: the tools the
    driver's ReadWrite level binds (there is no CLI access)."""
    spec = spec_for_type(ds_type)
    level = spec.access_level("ReadWrite") if spec else None
    level_tools = level.tools if level and level.tools != "*" else ()
    tools = [
        f"- `{tool}` — {_MANAGED_TOOL_HELP[tool]}"
        for tool in level_tools
        if tool in _MANAGED_TOOL_HELP
    ] or ["- Check available tools for this connector type"]
    lines = [
        f"## Connector: {name}",
        f"**Type:** {ds_type} | **Access:** read-write (tools)",
    ]
    if desc:
        lines.append(f"\n{desc}")
    lines.append("\n### Available Tools")
    lines.extend(tools)
    return "\n".join(lines)


def build_managed_readonly_note(name: str, desc: str, ds_type: str) -> str:
    """KB entry for managed connectors in read-only (tools) mode."""
    tool_info = {
        "postgresql": [
            "- `sql_query` — execute SELECT queries",
            "- `sql_schema` — inspect tables, columns, types, constraints",
        ],
        "neo4j": [
            "- `cypher_query` — execute read-only Cypher queries",
            "- `get_database_schema` — inspect labels, relationships, properties",
        ],
        "mongodb": [
            "- `mongo_query` — document queries with filters",
            "- `mongo_aggregate` — aggregation pipelines",
            "- `mongo_schema` — collections, fields, indexes",
        ],
    }
    tools = tool_info.get(ds_type, ["- Check available tools for this connector type"])
    lines = [
        f"## Connector: {name}",
        f"**Type:** {ds_type} | **Access:** read-only (tools)",
    ]
    if desc:
        lines.append(f"\n{desc}")
    lines.append("\n### Available Tools")
    lines.extend(tools)
    lines.append("\nNo CLI access or write operations available.")
    return "\n".join(lines)


def build_webdav_note(name: str, desc: str, is_read_only: bool) -> str:
    """KB entry for WebDAV connectors (always tools)."""
    access = "read-only" if is_read_only else "read-write"
    lines = [
        f"## Connector: {name}",
        f"**Type:** webdav | **Access:** {access}",
    ]
    if desc:
        lines.append(f"\n{desc}")
    lines.append("\n### Available Tools")
    lines.append("- `webdav_list` — list files and directories")
    lines.append("- `webdav_read` — read file contents")
    lines.append("- `webdav_info` — get file metadata")
    if not is_read_only:
        lines.append("- `webdav_write` — write/upload files")
        lines.append("- `webdav_delete` — delete files")
    return "\n".join(lines)


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
    content = build_datasource_note_content(datasource)

    if ds_type == "repository":
        retrieval_messages = [
            f"{ds_name} repository",
            f"git repo {ds_name}",
            f"How to access {ds_name} code",
            "available repositories",
        ]
    elif ds_type == "kb":
        retrieval_messages = [
            f"{ds_name} knowledge base",
            f"Search {ds_name} with the KB tools",
            "available OKF knowledge bases",
        ]
    elif ds_type in {"generic", "credentials"}:
        retrieval_messages = [
            f"{ds_name} connection",
            f"How to access {ds_name}",
            "available connectors",
        ]
    else:
        retrieval_messages = [
            f"{ds_name} database connection",
            f"{ds_type} access",
            f"How do I connect to {ds_name}?",
            "What databases are available?",
        ]

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
