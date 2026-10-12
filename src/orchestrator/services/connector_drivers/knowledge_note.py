"""What a connector's project knowledge note says, and how it is found.

Every connector a project can reach gets one derived note
(``knowledge_projection``). Its text is the driver's to write
(``DatasourceDriver.knowledge_note``); these are the shapes the built-in
drivers use. A managed connection lists the tools its access level binds,
straight from the driver's spec, so the note cannot offer a tool the agent
will not get.

Credential values never reach a note: an environment connector lists its
variable names only.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any

from shared.connectors.contract import DriverSpec, bound_read_only

#: What each managed-connection tool does, by tool name.
TOOL_HELP: dict[str, str] = {
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
    "webdav_list": "list files and directories",
    "webdav_read": "read file contents",
    "webdav_info": "get file metadata",
    "webdav_write": "write/upload files",
    "webdav_delete": "delete files",
}
_UNKNOWN_TOOLS = "- Check available tools for this connector type"


def _name(row: Mapping[str, Any]) -> str:
    return str(row.get("name", "Unnamed"))


def _description(row: Mapping[str, Any]) -> str:
    return row.get("description") or ""


def _type(row: Mapping[str, Any]) -> str:
    return str(row.get("type", "unknown"))


def _read_only(row: Mapping[str, Any]) -> bool:
    """Whether executions bind it read-only: its project link or its
    creator's read-only tag, the stricter."""
    return bound_read_only(row)


def bare_note(row: Mapping[str, Any]) -> str:
    """The note of a connector whose driver writes none of its own."""
    return f"## Connector: {_name(row)}\n{_description(row)}"


def connection_phrases(row: Mapping[str, Any]) -> list[str]:
    """The retrieval phrases a connector's note is found by, by default."""
    name = _name(row)
    return [
        f"{name} database connection",
        f"{_type(row)} access",
        f"How do I connect to {name}?",
        "What databases are available?",
    ]


def environment_note(row: Mapping[str, Any]) -> str:
    """An environment connector: its URL, CLI hint and variable names."""
    lines = [f"## Connector: {_name(row)}"]
    desc = _description(row)
    if desc:
        lines.append(desc)

    url = row.get("connection_url")
    cli_hint = row.get("cli_hint")
    if url or cli_hint:
        lines.append("\n### Connection")
        if url:
            lines.append(f"- **URL:** {url} (credentials via env vars)")
        if cli_hint:
            lines.append(f"- **CLI:** `{cli_hint}`")

    creds = row.get("credentials") or {}
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


def environment_phrases(row: Mapping[str, Any]) -> list[str]:
    name = _name(row)
    return [f"{name} connection", f"How to access {name}", "available connectors"]


def repository_note(row: Mapping[str, Any]) -> str:
    """A repository connector: where it is cloned and how to use git."""
    name = _name(row)
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    lines = [f"## Repository: {name}"]
    desc = _description(row)
    if desc:
        lines.append(desc)
    lines.append("\n### Location")
    lines.append(f"Cloned to `./repos/{slug}/` — git is pre-authenticated.")
    lines.append("\n### Usage")
    lines.append("Use standard git commands:")
    lines.append(f"- `cd repos/{slug} && git status`")
    lines.append("- `git pull`, `git commit`, `git push`")
    lines.append("- No login or credential setup required.")
    branch = row.get("default_branch")
    if branch:
        lines.append(f"- Default branch: `{branch}`")
    return "\n".join(lines)


def repository_phrases(row: Mapping[str, Any]) -> list[str]:
    name = _name(row)
    return [
        f"{name} repository",
        f"git repo {name}",
        f"How to access {name} code",
        "available repositories",
    ]


def knowledge_base_note(row: Mapping[str, Any]) -> str:
    """An OKF knowledge base SRW indexes centrally."""
    root = str((row.get("config") or {}).get("root_path") or "")
    lines = [f"## OKF Knowledge Base: {_name(row)}"]
    desc = _description(row)
    if desc:
        lines.append(desc)
    lines.append("Centrally indexed and read-only to agents in this release.")
    if root:
        lines.append(f"OKF root: `{root}`")
    lines.append("Attach this connector explicitly, then use the `kb_*` tools.")
    return "\n\n".join(lines)


def knowledge_base_phrases(row: Mapping[str, Any]) -> list[str]:
    name = _name(row)
    return [
        f"{name} knowledge base",
        f"Search {name} with the KB tools",
        "available OKF knowledge bases",
    ]


def _level_tools(spec: DriverSpec, level_id: str) -> list[str]:
    """One line per tool the access level binds, in the spec's order."""
    level = spec.access_level(level_id)
    tools = level.tools if level is not None and level.tools != "*" else ()
    return [
        f"- `{tool}` — {TOOL_HELP[tool]}" for tool in tools if tool in TOOL_HELP
    ] or [_UNKNOWN_TOOLS]


def database_note(row: Mapping[str, Any], spec: DriverSpec) -> str:
    """A managed database connection: its access and the tools it binds.

    The tools come from the spec's access level. A read-only Neo4j link's
    note projected before slice D1c still lists ``cypher_execute`` (the
    tool itself was never bound read-only) until that link is projected
    again: an edit of the connector's name, description, URL, credentials,
    config, visibility or read-only tag, or a re-link, enqueues it in
    ``datasource_project_reconcile_queue`` and the reconciler rewrites the
    note. No startup sweep does it: it would add a type-keyed write to every
    orchestrator start for a note the agent cannot act on.
    """
    read_only = _read_only(row)
    access = "read-only" if read_only else "read-write"
    lines = [
        f"## Connector: {_name(row)}",
        f"**Type:** {_type(row)} | **Access:** {access} (tools)",
    ]
    desc = _description(row)
    if desc:
        lines.append(f"\n{desc}")
    lines.append("\n### Available Tools")
    lines.extend(_level_tools(spec, "ReadOnly" if read_only else "ReadWrite"))
    if read_only:
        lines.append("\nNo CLI access or write operations available.")
    return "\n".join(lines)


def file_store_note(row: Mapping[str, Any], spec: DriverSpec) -> str:
    """A managed file store (WebDAV): its access and the tools it binds."""
    read_only = _read_only(row)
    access = "read-only" if read_only else "read-write"
    lines = [
        f"## Connector: {_name(row)}",
        f"**Type:** {_type(row)} | **Access:** {access}",
    ]
    desc = _description(row)
    if desc:
        lines.append(f"\n{desc}")
    lines.append("\n### Available Tools")
    lines.extend(_level_tools(spec, "ReadOnly" if read_only else "ReadWrite"))
    return "\n".join(lines)
