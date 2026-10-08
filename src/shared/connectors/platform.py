"""The platform-owned marker: connectors SRW provisions and manages itself.

A connector SRW creates for a project or a user (the project's own knowledge
base today; later the project cloud folder and the personal cloud root)
carries ``datasources.managed_key``, for example ``project-kb:<project>``. The
key is the connector's stable platform identity, unique across rows. Users may
use such a connector and SRW keeps it alive; the API refuses to change its
policy, delete it, link or unlink it, or reindex it as an external source.

``platform_owned`` is the one predicate behind those refusals, and
``platform_owned_sql`` is its twin for SQL that filters rows.  For one release
both fall back to the older marker, ``config.native_project_id`` on a ``kb``
row: an orchestrator that predates the key writes rows without it until the
startup backfill (``migrate_stored_connectors``) stamps them.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Datasources
become Connectors" (a general platform-owned marker).
"""

from __future__ import annotations

from typing import Any, Mapping
from uuid import UUID

#: A project's own knowledge base.
PROJECT_KB = "project-kb"
_KB_TYPE = "kb"
#: The older marker (``shared.native_kb``; this package imports only the
#: standard library, so the key is repeated here).
NATIVE_PROJECT_CONFIG_KEY = "native_project_id"


def project_kb_key(project_id: Any) -> str:
    """The managed key of ``project_id``'s own knowledge base connector."""
    return f"{PROJECT_KB}:{UUID(str(project_id))}"


def _marker(row: Mapping[str, Any]) -> str | None:
    """The legacy native marker's raw value, on a ``kb`` row only."""
    if str(row.get("type") or "") != _KB_TYPE:
        return None
    config = row.get("config") or {}
    if not isinstance(config, Mapping):
        return None
    value = config.get(NATIVE_PROJECT_CONFIG_KEY)
    return str(value) if value else None


def platform_owned(row: Mapping[str, Any] | None) -> str | None:
    """Why SRW owns this connector (its managed key), or ``None``.

    Reads ``managed_key`` first.  A ``kb`` row without one still counts when
    it carries the native knowledge-base marker, even a malformed one: a
    marked row was never a user's to edit, so it is refused rather than
    released.
    """
    if not row:
        return None
    key = row.get("managed_key")
    if isinstance(key, str) and key:
        return key
    marker = _marker(row)
    return f"{PROJECT_KB}:{marker}" if marker else None


def native_kb_project(row: Mapping[str, Any] | None) -> str | None:
    """The project whose own knowledge base this connector is, or ``None``.

    Stricter than :func:`platform_owned`: the id must be a UUID, so a
    malformed marker never names a project.
    """
    if not row:
        return None
    key = row.get("managed_key")
    raw: str | None
    if isinstance(key, str) and key:
        kind, _, raw = key.partition(":")
        if kind != PROJECT_KB:
            return None
    else:
        raw = _marker(row)
    if not raw:
        return None
    try:
        return str(UUID(raw))
    except (TypeError, ValueError):
        return None


def managed_key_for(row: Mapping[str, Any] | None) -> str | None:
    """The managed key a row should store: its own, else one derived from a
    well-formed native marker."""
    if not row:
        return None
    key = row.get("managed_key")
    if isinstance(key, str) and key:
        return key
    project = native_kb_project(row)
    return project_kb_key(project) if project else None


def platform_owned_sql(alias: str | None = None) -> str:
    """:func:`platform_owned` as a SQL predicate over a ``datasources`` row."""
    prefix = f"{alias}." if alias else ""
    return (
        f"({prefix}managed_key IS NOT NULL OR ({prefix}type = '{_KB_TYPE}' "
        f"AND NULLIF({prefix}config->>'{NATIVE_PROJECT_CONFIG_KEY}', '') "
        "IS NOT NULL))"
    )


__all__ = [
    "NATIVE_PROJECT_CONFIG_KEY",
    "PROJECT_KB",
    "managed_key_for",
    "native_kb_project",
    "platform_owned",
    "platform_owned_sql",
    "project_kb_key",
]
