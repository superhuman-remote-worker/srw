"""The platform-owned marker: connectors SRW provisions and manages itself.

A connector SRW creates for a project or a user carries
``datasources.managed_key``, ``<kind>:<uuid>``, for example
``project-kb:<project>``. The key is the connector's stable platform
identity, unique across rows. Users may use such a connector and SRW keeps it
alive; the API refuses to change its policy, delete it, link or unlink it, or
reindex it as an external source (:func:`managed_refusal` says why, by kind).

The kinds, and what the uuid names:

* ``project-kb:<project>``: the project's own knowledge base;
* ``project-cloud:<project>``: the project's folder in the main cloud;
* ``project-cloud-checkout:<project>``: the project folder checked out into
  a job's workspace;
* ``user-cloud-root:<user>``: the user's personal root in the main cloud;
* ``cloud-outbox:<user>``: the user's session outbox in the main cloud.

A project kind is the project's own connector: it is in scope for, and a
default of, that project's work (:func:`native_project`,
:func:`native_project_sql`). A user kind is the user's own
(:func:`native_user`, :func:`native_user_sql`): only that user's work may
attach it, whoever else could see it (an administrator, a project member).

``platform_owned`` is the one predicate behind the refusals, and
``platform_owned_sql`` is its twin for SQL that filters rows.  For one release
both fall back to the older marker, ``config.native_project_id`` on a ``kb``
row: an orchestrator that predates the key writes rows without it until the
startup backfill (``migrate_stored_connectors``) stamps them.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Datasources
become Connectors" (a general platform-owned marker), and
main_cloud_as_connectors.md (the main cloud's folders).
"""

from __future__ import annotations

from typing import Any, Mapping
from uuid import UUID

#: A project's own knowledge base.
PROJECT_KB = "project-kb"
#: A project's folder in the main cloud.
PROJECT_CLOUD = "project-cloud"
#: A project's folder checked out into a job's workspace.
PROJECT_CLOUD_CHECKOUT = "project-cloud-checkout"
#: A user's personal root in the main cloud.
USER_CLOUD_ROOT = "user-cloud-root"
#: A user's session outbox in the main cloud.
CLOUD_OUTBOX = "cloud-outbox"
#: Kinds whose uuid is a project: that project's own connectors.
PROJECT_KINDS = frozenset({PROJECT_KB, PROJECT_CLOUD, PROJECT_CLOUD_CHECKOUT})
#: Kinds whose uuid is a user: that user's own connectors.
USER_KINDS = frozenset({USER_CLOUD_ROOT, CLOUD_OUTBOX})
_KB_TYPE = "kb"
#: The older marker (``shared.native_kb``; this package imports only the
#: standard library, so the key is repeated here).
NATIVE_PROJECT_CONFIG_KEY = "native_project_id"


def managed_key(kind: str, uid: Any) -> str:
    """The managed key of the ``kind`` connector SRW keeps for ``uid``."""
    if kind not in PROJECT_KINDS | USER_KINDS:
        raise ValueError(f"Unknown platform connector kind {kind!r}")
    return f"{kind}:{UUID(str(uid))}"


def project_kb_key(project_id: Any) -> str:
    """The managed key of ``project_id``'s own knowledge base connector."""
    return managed_key(PROJECT_KB, project_id)


def project_cloud_key(project_id: Any) -> str:
    """The managed key of ``project_id``'s own cloud folder connector."""
    return managed_key(PROJECT_CLOUD, project_id)


def project_managed_keys(project_id: Any) -> list[str]:
    """Every managed key a connector of ``project_id``'s own may carry: the
    rows a project delete removes with it."""
    return [managed_key(kind, project_id) for kind in sorted(PROJECT_KINDS)]


def user_managed_keys(user_id: Any) -> list[str]:
    """Every managed key a connector of ``user_id``'s own may carry."""
    return [managed_key(kind, user_id) for kind in sorted(USER_KINDS)]


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


def managed_kind(row: Mapping[str, Any] | None) -> str | None:
    """The kind of platform-owned connector this is, or ``None``.

    The managed key's kind, or :data:`PROJECT_KB` for a row carrying only
    the older native marker (well-formed or not, as :func:`platform_owned`
    counts it).
    """
    owned = platform_owned(row)
    if owned is None:
        return None
    return owned.partition(":")[0] or None


def _owner_uuid(row: Mapping[str, Any] | None, kinds: frozenset[str]) -> str | None:
    """The uuid a well-formed key of one of ``kinds`` names, else ``None``."""
    if not row:
        return None
    key = row.get("managed_key")
    raw: str | None
    if isinstance(key, str) and key:
        kind, _, raw = key.partition(":")
        if kind not in kinds:
            return None
    elif PROJECT_KB in kinds:
        raw = _marker(row)
    else:
        return None
    if not raw:
        return None
    try:
        return str(UUID(raw))
    except (TypeError, ValueError):
        return None


def native_project(row: Mapping[str, Any] | None) -> str | None:
    """The project whose own connector this is (one of :data:`PROJECT_KINDS`),
    or ``None``.

    The project kinds' :func:`native_kb_project`: a knowledge base, a cloud
    folder or a checkout of it alike. A malformed id names no project.
    """
    return _owner_uuid(row, PROJECT_KINDS)


def native_user(row: Mapping[str, Any] | None) -> str | None:
    """The user whose own connector this is (one of :data:`USER_KINDS`), or
    ``None``."""
    return _owner_uuid(row, USER_KINDS)


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


def native_project_sql(alias: str | None = None) -> str:
    """:func:`native_project` as a SQL expression over a ``datasources`` row:
    the project id as text, or NULL.

    The key's shape is checked by the column (lowercase, canonical), so the
    text compares with ``project.id::text``. The older marker is lowercased
    the way :func:`native_project` reads it; a malformed one names no row's
    project and so matches none.
    """
    prefix = f"{alias}." if alias else ""
    kinds = ", ".join(f"'{kind}'" for kind in sorted(PROJECT_KINDS))
    return (
        f"(CASE WHEN {prefix}managed_key IS NOT NULL THEN CASE "
        f"WHEN split_part({prefix}managed_key, ':', 1) IN ({kinds}) "
        f"THEN split_part({prefix}managed_key, ':', 2) END "
        f"WHEN {prefix}type = '{_KB_TYPE}' THEN "
        f"lower(NULLIF({prefix}config->>'{NATIVE_PROJECT_CONFIG_KEY}', '')) END)"
    )


def native_user_sql(alias: str | None = None) -> str:
    """:func:`native_user` as a SQL expression over a ``datasources`` row: the
    user id as text, or NULL."""
    prefix = f"{alias}." if alias else ""
    kinds = ", ".join(f"'{kind}'" for kind in sorted(USER_KINDS))
    return (
        f"(CASE WHEN split_part({prefix}managed_key, ':', 1) IN ({kinds}) "
        f"THEN split_part({prefix}managed_key, ':', 2) END)"
    )


#: Today's wording for a project's knowledge base, which the API's goldens
#: pin byte for byte.
_KB_REFUSALS = {
    "policy": "The native project knowledge connector policy is managed by its project",
    "edit": "The project knowledge connector is managed by its project",
    "delete": "The project knowledge connector is managed by its project",
    "link": "The native knowledge connector cannot be linked to another project",
    "unlink": "The native knowledge connector link is managed by its project",
    "link_access": "The native knowledge connector link is managed by its project",
}
_SUBJECTS = {
    PROJECT_CLOUD: "The project cloud folder connector",
    PROJECT_CLOUD_CHECKOUT: "The project cloud checkout connector",
    USER_CLOUD_ROOT: "The personal cloud folder connector",
    CLOUD_OUTBOX: "The session outbox connector",
}
REFUSAL_ACTIONS = frozenset(_KB_REFUSALS)


def managed_refusal(row: Mapping[str, Any] | None, action: str) -> str:
    """Why the API refuses ``action`` on a platform-owned connector.

    ``action`` is one of :data:`REFUSAL_ACTIONS`: a ``policy`` edit (scope,
    project links, auto-attach), any other ``edit``, a ``delete``, a
    ``link`` to another project, an ``unlink`` from its own, or a change of
    its project link's access (``link_access``). A project's knowledge base
    keeps the wording it always had.
    """
    if action not in REFUSAL_ACTIONS:
        raise ValueError(f"Unknown refusal action {action!r}")
    kind = managed_kind(row)
    if kind == PROJECT_KB:
        return _KB_REFUSALS[action]
    subject = _SUBJECTS.get(kind or "", "This connector")
    if kind in USER_KINDS:
        manager, elsewhere = "SRW for its user", "a project"
    elif kind in PROJECT_KINDS:
        manager, elsewhere = "its project", "another project"
    else:
        manager, elsewhere = "SRW", "a project"
    return {
        "policy": f"{subject}'s policy is managed by {manager}",
        "edit": f"{subject} is managed by {manager} and cannot be edited",
        "delete": f"{subject} is managed by {manager}",
        "link": f"{subject} cannot be linked to {elsewhere}",
        "unlink": f"{subject}'s link is managed by {manager}",
        "link_access": f"{subject}'s access is managed by {manager}",
    }[action]


__all__ = [
    "CLOUD_OUTBOX",
    "NATIVE_PROJECT_CONFIG_KEY",
    "PROJECT_CLOUD",
    "PROJECT_CLOUD_CHECKOUT",
    "PROJECT_KB",
    "PROJECT_KINDS",
    "REFUSAL_ACTIONS",
    "USER_CLOUD_ROOT",
    "USER_KINDS",
    "managed_key",
    "managed_key_for",
    "managed_kind",
    "managed_refusal",
    "native_kb_project",
    "native_project",
    "native_project_sql",
    "native_user",
    "native_user_sql",
    "platform_owned",
    "platform_owned_sql",
    "project_cloud_key",
    "project_kb_key",
    "project_managed_keys",
    "user_managed_keys",
]
