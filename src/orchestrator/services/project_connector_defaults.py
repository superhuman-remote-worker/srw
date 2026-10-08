"""The Project layer of the connector defaults (slice D3c).

One row per Project, stored like ``project_workspace_defaults`` (0308): the
Project Settings API writes ``settings`` rows; a Project manifest that sets
``defaults.connectors`` writes and owns a ``manifest`` row, like Kubernetes
server-side apply.

The row holds connector ids (a datasource's id is its Connector's uid) that
must be linked to the Project; an id whose link is gone is ignored when read
and pruned on the next refresh.  The project's own knowledge base is never
stored: it is the layer's first, platform-owned entry, implied for every
Project (``effective_project_connector_defaults``).

Creation reads the layer through ``default_datasource_selection``, which adds
these connectors to the owner's ``auto_attach`` connectors and the native
knowledge base when a job or session takes its defaults.  ``auto_attach``
stays the creator's own preference and is not part of this layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from fastapi import HTTPException

from orchestrator.services.project_status import project_is_archived
from shared.connectors.platform import platform_owned_sql

MANAGED_BY_MANIFEST = "These defaults are managed by the Project manifest."
NOT_LINKED = (
    "A Project's connector defaults are connectors linked to it; link the "
    "connector first."
)
PLATFORM_ENTRY = (
    "The project knowledge base is always the first default; it is not set here."
)

_UPSERT = """
INSERT INTO project_connector_defaults
    (project_id, connector_ids, source, manifest_revision, updated_at, updated_by)
VALUES ($1, $2::uuid[], $3, $4, now(), $5)
ON CONFLICT (project_id) DO UPDATE SET
    connector_ids = EXCLUDED.connector_ids,
    source = EXCLUDED.source,
    manifest_revision = EXCLUDED.manifest_revision,
    updated_at = now(),
    updated_by = EXCLUDED.updated_by
"""
#: The project's linked connectors, with the platform marker, in link order
#: of id. ``platform_owned_sql`` is the predicate the datasource API refuses
#: link changes with.
_LINKED = f"""
SELECT d.id, {platform_owned_sql("d")} AS platform_owned
FROM project_datasources pd JOIN datasources d ON d.id = pd.datasource_id
WHERE pd.project_id = $1
ORDER BY d.id
"""


@dataclass(frozen=True)
class ProjectConnectorDefaults:
    """One Project's stored layer."""

    connector_ids: list[str] = field(default_factory=list)
    source: str = "settings"
    manifest_revision: str | None = None


def _defaults(row) -> ProjectConnectorDefaults:
    return ProjectConnectorDefaults(
        connector_ids=[str(value) for value in row["connector_ids"] or ()],
        source=row["source"],
        manifest_revision=row["manifest_revision"],
    )


def _uuids(values) -> list[UUID]:
    return list(dict.fromkeys(UUID(str(value)) for value in values))


async def read_project_connector_defaults(
    db, project_id
) -> ProjectConnectorDefaults | None:
    row = await db.fetchrow(
        "SELECT * FROM project_connector_defaults WHERE project_id=$1",
        UUID(str(project_id)),
    )
    return _defaults(row) if row else None


async def read_current_project_connector_defaults(
    db, project_id
) -> ProjectConnectorDefaults | None:
    """The Project's row, healed first when a manifest owns it.

    A manifest row holds only while its revision is the Project's active one,
    as for the workspace defaults: re-sync it from the active revision, or
    release it when the Project no longer has an active manifest.
    """
    current = await read_project_connector_defaults(db, project_id)
    if current is None or current.source != "manifest":
        return current
    from orchestrator.services.manifest_projects import active_project_resource

    active = await active_project_resource(db, project_id)
    if active is None:
        await release_manifest_connector_defaults(db, project_id)
    elif active["revision"] != current.manifest_revision:
        await sync_manifest_connector_defaults(
            db, {**active, "linked_id": str(project_id)}
        )
    else:
        return current
    return await read_project_connector_defaults(db, project_id)


async def linked_connectors(db, project_id) -> list[dict[str, Any]]:
    """The connectors linked to a Project: ``id`` and ``platform_owned``."""
    rows = await db.fetch(_LINKED, UUID(str(project_id)))
    return [
        {"id": str(row["id"]), "platform_owned": bool(row["platform_owned"])}
        for row in rows
    ]


async def effective_project_connector_defaults(db, project_id) -> list[dict[str, Any]]:
    """The layer as creation applies it: the platform-owned connectors linked
    to the Project (its knowledge base) first, then the stored ids still
    linked, each once."""
    linked = await linked_connectors(db, project_id)
    stored = await read_current_project_connector_defaults(db, project_id)
    by_id = {row["id"]: row for row in linked if not row["platform_owned"]}
    chosen = dict.fromkeys(
        value for value in (stored.connector_ids if stored else ()) if value in by_id
    )
    return [row for row in linked if row["platform_owned"]] + [
        by_id[value] for value in chosen
    ]


async def save_settings_connector_defaults(
    db, project_id, connector_ids, *, actor_id: str | None
) -> ProjectConnectorDefaults:
    """Write a Settings row; a manifest row is never overwritten from the API.

    Every id must be linked to the Project and not platform-owned (the
    knowledge base is implied): 422 otherwise.
    """
    try:
        wanted = _uuids(connector_ids)
    except (TypeError, ValueError):
        raise HTTPException(422, NOT_LINKED) from None
    linked = {row["id"]: row for row in await linked_connectors(db, project_id)}
    for value in wanted:
        row = linked.get(str(value))
        if row is None:
            raise HTTPException(422, NOT_LINKED)
        if row["platform_owned"]:
            raise HTTPException(422, PLATFORM_ENTRY)
    row = await db.fetchrow(
        _UPSERT + " WHERE project_connector_defaults.source = 'settings' RETURNING *",
        UUID(str(project_id)),
        wanted,
        "settings",
        None,
        UUID(str(actor_id)) if actor_id else None,
    )
    if row is None:
        raise HTTPException(409, MANAGED_BY_MANIFEST)
    return _defaults(row)


async def write_manifest_connector_defaults(
    db, project_id, connector_ids, *, revision: str
) -> None:
    """Project activation: the manifest sets defaults.connectors and owns the row."""
    if not revision:
        raise ValueError("Manifest-owned defaults need their Project revision.")
    await db.execute(
        _UPSERT,
        UUID(str(project_id)),
        _uuids(connector_ids),
        "manifest",
        revision,
        None,
    )


async def release_manifest_connector_defaults(db, project_id) -> None:
    """The manifest dropped defaults.connectors, or went away: clear and release."""
    await db.execute(
        """UPDATE project_connector_defaults SET
               connector_ids = '{}', source = 'settings',
               manifest_revision = NULL, updated_at = now()
           WHERE project_id = $1 AND source = 'manifest'""",
        UUID(str(project_id)),
    )


async def prune_unlinked_connector_defaults(db, project_id) -> None:
    """Drop stored ids whose link is gone (a Settings row; a manifest row
    follows its document, which the refresh prunes in the same step)."""
    await db.execute(
        """UPDATE project_connector_defaults pcd SET
               connector_ids = ARRAY(
                   SELECT value FROM unnest(pcd.connector_ids)
                       WITH ORDINALITY AS stored(value, position)
                   WHERE EXISTS (
                       SELECT 1 FROM project_datasources pd
                       WHERE pd.project_id = pcd.project_id
                         AND pd.datasource_id = stored.value)
                   ORDER BY position),
               updated_at = now()
           WHERE pcd.project_id = $1 AND pcd.source = 'settings'
             AND EXISTS (
                 SELECT 1 FROM unnest(pcd.connector_ids) AS stored(value)
                 WHERE NOT EXISTS (
                     SELECT 1 FROM project_datasources pd
                     WHERE pd.project_id = pcd.project_id
                       AND pd.datasource_id = stored.value))""",
        UUID(str(project_id)),
    )


async def sync_manifest_connector_defaults(db, resource: dict) -> None:
    """Project activation: own the row when the manifest sets the field, else
    release it.

    ``defaults.connectors`` names aliases of ``resources.connectors``; the
    row stores the connector ids of the aliases that name a datasource's
    Connector and are not platform-owned (the knowledge base is implied).
    An alias for another kind of connector (an inline env or files
    connector for a generic image) is not a job or session connector and is
    not stored.
    """
    from orchestrator.services.project_connectors import entry_datasource_ids

    if resource.get("kind") != "Project" or not resource.get("linked_id"):
        return
    project_id = str(resource["linked_id"])
    spec = resource["document"]["spec"]
    defaults = spec.get("defaults") or {}
    if "connectors" not in defaults:
        await release_manifest_connector_defaults(db, project_id)
        return
    connectors = (spec.get("resources") or {}).get("connectors") or {}
    named = await entry_datasource_ids(
        db,
        {
            alias: connectors[alias]
            for alias in defaults["connectors"]
            if alias in connectors
        },
        project_id=project_id,
        account_id=str(resource["owner_id"]) if resource.get("owner_id") else None,
    )
    platform = {
        row["id"]
        for row in await linked_connectors(db, project_id)
        if row["platform_owned"]
    }
    ids = [
        named[alias]
        for alias in defaults["connectors"]
        if named.get(alias) and named[alias] not in platform
    ]
    await write_manifest_connector_defaults(
        db, project_id, ids, revision=resource["revision"]
    )


# =============================================================================
# GET/PUT /api/projects/{project_id}/connector-defaults
# =============================================================================

#: Each connector's identity for the view: the Connector resource's display
#: name, name and scope when it has one, the row's name and type otherwise.
_DESCRIBE = """
SELECT d.id, d.name, d.type,
       r.name AS resource_name, r.scope_kind, r.scope_name,
       r.document #>> '{metadata,annotations,srw.io/display-name}' AS display_name
FROM datasources d
LEFT JOIN srw_resources r
       ON r.id = d.id AND r.kind = 'Connector' AND r.linked_id = d.id
      AND r.deleted_at IS NULL
WHERE d.id = ANY($1::uuid[])
"""


async def _describe(db, entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = await db.fetch(_DESCRIBE, [UUID(entry["id"]) for entry in entries])
    by_id = {str(row["id"]): row for row in rows}
    described = []
    for entry in entries:
        row = by_id.get(entry["id"])
        if row is None:
            continue
        item = {
            "id": entry["id"],
            "name": row["display_name"] or row["name"],
            "type": row["type"],
            "platform_owned": entry["platform_owned"],
            "ref": None,
        }
        if row["resource_name"]:
            item["ref"] = {
                "name": row["resource_name"],
                "scope": {"kind": row["scope_kind"], "name": row["scope_name"]},
            }
        described.append(item)
    return described


async def read_view(db, project: dict, user: dict) -> dict[str, Any]:
    """The Project's connector defaults: what is stored, what creation
    applies (the knowledge base first), and whether this caller may edit."""
    project_id = str(project["id"])
    stored = await read_current_project_connector_defaults(db, project_id)
    effective = await effective_project_connector_defaults(db, project_id)
    if user.get("is_admin"):
        is_owner_or_admin = True
    else:
        role = await db.get_user_role_in_project(project_id, str(user["id"]))
        is_owner_or_admin = role == "owner"
    managed = bool(stored and stored.source == "manifest")
    return {
        "stored": list(stored.connector_ids) if stored else [],
        "effective": await _describe(db, effective),
        "managed_by_manifest": managed,
        "can_edit": is_owner_or_admin
        and not project_is_archived(project)
        and not managed,
    }


async def update_view(db, project: dict, user: dict, connector_ids) -> dict[str, Any]:
    """Save the Project's connector defaults and return the refreshed view."""
    await save_settings_connector_defaults(
        db, project["id"], connector_ids, actor_id=str(user["id"])
    )
    return await read_view(db, project, user)


__all__ = [
    "MANAGED_BY_MANIFEST",
    "ProjectConnectorDefaults",
    "effective_project_connector_defaults",
    "linked_connectors",
    "prune_unlinked_connector_defaults",
    "read_current_project_connector_defaults",
    "read_project_connector_defaults",
    "read_view",
    "release_manifest_connector_defaults",
    "save_settings_connector_defaults",
    "sync_manifest_connector_defaults",
    "update_view",
    "write_manifest_connector_defaults",
]
