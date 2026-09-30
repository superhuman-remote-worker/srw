"""The Project layer of the workspace defaults (Slice A2b).

One row per Project. The Project Settings tab writes ``settings`` rows; a
Project manifest that sets ``defaults.workspace`` writes and owns a
``manifest`` row, like Kubernetes server-side apply. The resolver reads only
this table.
"""

from __future__ import annotations

import json
from uuid import UUID

from fastapi import HTTPException

from shared.workspace_defaults import ProjectDefaults

MANAGED_BY_MANIFEST = "These defaults are managed by the Project manifest."

_UPSERT = """
INSERT INTO project_workspace_defaults
    (project_id, jobs_mode, sessions_mode, container_template, vm_template,
     source, manifest_revision, updated_at, updated_by)
VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7, now(), $8)
ON CONFLICT (project_id) DO UPDATE SET
    jobs_mode = EXCLUDED.jobs_mode,
    sessions_mode = EXCLUDED.sessions_mode,
    container_template = EXCLUDED.container_template,
    vm_template = EXCLUDED.vm_template,
    source = EXCLUDED.source,
    manifest_revision = EXCLUDED.manifest_revision,
    updated_at = now(),
    updated_by = EXCLUDED.updated_by
"""


def _json(value):
    if value is None:
        return None
    return json.loads(value) if isinstance(value, str) else value


def _dumps(value):
    return None if value is None else json.dumps(value)


def _defaults(row) -> ProjectDefaults:
    return ProjectDefaults(
        jobs=row["jobs_mode"],
        sessions=row["sessions_mode"],
        container=_json(row["container_template"]),
        vm=_json(row["vm_template"]),
        source=row["source"],
        manifest_revision=row["manifest_revision"],
    )


def _args(project_id, values: ProjectDefaults, source, revision, actor_id):
    return (
        UUID(str(project_id)),
        values.jobs,
        values.sessions,
        _dumps(values.container),
        _dumps(values.vm),
        source,
        revision,
        UUID(str(actor_id)) if actor_id else None,
    )


async def read_project_defaults(db, project_id) -> ProjectDefaults | None:
    row = await db.fetchrow(
        "SELECT * FROM project_workspace_defaults WHERE project_id=$1",
        UUID(str(project_id)),
    )
    return _defaults(row) if row else None


async def read_current_project_defaults(db, project_id) -> ProjectDefaults | None:
    """The Project's row, healed first when a manifest owns it.

    A manifest row holds only while its revision is the Project's active one.
    A writer that moved the revision without syncing, or a pre-A2b pod during
    a rolling update, leaves it behind: re-sync it from the active revision,
    or release it when the Project no longer has an active manifest. The
    writes are the same idempotent upserts activation uses.
    """
    current = await read_project_defaults(db, project_id)
    if current is None or current.source != "manifest":
        return current
    from orchestrator.services.manifest_projects import active_project_resource

    active = await active_project_resource(db, project_id)
    if active is None:
        await release_manifest_defaults(db, project_id)
    elif active["revision"] != current.manifest_revision:
        await sync_manifest_defaults(db, {**active, "linked_id": str(project_id)})
    else:
        return current
    return await read_project_defaults(db, project_id)


async def save_settings_defaults(
    db, project_id, values: ProjectDefaults, *, actor_id: str | None
) -> ProjectDefaults:
    """Write a Settings row; a manifest row is never overwritten from the UI."""
    row = await db.fetchrow(
        _UPSERT + " WHERE project_workspace_defaults.source = 'settings' RETURNING *",
        *_args(project_id, values, "settings", None, actor_id),
    )
    if row is None:
        raise HTTPException(409, MANAGED_BY_MANIFEST)
    return _defaults(row)


async def write_manifest_defaults(db, project_id, values: ProjectDefaults) -> None:
    """Project activation: the manifest sets defaults.workspace and owns the row."""
    if values.source != "manifest" or not values.manifest_revision:
        raise ValueError("Manifest-owned defaults need their Project revision.")
    await db.execute(
        _UPSERT,
        *_args(project_id, values, "manifest", values.manifest_revision, None),
    )


async def release_manifest_defaults(db, project_id) -> None:
    """The manifest dropped defaults.workspace, or went away: clear and release."""
    await db.execute(
        """UPDATE project_workspace_defaults SET
               jobs_mode = NULL, sessions_mode = NULL,
               container_template = NULL, vm_template = NULL,
               source = 'settings', manifest_revision = NULL, updated_at = now()
           WHERE project_id = $1 AND source = 'manifest'""",
        UUID(str(project_id)),
    )


async def sync_manifest_defaults(db, resource: dict) -> None:
    """Project activation: own the row when the manifest sets the field, else release."""
    from shared.manifests.workspace_defaults import project_workspace_defaults

    if resource.get("kind") != "Project" or not resource.get("linked_id"):
        return
    values = project_workspace_defaults(resource["resolved"]["spec"])
    if values is None:
        await release_manifest_defaults(db, resource["linked_id"])
        return
    await write_manifest_defaults(
        db,
        resource["linked_id"],
        ProjectDefaults(
            **values, source="manifest", manifest_revision=resource["revision"]
        ),
    )
