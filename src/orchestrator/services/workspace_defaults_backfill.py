"""Move pre-A2b workspace defaults into project_workspace_defaults, once.

Idempotent: it only writes Projects that have no row yet, so running it at
every start is harmless.
"""

from __future__ import annotations

import json
from typing import Any

from orchestrator.services.project_workspace_defaults import (
    read_project_defaults,
    save_settings_defaults,
    sync_manifest_defaults,
)
from shared.workspace_defaults import ProjectDefaults, backend_mode


def _json(value):
    return json.loads(value) if isinstance(value, str) else (value or {})


async def backfill_workspace_defaults(db: Any) -> dict[str, int]:
    from orchestrator.services.manifest_projects import (
        active_project_resource,
        source_recipe,
    )

    counts = {"manifest": 0, "legacy_project": 0, "preference": 0}
    for row in await db.fetch(
        """SELECT linked_id FROM srw_resources
           WHERE kind='Project' AND deleted_at IS NULL
             AND active_revision IS NOT NULL AND linked_id IS NOT NULL"""
    ):
        project_id = str(row["linked_id"])
        if await read_project_defaults(db, project_id) is not None:
            continue
        project = await active_project_resource(db, project_id)
        if project is None:
            continue
        if "workspace" in (project["resolved"]["spec"].get("defaults") or {}):
            await sync_manifest_defaults(db, {**project, "linked_id": project_id})
            counts["manifest"] += 1
            continue
        # R1 (revised): sharedConfig comes from a legacy-authored Project's
        # migrated recipe (manifest_projects.persist_project_resource copies
        # projects.default_config_override into it on every legacy PATCH). It
        # is not an authored defaults.workspace, so it becomes a settings row
        # (editable, and untouched by later manifest re-activations) rather
        # than a manifest row.
        shared = (source_recipe(project) or {}).get("sharedConfig", {})
        backend = (shared.get("workspace") or {}).get("backend")
        if backend:
            mode = backend_mode(backend)
            await save_settings_defaults(
                db,
                project_id,
                ProjectDefaults(jobs=mode, sessions=mode),
                actor_id=None,
            )
            counts["legacy_project"] += 1
    for row in await db.fetch(
        """SELECT id, default_config_override FROM projects
           WHERE manifest_resource_id IS NULL
             AND default_config_override->'workspace'->>'backend' IS NOT NULL"""
    ):
        project_id = str(row["id"])
        if await read_project_defaults(db, project_id) is not None:
            continue
        mode = backend_mode(
            _json(row["default_config_override"])["workspace"]["backend"]
        )
        await save_settings_defaults(
            db, project_id, ProjectDefaults(jobs=mode, sessions=mode), actor_id=None
        )
        counts["legacy_project"] += 1
    for row in await db.fetch(
        """SELECT id, default_project_id, settings FROM users
           WHERE default_project_id IS NOT NULL
             AND settings->'persistent_agent'->>'workspace_backend' IS NOT NULL"""
    ):
        project_id = str(row["default_project_id"])
        current = await read_project_defaults(db, project_id)
        if current is not None and (current.source == "manifest" or current.sessions):
            continue
        backend = _json(row["settings"])["persistent_agent"]["workspace_backend"]
        base = current or ProjectDefaults()
        await save_settings_defaults(
            db,
            project_id,
            ProjectDefaults(
                jobs=base.jobs,
                sessions=backend_mode(backend),
                container=base.container,
                vm=base.vm,
            ),
            actor_id=str(row["id"]),
        )
        counts["preference"] += 1
    return counts
