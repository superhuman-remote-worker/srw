"""Move pre-A2b workspace defaults into project_workspace_defaults, once.

Idempotent: it only writes Projects that have no row yet, so running it at
every start is harmless. It also heals a manifest-owned row whose revision
is no longer the Project's active one. One bad Project is logged and
skipped; it never stops the rest of the pass.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from orchestrator.services.project_workspace_defaults import (
    read_current_project_defaults,
    read_project_defaults,
    save_settings_defaults,
    sync_manifest_defaults,
)
from shared.workspace_defaults import ProjectDefaults, backend_mode

logger = logging.getLogger(__name__)


def _json(value):
    return json.loads(value) if isinstance(value, str) else (value or {})


async def _manifest_project(db: Any, project_id: str) -> str | None:
    from orchestrator.services.manifest_projects import (
        active_project_resource,
        source_recipe,
    )

    current = await read_project_defaults(db, project_id)
    if current is not None:
        if current.source != "manifest":
            return None
        healed = await read_current_project_defaults(db, project_id)
        return "healed" if healed != current else None
    project = await active_project_resource(db, project_id)
    if project is None:
        return None
    if "workspace" in (project["resolved"]["spec"].get("defaults") or {}):
        await sync_manifest_defaults(db, {**project, "linked_id": project_id})
        return "manifest"
    # R1 (revised): sharedConfig comes from a legacy-authored Project's
    # migrated recipe (manifest_projects.persist_project_resource copies
    # projects.default_config_override into it on every legacy PATCH). It
    # is not an authored defaults.workspace, so it becomes a settings row
    # (editable, and untouched by later manifest re-activations) rather
    # than a manifest row.
    shared = (source_recipe(project) or {}).get("sharedConfig", {})
    backend = (shared.get("workspace") or {}).get("backend")
    if not backend:
        return None
    mode = backend_mode(backend)
    await save_settings_defaults(
        db, project_id, ProjectDefaults(jobs=mode, sessions=mode), actor_id=None
    )
    return "legacy_project"


async def _legacy_project(db: Any, project_id: str, override: Any) -> str | None:
    if await read_project_defaults(db, project_id) is not None:
        return None
    mode = backend_mode(_json(override)["workspace"]["backend"])
    await save_settings_defaults(
        db, project_id, ProjectDefaults(jobs=mode, sessions=mode), actor_id=None
    )
    return "legacy_project"


async def _preference(db: Any, row: Any) -> str | None:
    project_id = str(row["default_project_id"])
    current = await read_project_defaults(db, project_id)
    if current is not None and (current.source == "manifest" or current.sessions):
        return None
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
    return "preference"


async def backfill_workspace_defaults(db: Any) -> dict[str, int]:
    counts = {
        "manifest": 0,
        "legacy_project": 0,
        "preference": 0,
        "healed": 0,
        "skipped": 0,
    }

    async def step(what: str, key: Any, work) -> None:
        try:
            outcome = await work
        except Exception:
            logger.exception(
                "Workspace defaults backfill skipped %s %s; it retries at the next start",
                what,
                key,
            )
            counts["skipped"] += 1
            return
        if outcome:
            counts[outcome] += 1

    for row in await db.fetch(
        """SELECT linked_id FROM srw_resources
           WHERE kind='Project' AND deleted_at IS NULL
             AND active_revision IS NOT NULL AND linked_id IS NOT NULL"""
    ):
        project_id = str(row["linked_id"])
        await step("Project", project_id, _manifest_project(db, project_id))
    for row in await db.fetch(
        """SELECT id, default_config_override FROM projects
           WHERE manifest_resource_id IS NULL
             AND default_config_override->'workspace'->>'backend' IS NOT NULL"""
    ):
        project_id = str(row["id"])
        await step(
            "Project",
            project_id,
            _legacy_project(db, project_id, row["default_config_override"]),
        )
    for row in await db.fetch(
        """SELECT id, default_project_id, settings FROM users
           WHERE default_project_id IS NOT NULL
             AND settings->'persistent_agent'->>'workspace_backend' IS NOT NULL"""
    ):
        await step("user", str(row["id"]), _preference(db, row))
    return counts
