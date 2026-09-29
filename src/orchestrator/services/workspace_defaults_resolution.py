"""The workspace defaults chain against the database and the chart (Slice A2b)."""

from __future__ import annotations

from typing import Any, Mapping

from fastapi import HTTPException

from orchestrator.services.project_workspace_defaults import read_project_defaults
from shared.workspace_defaults import (
    InvalidWorkspaceDefaults,
    Upgrade,
    UpgradeRefused,
    WorkspaceResolution,
    installation_defaults,
    resolve_defaults,
)

INVALID_INSTALLATION = "The installation's workspace defaults (Helm workspace.defaults) are invalid: {reason}"
MISSING_PROJECT_TEMPLATE = "This Project's {tier} template '{name}' no longer exists."
MISSING_INSTALLATION_TEMPLATE = (
    "The installation's {tier} template '{name}' "
    "(Helm workspace.defaults.{tier}) no longer exists."
)
MISSING_BUILTIN_TEMPLATE = "The built-in {tier} template '{name}' is missing; see the orchestrator's startup log."
MISSING_REFERENCE = (
    "Referenced resource or requested immutable revision does not exist."
)
WRONG_TIER_TEMPLATE = "The {tier} template must be a {tier} workspace."
_MISSING = {
    "project": MISSING_PROJECT_TEMPLATE,
    "installation": MISSING_INSTALLATION_TEMPLATE,
    "builtin": MISSING_BUILTIN_TEMPLATE,
}


def declared_builtin_names() -> frozenset[str]:
    from orchestrator.services.builtin_workspace_templates import (
        declared_builtin_templates,
    )

    try:
        declared = declared_builtin_templates() or []
    except ValueError:
        return frozenset()
    return frozenset(
        doc["metadata"]["name"]
        for doc in declared
        if isinstance(doc, dict)
        and isinstance(doc.get("metadata"), dict)
        and isinstance(doc["metadata"].get("name"), str)
    )


async def resolve_workspace_defaults(
    db: Any, *, role: str, project_id: str | None, upgrade: Upgrade | None = None
) -> WorkspaceResolution:
    try:
        installation = installation_defaults()
    except InvalidWorkspaceDefaults as exc:
        raise HTTPException(503, INVALID_INSTALLATION.format(reason=exc)) from None
    project = await read_project_defaults(db, project_id) if project_id else None
    try:
        return resolve_defaults(
            role,
            project=project,
            installation=installation,
            builtins=declared_builtin_names(),
            upgrade=upgrade,
        )
    except UpgradeRefused as exc:
        raise HTTPException(400, str(exc)) from None


def missing_template_message(resolution: WorkspaceResolution) -> str:
    return _MISSING[resolution.template_source].format(
        tier=resolution.mode, name=resolution.template_name() or "?"
    )


def workspace_sources_record(selection: Mapping | None) -> dict | None:
    sources = (selection or {}).get("sources")
    if not sources:
        return None
    return {**sources, "template_name": (selection or {}).get("template_name")}
