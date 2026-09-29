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


VMS_UNAVAILABLE = "VM workspaces are not available on this installation."
_PROBLEMS: list[str] = []


def installation_problems() -> list[str]:
    return list(_PROBLEMS)


async def check_installation_workspace_defaults(db: Any) -> list[str]:
    """Log and remember what's wrong with the chart's values; never raise."""
    import logging

    from orchestrator.services.manifest_store import ManifestStore
    from shared.workspace_contract import vm_mode_from_env
    from shared.workspace_defaults import CATALOG_SHARED, TEMPLATE_TIERS, backend_mode

    problems: list[str] = []
    try:
        installation = installation_defaults()
    except InvalidWorkspaceDefaults as exc:
        problems.append(INVALID_INSTALLATION.format(reason=exc))
        installation = None
    if installation is not None:
        vms_off = vm_mode_from_env() == "off"
        for field in ("jobs", "sessions", "vm"):
            value = getattr(installation, field)
            if vms_off and (value == "vm" or (field == "vm" and value)):
                problems.append(f"{VMS_UNAVAILABLE} (Helm workspace.defaults.{field})")
        store = ManifestStore(db)
        for tier in TEMPLATE_TIERS:
            name = getattr(installation, tier)
            if not name:
                continue
            row = await store.by_name("WorkspaceTemplate", dict(CATALOG_SHARED), name)
            if row is None:
                problems.append(
                    MISSING_INSTALLATION_TEMPLATE.format(tier=tier, name=name)
                )
            elif backend_mode(row["resolved"]["spec"]["backend"]) != tier:
                problems.append(
                    f"The {tier} template must be a {tier} workspace. (Helm workspace.defaults.{tier})"
                )
    for problem in problems:
        logging.getLogger(__name__).error("Workspace defaults: %s", problem)
    _PROBLEMS[:] = problems
    return problems
