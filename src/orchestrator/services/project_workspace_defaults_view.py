"""GET/PUT /api/projects/{project_id}/workspace-defaults (Slice A2b)."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import HTTPException

from orchestrator.schemas.projects import ProjectWorkspaceDefaultsUpdate
from orchestrator.services.manifest_authority import ManifestAuthority
from orchestrator.services.manifest_store import ManifestStore
from orchestrator.services.project_status import project_is_archived
from orchestrator.services.project_workspace_defaults import (
    read_current_project_defaults,
    save_settings_defaults,
)
from orchestrator.services.workspace_defaults_resolution import (
    MISSING_PROJECT_TEMPLATE,
    MISSING_REFERENCE,
    VMS_UNAVAILABLE,
    WRONG_TIER_TEMPLATE,
    declared_builtin_names,
    installation_problems,
)
from shared.workspace_defaults import (
    TEMPLATE_TIERS,
    InstallationDefaults,
    InvalidWorkspaceDefaults,
    ProjectDefaults,
    backend_mode,
    installation_defaults,
    resolve_defaults,
    tier_template,
)

PICK_A_VISIBLE_TEMPLATE = "Pick a Catalog template or one of this Project's templates."


async def vms_available(db: Any) -> bool:
    """Whether this installation offers the VM tier at all.

    Combines the deployment's ``VM_MODE`` (``off`` means the cluster has no VM
    support to provision against) with the admin kill-switch in
    ``system_settings`` — the same two facts ``check_installation_workspace_defaults``
    checks when it flags the chart's own VM values, and the same fail-open
    reading of an absent/malformed row that ``vm_workspaces_response`` uses.
    This is an installation-wide question ("does VM exist here"), not the
    per-user ``check_vm_permission`` gate ("may *this* caller get one").
    """
    from orchestrator.services.system_settings import VM_WORKSPACES_SETTING_KEY
    from shared.workspace_contract import vm_mode_from_env

    if vm_mode_from_env() == "off":
        return False
    row = await db.get_system_setting(VM_WORKSPACES_SETTING_KEY)
    value = (row or {}).get("value") or {}
    return not (isinstance(value, dict) and value.get("enabled") is False)


def _template_name(selection: dict | None) -> str | None:
    ref = (selection or {}).get("ref")
    return ref.get("name") if isinstance(ref, dict) else None


async def read_view(db: Any, project: dict, user: dict) -> dict[str, Any]:
    """The Project's workspace defaults: stored, effective, any problems, and
    this caller's `can_edit`/`account_templates`."""
    row = await read_current_project_defaults(db, project["id"])
    stored = row or ProjectDefaults()
    try:
        installation = installation_defaults()
    except InvalidWorkspaceDefaults:
        installation = InstallationDefaults()
    builtins = declared_builtin_names()
    effective: dict[str, dict] = {}
    for role, field in (("worker", "jobs"), ("session", "sessions")):
        resolution = resolve_defaults(
            role, project=row, installation=installation, builtins=builtins
        )
        effective[field] = {"mode": resolution.mode, "source": resolution.tier_source}
    problems: dict[str, str] = {}
    store = ManifestStore(db)
    for tier in TEMPLATE_TIERS:
        template, source = tier_template(
            tier, project=row, installation=installation, builtins=builtins
        )
        effective[tier] = {"template_name": _template_name(template), "source": source}
        ref = (getattr(stored, tier) or {}).get("ref")
        if (
            ref
            and await store.by_name("WorkspaceTemplate", ref["scope"], ref["name"])
            is None
        ):
            problems[tier] = MISSING_PROJECT_TEMPLATE.format(
                tier=tier, name=ref["name"]
            )
    if user.get("is_admin"):
        is_owner_or_admin = True
    else:
        role = await db.get_user_role_in_project(str(project["id"]), str(user["id"]))
        is_owner_or_admin = role == "owner"
    can_edit = (
        is_owner_or_admin
        and not project_is_archived(project)
        and stored.source != "manifest"
    )
    account_templates = await _personal_owner(db, project) == str(user["id"])
    return {
        "stored": {
            "jobs": stored.jobs,
            "sessions": stored.sessions,
            "container": stored.container,
            "vm": stored.vm,
        },
        "managed_by_manifest": stored.source == "manifest",
        "effective": effective,
        "installation": {
            "jobs": installation.jobs,
            "sessions": installation.sessions,
            **{
                tier: _template_name(
                    tier_template(
                        tier, project=None, installation=installation, builtins=builtins
                    )[0]
                )
                for tier in TEMPLATE_TIERS
            },
        },
        "template_problems": problems,
        "installation_problems": installation_problems(),
        "vm_available": await vms_available(db),
        "can_edit": can_edit,
        "account_templates": account_templates,
    }


async def _personal_owner(db: Any, project: dict) -> str | None:
    """The owner UUID of this Project when it is a personal (default) one.

    ``users.default_project_id`` is the established reverse lookup for this
    (``manifest_projects.py`` and ``workspace_defaults_backfill.py`` both use
    it); a personal project has no separate owner column of its own.
    """
    if not project.get("is_default"):
        return None
    owner = await db.fetchval(
        "SELECT id FROM users WHERE default_project_id=$1", UUID(str(project["id"]))
    )
    return str(owner) if owner else None


def _visible(scope: dict, project: dict, owner: str | None) -> bool:
    """Spec §2: Catalog, this Project, or (personal project only) its owner's
    Account — the set every Project member can read without a 403."""
    return (
        scope.get("kind") == "Catalog"
        or scope == {"kind": "Project", "name": str(project["id"])}
        or (owner is not None and scope == {"kind": "Account", "name": owner})
    )


async def update_view(
    db: Any, project: dict, user: dict, body: ProjectWorkspaceDefaultsUpdate
) -> dict[str, Any]:
    """Save the Project's workspace defaults and return the refreshed view."""
    if not await vms_available(db) and (
        "vm" in (body.jobs, body.sessions) or body.vm is not None
    ):
        raise HTTPException(422, VMS_UNAVAILABLE)
    owner = await _personal_owner(db, project)
    selections: dict[str, dict | None] = {}
    for tier in TEMPLATE_TIERS:
        ref = getattr(body, tier)
        if ref is None:
            selections[tier] = None
            continue
        scope = dict(ref.scope)
        if scope.get("kind") == "Account" and scope.get("name") in ("me", "personal"):
            scope["name"] = str(user["id"])
        if not _visible(scope, project, owner):
            raise HTTPException(422, PICK_A_VISIBLE_TEMPLATE)
        row = await ManifestStore(db).by_name("WorkspaceTemplate", scope, ref.name)
        if row is None:
            raise HTTPException(422, MISSING_REFERENCE)
        await ManifestAuthority(db, user).resource(row)
        if backend_mode(row["resolved"]["spec"]["backend"]) != tier:
            raise HTTPException(422, WRONG_TIER_TEMPLATE.format(tier=tier))
        selections[tier] = {"ref": {"name": ref.name, "scope": scope}}
    await save_settings_defaults(
        db,
        project["id"],
        ProjectDefaults(jobs=body.jobs, sessions=body.sessions, **selections),
        actor_id=str(user["id"]),
    )
    return await read_view(db, project, user)
