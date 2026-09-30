"""The workspace defaults chain against the database and the chart (Slice A2b)."""

from __future__ import annotations

from typing import Any, Mapping

from fastapi import HTTPException

from orchestrator.services.project_workspace_defaults import (
    read_current_project_defaults,
)
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
    project = (
        await read_current_project_defaults(db, project_id) if project_id else None
    )
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
    from shared.workspace_defaults import (
        BUILTIN_TEMPLATES,
        CATALOG_SHARED,
        TEMPLATE_TIERS,
        backend_mode,
    )

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
        builtins = declared_builtin_names()
        for tier in TEMPLATE_TIERS:
            name = getattr(installation, tier)
            if name:
                missing = MISSING_INSTALLATION_TEMPLATE
                where = f"Helm workspace.defaults.{tier}"
            elif BUILTIN_TEMPLATES[tier] in builtins:
                # An empty name means the tier's built-in, when the chart
                # declares it: it must be there too.
                name = BUILTIN_TEMPLATES[tier]
                missing = MISSING_BUILTIN_TEMPLATE
                where = f"built-in {name}"
            else:
                continue
            row = await store.by_name("WorkspaceTemplate", dict(CATALOG_SHARED), name)
            if row is None:
                problems.append(missing.format(tier=tier, name=name))
            elif backend_mode(row["resolved"]["spec"]["backend"]) != tier:
                problems.append(
                    f"The {tier} template must be a {tier} workspace. ({where})"
                )
    for problem in problems:
        logging.getLogger(__name__).error("Workspace defaults: %s", problem)
    _PROBLEMS[:] = problems
    return problems


NO_SUCH_TEMPLATE = "No template named '{name}' is available here."
CONTAINER_UPGRADE_UNAVAILABLE = (
    "Container upgrades of a running Session are unavailable; "
    "start a new Session with this template."
)
RETAINED_UPGRADE_UNAVAILABLE = (
    "Upgrades can't use a template that keeps its workspace; "
    "start new work with this template."
)
OWNER_UNAVAILABLE = "The execution owner is unavailable."


async def work_owner(db: Any, user_id: Any) -> dict:
    """The user whose templates an upgrade reads; never the caller."""
    owner = await db.get_user(str(user_id)) if user_id else None
    if not owner:
        raise HTTPException(409, OWNER_UNAVAILABLE)
    return owner


async def find_readable_template(
    db: Any, user: dict, *, project_id: str | None, name: str
) -> tuple[dict, str]:
    """Look in the Project, then the user's Account, then Catalog/shared.

    The first scope that holds the name decides: its template is returned if
    the user may read it, else the read is refused. A later scope is
    never tried once one holds the name.
    """
    from orchestrator.services.manifest_authority import ManifestAuthority
    from orchestrator.services.manifest_store import ManifestStore
    from shared.workspace_defaults import CATALOG_SHARED, backend_mode

    scopes = []
    if project_id:
        scopes.append({"kind": "Project", "name": str(project_id)})
    scopes += [{"kind": "Account", "name": str(user["id"])}, dict(CATALOG_SHARED)]
    store, authority = ManifestStore(db), ManifestAuthority(db, user)
    for scope in scopes:
        row = await store.by_name("WorkspaceTemplate", scope, name)
        if row is None:
            continue
        await authority.resource(row)
        return (
            {"ref": {"name": name, "scope": scope}},
            backend_mode(row["resolved"]["spec"]["backend"]),
        )
    raise HTTPException(404, NO_SUCH_TEMPLATE.format(name=name))


async def render_upgrade_workspace(
    db: Any,
    user: dict,
    *,
    role: str,
    project_id: str | None,
    current_backend: str,
    requested_backend: str | None = None,
    template_name: str | None = None,
) -> tuple[str, dict, dict]:
    """The workspace an upgrade provisions: a named template, else the chain's.

    Returns ``(mode, rendered_config, sources_record)``; ``rendered_config`` is
    ``srw_workspace_config`` output.
    """
    from copy import deepcopy

    from orchestrator.services.manifest_authority import ManifestAuthority
    from orchestrator.services.manifest_resolution import LiveManifestResolver
    from orchestrator.services.manifest_store import ManifestStore
    from orchestrator.services.manifest_workspace_selection import srw_workspace_config
    from shared.manifests.errors import ManifestError
    from shared.workspace_defaults import MODE_RANK, UPGRADE_REFUSED, backend_mode

    current = backend_mode(current_backend)
    resolution = None
    if template_name:
        selection, mode = await find_readable_template(
            db, user, project_id=project_id, name=template_name
        )
        if MODE_RANK[mode] <= MODE_RANK[current]:
            raise HTTPException(400, UPGRADE_REFUSED)
        binding = {"template": selection}
        sources = {
            "tier": "explicit",
            "template": "explicit",
            "template_name": template_name,
        }
    else:
        resolution = await resolve_workspace_defaults(
            db,
            role=role,
            project_id=project_id,
            upgrade=Upgrade(
                current=current,
                requested=backend_mode(requested_backend)
                if requested_backend
                else None,
            ),
        )
        mode, binding = resolution.mode, resolution.binding()
        sources = workspace_sources_record(
            {
                "sources": resolution.sources(),
                "template_name": resolution.template_name(),
            }
        )
    resolver = LiveManifestResolver(ManifestStore(db), ManifestAuthority(db, user))
    scope = (
        {"kind": "Project", "name": str(project_id)}
        if project_id
        else resolver.authority.account
    )
    resolved = deepcopy(binding)
    try:
        resolved["template"] = await resolver.selection(
            "WorkspaceTemplate", resolved["template"], scope, []
        )
    except HTTPException as exc:
        if (
            resolution is not None
            and resolution.template_name() is not None
            and exc.status_code == 422
            and exc.detail == MISSING_REFERENCE
        ):
            raise HTTPException(409, missing_template_message(resolution)) from None
        raise
    except ManifestError as exc:
        raise HTTPException(422, str(exc)) from None
    # A template edited to another backend after it was chosen must never
    # move work to a different tier: fail closed.
    if backend_mode(resolved["template"]["inline"]["backend"]) != mode:
        raise HTTPException(409, WRONG_TIER_TEMPLATE.format(tier=mode))
    # The rendered settings can't carry retention: a kept workspace would be
    # provisioned as an ordinary one and deleted when the work ends.
    if resolved["template"]["inline"].get("retention") == "Retain":
        raise HTTPException(409, RETAINED_UPGRADE_UNAVAILABLE)
    return mode, srw_workspace_config(resolved), sources


async def upgrade_record(
    db: Any,
    user_id: Any,
    *,
    role: str,
    project_id: str | None,
    current_backend: str,
    requested_backend: str,
    template_name: str | None = None,
) -> tuple[str, dict]:
    """The tier an upgrade moves to and the record it keeps next to it.

    The record is ``{"upgrade_config": <rendered settings>, "upgrade_sources":
    <sources>}``. Templates are read as the work's owner. Ownerless work
    (trusted internal Jobs, their subjobs, agent child threads) has nobody to
    read them as: without a named template it keeps its bare provisioning.
    """
    from shared.workspace_defaults import MODE_BACKEND, backend_mode

    if not user_id and not template_name:
        return backend_mode(requested_backend), {
            "upgrade_config": {},
            "upgrade_sources": None,
        }
    owner = await work_owner(db, user_id)
    mode, config, sources = await render_upgrade_workspace(
        db,
        owner,
        role=role,
        project_id=project_id,
        current_backend=current_backend,
        requested_backend=requested_backend,
        template_name=template_name,
    )
    return mode, {
        "upgrade_config": config.get(MODE_BACKEND[mode], {}),
        "upgrade_sources": sources,
    }
