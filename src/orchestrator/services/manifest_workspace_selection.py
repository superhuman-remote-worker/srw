"""Workspace selections shared by the existing SRW Job and Session APIs."""

from copy import deepcopy
import re
from typing import Any

from fastapi import HTTPException

from orchestrator.services.manifest_authority import ManifestAuthority
from orchestrator.services.manifest_workspace_binding import (
    validate_workspace_selection,
)
from orchestrator.services.manifest_resolution import LiveManifestResolver
from orchestrator.services.manifest_store import ManifestStore
from shared.manifests.errors import ManifestError
from shared.runtime.core.workspace_selection import execution_workspace_config

_BUILD_YOUR_OWN_IMAGE = (
    "Build your own image FROM an SRW base image and name it in "
    "environment.image (see examples/manifests/container-workspace-templates.md)."
)
_IMAGE_REFERENCE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/@-]*")


def _sandbox_workspace_config(recipe: dict) -> dict:
    """Render a container template; SRW never builds or initializes images."""
    environment = recipe.get("environment", {})
    if environment.get("prepare") or environment.get("cache", "Reuse") != "Reuse":
        raise HTTPException(
            422, "Container templates can't run prepare steps. " + _BUILD_YOUR_OWN_IMAGE
        )
    if "initialize" in recipe:
        raise HTTPException(
            422,
            "Container templates don't support initialize; bake setup into the "
            "image. " + _BUILD_YOUR_OWN_IMAGE,
        )
    sandbox: dict = {}
    if "image" in environment:
        image = environment["image"]
        if _IMAGE_REFERENCE.fullmatch(image) is None:
            raise HTTPException(
                422, "Container image must be a registry image reference."
            )
        sandbox["image"] = image
        if "pullPolicy" in environment:
            sandbox["pull_policy"] = environment["pullPolicy"]
    resources = recipe.get("resources", {})
    for field in ("cpu", "memory", "storage"):
        if field in resources:
            sandbox[field] = resources[field]
    return {"sandbox": sandbox} if sandbox else {}


def srw_workspace_config(
    workspace: dict | None, *, instance_recipe: dict | None = None
) -> dict:
    """Render supported workspace recipes; never silently discard recipe fields."""
    if workspace is None:
        return {"backend": "none"}
    try:
        validate_workspace_selection(workspace)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    recipe = (
        instance_recipe
        if "instanceRef" in workspace
        else workspace.get("template", {}).get("inline")
    )
    if (
        not isinstance(recipe, dict)
        or set(recipe)
        - {"backend", "retention", "resources", "environment", "initialize"}
        or (
            recipe.get("retention", "Delete") != "Delete"
            and recipe.get("backend") != "vm"
        )
    ):
        raise HTTPException(
            422,
            "The SRW workspace provisioner supports backend-only templates and "
            "prebuilt VM images/resources and initialization. Retained VM references "
            "must be resolved by the workspace admission service.",
        )
    result = {"backend": recipe["backend"]}
    if not set(recipe) & {"resources", "environment", "initialize"}:
        return result
    if recipe["backend"] == "sandbox":
        return {**result, **_sandbox_workspace_config(recipe)}
    if recipe["backend"] != "vm":
        raise HTTPException(
            422,
            "SRW template images and resources require backend sandbox or vm; "
            "initialization requires backend vm.",
        )
    environment = recipe.get("environment", {})
    vm = {}
    if (
        "prepare" in environment
        or environment.get("pullPolicy", "IfNotPresent") != "IfNotPresent"
        or environment.get("cache", "Reuse") != "Reuse"
    ):
        from orchestrator.services.vm_preparation import validate_environment

        if instance_recipe is None:
            settings = validate_environment(environment, recipe.get("resources", {}))
            vm["preparation"] = deepcopy(environment)
            vm["disk_size"] = settings.disk_size
    if "initialize" in recipe:
        from shared.workspace_initialization import initialization_request

        try:
            initialization = initialization_request(recipe["initialize"])
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        if initialization["steps"]:
            vm["initialization"] = initialization
    if "image" in environment:
        image = environment["image"]
        # The VM controller embeds this registry reference in its disk manifest.
        # Accept registry paths/tags/digests, never whitespace or YAML syntax.
        if _IMAGE_REFERENCE.fullmatch(image) is None:
            raise HTTPException(422, "VM image must be a registry image reference.")
        vm["image"] = image
    resources = recipe.get("resources", {})
    if "cpu" in resources:
        cpu = resources["cpu"]
        if int(cpu) != cpu:
            raise HTTPException(
                422, "VM templates require a whole number of CPU cores."
            )
        vm["cpu_cores"] = int(cpu)
    for field, target in (("memory", "memory"), ("storage", "disk_size")):
        if field in resources:
            vm[target] = resources[field]
    if vm:
        result["vm"] = vm
    return result


async def select_execution_workspace(
    db: Any,
    user: dict,
    *,
    project_id: str | None,
    role: str,
    workspace: dict | None = None,
    supplied: bool = False,
    config_override: dict | None = None,
    account_defaults: dict | None = None,
    request: Any = None,
) -> tuple[dict, dict | None]:
    """Explicit selection > Project default > account/role fallback.

    The returned receipt carries frozen workspace and Project revisions into
    the insertion transaction. A recommendation never enters this function.
    Legacy config_override.workspace remains an explicit execution input.
    """
    from orchestrator.services.manifest_projects import (
        active_project_resource,
        source_recipe,
    )

    fallback = execution_workspace_config(account_defaults, config_override, role=role)
    legacy = (config_override or {}).get("workspace") or {}
    if supplied and "backend" in legacy:
        raise HTTPException(
            422,
            "Select workspace once; do not also set config_override.workspace.backend.",
        )
    if not supplied and "backend" in legacy:
        return fallback, None
    authority = ManifestAuthority(db, user, request=request)
    resolver = LiveManifestResolver(ManifestStore(db), authority)
    scope = {"kind": "Project", "name": project_id} if project_id else authority.account
    dependencies: list[dict] = []
    project_revision = None
    if not supplied and project_id:
        project = await active_project_resource(db, project_id)
        if project:
            defaults = project["resolved"]["spec"].get("defaults", {})
            if "workspace" in defaults:
                alias = defaults["workspace"]
                workspace = (
                    {
                        "template": deepcopy(
                            project["resolved"]["spec"]["resources"]["workspaces"][
                                alias
                            ]
                        )
                    }
                    if alias is not None
                    else None
                )
            else:
                # A versioned legacy Project source is already frozen. Its old
                # workspace default belongs to the Project, not to its Experts.
                shared = (source_recipe(project) or {}).get("sharedConfig", {})
                backend = (shared.get("workspace") or {}).get("backend")
                if backend is None:
                    return fallback, None
                workspace = (
                    None
                    if backend == "none"
                    else {"template": {"inline": {"backend": backend}}}
                )
            await authority.resource(project)
            await resolver.authorize_dependencies(project["dependencies"])
            dependencies = deepcopy(project["dependencies"])
            dependencies.append(
                {"uid": str(project["id"]), "revision": project["revision"]}
            )
            project_revision = project["revision"]
            supplied = True
    if not supplied:
        return fallback, None
    # A caller's malformed binding or unsupported template is a client error on
    # every admission route and form preview, never an internal one.
    try:
        validate_workspace_selection(workspace)
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from None
    resolved = deepcopy(workspace)
    if resolved and "template" in resolved:
        try:
            resolved["template"] = await resolver.selection(
                "WorkspaceTemplate", resolved["template"], scope, dependencies
            )
        except ManifestError as exc:
            raise HTTPException(422, str(exc)) from None
    instance_recipe = None
    instance_generation = None
    if resolved and "instanceRef" in resolved:
        from orchestrator.services.retained_vm_workspaces import read_instance

        row = await read_instance(
            db,
            resolved["instanceRef"]["uid"],
            user,
            project_id=project_id,
            request=request,
        )
        instance_recipe, instance_generation = row["recipe"], row["generation"]
    if (
        instance_recipe is not None
        or (resolved or {}).get("template", {}).get("inline", {}).get("retention")
        == "Retain"
    ):
        from orchestrator.services.retained_vm_workspaces import (
            require_retained_vm_hosting,
        )

        require_retained_vm_hosting()
    config = srw_workspace_config(resolved, instance_recipe=instance_recipe)
    return config, {
        **(
            {
                "instance_recipe": instance_recipe,
                "instance_generation": instance_generation,
                "instance_project_id": project_id,
            }
            if instance_recipe is not None
            else {}
        ),
        "document": deepcopy(workspace),
        "resolved": resolved,
        "dependencies": dependencies,
        "project_id": project_id if project_revision else None,
        "project_revision": project_revision,
    }


async def verify_workspace_selection(
    db: Any, selection: dict, owner_id: str | None
) -> None:
    """Recheck current access and the active Project at the atomic write boundary."""
    from orchestrator.services.manifest_projects import active_project_resource

    if selection.get("project_revision"):
        project = await active_project_resource(db, selection["project_id"])
        if project is None or project["revision"] != selection["project_revision"]:
            raise HTTPException(
                409, "The Project changed during workspace selection; submit again."
            )
    if selection.get("instance_recipe") is not None:
        from orchestrator.services.retained_vm_workspaces import read_instance

        user = await db.get_user(owner_id) if owner_id else None
        if not user:
            raise HTTPException(409, "Workspace selection owner is unavailable.")
        uid = selection["resolved"]["instanceRef"]["uid"]
        current = await read_instance(
            db, uid, user, project_id=selection.get("instance_project_id")
        )
        if (
            current["generation"] != selection["instance_generation"]
            or current["recipe"] != selection["instance_recipe"]
        ):
            raise HTTPException(409, "Workspace instance changed during selection.")
    if selection.get("dependencies"):
        user = await db.get_user(owner_id) if owner_id else None
        if not user:
            raise HTTPException(409, "The workspace selection owner is unavailable.")
        resolver = LiveManifestResolver(ManifestStore(db), ManifestAuthority(db, user))
        await resolver.authorize_dependencies(selection["dependencies"])


async def select_project_workspace_default(db, owner_id, project_id, config_override):
    """Unattended callers choose the workspace before selecting connectors/grants."""
    if (
        not owner_id
        or not project_id
        or "backend" in ((config_override or {}).get("workspace") or {})
    ):
        return config_override, None
    project = await db.get_project(str(project_id))
    if not project or project.get("manifest_composed") is not True:
        return config_override, None
    user = await db.get_user(str(owner_id))
    if not user:
        raise HTTPException(409, "The execution owner is unavailable.")
    config, selection = await select_execution_workspace(
        db,
        user,
        project_id=str(project_id),
        role="worker",
        config_override=config_override,
    )
    if selection is None:
        return config_override, None
    from shared.runtime.core.workspace_selection import bind_execution_workspace

    return bind_execution_workspace(config_override or {}, config), selection
