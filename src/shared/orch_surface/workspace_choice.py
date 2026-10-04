"""The `workspace` argument of the job and session creation tools (Slice A3).

REST takes a manifest WorkspaceBinding in `workspace`: `{"template": {"ref": ...}}`,
`{"template": {"inline": ...}}` or `null`, and omitting the field runs the defaults
chain. Tool callers are language models, so this module accepts the short forms and
deliberately refuses inline recipes. An inline recipe names an arbitrary image, which
is the kind of model-authored path to infrastructure that `create_persistent_thread`
keeps closed (tests/test_tool_override_boundary.py). Recipes stay available through
`manifest_apply`.

A bare name is looked up the way `/upgrade-workspace` looks it up: the work's Project,
then the caller's Account, then Catalog `shared`, and the first scope that holds the
name wins (workspace_defaults_resolution.find_readable_template). The server doesn't
search like this for a ref without a scope (it tries only the Project or the Account),
so the binding sent always carries the scope the template was found in.
"""

from __future__ import annotations

from typing import Any, Protocol

import httpx

NO_WORKSPACE = "none"
SHAPE_HINT = (
    'workspace takes a template name, "none", or {"template": {"ref": {"name": ..., '
    '"scope": {...}}}}. Inline recipes aren\'t accepted here: save the recipe with '
    "manifest_apply and pass its name."
)
UNKNOWN_TEMPLATE = (
    "No workspace template named '{name}' is visible here; list them with "
    "manifest_list kind=WorkspaceTemplate."
)


class WorkspaceArgumentError(ValueError):
    """The caller's `workspace` argument can't become a binding."""


class _ResourceLister(Protocol):
    async def list_manifest_resources(
        self, *, scope_kind: str = ..., scope_name: str = ..., kind: str | None = ...
    ) -> dict[str, Any]: ...


async def workspace_field(
    client: _ResourceLister,
    workspace: str | dict[str, Any] | None,
    *,
    project_id: str | None,
) -> tuple[bool, dict[str, Any] | None]:
    """Return `(supplied, binding)`; `supplied=False` means omit the REST field."""
    if workspace is None:
        return False, None
    if isinstance(workspace, str):
        name = workspace.strip()
        if not name:
            raise WorkspaceArgumentError(SHAPE_HINT)
        if name == NO_WORKSPACE:
            return True, None
        return True, await _lookup(client, name, project_id=project_id)
    if isinstance(workspace, dict) and set(workspace) == {"template"}:
        template = workspace["template"]
        if (
            isinstance(template, dict)
            and set(template) == {"ref"}
            and isinstance(template["ref"], dict)
        ):
            return True, {"template": {"ref": dict(template["ref"])}}
    raise WorkspaceArgumentError(SHAPE_HINT)


async def _lookup(
    client: _ResourceLister, name: str, *, project_id: str | None
) -> dict[str, Any]:
    scopes: list[dict[str, str]] = []
    if project_id:
        scopes.append({"kind": "Project", "name": str(project_id)})
    scopes += [{"kind": "Account", "name": "me"}, {"kind": "Catalog", "name": "shared"}]
    for scope in scopes:
        try:
            listing = await client.list_manifest_resources(
                scope_kind=scope["kind"],
                scope_name=scope["name"],
                kind="WorkspaceTemplate",
            )
        except httpx.HTTPStatusError as error:
            if error.response.status_code in (403, 404):
                continue
            raise
        for item in listing.get("resources", []):
            metadata = (item.get("resource") or {}).get("metadata") or {}
            if metadata.get("name") == name:
                stored_scope = metadata.get("scope") or scope
                return {
                    "template": {"ref": {"name": name, "scope": dict(stored_scope)}}
                }
    raise WorkspaceArgumentError(UNKNOWN_TEMPLATE.format(name=name))
