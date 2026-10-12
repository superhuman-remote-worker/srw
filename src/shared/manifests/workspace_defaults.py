"""Translate a resolved Project's defaults.workspace into row values (Slice A2b)."""

from __future__ import annotations

from copy import deepcopy

from shared.workspace_defaults import TEMPLATE_TIERS, backend_mode


def _mode(selection: dict) -> str:
    return backend_mode((selection.get("inline") or {}).get("backend", ""))


def project_workspace_defaults(spec: dict) -> dict | None:
    """Row values, or None when the Project doesn't set defaults.workspace.

    ``spec`` is resolved: ``resources.workspaces`` holds ``{"inline": {...}}``.
    The shorthand alias sets both modes to its tier and becomes that tier's
    template; ``null`` sets both modes to ``none``.
    """
    defaults = spec.get("defaults") or {}
    if "workspace" not in defaults:
        return None
    value = defaults["workspace"]
    workspaces = (spec.get("resources") or {}).get("workspaces") or {}
    result: dict = {"jobs": None, "sessions": None, "container": None, "vm": None}
    if value is None:
        return {**result, "jobs": "none", "sessions": "none"}
    if isinstance(value, str):
        selection = deepcopy(workspaces[value])
        mode = _mode(selection)
        result.update(jobs=mode, sessions=mode)
        if mode in TEMPLATE_TIERS:
            result[mode] = selection
        return result
    result.update(jobs=value.get("jobs"), sessions=value.get("sessions"))
    for tier in TEMPLATE_TIERS:
        if tier in value:
            selection = deepcopy(workspaces[value[tier]])
            if _mode(selection) != tier:
                raise ValueError(f"The {tier} template must be a {tier} workspace.")
            result[tier] = selection
    return result
