"""Execution-owned infrastructure in the reference harness's delivery format.

Expert configuration is behavioral. Only execution/account/Project selections
may supply infrastructure; a private Expert backend is never a provisioning input.
Generic harness configuration does not pass through this adapter.
"""

import logging
from copy import deepcopy
from typing import Any

from shared.workspace_contract import normalize_workspace_backend

logger = logging.getLogger(__name__)

INFRASTRUCTURE_KEYS = frozenset({"backend", "vm", "sandbox"})
LEGACY_WORKSPACE_KEYS = frozenset({"container"})


def execution_workspace_config(*layers: dict | None, role: str = "worker") -> dict:
    """Select infrastructure from explicit execution/default layers in order."""
    result: dict[str, Any] = {"backend": "virtual" if role == "session" else "sandbox"}
    for layer in layers:
        workspace = (layer or {}).get("workspace")
        if not isinstance(workspace, dict):
            continue
        for key in INFRASTRUCTURE_KEYS:
            if key in workspace:
                result[key] = deepcopy(workspace[key])
    result["backend"] = normalize_workspace_backend(result["backend"])
    return result


def bind_execution_workspace(data: dict, workspace: dict) -> dict:
    """Stamp a delivery copy after private merges, before grants/tool resolution."""
    result = deepcopy(data)
    private = result.get("workspace")
    private = deepcopy(private) if isinstance(private, dict) else {}
    if any(key in private for key in LEGACY_WORKSPACE_KEYS):
        logger.warning(
            "Dropped legacy workspace.container settings; container image and "
            "resources come from the selected WorkspaceTemplate."
        )
    for key in INFRASTRUCTURE_KEYS | LEGACY_WORKSPACE_KEYS:
        private.pop(key, None)
    private.update(deepcopy(workspace))
    result["workspace"] = private
    return result


def migrate_expert_workspace_preference(document: dict) -> dict:
    """Versioned SRW-only migration; history and arbitrary image settings survive."""
    result = deepcopy(document)
    if result.get("kind") != "Expert":
        return result
    spec = result.get("spec", {})
    runtime = spec.get("runtime", {})
    if runtime.get("adapter") != "srw/v1":
        return result
    private = runtime.get("config", {})
    fragment = private.get("config", {})
    workspace = fragment.get("workspace")
    if isinstance(workspace, dict) and "backend" in workspace:
        backend = normalize_workspace_backend(workspace.pop("backend"))
        spec.setdefault("workspacePreference", {"backend": backend})
        if not workspace:
            fragment.pop("workspace")
    return result
