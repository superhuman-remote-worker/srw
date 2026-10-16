"""Owner-side rules for upgrading a stateless lite Session's workspace (C1).

Pre-admission checks run outside the row locks, in today's order
(spec "Who may ask, and when"); the database transition re-proves the row
facts under them (``orchestrator.database.stateless_workspace_upgrade``).
``resolve_upgrade_target`` decides which slice serves a request: C1 serves
container targets; a VM target keeps a clear 409 until C2 replaces that branch
with its lane-switch admission.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

from fastapi import HTTPException

from orchestrator.services.container_startup_config import (
    startup_stage_activation_enabled,
)
from orchestrator.services.stateless_workspace_gate import (
    declared_thread_workspace_backend,
)
from shared.backend_kinds import LITE_BACKENDS
from shared.run_queue import LANE_STATELESS
from shared.workspace_upgrade import (
    WORKSPACE_UPGRADE_SEED_VERSION,
    metadata_object,
    workspace_upgrade_seed_from,
)

STATELESS_VM_UPGRADE_UNAVAILABLE = (
    "Upgrading a Session to a VM isn't available on this installation yet. "
    "Start a new Session with a VM workspace instead."
)
STARTUP_TRACKING_REQUIRED = (
    "Upgrading this Session needs workspace startup tracking, which this "
    "installation doesn't have turned on."
)
CONTAINER_WORKSPACES_UNAVAILABLE = (
    "Container workspaces aren't available on this installation."
)
PINNED_TRANSPORT_REFUSAL = (
    "Workspace upgrades use the live session transport on the pinned execution lane"
)
# The offer fix's agent-side copy for the in-process container refusal.
PINNED_CONTAINER_UPGRADE_UNAVAILABLE = (
    "Container upgrades aren't available in this session. Use "
    "/upgrade-workspace vm, or start a new Session with a container workspace."
)
WORKSPACE_UPGRADE_UPLOAD_REFUSED = (
    "This Session's workspace is being upgraded. Try the upload again when the "
    "upgrade finishes."
)


@dataclass(frozen=True, slots=True)
class StatelessWorkspaceUpgradeDependencies:
    """Collaborators the owner control route needs for an upgrade request."""

    container_provisioner: Any
    enforce_workspace_upgrade_grants: Callable[..., Awaitable[Any]]
    schedule_stateless_workspace_ensure: Callable[[str], Any]


def container_provisioner_available(container_provisioner: Any) -> bool:
    """Whether in-cluster container workspaces can be provisioned right now."""
    return bool(
        container_provisioner is not None
        and getattr(container_provisioner, "is_available", False)
        and getattr(container_provisioner, "in_cluster", False)
    )


async def resolve_upgrade_target(
    store: Any,
    thread: Mapping[str, Any],
    *,
    target_tier: str | None,
    template_name: str | None,
) -> Literal["container", "vm"]:
    """Which tier a request asks for; a named template's own tier decides."""
    if target_tier in ("sandbox", "container"):
        return "container"
    if target_tier == "vm":
        return "vm"
    if template_name:
        from orchestrator.services.thread_config_update import _thread_project_id
        from orchestrator.services.workspace_defaults_resolution import (
            find_readable_template,
            work_owner,
        )

        owner = await work_owner(store, thread.get("user_id"))
        _, mode = await find_readable_template(
            store,
            owner,
            project_id=_thread_project_id(dict(thread)),
            name=template_name,
        )
        return "vm" if mode == "vm" else "container"
    # Bare /upgrade-workspace on a stateless lite Session: the next tier.
    return "container"


async def prepare_stateless_container_upgrade(
    store: Any,
    thread: Mapping[str, Any],
    *,
    container_provisioner: Any,
    enforce_workspace_upgrade_grants: Callable[..., Awaitable[Any]],
    template_name: str | None,
) -> dict[str, Any]:
    """Today's helpers, in today's order; returns the upgrade record."""
    from orchestrator.services.thread_config_update import (
        _thread_project_id,
        require_unprotected_workspace_upgrade,
    )
    from orchestrator.services.workspace_defaults_resolution import upgrade_record
    from shared.workspace_defaults import UPGRADE_REFUSED

    if not startup_stage_activation_enabled():
        raise HTTPException(409, STARTUP_TRACKING_REQUIRED)
    require_unprotected_workspace_upgrade(dict(thread))
    await enforce_workspace_upgrade_grants(thread, target_tier="sandbox")
    if not container_provisioner_available(container_provisioner):
        raise HTTPException(409, CONTAINER_WORKSPACES_UNAVAILABLE)
    current = declared_thread_workspace_backend(thread)
    if current not in LITE_BACKENDS:
        raise HTTPException(400, UPGRADE_REFUSED)
    mode, record = await upgrade_record(
        store,
        thread.get("user_id"),
        role="session",
        project_id=_thread_project_id(dict(thread)),
        current_backend=current,
        requested_backend="sandbox",
        template_name=template_name,
    )
    if mode != "container":
        raise HTTPException(409, STATELESS_VM_UPGRADE_UNAVAILABLE)
    return {
        "upgrade_config": dict(record.get("upgrade_config") or {}),
        "upgrade_sources": record.get("upgrade_sources"),
    }


async def prepare_stateless_workspace_upgrade(
    store: Any,
    thread: Mapping[str, Any],
    *,
    target_tier: str | None,
    template_name: str | None,
    dependencies: StatelessWorkspaceUpgradeDependencies,
) -> dict[str, Any]:
    """The REST control's pre-admission: lane, target, then the slice's checks."""
    if thread.get("execution_lane") != LANE_STATELESS:
        raise HTTPException(409, PINNED_TRANSPORT_REFUSAL)
    target = await resolve_upgrade_target(
        store, thread, target_tier=target_tier, template_name=template_name
    )
    if target == "vm":
        # C2 replaces this branch with its lane-switch admission.
        raise HTTPException(409, STATELESS_VM_UPGRADE_UNAVAILABLE)
    return await prepare_stateless_container_upgrade(
        store,
        thread,
        container_provisioner=dependencies.container_provisioner,
        enforce_workspace_upgrade_grants=dependencies.enforce_workspace_upgrade_grants,
        template_name=template_name,
    )


def workspace_upgrade_targets(
    thread: Mapping[str, Any], *, container_provisioner: Any
) -> list[str]:
    """What ``/connection`` may declare: targets structurally possible now.

    Grants stay a client-side hint and an admission check (the ``rewind``
    precedent); C2 appends ``"vm"``.
    """
    if thread.get("execution_lane") != LANE_STATELESS:
        return []
    if declared_thread_workspace_backend(thread) not in LITE_BACKENDS:
        return []
    if not startup_stage_activation_enabled():
        return []
    if not container_provisioner_available(container_provisioner):
        return []
    return ["container"]


def workspace_upgrade_view(
    thread: Mapping[str, Any], creation_view: Mapping[str, Any] | None
) -> dict[str, Any] | None:
    """The Session read's ``workspace_upgrade`` field (shared with C2)."""
    metadata = metadata_object(thread.get("metadata"))
    if workspace_upgrade_seed_from(metadata) is None:
        return None
    if creation_view is not None and creation_view.get("state") == "attention":
        return {
            "target_tier": "sandbox",
            "phase": "failed",
            "reason_code": creation_view.get("reason_code"),
        }
    container = metadata.get("workspace_container") or {}
    phase = "copying" if container.get("status") == "ready" else "provisioning"
    return {"target_tier": "sandbox", "phase": phase}


def workspace_upgrade_seed_spec(
    thread_id: str, seed_from: Mapping[str, Any]
) -> dict[str, Any]:
    """The old lite workspace a claimant copies from, credentials in-flight only.

    Lite Sessions already receive these mounts at every claim
    (``session_attach_payload.py`` ~:148), so exposure doesn't grow.

    A ``virtual`` source bound to another object store is refused: mounting
    today's store would copy nothing and clear the marker as if it had.
    """
    from orchestrator.services import virtual_workspace
    from orchestrator.services.workspace_binding import virtual_thread_backing_id
    from orchestrator.services.workspace_tier_policy import (
        inject_lite_workspace_config,
    )

    backend = seed_from.get("backend")
    if backend == "virtual":
        binding = seed_from.get("binding")
        store = virtual_workspace.virtual_workspace_rclone_spec()
        if (
            isinstance(binding, Mapping)
            and store is not None
            and binding.get("backing_id") != virtual_thread_backing_id(thread_id, store)
        ):
            raise ValueError("the old workspace is bound to another object store")
        override = inject_lite_workspace_config(
            {"workspace": {"backend": "virtual"}}, prefix=f"threads/{thread_id}/"
        )
        workspace = dict((override or {})["workspace"])
    elif backend == "none":
        workspace = {"backend": "none"}
    else:
        raise ValueError("unsupported workspace upgrade source")
    return {
        "version": WORKSPACE_UPGRADE_SEED_VERSION,
        "seed_from": dict(seed_from),
        "workspace": workspace,
    }
