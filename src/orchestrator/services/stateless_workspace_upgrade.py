"""Owner-side rules for upgrading a stateless lite Session's workspace (C1).

Pre-admission checks run outside the row locks, in today's order
(spec "Who may ask, and when"); the database transition re-proves the row
facts under them (``orchestrator.database.stateless_workspace_upgrade``).
``resolve_upgrade_target`` decides which slice serves a request: C1 serves
container targets; a VM target goes through ``check_stateless_vm_upgrade`` and
the lane switch (``stateless_vm_upgrade_handoff``, C2).
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
    stateless_session_workspace_check,
)
from shared.backend_kinds import LITE_BACKENDS
from shared.run_queue import LANE_PINNED, LANE_STATELESS
from shared.workspace_upgrade import (
    UPGRADE_FAILED,
    UPGRADE_INTERRUPTED,
    VM_UPGRADE_SEED_PENDING,
    WORKSPACE_UPGRADE_FAILURE_CODES,
    WORKSPACE_UPGRADE_SEED_VERSION,
    metadata_object,
    vm_upgrade_seed,
    workspace_upgrade_seed_from,
)

UPGRADE_TARGET_CHANGED = (
    "The upgrade's target changed while it was being prepared. Try the upgrade again."
)
VM_UPGRADE_NOT_POSSIBLE = (
    "This Session can't move to a VM workspace in place. "
    "Start a new Session with a VM workspace."
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
    vm_provisioner: Any = None


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
        # A template edited to another tier after the route resolved it.
        raise HTTPException(409, UPGRADE_TARGET_CHANGED)
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
        # The route sends a VM target to the lane switch (C2) before calling
        # this; reaching here means the template changed tier in between.
        raise HTTPException(409, UPGRADE_TARGET_CHANGED)
    return await prepare_stateless_container_upgrade(
        store,
        thread,
        container_provisioner=dependencies.container_provisioner,
        enforce_workspace_upgrade_grants=dependencies.enforce_workspace_upgrade_grants,
        template_name=template_name,
    )


def is_root_session(thread: Mapping[str, Any]) -> bool:
    """A Session nobody else owns (``kind = 'session'`` has no parent)."""
    return str(thread.get("kind") or "") == "session"


async def check_stateless_vm_upgrade(
    store: Any,
    thread: Mapping[str, Any],
    *,
    template_name: str | None,
    vm_provisioner: Any,
    enforce_workspace_upgrade_grants: Callable[..., Awaitable[Any]],
) -> None:
    """Spec §1 pre-checks 2–7 for a VM target on the stateless lane (C2).

    Advisory, outside the row locks: the switch re-proves the row facts, and
    the pinned agent's VM request runs every check again under the advisory
    lock. VM provisioning off is a 409, never a 5xx (the cockpit retries 5xx).
    """
    from orchestrator.services.thread_config_update import (
        VM_PROVISIONING_UNAVAILABLE,
        _thread_project_id,
        require_unprotected_workspace_upgrade,
    )
    from orchestrator.services.workspace_defaults_resolution import upgrade_record
    from shared.workspace_defaults import MODE_RANK, UPGRADE_REFUSED, backend_mode

    backend, refusal = stateless_session_workspace_check(dict(thread))
    if (
        thread.get("execution_lane") != LANE_STATELESS
        or not is_root_session(thread)
        or refusal is not None
        or backend not in LITE_BACKENDS
    ):
        raise HTTPException(409, VM_UPGRADE_NOT_POSSIBLE)
    require_unprotected_workspace_upgrade(dict(thread))
    await enforce_workspace_upgrade_grants(thread, target_tier="vm")
    if vm_provisioner is None or not getattr(vm_provisioner, "is_available", False):
        raise HTTPException(409, VM_PROVISIONING_UNAVAILABLE)
    if MODE_RANK[backend_mode("vm")] <= MODE_RANK[backend_mode(backend)]:
        raise HTTPException(400, UPGRADE_REFUSED)
    mode, _record = await upgrade_record(
        store,
        thread.get("user_id"),
        role="session",
        project_id=_thread_project_id(dict(thread)),
        current_backend=backend,
        requested_backend="vm",
        template_name=template_name,
    )
    if mode != "vm":
        raise HTTPException(409, UPGRADE_TARGET_CHANGED)


def workspace_upgrade_targets(
    thread: Mapping[str, Any], *, container_provisioner: Any, vm_provisioner: Any = None
) -> list[str]:
    """What ``/connection`` may declare: targets structurally possible now.

    Grants stay a client-side hint and an admission check (the ``rewind``
    precedent). ``container`` needs startup tracking and an in-cluster
    provisioner (C1); ``vm`` needs the VM provisioner and a root Session (C2).
    """
    if thread.get("execution_lane") != LANE_STATELESS:
        return []
    if declared_thread_workspace_backend(thread) not in LITE_BACKENDS:
        return []
    targets: list[str] = []
    if startup_stage_activation_enabled() and container_provisioner_available(
        container_provisioner
    ):
        targets.append("container")
    if (
        vm_provisioner is not None
        and getattr(vm_provisioner, "is_available", False)
        and is_root_session(thread)
    ):
        targets.append("vm")
    return targets


def workspace_upgrade_container_ready(metadata: Any) -> bool:
    """Whether the upgrade's container has passed its Ready step.

    Ready writes the binding and endpoint an attach needs. Before it the
    Session read says ``provisioning`` and the claim bundle answers 425.
    """
    container = metadata_object(metadata).get("workspace_container")
    return isinstance(container, Mapping) and container.get("status") == "ready"


_LATEST_UPGRADE_REQUEST_SQL = """
SELECT id, outcome, error_code, payload
FROM thread_control_requests
WHERE thread_id = $1::uuid AND verb = 'upgrade-to-workspace'
  AND runtime_generation = $2::uuid
ORDER BY request_seq DESC
LIMIT 1
"""


async def latest_workspace_upgrade_request(
    store: Any, thread: Mapping[str, Any]
) -> dict[str, Any] | None:
    """The newest upgrade request of this runtime generation (C2's projection).

    Only a pinned row can hold a C2 request; 0185 re-stamps a pending one on
    Resume, so an End-then-Resume cancellation is still found.
    """
    if thread.get("execution_lane") != LANE_PINNED or not thread.get(
        "runtime_generation"
    ):
        return None
    row = await store.fetchrow(
        _LATEST_UPGRADE_REQUEST_SQL,
        str(thread["id"]),
        str(thread["runtime_generation"]),
    )
    return dict(row) if row is not None else None


def _vm_upgrade_view(
    thread: Mapping[str, Any],
    metadata: Mapping[str, Any],
    latest_request: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    vm = metadata.get("vm")
    status = vm.get("status") if isinstance(vm, Mapping) else None
    marker = vm_upgrade_seed(metadata)
    if (
        latest_request is not None
        and thread.get("execution_lane") == LANE_PINNED
        and latest_request.get("outcome") == "rejected"
        and latest_request.get("error_code") == UPGRADE_INTERRUPTED
    ):
        # The agent reports this on a VM attach, so the VM is already ready:
        # the receipt, not the working VM, is what a reload must show.
        return {
            "target_tier": "vm",
            "phase": "failed",
            "reason_code": UPGRADE_INTERRUPTED,
        }
    if marker is not None:
        # ``aborted`` is written only by the upgrade's abort route. A
        # ``seeded`` marker means the VM reached Ready, so a later provisioning
        # failure (the CAS keeps the marker) isn't the upgrade's.
        if status == "aborted" or (
            status == "failed" and marker["state"] == VM_UPGRADE_SEED_PENDING
        ):
            return {
                "target_tier": "vm",
                "phase": "failed",
                "reason_code": UPGRADE_FAILED,
            }
        if marker["state"] == VM_UPGRADE_SEED_PENDING:
            phase = "copying" if status == "ready" else "provisioning"
            return {"target_tier": "vm", "phase": phase}
    if status in ("ready", "suspended"):
        # A working VM: whatever an earlier request said is history.
        return None
    if latest_request is None or thread.get("execution_lane") != LANE_PINNED:
        return None
    if latest_request.get("outcome") is None:
        return {"target_tier": "vm", "phase": "requested"}
    code = latest_request.get("error_code")
    if (
        latest_request.get("outcome") == "rejected"
        and code in WORKSPACE_UPGRADE_FAILURE_CODES
    ):
        return {"target_tier": "vm", "phase": "failed", "reason_code": code}
    return None


def workspace_upgrade_view(
    thread: Mapping[str, Any],
    creation_view: Mapping[str, Any] | None,
    *,
    latest_request: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """The Session read's ``workspace_upgrade`` field (C1 container, C2 VM)."""
    metadata = metadata_object(thread.get("metadata"))
    if workspace_upgrade_seed_from(metadata) is None:
        return _vm_upgrade_view(thread, metadata, latest_request)
    # C1's container phases, unchanged.
    if creation_view is not None and creation_view.get("state") == "attention":
        return {
            "target_tier": "sandbox",
            "phase": "failed",
            "reason_code": creation_view.get("reason_code"),
        }
    ready = workspace_upgrade_container_ready(metadata)
    return {"target_tier": "sandbox", "phase": "copying" if ready else "provisioning"}


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
