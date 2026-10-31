"""Physical workspace evidence shared by pinned retirement Begin and recovery."""

import re
from collections.abc import Mapping
from typing import Any
from uuid import UUID


_PHYSICAL_WORKSPACE_FIELDS = (
    "_runtime_incarnation",
    "pod_ip",
    "pod_name",
    "host",
    "port",
    "ide_host",
    "ide_port",
    "_canvas_workspace_generation",
    "_docker_workspace_lease_id",
)


def has_pinned_physical_workspace_evidence(
    workspace: Mapping[str, Any], *, provision_intent_pending: bool = False
) -> bool:
    """Match Begin's JSONB status/external-field test; repository metadata is inert."""

    status = workspace.get("status")
    status_absent = status is None or (
        isinstance(status, str) and status in {"", "deleted"}
    )
    return bool(
        (not provision_intent_pending and not status_absent)
        or any(
            field in workspace and workspace[field] is not None
            for field in _PHYSICAL_WORKSPACE_FIELDS
        )
    )


_VIRTUAL_BINDING_FIELDS = frozenset(
    {"generation", "kind", "backing_id", "ssh_host_key_fingerprint"}
)


def canonical_virtual_binding(binding: Any) -> bool:
    """Whether ``binding`` is exactly the one the virtual tier binds.

    ``ensure_virtual_thread_workspace_binding`` writes it: an object-store
    prefix, with no process, Pod or disk behind it. SQL
    ``pinned_canonical_virtual_binding`` (0556) is the same test.
    """

    if not isinstance(binding, Mapping) or set(binding) != _VIRTUAL_BINDING_FIELDS:
        return False
    generation = binding.get("generation")
    backing_id = binding.get("backing_id")
    try:
        canonical_generation = (
            isinstance(generation, str) and str(UUID(generation)) == generation
        )
    except ValueError:
        canonical_generation = False
    return bool(
        canonical_generation
        and binding.get("kind") == "virtual"
        and isinstance(backing_id, str)
        and re.fullmatch(r"rclone:[0-9a-f]{64}", backing_id) is not None
        and binding.get("ssh_host_key_fingerprint") is None
    )


_PROVISION_MARKER_FIELDS = (
    "_workspace_provision_attempt",
    "_workspace_provision_generation",
)


def never_published_workspace_create(
    workspace: Mapping[str, Any],
    intent: Any,
    *,
    runtime_generation: str,
) -> bool:
    """Whether a first K8s workspace create never handed out its address.

    The pending marker names the captured attempt, no physical field was ever
    published and the create had no prior binding to inherit an overlay from.
    """

    if not isinstance(intent, Mapping) or not intent.get("attempt_id"):
        return False
    return bool(
        workspace.get("status") == "pending"
        and workspace.get("provisioner") == "k8s"
        and workspace.get("_workspace_provision_attempt")
        == str(intent.get("attempt_id"))
        and workspace.get("_workspace_provision_generation") == runtime_generation
        and str(intent.get("runtime_generation") or "") == runtime_generation
        and intent.get("previous_binding") == {}
        and not has_pinned_physical_workspace_evidence(
            workspace, provision_intent_pending=True
        )
    )


def soft_fenced_workspace(workspace: Mapping[str, Any]) -> dict[str, Any]:
    """The metadata a soft provision fence leaves in place of a pending create."""

    cleared = {
        key: value
        for key, value in workspace.items()
        if key not in _PROVISION_MARKER_FIELDS
    }
    cleared.update(
        {
            "status": "deleted",
            **{
                field: None
                for field in _PHYSICAL_WORKSPACE_FIELDS
                if field != "_docker_workspace_lease_id"
            },
        }
    )
    return cleared
