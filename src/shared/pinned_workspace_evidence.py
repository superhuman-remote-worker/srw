"""Physical workspace evidence shared by pinned retirement Begin and recovery."""

from collections.abc import Mapping
from typing import Any


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
