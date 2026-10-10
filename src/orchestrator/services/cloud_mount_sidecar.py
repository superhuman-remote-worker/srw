"""Reading back a workspace Pod's recorded cloud mount plan (D7).

:mod:`cloud_mount_plan` resolves a session Pod's cloud mounts when the Pod
is created and annotates the Pod with the non-secret half (its "recorded
plan"); the provisioner copies it into the thread's ``workspace_container``
tied to the Pod's UID. Everything after creation reads it from there: the
attach payload, End's drain, and the mount state the agent reports. This
module holds only that reading side, so the cloud payload builders can use
it without importing the planner.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from orchestrator.services.in_pod_mount import (
    CONTROL_DIR,
    STATUS_DIR,
    WORKSPACE_CLOUD_ROOT,
)

PLAN_VERSION = 1
#: The Pod annotation holding the recorded plan.
PLAN_ANNOTATION = "srw.io/cloud-mount-plan"
#: The recorded plan's key in a thread's ``workspace_container``.
PLAN_CONTEXT_KEY = "cloud_mount_plan"
#: How long the agent waits at attach for each mount to settle.
ATTACH_WAIT_SECONDS = 30
#: The header an agent that attaches to sidecar mounts sends with its
#: workspace poll. An older agent does not, and gets no cloud payload it
#: would misread.
DELIVERY_HEADER = "X-SRW-Cloud-Mount-Delivery"

#: Every reason a mount can be unavailable or left out. Nothing else is ever
#: recorded, so no remote's words reach a thread row, a prompt or the cockpit.
UNAVAILABLE_REASONS = frozenset(
    {
        # The supervisor's, from the status file.
        "credential_rejected",
        "not_found",
        "unreachable",
        "timeout",
        "mount_failed",
        "config_missing",
        # The agent's: no status file appeared in time.
        "sidecar_unavailable",
        # The orchestrator's, at Pod creation.
        "unbuildable",
        "set_fallback",
        "too_many_mounts",
        "protected_unavailable",
    }
)


def plan_fingerprint(body: Mapping[str, Any]) -> str:
    """The sha256 of a recorded plan's canonical JSON, fingerprint left out."""
    return hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _intact(plan: Any) -> bool:
    if not isinstance(plan, Mapping) or plan.get("version") != PLAN_VERSION:
        return False
    if plan.get("delivery") != "sidecar":
        return False
    body = {
        key: value
        for key, value in plan.items()
        if key not in {"fingerprint", "runtime_incarnation"}
    }
    return plan.get("fingerprint") == plan_fingerprint(body)


def recorded_plan_from_annotations(annotations: Any) -> dict[str, Any] | None:
    """A Pod's recorded plan, or ``None`` when it has none or it is not intact.

    The fingerprint is checked, so a plan that changed after it was written
    is never trusted as the Pod's own.
    """
    if not isinstance(annotations, Mapping):
        return None
    raw = annotations.get(PLAN_ANNOTATION)
    if not isinstance(raw, str) or not raw:
        return None
    try:
        plan = json.loads(raw)
    except ValueError:
        return None
    return dict(plan) if _intact(plan) else None


def recorded_sidecar_plan(metadata: Mapping[str, Any]) -> dict[str, Any] | None:
    """The current workspace Pod's recorded plan, from thread metadata.

    ``None`` unless the plan is intact and belongs to the Pod the thread's
    workspace now runs on (its UID): a Pod created on the in-workspace path
    clears it, and a later Pod never inherits an earlier one's.
    """
    workspace = metadata.get("workspace_container")
    if not isinstance(workspace, Mapping):
        return None
    plan = workspace.get(PLAN_CONTEXT_KEY)
    incarnation = workspace.get("_runtime_incarnation")
    if (
        not _intact(plan)
        or not incarnation
        or plan.get("runtime_incarnation") != incarnation
    ):
        return None
    return dict(plan)


def agent_payload(recorded: Mapping[str, Any]) -> dict[str, Any]:
    """The ``cloud_mount_sidecar`` an agent attaches with: no credential and
    no remote, only where each mount is and how to learn its state."""
    overlay = recorded.get("overlay")
    return {
        "version": PLAN_VERSION,
        "delivery": "sidecar",
        "fingerprint": recorded.get("fingerprint"),
        "cloud_root": WORKSPACE_CLOUD_ROOT,
        "workspace_entry": "cloud",
        "status_dir": STATUS_DIR,
        "control_dir": CONTROL_DIR,
        "wait_seconds": ATTACH_WAIT_SECONDS,
        "drain_seconds": int(recorded.get("drain_seconds") or 0),
        "protected": bool(recorded.get("protected")),
        "skip_workspace_links": bool(recorded.get("protected")),
        "overlay": dict(overlay) if isinstance(overlay, Mapping) else None,
        "mounts": [
            {
                "index": mount.get("index"),
                "mount_id": mount.get("mount_id"),
                "mount_kind": mount.get("mount_kind"),
                "target_path": mount.get("target_path"),
                "workspace_name": mount.get("name"),
                "access": mount.get("access"),
            }
            for mount in recorded.get("mounts") or []
        ],
        "excluded": [dict(entry) for entry in recorded.get("excluded") or []],
    }


__all__ = [
    "ATTACH_WAIT_SECONDS",
    "DELIVERY_HEADER",
    "PLAN_ANNOTATION",
    "PLAN_CONTEXT_KEY",
    "UNAVAILABLE_REASONS",
    "agent_payload",
    "plan_fingerprint",
    "recorded_plan_from_annotations",
    "recorded_sidecar_plan",
]
