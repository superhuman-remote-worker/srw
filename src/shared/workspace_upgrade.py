"""Names both sides of the stateless workspace upgrade share (C1 container, C2 VM).

The orchestrator writes ``workspace_container.seed_from`` in the transition and
ships the old lite workspace beside ``attach`` in the claim bundle; the
claimant declares that it can seed one, copies the files, then clears the key.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any
from uuid import UUID

WORKSPACE_UPGRADE_VERB = "upgrade-to-workspace"
# The canonical control payload: ``{target_tier?, template?, runtime_generation}``.
WORKSPACE_UPGRADE_PAYLOAD_KEYS = frozenset(
    {"target_tier", "template", "runtime_generation"}
)
SEED_FROM_KEY = "seed_from"
WORKSPACE_UPGRADE_SEED_KEY = "workspace_upgrade_seed"
WORKSPACE_UPGRADE_SEED_CAPABILITY = "workspace_upgrade_seed.v1"
WORKSPACE_UPGRADE_SEED_VERSION = 1


def metadata_object(value: Any) -> dict[str, Any]:
    """A thread's metadata as a dict; JSON text and junk degrade to ``{}``."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return {}
    return dict(value) if isinstance(value, Mapping) else {}


def workspace_upgrade_seed_from(metadata: Any) -> dict[str, Any] | None:
    """The pending upgrade's copy source, or None when no copy is owed."""
    container = metadata_object(metadata).get("workspace_container")
    if not isinstance(container, Mapping):
        return None
    seed = container.get(SEED_FROM_KEY)
    return dict(seed) if isinstance(seed, Mapping) else None


#: C2: the journal frame the stateless → pinned lane switch writes. It carries
#: the request id as data but no ``control_request_id`` link: a linked frame
#: would count as the request's receipt and stop the pinned adoption.
LANE_CHANGED_FRAME = "session.lane_changed"
#: C2 (D4 (a)): the durable seed marker inside ``metadata.vm``.
VM_UPGRADE_SEED_KEY = "upgrade_seed"
VM_UPGRADE_SEED_PENDING = "pending"
VM_UPGRADE_SEED_SEEDED = "seeded"
#: The copy's source is permanently gone: nothing is owed any more, and the
#: upgrade reads as ``upgrade_interrupted`` (the files stay in the old storage).
VM_UPGRADE_SEED_INTERRUPTED = "interrupted"
_VM_UPGRADE_SEED_STATES = frozenset(
    {VM_UPGRADE_SEED_PENDING, VM_UPGRADE_SEED_SEEDED, VM_UPGRADE_SEED_INTERRUPTED}
)
#: A typed 5xx of the VM request (``agent_upgrade_thread_to_vm``): VM
#: provisioning is off, so nothing was recorded. Any 5xx but the two typed
#: ones can follow a committed provision CAS, so the pinned agent reads it as
#: "no answer" (C2).
VM_PROVISIONING_NOT_AVAILABLE = "VM provisioning not available (no NATS or K8s)"
#: C2's other typed 503: an unexpected error stopped the lane switch's VM
#: request before anything was recorded for it (``vm_upgrade_recorded``), so
#: a retry would likely fail the same way. The pinned agent reads it as a
#: refusal; the text becomes the ``workspace_upgrade.failed`` reason.
VM_UPGRADE_NOT_RECORDED = "SRW couldn't start the VM upgrade. Try again."
#: C2: typed ``workspace_upgrade.failed`` codes (receipts and live frames).
UPGRADE_CANCELLED = "upgrade_cancelled"
UPGRADE_REFUSED = "upgrade_refused"
UPGRADE_FAILED = "upgrade_failed"
UPGRADE_INTERRUPTED = "upgrade_interrupted"
UPGRADE_ALREADY_RUNNING = "upgrade_already_running"
WORKSPACE_UPGRADE_FAILURE_CODES = frozenset(
    {
        UPGRADE_CANCELLED,
        UPGRADE_REFUSED,
        UPGRADE_FAILED,
        UPGRADE_INTERRUPTED,
        UPGRADE_ALREADY_RUNNING,
    }
)


def _canonical_uuid(value: Any) -> str | None:
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        return None


def vm_upgrade_seed(metadata: Any) -> dict[str, Any] | None:
    """A well-formed C2 seed marker from ``metadata.vm``, else None.

    ``source`` is C1's ``seed_from`` shape for a ``virtual`` Session and
    ``None`` for a ``none`` Session (nothing durable to copy).
    """
    vm = metadata_object(metadata).get("vm")
    if not isinstance(vm, Mapping):
        return None
    marker = vm.get(VM_UPGRADE_SEED_KEY)
    if not isinstance(marker, Mapping):
        return None
    state = marker.get("state")
    request_id = _canonical_uuid(marker.get("control_request_id"))
    source = marker.get("source")
    if state not in _VM_UPGRADE_SEED_STATES:
        return None
    if request_id is None:
        return None
    if source is not None and not (
        isinstance(source, Mapping) and source.get("backend") == "virtual"
    ):
        return None
    return {
        "control_request_id": request_id,
        "state": state,
        "source": dict(source) if source is not None else None,
    }


def vm_provision_generation(metadata: Any) -> str | None:
    """The Session's current VM provision generation, canonical, or None."""
    vm = metadata_object(metadata).get("vm")
    if not isinstance(vm, Mapping) or vm.get("provision_generation") is None:
        return None
    return _canonical_uuid(vm.get("provision_generation"))


def vm_upgrade_recorded(metadata: Any, control_request_id: Any) -> bool:
    """Whether the VM request a lane switch's request names took effect (C2).

    It did once the provision CAS committed a generation and the marker the
    pre-CAS merge records names the request. Both sides ask it when the VM
    request's outcome is unknown: the pinned agent after a 5xx or no answer,
    the orchestrator after an unexpected error.
    """
    marker = vm_upgrade_seed(metadata)
    return (
        vm_provision_generation(metadata) is not None
        and marker is not None
        and marker["control_request_id"] == _canonical_uuid(control_request_id)
    )


__all__ = [
    "LANE_CHANGED_FRAME",
    "SEED_FROM_KEY",
    "UPGRADE_ALREADY_RUNNING",
    "UPGRADE_CANCELLED",
    "UPGRADE_FAILED",
    "UPGRADE_INTERRUPTED",
    "UPGRADE_REFUSED",
    "VM_PROVISIONING_NOT_AVAILABLE",
    "VM_UPGRADE_NOT_RECORDED",
    "VM_UPGRADE_SEED_INTERRUPTED",
    "VM_UPGRADE_SEED_KEY",
    "VM_UPGRADE_SEED_PENDING",
    "VM_UPGRADE_SEED_SEEDED",
    "WORKSPACE_UPGRADE_FAILURE_CODES",
    "WORKSPACE_UPGRADE_PAYLOAD_KEYS",
    "WORKSPACE_UPGRADE_SEED_CAPABILITY",
    "WORKSPACE_UPGRADE_SEED_KEY",
    "WORKSPACE_UPGRADE_SEED_VERSION",
    "WORKSPACE_UPGRADE_VERB",
    "metadata_object",
    "vm_provision_generation",
    "vm_upgrade_recorded",
    "vm_upgrade_seed",
    "workspace_upgrade_seed_from",
]
