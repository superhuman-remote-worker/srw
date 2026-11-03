"""Prove a legacy-failed Session VM absent so End can retire the Session.

The legacy (non-durable) create path can fail and leave ``metadata.vm`` with a
provision generation but no creation request and no VM identity. Retirement
Begin can settle a VM only through one of those, so it refuses the Session and
its agent Pod stays on its protection finalizer for good (vault issue
``failed_vm_session_strands_agent_pod_on_finalizer``).

End therefore first proves the generation empty. It claims the generation
for retirement, deletes the VM by name (404 counts as done), requires a
signed absence observation afterwards, and records that as the generation's
process-zero receipt. Only then does it replace the marker with the proof (or,
for a failed upgrade from the virtual tier, undo the upgrade), and the Session
retires as a life that never had a VM.

A create sent before the marker turned ``failed`` may still reach the
controller within its signed-request window (``MAX_MESSAGE_AGE_SECONDS``). The
proof waits until this process has seen the same failed marker for longer than
that, so a late create runs before the delete, never after it. The wait is
kept in memory: a restart only delays the proof, it never shortens it.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from datetime import datetime, timezone
from typing import Any, Literal

from shared.pinned_vm_creation_retirement import legacy_failed_vm_generation
from shared.vm_lifecycle_auth import MAX_FUTURE_SKEW_SECONDS, MAX_MESSAGE_AGE_SECONDS
from shared.workspace_upgrade import VM_UPGRADE_SOURCE_KEY

__all__ = [
    "LEGACY_VM_ABSENCE_SETTLE_DELAY_SECONDS",
    "legacy_failed_vm_marker_generation",
    "settle_legacy_failed_vm_absence",
]

#: Twice the controller's replay window plus its clock skew allowance.
LEGACY_VM_ABSENCE_SETTLE_DELAY_SECONDS = 2 * (
    MAX_MESSAGE_AGE_SECONDS + MAX_FUTURE_SKEW_SECONDS
)

# (thread, provision generation) -> monotonic time this process first saw the
# failed marker. A marker is already failed when first seen, so every create
# for that generation was sent before this time.
_first_seen: dict[tuple[str, str], float] = {}

LegacyAbsenceOutcome = Literal["not_applicable", "pending", "proven"]


def _thread_metadata(thread: Mapping[str, Any]) -> Mapping[str, Any] | None:
    metadata = thread.get("metadata")
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (TypeError, ValueError):
            return None
    return metadata if isinstance(metadata, Mapping) else None


def _virtual_upgrade_source(thread: Mapping[str, Any]) -> Mapping[str, Any] | None:
    vm = (_thread_metadata(thread) or {}).get("vm")
    source = vm.get(VM_UPGRADE_SOURCE_KEY) if isinstance(vm, Mapping) else None
    if isinstance(source, Mapping) and source.get("backend") == "virtual":
        return source
    return None


def legacy_failed_vm_marker_generation(thread: Mapping[str, Any]) -> str | None:
    """The marker's generation when ``thread`` is an open pinned legacy-failed life."""

    if (
        str(thread.get("execution_lane") or "") != "pinned"
        or thread.get("runtime_retirement_token") is not None
    ):
        return None
    metadata = _thread_metadata(thread)
    if metadata is None:
        return None
    return legacy_failed_vm_generation(metadata.get("vm"))


async def settle_legacy_failed_vm_absence(
    thread: Mapping[str, Any],
    *,
    store: Any,
    vm_provisioner: Any,
    logger: Any,
    clock: Callable[[], float] = time.monotonic,
) -> LegacyAbsenceOutcome:
    """Prove the legacy VM generation absent and drop its marker.

    ``not_applicable``: the row is not an open pinned life with such a marker.
    ``pending``: not proven yet; End must refuse and a later attempt retries.
    ``proven``: the marker is gone and Begin sees a Session with no VM.
    """

    thread_id = str(thread.get("id") or "")
    runtime_generation = str(thread.get("runtime_generation") or "")
    generation = legacy_failed_vm_marker_generation(thread)
    if generation is None or not thread_id or not runtime_generation:
        return "not_applicable"
    key = (thread_id, generation)
    now = clock()
    first = _first_seen.setdefault(key, now)
    if now - first < LEGACY_VM_ABSENCE_SETTLE_DELAY_SECONDS:
        logger.info(
            "Thread %s has a legacy failed VM marker (generation %s); its VM "
            "absence proof waits out the controller replay window",
            thread_id,
            generation,
        )
        return "pending"
    if vm_provisioner is None or not getattr(
        vm_provisioner, "lifecycle_available", False
    ):
        logger.warning(
            "Thread %s keeps its legacy failed VM marker: no VM controller is "
            "available to prove generation %s absent",
            thread_id,
            generation,
        )
        return "pending"
    receipt = {
        "owner_kind": "thread",
        "scope": "vm",
        "provisioner": "vm",
        "runtime_incarnation": generation,
    }
    # The claim makes the generation absorbing (``retiring_process_zero``)
    # before the delete, so nothing can reopen it behind the proof.
    if not await store.claim_managed_repository_workspace_retirement(
        thread_id, **receipt
    ):
        logger.warning(
            "Thread %s keeps its legacy failed VM marker: generation %s could "
            "not be claimed for retirement",
            thread_id,
            generation,
        )
        return "pending"
    if not await vm_provisioner.prove_legacy_thread_vm_absent(
        thread_id, provision_generation=generation
    ):
        logger.warning(
            "Thread %s keeps its legacy failed VM marker: the controller has not "
            "yet confirmed generation %s absent after delete",
            thread_id,
            generation,
        )
        return "pending"
    # Exact runtime absence is this generation's process-zero proof; the
    # database lets its VM record go only with that receipt.
    if not await store.record_managed_repository_workspace_process_zero(
        thread_id, **receipt
    ):
        logger.warning(
            "Thread %s keeps its legacy failed VM marker: the process-zero "
            "receipt for generation %s was refused",
            thread_id,
            generation,
        )
        return "pending"
    if _virtual_upgrade_source(thread) is not None:
        # A failed upgrade from the virtual tier is undone, exactly as the
        # agent's own abort would (E4): End then retires a virtual Session.
        dropped = await store.undo_thread_vm_upgrade(
            thread_id, provision_generation=generation
        )
    else:
        dropped = await store.drop_legacy_failed_thread_vm_marker(
            thread_id,
            runtime_generation=runtime_generation,
            provision_generation=generation,
            proof={
                "evidence": "controller_delete_then_exact_absence_v1",
                "proven_at": datetime.now(timezone.utc).isoformat(),
            },
        )
    if not dropped:
        logger.warning(
            "Thread %s: VM generation %s is absent, but its legacy marker or "
            "VM ledgers changed; the marker stays",
            thread_id,
            generation,
        )
        return "pending"
    _first_seen.pop(key, None)
    logger.info(
        "Thread %s: legacy failed VM generation %s proven absent; End can retire "
        "the Session",
        thread_id,
        generation,
    )
    return "proven"
