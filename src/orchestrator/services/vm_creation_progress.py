"""Coordinate-free VM creation progress; advisory only, never admission proof."""

from collections.abc import Mapping
from datetime import datetime, timezone
import json
import os
from uuid import UUID


_MESSAGES = {
    "creation_configuration_pending": "Waiting to resolve VM configuration.",
    "creation_configuration_unproven": "VM creation paused because its configuration could not be verified.",
    "capacity_wait": "Waiting for VM capacity.",
    "controller_count_wait": "Waiting for the VM controller count limit.",
    "resource_wait": "Waiting for VM workspace resources.",
    "golden_wait": "Waiting for the VM base disk.",
    "preparation_wait": "Waiting for workspace preparation.",
    "headscale_wait": "Waiting for VM network registration.",
    "disk_wait": "Waiting for VM disk preparation.",
    "creation_dependency_pending": "Waiting for a VM provisioning dependency.",
    "creation_observation_pending": "Verifying the result of VM creation.",
    "controller_unavailable": "VM creation is waiting for the controller to become available.",
    "vm_creation_retry_pending": "VM creation is queued for reconciliation.",
    "vm_creation_retry_blocked": "VM creation paused because its result could not be verified.",
    "creation_evidence_unproven": "VM creation needs attention because its history could not be verified.",
    "job_admission_expired": "VM creation stopped because the original job deadline elapsed.",
    "execution_manifest_changed": "VM creation paused because the admitted execution changed.",
    "creation_adopted": "Waiting for VM connectivity and workspace initialization.",
    "job_cancelled": "Waiting for VM creation cancellation to be reconciled.",
}
_STATES = {
    "queued",
    "resolving",
    "reconciling",
    "attention",
    "succeeded",
    "cancel_requested",
}
_RETRYABLE = {
    "controller_unavailable",
    "creation_configuration_unproven",
    "vm_creation_retry_blocked",
}


def _object(value):
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            return {}
    return value if isinstance(value, Mapping) else {}


def preflight_creation_progress(context):
    """Bound the private preflight before list queries discard job context."""
    context = _object(context)
    vm = context.get("vm")
    if not isinstance(vm, Mapping):
        return None
    value = vm.get("creation_preflight")
    if (
        not isinstance(value, Mapping)
        or not isinstance(value.get("state"), str)
        or value["state"] in {"admitted", "settled"}
    ):
        return None
    return {
        "stage": "configuration",
        **{
            key: value.get(key)
            for key in ("request_id", "state", "reason", "admission_deadline")
        },
        "pending": context.get("_vm_creation_pending") == value.get("request_id"),
        "resume_blocked": any(
            key in context
            for key in (
                "_completion_control_claim",
                "_stateless_delete_pending",
                "_stateless_cancel_cleanup_pending",
            )
        )
        or vm.get("retirement_cleanup_pending") is True
        or vm.get("status")
        in ("retiring_process_zero", "deleting", "deleted", "delete_failed"),
    }


def _deadline_valid(value):
    if value is None:
        return True
    if not isinstance(value, str):
        return False
    try:
        deadline = datetime.fromisoformat(value)
        return deadline.tzinfo is not None and deadline > datetime.now(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        return False


def _owner_wait(raw, reason):
    """One request's bounded wait; no cluster budget, fleet count or queue rank."""
    if reason == "controller_unavailable":
        return {"kind": "controller", "since": None, "size_nonfit": False,
                "guest_vcpus": None, "guest_memory_bytes": None}
    if reason == "controller_count_wait":
        return {"kind": "count", "since": None, "size_nonfit": False,
                "guest_vcpus": None, "guest_memory_bytes": None}
    if reason not in {"resource_wait", "capacity_wait", "vm_creation_retry_pending"}:
        return None
    resource = raw.get("resource_wait")
    if isinstance(resource, Mapping) and resource.get("state") in {"waiting", "nonfit"}:
        vcpus, memory = resource.get("guest_vcpus"), resource.get("guest_memory_bytes")
        since = resource.get("enqueued_at")
        return {
            "kind": "resource",
            "since": since if isinstance(since, str) else None,
            "size_nonfit": resource["state"] == "nonfit"
            and resource.get("reason") == "resource_size_nonfit",
            "guest_vcpus": vcpus if type(vcpus) is int and vcpus > 0 else None,
            "guest_memory_bytes": memory if type(memory) is int and memory > 0 else None,
        }
    return {"kind": "unknown", "since": None, "size_nonfit": False,
            "guest_vcpus": None, "guest_memory_bytes": None}


def vm_creation_projection(job):
    raw = _object(job.get("_vm_creation")) or preflight_creation_progress(
        job.get("context")
    )
    if (
        not raw
        or not isinstance(raw.get("state"), str)
        or raw["state"] not in _STATES
        or raw.get("ready_at") is not None
    ):
        return None
    try:
        request_id = str(UUID(raw["request_id"]))
    except (ValueError, TypeError, KeyError, AttributeError):
        return None
    if request_id != raw["request_id"]:
        return None
    stage = raw.get("stage")
    if not isinstance(stage, str) or stage not in {"configuration", "creation"}:
        return None
    state = raw["state"]
    reason = raw.get("reason")
    if reason is None:
        reason = (
            "creation_configuration_pending"
            if stage == "configuration"
            else "vm_creation_retry_pending"
        )
    if state == "succeeded":
        stage, reason = "readiness", "creation_adopted"
    elif state == "cancel_requested":
        reason = "job_cancelled"
    if not isinstance(reason, str) or reason not in _MESSAGES:
        reason = "creation_evidence_unproven"
    wait = _owner_wait(raw, reason) if state in {"queued", "reconciling"} else None
    resumable = (
        state == "attention"
        and reason in _RETRYABLE
        and os.getenv("VM_CREATION_RETRY_ENABLED", "false").lower() == "true"
        and job.get("status") in {"created", "paused", "failed"}
        and job.get("assigned_agent_id") is None
        and raw.get("pending") is True
        and raw.get("resume_blocked") is False
        and not job.get("workspace_recovery")
        and _deadline_valid(raw.get("admission_deadline"))
    )
    return {
        "request_id": request_id,
        "state": state,
        "stage": stage,
        "reason_code": reason,
        "message": _MESSAGES[reason],
        "wait": wait,
        "resumable": resumable,
    }
