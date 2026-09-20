"""Existing public job projections with explicit application collaborators.

The projection preserves extension fields and the original JSONB representation.
Workspace lifecycle and cloud backend selection remain application-owned; these
helpers neither construct provisioners nor perform network or database I/O.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Callable, Mapping, Protocol
from uuid import UUID

from orchestrator.services.cloud.handles import SessionFolderHandle
from shared.workspace_contract import (
    WORKSPACE_CONTRACT_CONTEXT_KEY,
    WORKSPACE_DISPATCH_AUTHORITY_CONTEXT_KEY,
    WORKSPACE_RUNTIME_CONTEXT_KEY,
    workspace_contract_projection,
)


class JobCloudBackend(Protocol):
    @property
    def backend_id(self) -> str: ...

    @property
    def is_initialized(self) -> bool: ...

    def get_session_folder_browser_url(
        self, handle: SessionFolderHandle
    ) -> str | None: ...


_WORKSPACE_RECOVERY_MESSAGES = {
    "workspace_runtime_not_ready": "Recovering workspace — waiting for guest networking.",
    "workspace_transport_unavailable": "Recovering workspace — transport is temporarily unavailable.",
    "workspace_replacement_observed": "Recovering workspace — validating the replacement runtime.",
    "workspace_identity_conflict": "Recovery paused — workspace identity changed unexpectedly.",
    "prior_runtime_unfenced": "Previous workspace execution could not be proven stopped.",
    "shared_workspace_writers_unfenced": "Recovery paused — another workspace writer could not be proven stopped.",
    "tool_outcome_unknown": "Recovery paused — an interrupted command may have completed remotely.",
    "checkpoint_unavailable": "Recovery paused — the durable checkpoint is unavailable.",
    "workspace_recovery_deadline_exceeded": "Recovery paused — the recovery deadline elapsed.",
}
_WORKSPACE_RECOVERY_STATES = {
    "recovering",
    "observing",
    "waiting_runtime",
    "verifying_stop",
    "attesting",
    "reconciling_outcome",
    "paused_attention",
}

_VM_PHASE_ATTENTION_MESSAGES = {
    "vm_rootdisk_stalled": "VM disk preparation needs attention because progress has stopped.",
    "vm_runtime_changed": "VM startup needs attention because its runtime changed unexpectedly.",
    "vm_phase_identity_conflict": "VM startup needs attention because its workspace identity could not be verified.",
}


def _vm_phase_attention_message(vm: Mapping[str, Any]) -> str | None:
    from orchestrator.services.dispatch_guards import vm_phase_decision

    if vm.get("status") == "query_failed":
        return "VM startup needs attention because its progress could not be verified. The workspace disk is retained."
    # Existing cleanup, dependency and initialization handlers own their
    # diagnostics. Only initial VM startup uses the new phase policy.
    if vm.get("status") not in {
        "creating",
        "created",
        "provisioning",
        "starting",
        "ssh_pending",
        "running",
        "query_failed",
    }:
        return None
    if vm.get("initialization_started_at") is not None:
        return None
    decision = vm_phase_decision(
        vm,
        now=time.time(),
        timeout_s=int(os.environ.get("VM_PROVISION_TIMEOUT_S", "600")),
        rootdisk_stall_timeout_s=int(
            os.environ.get("VM_ROOTDISK_STALL_TIMEOUT_S", "2700")
        ),
    )
    if decision.action != "attention":
        return None
    return (
        _VM_PHASE_ATTENTION_MESSAGES.get(
            decision.reason,
            "VM startup needs attention because its progress could not be verified.",
        )
        + " The workspace disk is retained."
    )


def workspace_recovery_projection(job: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return the coordinate-free public view of one unresolved recovery."""

    raw = job.get("_workspace_recovery", job.get("workspace_recovery"))
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return None
    if not isinstance(raw, Mapping):
        return None
    try:
        operation_id = str(UUID(str(raw.get("operation_id"))))
    except (TypeError, ValueError, AttributeError):
        return None
    state = str(raw.get("state") or "")
    reason_code = str(raw.get("reason_code") or "")
    if (
        state not in _WORKSPACE_RECOVERY_STATES
        or reason_code not in _WORKSPACE_RECOVERY_MESSAGES
    ):
        return None
    return {
        "operation_id": operation_id,
        "state": state,
        "reason_code": reason_code,
        "message": _WORKSPACE_RECOVERY_MESSAGES[reason_code],
        "started_at": raw.get("started_at"),
        "deadline_at": raw.get("deadline_at"),
        "next_check_at": raw.get("next_check_at"),
        # Shared workspace participants see the same coordinate-free recovery
        # state, but only the canonical job owner may create its successor.
        # Missing ownership evidence fails closed for rolling-upgrade rows.
        "retryable": state == "paused_attention" and raw.get("canonical_owner") is True,
        "cleanup_pending": raw.get("cleanup_pending") is True,
    }


def redact_job_config_override(
    job: dict[str, Any],
    *,
    vm_mode: Callable[[], str],
    runtime_incarnation_key: str,
    redact_config_override: Callable[[Any], Any],
) -> dict[str, Any]:
    """Strip credentials and private workspace lease identity from a job."""

    job = dict(job)
    if (
        not isinstance(job.get("workspace_contract"), dict)
        or "state" not in job["workspace_contract"]
    ):
        job["workspace_contract"] = workspace_contract_projection(
            job, vm_mode=vm_mode()
        )
    recovery = workspace_recovery_projection(job)
    job.pop("_workspace_recovery", None)
    job["workspace_recovery"] = recovery
    job = redact_nested_workspace_state(
        job, field="context", runtime_incarnation_key=runtime_incarnation_key
    )
    context = job.get("context")
    context_was_str = isinstance(context, str)
    if context_was_str:
        try:
            context = json.loads(context)
        except (json.JSONDecodeError, TypeError):
            context = None
    if isinstance(context, dict):
        vm = context.get("vm")
        if (
            job.get("status") in {"created", "paused"}
            and not job.get("error_message")
            and isinstance(vm, dict)
            and (
                vm.get("status") == "retiring_process_zero"
                or vm.get("retirement_cleanup_pending") is True
            )
        ):
            reason = (
                "VM cleanup is waiting to verify that previous workspace processes "
                "have stopped. "
                if vm.get("status") == "retiring_process_zero"
                else "VM cleanup is waiting for the previous workspace release to finish. "
            )
            job["error_message"] = reason + (
                "The workspace disk is retained; cleanup retries automatically."
            )
        if (
            job.get("status") in {"created", "paused"}
            and not job.get("error_message")
            and isinstance(vm, dict)
        ):
            message = _vm_phase_attention_message(vm)
            if message:
                job["error_message"] = message
        # The coordinate-free workspace_contract projection above is the
        # public contract. Provisioner branches contain SSH hosts, pod/service
        # coordinates and generation authority needed only by server and
        # worker paths; never return them from the user-facing job API.
        public_context = dict(context)
        for key in (
            "vm",
            "workspace_container",
            WORKSPACE_CONTRACT_CONTEXT_KEY,
            WORKSPACE_DISPATCH_AUTHORITY_CONTEXT_KEY,
            WORKSPACE_RUNTIME_CONTEXT_KEY,
            "workspace_backend",
        ):
            public_context.pop(key, None)
        job["context"] = (
            json.dumps(public_context) if context_was_str else public_context
        )
    co = job.get("config_override")
    if co is None:
        return job
    was_str = isinstance(co, str)
    if was_str:
        try:
            co = json.loads(co)
        except (json.JSONDecodeError, TypeError):
            # Opaque/garbage — drop it rather than risk returning a raw secret.
            job = dict(job)
            job["config_override"] = None
            return job
    job = dict(job)
    cleaned = redact_config_override(co)
    if isinstance(cleaned, dict) and isinstance(cleaned.get("workspace"), dict):
        workspace = dict(cleaned["workspace"])
        workspace.pop("remote", None)
        cleaned = {**cleaned, "workspace": workspace}
    job["config_override"] = json.dumps(cleaned) if was_str else cleaned
    return job


def redact_nested_workspace_state(
    record: dict[str, Any], *, field: str, runtime_incarnation_key: str
) -> dict[str, Any]:
    """Remove provisioner-only lease fields while preserving JSONB shape."""

    value = record.get(field)
    was_str = isinstance(value, str)
    if was_str:
        try:
            value = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return record
    if not isinstance(value, dict):
        return record

    cleaned = value
    changed = False
    for context_key in ("workspace_container", "vm"):
        context = cleaned.get(context_key)
        if not isinstance(context, dict) or not any(
            key in context
            for key in (
                "_canvas_workspace_generation",
                runtime_incarnation_key,
                "_docker_workspace_lease_id",
                "_docker_workspace_trust_mode",
                "_docker_workspace_attested",
                "_docker_workspace_host_key_fingerprint",
                "quarantine_reason",
            )
        ):
            continue
        if not changed:
            cleaned = dict(cleaned)
            changed = True
        context = dict(context)
        context.pop("_canvas_workspace_generation", None)
        context.pop(runtime_incarnation_key, None)
        context.pop("_docker_workspace_lease_id", None)
        context.pop("_docker_workspace_trust_mode", None)
        context.pop("_docker_workspace_attested", None)
        context.pop("_docker_workspace_host_key_fingerprint", None)
        context.pop("quarantine_reason", None)
        cleaned[context_key] = context
    if not changed:
        return record
    result = dict(record)
    result[field] = json.dumps(cleaned) if was_str else cleaned
    return result


def resolve_exported_folder_url(
    handle_str: str | None,
    *,
    resolve_backend: Callable[[str | None], JobCloudBackend],
) -> str | None:
    """Browser URL for a job's Mode B export folder, or None.

    The job row stores only the opaque handle, so without this the cockpit has
    no way to re-open the folder after the export response is gone (a reload,
    or a popup the browser blocked). Jobs carry no per-row backend column —
    unlike threads, which is why this doesn't reuse
    :func:`_resolve_cloud_session_url` — so the discriminator comes from the
    serialized handle itself, falling back to the active backend for the bare
    legacy form. Pure URL construction, no I/O.
    """
    if not handle_str:
        return None
    backend = resolve_backend(None)
    if not backend.is_initialized:
        return None
    try:
        handle = SessionFolderHandle.from_db(handle_str, backend=backend.backend_id)
        if handle.backend != backend.backend_id:
            backend = resolve_backend(handle.backend)
            if not backend.is_initialized:
                return None
        return backend.get_session_folder_browser_url(handle)
    except Exception:
        return None


def with_cloud_review_mode(
    job: dict[str, Any], *, resolve_folder_url: Callable[[str | None], str | None]
) -> dict[str, Any]:
    """Attach the computed ``cloud_review_mode`` and drop the raw join column.

    Routing signal for the cockpit's job-review UI: a job whose project has a
    main-cloud folder goes through the Mode A diff-review flow (``'diff'``);
    everything else — loose jobs and projects without a cloud folder, including
    the auto-assigned default project — gets the Mode B "Open cloud folder"
    affordance (``'open_folder'``). Mirrors the seed-time gate in
    ``services/job_cloud_baseline.py``. ``project_has_cloud_folder`` is computed
    by the ``LEFT JOIN projects`` in the postgres read queries; we pop it so the
    raw column never leaves over REST.

    Also resolves ``exported_folder_handle`` into a ready-to-open
    ``exported_folder_url`` for the same reason: handles are opaque to
    everything outside the owning backend, so the cockpit can't derive it.
    """
    job = dict(job)
    job["cloud_review_mode"] = (
        "diff" if job.pop("project_has_cloud_folder", False) else "open_folder"
    )
    job["exported_folder_url"] = resolve_folder_url(job.get("exported_folder_handle"))
    return job
