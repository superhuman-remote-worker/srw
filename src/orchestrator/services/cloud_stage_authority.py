"""Frozen authority for one protected-cloud stage, and its terminal receipt.

Extracted verbatim from ``orchestrator.main`` (R1.B04 lane C). Port contract
§3.4 freezes the five names below: the thread-cloud-diff restage path (lane M)
imports four of them directly, and the retirement settlement path (still in
``main``) imports the receipt validator. They stay module-level functions with
their collaborators passed explicitly rather than closed over.

Three properties are load-bearing and moved unchanged:

* **``_capture_cloud_stage_authority`` is a whole-tuple check.** Runtime
  generation, workspace generation, workspace runtime incarnation, engage
  attempt and (unless the orchestrator owns the retirement) the attach token
  must every one parse as a UUID, and the mount row must be ``active`` on the
  *same* runtime generation. Anything else returns ``None`` — there is no
  partial authority.
* **``_retirement_stage_event_from_receipt`` fails closed.** ``True`` means the
  final probe was already published and cleanup may be retried without touching
  the now-absent workspace/reader. Any malformed or cross-generation shape
  returns ``False`` and must never be treated as a successful stage. The
  generation fencing is the whole point: a receipt from a *different* runtime
  generation, a different retirement token, a different mount/engage attempt or
  a staged epoch that does not match the append-once rule (``expected`` for an
  unchanged probe, ``expected + 1`` otherwise) is not this caller's receipt.
* **A retirement-pending stage is not advertised early.**
  ``_broadcast_cloud_stage_result`` drops the event when the publication says
  the runtime retirement is pending, because retirement journals the same
  payload atomically at settlement once the old agent Pod is gone.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any
from uuid import UUID

from orchestrator.services.cloud.protected_reader_authority import (
    ProtectedNextcloudReaderGrantPlan,
    readerless_protected_life,
)
from orchestrator.services.cloud_staging.source_identity import (
    ProtectedMountSourceIdentity,
)
from orchestrator.services.container_provisioner import (
    WORKSPACE_RUNTIME_INCARNATION_KEY,
)
from orchestrator.services.stateless_workspace_gate import thread_metadata_object
from orchestrator.services.vm_provisioner import (
    vm_provisioner as _default_vm_provisioner,
)
from shared.workspace_contract import resolve_workspace_runtime

#: Prove that the thread's protected reader was *never* delivered. Owned by the
#: application (B09) and injected, because the retirement receipt check must
#: agree with the live reader-shape authority rather than hold a second copy.
NeverDeliveredProtectedReaderShape = Callable[..., bool]


def _capture_cloud_stage_authority(
    thread: dict[str, Any], row: dict[str, Any]
) -> dict[str, Any] | None:
    """Freeze the complete runtime/workspace/reader source for one stage."""

    if thread.get("execution_lane") != "pinned":
        return None
    generation = str(thread.get("runtime_generation") or "")
    agent_id = str(thread.get("agent_id") or "")
    attach_token = str(thread.get("runtime_attach_token") or "")
    orchestrator_owned_retirement = bool(
        thread.get("runtime_retirement_token") is not None
        and thread.get("runtime_retirement_authorized_at") is not None
        and thread.get("runtime_authority_exposed") is False
        and not agent_id
        and not attach_token
    )
    metadata = thread_metadata_object(thread)
    ws = metadata.get("workspace_container") or {}
    binding = metadata.get("_workspace_binding") or {}
    if not isinstance(ws, dict) or not isinstance(binding, dict):
        return None
    workspace_generation = str(binding.get("generation") or "")
    workspace_runtime = str(ws.get(WORKSPACE_RUNTIME_INCARNATION_KEY) or "")
    fingerprint = str(binding.get("ssh_host_key_fingerprint") or "")
    source = ProtectedMountSourceIdentity.from_binding(
        row.get("source_binding"),
        expected_sha256=str(row.get("source_binding_sha256") or ""),
    )
    try:
        UUID(generation)
        UUID(workspace_generation)
        UUID(workspace_runtime)
        UUID(str(row.get("engage_attempt") or ""))
        if not orchestrator_owned_retirement:
            UUID(attach_token)
    except (TypeError, ValueError):
        return None
    if not (
        (agent_id or orchestrator_owned_retirement)
        and ws.get("status") == "ready"
        and binding.get("kind") == "remote"
        and str(ws.get("_canvas_workspace_generation") or "") == workspace_generation
        and fingerprint.startswith("SHA256:")
        and source is not None
        and str(row.get("status") or "") == "active"
        and str(row.get("runtime_generation") or "") == generation
    ):
        return None
    return {
        "runtime_generation": generation,
        "runtime_retirement_token": (
            str(thread["runtime_retirement_token"])
            if thread.get("runtime_retirement_token") is not None
            else None
        ),
        "agent_id": agent_id or None,
        "runtime_attach_token": attach_token or None,
        "workspace": dict(ws),
        "workspace_binding": dict(binding),
        "workspace_generation": workspace_generation,
        "workspace_runtime_incarnation": workspace_runtime,
        "workspace_ssh_host_key_fingerprint": fingerprint,
        "mount_row_id": str(row.get("id") or ""),
        "engage_attempt": str(row.get("engage_attempt") or ""),
        "source_binding_sha256": source.sha256,
        "expected_staged_epoch": int(row.get("staged_epoch") or 0),
    }


def _retirement_stage_event_from_receipt(
    retirement: Mapping[str, Any],
    thread: Mapping[str, Any],
    row: Mapping[str, Any] | None,
    *,
    never_delivered_protected_reader_shape: NeverDeliveredProtectedReaderShape,
) -> tuple[bool, dict[str, Any] | None]:
    """Validate an append-once terminal stage receipt for retry recovery.

    ``True`` means the final probe was already published and cleanup may be
    retried without touching the now-absent workspace/reader.  ``event`` is
    ``None`` only for a proven empty publication.  Any malformed or
    cross-generation shape returns ``False`` and must never be treated as a
    successful stage.
    """

    receipt = thread.get("runtime_retirement_stage_receipt")
    if receipt is None:
        return False, None
    if isinstance(receipt, str):
        try:
            receipt = json.loads(receipt)
        except (TypeError, ValueError):
            return False, None
    context = retirement.get("context") or {}
    protected = context.get("protected_ro") or {}
    workspace = context.get("workspace_container") or {}
    binding = context.get("workspace_binding") or {}
    if isinstance(receipt, Mapping) and receipt.get("kind") == "never_engaged":
        captured_ro = context.get("protected_ro")
        current_ro_proves_unpublished = never_delivered_protected_reader_shape(
            retirement,
            thread,
            row,
            require_current_revoked=True,
        ) or bool(
            # A life that minted no reader receipts zero even after its agent
            # ran; the receipt names the earlier row Begin captured.
            context.get("protected_cloud") is True
            and str(thread.get("runtime_generation") or "")
            == str(retirement.get("generation") or "")
            and readerless_protected_life(
                captured_ro,
                row,
                thread_id=str(context.get("thread_id") or ""),
                runtime_generation=str(retirement.get("generation") or ""),
            )
        )
        names_captured_reader = bool(
            current_ro_proves_unpublished and isinstance(captured_ro, Mapping)
        )
        expected_mount_id = (
            str(captured_ro.get("id")) if names_captured_reader else None
        )
        expected_attempt = (
            str(captured_ro.get("engage_attempt")) if names_captured_reader else None
        )
        captured_plan = (
            ProtectedNextcloudReaderGrantPlan.from_ro_mount_row(captured_ro)
            if names_captured_reader
            else None
        )
        expected_source_sha256 = (
            captured_plan.source_sha256 if captured_plan is not None else None
        )
        if not (
            current_ro_proves_unpublished
            and int(receipt.get("version") or 0) == 1
            and str(receipt.get("runtime_generation") or "")
            == str(retirement.get("generation") or "")
            and str(receipt.get("retirement_token") or "")
            == str(retirement.get("token") or "")
            and (
                receipt.get("mount_id") is None
                if expected_mount_id is None
                else str(receipt.get("mount_id") or "") == expected_mount_id
            )
            and (
                receipt.get("engage_attempt") is None
                if expected_attempt is None
                else str(receipt.get("engage_attempt") or "") == expected_attempt
            )
            and (
                receipt.get("source_binding_sha256") is None
                if expected_source_sha256 is None
                else str(receipt.get("source_binding_sha256") or "")
                == expected_source_sha256
            )
            and receipt.get("workspace_generation") is None
            and receipt.get("workspace_runtime_incarnation") is None
            and int(receipt.get("expected_staged_epoch") or 0) == 0
            and int(receipt.get("staged_epoch") or 0) == 0
            and receipt.get("staged_summary") is None
        ):
            return False, None
        return True, {
            "thread_id": str(context.get("thread_id") or ""),
            "session_runtime_generation": str(retirement.get("generation") or ""),
            "staged_epoch": 0,
            "file_count": 0,
            "counts": {"added": 0, "modified": 0, "deleted": 0},
            "mount_id": expected_mount_id,
        }
    if row is None:
        return False, None
    if not all(
        isinstance(value, Mapping)
        for value in (receipt, context, protected, workspace, binding, row)
    ):
        return False, None
    assert isinstance(receipt, Mapping)
    assert isinstance(row, Mapping)
    generation = str(retirement.get("generation") or "")
    token = str(retirement.get("token") or "")
    mount_id = str(protected.get("id") or "")
    engage_attempt = str(protected.get("engage_attempt") or "")
    workspace_generation = str(binding.get("generation") or "")
    workspace_runtime = str(workspace.get(WORKSPACE_RUNTIME_INCARNATION_KEY) or "")
    expected_epoch = int(protected.get("staged_epoch") or 0)
    staged_epoch = int(receipt.get("staged_epoch") or -1)
    receipt_kind = str(receipt.get("kind") or "")
    captured_plan = ProtectedNextcloudReaderGrantPlan.from_ro_mount_row(protected)
    current_plan = ProtectedNextcloudReaderGrantPlan.from_ro_mount_row(row)
    if captured_plan is None or current_plan != captured_plan:
        return False, None
    source_sha256 = captured_plan.source_sha256
    if not (
        int(receipt.get("version") or 0) == 1
        and str(receipt.get("runtime_generation") or "") == generation
        and str(receipt.get("retirement_token") or "") == token
        and str(receipt.get("mount_id") or "") == mount_id
        and str(receipt.get("engage_attempt") or "") == engage_attempt
        and str(receipt.get("source_binding_sha256") or "") == source_sha256
        and str(receipt.get("workspace_generation") or "") == workspace_generation
        and str(receipt.get("workspace_runtime_incarnation") or "") == workspace_runtime
        and int(receipt.get("expected_staged_epoch") or -1) == expected_epoch
        and staged_epoch
        == (expected_epoch if receipt_kind == "unchanged" else expected_epoch + 1)
        and str(row.get("id") or "") == mount_id
        and str(row.get("runtime_generation") or "") == generation
        and str(row.get("engage_attempt") or "") == engage_attempt
        and int(row.get("staged_epoch") or -1) == staged_epoch
    ):
        return False, None
    current_summary = row.get("staged_summary")
    if isinstance(current_summary, str):
        try:
            current_summary = json.loads(current_summary)
        except (TypeError, ValueError):
            return False, None
    summary = receipt.get("staged_summary")
    if current_summary != summary:
        return False, None
    kind = receipt_kind
    if kind == "empty":
        if summary is not None:
            return False, None
        return True, {
            "thread_id": str(context.get("thread_id") or ""),
            "session_runtime_generation": generation,
            "staged_epoch": staged_epoch,
            "file_count": 0,
            "counts": {"added": 0, "modified": 0, "deleted": 0},
            "mount_id": mount_id,
        }
    if not isinstance(summary, Mapping):
        return False, None
    if not (
        summary.get("source_binding") == captured_plan.source.binding
        and str(summary.get("source_binding_sha256") or "") == source_sha256
    ):
        return False, None
    if kind == "unchanged":
        captured_summary = protected.get("staged_summary")
        if isinstance(captured_summary, str):
            try:
                captured_summary = json.loads(captured_summary)
            except (TypeError, ValueError):
                return False, None
        if captured_summary != summary:
            return False, None
    elif kind == "uploaded":
        tar_sha = str(summary.get("tar_sha256") or "")
        prefix = (
            f"cloud-staging/{context.get('thread_id')}/{generation}/"
            f"{workspace_generation}/{staged_epoch}/{source_sha256}/{tar_sha}/"
        )
        if not (
            len(tar_sha) == 64
            and str(summary.get("tar_key") or "") == f"{prefix}upper.tar"
            and str(summary.get("manifest_key") or "") == f"{prefix}manifest.json"
        ):
            return False, None
    else:
        return False, None
    counts = summary.get("counts") or {}
    if not isinstance(counts, Mapping):
        return False, None
    event = {
        "thread_id": str(context.get("thread_id") or ""),
        "session_runtime_generation": generation,
        "staged_epoch": staged_epoch,
        "file_count": sum(int(value or 0) for value in counts.values()),
        "counts": dict(counts),
        "mount_id": mount_id,
    }
    return True, event


def _cloud_stage_task_key(thread_id: str, authority: Mapping[str, Any] | None) -> str:
    if authority is None:
        return f"{thread_id}:vm"
    return ":".join(
        (
            thread_id,
            str(authority.get("runtime_generation") or ""),
            str(authority.get("workspace_generation") or ""),
            str(authority.get("expected_staged_epoch") or ""),
        )
    )


def _thread_selected_vm_workspace(
    thread: Mapping[str, Any],
    *,
    vm_provisioner: Any = _default_vm_provisioner,
) -> bool:
    metadata = thread_metadata_object(thread)
    decision = resolve_workspace_runtime(
        {
            "context": metadata,
            "config_override": metadata.get("config_override"),
        },
        vm_mode=vm_provisioner.mode,
    )
    return bool(decision.ready and decision.effective_backend == "vm")


def _broadcast_cloud_stage_result(result: Mapping[str, Any] | None) -> None:
    if not result or not isinstance(result.get("event"), dict):
        return
    publication = result.get("publication") or {}
    if publication.get("runtime_retirement_pending"):
        # Retirement journals the same payload atomically at settlement after
        # the old agent Pod is gone; do not advertise it early.
        return
    user_id = str(publication.get("user_id") or "")
    if user_id:
        from orchestrator.services.notification_feed import notification_feed

        notification_feed.broadcast(user_id, "cloud.diff_staged", result["event"])


__all__ = [
    "NeverDeliveredProtectedReaderShape",
    "_broadcast_cloud_stage_result",
    "_capture_cloud_stage_authority",
    "_cloud_stage_task_key",
    "_retirement_stage_event_from_receipt",
    "_thread_selected_vm_workspace",
]
