"""Coordinate-free VM idle state for already-authorized owner reads.

The caller supplies only IDs which its route has already authorized.  The
database read is batched for lists; no lease is acquired or renewed here.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any
from uuid import UUID

from orchestrator.services.vm_creation_owner_view import thread_creation_is_starting
from shared.workspace_idle_policy import (
    DEFAULT_WARM_SECONDS,
    IdlePolicyError,
    read_episode,
)

_ACTIVE = {"releasing", "release_held", "suspended", "waking", "wake_held"}
_SAFE_REASONS = {
    "pinned_agent_stop_unproven",
    "pinned_agent_stop_receipt_changed",
    "capture_identity_changed",
    "cleanup_authority_held",
    "physical_stop_unproven",
    "fresh_agent_prepare_pending",
    "resource_reservation_unavailable",
    "resource_reservation_held",
    "effect_unavailable",
    "phase_approval_source_changed",
    "active_workspace_access",
}


def _object(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            return {}
    return value if isinstance(value, dict) else {}


def project_vm_idle_state(
    row: dict[str, Any],
    *,
    thread_creation: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Project one owner and its latest operation without private identity."""
    owner_kind = row["owner_kind"]
    # Preflight is abortable and hidden; only authorized terminal retirement
    # supersedes idle readiness. Suspended retirement keeps its idle view.
    if (
        owner_kind == "thread"
        and row.get("runtime_retirement_token") is not None
        and row.get("runtime_retirement_authorized_at") is not None
        and _object(row.get("runtime_retirement_context")).get("settle_status")
        == "ended"
    ):
        return None
    context = _object(row["context"] if owner_kind == "job" else row["metadata"])
    vm = _object(context.get("vm"))
    if not vm:
        return None
    if (owner_kind == "thread" and row["execution_lane"] != "pinned") or (
        owner_kind == "job" and row["execution_lane"] not in {"stateless", "pinned"}
    ):
        return {"state": "unsupported", "reason_code": "unsupported_lane"}
    if row["status"] in {"completed", "failed", "cancelled", "ended"}:
        return None

    document = _object(row.get("workspace_idle_episode"))
    episode = None
    if document:
        try:
            episode = read_episode(
                document, revision=row.get("workspace_idle_revision")
            )
        except (IdlePolicyError, TypeError, ValueError):
            return {"state": "release_held", "reason_code": "identity_unverified"}

    phase = row.get("idle_phase")
    if (
        phase in _ACTIVE
        and episode
        and str(row.get("idle_episode_id")) == episode.episode_id
    ):
        result: dict[str, Any] = {"state": phase}
        if phase in {"release_held", "wake_held"}:
            reason = row.get("idle_reason")
            result["reason_code"] = (
                reason if reason in _SAFE_REASONS else "workspace_attention"
            )
            retry = row.get("idle_retry_after")
            if retry is not None:
                result["next_retry_at"] = retry.isoformat()
        return result

    # Initial VM creation has no idle lifecycle yet. Reporting a hold here
    # would hide the IDE's restoring indicator behind a false idle warning.
    if (
        (
            vm.get("status") in {"pending", "provisioning", "created", "ssh_pending"}
            or (
                owner_kind == "job"
                and vm.get("status") == "waiting_creation_configuration"
            )
        )
        and row.get("workspace_idle_episode") is None
        and row.get("workspace_idle_revision") == 0
        and phase is None
    ):
        return None

    # Ordinary Resume preserves the owner's idle revision and may retain its
    # last natural-pause episode. The owner-scoped durable proof below must
    # identify that exact cleaned predecessor; a current or uncertain episode
    # still holds.
    if (
        owner_kind == "thread"
        and row.get("runtime_retirement_token") is None
        and phase is None
        and (
            row.get("workspace_idle_episode") is None
            or (
                episode is not None
                and row["status"] in {"created", "active", "awaiting_user"}
                and episode.wait_kind == "natural_pause"
                and row.get("idle_operation_id") is None
                and row.get("retained_prior_idle_episode_proven") is True
            )
        )
        and thread_creation_is_starting(vm, thread_creation)
    ):
        return None

    if vm.get("status") != "ready":
        return {"state": "release_held", "reason_code": "identity_unverified"}
    if (
        vm.get("identity_authenticated") is not True
        or vm.get("identity_provision_generation") != vm.get("provision_generation")
        or not all(
            vm.get(key)
            for key in (
                "provision_generation",
                "vm_uid",
                "vmi_uid",
                "active_pod_uid",
                "rootdisk_pvc_uid",
            )
        )
    ):
        return {"state": "release_held", "reason_code": "identity_unverified"}
    if episode is None:
        return {"state": "ready"}
    if (
        owner_kind == "thread"
        and row["status"] in {"created", "active", "awaiting_user"}
        and row.get("runtime_retirement_token") is None
        and phase is None
        and row.get("idle_operation_id") is None
        and episode.wait_kind == "natural_pause"
        and row.get("retained_prior_idle_episode_proven") is True
    ):
        return {"state": "ready"}
    identity = episode.runtime_identity
    if (
        identity.owner_kind != owner_kind
        or identity.owner_id != str(row["id"])
        or identity.backend != "vm"
        or identity.runtime_generation != vm["provision_generation"]
        or identity.runtime_uid != vm["vm_uid"]
    ):
        return {"state": "release_held", "reason_code": "identity_unverified"}
    due = episode.entered_at + timedelta(seconds=DEFAULT_WARM_SECONDS)
    if episode.override_until and episode.override_until > due:
        due = episode.override_until
    return {"state": "warm", "idle_expires_at": due.isoformat()}


async def read_vm_idle_states(
    store: Any,
    *,
    owner_kind: str,
    owner_ids: list[str],
    current_thread_creations: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Read the latest owner state in one query after list/detail authorization."""
    if (
        owner_kind not in {"job", "thread"}
        or not owner_ids
        or not callable(getattr(store, "acquire", None))
    ):
        return {}
    ids = [UUID(str(value)) for value in owner_ids]
    table, document = (
        ("jobs", "context") if owner_kind == "job" else ("threads", "metadata")
    )
    retirement_fields = (
        "owner.runtime_retirement_token,owner.runtime_retirement_authorized_at,"
        "owner.runtime_retirement_context,"
        if owner_kind == "thread"
        else ""
    )
    retained_prior_episode_proof = (
        """
        EXISTS (
            SELECT 1 FROM vm_creation_retries retry
            JOIN vm_thread_retained_resumes resume
              ON resume.id=retry.thread_retained_resume_id
            JOIN vm_resource_thread_cleanup_authorities prior
              ON prior.cleanup_admission_id=resume.compute_cleanup_admission_id
            JOIN vm_resource_thread_cleanup_stops stopped
              ON stopped.cleanup_admission_id=prior.cleanup_admission_id
            JOIN vm_workspace_cleanup_admissions cleanup
              ON cleanup.id=prior.cleanup_admission_id
            JOIN vm_resource_reservations charge
              ON charge.id=prior.reservation_id
            JOIN thread_runtime_retirement_outcomes terminal
              ON terminal.thread_id=prior.thread_id
             AND terminal.runtime_generation=prior.runtime_generation
             AND terminal.retirement_token=prior.retirement_token
            WHERE owner.kind='session' AND owner.execution_lane='pinned'
              AND owner.status IN ('created','active','awaiting_user')
              AND owner.runtime_retirement_token IS NULL
              AND retry.owner_kind='thread' AND retry.thread_id=owner.id
              AND retry.thread_runtime_generation=owner.runtime_generation
              AND retry.thread_agent_id IS NOT DISTINCT FROM owner.agent_id
              AND retry.thread_attach_token IS NOT DISTINCT FROM owner.runtime_attach_token
              AND retry.thread_owner_user_id IS NOT DISTINCT FROM owner.user_id
              AND retry.thread_owner_project_id IS NOT DISTINCT FROM owner.project_id
              AND retry.provision_generation::text=owner.metadata->'vm'->>'provision_generation'
              AND retry.request_id::text=owner.metadata->'vm'->>'creation_request_id'
              AND retry.thread_wake_operation_id::text IS NOT DISTINCT FROM
                  owner.metadata->'vm'->>'idle_wake_operation_id'
              AND retry.state IN ('queued','resolving','reconciling','succeeded')
              AND (
                  (owner.metadata->'vm'->>'status'='ready'
                   AND retry.state='succeeded' AND retry.ready_at IS NOT NULL)
                  OR (owner.metadata->'vm'->>'status' IN
                      ('pending','provisioning','created','ssh_pending')
                      AND retry.ready_at IS NULL)
              )
              AND public.valid_vm_thread_retained_resume_source(retry)
              AND resume.thread_id=owner.id
              AND resume.runtime_generation=owner.runtime_generation
              AND resume.request_id=retry.request_id
              AND resume.provision_generation=retry.provision_generation
              AND prior.thread_id=owner.id AND prior.purge_disk=false
              AND prior.pvc_uid=retry.expected_pvc_uid
              AND cleanup.completed_at IS NOT NULL AND cleanup.outcome='completed'
              AND stopped.accepted_at<=terminal.settled_at
              AND terminal.outcome='settled' AND terminal.disposition='ended'
              AND terminal.permanent=false
              AND charge.state='released'
              AND charge.release_evidence->>'kind'='exact_cleanup_compute_absent'
              AND charge.release_evidence->>'cleanup_admission_id'=prior.cleanup_admission_id::text
              AND owner.workspace_idle_episode->>'wait_kind'='natural_pause'
              AND owner.workspace_idle_episode#>>'{runtime_identity,owner_kind}'='thread'
              AND owner.workspace_idle_episode#>>'{runtime_identity,owner_id}'=owner.id::text
              AND owner.workspace_idle_episode#>>'{runtime_identity,backend}'='vm'
              AND owner.workspace_idle_episode#>>'{runtime_identity,runtime_generation}'=prior.provision_generation::text
              AND owner.workspace_idle_episode#>>'{runtime_identity,runtime_uid}'=prior.vm_uid::text
        ) AS retained_prior_idle_episode_proven,
        """
        if owner_kind == "thread"
        else "false AS retained_prior_idle_episode_proven,"
    )
    async with store.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT owner.id,owner.status,owner.execution_lane,owner.{document},"
            f"{retirement_fields}owner.workspace_idle_episode,owner.workspace_idle_revision,"
            f"{retained_prior_episode_proof}"
            "op.id AS idle_operation_id,op.phase AS idle_phase,op.episode_id AS idle_episode_id,"
            "op.reason AS idle_reason,op.retry_after AS idle_retry_after "
            f"FROM {table} owner LEFT JOIN LATERAL ("
            "SELECT id,phase,episode_id,reason,retry_after FROM vm_idle_operations "
            "WHERE owner_kind=$1 AND owner_id=owner.id "
            "ORDER BY admitted_at DESC,id DESC LIMIT 1) op ON TRUE "
            "WHERE owner.id=ANY($2::uuid[])",
            owner_kind,
            ids,
        )
    result = {}
    for fetched in rows:
        row = {**dict(fetched), "owner_kind": owner_kind}
        if "context" not in row and "metadata" not in row:
            # Narrow synthetic read stores may return rows for another query.
            continue
        projected = project_vm_idle_state(
            row,
            thread_creation=(current_thread_creations or {}).get(str(row["id"])),
        )
        if projected is not None:
            result[str(row["id"])] = projected
    return result
