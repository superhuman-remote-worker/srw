"""Coordinate-free VM idle state for already-authorized owner reads.

The caller supplies only IDs which its route has already authorized.  The
database read is batched for lists; no lease is acquired or renewed here.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any
from uuid import UUID

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


def project_vm_idle_state(row: dict[str, Any]) -> dict[str, Any] | None:
    """Project one owner and its latest operation without private identity."""
    owner_kind = row["owner_kind"]
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
    store: Any, *, owner_kind: str, owner_ids: list[str]
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
    async with store.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT owner.id,owner.status,owner.execution_lane,owner.{document},"
            "owner.workspace_idle_episode,owner.workspace_idle_revision,"
            "op.phase AS idle_phase,op.episode_id AS idle_episode_id,"
            "op.reason AS idle_reason,op.retry_after AS idle_retry_after "
            f"FROM {table} owner LEFT JOIN LATERAL ("
            "SELECT phase,episode_id,reason,retry_after FROM vm_idle_operations "
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
        projected = project_vm_idle_state(row)
        if projected is not None:
            result[str(row["id"])] = projected
    return result
