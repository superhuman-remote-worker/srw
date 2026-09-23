"""Exact, source-transaction acknowledgment of a pinned VM Job delivery."""

from __future__ import annotations

from dataclasses import asdict
import hashlib
import hmac
import json
from typing import Any
from uuid import UUID

from orchestrator.services.vm_remote_operation import (
    VMRemoteOperationUnavailable, _identity_from_row,
)
from shared.pinned_job_delivery import pinned_job_delivery_proof
from shared.vm_lifecycle_auth import LifecycleAuthConfigurationError, configured_secret
from shared.workspace_contract import vm_mode_from_env, workspace_runtime_authority_digest


def _object(value: Any) -> dict:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return {}
    return value if isinstance(value, dict) else {}


def _uuid(value: Any) -> UUID | None:
    try:
        parsed = UUID(str(value))
        return parsed if str(parsed) == str(value) else None
    except (TypeError, ValueError, AttributeError):
        return None


async def accept_pinned_report_on_conn(
    conn: Any,
    *,
    job_id: UUID,
    agent_id: UUID,
    delivery_id: UUID,
    projection_digest: str,
    process_generation: str,
    pod_uid: str,
    delivery_proof: str,
    source_kind: str,
) -> dict[str, Any] | None:
    """Accept an exact agent report, including one racing the POST response.

    The caller holds the Job row through its existing route or completion
    transaction. This function reads the agent identity without adding an
    agent-row lock in the opposite order from dispatch and heartbeat.
    """

    if (
        not conn.is_in_transaction()
        or source_kind not in {"route", "completion"}
        or any(_uuid(value) is None for value in (job_id, agent_id, delivery_id))
        or not isinstance(delivery_proof, str)
        or len(delivery_proof) != 64
    ):
        return None
    try:
        secret = configured_secret()
    except LifecycleAuthConfigurationError:
        return None
    if secret is None:
        return None
    delivery_id, agent_id, job_id = (
        UUID(str(delivery_id)), UUID(str(agent_id)), UUID(str(job_id))
    )
    delivery = await conn.fetchrow(
        "SELECT * FROM pinned_job_deliveries WHERE id=$1 AND job_id=$2 FOR UPDATE",
        delivery_id, job_id,
    )
    if delivery is None:
        return None
    if (
        delivery["agent_id"] != agent_id
        or delivery["projection_digest"] != projection_digest
        or delivery["process_generation"] != process_generation
        or delivery["pod_uid"] != pod_uid
    ):
        return None
    expected_proof = pinned_job_delivery_proof(
        secret, delivery_id=str(delivery_id), agent_id=str(agent_id),
        process_generation=process_generation, pod_uid=pod_uid,
        projection_digest=projection_digest,
    )
    if not hmac.compare_digest(delivery_proof, expected_proof):
        return None
    job = await conn.fetchrow("SELECT * FROM jobs WHERE id=$1 FOR UPDATE", job_id)
    if (
        job is None or job["execution_lane"] != "pinned"
        or job["assigned_agent_id"] != agent_id
        or job["lease_expires_at"] is None
        or job["status"] not in {"processing", "waiting_for_reply", "pending_review"}
    ):
        return None
    context = _object(job["context"])
    marker = _object(context.get("_workspace_dispatch_authority"))
    vm = _object(context.get("vm"))
    if (
        marker != _object(delivery["original_dispatch_marker"])
        or vm.get("status") != "ready"
        or _uuid(vm.get("provision_generation")) != delivery["provision_generation"]
        or _uuid(vm.get("vm_uid")) != delivery["vm_uid"]
        or _uuid(vm.get("vmi_uid")) != delivery["vmi_uid"]
        or _uuid(vm.get("active_pod_uid")) != delivery["launcher_uid"]
        or _uuid(vm.get("rootdisk_pvc_uid")) != delivery["pvc_uid"]
    ):
        return None
    try:
        identity = _identity_from_row(
            dict(job), owner_kind="job", owner_id=str(job_id),
            operation_kind="idle_policy",
        )
    except VMRemoteOperationUnavailable:
        return None
    identity_digest = "sha256:" + hashlib.sha256(
        json.dumps(asdict(identity), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    runtime_digest = workspace_runtime_authority_digest(
        dict(job), vm_mode=vm_mode_from_env(),
    )
    if (
        runtime_digest is None
        or "sha256:" + runtime_digest != delivery["runtime_authority_digest"]
        or identity_digest != delivery["identity_digest"]
    ):
        return None
    agent = await conn.fetchrow(
        "SELECT id,hostname,pod_uid,current_job_id,metadata FROM agents WHERE id=$1",
        agent_id,
    )
    metadata = _object(agent["metadata"]) if agent is not None else {}
    if (
        agent is None
        or agent["hostname"] != delivery["pod_name"]
        or agent["pod_uid"] != delivery["pod_uid"]
        or metadata.get("dispatch_process_generation") != process_generation
        or agent["current_job_id"] not in {None, job_id}
    ):
        return None
    if delivery["accepted_at"] is None:
        delivery = await conn.fetchrow(
            "UPDATE pinned_job_deliveries SET accepted_at=clock_timestamp(),"
            "accepted_via=$2,accepted_lease_expires_at=$3 WHERE id=$1 "
            "AND accepted_at IS NULL RETURNING *",
            delivery_id, source_kind, job["lease_expires_at"],
        )
        if delivery is None:
            return None
    return dict(delivery)


async def record_pinned_wait_receipt_on_conn(
    conn: Any, *, delivery: dict, source_kind: str, source_id: UUID,
) -> dict | None:
    """Link an accepted delivery to the exact immutable route or command."""

    if (
        not conn.is_in_transaction()
        or source_kind not in {"route", "completion"}
        or _uuid(source_id) is None
        or delivery.get("accepted_at") is None
    ):
        return None
    job = await conn.fetchrow(
        "SELECT id,lease_expires_at FROM jobs WHERE id=$1 FOR UPDATE",
        delivery["job_id"],
    )
    if job is None or job["lease_expires_at"] is None:
        return None
    existing = await conn.fetchrow(
        "SELECT * FROM pinned_job_wait_receipts WHERE source_kind=$1 AND source_id=$2",
        source_kind, UUID(str(source_id)),
    )
    if existing is not None:
        return dict(existing) if existing["delivery_id"] == delivery["id"] else None
    row = await conn.fetchrow(
        "INSERT INTO pinned_job_wait_receipts "
        "(delivery_id,job_id,source_kind,source_id,lease_expires_at) "
        "VALUES($1,$2,$3,$4,$5) RETURNING *",
        delivery["id"], delivery["job_id"], source_kind,
        UUID(str(source_id)), job["lease_expires_at"],
    )
    return dict(row)


async def pinned_wait_receipt_on_conn(
    conn: Any, *, job: Any, source_kind: str, source_id: UUID,
    episode: Any = None,
) -> dict[str, Any] | None:
    """Read one accepted source and recheck the current exact assignment."""

    if source_kind not in {"route", "completion"} or _uuid(source_id) is None:
        return None
    job_id = _uuid(job["id"])
    if job_id is None or job["execution_lane"] != "pinned":
        return None
    receipt = await conn.fetchrow(
        "SELECT * FROM pinned_job_wait_receipts "
        "WHERE job_id=$1 AND source_kind=$2 AND source_id=$3",
        job_id, source_kind, UUID(str(source_id)),
    )
    if receipt is None:
        return None
    delivery = await conn.fetchrow(
        "SELECT * FROM pinned_job_deliveries WHERE id=$1 AND job_id=$2",
        receipt["delivery_id"], job_id,
    )
    if delivery is None or delivery["accepted_at"] is None:
        return None
    context = _object(job["context"])
    vm = _object(context.get("vm"))
    original_runtime = (
        _uuid(vm.get("provision_generation")) == delivery["provision_generation"]
        and _uuid(vm.get("vm_uid")) == delivery["vm_uid"]
        and _uuid(vm.get("vmi_uid")) == delivery["vmi_uid"]
        and _uuid(vm.get("active_pod_uid")) == delivery["launcher_uid"]
    )
    if (
        job["assigned_agent_id"] != delivery["agent_id"]
        or job["lease_expires_at"] is None
        or _object(context.get("_workspace_dispatch_authority"))
            != _object(delivery["original_dispatch_marker"])
        or vm.get("status") != "ready"
        or _uuid(vm.get("rootdisk_pvc_uid")) != delivery["pvc_uid"]
    ):
        return None
    try:
        identity = _identity_from_row(
            dict(job), owner_kind="job", owner_id=str(job_id),
            operation_kind="idle_policy",
        )
    except VMRemoteOperationUnavailable:
        return None
    identity_digest = "sha256:" + hashlib.sha256(
        json.dumps(asdict(identity), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    runtime_digest = workspace_runtime_authority_digest(
        dict(job), vm_mode=vm_mode_from_env(),
    )
    if original_runtime:
        if (
            runtime_digest is None
            or "sha256:" + runtime_digest != delivery["runtime_authority_digest"]
            or identity_digest != delivery["identity_digest"]
        ):
            return None
    else:
        if episode is None:
            return None
        from orchestrator.services.vm_idle_phase_approval import (
            _source_runtime_continuity,
        )

        origin = {
            "runtime_identity": {
                "owner_kind": "job", "owner_id": str(job_id), "backend": "vm",
                "runtime_generation": str(delivery["provision_generation"]),
                "runtime_uid": str(delivery["vm_uid"]),
            },
            "launcher_uid": str(delivery["launcher_uid"]),
            "rootdisk_pvc_uid": str(delivery["pvc_uid"]),
        }
        if not await _source_runtime_continuity(
            conn, job=job, episode=episode, source=origin,
            generation=identity.workspace_generation, vm_uid=identity.vm_uid,
            vmi_uid=vm.get("vmi_uid"),
            launcher_uid=identity.launcher_pod_uid,
            pvc_uid=vm.get("rootdisk_pvc_uid"),
        ):
            return None
    agent = await conn.fetchrow(
        "SELECT hostname,pod_uid,current_job_id,metadata FROM agents WHERE id=$1",
        delivery["agent_id"],
    )
    metadata = _object(agent["metadata"]) if agent is not None else {}
    if (
        agent is None or agent["hostname"] != delivery["pod_name"]
        or agent["pod_uid"] != delivery["pod_uid"]
        or metadata.get("dispatch_process_generation")
            != delivery["process_generation"]
        or agent["current_job_id"] not in {None, job_id}
    ):
        return None
    return {
        "receipt": dict(receipt), "delivery": dict(delivery),
        "access_rebound": not original_runtime,
    }


async def pinned_source_origin_on_conn(
    conn: Any, *, job_id: UUID, source_id: UUID, source: dict,
) -> bool:
    """Verify an immutable S17 pinned origin, including after access wakes."""

    delivery_id = _uuid(source.get("pinned_delivery_id"))
    if delivery_id is None:
        return False
    receipt = await conn.fetchrow(
        "SELECT * FROM pinned_job_wait_receipts WHERE job_id=$1 "
        "AND source_kind='completion' AND source_id=$2 AND delivery_id=$3",
        job_id, source_id, delivery_id,
    )
    delivery = await conn.fetchrow(
        "SELECT * FROM pinned_job_deliveries WHERE id=$1 AND job_id=$2",
        delivery_id, job_id,
    )
    runtime = _object(source.get("runtime_identity"))
    return bool(
        receipt is not None and delivery is not None
        and delivery["accepted_at"] is not None
        and runtime.get("owner_kind") == "job"
        and runtime.get("owner_id") == str(job_id)
        and runtime.get("backend") == "vm"
        and runtime.get("runtime_generation") == str(delivery["provision_generation"])
        and runtime.get("runtime_uid") == str(delivery["vm_uid"])
        and source.get("launcher_uid") == str(delivery["launcher_uid"])
        and source.get("rootdisk_pvc_uid") == str(delivery["pvc_uid"])
        and source.get("runtime_authority_digest")
            == delivery["runtime_authority_digest"].removeprefix("sha256:")
        and source.get("identity_digest")
            == delivery["identity_digest"].removeprefix("sha256:")
    )
