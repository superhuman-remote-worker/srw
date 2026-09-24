"""Settle final creation Ready for exact worker-disabled gate fixtures.

The production readiness writer owns every release predicate and mutation.
This adapter only limits which already-frozen fixture may ask it to run.
"""

from __future__ import annotations

import json
from uuid import UUID

from orchestrator.services.vm_creation_preflight import _preflight
from orchestrator.services.vm_creation_readiness import VMCreationReadinessStore
from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict
from orchestrator.services.vm_creation_transport import validate_creation_resolution
from shared.vm_creation_retry import canonical_request_digest


_MARKERS = frozenset({
    "vm_retained_resume_acceptance_gate",
    "vm_workspace_recovery_acceptance_gate",
})


def _object(value: object) -> dict:
    if isinstance(value, str):
        value = json.loads(value)
    return value if isinstance(value, dict) else {}


def _uuid(value: str) -> UUID | None:
    try:
        parsed = UUID(value)
        return parsed if str(parsed) == value else None
    except (TypeError, ValueError, AttributeError):
        return None


def creation_source_matches(vm: dict, preflight: dict, retry: dict) -> bool:
    """Bind original caller intent and authenticated resolution to one retry."""
    canonical = _object(retry.get("canonical_request"))
    if not canonical:
        return False
    try:
        digest = canonical_request_digest(canonical)
        if digest != retry.get("request_digest"):
            return False
        snapshot_value = vm.get("creation_request")
        if snapshot_value is None:
            # Older exact fixtures have no captured controller resolution.
            return (
                canonical == preflight["request"]
                and digest == preflight["request_digest"]
            )
        snapshot = _object(snapshot_value)
        if (
            type(snapshot.get("version")) is not int
            or snapshot["version"] != 1
            or snapshot.get("initial_request") is not True
            or snapshot.get("controller_configuration_authenticated") is not True
            or snapshot.get("provision_generation")
            != preflight["request"]["provision_generation"]
            or snapshot.get("request") != canonical
            or snapshot.get("request_digest") != digest
            or snapshot.get("controller_configuration_digest")
            != retry.get("controller_configuration_digest")
        ):
            return False
        validate_creation_resolution(
            preflight["request"],
            {
                "creation_retry_protocol": 1,
                "request": canonical,
                "request_digest": digest,
                "controller_configuration": snapshot["controller_configuration"],
                "controller_configuration_digest": snapshot["controller_configuration_digest"],
            },
            allow_legacy_floor=True,
        )
        return True
    except (ValueError, TypeError, KeyError, AttributeError):
        return False


async def _matches(
    db, *, job_id: UUID, owner_id: UUID, run_id: str, marker_key: str,
    request_id: UUID, generation: UUID, vm_uid: UUID, pvc_uid: UUID,
    released: bool,
) -> bool:
    async with db.acquire() as conn:
        job = await conn.fetchrow(
            "SELECT id,user_id,status::text AS status,execution_lane,"
            "assigned_agent_id,context FROM jobs WHERE id=$1", job_id,
        )
        retry = await conn.fetchrow(
            "SELECT request_id,job_id,provision_generation,state,reason,"
            "boot_counted,ready_at,canonical_request,request_digest,"
            "controller_configuration_digest,"
            "observed_vm_uid,observed_pvc_uid,execution_id,execution_revision,"
            "execution_generation,admission_deadline "
            "FROM vm_creation_retries WHERE request_id=$1", request_id,
        )
        queue = await conn.fetchrow(
            "SELECT state,leased_by,lease_token FROM run_queue "
            "WHERE unit_kind='worker_batch' AND unit_id=$1", job_id,
        )
        attempts = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM worker_batch_attempts WHERE job_id=$1)",
            job_id,
        )
    if not job or not retry or not queue or attempts:
        return False
    try:
        context = _object(job["context"])
        vm = _object(context.get("vm"))
        preflight = _preflight(vm)
        if not preflight:
            return False
        source_matches = creation_source_matches(vm, preflight, retry)
    except (VMCreationRetryConflict, ValueError, KeyError, TypeError):
        return False
    return (
        job["user_id"] == owner_id
        and job["status"] == "paused"
        and job["execution_lane"] == "stateless"
        and job["assigned_agent_id"] is None
        and context.get(marker_key) == run_id
        and not any(context.get(key) is not None for key in (
            "_workspace_dispatch_authority", "_completion_control_claim",
            "_stateless_control_claim", "_operator_pause_hold",
        ))
        and context.get("_vm_creation_pending")
        == (None if released else str(request_id))
        and vm.get("status") == "ready"
        and vm.get("identity_authenticated") is True
        and vm.get("identity_provision_generation") == str(generation)
        and vm.get("creation_request_id") == str(request_id)
        and vm.get("provision_generation") == str(generation)
        and vm.get("vm_uid") == str(vm_uid)
        and vm.get("rootdisk_pvc_uid") == str(pvc_uid)
        and preflight["request_id"] == str(request_id)
        and source_matches
        and preflight["execution_id"] == str(retry["execution_id"])
        and preflight["execution_revision"] == retry["execution_revision"]
        and preflight["execution_generation"] == retry["execution_generation"]
        and preflight["admission_deadline"] == (
            retry["admission_deadline"].isoformat()
            if retry["admission_deadline"] is not None else None
        )
        and retry["job_id"] == job_id
        and retry["provision_generation"] == generation
        and retry["state"] == "succeeded"
        and retry["reason"] == "creation_adopted"
        and retry["boot_counted"] is True
        and retry["observed_vm_uid"] == vm_uid
        and retry["observed_pvc_uid"] == pvc_uid
        and (retry["ready_at"] is not None) == released
        and (queue["state"], queue["leased_by"], queue["lease_token"])
        == ("done", None, 0)
    )


async def settle_owned_fixture_ready(
    *, db, job_id: str, owner_id: str, run_id: str, marker_key: str,
    request_id: str, generation: str, vm_uid: str, pvc_uid: str,
) -> bool:
    """Ask the real writer once, then recheck the exact fixture authority."""
    if marker_key not in _MARKERS or not isinstance(run_id, str) or not run_id:
        return False
    parsed = tuple(_uuid(value) for value in (
        job_id, owner_id, request_id, generation, vm_uid, pvc_uid,
    ))
    if any(value is None for value in parsed):
        return False
    job, owner, request, gen, vm, pvc = parsed
    keys = dict(
        db=db, job_id=job, owner_id=owner, run_id=run_id,
        marker_key=marker_key, request_id=request, generation=gen,
        vm_uid=vm, pvc_uid=pvc,
    )
    if await _matches(**keys, released=True):
        return True
    if not await _matches(**keys, released=False):
        return False
    if not await VMCreationReadinessStore(db).release(request_id=request_id):
        return False
    return await _matches(**keys, released=True)
