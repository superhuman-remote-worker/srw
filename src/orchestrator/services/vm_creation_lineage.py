"""Exact retained-instance handoff proof, composed inside creation transactions."""

import json
from uuid import UUID

from shared.vm_workspace_storage import storage_binding, storage_name
from orchestrator.services.vm_workspace_recovery_store import (
    cleanup_intent_digest,
    _job_workspace_owner,
)


def _object(value):
    value = json.loads(value) if isinstance(value, str) else value
    if value is None:
        return {}
    if not isinstance(value, dict):
        _refuse()
    return value


def _refuse():
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict

    raise VMCreationRetryConflict("creation_attachment_lineage_unproven")


async def discover(conn, job_id):
    """Read candidates before locks; this is never a grant or a receipt."""
    current = await conn.fetchrow(
        "SELECT s.id AS execution_id,i.id,i.backend_state,i.pvc_uid,i.generation "
        "FROM srw_execution_specs s JOIN srw_execution_workspace_bindings b ON b.execution_id=s.id "
        "JOIN srw_workspace_instances i ON i.id=b.instance_id WHERE s.work_kind='Job' AND s.work_id=$1",
        job_id,
    )
    if not current:
        return None
    try:
        binding = storage_binding(_object(current["backend_state"])["storage"])
    except (ValueError, KeyError, TypeError):
        _refuse()
    if binding["owner_id"] == str(job_id):
        return None
    if (
        binding["owner_kind"] != "job"
        or binding["generation"] < 2
        or not binding["pvc_uid"]
    ):
        _refuse()
    history = await conn.fetch(
        "SELECT s.id AS execution_id,s.revision,s.generation AS execution_generation,s.harness_adapter,j.id,j.context FROM srw_execution_workspace_bindings b "
        "JOIN srw_execution_specs s ON s.id=b.execution_id JOIN jobs j ON s.work_kind='Job' AND j.id=s.work_id "
        "WHERE b.instance_id=$1 AND j.id<>$2 ORDER BY s.id",
        current["id"],
        job_id,
    )
    matches = []
    for item in history:
        context = _object(item["context"])
        seen = []
        for name in ("vm", "last_vm"):
            vm = context.get(name)
            if not isinstance(vm, dict) or vm in seen:
                continue
            seen.append(vm)
            old = vm.get("workspace_storage")
            try:
                old = storage_binding(old)
            except (ValueError, TypeError, KeyError):
                continue
            if (
                all(
                    old[key] == binding[key]
                    for key in ("uid", "owner_kind", "owner_id")
                )
                and old["pvc_uid"] in (None, binding["pvc_uid"])
                and vm.get("rootdisk_pvc_uid") == binding["pvc_uid"]
            ):
                # Freeze only the authenticated predecessor identity and its
                # retirement/source facts, not mutable progress or credentials.
                facts = {
                    key: vm.get(key)
                    for key in (
                        "status",
                        "provision_generation",
                        "identity_provision_generation",
                        "identity_authenticated",
                        "vm_uid",
                        "rootdisk_pvc_uid",
                        "workspace_storage",
                        "retirement_cleanup_pending",
                        "preparation",
                        "preparation_request",
                    )
                }
                matches.append((item, facts, old["generation"]))
    previous = [m for m in matches if m[2] == binding["generation"] - 1]
    original = [
        m for m in matches if m[2] == 1 and str(m[0]["id"]) == binding["owner_id"]
    ]
    if len(previous) != 1 or len(original) != 1:
        _refuse()
    prev, vm, _ = previous[0]
    first, first_vm, _ = original[0]
    if any(item["harness_adapter"] != "srw/v1" for item in (prev, first)):
        _refuse()
    # Every historical row can contribute to predecessor uniqueness. Lock all
    # such Jobs in the caller's sorted scope, including currently nonmatching
    # candidates, so a late metadata write cannot introduce an unseen match.
    members = {job_id, *(item["id"] for item in history)}
    parents = await conn.fetch(
        "SELECT id,parent_job_id FROM jobs WHERE id=ANY($1::uuid[]) ORDER BY id",
        sorted(members),
    )
    if len(parents) != len(members):
        _refuse()
    owners = members | {r["parent_job_id"] for r in parents if r["parent_job_id"]}
    return {
        "binding": binding,
        "execution_id": str(current["execution_id"]),
        "previous_execution_id": str(prev["execution_id"]),
        "previous_execution_revision": prev["revision"],
        "previous_execution_generation": prev["execution_generation"],
        "previous_job_id": str(prev["id"]),
        "previous_vm": vm,
        "original_execution_id": str(first["execution_id"]),
        "original_execution_revision": first["revision"],
        "original_execution_generation": first["execution_generation"],
        "original_vm": first_vm,
        "members": [str(v) for v in sorted(members)],
        "owners": [str(v) for v in sorted(owners)],
        "parents": {
            str(r["id"]): str(r["parent_job_id"]) if r["parent_job_id"] else None
            for r in parents
        },
    }


async def prove(conn, *, job_id, binding, scope=None, expected=None, adoption=False):
    """After owner/PVC, queue/Job, execution and retry locks, lock bindings/instance.

    Re-read the discovery after the instance lock: catalog reserve/delete uses
    that same row, and a changed predecessor/membership cannot expand our locks.
    """
    discovered = await discover(conn, job_id)
    if not discovered or discovered["binding"] != binding:
        _refuse()
    if scope is not None and discovered != scope:
        _refuse()
    execution_ids = sorted(
        {
            UUID(discovered[k])
            for k in ("execution_id", "previous_execution_id", "original_execution_id")
        }
    )
    await conn.fetch(
        "SELECT execution_id FROM srw_execution_workspace_bindings WHERE execution_id=ANY($1::uuid[]) ORDER BY execution_id FOR SHARE",
        execution_ids,
    )
    instance = await conn.fetchrow(
        "SELECT * FROM srw_workspace_instances WHERE id=$1 FOR UPDATE",
        UUID(binding["uid"]),
    )
    if await discover(conn, job_id) != discovered:
        _refuse()
    if (
        not instance
        or str(instance["execution_id"]) != discovered["execution_id"]
        or instance["generation"] != binding["generation"]
        or instance["status"]
        not in (
            {"Reserved", "Attached", "Deleting"}
            if adoption
            else {"Reserved", "Attached"}
        )
        or instance["pvc_uid"] != binding["pvc_uid"]
        or instance["pvc_name"] != storage_name(binding)
        or _object(instance["recipe"]).get("backend") != "vm"
        or _object(instance["recipe"]).get("retention") != "Retain"
    ):
        _refuse()
    for identity, vm in (
        (binding["owner_id"], discovered["original_vm"]),
        (discovered["previous_job_id"], discovered["previous_vm"]),
    ):
        job = await conn.fetchrow(
            "SELECT status,context,parent_job_id FROM jobs WHERE id=$1", UUID(identity)
        )
        owner, ambiguous = _job_workspace_owner(UUID(identity), job)
        from orchestrator.database.postgres import _completion_control_active_sql

        controls = await conn.fetchval(
            "SELECT ("
            + _completion_control_active_sql("context")
            + ") FROM jobs WHERE id=$1",
            UUID(identity),
        )
        current_vm = _object(_object(job["context"]).get("vm")) if job else {}
        if (
            not job
            or job["status"] not in {"completed", "failed", "cancelled"}
            or ambiguous
            or owner != UUID(identity)
            or controls
            or any(
                key in _object(job["context"])
                for key in (
                    "_stateless_cancel_cleanup_pending",
                    "_stateless_delete_pending",
                )
            )
            or vm.get("preparation") is not None
            or vm.get("preparation_request") is not None
            or await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM run_queue WHERE unit_id=$1 AND state='leased')",
                UUID(identity),
            )
            # A historical candidate cannot override contradictory or missing
            # current retirement identity, even within the same generation.
            or any(current_vm.get(key) != value for key, value in vm.items())
            or vm.get("status") != "deleted"
            or vm.get("retirement_cleanup_pending") is True
            or vm.get("identity_authenticated") is not True
            or vm.get("identity_provision_generation") != vm.get("provision_generation")
        ):
            _refuse()
        for field in ("provision_generation", "vm_uid", "rootdisk_pvc_uid"):
            try:
                if str(UUID(vm[field])) != vm[field]:
                    _refuse()
            except (ValueError, TypeError, KeyError):
                _refuse()
    vm = discovered["previous_vm"]
    previous_job = UUID(discovered["previous_job_id"])
    receipt = await conn.fetchval(
        "SELECT id FROM managed_repository_process_zero_receipts WHERE owner_kind='job' AND owner_id=$1 "
        "AND scope='vm' AND provisioner='vm' AND runtime_incarnation=$2",
        previous_job,
        vm["provision_generation"],
    )
    if not receipt:
        _refuse()
    cleanups = await conn.fetch(
        "SELECT * FROM vm_workspace_cleanup_admissions WHERE owner_kind='job' AND owner_id=$1 AND pvc_uid=$2 AND completed_at IS NOT NULL ORDER BY id",
        previous_job,
        UUID(binding["pvc_uid"]),
    )
    retain, detached = [], []
    for cleanup in cleanups:
        intent = {
            "owner_kind": "job",
            "owner_id": str(previous_job),
            "pvc_uid": binding["pvc_uid"],
            "source": cleanup["source"],
        }
        if cleanup["source"] == "retained_workspace_detach":
            intent.update(
                generation=vm["provision_generation"],
                resource="retained_workspace_binding",
            )
            matches = detached
            outcome = "retained_workspace_detached"
        else:
            intent.update(
                provision_generation=vm["provision_generation"],
                vm_uid=vm["vm_uid"],
                purge_disk=False,
                resource="vm_workspace",
            )
            matches = retain
            outcome = "completed"
        if cleanup["outcome"] == outcome and cleanup[
            "intent_digest"
        ] == cleanup_intent_digest(intent):
            matches.append((cleanup, intent))
    if len(retain) != 1 or len(detached) != 1:
        _refuse()
    cleanup, vm_intent = retain[0]
    detach, detach_intent = detached[0]
    proof = {
        "kind": "retained_attachment_handoff",
        "version": 1,
        "storage_owner_id": binding["owner_id"],
        "current_job_id": str(job_id),
        "instance_id": binding["uid"],
        "attachment_generation": binding["generation"],
        "previous_job_id": str(previous_job),
        "previous_execution_id": discovered["previous_execution_id"],
        "previous_attachment_generation": binding["generation"] - 1,
        "provision_generation": vm["provision_generation"],
        "vm_uid": vm["vm_uid"],
        "pvc_uid": binding["pvc_uid"],
        "receipt_id": str(receipt),
        "cleanup_admission_id": str(cleanup["id"]),
        "cleanup_intent": vm_intent,
        "detach_admission_id": str(detach["id"]),
        "detach_intent": detach_intent,
        "scope": discovered,
    }
    if expected is not None and proof != expected:
        _refuse()
    return proof, cleanup["id"]
