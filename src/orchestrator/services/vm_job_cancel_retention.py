"""Immutable retention admission for an exact cancelled, never-ready Job VM.

The rollout flag admits new authority only. Persisted authority, its deletion
fence, and typed permanent Delete remain effective when admission is disabled.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, replace
from uuid import NAMESPACE_URL, UUID, uuid5

from shared.vm_resource_admission import ResourceAdmissionError

from orchestrator.services.vm_workspace_recovery_store import (
    CleanupPermit,
    bind_vm_cleanup_permit,
    prepare_vm_cleanup_resource,
    vm_cleanup_request_identity,
)


def cancel_retention_admission_enabled(environ=None) -> bool:
    source = os.environ if environ is None else environ
    value = source.get("VM_JOB_CANCEL_RETENTION_ENABLED", "false").strip().lower()
    if value not in {"true", "false"}:
        raise ValueError("VM_JOB_CANCEL_RETENTION_ENABLED must be true or false")
    return value == "true"


def _json(value):
    return json.loads(value) if isinstance(value, str) else value


def _uuid(value):
    parsed = UUID(str(value))
    if str(parsed) != str(value):
        raise ValueError("noncanonical retention identity")
    return parsed


async def _installed(conn):
    return await conn.fetchval(
        "SELECT to_regclass('public.vm_job_cancel_retention_authorities') IS NOT NULL"
    )


async def _initial_ready_installed(conn):
    return await conn.fetchval(
        "SELECT to_regprocedure('public.vm_job_initial_ready_retention_candidate(uuid,uuid,uuid,uuid,uuid,uuid,boolean)') IS NOT NULL"
    )


def _initial_ready_wire(owner, identity, candidate, request_id, digest):
    return {
        "version": 1,
        "kind": "vm_job_initial_ready_stop_candidate_v1",
        "owner_kind": "job",
        "job_id": str(owner),
        "namespace": candidate["namespace"],
        "cluster_id": candidate["cluster_id"],
        "request_id": candidate["creation_request_id"],
        "provision_generation": identity.provision_generation,
        "reservation_id": candidate["reservation_id"],
        "reservation_revision": candidate["reservation_revision"],
        "vm_uid": identity.vm_uid,
        "vmi_uid": candidate["vmi_uid"],
        "launcher_uid": candidate["launcher_uid"],
        "node_uid": candidate["node_uid"],
        "pvc_uid": identity.rootdisk_pvc_uid,
        "cleanup_request_id": str(request_id),
        "cleanup_intent_digest": digest,
    }


async def initial_ready_retention_candidate(db, *, job_id, identity):
    """Read the exact root candidate; release all locks before signed status I/O."""
    owner, pvc = _uuid(job_id), _uuid(identity.rootdisk_pvc_uid)
    async with db.acquire() as conn, conn.transaction():
        if not await _initial_ready_installed(conn):
            return None
        for key in (f"workspace-recovery:job:{owner}", f"workspace-recovery-pvc:{pvc}"):
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", key
            )
        _, _, request_id, digest, _ = vm_cleanup_request_identity(
            owner_kind="job",
            owner_id=owner,
            identity=identity,
            source="job_terminal_vm_release",
            purge_disk=False,
        )
        old = await conn.fetchrow(
            "SELECT id FROM vm_workspace_cleanup_admissions WHERE owner_kind='job' "
            "AND owner_id=$1 AND request_id=$2 FOR UPDATE",
            owner,
            request_id,
        )
        candidate = _json(
            await conn.fetchval(
                "SELECT public.vm_job_initial_ready_retention_candidate($1,$2,$3,$4,$5)",
                owner,
                _uuid(identity.provision_generation),
                _uuid(identity.vm_uid),
                pvc,
                old["id"] if old else None,
            )
        )
        if not isinstance(candidate, dict):
            return None
        if old is not None:
            request_id = uuid5(
                NAMESPACE_URL, f"vm-job-cancel-retain-v1:{old['id']}:{digest}"
            )
        return _initial_ready_wire(owner, identity, candidate, request_id, digest)


async def read_current_initial_ready_preflight(
    db, parent_cleanup, *, job_id, generation
):
    """Recognize immutable policy3 before any proof/SSH fallback; end locks here."""
    from shared.vm_cancel_retention import valid_ready_retention_preflight

    owner = _uuid(job_id)
    async with db.acquire() as conn, conn.transaction():
        if not await _initial_ready_installed(conn):
            return False, None
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery:job:{owner}",
        )
        locator = await conn.fetchrow(
            "SELECT a.pvc_uid FROM vm_job_cancel_retention_authorities a JOIN jobs j ON j.id=a.job_id "
            "WHERE a.job_id=$1 AND a.policy_version=3 AND NOT j.context ? '_vm_job_retained_resume'",
            owner,
        )
        if locator is None:
            return False, None
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery-pvc:{locator['pvc_uid']}",
        )
        await conn.fetchrow(
            "SELECT unit_id FROM run_queue WHERE unit_id=$1 FOR UPDATE", owner
        )
        await conn.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", owner)
        authority = await conn.fetchrow(
            "SELECT a.*,c.completed_at,a.admitted_xact_id<>pg_current_xact_id() AS committed "
            "FROM vm_job_cancel_retention_authorities a JOIN vm_workspace_cleanup_admissions c "
            "ON c.id=a.cleanup_admission_id JOIN jobs j ON j.id=a.job_id "
            "WHERE a.job_id=$1 AND a.policy_version=3 AND NOT j.context ? '_vm_job_retained_resume' "
            "AND j.context->'vm'->>'provision_generation'=a.provision_generation::text "
            "AND j.context->'vm'->>'vm_uid'=a.vm_uid::text "
            "AND j.context->'vm'->>'rootdisk_pvc_uid'=a.pvc_uid::text",
            owner,
        )
        if (
            authority is None
            or not authority["committed"]
            or str(authority["provision_generation"]) != str(generation)
        ):
            raise ResourceAdmissionError("initial_ready_retention_changed")
        if (
            isinstance(parent_cleanup, dict)
            and isinstance(parent_cleanup.get("intent"), dict)
            and parent_cleanup["intent"].get("purge_disk") is True
        ):
            purge = await conn.fetchrow(
                "SELECT * FROM vm_job_retained_disk_purge_authorities WHERE job_id=$1 "
                "AND cleanup_admission_id::text=$2 AND provision_generation=$3 AND vm_uid=$4 AND pvc_uid=$5 "
                "AND admitted_xact_id<>pg_current_xact_id()",
                owner,
                parent_cleanup.get("admission_id"),
                authority["provision_generation"],
                authority["vm_uid"],
                authority["pvc_uid"],
            )
            if purge is None:
                raise ResourceAdmissionError("initial_ready_purge_unproven")
            intent = {
                **_json(authority["retaining_intent"]),
                "purge_disk": True,
                "source": "public_vm_delete",
            }
            expected = bind_vm_cleanup_permit(
                CleanupPermit(allowed=True, admission_id=purge["cleanup_admission_id"]),
                request_id=purge["cleanup_request_id"],
                intent=intent,
            ).parent_cleanup
            if parent_cleanup != expected or not await conn.fetchval(
                "SELECT public.validate_vm_job_retained_disk_purge($1,false)",
                purge["cleanup_admission_id"],
            ):
                raise ResourceAdmissionError("initial_ready_purge_changed")
            return True, None
        if authority["completed_at"] is not None:
            raise ResourceAdmissionError("initial_ready_retention_completed")
        proof = _json(authority["ready_retention_preflight"])
        candidate = _json(
            await conn.fetchval(
                "SELECT public.vm_job_retained_ready_candidate(a) FROM vm_job_cancel_retention_authorities a WHERE cleanup_admission_id=$1",
                authority["cleanup_admission_id"],
            )
        )
        expected = bind_vm_cleanup_permit(
            CleanupPermit(allowed=True, admission_id=authority["cleanup_admission_id"]),
            request_id=authority["cleanup_request_id"],
            intent=_json(authority["retaining_intent"]),
        ).parent_cleanup
        if (
            not valid_ready_retention_preflight(proof, candidate)
            or parent_cleanup != {**expected, "retention_preflight": proof}
            or not await conn.fetchval(
                "SELECT public.validate_vm_job_cancel_retention($1,false)",
                authority["cleanup_admission_id"],
            )
        ):
            raise ResourceAdmissionError("initial_ready_retention_changed")
        return True, proof


async def retention_for_admission_on_conn(conn, admission_id: UUID):
    if not await _installed(conn):
        return None
    row = await conn.fetchrow(
        "SELECT * FROM vm_job_cancel_retention_authorities "
        "WHERE cleanup_admission_id=$1 OR superseded_admission_id=$1",
        admission_id,
    )
    return dict(row) if row else None


@dataclass(frozen=True)
class JobRetainedPurgeBootstrap:
    """Private transaction-only proposal; never physical effect authority."""

    job_id: UUID
    pvc_uid: UUID
    final_request_id: UUID
    provision_generation: UUID
    cleanup_request_id: UUID
    intent_digest: str
    retention_admission_ids: frozenset[UUID]


async def guard_retention_cleanup_on_conn(
    conn,
    *,
    owner_kind: str,
    owner_id: UUID,
    pvc_uid: UUID | None,
    source: str,
    request_id: UUID,
    intent_digest: str,
    parent_admission_id: UUID | None,
    bootstrap=None,
) -> None:
    """Called under owner/PVC locks before replay, creation, or resumed effect."""

    if not await _installed(conn):
        return
    if await conn.fetchval(
        "SELECT public.vm_job_cancel_retention_cleanup_allowed($1,$2,$3,$4,$5,$6,$7,false)",
        owner_kind,
        owner_id,
        pvc_uid,
        source,
        request_id,
        intent_digest,
        parent_admission_id,
    ):
        return
    # Only the dedicated service constructs this after locked exact reads.
    # The deferred SQL constraint independently requires its complete chain.
    if (
        type(bootstrap) is JobRetainedPurgeBootstrap
        and owner_kind == "job"
        and parent_admission_id is None
        and source == "public_vm_delete"
        and bootstrap.job_id == owner_id
        and bootstrap.pvc_uid == pvc_uid
        and bootstrap.cleanup_request_id == request_id
        and bootstrap.intent_digest == intent_digest
    ):
        rows = await conn.fetch(
            "SELECT cleanup_admission_id,job_id,pvc_uid FROM vm_job_cancel_retention_authorities "
            "WHERE (job_id=$1 OR pvc_uid=$2) AND NOT "
            "public.vm_job_cancel_retention_discharged(cleanup_admission_id)",
            owner_id,
            pvc_uid,
        )
        if (
            rows
            and {r["cleanup_admission_id"] for r in rows}
            == bootstrap.retention_admission_ids
            and all(r["job_id"] == owner_id and r["pvc_uid"] == pvc_uid for r in rows)
            and await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM vm_creation_retries WHERE request_id=$1 "
                "AND owner_kind='job' AND job_id=$2 AND provision_generation=$3 "
                "AND observed_pvc_uid=$4 AND state='succeeded')",
                bootstrap.final_request_id,
                owner_id,
                bootstrap.provision_generation,
                pvc_uid,
            )
        ):
            return
    raise ResourceAdmissionError("cancel_retention_disk_protected")


async def retention_settlement_is_current_on_conn(
    conn,
    *,
    job_id: UUID,
    generation: UUID,
    admission_id: UUID,
) -> bool:
    if not await _installed(conn):
        return False
    ready_scope = "r.ready_at IS NULL"
    if await _initial_ready_installed(conn):
        ready_scope = "((a.policy_version=1 AND r.ready_at IS NULL) OR (a.policy_version=3 AND r.ready_at IS NOT NULL AND r.job_retained_resume_id IS NULL))"
    return bool(
        await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_job_cancel_retention_authorities a "
            "JOIN jobs j ON j.id=a.job_id JOIN run_queue q ON q.unit_id=j.id "
            "JOIN vm_creation_retries r ON r.request_id=a.creation_request_id "
            "WHERE a.cleanup_admission_id=$1 AND a.job_id=$2 AND a.provision_generation=$3 "
            "AND j.status='cancelled' AND j.execution_lane='stateless' "
            "AND j.parent_job_id IS NULL AND j.assigned_agent_id IS NULL "
            "AND j.context->'vm'->>'provision_generation'=a.provision_generation::text "
            "AND j.context->'vm'->>'vm_uid'=a.vm_uid::text "
            "AND j.context->'vm'->>'rootdisk_pvc_uid'=a.pvc_uid::text "
            "AND q.unit_kind='worker_batch' AND q.state='done' "
            "AND q.leased_by IS NULL AND q.leased_until IS NULL "
            f"AND r.state='succeeded' AND {ready_scope} "
            "AND NOT EXISTS(SELECT 1 FROM vm_creation_retries later WHERE later.owner_kind='job' "
            "AND later.job_id=a.job_id AND (later.created_at,later.request_id)>(r.created_at,r.request_id))) "
            "AND public.vm_job_cancel_retention_settled($1)",
            admission_id,
            job_id,
            generation,
        )
    )


async def _ready_purge_replay_on_conn(
    conn, store, *, owner, identity, generation, vm_uid, pvc
):
    """Classify an already-admitted Ready purge; grant no new cleanup authority."""
    _, _, request_id, digest, _ = vm_cleanup_request_identity(
        owner_kind="job",
        owner_id=owner,
        identity=identity,
        source="job_terminal_vm_release",
        purge_disk=True,
    )
    parent = await conn.fetchrow(
        "SELECT * FROM vm_workspace_cleanup_admissions WHERE owner_kind='job' "
        "AND owner_id=$1 AND request_id=$2 AND pvc_uid=$3 "
        "AND source='job_terminal_vm_release' AND intent_digest=$4 "
        "AND parent_admission_id IS NULL AND completed_at IS NULL AND outcome IS NULL "
        "FOR UPDATE",
        owner,
        request_id,
        pvc,
        digest,
    )
    if parent is None:
        return False
    from orchestrator.services.vm_creation_retry_store import (
        VMCreationRetryConflict,
        VMCreationRetryStore,
    )

    try:
        job = await VMCreationRetryStore(store.db)._scope(
            conn, owner, pvc, own_admission=parent["id"], hold_queue=False
        )
    except VMCreationRetryConflict:
        return False
    # Re-read after all owner/PVC/parent/queue/Job lock waits. Projection alone
    # cannot establish Ready history, and a later Ready observation is drift.
    context = _json(job["context"])
    vm = context.get("vm") if isinstance(context, dict) else None
    if (
        job["status"] != "cancelled"
        or job["execution_lane"] != "stateless"
        or job["parent_job_id"] is not None
        or job["assigned_agent_id"] is not None
        or not isinstance(vm, dict)
        or vm.get("status") not in {"retiring_process_zero", "deleting", "deleted"}
        or vm.get("workspace_storage") is not None
        or context.get("_vm_job_retained_resume") is not None
        or context.get("_vm_creation_pending") is not None
        or context.get("_stateless_cancel_cleanup_pending") is not True
        or vm.get("identity_authenticated") is not True
        or vm.get("identity_provision_generation") != str(generation)
        or vm.get("provision_generation") != str(generation)
        or vm.get("vm_uid") != str(vm_uid)
        or vm.get("rootdisk_pvc_uid") != str(pvc)
    ):
        return False
    retry = await conn.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE owner_kind='job' AND job_id=$1 "
        "AND request_id::text=$2 FOR SHARE",
        owner,
        vm.get("creation_request_id"),
    )
    if (
        retry is None
        or retry["state"] != "succeeded"
        or retry["reason"] != "creation_adopted"
        or retry["provision_generation"] != generation
        or retry["observed_vm_uid"] != vm_uid
        or retry["observed_pvc_uid"] != pvc
        or retry["ready_at"] is None
        or retry["ready_at"] >= parent["admitted_at"]
    ):
        return False
    return bool(
        await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM run_queue WHERE unit_kind='worker_batch' "
            "AND unit_id=$1 AND state='done' AND leased_by IS NULL AND leased_until IS NULL) "
            "AND NOT EXISTS(SELECT 1 FROM vm_creation_retries WHERE owner_kind='job' "
            "AND job_id=$1 AND (created_at,request_id)>($2,$3)) "
            "AND NOT EXISTS(SELECT 1 FROM vm_job_cancel_retention_authorities "
            "WHERE job_id=$1 OR pvc_uid=$4)",
            owner,
            retry["created_at"],
            retry["request_id"],
            pvc,
        )
    )


async def _bind_cancelled_boot_occupancy(conn, store, *, job, identity):
    """Account for a guest that booted after Cancel stopped phase polling.

    The caller holds the normal owner/PVC/queue/Job locks. Only the signed,
    current inventory can supply the missing VMI/launcher, through the same
    exact reservation binding used before Ready. This grants neither Ready
    nor cleanup: the complete retention predicate still runs afterwards.
    """
    from orchestrator.services.vm_resource_job_runtime import (
        installed_job_resource_store,
    )
    from shared.vm_resource_inventory import InventoryError

    context = _json(job["context"])
    vm = context.get("vm") if isinstance(context, dict) else None
    if (
        job["status"] != "cancelled"
        or job["execution_lane"] != "stateless"
        or job["parent_job_id"] is not None
        or job["assigned_agent_id"] is not None
        or not isinstance(vm, dict)
        or context.get("_stateless_cancel_cleanup_pending") is not True
        or context.get("_vm_job_retained_resume") is not None
        or vm.get("status") not in {"created", "ssh_pending", "ssh_unreachable"}
        or vm.get("ssh_verified_at") is not None
        or vm.get("active_pod_uid") is not None
        or vm.get("workspace_storage") is not None
        or vm.get("identity_authenticated") is not True
        or vm.get("identity_provision_generation") != identity.provision_generation
        or vm.get("provision_generation") != identity.provision_generation
        or vm.get("vm_uid") != identity.vm_uid
        or vm.get("rootdisk_pvc_uid") != identity.rootdisk_pvc_uid
    ):
        return
    retry = await conn.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE owner_kind='job' AND job_id=$1 "
        "ORDER BY created_at DESC,request_id DESC LIMIT 1 FOR UPDATE",
        job["id"],
    )
    if (
        retry is None
        or str(retry["request_id"]) != vm.get("creation_request_id")
        or str(retry["request_id"]) != context.get("_vm_creation_pending")
        or retry["state"] != "succeeded"
        or retry["reason"] != "creation_adopted"
        or retry["ready_at"] is not None
        or retry["claim_token"] is not None
        or retry["claim_expires_at"] is not None
        or str(retry["provision_generation"]) != identity.provision_generation
        or str(retry["observed_vm_uid"]) != identity.vm_uid
        or str(retry["observed_pvc_uid"]) != identity.rootdisk_pvc_uid
        or _json(retry["canonical_request"]).get("workspace_storage") is not None
        or not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM run_queue WHERE unit_kind='worker_batch' "
            "AND unit_id=$1 AND state='done' AND leased_by IS NULL AND leased_until IS NULL)",
            job["id"],
        )
    ):
        return
    try:
        resource = await installed_job_resource_store(
            conn, store.db, retry["controller_configuration"], fresh=False
        )
        if resource is None:
            return
        await resource._lock_policy(conn, allow_off=True)
        head = await conn.fetchrow(
            "SELECT s.document,s.digest FROM vm_resource_inventory_heads h "
            "JOIN vm_resource_inventory_snapshots s ON s.snapshot_id=h.current_snapshot_id "
            "WHERE h.cluster_id=$1 AND h.policy_digest=$2 AND NOT h.observation_conflict FOR UPDATE OF h",
            resource.inventory.cluster_id,
            resource.inventory.policy_digest,
        )
        if head is None:
            return
        snapshot = resource.inventory._snapshot(_json(head["document"]), head["digest"])
        candidates = [
            vmi for vmi in snapshot["vmis"] if vmi["vm_uid"] == identity.vm_uid
        ]
        if len(candidates) != 1 or vm.get("vmi_uid") not in (
            None,
            candidates[0]["uid"],
        ):
            return
        # Match the binder's waiter-before-reservation lock order.
        await conn.fetchrow(
            "SELECT request_id FROM vm_resource_waiters WHERE request_id=$1 FOR UPDATE",
            retry["request_id"],
        )
        charge = await conn.fetchrow(
            "SELECT state,vm_uid,vmi_uid,launcher_uid FROM vm_resource_reservations "
            "WHERE request_id=$1 FOR UPDATE",
            retry["request_id"],
        )
        if charge is None or dict(charge) != {
            "state": "reserved",
            "vm_uid": None,
            "vmi_uid": None,
            "launcher_uid": None,
        }:
            return
        # Rechecks complete/fresh inventory, source/configuration, singular
        # launcher, node, owner/generation, and immutable reservation identities.
        vmi_uid = candidates[0]["uid"]
        if not await resource.bind_observed_runtime_on_conn(
            conn,
            retry=retry,
            vm={**vm, "vmi_uid": vmi_uid},
            job_id=str(job["id"]),
            generation=identity.provision_generation,
        ):
            return
        await conn.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{vm,vmi_uid}',to_jsonb($2::text)),"
            "updated_at=clock_timestamp() WHERE id=$1",
            job["id"],
            vmi_uid,
        )
    except (ResourceAdmissionError, InventoryError):
        return


async def acquire_cancel_retention(
    store, *, job_id: str, identity, retention_preflight=None
):
    """None is reserved for clearly unrelated work, never a recognized refusal."""

    from orchestrator.services.vm_job_retained_resume import (
        acquire_retained_resume_cleanup,
    )

    try:
        owner = _uuid(job_id)
    except (TypeError, ValueError, AttributeError):
        return CleanupPermit(allowed=False, reason="cancel_retention_identity_unproven")
    continuation = await acquire_retained_resume_cleanup(
        store,
        job_id=job_id,
        identity=identity,
        retention_preflight=retention_preflight,
    )
    if continuation is not None:
        return continuation
    async with store.db.acquire() as conn, conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery:job:{owner}",
        )
        prior = None
        if await _installed(conn):
            prior = await conn.fetchrow(
                "SELECT a.*,c.completed_at,c.outcome FROM vm_job_cancel_retention_authorities a "
                "JOIN vm_workspace_cleanup_admissions c ON c.id=a.cleanup_admission_id "
                "WHERE a.job_id=$1 ORDER BY a.admitted_at DESC LIMIT 1",
                owner,
            )
        job = await conn.fetchrow("SELECT * FROM jobs WHERE id=$1", owner)
        context = _json(job["context"]) if job else None
        vm = context.get("vm") if isinstance(context, dict) else None
        if prior is None and (
            job is None
            or job["status"] != "cancelled"
            or job["execution_lane"] != "stateless"
            or not isinstance(vm, dict)
        ):
            return None
        try:
            generation = _uuid(identity.provision_generation)
            vm_uid = _uuid(identity.vm_uid)
            pvc = _uuid(identity.rootdisk_pvc_uid)
        except (TypeError, ValueError, AttributeError):
            return CleanupPermit(
                allowed=False, reason="cancel_retention_identity_unproven"
            )
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery-pvc:{pvc}",
        )
        ready_root = bool(
            await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM vm_creation_retries WHERE owner_kind='job' "
                "AND job_id=$1 AND provision_generation=$2 AND observed_vm_uid=$3 "
                "AND observed_pvc_uid=$4 AND ready_at IS NOT NULL)",
                owner,
                generation,
                vm_uid,
                pvc,
            )
        )
        if (
            prior is None
            and await _installed(conn)
            and await _ready_purge_replay_on_conn(
                conn,
                store,
                owner=owner,
                identity=identity,
                generation=generation,
                vm_uid=vm_uid,
                pvc=pvc,
            )
        ):
            # Ordinary cleanup changes the Ready projection while its original
            # True parent is still open. Resume that parent through the existing
            # stop, process-zero, signed attestation and charge-release checks.
            return None
        if prior is not None:
            if (
                prior["provision_generation"] != generation
                or prior["vm_uid"] != vm_uid
                or prior["pvc_uid"] != pvc
            ):
                return CleanupPermit(
                    allowed=False, reason="cancel_retention_identity_changed"
                )
            # Existing immutable authority wins even when flag text is invalid.
            if prior["completed_at"] is not None:
                valid = await retention_settlement_is_current_on_conn(
                    conn,
                    job_id=owner,
                    generation=generation,
                    admission_id=prior["cleanup_admission_id"],
                )
            else:
                valid = await conn.fetchval(
                    "SELECT public.validate_vm_job_cancel_retention($1,false)",
                    prior["cleanup_admission_id"],
                )
            if not valid:
                return CleanupPermit(
                    allowed=False, reason="cancel_retention_authority_changed"
                )
            permit = bind_vm_cleanup_permit(
                CleanupPermit(
                    allowed=True,
                    admission_id=prior["cleanup_admission_id"],
                    completed_outcome=prior["outcome"],
                ),
                request_id=prior["cleanup_request_id"],
                intent=_json(prior["retaining_intent"]),
            )
            if prior["policy_version"] == 3:
                permit = replace(
                    permit,
                    parent_cleanup={
                        **permit.parent_cleanup,
                        "retention_preflight": _json(
                            prior["ready_retention_preflight"]
                        ),
                    },
                )
            return permit
        if not cancel_retention_admission_enabled():
            return CleanupPermit(allowed=False, reason="cancel_retention_not_activated")
        if not await _installed(conn):
            return CleanupPermit(
                allowed=False, reason="cancel_retention_schema_unavailable"
            )
        if ready_root and not await _initial_ready_installed(conn):
            return CleanupPermit(
                allowed=False, reason="cancel_retention_schema_unavailable"
            )
        _, _, old_request, old_digest, _ = vm_cleanup_request_identity(
            owner_kind="job",
            owner_id=owner,
            identity=identity,
            source="job_terminal_vm_release",
            purge_disk=True,
        )
        _, _, request_id, digest, intent = vm_cleanup_request_identity(
            owner_kind="job",
            owner_id=owner,
            identity=identity,
            source="job_terminal_vm_release",
            purge_disk=False,
        )
        old = await conn.fetchrow(
            "SELECT * FROM vm_workspace_cleanup_admissions "
            "WHERE owner_kind='job' AND owner_id=$1 AND request_id=$2 FOR UPDATE",
            owner,
            old_request,
        )
        if old is not None and (
            old["source"] != "job_terminal_vm_release"
            or old["pvc_uid"] != pvc
            or old["intent_digest"] != old_digest
            or old["completed_at"] is not None
            or old["parent_admission_id"] is not None
        ):
            return CleanupPermit(
                allowed=False, reason="cancel_retention_old_parent_changed"
            )
        from orchestrator.services.vm_creation_retry_store import (
            VMCreationRetryConflict,
            VMCreationRetryStore,
        )

        try:
            job = await VMCreationRetryStore(store.db)._scope(
                conn,
                owner,
                pvc,
                own_admission=old["id"] if old else None,
                hold_queue=False,
            )
        except VMCreationRetryConflict as exc:
            return CleanupPermit(allowed=False, reason=str(exc))
        if not ready_root:
            await _bind_cancelled_boot_occupancy(
                conn, store, job=job, identity=identity
            )
        candidate_function = (
            "vm_job_initial_ready_retention_candidate"
            if ready_root
            else "vm_job_cancel_retention_candidate"
        )
        candidate = await conn.fetchval(
            f"SELECT public.{candidate_function}($1,$2,$3,$4,$5)",
            owner,
            generation,
            vm_uid,
            pvc,
            old["id"] if old else None,
        )
        candidate = _json(candidate)
        if not isinstance(candidate, dict):
            return CleanupPermit(
                allowed=False, reason="cancel_retention_scope_unproven"
            )
        if old is not None:
            request_id = uuid5(
                NAMESPACE_URL, f"vm-job-cancel-retain-v1:{old['id']}:{digest}"
            )
        if ready_root:
            from shared.vm_cancel_retention import valid_ready_retention_preflight

            frozen = _initial_ready_wire(owner, identity, candidate, request_id, digest)
            if not valid_ready_retention_preflight(retention_preflight, frozen):
                return CleanupPermit(
                    allowed=False, reason="initial_ready_retention_unproven"
                )
        if old is not None:
            request_id = uuid5(
                NAMESPACE_URL,
                f"vm-job-cancel-retain-v1:{old['id']}:{digest}",
            )
            await conn.execute(
                "UPDATE vm_workspace_cleanup_admissions SET "
                "completed_at=clock_timestamp(),outcome='superseded_by_retention' WHERE id=$1",
                old["id"],
            )
        permit = await store.acquire_cleanup_permit_on_conn(
            conn,
            owner_kind="job",
            owner_id=owner,
            pvc_uid=pvc,
            request_id=request_id,
            source="job_terminal_vm_release",
            intent_digest=digest,
        )
        if not permit.allowed:
            # Never commit a superseded old parent without its successor.
            raise ResourceAdmissionError("cancel_retention_successor_refused")
        permit = bind_vm_cleanup_permit(permit, request_id=request_id, intent=intent)
        await conn.execute(
            "INSERT INTO vm_job_cancel_retention_authorities "
            "(cleanup_admission_id,superseded_admission_id,job_id,creation_request_id,"
            "provision_generation,reservation_id,reservation_revision,vm_uid,vmi_uid,"
            "launcher_uid,pvc_uid,node_uid,namespace,cluster_id,cleanup_request_id,"
            "intent_digest,retaining_intent,superseded_request_id,superseded_intent_digest,policy_version,ready_retention_preflight) "
            "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17::jsonb,$18,$19,$20,$21::jsonb)",
            permit.admission_id,
            old["id"] if old else None,
            owner,
            UUID(candidate["creation_request_id"]),
            generation,
            UUID(candidate["reservation_id"]),
            candidate["reservation_revision"],
            vm_uid,
            UUID(candidate["vmi_uid"]),
            UUID(candidate["launcher_uid"]),
            pvc,
            UUID(candidate["node_uid"]),
            candidate["namespace"],
            candidate["cluster_id"],
            request_id,
            digest,
            json.dumps(intent),
            old["request_id"] if old else None,
            old["intent_digest"] if old else None,
            3 if ready_root else 1,
            json.dumps(retention_preflight) if ready_root else None,
        )
        if old is not None:
            await conn.execute(
                "UPDATE jobs SET context=jsonb_set(context,"
                "'{_job_terminal_vm_cleanup,admission_id}',to_jsonb($3::text)) "
                "WHERE id=$1 AND context->'_job_terminal_vm_cleanup'->>'admission_id'=$2",
                owner,
                str(old["id"]),
                str(permit.admission_id),
            )
        if ready_root:
            permit = replace(
                permit,
                parent_cleanup={
                    **permit.parent_cleanup,
                    "retention_preflight": retention_preflight,
                },
            )
        await prepare_vm_cleanup_resource(store, permit, _conn=conn)
        return permit


async def complete_cancel_retention_marker(db, job_id: str):
    """None means legacy cleanup; a retaining owner needs its full settled proof."""
    owner = _uuid(job_id)
    async with db.acquire() as conn, conn.transaction():
        if not await _installed(conn):
            return None
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery:job:{owner}",
        )
        authority = await conn.fetchrow(
            "SELECT * FROM vm_job_cancel_retention_authorities WHERE job_id=$1 ORDER BY admitted_at DESC LIMIT 1",
            owner,
        )
        if authority is None:
            return None
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery-pvc:{authority['pvc_uid']}",
        )
        await conn.fetchrow(
            "SELECT id FROM vm_workspace_cleanup_admissions WHERE id=$1 FOR SHARE",
            authority["cleanup_admission_id"],
        )
        await conn.fetchrow(
            "SELECT unit_id FROM run_queue WHERE unit_id=$1 FOR UPDATE", owner
        )
        job = await conn.fetchrow("SELECT id FROM jobs WHERE id=$1 FOR UPDATE", owner)
        if job is None:
            return True
        if not await retention_settlement_is_current_on_conn(
            conn,
            job_id=owner,
            generation=authority["provision_generation"],
            admission_id=authority["cleanup_admission_id"],
        ):
            return False
        disk_kept = not await conn.fetchval(
            "SELECT public.vm_job_cancel_retention_discharged($1)",
            authority["cleanup_admission_id"],
        )
        await conn.execute(
            "UPDATE jobs SET context=jsonb_set(context-'_stateless_cancel_cleanup_pending','{vm}',"
            "(context->'vm')||$2::jsonb),updated_at=clock_timestamp() WHERE id=$1",
            owner,
            json.dumps(
                {"status": "deleted", "compute_released": True, "disk_kept": disk_kept}
            ),
        )
        return True
