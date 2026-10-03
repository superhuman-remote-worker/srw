"""Exact pinned End accounting, including adopted sources that never became Ready.

The database owns the authority predicate. Controller I/O is performed by the
shared cleanup completion path between preparation and the final transaction.
"""

import asyncio
import json
from uuid import UUID

from shared.vm_resource_admission import ResourceAdmissionError
from orchestrator.services.vm_resource_reservation_store import _json


async def thread_cleanup_scope(conn, recovery_store, permit, proof):
    from orchestrator.services.vm_resource_job_runtime import (
        installed_job_resource_store,
    )

    intent = proof["intent"]
    try:
        owner = UUID(intent["owner_id"])
        generation = UUID(intent["provision_generation"])
        pvc = UUID(intent["pvc_uid"])
    except (KeyError, ValueError, TypeError):
        raise ResourceAdmissionError("resource_cleanup_identity_unproven") from None
    # Same owner/PVC order as cleanup admission and creation authority.
    await conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        f"workspace-recovery:thread:{owner}",
    )
    await conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        f"workspace-recovery-pvc:{pvc}",
    )
    thread = await conn.fetchrow("SELECT * FROM threads WHERE id=$1 FOR UPDATE", owner)
    retry = await conn.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE owner_kind='thread' "
        "AND thread_id=$1 AND provision_generation=$2 FOR UPDATE",
        owner,
        generation,
    )
    if retry is None:
        return None
    version = _json(retry["controller_configuration"]).get("version")
    nonquota = version == 1 and proof["intent"]["purge_disk"] is False
    if version != 3 and not nonquota:
        return None
    if nonquota:
        from orchestrator.services.vm_thread_network import verified_source

        if (
            verified_source(retry, thread_id=str(owner), generation=str(generation))
            is None
        ):
            raise ResourceAdmissionError("resource_cleanup_source_unproven")
    if thread is None:
        raise ResourceAdmissionError("resource_cleanup_identity_unproven")
    cleanup = await conn.fetchrow(
        "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1 FOR UPDATE",
        permit.admission_id,
    )
    charge = await conn.fetchrow(
        "SELECT state FROM vm_resource_reservations WHERE request_id=$1 "
        "ORDER BY revision DESC LIMIT 1",
        retry["request_id"],
    )
    if charge is None and not nonquota:
        raise ResourceAdmissionError("resource_cleanup_charge_unproven")
    if nonquota:
        recorded_stop = await conn.fetchval(
            "SELECT CASE WHEN a.runtime_generation IS DISTINCT FROM t.runtime_generation "
            "AND t.status='created' AND NOT t.runtime_authority_exposed "
            "AND t.agent_id IS NULL AND t.runtime_attach_token IS NULL "
            "AND t.control_admission_agent_id IS NULL "
            "AND t.runtime_retirement_token IS NOT NULL "
            "AND t.runtime_retirement_authorized_at IS NOT NULL "
            "AND t.runtime_retirement_permanent IS FALSE "
            "AND t.runtime_retirement_context->>'thread_id'=t.id::text "
            "AND t.runtime_retirement_context->>'generation'=t.runtime_generation::text "
            "AND t.runtime_retirement_context->>'settle_status'='ended' "
            "AND t.runtime_retirement_context->'runtime_authority_exposed'='false'::jsonb "
            "AND t.runtime_retirement_context->'agent_id'='null'::jsonb "
            "AND t.runtime_retirement_context->'runtime_attach_token'='null'::jsonb "
            "AND t.runtime_retirement_context->'control_admission_agent_id'='null'::jsonb "
            "AND NOT EXISTS (SELECT 1 FROM agents actor WHERE actor.thread_id=t.id) "
            "AND EXISTS (SELECT 1 FROM vm_thread_retained_resumes op "
            "WHERE op.thread_id=t.id AND op.compute_cleanup_admission_id=a.cleanup_admission_id "
            "AND public.valid_vm_thread_retained_runtime(op,t) "
            "AND t.runtime_retirement_context->'vm'=op.retained_vm "
            "AND NOT EXISTS (SELECT 1 FROM vm_creation_retries r WHERE r.request_id=op.request_id) "
            "AND NOT EXISTS (SELECT 1 FROM vm_resource_waiters w WHERE w.request_id=op.request_id) "
            "AND NOT EXISTS (SELECT 1 FROM vm_resource_reservations r WHERE r.request_id=op.request_id) "
            "AND NOT EXISTS (SELECT 1 FROM vm_creation_effects e WHERE e.request_id=op.request_id)) "
            "THEN public.validate_vm_thread_retained_compute(a.cleanup_admission_id) "
            "ELSE public.validate_vm_thread_cleanup_stop(a,s,true) END "
            "FROM vm_resource_thread_cleanup_authorities a "
            "JOIN vm_resource_thread_cleanup_stops s USING(cleanup_admission_id) "
            "JOIN threads t ON t.id=a.thread_id "
            "WHERE a.cleanup_admission_id=$1",
            cleanup["id"],
        )
        if recorded_stop is True:
            return None
        return ThreadCleanupResource(None, nonquota=True), retry, thread, cleanup, proof
    if charge["state"] == "released":
        return None
    resource = await installed_job_resource_store(
        conn,
        recovery_store.db,
        retry["controller_configuration"],
        fresh=False,
    )
    if resource is None:
        raise ResourceAdmissionError("resource_cleanup_policy_unproven")
    return ThreadCleanupResource(resource), retry, thread, cleanup, proof


class ThreadCleanupResource:
    def __init__(self, resource, *, nonquota=False):
        self.resource = resource
        self.nonquota = nonquota

    async def mark_cleanup_teardown_on_conn(self, conn, *, retry, job, cleanup, intent):
        if not self.nonquota:
            await self.resource._lock_policy(conn, allow_off=True)
        charge = await conn.fetchrow(
            "SELECT * FROM vm_resource_reservations WHERE request_id=$1 "
            "AND state<>'released' FOR UPDATE",
            retry["request_id"],
        )
        if cleanup is None or (charge is None and not self.nonquota):
            raise ResourceAdmissionError("resource_cleanup_charge_unproven")
        if self.nonquota:
            if charge is not None:
                raise ResourceAdmissionError("resource_cleanup_charge_unproven")
            vm = _json(job["metadata"]).get("vm") or {}
            # NULL records absence of quota authority. Physical identities still
            # come from the captured native VM, before its delete effect.
            charge = {
                "id": None,
                "revision": None,
                "state": "teardown",
                "vmi_uid": UUID(vm["vmi_uid"]) if vm.get("vmi_uid") else None,
                "launcher_uid": UUID(vm["active_pod_uid"])
                if vm.get("active_pod_uid")
                else None,
            }
        authority = await conn.fetchrow(
            "SELECT * FROM vm_resource_thread_cleanup_authorities "
            "WHERE cleanup_admission_id=$1",
            cleanup["id"],
        )
        if authority is None:
            context = _json(job["runtime_retirement_context"])
            if job["runtime_retirement_token"] is not None:
                generation = job["runtime_generation"]
                token = job["runtime_retirement_token"]
                agent, attach = job["agent_id"], job["runtime_attach_token"]
            else:
                # Only a surviving exact soft-End outcome can reconstruct old
                # authority. No outcome, newer generation, or Resume is guessed.
                outcome = await conn.fetchrow(
                    "SELECT * FROM thread_runtime_retirement_outcomes "
                    "WHERE thread_id=$1 AND runtime_generation=$2 AND NOT permanent "
                    "AND disposition='ended' AND outcome='settled' "
                    "ORDER BY settled_at DESC LIMIT 1",
                    job["id"],
                    retry["thread_runtime_generation"],
                )
                if outcome is None:
                    raise ResourceAdmissionError("resource_cleanup_retirement_unproven")
                generation, token = (
                    outcome["runtime_generation"],
                    outcome["retirement_token"],
                )
                agent, attach = outcome["agent_id"], outcome["runtime_attach_token"]
                context = None
            authority = await conn.fetchrow(
                "INSERT INTO vm_resource_thread_cleanup_authorities "
                "(cleanup_admission_id,reservation_id,reservation_revision,request_id,"
                "thread_id,runtime_generation,retirement_token,agent_id,attach_token,"
                "provision_generation,vm_uid,pvc_uid,vmi_uid,launcher_uid,purge_disk,"
                "intent_digest,cleanup_request_id,retirement_context) "
                "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18::jsonb) "
                "RETURNING *",
                cleanup["id"],
                charge["id"],
                charge["revision"],
                retry["request_id"],
                job["id"],
                generation,
                token,
                agent,
                attach,
                retry["provision_generation"],
                retry["observed_vm_uid"],
                retry["observed_pvc_uid"],
                charge["vmi_uid"],
                charge["launcher_uid"],
                intent["intent"]["purge_disk"],
                intent["intent_digest"],
                UUID(intent["request_id"]),
                json.dumps(context) if context is not None else None,
            )
        if (
            authority["intent_digest"] != intent["intent_digest"]
            or str(authority["cleanup_request_id"]) != intent["request_id"]
            or authority["purge_disk"] is not intent["intent"]["purge_disk"]
        ):
            raise ResourceAdmissionError("resource_cleanup_identity_unproven")
        await conn.fetchval(
            "SELECT public.validate_vm_thread_cleanup_authority(a) "
            "FROM vm_resource_thread_cleanup_authorities a WHERE cleanup_admission_id=$1",
            cleanup["id"],
        )
        # A never-Ready reservation deliberately keeps its runtime binding NULL.
        # Its adopted VM UID belongs to the source, not to a synthetic Ready.
        if not self.nonquota and charge["state"] != "teardown":
            await conn.execute(
                "UPDATE vm_resource_reservations SET state='teardown' WHERE id=$1",
                charge["id"],
            )
        return self.candidate(authority)

    @staticmethod
    def candidate(authority):
        return {
            "owner_kind": "thread",
            "owner_id": str(authority["thread_id"]),
            **{
                key: str(authority[key]) if authority[key] is not None else None
                for key in (
                    "provision_generation",
                    "vm_uid",
                    "pvc_uid",
                    "vmi_uid",
                    "launcher_uid",
                )
            },
            "purge_disk": authority["purge_disk"],
        }

    async def release_cleanup_compute_on_conn(
        self, conn, *, retry, job, cleanup, intent, proof
    ):
        candidate = await self.mark_cleanup_teardown_on_conn(
            conn,
            retry=retry,
            job=job,
            cleanup=cleanup,
            intent=intent,
        )
        expected = {
            "version": 1,
            "kind": "vm_cleanup_physical_stop",
            **{key: value for key, value in candidate.items() if key != "purge_disk"},
            "vm_absent": True,
            "vmi_absent": True,
            "launcher_absent": True,
            "same_generation_replacement": False,
            "controller_authenticated": True,
            "pvc_disposition": "purged" if candidate["purge_disk"] else "retained",
        }
        if proof != expected:
            raise ResourceAdmissionError("resource_cleanup_stop_unproven")
        zero = await conn.fetchval(
            "SELECT id FROM managed_repository_process_zero_receipts "
            "WHERE owner_kind='thread' AND owner_id=$1 AND scope='vm' "
            "AND provisioner='vm' AND runtime_incarnation=$2",
            job["id"],
            str(retry["provision_generation"]),
        )
        if zero is None:
            raise ResourceAdmissionError("resource_cleanup_stop_unproven")
        await conn.execute(
            "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),"
            "outcome='completed' WHERE id=$1 AND completed_at IS NULL",
            cleanup["id"],
        )
        await conn.execute(
            "INSERT INTO vm_resource_thread_cleanup_stops "
            "(cleanup_admission_id,process_zero_receipt_id,stop_evidence) "
            "VALUES($1,$2,$3::jsonb)",
            cleanup["id"],
            zero,
            json.dumps(proof),
        )
        if self.nonquota:
            return True
        await conn.execute(
            "UPDATE vm_resource_reservations r SET state='released',released_at=clock_timestamp(),"
            "release_evidence=jsonb_build_object('kind','exact_cleanup_compute_absent',"
            "'owner_kind','thread','thread_id',a.thread_id,'cleanup_admission_id',a.cleanup_admission_id,"
            "'reservation_revision',a.reservation_revision,'stop_evidence_digest',"
            "'sha256:'||encode(sha256(convert_to(s.stop_evidence::text,'UTF8')),'hex')) "
            "FROM vm_resource_thread_cleanup_authorities a "
            "JOIN vm_resource_thread_cleanup_stops s USING(cleanup_admission_id) "
            "WHERE a.cleanup_admission_id=$1 AND r.id=a.reservation_id",
            cleanup["id"],
        )
        await conn.execute(
            "UPDATE vm_resource_waiters SET state='released',revision=revision+1 "
            "WHERE request_id=$1 AND state='admitted'",
            retry["request_id"],
        )
        return True


async def reconcile_settled_thread_cleanup(
    recovery_store,
    provisioner,
    *,
    limit=25,
    after=None,
    timeout_seconds=5.0,
):
    """Bounded nomination; every candidate goes through the native locked guards.

    The cursor advances over refused history too, so one incomplete old End
    cannot starve later eligible charges. No new destructive permit is minted.
    """
    import asyncpg
    from orchestrator.services.vm_workspace_recovery_store import (
        CleanupPermit,
        bind_vm_cleanup_permit,
        complete_vm_cleanup_permit,
    )

    if not 0 < timeout_seconds <= 30:
        raise ValueError("maintenance_timeout")
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("maintenance_limit")
    try:
        # Nomination shares the probe budget: a blocked table or exhausted
        # connection pool must not stall the rest of the maintenance sweep.
        async with asyncio.timeout_at(deadline), recovery_store.db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT c.*,s.provision_generation,s.observed_vm_uid "
                "FROM vm_workspace_cleanup_admissions c "
                "JOIN threads t ON t.id=c.owner_id AND t.status='ended' "
                "JOIN vm_creation_retries s ON s.thread_id=t.id AND s.owner_kind='thread' "
                "AND s.observed_pvc_uid=c.pvc_uid AND s.state='succeeded' "
                "JOIN vm_resource_reservations r ON r.request_id=s.request_id AND r.state<>'released' "
                "WHERE c.owner_kind='thread' AND c.source='pinned_thread_retirement' "
                "AND c.outcome='completed' AND c.completed_at IS NOT NULL "
                "AND ($1::timestamptz IS NULL OR (c.admitted_at,c.id)>($1,$2::uuid)) "
                "ORDER BY c.admitted_at,c.id LIMIT $3",
                after[0] if after else None,
                UUID(str(after[1])) if after else None,
                limit,
            )
    except TimeoutError:
        return {
            "after": after,
            "results": [],
            "reason": "resource_cleanup_nomination_timeout",
        }
    results = []
    cursor = after
    for row in rows:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            break
        cursor = (row["admitted_at"], str(row["id"]))
        intent = {
            "owner_kind": "thread",
            "owner_id": str(row["owner_id"]),
            "provision_generation": str(row["provision_generation"]),
            "vm_uid": str(row["observed_vm_uid"]),
            "pvc_uid": str(row["pvc_uid"]),
            "purge_disk": False,
            "resource": "vm_workspace",
            "source": row["source"],
        }
        permit = bind_vm_cleanup_permit(
            CleanupPermit(
                allowed=True, admission_id=row["id"], completed_outcome="completed"
            ),
            request_id=row["request_id"],
            intent=intent,
        )
        try:
            # SQL compares this recomputed soft intent with the original digest.
            # This deadline belongs only to maintenance. Native End keeps its
            # controller timeouts. Cancellation rolls back any active SQL
            # transaction and never converts an uncertain probe into release.
            await asyncio.wait_for(
                complete_vm_cleanup_permit(
                    recovery_store,
                    permit,
                    outcome="completed",
                    provisioner=provisioner,
                ),
                timeout=remaining,
            )
            results.append({"admission_id": str(row["id"]), "state": "released"})
        except TimeoutError:
            results.append(
                {
                    "admission_id": str(row["id"]),
                    "state": "held",
                    "reason": "resource_cleanup_probe_timeout",
                }
            )
        except (ResourceAdmissionError, asyncpg.PostgresError) as exc:
            results.append(
                {"admission_id": str(row["id"]), "state": "held", "reason": str(exc)}
            )
    return {
        "after": cursor if len(rows) == limit or len(results) < len(rows) else None,
        "results": results,
    }
