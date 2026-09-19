"""Durable VM creation intent composed with existing cleanup and job authority.

All controller observations are trusted internal inputs after lifecycle MAC and
correlation validation by the transport adapter. No method performs network I/O.
"""

from __future__ import annotations

from datetime import timedelta
import json
from uuid import UUID, uuid4, uuid5, NAMESPACE_URL

from orchestrator.services.vm_workspace_recovery_store import (
    VMWorkspaceRecoveryStore,
    _job_workspace_owner,
    cleanup_intent_digest,
)
from shared.vm_creation_retry import canonical_request_digest, retry_delay_seconds
from shared.worker_queue import hold_worker_batch_for_preflight


class VMCreationRetryConflict(RuntimeError):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def _json(value):
    return json.loads(value) if isinstance(value, str) else value


def _record(row):
    if row is None:
        return None
    result = dict(row)
    for key in ("canonical_request", "predecessor_evidence"):
        if key in result:
            result[key] = _json(result[key])
    return result


class VMCreationRetryStore:
    def __init__(self, db):
        self.db = db
        self.cleanup = VMWorkspaceRecoveryStore(db)

    async def _scope(
        self, conn, job_id, pvc_uid, *, own_admission=None, hold_queue=True
    ):
        """Owner/PVC -> cleanup/recovery -> queue -> job, never retry -> job."""
        membership = await conn.fetchrow(
            "SELECT parent_job_id,context FROM jobs WHERE id=$1", job_id
        )
        owners = {job_id}
        if membership and membership["parent_job_id"]:
            owners.add(membership["parent_job_id"])
        for owner in sorted(owners):
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                f"workspace-recovery:job:{owner}",
            )
        if pvc_uid:
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                f"workspace-recovery-pvc:{pvc_uid}",
            )
        current = await conn.fetchrow(
            "SELECT parent_job_id,context,execution_lane FROM jobs WHERE id=$1", job_id
        )
        if current is None:
            raise VMCreationRetryConflict("job_changed")
        owner, ambiguous = _job_workspace_owner(job_id, current)
        if ambiguous or owner != job_id:
            raise VMCreationRetryConflict("workspace_owner_changed")
        cleanups = await conn.fetch(
            "SELECT id FROM vm_workspace_cleanup_admissions WHERE completed_at IS NULL "
            "AND ((owner_kind='job' AND owner_id=$1) OR ($2::uuid IS NOT NULL AND pvc_uid=$2)) ORDER BY id FOR UPDATE",
            job_id,
            pvc_uid,
        )
        if any(row["id"] != own_admission for row in cleanups):
            raise VMCreationRetryConflict("workspace_cleanup_already_admitted")
        recovery = await conn.fetchval(
            "SELECT r.id FROM vm_workspace_recoveries r LEFT JOIN vm_workspace_recovery_retention_pins p "
            "ON p.recovery_id=r.id AND p.released_at IS NULL WHERE r.resolved_at IS NULL AND "
            "((r.owner_kind='job' AND r.owner_id=$1) OR ($2::uuid IS NOT NULL AND p.pvc_uid=$2)) LIMIT 1 FOR UPDATE OF r",
            job_id,
            pvc_uid,
        )
        if recovery or await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_workspace_recovery_jobs WHERE job_id=$1 AND resolved_at IS NULL)",
            job_id,
        ):
            raise VMCreationRetryConflict("workspace_recovery_held")
        queue = await conn.fetchrow(
            "SELECT state FROM run_queue WHERE unit_id=$1 FOR UPDATE", job_id
        )
        if hold_queue and queue and queue["state"] == "leased":
            raise VMCreationRetryConflict("worker_lease_active")
        if current["execution_lane"] == "stateless" and hold_queue:
            await hold_worker_batch_for_preflight(
                conn, job_id=job_id, preserve_attempts=True
            )
        return await conn.fetchrow(
            "SELECT * FROM jobs WHERE id=$1 FOR UPDATE",
            job_id,
        )

    async def _current(self, conn, job, generation, *, retry=None):
        context = _json(job["context"]) or {}
        vm = context.get("vm") or {}
        if vm.get("provision_generation") != str(generation):
            raise VMCreationRetryConflict("generation_changed")
        if job["status"] in {"completed", "cancelled"} or any(
            key in context
            for key in (
                "_stateless_delete_pending",
                "_stateless_cancel_cleanup_pending",
            )
        ):
            raise VMCreationRetryConflict("job_cancelled")
        if vm.get("retirement_cleanup_pending") is True or vm.get("status") in {
            "retiring_process_zero",
            "retired",
            "deleting",
            "deleted",
            "delete_failed",
        }:
            raise VMCreationRetryConflict("vm_retirement_pending")
        from orchestrator.database.postgres import _completion_control_active_sql

        if await conn.fetchval(
            "SELECT ("
            + _completion_control_active_sql("context")
            + ") FROM jobs WHERE id=$1",
            job["id"],
        ):
            raise VMCreationRetryConflict("job_control_busy")
        if await self.db._completion_resume_blocked_on_conn(conn, job["id"]):
            raise VMCreationRetryConflict("job_completion_pending")
        execution = await conn.fetchrow(
            "SELECT *,CASE WHEN resolved->'spec'->>'timeoutSeconds' IS NULL THEN NULL "
            "ELSE created_at+((resolved->'spec'->>'timeoutSeconds')::double precision * interval '1 second') END AS deadline "
            "FROM srw_execution_specs WHERE work_kind='Job' AND work_id=$1 FOR SHARE",
            job["id"],
        )
        if not execution or execution["harness_adapter"] != "srw/v1":
            raise VMCreationRetryConflict("creation_request_unproven")
        if retry and (
            execution["id"] != retry["execution_id"]
            or execution["revision"] != retry["execution_revision"]
            or execution["generation"] != retry["execution_generation"]
            or execution["deadline"] != retry["admission_deadline"]
        ):
            raise VMCreationRetryConflict("execution_manifest_changed")
        # A SELECT target expression can run before its FOR UPDATE/SHARE wait.
        # Read the clock separately after every relevant row is locked.
        database_now = await conn.fetchval("SELECT clock_timestamp()")
        if execution["deadline"] and execution["deadline"] <= database_now:
            raise VMCreationRetryConflict("job_admission_expired")
        return context, vm, execution

    async def _predecessor(self, conn, job, pvc_uid, proposal):
        evidence = proposal.get("predecessor_evidence") or {}
        old = (_json(job["context"]) or {}).get("last_vm") or {}
        if pvc_uid is None:
            if (
                old.get("rootdisk_pvc_uid")
                or evidence
                or proposal.get("predecessor_cleanup_admission_id")
            ):
                raise VMCreationRetryConflict("retained_disk_changed")
            return {}, None
        # Evidence must match durable predecessor context, receipt and the full
        # completed cleanup intent, not a same-name disk or arbitrary receipt.
        if (
            old.get("identity_authenticated") is not True
            or old.get("identity_provision_generation")
            != old.get("provision_generation")
            or old.get("rootdisk_pvc_uid") != str(pvc_uid)
        ):
            raise VMCreationRetryConflict("creation_request_unproven")
        if evidence.get("provision_generation") != old.get(
            "provision_generation"
        ) or evidence.get("vm_uid") != old.get("vm_uid"):
            raise VMCreationRetryConflict("creation_request_unproven")
        receipt = await conn.fetchval(
            "SELECT id FROM managed_repository_process_zero_receipts WHERE owner_kind='job' AND owner_id=$1 AND scope='vm' AND provisioner='vm' AND runtime_incarnation=$2",
            job["id"],
            evidence["provision_generation"],
        )
        try:
            cleanup_id = UUID(str(proposal.get("predecessor_cleanup_admission_id")))
        except ValueError as exc:
            raise VMCreationRetryConflict("creation_request_unproven") from exc
        cleanup = await conn.fetchrow(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1", cleanup_id
        )
        if (
            not receipt
            or not cleanup
            or cleanup["completed_at"] is None
            or cleanup["outcome"] != "completed"
            or cleanup["owner_kind"] != "job"
            or cleanup["owner_id"] != job["id"]
            or cleanup["pvc_uid"] != pvc_uid
        ):
            raise VMCreationRetryConflict("predecessor_cleanup_pending")
        intent = {
            "owner_kind": "job",
            "owner_id": str(job["id"]),
            "provision_generation": evidence["provision_generation"],
            "vm_uid": evidence["vm_uid"],
            "pvc_uid": str(pvc_uid),
            "purge_disk": False,
            "resource": "vm_workspace",
            "source": cleanup["source"],
        }
        if cleanup_intent_digest(intent) != cleanup["intent_digest"]:
            raise VMCreationRetryConflict("creation_request_unproven")
        return {**intent, "receipt_id": str(receipt)}, cleanup_id

    async def admit_on_conn(
        self,
        conn,
        *,
        job_id: str,
        expected_generation: str,
        request_id: str,
        proposal: dict,
    ) -> dict:
        """Admit inside the caller transaction, before any protocol-on initial I/O.

        ``proposal`` is constructed by the trusted initial-create or Resume
        adapter. Origin alone is not proof that a legacy create never issued;
        only an existing ledger row supports subsequent same-generation Resume.
        """
        job_uuid, generation, request_uuid = (
            UUID(job_id),
            UUID(expected_generation),
            UUID(request_id),
        )
        pvc_uid = (
            UUID(str(proposal["expected_pvc_uid"]))
            if proposal.get("expected_pvc_uid")
            else None
        )
        prior = await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE job_id=$1 AND provision_generation=$2",
            job_uuid,
            generation,
        )
        job = await self._scope(
            conn,
            job_uuid,
            pvc_uid,
            own_admission=prior["creation_admission_id"] if prior else None,
            hold_queue=not prior or prior["state"] != "succeeded",
        )
        context, vm, execution = await self._current(conn, job, generation, retry=prior)
        existing = _record(
            await conn.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE job_id=$1 AND provision_generation=$2 FOR UPDATE",
                job_uuid,
                generation,
            )
        )
        snapshot = vm.get("creation_request") or {}
        payload = snapshot.get("request")
        digest = proposal.get("request_digest")
        config_digest = proposal.get("controller_configuration_digest")
        if (
            not isinstance(payload, dict)
            or type(snapshot.get("version")) is not int
            or snapshot.get("version") != 1
            or snapshot.get("provision_generation") != str(generation)
            or payload.get("job_id") != str(job_uuid)
            or payload.get("entity_type") != "job"
            or payload.get("provision_generation") != str(generation)
            or snapshot.get("request_digest") != digest
            or canonical_request_digest(payload) != digest
            or snapshot.get("controller_configuration_authenticated") is not True
            or snapshot.get("controller_configuration_digest") != config_digest
            or not config_digest
        ):
            raise VMCreationRetryConflict("creation_request_unproven")
        storage = payload.get("workspace_storage")
        if storage is not None and (
            not isinstance(storage, dict)
            or storage.get("pvc_uid") != (str(pvc_uid) if pvc_uid else None)
        ):
            raise VMCreationRetryConflict("retained_disk_changed")
        # Ordinary per-job retained rootdisks have no workspace_storage binding.
        # Their expected PVC still requires the exact predecessor receipt and
        # cleanup chain checked below; absence of a binding is not new-disk proof.
        if existing:
            await self._current(conn, job, generation, retry=existing)
            if (
                existing["request_digest"] != digest
                or existing["controller_configuration_digest"] != config_digest
                or existing["expected_pvc_uid"] != pvc_uid
            ):
                raise VMCreationRetryConflict("creation_request_changed")
            if existing["state"] in {"cancel_requested", "settled"}:
                raise VMCreationRetryConflict("job_cancelled")
            if existing["state"] == "attention":
                existing = _record(
                    await conn.fetchrow(
                        "UPDATE vm_creation_retries SET state='queued',revision=revision+1,next_probe_at=clock_timestamp(),transport_outage_started_at=NULL,reason=NULL,backoff_attempt=0,updated_at=clock_timestamp() WHERE request_id=$1 RETURNING *",
                        existing["request_id"],
                    )
                )
            if existing["state"] != "succeeded":
                await self._resume_on_conn(conn, job, existing["request_id"])
            return existing
        if (
            proposal.get("origin") != "initial"
            or snapshot.get("initial_request") is not True
        ):
            raise VMCreationRetryConflict("creation_request_unproven")
        if job["status"] != proposal.get("expected_status"):
            raise VMCreationRetryConflict("job_changed")
        predecessor, predecessor_id = await self._predecessor(
            conn, job, pvc_uid, proposal
        )
        row = await conn.fetchrow(
            "INSERT INTO vm_creation_retries(request_id,job_id,provision_generation,origin,request_digest,canonical_request,controller_configuration_digest,execution_id,execution_revision,execution_generation,admission_deadline,expected_pvc_uid,predecessor_evidence,predecessor_cleanup_admission_id) "
            "VALUES($1,$2,$3,'initial',$4,$5::jsonb,$6,$7,$8,$9,$10,$11,$12::jsonb,$13) RETURNING *",
            request_uuid,
            job_uuid,
            generation,
            digest,
            json.dumps(payload),
            config_digest,
            execution["id"],
            execution["revision"],
            execution["generation"],
            execution["deadline"],
            pvc_uid,
            json.dumps(predecessor),
            predecessor_id,
        )
        await self._resume_on_conn(conn, job, request_uuid)
        return _record(row)

    async def _resume_on_conn(self, conn, job, request_uuid):
        queued = await self.db._queue_job_for_resume_on_conn(
            conn,
            job["id"],
            {"_vm_creation_pending": str(request_uuid)},
            void_completion_decision=False,
            stateless_only=job["execution_lane"] == "stateless",
            expected_status=str(job["status"]),
            completion_commands_enabled=True,
        )
        if queued is None:
            raise VMCreationRetryConflict("job_control_busy")

    async def claim_due(self, *, limit: int) -> list[dict]:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("Invalid creation claim batch size")
        async with self.db.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    "SELECT request_id FROM vm_creation_retries WHERE state IN ('queued','reconciling','cancel_requested') AND next_probe_at<=clock_timestamp() AND (claim_expires_at IS NULL OR claim_expires_at<=clock_timestamp()) ORDER BY next_probe_at,request_id LIMIT $1 FOR UPDATE SKIP LOCKED",
                    limit,
                )
                result = []
                for row in rows:
                    result.append(
                        _record(
                            await conn.fetchrow(
                                "UPDATE vm_creation_retries SET state=CASE WHEN state='cancel_requested' THEN state ELSE 'reconciling' END,revision=revision+1,claim_token=$2,claim_expires_at=clock_timestamp()+interval '60 seconds',updated_at=clock_timestamp() WHERE request_id=$1 RETURNING *",
                                row["request_id"],
                                uuid4(),
                            )
                        )
                    )
                return result

    async def authorize_controller(
        self, *, request_id: str, claim_token: str, observed: dict
    ) -> dict:
        try:
            async with self.db.acquire() as conn:
                async with conn.transaction():
                    row = _record(
                        await conn.fetchrow(
                            "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                            UUID(request_id),
                        )
                    )
                    if row is None:
                        raise VMCreationRetryConflict("retry_request_missing")
                    if row["state"] != "reconciling":
                        raise VMCreationRetryConflict("retry_claim_changed")
                    job = await self._scope(
                        conn,
                        row["job_id"],
                        row["expected_pvc_uid"],
                        own_admission=row["creation_admission_id"],
                    )
                    await self._current(
                        conn, job, row["provision_generation"], retry=row
                    )
                    row = _record(
                        await conn.fetchrow(
                            "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
                            UUID(request_id),
                        )
                    )
                    database_now = await conn.fetchval("SELECT clock_timestamp()")
                    if (
                        row["state"] != "reconciling"
                        or row["claim_token"] != UUID(claim_token)
                        or row["claim_expires_at"] <= database_now
                    ):
                        raise VMCreationRetryConflict("retry_claim_changed")
                    expected = {
                        "job_id": str(row["job_id"]),
                        "provision_generation": str(row["provision_generation"]),
                        "request_digest": row["request_digest"],
                        "controller_configuration_digest": row[
                            "controller_configuration_digest"
                        ],
                        "expected_pvc_uid": str(row["expected_pvc_uid"])
                        if row["expected_pvc_uid"]
                        else None,
                    }
                    if any(
                        key not in observed or observed[key] != value
                        for key, value in expected.items()
                    ):
                        raise VMCreationRetryConflict("creation_request_changed")
                    # _scope already owns all owner/PVC and cleanup/recovery locks.
                    # The helper reacquires those same locks, never another scope.
                    intent = {
                        **expected,
                        "source": "controller_vm_create",
                        "request_id": request_id,
                    }
                    reservation_request = uuid5(
                        NAMESPACE_URL, "vm-create:" + request_id
                    )
                    permit = await self.cleanup.acquire_cleanup_permit_on_conn(
                        conn,
                        owner_kind="job",
                        owner_id=row["job_id"],
                        pvc_uid=row["expected_pvc_uid"],
                        request_id=reservation_request,
                        source="controller_vm_create",
                        intent_digest=cleanup_intent_digest(intent),
                        revalidate_completed=True,
                    )
                    if not permit.allowed or permit.completed_outcome is not None:
                        raise VMCreationRetryConflict(
                            permit.reason or "creation_reservation_completed"
                        )
                    # The composed permit helper can itself wait for a row.
                    # Expiry here must roll back its tentative admission too.
                    database_now = await conn.fetchval("SELECT clock_timestamp()")
                    if row["claim_expires_at"] <= database_now:
                        raise VMCreationRetryConflict("retry_claim_changed")
                    if (
                        row["admission_deadline"]
                        and row["admission_deadline"] <= database_now
                    ):
                        raise VMCreationRetryConflict("job_admission_expired")
                    await conn.execute(
                        "UPDATE vm_creation_retries SET creation_admission_id=$2,updated_at=clock_timestamp() WHERE request_id=$1",
                        row["request_id"],
                        permit.admission_id,
                    )
                    return {
                        "allowed": True,
                        "admission_id": permit.admission_id,
                        "request_id": str(reservation_request),
                        "intent_digest": cleanup_intent_digest(intent),
                    }
        except VMCreationRetryConflict as exc:
            return {"allowed": False, "reason": exc.reason}

    async def request_cancel_on_conn(
        self, conn, *, job_id: str, expected_generation: str
    ) -> bool:
        return await conn.fetchval(
            "SELECT request_vm_creation_retry_cancel($1,$2)",
            UUID(job_id),
            expected_generation,
        )

    async def apply_observation(
        self,
        *,
        request_id: str,
        claim_token: str,
        expected_revision: int,
        observation: dict,
    ) -> bool:
        # Task 3 supplies exact issuance/admission settlement; until then only
        # observations that retain authority may be committed here.
        outcome = observation.get("outcome")
        if outcome not in {"transport_unknown", "capacity_wait", "blocked"}:
            raise ValueError(
                "Creation settlement requires authenticated controller integration"
            )
        async with self.db.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
                    UUID(request_id),
                )
                database_now = await conn.fetchval("SELECT clock_timestamp()")
                if (
                    not row
                    or row["claim_token"] != UUID(claim_token)
                    or row["revision"] != expected_revision
                    or row["claim_expires_at"] <= database_now
                    or row["state"] not in {"reconciling", "cancel_requested"}
                ):
                    return False
                attempt = row["backoff_attempt"] + 1
                outage = (
                    (row["transport_outage_started_at"] or database_now)
                    if outcome == "transport_unknown"
                    else None
                )
                attention = outcome == "blocked" or (
                    outage is not None
                    and database_now - outage >= timedelta(seconds=900)
                )
                state = (
                    "cancel_requested"
                    if row["state"] == "cancel_requested"
                    else "attention"
                    if attention
                    else "queued"
                )
                await conn.execute(
                    "UPDATE vm_creation_retries SET state=$2,revision=revision+1,claim_token=NULL,claim_expires_at=NULL,backoff_attempt=$3,transport_outage_started_at=$4,reason=$5,next_probe_at=clock_timestamp()+$6*interval '1 second',updated_at=clock_timestamp() WHERE request_id=$1",
                    row["request_id"],
                    state,
                    attempt,
                    outage,
                    "vm_creation_retry_blocked"
                    if attention
                    else "vm_creation_retry_pending",
                    retry_delay_seconds(attempt),
                )
                return True
