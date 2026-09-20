"""Durable VM creation intent composed with existing cleanup and job authority.

All controller observations are trusted internal inputs after lifecycle MAC and
correlation validation by the transport adapter. No method performs network I/O.
"""

from __future__ import annotations

from datetime import timedelta
import json
import random
from uuid import UUID, uuid4, uuid5, NAMESPACE_URL

from orchestrator.services.vm_workspace_recovery_store import (
    VMWorkspaceRecoveryStore,
    _job_workspace_owner,
    cleanup_intent_digest,
)
from shared.vm_creation_retry import canonical_request_digest, retry_delay_seconds
from shared.vm_creation_issuance import canonical_configuration_digest
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
    for key in (
        "canonical_request",
        "predecessor_evidence",
        "controller_configuration",
    ):
        if key in result:
            result[key] = _json(result[key])
    return result


def _creation_intent(row):
    """Complete immutable source-specific identity, including explicit new disk."""
    return {
        "job_id": str(row["job_id"]),
        "provision_generation": str(row["provision_generation"]),
        "request_digest": row["request_digest"],
        "controller_configuration_digest": row["controller_configuration_digest"],
        "expected_pvc_uid": str(row["expected_pvc_uid"])
        if row["expected_pvc_uid"]
        else None,
        "source": "controller_vm_create",
        "request_id": str(row["request_id"]),
    }


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
        from orchestrator.services.vm_creation_lineage import discover

        lineage = await discover(conn, job_id)
        owners = {UUID(v) for v in lineage["owners"]} if lineage else {job_id}
        if lineage and str(pvc_uid) != lineage["binding"]["pvc_uid"]:
            raise VMCreationRetryConflict("retained_disk_changed")
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
        guard_owners = sorted(owners if lineage else {job_id})
        cleanups = await conn.fetch(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE completed_at IS NULL "
            "AND ((owner_kind='job' AND owner_id=ANY($1::uuid[])) OR ($2::uuid IS NOT NULL AND pvc_uid=$2)) ORDER BY id FOR UPDATE",
            guard_owners,
            pvc_uid,
        )
        competing = [row for row in cleanups if row["id"] != own_admission]
        if competing:
            from orchestrator.services.vm_creation_disposition_cleanup import (
                exact_root_child,
            )

            disposition = (
                await conn.fetchval(
                    "SELECT cancellation_disposition FROM vm_creation_retries WHERE creation_admission_id=$1 AND job_id=$2 AND state='cancel_requested'",
                    own_admission,
                    job_id,
                )
                if own_admission is not None
                else None
            )
            if disposition is None or any(
                not exact_root_child(row, _json(disposition)) for row in competing
            ):
                raise VMCreationRetryConflict("workspace_cleanup_already_admitted")
        recovery = await conn.fetchval(
            "SELECT r.id FROM vm_workspace_recoveries r LEFT JOIN vm_workspace_recovery_retention_pins p "
            "ON p.recovery_id=r.id AND p.released_at IS NULL WHERE r.resolved_at IS NULL AND "
            "((r.owner_kind='job' AND r.owner_id=ANY($1::uuid[])) OR ($2::uuid IS NOT NULL AND p.pvc_uid=$2)) LIMIT 1 FOR UPDATE OF r",
            guard_owners,
            pvc_uid,
        )
        if recovery or await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_workspace_recovery_jobs WHERE job_id=ANY($1::uuid[]) AND resolved_at IS NULL)",
            guard_owners,
        ):
            raise VMCreationRetryConflict("workspace_recovery_held")
        if lineage:
            await conn.fetch(
                "SELECT unit_id FROM run_queue WHERE unit_id=ANY($1::uuid[]) ORDER BY unit_id FOR UPDATE",
                sorted(owners),
            )
        queue = await conn.fetchrow(
            "SELECT state FROM run_queue WHERE unit_id=$1 FOR UPDATE", job_id
        )
        if hold_queue and queue and queue["state"] == "leased":
            raise VMCreationRetryConflict("worker_lease_active")
        if current["execution_lane"] == "stateless" and hold_queue:
            await hold_worker_batch_for_preflight(
                conn, job_id=job_id, preserve_attempts=True
            )
        if lineage:
            await conn.fetch(
                "SELECT id FROM jobs WHERE id=ANY($1::uuid[]) ORDER BY id FOR UPDATE",
                sorted(owners),
            )
            await conn.fetch(
                "SELECT id FROM srw_execution_specs WHERE work_kind='Job' AND work_id=ANY($1::uuid[]) ORDER BY id FOR SHARE",
                sorted(owners),
            )
            if await discover(conn, job_id) != lineage:
                raise VMCreationRetryConflict("creation_attachment_lineage_unproven")
            if await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM vm_workspace_recovery_jobs WHERE job_id=ANY($1::uuid[]) AND resolved_at IS NULL)",
                sorted(owners),
            ):
                raise VMCreationRetryConflict("workspace_recovery_held")
        locked = await conn.fetchrow(
            "SELECT * FROM jobs WHERE id=$1 FOR UPDATE", job_id
        )
        owner, ambiguous = _job_workspace_owner(job_id, locked)
        if ambiguous or owner != job_id:
            raise VMCreationRetryConflict("workspace_owner_changed")
        return (
            {**dict(locked), "_creation_lineage_scope": lineage} if lineage else locked
        )

    async def _current(self, conn, job, generation, *, retry=None):
        context = _json(job["context"]) or {}
        if not isinstance(context, dict):
            raise VMCreationRetryConflict("creation_request_unproven")
        vm = context.get("vm") or {}
        if not isinstance(vm, dict):
            raise VMCreationRetryConflict("creation_request_unproven")
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
        lineage = job.get("_creation_lineage_scope")
        if lineage:
            if old:
                from orchestrator.services.vm_creation_replacement import (
                    prove_replacement,
                )

                return await prove_replacement(
                    conn,
                    job=job,
                    binding=lineage["binding"],
                    expected=evidence,
                    cleanup_id=proposal.get("predecessor_cleanup_admission_id"),
                )
            from orchestrator.services.vm_creation_lineage import prove

            proof, cleanup_id = await prove(
                conn,
                job_id=job["id"],
                binding=lineage["binding"],
                scope=lineage,
                expected=evidence,
            )
            if str(cleanup_id) != proposal.get("predecessor_cleanup_admission_id"):
                raise VMCreationRetryConflict("creation_attachment_lineage_unproven")
            return proof, cleanup_id
        return await self._own_predecessor(conn, job, pvc_uid, proposal)

    @staticmethod
    async def _own_predecessor(conn, job, pvc_uid, proposal):
        """Existing per-Job process-zero and exact retain-cleanup authority."""
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
        scope_pvc = pvc_uid or (prior["observed_pvc_uid"] if prior else None)
        job = await self._scope(
            conn,
            job_uuid,
            scope_pvc,
            own_admission=prior["creation_admission_id"] if prior else None,
            hold_queue=not prior or prior["state"] != "succeeded",
        )
        context, vm, execution = await self._current(conn, job, generation, retry=prior)
        if proposal.get("origin") == "resume":
            await self._validate_resume_on_conn(conn, job, request_uuid)
        existing = _record(
            await conn.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE job_id=$1 AND provision_generation=$2 FOR UPDATE",
                job_uuid,
                generation,
            )
        )
        snapshot = vm.get("creation_request") or {}
        if not isinstance(snapshot, dict):
            raise VMCreationRetryConflict("creation_request_unproven")
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
        configuration = snapshot.get("controller_configuration")
        if (
            configuration is not None
            and canonical_configuration_digest(configuration) != config_digest
        ):
            raise VMCreationRetryConflict("creation_configuration_changed")
        storage = payload.get("workspace_storage")
        if job.get("_creation_lineage_scope") and (
            storage != job["_creation_lineage_scope"]["binding"]
        ):
            raise VMCreationRetryConflict("creation_attachment_lineage_unproven")
        if storage is not None and (
            not isinstance(storage, dict)
            or storage.get("pvc_uid") != (str(pvc_uid) if pvc_uid else None)
        ):
            raise VMCreationRetryConflict("retained_disk_changed")
        # Ordinary per-job retained rootdisks have no workspace_storage binding.
        # Their expected PVC still requires the exact predecessor receipt and
        # cleanup chain checked below; absence of a binding is not new-disk proof.
        if existing:
            if (
                existing["observed_pvc_uid"]
                and existing["observed_pvc_uid"] != scope_pvc
            ):
                raise VMCreationRetryConflict("retry_identity_changed")
            await self._current(conn, job, generation, retry=existing)
            if (
                existing["request_digest"] != digest
                or existing["controller_configuration_digest"] != config_digest
                or existing["expected_pvc_uid"] != pvc_uid
            ):
                raise VMCreationRetryConflict("creation_request_changed")
            if existing["state"] in {"cancel_requested", "settled"}:
                raise VMCreationRetryConflict("job_cancelled")
            if proposal.get("origin") == "resume" and existing["state"] == "succeeded":
                raise VMCreationRetryConflict("creation_already_adopted")
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
        if job.get("_creation_lineage_scope"):
            from orchestrator.services.vm_creation_prepared_lineage import (
                validate_prepared_request,
            )

            validate_prepared_request(predecessor, payload, configuration)

        row = await conn.fetchrow(
            "INSERT INTO vm_creation_retries(request_id,job_id,provision_generation,origin,request_digest,canonical_request,controller_configuration_digest,execution_id,execution_revision,execution_generation,admission_deadline,expected_pvc_uid,predecessor_evidence,predecessor_cleanup_admission_id,controller_configuration) "
            "VALUES($1,$2,$3,'initial',$4,$5::jsonb,$6,$7,$8,$9,$10,$11,$12::jsonb,$13,$14::jsonb) RETURNING *",
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
            json.dumps(configuration) if configuration is not None else None,
        )
        await self._resume_on_conn(conn, job, request_uuid)
        return _record(row)

    async def _validate_resume_on_conn(self, conn, job, request_uuid):
        """Recheck public Resume authority after the canonical scope/job wait."""
        if (
            job["status"] not in {"created", "paused", "failed"}
            or job["assigned_agent_id"] is not None
        ):
            raise VMCreationRetryConflict("job_changed")
        if (_json(job["context"]) or {}).get("_vm_creation_pending") != str(
            request_uuid
        ):
            raise VMCreationRetryConflict("creation_request_unproven")
        # A foreign owner's recovery may enroll this participant while _scope
        # waits for its job row. Read after that wait before requeueing anything.
        if await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_workspace_recovery_jobs WHERE job_id=$1 AND resolved_at IS NULL)",
            job["id"],
        ):
            raise VMCreationRetryConflict("workspace_recovery_held")

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
                    scope_pvc = row["expected_pvc_uid"] or row["observed_pvc_uid"]
                    job = await self._scope(
                        conn,
                        row["job_id"],
                        scope_pvc,
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
                    if (
                        row["expected_pvc_uid"] or row["observed_pvc_uid"]
                    ) != scope_pvc:
                        raise VMCreationRetryConflict("retry_identity_changed")
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
                    intent = _creation_intent(row)
                    reservation_request = uuid5(
                        NAMESPACE_URL, "vm-create:" + request_id
                    )
                    permit = await self.cleanup.acquire_cleanup_permit_on_conn(
                        conn,
                        owner_kind="job",
                        owner_id=row["job_id"],
                        pvc_uid=scope_pvc,
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
        # Scheduling observations retain authority. Exact effect settlement is
        # separate and cannot be inferred from this observer's transport reply.
        outcome = observation.get("outcome")
        if outcome not in {
            "transport_unknown",
            "capacity_wait",
            "dependency_wait",
            "observation_wait",
            "blocked",
        }:
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
                reason = {
                    "transport_unknown": "controller_unavailable",
                    "capacity_wait": "capacity_wait",
                    "dependency_wait": "creation_dependency_pending",
                    "observation_wait": "creation_observation_pending",
                    "blocked": "vm_creation_retry_blocked",
                }[outcome]
                if outcome == "dependency_wait" and observation.get("reason") in (
                    "golden_wait",
                    "preparation_wait",
                    "headscale_wait",
                    "disk_wait",
                ):
                    reason = observation["reason"]
                await conn.execute(
                    "UPDATE vm_creation_retries SET state=$2,revision=revision+1,claim_token=NULL,claim_expires_at=NULL,backoff_attempt=$3,transport_outage_started_at=$4,reason=$5,next_probe_at=clock_timestamp()+$6*interval '1 second',updated_at=clock_timestamp() WHERE request_id=$1",
                    row["request_id"],
                    state,
                    attempt,
                    outage,
                    reason,
                    retry_delay_seconds(attempt, random.uniform(0, 0.2)),
                )
                return True

    @staticmethod
    def _carrier(carrier):
        from shared.vm_creation_issuance import verify_creation_carrier
        from shared.vm_lifecycle_auth import configured_secret

        return verify_creation_carrier(carrier, secret=configured_secret())

    async def _effect_scope(self, conn, request_id, *, observed_pvc=None):
        row = _record(
            await conn.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                UUID(request_id),
            )
        )
        if row is None:
            raise VMCreationRetryConflict("retry_request_missing")
        scope_pvc = row["expected_pvc_uid"] or row["observed_pvc_uid"] or observed_pvc
        job = await self._scope(
            conn,
            row["job_id"],
            scope_pvc,
            own_admission=row["creation_admission_id"],
            hold_queue=False,
        )
        locked = _record(
            await conn.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR UPDATE",
                row["request_id"],
            )
        )
        if (
            locked["observed_pvc_uid"] and locked["observed_pvc_uid"] != scope_pvc
        ) or locked["creation_admission_id"] != row["creation_admission_id"]:
            raise VMCreationRetryConflict("retry_identity_changed")
        if ((_json(job["context"]) or {}).get("vm") or {}).get(
            "provision_generation"
        ) != str(row["provision_generation"]):
            raise VMCreationRetryConflict("generation_changed")
        return locked, job

    async def _check_carrier(
        self, conn, row, carrier, values, *, allow_completed=False
    ):
        from shared.vm_workspace_storage import storage_name

        configuration = row["controller_configuration"]
        if configuration is None:
            raise VMCreationRetryConflict("creation_configuration_unproven")
        if configuration.get("persistent_rootdisk") is not True:
            raise VMCreationRetryConflict("retry_protocol_unavailable")
        if (
            canonical_configuration_digest(configuration)
            != row["controller_configuration_digest"]
            or configuration["namespace"] != carrier["metadata"]["namespace"]
        ):
            raise VMCreationRetryConflict("creation_configuration_changed")
        if values["version"] == 3:
            from shared.vm_creation_attachment import validate_attachment_intent

            try:
                validate_attachment_intent(
                    values["workspace_attachment"],
                    request=row["canonical_request"],
                    expected_pvc_uid=str(row["expected_pvc_uid"])
                    if row["expected_pvc_uid"]
                    else None,
                )
            except (ValueError, KeyError, TypeError) as exc:
                raise VMCreationRetryConflict("creation_attachment_changed") from exc
            original = await conn.fetchval(
                "SELECT carrier_intent FROM vm_creation_effects WHERE request_id=$1 ORDER BY effect_number LIMIT 1",
                row["request_id"],
            )
            if (
                original
                and _json(original).get("workspace_attachment")
                != values["workspace_attachment"]
            ):
                raise VMCreationRetryConflict("creation_attachment_changed")
        if values["version"] in (2, 3) and values["effect_kind"] != "workspace_attach":
            from shared.vm_creation_issuance import validate_rootdisk_source

            try:
                validate_rootdisk_source(
                    values["rootdisk_source"],
                    request=row["canonical_request"],
                    configuration=configuration,
                    expected_pvc_uid=str(row["expected_pvc_uid"])
                    if row["expected_pvc_uid"]
                    else None,
                )
            except (ValueError, KeyError, TypeError) as exc:
                raise VMCreationRetryConflict(
                    "creation_rootdisk_source_changed"
                ) from exc
            source = values["rootdisk_source"]
            from orchestrator.services.vm_creation_prepared_lineage import (
                prepared_origin,
            )

            origin = prepared_origin(row["predecessor_evidence"])
            if origin is not None or source.get("inherited_origin") is not None:
                from shared.vm_inherited_preparation import retained_prepared_source

                if origin is None or source != retained_prepared_source(origin):
                    raise VMCreationRetryConflict("creation_rootdisk_source_changed")
            if (
                source.get("kind") == "prepared"
                and source.get("mode") == "retained"
                and source["retained_root"]["dv_uid"] != values["retained_dv_uid"]
            ):
                raise VMCreationRetryConflict("creation_rootdisk_source_changed")
            original = await conn.fetchval(
                "SELECT carrier_intent FROM vm_creation_effects WHERE request_id=$1 AND effect_kind='rootdisk' ORDER BY effect_number LIMIT 1",
                row["request_id"],
            )
            if (
                original
                and _json(original).get("rootdisk_source") != values["rootdisk_source"]
            ):
                raise VMCreationRetryConflict("creation_rootdisk_source_changed")
        expected = {
            "retry_request_id": str(row["request_id"]),
            "job_id": str(row["job_id"]),
            "provision_generation": str(row["provision_generation"]),
            "request_digest": row["request_digest"],
            "controller_configuration_digest": row["controller_configuration_digest"],
            "expected_pvc_uid": str(row["expected_pvc_uid"])
            if row["expected_pvc_uid"]
            else None,
        }
        if any(values[key] != value for key, value in expected.items()) or values[
            "admission_id"
        ] != str(row["creation_admission_id"]):
            raise VMCreationRetryConflict("creation_carrier_changed")
        permit = await self._creation_permit_on_conn(
            conn, row, allow_completed=allow_completed
        )
        if (
            values["reservation_request_id"] != str(permit["request_id"])
            or values["intent_digest"] != permit["intent_digest"]
        ):
            raise VMCreationRetryConflict("creation_reservation_changed")
        metadata = carrier["metadata"]
        if row["creation_carrier_uid"] is not None and (
            str(row["creation_carrier_uid"]) != metadata["uid"]
            or row["creation_carrier_namespace"] != metadata["namespace"]
        ):
            raise VMCreationRetryConflict("creation_carrier_changed")
        binding = row["canonical_request"].get("workspace_storage")
        name = (
            storage_name(binding) if binding else f"agent-vm-{row['job_id']}-rootdisk"
        )
        if (
            values["effect_kind"] in {"rootdisk", "workspace_attach"}
            and values["object_name"] != name
        ):
            raise VMCreationRetryConflict("retained_disk_changed")
        return permit

    async def _creation_permit_on_conn(self, conn, row, *, allow_completed=False):
        """Validate the original source admission; never allocate another permit."""
        permit = await conn.fetchrow(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
            row["creation_admission_id"],
        )
        # Agreement between two supplied hashes is not authority. Recompute the
        # complete source-specific reservation intent from immutable DB fields.
        intent = _creation_intent(row)
        reservation_request = uuid5(
            NAMESPACE_URL, "vm-create:" + str(row["request_id"])
        )
        if (
            not permit
            or (permit["completed_at"] is not None and not allow_completed)
            or permit["source"] != "controller_vm_create"
            or permit["owner_kind"] != "job"
            or permit["owner_id"] != row["job_id"]
            or permit["pvc_uid"] != (row["expected_pvc_uid"] or row["observed_pvc_uid"])
            or permit["request_id"] != reservation_request
            or permit["intent_digest"] != cleanup_intent_digest(intent)
        ):
            raise VMCreationRetryConflict("creation_reservation_changed")
        return permit

    async def begin_effect(
        self, *, request_id: str, claim_token: str, carrier: dict
    ) -> dict:
        """One successful CAS grants one actuation; repeats only permit observation."""
        from shared.vm_creation_attachment import attachment_effect_kinds

        values = self._carrier(carrier)
        async with self.db.acquire() as conn:
            async with conn.transaction():
                row, job = await self._effect_scope(conn, request_id)
                await self._current(conn, job, row["provision_generation"], retry=row)
                await self._check_carrier(conn, row, carrier, values)
                latest = await conn.fetchrow(
                    "SELECT * FROM vm_creation_effects WHERE request_id=$1 ORDER BY effect_number DESC LIMIT 1 FOR UPDATE",
                    row["request_id"],
                )
                prior = await conn.fetchrow(
                    "SELECT * FROM vm_creation_effects WHERE effect_nonce=$1",
                    UUID(values["effect_nonce"]),
                )
                database_now = await conn.fetchval("SELECT clock_timestamp()")
                if (
                    row["state"] != "reconciling"
                    or row["claim_token"] != UUID(claim_token)
                    or row["claim_expires_at"] <= database_now
                ):
                    raise VMCreationRetryConflict("retry_claim_changed")
                if (
                    row["admission_deadline"]
                    and row["admission_deadline"] <= database_now
                ):
                    raise VMCreationRetryConflict("job_admission_expired")
                if prior:
                    if (
                        prior["request_id"] != row["request_id"]
                        or _json(prior["carrier_intent"]) != values
                        or str(prior["carrier_uid"]) != carrier["metadata"]["uid"]
                        or prior["carrier_namespace"]
                        != carrier["metadata"]["namespace"]
                    ):
                        raise VMCreationRetryConflict("creation_effect_changed")
                    return {
                        "actuation_allowed": False,
                        "disposition": "observe_only",
                        "effect_state": prior["state"],
                    }
                stages = attachment_effect_kinds(row["canonical_request"])
                if row["canonical_request"].get("workspace_storage") is not None:
                    if (
                        values["version"] != 3
                        or values["workspace_attachment"]["action"] == "observe"
                    ):
                        raise VMCreationRetryConflict(
                            "creation_attachment_authority_unproven"
                        )
                    from orchestrator.services.vm_creation_attachment_store import (
                        attachment_instance_on_conn,
                    )

                    await attachment_instance_on_conn(conn, row)
                    prior_lease = values["workspace_attachment"]["prior"]
                    prior_job = str(row["job_id"])
                    lineage = row["predecessor_evidence"]
                    if lineage.get("kind") == "retained_attachment_handoff":
                        prior_job = lineage["previous_job_id"]
                        if values["workspace_attachment"]["action"] != "replace":
                            raise VMCreationRetryConflict(
                                "creation_attachment_lineage_unproven"
                            )
                    if lineage.get("kind") == "retained_attachment_replacement":
                        from orchestrator.services.vm_creation_replacement import (
                            validate_replacement_claim,
                        )

                        validate_replacement_claim(
                            lineage, values["workspace_attachment"]
                        )
                    if prior_lease and prior_lease["execution_id"] != prior_job:
                        raise VMCreationRetryConflict(
                            "creation_attachment_lineage_unproven"
                        )
                    if values["effect_kind"] != "workspace_attach":
                        attachment = _json(
                            await conn.fetchval(
                                "SELECT evidence FROM vm_creation_effects WHERE request_id=$1 AND effect_kind='workspace_attach' AND state='observed' ORDER BY effect_number DESC LIMIT 1",
                                row["request_id"],
                            )
                        )
                        if (
                            not attachment
                            or values["current_attachment_uid"] != attachment["uid"]
                        ):
                            raise VMCreationRetryConflict("creation_attachment_changed")
                # Historical v1 effects remain observable above and through
                # observe/settle, but cannot mint a fresh source-less grant for
                # modes that require a frozen clone/retained-source document.
                if values["version"] not in (2, 3) and (
                    row["controller_configuration"]["golden_enabled"]
                    or row["canonical_request"].get("preparation") is not None
                ):
                    raise VMCreationRetryConflict("creation_rootdisk_source_required")
                if latest and latest["state"] == "issued":
                    raise VMCreationRetryConflict("creation_effect_unresolved")
                if latest is None:
                    expected_kind = stages[0]
                elif latest["state"] == "rejected":
                    expected_kind = latest["effect_kind"]
                elif latest["effect_kind"] == "vm":
                    raise VMCreationRetryConflict("creation_already_admitted")
                else:
                    expected_kind = stages[stages.index(latest["effect_kind"]) + 1]
                if values["effect_kind"] != expected_kind:
                    raise VMCreationRetryConflict("creation_effect_out_of_order")
                if expected_kind not in {"rootdisk", "workspace_attach"}:
                    rootdisk = _json(
                        await conn.fetchval(
                            "SELECT evidence FROM vm_creation_effects WHERE request_id=$1 AND effect_kind='rootdisk' AND state='observed' ORDER BY effect_number DESC LIMIT 1",
                            row["request_id"],
                        )
                    )
                    if (
                        not rootdisk
                        or values["current_dv_uid"] != rootdisk["uid"]
                        or values["current_pvc_uid"] != rootdisk["pvc_uid"]
                    ):
                        raise VMCreationRetryConflict("retained_disk_changed")
                elif (
                    values["current_pvc_uid"] != values["expected_pvc_uid"]
                    or values["current_dv_uid"] != values["retained_dv_uid"]
                ):
                    raise VMCreationRetryConflict("retained_disk_changed")
                if expected_kind == "vm":
                    cloud_init = _json(
                        await conn.fetchval(
                            "SELECT evidence FROM vm_creation_effects WHERE request_id=$1 AND effect_kind='cloud_init' AND state='observed' ORDER BY effect_number DESC LIMIT 1",
                            row["request_id"],
                        )
                    )
                    if (
                        not cloud_init
                        or values["current_secret_uid"] != cloud_init["uid"]
                    ):
                        raise VMCreationRetryConflict("creation_secret_changed")
                database_now = await conn.fetchval("SELECT clock_timestamp()")
                if row["claim_expires_at"] <= database_now:
                    raise VMCreationRetryConflict("retry_claim_changed")
                if (
                    row["admission_deadline"]
                    and row["admission_deadline"] <= database_now
                ):
                    raise VMCreationRetryConflict("job_admission_expired")
                await conn.execute(
                    "UPDATE vm_creation_retries SET creation_carrier_uid=$2,creation_carrier_namespace=$3,updated_at=clock_timestamp() WHERE request_id=$1",
                    row["request_id"],
                    UUID(carrier["metadata"]["uid"]),
                    carrier["metadata"]["namespace"],
                )
                await conn.execute(
                    "INSERT INTO vm_creation_effects(effect_nonce,request_id,effect_number,effect_kind,carrier_uid,carrier_namespace,carrier_intent) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb)",
                    UUID(values["effect_nonce"]),
                    row["request_id"],
                    latest["effect_number"] + 1 if latest else 1,
                    values["effect_kind"],
                    UUID(carrier["metadata"]["uid"]),
                    carrier["metadata"]["namespace"],
                    json.dumps(values),
                )
                return {
                    "actuation_allowed": True,
                    "disposition": "issued",
                    "effect_nonce": values["effect_nonce"],
                }

    async def settle_never_issued(self, *, request_id: str) -> dict:
        """Definitive DB non-issuance: cancellation won before any possible effect."""
        async with self.db.acquire() as conn:
            async with conn.transaction():
                row, _ = await self._effect_scope(conn, request_id)
                if row["state"] == "settled":
                    if row["reason"] != "creation_never_issued":
                        raise VMCreationRetryConflict("creation_already_settled")
                    return {"settled": True, "disposition": "never_issued"}
                if row["state"] != "cancel_requested":
                    raise VMCreationRetryConflict("job_not_cancelled")
                if (
                    row["cancellation_disposition"] is not None
                    or row["canonical_request"].get("workspace_storage") is not None
                ):
                    return {"settled": False, "reason": "creation_disposition_pending"}
                if await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM vm_creation_effects WHERE request_id=$1 AND state IN ('issued','observed'))",
                    row["request_id"],
                ):
                    return {"settled": False, "reason": "creation_effect_unresolved"}
                if row["creation_admission_id"]:
                    from orchestrator.services.vm_creation_disposition_store import (
                        source_resolution,
                    )

                    # Source pin/allocation publication precedes the rootdisk
                    # grant. An empty/rejected effect ledger cannot release it.
                    if source_resolution(row, None) != "not_required":
                        return {
                            "settled": False,
                            "reason": "creation_source_unresolved",
                        }
                if row["creation_admission_id"]:
                    permit = await conn.fetchrow(
                        "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1",
                        row["creation_admission_id"],
                    )
                    if (
                        not permit
                        or permit["source"] != "controller_vm_create"
                        or permit["owner_kind"] != "job"
                        or permit["owner_id"] != row["job_id"]
                        or permit["pvc_uid"] != row["expected_pvc_uid"]
                        or permit["request_id"]
                        != uuid5(NAMESPACE_URL, "vm-create:" + str(row["request_id"]))
                        or permit["intent_digest"]
                        != cleanup_intent_digest(_creation_intent(row))
                        or permit["completed_at"] is not None
                    ):
                        raise VMCreationRetryConflict("creation_reservation_changed")
                    await conn.execute(
                        "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),outcome='never_issued' WHERE id=$1",
                        permit["id"],
                    )
                await conn.execute(
                    "UPDATE vm_creation_retries SET state='settled',revision=revision+1,claim_token=NULL,claim_expires_at=NULL,resolved_at=clock_timestamp(),reason='creation_never_issued',updated_at=clock_timestamp() WHERE request_id=$1",
                    row["request_id"],
                )
                return {"settled": True, "disposition": "never_issued"}

    async def prepare_disposition(self, *, request_id: str) -> dict:
        from orchestrator.services.vm_creation_disposition_store import (
            VMCreationDispositionStore,
        )

        return await VMCreationDispositionStore(self).prepare(request_id=request_id)

    async def freeze_disposition(self, *, request_id: str, carrier: dict) -> dict:
        from orchestrator.services.vm_creation_disposition_store import (
            VMCreationDispositionStore,
        )

        return await VMCreationDispositionStore(self).freeze(
            request_id=request_id, carrier=carrier
        )

    async def observe_effect(
        self, *, request_id: str, carrier: dict, observation: dict
    ) -> dict:
        """Record exact late facts even after cancellation; never grant actuation."""
        from shared.vm_creation_issuance import public_effect_observation

        values = self._carrier(carrier)
        observed_pvc = None
        if (
            values["effect_kind"] == "rootdisk"
            and observation.get("outcome") == "observed"
        ):
            # Obtain the disk scope before any job/retry lock. Validation below
            # still checks the complete authenticated evidence and effect nonce.
            observed_pvc = UUID(str(observation["pvc"]["metadata"]["uid"]))
        async with self.db.acquire() as conn:
            async with conn.transaction():
                row, _ = await self._effect_scope(
                    conn, request_id, observed_pvc=observed_pvc
                )
                await self._check_carrier(conn, row, carrier, values)
                effect = await conn.fetchrow(
                    "SELECT * FROM vm_creation_effects WHERE effect_nonce=$1 FOR UPDATE",
                    UUID(values["effect_nonce"]),
                )
                if (
                    not effect
                    or effect["request_id"] != row["request_id"]
                    or _json(effect["carrier_intent"]) != values
                    or str(effect["carrier_uid"]) != carrier["metadata"]["uid"]
                    or effect["carrier_namespace"] != carrier["metadata"]["namespace"]
                ):
                    raise VMCreationRetryConflict("creation_effect_changed")
                rootdisk = await conn.fetchval(
                    "SELECT evidence FROM vm_creation_effects WHERE request_id=$1 AND effect_kind='rootdisk' AND state='observed' ORDER BY effect_number DESC LIMIT 1",
                    row["request_id"],
                )
                cloud_init = await conn.fetchval(
                    "SELECT evidence FROM vm_creation_effects WHERE request_id=$1 AND effect_kind='cloud_init' AND state='observed' ORDER BY effect_number DESC LIMIT 1",
                    row["request_id"],
                )
                evidence = public_effect_observation(
                    values,
                    carrier,
                    observation,
                    rootdisk=_json(rootdisk),
                    cloud_init=_json(cloud_init),
                )
                state = "rejected" if evidence["outcome"] == "rejected" else "observed"
                if effect["state"] != "issued":
                    if (
                        effect["state"] != state
                        or _json(effect["evidence"]) != evidence
                    ):
                        raise VMCreationRetryConflict("creation_effect_changed")
                    return {"recorded": True, "effect_state": state}
                if state == "observed" and values["effect_kind"] == "rootdisk":
                    if (
                        row["observed_pvc_uid"]
                        and str(row["observed_pvc_uid"]) != evidence["pvc_uid"]
                    ):
                        raise VMCreationRetryConflict("retained_disk_changed")
                    await conn.execute(
                        "UPDATE vm_creation_retries SET observed_pvc_uid=$2,updated_at=clock_timestamp() WHERE request_id=$1",
                        row["request_id"],
                        UUID(evidence["pvc_uid"]),
                    )
                    # Extend this same owner reservation to the now-known PVC;
                    # never acquire a competing cleanup admission for adoption.
                    await conn.execute(
                        "UPDATE vm_workspace_cleanup_admissions SET pvc_uid=$2 WHERE id=$1 AND (pvc_uid IS NULL OR pvc_uid=$2)",
                        row["creation_admission_id"],
                        UUID(evidence["pvc_uid"]),
                    )
                if state == "observed" and values["effect_kind"] == "vm":
                    await conn.execute(
                        "UPDATE vm_creation_retries SET observed_vm_uid=$2,updated_at=clock_timestamp() WHERE request_id=$1",
                        row["request_id"],
                        UUID(evidence["uid"]),
                    )
                await conn.execute(
                    "UPDATE vm_creation_effects SET state=$2,evidence=$3::jsonb,resolved_at=clock_timestamp() WHERE effect_nonce=$1",
                    effect["effect_nonce"],
                    state,
                    json.dumps(evidence),
                )
                return {"recorded": True, "effect_state": state}

    async def inspect(self, *, request_id: str) -> dict:
        """Authenticated controller read; never returns an observer or actuation grant."""
        from orchestrator.services.vm_creation_prepared_lineage import prepared_origin

        async with self.db.acquire() as conn:
            async with conn.transaction(isolation="repeatable_read", readonly=True):
                row = _record(
                    await conn.fetchrow(
                        "SELECT * FROM vm_creation_retries WHERE request_id=$1",
                        UUID(request_id),
                    )
                )
                if row is None:
                    raise VMCreationRetryConflict("retry_request_missing")
                effects = await conn.fetch(
                    "SELECT carrier_intent,state,evidence FROM vm_creation_effects WHERE request_id=$1 ORDER BY effect_number",
                    row["request_id"],
                )
                return {
                    **{
                        key: str(row[key]) if isinstance(row[key], UUID) else row[key]
                        for key in (
                            "request_id",
                            "job_id",
                            "provision_generation",
                            "request_digest",
                            "controller_configuration_digest",
                            "expected_pvc_uid",
                            "state",
                            "reason",
                            "creation_admission_id",
                            "creation_carrier_uid",
                        )
                    },
                    "request": row["canonical_request"],
                    "cancellation_disposition": _json(row["cancellation_disposition"]),
                    "cancellation_progress": _json(row["cancellation_progress"]),
                    **(
                        {
                            "prepared_origin": prepared_origin(
                                row["predecessor_evidence"]
                            )
                        }
                        if prepared_origin(row["predecessor_evidence"]) is not None
                        else {}
                    ),
                    "effects": [
                        {
                            "carrier_intent": _json(effect["carrier_intent"]),
                            "state": effect["state"],
                            "evidence": _json(effect["evidence"]),
                        }
                        for effect in effects
                    ],
                }

    async def settle_adopted(
        self, *, request_id: str, carrier: dict, observations: dict
    ) -> dict:
        """Adopt an exactly reobserved single VM and release its own create permit.

        Late accepted facts remain useful after observer expiry or cancellation.
        This is not Ready release and does not authorize any new controller I/O.
        """
        from shared.vm_creation_issuance import (
            public_effect_observation,
            seal_creation_carrier,
        )
        from shared.vm_lifecycle_auth import configured_secret

        values = self._carrier(carrier)
        expected_kinds = {"rootdisk", "cloud_init", "vm"}
        if values["version"] == 3:
            expected_kinds.add("workspace_attach")
        if values["effect_kind"] != "vm" or set(observations) != expected_kinds:
            raise VMCreationRetryConflict("creation_adoption_unproven")
        async with self.db.acquire() as conn:
            async with conn.transaction():
                row, job = await self._effect_scope(conn, request_id)
                permit = await self._check_carrier(
                    conn, row, carrier, values, allow_completed=True
                )
                effects = await conn.fetch(
                    "SELECT * FROM vm_creation_effects WHERE request_id=$1 ORDER BY effect_number FOR UPDATE",
                    row["request_id"],
                )
                proven = {}
                for effect in effects:
                    if effect["state"] == "rejected":
                        continue
                    if effect["state"] != "observed":
                        raise VMCreationRetryConflict("creation_effect_unresolved")
                    intent = _json(effect["carrier_intent"])
                    kind = effect["effect_kind"]
                    if (
                        kind in proven
                        or str(effect["carrier_uid"]) != carrier["metadata"]["uid"]
                        or effect["carrier_namespace"]
                        != carrier["metadata"]["namespace"]
                    ):
                        raise VMCreationRetryConflict("creation_effect_changed")
                    historical = seal_creation_carrier(
                        intent,
                        namespace=effect["carrier_namespace"],
                        uid=str(effect["carrier_uid"]),
                        resource_version=carrier["metadata"]["resourceVersion"],
                        secret=configured_secret(),
                    )
                    actual = public_effect_observation(
                        intent,
                        historical,
                        observations[kind],
                        rootdisk=proven.get("rootdisk"),
                        cloud_init=proven.get("cloud_init"),
                    )
                    if actual != _json(effect["evidence"]):
                        raise VMCreationRetryConflict(
                            "creation_observed_object_changed"
                        )
                    proven[kind] = actual
                if (
                    set(proven) != expected_kinds
                    or _json(effects[-1]["carrier_intent"]) != values
                ):
                    raise VMCreationRetryConflict("creation_adoption_unproven")
                vm = proven["vm"]
                if (
                    str(row["observed_vm_uid"]) != vm["uid"]
                    or str(row["observed_pvc_uid"]) != vm["pvc_uid"]
                ):
                    raise VMCreationRetryConflict("creation_adoption_changed")
                result = {"settled": True, "disposition": "adopted"}
                context = _json(job["context"]) or {}
                current = dict(context.get("vm") or {})
                if current.get("vm_uid") not in (None, vm["uid"]) or current.get(
                    "rootdisk_pvc_uid"
                ) not in (None, vm["pvc_uid"]):
                    raise VMCreationRetryConflict("creation_adoption_changed")
                if permit["completed_at"] is not None:
                    if (
                        row["state"] not in {"succeeded", "settled"}
                        or row["reason"] != "creation_adopted"
                        or permit["outcome"] != "adopted"
                    ):
                        raise VMCreationRetryConflict("creation_reservation_changed")
                    return result
                if row["canonical_request"].get("workspace_storage") is not None:
                    from orchestrator.services.vm_creation_attachment_store import (
                        record_attachment_adoption_on_conn,
                    )

                    await record_attachment_adoption_on_conn(
                        conn, row, pvc_uid=vm["pvc_uid"], namespace=vm["namespace"]
                    )
                    current["workspace_storage"] = {
                        **row["canonical_request"]["workspace_storage"],
                        "pvc_uid": vm["pvc_uid"],
                    }
                cancelled = row["state"] == "cancel_requested"
                if row["state"] not in {
                    "queued",
                    "reconciling",
                    "attention",
                    "cancel_requested",
                }:
                    raise VMCreationRetryConflict("creation_adoption_changed")
                attempts = current.get("provision_attempts", 0)
                if type(attempts) is not int or attempts < 0:
                    raise VMCreationRetryConflict("creation_attempts_unproven")
                current.update(
                    {
                        "vm_uid": vm["uid"],
                        "vm_name": vm["name"],
                        "namespace": vm["namespace"],
                        "rootdisk_pvc_uid": vm["pvc_uid"],
                        "cloud_init_secret_uid": vm["cloud_init_uid"],
                        "ssh_host_key_fingerprint": vm["ssh_host_key_fingerprint"],
                        "identity_authenticated": True,
                        "identity_provision_generation": str(
                            row["provision_generation"]
                        ),
                        "creation_request_id": str(row["request_id"]),
                        "provisioned_by": "http",
                        "provision_attempts": attempts
                        + (0 if row["boot_counted"] else 1),
                    }
                )
                source = values.get("rootdisk_source", {})
                if source.get("kind") == "prepared":
                    from shared.vm_creation_issuance import prepared_source_metadata

                    current["preparation"] = prepared_source_metadata(source)
                    current["preparation_request"] = row["canonical_request"][
                        "preparation"
                    ]
                if not cancelled:
                    current["status"] = "created"
                context["vm"] = current
                # Queue hold and job control context remain intact. The jobs
                # cancellation trigger may touch this retry before its terminal
                # update; all locks are already held in the standard order.
                await conn.execute(
                    "UPDATE jobs SET context=$2::jsonb,updated_at=clock_timestamp() WHERE id=$1",
                    row["job_id"],
                    json.dumps(context),
                )
                await conn.execute(
                    "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),outcome='adopted' WHERE id=$1",
                    row["creation_admission_id"],
                )
                await conn.execute(
                    "UPDATE vm_creation_retries SET state=$2,reason='creation_adopted',boot_counted=true,revision=revision+1,claim_token=NULL,claim_expires_at=NULL,resolved_at=clock_timestamp(),updated_at=clock_timestamp() WHERE request_id=$1",
                    row["request_id"],
                    "settled" if cancelled else "succeeded",
                )
                return result
