"""Short, proof-bound PostgreSQL transactions for a pre-SSH Job VM stop."""

from __future__ import annotations

from collections.abc import Mapping
import json
from typing import Any
from uuid import UUID

from shared.vm_pre_ssh_stop import (
    valid_frozen_stop_candidate,
    valid_positive_stop_proof,
)
from shared.vm_cancel_retention import valid_retention_preflight


class VMPreSSHStopConflict(RuntimeError):
    """A candidate no longer has the exact retiring Job's authority."""


def _json_object(value: Any) -> dict:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise VMPreSSHStopConflict("malformed_vm_context")
    return value


def _uuid(value: object) -> UUID:
    try:
        result = UUID(str(value))
    except (ValueError, TypeError, AttributeError) as exc:
        raise VMPreSSHStopConflict("noncanonical_stop_identity") from exc
    if str(result) != value:
        raise VMPreSSHStopConflict("noncanonical_stop_identity")
    return result


def _require_intent_current(intent: Any, cleanup: Any, retry: Any, charge: Any) -> None:
    """Keep every replay bound to the original admission and held charge."""

    if (
        intent["cleanup_admission_id"] != cleanup["id"]
        or intent["creation_request_id"] != retry["request_id"]
        or intent["reservation_id"] != charge["id"]
        or intent["reservation_revision"] != charge["revision"]
        or intent["vm_uid"] != charge["vm_uid"]
        or intent["vmi_uid"] != charge["vmi_uid"]
        or intent["launcher_uid"] != charge["launcher_uid"]
        or intent["pvc_uid"] != retry["observed_pvc_uid"]
        or intent["node_uid"] != charge["node_uid"]
        or intent["cleanup_intent_digest"] != cleanup["intent_digest"]
    ):
        raise VMPreSSHStopConflict("stop_intent_parent_changed")


def _intent_wire(row):
    frozen = _json_object(row["frozen"])
    result = {"frozen": frozen, "frozen_digest": row["frozen_digest"]}
    preflight = row.get("retention_preflight")
    if preflight is not None:
        preflight = _json_object(preflight)
        if not valid_retention_preflight(preflight, frozen):
            raise VMPreSSHStopConflict("retention_preflight_unproven")
        result["retention_preflight"] = preflight
    return result


class VMPreSSHStopStore:
    def __init__(self, db: Any):
        self.db = db

    async def requires_retention_preflight(self, parent_cleanup) -> bool:
        from orchestrator.services.vm_job_cancel_retention import (
            retention_for_admission_on_conn,
        )

        async with self.db.acquire() as conn:
            return (
                await retention_for_admission_on_conn(
                    conn, _uuid(parent_cleanup["admission_id"])
                )
                is not None
            )

    async def _retention_preflight_current(self, conn, cleanup, frozen, preflight):
        from orchestrator.services.vm_job_cancel_retention import (
            retention_for_admission_on_conn,
        )

        authority = await retention_for_admission_on_conn(conn, cleanup["id"])
        if authority is None:
            if preflight is not None:
                raise VMPreSSHStopConflict("retention_preflight_without_authority")
            return
        if authority["cleanup_admission_id"] != cleanup[
            "id"
        ] or not valid_retention_preflight(preflight, frozen):
            raise VMPreSSHStopConflict("retention_preflight_unproven")
        if authority.get("policy_version") == 3 and preflight != _json_object(
            authority["ready_retention_preflight"]
        ):
            raise VMPreSSHStopConflict("initial_ready_preflight_changed")
        if not await conn.fetchval(
            "SELECT public.validate_vm_job_cancel_retention($1,false)", cleanup["id"]
        ):
            raise VMPreSSHStopConflict("retention_authority_changed")

    async def has_intent(self, job_id: str, generation: str) -> bool:
        owner, incarnation = _uuid(job_id), _uuid(generation)
        async with self.db.acquire() as conn:
            return bool(
                await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM vm_pre_ssh_stop_intents "
                    "WHERE job_id=$1 AND provision_generation=$2)",
                    owner,
                    incarnation,
                )
            )

    async def _locked_current(
        self,
        conn: Any,
        *,
        job_id: UUID,
        generation: UUID,
        parent_cleanup: Mapping[str, Any],
        frozen: Mapping[str, Any] | None,
    ) -> tuple[Any, Any, Any, Any]:
        """Reuse the active owner/admission/retry/charge lock and identity gates."""

        try:
            cleanup_id = _uuid(parent_cleanup["admission_id"])
            cleanup_request = _uuid(parent_cleanup["request_id"])
            cleanup_digest = parent_cleanup["intent_digest"]
            cleanup_intent = parent_cleanup["intent"]
        except (KeyError, TypeError) as exc:
            raise VMPreSSHStopConflict("cleanup_admission_missing") from exc
        if (
            not isinstance(cleanup_intent, Mapping)
            or cleanup_intent.get("owner_kind") != "job"
            or cleanup_intent.get("owner_id") != str(job_id)
            or cleanup_intent.get("provision_generation") != str(generation)
            or cleanup_intent.get("purge_disk") is not False
            or not isinstance(cleanup_digest, str)
            or not cleanup_digest.startswith("sha256:")
        ):
            raise VMPreSSHStopConflict("cleanup_intent_changed")
        from orchestrator.services.vm_job_cancel_retention import (
            retention_for_admission_on_conn,
        )

        authority = await retention_for_admission_on_conn(conn, cleanup_id)
        ready_stop = authority is not None and authority.get("policy_version") == 3
        held_stop = (
            authority is not None
            and authority.get("policy_version") == 1
            and (
                frozen is not None
                and frozen.get("kind")
                == "vm_job_never_app_ready_retained_stop_candidate_v1"
                or frozen is None
                and await conn.fetchval(
                    "SELECT frozen->>'kind'='vm_job_never_app_ready_retained_stop_candidate_v1' "
                    "FROM vm_pre_ssh_stop_intents WHERE cleanup_admission_id=$1",
                    cleanup_id,
                )
                is True
            )
        )
        if ready_stop or held_stop:
            for key in (
                f"workspace-recovery:job:{job_id}",
                f"workspace-recovery-pvc:{authority['pvc_uid']}",
            ):
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", key
                )
            await conn.fetchrow(
                "SELECT unit_id FROM run_queue WHERE unit_id=$1 FOR UPDATE", job_id
            )
        job = await conn.fetchrow(
            "SELECT id,status,execution_lane,assigned_agent_id,context "
            "FROM jobs WHERE id=$1 FOR UPDATE",
            job_id,
        )
        cleanup = await conn.fetchrow(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE id=$1 FOR UPDATE",
            cleanup_id,
        )
        retry = await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE job_id=$1 "
            "AND provision_generation=$2 FOR UPDATE",
            job_id,
            generation,
        )
        if job is None or cleanup is None or retry is None:
            raise VMPreSSHStopConflict("retirement_authority_absent")
        context = _json_object(job["context"])
        vm = context.get("vm")
        if not isinstance(vm, dict):
            raise VMPreSSHStopConflict("vm_context_absent")
        if (
            job["execution_lane"] not in {"pinned", "stateless"}
            or job["assigned_agent_id"] is not None
            or vm.get("status") != "retiring_process_zero"
            or vm.get("provision_generation") != str(generation)
            or cleanup["owner_kind"] != "job"
            or cleanup["owner_id"] != job_id
            or cleanup["request_id"] != cleanup_request
            or cleanup["source"] != cleanup_intent.get("source")
            or cleanup["intent_digest"] != cleanup_digest
            or cleanup["completed_at"] is not None
            or retry["state"] != "succeeded"
            or (
                retry["ready_at"] is None
                if ready_stop
                else retry["ready_at"] is not None
            )
            or retry["request_id"] is None
            or retry["observed_vm_uid"] is None
            or retry["observed_pvc_uid"] is None
            or vm.get("vm_uid") != str(retry["observed_vm_uid"])
            or vm.get("rootdisk_pvc_uid") != str(retry["observed_pvc_uid"])
            or cleanup["pvc_uid"] != retry["observed_pvc_uid"]
            or cleanup_intent.get("vm_uid") != str(retry["observed_vm_uid"])
            or cleanup_intent.get("pvc_uid") != str(retry["observed_pvc_uid"])
        ):
            raise VMPreSSHStopConflict("retirement_authority_changed")
        if ready_stop and (
            authority["cleanup_admission_id"] != cleanup_id
            or parent_cleanup.get("retention_preflight")
            != _json_object(authority["ready_retention_preflight"])
            or not await conn.fetchval(
                "SELECT admitted_xact_id<>pg_current_xact_id() FROM vm_job_cancel_retention_authorities WHERE cleanup_admission_id=$1",
                cleanup_id,
            )
            or not await conn.fetchval(
                "SELECT public.validate_vm_job_cancel_retention($1,false)", cleanup_id
            )
        ):
            raise VMPreSSHStopConflict("initial_ready_authority_changed")
        if held_stop and (
            authority["cleanup_admission_id"] != cleanup_id
            or authority["admitted_xact_id"]
            == await conn.fetchval("SELECT pg_current_xact_id()")
            or not await conn.fetchval(
                "SELECT public.validate_vm_job_cancel_retention($1,false)", cleanup_id
            )
        ):
            raise VMPreSSHStopConflict("held_stop_authority_changed")
        charge = await conn.fetchrow(
            "SELECT * FROM vm_resource_reservations WHERE request_id=$1 "
            "AND state<>'released' FOR UPDATE",
            retry["request_id"],
        )
        if (
            charge is None
            or charge["resource_version"] != 2
            or charge["state"] != "teardown"
            or charge["vm_uid"] != retry["observed_vm_uid"]
            or charge["vmi_uid"] is None
            or charge["launcher_uid"] is None
            or await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM vm_resource_recovery_successors "
                "WHERE reservation_id=$1)",
                charge["id"] if charge else None,
            )
        ):
            raise VMPreSSHStopConflict("teardown_charge_changed")
        policy = await conn.fetchrow(
            "SELECT namespace,policy_digest,mode FROM vm_resource_admission_policy "
            "WHERE cluster_id=$1 FOR UPDATE",
            charge["cluster_id"],
        )
        if (
            policy is None
            or policy["policy_digest"] != charge["policy_digest"]
            or policy["mode"] != "enforce"
        ):
            raise VMPreSSHStopConflict("resource_policy_changed")
        if frozen is not None and (
            not valid_frozen_stop_candidate(frozen)
            or (frozen.get("kind") == "vm_initial_ready_positive_stop_candidate_v1")
            != ready_stop
            or (
                frozen.get("kind")
                == "vm_job_never_app_ready_retained_stop_candidate_v1"
            )
            != held_stop
            or (
                (ready_stop or held_stop)
                and (
                    frozen.get("cleanup_admission_id") != str(cleanup_id)
                    or frozen.get("cleanup_request_id") != str(cleanup_request)
                    or frozen.get("cleanup_intent_digest") != cleanup_digest
                    or (
                        ready_stop
                        and not valid_retention_preflight(
                            parent_cleanup.get("retention_preflight"), frozen
                        )
                    )
                )
            )
            or frozen["job_id"] != str(job_id)
            or frozen["provision_generation"] != str(generation)
            or frozen["vm_uid"] != str(charge["vm_uid"])
            or frozen["vmi_uid"] != str(charge["vmi_uid"])
            or frozen["launcher_uid"] != str(charge["launcher_uid"])
            or frozen["pvc_uid"] != str(retry["observed_pvc_uid"])
            or frozen["node_uid"] != str(charge["node_uid"])
            or frozen["namespace"] != policy["namespace"]
            or vm.get("vmi_uid") != frozen["vmi_uid"]
            or vm.get("active_pod_uid") not in {None, frozen["launcher_uid"]}
        ):
            raise VMPreSSHStopConflict("candidate_identity_changed")
        return job, cleanup, retry, charge

    async def current_intent(
        self,
        job_id: str,
        generation: str,
        parent_cleanup: Mapping[str, Any],
    ) -> dict | None:
        owner, incarnation = _uuid(job_id), _uuid(generation)
        async with self.db.acquire() as conn:
            async with conn.transaction():
                _, cleanup, retry, charge = await self._locked_current(
                    conn,
                    job_id=owner,
                    generation=incarnation,
                    parent_cleanup=parent_cleanup,
                    frozen=None,
                )
                row = await conn.fetchrow(
                    "SELECT * FROM vm_pre_ssh_stop_intents WHERE job_id=$1 "
                    "AND provision_generation=$2",
                    owner,
                    incarnation,
                )
                if row is None:
                    return None
                _require_intent_current(row, cleanup, retry, charge)
                frozen = _json_object(row["frozen"])
                if frozen["kind"] in {
                    "vm_initial_ready_positive_stop_candidate_v1",
                    "vm_job_never_app_ready_retained_stop_candidate_v1",
                } and not await conn.fetchval(
                    "SELECT "
                    + (
                        "initial_ready_xact_id"
                        if frozen["kind"]
                        == "vm_initial_ready_positive_stop_candidate_v1"
                        else "held_stop_xact_id"
                    )
                    + "<>pg_current_xact_id() FROM vm_pre_ssh_stop_intents "
                    "WHERE cleanup_admission_id=$1",
                    cleanup["id"],
                ):
                    raise VMPreSSHStopConflict("initial_ready_intent_uncommitted")
                await self._locked_current(
                    conn,
                    job_id=owner,
                    generation=incarnation,
                    parent_cleanup=parent_cleanup,
                    frozen=frozen,
                )
                wire = _intent_wire(row)
                await self._retention_preflight_current(
                    conn, cleanup, frozen, wire.get("retention_preflight")
                )
                return wire

    async def admit_intent(
        self,
        job_id: str,
        generation: str,
        parent_cleanup: Mapping[str, Any],
        frozen: Mapping[str, Any],
        *,
        retention_preflight: Mapping[str, Any] | None = None,
        prior_zero_receipt_id: str | None = None,
    ) -> dict:
        owner, incarnation = _uuid(job_id), _uuid(generation)
        if not valid_frozen_stop_candidate(frozen):
            raise VMPreSSHStopConflict("candidate_malformed")
        async with self.db.acquire() as conn:
            async with conn.transaction():
                _, cleanup, retry, charge = await self._locked_current(
                    conn,
                    job_id=owner,
                    generation=incarnation,
                    parent_cleanup=parent_cleanup,
                    frozen=frozen,
                )
                await self._retention_preflight_current(
                    conn, cleanup, frozen, retention_preflight
                )
                prior = await conn.fetchrow(
                    "SELECT * FROM vm_pre_ssh_stop_intents "
                    "WHERE cleanup_admission_id=$1",
                    cleanup["id"],
                )
                if prior is not None:
                    _require_intent_current(prior, cleanup, retry, charge)
                    if _json_object(prior["frozen"]) != dict(frozen):
                        raise VMPreSSHStopConflict("stop_intent_rebound")
                    wire = _intent_wire(prior)
                    if wire.get("retention_preflight") != retention_preflight:
                        raise VMPreSSHStopConflict("retention_preflight_rebound")
                    if prior.get("prior_zero_receipt_id") != (
                        _uuid(prior_zero_receipt_id) if prior_zero_receipt_id else None
                    ):
                        raise VMPreSSHStopConflict("prior_zero_rebound")
                    return wire
                zero = await conn.fetchrow(
                    "SELECT id FROM managed_repository_process_zero_receipts "
                    "WHERE owner_kind='job' AND owner_id=$1 AND scope='vm' "
                    "AND provisioner='vm' AND runtime_incarnation=$2",
                    owner,
                    str(incarnation),
                )
                held_stop = (
                    frozen["kind"]
                    == "vm_job_never_app_ready_retained_stop_candidate_v1"
                )
                if (zero is not None and not held_stop) or (
                    (zero["id"] if zero else None)
                    != (_uuid(prior_zero_receipt_id) if prior_zero_receipt_id else None)
                ):
                    raise VMPreSSHStopConflict("preexisting_zero_receipt")
                preflight_column = (
                    ",retention_preflight" if retention_preflight is not None else ""
                )
                preflight_value = (
                    ",$14::jsonb" if retention_preflight is not None else ""
                )
                prior_column = ",prior_zero_receipt_id" if held_stop else ""
                prior_value = (
                    f",${15 if retention_preflight is not None else 14}"
                    if held_stop
                    else ""
                )
                row = await conn.fetchrow(
                    "INSERT INTO vm_pre_ssh_stop_intents ("
                    "cleanup_admission_id,job_id,provision_generation,"
                    "creation_request_id,reservation_id,reservation_revision,"
                    "vm_uid,vmi_uid,launcher_uid,pvc_uid,node_uid,"
                    f"cleanup_intent_digest,frozen,frozen_digest{preflight_column}{prior_column}) VALUES ("
                    "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,"
                    "'sha256:'||encode(sha256(convert_to($13::jsonb::text,'UTF8')),'hex')"
                    f"{preflight_value}{prior_value}) RETURNING *",
                    cleanup["id"],
                    owner,
                    incarnation,
                    retry["request_id"],
                    charge["id"],
                    charge["revision"],
                    charge["vm_uid"],
                    charge["vmi_uid"],
                    charge["launcher_uid"],
                    retry["observed_pvc_uid"],
                    charge["node_uid"],
                    cleanup["intent_digest"],
                    json.dumps(dict(frozen)),
                    *(
                        [json.dumps(dict(retention_preflight))]
                        if retention_preflight is not None
                        else []
                    ),
                    *(
                        [
                            _uuid(prior_zero_receipt_id)
                            if prior_zero_receipt_id
                            else None
                        ]
                        if held_stop
                        else []
                    ),
                )
                return _intent_wire(row)

    async def commit_positive_proof(
        self,
        job_id: str,
        generation: str,
        parent_cleanup: Mapping[str, Any],
        observed: Mapping[str, Any],
    ) -> dict:
        owner, incarnation = _uuid(job_id), _uuid(generation)
        async with self.db.acquire() as conn:
            async with conn.transaction():
                _, cleanup, retry, charge = await self._locked_current(
                    conn,
                    job_id=owner,
                    generation=incarnation,
                    parent_cleanup=parent_cleanup,
                    frozen=None,
                )
                intent = await conn.fetchrow(
                    "SELECT * FROM vm_pre_ssh_stop_intents WHERE job_id=$1 "
                    "AND provision_generation=$2 FOR UPDATE",
                    owner,
                    incarnation,
                )
                if intent is None:
                    raise VMPreSSHStopConflict("stop_intent_absent")
                _require_intent_current(intent, cleanup, retry, charge)
                frozen = _json_object(intent["frozen"])
                if frozen[
                    "kind"
                ] == "vm_job_never_app_ready_retained_stop_candidate_v1" and (
                    intent["held_stop_xact_id"] is None
                    or intent["held_stop_xact_id"]
                    == await conn.fetchval("SELECT pg_current_xact_id()")
                ):
                    raise VMPreSSHStopConflict("held_stop_intent_uncommitted")
                _, cleanup, retry, charge = await self._locked_current(
                    conn,
                    job_id=owner,
                    generation=incarnation,
                    parent_cleanup=parent_cleanup,
                    frozen=frozen,
                )
                if not valid_positive_stop_proof(
                    frozen, observed, frozen_digest=intent["frozen_digest"]
                ):
                    raise VMPreSSHStopConflict("positive_stop_unproven")
                prior = await conn.fetchrow(
                    "SELECT terminal_evidence,evidence_digest FROM vm_pre_ssh_stop_proofs "
                    "WHERE cleanup_admission_id=$1",
                    intent["cleanup_admission_id"],
                )
                if prior is not None and _json_object(
                    prior["terminal_evidence"]
                ) != dict(observed):
                    raise VMPreSSHStopConflict("stop_proof_rebound")
                if prior is None:
                    prior = await conn.fetchrow(
                        "INSERT INTO vm_pre_ssh_stop_proofs ("
                        "cleanup_admission_id,job_id,provision_generation,"
                        "frozen_digest,terminal_evidence,evidence_digest) VALUES ("
                        "$1,$2,$3,$4,$5::jsonb,"
                        "'sha256:'||encode(sha256(convert_to($5::jsonb::text,'UTF8')),'hex')"
                        ") RETURNING terminal_evidence,evidence_digest",
                        intent["cleanup_admission_id"],
                        owner,
                        incarnation,
                        intent["frozen_digest"],
                        json.dumps(dict(observed)),
                    )
                zero = await conn.fetchrow(
                    "INSERT INTO managed_repository_process_zero_receipts "
                    "(owner_kind,owner_id,scope,provisioner,runtime_incarnation) "
                    "VALUES ('job',$1,'vm','vm',$2) "
                    "ON CONFLICT (owner_kind,owner_id,scope,runtime_incarnation) "
                    "DO NOTHING RETURNING id",
                    owner,
                    str(incarnation),
                )
                if zero is None:
                    zero = await conn.fetchrow(
                        "SELECT id FROM managed_repository_process_zero_receipts "
                        "WHERE owner_kind='job' AND owner_id=$1 AND scope='vm' "
                        "AND provisioner='vm' AND runtime_incarnation=$2",
                        owner,
                        str(incarnation),
                    )
                if zero is None:
                    raise VMPreSSHStopConflict("zero_receipt_uncommitted")
                return {
                    "cleanup_admission_id": str(intent["cleanup_admission_id"]),
                    "frozen_digest": intent["frozen_digest"],
                    "evidence_digest": prior["evidence_digest"],
                    "process_zero_receipt_id": str(zero["id"]),
                }

    async def committed_proof(
        self,
        job_id: str,
        generation: str,
        parent_cleanup: Mapping[str, Any],
    ) -> dict | None:
        """Read the exact immutable proof/zero pair before finalizer release."""

        owner, incarnation = _uuid(job_id), _uuid(generation)
        async with self.db.acquire() as conn:
            async with conn.transaction():
                _, cleanup, retry, charge = await self._locked_current(
                    conn,
                    job_id=owner,
                    generation=incarnation,
                    parent_cleanup=parent_cleanup,
                    frozen=None,
                )
                row = await conn.fetchrow(
                    "SELECT i.*,p.terminal_evidence,"
                    "p.evidence_digest,z.id AS zero_id "
                    "FROM vm_pre_ssh_stop_intents i "
                    "JOIN vm_pre_ssh_stop_proofs p "
                    "ON p.cleanup_admission_id=i.cleanup_admission_id "
                    "JOIN managed_repository_process_zero_receipts z "
                    "ON z.owner_kind='job' AND z.owner_id=i.job_id "
                    "AND z.scope='vm' AND z.provisioner='vm' "
                    "AND z.runtime_incarnation=i.provision_generation::text "
                    "WHERE i.job_id=$1 AND i.provision_generation=$2",
                    owner,
                    incarnation,
                )
                if row is None:
                    return None
                frozen_kind = _json_object(row["frozen"])["kind"]
                if (
                    frozen_kind == "vm_job_never_app_ready_retained_stop_candidate_v1"
                    and (
                        (
                            row["prior_zero_receipt_id"] is not None
                            and row["prior_zero_receipt_id"] != row["zero_id"]
                        )
                        or _json_object(row["terminal_evidence"]).get("kind")
                        != "vm_job_never_app_ready_retained_positive_stop_v1"
                    )
                ):
                    raise VMPreSSHStopConflict("held_stop_proof_zero_changed")
                if frozen_kind in {
                    "vm_initial_ready_positive_stop_candidate_v1",
                    "vm_job_never_app_ready_retained_stop_candidate_v1",
                } and not await conn.fetchval(
                    "SELECT "
                    + (
                        "initial_ready_xact_id"
                        if frozen_kind == "vm_initial_ready_positive_stop_candidate_v1"
                        else "held_stop_xact_id"
                    )
                    + "<>pg_current_xact_id() FROM vm_pre_ssh_stop_proofs "
                    "WHERE cleanup_admission_id=$1",
                    cleanup["id"],
                ):
                    raise VMPreSSHStopConflict("initial_ready_proof_uncommitted")
                _require_intent_current(row, cleanup, retry, charge)
                frozen = _json_object(row["frozen"])
                proof = _json_object(row["terminal_evidence"])
                await self._locked_current(
                    conn,
                    job_id=owner,
                    generation=incarnation,
                    parent_cleanup=parent_cleanup,
                    frozen=frozen,
                )
                if not valid_positive_stop_proof(
                    frozen, proof, frozen_digest=row["frozen_digest"]
                ):
                    raise VMPreSSHStopConflict("committed_stop_proof_invalid")
                return {
                    "frozen": frozen,
                    "frozen_digest": row["frozen_digest"],
                    "terminal_evidence": proof,
                    "evidence_digest": row["evidence_digest"],
                    "process_zero_receipt_id": str(row["zero_id"]),
                    **(
                        {
                            "retention_preflight": _json_object(
                                row["retention_preflight"]
                            )
                        }
                        if frozen["kind"]
                        in {
                            "vm_initial_ready_positive_stop_candidate_v1",
                            "vm_job_never_app_ready_retained_stop_candidate_v1",
                        }
                        else {}
                    ),
                }
