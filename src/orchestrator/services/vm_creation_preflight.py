"""Durable read-only resolution before the VM creation effect ledger exists.

The preflight freezes caller intent and holds worker admission. It grants no
controller or Kubernetes effects. Full configuration and ledger admission must
commit together before any protocol create is eligible.
"""

from copy import deepcopy
from datetime import datetime
import json
import math
import os
from uuid import UUID, uuid4

from fastapi import HTTPException

from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryConflict,
    VMCreationRetryStore,
    _json,
)
from orchestrator.services.vm_workspace_recovery_store import cleanup_intent_digest
from shared.vm_creation_retry import canonical_request_digest, retry_delay_seconds


def _object(value):
    value = _json(value)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise VMCreationRetryConflict("creation_request_unproven")
    return value


def _execution_binding(value):
    if not isinstance(value["execution_id"], str):
        raise ValueError("Invalid execution identity")
    execution_id = UUID(value["execution_id"])
    if str(execution_id) != value["execution_id"]:
        raise ValueError("Invalid execution identity")
    revision = value["execution_revision"]
    generation = value["execution_generation"]
    if (
        not isinstance(revision, str)
        or not revision
        or type(generation) is not int
        or generation < 1
    ):
        raise ValueError("Invalid execution snapshot")
    deadline = value["admission_deadline"]
    if deadline is not None:
        deadline = datetime.fromisoformat(deadline)
        if deadline.tzinfo is None or deadline.utcoffset() is None:
            raise ValueError("Invalid execution deadline")
    return {
        "execution_id": execution_id,
        "execution_revision": revision,
        "execution_generation": generation,
        "admission_deadline": deadline,
    }


def _preflight(vm):
    value = _object(vm.get("creation_preflight"))
    if not value:
        return None
    try:
        _execution_binding(value)
        if (
            type(value["version"]) is not int
            or value["version"] != 1
            or value["request"]["provision_generation"]
            != vm.get("provision_generation")
            or canonical_request_digest(value["request"]) != value["request_digest"]
            or not isinstance(value["request_id"], str)
            or str(UUID(value["request_id"])) != value["request_id"]
            or type(value["revision"]) is not int
            or value["revision"] < 0
            or type(value["attempt"]) is not int
            or value["attempt"] < 0
            or value["state"]
            not in {"queued", "resolving", "attention", "admitted", "settled"}
        ):
            raise ValueError
        expected_pvc = value.get("expected_pvc_uid")
        if expected_pvc is not None and (
            not isinstance(expected_pvc, str) or str(UUID(expected_pvc)) != expected_pvc
        ):
            raise ValueError
        for field in ("next_probe_at", "claim_expires_at", "outage_started_at"):
            number = value.get(field)
            if number is not None and (
                type(number) not in (int, float)
                or not math.isfinite(number)
                or number <= 0
            ):
                raise ValueError
    except (ValueError, TypeError, KeyError):
        raise VMCreationRetryConflict("creation_request_unproven") from None
    return deepcopy(value)


def creation_preflight_response(value):
    """A scheduling acknowledgement; it is never an actual boot attempt."""
    return {
        "creation_retry_protocol": 1,
        "status": "creation_attention"
        if value["state"] == "attention"
        else "creation_pending",
        "job_id": value["job_id"],
        "provision_generation": value["request"]["provision_generation"],
        "request_id": value["request_id"],
    }


def idle_wake_predecessor(
    vm, *, job_id: str, pvc_uid: str, max_attempts: int,
    current_storage: dict | None = None,
):
    """Validate the immutable first-create intent before retiring a usable VM.

    A stopped workspace may reuse its disk, but it does not receive a fresh
    provision-attempt or execution deadline budget. The original preflight
    remains the predecessor record; its request is never edited in place.
    """
    prior = _preflight(vm)
    attempts = vm.get("provision_attempts")
    if (
        prior is None
        or prior["state"] not in {"admitted", "settled"}
        or vm.get("creation_request_id") != prior["request_id"]
        or vm.get("identity_authenticated") is not True
        or vm.get("identity_provision_generation") != vm.get("provision_generation")
        or vm.get("rootdisk_pvc_uid") != pvc_uid
        or prior["request"].get("job_id") != job_id
        or prior["request"].get("entity_type") != "job"
        or type(attempts) is not int
        or not 0 <= attempts < max_attempts
    ):
        raise VMCreationRetryConflict(
            "vm_provisioning_exhausted"
            if type(attempts) is int and attempts >= max_attempts
            else "idle_wake_predecessor_unproven"
        )
    original_storage = prior["request"].get("workspace_storage")
    if original_storage is not None:
        from shared.vm_workspace_storage import storage_binding

        try:
            old_storage = storage_binding(original_storage)
            current = storage_binding(current_storage)
        except (TypeError, ValueError, AttributeError) as exc:
            raise VMCreationRetryConflict("retained_disk_changed") from exc
        if (
            current["pvc_uid"] != pvc_uid
            or old_storage["pvc_uid"] not in {None, pvc_uid}
            or any(
                old_storage[key] != current[key]
                for key in ("uid", "generation", "owner_id", "owner_kind")
            )
        ):
            raise VMCreationRetryConflict("retained_disk_changed")
    elif current_storage is not None:
        raise VMCreationRetryConflict("retained_disk_changed")
    if "network_profile" in prior["request"]:
        from shared.vm_network_profile import reusable_profile_evidence

        if not reusable_profile_evidence(
            vm.get("network_profile_evidence"), prior["request"]["network_profile"],
            provision_generation=vm.get("provision_generation"),
            vm_uid=vm.get("vm_uid"), pvc_uid=pvc_uid,
            vmi_uid=vm.get("vmi_uid"), launcher_uid=vm.get("active_pod_uid"),
        ):
            raise VMCreationRetryConflict("retained_network_profile_unproven")
    return prior


def idle_wake_request(prior, *, generation: str, current_storage=None):
    """Build a successor from captured options, changing only its generation.

    A retained binding may gain the controller-attested PVC UID after its
    initial create. Every other storage/accounting field stays immutable.
    """
    request = deepcopy(prior["request"])
    request["provision_generation"] = generation
    if current_storage is not None:
        request["workspace_storage"] = deepcopy(current_storage)
    canonical_request_digest(request)
    return request


class VMCreationPreflightStore:
    def __init__(self, db):
        self.db = db
        self.retry = VMCreationRetryStore(db)

    async def _lock(self, conn, job_id):
        raw = await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job_id)
        context = _object(raw)
        vm = _object(context.get("vm"))
        prior = _preflight(vm)
        retained = _object(context.get("last_vm"))
        pvc = vm.get("rootdisk_pvc_uid") or (
            prior.get("expected_pvc_uid") if prior else retained.get("rootdisk_pvc_uid")
        )
        from orchestrator.services.vm_creation_lineage import discover

        lineage = await discover(conn, job_id)
        if lineage:
            inherited_pvc = lineage["binding"]["pvc_uid"]
            if pvc is not None and pvc != inherited_pvc:
                raise VMCreationRetryConflict("retained_disk_changed")
            pvc = inherited_pvc
        job = await self.retry._scope(conn, job_id, UUID(pvc) if pvc else None)
        if job is None:
            raise VMCreationRetryConflict("job_changed")
        current = _object(job["context"])
        current_vm = _object(current.get("vm"))
        current_prior = _preflight(current_vm)
        current_old = _object(current.get("last_vm"))
        current_pvc = current_vm.get("rootdisk_pvc_uid") or (
            current_prior.get("expected_pvc_uid")
            if current_prior
            else current_old.get("rootdisk_pvc_uid")
        )
        if lineage:
            current_lineage = job.get("_creation_lineage_scope")
            if current_lineage != lineage:
                raise VMCreationRetryConflict("creation_attachment_lineage_unproven")
            current_pvc = current_pvc or current_lineage["binding"]["pvc_uid"]
        if current_pvc != pvc:
            raise VMCreationRetryConflict("retained_disk_changed")
        return job, current, current_vm, current_prior

    async def _predecessor(self, conn, job, old):
        lineage = job.get("_creation_lineage_scope")
        if lineage:
            if old:
                from orchestrator.services.vm_creation_replacement import (
                    prove_replacement,
                )

                own = await self._own_predecessor(conn, job, old)
                evidence, cleanup_id = await prove_replacement(
                    conn, job=job, binding=lineage["binding"], own_proposal=own
                )
                return {
                    "expected_pvc_uid": old["rootdisk_pvc_uid"],
                    "predecessor_evidence": evidence,
                    "predecessor_cleanup_admission_id": str(cleanup_id),
                }
            from orchestrator.services.vm_creation_lineage import prove

            evidence, cleanup_id = await prove(
                conn, job_id=job["id"], binding=lineage["binding"], scope=lineage
            )
            return {
                "expected_pvc_uid": lineage["binding"]["pvc_uid"],
                "predecessor_evidence": evidence,
                "predecessor_cleanup_admission_id": str(cleanup_id),
            }
        return await self._own_predecessor(conn, job, old)

    async def _own_predecessor(self, conn, job, old):
        if not old:
            return {"expected_pvc_uid": None}
        pvc = old.get("rootdisk_pvc_uid")
        if not pvc or old.get("identity_authenticated") is not True:
            raise VMCreationRetryConflict("creation_request_unproven")
        proposal = {
            "expected_pvc_uid": pvc,
            "predecessor_evidence": {
                "provision_generation": old.get("provision_generation"),
                "vm_uid": old.get("vm_uid"),
            },
        }
        cleanups = await conn.fetch(
            "SELECT id,source,intent_digest FROM vm_workspace_cleanup_admissions WHERE owner_kind='job' AND owner_id=$1 "
            "AND pvc_uid=$2 AND completed_at IS NOT NULL AND outcome='completed' ORDER BY completed_at DESC",
            job["id"],
            UUID(pvc),
        )
        for cleanup in cleanups:
            intent = {
                "owner_kind": "job",
                "owner_id": str(job["id"]),
                "provision_generation": old.get("provision_generation"),
                "vm_uid": old.get("vm_uid"),
                "pvc_uid": pvc,
                "purge_disk": False,
                "resource": "vm_workspace",
                "source": cleanup["source"],
            }
            if cleanup_intent_digest(intent) == cleanup["intent_digest"]:
                proposal["predecessor_cleanup_admission_id"] = str(cleanup["id"])
                await self.retry._own_predecessor(conn, job, UUID(pvc), proposal)
                return proposal
        raise VMCreationRetryConflict("predecessor_cleanup_pending")

    async def _network_profile_request(self, conn, job, request, predecessor, old):
        """Choose new intent or verify the immutable chain under existing locks."""
        from shared.vm_network_profile import selected_profile, validate_network_profile

        request = deepcopy(request)
        pvc_uid = predecessor["expected_pvc_uid"]
        lineage = job.get("_creation_lineage_scope")
        storage = request.get("workspace_storage")
        if storage is not None:
            from orchestrator.services.retained_vm_workspaces import provision_authority

            authority = await provision_authority(conn, str(job["id"]))
            if authority is None or authority["storage"] != storage:
                raise VMCreationRetryConflict("retained_disk_changed")
            expected = authority.get("network_profile")
            if "network_profile" in request and request["network_profile"] != expected:
                raise VMCreationRetryConflict("retained_network_profile_unproven")
            if expected is not None:
                validate_network_profile(expected)
                request["network_profile"] = expected
                if lineage:
                    original = await conn.fetchrow(
                        "SELECT canonical_request,request_digest FROM vm_creation_retries "
                        "WHERE job_id=$1 AND provision_generation=$2 FOR SHARE",
                        UUID(lineage["binding"]["owner_id"]),
                        UUID(lineage["original_vm"]["provision_generation"]),
                    )
                    prior_request = _object(original["canonical_request"]) if original else {}
                    if (
                        not original
                        or canonical_request_digest(prior_request) != original["request_digest"]
                        or prior_request.get("vm_image") != request.get("vm_image")
                    ):
                        raise VMCreationRetryConflict("retained_network_profile_source_changed")
                elif pvc_uid is None:
                    from orchestrator.services.manifest_execution_snapshot import srw_snapshot_config

                    snapshot = await conn.fetchrow(
                        "SELECT resolved,harness_adapter FROM srw_execution_specs "
                        "WHERE work_kind='Job' AND work_id=$1",
                        job["id"],
                    )
                    try:
                        _, policy = srw_snapshot_config(dict(snapshot))
                        source_image = (policy.get("workspace") or {}).get("vm", {}).get("image")
                    except (TypeError, KeyError, ValueError, HTTPException) as exc:
                        raise VMCreationRetryConflict("retained_network_profile_source_changed") from exc
                    if source_image != request.get("vm_image"):
                        raise VMCreationRetryConflict("retained_network_profile_source_changed")
            if (pvc_uid is not None and expected is None
                    and os.getenv("VM_NETWORK_PROFILE_ENABLED", "false").lower() == "true"):
                raise VMCreationRetryConflict("retained_network_profile_unproven")
        elif lineage:
            raise VMCreationRetryConflict("creation_attachment_lineage_unproven")
        elif pvc_uid is None:
            expected = selected_profile(
                request.get("vm_image"), prepared=request.get("preparation") is not None
            )
            if "network_profile" in request and request["network_profile"] != expected:
                raise VMCreationRetryConflict("network_profile_selection_unproven")
            if expected is not None:
                request["network_profile"] = expected
        else:
            rows = await conn.fetch(
                "SELECT canonical_request,request_digest,expected_pvc_uid,observed_pvc_uid,state "
                "FROM vm_creation_retries WHERE job_id=$1 AND "
                "(observed_pvc_uid=$2 OR expected_pvc_uid=$2) "
                "ORDER BY created_at,request_id FOR SHARE",
                job["id"], UUID(pvc_uid),
            )
            if (
                os.getenv("VM_NETWORK_PROFILE_ENABLED", "false").lower() != "true"
                and "network_profile" not in request
                and not any(
                    "network_profile" in _object(row["canonical_request"])
                    for row in rows
                )
            ):
                return request
            if not rows or rows[0]["expected_pvc_uid"] is not None:
                raise VMCreationRetryConflict("retained_network_profile_unproven")
            original = _object(rows[0]["canonical_request"])
            expected = original.get("network_profile")
            if expected is None:
                if (os.getenv("VM_NETWORK_PROFILE_ENABLED", "false").lower() == "true"
                        or "network_profile" in request
                        or any("network_profile" in _object(row["canonical_request"]) for row in rows)):
                    raise VMCreationRetryConflict("retained_network_profile_unproven")
                return request
            validate_network_profile(expected)
            if (
                rows[0]["state"] != "succeeded"
                or str(rows[0]["observed_pvc_uid"]) != pvc_uid
                or any(
                    _object(row["canonical_request"]).get("network_profile") != expected
                    or canonical_request_digest(_object(row["canonical_request"])) != row["request_digest"]
                    for row in rows
                )
                or ("network_profile" in request and request["network_profile"] != expected)
            ):
                raise VMCreationRetryConflict("retained_network_profile_unproven")
            request["network_profile"] = expected
            from shared.vm_network_profile import reusable_profile_evidence

            if not reusable_profile_evidence(
                old.get("network_profile_evidence"), expected,
                provision_generation=old.get("provision_generation"),
                vm_uid=old.get("vm_uid"), pvc_uid=pvc_uid,
                vmi_uid=old.get("vmi_uid"), launcher_uid=old.get("active_pod_uid"),
            ):
                raise VMCreationRetryConflict("retained_network_profile_unproven")
        return request

    async def begin(
        self, *, job_id, request, fresh_context, max_attempts=3,
        idle_wake_id: str | None = None,
    ):
        """Freeze a first initial request, or return the existing preflight."""
        if type(max_attempts) is not int or max_attempts < 1:
            raise ValueError("Invalid VM boot attempt limit")
        owner = UUID(job_id)
        if request.get("job_id") != job_id or request.get(
            "provision_generation"
        ) != fresh_context.get("provision_generation"):
            raise VMCreationRetryConflict("creation_request_unproven")
        canonical_request_digest(request)
        async with self.db.acquire() as conn:
            async with conn.transaction():
                job, context, old_vm, prior = await self._lock(conn, owner)
                idle_wake = None
                if idle_wake_id is None and (
                    old_vm.get("status") in {"suspending", "suspended"}
                    or old_vm.get("_suspend_remote_io_closed") is not None
                    or old_vm.get("idle_wake_operation_id") is not None
                ):
                    raise VMCreationRetryConflict("idle_wake_unproven")
                if idle_wake_id is not None:
                    try:
                        wake_uuid = UUID(idle_wake_id)
                        if str(wake_uuid) != idle_wake_id:
                            raise ValueError
                    except (TypeError, ValueError) as exc:
                        raise VMCreationRetryConflict("idle_wake_unproven") from exc
                    idle_wake = await conn.fetchrow(
                        "SELECT * FROM vm_idle_operations WHERE id=$1 AND owner_kind='job' "
                        "AND owner_id=$2 AND closed_at IS NULL FOR UPDATE",
                        wake_uuid, owner,
                    )
                    if (
                        idle_wake is None
                        or idle_wake["phase"] not in {"waking", "wake_held"}
                        or idle_wake["stop_verified_at"] is None
                        or idle_wake["retained_kind"] != "rootdisk"
                        or idle_wake["wake_generation"] is None
                        or str(idle_wake["wake_generation"])
                           != fresh_context.get("provision_generation")
                        or old_vm.get("idle_wake_operation_id") not in {None, idle_wake_id}
                        or (
                            old_vm.get("status") == "suspended"
                            and (
                                old_vm.get("provision_generation")
                                != str(idle_wake["provision_generation"])
                                or old_vm.get("vm_uid") != str(idle_wake["vm_uid"])
                                or old_vm.get("rootdisk_pvc_uid")
                                != str(idle_wake["pvc_uid"])
                            )
                        )
                    ):
                        raise VMCreationRetryConflict("idle_wake_unproven")
                idle_first = bool(
                    idle_wake is not None
                    and old_vm.get("status") == "suspended"
                    and job["status"] in {"waiting_for_reply", "pending_review", "paused"}
                )
                if idle_first:
                    current_storage = None
                    if prior and prior["request"].get("workspace_storage") is not None:
                        from orchestrator.services.retained_vm_workspaces import provision_binding

                        current_storage = await provision_binding(conn, job_id)
                    predecessor = idle_wake_predecessor(
                        old_vm, job_id=job_id, pvc_uid=str(idle_wake["pvc_uid"]),
                        max_attempts=max_attempts, current_storage=current_storage,
                    )
                    if request != idle_wake_request(
                        predecessor,
                        generation=str(idle_wake["wake_generation"]),
                        current_storage=current_storage,
                    ):
                        raise VMCreationRetryConflict("idle_wake_request_changed")
                # Completed provenance belongs to the retired generation. Its
                # successor still needs the exact receipt and retained-disk
                # cleanup chain below; keeping provenance must not bar it.
                retired = (
                    old_vm.get("status") == "deleted"
                    and old_vm.get("retirement_cleanup_pending") is not True
                )
                if prior and not idle_first and not (retired and prior["state"] == "admitted"):
                    if (old_vm.get("idle_wake_operation_id") or idle_wake_id) and (
                        old_vm.get("idle_wake_operation_id") != idle_wake_id
                    ):
                        raise VMCreationRetryConflict("idle_wake_unproven")
                    await self.retry._current(
                        conn,
                        job,
                        UUID(old_vm["provision_generation"]),
                        retry=_execution_binding(prior),
                    )
                    return prior
                if idle_wake is not None and not idle_first:
                    raise VMCreationRetryConflict("idle_wake_unproven")
                if (not idle_first and job["status"] not in {"created", "paused"}) or (
                    job["assigned_agent_id"] is not None
                ):
                    raise VMCreationRetryConflict("job_changed")
                if old_vm and (
                    old_vm.get("status") != "deleted" and not idle_first
                    or old_vm.get("retirement_cleanup_pending") is True
                ):
                    raise VMCreationRetryConflict("creation_request_unproven")
                if old_vm:
                    context["last_vm"] = old_vm
                attempts = old_vm.get(
                    "provision_attempts",
                    _object(context.get("last_vm")).get("provision_attempts", 0),
                )
                if type(attempts) is not int or attempts < 0:
                    raise VMCreationRetryConflict("creation_request_unproven")
                if attempts >= max_attempts:
                    raise VMCreationRetryConflict("vm_provisioning_exhausted")
                predecessor = await self._predecessor(
                    conn,
                    {**dict(job), "context": context},
                    _object(context.get("last_vm")),
                )
                request = await self._network_profile_request(
                    conn, job, request, predecessor, _object(context.get("last_vm"))
                )
                digest = canonical_request_digest(request)
                if job.get("_creation_lineage_scope"):
                    from orchestrator.services.vm_creation_prepared_lineage import (
                        validate_prepared_request,
                    )

                    validate_prepared_request(
                        predecessor["predecessor_evidence"], request
                    )
                storage = request.get("workspace_storage")
                if job.get("_creation_lineage_scope") and (
                    storage != job["_creation_lineage_scope"]["binding"]
                ):
                    raise VMCreationRetryConflict(
                        "creation_attachment_lineage_unproven"
                    )
                if (
                    storage is not None
                    and _object(storage).get("pvc_uid")
                    != predecessor["expected_pvc_uid"]
                ):
                    raise VMCreationRetryConflict("retained_disk_changed")
                vm = {
                    **deepcopy(fresh_context),
                    "status": "waiting_creation_configuration",
                    "initialization": request.get("initialization"),
                    "workspace_storage": storage,
                    "preparation_request": request.get("preparation"),
                    "provision_attempts": attempts,
                }
                if idle_first:
                    vm["idle_wake_operation_id"] = idle_wake_id
                context["vm"] = vm
                _, _, execution = await self.retry._current(
                    conn,
                    {**dict(job), "context": context},
                    UUID(vm["provision_generation"]),
                    retry=_execution_binding(predecessor_preflight)
                    if (
                        predecessor_preflight := prior
                        or _preflight(_object(context.get("last_vm")))
                    )
                    else None,
                )
                now = await conn.fetchval(
                    "SELECT extract(epoch FROM clock_timestamp())::double precision"
                )
                value = {
                    "version": 1,
                    "request_id": (
                        str(idle_wake["wake_request_id"])
                        if idle_first else str(uuid4())
                    ),
                    "job_id": job_id,
                    "request": deepcopy(request),
                    "request_digest": digest,
                    "revision": 0,
                    "attempt": 0,
                    "state": "queued",
                    "next_probe_at": now,
                    "claim_token": None,
                    "claim_expires_at": None,
                    "outage_started_at": None,
                    "execution_id": str(execution["id"]),
                    "execution_revision": execution["revision"],
                    "execution_generation": execution["generation"],
                    "admission_deadline": execution["deadline"].isoformat()
                    if execution["deadline"]
                    else None,
                    **predecessor,
                }
                vm["creation_preflight"] = value
                context["_vm_creation_pending"] = value["request_id"]
                await conn.execute(
                    "UPDATE jobs SET context=$2::jsonb,updated_at=clock_timestamp() WHERE id=$1",
                    owner,
                    json.dumps(context),
                )
                return value

    async def claim_due(self, *, limit):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("Invalid preflight batch size")
        # Page past held jobs instead of letting one ineligible head consume
        # LIMIT forever. Only scan jobs present when this pass began.
        async with self.db.acquire() as conn:
            scan_started = await conn.fetchval("SELECT clock_timestamp()")
            idle_schema = await conn.fetchval(
                "SELECT to_regclass('public.vm_idle_operations') IS NOT NULL"
            )
        due_status = "status IN ('created','paused')"
        if idle_schema:
            due_status = (
                "(status IN ('created','paused') OR (status IN ('waiting_for_reply','pending_review') "
                "AND EXISTS(SELECT 1 FROM vm_idle_operations i WHERE i.owner_kind='job' "
                "AND i.owner_id=jobs.id AND i.closed_at IS NULL AND i.phase IN ('waking','wake_held') "
                "AND i.id::text=jobs.context->'vm'->>'idle_wake_operation_id' "
                "AND i.wake_generation::text=jobs.context->'vm'->>'provision_generation' "
                "AND i.wake_request_id::text=jobs.context->'vm'->'creation_preflight'->>'request_id' "
                "AND i.stop_verified_at IS NOT NULL)))"
            )
        result = []
        cursor_time = None
        cursor_id = None
        while len(result) < limit:
            async with self.db.acquire() as conn:
                candidates = await conn.fetch(
                    "SELECT id,created_at FROM jobs WHERE context->'vm'->'creation_preflight'->>'state' IN ('queued','resolving') "
                    "AND " + due_status + " "
                    "AND NOT EXISTS(SELECT 1 FROM vm_creation_retries r "
                    "WHERE r.job_id=jobs.id AND r.provision_generation::text=context->'vm'->>'provision_generation') "
                    "AND CASE WHEN jsonb_typeof(context->'vm'->'creation_preflight'->'next_probe_at')='number' "
                    "THEN (context->'vm'->'creation_preflight'->>'next_probe_at')::double precision <= extract(epoch FROM clock_timestamp()) ELSE false END "
                    "AND CASE WHEN context->'vm'->'creation_preflight'->>'claim_expires_at' IS NULL THEN true "
                    "WHEN jsonb_typeof(context->'vm'->'creation_preflight'->'claim_expires_at')='number' "
                    "THEN (context->'vm'->'creation_preflight'->>'claim_expires_at')::double precision <= extract(epoch FROM clock_timestamp()) ELSE false END "
                    "AND created_at <= $1 AND ($2::timestamptz IS NULL OR (created_at,id)>($2,$3::uuid)) "
                    "ORDER BY created_at,id LIMIT $4",
                    scan_started,
                    cursor_time,
                    cursor_id,
                    max(20, limit),
                )
            if not candidates:
                break
            for candidate in candidates:
                value = await self._claim_candidate(candidate["id"])
                if value is not None:
                    result.append(value)
                    if len(result) == limit:
                        break
            cursor_time, cursor_id = candidates[-1]["created_at"], candidates[-1]["id"]
        return result

    async def _claim_candidate(self, job_id):
        try:
            async with self.db.acquire() as conn:
                async with conn.transaction():
                    job, _, vm, value = await self._lock(conn, job_id)
                    try:
                        await self.retry._current(
                            conn,
                            job,
                            UUID(vm["provision_generation"]),
                            retry=_execution_binding(value) if value else None,
                        )
                    except VMCreationRetryConflict as exc:
                        if (
                            exc.reason
                            not in {
                                "job_admission_expired",
                                "execution_manifest_changed",
                            }
                            or not value
                        ):
                            raise
                        value.update(
                            state="attention",
                            reason=exc.reason,
                            revision=value["revision"] + 1,
                            claim_token=None,
                            claim_expires_at=None,
                        )
                        await self._write(conn, job["id"], value)
                        return None
                    now = await conn.fetchval(
                        "SELECT extract(epoch FROM clock_timestamp())::double precision"
                    )
                    if (
                        not value
                        or value["state"] not in {"queued", "resolving"}
                        or value["next_probe_at"] > now
                        or value.get("claim_expires_at") is not None
                        and value["claim_expires_at"] > now
                    ):
                        return None
                    if await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM vm_creation_retries WHERE job_id=$1 AND provision_generation=$2)",
                        job["id"],
                        UUID(vm["provision_generation"]),
                    ):
                        return None
                    value.update(
                        state="resolving",
                        revision=value["revision"] + 1,
                        claim_token=str(uuid4()),
                        claim_expires_at=now + 60,
                    )
                    await self._write(conn, job["id"], value)
                    return value
        except VMCreationRetryConflict:
            return None

    async def _write(self, conn, job_id, value):
        await conn.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{vm,creation_preflight}',$2::jsonb),updated_at=clock_timestamp() WHERE id=$1",
            job_id,
            json.dumps(value),
        )

    async def _claimed(self, conn, claim):
        job, _, vm, value = await self._lock(conn, UUID(claim["job_id"]))
        await self.retry._current(
            conn,
            job,
            UUID(vm["provision_generation"]),
            retry=_execution_binding(value) if value else None,
        )
        now = await conn.fetchval(
            "SELECT extract(epoch FROM clock_timestamp())::double precision"
        )
        if (
            not value
            or value["state"] != "resolving"
            or value["revision"] != claim["revision"]
            or value["claim_token"] != claim["claim_token"]
            or value["claim_expires_at"] <= now
            or value["request_id"] != claim["request_id"]
        ):
            raise VMCreationRetryConflict("creation_claim_stale")
        if await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_creation_retries WHERE job_id=$1 AND provision_generation=$2)",
            job["id"],
            UUID(vm["provision_generation"]),
        ):
            raise VMCreationRetryConflict("creation_already_admitted")
        return job, value, now

    async def record_failure(self, claim, *, reason):
        if reason not in {"controller_unavailable", "creation_configuration_unproven"}:
            raise ValueError("Invalid creation preflight reason")
        try:
            async with self.db.acquire() as conn:
                async with conn.transaction():
                    job, value, now = await self._claimed(conn, claim)
                    attempt = value["attempt"] + 1
                    outage = value.get("outage_started_at") or now
                    attention = (
                        reason == "creation_configuration_unproven"
                        or now - outage >= 900
                    )
                    value.update(
                        state="attention" if attention else "queued",
                        revision=value["revision"] + 1,
                        attempt=attempt,
                        claim_token=None,
                        claim_expires_at=None,
                        outage_started_at=outage,
                        reason=reason,
                        next_probe_at=now + retry_delay_seconds(attempt),
                    )
                    await self._write(conn, job["id"], value)
                    return True
        except VMCreationRetryConflict:
            return False

    async def complete_resolution(self, claim, resolved):
        """Atomically freeze authenticated configuration and hand off to ledger."""
        from orchestrator.services.vm_creation_request import (
            capture_vm_creation_request,
        )
        from orchestrator.services.vm_creation_transport import (
            validate_creation_resolution,
        )

        async with self.db.acquire() as conn:
            async with conn.transaction():
                job, value, _ = await self._claimed(conn, claim)
                resolved = validate_creation_resolution(value["request"], resolved)
                generation = value["request"]["provision_generation"]
                snapshot = await capture_vm_creation_request(
                    self.db,
                    job_id=str(job["id"]),
                    generation=generation,
                    request=resolved["request"],
                    initial_request=True,
                    controller_configuration=resolved["controller_configuration"],
                    controller_configuration_digest=resolved[
                        "controller_configuration_digest"
                    ],
                    _conn=conn,
                )
                if snapshot is None:
                    raise VMCreationRetryConflict("creation_request_unproven")
                proposal = {
                    "origin": "initial",
                    "idle_wake_id": (
                        (_object(_object(job["context"]).get("vm")))
                        .get("idle_wake_operation_id")
                    ),
                    "expected_status": job["status"],
                    "request_digest": snapshot["request_digest"],
                    "controller_configuration_digest": snapshot[
                        "controller_configuration_digest"
                    ],
                    **{
                        key: value.get(key)
                        for key in (
                            "expected_pvc_uid",
                            "predecessor_evidence",
                            "predecessor_cleanup_admission_id",
                        )
                    },
                }
                admitted = await self.retry.admit_on_conn(
                    conn,
                    job_id=str(job["id"]),
                    expected_generation=generation,
                    request_id=value["request_id"],
                    proposal=proposal,
                )
                value.update(
                    state="admitted",
                    revision=value["revision"] + 1,
                    claim_token=None,
                    claim_expires_at=None,
                )
                await self._write(conn, job["id"], value)
                return admitted

    async def settle_cancelled(self, *, limit):
        """Settle only read-only preflights; an existing ledger always owns effects."""
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("Invalid preflight batch size")
        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id FROM jobs WHERE (status IN ('cancelled','completed') OR "
                "context ?| ARRAY['_stateless_delete_pending','_stateless_cancel_cleanup_pending']) "
                "AND context->'vm'->'creation_preflight'->>'state' IN ('queued','resolving','attention') "
                "AND NOT EXISTS(SELECT 1 FROM vm_creation_retries r WHERE r.job_id=jobs.id "
                "AND r.provision_generation::text=context->'vm'->>'provision_generation') ORDER BY id LIMIT $1",
                limit,
            )
        count = 0
        for row in rows:
            try:
                async with self.db.acquire() as conn:
                    async with conn.transaction():
                        job, context, vm, value = await self._lock(conn, row["id"])
                        if (
                            not value
                            or value["state"]
                            not in {"queued", "resolving", "attention"}
                            or context.get("_vm_creation_pending")
                            != value["request_id"]
                            or job["status"] not in {"cancelled", "completed"}
                            and not any(
                                key in context
                                for key in (
                                    "_stateless_delete_pending",
                                    "_stateless_cancel_cleanup_pending",
                                )
                            )
                        ):
                            raise VMCreationRetryConflict("preflight_control_changed")
                        if await conn.fetchval(
                            "SELECT EXISTS(SELECT 1 FROM vm_creation_retries WHERE job_id=$1 AND provision_generation=$2)",
                            job["id"],
                            UUID(vm["provision_generation"]),
                        ):
                            raise VMCreationRetryConflict("creation_already_admitted")
                        value.update(
                            state="settled",
                            revision=value["revision"] + 1,
                            claim_token=None,
                            claim_expires_at=None,
                            reason="creation_cancelled_before_issuance",
                        )
                        vm["creation_preflight"] = value
                        context.pop("_vm_creation_pending")
                        await conn.execute(
                            "UPDATE jobs SET context=$2::jsonb,updated_at=clock_timestamp() WHERE id=$1",
                            job["id"],
                            json.dumps(context),
                        )
                        count += 1
            except VMCreationRetryConflict:
                continue
        return count
