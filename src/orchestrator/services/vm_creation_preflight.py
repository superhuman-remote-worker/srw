"""Durable read-only resolution before the VM creation effect ledger exists.

The preflight freezes caller intent and holds worker admission. It grants no
controller or Kubernetes effects. Full configuration and ledger admission must
commit together before any protocol create is eligible.
"""

from copy import deepcopy
import json
import math
from uuid import UUID, uuid4

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


def _preflight(vm):
    value = _object(vm.get("creation_preflight"))
    if not value:
        return None
    try:
        if (
            type(value["version"]) is not int
            or value["version"] != 1
            or value["request"]["provision_generation"]
            != vm.get("provision_generation")
            or canonical_request_digest(value["request"]) != value["request_digest"]
            or str(UUID(value["request_id"])) != value["request_id"]
            or type(value["revision"]) is not int
            or value["revision"] < 0
            or type(value["attempt"]) is not int
            or value["attempt"] < 0
            or value["state"]
            not in {"queued", "resolving", "attention", "admitted", "settled"}
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
        pvc = (
            prior.get("expected_pvc_uid")
            if prior
            else vm.get("rootdisk_pvc_uid") or retained.get("rootdisk_pvc_uid")
        )
        job = await self.retry._scope(conn, job_id, UUID(pvc) if pvc else None)
        if job is None:
            raise VMCreationRetryConflict("job_changed")
        current = _object(job["context"])
        current_vm = _object(current.get("vm"))
        current_prior = _preflight(current_vm)
        current_old = _object(current.get("last_vm"))
        current_pvc = (
            current_prior.get("expected_pvc_uid")
            if current_prior
            else current_vm.get("rootdisk_pvc_uid")
            or current_old.get("rootdisk_pvc_uid")
        )
        if current_pvc != pvc:
            raise VMCreationRetryConflict("retained_disk_changed")
        return job, current, current_vm, current_prior

    async def _predecessor(self, conn, job, old):
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
                await self.retry._predecessor(conn, job, UUID(pvc), proposal)
                return proposal
        raise VMCreationRetryConflict("predecessor_cleanup_pending")

    async def begin(self, *, job_id, request, fresh_context, max_attempts=3):
        """Freeze a first initial request, or return the existing preflight."""
        if type(max_attempts) is not int or max_attempts < 1:
            raise ValueError("Invalid VM boot attempt limit")
        owner = UUID(job_id)
        if request.get("job_id") != job_id or request.get(
            "provision_generation"
        ) != fresh_context.get("provision_generation"):
            raise VMCreationRetryConflict("creation_request_unproven")
        digest = canonical_request_digest(request)
        async with self.db.acquire() as conn:
            async with conn.transaction():
                job, context, old_vm, prior = await self._lock(conn, owner)
                if prior:
                    await self.retry._current(
                        conn, job, UUID(old_vm["provision_generation"])
                    )
                    return prior
                if (
                    job["status"] not in {"created", "paused"}
                    or job["assigned_agent_id"] is not None
                ):
                    raise VMCreationRetryConflict("job_changed")
                if old_vm and (
                    old_vm.get("status") != "deleted"
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
                storage = request.get("workspace_storage")
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
                context["vm"] = vm
                await self.retry._current(
                    conn,
                    {**dict(job), "context": context},
                    UUID(vm["provision_generation"]),
                )
                now = await conn.fetchval(
                    "SELECT extract(epoch FROM clock_timestamp())::double precision"
                )
                value = {
                    "version": 1,
                    "request_id": str(uuid4()),
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
        async with self.db.acquire() as conn:
            candidates = await conn.fetch(
                "SELECT id FROM jobs WHERE context->'vm'->'creation_preflight'->>'state' IN ('queued','resolving') "
                "AND status IN ('created','paused') AND NOT EXISTS(SELECT 1 FROM vm_creation_retries r "
                "WHERE r.job_id=jobs.id AND r.provision_generation::text=context->'vm'->>'provision_generation') "
                "AND CASE WHEN jsonb_typeof(context->'vm'->'creation_preflight'->'next_probe_at')='number' "
                "THEN (context->'vm'->'creation_preflight'->>'next_probe_at')::double precision <= extract(epoch FROM clock_timestamp()) ELSE false END "
                "ORDER BY created_at,id LIMIT $1",
                limit,
            )
        result = []
        for candidate in candidates:
            try:
                async with self.db.acquire() as conn:
                    async with conn.transaction():
                        job, _, vm, value = await self._lock(conn, candidate["id"])
                        try:
                            await self.retry._current(
                                conn, job, UUID(vm["provision_generation"])
                            )
                        except VMCreationRetryConflict as exc:
                            if exc.reason != "job_admission_expired" or not value:
                                raise
                            value.update(
                                state="attention",
                                reason=exc.reason,
                                revision=value["revision"] + 1,
                                claim_token=None,
                                claim_expires_at=None,
                            )
                            await self._write(conn, job["id"], value)
                            continue
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
                            continue
                        if await conn.fetchval(
                            "SELECT EXISTS(SELECT 1 FROM vm_creation_retries WHERE job_id=$1 AND provision_generation=$2)",
                            job["id"],
                            UUID(vm["provision_generation"]),
                        ):
                            continue
                        value.update(
                            state="resolving",
                            revision=value["revision"] + 1,
                            claim_token=str(uuid4()),
                            claim_expires_at=now + 60,
                        )
                        await self._write(conn, job["id"], value)
                        result.append(value)
            except VMCreationRetryConflict:
                continue
        return result

    async def _write(self, conn, job_id, value):
        await conn.execute(
            "UPDATE jobs SET context=jsonb_set(context,'{vm,creation_preflight}',$2::jsonb),updated_at=clock_timestamp() WHERE id=$1",
            job_id,
            json.dumps(value),
        )

    async def _claimed(self, conn, claim):
        job, _, vm, value = await self._lock(conn, UUID(claim["job_id"]))
        await self.retry._current(conn, job, UUID(vm["provision_generation"]))
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
