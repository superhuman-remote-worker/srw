"""Durable exact-runtime VM idle operations.

The Job adapter serializes with stateless worker claims (queue before Job).
External VM effects are deliberately outside these transactions.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import json
import inspect
import logging
import os
from typing import Any, Mapping
from uuid import UUID, uuid4

from orchestrator.services.vm_provisioner import (
    VMTeardownIdentity,
    vm_persistent_rootdisk_enabled,
)
from orchestrator.services.vm_remote_operation import (
    VMRemoteOperationUnavailable,
    _identity_from_row,
    vm_remote_operation_protocol_enabled,
)
from orchestrator.services.vm_workspace_recovery_store import (
    _job_workspace_owner,
    acquire_vm_cleanup_permit,
    complete_vm_cleanup_permit,
    completed_cleanup_outcome,
    vm_cleanup_kwargs,
)
from shared.workspace_idle_policy import RuntimeIdentity, evaluate_idle, read_episode

logger = logging.getLogger(__name__)


def _object(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    return value if isinstance(value, dict) else {}


def _episode_document(value: Any) -> dict[str, Any] | None:
    return None if value is None else _object(value)


def _uuid(value: Any) -> UUID | None:
    try:
        parsed = UUID(str(value))
        return parsed if str(parsed) == str(value) else None
    except (TypeError, ValueError, AttributeError):
        return None


class VMIdleLifecycleStore:
    def __init__(self, db: Any) -> None:
        self.db = db

    async def schema_available(self) -> bool:
        async with self.db.acquire() as conn:
            return bool(await conn.fetchval(
                "SELECT to_regclass('public.vm_idle_operations') IS NOT NULL"
            ))

    async def admit_release(
        self,
        job_id: str,
        *,
        episode_id: str,
        revision: int,
        identity: Mapping[str, str],
        warm_seconds: int = 900,
    ) -> dict[str, Any] | None:
        """Reserve one stateless Job release after a fresh, locked recheck."""

        if (
            os.getenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false").lower() != "true"
            or os.getenv("VM_CREATION_RETRY_ENABLED", "false").lower() != "true"
            or not vm_remote_operation_protocol_enabled()
            or not vm_persistent_rootdisk_enabled()
            or getattr(self.db, "supports_vm_creation_retry", False) is not True
            or _uuid(job_id) is None
            or _uuid(episode_id) is None
            or type(revision) is not int
            or revision < 1
        ):
            return None
        expected = {
            key: _uuid(identity.get(key))
            for key in ("generation", "vm_uid", "vmi_uid", "launcher_uid", "pvc_uid")
        }
        if any(value is None for value in expected.values()):
            return None
        owner_id = UUID(job_id)
        async with self.db.acquire() as conn, conn.transaction():
            queue = await conn.fetchrow(
                "SELECT state,unit_kind,lease_token,leased_until FROM run_queue "
                "WHERE unit_id=$1 FOR UPDATE",
                owner_id,
            )
            if queue is None or queue["unit_kind"] != "worker_batch":
                return None
            row = await conn.fetchrow(
                "SELECT * FROM jobs WHERE id=$1 FOR UPDATE", owner_id
            )
            if row is None:
                return None
            operation = await conn.fetchrow(
                "SELECT * FROM vm_idle_operations WHERE owner_kind='job' "
                "AND owner_id=$1 AND closed_at IS NULL FOR UPDATE",
                owner_id,
            )
            if operation is not None:
                if (
                    operation["episode_id"] == UUID(episode_id)
                    and operation["episode_revision"] == revision
                    and all(
                        operation[column] == expected[key]
                        for key, column in (
                            ("generation", "provision_generation"),
                            ("vm_uid", "vm_uid"),
                            ("vmi_uid", "vmi_uid"),
                            ("launcher_uid", "launcher_uid"),
                            ("pvc_uid", "pvc_uid"),
                        )
                    )
                ):
                    return dict(operation)
                return None
            if row["execution_lane"] != "stateless" or row["assigned_agent_id"] is not None:
                return None
            # A parked VM is only safe to stop when its successor can enter
            # the already deployed creation protocol within the original
            # execution deadline. Do not strand an active human wait.
            executable = await conn.fetchval(
                """
                SELECT EXISTS(
                  SELECT 1 FROM srw_execution_specs s
                  WHERE s.work_kind='Job' AND s.work_id=$1
                    AND s.harness_adapter='srw/v1'
                    AND (s.resolved->'spec'->>'timeoutSeconds' IS NULL
                         OR s.created_at +
                            ((s.resolved->'spec'->>'timeoutSeconds')::double precision
                             * interval '1 second') > clock_timestamp())
                )
                """,
                owner_id,
            )
            if not executable:
                return None
            context = _object(row["context"])
            vm = _object(context.get("vm"))
            freeze = _object(row["freeze_data"])
            episode = read_episode(
                _episode_document(row["workspace_idle_episode"]),
                revision=row["workspace_idle_revision"],
            )
            if (
                episode is None
                or episode.wait_kind != "human_message"
                or episode.episode_id != episode_id
                or episode.revision != revision
                or vm.get("status") != "ready"
                or queue["state"] not in {"done", "parked"}
            ):
                return None
            owner, ambiguous = _job_workspace_owner(owner_id, dict(row))
            if ambiguous or owner != owner_id:
                return None
            try:
                current = _identity_from_row(
                    dict(row),
                    owner_kind="job",
                    owner_id=job_id,
                    operation_kind="idle_policy",
                )
            except VMRemoteOperationUnavailable:
                return None
            if (
                current.workspace_generation != str(expected["generation"])
                or current.vm_uid != str(expected["vm_uid"])
                or current.launcher_pod_uid != str(expected["launcher_uid"])
                or _uuid(vm.get("vmi_uid")) != expected["vmi_uid"]
                or _uuid(vm.get("rootdisk_pvc_uid")) != expected["pvc_uid"]
            ):
                return None
            human_wait_current = bool(
                row["status"] == "waiting_for_reply"
                and freeze.get("route_id") == episode.wait_key
            )
            now = await conn.fetchval("SELECT clock_timestamp()")
            decision = evaluate_idle(
                episode,
                now=now,
                runtime=RuntimeIdentity(
                    "job", job_id, "vm", current.workspace_generation, current.vm_uid
                ),
                human_wait_current=human_wait_current,
                supported=True,
                enabled=True,
                warm_seconds=warm_seconds,
            )
            if decision.state != "eligible":
                return None
            holds = await conn.fetchval(
                """
                SELECT EXISTS(
                    SELECT 1 FROM vm_idle_access_leases a
                    WHERE a.owner_kind='job' AND a.owner_id=$1
                      AND a.closed_at IS NULL AND a.expires_at>clock_timestamp()
                ) OR EXISTS(
                    SELECT 1 FROM vm_remote_operation_leases r
                    WHERE r.owner_kind='job' AND r.owner_id=$1 AND r.settled_at IS NULL
                ) OR EXISTS(
                    SELECT 1 FROM vm_workspace_recoveries r
                    WHERE r.owner_kind='job' AND r.owner_id=$1 AND r.resolved_at IS NULL
                ) OR EXISTS(
                    SELECT 1 FROM vm_workspace_recovery_jobs r
                    WHERE r.job_id=$1 AND r.resolved_at IS NULL
                ) OR EXISTS(
                    SELECT 1 FROM vm_workspace_cleanup_admissions a
                    WHERE a.owner_kind='job' AND a.owner_id=$1 AND a.completed_at IS NULL
                ) OR EXISTS(
                    SELECT 1 FROM jobs child WHERE child.parent_job_id=$1
                      AND child.status NOT IN ('completed','failed','cancelled')
                      AND child.context->>'inherits_parent_workspace'='true'
                ) OR EXISTS(
                    SELECT 1 FROM job_completion_commands c WHERE c.job_id=$1
                      AND c.state IN ('pending','finalizing','parked')
                ) OR EXISTS(
                    SELECT 1 FROM job_completion_sweep_exclusions c WHERE c.job_id=$1
                )
                """,
                owner_id,
            )
            ide = _object(context.get("ide_session"))
            if holds or ide.get("status") in {"active", "idle", "restoring"}:
                return None
            operation = await conn.fetchrow(
                """
                INSERT INTO vm_idle_operations
                    (owner_kind,owner_id,phase,episode_id,episode_revision,
                     provision_generation,vm_uid,vmi_uid,launcher_uid,pvc_uid,retained_kind)
                VALUES ('job',$1,'releasing',$2,$3,$4,$5,$6,$7,$8,'rootdisk')
                RETURNING *
                """,
                owner_id,
                UUID(episode_id),
                revision,
                expected["generation"],
                expected["vm_uid"],
                expected["vmi_uid"],
                expected["launcher_uid"],
                expected["pvc_uid"],
            )
            vm.update(status="suspending", _suspend_remote_io_closed=str(operation["id"]))
            context["vm"] = vm
            await conn.execute(
                "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                owner_id,
                json.dumps(context),
            )
            return dict(operation)

    async def complete_release(
        self, operation_id: str, *, evidence: Mapping[str, Any]
    ) -> bool:
        """Publish suspended only after exact process zero and physical stop."""

        if _uuid(operation_id) is None or not isinstance(evidence, Mapping):
            return False
        async with self.db.acquire() as conn, conn.transaction():
            located = await conn.fetchrow(
                "SELECT owner_kind,owner_id FROM vm_idle_operations WHERE id=$1",
                UUID(operation_id),
            )
            if located is None or located["owner_kind"] != "job":
                return False
            owner_id = located["owner_id"]
            await conn.fetchrow(
                "SELECT unit_id FROM run_queue WHERE unit_id=$1 FOR UPDATE", owner_id
            )
            job = await conn.fetchrow(
                "SELECT status,context,workspace_idle_revision,workspace_idle_episode "
                "FROM jobs WHERE id=$1 FOR UPDATE",
                owner_id,
            )
            operation = await conn.fetchrow(
                "SELECT * FROM vm_idle_operations WHERE id=$1 FOR UPDATE",
                UUID(operation_id),
            )
            if job is None or operation is None:
                return False
            if operation["phase"] == "suspended":
                return True
            if operation["phase"] not in {"releasing", "release_held"}:
                return False
            expected = {
                "operation_id": str(operation["id"]),
                "generation": str(operation["provision_generation"]),
                "vm_uid": str(operation["vm_uid"]),
                "vmi_uid": str(operation["vmi_uid"]),
                "launcher_uid": str(operation["launcher_uid"]),
                "pvc_uid": str(operation["pvc_uid"]),
            }
            if (
                evidence.get("version") != 1
                or evidence.get("kind") != "vm_idle_physical_stop"
                or any(evidence.get(key) != value for key, value in expected.items())
                or any(
                    evidence.get(key) is not True
                    for key in (
                        "vm_absent", "vmi_absent", "launcher_absent",
                        "retained_pvc", "controller_authenticated",
                    )
                )
                or evidence.get("same_generation_replacement") is not False
            ):
                return False
            process_zero = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM managed_repository_process_zero_receipts "
                "WHERE owner_kind='job' AND owner_id=$1 AND scope='vm' "
                "AND provisioner='vm' AND runtime_incarnation=$2)",
                owner_id,
                str(operation["provision_generation"]),
            )
            if not process_zero:
                return False
            context = _object(job["context"])
            vm = _object(context.get("vm"))
            if (
                _uuid(vm.get("provision_generation"))
                != operation["provision_generation"]
                or _uuid(vm.get("vm_uid")) != operation["vm_uid"]
                or _uuid(vm.get("rootdisk_pvc_uid")) != operation["pvc_uid"]
                or vm.get("_suspend_remote_io_closed") != operation_id
                or vm.get("status") != "suspending"
            ):
                return False
            vm.update(status="suspended", rootdisk="kept")
            context["vm"] = vm
            await conn.execute(
                "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                owner_id,
                json.dumps(context),
            )
            current_episode = read_episode(
                _episode_document(job["workspace_idle_episode"]),
                revision=job["workspace_idle_revision"],
            )
            wake_requested = bool(
                current_episode is None
                or current_episode.episode_id != str(operation["episode_id"])
                or current_episode.revision != operation["episode_revision"]
            )
            await conn.execute(
                "UPDATE vm_idle_operations SET phase='suspended',"
                "stop_evidence=$2::jsonb,stop_verified_at=clock_timestamp(),"
                "wake_requested=wake_requested OR $3,"
                "wake_execution_requested=wake_execution_requested OR $3,"
                "last_progress_at=clock_timestamp() "
                "WHERE id=$1",
                operation["id"],
                json.dumps(dict(evidence)),
                wake_requested,
            )
            return True

    async def request_wake(
        self, job_id: str, *, execution_requested: bool,
        access_kind: str | None = None, access_claimant: str | None = None,
    ) -> dict[str, Any] | None:
        """Share one exact successor for access and execution across replicas."""
        owner_id = _uuid(job_id)
        if owner_id is None:
            return None
        if access_kind is not None and (
            execution_requested or access_kind not in {"ssh", "sftp", "ide"}
            or not isinstance(access_claimant, str)
            or not 1 <= len(access_claimant) <= 256
        ):
            return None
        async with self.db.acquire() as conn, conn.transaction():
            queue = await conn.fetchrow(
                "SELECT unit_kind FROM run_queue WHERE unit_id=$1 FOR UPDATE",
                owner_id,
            )
            job = await conn.fetchrow(
                "SELECT status,execution_lane,context FROM jobs WHERE id=$1 FOR UPDATE",
                owner_id,
            )
            if (
                queue is None or queue["unit_kind"] != "worker_batch"
                or job is None or job["execution_lane"] != "stateless"
                or job["status"] in {"completed", "failed", "cancelled"}
            ):
                return None
            operation = await conn.fetchrow(
                "SELECT * FROM vm_idle_operations WHERE owner_kind='job' "
                "AND owner_id=$1 AND closed_at IS NULL FOR UPDATE", owner_id,
            )
            if operation is None or operation["phase"] not in {
                "releasing", "release_held", "suspended", "waking", "wake_held",
            }:
                return None
            vm = _object(_object(job["context"]).get("vm"))
            context = _object(job["context"])
            previous = _object(context.get("last_vm"))
            original = (
                _uuid(vm.get("provision_generation")) == operation["provision_generation"]
                and _uuid(vm.get("rootdisk_pvc_uid")) == operation["pvc_uid"]
            )
            successor = (
                operation["wake_generation"] is not None
                and _uuid(vm.get("provision_generation")) == operation["wake_generation"]
                and vm.get("idle_wake_operation_id") == str(operation["id"])
                and _uuid(previous.get("rootdisk_pvc_uid")) == operation["pvc_uid"]
            )
            if not (original or successor):
                return None
            wake_id = operation["wake_id"] or uuid4()
            generation = operation["wake_generation"] or uuid4()
            request_id = operation["wake_request_id"] or uuid4()
            phase = (
                "waking" if operation["phase"] == "suspended"
                else operation["phase"]
            )
            row = await conn.fetchrow(
                """
                UPDATE vm_idle_operations SET phase=$2,wake_requested=true,
                  wake_execution_requested=wake_execution_requested OR $3,
                  wake_id=$4,wake_generation=$5,wake_request_id=$6,
                  retry_after=NULL,last_progress_at=clock_timestamp()
                WHERE id=$1 RETURNING *
                """,
                operation["id"], phase, execution_requested,
                wake_id, generation, request_id,
            )
            if access_kind is not None:
                lease = await conn.fetchrow(
                    """
                    SELECT id FROM vm_idle_access_leases
                    WHERE owner_kind='job' AND owner_id=$1 AND kind=$2
                      AND claimed_by=$3 AND wake_id=$4 AND closed_at IS NULL
                      AND max_expires_at>clock_timestamp()
                    ORDER BY acquired_at DESC LIMIT 1 FOR UPDATE
                    """,
                    owner_id, access_kind, access_claimant, wake_id,
                )
                if lease:
                    await conn.execute(
                        "UPDATE vm_idle_access_leases SET expires_at=LEAST(max_expires_at,"
                        "clock_timestamp()+interval '2 minutes') WHERE id=$1",
                        lease["id"],
                    )
                else:
                    lease_generation = (
                        operation["wake_generation"]
                        if operation["wake_ready_at"] is not None
                        else operation["provision_generation"]
                    )
                    lease_vm_uid = (
                        _uuid(vm.get("vm_uid"))
                        if operation["wake_ready_at"] is not None
                        else operation["vm_uid"]
                    )
                    if lease_vm_uid is None:
                        return None
                    await conn.execute(
                        """
                        INSERT INTO vm_idle_access_leases
                          (owner_kind,owner_id,provision_generation,vm_uid,wake_id,
                           kind,claimed_by,expires_at,max_expires_at)
                        VALUES('job',$1,$2,$3,$4,$5,$6,
                               clock_timestamp()+interval '2 minutes',
                               clock_timestamp()+interval '1 hour')
                        """,
                        owner_id, lease_generation, lease_vm_uid,
                        wake_id, access_kind, access_claimant,
                    )
            return dict(row)

    async def mark_wake_ready(
        self, operation_id: str, *, generation: str, vm_uid: str,
        vmi_uid: str, launcher_uid: str, pvc_uid: str,
    ) -> bool:
        """Publish only the exact attested successor; retain execution intent."""
        if any(
            _uuid(value) is None
            for value in (operation_id, generation, vm_uid, vmi_uid, launcher_uid, pvc_uid)
        ):
            return False
        async with self.db.acquire() as conn, conn.transaction():
            located = await conn.fetchrow(
                "SELECT owner_id FROM vm_idle_operations WHERE id=$1", UUID(operation_id)
            )
            if located is None:
                return False
            owner_id = located["owner_id"]
            await conn.fetchrow("SELECT unit_id FROM run_queue WHERE unit_id=$1 FOR UPDATE", owner_id)
            job = await conn.fetchrow(
                "SELECT status,context,workspace_idle_episode,workspace_idle_revision "
                "FROM jobs WHERE id=$1 FOR UPDATE", owner_id,
            )
            operation = await conn.fetchrow(
                "SELECT * FROM vm_idle_operations WHERE id=$1 FOR UPDATE", UUID(operation_id)
            )
            if job is None or operation is None or operation["phase"] not in {"waking", "wake_held"}:
                return False
            vm = _object(_object(job["context"]).get("vm"))
            if (
                vm.get("status") != "ready"
                or vm.get("idle_wake_operation_id") != operation_id
                or vm.get("provision_generation") != generation
                or vm.get("vm_uid") != vm_uid
                or vm.get("vmi_uid") != vmi_uid
                or vm.get("active_pod_uid") != launcher_uid
                or vm.get("rootdisk_pvc_uid") != pvc_uid
                or operation["wake_generation"] != UUID(generation)
                or operation["pvc_uid"] != UUID(pvc_uid)
                or operation["vm_uid"] == UUID(vm_uid)
                or operation["vmi_uid"] == UUID(vmi_uid)
                or operation["launcher_uid"] == UUID(launcher_uid)
                or operation["stop_verified_at"] is None
            ):
                return False
            episode = read_episode(
                _episode_document(job["workspace_idle_episode"]),
                revision=job["workspace_idle_revision"],
            )
            episode_changed = bool(
                episode is None
                or episode.episode_id != str(operation["episode_id"])
                or episode.revision != operation["episode_revision"]
            )
            await conn.execute(
                "UPDATE vm_idle_operations SET wake_ready_at=clock_timestamp(),"
                "wake_execution_requested=wake_execution_requested OR $2,"
                "retry_after=NULL,last_progress_at=clock_timestamp() WHERE id=$1",
                operation["id"], episode_changed,
            )
            await conn.execute(
                """
                UPDATE vm_idle_access_leases SET
                  provision_generation=$2,vm_uid=$3,
                  expires_at=LEAST(max_expires_at,clock_timestamp()+interval '2 minutes')
                WHERE owner_kind='job' AND owner_id=$1 AND wake_id=$4
                  AND closed_at IS NULL AND max_expires_at>clock_timestamp()
                """,
                owner_id, UUID(generation), UUID(vm_uid), operation["wake_id"],
            )
            if not operation["wake_execution_requested"] and not episode_changed and episode is not None:
                from shared.workspace_idle_store import apply_idle_transition_on_conn

                await apply_idle_transition_on_conn(
                    conn,
                    runtime=RuntimeIdentity(
                        "job", str(owner_id), "vm", generation, vm_uid,
                    ),
                    event="rebind",
                    expected_revision=episode.revision,
                    expected_episode_id=episode.episode_id,
                )
            return True

    async def finish_wake(self, operation_id: str) -> bool:
        if _uuid(operation_id) is None:
            return False
        class _WakeChanged(Exception):
            pass

        try:
            async with self.db.acquire() as conn, conn.transaction():
                located = await conn.fetchrow(
                    "SELECT owner_id FROM vm_idle_operations WHERE id=$1",
                    UUID(operation_id),
                )
                if located is None:
                    return False
                owner_id = located["owner_id"]
                await conn.fetchrow(
                    "SELECT unit_id FROM run_queue WHERE unit_id=$1 FOR UPDATE",
                    owner_id,
                )
                job = await conn.fetchrow(
                    "SELECT * FROM jobs WHERE id=$1 FOR UPDATE", owner_id
                )
                operation = await conn.fetchrow(
                    "SELECT * FROM vm_idle_operations WHERE id=$1 FOR UPDATE",
                    UUID(operation_id),
                )
                if job is None or operation is None:
                    return False
                if operation["phase"] == "ready":
                    return True
                if operation["wake_ready_at"] is None or operation["phase"] not in {"waking", "wake_held"}:
                    return False
                vm = _object(_object(job["context"]).get("vm"))
                if (
                    vm.get("status") != "ready"
                    or vm.get("idle_wake_operation_id") != operation_id
                    or _uuid(vm.get("provision_generation")) != operation["wake_generation"]
                    or _uuid(vm.get("rootdisk_pvc_uid")) != operation["pvc_uid"]
                ):
                    return False
                episode = read_episode(
                    _episode_document(job["workspace_idle_episode"]),
                    revision=job["workspace_idle_revision"],
                )
                execute = bool(
                    operation["wake_execution_requested"]
                    or episode is None
                    or episode.episode_id != str(operation["episode_id"])
                )
                if execute:
                    from orchestrator.database.postgres import _stateless_resume_context
                    from shared.worker_queue import (
                        cancel_queued_worker_batch,
                        enqueue_worker_batch_wake,
                        reset_worker_batch_attempts,
                    )
                    from shared.run_queue import unpark_unit

                    admitted = await enqueue_worker_batch_wake(
                        conn, job_id=owner_id,
                        fair_key=str(job["user_id"]) if job["user_id"] else None,
                        priority=int(job["priority"] or 0),
                    )
                    if await reset_worker_batch_attempts(conn, job_id=owner_id) is None:
                        raise _WakeChanged
                    if admitted.state == "parked" and not await unpark_unit(
                        conn, unit_id=owner_id
                    ):
                        raise _WakeChanged
                    updated = await self.db._queue_job_for_resume_on_conn(
                        conn, owner_id, _stateless_resume_context(None),
                        void_completion_decision=True, stateless_only=True,
                        expected_status=job["status"],
                        completion_commands_enabled=True,
                    )
                    if updated is None:
                        raise _WakeChanged
                    if updated.get("operator_pause_held"):
                        await cancel_queued_worker_batch(conn, job_id=owner_id)
                await conn.execute(
                    "UPDATE vm_idle_operations SET phase='ready',closed_at=clock_timestamp(),"
                    "last_progress_at=clock_timestamp() WHERE id=$1",
                    operation["id"],
                )
                return True
        except _WakeChanged:
            return False

    async def renew_access(
        self, job_id: str, *, kind: str, claimant: str
    ) -> bool:
        """Renew one live exact-runtime lease, capped at its original hour."""
        owner_id = _uuid(job_id)
        if owner_id is None or kind not in {"ssh", "sftp", "ide"} or not claimant:
            return False
        async with self.db.acquire() as conn, conn.transaction():
            await conn.fetchrow(
                "SELECT unit_id FROM run_queue WHERE unit_id=$1 FOR UPDATE", owner_id
            )
            job = await conn.fetchrow(
                "SELECT context FROM jobs WHERE id=$1 FOR UPDATE", owner_id
            )
            vm = _object(_object(job["context"]).get("vm")) if job else {}
            if vm.get("status") != "ready":
                return False
            lease = await conn.fetchrow(
                """
                UPDATE vm_idle_access_leases SET
                  expires_at=LEAST(max_expires_at,clock_timestamp()+interval '2 minutes')
                WHERE owner_kind='job' AND owner_id=$1 AND kind=$2 AND claimed_by=$3
                  AND provision_generation=$4 AND vm_uid=$5 AND closed_at IS NULL
                  AND expires_at>clock_timestamp() AND max_expires_at>clock_timestamp()
                RETURNING id
                """,
                owner_id, kind, claimant,
                _uuid(vm.get("provision_generation")), _uuid(vm.get("vm_uid")),
            )
            return lease is not None

    async def close_access(
        self, job_id: str, *, kind: str, claimant: str
    ) -> int:
        owner_id = _uuid(job_id)
        if owner_id is None or kind not in {"ssh", "sftp", "ide"} or not claimant:
            return 0
        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                """
                UPDATE vm_idle_access_leases SET closed_at=clock_timestamp()
                WHERE owner_kind='job' AND owner_id=$1 AND kind=$2 AND claimed_by=$3
                  AND closed_at IS NULL RETURNING id
                """,
                owner_id, kind, claimant,
            )
            return len(rows)

    async def claim(
        self, operation_id: str, *, claimant: str, seconds: int = 30
    ) -> dict[str, Any] | None:
        """Claim an existing effect for one replica; expiry permits crash replay."""
        if _uuid(operation_id) is None or not 1 <= len(claimant) <= 256 or not 5 <= seconds <= 120:
            return None
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE vm_idle_operations SET claim_token=claim_token+1,
                    claimed_by=$2, claim_expires_at=clock_timestamp()+$3::int*interval '1 second'
                WHERE id=$1 AND closed_at IS NULL
                  AND (claim_expires_at IS NULL OR claim_expires_at<=clock_timestamp())
                RETURNING *
                """,
                UUID(operation_id), claimant, seconds,
            )
            return dict(row) if row else None

    async def renew_claim(
        self, operation_id: str, *, token: int, claimant: str, seconds: int = 30
    ) -> bool:
        if _uuid(operation_id) is None or not 5 <= seconds <= 120:
            return False
        async with self.db.acquire() as conn:
            return bool(await conn.fetchval(
                """
                UPDATE vm_idle_operations SET
                    claim_expires_at=clock_timestamp()+$4::int*interval '1 second'
                WHERE id=$1 AND claim_token=$2 AND claimed_by=$3
                  AND claim_expires_at>clock_timestamp() AND closed_at IS NULL
                RETURNING true
                """,
                UUID(operation_id), token, claimant, seconds,
            ))

    async def release_claim(
        self, operation_id: str, *, token: int, claimant: str
    ) -> None:
        if _uuid(operation_id) is None:
            return
        async with self.db.acquire() as conn:
            await conn.execute(
                """
                UPDATE vm_idle_operations SET claimed_by=NULL, claim_expires_at=NULL
                WHERE id=$1 AND claim_token=$2 AND claimed_by=$3
                """,
                UUID(operation_id), token, claimant,
            )

    async def due_job_ids(self, *, limit: int = 16) -> list[str]:
        if not 1 <= limit <= 64:
            raise ValueError("Invalid idle scan limit")
        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT j.id FROM jobs j JOIN run_queue q ON q.unit_id=j.id
                WHERE j.execution_lane='stateless' AND j.assigned_agent_id IS NULL
                  AND j.workspace_idle_episode IS NOT NULL
                  AND j.status IN ('waiting_for_reply','pending_review')
                  AND j.context->'vm'->>'status'='ready'
                  AND q.unit_kind='worker_batch' AND q.state IN ('done','parked')
                  AND NOT EXISTS (
                    SELECT 1 FROM vm_idle_operations o
                    WHERE o.owner_kind='job' AND o.owner_id=j.id AND o.closed_at IS NULL
                  )
                ORDER BY j.id LIMIT $1
                """,
                limit,
            )
            return [str(row["id"]) for row in rows]

    async def pending_operations(self, *, limit: int = 16) -> list[dict[str, Any]]:
        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM vm_idle_operations
                WHERE closed_at IS NULL AND (retry_after IS NULL OR retry_after<=clock_timestamp())
                  AND (claim_expires_at IS NULL OR claim_expires_at<=clock_timestamp())
                  AND (phase<>'suspended' OR wake_requested)
                ORDER BY admitted_at LIMIT $1
                """,
                limit,
            )
            return [dict(row) for row in rows]

    async def get_operation(self, operation_id: str) -> dict[str, Any] | None:
        if _uuid(operation_id) is None:
            return None
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM vm_idle_operations WHERE id=$1", UUID(operation_id)
            )
            return dict(row) if row else None

    async def get_open_for_owner(self, job_id: str) -> dict[str, Any] | None:
        owner_id = _uuid(job_id)
        if owner_id is None:
            return None
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM vm_idle_operations WHERE owner_kind='job' "
                "AND owner_id=$1 AND closed_at IS NULL", owner_id,
            )
            return dict(row) if row else None

    async def hold(
        self, operation_id: str, *, token: int, claimant: str, reason: str,
        seconds: int = 60,
    ) -> bool:
        if _uuid(operation_id) is None or not 5 <= seconds <= 3600:
            return False
        async with self.db.acquire() as conn:
            return bool(await conn.fetchval(
                """
                UPDATE vm_idle_operations SET
                  phase=CASE WHEN phase='releasing' THEN 'release_held'
                             WHEN phase='waking' THEN 'wake_held' ELSE phase END,
                  reason=$4,retry_after=clock_timestamp()+$5::int*interval '1 second',
                  claimed_by=NULL,claim_expires_at=NULL,last_progress_at=clock_timestamp()
                WHERE id=$1 AND claim_token=$2 AND claimed_by=$3 AND closed_at IS NULL
                RETURNING true
                """,
                UUID(operation_id), token, claimant, reason[:200], seconds,
            ))


@asynccontextmanager
async def _renewing(store: VMIdleLifecycleStore, operation: Mapping[str, Any]):
    stop = asyncio.Event()
    operation_id = str(operation["id"])
    token = operation["claim_token"]
    claimant = operation["claimed_by"]

    async def heartbeat() -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=10)
                break
            except asyncio.TimeoutError:
                try:
                    renewed = await store.renew_claim(
                        operation_id, token=token, claimant=claimant, seconds=30
                    )
                except Exception:
                    logger.exception("VM idle claim renewal unavailable for %s", operation_id)
                    renewed = False
                if not renewed:
                    stop.set()
                    break

    task = asyncio.create_task(heartbeat())
    try:
        yield lambda: not stop.is_set()
    finally:
        stop.set()
        await task
        await store.release_claim(operation_id, token=token, claimant=claimant)


class VMIdleLifecycleService:
    """Bounded, replayable Job adapter for the existing VM cleanup authority."""

    def __init__(self, db: Any, provisioner: Any, recovery_store: Any, *,
                 claimant: str = "vm-idle", before_first_start: Any = None) -> None:
        self.store = VMIdleLifecycleStore(db)
        self.db = db
        self.provisioner = provisioner
        self.recovery_store = recovery_store
        self.claimant = claimant
        self.before_first_start = before_first_start

    async def nominate(self, *, limit: int = 16) -> int:
        if (
            os.getenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false").lower() != "true"
            or os.getenv("VM_CREATION_RETRY_ENABLED", "false").lower() != "true"
            or not vm_remote_operation_protocol_enabled()
            or not vm_persistent_rootdisk_enabled()
            or self.provisioner.mode != "same-cluster"
            or not self.provisioner.lifecycle_available
        ):
            return 0
        admitted = 0
        for job_id in await self.store.due_job_ids(limit=limit):
            try:
                job = await self.db.get_job(job_id)
                episode = read_episode(
                    _episode_document(job.get("workspace_idle_episode")),
                    revision=job.get("workspace_idle_revision"),
                )
                if episode is None:
                    continue
                identity = await self.provisioner.capture_vm_teardown_identity(job_id)
                attestation = await self.provisioner.attest_workspace_runtime(job_id)
                exact = {
                    "generation": identity.provision_generation,
                    "vm_uid": identity.vm_uid,
                    "vmi_uid": attestation.vmi_uid,
                    "launcher_uid": attestation.launcher_pod_uid,
                    "pvc_uid": identity.rootdisk_pvc_uid,
                }
                if (
                    identity.provision_generation != attestation.workspace_generation
                    or identity.vm_uid != attestation.vm_uid
                    or identity.rootdisk_pvc_uid != attestation.rootdisk_pvc_uid
                    or not all(exact.values())
                ):
                    continue
                if await self.store.admit_release(
                    job_id, episode_id=episode.episode_id,
                    revision=episode.revision, identity=exact,
                ):
                    admitted += 1
            except Exception:
                logger.exception("VM idle admission held for job %s", job_id)
        return admitted

    async def reconcile_once(self, *, limit: int = 16) -> int:
        """Advance only bounded due operations; every remote effect is outside locks."""
        await self.nominate(limit=limit)
        advanced = 0
        for candidate in await self.store.pending_operations(limit=limit):
            operation = await self.store.claim(
                str(candidate["id"]), claimant=self.claimant, seconds=30
            )
            if operation is None:
                continue
            async with _renewing(self.store, operation) as current:
                try:
                    if operation["phase"] in {"releasing", "release_held"}:
                        if await asyncio.wait_for(
                            self._release(operation, current=current), timeout=300
                        ):
                            advanced += 1
                    elif operation["phase"] in {"suspended", "waking", "wake_held"}:
                        if await asyncio.wait_for(
                            self._wake(operation, current=current), timeout=300
                        ):
                            advanced += 1
                except Exception:
                    logger.exception("VM idle operation %s held", operation["id"])
                    await self.store.hold(
                        str(operation["id"]), token=operation["claim_token"],
                        claimant=self.claimant, reason="effect_unavailable",
                    )
        return advanced

    async def _release(self, operation: Mapping[str, Any], *, current: Any) -> bool:
        owner_id = str(operation["owner_id"])
        identity = await self.provisioner.capture_vm_teardown_identity(owner_id)
        if (
            not isinstance(identity, VMTeardownIdentity)
            or identity.provision_generation != str(operation["provision_generation"])
            or identity.vm_uid != str(operation["vm_uid"])
            or identity.rootdisk_pvc_uid != str(operation["pvc_uid"])
        ):
            await self.store.hold(
                str(operation["id"]), token=operation["claim_token"],
                claimant=self.claimant, reason="capture_identity_changed",
            )
            return False
        permit = await acquire_vm_cleanup_permit(
            self.recovery_store, owner_kind="job", owner_id=owner_id,
            identity=identity, source="vm_idle_release", purge_disk=False,
        )
        if not permit.allowed:
            await self.store.hold(
                str(operation["id"]), token=operation["claim_token"],
                claimant=self.claimant, reason="cleanup_authority_held",
            )
            return False
        disposition = completed_cleanup_outcome(permit)
        if disposition is None:
            result = await self.provisioner.release_vm_captured(
                owner_id, identity, purge_disk=False, capture_snapshot=False,
                entity_type="job", **vm_cleanup_kwargs(permit),
            )
            disposition = result.disposition
            if disposition in {"completed", "identity_superseded"}:
                await complete_vm_cleanup_permit(
                    self.recovery_store, permit, outcome=disposition
                )
        if disposition != "completed" or not current():
            await self.store.hold(
                str(operation["id"]), token=operation["claim_token"],
                claimant=self.claimant, reason=str(disposition),
            )
            return False
        evidence = await self.provisioner.attest_vm_idle_stop(operation)
        if evidence is None or not current():
            await self.store.hold(
                str(operation["id"]), token=operation["claim_token"],
                claimant=self.claimant, reason="physical_stop_unproven",
            )
            return False
        return await self.store.complete_release(
            str(operation["id"]), evidence=evidence
        )

    async def _wake(self, operation: Mapping[str, Any], *, current: Any) -> bool:
        operation_id = str(operation["id"])
        owner_id = str(operation["owner_id"])
        if operation["phase"] == "suspended":
            if not operation["wake_requested"]:
                return False
            operation = await self.store.request_wake(
                owner_id,
                execution_requested=operation["wake_execution_requested"],
            )
            if operation is None:
                return False
        if operation["wake_id"] is None or operation["wake_generation"] is None:
            return False
        if operation["wake_ready_at"] is not None:
            return await self.store.finish_wake(operation_id)

        job = await self.db.get_job(owner_id)
        vm = _object(_object(job.get("context") if job else None).get("vm"))
        generation = str(operation["wake_generation"])
        if vm.get("provision_generation") == generation:
            if vm.get("status") != "ready":
                # The existing durable creation retry and readiness prober own
                # boot/replay; this adapter never issues another start request.
                return False
            attested = await self.provisioner.attest_workspace_runtime(owner_id)
            if (
                attested.workspace_generation != generation
                or attested.vm_uid != vm.get("vm_uid")
                or attested.vmi_uid != vm.get("vmi_uid")
                or attested.launcher_pod_uid != vm.get("active_pod_uid")
                or attested.rootdisk_pvc_uid != str(operation["pvc_uid"])
                or not current()
            ):
                return False
            if not await self.store.mark_wake_ready(
                operation_id, generation=generation,
                vm_uid=attested.vm_uid,
                vmi_uid=attested.vmi_uid,
                launcher_uid=attested.launcher_pod_uid,
                pvc_uid=attested.rootdisk_pvc_uid,
            ):
                return False
            return True

        if (
            vm.get("status") != "suspended"
            or vm.get("provision_generation") != str(operation["provision_generation"])
            or vm.get("rootdisk_pvc_uid") != str(operation["pvc_uid"])
            or operation["stop_verified_at"] is None
        ):
            return False
        config = _object(os.getenv("VM_RESOURCE_ADMISSION_CONFIG", "{}"))
        if self.before_first_start is None:
            if _object(config.get("policy")).get("enforcementEnabled") is True:
                await self.store.hold(
                    operation_id, token=operation["claim_token"],
                    claimant=self.claimant, reason="resource_reservation_unavailable",
                )
                return False
        else:
            admitted = self.before_first_start(operation)
            if inspect.isawaitable(admitted):
                admitted = await admitted
            if not admitted:
                await self.store.hold(
                    operation_id, token=operation["claim_token"],
                    claimant=self.claimant, reason="resource_reservation_held",
                )
                return False
        if not current():
            return False
        result = await self.provisioner.create_vm(
            owner_id, idle_wake_id=operation_id
        )
        return bool(result)
