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

from fastapi import HTTPException

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


async def retained_terminal_rootdisk(db: Any, *, job_id: str,
                                     generation: str | None,
                                     pvc_uid: str | None) -> bool:
    """Read a permanent exact-disk hold, independent of idle feature flags."""
    owner, generation_id, pvc_id = (_uuid(job_id), _uuid(generation), _uuid(pvc_uid))
    if None in (owner, generation_id, pvc_id):
        return False
    async with db.acquire() as conn:
        if not await conn.fetchval("SELECT to_regclass('public.vm_idle_operations') IS NOT NULL"):
            return False
        return bool(await conn.fetchval(
            "SELECT 1 FROM vm_idle_operations WHERE owner_kind='job' AND owner_id=$1 "
            "AND provision_generation=$2 AND pvc_uid=$3 "
            "AND storage_disposition='retention_unknown' LIMIT 1",
            owner, generation_id, pvc_id,
        ))


class VMIdleLifecycleStore:
    def __init__(self, db: Any) -> None:
        self.db = db

    async def schema_available(self) -> bool:
        async with self.db.acquire() as conn:
            return bool(await conn.fetchval(
                "SELECT to_regclass('public.vm_idle_operations') IS NOT NULL"
            ))

    async def approve_terminal_review_on_conn(
        self, conn, *, job_id: str, claim_id: str,
        expected_source: Mapping[str, Any] | None,
        publication: Mapping[str, Any],
        queue_state_before_claim: str | None = None,
        queue_token_before_claim: str | None = None,
    ) -> dict[str, Any] | None:
        """Bind final approval, no-wake and retained storage before Job terminal exit.

        CompletionControl.finish_claim owns the queue -> Job locks and the
        surrounding transaction. This method performs no external I/O.
        """
        if not conn.is_in_transaction() or _uuid(job_id) is None or _uuid(claim_id) is None:
            return None
        owner_id = UUID(job_id)
        job = await conn.fetchrow("SELECT * FROM jobs WHERE id=$1 FOR UPDATE", owner_id)
        if job is None or job["status"] != "pending_review" or job["execution_lane"] != "stateless":
            return None
        context = _object(job["context"])
        from orchestrator.services.completion_control import (
            completion_control_claim_owned_active,
        )
        now = await conn.fetchval("SELECT clock_timestamp()")
        if (
            _object(context.get("_completion_control_claim")).get("source") != "public_approve"
            or not completion_control_claim_owned_active(
                context, claim_id, now_epoch=now.timestamp(),
            )
        ):
            return None
        episode = read_episode(
            _episode_document(job["workspace_idle_episode"]),
            revision=job["workspace_idle_revision"],
        )
        from orchestrator.services.vm_idle_phase_approval import (
            finalized_review_source,
        )
        from orchestrator.services.vm_idle_phase_approval import (
            review_source_snapshot,
        )
        if episode is None or review_source_snapshot(dict(job)) != expected_source:
            return None
        operation = await conn.fetchrow(
            "SELECT * FROM vm_idle_operations WHERE owner_kind='job' AND owner_id=$1 "
            "AND closed_at IS NULL FOR UPDATE", owner_id,
        )
        vm = _object(context.get("vm"))
        if operation is not None:
            if (
                operation["phase"] not in {"releasing", "release_held", "suspended"}
                or operation["episode_id"] != UUID(episode.episode_id)
                or operation["episode_revision"] != episode.revision
                or operation["terminal_source_command_id"] is not None
                or _uuid(vm.get("provision_generation")) != operation["provision_generation"]
                or _uuid(vm.get("vm_uid")) != operation["vm_uid"]
                or _uuid(vm.get("vmi_uid")) != operation["vmi_uid"]
                or _uuid(vm.get("active_pod_uid")) != operation["launcher_uid"]
                or _uuid(vm.get("rootdisk_pvc_uid")) != operation["pvc_uid"]
                or vm.get("status") not in {"suspending", "suspended"}
                or operation["wake_requested"]
                or operation["wake_execution_requested"]
                or any(operation[key] is not None for key in (
                    "wake_id", "wake_generation", "wake_request_id", "wake_ready_at",
                ))
            ):
                return None
            generation, vm_uid, launcher_uid = (
                operation["provision_generation"], operation["vm_uid"],
                operation["launcher_uid"],
            )
        else:
            if vm.get("status") != "ready":
                return None
            try:
                identity = _identity_from_row(
                    dict(job), owner_kind="job", owner_id=job_id,
                    operation_kind="idle_policy",
                )
            except VMRemoteOperationUnavailable:
                return None
            generation = _uuid(identity.workspace_generation)
            vm_uid = _uuid(identity.vm_uid)
            launcher_uid = _uuid(identity.launcher_pod_uid)
            if (
                None in (generation, vm_uid, launcher_uid)
                or _uuid(vm.get("vmi_uid")) is None
                or _uuid(vm.get("rootdisk_pvc_uid")) is None
            ):
                return None
        source = await finalized_review_source(
            conn, job=job, episode=episode, generation=generation,
            vm_uid=vm_uid, launcher_uid=launcher_uid,
            pvc_uid=_uuid(vm.get("rootdisk_pvc_uid")),
        )
        if source is None or expected_source != {
            "command_id": source["command_id"],
            "episode_id": episode.episode_id,
            "episode_revision": episode.revision,
            "freeze_type": "job_complete",
            "runtime_generation": str(generation),
            "runtime_uid": str(vm_uid),
        }:
            return None
        # A previously admitted destructive S36/kept-disk cleanup or captured
        # S36 intent cannot be relabelled as retained. The conflict is visible
        # as a 409 and leaves the approval source intact for operator review.
        if await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_workspace_cleanup_admissions "
            "WHERE owner_kind='job' AND owner_id=$1 "
            "AND (pvc_uid=$2 OR pvc_uid IS NULL) "
            "AND source IN ('completion_workspace_teardown','kept_disk')) OR "
            "EXISTS(SELECT 1 FROM completion_effects WHERE scope_id=$1 "
            "AND effect_name='workspace_archive_teardown' AND intent_at IS NOT NULL)",
            owner_id, _uuid(vm.get("rootdisk_pvc_uid")),
        ):
            return None
        if operation is None:
            # This is a terminal, no-successor release. Do not ask for a new
            # VM budget or reusable network profile. Ordinary waiting release
            # keeps those stricter prerequisites in admit_release.
            if not vm_remote_operation_protocol_enabled() or not vm_persistent_rootdisk_enabled():
                return None
            try:
                previous_token = int(queue_token_before_claim)
            except (TypeError, ValueError):
                return None
            queue = await conn.fetchrow(
                "SELECT state,lease_token,leased_by FROM run_queue "
                "WHERE unit_id=$1 AND unit_kind='worker_batch' FOR UPDATE",
                owner_id,
            )
            ide = _object(context.get("ide_session"))
            if (
                queue_state_before_claim not in {"done", "parked"}
                or queue is None
                or queue["state"] != "done"
                or queue["lease_token"] != previous_token + 1
                or queue["leased_by"] is not None
                or job["assigned_agent_id"] is not None
                or ide.get("status") in {"active", "idle", "restoring"}
            ):
                return None
            if await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM vm_idle_access_leases WHERE owner_kind='job' "
                "AND owner_id=$1 AND closed_at IS NULL AND expires_at>clock_timestamp()) "
                "OR EXISTS(SELECT 1 FROM vm_remote_operation_leases WHERE owner_kind='job' "
                "AND owner_id=$1 AND settled_at IS NULL) "
                "OR EXISTS(SELECT 1 FROM vm_workspace_recoveries WHERE owner_kind='job' "
                "AND owner_id=$1 AND resolved_at IS NULL) "
                "OR EXISTS(SELECT 1 FROM vm_workspace_recovery_jobs WHERE job_id=$1 "
                "AND resolved_at IS NULL) "
                "OR EXISTS(SELECT 1 FROM vm_workspace_cleanup_admissions WHERE owner_kind='job' "
                "AND owner_id=$1 AND completed_at IS NULL) "
                "OR EXISTS(SELECT 1 FROM jobs WHERE parent_job_id=$1 "
                "AND status NOT IN ('completed','failed','cancelled') "
                "AND context->>'inherits_parent_workspace'='true') "
                "OR EXISTS(SELECT 1 FROM job_completion_commands WHERE job_id=$1 "
                "AND state IN ('pending','finalizing','parked')) "
                "OR EXISTS(SELECT 1 FROM job_completion_sweep_exclusions WHERE job_id=$1)",
                owner_id,
            ):
                return None
            operation = await conn.fetchrow(
                "INSERT INTO vm_idle_operations "
                "(owner_kind,owner_id,phase,episode_id,episode_revision,"
                "provision_generation,vm_uid,vmi_uid,launcher_uid,pvc_uid,retained_kind) "
                "VALUES('job',$1,'releasing',$2,$3,$4,$5,$6,$7,$8,'rootdisk') RETURNING *",
                owner_id, UUID(episode.episode_id), episode.revision,
                generation, vm_uid, _uuid(vm["vmi_uid"]), launcher_uid,
                _uuid(vm["rootdisk_pvc_uid"]),
            )
            vm.update(status="suspending", _suspend_remote_io_closed=str(operation["id"]))
            context["vm"] = vm
            await conn.execute(
                "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                owner_id, json.dumps(context),
            )
        operation = await conn.fetchrow(
            "UPDATE vm_idle_operations SET terminal_source_command_id=$2,"
            "terminal_decided_at=clock_timestamp(),storage_disposition='retention_unknown',"
            "terminal_publication=$3::jsonb,reason=NULL,retry_after=NULL "
            "WHERE id=$1 RETURNING *",
            operation["id"], UUID(source["command_id"]),
            json.dumps(dict(publication)),
        )
        return dict(operation)

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
            execution = await conn.fetchrow(
                """
                SELECT id,revision,generation,
                  CASE WHEN resolved->'spec'->>'timeoutSeconds' IS NULL THEN NULL
                  ELSE created_at +
                       ((resolved->'spec'->>'timeoutSeconds')::double precision
                        * interval '1 second') END AS deadline
                FROM srw_execution_specs
                WHERE work_kind='Job' AND work_id=$1 AND harness_adapter='srw/v1'
                FOR SHARE
                """,
                owner_id,
            )
            if execution is None:
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
                or episode.wait_kind not in {"human_message", "human_approval", "human_review"}
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
            phase_source = None
            review_source = None
            if episode.wait_kind == "human_approval":
                from orchestrator.services.vm_idle_phase_approval import (
                    finalized_phase_source,
                )

                phase_source = await finalized_phase_source(
                    conn, job=row, episode=episode,
                    generation=expected["generation"], vm_uid=expected["vm_uid"],
                    launcher_uid=expected["launcher_uid"],
                )
                if phase_source is None:
                    return None
            if episode.wait_kind == "human_review":
                from orchestrator.services.vm_idle_phase_approval import (
                    finalized_review_source,
                )

                review_source = await finalized_review_source(
                    conn, job=row, episode=episode,
                    generation=expected["generation"], vm_uid=expected["vm_uid"],
                    launcher_uid=expected["launcher_uid"],
                    pvc_uid=expected["pvc_uid"],
                )
                if review_source is None:
                    return None
            from orchestrator.services.vm_creation_preflight import (
                _execution_binding,
                idle_wake_predecessor,
            )
            from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict

            try:
                max_attempts = int(os.getenv("VM_PROVISION_MAX_ATTEMPTS", "3"))
                raw_request = _object(_object(vm.get("creation_preflight")).get("request"))
                if raw_request.get("network_profile") is None:
                    raise VMCreationRetryConflict("retained_network_profile_unproven")
                if raw_request.get("network_profile") is not None:
                    from shared.vm_creation_retry import canonical_request_digest

                    ledger = await conn.fetchrow(
                        "SELECT canonical_request,request_digest,state,provision_generation,observed_pvc_uid,observed_vm_uid "
                        "FROM vm_creation_retries WHERE job_id=$1 AND provision_generation=$2 FOR SHARE",
                        owner_id, expected["generation"],
                    )
                    if (
                        not ledger
                        or ledger["state"] != "succeeded"
                        or _object(ledger["canonical_request"]) != raw_request
                        or canonical_request_digest(raw_request) != ledger["request_digest"]
                        or ledger["observed_pvc_uid"] != expected["pvc_uid"]
                        or ledger["observed_vm_uid"] != expected["vm_uid"]
                    ):
                        raise VMCreationRetryConflict("retained_network_profile_unproven")
                    chain = await conn.fetch(
                        "SELECT canonical_request,request_digest,expected_pvc_uid,observed_pvc_uid,state "
                        "FROM vm_creation_retries WHERE job_id=$1 AND "
                        "(observed_pvc_uid=$2 OR expected_pvc_uid=$2) "
                        "ORDER BY created_at,request_id FOR SHARE",
                        owner_id, expected["pvc_uid"],
                    )
                    if (
                        not chain
                        or any(
                            _object(item["canonical_request"]).get("network_profile")
                            != raw_request["network_profile"]
                            or canonical_request_digest(_object(item["canonical_request"]))
                            != item["request_digest"]
                            for item in chain
                        )
                    ):
                        raise VMCreationRetryConflict("retained_network_profile_unproven")
                    storage = raw_request.get("workspace_storage")
                    original_owner = (
                        UUID(storage["owner_id"]) if storage is not None else owner_id
                    )
                    originals = await conn.fetch(
                        "SELECT canonical_request,request_digest,state,provision_generation,observed_pvc_uid,observed_vm_uid "
                        "FROM vm_creation_retries WHERE job_id=$1 AND expected_pvc_uid IS NULL "
                        "AND observed_pvc_uid=$2 ORDER BY created_at,request_id FOR SHARE",
                        original_owner, expected["pvc_uid"],
                    )
                    if (
                        len(originals) != 1
                        or originals[0]["state"] != "succeeded"
                        or _object(originals[0]["canonical_request"]).get("network_profile")
                        != raw_request["network_profile"]
                        or canonical_request_digest(_object(originals[0]["canonical_request"]))
                        != originals[0]["request_digest"]
                    ):
                        raise VMCreationRetryConflict("retained_network_profile_unproven")
                    if storage is not None:
                        from orchestrator.services.retained_vm_workspaces import provision_authority

                        authority = await provision_authority(conn, job_id)
                        if (
                            not authority
                            or any(
                                authority["storage"].get(key) != storage.get(key)
                                for key in ("uid", "generation", "owner_id", "owner_kind")
                            )
                            or storage.get("pvc_uid") not in (
                                None, authority["storage"].get("pvc_uid")
                            )
                            or authority["storage"].get("pvc_uid") != str(expected["pvc_uid"])
                            or authority.get("network_profile") != raw_request["network_profile"]
                        ):
                            raise VMCreationRetryConflict("retained_network_profile_unproven")
                        original_job = await conn.fetchrow(
                            "SELECT context FROM jobs WHERE id=$1", original_owner
                        )
                        original_context = _object(original_job["context"]) if original_job else {}
                        original_evidence = [
                            _object(_object(original_context.get(name)).get("network_profile_evidence"))
                            for name in ("vm", "last_vm")
                        ]
                        from shared.vm_network_profile import reusable_profile_evidence

                        if not any(
                            reusable_profile_evidence(
                                proof, raw_request["network_profile"],
                                provision_generation=str(originals[0]["provision_generation"]),
                                vm_uid=str(originals[0]["observed_vm_uid"]),
                                pvc_uid=str(expected["pvc_uid"]),
                            ) for proof in original_evidence
                        ):
                            raise VMCreationRetryConflict("retained_network_profile_unproven")
                current_storage = None
                if raw_request.get("workspace_storage") is not None:
                    from orchestrator.services.retained_vm_workspaces import provision_binding

                    current_storage = await provision_binding(conn, job_id)
                prior = idle_wake_predecessor(
                    vm, job_id=job_id, pvc_uid=str(expected["pvc_uid"]),
                    max_attempts=max_attempts, current_storage=current_storage,
                )
                binding = _execution_binding(prior)
            except (
                ValueError, TypeError, KeyError, HTTPException,
                VMCreationRetryConflict,
            ) as exc:
                logger.info("VM idle release held for job %s: %s", job_id, exc)
                return None
            if (
                execution["id"] != binding["execution_id"]
                or execution["revision"] != binding["execution_revision"]
                or execution["generation"] != binding["execution_generation"]
                or execution["deadline"] != binding["admission_deadline"]
            ):
                logger.info("VM idle release held for job %s: execution_manifest_changed", job_id)
                return None
            human_wait_current = bool(
                (
                    row["status"] == "waiting_for_reply"
                    and freeze.get("route_id") == episode.wait_key
                )
                or phase_source is not None
                or review_source is not None
            )
            now = await conn.fetchval("SELECT clock_timestamp()")
            from orchestrator.services.completion_control import (
                completion_control_claim_active,
            )

            if completion_control_claim_active(context, now_epoch=now.timestamp()):
                return None
            if execution["deadline"] is not None and execution["deadline"] <= now:
                logger.info("VM idle release held for job %s: job_admission_expired", job_id)
                return None
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
                operation["terminal_source_command_id"] is None
                and (
                    current_episode is None
                    or current_episode.episode_id != str(operation["episode_id"])
                    or current_episode.revision != operation["episode_revision"]
                )
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

    @staticmethod
    async def _reserve_wake_on_conn(conn, operation, *, execution_requested: bool):
        return await conn.fetchrow(
            """
            UPDATE vm_idle_operations SET
              phase=CASE WHEN phase='suspended' THEN 'waking' ELSE phase END,
              wake_requested=true,
              wake_execution_requested=wake_execution_requested OR $2,
              wake_id=COALESCE(wake_id,$3),
              wake_generation=COALESCE(wake_generation,$4),
              wake_request_id=COALESCE(wake_request_id,$5),
              retry_after=NULL,last_progress_at=clock_timestamp()
            WHERE id=$1 AND closed_at IS NULL RETURNING *
            """,
            operation["id"], execution_requested, uuid4(), uuid4(), uuid4(),
        )

    async def approve_phase_wake_on_conn(
        self, conn, *, job_id: str, claim_id: str,
        expected_source: Mapping[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Commit an exact phase approval under CompletionControl.finish_claim."""
        if not conn.is_in_transaction() or _uuid(job_id) is None or _uuid(claim_id) is None:
            return None
        owner_id = UUID(job_id)
        job = await conn.fetchrow("SELECT * FROM jobs WHERE id=$1 FOR UPDATE", owner_id)
        if job is None or job["status"] != "pending_review" or job["execution_lane"] != "stateless":
            return None
        context = _object(job["context"])
        from orchestrator.services.completion_control import (
            completion_control_claim_owned_active,
        )

        now = await conn.fetchval("SELECT clock_timestamp()")
        marker = _object(context.get("_completion_control_claim"))
        if (
            marker.get("source") != "public_approve"
            or not completion_control_claim_owned_active(
                context, claim_id, now_epoch=now.timestamp(),
            )
        ):
            return None
        operation = await conn.fetchrow(
            "SELECT * FROM vm_idle_operations WHERE owner_kind='job' AND owner_id=$1 "
            "AND closed_at IS NULL FOR UPDATE",
            owner_id,
        )
        episode = read_episode(
            _episode_document(job["workspace_idle_episode"]),
            revision=job["workspace_idle_revision"],
        )
        if (
            operation is None
            or operation["phase"] not in {
                "releasing", "release_held", "suspended", "waking", "wake_held",
            }
            or episode is None
            or episode.episode_id != str(operation["episode_id"])
            or episode.revision != operation["episode_revision"]
            or episode.wait_kind != "human_approval"
        ):
            return None
        from orchestrator.services.vm_idle_phase_approval import (
            finalized_phase_source,
        )

        source = await finalized_phase_source(
            conn, job=job, episode=episode,
            generation=operation["provision_generation"],
            vm_uid=operation["vm_uid"], launcher_uid=operation["launcher_uid"],
        )
        if source is None:
            return None
        if expected_source != {
            "command_id": source["command_id"],
            "episode_id": episode.episode_id,
            "episode_revision": episode.revision,
            "freeze_type": "phase_boundary",
            "phase_type": source["semantics"]["freeze"]["phase_type"],
            "phase_number": source["semantics"]["freeze"]["phase_number"],
            "runtime_generation": str(operation["provision_generation"]),
            "runtime_uid": str(operation["vm_uid"]),
        }:
            return None
        vm = _object(context.get("vm"))
        predecessor = _object(context.get("last_vm"))
        original = (
            vm.get("status") in {"suspending", "suspended"}
            and _uuid(vm.get("provision_generation")) == operation["provision_generation"]
            and _uuid(vm.get("vm_uid")) == operation["vm_uid"]
            and _uuid(vm.get("rootdisk_pvc_uid")) == operation["pvc_uid"]
        )
        successor = (
            operation["wake_generation"] is not None
            and _uuid(vm.get("provision_generation")) == operation["wake_generation"]
            and vm.get("idle_wake_operation_id") == str(operation["id"])
            and _uuid(predecessor.get("vm_uid")) == operation["vm_uid"]
            and _uuid(predecessor.get("rootdisk_pvc_uid")) == operation["pvc_uid"]
        )
        if not (original or successor):
            return None
        wake = await self._reserve_wake_on_conn(
            conn, operation, execution_requested=True,
        )
        if wake is None:
            return None
        intent = {
            "version": 1,
            "operation_id": str(operation["id"]),
            "command_id": source["command_id"],
            "episode_id": episode.episode_id,
            "episode_revision": episode.revision,
            "phase_type": source["semantics"]["freeze"]["phase_type"],
            "phase_number": source["semantics"]["freeze"]["phase_number"],
            "generation": str(operation["provision_generation"]),
        }
        await conn.execute(
            "UPDATE jobs SET context=jsonb_set(context,"
            "'{_vm_idle_phase_approval}',$2::jsonb,true) WHERE id=$1",
            owner_id, json.dumps(intent),
        )
        return dict(wake)

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
            } or operation["terminal_source_command_id"] is not None:
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
            row = await self._reserve_wake_on_conn(
                conn, operation, execution_requested=execution_requested,
            )
            wake_id = row["wake_id"]
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
            if (job is None or job["status"] in {"completed", "failed", "cancelled"}
                or operation is None or operation["terminal_source_command_id"] is not None
                or operation["phase"] not in {"waking", "wake_held"}):
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
                if (job["status"] in {"completed", "failed", "cancelled"}
                    or operation["terminal_source_command_id"] is not None):
                    return False
                if operation["phase"] == "ready":
                    return True
                if operation["wake_ready_at"] is None or operation["phase"] not in {"waking", "wake_held"}:
                    return False
                context = _object(job["context"])
                vm = _object(context.get("vm"))
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
                approval_intent = None
                if "_vm_idle_phase_approval" in context:
                    approval_intent = _object(context["_vm_idle_phase_approval"])
                    from orchestrator.services.vm_idle_phase_approval import (
                        finalized_phase_source,
                    )

                    source = await finalized_phase_source(
                        conn, job=job, episode=episode,
                        generation=operation["provision_generation"],
                        vm_uid=operation["vm_uid"],
                        launcher_uid=operation["launcher_uid"],
                    )
                    if (
                        source is None
                        or not operation["wake_execution_requested"]
                        or approval_intent.get("version") != 1
                        or approval_intent.get("operation_id") != operation_id
                        or approval_intent.get("command_id") != source["command_id"]
                        or approval_intent.get("episode_id") != episode.episode_id
                        or approval_intent.get("episode_revision") != episode.revision
                        or episode.revision != operation["episode_revision"]
                        or approval_intent.get("phase_type")
                           != source["semantics"]["freeze"]["phase_type"]
                        or approval_intent.get("phase_number")
                           != source["semantics"]["freeze"]["phase_number"]
                        or approval_intent.get("generation")
                           != str(operation["provision_generation"])
                    ):
                        await conn.execute(
                            "UPDATE vm_idle_operations SET phase='wake_held',"
                            "reason='phase_approval_source_changed',"
                            "retry_after=clock_timestamp()+interval '1 minute' "
                            "WHERE id=$1",
                            operation["id"],
                        )
                        return False
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
                    if approval_intent is not None:
                        await conn.execute(
                            "UPDATE jobs SET context="
                            "(context-'_vm_idle_phase_approval') || "
                            "jsonb_build_object('_vm_idle_last_phase_approval',"
                            "$2::jsonb || jsonb_build_object("
                            "'approved_at',to_jsonb(clock_timestamp()))) "
                            "WHERE id=$1",
                            owner_id, json.dumps(approval_intent),
                        )
                # The operation ID authorizes this successor only while the
                # wake is open. Remove its marker in the same close transaction
                # so a later idle episode can reserve a different successor.
                await conn.execute(
                    "UPDATE jobs SET context=context #- '{vm,idle_wake_operation_id}' "
                    "WHERE id=$1 AND context->'vm'->>'idle_wake_operation_id'=$2",
                    owner_id, operation_id,
                )
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

    async def due_job_ids(
        self, *, limit: int = 16, after_id: UUID | None = None,
    ) -> list[str]:
        if not 1 <= limit <= 64:
            raise ValueError("Invalid idle scan limit")
        async with self.db.acquire() as conn:
            query = (
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
                  AND ($2::uuid IS NULL OR j.id>$2)
                ORDER BY j.id LIMIT $1
                """
            )
            rows = await conn.fetch(query, limit, after_id)
            if not rows and after_id is not None:
                rows = await conn.fetch(query, limit, None)
            return [str(row["id"]) for row in rows]

    async def pending_operations(
        self, *, limit: int = 16, after_id: UUID | None = None,
    ) -> list[dict[str, Any]]:
        if not 1 <= limit <= 64:
            raise ValueError("Invalid idle scan limit")
        async with self.db.acquire() as conn:
            query = (
                """
                SELECT * FROM vm_idle_operations
                WHERE closed_at IS NULL AND (retry_after IS NULL OR retry_after<=clock_timestamp())
                  AND (claim_expires_at IS NULL OR claim_expires_at<=clock_timestamp())
                  AND (phase<>'suspended' OR wake_requested)
                  AND ($2::uuid IS NULL OR id>$2)
                ORDER BY id LIMIT $1
                """
            )
            rows = await conn.fetch(query, limit, after_id)
            if not rows and after_id is not None:
                rows = await conn.fetch(query, limit, None)
            return [dict(row) for row in rows]

    async def pending_terminal_publications(self, *, limit: int = 16) -> list[str]:
        async with self.db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id FROM vm_idle_operations WHERE terminal_source_command_id IS NOT NULL "
                "AND terminal_published_at IS NULL "
                "AND (terminal_publication_retry_after IS NULL OR "
                "terminal_publication_retry_after<=clock_timestamp()) "
                "AND (claim_expires_at IS NULL OR claim_expires_at<=clock_timestamp()) "
                "ORDER BY COALESCE(terminal_publication_retry_after,terminal_decided_at),id "
                "LIMIT $1", limit,
            )
            return [str(row["id"]) for row in rows]

    async def claim_terminal_publication(
        self, operation_id: str, *, claimant: str,
    ) -> dict[str, Any] | None:
        """Claim publication even after compute operation closure or flag-off."""
        if _uuid(operation_id) is None or not 1 <= len(claimant) <= 256:
            return None
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE vm_idle_operations SET claim_token=claim_token+1,"
                "claimed_by=$2,claim_expires_at=clock_timestamp()+interval '30 seconds' "
                "WHERE id=$1 AND terminal_source_command_id IS NOT NULL "
                "AND terminal_published_at IS NULL "
                "AND (terminal_publication_retry_after IS NULL OR "
                "terminal_publication_retry_after<=clock_timestamp()) "
                "AND (claim_expires_at IS NULL OR claim_expires_at<=clock_timestamp()) "
                "RETURNING *", UUID(operation_id), claimant,
            )
            return dict(row) if row else None

    async def defer_terminal_publication(
        self, operation_id: str, *, token: int, claimant: str,
    ) -> None:
        async with self.db.acquire() as conn:
            await conn.execute(
                "UPDATE vm_idle_operations SET "
                "terminal_publication_retry_after=clock_timestamp()+interval '1 minute' "
                "WHERE id=$1 AND claim_token=$2 AND claimed_by=$3 "
                "AND terminal_published_at IS NULL",
                UUID(operation_id), token, claimant,
            )

    async def mark_terminal_published(self, operation_id: str, *, token: int,
                                      claimant: str) -> bool:
        async with self.db.acquire() as conn:
            return bool(await conn.fetchval(
                "UPDATE vm_idle_operations SET terminal_published_at=clock_timestamp(),"
                "terminal_publication_retry_after=NULL,claimed_by=NULL,claim_expires_at=NULL "
                "WHERE id=$1 AND terminal_source_command_id IS NOT NULL "
                "AND terminal_published_at IS NULL AND claim_token=$2 "
                "AND claimed_by=$3 AND claim_expires_at>clock_timestamp() RETURNING true",
                UUID(operation_id), token, claimant,
            ))

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
                 claimant: str = "vm-idle", before_first_start: Any = None,
                 terminal_publication_handler: Any = None) -> None:
        self.store = VMIdleLifecycleStore(db)
        self.db = db
        self.provisioner = provisioner
        self.recovery_store = recovery_store
        self.claimant = claimant
        self.before_first_start = before_first_start
        self.terminal_publication_handler = terminal_publication_handler
        # Selection is process-local, while operation claims and admission are
        # database-authoritative. The long-lived sweeper advances these cursors
        # even when a whole page is ineligible, then wraps at the end.
        self._nomination_cursor: UUID | None = None
        self._operation_cursor: UUID | None = None

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
        due = await self.store.due_job_ids(
            limit=limit, after_id=self._nomination_cursor,
        )
        self._nomination_cursor = UUID(due[-1]) if due else None
        for job_id in due:
            try:
                # get_job's public projection does not carry the native idle
                # revision/episode columns. Read the source row for nomination;
                # admit_release still performs the authoritative locked check.
                job = await self.db.fetchrow(
                    "SELECT workspace_idle_episode,workspace_idle_revision "
                    "FROM jobs WHERE id=$1", UUID(job_id)
                )
                if job is None:
                    continue
                episode = read_episode(
                    _episode_document(job["workspace_idle_episode"]),
                    revision=job["workspace_idle_revision"],
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
        pending = await self.store.pending_operations(
            limit=limit, after_id=self._operation_cursor,
        )
        self._operation_cursor = pending[-1]["id"] if pending else None
        for candidate in pending:
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
        # Publication is replayable and less urgent than stopping compute.
        # A failing forge gets a short bounded attempt after physical work;
        # release the claim so later rows cannot be starved by one outage.
        if self.terminal_publication_handler is not None:
            for operation_id in await self.store.pending_terminal_publications(
                limit=min(limit, 4),
            ):
                operation = await self.store.claim_terminal_publication(
                    operation_id, claimant=self.claimant,
                )
                if operation is None:
                    continue
                try:
                    await asyncio.wait_for(
                        self.terminal_publication_handler(operation), timeout=10,
                    )
                    if await self.store.mark_terminal_published(
                        operation_id, token=operation["claim_token"],
                        claimant=self.claimant,
                    ):
                        advanced += 1
                except Exception:
                    logger.exception("Terminal review publication held for %s", operation_id)
                    await self.store.defer_terminal_publication(
                        operation_id, token=operation["claim_token"],
                        claimant=self.claimant,
                    )
                finally:
                    await self.store.release_claim(
                        operation_id, token=operation["claim_token"],
                        claimant=self.claimant,
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
        if operation["terminal_source_command_id"] is not None:
            return False
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
