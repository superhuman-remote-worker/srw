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
import httpx
from typing import Any, Mapping
from uuid import UUID, uuid4

from fastapi import HTTPException
from shared.pinned_job_delivery import stamp_pinned_resume_input_ids
from shared.pinned_session_identity import PinnedJobRecipient
from orchestrator.services.pinned_k8s_effect import pod_containers_are_terminal

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
from shared.workspace_idle_policy import IdlePolicyError, RuntimeIdentity, evaluate_idle, read_episode
from shared.vm_resource_admission import ResourceAdmissionError

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


def _release_kind(operation: Mapping[str, Any]) -> str:
    # During the 0275 rollout an older 0270-0273 schema has only stateless
    # operations. Never index the new field before the migration is present.
    return operation["release_kind"] if "release_kind" in operation else "stateless"


async def _idle_resource_binding_on_conn(conn, db, *, retry, operation,
                                         released_replay: bool = False):
    """Resolve an owed v3 charge from its frozen retry, never from VM context."""
    from orchestrator.services.vm_resource_job_runtime import (
        installed_job_resource_store,
    )
    configuration = _object(retry["controller_configuration"])
    charge = await conn.fetchrow(
        "SELECT state,release_evidence FROM vm_resource_reservations "
        "WHERE request_id=$1", retry["request_id"],
    )
    if charge is None:
        if configuration.get("version") == 3:
            raise ResourceAdmissionError("resource_idle_charge_unproven")
        return None
    if configuration.get("version") != 3:
        raise ResourceAdmissionError("resource_idle_policy_unproven")
    if charge["state"] == "released":
        proof = _object(charge["release_evidence"])
        stop_digest = await conn.fetchval(
            "SELECT 'sha256:'||encode(sha256(convert_to(stop_evidence::text,'UTF8')),'hex') "
            "FROM vm_idle_operations WHERE id=$1", operation["id"],
        )
        if (
            not released_replay
            or proof.get("kind") != "exact_compute_absent"
            or proof.get("operation_id") != str(operation["id"])
            or proof.get(
                "thread_id" if operation["owner_kind"] == "thread" else "job_id"
            ) != str(operation["owner_id"])
            or proof.get("provision_generation")
                != str(operation["provision_generation"])
            or proof.get("vm_uid") != str(operation["vm_uid"])
            or proof.get("vmi_uid") != str(operation["vmi_uid"])
            or proof.get("launcher_uid") != str(operation["launcher_uid"])
            or proof.get("pvc_uid") != str(operation["pvc_uid"])
            or proof.get("stop_evidence_digest") != stop_digest
        ):
            raise ResourceAdmissionError("resource_idle_charge_changed")
        return None
    resource = await installed_job_resource_store(
        conn, db, retry["controller_configuration"], fresh=False,
    )
    if resource is None:
        raise ResourceAdmissionError("resource_idle_policy_unproven")
    return resource


async def _pinned_stop_valid_on_conn(conn: Any, operation: Mapping[str, Any]) -> bool:
    """Check one direct stop or one immutable prior access-only stop link."""
    if _release_kind(operation) != "pinned_job":
        return False
    stop = _object(operation["pinned_stop_evidence"])
    if (
        operation["pinned_terminal_observed_at"] is None
        or operation["pinned_stop_verified_at"] is None
        or stop.get("version") != 1
        or stop.get("kind") not in {
            "pinned_job_agent_stop", "pinned_job_agent_stop_reuse",
        }
        or stop.get("operation_id") != str(operation["id"])
        or stop.get("job_id") != str(operation["owner_id"])
        or stop.get("agent_id") != str(operation["pinned_agent_id"])
        or stop.get("process_generation") != operation["pinned_process_generation"]
        or stop.get("pod_name") != operation["pinned_agent_pod_name"]
        or stop.get("pod_namespace") != operation["pinned_agent_pod_namespace"]
        or stop.get("pod_uid") != operation["pinned_agent_pod_uid"]
        or stop.get("delivery_id") != str(operation["pinned_delivery_id"])
        or stop.get("terminal_containers_observed") is not True
        or stop.get("final_absence") not in {"exact_absent", "replacement"}
    ):
        return False
    if stop["kind"] == "pinned_job_agent_stop":
        return stop.get("prior_operation_id") is None
    prior_id = _uuid(stop.get("prior_operation_id"))
    if prior_id is None:
        return False
    prior = await conn.fetchrow(
        "SELECT * FROM vm_idle_operations WHERE id=$1 AND owner_kind='job' "
        "AND owner_id=$2 AND episode_id=$3",
        prior_id, operation["owner_id"], operation["episode_id"],
    )
    if prior is None:
        return False
    prior_stop = _object(prior["pinned_stop_evidence"])
    successor = _object(_object(prior["access_rebind_proof"]).get("successor"))
    return bool(
        prior["release_kind"] == "pinned_job"
        and prior["phase"] == "ready" and prior["closed_at"] is not None
        and prior["episode_revision"] + 1 == operation["episode_revision"]
        and prior["pinned_delivery_id"] == operation["pinned_delivery_id"]
        and prior["pinned_wait_receipt_id"] == operation["pinned_wait_receipt_id"]
        and prior["pinned_agent_id"] == operation["pinned_agent_id"]
        and prior["pinned_agent_pod_uid"] == operation["pinned_agent_pod_uid"]
        and prior["pinned_terminal_observed_at"] == operation["pinned_terminal_observed_at"]
        and prior["pinned_stop_verified_at"] == operation["pinned_stop_verified_at"]
        and prior_stop.get("operation_id") == str(prior["id"])
        and prior_stop.get("pod_uid") == stop["pod_uid"]
        and prior_stop.get("final_absence") == stop["final_absence"]
        and successor == {
            "generation": str(operation["provision_generation"]),
            "vm_uid": str(operation["vm_uid"]),
            "vmi_uid": str(operation["vmi_uid"]),
            "launcher_uid": str(operation["launcher_uid"]),
            "pvc_uid": str(operation["pvc_uid"]),
        }
    )


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


class _ThreadIdleAdmissionLost(Exception):
    """Roll back a staged retirement token when its atomic idle source loses."""


class VMIdleLifecycleStore:
    def __init__(self, db: Any) -> None:
        self.db = db

    async def schema_available(self) -> bool:
        async with self.db.acquire() as conn:
            return bool(await conn.fetchval(
                "SELECT to_regclass('public.vm_idle_operations') IS NOT NULL"
            ))

    async def admit_thread_release(
        self, thread_id: str, *, episode_id: str, revision: int,
        identity: Mapping[str, Any], turn_quiescent: bool,
    ) -> dict[str, Any] | None:
        """Bind one proved natural pause to the existing pinned retirement token.

        The caller probes the exact agent turn outside SQL locks. This method
        rechecks durable admission and makes the owner/token/operation one
        transaction; the later retirement funnel still rechecks quiescence.
        """
        from orchestrator.services.vm_remote_operation import (
            VMRemoteOperationUnavailable, _identity_from_row,
        )
        from orchestrator.services.vm_resource_job_runtime import (
            configured_enforcement_policy,
        )
        from shared.workspace_contract import vm_mode_from_env

        owner_id, source_id = _uuid(thread_id), _uuid(episode_id)
        expected = {
            key: _uuid(identity.get(key)) if isinstance(identity, Mapping) else None
            for key in ("generation", "vm_uid", "vmi_uid", "launcher_uid", "pvc_uid")
        }
        if (
            owner_id is None or source_id is None or type(revision) is not int
            or revision < 1 or None in expected.values()
        ):
            return None
        try:
            async with self.db.acquire() as conn, conn.transaction():
                thread = await conn.fetchrow(
                    "SELECT * FROM threads WHERE id=$1 FOR UPDATE", owner_id,
                )
                if thread is None or thread["execution_lane"] != "pinned":
                    return None
                open_operation = await conn.fetchrow(
                    "SELECT * FROM vm_idle_operations WHERE owner_kind='thread' "
                    "AND owner_id=$1 AND closed_at IS NULL FOR UPDATE", owner_id,
                )
                if open_operation is not None:
                    return dict(open_operation) if (
                        open_operation["release_kind"] == "pinned_thread"
                        and open_operation["episode_id"] == source_id
                        and open_operation["episode_revision"] == revision
                        and all(
                            open_operation[column] == expected[key]
                            for key, column in (
                                ("generation", "provision_generation"),
                                ("vm_uid", "vm_uid"), ("vmi_uid", "vmi_uid"),
                                ("launcher_uid", "launcher_uid"),
                                ("pvc_uid", "pvc_uid"),
                            )
                        )
                    ) else None
                if (
                    not turn_quiescent
                    or thread["status"] != "awaiting_user"
                    or thread["agent_id"] is None
                    or thread["runtime_attach_token"] is None
                    or thread["runtime_retirement_token"] is not None
                    or thread["workspace_idle_revision"] != revision
                    or os.getenv("WORKSPACE_IDLE_RELEASE_ENABLED", "").lower()
                        not in {"1", "true", "yes", "on"}
                    or not vm_remote_operation_protocol_enabled()
                    or not vm_persistent_rootdisk_enabled()
                    or vm_mode_from_env() != "same-cluster"
                ):
                    return None
                episode = read_episode(
                    _episode_document(thread["workspace_idle_episode"]),
                    revision=revision,
                )
                runtime = RuntimeIdentity(
                    "thread", str(owner_id), "vm", str(expected["generation"]),
                    str(expected["vm_uid"]),
                )
                now = await conn.fetchval("SELECT clock_timestamp()")
                decision = evaluate_idle(
                    episode, now=now, runtime=runtime, enabled=True,
                    human_wait_current=bool(
                        episode is not None and episode.wait_kind == "natural_pause"
                        and episode.episode_id == episode_id
                    ),
                    supported=True,
                )
                if decision.state != "eligible":
                    return None
                metadata = _object(thread["metadata"])
                vm = _object(metadata.get("vm"))
                try:
                    attested = _identity_from_row(
                        dict(thread), owner_kind="thread",
                        owner_id=str(owner_id), operation_kind="idle_policy",
                    )
                except VMRemoteOperationUnavailable:
                    return None
                if (
                    attested.workspace_generation != str(expected["generation"])
                    or attested.vm_uid != str(expected["vm_uid"])
                    or attested.launcher_pod_uid != str(expected["launcher_uid"])
                    or _uuid(vm.get("vmi_uid")) != expected["vmi_uid"]
                    or _uuid(vm.get("rootdisk_pvc_uid")) != expected["pvc_uid"]
                    or _uuid(vm.get("vm_uid")) != expected["vm_uid"]
                    or _object(metadata.get("ide_session")).get("status")
                        in {"active", "idle", "restoring"}
                ):
                    return None
                agent = await conn.fetchrow(
                    "SELECT id,thread_id,pod_uid,hostname,status,metadata "
                    "FROM agents WHERE id=$1 FOR SHARE", thread["agent_id"],
                )
                pod = _object(metadata.get("agent_pod"))
                if (
                    agent is None or agent["thread_id"] != owner_id
                    or str(agent["pod_uid"] or "") != str(pod.get("pod_uid") or "")
                    or agent["hostname"] != pod.get("pod_name")
                    or agent["status"] not in {"session", "working", "ready"}
                ):
                    return None
                held = await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM thread_input_deliveries "
                    "WHERE thread_id=$1 AND state NOT IN ('settled','cancelled')) "
                    "OR EXISTS(SELECT 1 FROM thread_control_requests "
                    "WHERE thread_id=$1 AND outcome IS NULL) "
                    "OR EXISTS(SELECT 1 FROM thread_permission_requests "
                    "WHERE thread_id=$1 AND status='pending') "
                    "OR EXISTS(SELECT 1 FROM threads WHERE parent_thread_id=$1 "
                    "AND status NOT IN ('ended','deleted')) "
                    "OR EXISTS(SELECT 1 FROM vm_idle_access_leases "
                    "WHERE owner_kind='thread' AND owner_id=$1 AND closed_at IS NULL "
                    "AND expires_at>clock_timestamp()) "
                    "OR EXISTS(SELECT 1 FROM vm_remote_operation_leases "
                    "WHERE owner_kind='thread' AND owner_id=$1 AND settled_at IS NULL) "
                    "OR EXISTS(SELECT 1 FROM vm_workspace_recoveries "
                    "WHERE owner_kind='thread' AND owner_id=$1 AND resolved_at IS NULL) "
                    "OR EXISTS(SELECT 1 FROM vm_workspace_cleanup_admissions "
                    "WHERE owner_kind='thread' AND owner_id=$1 AND completed_at IS NULL)",
                    owner_id,
                )
                if held:
                    return None
                selected_resource = configured_enforcement_policy()
                retry = await conn.fetchrow(
                    "SELECT * FROM vm_creation_retries WHERE owner_kind='thread' "
                    "AND thread_id=$1 AND provision_generation=$2 FOR UPDATE",
                    owner_id, expected["generation"],
                )
                if retry is None:
                    if selected_resource is not None:
                        return None
                elif (
                    retry["state"] != "succeeded"
                    or retry["observed_vm_uid"] != expected["vm_uid"]
                    or retry["observed_pvc_uid"] != expected["pvc_uid"]
                    or selected_resource is not None
                    and _object(retry["controller_configuration"]).get("version") != 3
                ):
                    return None
                retirement = await self.db.begin_pinned_thread_retirement(
                    str(owner_id), permanent=False, settle_status="suspended",
                    expected_runtime_generation=str(thread["runtime_generation"]),
                    expected_agent_id=str(thread["agent_id"]),
                    expected_attach_token=str(thread["runtime_attach_token"]),
                    initiator="system", _connection=conn,
                )
                if retirement.get("state") != "pending" or retirement.get("reused"):
                    return None
                if not await self.db.authorize_pinned_thread_retirement(
                    str(owner_id), token=retirement["token"],
                    generation=retirement["generation"],
                    settle_status="suspended", _connection=conn,
                ):
                    raise _ThreadIdleAdmissionLost
                captured_pod = _object(
                    _object(retirement.get("context")).get("agent_pod")
                )
                if (
                    captured_pod.get("protection_protocol") != "finalizer_v1"
                    or not all(captured_pod.get(key) for key in (
                        "pod_name", "pod_uid", "namespace",
                    ))
                ):
                    raise _ThreadIdleAdmissionLost
                operation = await conn.fetchrow(
                    "INSERT INTO vm_idle_operations "
                    "(owner_kind,owner_id,release_kind,phase,episode_id,"
                    "episode_revision,provision_generation,vm_uid,vmi_uid,"
                    "launcher_uid,pvc_uid,retained_kind,thread_runtime_generation,"
                    "thread_retirement_token,thread_agent_pod_identity) VALUES "
                    "('thread',$1,'pinned_thread','releasing',$2,$3,$4,$5,$6,"
                    "$7,$8,'rootdisk',$9,$10,$11::jsonb) RETURNING *",
                    owner_id, source_id, revision, expected["generation"],
                    expected["vm_uid"], expected["vmi_uid"],
                    expected["launcher_uid"], expected["pvc_uid"],
                    UUID(retirement["generation"]), UUID(retirement["token"]),
                    json.dumps(captured_pod),
                )
                vm.update(status="suspending", _suspend_remote_io_closed=str(operation["id"]))
                metadata["vm"] = vm
                changed = await conn.fetchval(
                    "UPDATE threads SET metadata=$2::jsonb WHERE id=$1 "
                    "AND runtime_retirement_token=$3 RETURNING id",
                    owner_id, json.dumps(metadata), UUID(retirement["token"]),
                )
                if changed is None:
                    raise _ThreadIdleAdmissionLost
                if retry is not None:
                    resource = await _idle_resource_binding_on_conn(
                        conn, self.db, retry=retry, operation=operation,
                    )
                    if resource is not None and not await resource.mark_idle_teardown_on_conn(
                        conn, retry=retry, operation=operation,
                    ):
                        raise ResourceAdmissionError("resource_idle_charge_changed")
                return dict(operation)
        except _ThreadIdleAdmissionLost:
            return None

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
        if job is None or job["status"] != "pending_review" or job["execution_lane"] not in {"stateless", "pinned"}:
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
        current_generation = _uuid(_object(context.get("vm")).get("provision_generation"))
        retry = await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE job_id=$1 "
            "AND provision_generation=$2 FOR UPDATE",
            owner_id, current_generation,
        ) if current_generation is not None else None
        operation = await conn.fetchrow(
            "SELECT * FROM vm_idle_operations WHERE owner_kind='job' AND owner_id=$1 "
            "AND closed_at IS NULL FOR UPDATE", owner_id,
        )
        new_operation = operation is None
        if operation is not None and _release_kind(operation) != (
            "pinned_job" if job["execution_lane"] == "pinned" else "stateless"
        ):
            return None
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
            vmi_uid=operation["vmi_uid"] if operation is not None else _uuid(vm.get("vmi_uid")),
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
        if operation is None and job["execution_lane"] == "pinned":
            from orchestrator.services.pinned_job_delivery import (
                pinned_wait_receipt_on_conn,
            )

            if (
                not vm_remote_operation_protocol_enabled()
                or not vm_persistent_rootdisk_enabled()
                or vm.get("status") != "ready"
                or job["assigned_agent_id"] is None
                or _object(context.get("ide_session")).get("status")
                    in {"active", "idle", "restoring"}
            ):
                return None
            if await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM vm_idle_access_leases "
                "WHERE owner_kind='job' AND owner_id=$1 AND closed_at IS NULL "
                "AND expires_at>clock_timestamp()) OR "
                "EXISTS(SELECT 1 FROM vm_remote_operation_leases "
                "WHERE owner_kind='job' AND owner_id=$1 AND settled_at IS NULL) OR "
                "EXISTS(SELECT 1 FROM vm_workspace_recoveries "
                "WHERE owner_kind='job' AND owner_id=$1 AND resolved_at IS NULL) OR "
                "EXISTS(SELECT 1 FROM vm_workspace_recovery_jobs "
                "WHERE job_id=$1 AND resolved_at IS NULL) OR "
                "EXISTS(SELECT 1 FROM vm_workspace_cleanup_admissions "
                "WHERE owner_kind='job' AND owner_id=$1 AND completed_at IS NULL) OR "
                "EXISTS(SELECT 1 FROM jobs WHERE parent_job_id=$1 "
                "AND status NOT IN ('completed','failed','cancelled') "
                "AND context->>'inherits_parent_workspace'='true') OR "
                "EXISTS(SELECT 1 FROM job_completion_commands WHERE job_id=$1 "
                "AND state IN ('pending','finalizing','parked'))",
                owner_id,
            ):
                return None
            source_receipt = await pinned_wait_receipt_on_conn(
                conn, job=job, source_kind="completion",
                source_id=UUID(source["command_id"]), episode=episode,
            )
            if source_receipt is None:
                return None
            delivery, receipt = (
                source_receipt["delivery"], source_receipt["receipt"]
            )
            agent = await conn.fetchrow(
                "SELECT * FROM agents WHERE id=$1 FOR UPDATE",
                delivery["agent_id"],
            )
            if (
                agent is None
                or job["assigned_agent_id"] != delivery["agent_id"]
                or agent["hostname"] != delivery["pod_name"]
                or agent["pod_uid"] != delivery["pod_uid"]
                or _object(agent["metadata"]).get("dispatch_process_generation")
                    != delivery["process_generation"]
                or agent["current_job_id"] not in {None, owner_id}
                or agent["status"] not in {"ready", "working", "draining", "offline"}
                or await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM jobs WHERE assigned_agent_id=$1 "
                    "AND id<>$2 AND execution_lane='pinned' "
                    "AND status NOT IN ('completed','failed','cancelled'))",
                    delivery["agent_id"], owner_id,
                )
            ):
                return None
            operation = await conn.fetchrow(
                "INSERT INTO vm_idle_operations "
                "(owner_kind,owner_id,phase,episode_id,episode_revision,"
                "provision_generation,vm_uid,vmi_uid,launcher_uid,pvc_uid,retained_kind,"
                "release_kind,pinned_delivery_id,pinned_wait_receipt_id,"
                "pinned_agent_id,pinned_process_generation,pinned_agent_pod_name,"
                "pinned_agent_pod_namespace,pinned_agent_pod_uid,"
                "pinned_original_dispatch_marker,pinned_lease_observed_at,"
                "pinned_lease_expires_at) "
                "VALUES('job',$1,'releasing',$2,$3,$4,$5,$6,$7,$8,'rootdisk',"
                "'pinned_job',$9,$10,$11,$12,$13,$14,$15,$16::jsonb,$17,$18) "
                "RETURNING *",
                owner_id, UUID(episode.episode_id), episode.revision,
                generation, vm_uid, _uuid(vm["vmi_uid"]), launcher_uid,
                _uuid(vm["rootdisk_pvc_uid"]), delivery["id"], receipt["id"],
                delivery["agent_id"], delivery["process_generation"],
                delivery["pod_name"], delivery["pod_namespace"], delivery["pod_uid"],
                json.dumps(_object(delivery["original_dispatch_marker"])),
                receipt["observed_at"], receipt["lease_expires_at"],
            )
            await conn.execute(
                "UPDATE agents SET status='draining' WHERE id=$1",
                delivery["agent_id"],
            )
            vm.update(status="suspending", _suspend_remote_io_closed=str(operation["id"]))
            context["vm"] = vm
            await conn.execute(
                "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                owner_id, json.dumps(context),
            )
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
        if retry is not None:
            resource = await _idle_resource_binding_on_conn(
                conn, self.db, retry=retry, operation=operation,
            )
            if resource is not None:
                changed = await resource.mark_idle_teardown_on_conn(
                    conn, retry=retry, operation=operation,
                )
                if new_operation and not changed:
                    raise ResourceAdmissionError("resource_idle_charge_changed")
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
        """Reserve one exact Job release after a fresh, locked source recheck."""

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
        # Candidate nomination is advisory. A pinned agent is always locked
        # before queue/Job, then rechecked under the Job lock, matching the
        # heartbeat's agent -> Job order and final dispatch claim order.
        async with self.db.acquire() as probe:
            candidate = await probe.fetchrow(
                "SELECT execution_lane,assigned_agent_id FROM jobs WHERE id=$1",
                owner_id,
            )
        if candidate is None or candidate["execution_lane"] not in {"stateless", "pinned"}:
            return None
        if candidate["execution_lane"] == "pinned":
            async with self.db.acquire() as probe:
                if not await probe.fetchval(
                    "SELECT to_regclass('public.pinned_job_deliveries') IS NOT NULL"
                ):
                    return None
        candidate_agent_id = (
            candidate["assigned_agent_id"]
            if candidate["execution_lane"] == "pinned" else None
        )
        if candidate["execution_lane"] == "pinned" and candidate_agent_id is None:
            return None
        async with self.db.acquire() as conn, conn.transaction():
            pinned_agent = None
            if candidate_agent_id is not None:
                pinned_agent = await conn.fetchrow(
                    "SELECT * FROM agents WHERE id=$1 FOR UPDATE",
                    candidate_agent_id,
                )
                if pinned_agent is None:
                    return None
            queue = await conn.fetchrow(
                "SELECT state,unit_kind,lease_token,leased_until FROM run_queue "
                "WHERE unit_id=$1 FOR UPDATE",
                owner_id,
            )
            if candidate_agent_id is None and (
                queue is None or queue["unit_kind"] != "worker_batch"
            ):
                return None
            row = await conn.fetchrow(
                "SELECT * FROM jobs WHERE id=$1 FOR UPDATE", owner_id
            )
            if (
                row is None
                or row["execution_lane"] != candidate["execution_lane"]
                or row["assigned_agent_id"] != candidate_agent_id
                or (candidate_agent_id is not None and queue is not None)
            ):
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
            if row["execution_lane"] == "stateless" and row["assigned_agent_id"] is not None:
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
                or (candidate_agent_id is None and queue["state"] not in {"done", "parked"})
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
                    vmi_uid=expected["vmi_uid"], pvc_uid=expected["pvc_uid"],
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
                    vmi_uid=expected["vmi_uid"],
                )
                if review_source is None:
                    return None
            pinned_source = None
            if candidate_agent_id is not None:
                from orchestrator.services.pinned_job_delivery import (
                    pinned_wait_receipt_on_conn,
                )

                try:
                    source_id = UUID(episode.wait_key)
                except (TypeError, ValueError):
                    return None
                source_kind = (
                    "route" if episode.wait_kind == "human_message"
                    else "completion"
                )
                if source_kind == "route":
                    route = await conn.fetchrow(
                        "SELECT job_id,project_id,state,blocking FROM job_message_routes "
                        "WHERE route_id=$1", source_id,
                    )
                    if (
                        route is None or route["job_id"] != owner_id
                        or route["project_id"] != row["project_id"]
                        or not route["blocking"]
                        or route["state"] not in {
                            "user_direct", "pending_both", "escalated_to_user",
                        }
                        or freeze.get("route_id") != episode.wait_key
                    ):
                        return None
                pinned_source = await pinned_wait_receipt_on_conn(
                    conn, job=row, source_kind=source_kind, source_id=source_id,
                    episode=episode,
                )
                if pinned_source is None:
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
                        "SELECT * FROM vm_creation_retries "
                        "WHERE job_id=$1 AND provision_generation=$2 FOR SHARE",
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
            if pinned_source is not None:
                delivery = pinned_source["delivery"]
                receipt = pinned_source["receipt"]
                prior_stop = None
                if pinned_source["access_rebound"]:
                    prior = await conn.fetchrow(
                        "SELECT * FROM vm_idle_operations WHERE owner_kind='job' "
                        "AND owner_id=$1 AND episode_id=$2 "
                        "AND episode_revision=$3 ORDER BY id DESC LIMIT 1 FOR SHARE",
                        owner_id, UUID(episode_id), revision - 1,
                    )
                    prior_evidence = _object(prior["pinned_stop_evidence"]) if prior else {}
                    if (
                        prior is None or prior["release_kind"] != "pinned_job"
                        or prior["phase"] != "ready" or prior["closed_at"] is None
                        or prior["pinned_delivery_id"] != delivery["id"]
                        or prior["pinned_wait_receipt_id"] != receipt["id"]
                        or prior["pinned_agent_id"] != candidate_agent_id
                        or prior["pinned_agent_pod_uid"] != delivery["pod_uid"]
                        or prior["pinned_terminal_observed_at"] is None
                        or prior["pinned_stop_verified_at"] is None
                        or prior_evidence.get("operation_id") != str(prior["id"])
                        or prior_evidence.get("pod_uid") != delivery["pod_uid"]
                        or prior_evidence.get("terminal_containers_observed") is not True
                        or prior_evidence.get("final_absence")
                            not in {"exact_absent", "replacement"}
                    ):
                        return None
                    prior_stop = prior
                namespace = os.getenv(
                    "AGENT_NAMESPACE",
                    os.getenv("WORKSPACE_NAMESPACE", "superhuman-remote-worker"),
                )
                if (
                    delivery["agent_id"] != candidate_agent_id
                    or delivery["pod_namespace"] != namespace
                    or pinned_agent["hostname"] != delivery["pod_name"]
                    or pinned_agent["pod_uid"] != delivery["pod_uid"]
                    or _object(pinned_agent["metadata"]).get("dispatch_process_generation")
                        != delivery["process_generation"]
                    or pinned_agent["current_job_id"] not in {None, owner_id}
                    or pinned_agent["status"] not in {
                        "ready", "working", "draining", "offline",
                    }
                    or _object(context.get("_workspace_dispatch_authority"))
                        != _object(delivery["original_dispatch_marker"])
                ):
                    return None
                if await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM jobs WHERE assigned_agent_id=$1 "
                    "AND id<>$2 AND execution_lane='pinned' "
                    "AND status NOT IN ('completed','failed','cancelled')) "
                    "OR EXISTS(SELECT 1 FROM vm_idle_operations "
                    "WHERE release_kind='pinned_job' AND pinned_agent_id=$1 "
                    "AND closed_at IS NULL)",
                    candidate_agent_id, owner_id,
                ):
                    return None
                new_operation_id = uuid4()
                reuse_evidence = None
                if prior_stop is not None:
                    reuse_evidence = {
                        "version": 1,
                        "kind": "pinned_job_agent_stop_reuse",
                        "operation_id": str(new_operation_id),
                        "prior_operation_id": str(prior_stop["id"]),
                        "job_id": str(owner_id),
                        "agent_id": str(candidate_agent_id),
                        "process_generation": delivery["process_generation"],
                        "pod_name": delivery["pod_name"],
                        "pod_namespace": delivery["pod_namespace"],
                        "pod_uid": delivery["pod_uid"],
                        "delivery_id": str(delivery["id"]),
                        "terminal_containers_observed": True,
                        "final_absence": _object(
                            prior_stop["pinned_stop_evidence"]
                        )["final_absence"],
                    }
                operation = await conn.fetchrow(
                    "INSERT INTO vm_idle_operations "
                    "(owner_kind,owner_id,phase,episode_id,episode_revision,"
                    "provision_generation,vm_uid,vmi_uid,launcher_uid,pvc_uid,retained_kind,"
                    "release_kind,pinned_delivery_id,pinned_wait_receipt_id,"
                    "pinned_agent_id,pinned_process_generation,pinned_agent_pod_name,"
                    "pinned_agent_pod_namespace,pinned_agent_pod_uid,"
                    "pinned_original_dispatch_marker,pinned_lease_observed_at,"
                    "pinned_lease_expires_at,id,pinned_terminal_observed_at,"
                    "pinned_stop_evidence,pinned_stop_verified_at) "
                    "VALUES('job',$1,'releasing',$2,$3,$4,$5,$6,$7,$8,'rootdisk',"
                    "'pinned_job',$9,$10,$11,$12,$13,$14,$15,$16::jsonb,$17,$18,"
                    "$19,$20,$21::jsonb,$22) "
                    "RETURNING *",
                    owner_id, UUID(episode_id), revision,
                    expected["generation"], expected["vm_uid"],
                    expected["vmi_uid"], expected["launcher_uid"],
                    expected["pvc_uid"], delivery["id"], receipt["id"],
                    candidate_agent_id, delivery["process_generation"],
                    delivery["pod_name"], delivery["pod_namespace"],
                    delivery["pod_uid"],
                    json.dumps(_object(delivery["original_dispatch_marker"])),
                    receipt["observed_at"], receipt["lease_expires_at"],
                    new_operation_id,
                    prior_stop["pinned_terminal_observed_at"] if prior_stop else None,
                    json.dumps(reuse_evidence) if reuse_evidence else None,
                    prior_stop["pinned_stop_verified_at"] if prior_stop else None,
                )
                await conn.execute(
                    "UPDATE agents SET status='draining' WHERE id=$1",
                    candidate_agent_id,
                )
            else:
                operation = await conn.fetchrow(
                    """
                    INSERT INTO vm_idle_operations
                        (owner_kind,owner_id,phase,episode_id,episode_revision,
                         provision_generation,vm_uid,vmi_uid,launcher_uid,pvc_uid,retained_kind)
                    VALUES ('job',$1,'releasing',$2,$3,$4,$5,$6,$7,$8,'rootdisk')
                    RETURNING *
                    """,
                    owner_id, UUID(episode_id), revision,
                    expected["generation"], expected["vm_uid"],
                    expected["vmi_uid"], expected["launcher_uid"],
                    expected["pvc_uid"],
                )
            vm.update(status="suspending", _suspend_remote_io_closed=str(operation["id"]))
            context["vm"] = vm
            await conn.execute(
                "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                owner_id,
                json.dumps(context),
            )
            resource = await _idle_resource_binding_on_conn(
                conn, self.db, retry=ledger, operation=operation,
            )
            if resource is not None:
                if not await resource.mark_idle_teardown_on_conn(
                    conn, retry=ledger, operation=operation,
                ):
                    raise ResourceAdmissionError("resource_idle_charge_changed")
            return dict(operation)

    async def record_pinned_terminal(
        self, operation: Mapping[str, Any], *, claimant: str,
    ) -> bool:
        """Remember the exact Pod's terminal-container observation before finalizer release."""
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE vm_idle_operations SET "
                "pinned_terminal_observed_at=COALESCE(pinned_terminal_observed_at,"
                "clock_timestamp()) WHERE id=$1 AND release_kind='pinned_job' "
                "AND phase IN ('releasing','release_held') "
                "AND claim_token=$2 AND claimed_by=$3 "
                "AND claim_expires_at>clock_timestamp() AND closed_at IS NULL "
                "RETURNING pinned_terminal_observed_at",
                operation["id"], operation["claim_token"], claimant,
            )
            return row is not None

    async def record_pinned_stop(
        self, operation: Mapping[str, Any], *, claimant: str,
        absence: str,
    ) -> bool:
        """Append one Job-scoped process-stop receipt after terminal and absence."""
        if absence not in {"exact_absent", "replacement"}:
            return False
        expected = {
            "version": 1,
            "kind": "pinned_job_agent_stop",
            "operation_id": str(operation["id"]),
            "job_id": str(operation["owner_id"]),
            "agent_id": str(operation["pinned_agent_id"]),
            "process_generation": operation["pinned_process_generation"],
            "pod_name": operation["pinned_agent_pod_name"],
            "pod_namespace": operation["pinned_agent_pod_namespace"],
            "pod_uid": operation["pinned_agent_pod_uid"],
            "delivery_id": str(operation["pinned_delivery_id"]),
            "terminal_containers_observed": True,
            "final_absence": absence,
        }
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE vm_idle_operations SET pinned_stop_evidence=$4::jsonb,"
                "pinned_stop_verified_at=clock_timestamp() "
                "WHERE id=$1 AND release_kind='pinned_job' "
                "AND phase IN ('releasing','release_held') "
                "AND claim_token=$2 AND claimed_by=$3 "
                "AND claim_expires_at>clock_timestamp() AND closed_at IS NULL "
                "AND pinned_terminal_observed_at IS NOT NULL "
                "AND pinned_stop_evidence IS NULL RETURNING id",
                operation["id"], operation["claim_token"], claimant,
                json.dumps(expected),
            )
            if row is not None:
                return True
            stored = await conn.fetchrow(
                "SELECT pinned_stop_evidence,pinned_stop_verified_at "
                "FROM vm_idle_operations WHERE id=$1 AND claim_token=$2 "
                "AND claimed_by=$3 AND claim_expires_at>clock_timestamp()",
                operation["id"], operation["claim_token"], claimant,
            )
            return bool(
                stored and stored["pinned_stop_verified_at"] is not None
                and _object(stored["pinned_stop_evidence"]) == expected
            )

    async def pinned_stop_valid(self, operation_id: str, *, token: int,
                                claimant: str) -> bool:
        if _uuid(operation_id) is None:
            return False
        async with self.db.acquire() as conn:
            operation = await conn.fetchrow(
                "SELECT * FROM vm_idle_operations WHERE id=$1 AND claim_token=$2 "
                "AND claimed_by=$3 AND claim_expires_at>clock_timestamp() "
                "AND closed_at IS NULL",
                UUID(operation_id), token, claimant,
            )
            return bool(operation and await _pinned_stop_valid_on_conn(conn, operation))

    async def complete_release(
        self, operation_id: str, *, evidence: Mapping[str, Any]
    ) -> bool:
        """Publish suspended only after exact process zero and physical stop."""

        if _uuid(operation_id) is None or not isinstance(evidence, Mapping):
            return False
        async with self.db.acquire() as conn, conn.transaction():
            located = await conn.fetchrow(
                "SELECT owner_kind,owner_id,provision_generation "
                "FROM vm_idle_operations WHERE id=$1",
                UUID(operation_id),
            )
            if located is None:
                return False
            if located["owner_kind"] == "thread":
                # The thread branch takes the thread -> operation lock order.
                return await self._complete_thread_release_on_conn(
                    conn, UUID(operation_id), evidence=evidence,
                )
            if located["owner_kind"] != "job":
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
            retry = await conn.fetchrow(
                "SELECT * FROM vm_creation_retries WHERE job_id=$1 "
                "AND provision_generation=$2 FOR UPDATE",
                owner_id, located["provision_generation"],
            )
            operation = await conn.fetchrow(
                "SELECT * FROM vm_idle_operations WHERE id=$1 FOR UPDATE",
                UUID(operation_id),
            )
            if job is None or operation is None:
                return False
            if operation["phase"] == "suspended":
                if retry is not None:
                    resource = await _idle_resource_binding_on_conn(
                        conn, self.db, retry=retry, operation=operation,
                        released_replay=True,
                    )
                    if resource is not None:
                        await resource.release_idle_compute_on_conn(
                            conn, retry=retry, operation=operation,
                        )
                return True
            if operation["phase"] not in {"releasing", "release_held"}:
                return False
            if _release_kind(operation) == "pinned_job":
                if not await _pinned_stop_valid_on_conn(conn, operation):
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
            suspended = await conn.fetchrow(
                "UPDATE vm_idle_operations SET phase='suspended',"
                "stop_evidence=$2::jsonb,stop_verified_at=clock_timestamp(),"
                "wake_requested=wake_requested OR $3,"
                "wake_execution_requested=wake_execution_requested OR $3,"
                "last_progress_at=clock_timestamp() "
                "WHERE id=$1 RETURNING *",
                operation["id"],
                json.dumps(dict(evidence)),
                wake_requested,
            )
            if retry is not None:
                resource = await _idle_resource_binding_on_conn(
                    conn, self.db, retry=retry, operation=suspended,
                )
                if resource is not None:
                    await resource.release_idle_compute_on_conn(
                        conn, retry=retry, operation=suspended,
                    )
            return True

    async def _complete_thread_release_on_conn(
        self, conn: Any, operation_id: UUID, *, evidence: Mapping[str, Any],
    ) -> bool:
        located = await conn.fetchrow(
            "SELECT owner_id FROM vm_idle_operations WHERE id=$1 "
            "AND owner_kind='thread'", operation_id,
        )
        if located is None:
            return False
        owner_id = located["owner_id"]
        thread = await conn.fetchrow(
            "SELECT status,metadata,runtime_generation,agent_id,"
            "runtime_attach_token,workspace_idle_revision,"
            "workspace_idle_episode FROM threads WHERE id=$1 FOR UPDATE", owner_id,
        )
        operation = await conn.fetchrow(
            "SELECT * FROM vm_idle_operations WHERE id=$1 FOR UPDATE", operation_id,
        )
        if thread is None or operation is None or operation["owner_id"] != owner_id:
            return False
        retry = await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE owner_kind='thread' "
            "AND thread_id=$1 AND provision_generation=$2 FOR UPDATE",
            owner_id, operation["provision_generation"],
        )
        from orchestrator.services.vm_resource_job_runtime import (
            configured_enforcement_policy,
        )

        if retry is None and configured_enforcement_policy() is not None:
            return False
        if operation["phase"] == "suspended":
            if _object(operation["stop_evidence"]) != dict(evidence):
                return False
            if retry is not None:
                resource = await _idle_resource_binding_on_conn(
                    conn, self.db, retry=retry, operation=dict(operation),
                    released_replay=True,
                )
                if resource is not None:
                    return False
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
            or any(evidence.get(k) != v for k, v in expected.items())
            or any(evidence.get(k) is not True for k in (
                "vm_absent", "vmi_absent", "launcher_absent",
                "retained_pvc", "controller_authenticated",
            ))
            or evidence.get("same_generation_replacement") is not False
        ):
            return False
        outcome = await conn.fetchrow(
            "SELECT disposition,permanent,outcome FROM "
            "thread_runtime_retirement_outcomes WHERE thread_id=$1 "
            "AND runtime_generation=$2 AND retirement_token=$3 FOR SHARE",
            owner_id, operation["thread_runtime_generation"],
            operation["thread_retirement_token"],
        )
        if (
            outcome is None or outcome["disposition"] != "suspended"
            or outcome["permanent"] is not False
            or outcome["outcome"] != "settled"
            or thread["status"] != "suspended"
            or thread["agent_id"] is not None
            or thread["runtime_attach_token"] is not None
            or thread["runtime_generation"]
                == operation["thread_runtime_generation"]
            or operation["thread_agent_stop_verified_at"] is None
            or _object(operation["thread_agent_stop_evidence"]) != {
                "version": 1,
                "pod": _object(operation["thread_agent_pod_identity"]),
                "disposition": _object(operation["thread_agent_stop_evidence"])
                    .get("disposition"),
                "retirement_token": str(operation["thread_retirement_token"]),
                "controller_authenticated": True,
            }
            or _object(operation["thread_agent_stop_evidence"])
                .get("disposition") not in {"exact_absent", "replacement"}
        ):
            return False
        process_zero = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM managed_repository_process_zero_receipts "
            "WHERE owner_kind='thread' AND owner_id=$1 AND scope='vm' "
            "AND provisioner='vm' AND runtime_incarnation=$2)",
            owner_id, str(operation["provision_generation"]),
        )
        if not process_zero:
            return False
        metadata = _object(thread["metadata"])
        vm = _object(metadata.get("vm"))
        if (
            _uuid(vm.get("provision_generation"))
                != operation["provision_generation"]
            or _uuid(vm.get("vm_uid")) != operation["vm_uid"]
            or _uuid(vm.get("rootdisk_pvc_uid")) != operation["pvc_uid"]
            or vm.get("_suspend_remote_io_closed") != str(operation_id)
            or vm.get("status") != "suspending"
        ):
            return False
        vm.update(status="suspended", rootdisk="kept")
        metadata["vm"] = vm
        await conn.execute(
            "UPDATE threads SET metadata=$2::jsonb WHERE id=$1",
            owner_id, json.dumps(metadata),
        )
        episode = read_episode(
            _episode_document(thread["workspace_idle_episode"]),
            revision=thread["workspace_idle_revision"],
        )
        wake_requested = bool(
            episode is None or episode.episode_id != str(operation["episode_id"])
            or episode.revision != operation["episode_revision"]
        )
        updated_operation = await conn.fetchrow(
            "UPDATE vm_idle_operations SET phase='suspended',"
            "stop_evidence=$2::jsonb,stop_verified_at=clock_timestamp(),"
            "wake_requested=wake_requested OR $3,"
            "wake_execution_requested=wake_execution_requested OR $3,"
            "last_progress_at=clock_timestamp() WHERE id=$1 RETURNING *",
            operation_id, json.dumps(dict(evidence)), wake_requested,
        )
        if retry is not None:
            resource = await _idle_resource_binding_on_conn(
                conn, self.db, retry=retry, operation=dict(updated_operation),
            )
            if resource is not None:
                await resource.release_idle_compute_on_conn(
                    conn, retry=retry, operation=dict(updated_operation),
                )
        return True

    @staticmethod
    async def _reserve_wake_on_conn(conn, operation, *, execution_requested: bool):
        if operation["access_rebind_proof"] is not None:
            # Ready access rebind already committed. The proof's original
            # no-execution decision cannot be rewritten by a later Resume.
            if not execution_requested:
                return operation
            return await conn.fetchrow(
                "UPDATE vm_idle_operations SET post_ready_resume_requested=true,"
                "retry_after=NULL,last_progress_at=clock_timestamp() "
                "WHERE id=$1 AND closed_at IS NULL "
                "RETURNING *",
                operation["id"],
            )
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
        if job is None or job["status"] != "pending_review" or job["execution_lane"] not in {"stateless", "pinned"}:
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
            or _release_kind(operation) != (
                "pinned_job" if job["execution_lane"] == "pinned" else "stateless"
            )
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
            vmi_uid=operation["vmi_uid"], pvc_uid=operation["pvc_uid"],
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
        context_merge: Mapping[str, Any] | None = None,
        expected_route_id: str | None = None,
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
        if context_merge is not None and (
            not execution_requested or not isinstance(context_merge, Mapping)
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
                job is None or job["execution_lane"] not in {"stateless", "pinned"}
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
            if (
                (job["execution_lane"] == "stateless" and (
                    queue is None or queue["unit_kind"] != "worker_batch"
                    or _release_kind(operation) != "stateless"
                ))
                or (job["execution_lane"] == "pinned" and (
                    queue is not None or _release_kind(operation) != "pinned_job"
                ))
            ):
                return None
            if expected_route_id is not None and (
                _object(await conn.fetchval(
                    "SELECT freeze_data FROM jobs WHERE id=$1", owner_id,
                )).get("route_id") != expected_route_id
            ):
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
            if context_merge and not operation["wake_execution_requested"]:
                merge = (
                    stamp_pinned_resume_input_ids(context_merge)
                    if job["execution_lane"] == "pinned" else dict(context_merge)
                )
                await conn.execute(
                    "UPDATE jobs SET context=COALESCE(context,'{}'::jsonb) || $2::jsonb "
                    "WHERE id=$1", owner_id, json.dumps(merge),
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

    async def request_thread_wake(
        self, thread_id: str, *, execution_requested: bool,
        access_kind: str | None = None, access_claimant: str | None = None,
    ) -> dict[str, Any] | None:
        """Join the one retained-disk wake under thread and operation locks."""
        owner_id = _uuid(thread_id)
        if owner_id is None or type(execution_requested) is not bool:
            return None
        if access_kind is not None and (
            execution_requested or access_kind not in {"ssh", "sftp", "ide"}
            or not isinstance(access_claimant, str)
            or not 1 <= len(access_claimant) <= 256
        ):
            return None
        async with self.db.acquire() as conn, conn.transaction():
            thread = await conn.fetchrow(
                "SELECT status,execution_lane,metadata,runtime_retirement_token,"
                "workspace_idle_revision,workspace_idle_episode "
                "FROM threads WHERE id=$1 FOR UPDATE", owner_id,
            )
            if (
                thread is None or thread["execution_lane"] != "pinned"
                or thread["status"] not in {
                    "awaiting_user", "suspended", "created",
                }
            ):
                return None
            operation = await conn.fetchrow(
                "SELECT * FROM vm_idle_operations WHERE owner_kind='thread' "
                "AND owner_id=$1 AND closed_at IS NULL FOR UPDATE", owner_id,
            )
            if operation is None:
                if thread["status"] == "created":
                    continuing = await conn.fetchrow(
                        "SELECT o.* FROM vm_idle_thread_access_continuations c "
                        "JOIN vm_idle_operations o ON o.id=c.operation_id "
                        "WHERE c.thread_id=$1 AND c.prepared_at IS NULL "
                        "AND o.phase='ready' AND o.closed_at IS NOT NULL "
                        "FOR UPDATE OF o", owner_id,
                    )
                    return dict(continuing) if (
                        execution_requested and continuing is not None
                        and thread["runtime_retirement_token"] is None
                    ) else None
                # A completed access-only wake left the session suspended,
                # with its wait rebound to the Ready successor. A later
                # explicit Resume/input joins that same successor without a
                # second create or a retroactive execution on access alone.
                prior = await conn.fetchrow(
                    "SELECT * FROM vm_idle_operations WHERE "
                    "owner_kind='thread' AND owner_id=$1 "
                    "AND release_kind='pinned_thread' AND phase='ready' "
                    "AND closed_at IS NOT NULL ORDER BY closed_at DESC LIMIT 1 "
                    "FOR UPDATE", owner_id,
                )
                vm = _object(_object(thread["metadata"]).get("vm"))
                episode = read_episode(
                    _episode_document(thread["workspace_idle_episode"]),
                    revision=thread["workspace_idle_revision"],
                )
                if (
                    prior is None or thread["status"] != "suspended"
                    or prior["wake_ready_at"] is None
                    or prior["wake_execution_requested"]
                    or vm.get("status") != "ready"
                    or vm.get("idle_wake_operation_id") != str(prior["id"])
                    or _uuid(vm.get("provision_generation"))
                        != prior["wake_generation"]
                    or _uuid(vm.get("rootdisk_pvc_uid")) != prior["pvc_uid"]
                    or episode is None
                    or episode.episode_id != str(prior["episode_id"])
                    or episode.revision != prior["episode_revision"] + 1
                    or episode.runtime_identity.runtime_generation
                        != str(prior["wake_generation"])
                    or episode.runtime_identity.runtime_uid != vm.get("vm_uid")
                    or _object(prior["thread_wake_ready_identity"]) != {
                        "generation": str(prior["wake_generation"]),
                        "vm_uid": vm.get("vm_uid"),
                        "vmi_uid": vm.get("vmi_uid"),
                        "launcher_uid": vm.get("active_pod_uid"),
                        "pvc_uid": str(prior["pvc_uid"]),
                    }
                ):
                    return None
                if execution_requested:
                    changed = await conn.execute(
                        "UPDATE threads SET status='created' WHERE id=$1 "
                        "AND status='suspended' AND agent_id IS NULL "
                        "AND runtime_retirement_token IS NULL",
                        owner_id,
                    )
                    if changed != "UPDATE 1":
                        return None
                    inserted = await conn.execute(
                        "INSERT INTO vm_idle_thread_access_continuations "
                        "(operation_id,thread_id) VALUES($1,$2)",
                        prior["id"], owner_id,
                    )
                    if inserted != "INSERT 0 1":
                        raise _ThreadIdleAdmissionLost
                if access_kind is not None:
                    lease = await conn.fetchrow(
                        "SELECT id FROM vm_idle_access_leases WHERE "
                        "owner_kind='thread' AND owner_id=$1 AND kind=$2 "
                        "AND claimed_by=$3 AND wake_id=$4 AND closed_at IS NULL "
                        "AND max_expires_at>clock_timestamp() "
                        "ORDER BY acquired_at DESC LIMIT 1 FOR UPDATE",
                        owner_id, access_kind, access_claimant,
                        prior["wake_id"],
                    )
                    if lease is not None:
                        await conn.execute(
                            "UPDATE vm_idle_access_leases SET "
                            "expires_at=LEAST(max_expires_at,"
                            "clock_timestamp()+interval '2 minutes') WHERE id=$1",
                            lease["id"],
                        )
                    else:
                        await conn.execute(
                            "INSERT INTO vm_idle_access_leases "
                            "(owner_kind,owner_id,provision_generation,vm_uid,wake_id,"
                            "kind,claimed_by,expires_at,max_expires_at) "
                            "VALUES('thread',$1,$2,$3,$4,$5,$6,"
                            "clock_timestamp()+interval '2 minutes',"
                            "clock_timestamp()+interval '1 hour')",
                            owner_id, prior["wake_generation"],
                            _uuid(vm.get("vm_uid")), prior["wake_id"],
                            access_kind, access_claimant,
                        )
                return dict(prior)
            if operation["release_kind"] != "pinned_thread":
                return None
            if operation["thread_terminal_intent_at"] is not None:
                return None
            if operation["phase"] not in {
                "releasing", "release_held", "suspended", "waking", "wake_held",
            }:
                return None
            if operation["phase"] in {"releasing", "release_held"}:
                if (
                    thread["runtime_retirement_token"]
                        != operation["thread_retirement_token"]
                    or thread["status"] != "awaiting_user"
                ):
                    return None
            elif (
                thread["status"] not in (
                    {"suspended", "created"}
                    if operation["wake_execution_requested"]
                    and operation["wake_ready_at"] is not None
                    else {"suspended"}
                )
                or thread["runtime_retirement_token"] is not None
                or operation["stop_verified_at"] is None
                or not await self._thread_retirement_outcome_on_conn(
                    conn, operation,
                )
            ):
                return None
            vm = _object(_object(thread["metadata"]).get("vm"))
            original = (
                _uuid(vm.get("provision_generation"))
                    == operation["provision_generation"]
                and _uuid(vm.get("vm_uid")) == operation["vm_uid"]
                and _uuid(vm.get("rootdisk_pvc_uid")) == operation["pvc_uid"]
            )
            successor = (
                operation["wake_generation"] is not None
                and _uuid(vm.get("provision_generation"))
                    == operation["wake_generation"]
                and vm.get("idle_wake_operation_id") == str(operation["id"])
                and _uuid(vm.get("idle_predecessor_pvc_uid"))
                    == operation["pvc_uid"]
            )
            if not (original or successor):
                return None
            row = await self._reserve_wake_on_conn(
                conn, operation, execution_requested=execution_requested,
            )
            if access_kind is not None:
                lease = await conn.fetchrow(
                    "SELECT id FROM vm_idle_access_leases WHERE "
                    "owner_kind='thread' AND owner_id=$1 AND kind=$2 "
                    "AND claimed_by=$3 AND wake_id=$4 AND closed_at IS NULL "
                    "AND max_expires_at>clock_timestamp() "
                    "ORDER BY acquired_at DESC LIMIT 1 FOR UPDATE",
                    owner_id, access_kind, access_claimant, row["wake_id"],
                )
                if lease is not None:
                    await conn.execute(
                        "UPDATE vm_idle_access_leases SET "
                        "expires_at=LEAST(max_expires_at,"
                        "clock_timestamp()+interval '2 minutes') WHERE id=$1",
                        lease["id"],
                    )
                else:
                    await conn.execute(
                        "INSERT INTO vm_idle_access_leases "
                        "(owner_kind,owner_id,provision_generation,vm_uid,wake_id,"
                        "kind,claimed_by,expires_at,max_expires_at) "
                        "VALUES('thread',$1,$2,$3,$4,$5,$6,"
                        "clock_timestamp()+interval '2 minutes',"
                        "clock_timestamp()+interval '1 hour')",
                        owner_id,
                        row["wake_generation"] if row["wake_ready_at"] else
                            operation["provision_generation"],
                        _uuid(vm.get("vm_uid")) if row["wake_ready_at"] else
                            operation["vm_uid"],
                        row["wake_id"], access_kind, access_claimant,
                    )
            return dict(row)

    @staticmethod
    async def _thread_retirement_outcome_on_conn(
        conn: Any, operation: Mapping[str, Any],
    ) -> bool:
        outcome = await conn.fetchrow(
            "SELECT disposition,permanent,outcome FROM "
            "thread_runtime_retirement_outcomes WHERE thread_id=$1 "
            "AND runtime_generation=$2 AND retirement_token=$3 FOR SHARE",
            operation["owner_id"], operation["thread_runtime_generation"],
            operation["thread_retirement_token"],
        )
        return bool(
            outcome is not None and outcome["disposition"] == "suspended"
            and outcome["permanent"] is False and outcome["outcome"] == "settled"
        )

    async def record_thread_agent_stop(
        self, operation_id: str, *, evidence: Mapping[str, Any],
    ) -> bool:
        """Retain exact Pod-zero observation only after settled source retirement."""
        if _uuid(operation_id) is None:
            return False
        async with self.db.acquire() as conn, conn.transaction():
            located = await conn.fetchrow(
                "SELECT owner_id FROM vm_idle_operations WHERE id=$1 "
                "AND owner_kind='thread'", UUID(operation_id),
            )
            if located is None:
                return False
            thread = await conn.fetchrow(
                "SELECT status,runtime_generation,agent_id,runtime_attach_token,"
                "metadata FROM threads WHERE id=$1 FOR UPDATE", located["owner_id"],
            )
            operation = await conn.fetchrow(
                "SELECT * FROM vm_idle_operations WHERE id=$1 FOR UPDATE",
                UUID(operation_id),
            )
            if thread is None or operation is None:
                return False
            if operation["thread_agent_stop_verified_at"] is not None:
                return _object(operation["thread_agent_stop_evidence"]) == dict(evidence)
            vm = _object(_object(thread["metadata"]).get("vm"))
            if (
                operation["release_kind"] != "pinned_thread"
                or operation["phase"] not in {"releasing", "release_held"}
                or thread["status"] != "suspended"
                or thread["runtime_generation"]
                    == operation["thread_runtime_generation"]
                or thread["agent_id"] is not None
                or thread["runtime_attach_token"] is not None
                or vm.get("_suspend_remote_io_closed") != operation_id
                or _uuid(vm.get("provision_generation"))
                    != operation["provision_generation"]
                or _uuid(vm.get("vm_uid")) != operation["vm_uid"]
                or _uuid(vm.get("rootdisk_pvc_uid")) != operation["pvc_uid"]
                or not await self._thread_retirement_outcome_on_conn(
                    conn, operation,
                )
                or dict(evidence) != {
                    "version": 1,
                    "pod": _object(operation["thread_agent_pod_identity"]),
                    "disposition": evidence.get("disposition"),
                    "retirement_token": str(operation["thread_retirement_token"]),
                    "controller_authenticated": True,
                }
                or evidence.get("disposition")
                    not in {"exact_absent", "replacement"}
            ):
                return False
            return bool(await conn.fetchval(
                "UPDATE vm_idle_operations SET "
                "thread_agent_stop_evidence=$2::jsonb,"
                "thread_agent_stop_verified_at=clock_timestamp() "
                "WHERE id=$1 AND thread_agent_stop_verified_at IS NULL "
                "RETURNING true",
                operation["id"], json.dumps(dict(evidence)),
            ))

    async def mark_thread_wake_ready(
        self, operation_id: str, *, generation: str, vm_uid: str,
        vmi_uid: str, launcher_uid: str, pvc_uid: str,
    ) -> bool:
        if any(_uuid(value) is None for value in (
            operation_id, generation, vm_uid, vmi_uid, launcher_uid, pvc_uid,
        )):
            return False
        async with self.db.acquire() as conn, conn.transaction():
            located = await conn.fetchrow(
                "SELECT owner_id FROM vm_idle_operations WHERE id=$1 "
                "AND owner_kind='thread'", UUID(operation_id),
            )
            if located is None:
                return False
            thread = await conn.fetchrow(
                "SELECT status,metadata,agent_id,runtime_attach_token,"
                "runtime_retirement_token,workspace_idle_revision,"
                "workspace_idle_episode FROM threads WHERE id=$1 FOR UPDATE",
                located["owner_id"],
            )
            operation = await conn.fetchrow(
                "SELECT * FROM vm_idle_operations WHERE id=$1 FOR UPDATE",
                UUID(operation_id),
            )
            if (
                thread is None or operation is None
                or thread["status"] != "suspended"
                or operation["phase"] not in {"waking", "wake_held"}
                or operation["thread_terminal_intent_at"] is not None
                or operation["stop_verified_at"] is None
                or not await self._thread_retirement_outcome_on_conn(
                    conn, operation,
                )
            ):
                return False
            vm = _object(_object(thread["metadata"]).get("vm"))
            if (
                vm.get("status") != "ready"
                or vm.get("identity_authenticated") is not True
                or vm.get("identity_provision_generation") != generation
                or vm.get("idle_wake_operation_id") != operation_id
                or vm.get("idle_wake_request_id")
                    != str(operation["wake_request_id"])
                or vm.get("idle_predecessor_pvc_uid")
                    != str(operation["pvc_uid"])
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
            ):
                return False
            episode = read_episode(
                _episode_document(thread["workspace_idle_episode"]),
                revision=thread["workspace_idle_revision"],
            )
            if (
                episode is None
                or episode.episode_id != str(operation["episode_id"])
                or episode.revision != operation["episode_revision"]
                or episode.wait_kind != "natural_pause"
            ):
                return False
            from shared.workspace_idle_store import apply_idle_transition_on_conn

            rebound = await apply_idle_transition_on_conn(
                conn,
                runtime=RuntimeIdentity(
                    "thread", str(operation["owner_id"]), "vm", generation, vm_uid,
                ),
                event="rebind", expected_revision=episode.revision,
                expected_episode_id=episode.episode_id,
            )
            if (
                rebound.episode is None
                or rebound.revision != episode.revision + 1
                or rebound.episode.episode_id != episode.episode_id
                or rebound.episode.wait_key != episode.wait_key
                or rebound.episode.entered_at != episode.entered_at
            ):
                raise IdlePolicyError("thread_wake_episode_changed")
            # The predecessor's remote-I/O closure belongs only to its
            # stopped generation. Reopen it in the same exact Ready/rebind
            # transaction; otherwise the successor can be Ready yet every
            # genuine input and natural-pause source remains fenced.
            if vm.get("_suspend_remote_io_closed") == operation_id:
                vm.pop("_suspend_remote_io_closed", None)
                metadata = _object(thread["metadata"])
                metadata["vm"] = vm
                await conn.execute(
                    "UPDATE threads SET metadata=$2::jsonb WHERE id=$1",
                    operation["owner_id"], json.dumps(metadata),
                )
            await conn.execute(
                "UPDATE vm_idle_operations SET wake_ready_at=clock_timestamp(),"
                "thread_wake_ready_identity=$2::jsonb,"
                "last_progress_at=clock_timestamp(),retry_after=NULL "
                "WHERE id=$1 AND wake_ready_at IS NULL", operation["id"],
                json.dumps({
                    "generation": generation, "vm_uid": vm_uid,
                    "vmi_uid": vmi_uid, "launcher_uid": launcher_uid,
                    "pvc_uid": pvc_uid,
                }),
            )
            await conn.execute(
                "UPDATE vm_idle_access_leases SET "
                "provision_generation=$2,vm_uid=$3,"
                "expires_at=LEAST(max_expires_at,"
                "clock_timestamp()+interval '2 minutes') "
                "WHERE owner_kind='thread' AND owner_id=$1 AND wake_id=$4 "
                "AND closed_at IS NULL AND max_expires_at>clock_timestamp()",
                operation["owner_id"], UUID(generation), UUID(vm_uid),
                operation["wake_id"],
            )
            return True

    async def finish_thread_wake(self, operation_id: str) -> bool:
        if _uuid(operation_id) is None:
            return False
        async with self.db.acquire() as conn, conn.transaction():
            located = await conn.fetchrow(
                "SELECT owner_id FROM vm_idle_operations WHERE id=$1 "
                "AND owner_kind='thread'", UUID(operation_id),
            )
            if located is None:
                return False
            thread = await conn.fetchrow(
                "SELECT status,metadata,agent_id,runtime_attach_token,"
                "runtime_retirement_token,workspace_idle_revision,"
                "workspace_idle_episode FROM threads WHERE id=$1 FOR UPDATE",
                located["owner_id"],
            )
            operation = await conn.fetchrow(
                "SELECT * FROM vm_idle_operations WHERE id=$1 FOR UPDATE",
                UUID(operation_id),
            )
            if thread is None or operation is None:
                return False
            if operation["phase"] == "ready":
                return True
            vm = _object(_object(thread["metadata"]).get("vm"))
            episode = read_episode(
                _episode_document(thread["workspace_idle_episode"]),
                revision=thread["workspace_idle_revision"],
            )
            if (
                thread["status"] not in (
                    {"suspended", "created"}
                    if operation["wake_execution_requested"] else {"suspended"}
                )
                or operation["phase"] not in {"waking", "wake_held"}
                or operation["thread_terminal_intent_at"] is not None
                or operation["wake_ready_at"] is None
                or vm.get("status") != "ready"
                or vm.get("identity_authenticated") is not True
                or vm.get("identity_provision_generation")
                    != str(operation["wake_generation"])
                or vm.get("idle_wake_operation_id") != operation_id
                or vm.get("idle_wake_request_id")
                    != str(operation["wake_request_id"])
                or vm.get("idle_predecessor_pvc_uid")
                    != str(operation["pvc_uid"])
                or _uuid(vm.get("provision_generation"))
                    != operation["wake_generation"]
                or _uuid(vm.get("rootdisk_pvc_uid")) != operation["pvc_uid"]
                or episode is None
                or episode.episode_id != str(operation["episode_id"])
                or episode.revision != operation["episode_revision"] + 1
                or episode.runtime_identity.runtime_generation
                    != str(operation["wake_generation"])
                or episode.runtime_identity.runtime_uid != vm.get("vm_uid")
                or _object(operation["thread_wake_ready_identity"]) != {
                    "generation": str(operation["wake_generation"]),
                    "vm_uid": vm.get("vm_uid"),
                    "vmi_uid": vm.get("vmi_uid"),
                    "launcher_uid": vm.get("active_pod_uid"),
                    "pvc_uid": str(operation["pvc_uid"]),
                }
            ):
                return False
            if operation["wake_execution_requested"]:
                if thread["status"] == "suspended":
                    changed = await conn.execute(
                        "UPDATE threads SET status='created' WHERE id=$1 "
                        "AND status='suspended' AND agent_id IS NULL "
                        "AND runtime_retirement_token IS NULL",
                        operation["owner_id"],
                    )
                    # Keep the fixed wake open: a crash here must replay the
                    # authorized fresh-agent preparation after VM Ready.
                    if changed != "UPDATE 1":
                        return False
                    return False
                pod = _object(_object(thread["metadata"]).get("agent_pod"))
                agent = await conn.fetchrow(
                    "SELECT thread_id,hostname,pod_uid,status FROM agents "
                    "WHERE id=$1 FOR SHARE", thread["agent_id"],
                ) if thread.get("agent_id") is not None else None
                if (
                    thread["runtime_retirement_token"] is not None
                    or thread["runtime_attach_token"] is None
                    or agent is None
                    or agent["thread_id"] != operation["owner_id"]
                    or agent["status"] not in {"ready", "working", "session"}
                    or pod.get("pod_name") != agent["hostname"]
                    or pod.get("pod_uid") != agent["pod_uid"]
                    or pod.get("protection_protocol") != "finalizer_v1"
                    or pod.get("pod_uid") == _object(
                        operation["thread_agent_pod_identity"]
                    ).get("pod_uid")
                ):
                    return False
            await conn.execute(
                "UPDATE vm_idle_operations SET phase='ready',"
                "closed_at=clock_timestamp(),last_progress_at=clock_timestamp() "
                "WHERE id=$1", operation["id"],
            )
            return True

    async def close_thread_terminal_after_delete(self, operation_id: str) -> bool:
        """Close the retained soft row only after separate permanent End deleted T."""
        if _uuid(operation_id) is None:
            return False
        async with self.db.acquire() as conn, conn.transaction():
            operation = await conn.fetchrow(
                "SELECT * FROM vm_idle_operations WHERE id=$1 FOR UPDATE",
                UUID(operation_id),
            )
            if (
                operation is None or operation["release_kind"] != "pinned_thread"
                or operation["phase"] not in {
                    "suspended", "waking", "wake_held",
                }
                or operation["thread_terminal_intent_at"] is None
                or operation["stop_verified_at"] is None
                or operation["thread_agent_stop_verified_at"] is None
                or await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM threads WHERE id=$1)",
                    operation["owner_id"],
                )
            ):
                return False
            return bool(await conn.fetchval(
                "UPDATE vm_idle_operations SET phase='superseded',"
                "closed_at=clock_timestamp(),last_progress_at=clock_timestamp() "
                "WHERE id=$1 AND closed_at IS NULL RETURNING true",
                operation["id"],
            ))

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
            existing_proof = _object(operation["access_rebind_proof"])
            if existing_proof:
                return bool(
                    episode is not None
                    and episode.episode_id == str(operation["episode_id"])
                    and episode.revision == operation["episode_revision"] + 1
                    and existing_proof.get("successor") == {
                        "generation": generation, "vm_uid": vm_uid,
                        "vmi_uid": vmi_uid, "launcher_uid": launcher_uid,
                        "pvc_uid": pvc_uid,
                    }
                )
            episode_changed = bool(
                episode is None
                or episode.episode_id != str(operation["episode_id"])
                or episode.revision != operation["episode_revision"]
            )
            anchor = None
            if not operation["wake_execution_requested"] and not episode_changed:
                prior = await conn.fetchrow(
                    "SELECT * FROM vm_idle_operations WHERE owner_kind='job' "
                    "AND owner_id=$1 AND episode_id=$2 AND id<>$3 "
                    "AND episode_revision<=$4 ORDER BY episode_revision DESC,id DESC LIMIT 1",
                    owner_id, operation["episode_id"], operation["id"],
                    operation["episode_revision"],
                )
                if prior is None:
                    anchor = (str(operation["id"]), operation["episode_revision"], 1)
                else:
                    prior_proof = _object(prior["access_rebind_proof"])
                    if (
                        prior["phase"] != "ready" or prior["closed_at"] is None
                        or prior["post_ready_resume_requested"]
                        or prior["episode_revision"] != operation["episode_revision"] - 1
                        or _object(prior_proof.get("successor")) != {
                            "generation": str(operation["provision_generation"]),
                            "vm_uid": str(operation["vm_uid"]),
                            "vmi_uid": str(operation["vmi_uid"]),
                            "launcher_uid": str(operation["launcher_uid"]),
                            "pvc_uid": str(operation["pvc_uid"]),
                        }
                        or type(prior_proof.get("chain_length")) is not int
                        or prior_proof["chain_length"] < 1
                    ):
                        return False
                    anchor = (
                        prior_proof.get("root_operation_id"),
                        prior_proof.get("root_revision"),
                        prior_proof["chain_length"] + 1,
                    )
            ready = await conn.fetchrow(
                "UPDATE vm_idle_operations SET wake_ready_at=clock_timestamp(),"
                "wake_execution_requested=wake_execution_requested OR $2,"
                "retry_after=NULL,last_progress_at=clock_timestamp() WHERE id=$1 "
                "RETURNING wake_ready_at",
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

                rebound = await apply_idle_transition_on_conn(
                    conn,
                    runtime=RuntimeIdentity(
                        "job", str(owner_id), "vm", generation, vm_uid,
                    ),
                    event="rebind",
                    expected_revision=episode.revision,
                    expected_episode_id=episode.episode_id,
                )
                if (
                    rebound.episode is None
                    or rebound.revision != episode.revision + 1
                    or rebound.episode.episode_id != episode.episode_id
                    or rebound.episode.wait_key != episode.wait_key
                    or rebound.episode.wait_kind != episode.wait_kind
                    or rebound.episode.entered_at != episode.entered_at
                ):
                    raise IdlePolicyError("episode_changed")
                proof = {
                    "version": 1,
                    "operation_id": operation_id,
                    "episode_id": episode.episode_id,
                    "wait_kind": episode.wait_kind,
                    "wait_key": episode.wait_key,
                    "entered_at": episode.entered_at.isoformat(),
                    "from_revision": episode.revision,
                    "to_revision": rebound.revision,
                    "root_operation_id": anchor[0],
                    "root_revision": anchor[1],
                    "chain_length": anchor[2],
                    "wake_id": str(operation["wake_id"]),
                    "stop_verified_at": operation["stop_verified_at"].isoformat(),
                    "ready_at": ready["wake_ready_at"].isoformat(),
                    "predecessor": {
                        "generation": str(operation["provision_generation"]),
                        "vm_uid": str(operation["vm_uid"]),
                        "vmi_uid": str(operation["vmi_uid"]),
                        "launcher_uid": str(operation["launcher_uid"]),
                        "pvc_uid": str(operation["pvc_uid"]),
                    },
                    "successor": {
                        "generation": generation,
                        "vm_uid": vm_uid,
                        "vmi_uid": vmi_uid,
                        "launcher_uid": launcher_uid,
                        "pvc_uid": pvc_uid,
                    },
                }
                await conn.execute(
                    "UPDATE vm_idle_operations SET access_rebind_proof=$2::jsonb "
                    "WHERE id=$1 AND access_rebind_proof IS NULL",
                    operation["id"], json.dumps(proof),
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
                        vmi_uid=operation["vmi_uid"], pvc_uid=operation["pvc_uid"],
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
                    or operation["post_ready_resume_requested"]
                    or episode is None
                    or episode.episode_id != str(operation["episode_id"])
                )
                if not execute or operation["post_ready_resume_requested"]:
                    proof = _object(operation["access_rebind_proof"])
                    if (
                        proof.get("version") != 1
                        or proof.get("episode_id") != episode.episode_id
                        or proof.get("wait_key") != episode.wait_key
                        or proof.get("wait_kind") != episode.wait_kind
                        or proof.get("entered_at") != episode.entered_at.isoformat()
                        or proof.get("to_revision") != episode.revision
                        or _object(proof.get("successor")) != {
                            "generation": vm.get("provision_generation"),
                            "vm_uid": vm.get("vm_uid"),
                            "vmi_uid": vm.get("vmi_uid"),
                            "launcher_uid": vm.get("active_pod_uid"),
                            "pvc_uid": vm.get("rootdisk_pvc_uid"),
                        }
                    ):
                        return False
                if execute:
                    from orchestrator.database.postgres import _stateless_resume_context
                    from shared.worker_queue import (
                        cancel_queued_worker_batch,
                        enqueue_worker_batch_wake,
                        reset_worker_batch_attempts,
                    )
                    from shared.run_queue import unpark_unit
                    if _release_kind(operation) == "pinned_job":
                        if (
                            operation["pinned_stop_verified_at"] is None
                            or operation["stop_verified_at"] is None
                            or job["execution_lane"] != "pinned"
                            or job["assigned_agent_id"] != operation["pinned_agent_id"]
                            or await conn.fetchval(
                                "SELECT 1 FROM run_queue WHERE unit_id=$1", owner_id,
                            ) is not None
                        ):
                            raise _WakeChanged
                        # Close the old physical owner before the paused write.
                        # Both writes are one transaction, so a failed CAS
                        # rolls the closure back and the old fence remains.
                        await conn.execute(
                            "UPDATE vm_idle_operations SET phase='ready',"
                            "closed_at=clock_timestamp(),last_progress_at=clock_timestamp() "
                            "WHERE id=$1", operation["id"],
                        )
                        updated = await self.db._queue_job_for_resume_on_conn(
                            conn, owner_id, None, void_completion_decision=True,
                            expected_status=job["status"],
                            completion_commands_enabled=True,
                        )
                    else:
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
                    "last_progress_at=clock_timestamp() WHERE id=$1 AND closed_at IS NULL",
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
                SELECT j.id FROM jobs j LEFT JOIN run_queue q ON q.unit_id=j.id
                WHERE (
                    (j.execution_lane='stateless' AND j.assigned_agent_id IS NULL
                     AND q.unit_kind='worker_batch' AND q.state IN ('done','parked'))
                    OR (j.execution_lane='pinned' AND j.assigned_agent_id IS NOT NULL
                        AND q.unit_id IS NULL)
                  )
                  AND j.workspace_idle_episode IS NOT NULL
                  AND j.status IN ('waiting_for_reply','pending_review')
                  AND j.context->'vm'->>'status'='ready'
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

    async def due_thread_ids(
        self, *, limit: int = 16, after_id: UUID | None = None,
    ) -> list[str]:
        if not 1 <= limit <= 64:
            raise ValueError("Invalid idle scan limit")
        async with self.db.acquire() as conn:
            query = """
                SELECT t.id FROM threads t
                WHERE t.execution_lane='pinned' AND t.status='awaiting_user'
                  AND t.workspace_idle_episode IS NOT NULL
                  AND t.metadata->'vm'->>'status'='ready'
                  AND t.runtime_retirement_token IS NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM vm_idle_operations o
                    WHERE o.owner_kind='thread' AND o.owner_id=t.id
                      AND o.closed_at IS NULL)
                  AND ($2::uuid IS NULL OR t.id>$2)
                ORDER BY t.id LIMIT $1
            """
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
                  AND (phase<>'suspended' OR wake_requested OR
                       to_jsonb(vm_idle_operations)->>'thread_terminal_intent_at'
                       IS NOT NULL)
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

    async def get_open_for_thread(self, thread_id: str) -> dict[str, Any] | None:
        owner_id = _uuid(thread_id)
        if owner_id is None:
            return None
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM vm_idle_operations WHERE owner_kind='thread' "
                "AND owner_id=$1 AND closed_at IS NULL", owner_id,
            )
            return dict(row) if row else None

    async def get_pending_access_continuation(
        self, thread_id: str,
    ) -> dict[str, Any] | None:
        owner_id = _uuid(thread_id)
        if owner_id is None:
            return None
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT c.operation_id,o.wake_id FROM "
                "vm_idle_thread_access_continuations c "
                "JOIN vm_idle_operations o ON o.id=c.operation_id "
                "WHERE c.thread_id=$1 AND c.prepared_at IS NULL",
                owner_id,
            )
            return dict(row) if row else None

    async def pending_access_continuations(
        self, *, limit: int = 16, after_id: UUID | None = None,
    ) -> list[dict[str, Any]]:
        if not 1 <= limit <= 64:
            raise ValueError("Invalid idle scan limit")
        async with self.db.acquire() as conn:
            query = (
                "SELECT c.operation_id,c.thread_id FROM "
                "vm_idle_thread_access_continuations c "
                "JOIN threads t ON t.id=c.thread_id "
                "WHERE c.prepared_at IS NULL AND t.status='created' "
                "AND t.runtime_retirement_token IS NULL "
                "AND ($2::uuid IS NULL OR c.operation_id>$2) "
                "ORDER BY c.operation_id LIMIT $1"
            )
            rows = await conn.fetch(query, limit, after_id)
            if not rows and after_id is not None:
                rows = await conn.fetch(query, limit, None)
            return [dict(row) for row in rows]

    async def complete_access_continuation(self, operation_id: str) -> bool:
        source_id = _uuid(operation_id)
        if source_id is None:
            return False
        async with self.db.acquire() as conn, conn.transaction():
            owner_id = await conn.fetchval(
                "SELECT thread_id FROM vm_idle_thread_access_continuations "
                "WHERE operation_id=$1", source_id,
            )
            if owner_id is None:
                return False
            thread = await conn.fetchrow(
                "SELECT id FROM threads WHERE id=$1 FOR UPDATE", owner_id,
            )
            if thread is None:
                return False
            operation = await conn.fetchrow(
                "SELECT id FROM vm_idle_operations WHERE id=$1 FOR UPDATE",
                source_id,
            )
            if operation is None:
                return False
            return bool(await conn.fetchval(
                "UPDATE vm_idle_thread_access_continuations SET "
                "prepared_at=clock_timestamp() WHERE operation_id=$1 "
                "AND thread_id=$2 AND prepared_at IS NULL RETURNING true",
                source_id, owner_id,
            ))

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
    """Bounded, replayable Job and pinned-session VM idle adapter."""

    def __init__(self, db: Any, provisioner: Any, recovery_store: Any, *,
                 claimant: str = "vm-idle", before_first_start: Any = None,
                 terminal_publication_handler: Any = None,
                 agent_provisioner: Any = None,
                 thread_retirement: Any = None,
                 thread_workspace_suspension: Any = None,
                 thread_prepare: Any = None) -> None:
        self.store = VMIdleLifecycleStore(db)
        self.db = db
        self.provisioner = provisioner
        self.recovery_store = recovery_store
        self.claimant = claimant
        self.before_first_start = before_first_start
        self.terminal_publication_handler = terminal_publication_handler
        self.agent_provisioner = agent_provisioner
        self.thread_retirement = thread_retirement
        self.thread_workspace_suspension = thread_workspace_suspension
        self.thread_prepare = thread_prepare
        # Selection is process-local, while operation claims and admission are
        # database-authoritative. The long-lived sweeper advances these cursors
        # even when a whole page is ineligible, then wraps at the end.
        self._nomination_cursor: UUID | None = None
        self._thread_nomination_cursor: UUID | None = None
        self._operation_cursor: UUID | None = None
        self._continuation_cursor: UUID | None = None

    async def nominate(self, *, limit: int = 16) -> int:
        if (
            os.getenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false").lower() != "true"
            or not vm_remote_operation_protocol_enabled()
            or not vm_persistent_rootdisk_enabled()
            or self.provisioner.mode != "same-cluster"
            or not self.provisioner.lifecycle_available
        ):
            return 0
        admitted = 0
        due = (
            await self.store.due_job_ids(
                limit=limit, after_id=self._nomination_cursor,
            )
            if os.getenv("VM_CREATION_RETRY_ENABLED", "false").lower() == "true"
            else []
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
        if self.thread_retirement is not None:
            due_threads = await self.store.due_thread_ids(
                limit=limit, after_id=self._thread_nomination_cursor,
            )
            self._thread_nomination_cursor = (
                UUID(due_threads[-1]) if due_threads else None
            )
            for thread_id in due_threads:
                try:
                    thread = await self.db.get_thread(thread_id)
                    if not isinstance(thread, dict):
                        continue
                    episode = read_episode(
                        _episode_document(thread.get("workspace_idle_episode")),
                        revision=thread.get("workspace_idle_revision"),
                    )
                    if episode is None or episode.wait_kind != "natural_pause":
                        continue
                    identity = await self.provisioner.capture_vm_teardown_identity(
                        thread_id, entity_type="thread",
                    )
                    attestation = await self.provisioner.attest_workspace_runtime(
                        thread_id, entity_type="thread",
                    )
                    exact = {
                        "generation": identity.provision_generation,
                        "vm_uid": identity.vm_uid,
                        "vmi_uid": attestation.vmi_uid,
                        "launcher_uid": attestation.launcher_pod_uid,
                        "pvc_uid": identity.rootdisk_pvc_uid,
                    }
                    if (
                        identity.provision_generation
                            != attestation.workspace_generation
                        or identity.vm_uid != attestation.vm_uid
                        or identity.rootdisk_pvc_uid
                            != attestation.rootdisk_pvc_uid
                        or not all(exact.values())
                        or await self.thread_retirement.thread_turn_in_flight(thread)
                    ):
                        continue
                    if await self.store.admit_thread_release(
                        thread_id, episode_id=episode.episode_id,
                        revision=episode.revision, identity=exact,
                        turn_quiescent=True,
                    ):
                        admitted += 1
                except Exception:
                    logger.exception("VM idle admission held for thread %s", thread_id)
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
                    elif (
                        _release_kind(operation) == "pinned_thread"
                        and operation["phase"] in {
                            "suspended", "waking", "wake_held",
                        }
                        and operation["thread_terminal_intent_at"] is not None
                    ):
                        if await asyncio.wait_for(
                            self._finish_thread_terminal(
                                operation, current=current,
                            ), timeout=300,
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
        if self.thread_prepare is not None:
            continuations = await self.store.pending_access_continuations(
                limit=min(limit, 4), after_id=self._continuation_cursor,
            )
            self._continuation_cursor = (
                continuations[-1]["operation_id"] if continuations else None
            )
            for continuation in continuations:
                operation_id = str(continuation["operation_id"])
                try:
                    if await asyncio.wait_for(
                        self.thread_prepare(
                            str(continuation["thread_id"]), operation_id,
                        ), timeout=300,
                    ) and await self.store.complete_access_continuation(
                        operation_id,
                    ):
                        advanced += 1
                except Exception:
                    logger.exception(
                        "Pinned access execution continuation %s held",
                        operation_id,
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

    async def _stop_pinned_agent(
        self, operation: Mapping[str, Any], *, current: Any,
    ) -> bool:
        """Stop only the captured agent Pod, then append its Job-specific proof."""
        actor = self.agent_provisioner
        if actor is None or not actor.is_available or not current():
            return False
        fresh = await self.store.get_operation(str(operation["id"]))
        if (
            fresh is None or fresh["claim_token"] != operation["claim_token"]
            or fresh["claimed_by"] != self.claimant
            or fresh["phase"] not in {"releasing", "release_held"}
        ):
            return False
        if fresh["pinned_stop_verified_at"] is not None:
            return await self.store.pinned_stop_valid(
                str(operation["id"]), token=operation["claim_token"],
                claimant=self.claimant,
            )
        agent = await self.db.fetchrow(
            "SELECT id,hostname,pod_uid,pod_ip,pod_port,current_job_id,metadata "
            "FROM agents WHERE id=$1", operation["pinned_agent_id"],
        )
        job = await self.db.fetchrow(
            "SELECT status,assigned_agent_id,context FROM jobs WHERE id=$1",
            operation["owner_id"],
        )
        terminal_detached = bool(
            job is not None
            and operation["terminal_source_command_id"] is not None
            and job["status"] == "completed"
            and job["assigned_agent_id"] is None
        )
        if (
            agent is None or job is None
            or (job["assigned_agent_id"] != operation["pinned_agent_id"]
                and not terminal_detached)
            or _object(job["context"]).get("_workspace_dispatch_authority")
                != _object(operation["pinned_original_dispatch_marker"])
            or agent["hostname"] != operation["pinned_agent_pod_name"]
            or agent["pod_uid"] != operation["pinned_agent_pod_uid"]
            or _object(agent["metadata"]).get("dispatch_process_generation")
                != operation["pinned_process_generation"]
            or agent["current_job_id"] not in {None, operation["owner_id"]}
        ):
            return False
        name = operation["pinned_agent_pod_name"]
        uid = operation["pinned_agent_pod_uid"]
        namespace = operation["pinned_agent_pod_namespace"]
        try:
            state, pod = await actor.observe_agent_pod_exact(
                name, expected_pod_uid=uid, namespace=namespace,
            )
        except Exception:
            return False
        if state not in {"exact_live", "exact_terminal", "exact_absent", "replacement"}:
            return False
        if state == "exact_terminal" and not pod_containers_are_terminal(pod):
            return False
        # A 200 is cooperative quiescence only, never physical stop. A lost
        # response or timeout does not change the immutable Pod target.
        if state == "exact_live" and agent["current_job_id"] == operation["owner_id"]:
            ip, port = agent["pod_ip"], agent["pod_port"]
            if ip and port and await actor.attest_pinned_job_recipient(
                name, expected_pod_uid=uid, expected_pod_ip=ip,
            ):
                recipient = PinnedJobRecipient(
                    expected_agent_id=str(agent["id"]),
                    expected_pod_uid=uid,
                    expected_process_generation=operation["pinned_process_generation"],
                    expected_job_id=str(operation["owner_id"]),
                )
                try:
                    async with httpx.AsyncClient(timeout=125.0) as client:
                        await client.post(
                            f"http://{ip}:{port}/job/pause",
                            json={"recipient": recipient.model_dump(mode="json")},
                        )
                except Exception:
                    pass
        if not current():
            return False
        if state == "exact_live":
            if not await actor.delete_agent_pod_exact(
                name, expected_pod_uid=uid, namespace=namespace,
            ):
                return False
        terminal_seen = fresh["pinned_terminal_observed_at"] is not None
        for _ in range(8):
            if not current():
                return False
            try:
                state, pod = await actor.observe_agent_pod_exact(
                    name, expected_pod_uid=uid, namespace=namespace,
                )
            except Exception:
                return False
            if state == "exact_terminal":
                if not pod_containers_are_terminal(pod):
                    return False
                if not await self.store.record_pinned_terminal(
                    operation, claimant=self.claimant,
                ):
                    return False
                terminal_seen = True
                break
            if state in {"exact_absent", "replacement"}:
                break
            await asyncio.sleep(0.25)
        if not terminal_seen:
            return False
        if state == "exact_terminal" and not await actor.release_agent_pod_finalizer_exact(
            name, expected_pod_uid=uid, namespace=namespace,
            terminal_required=True,
        ):
            return False
        for _ in range(8):
            if not current():
                return False
            try:
                state, _ = await actor.observe_agent_pod_exact(
                    name, expected_pod_uid=uid, namespace=namespace,
                )
            except Exception:
                return False
            if state in {"exact_absent", "replacement"}:
                return await self.store.record_pinned_stop(
                    operation, claimant=self.claimant, absence=state,
                )
            await asyncio.sleep(0.25)
        return False

    async def _release(self, operation: Mapping[str, Any], *, current: Any) -> bool:
        if _release_kind(operation) == "pinned_thread":
            return await self._release_thread(operation, current=current)
        if _release_kind(operation) == "pinned_job":
            if not await self._stop_pinned_agent(operation, current=current):
                await self.store.hold(
                    str(operation["id"]), token=operation["claim_token"],
                    claimant=self.claimant, reason="pinned_agent_stop_unproven",
                )
                return False
            if not await self.store.pinned_stop_valid(
                str(operation["id"]), token=operation["claim_token"],
                claimant=self.claimant,
            ):
                await self.store.hold(
                    str(operation["id"]), token=operation["claim_token"],
                    claimant=self.claimant, reason="pinned_agent_stop_receipt_changed",
                )
                return False
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

    async def _release_thread(
        self, operation: Mapping[str, Any], *, current: Any,
    ) -> bool:
        """Replay the exact existing soft-retirement funnel before VM stop proof."""
        if self.thread_retirement is None or not current():
            return False
        owner_id = str(operation["owner_id"])
        thread = await self.db.get_thread(owner_id)
        if not isinstance(thread, dict):
            return False
        if thread.get("status") != "suspended":
            if (
                str(thread.get("runtime_generation"))
                    != str(operation["thread_runtime_generation"])
                or str(thread.get("runtime_retirement_token"))
                    != str(operation["thread_retirement_token"])
                or thread.get("runtime_retirement_authorized_at") is None
            ):
                return False
            context = _object(thread.get("runtime_retirement_context"))
            result = await self.thread_retirement.end_thread_flow(
                owner_id, thread, permanent=False, force=False,
                settle_status="suspended",
                require_physical_agent_stop=True,
                expected_runtime_generation=str(
                    operation["thread_runtime_generation"]
                ),
                expected_agent_id=str(context.get("agent_id") or ""),
                expected_attach_token=str(
                    context.get("runtime_attach_token") or ""
                ),
            )
            if result.get("status") != "suspended":
                return False
        if not current():
            return False
        if operation["thread_agent_stop_verified_at"] is None:
            pod = _object(operation["thread_agent_pod_identity"])
            if not all(pod.get(key) for key in (
                "pod_name", "pod_uid", "namespace",
            )) or pod.get("protection_protocol") != "finalizer_v1":
                return False
            # A crash after the retirement funnel settled but before this
            # receipt may be replayed from the immutable captured Pod tuple.
            # The exact-UID helper is idempotent and refuses a live unknown.
            pinned = self.thread_retirement.dependencies.pinned_retirement
            await pinned.stop_captured_retirement_agent(
                {"context": {"agent_pod": pod}},
            )
            if not current():
                return False
            observed = await pinned.wait_for_captured_agent_pod_retired(
                pod["pod_name"], pod["pod_uid"],
                namespace=pod["namespace"],
                allowed={"exact_absent", "replacement"},
            )
            if observed not in {"exact_absent", "replacement"}:
                return False
            if not await self.store.record_thread_agent_stop(
                str(operation["id"]), evidence={
                    "version": 1, "pod": pod, "disposition": observed,
                    "retirement_token": str(operation["thread_retirement_token"]),
                    "controller_authenticated": True,
                },
            ):
                return False
        evidence = await self.provisioner.attest_vm_idle_stop(operation)
        if evidence is None or not current():
            return False
        return await self.store.complete_release(
            str(operation["id"]), evidence=evidence,
        )

    async def _wake_thread(
        self, operation: Mapping[str, Any], *, current: Any,
    ) -> bool:
        owner_id = str(operation["owner_id"])
        operation_id = str(operation["id"])
        if (
            not operation["wake_requested"] or not current()
            or operation["thread_terminal_intent_at"] is not None
        ):
            return False
        if operation["phase"] == "suspended":
            operation = await self.store.request_thread_wake(
                owner_id,
                execution_requested=bool(operation["wake_execution_requested"]),
            )
            if operation is None:
                return False
        if operation["wake_generation"] is None or operation["wake_id"] is None:
            return False
        if operation["wake_ready_at"] is not None:
            if not operation["wake_execution_requested"]:
                return await self.store.finish_thread_wake(operation_id)
            fresh = await self.db.get_thread(owner_id)
            if not isinstance(fresh, dict):
                return False
            if fresh.get("status") == "suspended":
                await self.store.finish_thread_wake(operation_id)
                fresh = await self.db.get_thread(owner_id)
            if not isinstance(fresh, dict) or fresh.get("status") != "created":
                return False
            # The first finalization CAS opens the fresh runtime only after
            # exact Ready. Keep the wake open until the existing prepare path
            # has produced an authoritative new agent and route. A crash here
            # replays the same fixed wake rather than stranding `created`.
            if self.thread_prepare is None or not current():
                return False
            prepared = await self.thread_prepare(owner_id, operation_id)
            if not prepared:
                if current() and operation.get("claim_token") is not None:
                    await self.store.hold(
                        operation_id, token=operation["claim_token"],
                        claimant=self.claimant,
                        reason="fresh_agent_prepare_pending", seconds=60,
                    )
                return False
            if not current():
                return False
            return await self.store.finish_thread_wake(operation_id)
        thread = await self.db.get_thread(owner_id)
        if not isinstance(thread, dict) or thread.get("status") != "suspended":
            return False
        vm = _object(_object(thread.get("metadata")).get("vm"))
        generation = str(operation["wake_generation"])
        if vm.get("provision_generation") == generation:
            if vm.get("status") != "ready":
                return False
            attested = await self.provisioner.attest_workspace_runtime(
                owner_id, entity_type="thread",
            )
            if (
                attested.workspace_generation != generation
                or attested.vm_uid != vm.get("vm_uid")
                or attested.vmi_uid != vm.get("vmi_uid")
                or attested.launcher_pod_uid != vm.get("active_pod_uid")
                or attested.rootdisk_pvc_uid != str(operation["pvc_uid"])
                or not current()
            ):
                return False
            return await self.store.mark_thread_wake_ready(
                operation_id, generation=generation,
                vm_uid=attested.vm_uid, vmi_uid=attested.vmi_uid,
                launcher_uid=attested.launcher_pod_uid,
                pvc_uid=attested.rootdisk_pvc_uid,
            )
        if (
            vm.get("status") != "suspended"
            or vm.get("rootdisk") != "kept"
            or vm.get("provision_generation")
                != str(operation["provision_generation"])
            or vm.get("rootdisk_pvc_uid") != str(operation["pvc_uid"])
            or operation["stop_verified_at"] is None
            or self.thread_workspace_suspension is None
        ):
            return False
        # Restore uses the existing VM create path with the operation's fixed
        # generation/request. Its DB CAS runs before controller I/O; a lost
        # response waits on that one generation instead of issuing another.
        return bool(await self.thread_workspace_suspension.restore_thread_workspace(
            owner_id, wake_operation_id=operation_id,
        ))

    async def _finish_thread_terminal(
        self, operation: Mapping[str, Any], *, current: Any,
    ) -> bool:
        if self.thread_retirement is None or not current():
            return False
        owner_id = str(operation["owner_id"])
        thread = await self.db.get_thread(owner_id)
        if isinstance(thread, dict):
            if thread.get("status") not in {"suspended", "created"}:
                return False
            source = await self.db.reserve_pinned_thread_idle_terminal_end(
                owner_id,
            )
            if source != "ready_for_destructive_retirement":
                return False
            vm = _object(_object(thread.get("metadata")).get("vm"))
            permanent_replay = bool(
                thread.get("runtime_retirement_token") is not None
                and thread.get("runtime_retirement_permanent") is True
                and thread.get("runtime_retirement_authorized_at") is not None
            )
            if (
                not permanent_replay
                and vm.get("provision_generation")
                    == str(operation["wake_generation"])
            ):
                attested = await self.provisioner.attest_workspace_runtime(
                    owner_id, entity_type="thread",
                )
                if (
                    not current()
                    or attested.workspace_generation
                        != str(operation["wake_generation"])
                    or attested.vm_uid != vm.get("vm_uid")
                    or attested.vmi_uid != vm.get("vmi_uid")
                    or attested.launcher_pod_uid != vm.get("active_pod_uid")
                    or attested.rootdisk_pvc_uid != str(operation["pvc_uid"])
                ):
                    return False
            result = await self.thread_retirement.end_thread_flow(
                owner_id, thread, permanent=True, force=False,
                settle_status="ended",
            )
            if result.get("status") != "deleted" or not current():
                return False
        return await self.store.close_thread_terminal_after_delete(
            str(operation["id"]),
        )

    async def _wake(self, operation: Mapping[str, Any], *, current: Any) -> bool:
        if _release_kind(operation) == "pinned_thread":
            return await self._wake_thread(operation, current=current)
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
