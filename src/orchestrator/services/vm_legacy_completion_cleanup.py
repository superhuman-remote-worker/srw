"""Transfer an unissued legacy completion claim to exact ordinary VM cleanup."""

from __future__ import annotations

import json
from uuid import UUID

from shared.vm_resource_admission import ResourceAdmissionError
from shared.vm_creation_retry import canonical_request_digest
from shared.vm_creation_issuance import canonical_configuration_digest


def _document(value):
    return json.loads(value) if isinstance(value, str) else value


def routes_terminal_vm_to_archive(job):
    """Select the VM backend only; archive retains every mutation guard."""
    if not job:
        return False
    context = _document(job.get("context")) or {}
    vm = context.get("vm") if isinstance(context, dict) else None
    return bool(
        job.get("status") in {"failed", "completed"}
        and job.get("execution_lane") == "stateless"
        and job.get("assigned_agent_id") is None
        and job.get("parent_job_id") is None
        and isinstance(vm, dict)
        and vm
        and context.get("inherits_parent_workspace") in (None, False)
        and context.get("workspace_container") in (None, {})
        and context.get("ide_session") in (None, {})
    )


def pure_terminal_vm(job):
    """Strict eligibility for historical, unissued legacy claim supersession."""
    if not routes_terminal_vm_to_archive(job):
        return False
    context = _document(job["context"])
    vm = context["vm"]
    return bool(
        vm.get("status") == "ready"
        and vm.get("identity_authenticated") is True
        and vm.get("workspace_storage") is None
        and not any(
            key in context
            for key in (
                "_vm_job_retained_resume",
                "_vm_creation_pending",
                "_stateless_resume_pending",
                "_stateless_cancel_cleanup_pending",
                "last_vm",
            )
        )
    )


async def supersede_unissued_legacy_completion(recovery_store, *, owner_id, identity):
    """Close only a logical no-effect claim and admit its successor atomically.

    The outcome names the deterministic successor request, preserving the old
    immutable intent. It is not a stop receipt or resource release. Every refusal
    rolls back the supersession; no database lock spans external I/O.
    """
    from orchestrator.services.completion_teardown_replay import (
        legacy_completion_cleanup_identity,
    )
    from orchestrator.services.vm_workspace_recovery_store import (
        acquire_vm_cleanup_permit,
        vm_cleanup_request_identity,
    )

    owner = UUID(str(owner_id))
    generation = UUID(identity.provision_generation)
    vm_uid, pvc_uid = UUID(identity.vm_uid), UUID(identity.rootdisk_pvc_uid)
    legacy_request, legacy_digest = legacy_completion_cleanup_identity(owner)
    _, _, next_request, next_digest, _ = vm_cleanup_request_identity(
        owner_kind="job",
        owner_id=owner,
        identity=identity,
        source="job_terminal_vm_release",
        purge_disk=True,
    )
    outcome = "superseded_before_issue:" + str(next_request)

    def require(ok):
        if not ok:
            raise ResourceAdmissionError("legacy_vm_completion_supersession_unproven")

    async with recovery_store.db.acquire() as conn, conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery:job:{owner}",
        )
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
            f"workspace-recovery-pvc:{pvc_uid}",
        )
        queue = await conn.fetchrow(
            "SELECT * FROM run_queue WHERE unit_id=$1 AND unit_kind='worker_batch' FOR UPDATE",
            owner,
        )
        job = await conn.fetchrow("SELECT * FROM jobs WHERE id=$1 FOR UPDATE", owner)
        require(job is not None and pure_terminal_vm(dict(job)))
        require(
            job["lease_expires_at"] is None
            and queue is not None
            and queue["state"] == "done"
            and queue["leased_by"] is None
            and queue["leased_until"] is None
            and queue["lease_token"] > 0
        )
        context = _document(job["context"])
        vm = context["vm"]
        require(
            vm.get("provision_generation") == str(generation)
            and vm.get("identity_provision_generation") == str(generation)
            and vm.get("vm_uid") == str(vm_uid)
            and vm.get("rootdisk_pvc_uid") == str(pvc_uid)
        )
        parent = await conn.fetchrow(
            "SELECT * FROM vm_workspace_cleanup_admissions WHERE owner_kind='job' "
            "AND owner_id=$1 AND request_id=$2 FOR UPDATE",
            owner,
            legacy_request,
        )
        require(
            parent is not None
            and parent["source"] == "completion_workspace_teardown"
            and parent["intent_digest"] == legacy_digest
            and parent["pvc_uid"] is None
            and parent["parent_admission_id"] is None
        )
        require(parent["completed_at"] is None or parent["outcome"] == outcome)
        retry = await conn.fetchrow(
            "SELECT * FROM vm_creation_retries WHERE owner_kind='job' AND job_id=$1 "
            "AND provision_generation=$2 FOR UPDATE",
            owner,
            generation,
        )
        require(
            retry is not None
            and retry["state"] == "succeeded"
            and retry["reason"] == "creation_adopted"
            and retry["ready_at"] is not None
            and retry["ready_at"] <= parent["admitted_at"]
            and retry["observed_vm_uid"] == vm_uid
            and retry["observed_pvc_uid"] == pvc_uid
            and vm.get("creation_request_id") == str(retry["request_id"])
            and retry["origin"] == "initial"
            and retry["expected_pvc_uid"] is None
            and retry["predecessor_cleanup_admission_id"] is None
        )
        configuration = _document(retry["controller_configuration"])
        request = _document(retry["canonical_request"])
        require(
            isinstance(configuration, dict)
            and configuration.get("version") == 3
            and isinstance(request, dict)
            and request.get("entity_type") == "job"
            and request.get("job_id") == str(owner)
            and request.get("provision_generation") == str(generation)
            and canonical_request_digest(request) == retry["request_digest"]
            and canonical_configuration_digest(configuration)
            == retry["controller_configuration_digest"]
        )
        require(
            not await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM vm_creation_retries WHERE job_id=$1 AND request_id<>$2) "
                "OR EXISTS(SELECT 1 FROM jobs WHERE parent_job_id=$1)",
                owner,
                retry["request_id"],
            )
        )
        require(
            not await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM vm_workspace_cleanup_admissions WHERE parent_admission_id=$1) "
                "OR EXISTS(SELECT 1 FROM vm_resource_cleanup_stop_receipts WHERE cleanup_admission_id=$1) "
                "OR EXISTS(SELECT 1 FROM vm_pre_ssh_stop_intents WHERE cleanup_admission_id=$1)",
                parent["id"],
            )
        )
        if parent["completed_at"] is not None:
            # A second caller may have observed the old refusal before this
            # transaction won the locks. Replay only an existing exact successor;
            # a closed legacy row is never authority to create another parent.
            successor = await conn.fetchrow(
                "SELECT * FROM vm_workspace_cleanup_admissions WHERE owner_kind='job' "
                "AND owner_id=$1 AND request_id=$2 FOR UPDATE",
                owner,
                next_request,
            )
            require(
                successor is not None
                and successor["pvc_uid"] == pvc_uid
                and successor["source"] == "job_terminal_vm_release"
                and successor["intent_digest"] == next_digest
                and successor["parent_admission_id"] is None
            )
            permit = await acquire_vm_cleanup_permit(
                recovery_store,
                owner_kind="job",
                owner_id=owner,
                identity=identity,
                source="job_terminal_vm_release",
                purge_disk=True,
                _conn=conn,
            )
            require(permit.allowed)
            return permit
        require(
            not await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM managed_repository_process_zero_receipts "
                "WHERE owner_kind='job' AND owner_id=$1 AND scope='vm' AND runtime_incarnation=$2) "
                "OR EXISTS(SELECT 1 FROM vm_job_cancel_retention_authorities WHERE job_id=$1 OR pvc_uid=$3) "
                "OR EXISTS(SELECT 1 FROM vm_job_retained_resumes WHERE job_id=$1) "
                "OR EXISTS(SELECT 1 FROM vm_idle_operations WHERE owner_kind='job' AND owner_id=$1) "
                "OR EXISTS(SELECT 1 FROM srw_execution_workspace_bindings b JOIN srw_execution_specs s "
                "ON s.id=b.execution_id WHERE s.work_kind='Job' AND s.work_id=$1)",
                owner,
                str(generation),
                pvc_uid,
            )
        )
        charge = await conn.fetchrow(
            "SELECT * FROM vm_resource_reservations WHERE request_id=$1 AND state<>'released'",
            retry["request_id"],
        )
        require(
            charge is not None
            and charge["resource_version"] == 2
            and charge["state"] == "active"
            and charge["vm_uid"] == vm_uid
            and str(charge["vmi_uid"]) == vm.get("vmi_uid")
            and str(charge["launcher_uid"]) == vm.get("active_pod_uid")
            and charge["release_evidence"] is None
        )
        if parent["completed_at"] is None:
            await conn.execute(
                "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),outcome=$2 "
                "WHERE id=$1 AND completed_at IS NULL",
                parent["id"],
                outcome,
            )
        permit = await acquire_vm_cleanup_permit(
            recovery_store,
            owner_kind="job",
            owner_id=owner,
            identity=identity,
            source="job_terminal_vm_release",
            purge_disk=True,
            _conn=conn,
        )
        require(permit.allowed)
        # prepare_vm_cleanup_resource above rechecks identity and current policy,
        # charges teardown and rejects all access/recovery/retained authority.
        return permit


def superseded_legacy_completion_matches(permit, *, job_id, vm):
    """Recognize the exact nonphysical audit link, never a stop outcome."""
    from types import SimpleNamespace
    from orchestrator.services.completion_teardown_replay import (
        legacy_completion_cleanup_identity,
    )
    from orchestrator.services.vm_workspace_recovery_store import (
        vm_cleanup_request_identity,
    )

    try:
        owner = UUID(str(job_id))
        old_request, old_digest = legacy_completion_cleanup_identity(owner)
        proof = permit.parent_cleanup
        if not isinstance(proof, dict) or (
            proof.get("request_id") != str(old_request)
            or proof.get("intent_digest") != old_digest
        ):
            return False
        _, _, request, _, _ = vm_cleanup_request_identity(
            owner_kind="job",
            owner_id=owner,
            source="job_terminal_vm_release",
            purge_disk=True,
            identity=SimpleNamespace(
                provision_generation=vm["provision_generation"],
                vm_uid=vm["vm_uid"],
                rootdisk_pvc_uid=vm["rootdisk_pvc_uid"],
            ),
        )
        return permit.completed_outcome == "superseded_before_issue:" + str(request)
    except (AttributeError, KeyError, TypeError, ValueError):
        return False
