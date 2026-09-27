"""Ordinary Session Resume: immutable disk lineage and actor-gated admission."""

from __future__ import annotations

import json
from uuid import NAMESPACE_URL, UUID, uuid5

from orchestrator.services.vm_resource_reservation_store import _json
from orchestrator.services.vm_thread_network import verified_source
from orchestrator.services.vm_thread_retained_disk_purge import _lock_owner
from shared.vm_network_profile import reusable_profile_evidence


async def lock_owner_on_conn(conn, thread_id):
    """Read a locator, acquire owner/PVC, then let the caller lock/re-read owner."""
    locator = await conn.fetchrow(
        "SELECT COALESCE(t.metadata->'vm'->>'rootdisk_pvc_uid',source.expected_pvc_uid::text) AS pvc "
        "FROM threads t LEFT JOIN vm_creation_retries source "
        "ON source.request_id::text=t.metadata->'vm'->>'creation_request_id' AND source.thread_id=t.id "
        "WHERE t.id=$1",
        UUID(str(thread_id)),
    )
    await conn.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
        f"workspace-recovery:thread:{thread_id}",
    )
    if locator and locator["pvc"]:
        await _lock_owner(conn, thread_id, locator["pvc"])


async def predecessor_on_conn(conn, thread):
    """None is non-VM; recognized unproved retained history is a hard refusal."""
    vm = _json(thread["metadata"]).get("vm")
    if not isinstance(vm, dict) or not vm.get("creation_request_id"):
        return None
    source = await conn.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id::text=$1 "
        "AND owner_kind='thread' AND thread_id=$2 FOR SHARE",
        vm["creation_request_id"],
        thread["id"],
    )
    if source is None or _json(source["controller_configuration"]).get("version") != 3:
        return None
    outcome = await conn.fetchrow(
        "SELECT * FROM thread_runtime_retirement_outcomes WHERE thread_id=$1 "
        "AND runtime_generation=$2 AND outcome='settled' AND disposition='ended' "
        "AND NOT permanent ORDER BY settled_at DESC LIMIT 1",
        thread["id"],
        thread["runtime_generation"],
    )
    terminal = await conn.fetchrow(
        "SELECT terminal.* FROM vm_thread_retained_resume_terminals terminal "
        "JOIN vm_thread_retained_resumes op ON op.id=terminal.operation_id "
        "WHERE op.thread_id=$1 AND terminal.runtime_generation=$2 "
        "AND terminal.retirement_token=$3",
        thread["id"],
        thread["runtime_generation"],
        outcome["retirement_token"] if outcome else None,
    )
    cleanup = await conn.fetchrow(
        "SELECT * FROM vm_resource_thread_cleanup_authorities "
        "WHERE request_id=$1 AND NOT purge_disk "
        "AND (runtime_generation=$2 OR cleanup_admission_id=$3)",
        source["request_id"],
        thread["runtime_generation"],
        terminal["compute_cleanup_admission_id"] if terminal else None,
    )
    if (
        outcome is None
        or cleanup is None
        or (
            thread["ended_at"] != outcome["settled_at"]
            or vm.get("status") != "deleted"
            or vm.get("vm_uid") != str(cleanup["vm_uid"])
            or vm.get("rootdisk_pvc_uid") != str(cleanup["pvc_uid"])
        )
    ):
        raise RuntimeError("Retained Session Resume predecessor is unproven")
    await conn.fetchval(
        "SELECT public.validate_vm_thread_retained_compute($1)",
        cleanup["cleanup_admission_id"],
    )
    request = await inherited_request_on_conn(conn, cleanup, vm)
    if request is None:
        raise RuntimeError("Retained Session image/network lineage is unproven")
    return {
        "source": source,
        "cleanup": cleanup,
        "outcome": outcome,
        "terminal": terminal,
        "vm": vm,
    }


async def record_resume_on_conn(conn, thread, predecessor, generation):
    operation = uuid5(
        NAMESPACE_URL,
        f"vm-thread-retained-resume:{thread['id']}:{predecessor['outcome']['retirement_token']}:{generation}",
    )
    await conn.execute(
        "INSERT INTO vm_thread_retained_resumes "
        "(id,thread_id,runtime_generation,predecessor_runtime_generation,"
        "predecessor_retirement_token,compute_cleanup_admission_id,predecessor_terminal_id,"
        "source_revision,retained_vm,request_id,provision_generation) "
        "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11)",
        operation,
        thread["id"],
        generation,
        thread["runtime_generation"],
        predecessor["outcome"]["retirement_token"],
        predecessor["cleanup"]["cleanup_admission_id"],
        predecessor["terminal"]["id"] if predecessor["terminal"] else None,
        predecessor["source"]["revision"],
        json.dumps(predecessor["vm"]),
        uuid5(operation, "source"),
        uuid5(operation, "provision"),
    )


async def inherited_request_on_conn(conn, cleanup, vm):
    """An absent profile is immutable too; today's feature flag is irrelevant."""
    source = await conn.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1 FOR SHARE",
        cleanup["request_id"],
    )
    request = verified_source(
        source,
        thread_id=str(cleanup["thread_id"]),
        generation=str(cleanup["provision_generation"]),
        request_id=str(cleanup["request_id"]),
    )
    if request is None:
        return None
    profile = request.get("network_profile")
    if profile is None:
        return request if vm.get("network_profile_evidence") is None else None
    if not isinstance(vm.get("interface_mac"), str) or not vm["interface_mac"]:
        return None
    if not reusable_profile_evidence(
        vm.get("network_profile_evidence"),
        profile,
        provision_generation=str(cleanup["provision_generation"]),
        vm_uid=str(cleanup["vm_uid"]),
        pvc_uid=str(cleanup["pvc_uid"]),
        vmi_uid=str(cleanup["vmi_uid"]),
        launcher_uid=str(cleanup["launcher_uid"]),
        interface_mac=vm.get("interface_mac"),
    ):
        return None
    chain = await conn.fetch(
        "SELECT * FROM vm_creation_retries WHERE owner_kind='thread' AND thread_id=$1 "
        "AND (expected_pvc_uid=$2 OR observed_pvc_uid=$2) FOR SHARE",
        cleanup["thread_id"],
        cleanup["pvc_uid"],
    )
    for item in chain:
        prior = verified_source(
            item,
            thread_id=str(cleanup["thread_id"]),
            generation=str(item["provision_generation"]),
        )
        if (
            prior is None
            or prior.get("network_profile") != profile
            or (prior.get("vm_image") != request.get("vm_image"))
        ):
            return None
    return request


async def operation_on_conn(conn, operation_id, thread_id):
    operation = await conn.fetchrow(
        "SELECT * FROM vm_thread_retained_resumes WHERE id=$1 AND thread_id=$2",
        UUID(str(operation_id)),
        UUID(str(thread_id)),
    )
    if operation is None:
        return None
    cleanup = await conn.fetchrow(
        "SELECT * FROM vm_resource_thread_cleanup_authorities WHERE cleanup_admission_id=$1",
        operation["compute_cleanup_admission_id"],
    )
    request = await inherited_request_on_conn(
        conn, cleanup, _json(operation["retained_vm"])
    )
    if request is None:
        return None
    return {**dict(operation), "pvc_uid": cleanup["pvc_uid"], "request": request}


async def ensure_retained_thread_vm(thread, *, store, provisioner):
    """None means no current operation; false means pending, with no effects."""
    metadata = _json(thread["metadata"])
    vm = metadata.get("vm")
    if not isinstance(vm, dict) or not vm.get("creation_request_id"):
        return None
    operation = await store.fetchrow(
        "SELECT * FROM vm_thread_retained_resumes WHERE thread_id=$1 AND runtime_generation=$2",
        UUID(str(thread["id"])),
        UUID(str(thread["runtime_generation"])),
    )
    if operation is None:
        retired_operation = await store.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_thread_retained_resumes op JOIN threads t ON t.id=op.thread_id "
            "WHERE op.thread_id=$1 AND t.runtime_generation=$2 AND public.valid_vm_thread_retained_runtime(op,t))",
            UUID(str(thread["id"])),
            UUID(str(thread["runtime_generation"])),
        )
        return False if retired_operation else None
    if (
        thread.get("agent_id") is None
        or thread.get("runtime_attach_token") is None
        or thread.get("runtime_retirement_token") is not None
        or thread.get("status") == "ended"
    ):
        return False
    source = await store.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE request_id=$1",
        operation["request_id"],
    )
    if source is not None:
        return bool(
            source["thread_agent_id"] == thread["agent_id"]
            and source["thread_attach_token"] == thread["runtime_attach_token"]
        )
    async with store.acquire() as conn:
        frozen = await operation_on_conn(conn, operation["id"], thread["id"])
    if frozen is None or provisioner is None:
        return False
    from orchestrator.services.vm_workspace_config import vm_provisioning_options

    options = {
        key: frozen["request"][key]
        for key in ("agent_config", "cpu_cores", "memory", "disk_size", "description")
        if key in frozen["request"]
    }
    options.update(
        await vm_provisioning_options(
            store,
            "Session",
            thread,
            fallback=metadata.get("config_override"),
        )
    )
    options.pop("preparation", None)
    options.pop("initialization", None)
    options.update(
        vm_image=frozen["request"].get("vm_image"),
        network_profile=frozen["request"].get("network_profile"),
    )
    return await provisioner.create_thread_vm(
        str(thread["id"]),
        **options,
        retained_resume_id=str(operation["id"]),
        expected_runtime_generation=str(thread["runtime_generation"]),
        expected_agent_id=str(thread["agent_id"]),
        expected_attach_token=str(thread["runtime_attach_token"]),
        expected_vm_context=vm,
    )


async def handoff_on_conn(retries, conn, source, thread, create_permit):
    """Commit the verified cancelled create into its existing exact stop protocol."""
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict
    from orchestrator.services.vm_provisioner import VMTeardownIdentity
    from orchestrator.services.vm_workspace_recovery_store import (
        acquire_vm_cleanup_permit,
    )

    if create_permit["completed_at"] is not None:
        if (
            source["state"] != "settled"
            or source["reason"] != "retained_creation_handoff"
            or create_permit["outcome"] != "adopted"
        ):
            raise VMCreationRetryConflict("creation_reservation_changed")
        valid = await conn.fetchval(
            "SELECT public.validate_vm_thread_cleanup_authority(a,true) FROM vm_resource_thread_cleanup_authorities a "
            "WHERE a.request_id=$1 AND a.runtime_generation=$2 AND a.retirement_token=$3",
            source["request_id"],
            thread["runtime_generation"],
            thread["runtime_retirement_token"],
        )
        if not valid:
            raise VMCreationRetryConflict("retained_creation_handoff_missing")
        return {"settled": True, "disposition": "retirement_handoff"}
    if source["state"] != "cancel_requested":
        raise VMCreationRetryConflict("thread_retirement_source_changed")
    await conn.execute(
        "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),outcome='adopted' WHERE id=$1",
        source["creation_admission_id"],
    )
    await conn.execute(
        "UPDATE vm_creation_retries SET state='settled',reason='retained_creation_handoff',boot_counted=true,"
        "revision=revision+1,claim_token=NULL,claim_expires_at=NULL,resolved_at=clock_timestamp(),updated_at=clock_timestamp() "
        "WHERE request_id=$1",
        source["request_id"],
    )
    permit = await acquire_vm_cleanup_permit(
        retries.cleanup,
        owner_kind="thread",
        owner_id=thread["id"],
        identity=VMTeardownIdentity(
            provision_generation=str(source["provision_generation"]),
            vm_uid=str(source["observed_vm_uid"]),
            rootdisk_pvc_uid=str(source["observed_pvc_uid"]),
        ),
        source="pinned_thread_retirement",
        purge_disk=thread["runtime_retirement_permanent"],
        _conn=conn,
    )
    if not permit.allowed:
        raise VMCreationRetryConflict("retained_creation_cleanup_held")
    return {"settled": True, "disposition": "retirement_handoff"}


async def handoff_identity(store, retirement):
    """Resolve only an existing exact cleanup; this cannot adopt or grant create."""
    from orchestrator.services.vm_provisioner import VMTeardownIdentity

    context = retirement["context"]
    source = context["vm_creation_source"]
    async with store.acquire() as conn, conn.transaction():
        locator = await conn.fetchrow(
            "SELECT a.*,e.evidence FROM vm_resource_thread_cleanup_authorities a "
            "JOIN vm_creation_effects e ON e.request_id=a.request_id AND e.effect_kind='vm' AND e.state='observed' "
            "WHERE a.request_id=$1 AND a.thread_id=$2 AND a.runtime_generation=$3 AND a.retirement_token=$4",
            UUID(source["request_id"]),
            UUID(context["thread_id"]),
            UUID(retirement["generation"]),
            UUID(retirement["token"]),
        )
        if locator is None:
            return None
        await _lock_owner(conn, locator["thread_id"], locator["pvc_uid"])
        await conn.fetchval(
            "SELECT public.validate_vm_thread_cleanup_authority(a,true) FROM vm_resource_thread_cleanup_authorities a WHERE cleanup_admission_id=$1",
            locator["cleanup_admission_id"],
        )
        return VMTeardownIdentity(
            provision_generation=str(locator["provision_generation"]),
            vm_uid=str(locator["vm_uid"]),
            rootdisk_pvc_uid=str(locator["pvc_uid"]),
            ssh_host_key_fingerprint=_json(locator["evidence"])[
                "ssh_host_key_fingerprint"
            ],
        )


async def terminal_disk_identity(store, retirement):
    """Project positively settled no-VM creation back to its exact retained disk."""
    from orchestrator.services.vm_provisioner import VMTeardownIdentity

    context = retirement["context"]
    async with store.acquire() as conn, conn.transaction():
        locator = await conn.fetchrow(
            "SELECT a.thread_id,a.pvc_uid FROM vm_thread_retained_resumes op "
            "JOIN vm_resource_thread_cleanup_authorities a ON a.cleanup_admission_id=op.compute_cleanup_admission_id "
            "JOIN threads t ON t.id=op.thread_id "
            "WHERE op.thread_id=$1 AND t.runtime_generation=$2 AND public.valid_vm_thread_retained_runtime(op,t)",
            UUID(context["thread_id"]),
            UUID(retirement["generation"]),
        )
        if locator is None:
            return None
        await _lock_owner(conn, locator["thread_id"], locator["pvc_uid"])
        await conn.fetchrow(
            "SELECT id FROM threads WHERE id=$1 FOR UPDATE", UUID(context["thread_id"])
        )
        op = await conn.fetchrow(
            "SELECT op.* FROM vm_thread_retained_resumes op JOIN threads t ON t.id=op.thread_id "
            "WHERE op.thread_id=$1 AND t.runtime_generation=$2 "
            "AND t.runtime_retirement_token=$3 AND public.valid_vm_thread_retained_early_end(t,op)",
            UUID(context["thread_id"]),
            UUID(retirement["generation"]),
            UUID(retirement["token"]),
        )
        if op is None:
            return None
        await conn.execute(
            "UPDATE threads SET metadata=jsonb_set(metadata,'{vm}',$2::jsonb) WHERE id=$1",
            UUID(context["thread_id"]),
            op["retained_vm"],
        )
        vm = _json(op["retained_vm"])
        return VMTeardownIdentity(
            provision_generation=vm["provision_generation"],
            vm_uid=vm["vm_uid"],
            rootdisk_pvc_uid=vm["rootdisk_pvc_uid"],
        )


async def clear_terminal_vm_projection(store, retirement):
    """Clear only a cancelled source's proven permanent cleanup projection."""
    if not retirement.get("permanent"):
        return True
    context = retirement["context"]
    async with store.acquire() as conn, conn.transaction():
        locator = await conn.fetchrow(
            "SELECT r.request_id,r.thread_id,r.expected_pvc_uid FROM vm_creation_retries r "
            "WHERE r.request_id=$1 AND r.thread_id=$2",
            UUID(context["vm_creation_source"]["request_id"]),
            UUID(context["thread_id"]),
        )
        if locator is None:
            return False
        await _lock_owner(conn, locator["thread_id"], locator["expected_pvc_uid"])
        row = await conn.fetchrow(
            "SELECT t.metadata,r.observed_vm_uid,op.retained_vm FROM threads t "
            "JOIN vm_creation_retries r ON r.request_id=$4 AND r.thread_id=t.id "
            "JOIN vm_thread_retained_resumes op ON op.id=r.thread_retained_resume_id "
            "WHERE t.id=$1 AND t.runtime_generation=$2 AND t.runtime_retirement_token=$3 "
            "AND t.runtime_retirement_permanent AND public.valid_thread_vm_creation_retirement_source(r,false) "
            "AND public.vm_thread_retained_source_terminal_evidence(r) IS NOT NULL FOR UPDATE OF t",
            locator["thread_id"],
            UUID(retirement["generation"]),
            UUID(retirement["token"]),
            locator["request_id"],
        )
        if row is None:
            return False
        if row["observed_vm_uid"] is None:
            proven = await conn.fetchval(
                "SELECT public.validate_vm_thread_retained_disk_purge_receipt(d,p.purge_evidence,true) "
                "FROM vm_thread_retained_disk_purge_authorities d "
                "JOIN vm_thread_retained_disk_purge_receipts p USING(cleanup_admission_id) "
                "JOIN vm_workspace_cleanup_admissions c ON c.id=d.cleanup_admission_id "
                "WHERE d.runtime_generation=$1 AND d.retirement_token=$2 AND c.completed_at IS NOT NULL AND c.outcome='completed'",
                UUID(retirement["generation"]),
                UUID(retirement["token"]),
            )
        else:
            proven = await conn.fetchval(
                "SELECT public.validate_vm_thread_cleanup_authority(a,true) "
                "FROM vm_resource_thread_cleanup_authorities a JOIN vm_workspace_cleanup_admissions c ON c.id=a.cleanup_admission_id "
                "WHERE a.request_id=$1 AND a.runtime_generation=$2 AND a.retirement_token=$3 AND a.purge_disk "
                "AND c.completed_at IS NOT NULL AND c.outcome='completed'",
                locator["request_id"],
                UUID(retirement["generation"]),
                UUID(retirement["token"]),
            )
        if not proven:
            return False
        await conn.execute(
            "UPDATE threads SET metadata=metadata-'vm' WHERE id=$1",
            locator["thread_id"],
        )
        return True
