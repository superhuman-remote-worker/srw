"""Replacing an inherited VM requires this Job's own exact retirement authority."""

import json
from uuid import UUID, uuid4

import pytest

from tests.test_vm_creation_inherited_attachment_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    setup as _setup_fixture,
    attached as _attached_fixture,
    controller_bridge,
)
from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore
from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict
from orchestrator.services.vm_workspace_recovery_store import cleanup_intent_digest
from orchestrator.services.vm_provisioner import VMProvisioner
from vm_controller.creation_configuration import resolve_creation_configuration
from shared.vm_workspace_storage import storage_name

setup, attached, db = _setup_fixture, _attached_fixture, _db_fixture


async def retire(db, attached, payload):
    ctrl, api, _, _ = attached
    job = UUID(payload["job_id"])
    async with db.acquire() as conn:
        context = json.loads(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", job)
        )
        old = context["vm"]
        old["status"] = "deleted"
        receipt = await conn.fetchval(
            "INSERT INTO managed_repository_process_zero_receipts(owner_kind,owner_id,scope,provisioner,runtime_incarnation) VALUES('job',$1,'vm','vm',$2) RETURNING id",
            job,
            old["provision_generation"],
        )
        await conn.execute(
            "UPDATE jobs SET status='paused',context=$2::jsonb WHERE id=$1",
            job,
            json.dumps(context),
        )
        intent = {
            "owner_kind": "job",
            "owner_id": str(job),
            "provision_generation": old["provision_generation"],
            "vm_uid": old["vm_uid"],
            "pvc_uid": old["rootdisk_pvc_uid"],
            "purge_disk": False,
            "resource": "vm_workspace",
            "source": "lifecycle_vm_reap",
        }
        cleanup = uuid4()
        await conn.execute(
            "INSERT INTO vm_workspace_cleanup_admissions(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,completed_at,outcome) VALUES($1,'job',$2,$3,'lifecycle_vm_reap',$4,$5,clock_timestamp(),'completed')",
            cleanup,
            job,
            UUID(old["rootdisk_pvc_uid"]),
            uuid4(),
            cleanup_intent_digest(intent),
        )
    api.objects.pop(("VirtualMachine", "agent-vm-" + str(job)))
    api.objects.pop(("Secret", "agent-vm-" + str(job) + "-cloudinit"))
    fresh = VMProvisioner._fresh_provision_ctx()
    request = {key: value for key, value in payload.items() if key != "creation_retry"}
    request["provision_generation"] = fresh["provision_generation"]
    api.writes.clear()
    return request, fresh, old, receipt, cleanup


async def replacing(db, attached):
    ctrl, _, _, _ = attached
    jobs, _, store, first, payload = await controller_bridge(db, attached)
    assert (await ctrl._do_create_serialized(payload))["status"] == "created"
    return store, first, await retire(db, attached, payload)


async def admit_replacement(db, ctrl, store, request, fresh):
    preflight = VMCreationPreflightStore(db)
    value = await preflight.begin(
        job_id=request["job_id"], request=request, fresh_context=fresh
    )
    claim = (await preflight.claim_due(limit=1))[0]
    resolved = resolve_creation_configuration(ctrl, claim["request"])
    resolved["creation_retry_protocol"] = 1
    row = await preflight.complete_resolution(claim, resolved)
    claim = (await store.claim_due(limit=1))[0]
    payload = {
        **resolved["request"],
        "creation_retry": {
            "version": 1,
            "request_id": str(row["request_id"]),
            "claim_token": str(claim["claim_token"]),
            "request_digest": row["request_digest"],
            "controller_configuration_digest": row["controller_configuration_digest"],
        },
    }
    return value, row, payload


@pytest.mark.asyncio
async def test_inherited_replacement_claims_same_lease_with_own_cleanup_proof(
    db, attached
):
    ctrl, api, _, _ = attached
    store, first, (request, fresh, old, receipt, cleanup) = await replacing(
        db, attached
    )
    name = storage_name(request["workspace_storage"])
    original_disk, original_lease = (
        api.read("DataVolume", name),
        api.read("Lease", name),
    )
    value, row, payload = await admit_replacement(db, ctrl, store, request, fresh)
    proof = value["predecessor_evidence"]
    assert proof["kind"] == "retained_attachment_replacement"
    assert proof["handoff"] == first["predecessor_evidence"]
    assert proof["retired_request_id"] == str(first["request_id"])
    assert proof["retirement"]["receipt_id"] == str(receipt)
    assert row["predecessor_cleanup_admission_id"] == cleanup
    assert row["admission_deadline"] == first["admission_deadline"]
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "created", result
    assert result["vm_uid"] != old["vm_uid"]
    current = await store.inspect(request_id=str(row["request_id"]))
    attachment = current["effects"][0]
    assert attachment["carrier_intent"]["workspace_attachment"]["action"] == "claim"
    assert attachment["evidence"]["uid"] == original_lease["metadata"]["uid"]
    assert api.read("DataVolume", name) == original_disk
    assert "DataVolume" not in api.writes and api.writes.count("VirtualMachine") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "receipt",
        "wrong_receipt",
        "cleanup",
        "purge",
        "vm_uid",
        "authentication",
        "binding",
        "last_vm_override",
    ],
)
async def test_inherited_replacement_requires_own_exact_retirement(
    db, attached, change
):
    _, _, _, _ = attached
    _, _, (request, fresh, old, receipt, cleanup) = await replacing(db, attached)
    async with db.acquire() as conn:
        if change == "receipt":
            await conn.execute(
                "DELETE FROM managed_repository_process_zero_receipts WHERE id=$1",
                receipt,
            )
        elif change == "wrong_receipt":
            await conn.execute(
                "UPDATE managed_repository_process_zero_receipts SET runtime_incarnation=$2 WHERE id=$1",
                receipt,
                str(uuid4()),
            )
        elif change == "cleanup":
            await conn.execute(
                "UPDATE vm_workspace_cleanup_admissions SET outcome='other' WHERE id=$1",
                cleanup,
            )
        elif change == "purge":
            await conn.execute(
                "UPDATE vm_workspace_cleanup_admissions SET intent_digest=$2 WHERE id=$1",
                cleanup,
                cleanup_intent_digest(
                    {
                        "owner_kind": "job",
                        "owner_id": request["job_id"],
                        "provision_generation": old["provision_generation"],
                        "vm_uid": old["vm_uid"],
                        "pvc_uid": old["rootdisk_pvc_uid"],
                        "purge_disk": True,
                        "resource": "vm_workspace",
                        "source": "lifecycle_vm_reap",
                    }
                ),
            )
        else:
            changed = {**old}
            if change == "authentication":
                changed["identity_authenticated"] = False
            elif change == "binding":
                changed.pop("workspace_storage")
            else:
                changed["vm_uid"] = str(uuid4())
            context = json.loads(
                await conn.fetchval(
                    "SELECT context FROM jobs WHERE id=$1", UUID(request["job_id"])
                )
            )
            context["vm"] = changed
            if change == "last_vm_override":
                context["last_vm"] = old
            await conn.execute(
                "UPDATE jobs SET context=$2::jsonb WHERE id=$1",
                UUID(request["job_id"]),
                json.dumps(context),
            )
    with pytest.raises(VMCreationRetryConflict):
        await VMCreationPreflightStore(db).begin(
            job_id=request["job_id"], request=request, fresh_context=fresh
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("lost", ["grant", "attachment", "vm", "cancelled_vm"])
async def test_replacement_lost_reply_never_duplicates_effect(db, attached, lost):
    ctrl, api, _, _ = attached
    store, _, (request, fresh, _, _, _) = await replacing(db, attached)
    _, row, payload = await admit_replacement(db, ctrl, store, request, fresh)
    name = storage_name(request["workspace_storage"])
    if lost == "grant":
        authority = ctrl._workspace_cleanup_authority_request
        dropped = False

        async def lose_grant(path, body, *, operation):
            nonlocal dropped
            result = await authority(path, body, operation=operation)
            if operation == "creation_retry_begin_effect" and not dropped:
                dropped = True
                raise TimeoutError("grant committed, reply lost")
            return result

        ctrl._workspace_cleanup_authority_request = lose_grant
    elif lost == "attachment":
        replace = api.replace

        def lose_attachment(body):
            result = replace(body)
            if body["metadata"]["name"] == name:
                raise TimeoutError("attachment committed, reply lost")
            return result

        api.replace = lose_attachment
    else:
        api.lost.add("VirtualMachine")
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
    if lost == "cancelled_vm":
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET status='cancelled' WHERE id=$1", row["job_id"]
            )
            await conn.execute(
                "UPDATE srw_workspace_instances SET status='Deleting' WHERE id=$1",
                UUID(request["workspace_storage"]["uid"]),
            )
    result = await ctrl._do_create_serialized(payload)
    observed = await store.inspect(request_id=str(row["request_id"]))
    if lost == "grant":
        # The committed grant is ambiguous. No controller may repeat it merely
        # because the old Lease still exists without the new request marker.
        assert result["status"] == "creation_attention"
        assert len(observed["effects"]) == 1
        assert observed["effects"][0]["state"] == "issued"
        assert api.writes.count("VirtualMachine") == 0
        assert (
            api.read("Lease", name)["metadata"]["resourceVersion"]
            == row["predecessor_evidence"]["attachment"]["resource_version"]
        )
    else:
        assert result["status"] == "created", result
        assert observed["state"] == (
            "settled" if lost == "cancelled_vm" else "succeeded"
        )
        assert len(observed["effects"]) == 4
        assert api.writes.count("VirtualMachine") == 1
    assert "DataVolume" not in api.writes


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["receipt", "cleanup", "last_vm", "lease_uid", "lease_version", "cancel"]
)
async def test_replacement_rechecks_own_authority_at_fresh_effect(db, attached, change):
    ctrl, api, _, _ = attached
    store, _, (request, fresh, old, receipt, cleanup) = await replacing(db, attached)
    _, row, payload = await admit_replacement(db, ctrl, store, request, fresh)
    async with db.acquire() as conn:
        if change == "receipt":
            await conn.execute(
                "DELETE FROM managed_repository_process_zero_receipts WHERE id=$1",
                receipt,
            )
        elif change == "cleanup":
            await conn.execute(
                "UPDATE vm_workspace_cleanup_admissions SET outcome='other' WHERE id=$1",
                cleanup,
            )
        elif change == "last_vm":
            old["vm_uid"] = str(uuid4())
            await conn.execute(
                "UPDATE jobs SET context=jsonb_set(context,'{last_vm}',$2::jsonb) WHERE id=$1",
                row["job_id"],
                json.dumps(old),
            )
        elif change == "cancel":
            await conn.execute(
                "UPDATE jobs SET status='cancelled' WHERE id=$1", row["job_id"]
            )
        else:
            lease = api.objects[("Lease", storage_name(request["workspace_storage"]))]
            lease["metadata"]["uid" if change == "lease_uid" else "resourceVersion"] = (
                str(uuid4()) if change == "lease_uid" else "9999"
            )
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] != "created", result
    observed = await store.inspect(request_id=str(row["request_id"]))
    assert observed["effects"] == []
    assert (
        "VirtualMachine" not in api.writes
        and "Secret" not in api.writes
        and "DataVolume" not in api.writes
    )


@pytest.mark.asyncio
async def test_second_replacement_preserves_flat_handoff_and_uses_latest_own_retirement(
    db, attached
):
    ctrl, _, _, _ = attached
    store, first, (request, fresh, _, _, _) = await replacing(db, attached)
    _, second, payload = await admit_replacement(db, ctrl, store, request, fresh)
    assert (await ctrl._do_create_serialized(payload))["status"] == "created"
    request, fresh, _, receipt, cleanup = await retire(db, attached, payload)
    proof, third, payload = await admit_replacement(db, ctrl, store, request, fresh)
    assert proof["predecessor_evidence"]["handoff"] == first["predecessor_evidence"]
    assert proof["predecessor_evidence"]["retired_request_id"] == str(
        second["request_id"]
    )
    assert proof["predecessor_evidence"]["retirement"]["receipt_id"] == str(receipt)
    assert third["predecessor_cleanup_admission_id"] == cleanup
    assert (await ctrl._do_create_serialized(payload))["status"] == "created"


@pytest.mark.asyncio
async def test_replacement_rechecks_own_cleanup_between_effects(db, attached):
    ctrl, api, _, _ = attached
    store, _, (request, fresh, _, _, cleanup) = await replacing(db, attached)
    _, row, payload = await admit_replacement(db, ctrl, store, request, fresh)
    authority = ctrl._workspace_cleanup_authority_request

    revoked = False

    async def revoke_after_attachment(path, body, *, operation):
        nonlocal revoked
        result = await authority(path, body, operation=operation)
        if operation == "creation_retry_observe_effect" and not revoked:
            revoked = True
            async with db.acquire() as conn:
                await conn.execute(
                    "UPDATE vm_workspace_cleanup_admissions SET outcome='other' WHERE id=$1",
                    cleanup,
                )
        return result

    ctrl._workspace_cleanup_authority_request = revoke_after_attachment
    assert (await ctrl._do_create_serialized(payload))["status"] != "created"
    assert revoked
    current = await store.inspect(request_id=str(row["request_id"]))
    assert len(current["effects"]) == 1
    assert current["effects"][0]["state"] == "observed"
    assert (
        "VirtualMachine" not in api.writes
        and "Secret" not in api.writes
        and "DataVolume" not in api.writes
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["revision", "generation", "deadline"])
async def test_replacement_keeps_prior_execution_binding_without_context_copy(
    db, attached, change
):
    _, first, (request, fresh, _, _, _) = await replacing(db, attached)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=context #- '{vm,creation_preflight}' WHERE id=$1",
            UUID(request["job_id"]),
        )
        if change == "revision":
            await conn.execute(
                "UPDATE srw_execution_specs SET revision='changed' WHERE id=$1",
                first["execution_id"],
            )
        elif change == "generation":
            await conn.execute(
                "UPDATE srw_execution_specs SET generation=generation+1 WHERE id=$1",
                first["execution_id"],
            )
        else:
            await conn.execute(
                "UPDATE srw_execution_specs SET resolved=jsonb_set(resolved,'{spec,timeoutSeconds}','7200'::jsonb) WHERE id=$1",
                first["execution_id"],
            )
    with pytest.raises(VMCreationRetryConflict):
        await VMCreationPreflightStore(db).begin(
            job_id=request["job_id"], request=request, fresh_context=fresh
        )


@pytest.mark.asyncio
async def test_replacement_uses_unchanged_ledger_without_context_copy(db, attached):
    ctrl, _, _, _ = attached
    store, first, (request, fresh, _, _, _) = await replacing(db, attached)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=context #- '{vm,creation_preflight}' WHERE id=$1",
            UUID(request["job_id"]),
        )
    _, row, payload = await admit_replacement(db, ctrl, store, request, fresh)
    assert row["admission_deadline"] == first["admission_deadline"]
    assert (await ctrl._do_create_serialized(payload))["status"] == "created"
