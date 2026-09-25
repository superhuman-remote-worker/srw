"""Prepared inheritance uses real original clone authority and target completion."""

import json
from copy import deepcopy
from uuid import UUID, uuid4

import pytest

from tests.test_vm_creation_actuation import poll_until_terminal

from tests.test_vm_creation_prepared_attachment_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    setup as _setup_fixture,
    prepared as _prepared_fixture,
    workspace as _workspace_fixture,
)
from tests.test_vm_creation_attachment_bridge_real_postgres import bridge
from tests.test_vm_creation_prepared_actuation import finish_prepared
from tests.test_vm_creation_inherited_replacement_real_postgres import (
    retire,
    admit_replacement,
)
from tests.test_vm_creation_preflight_real_postgres import initial_job
from orchestrator.services.vm_workspace_recovery_store import cleanup_intent_digest
from shared.vm_workspace_storage import storage_name
from shared.workspace_preparation import preparation_request
from vm_controller.creation_preparation import PreparedSources

setup, prepared, workspace, db = (
    _setup_fixture,
    _prepared_fixture,
    _workspace_fixture,
    _db_fixture,
)


async def inheritance(db, workspace, *, allocation_lost=False):
    ctrl, api, _, payload, service = workspace
    store, original = await bridge(db, workspace[:4])
    assert (await finish_prepared(workspace))["status"] == "created"
    name = storage_name(payload["workspace_storage"])
    api.objects["DataVolume", name]["status"] = {"phase": "Succeeded"}
    api.objects["PersistentVolumeClaim", name]["status"] = {"phase": "Bound"}
    observed = await store.inspect(request_id=str(original["request_id"]))
    await PreparedSources(ctrl).release_completed(observed)
    source = next(
        e["carrier_intent"]["rootdisk_source"]
        for e in observed["effects"]
        if e["carrier_intent"]["effect_kind"] == "rootdisk"
    )
    if allocation_lost:
        service.store.data.clear()
        api.objects.pop(("DataVolume", source["name"]))
        api.objects.pop(("PersistentVolumeClaim", source["name"]))
    request, fresh = await next_job(db, workspace, original, payload)
    return store, original, request, fresh, source


async def next_job(db, workspace, previous, payload):
    ctrl, api, _, _, _ = workspace
    name = storage_name(payload["workspace_storage"])
    request, fresh, old, _, _ = await retire(db, workspace[:4], payload)
    job = await initial_job(db)
    binding = {
        **old["workspace_storage"],
        "generation": old["workspace_storage"]["generation"] + 1,
    }
    request["job_id"] = str(job)
    request["workspace_storage"] = binding
    prep = payload["preparation"]
    request["preparation"] = preparation_request(
        {
            "image": prep["image"],
            "prepare": prep["steps"],
            "cache": prep["cache"],
            "pullPolicy": prep["pullPolicy"],
        },
        scope_kind=prep["scope"]["kind"],
        scope_uid=prep["scope"]["uid"],
        allocation_id=str(job),
        owner_kind="job",
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET status='completed' WHERE id=$1", previous["job_id"]
        )
        intent = {
            "owner_kind": "job",
            "owner_id": str(previous["job_id"]),
            "generation": old["provision_generation"],
            "pvc_uid": binding["pvc_uid"],
            "resource": "retained_workspace_binding",
            "source": "retained_workspace_detach",
        }
        await conn.execute(
            "INSERT INTO vm_workspace_cleanup_admissions(id,owner_kind,owner_id,pvc_uid,source,request_id,intent_digest,completed_at,outcome) VALUES($1,'job',$2,$3,'retained_workspace_detach',$4,$5,clock_timestamp(),'retained_workspace_detached')",
            uuid4(),
            previous["job_id"],
            UUID(binding["pvc_uid"]),
            uuid4(),
            cleanup_intent_digest(intent),
        )
        execution = await conn.fetchval(
            "SELECT id FROM srw_execution_specs WHERE work_id=$1", job
        )
        await conn.execute(
            "INSERT INTO srw_execution_workspace_bindings(execution_id,instance_id) VALUES($1,$2)",
            execution,
            UUID(binding["uid"]),
        )
        await conn.execute(
            "UPDATE srw_workspace_instances SET execution_id=$2,generation=$4,status='Reserved',backend_state=jsonb_set(backend_state,'{storage}',$3::jsonb) WHERE id=$1",
            UUID(binding["uid"]),
            execution,
            json.dumps(binding),
            binding["generation"],
        )
    lease = api.objects["Lease", name]
    lease["metadata"]["annotations"]["srw.io/detached"] = "true"
    lease["metadata"]["resourceVersion"] = str(
        int(lease["metadata"]["resourceVersion"]) + 1
    )
    render_original = ctrl.render_template

    def render(request, key):
        result = render_original(request, key)
        result["metadata"]["name"] = "agent-vm-" + request["job_id"]
        result["spec"]["template"]["spec"]["volumes"][1]["cloudInitNoCloud"][
            "secretRef"
        ]["name"] = "agent-vm-" + request["job_id"] + "-cloudinit"
        return result

    ctrl.render_template = render
    api.writes.clear()
    return request, fresh


@pytest.mark.asyncio
@pytest.mark.parametrize("allocation_lost", [False, True])
async def test_prepared_inheritance_keeps_exact_completed_target_and_original_receipt(
    db, workspace, allocation_lost
):
    ctrl, api, _, _, service = workspace
    store, original, request, fresh, source = await inheritance(
        db, workspace, allocation_lost=allocation_lost
    )
    disk = deepcopy(api.read("DataVolume", storage_name(request["workspace_storage"])))
    _, row, payload = await admit_replacement(db, ctrl, store, request, fresh)
    result = await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5)
    assert result["status"] == "created", result
    current = await store.inspect(request_id=str(row["request_id"]))
    assert current["prepared_origin"]["request_id"] == str(original["request_id"])
    assert result["preparation"]["pvcUid"] == source["receipt"]["pvcUid"]
    assert result["rootdisk_pvc_uid"] != source["receipt"]["pvcUid"]
    assert api.read("DataVolume", storage_name(request["workspace_storage"])) == disk
    assert "DataVolume" not in api.writes and api.writes.count("VirtualMachine") == 1
    assert len(service.store.created_pods) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "dv_pending",
        "pvc_pending",
        "dv_uid",
        "pvc_uid",
        "source",
        "nonce",
        "owner_ref",
        "deleting",
    ],
)
async def test_prepared_inheritance_requires_actual_exact_completed_target_before_attachment(
    db, workspace, change
):
    ctrl, api, _, _, _ = workspace
    store, _, request, fresh, _ = await inheritance(db, workspace, allocation_lost=True)
    _, row, payload = await admit_replacement(db, ctrl, store, request, fresh)
    name = storage_name(request["workspace_storage"])
    dv, pvc = (
        api.objects["DataVolume", name],
        api.objects["PersistentVolumeClaim", name],
    )
    if change == "dv_pending":
        dv["status"]["phase"] = "CloneInProgress"
    elif change == "pvc_pending":
        pvc["status"]["phase"] = "Pending"
    elif change == "dv_uid":
        dv["metadata"]["uid"] = str(uuid4())
    elif change == "pvc_uid":
        pvc["metadata"]["uid"] = str(uuid4())
    elif change == "source":
        dv["spec"]["source"]["pvc"]["name"] = "other"
    elif change == "nonce":
        dv["metadata"]["annotations"]["srw.io/vm-create-effect-nonce"] = str(uuid4())
    elif change == "owner_ref":
        pvc["metadata"]["ownerReferences"][0]["controller"] = False
    else:
        dv["metadata"]["deletionTimestamp"] = "2026-09-20T00:00:00Z"
    assert (await ctrl._do_create_serialized(payload))["status"] != "created"
    assert (await store.inspect(request_id=str(row["request_id"])))["effects"] == []
    assert api.writes == []


@pytest.mark.asyncio
async def test_prepared_metadata_cannot_be_shed_to_downgrade_to_plain_inheritance(
    db, workspace
):
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict

    _, original, request, fresh, _ = await inheritance(
        db, workspace, allocation_lost=True
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET context=context #- '{vm,preparation}' #- '{vm,preparation_request}' WHERE id=$1",
            original["job_id"],
        )
    request.pop("preparation")
    with pytest.raises(VMCreationRetryConflict):
        await VMCreationPreflightStore(db).begin(
            job_id=request["job_id"], request=request, fresh_context=fresh
        )


@pytest.mark.asyncio
async def test_prepared_inheriting_job_can_replace_using_own_retirement(db, workspace):
    ctrl, api, _, _, _ = workspace
    store, _, request, fresh, _ = await inheritance(db, workspace, allocation_lost=True)
    _, first, payload = await admit_replacement(db, ctrl, store, request, fresh)
    assert (await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5))["status"] == "created"
    request, fresh, _, _, _ = await retire(db, workspace[:4], payload)
    _, second, payload = await admit_replacement(db, ctrl, store, request, fresh)
    assert second["admission_deadline"] == first["admission_deadline"]
    assert (await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5))["status"] == "created"
    assert "DataVolume" not in api.writes


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    ["missing_clone", "current_preparation", "original_receipt", "cache_rebuild"],
)
async def test_prepared_inheritance_refuses_missing_or_incompatible_durable_evidence(
    db, workspace, change
):
    from orchestrator.services.vm_creation_preflight import VMCreationPreflightStore
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict
    from shared.workspace_preparation import revision

    _, original, request, fresh, _ = await inheritance(
        db, workspace, allocation_lost=True
    )
    async with db.acquire() as conn:
        if change == "missing_clone":
            await conn.execute(
                "DELETE FROM vm_creation_effects WHERE request_id=$1 AND effect_kind='rootdisk'",
                original["request_id"],
            )
        elif change == "original_receipt":
            await conn.execute(
                "UPDATE jobs SET context=jsonb_set(context,'{vm,preparation,pvcUid}',to_jsonb($2::text)) WHERE id=$1",
                original["job_id"],
                str(uuid4()),
            )
    if change == "current_preparation":
        request.pop("preparation")
    elif change == "cache_rebuild":
        prep = request["preparation"]
        prep["cache"] = "Rebuild"
        prep["revision"] = revision({k: v for k, v in prep.items() if k != "revision"})
    with pytest.raises(VMCreationRetryConflict):
        await VMCreationPreflightStore(db).begin(
            job_id=request["job_id"], request=request, fresh_context=fresh
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["origin_nonce", "target_dv"])
async def test_signed_inherited_origin_cannot_replace_full_durable_clone_identity(
    db, workspace, change
):
    from shared.vm_creation_issuance import (
        verify_creation_carrier,
        seal_creation_carrier,
    )
    from vm_controller.creation_actuation import CreationActuator

    ctrl, api, _, _, _ = workspace
    store, _, request, fresh, _ = await inheritance(db, workspace)
    _, row, payload = await admit_replacement(db, ctrl, store, request, fresh)
    authority = ctrl._workspace_cleanup_authority_request
    attempted = False

    async def forge(path, body, *, operation):
        nonlocal attempted
        if operation == "creation_retry_begin_effect":
            carrier = body["carrier"]
            values = verify_creation_carrier(
                carrier, secret=CreationActuator(ctrl).secret
            )
            if values["effect_kind"] == "rootdisk":
                attempted = True
                if change == "origin_nonce":
                    values["rootdisk_source"]["inherited_origin"]["effect_nonce"] = str(
                        uuid4()
                    )
                else:
                    values["rootdisk_source"]["retained_root"]["dv_uid"] = str(uuid4())
                body["carrier"] = seal_creation_carrier(
                    values,
                    namespace=carrier["metadata"]["namespace"],
                    uid=carrier["metadata"]["uid"],
                    resource_version=carrier["metadata"]["resourceVersion"],
                    secret=CreationActuator(ctrl).secret,
                )
        return await authority(path, body, operation=operation)

    ctrl._workspace_cleanup_authority_request = forge
    assert (await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5))["status"] != "created"
    assert attempted
    assert len((await store.inspect(request_id=str(row["request_id"])))["effects"]) == 1
    assert (
        "DataVolume" not in api.writes
        and "Secret" not in api.writes
        and "VirtualMachine" not in api.writes
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_inherited_prepared_lost_vm_reply_retains_exact_original_receipt(
    db, workspace, cancelled
):
    ctrl, api, _, _, _ = workspace
    store, _, request, fresh, source = await inheritance(
        db, workspace, allocation_lost=True
    )
    _, row, payload = await admit_replacement(db, ctrl, store, request, fresh)
    api.lost.add("VirtualMachine")
    for _ in range(4):
        assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
        if "VirtualMachine" in api.writes:
            break
    assert api.writes.count("VirtualMachine") == 1
    if cancelled:
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET status='cancelled' WHERE id=$1", row["job_id"]
            )
            await conn.execute(
                "UPDATE srw_workspace_instances SET status='Deleting' WHERE id=$1",
                UUID(request["workspace_storage"]["uid"]),
            )
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "created"
    assert result["preparation"]["buildUid"] == source["artifact"]["uid"]
    assert (await store.inspect(request_id=str(row["request_id"])))["state"] == (
        "settled" if cancelled else "succeeded"
    )
    async with db.acquire() as conn:
        context = json.loads(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", row["job_id"])
        )
        assert context["vm"]["preparation"] == result["preparation"]
        assert context["vm"]["preparation_request"] == request["preparation"]
    assert api.writes.count("VirtualMachine") == 1 and "DataVolume" not in api.writes


@pytest.mark.asyncio
async def test_inherited_prepared_completion_is_rechecked_after_vm_grant(db, workspace):
    from shared.vm_creation_issuance import verify_creation_carrier
    from vm_controller.creation_actuation import CreationActuator

    ctrl, api, _, _, _ = workspace
    store, _, request, fresh, _ = await inheritance(db, workspace, allocation_lost=True)
    _, row, payload = await admit_replacement(db, ctrl, store, request, fresh)
    authority = ctrl._workspace_cleanup_authority_request
    changed = False
    surrenders = []
    retained_name = storage_name(request["workspace_storage"])
    retained_uid = api.read("DataVolume", retained_name)["metadata"]["uid"]
    retained_pvc_uid = api.read("PersistentVolumeClaim", retained_name)["metadata"][
        "uid"
    ]

    async def drift_after_grant(path, body, *, operation):
        nonlocal changed
        result = await authority(path, body, operation=operation)
        if operation == "creation_retry_begin_effect":
            values = verify_creation_carrier(
                body["carrier"], secret=CreationActuator(ctrl).secret
            )
            if values["effect_kind"] == "vm":
                changed = True
                api.objects["DataVolume", storage_name(request["workspace_storage"])][
                    "status"
                ]["phase"] = "CloneInProgress"
        elif operation == "creation_retry_record_not_attempted":
            surrenders.append((body["effect_nonce"], body["reason"]))
        return result

    ctrl._workspace_cleanup_authority_request = drift_after_grant
    for _ in range(4):
        result = await ctrl._do_create_serialized(payload)
        if changed:
            break
    assert result["status"] != "created"
    assert changed and "VirtualMachine" not in api.writes
    current = await store.inspect(request_id=str(row["request_id"]))
    effect = current["effects"][-1]
    assert effect["carrier_intent"]["effect_kind"] == "vm"
    assert effect["state"] == "rejected"
    assert effect["evidence"] == {
        "outcome": "not_attempted",
        "reason": "creation_rootdisk_source_unproven",
    }
    assert surrenders == [
        (effect["carrier_intent"]["effect_nonce"], "creation_rootdisk_source_unproven")
    ]
    assert current["state"] == "attention"
    assert current["reason"] == "vm_creation_retry_blocked"
    assert "issuer_receipt" not in str(current)
    assert api.read("DataVolume", retained_name)["metadata"]["uid"] == retained_uid
    assert api.read("PersistentVolumeClaim", retained_name)["metadata"][
        "uid"
    ] == retained_pvc_uid
    assert (
        api.read("Secret", "agent-vm-" + request["job_id"] + "-cloudinit") is not None
    )


@pytest.mark.asyncio
async def test_three_job_prepared_chain_preserves_first_clone_and_exact_previous_handoff(
    db, workspace
):
    ctrl, api, _, _, _ = workspace
    store, original, request, fresh, source = await inheritance(
        db, workspace, allocation_lost=True
    )
    _, second, payload = await admit_replacement(db, ctrl, store, request, fresh)
    assert (await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5))["status"] == "created"
    request, fresh = await next_job(db, workspace, second, payload)
    proof, third, payload = await admit_replacement(db, ctrl, store, request, fresh)
    assert proof["predecessor_evidence"]["previous_job_id"] == str(second["job_id"])
    assert proof["predecessor_evidence"]["prepared_origin"]["request_id"] == str(
        original["request_id"]
    )
    result = await poll_until_terminal(ctrl._do_create_serialized, payload, limit=5)
    assert result["status"] == "created", result
    assert result["preparation"]["buildUid"] == source["artifact"]["uid"]
    assert "DataVolume" not in api.writes
