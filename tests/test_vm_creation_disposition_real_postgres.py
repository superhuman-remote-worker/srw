"""Cancellation snapshots preserve exact effects and hold the create admission."""

import asyncio
import json
from uuid import UUID, uuid4

import asyncpg
import pytest

from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict
from tests.test_vm_creation_effects_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    disk_observation,
    observed_creation,
    reserved,
    SECRET,
)

db = _db_fixture


async def freeze(store, row, carrier):
    from orchestrator.services.vm_creation_disposition_store import (
        VMCreationDispositionStore,
    )

    return await VMCreationDispositionStore(store).freeze(
        request_id=str(row["request_id"]), carrier=carrier
    )


async def cancelled_disk(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    observation = disk_observation(carrier)
    await store.observe_effect(
        request_id=str(row["request_id"]), carrier=carrier, observation=observation
    )
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    return store, row, carrier, observation


@pytest.mark.asyncio
async def test_cancelled_new_disk_freezes_exact_purge_identity_and_keeps_permit(
    db, monkeypatch
):
    store, row, carrier, observation = await cancelled_disk(db, monkeypatch)
    result = await freeze(store, row, carrier)
    assert result["frozen"] is True
    intent = result["disposition"]
    assert intent["disk_policy"] == "purge_new_job_disk"
    assert (
        intent["objects"]["rootdisk"]["uid"] == observation["object"]["metadata"]["uid"]
    )
    assert (
        intent["objects"]["rootdisk"]["pvc_uid"]
        == observation["pvc"]["metadata"]["uid"]
    )
    assert intent["request_id"] == str(row["request_id"])
    assert intent["source_resolution"] == "not_required"
    assert await freeze(store, row, carrier) == result
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions WHERE id=$1",
            UUID(intent["admission_id"]),
        )
        assert (
            await conn.fetchval(
                "SELECT state FROM vm_creation_retries WHERE request_id=$1",
                row["request_id"],
            )
            == "cancel_requested"
        )


@pytest.mark.asyncio
async def test_unknown_root_create_cannot_freeze_even_after_cancel(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    assert await freeze(store, row, carrier) == {
        "frozen": False,
        "reason": "creation_effect_unresolved",
    }


@pytest.mark.asyncio
async def test_observed_vm_requires_adoption_instead_of_partial_disposition(
    db, monkeypatch
):
    store, row, carrier, observations = await observed_creation(db, monkeypatch)
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    assert await freeze(store, row, carrier) == {
        "frozen": False,
        "reason": "creation_vm_requires_adoption",
    }
    assert (
        await store.settle_adopted(
            request_id=str(row["request_id"]),
            carrier=carrier,
            observations=observations,
        )
    )["settled"] is True


@pytest.mark.asyncio
async def test_freeze_requires_cancellation_and_exact_carrier(db, monkeypatch):
    store, row, _, carrier = await reserved(db, monkeypatch)
    with pytest.raises(VMCreationRetryConflict, match="job_not_cancelled"):
        await freeze(store, row, carrier)
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    carrier["metadata"]["uid"] = str(uuid4())
    with pytest.raises(ValueError):
        await freeze(store, row, carrier)


@pytest.mark.asyncio
async def test_parallel_freeze_returns_one_immutable_disposition(db, monkeypatch):
    store, row, carrier, _ = await cancelled_disk(db, monkeypatch)
    left, right = await asyncio.gather(
        freeze(store, row, carrier), freeze(store, row, carrier)
    )
    assert left == right


@pytest.mark.asyncio
async def test_database_disposition_is_write_once_and_cannot_resume(db, monkeypatch):
    store, row, carrier, _ = await cancelled_disk(db, monkeypatch)
    result = await freeze(store, row, carrier)
    changed = {**result["disposition"], "disk_policy": "retain"}
    async with db.acquire() as conn:
        for sql, args in (
            (
                "UPDATE vm_creation_retries SET cancellation_disposition=$2::jsonb WHERE request_id=$1",
                (row["request_id"], json.dumps(changed)),
            ),
            (
                "UPDATE vm_creation_retries SET cancellation_disposition=NULL WHERE request_id=$1",
                (row["request_id"],),
            ),
            (
                "UPDATE vm_creation_retries SET state='queued' WHERE request_id=$1",
                (row["request_id"],),
            ),
        ):
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(sql, *args)


@pytest.mark.asyncio
async def test_frozen_disposition_cannot_bypass_through_never_issued(db, monkeypatch):
    store, row, _, carrier = await reserved(db, monkeypatch)
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    assert (await freeze(store, row, carrier))["frozen"] is True
    assert await store.settle_never_issued(request_id=str(row["request_id"])) == {
        "settled": False,
        "reason": "creation_disposition_pending",
    }


@pytest.mark.asyncio
async def test_source_can_be_published_before_first_effect_and_keeps_authority(
    db, monkeypatch
):
    from vm_controller import controller as settings

    monkeypatch.setattr(settings, "VM_GOLDEN_IMAGE_ENABLED", True)
    store, row, _, carrier = await reserved(db, monkeypatch)
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    assert await store.settle_never_issued(request_id=str(row["request_id"])) == {
        "settled": False,
        "reason": "creation_source_unresolved",
    }
    intent = (await freeze(store, row, carrier))["disposition"]
    assert intent["effects"] == [] and intent["source"] is None
    assert intent["source_resolution"] == "unknown"


@pytest.mark.asyncio
async def test_missing_frozen_configuration_cannot_prove_no_source_hold(
    db, monkeypatch
):
    store, row, _, _ = await reserved(db, monkeypatch, configuration_proven=False)
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    assert await store.settle_never_issued(request_id=str(row["request_id"])) == {
        "settled": False,
        "reason": "creation_source_unresolved",
    }


@pytest.mark.asyncio
async def test_missing_golden_setting_cannot_prove_source_disabled(db, monkeypatch):
    from vm_controller import controller as settings

    monkeypatch.setattr(settings, "VM_GOLDEN_IMAGE_ENABLED", None)
    store, row, _, _ = await reserved(db, monkeypatch)
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    assert await store.settle_never_issued(request_id=str(row["request_id"])) == {
        "settled": False,
        "reason": "creation_source_unresolved",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("rejected", [False, True])
async def test_vm_grant_is_permanent_barrier_unless_api_definitively_rejected(
    db, monkeypatch, rejected
):
    from shared.vm_creation_issuance import (
        verify_creation_carrier,
        seal_creation_carrier,
    )

    store, row, carrier, observations = await observed_creation(
        db, monkeypatch, stop_after="cloud_init"
    )
    values = verify_creation_carrier(carrier, secret=SECRET)
    values.update(
        effect_kind="vm",
        effect_nonce=str(uuid4()),
        object_name=f"agent-vm-{row['job_id']}",
        current_secret_uid=observations["cloud_init"]["object"]["metadata"]["uid"],
    )
    carrier = seal_creation_carrier(
        values,
        namespace="agent-vms",
        uid=carrier["metadata"]["uid"],
        resource_version="4",
        secret=SECRET,
    )
    async with db.acquire() as conn:
        claim = await conn.fetchval(
            "SELECT claim_token FROM vm_creation_retries WHERE request_id=$1",
            row["request_id"],
        )
    await store.begin_effect(
        request_id=str(row["request_id"]), claim_token=str(claim), carrier=carrier
    )
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    if rejected:
        await store.observe_effect(
            request_id=str(row["request_id"]),
            carrier=carrier,
            observation={
                "outcome": "rejected",
                "api_status": {
                    "kind": "Status",
                    "apiVersion": "v1",
                    "status": "Failure",
                    "reason": "Invalid",
                    "code": 422,
                },
            },
        )
    result = await freeze(store, row, carrier)
    assert result["frozen"] is rejected
    if rejected:
        assert set(result["disposition"]["objects"]) == {"rootdisk", "cloud_init"}
        assert (
            result["disposition"]["objects"]["cloud_init"]["uid"]
            == values["current_secret_uid"]
        )
    else:
        assert result["reason"] == "creation_effect_unresolved"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "generation", "recipe"])
async def test_retained_workspace_policy_comes_from_exact_locked_instance(
    db, monkeypatch, change
):
    from tests.test_vm_creation_attachment_authority_real_postgres import admitted

    store, row, _, carrier, request = await admitted(db, monkeypatch)
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    uid = UUID(request["workspace_storage"]["uid"])
    async with db.acquire() as conn:
        if change == "generation":
            await conn.execute(
                "UPDATE srw_workspace_instances SET generation=generation+1 WHERE id=$1",
                uid,
            )
        elif change == "recipe":
            await conn.execute(
                'UPDATE srw_workspace_instances SET recipe=\'{"backend":"vm","retention":"Delete"}\' WHERE id=$1',
                uid,
            )
    if change:
        with pytest.raises(
            VMCreationRetryConflict, match="creation_attachment_binding_changed"
        ):
            await freeze(store, row, carrier)
    else:
        intent = (await freeze(store, row, carrier))["disposition"]
        assert intent["disk_policy"] == "retain"
        assert intent["workspace_instance_id"] == str(uid)
        assert intent["workspace_storage"] == request["workspace_storage"]


@pytest.mark.asyncio
async def test_effect_insert_and_progress_rewrites_rejected_after_freeze(
    db, monkeypatch
):
    store, row, carrier, _ = await cancelled_disk(db, monkeypatch)
    await freeze(store, row, carrier)
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "INSERT INTO vm_creation_effects(effect_nonce,request_id,effect_number,effect_kind,carrier_uid,carrier_namespace,carrier_intent) "
                "SELECT $2,request_id,2,'cloud_init',carrier_uid,carrier_namespace,carrier_intent FROM vm_creation_effects WHERE request_id=$1",
                row["request_id"],
                uuid4(),
            )
        await conn.execute(
            'UPDATE vm_creation_retries SET cancellation_progress=\'{"rootdisk":{"outcome":"deleted"}}\' WHERE request_id=$1',
            row["request_id"],
        )
        for value in ({}, {"rootdisk": {"outcome": "retained"}}, {"unexpected": {}}):
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute(
                    "UPDATE vm_creation_retries SET cancellation_progress=$2::jsonb WHERE request_id=$1",
                    row["request_id"],
                    json.dumps(value),
                )


@pytest.mark.asyncio
async def test_database_refuses_missing_disposition_identity(db, monkeypatch):
    _, row, _, _ = await cancelled_disk(db, monkeypatch)
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE vm_creation_retries SET cancellation_disposition='{}' WHERE request_id=$1",
                row["request_id"],
            )


@pytest.mark.asyncio
async def test_cancellation_racing_final_vm_grant_never_disposes_possible_vm(
    db, monkeypatch
):
    from shared.vm_creation_issuance import (
        verify_creation_carrier,
        seal_creation_carrier,
    )

    store, row, carrier, observations = await observed_creation(
        db, monkeypatch, stop_after="cloud_init"
    )
    values = verify_creation_carrier(carrier, secret=SECRET)
    values.update(
        effect_kind="vm",
        effect_nonce=str(uuid4()),
        object_name=f"agent-vm-{row['job_id']}",
        current_secret_uid=observations["cloud_init"]["object"]["metadata"]["uid"],
    )
    carrier = seal_creation_carrier(
        values,
        namespace="agent-vms",
        uid=carrier["metadata"]["uid"],
        resource_version="4",
        secret=SECRET,
    )
    async with db.acquire() as conn:
        claim = await conn.fetchval(
            "SELECT claim_token FROM vm_creation_retries WHERE request_id=$1",
            row["request_id"],
        )

    async def issue():
        try:
            return await store.begin_effect(
                request_id=str(row["request_id"]),
                claim_token=str(claim),
                carrier=carrier,
            )
        except VMCreationRetryConflict as exc:
            assert exc.reason in {"job_cancelled", "retry_claim_changed"}
            return {"actuation_allowed": False}

    async def cancel():
        await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
        return await freeze(store, row, carrier)

    granted, disposition = await asyncio.gather(issue(), cancel())
    assert disposition["frozen"] is not granted["actuation_allowed"]
    async with db.acquire() as conn:
        issued = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_creation_effects WHERE request_id=$1 AND effect_kind='vm')",
            row["request_id"],
        )
    assert issued is granted["actuation_allowed"]


@pytest.mark.asyncio
@pytest.mark.parametrize("observed", [False, True])
async def test_attachment_disposition_requires_exact_observed_lease(
    db, monkeypatch, observed
):
    from tests.test_vm_creation_attachment_authority_real_postgres import admitted
    from shared.vm_creation_issuance import verify_creation_carrier
    from shared.vm_workspace_storage import storage_labels, storage_name

    store, row, claim, carrier, request = await admitted(db, monkeypatch)
    await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    values = verify_creation_carrier(carrier, secret=SECRET)
    uid = str(uuid4())
    if observed:
        await store.observe_effect(
            request_id=str(row["request_id"]),
            carrier=carrier,
            observation={
                "outcome": "observed",
                "object": {
                    "apiVersion": "coordination.k8s.io/v1",
                    "kind": "Lease",
                    "metadata": {
                        "name": storage_name(request["workspace_storage"]),
                        "namespace": "agent-vms",
                        "uid": uid,
                        "resourceVersion": "19",
                        "labels": storage_labels(
                            request["workspace_storage"], request["job_id"]
                        ),
                        "annotations": {
                            "srw.io/vm-create-request-id": str(row["request_id"]),
                            "srw.io/vm-create-effect-nonce": values["effect_nonce"],
                            "srw.io/provision-generation": values[
                                "provision_generation"
                            ],
                        },
                    },
                },
            },
        )
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    result = await freeze(store, row, carrier)
    assert result["frozen"] is observed
    if observed:
        intent = result["disposition"]
        assert intent["disk_policy"] == "retain"
        assert intent["objects"]["workspace_attach"]["uid"] == uid
        assert intent["objects"]["workspace_attach"]["resource_version"] == "19"
        assert "vm" not in intent["objects"]


@pytest.mark.asyncio
async def test_definitively_rejected_clone_still_preserves_exact_source_hold(
    db, monkeypatch
):
    from vm_controller import controller as settings
    from shared.vm_creation_issuance import (
        verify_creation_carrier,
        seal_creation_carrier,
    )

    monkeypatch.setattr(settings, "VM_GOLDEN_IMAGE_ENABLED", True)
    store, row, claim, carrier = await reserved(db, monkeypatch)
    image = row["canonical_request"]["vm_image"]
    source_uid = str(uuid4())
    source = {
        "kind": "golden",
        "image": image,
        "namespace": "agent-vms",
        "name": settings._golden_name(image),
        "dv_uid": source_uid,
        "pvc_uid": str(uuid4()),
        "pvc_owner_dv_uid": source_uid,
        "image_ref": image,
        "registry_source": {"registry": {"url": "docker://" + image}},
        "storage": settings.VMController._golden_dv_manifest(
            None, settings._golden_name(image), image
        )["spec"]["storage"],
        "pvc_volume_mode": "Filesystem",
    }
    values = verify_creation_carrier(carrier, secret=SECRET)
    values.update(version=2, rootdisk_source=source)
    carrier = seal_creation_carrier(
        values,
        namespace="agent-vms",
        uid=carrier["metadata"]["uid"],
        resource_version="3",
        secret=SECRET,
    )
    await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    await store.observe_effect(
        request_id=str(row["request_id"]),
        carrier=carrier,
        observation={
            "outcome": "rejected",
            "api_status": {
                "kind": "Status",
                "apiVersion": "v1",
                "status": "Failure",
                "reason": "Invalid",
                "code": 422,
            },
        },
    )
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    intent = (await freeze(store, row, carrier))["disposition"]
    assert intent["source"] == source
    assert intent["source_resolution"] == "required"
    assert intent["objects"] == {}


@pytest.mark.asyncio
async def test_zero_effect_workspace_requires_instance_disposition(db, monkeypatch):
    from tests.test_vm_creation_attachment_authority_real_postgres import admitted

    store, row, _, _, _ = await admitted(db, monkeypatch)
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    assert await store.settle_never_issued(request_id=str(row["request_id"])) == {
        "settled": False,
        "reason": "creation_disposition_pending",
    }


@pytest.mark.asyncio
async def test_prepare_disposition_only_returns_existing_cancelled_admission(
    db, monkeypatch
):
    store, row, _, carrier = await reserved(db, monkeypatch)
    with pytest.raises(VMCreationRetryConflict, match="job_not_cancelled"):
        await store.prepare_disposition(request_id=str(row["request_id"]))
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    result = await store.prepare_disposition(request_id=str(row["request_id"]))
    assert result["actuation_allowed"] is False
    assert result["carrier_intent"]["admission_id"] == carrier["spec"]["holderIdentity"]
    assert result == await store.prepare_disposition(request_id=str(row["request_id"]))
    assert "claim_token" not in str(result)
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
                row["request_id"],
            )
            == 0
        )


@pytest.mark.asyncio
async def test_prepare_disposition_never_acquires_missing_admission(db):
    from tests.test_vm_creation_retry_real_postgres import admitted_job, admit
    from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore

    job, generation, proposal = await admitted_job(db)
    row = await admit(db, job, generation, proposal)
    await db.linearize_pinned_cancel(str(job), expected_status="paused")
    with pytest.raises(VMCreationRetryConflict, match="creation_reservation_changed"):
        await VMCreationRetryStore(db).prepare_disposition(
            request_id=str(row["request_id"])
        )
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_workspace_cleanup_admissions WHERE owner_kind='job' AND owner_id=$1",
                job,
            )
            == 0
        )
