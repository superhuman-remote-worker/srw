"""Single-flight create effects using actual cleanup, job and claim locks."""

import asyncio
from uuid import UUID, uuid4
import pytest

from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryStore,
    VMCreationRetryConflict,
)
from shared.vm_creation_issuance import seal_creation_carrier
from tests.test_vm_creation_retry_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    admitted_job,
    admit,
    observed,
)

db = _db_fixture

SECRET = b"creation-issuance-test-secret-at-least-32-bytes"


async def reserved(
    db,
    monkeypatch,
    *,
    timeout=3600,
    configuration_proven=True,
    persistent_rootdisk=True,
):
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", SECRET.decode())
    from types import SimpleNamespace
    from vm_controller.creation_configuration import resolve_creation_configuration
    from vm_controller import controller as controller_settings

    monkeypatch.setattr(
        controller_settings, "VM_PERSISTENT_ROOTDISK", persistent_rootdisk
    )

    configuration = resolve_creation_configuration(
        SimpleNamespace(
            template_text="kind: VirtualMachine",
            cloud_init_text="#cloud-config",
            headscale=SimpleNamespace(is_available=True),
        ),
        {
            "job_id": str(uuid4()),
            "entity_type": "job",
            "provision_generation": str(uuid4()),
        },
    )["controller_configuration"]
    job, generation, proposal = await admitted_job(
        db,
        timeout=timeout,
        controller_configuration=configuration if configuration_proven else None,
    )
    row = await admit(db, job, generation, proposal)
    store = VMCreationRetryStore(db)
    claim = (await store.claim_due(limit=1))[0]
    reservation = await store.authorize_controller(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed=observed(job, generation, proposal),
    )
    values = dict(
        version=1,
        source="controller_vm_create",
        admission_id=str(reservation["admission_id"]),
        reservation_request_id=reservation["request_id"],
        intent_digest=reservation["intent_digest"],
        retry_request_id=str(row["request_id"]),
        job_id=str(job),
        provision_generation=str(generation),
        request_digest=proposal["request_digest"],
        controller_configuration_digest=proposal["controller_configuration_digest"],
        expected_pvc_uid=None,
        retained_dv_uid=None,
        current_dv_uid=None,
        current_pvc_uid=None,
        current_secret_uid=None,
        effect_kind="rootdisk",
        effect_nonce=str(uuid4()),
        object_name=f"agent-vm-{job}-rootdisk",
    )
    carrier = seal_creation_carrier(
        values,
        namespace="agent-vms",
        uid=str(uuid4()),
        resource_version="2",
        secret=SECRET,
    )
    return store, row, claim, carrier


@pytest.mark.asyncio
async def test_two_replicas_receive_only_one_effect_grant(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)

    async def begin():
        return await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=carrier,
        )

    results = await asyncio.gather(begin(), begin())
    assert sum(result["actuation_allowed"] for result in results) == 1
    assert {result["disposition"] for result in results} == {"issued", "observe_only"}


@pytest.mark.asyncio
async def test_unknown_issuance_survives_claim_expiry_and_cancellation(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET claim_expires_at=clock_timestamp()-interval '1 second' WHERE request_id=$1",
            row["request_id"],
        )
    new_claim = (await store.claim_due(limit=1))[0]
    replay = await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(new_claim["claim_token"]),
        carrier=carrier,
    )
    assert replay["actuation_allowed"] is False
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    result = await store.settle_never_issued(request_id=str(row["request_id"]))
    assert result == {"settled": False, "reason": "creation_effect_unresolved"}
    async with db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT completed_at IS NULL FROM vm_workspace_cleanup_admissions WHERE id=$1",
            row["creation_admission_id"] or UUID(carrier["spec"]["holderIdentity"]),
        )


@pytest.mark.asyncio
async def test_cancel_before_effect_grant_can_settle_never_issued(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    with pytest.raises(VMCreationRetryConflict, match="job_cancelled"):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=carrier,
        )
    assert await store.settle_never_issued(request_id=str(row["request_id"])) == {
        "settled": True,
        "disposition": "never_issued",
    }
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT state FROM vm_creation_retries WHERE request_id=$1",
                row["request_id"],
            )
            == "settled"
        )


@pytest.mark.asyncio
async def test_carrier_boolean_or_foreign_source_does_not_grant(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    with pytest.raises(ValueError):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier={"authenticated": True},
        )
    carrier["metadata"]["namespace"] = "foreign"
    with pytest.raises(ValueError):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=carrier,
        )


@pytest.mark.asyncio
async def test_generic_cleanup_completion_cannot_release_create_authority(
    db, monkeypatch
):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    from uuid import UUID

    assert (
        await store.cleanup.complete_cleanup_permit(
            UUID(carrier["spec"]["holderIdentity"]), outcome="adopted"
        )
        is False
    )


@pytest.mark.asyncio
async def test_definitive_api_rejection_allows_new_nonce_but_unknown_does_not(
    db, monkeypatch
):
    from shared.vm_creation_issuance import verify_creation_carrier

    store, row, claim, carrier = await reserved(db, monkeypatch)
    await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    new_values = {
        **verify_creation_carrier(carrier, secret=SECRET),
        "effect_nonce": str(uuid4()),
    }
    next_carrier = seal_creation_carrier(
        new_values,
        namespace="agent-vms",
        uid=carrier["metadata"]["uid"],
        resource_version="3",
        secret=SECRET,
    )
    with pytest.raises(VMCreationRetryConflict, match="creation_effect_unresolved"):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=next_carrier,
        )
    for observation in [
        {"outcome": "not_issued", "authenticated": True},
        {
            "outcome": "rejected",
            "api_status": {
                "kind": "Status",
                "apiVersion": "v1",
                "status": "Failure",
                "reason": "InternalError",
                "code": 500,
            },
        },
    ]:
        with pytest.raises(ValueError):
            await store.observe_effect(
                request_id=str(row["request_id"]),
                carrier=carrier,
                observation=observation,
            )
    assert await store.observe_effect(
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
    ) == {"recorded": True, "effect_state": "rejected"}
    assert (
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=next_carrier,
        )
    )["actuation_allowed"] is True


def disk_observation(carrier):
    from shared.vm_creation_issuance import (
        verify_creation_carrier,
        EFFECT_NONCE_ANNOTATION,
        REQUEST_ANNOTATION,
    )

    values = verify_creation_carrier(carrier, secret=SECRET)
    dv, pvc = str(uuid4()), str(uuid4())
    metadata = {
        "name": values["object_name"],
        "namespace": "agent-vms",
        "uid": dv,
        "labels": {"srw.io/owner-kind": "job", "srw.io/owner-id": values["job_id"]},
        "annotations": {
            EFFECT_NONCE_ANNOTATION: values["effect_nonce"],
            REQUEST_ANNOTATION: values["retry_request_id"],
            "srw.io/provision-generation": values["provision_generation"],
        },
    }
    return {
        "outcome": "observed",
        "object": {
            "apiVersion": "cdi.kubevirt.io/v1beta1",
            "kind": "DataVolume",
            "metadata": metadata,
        },
        "pvc": {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": values["object_name"],
                "namespace": "agent-vms",
                "uid": pvc,
                "ownerReferences": [{"kind": "DataVolume", "uid": dv}],
            },
        },
    }


@pytest.mark.asyncio
async def test_disk_observation_after_cancel_binds_identity_without_new_authority(
    db, monkeypatch
):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    await db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused")
    observation = disk_observation(carrier)
    assert await store.observe_effect(
        request_id=str(row["request_id"]), carrier=carrier, observation=observation
    ) == {"recorded": True, "effect_state": "observed"}
    assert await store.observe_effect(
        request_id=str(row["request_id"]), carrier=carrier, observation=observation
    ) == {"recorded": True, "effect_state": "observed"}
    changed = disk_observation(carrier)
    with pytest.raises(VMCreationRetryConflict):
        await store.observe_effect(
            request_id=str(row["request_id"]), carrier=carrier, observation=changed
        )
    assert (await store.settle_never_issued(request_id=str(row["request_id"])))[
        "settled"
    ] is False
    async with db.acquire() as conn:
        value = await conn.fetchrow(
            "SELECT state,observed_pvc_uid,boot_counted FROM vm_creation_retries WHERE request_id=$1",
            row["request_id"],
        )
        assert value["state"] == "cancel_requested"
        assert str(value["observed_pvc_uid"]) == observation["pvc"]["metadata"]["uid"]
        assert value["boot_counted"] is False


@pytest.mark.asyncio
async def test_next_stage_revalidates_exact_new_disk_and_completed_vm_never_recreates(
    db, monkeypatch
):
    from shared.vm_creation_issuance import (
        verify_creation_carrier,
        EFFECT_NONCE_ANNOTATION,
        REQUEST_ANNOTATION,
    )

    store, row, claim, carrier = await reserved(db, monkeypatch)
    await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    disk = disk_observation(carrier)
    await store.observe_effect(
        request_id=str(row["request_id"]), carrier=carrier, observation=disk
    )
    original = verify_creation_carrier(carrier, secret=SECRET)
    reservation = await store.authorize_controller(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(row["job_id"]),
            "provision_generation": str(row["provision_generation"]),
            "request_digest": row["request_digest"],
            "controller_configuration_digest": row["controller_configuration_digest"],
            "expected_pvc_uid": None,
        },
    )
    assert reservation["allowed"] is True
    assert str(reservation["admission_id"]) == original["admission_id"]
    secret_uid = None
    for kind, suffix in [("cloud_init", "-cloudinit"), ("vm", "")]:
        values = {
            **original,
            "effect_kind": kind,
            "effect_nonce": str(uuid4()),
            "object_name": f"agent-vm-{row['job_id']}{suffix}",
            "current_dv_uid": disk["object"]["metadata"]["uid"],
            "current_pvc_uid": disk["pvc"]["metadata"]["uid"],
            "current_secret_uid": secret_uid,
        }
        next_carrier = seal_creation_carrier(
            values,
            namespace="agent-vms",
            uid=carrier["metadata"]["uid"],
            resource_version="3",
            secret=SECRET,
        )
        wrong = {**values, "current_pvc_uid": str(uuid4())}
        wrong_carrier = seal_creation_carrier(
            wrong,
            namespace="agent-vms",
            uid=carrier["metadata"]["uid"],
            resource_version="4",
            secret=SECRET,
        )
        with pytest.raises(VMCreationRetryConflict, match="retained_disk_changed"):
            await store.begin_effect(
                request_id=str(row["request_id"]),
                claim_token=str(claim["claim_token"]),
                carrier=wrong_carrier,
            )
        assert (
            await store.begin_effect(
                request_id=str(row["request_id"]),
                claim_token=str(claim["claim_token"]),
                carrier=next_carrier,
            )
        )["actuation_allowed"]
        obj = {
            "apiVersion": "v1" if kind == "cloud_init" else "kubevirt.io/v1",
            "kind": "Secret" if kind == "cloud_init" else "VirtualMachine",
            "metadata": {
                "name": values["object_name"],
                "namespace": "agent-vms",
                "uid": str(uuid4()),
                "labels": {
                    "srw.io/owner-kind": "job",
                    "srw.io/owner-id": str(row["job_id"]),
                },
                "annotations": {
                    EFFECT_NONCE_ANNOTATION: values["effect_nonce"],
                    REQUEST_ANNOTATION: str(row["request_id"]),
                    "srw.io/provision-generation": str(row["provision_generation"]),
                    "srw.io/ssh-host-key-fingerprint": "SHA256:" + "A" * 43,
                },
            },
            "spec": {
                "template": {
                    "spec": {
                        "volumes": [
                            {
                                "name": "rootdisk",
                                "dataVolume": {
                                    "name": disk["object"]["metadata"]["name"]
                                },
                            }
                        ]
                    }
                }
            },
            "data": {"userdata": "must-not-persist-secret"},
        }
        if kind == "vm":
            from copy import deepcopy

            wrong_vm = deepcopy(obj)
            wrong_vm["spec"]["template"]["spec"]["volumes"].append(
                {
                    "name": "cloud-init",
                    "cloudInitNoCloud": {"secretRef": {"name": "foreign-secret"}},
                }
            )
            with pytest.raises(ValueError, match="cloud-init"):
                await store.observe_effect(
                    request_id=str(row["request_id"]),
                    carrier=next_carrier,
                    observation={"outcome": "observed", "object": wrong_vm},
                )
            obj["spec"]["template"]["spec"]["volumes"].append(
                {
                    "name": "cloud-init",
                    "cloudInitNoCloud": {
                        "secretRef": {"name": f"agent-vm-{row['job_id']}-cloudinit"}
                    },
                }
            )
        assert (
            await store.observe_effect(
                request_id=str(row["request_id"]),
                carrier=next_carrier,
                observation={"outcome": "observed", "object": obj},
            )
        )["recorded"]
        if kind == "cloud_init":
            secret_uid = obj["metadata"]["uid"]
        carrier = next_carrier
    replay = {**values, "effect_nonce": str(uuid4())}
    with pytest.raises(VMCreationRetryConflict, match="creation_already_admitted"):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=seal_creation_carrier(
                replay,
                namespace="agent-vms",
                uid=carrier["metadata"]["uid"],
                resource_version="9",
                secret=SECRET,
            ),
        )
    async with db.acquire() as conn:
        effects = await conn.fetch(
            "SELECT evidence::text FROM vm_creation_effects WHERE request_id=$1",
            row["request_id"],
        )
        assert all(
            "must-not-persist-secret" not in effect["evidence"] for effect in effects
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("expires", ["claim", "deadline"])
async def test_begin_effect_expiry_after_lock_wait_creates_no_grant(
    db, monkeypatch, expires
):
    store, row, claim, carrier = await reserved(
        db, monkeypatch, timeout=2 if expires == "deadline" else 3600
    )
    async with db.acquire() as conn:
        if expires == "claim":
            await conn.execute(
                "UPDATE vm_creation_retries SET claim_expires_at=clock_timestamp()+interval '1 second' WHERE request_id=$1",
                row["request_id"],
            )
        async with conn.transaction():
            await conn.execute(
                "SELECT id FROM jobs WHERE id=$1 FOR UPDATE", row["job_id"]
            )
            begin = asyncio.create_task(
                store.begin_effect(
                    request_id=str(row["request_id"]),
                    claim_token=str(claim["claim_token"]),
                    carrier=carrier,
                )
            )
            await asyncio.sleep(2.2 if expires == "deadline" else 1.3)
            assert not begin.done()
        with pytest.raises(
            VMCreationRetryConflict,
            match="job_admission_expired"
            if expires == "deadline"
            else "retry_claim_changed",
        ):
            await asyncio.wait_for(begin, timeout=5)
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
                row["request_id"],
            )
            == 0
        )


@pytest.mark.asyncio
async def test_carrier_and_admission_agreement_does_not_replace_full_intent_binding(
    db, monkeypatch
):
    from shared.vm_creation_issuance import verify_creation_carrier

    store, row, claim, carrier = await reserved(db, monkeypatch)
    digest = "sha256:" + "d" * 64
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_workspace_cleanup_admissions SET intent_digest=$2 WHERE id=$1",
            UUID(carrier["spec"]["holderIdentity"]),
            digest,
        )
    values = {
        **verify_creation_carrier(carrier, secret=SECRET),
        "intent_digest": digest,
    }
    changed = seal_creation_carrier(
        values,
        namespace="agent-vms",
        uid=carrier["metadata"]["uid"],
        resource_version="5",
        secret=SECRET,
    )
    with pytest.raises(VMCreationRetryConflict, match="creation_reservation_changed"):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=changed,
        )


@pytest.mark.asyncio
async def test_cancel_racing_single_effect_grant_never_loses_issuance(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    effect, cancelled = await asyncio.wait_for(
        asyncio.gather(
            store.begin_effect(
                request_id=str(row["request_id"]),
                claim_token=str(claim["claim_token"]),
                carrier=carrier,
            ),
            db.linearize_pinned_cancel(str(row["job_id"]), expected_status="paused"),
            return_exceptions=True,
        ),
        timeout=5,
    )
    assert cancelled is True
    result = await store.settle_never_issued(request_id=str(row["request_id"]))
    if isinstance(effect, VMCreationRetryConflict):
        assert result["settled"] is True
    else:
        assert effect["actuation_allowed"] is True
        assert result["settled"] is False


@pytest.mark.asyncio
async def test_completed_reservation_without_vm_never_grants_create(db, monkeypatch):
    store, row, claim, carrier = await reserved(db, monkeypatch)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_workspace_cleanup_admissions SET completed_at=clock_timestamp(),outcome='adopted' WHERE id=$1",
            UUID(carrier["spec"]["holderIdentity"]),
        )
    with pytest.raises(VMCreationRetryConflict, match="creation_reservation_changed"):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=carrier,
        )


@pytest.mark.asyncio
async def test_sealed_foreign_namespace_is_not_authority_for_frozen_configuration(
    db, monkeypatch
):
    from shared.vm_creation_issuance import verify_creation_carrier

    store, row, claim, carrier = await reserved(db, monkeypatch)
    foreign = seal_creation_carrier(
        verify_creation_carrier(carrier, secret=SECRET),
        namespace="foreign",
        uid=carrier["metadata"]["uid"],
        resource_version="8",
        secret=SECRET,
    )
    with pytest.raises(VMCreationRetryConflict, match="creation_configuration_changed"):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=foreign,
        )


@pytest.mark.asyncio
async def test_digest_only_record_cannot_receive_effect_grant(db, monkeypatch):
    store, row, claim, carrier = await reserved(
        db, monkeypatch, configuration_proven=False
    )
    with pytest.raises(
        VMCreationRetryConflict, match="creation_configuration_unproven"
    ):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=carrier,
        )
    async with db.acquire() as conn:
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1",
                row["request_id"],
            )
            == 0
        )


@pytest.mark.asyncio
async def test_database_rejects_carrier_effect_and_configuration_identity_mutation(
    db, monkeypatch
):
    import asyncpg
    from shared.vm_creation_issuance import verify_creation_carrier

    store, row, claim, carrier = await reserved(db, monkeypatch)
    await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        carrier=carrier,
    )
    nonce = UUID(verify_creation_carrier(carrier, secret=SECRET)["effect_nonce"])
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE vm_creation_effects SET effect_nonce=$2 WHERE effect_nonce=$1",
                nonce,
                uuid4(),
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE vm_creation_retries SET controller_configuration=NULL WHERE request_id=$1",
                row["request_id"],
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE vm_creation_retries SET creation_carrier_uid=$2 WHERE request_id=$1",
                row["request_id"],
                uuid4(),
            )


@pytest.mark.asyncio
async def test_nonpersistent_controller_configuration_cannot_grant_staged_creation(
    db, monkeypatch
):
    store, row, claim, carrier = await reserved(
        db, monkeypatch, persistent_rootdisk=False
    )
    with pytest.raises(VMCreationRetryConflict, match="retry_protocol_unavailable"):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=carrier,
        )
