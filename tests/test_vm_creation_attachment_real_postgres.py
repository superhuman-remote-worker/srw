"""Attachment carrier rollout remains closed before instance authority exists."""

import json
from uuid import UUID, uuid4

import asyncpg
import pytest

from tests.test_vm_creation_retry_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
)
from tests.test_vm_creation_effects_real_postgres import reserved, SECRET
from shared.vm_creation_issuance import seal_creation_carrier, verify_creation_carrier
from shared.vm_workspace_storage import storage_name, storage_labels
from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict

db = _db_fixture


async def attachment_reserved(db, monkeypatch):
    from tests import test_vm_creation_retry_real_postgres as fixtures
    from vm_controller import controller as settings

    monkeypatch.setattr(settings, "VM_GOLDEN_IMAGE_ENABLED", False)
    original = fixtures.build_vm_creation_request

    def build(**kwargs):
        request = original(**kwargs)
        request["workspace_storage"] = {
            "uid": str(uuid4()),
            "generation": 1,
            "pvc_uid": None,
            "owner_id": request["job_id"],
            "owner_kind": "job",
        }
        return request

    monkeypatch.setattr(fixtures, "build_vm_creation_request", build)
    store, row, claim, carrier = await reserved(db, monkeypatch)
    request = (await store.inspect(request_id=str(row["request_id"])))["request"]
    return store, row, claim, carrier, request


def attach_carrier(carrier, request):
    values = verify_creation_carrier(carrier, secret=SECRET)
    values.update(
        version=3,
        rootdisk_source=None,
        current_attachment_uid=None,
        workspace_attachment={
            "binding": request["workspace_storage"],
            "execution_id": request["job_id"],
            "action": "create",
            "prior": None,
        },
        effect_kind="workspace_attach",
        object_name=storage_name(request["workspace_storage"]),
    )
    return seal_creation_carrier(
        values,
        namespace=carrier["metadata"]["namespace"],
        uid=carrier["metadata"]["uid"],
        resource_version=carrier["metadata"]["resourceVersion"],
        secret=SECRET,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [1, 3])
async def test_attachment_fresh_grants_require_instance_authority(
    db, monkeypatch, version
):
    store, row, claim, carrier, request = await attachment_reserved(db, monkeypatch)
    if version == 3:
        carrier = attach_carrier(carrier, request)
    else:
        values = verify_creation_carrier(carrier, secret=SECRET)
        values["object_name"] = storage_name(request["workspace_storage"])
        carrier = seal_creation_carrier(
            values,
            namespace=carrier["metadata"]["namespace"],
            uid=carrier["metadata"]["uid"],
            resource_version=carrier["metadata"]["resourceVersion"],
            secret=SECRET,
        )
    with pytest.raises(
        VMCreationRetryConflict, match="creation_attachment_authority_unproven"
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
async def test_attachment_effect_schema_keeps_singleflight_and_immutable_history(
    db, monkeypatch
):
    store, row, _, carrier, request = await attachment_reserved(db, monkeypatch)
    carrier = attach_carrier(carrier, request)
    values = verify_creation_carrier(carrier, secret=SECRET)
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET creation_carrier_uid=$2,creation_carrier_namespace=$3 WHERE request_id=$1",
            row["request_id"],
            UUID(carrier["metadata"]["uid"]),
            carrier["metadata"]["namespace"],
        )
        await conn.execute(
            "INSERT INTO vm_creation_effects(effect_nonce,request_id,effect_number,effect_kind,carrier_uid,carrier_namespace,carrier_intent) VALUES($1,$2,1,'workspace_attach',$3,$4,$5::jsonb)",
            UUID(values["effect_nonce"]),
            row["request_id"],
            UUID(carrier["metadata"]["uid"]),
            carrier["metadata"]["namespace"],
            json.dumps(values),
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await conn.execute(
                "INSERT INTO vm_creation_effects(effect_nonce,request_id,effect_number,effect_kind,carrier_uid,carrier_namespace,carrier_intent) VALUES($1,$2,2,'workspace_attach',$3,$4,$5::jsonb)",
                uuid4(),
                row["request_id"],
                UUID(carrier["metadata"]["uid"]),
                carrier["metadata"]["namespace"],
                json.dumps(values),
            )
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE vm_creation_effects SET carrier_intent='{}' WHERE effect_nonce=$1",
                UUID(values["effect_nonce"]),
            )
    obj = {
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": {
            "name": values["object_name"],
            "namespace": carrier["metadata"]["namespace"],
            "uid": str(uuid4()),
            "resourceVersion": "19",
            "labels": storage_labels(request["workspace_storage"], request["job_id"]),
            "annotations": {
                "srw.io/vm-create-request-id": str(row["request_id"]),
                "srw.io/vm-create-effect-nonce": values["effect_nonce"],
                "srw.io/provision-generation": values["provision_generation"],
            },
        },
    }
    observed = await store.observe_effect(
        request_id=str(row["request_id"]),
        carrier=carrier,
        observation={"outcome": "observed", "object": obj},
    )
    assert observed == {"recorded": True, "effect_state": "observed"}
    async with db.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                "UPDATE vm_creation_effects SET evidence='{}' WHERE effect_nonce=$1",
                UUID(values["effect_nonce"]),
            )


@pytest.mark.asyncio
async def test_historical_binding_nonce_stays_observe_only(db, monkeypatch):
    store, row, claim, carrier, request = await attachment_reserved(db, monkeypatch)
    values = verify_creation_carrier(carrier, secret=SECRET)
    values["object_name"] = storage_name(request["workspace_storage"])
    carrier = seal_creation_carrier(
        values,
        namespace=carrier["metadata"]["namespace"],
        uid=carrier["metadata"]["uid"],
        resource_version=carrier["metadata"]["resourceVersion"],
        secret=SECRET,
    )
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE vm_creation_retries SET creation_carrier_uid=$2,creation_carrier_namespace=$3 WHERE request_id=$1",
            row["request_id"],
            UUID(carrier["metadata"]["uid"]),
            carrier["metadata"]["namespace"],
        )
        await conn.execute(
            "INSERT INTO vm_creation_effects(effect_nonce,request_id,effect_number,effect_kind,carrier_uid,carrier_namespace,carrier_intent) VALUES($1,$2,1,'rootdisk',$3,$4,$5::jsonb)",
            UUID(values["effect_nonce"]),
            row["request_id"],
            UUID(carrier["metadata"]["uid"]),
            carrier["metadata"]["namespace"],
            json.dumps(values),
        )
    assert await store.begin_effect(
        request_id=str(row["request_id"]),
        claim_token=str(claim["claim_token"]),
        carrier=carrier,
    ) == {
        "actuation_allowed": False,
        "disposition": "observe_only",
        "effect_state": "issued",
    }


@pytest.mark.asyncio
async def test_signed_attachment_cannot_replace_missing_canonical_binding(
    db, monkeypatch
):
    from vm_controller import controller as settings

    monkeypatch.setattr(settings, "VM_GOLDEN_IMAGE_ENABLED", False)
    store, row, claim, carrier = await reserved(db, monkeypatch)
    request = (await store.inspect(request_id=str(row["request_id"])))["request"]
    request["workspace_storage"] = {
        "uid": str(uuid4()),
        "generation": 1,
        "pvc_uid": None,
        "owner_id": request["job_id"],
        "owner_kind": "job",
    }
    with pytest.raises(VMCreationRetryConflict, match="creation_attachment_changed"):
        await store.begin_effect(
            request_id=str(row["request_id"]),
            claim_token=str(claim["claim_token"]),
            carrier=attach_carrier(carrier, request),
        )
