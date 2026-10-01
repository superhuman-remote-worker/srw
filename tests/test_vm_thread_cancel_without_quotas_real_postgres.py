"""Durable thread cancellation retains authority when optional quotas are off."""

import json

import pytest

from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
from tests.test_pinned_vm_initial_binding_real_postgres import (
    _bind_cold_agent,
    _initial_vm,
)
from tests.test_vm_creation_actuation import SECRET
from tests.test_vm_resource_configuration import whole_launcher_configuration
from tests.test_vm_resource_thread_source_real_postgres import (
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    db as _db,
    pg_dsn,  # noqa: F401
    thread_schema,  # noqa: F401
)

db = _db


async def cancelled_source(db, monkeypatch):
    thread_id, _, override, dependencies = await _initial_vm(
        db, monkeypatch, native=True
    )
    current = await _bind_cold_agent(db, thread_id)
    configuration = whole_launcher_configuration()
    configuration.update(
        version=1, namespace="workers", storage_class="local", golden_enabled=True
    )
    configuration.pop("resource_admission")
    monkeypatch.delenv("VM_RESOURCE_ADMISSION_CONFIG")
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", SECRET.decode())

    async def resolve(_client, request, *, secret):
        return {"request": request, "controller_configuration": configuration}

    monkeypatch.setattr(
        "orchestrator.services.vm_creation_transport.resolve_vm_creation_configuration",
        resolve,
    )
    assert await dependencies.vm_provisioner.create_thread_vm(
        str(thread_id),
        vm_image=override["workspace"]["vm"]["image"],
        expected_runtime_generation=str(current["runtime_generation"]),
        expected_agent_id=str(current["agent_id"]),
        expected_attach_token=str(current["runtime_attach_token"]),
    )
    source = await db.fetchrow(
        "SELECT * FROM vm_creation_retries WHERE thread_id=$1", thread_id
    )
    retry = VMCreationRetryStore(db)
    claim = (await retry.claim_due(limit=1))[0]
    grant = await retry.authorize_controller(
        request_id=str(source["request_id"]),
        claim_token=str(claim["claim_token"]),
        observed={
            "job_id": str(thread_id),
            "provision_generation": str(source["provision_generation"]),
            "request_digest": source["request_digest"],
            "controller_configuration_digest": source[
                "controller_configuration_digest"
            ],
            "expected_pvc_uid": None,
        },
    )
    assert grant["allowed"] is True
    retirement = await db.begin_pinned_thread_retirement(str(thread_id), permanent=True)
    assert retirement["state"] == "pending"
    assert await db.authorize_pinned_thread_retirement(
        str(thread_id),
        token=retirement["token"],
        generation=retirement["generation"],
        settle_status="ended",
    )
    row = await retry.inspect(request_id=str(source["request_id"]))
    assert row["state"] == "cancel_requested"
    assert row["creation_admission_id"] == str(grant["admission_id"])
    assert row["creation_carrier_uid"] is None and row["effects"] == []
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_resource_reservations WHERE request_id=$1",
            source["request_id"],
        )
        == 0
    )
    return retry, row, current, retirement


@pytest.mark.asyncio
async def test_cancel_with_original_admission_without_quotas_keeps_source_obligation(
    db, monkeypatch
):
    retry, row, current, retirement = await cancelled_source(db, monkeypatch)
    # An admission may already have published a golden pin before its create
    # Lease/effect. Empty effects alone must not bypass that obligation.
    assert await retry.settle_never_issued(request_id=row["request_id"]) == {
        "settled": False,
        "reason": "creation_source_unresolved",
    }
    prepared = await retry.prepare_disposition(request_id=row["request_id"])
    assert prepared["actuation_allowed"] is False
    intent = prepared["carrier_intent"]
    assert intent["version"] == 2
    assert intent["kind"] == "thread_creation_cancel"
    assert intent["admission_id"] == row["creation_admission_id"]
    assert intent["thread_runtime_generation"] == str(current["runtime_generation"])
    assert intent["thread_agent_id"] == str(current["agent_id"])
    assert intent["thread_attach_token"] == str(current["runtime_attach_token"])
    assert intent["retirement_token"] == retirement["token"]
    assert intent["source_pin_key"] == row["request_id"]
    assert not {
        "reservation_id",
        "reservation_revision",
        "reservation_cluster_id",
        "reservation_policy_digest",
        "effect_nonce",
        "effect_kind",
        "resource_grant",
    }.intersection(intent)
    owner = await db.get_thread(str(current["id"]))
    metadata = json.loads(owner["metadata"])
    assert metadata["vm"]["creation_request_id"] == row["request_id"]
    assert metadata["vm"].get("vm_uid") is None
    assert owner["runtime_retirement_token"] is not None
