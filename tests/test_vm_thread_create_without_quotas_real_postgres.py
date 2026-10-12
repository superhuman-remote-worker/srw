"""Quota-free durable startup must keep typed thread creation authority."""

from uuid import UUID, uuid4

import pytest

from shared.vm_creation_issuance import seal_creation_carrier, verify_creation_carrier
from orchestrator.services.vm_creation_retry_store import (
    VMCreationRetryConflict,
    VMCreationRetryStore,
)
from tests.test_vm_creation_actuation import SECRET, setup as _setup
from tests.test_vm_thread_cancel_without_quotas_real_postgres import cancelled_source
from tests.test_vm_resource_thread_source_real_postgres import (
    _base_db,  # noqa: F401
    _schema_applied,  # noqa: F401
    db as _db,
    pg_dsn,  # noqa: F401
    thread_schema,  # noqa: F401
)
from vm_controller.creation_actuation import CreationActuator

db = _db
setup = _setup


async def controller_proposal(db, setup, monkeypatch):
    retry, row, current, _ = await cancelled_source(
        db, monkeypatch, retire=False, golden_enabled=False
    )
    permit = await db.fetchrow(
        "SELECT id,request_id,intent_digest FROM vm_workspace_cleanup_admissions WHERE id=$1",
        UUID(row["creation_admission_id"]),
    )
    source = await db.fetchrow(
        "SELECT claim_token FROM vm_creation_retries WHERE request_id=$1",
        UUID(row["request_id"]),
    )
    reservation = {
        "admission_id": str(permit["id"]),
        "request_id": str(permit["request_id"]),
        "intent_digest": permit["intent_digest"],
    }
    ctrl, _, _, _ = setup
    values = CreationActuator(ctrl).values(
        row,
        reservation,
        "rootdisk",
        None,
        None,
        None,
        {"kind": "registry", "image": row["request"]["vm_image"]},
    )
    return retry, row, current, source, values


@pytest.mark.asyncio
async def test_controller_quota_free_typed_source_can_claim_one_rootdisk_effect(
    db, setup, monkeypatch
):
    retry, row, current, source, values = await controller_proposal(
        db, setup, monkeypatch
    )
    assert values["version"] == 2 and values["owner_kind"] == "thread"
    assert values["thread_runtime_generation"] == str(current["runtime_generation"])
    assert values["thread_agent_id"] == str(current["agent_id"])
    assert values["thread_attach_token"] == str(current["runtime_attach_token"])
    assert "resource_grant" not in values
    carrier = seal_creation_carrier(
        values,
        namespace="workers",
        uid=str(uuid4()),
        resource_version="1",
        secret=SECRET,
    )
    assert verify_creation_carrier(carrier, secret=SECRET) == values
    grant = await retry.begin_effect(
        request_id=row["request_id"],
        claim_token=str(source["claim_token"]),
        carrier=carrier,
    )
    assert grant["actuation_allowed"] is True
    repeated = await retry.begin_effect(
        request_id=row["request_id"],
        claim_token=str(source["claim_token"]),
        carrier=carrier,
    )
    assert repeated["actuation_allowed"] is False
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1 AND state='issued'",
            UUID(row["request_id"]),
        )
        == 1
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    [
        "admission_id",
        "reservation_request_id",
        "intent_digest",
        "retry_request_id",
        "job_id",
        "thread_runtime_generation",
        "thread_agent_id",
        "thread_attach_token",
        "thread_wake_operation_id",
        "provision_generation",
        "request_digest",
        "controller_configuration_digest",
    ],
)
async def test_quota_free_creation_refuses_changed_captured_source(
    db, setup, monkeypatch, field
):
    retry, row, _, source, values = await controller_proposal(db, setup, monkeypatch)
    changed = dict(values)
    changed[field] = "sha256:" + "f" * 64 if field.endswith("digest") else str(uuid4())
    carrier = seal_creation_carrier(
        changed,
        namespace="workers",
        uid=str(uuid4()),
        resource_version="1",
        secret=SECRET,
    )
    with pytest.raises(VMCreationRetryConflict):
        await retry.begin_effect(
            request_id=row["request_id"],
            claim_token=str(source["claim_token"]),
            carrier=carrier,
        )
    assert (await retry.inspect(request_id=row["request_id"]))["effects"] == []


@pytest.mark.asyncio
async def test_quota_free_creation_keeps_legacy_and_extra_authority_refused(
    db, setup, monkeypatch
):
    _, _, _, _, values = await controller_proposal(db, setup, monkeypatch)
    legacy = {key: value for key, value in values.items() if key != "rootdisk_source"}
    legacy["version"] = 1
    with pytest.raises(ValueError, match="Thread creation requires a rootdisk source"):
        seal_creation_carrier(
            legacy,
            namespace="workers",
            uid=str(uuid4()),
            resource_version="1",
            secret=SECRET,
        )
    with pytest.raises(ValueError, match="Incomplete creation carrier intent"):
        seal_creation_carrier(
            {**values, "resource_grant": {}},
            namespace="workers",
            uid=str(uuid4()),
            resource_version="1",
            secret=SECRET,
        )


@pytest.mark.asyncio
async def test_resource_creation_cannot_downgrade_to_quota_free_typed_version(
    db, monkeypatch
):
    from tests.test_vm_resource_thread_source_real_postgres import (
        _adopted_charged_thread,
    )
    from orchestrator.services.vm_creation_retry_store import _json

    _, _, _, _, _, _, _, request_id, _, _, _ = await _adopted_charged_thread(
        db, monkeypatch, stop_after="rootdisk", adopt=False
    )
    row = await db.fetchrow(
        "SELECT claim_token FROM vm_creation_retries WHERE request_id=$1", request_id
    )
    original = _json(
        await db.fetchval(
            "SELECT carrier_intent FROM vm_creation_effects WHERE request_id=$1",
            request_id,
        )
    )
    assert original["version"] == 4
    downgraded = {
        key: value for key, value in original.items() if key != "resource_grant"
    }
    downgraded["version"] = 2
    carrier = seal_creation_carrier(
        downgraded,
        namespace="workers",
        uid=str(uuid4()),
        resource_version="1",
        secret=SECRET,
    )
    with pytest.raises(VMCreationRetryConflict, match="resource_carrier_changed"):
        await VMCreationRetryStore(db).begin_effect(
            request_id=str(request_id),
            claim_token=str(row["claim_token"]),
            carrier=carrier,
        )
    assert (
        await db.fetchval(
            "SELECT count(*) FROM vm_creation_effects WHERE request_id=$1", request_id
        )
        == 1
    )
