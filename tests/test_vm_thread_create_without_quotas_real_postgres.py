"""Quota-free durable startup must keep typed thread creation authority."""

from uuid import UUID, uuid4

import pytest

from shared.vm_creation_issuance import seal_creation_carrier, verify_creation_carrier
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


@pytest.mark.asyncio
async def test_controller_quota_free_typed_source_can_claim_one_rootdisk_effect(
    db, setup, monkeypatch
):
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
