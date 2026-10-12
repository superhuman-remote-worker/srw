"""A cancelled no-effect Resume disposes C while preserving B's exact disk."""

import json
from uuid import UUID, uuid4

import pytest

from orchestrator.services.vm_creation_disposition_store import (
    VMCreationDispositionStore,
)
from orchestrator.services.vm_creation_retry_store import VMCreationRetryStore
from shared.vm_creation_issuance import seal_creation_carrier
from shared.vm_creation_source_completion import source_completion
from tests import test_vm_job_cancel_retention_real_postgres as retention_fixture
from tests.test_vm_job_retained_resume_real_postgres import (
    _base_db,  # noqa: F401
    _db_fixture,  # noqa: F401
    _schema_applied,  # noqa: F401
    _pre_ssh_db,  # noqa: F401
    _retention_db,  # noqa: F401
    db as _resume_db,
    enabled,  # noqa: F401
    pg_dsn,  # noqa: F401
    postgres_db_fixture,  # noqa: F401
    pre_ssh_schema,  # noqa: F401
    resume_schema,  # noqa: F401
    retention_schema,  # noqa: F401
    whole_schema,  # noqa: F401
    claimed_resume,
    authorize_resume,
)


SECRET = b"retained-resume-disposition-test-secret"
db = _resume_db


@pytest.mark.asyncio
async def test_no_effect_c_freeze_record_and_settle_keeps_b_disk(db, monkeypatch):
    original_preflight = retention_fixture.preflight

    def canonical_preflight(state):
        return {
            **original_preflight(state),
            "pvc_name": f"agent-vm-{state['job_id']}-rootdisk",
        }

    monkeypatch.setattr(retention_fixture, "preflight", canonical_preflight)
    monkeypatch.setenv("VM_LIFECYCLE_HMAC_SECRET", SECRET.decode())
    state = await claimed_resume(db)
    permit = await authorize_resume(db, state)
    retry = state["retry"]
    request_id = str(retry["request_id"])
    pvc_uid = str(retry["expected_pvc_uid"])
    dv_uid = state["preflight"]["dv_uid"]
    values = {
        "version": 4,
        "resource_grant": permit["resource_grant"],
        "rootdisk_source": {"kind": "retained", "pvc_uid": pvc_uid},
        "source": "controller_vm_create",
        "admission_id": str(permit["admission_id"]),
        "reservation_request_id": permit["request_id"],
        "intent_digest": permit["intent_digest"],
        "retry_request_id": request_id,
        "job_id": state["job_id"],
        "provision_generation": str(retry["provision_generation"]),
        "request_digest": retry["request_digest"],
        "controller_configuration_digest": retry["controller_configuration_digest"],
        "expected_pvc_uid": pvc_uid,
        "retained_dv_uid": dv_uid,
        "current_dv_uid": None,
        "current_pvc_uid": None,
        "current_secret_uid": None,
        "effect_kind": "rootdisk",
        "effect_nonce": str(uuid4()),
        "object_name": state["preflight"]["pvc_name"],
    }
    carrier = seal_creation_carrier(
        values,
        namespace=retry["controller_configuration"]["namespace"],
        uid=str(uuid4()),
        resource_version="1",
        secret=SECRET,
    )
    assert (await db.cancel_stateless_job(state["job_id"]))[0]
    service = VMCreationDispositionStore(VMCreationRetryStore(db))
    frozen = await service.freeze(request_id=request_id, carrier=carrier)
    assert frozen["frozen"] is True
    disposition = frozen["disposition"]
    assert disposition["objects"] == {}
    assert disposition["effects"] == []
    assert disposition["disk_policy"] == "retain"

    root = await service.authorize(
        request_id=request_id, carrier=carrier, stage="rootdisk"
    )
    assert root["operation"] == "retain_inherited_rootdisk"
    assert root["resource"] == {
        "name": state["preflight"]["pvc_name"],
        "namespace": state["preflight"]["namespace"],
        "uid": dv_uid,
        "pvc_uid": pvc_uid,
    }
    assert await service.record(
        request_id=request_id,
        carrier=carrier,
        stage="rootdisk",
        evidence=root["completion"],
    ) == {"recorded": True, "stage": "rootdisk", "evidence": root["completion"]}
    cloud = await service.authorize(
        request_id=request_id, carrier=carrier, stage="cloud_init"
    )
    assert cloud["operation"] == "confirm_absent"
    await service.record(
        request_id=request_id,
        carrier=carrier,
        stage="cloud_init",
        evidence=cloud["completion"],
    )
    source = await service.authorize(
        request_id=request_id, carrier=carrier, stage="source"
    )
    assert source["plan"]["source"] is None
    await service.record(
        request_id=request_id,
        carrier=carrier,
        stage="source",
        evidence=source_completion(source["plan"]),
    )
    attachment = await service.authorize(
        request_id=request_id, carrier=carrier, stage="workspace_attachment"
    )
    assert attachment["plan"]["outcome"] == "not_applicable"
    await service.record(
        request_id=request_id,
        carrier=carrier,
        stage="workspace_attachment",
        evidence=None,
    )
    assert await service.settle(request_id=request_id, carrier=carrier) == {
        "settled": True,
        "disposition": "creation_disposed",
    }
    assert await service.settle(request_id=request_id, carrier=carrier) == {
        "settled": True,
        "disposition": "creation_disposed",
    }
    async with db.acquire() as conn:
        actual = await conn.fetchrow(
            "SELECT state,reason,cancellation_disposition,cancellation_completion "
            "FROM vm_creation_retries WHERE request_id=$1",
            UUID(request_id),
        )
        assert actual["state"] == "settled"
        assert actual["reason"] == "creation_disposed"
        assert json.loads(actual["cancellation_disposition"])["objects"] == {}
        assert (
            json.loads(actual["cancellation_completion"])["rootdisk"]
            == root["completion"]
        )
        assert not await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM vm_creation_effects WHERE request_id=$1)",
            UUID(request_id),
        )
        assert await conn.fetchval(
            "SELECT completed_at IS NOT NULL AND outcome='creation_disposed' "
            "FROM vm_workspace_cleanup_admissions WHERE id=$1",
            permit["admission_id"],
        )
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM vm_workspace_cleanup_admissions "
                "WHERE parent_admission_id=$1",
                permit["admission_id"],
            )
            == 0
        )
        assert (
            await conn.fetchval(
                "SELECT state FROM vm_resource_reservations WHERE request_id=$1",
                UUID(request_id),
            )
            == "released"
        )
        assert await conn.fetchval(
            "SELECT completed_at IS NOT NULL AND outcome='completed' "
            "FROM vm_workspace_cleanup_admissions WHERE id=$1",
            state["retention"].admission_id,
        )
