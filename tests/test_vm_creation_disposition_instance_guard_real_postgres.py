"""A frozen no-VM disposition keeps exact instance authority until full settlement."""

from uuid import UUID

import asyncpg
import pytest

from tests.test_vm_creation_attachment_bridge_real_postgres import (
    db as _db_fixture,
    setup as _setup_fixture,
    attached as _attached_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
    bridge,
)
from orchestrator.services.vm_creation_disposition_store import (
    VMCreationDispositionStore,
)
from shared.vm_creation_issuance import verify_creation_carrier
from vm_controller.creation_actuation import CreationActuator

db, setup, attached = _db_fixture, _setup_fixture, _attached_fixture


async def frozen_attachment(db, attached):
    ctrl, api, _, payload = attached
    retries, row = await bridge(db, attached)
    original = ctrl._workspace_cleanup_authority_request

    async def authority(path, body, *, operation):
        if (
            path.endswith("/begin-effect")
            and verify_creation_carrier(
                body["carrier"], secret=CreationActuator(ctrl).secret
            )["effect_kind"]
            == "rootdisk"
        ):
            raise TimeoutError("no root grant yet")
        return await original(path, body, operation=operation)

    ctrl._workspace_cleanup_authority_request = authority
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
    assert await db.cancel_job(str(row["job_id"]))
    current = await retries.inspect(request_id=str(row["request_id"]))
    carrier = api.read(
        "Lease", "srw-cleanup-" + UUID(current["creation_admission_id"]).hex
    )
    service = VMCreationDispositionStore(retries)
    disposition = (
        await service.freeze(request_id=current["request_id"], carrier=carrier)
    )["disposition"]
    assert disposition["objects"]["workspace_attach"]["uid"]
    assert "rootdisk" not in disposition["objects"]
    return service, current, carrier, disposition


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["commit", "immediate"])
async def test_native_instance_release_cannot_bypass_frozen_disposition(
    db, attached, boundary
):
    _, row, _, disposition = await frozen_attachment(db, attached)
    async with db.acquire() as conn:
        before = dict(
            await conn.fetchrow(
                "SELECT status,execution_id,generation,pvc_uid,backend_state FROM srw_workspace_instances WHERE id=$1",
                UUID(disposition["workspace_instance_id"]),
            )
        )
        with pytest.raises(asyncpg.CheckViolationError):
            async with conn.transaction():
                await conn.execute(
                    "UPDATE srw_workspace_instances SET status='Released',execution_id=NULL WHERE id=$1",
                    UUID(disposition["workspace_instance_id"]),
                )
                if boundary == "immediate":
                    await conn.execute("SET CONSTRAINTS ALL IMMEDIATE")
        assert (
            dict(
                await conn.fetchrow(
                    "SELECT status,execution_id,generation,pvc_uid,backend_state FROM srw_workspace_instances WHERE id=$1",
                    UUID(disposition["workspace_instance_id"]),
                )
            )
            == before
        )
