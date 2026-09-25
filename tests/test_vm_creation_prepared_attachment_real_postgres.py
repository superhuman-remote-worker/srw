"""Prepared workspace target and receipt through the actual four-stage authority."""

import json
from uuid import UUID

import pytest

from tests.test_vm_creation_prepared_attachment import (
    setup as _setup_fixture,
    prepared as _prepared_fixture,
    workspace as _workspace_fixture,
)
from tests.test_vm_creation_attachment_bridge_real_postgres import bridge
from tests.test_vm_creation_retry_real_postgres import (
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
)
from tests.test_vm_creation_prepared_actuation import finish_prepared
from vm_controller.workspace_preparation import allocation_name, creation_held
from vm_controller.creation_preparation import PreparedSources
from shared.vm_workspace_storage import storage_name

setup = _setup_fixture
prepared = _prepared_fixture
workspace = _workspace_fixture
db = _db_fixture


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_prepared_workspace_adopts_lost_vm_reply_and_releases_completed_clone(
    db, workspace, cancelled
):
    ctrl, api, _, payload, service = workspace
    store, row = await bridge(db, workspace[:4])
    api.lost.add("VirtualMachine")
    assert (await finish_prepared(workspace))["status"] == "creation_pending"
    for _ in range(5):
        if "VirtualMachine" in api.writes:
            break
        assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
    assert api.writes.count("VirtualMachine") == 1
    if cancelled:
        async with db.acquire() as conn:
            await conn.execute(
                "UPDATE jobs SET status='cancelled' WHERE id=$1", row["job_id"]
            )
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "created"
    observed = await store.inspect(request_id=str(row["request_id"]))
    assert len(observed["effects"]) == 4
    async with db.acquire() as conn:
        context = json.loads(
            await conn.fetchval("SELECT context FROM jobs WHERE id=$1", row["job_id"])
        )
        instance = await conn.fetchrow(
            "SELECT pvc_uid,status FROM srw_workspace_instances WHERE id=$1",
            UUID(payload["workspace_storage"]["uid"]),
        )
    assert context["vm"]["preparation"] == result["preparation"]
    assert (
        context["vm"]["workspace_storage"]["pvc_uid"]
        == instance["pvc_uid"]
        == result["rootdisk_pvc_uid"]
    )
    name = storage_name(payload["workspace_storage"])
    api.objects["DataVolume", name]["status"] = {"phase": "Succeeded"}
    api.objects["PersistentVolumeClaim", name]["status"] = {"phase": "Bound"}
    await PreparedSources(ctrl).release_completed(observed)
    assert not creation_held(
        await service.store.get(allocation_name(payload["preparation"]))
    )
    assert (
        api.writes.count("DataVolume") == 1 and api.writes.count("VirtualMachine") == 1
    )


@pytest.mark.asyncio
async def test_signed_prepared_source_cannot_substitute_another_workspace_target(
    db, workspace
):
    from uuid import uuid4
    from vm_controller.creation_actuation import CreationActuator
    from shared.vm_creation_issuance import (
        seal_creation_carrier,
        verify_creation_carrier,
    )

    ctrl, api, _, payload, service = workspace
    store, row = await bridge(db, workspace[:4])
    original = ctrl._workspace_cleanup_authority_request
    attempted = False

    async def authority(path, body, *, operation):
        nonlocal attempted
        if path.endswith("begin-effect"):
            carrier = body["carrier"]
            values = verify_creation_carrier(
                carrier, secret=CreationActuator(ctrl).secret
            )
            if values["effect_kind"] == "rootdisk":
                attempted = True
                target = values["rootdisk_source"]["target"]
                target["workspace_storage"]["uid"] = str(uuid4())
                target["name"] = storage_name(target["workspace_storage"])
                body["carrier"] = seal_creation_carrier(
                    values,
                    namespace=carrier["metadata"]["namespace"],
                    uid=carrier["metadata"]["uid"],
                    resource_version=carrier["metadata"]["resourceVersion"],
                    secret=CreationActuator(ctrl).secret,
                )
        return await original(path, body, operation=operation)

    ctrl._workspace_cleanup_authority_request = authority
    for _ in range(3):
        result = await ctrl._do_create_serialized(payload)
        if result.get("reason") == "preparation_wait":
            break
    assert result.get("reason") == "preparation_wait"
    service.store.finish(next(iter(service.store.pods)))
    for _ in range(5):
        result = await ctrl._do_create_serialized(payload)
        if attempted:
            break
    assert attempted and result["status"] != "created"
    observed = await store.inspect(request_id=str(row["request_id"]))
    assert len(observed["effects"]) == 1
    assert observed["effects"][0]["carrier_intent"]["effect_kind"] == "workspace_attach"
    assert "DataVolume" not in api.writes and "Secret" not in api.writes
