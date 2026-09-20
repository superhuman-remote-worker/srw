"""Completed retained clones release only source holds, never their attachment."""

from copy import deepcopy

import pytest

from tests.test_vm_creation_prepared_attachment_real_postgres import (
    setup as _setup_fixture,
    prepared as _prepared_fixture,
    workspace as _workspace_fixture,
    db as _db_fixture,
    postgres_db_fixture,  # noqa: F401
    pg_dsn,  # noqa: F401
    _schema_applied,  # noqa: F401
)
from tests.test_vm_creation_attachment_bridge_real_postgres import bridge
from tests.test_vm_creation_prepared_actuation import finish_prepared
from shared.vm_creation_disposition import disposition_identity
from shared.vm_creation_issuance import verify_creation_carrier
from shared.vm_workspace_storage import storage_name
from vm_controller.creation_actuation import CreationActuator
from vm_controller.creation_disposition import CreationDisposer
from vm_controller.creation_sources import pins
from vm_controller.workspace_preparation import allocation_name, creation_held

setup, prepared, workspace, db = (
    _setup_fixture,
    _prepared_fixture,
    _workspace_fixture,
    _db_fixture,
)


@pytest.mark.asyncio
async def test_retained_completed_clone_releases_source_but_keeps_exact_attachment(
    db, workspace
):
    ctrl, api, _, payload, service = workspace
    store, row = await bridge(db, workspace[:4])
    original = ctrl._workspace_cleanup_authority_request

    async def authority(path, body, *, operation):
        if (
            path.endswith("begin-effect")
            and verify_creation_carrier(
                body["carrier"], secret=CreationActuator(ctrl).secret
            )["effect_kind"]
            == "cloud_init"
        ):
            raise TimeoutError("before cloud-init grant")
        return await original(path, body, operation=operation)

    ctrl._workspace_cleanup_authority_request = authority
    assert (await finish_prepared(workspace))["status"] == "creation_pending"
    current = await store.inspect(request_id=str(row["request_id"]))
    assert len(current["effects"]) == 2
    assert await store.db.cancel_job(current["job_id"])
    root = storage_name(payload["workspace_storage"])
    original_lease = deepcopy(api.read("Lease", root))
    allocation = await service.store.get(allocation_name(payload["preparation"]))
    source = allocation.state["creation_source"]
    ctrl.k8s_client.list_namespaced_custom_object = lambda **_: {
        "metadata": {"resourceVersion": "1"},
        "items": [],
    }
    ctrl.core_api.list_namespaced_pod = lambda **_: {
        "metadata": {"resourceVersion": "1"},
        "items": [],
    }
    await CreationDisposer(ctrl).run(disposition_identity(current))
    assert creation_held(await service.store.get(allocation.name))
    assert (
        pins(api.read("DataVolume", source["name"]))[current["request_id"]]["state"]
        == "active"
    )
    api.objects["DataVolume", root]["status"] = {"phase": "Succeeded"}
    api.objects["PersistentVolumeClaim", root]["status"] = {"phase": "Bound"}
    await CreationDisposer(ctrl)._run(disposition_identity(current), {})
    allocation = await service.store.get(allocation.name)
    assert not creation_held(allocation)
    assert allocation.state["phase"] == "Cancelled"
    assert "creation_root" not in allocation.state
    assert api.read("Lease", root) == original_lease
    assert (await store.inspect(request_id=current["request_id"]))[
        "state"
    ] == "cancel_requested"
    assert (
        pins(api.read("DataVolume", source["name"]))[current["request_id"]]["state"]
        == "disposed"
    )
