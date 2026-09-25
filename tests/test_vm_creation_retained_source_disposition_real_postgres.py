"""Completed retained clones release only source holds, never their attachment."""

from copy import deepcopy
from uuid import UUID

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
from shared.vm_creation_disposition import disposition_identity
from shared.vm_creation_issuance import verify_creation_carrier
from shared.vm_workspace_storage import storage_name
from vm_controller.creation_actuation import CreationActuator
from vm_controller.creation_actuation import CreationUnproven
from vm_controller.creation_disposition import CreationDisposer
from vm_controller.creation_sources import pins
from vm_controller.workspace_preparation import allocation_name, creation_held

setup, prepared, workspace, db = (
    _setup_fixture,
    _prepared_fixture,
    _workspace_fixture,
    _db_fixture,
)


async def prepared_at_cloud_init_refusal(workspace, store):
    """Stop at the real issued-rootdisk boundary before a Secret or VM exists."""
    ctrl, api, _, payload, service = workspace
    original = ctrl._workspace_cleanup_authority_request
    blocked = False

    async def authority(path, body, *, operation):
        nonlocal blocked
        if path.endswith("begin-effect") and verify_creation_carrier(
            body["carrier"], secret=CreationActuator(ctrl).secret
        )["effect_kind"] == "cloud_init":
            blocked = True
            raise TimeoutError("before cloud-init grant")
        return await original(path, body, operation=operation)

    ctrl._workspace_cleanup_authority_request = authority
    for _ in range(3):
        result = await ctrl._do_create_serialized(payload)
        if result.get("reason") == "preparation_wait":
            break
    assert result["status"] == "creation_pending"
    assert result["reason"] == "preparation_wait"
    service.store.finish(next(iter(service.store.pods)))
    for _ in range(10):
        result = await ctrl._do_create_serialized(payload)
        if blocked:
            current = await store.inspect(
                request_id=payload["creation_retry"]["request_id"]
            )
            assert result["status"] == "creation_pending"
            assert [
                (effect["carrier_intent"]["effect_kind"], effect["state"])
                for effect in current["effects"]
            ] == [("workspace_attach", "observed"), ("rootdisk", "observed")]
            assert "Secret" not in api.writes
            assert "VirtualMachine" not in api.writes
            return current
        assert result["status"] == "creation_pending"
    raise AssertionError("Cloud-init grant refusal was not reached")


@pytest.mark.asyncio
async def test_retained_completed_clone_releases_source_but_keeps_exact_attachment(
    db, workspace
):
    ctrl, api, _, payload, service = workspace
    store, row = await bridge(db, workspace[:4])
    current = await prepared_at_cloud_init_refusal(workspace, store)
    assert current["request_id"] == str(row["request_id"])
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
        "rootdisk"
        not in (await store.inspect(request_id=current["request_id"]))[
            "cancellation_completion"
        ]
    )
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
    completion = (await store.inspect(request_id=current["request_id"]))[
        "cancellation_completion"
    ]
    assert completion["rootdisk"]["kind"] == "rootdisk_retained"
    assert (
        completion["rootdisk"]["uid"] == api.read("DataVolume", root)["metadata"]["uid"]
    )
    assert (
        completion["rootdisk"]["pvc_uid"]
        == api.read("PersistentVolumeClaim", root)["metadata"]["uid"]
    )
    assert completion["cloud_init"]["kind"] == "secret_never_issued"
    assert "creation_root" not in allocation.state
    final_lease = api.read("Lease", root)
    assert final_lease["metadata"]["uid"] == original_lease["metadata"]["uid"]
    assert final_lease["metadata"]["labels"] == original_lease["metadata"]["labels"]
    assert final_lease["metadata"]["annotations"]["srw.io/detached"] == "true"
    assert (await store.inspect(request_id=current["request_id"]))[
        "state"
    ] == "settled"
    assert (
        pins(api.read("DataVolume", source["name"]))[current["request_id"]]["state"]
        == "disposed"
    )


@pytest.mark.asyncio
async def test_missing_retained_pvc_cannot_acquire_root_completion(db, workspace):
    ctrl, api, _, payload, _ = workspace
    store, row = await bridge(db, workspace[:4])
    current = await prepared_at_cloud_init_refusal(workspace, store)
    assert current["request_id"] == str(row["request_id"])
    assert await store.db.cancel_job(current["job_id"])
    root = storage_name(payload["workspace_storage"])
    ctrl.k8s_client.list_namespaced_custom_object = lambda **_: {
        "metadata": {"resourceVersion": "1"}, "items": []
    }
    ctrl.core_api.list_namespaced_pod = lambda **_: {
        "metadata": {"resourceVersion": "1"}, "items": []
    }
    await CreationDisposer(ctrl).run(disposition_identity(current))
    api.objects["DataVolume", root]["status"] = {"phase": "Succeeded"}
    del api.objects["PersistentVolumeClaim", root]
    from vm_controller.creation_disposition_resources import DispositionResources

    inspected = await store.inspect(request_id=current["request_id"])
    carrier = api.read("Lease", "srw-cleanup-" + UUID(inspected["creation_admission_id"]).hex)
    with pytest.raises(CreationUnproven, match="retained_disk_changed"):
        await DispositionResources(
            CreationActuator(ctrl), inspected, carrier, inspected["cancellation_disposition"]
        ).run()
    inspected = await store.inspect(request_id=current["request_id"])
    assert "rootdisk" not in inspected["cancellation_completion"]
    assert inspected["state"] == "cancel_requested"
