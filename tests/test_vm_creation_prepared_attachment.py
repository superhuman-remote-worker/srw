"""Engine-produced prepared sources bind exact retained workspace targets."""

from copy import deepcopy
from uuid import uuid4

import pytest

from tests.test_vm_creation_prepared_actuation import (
    setup as _setup_fixture,
    prepared as _prepared_fixture,
    finish_prepared,
)
from tests.test_vm_creation_attachment_actuation import bind_attachment
from shared.vm_workspace_storage import storage_name
from vm_controller.workspace_preparation import allocation_name, creation_held
from vm_controller.creation_sources import pins
from vm_controller.creation_preparation import creation_binding

setup = _setup_fixture
prepared = _prepared_fixture


@pytest.fixture
def workspace(prepared):
    ctrl, api, authority, payload, service = prepared
    bind_attachment((ctrl, api, authority, payload))
    return ctrl, api, authority, payload, service


@pytest.mark.asyncio
async def test_engine_prepared_workspace_has_frozen_target_and_exact_completion(
    workspace,
):
    ctrl, api, authority, payload, service = workspace
    assert (await finish_prepared(workspace))["status"] == "created"
    allocation = await service.store.get(allocation_name(payload["preparation"]))
    source = allocation.state["creation_source"]
    target = allocation.state["creation_binding"]["target"]
    name = storage_name(payload["workspace_storage"])
    assert target == {
        "name": name,
        "namespace": service.store.namespace,
        "workspace_storage": payload["workspace_storage"],
    }
    assert source["target"] == target
    assert source["receipt"]["buildUid"] == source["artifact"]["uid"]
    assert creation_held(allocation)
    assert [e["carrier_intent"]["effect_kind"] for e in authority.row["effects"]] == [
        "workspace_attach",
        "rootdisk",
        "cloud_init",
        "vm",
    ]
    api.objects["DataVolume", name]["status"] = {"phase": "Succeeded"}
    api.objects["PersistentVolumeClaim", name]["status"] = {"phase": "Bound"}
    for _ in range(2):
        assert (await ctrl._do_create_serialized(payload))["status"] == "created"
    allocation = await service.store.get(allocation.name)
    assert not creation_held(allocation)
    assert allocation.state["creation_root"] == {
        "name": name,
        "dv_uid": api.read("DataVolume", name)["metadata"]["uid"],
        "pvc_uid": api.read("PersistentVolumeClaim", name)["metadata"]["uid"],
    }
    assert (
        pins(api.read("DataVolume", source["name"]))[authority.row["request_id"]][
            "state"
        ]
        == "released"
    )
    assert len(service.store.created_pods) == 1
    assert api.writes.count("DataVolume") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["name", "namespace", "uid", "generation", "owner", "missing", "extra"]
)
async def test_prepared_target_drift_is_refused_before_delivery(workspace, change):
    ctrl, api, authority, payload, service = workspace
    first = await ctrl._do_create_serialized(payload)
    assert first["reason"] == "preparation_wait"
    allocation = await service.store.get(allocation_name(payload["preparation"]))
    target = allocation.state["creation_binding"]["target"]
    if change in {"name", "namespace"}:
        target[change] = "other"
    elif change == "uid":
        target["workspace_storage"]["uid"] = str(uuid4())
    elif change == "generation":
        target["workspace_storage"]["generation"] = 2
    elif change == "owner":
        target["workspace_storage"]["owner_id"] = str(uuid4())
    elif change == "missing":
        del allocation.state["creation_binding"]["target"]
    else:
        target["extra"] = True
    await service.store.save(allocation, allocation.state)
    service.store.finish(next(iter(service.store.pods)))
    assert (await ctrl._do_create_serialized(payload))["status"] != "created"
    assert "DataVolume" not in api.writes and "Secret" not in api.writes
    assert len(authority.row["effects"]) == 1


def test_ordinary_creation_binding_keeps_three_field_shape(prepared):
    _, _, authority, _, _ = prepared
    assert set(creation_binding(authority.row)) == {
        "request_id",
        "provision_generation",
        "request_digest",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("lost", ["allocation_completion", "source_release"])
async def test_workspace_completion_lost_reply_preserves_then_releases_exact_holds(
    workspace, lost
):
    from vm_controller.creation_sources import PINS
    import json

    ctrl, api, authority, payload, service = workspace
    assert (await finish_prepared(workspace))["status"] == "created"
    name = storage_name(payload["workspace_storage"])
    api.objects["DataVolume", name]["status"] = {"phase": "Succeeded"}
    api.objects["PersistentVolumeClaim", name]["status"] = {"phase": "Bound"}
    allocation = await service.store.get(allocation_name(payload["preparation"]))
    source = allocation.state["creation_source"]
    failed = False
    if lost == "allocation_completion":
        original = service.store.save

        async def save(record, state):
            nonlocal failed
            result = await original(record, state)
            if (
                record.name == allocation.name
                and state.get("phase") == "Allocated"
                and not failed
            ):
                failed = True
                raise TimeoutError("completed allocation CAS reply lost")
            return result

        service.store.save = save
    else:
        original = ctrl.k8s_client.replace_namespaced_custom_object

        def replace(**kw):
            nonlocal failed
            result = original(**kw)
            data = json.loads(
                kw["body"]["metadata"].get("annotations", {}).get(PINS, "{}")
            )
            if (
                kw["name"] == source["name"]
                and data.get(authority.row["request_id"], {}).get("state") == "released"
                and not failed
            ):
                failed = True
                raise TimeoutError("source release CAS reply lost")
            return result

        ctrl.k8s_client.replace_namespaced_custom_object = replace
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_pending"
    assert failed
    assert (await ctrl._do_create_serialized(payload))["status"] == "created"
    allocation = await service.store.get(allocation.name)
    assert not creation_held(allocation)
    assert (
        pins(api.read("DataVolume", source["name"]))[authority.row["request_id"]][
            "state"
        ]
        == "released"
    )
    assert api.writes.count("DataVolume") == 1 and len(service.store.created_pods) == 1


@pytest.mark.asyncio
async def test_ordinary_root_cannot_complete_workspace_allocation(workspace):
    from vm_controller.creation_preparation import PreparedSources
    from vm_controller.preparation_store import PreparationConflict

    ctrl, api, authority, payload, service = workspace
    assert (await finish_prepared(workspace))["status"] == "created"
    allocation = await service.store.get(allocation_name(payload["preparation"]))
    source = allocation.state["creation_source"]
    name = storage_name(payload["workspace_storage"])
    ordinary = "agent-vm-" + payload["job_id"] + "-rootdisk"
    api.objects["DataVolume", ordinary] = deepcopy(api.read("DataVolume", name))
    api.objects["PersistentVolumeClaim", ordinary] = deepcopy(
        api.read("PersistentVolumeClaim", name)
    )
    api.objects["DataVolume", ordinary]["status"] = {"phase": "Succeeded"}
    api.objects["PersistentVolumeClaim", ordinary]["status"] = {"phase": "Bound"}
    with pytest.raises(PreparationConflict, match="authority"):
        await service.mark_allocated(
            payload["preparation"],
            rootdisk=ordinary,
            pvc_uid=api.read("PersistentVolumeClaim", name)["metadata"]["uid"],
            creation=allocation.state["creation_binding"],
            creation_source=source,
            rootdisk_dv_uid=api.read("DataVolume", name)["metadata"]["uid"],
        )
    await PreparedSources(ctrl).release_completed(authority.row)
    assert creation_held(await service.store.get(allocation.name))
    assert (
        pins(api.read("DataVolume", source["name"]))[authority.row["request_id"]][
            "state"
        ]
        == "active"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("advance", [False, True])
async def test_same_owner_prepared_retained_workspace_reuses_original_target_after_gc(
    workspace,
    advance,
):
    from vm_controller.creation_preparation import PreparedSources

    ctrl, api, authority, payload, service = workspace
    await test_engine_prepared_workspace_has_frozen_target_and_exact_completion(
        workspace
    )
    allocation = await service.store.get(allocation_name(payload["preparation"]))
    source = allocation.state["creation_source"]
    saved_binding = deepcopy(allocation.state["creation_binding"])
    del service.store.data[source["artifact"]["name"]]
    del api.objects["DataVolume", source["name"]]
    del api.objects["PersistentVolumeClaim", source["name"]]
    row = deepcopy(authority.row)
    row["expected_pvc_uid"] = allocation.state["creation_root"]["pvc_uid"]
    row["request"]["workspace_storage"]["pvc_uid"] = row["expected_pvc_uid"]
    if advance:
        row["request"]["workspace_storage"]["generation"] = 2
        api.objects["Lease", storage_name(payload["workspace_storage"])]["metadata"][
            "labels"
        ]["srw.io/workspace-generation"] = "2"
    row["request_id"], row["provision_generation"] = str(uuid4()), str(uuid4())
    actual = await PreparedSources(ctrl).prepare(row)
    assert actual["mode"] == "retained" and actual["target"] == source["target"]
    assert (await service.store.get(allocation.name)).state[
        "creation_binding"
    ] == saved_binding
    assert len(service.store.created_pods) == 1 and api.writes.count("DataVolume") == 1


@pytest.mark.asyncio
async def test_changed_completion_target_cannot_hide_original_source_hold(workspace):
    _, _, _, payload, service = workspace
    await test_engine_prepared_workspace_has_frozen_target_and_exact_completion(
        workspace
    )
    allocation = await service.store.get(allocation_name(payload["preparation"]))
    target = allocation.state["creation_binding"]["target"]
    target["workspace_storage"]["uid"] = str(uuid4())
    target["name"] = storage_name(target["workspace_storage"])
    allocation.state["creation_root"]["name"] = target["name"]
    # A well-formed replacement target is still inconsistent with this
    # allocation's immutable delivered source; GC must retain its hold.
    assert creation_held(allocation)
