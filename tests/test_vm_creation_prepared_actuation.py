"""Protocol actuation connected to the existing durable preparation engine."""

from copy import deepcopy
from uuid import uuid4

import pytest
from kubernetes import client
from kubernetes.client.exceptions import ApiException

from tests.test_vm_creation_actuation import setup as _actuation_fixture
from tests.test_vm_preparation_lifecycle import engine
from shared.workspace_preparation import preparation_request
from vm_controller.preparation_store import DISK_LABEL, SCOPE_LABEL, scope_key
from vm_controller import controller as settings
from vm_controller.creation_configuration import resolve_creation_configuration

setup = _actuation_fixture


@pytest.fixture
def prepared(setup):
    ctrl, api, authority, payload = setup
    service = engine(registry_hosts=("registry.example",))
    service.store.namespace = settings.VM_NAMESPACE
    ctrl._workspace_preparation_service = service
    request = preparation_request(
        {
            "image": "registry.example/base:latest",
            "prepare": [{"command": ["touch", "/opt/prepared"]}],
        },
        scope_kind="Project",
        scope_uid=str(uuid4()),
        allocation_id=payload["job_id"],
        owner_kind="job",
    )
    original_ensure = service.store.ensure_disk

    async def ensure(name, *, owner_uid, scope, source, size):
        await original_ensure(
            name, owner_uid=owner_uid, scope=scope, source=source, size=size
        )
        record = service.store.disks[name]
        dv = record["dv"]
        dv.update(apiVersion="cdi.kubevirt.io/v1beta1", kind="DataVolume")
        dv["metadata"].update(
            name=name,
            namespace=settings.VM_NAMESPACE,
            resourceVersion="1",
            labels={DISK_LABEL: owner_uid, SCOPE_LABEL: scope_key(scope)},
            annotations={},
        )
        dv["spec"]["storage"] = {
            "volumeMode": "Filesystem",
            "resources": {"requests": {"storage": size}},
        }
        api.objects["DataVolume", name] = deepcopy(dv)
        api.objects["PersistentVolumeClaim", name] = {
            "apiVersion": "v1",
            "kind": "PersistentVolumeClaim",
            "metadata": {
                "name": name,
                "namespace": settings.VM_NAMESPACE,
                "uid": record["pvc"].metadata.uid,
                "resourceVersion": "1",
                "labels": deepcopy(dv["metadata"]["labels"]),
                "ownerReferences": [
                    {
                        "kind": "DataVolume",
                        "uid": dv["metadata"]["uid"],
                        "controller": True,
                    }
                ],
            },
            "spec": {"volumeMode": "Filesystem"},
            "status": {"phase": "Bound"},
        }
        return deepcopy(dv)

    original_dv, original_pvc = service.store.dv, service.store.pvc

    async def dv(name):
        return (
            api.read("DataVolume", name)
            if ("DataVolume", name) in api.objects
            else await original_dv(name)
        )

    async def pvc(name):
        if ("PersistentVolumeClaim", name) in api.objects:
            value = api.read("PersistentVolumeClaim", name)
            for ref in value["metadata"].get("ownerReferences", []):
                ref.update(apiVersion="cdi.kubevirt.io/v1beta1", name=name)
            return client.ApiClient()._ApiClient__deserialize(
                value, "V1PersistentVolumeClaim"
            )
        return await original_pvc(name)

    def replace(**kwargs):
        body = deepcopy(kwargs["body"])
        previous = api.read("DataVolume", kwargs["name"])
        if any(
            body["metadata"][field] != previous["metadata"][field]
            for field in ("uid", "resourceVersion")
        ):
            raise ApiException(status=409)
        body["metadata"]["resourceVersion"] = str(
            int(previous["metadata"]["resourceVersion"]) + 1
        )
        api.objects["DataVolume", kwargs["name"]] = body
        if kwargs["name"] in service.store.disks:
            service.store.disks[kwargs["name"]]["dv"] = deepcopy(body)
        return deepcopy(body)

    service.store.ensure_disk, service.store.dv, service.store.pvc = ensure, dv, pvc
    ctrl.k8s_client.replace_namespaced_custom_object = replace
    canonical = {
        **authority.row["request"],
        "preparation": request,
        "vm_image": request["image"],
        "disk_size": "40Gi",
    }
    resolved = resolve_creation_configuration(ctrl, canonical)
    authority.row.update(resolved)
    payload.update(resolved["request"])
    payload["creation_retry"].update(
        {
            key: resolved[key]
            for key in ("request_digest", "controller_configuration_digest")
        }
    )
    return ctrl, api, authority, payload, service


async def finish_prepared(prepared):
    ctrl, api, authority, payload, service = prepared
    initial = await ctrl._do_create_serialized(payload)
    assert (
        initial["status"] == "creation_pending"
        and initial["reason"] == "preparation_wait"
    )
    service.store.finish(next(iter(service.store.pods)))
    for _ in range(4):
        result = await ctrl._do_create_serialized(payload)
        if (
            result["status"] != "creation_pending"
            or result["reason"] != "preparation_wait"
        ):
            return result
    raise AssertionError("Prepared source did not finish")


@pytest.mark.asyncio
async def test_requested_preparation_waits_without_job_effects(prepared):
    ctrl, api, authority, payload, service = prepared
    result = await ctrl._do_create_serialized(payload)
    assert (
        result["status"] == "creation_pending"
        and result["reason"] == "preparation_wait"
    )
    assert authority.row["effects"] == []
    assert api.writes == []
    assert len(service.store.created_pods) == 1


@pytest.mark.asyncio
async def test_prepared_clone_uses_actual_allocation_receipt_and_source_pin(prepared):
    from vm_controller.creation_sources import pins
    from vm_controller.workspace_preparation import allocation_name

    ctrl, api, authority, payload, service = prepared
    result = await finish_prepared(prepared)
    assert result["status"] == "created"
    source = authority.row["effects"][0]["carrier_intent"]["rootdisk_source"]
    assert source["kind"] == "prepared"
    allocation = await service.store.get(allocation_name(payload["preparation"]))
    assert source == allocation.state["creation_source"]
    assert source["receipt"]["buildUid"] == source["artifact"]["uid"]
    assert (
        pins(api.read("DataVolume", source["name"]))[authority.row["request_id"]][
            "state"
        ]
        == "active"
    )
    root = api.read("DataVolume", "agent-vm-" + payload["job_id"] + "-rootdisk")
    assert root["spec"]["source"] == {
        "pvc": {"name": source["name"], "namespace": settings.VM_NAMESPACE}
    }
    assert result["preparation"]["diskSha256"] == source["receipt"]["diskSha256"]
    assert api.writes.count("DataVolume") == 1


@pytest.mark.asyncio
async def test_exact_completed_clone_releases_pin_and_allocation_once(prepared):
    from vm_controller.creation_sources import pins
    from vm_controller.workspace_preparation import allocation_name, creation_held

    ctrl, api, authority, payload, service = prepared
    assert (await finish_prepared(prepared))["status"] == "created"
    allocation = await service.store.get(allocation_name(payload["preparation"]))
    assert creation_held(allocation)
    root = "agent-vm-" + payload["job_id"] + "-rootdisk"
    api.objects["DataVolume", root]["status"] = {"phase": "Succeeded"}
    api.objects["PersistentVolumeClaim", root]["status"] = {"phase": "Bound"}
    for _ in range(2):
        assert (await ctrl._do_create_serialized(payload))["status"] == "created"
    allocation = await service.store.get(allocation.name)
    assert allocation.state["phase"] == "Allocated"
    assert not creation_held(allocation)
    source = allocation.state["creation_source"]
    assert (
        pins(api.read("DataVolume", source["name"]))[authority.row["request_id"]][
            "state"
        ]
        == "released"
    )
    assert len(service.store.created_pods) == 1
    assert api.writes.count("DataVolume") == 1


@pytest.mark.asyncio
async def test_lost_root_reply_and_cancel_preserve_prepared_source(prepared):
    from vm_controller.creation_sources import pins
    from vm_controller.workspace_preparation import allocation_name, creation_held

    ctrl, api, authority, payload, service = prepared
    api.lost.add("DataVolume")
    result = await finish_prepared(prepared)
    # The next poll observes the exact root; no source resolution or second POST.
    if result["status"] == "creation_pending":
        result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "created"
    cancelled = await service.cancel_with_receipt(payload["preparation"])
    assert cancelled["cancelled"] is False
    assert cancelled["workspaceNeverIssued"] is False
    allocation = await service.store.get(allocation_name(payload["preparation"]))
    assert creation_held(allocation)
    source = allocation.state["creation_source"]
    assert (
        pins(api.read("DataVolume", source["name"]))[authority.row["request_id"]][
            "state"
        ]
        == "active"
    )
    assert api.writes.count("DataVolume") == 1


@pytest.mark.asyncio
async def test_prepared_source_replacement_before_grant_has_no_job_write(prepared):
    from vm_controller.creation_preparation import PreparedSources

    ctrl, api, authority, payload, service = prepared
    assert (await ctrl._do_create_serialized(payload))["reason"] == "preparation_wait"
    service.store.finish(next(iter(service.store.pods)))
    original = PreparedSources.validate

    async def replace(self, row, source):
        api.objects["PersistentVolumeClaim", source["name"]]["metadata"]["uid"] = str(
            uuid4()
        )
        await original(self, row, source)

    from unittest.mock import patch

    with patch.object(PreparedSources, "validate", replace):
        for _ in range(4):
            result = await ctrl._do_create_serialized(payload)
            if result.get("reason") != "preparation_wait":
                break
    assert result["status"] == "creation_attention"
    assert not authority.row["effects"]
    assert not api.writes


@pytest.mark.asyncio
async def test_configuration_floors_omitted_disk_to_prepared_capacity(prepared):
    ctrl, _, authority, _, _ = prepared
    request = {**authority.row["request"]}
    request.pop("disk_size")
    resolved = resolve_creation_configuration(ctrl, request)
    assert resolved["request"]["disk_size"] == "30Gi"


@pytest.mark.asyncio
async def test_configuration_refuses_explicit_disk_smaller_than_prepared(prepared):
    ctrl, _, authority, _, _ = prepared
    request = {**authority.row["request"], "disk_size": "20Gi"}
    with pytest.raises(ValueError, match="[Pp]repared"):
        resolve_creation_configuration(ctrl, request)


@pytest.mark.asyncio
async def test_prepared_retained_reuses_completed_proof_after_source_gc(prepared):
    from vm_controller.creation_preparation import PreparedSources
    from vm_controller.workspace_preparation import allocation_name

    ctrl, api, authority, payload, service = prepared
    await test_exact_completed_clone_releases_pin_and_allocation_once(prepared)
    allocation = await service.store.get(allocation_name(payload["preparation"]))
    source = allocation.state["creation_source"]
    del service.store.data[source["artifact"]["name"]]
    del api.objects["DataVolume", source["name"]]
    del api.objects["PersistentVolumeClaim", source["name"]]
    row = deepcopy(authority.row)
    row["expected_pvc_uid"] = allocation.state["creation_root"]["pvc_uid"]
    row["request_id"], row["provision_generation"] = str(uuid4()), str(uuid4())
    retained = await PreparedSources(ctrl).prepare(row)
    assert retained["mode"] == "retained"
    assert retained["retained_root"] == allocation.state["creation_root"]
    assert len(service.store.created_pods) == 1
    root = retained["retained_root"]["name"]
    api.objects["DataVolume", root]["metadata"]["uid"] = str(uuid4())
    with pytest.raises(ValueError, match="retained.disk"):
        await PreparedSources(ctrl).prepare(row)


@pytest.mark.asyncio
async def test_prepared_completed_root_replacement_never_releases_hold(prepared):
    from vm_controller.creation_preparation import PreparedSources
    from vm_controller.workspace_preparation import allocation_name, creation_held

    ctrl, api, authority, payload, service = prepared
    assert (await finish_prepared(prepared))["status"] == "created"
    root = "agent-vm-" + payload["job_id"] + "-rootdisk"
    api.objects["DataVolume", root]["status"] = {"phase": "Succeeded"}
    api.objects["DataVolume", root]["metadata"]["uid"] = str(uuid4())
    api.objects["PersistentVolumeClaim", root]["status"] = {"phase": "Bound"}
    with pytest.raises(ValueError):
        await PreparedSources(ctrl).release_completed(authority.row)
    assert creation_held(
        await service.store.get(allocation_name(payload["preparation"]))
    )


@pytest.mark.asyncio
async def test_authority_rejects_prepared_retained_root_dv_disagreement(prepared):
    from types import SimpleNamespace
    from uuid import UUID
    from orchestrator.services.vm_creation_retry_store import (
        VMCreationRetryStore,
        VMCreationRetryConflict,
    )
    from vm_controller.workspace_preparation import allocation_name

    _, _, authority, payload, service = prepared
    await test_exact_completed_clone_releases_pin_and_allocation_once(prepared)
    allocation = await service.store.get(allocation_name(payload["preparation"]))
    source = {
        **allocation.state["creation_source"],
        "mode": "retained",
        "retained_root": allocation.state["creation_root"],
    }
    row = {
        **authority.row,
        "canonical_request": authority.row["request"],
        "predecessor_evidence": {},
        "expected_pvc_uid": UUID(source["retained_root"]["pvc_uid"]),
    }
    with pytest.raises(
        VMCreationRetryConflict, match="creation_rootdisk_source_changed"
    ):
        await VMCreationRetryStore(None)._check_carrier(
            SimpleNamespace(),
            row,
            {"metadata": {"namespace": settings.VM_NAMESPACE}},
            {
                "version": 2,
                "effect_kind": "rootdisk",
                "rootdisk_source": source,
                "retained_dv_uid": str(uuid4()),
            },
        )


@pytest.mark.asyncio
async def test_prepared_vm_receipt_mutation_is_not_adopted(prepared):
    ctrl, api, _, payload, _ = prepared
    assert (await finish_prepared(prepared))["status"] == "created"
    vm = api.objects["VirtualMachine", "agent-vm-" + payload["job_id"]]
    vm["metadata"]["annotations"]["srw.io/prepared-artifact"] = "{}"
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_attention"


@pytest.mark.asyncio
@pytest.mark.parametrize("completion", [False, True])
async def test_lost_prepared_capture_or_completion_reply_replays_exact_state(
    prepared, completion
):
    ctrl, api, authority, payload, service = prepared
    if completion:
        assert (await finish_prepared(prepared))["status"] == "created"
        root = "agent-vm-" + payload["job_id"] + "-rootdisk"
        api.objects["DataVolume", root]["status"] = {"phase": "Succeeded"}
        api.objects["PersistentVolumeClaim", root]["status"] = {"phase": "Bound"}
    else:
        assert (await ctrl._do_create_serialized(payload))[
            "reason"
        ] == "preparation_wait"
        service.store.finish(next(iter(service.store.pods)))
    original = service.store.save
    lost = False

    async def save(record, state):
        nonlocal lost
        result = await original(record, state)
        field = "creation_root" if completion else "creation_source"
        if field in state and not lost:
            lost = True
            raise TimeoutError("accepted record update reply lost")
        return result

    service.store.save = save
    for _ in range(4):
        result = await ctrl._do_create_serialized(payload)
        if lost:
            break
    assert lost and result["status"] == "creation_pending"
    before = api.writes.count("DataVolume")
    result = await ctrl._do_create_serialized(payload)
    assert result["status"] == "created"
    assert api.writes.count("DataVolume") == 1
    assert before == (1 if completion else 0)
    assert len(service.store.created_pods) == 1


@pytest.mark.asyncio
async def test_status_receipt_requires_exact_observed_vm(prepared):
    from vm_controller.creation_preparation import observed_preparation_metadata

    ctrl, api, authority, payload, _ = prepared
    result = await finish_prepared(prepared)
    vm = api.read("VirtualMachine", "agent-vm-" + payload["job_id"])
    assert observed_preparation_metadata(authority.row, vm) == result["preparation"]
    vm["metadata"]["uid"] = str(uuid4())
    with pytest.raises(ValueError):
        observed_preparation_metadata(authority.row, vm)


@pytest.mark.asyncio
async def test_prepared_root_unknown_with_builder_drift_never_rebuilds(prepared):
    from dataclasses import replace
    from vm_controller.workspace_preparation import allocation_name, creation_held

    ctrl, api, _, payload, service = prepared
    api.lost.add("DataVolume")
    assert (await finish_prepared(prepared))["status"] == "creation_pending"
    service.settings = replace(
        service.settings, builder_image="registry.example/builder:changed"
    )
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_attention"
    assert api.writes.count("DataVolume") == 1
    assert api.writes.count("VirtualMachine") == 0
    assert len(service.store.created_pods) == 1
    assert creation_held(
        await service.store.get(allocation_name(payload["preparation"]))
    )


@pytest.mark.asyncio
async def test_prepared_workspace_binding_requires_complete_identity(prepared):
    from vm_controller.creation_preparation import PreparedSources

    ctrl, api, authority, _, service = prepared
    row = deepcopy(authority.row)
    row["request"]["workspace_storage"] = {"uid": str(uuid4())}
    with pytest.raises(ValueError, match="binding"):
        await PreparedSources(ctrl).prepare(row)
    assert not api.writes
    assert not service.store.created_pods


@pytest.mark.asyncio
async def test_delivered_source_lost_before_capture_requires_attention(prepared):
    from vm_controller.creation_preparation import creation_binding
    from vm_controller.workspace_preparation import allocation_name, creation_held

    ctrl, api, authority, payload, service = prepared
    assert (await ctrl._do_create_serialized(payload))["reason"] == "preparation_wait"
    service.store.finish(next(iter(service.store.pods)))
    for _ in range(4):
        ready, _ = await service.prepare(
            payload["preparation"], creation=creation_binding(authority.row)
        )
        if ready:
            break
    assert ready
    allocation = await service.store.get(allocation_name(payload["preparation"]))
    assert creation_held(allocation)
    del service.store.data[allocation.state["artifact"]]
    assert (await ctrl._do_create_serialized(payload))["status"] == "creation_attention"
    assert not api.writes
    assert creation_held(await service.store.get(allocation.name))
