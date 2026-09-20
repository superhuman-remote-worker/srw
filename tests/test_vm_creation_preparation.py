"""Prepared creation must retain the actual allocation/artifact/receipt proof."""

from copy import deepcopy
from dataclasses import asdict
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio

from shared.vm_creation_issuance import validate_rootdisk_source
from tests.test_vm_preparation_lifecycle import (
    engine,
    complete,
    request as preparation_request,
)
from vm_controller.workspace_preparation import allocation_name


@pytest_asyncio.fixture
async def prepared_case():
    service, preparation = (
        engine(registry_hosts=("registry.example",)),
        preparation_request(),
    )
    ready, wait = await complete(service, preparation)
    assert wait is None
    allocation = await service.store.get(allocation_name(preparation))
    artifact = await service.store.get(allocation.state["artifact"])
    source = {
        "kind": "prepared",
        "mode": "clone",
        "namespace": service.store.namespace,
        "name": ready["name"],
        "dv_uid": artifact.state["dv_uid"],
        "pvc_uid": ready["pvc_uid"],
        "allocation": {
            "name": allocation.name,
            "uid": allocation.uid,
            "request": allocation.request,
        },
        "artifact": {
            "name": artifact.name,
            "uid": artifact.uid,
            "request": artifact.request,
        },
        "receipt": artifact.state["receipt"],
    }
    request = {
        "job_id": preparation["allocationId"],
        "vm_image": preparation["image"],
        "preparation": preparation,
    }
    configuration = {
        "namespace": service.store.namespace,
        "preparation": asdict(service.settings),
        "golden_enabled": True,
    }
    return service, source, request, configuration


@pytest.mark.asyncio
async def test_prepared_source_accepts_actual_completed_builder_contract(prepared_case):
    _, source, request, configuration = prepared_case
    validate_rootdisk_source(
        source, request=request, configuration=configuration, expected_pvc_uid=None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "path",
    [
        ("allocation", "uid"),
        ("allocation", "name"),
        ("artifact", "uid"),
        ("artifact", "request", "scope"),
        ("artifact", "request", "steps"),
        ("artifact", "request", "builderImage"),
        ("artifact", "request", "baseImage"),
        ("artifact", "request", "cacheKey"),
        ("receipt", "buildUid"),
        ("receipt", "pvcUid"),
        ("receipt", "diskSha256"),
        ("receipt", "diskBytes"),
        ("namespace",),
        ("name",),
        ("pvc_uid",),
        ("dv_uid",),
    ],
)
async def test_prepared_source_rejects_incomplete_or_changed_proof(prepared_case, path):
    _, source, request, configuration = prepared_case
    source = deepcopy(source)
    target = source
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = "unproven"
    with pytest.raises((ValueError, TypeError)):
        validate_rootdisk_source(
            source, request=request, configuration=configuration, expected_pvc_uid=None
        )


@pytest.mark.asyncio
async def test_prepared_source_receipt_and_cache_key_cannot_hide_wrong_request(
    prepared_case,
):
    _, source, request, configuration = prepared_case
    request = {**request, "job_id": str(uuid4())}
    with pytest.raises(ValueError):
        validate_rootdisk_source(
            source, request=request, configuration=configuration, expected_pvc_uid=None
        )


def test_configuration_uses_cached_preparation_service_settings(monkeypatch):
    from shared.workspace_preparation_settings import PreparationSettings
    from vm_controller.creation_configuration import resolve_creation_configuration
    from tests.test_vm_creation_configuration import controller, request

    ctrl = controller()
    actual = PreparationSettings(
        enabled=True,
        builder_image="ghcr.io/example/builder@sha256:" + "1" * 64,
        disk_size="40Gi",
    )
    ctrl._workspace_preparation_service = SimpleNamespace(settings=actual)
    monkeypatch.setenv("VM_PREPARATION_DISK_SIZE", "80Gi")
    resolved = resolve_creation_configuration(ctrl, request())
    assert resolved["controller_configuration"]["preparation"]["disk_size"] == "40Gi"


@pytest.mark.asyncio
async def test_prepared_source_must_obey_frozen_registry_policy(prepared_case):
    _, source, request, configuration = prepared_case
    configuration = deepcopy(configuration)
    configuration["preparation"]["registry_hosts"] = []
    with pytest.raises(ValueError):
        validate_rootdisk_source(
            source, request=request, configuration=configuration, expected_pvc_uid=None
        )


@pytest.mark.asyncio
async def test_prepared_retained_source_requires_exact_completed_root(prepared_case):
    _, source, request, configuration = prepared_case
    pvc_uid = str(uuid4())
    source = {
        **source,
        "mode": "retained",
        "retained_root": {
            "name": "agent-vm-" + request["job_id"] + "-rootdisk",
            "dv_uid": str(uuid4()),
            "pvc_uid": pvc_uid,
        },
    }
    validate_rootdisk_source(
        source, request=request, configuration=configuration, expected_pvc_uid=pvc_uid
    )
    with pytest.raises(ValueError):
        validate_rootdisk_source(
            source,
            request=request,
            configuration=configuration,
            expected_pvc_uid=str(uuid4()),
        )


@pytest_asyncio.fixture
async def bound_preparation():
    service, request = engine(), preparation_request()
    binding = {
        "request_id": str(uuid4()),
        "provision_generation": str(uuid4()),
        "request_digest": "sha256:" + "3" * 64,
    }
    _, waiting = await service.prepare(request, creation=binding)
    pod = next(name for name, pod in service.store.pods.items())
    service.store.finish(pod)
    for _ in range(3):
        ready, waiting = await service.prepare(request, creation=binding)
        if ready:
            break
    assert ready is not None
    return service, request, binding, ready


@pytest.mark.asyncio
async def test_protocol_preparation_cancel_preserves_delivered_source_hold(
    bound_preparation,
):
    service, request, binding, ready = bound_preparation
    assert await service.cancel_with_receipt(request) == {
        "cancelled": False,
        "workspaceNeverIssued": False,
    }
    allocation = await service.store.get(allocation_name(request))
    assert allocation.state["phase"] == "Cloning"
    assert allocation.state["creation_binding"] == binding
    assert not await service.delete_artifact(
        ready["preparation"]["uid"], request["scope"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("disabled", [False, True])
async def test_protocol_source_hold_survives_expiry_and_disabled_preparation(
    bound_preparation, monkeypatch, disabled
):
    from dataclasses import replace
    from vm_controller import workspace_preparation as module

    service, request, binding, ready = bound_preparation
    allocation = await service.store.get(allocation_name(request))
    monkeypatch.setattr(
        module, "now", lambda: allocation.state["expires_at"] + 9 * 86400
    )
    if disabled:
        service.settings = replace(service.settings, enabled=False)
    await service.reconcile()
    allocation = await service.store.get(allocation_name(request))
    assert allocation is not None and allocation.state["phase"] == "Cloning"
    assert ready["name"] in service.store.disks


@pytest.mark.asyncio
async def test_existing_protocol_allocation_cannot_deliver_source_to_legacy_or_other_request(
    bound_preparation,
):
    from vm_controller.preparation_store import PreparationConflict

    service, request, binding, _ = bound_preparation
    with pytest.raises(PreparationConflict):
        await service.prepare(request)
    with pytest.raises(PreparationConflict):
        await service.prepare(request, creation={**binding, "request_id": str(uuid4())})


@pytest.mark.asyncio
async def test_bound_preparation_without_source_delivery_can_cancel():
    service, request = engine(), preparation_request()
    binding = {
        "request_id": str(uuid4()),
        "provision_generation": str(uuid4()),
        "request_digest": "sha256:" + "3" * 64,
    }
    await service.prepare(request, creation=binding)
    for _ in range(3):
        result = await service.cancel_with_receipt(request)
        if result["cancelled"]:
            break
        await service.reconcile()
    assert result == {"cancelled": True, "workspaceNeverIssued": True}


@pytest.mark.asyncio
async def test_legacy_workspace_observation_cannot_release_protocol_hold(
    bound_preparation,
):
    service, request, _, ready = bound_preparation
    root = "agent-vm-" + request["allocationId"] + "-rootdisk"
    await service.store.ensure_disk(
        root,
        owner_uid=request["allocationId"],
        scope=request["scope"],
        source={"pvc": {"name": ready["name"], "namespace": service.store.namespace}},
        size="30Gi",
    )
    pvc_uid = (await service.store.pvc(root)).metadata.uid
    await service.observe_workspace(
        "job", request["allocationId"], rootdisk=root, pvc_uid=pvc_uid
    )
    allocation = await service.store.get(allocation_name(request))
    assert allocation.state["phase"] == "Cloning"


@pytest.mark.asyncio
@pytest.mark.parametrize("broken", [None, {}])
async def test_unproven_existing_creation_binding_does_not_release_source(
    bound_preparation, broken
):
    service, request, _, _ = bound_preparation
    service.store.data[allocation_name(request)].state["creation_binding"] = broken
    result = await service.cancel_with_receipt(request)
    assert result == {"cancelled": False, "workspaceNeverIssued": False}


@pytest.mark.asyncio
async def test_incomplete_completion_fact_does_not_release_prepared_source(
    bound_preparation,
):
    service, request, _, _ = bound_preparation
    service.store.data[allocation_name(request)].state["creation_root"] = {
        "authenticated": True
    }
    assert await service.cancel_with_receipt(request) == {
        "cancelled": False,
        "workspaceNeverIssued": False,
    }
