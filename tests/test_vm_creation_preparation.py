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
