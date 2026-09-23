"""A typed Lease LIST must not erase creation-carrier evidence."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from kubernetes.client import ApiClient, V1Lease, V1LeaseList

from shared.vm_creation_issuance import (
    CREATION_SIGNATURE_ANNOTATION,
    seal_creation_carrier,
)
from vm_controller import controller as controller_module
from vm_controller.controller import VMController


SECRET = b"cleanup-projection-test-secret-at-least-32-bytes"


def _carrier():
    job = str(uuid4())
    values = {
        "version": 1,
        "source": "controller_vm_create",
        "admission_id": str(uuid4()),
        "reservation_request_id": str(uuid4()),
        "intent_digest": "sha256:" + "a" * 64,
        "retry_request_id": str(uuid4()),
        "job_id": job,
        "provision_generation": str(uuid4()),
        "request_digest": "sha256:" + "b" * 64,
        "controller_configuration_digest": "sha256:" + "c" * 64,
        "expected_pvc_uid": None,
        "retained_dv_uid": None,
        "current_dv_uid": str(uuid4()),
        "current_pvc_uid": str(uuid4()),
        "current_secret_uid": str(uuid4()),
        "effect_kind": "vm",
        "effect_nonce": str(uuid4()),
        "object_name": f"agent-vm-{job}",
    }
    return seal_creation_carrier(
        values,
        namespace=controller_module.VM_NAMESPACE,
        uid=str(uuid4()),
        resource_version="17",
        secret=SECRET,
    )


def _typed(lease):
    return ApiClient()._ApiClient__deserialize_model(lease, V1Lease)


def _controller(monkeypatch, listed, read):
    monkeypatch.setattr(controller_module, "LIFECYCLE_HMAC_SECRET", SECRET)
    api = SimpleNamespace(
        list_namespaced_lease=MagicMock(return_value=V1LeaseList(items=[listed])),
        read_namespaced_lease=MagicMock(return_value=read),
        create_namespaced_lease=MagicMock(),
        replace_namespaced_lease=MagicMock(),
        delete_namespaced_lease=MagicMock(),
    )
    controller = VMController.__new__(VMController)
    controller.coordination_api = api
    return controller, api


def _list_shape(lease):
    listed = deepcopy(lease)
    listed.pop("apiVersion")
    listed.pop("kind")
    return _typed(listed)


def _rootdisk_carrier():
    owner = str(uuid4())
    admission = str(uuid4())
    name = f"srw-cleanup-{admission.replace('-', '')}"
    values = {
        "admission_id": admission,
        "request_id": str(uuid4()),
        "intent_digest": "sha256:" + "d" * 64,
        "owner_kind": "job",
        "owner_id": owner,
        "source": "controller_rootdisk_delete",
        "outcome": "deleted",
        "name": f"agent-vm-{owner}-rootdisk",
        "old_dv_uid": str(uuid4()),
        "old_pvc_uid": str(uuid4()),
        "provision_generation": str(uuid4()),
        "nonce": str(uuid4()),
        "successor_dv_uid": "",
        "successor_pvc_uid": "",
    }
    uid = str(uuid4())
    signature = VMController._workspace_cleanup_carrier_signature(
        name=name,
        uid=uid,
        values=values,
    )
    return {
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": {
            "name": name,
            "namespace": controller_module.VM_NAMESPACE,
            "uid": uid,
            "resourceVersion": "9",
            "labels": {controller_module.WORKSPACE_CLEANUP_CARRIER_LABEL: "true"},
            "annotations": {
                **{
                    controller_module._CLEANUP_ANNOTATIONS[k]: v
                    for k, v in values.items()
                },
                "srw.io/cleanup-carrier-signature": signature,
            },
        },
    }


def _no_writes(api):
    api.create_namespaced_lease.assert_not_called()
    api.replace_namespaced_lease.assert_not_called()
    api.delete_namespaced_lease.assert_not_called()


@pytest.mark.asyncio
async def test_gc_off_continuation_uses_authenticated_source_not_unsigned_annotation(
    monkeypatch,
):
    monkeypatch.setattr(controller_module, "LIFECYCLE_HMAC_SECRET", SECRET)
    creation = _carrier()
    # This annotation is outside the signed creation intent. It must not
    # select the creation carrier for rootdisk-only continuation.
    creation["metadata"]["annotations"]["srw.io/cleanup-source"] = (
        "controller_rootdisk_delete"
    )
    rootdisk = _rootdisk_carrier()
    api = SimpleNamespace(
        list_namespaced_lease=MagicMock(
            return_value=V1LeaseList(
                items=[
                    _list_shape(creation),
                    _list_shape(rootdisk),
                ]
            )
        ),
        read_namespaced_lease=MagicMock(return_value=_typed(creation)),
        create_namespaced_lease=MagicMock(),
        replace_namespaced_lease=MagicMock(),
        delete_namespaced_lease=MagicMock(),
    )
    controller = VMController.__new__(VMController)
    controller.coordination_api = api
    controller._reconcile_workspace_cleanup_carrier = AsyncMock(return_value=True)

    await controller._reconcile_workspace_cleanup_carriers(
        sources=frozenset({"controller_rootdisk_delete"})
    )
    assert [
        call.args[0]["source"]
        for call in controller._reconcile_workspace_cleanup_carrier.await_args_list
    ] == ["controller_rootdisk_delete"]
    controller._reconcile_workspace_cleanup_carrier.reset_mock()
    await controller._reconcile_workspace_cleanup_carriers()
    assert [
        call.args[0]["source"]
        for call in controller._reconcile_workspace_cleanup_carrier.await_args_list
    ] == ["controller_vm_create", "controller_rootdisk_delete"]
    _no_writes(api)


@pytest.mark.asyncio
async def test_typed_list_creation_carrier_reaches_unrelated_cleanup_selection(
    monkeypatch,
):
    lease = _carrier()
    controller, api = _controller(monkeypatch, _list_shape(lease), _typed(lease))

    assert (
        await controller._find_workspace_cleanup_carrier(
            owner_kind="job",
            owner_id=str(uuid4()),
            source="controller_rootdisk_delete",
            name="unrelated-rootdisk",
        )
        is None
    )
    api.read_namespaced_lease.assert_called_once_with(
        name=lease["metadata"]["name"],
        namespace=controller_module.VM_NAMESPACE,
    )
    _no_writes(api)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "listed_uid_missing",
        "read_uid_changed",
        "read_name_changed",
        "read_namespace_changed",
        "read_signature_changed",
        "read_resource_version_missing",
        "read_kind_changed",
    ],
)
async def test_typed_list_refetch_refuses_changed_or_incomplete_carrier(
    monkeypatch, change
):
    lease = _carrier()
    listed = _list_shape(lease)
    read = deepcopy(lease)
    if change == "listed_uid_missing":
        listed.metadata.uid = None
    elif change == "read_uid_changed":
        read["metadata"]["uid"] = str(uuid4())
    elif change == "read_name_changed":
        read["metadata"]["name"] = "another-carrier"
    elif change == "read_namespace_changed":
        read["metadata"]["namespace"] = "another-namespace"
    elif change == "read_signature_changed":
        read["metadata"]["annotations"][CREATION_SIGNATURE_ANNOTATION] = "0" * 64
    elif change == "read_resource_version_missing":
        read["metadata"].pop("resourceVersion")
    else:
        read["kind"] = "ConfigMap"
    controller, api = _controller(monkeypatch, listed, _typed(read))
    controller.core_api = SimpleNamespace(
        delete_namespaced_persistent_volume_claim=MagicMock()
    )
    controller._acquire_workspace_cleanup_reservation = AsyncMock()
    controller._delete_dv = AsyncMock()
    owner_id = str(uuid4())

    with pytest.raises((ValueError, RuntimeError)):
        await controller._delete_captured_rootdisk(
            f"agent-vm-{owner_id}-rootdisk",
            owner_kind="job",
            owner_id=owner_id,
            expected_pvc_uid=str(uuid4()),
        )
    controller._acquire_workspace_cleanup_reservation.assert_not_awaited()
    controller._delete_dv.assert_not_awaited()
    controller.core_api.delete_namespaced_persistent_volume_claim.assert_not_called()
    _no_writes(api)
