"""Retained disk identity, generation fencing, and deletion preconditions."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from kubernetes import client
from kubernetes.client.exceptions import ApiException
import pytest

from shared.vm_workspace_storage import storage_binding, storage_labels, storage_name
from vm_controller.retained_storage import RetainedStorage


def binding():
    return storage_binding(
        {
            "uid": str(uuid4()),
            "generation": 1,
            "pvc_uid": None,
            "owner_id": str(uuid4()),
            "owner_kind": "job",
        }
    )


class LeaseAPI:
    def __init__(self):
        self.lease = None
        self.version = 0

    def read_namespaced_lease(self, **kwargs):
        if not self.lease:
            raise ApiException(status=404)
        return deepcopy(self.lease)

    def create_namespaced_lease(self, *, body, **kwargs):
        if self.lease:
            raise ApiException(status=409)
        return self._store(body)

    def replace_namespaced_lease(self, *, body, **kwargs):
        version = (
            body["metadata"].get("resourceVersion")
            if isinstance(body, dict)
            else body.metadata.resource_version
        )
        if version != str(self.version):
            raise ApiException(status=409)
        return self._store(body)

    def _store(self, body):
        self.version += 1
        if isinstance(body, dict):
            self.lease = client.V1Lease(
                metadata=client.V1ObjectMeta(
                    name=body["metadata"]["name"],
                    labels=body["metadata"].get("labels"),
                    annotations=body["metadata"].get("annotations"),
                )
            )
        else:
            self.lease = deepcopy(body)
        self.lease.metadata.resource_version = str(self.version)
        return deepcopy(self.lease)


def runtime(value):
    pvc = client.V1PersistentVolumeClaim(
        metadata=client.V1ObjectMeta(
            name=storage_name(value),
            uid=value["pvc_uid"] or str(uuid4()),
            labels={
                **storage_labels(value, value["owner_id"]),
                "srw.io/owner-kind": "job",
                "srw.io/owner-id": value["owner_id"],
            },
        )
    )
    core = MagicMock()
    core.read_namespaced_persistent_volume_claim.return_value = pvc
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[])
    custom = MagicMock()
    custom.list_namespaced_custom_object.return_value = {"items": []}
    controller = SimpleNamespace(
        core_api=core,
        k8s_client=custom,
        coordination_api=LeaseAPI(),
        _get_dv=AsyncMock(return_value=None),
        _delete_captured_rootdisk=AsyncMock(),
        _active_recovery_pins=AsyncMock(return_value=()),
    )
    return RetainedStorage(controller, "test"), pvc


@pytest.mark.asyncio
async def test_handoff_requires_a_durable_fence_and_keeps_the_same_pvc():
    first = binding()
    service, pvc = runtime(first)
    await service.claim(first, first["owner_id"])
    second = {**first, "generation": 2, "pvc_uid": pvc.metadata.uid}
    with pytest.raises(RuntimeError, match="not been fenced"):
        await service.claim(second, str(uuid4()))
    assert await service.detach({**first, "pvc_uid": pvc.metadata.uid})
    with pytest.raises(RuntimeError, match="has been fenced"):
        await service.claim(first, first["owner_id"])
    next_job = str(uuid4())
    await service.claim(second, next_job)
    assert await service.probe(second) == pvc.metadata.uid
    with pytest.raises(RuntimeError, match="another execution"):
        await service.claim(second, first["owner_id"])
    with pytest.raises(RuntimeError):
        await service.probe(first)
    with pytest.raises(RuntimeError):
        await service.delete(first)
    service.controller._delete_captured_rootdisk.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("plural", ["virtualmachines", "virtualmachineinstances"])
async def test_even_a_terminating_vm_or_vmi_blocks_detach(plural):
    value = binding()
    service, pvc = runtime(value)
    await service.claim(value, value["owner_id"])
    spec = {"volumes": [{"dataVolume": {"name": storage_name(value)}}]}
    if plural == "virtualmachines":
        spec = {"template": {"spec": spec}}

    def listed(**kwargs):
        return {
            "items": [{"metadata": {"deletionTimestamp": "now"}, "spec": spec}]
            if kwargs["plural"] == plural
            else []
        }

    service.controller.k8s_client.list_namespaced_custom_object.side_effect = listed
    assert not await service.detach({**value, "pvc_uid": pvc.metadata.uid})
    with pytest.raises(RuntimeError, match="still in use"):
        await service.delete({**value, "pvc_uid": pvc.metadata.uid})


@pytest.mark.asyncio
async def test_launcher_volume_reference_alone_blocks_handoff():
    value = binding()
    service, pvc = runtime(value)
    await service.claim(value, value["owner_id"])
    service.controller.core_api.list_namespaced_pod.return_value = SimpleNamespace(
        items=[
            client.V1Pod(
                metadata=client.V1ObjectMeta(),
                spec=client.V1PodSpec(
                    containers=[],
                    volumes=[
                        client.V1Volume(
                            name="root",
                            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                                claim_name=storage_name(value)
                            ),
                        )
                    ],
                ),
            )
        ]
    )
    assert not await service.detach({**value, "pvc_uid": pvc.metadata.uid})


@pytest.mark.asyncio
async def test_exact_deletion_tombstone_survives_controller_restart():
    value = binding()
    service, pvc = runtime(value)
    await service.claim(value, value["owner_id"])
    captured = {**value, "pvc_uid": pvc.metadata.uid}
    assert not await service.delete(captured)
    service.controller._delete_captured_rootdisk.assert_awaited_once_with(
        storage_name(value),
        owner_id=value["owner_id"],
        expected_pvc_uid=pvc.metadata.uid,
    )
    restarted = RetainedStorage(service.controller, "test")
    with pytest.raises(RuntimeError, match="released"):
        await restarted.claim(value, value["owner_id"])
    service.controller.core_api.read_namespaced_persistent_volume_claim.side_effect = (
        ApiException(status=404)
    )
    assert await restarted.delete(captured)


@pytest.mark.asyncio
async def test_replaced_pvc_is_never_adopted_or_deleted():
    value = binding()
    service, pvc = runtime(value)
    await service.claim(value, value["owner_id"])
    captured = {**value, "pvc_uid": str(uuid4())}
    with pytest.raises(RuntimeError, match="identity changed"):
        await service.delete(captured)
    service.controller._delete_captured_rootdisk.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["claim", "detach", "delete"])
async def test_recovery_pin_blocks_retained_storage_reuse_and_cleanup(operation):
    value = binding()
    service, pvc = runtime(value)
    await service.claim(value, value["owner_id"])
    captured = {**value, "pvc_uid": pvc.metadata.uid}
    service.controller._active_recovery_pins.return_value = (
        {"pvc_uid": pvc.metadata.uid},
    )

    with pytest.raises(RuntimeError, match="pinned for recovery"):
        if operation == "claim":
            await service.claim(
                {**captured, "generation": 2},
                str(uuid4()),
            )
        elif operation == "detach":
            await service.detach(captured)
        else:
            await service.delete(captured)

    service.controller._delete_captured_rootdisk.assert_not_awaited()


@pytest.mark.parametrize(
    "change",
    [
        {"generation": True},
        {"generation": 0},
        {"generation": 2},
        {"uid": "../../claim"},
        {"pvc_uid": "wrong"},
        {"owner_kind": "workspace"},
        {"arbitrary_pvc": "claim"},
    ],
)
def test_storage_binding_cannot_select_an_arbitrary_claim(change):
    with pytest.raises(ValueError):
        storage_binding({**binding(), **change})


@pytest.mark.asyncio
async def test_cancel_before_allocation_permanently_closes_the_empty_identity():
    value = binding()
    service, _ = runtime(value)
    service.controller.core_api.read_namespaced_persistent_volume_claim.side_effect = (
        ApiException(status=404)
    )
    assert await service.delete(value)
    with pytest.raises(RuntimeError, match="released"):
        await service.claim(value, value["owner_id"])


@pytest.mark.parametrize("wrong", ["attachment", "rootdisk"])
def test_existing_vm_cannot_be_adopted_with_an_unrelated_disk(wrong):
    value = binding()
    vm = {
        "metadata": {"labels": storage_labels(value, value["owner_id"])},
        "spec": {
            "template": {
                "spec": {"volumes": [{"dataVolume": {"name": storage_name(value)}}]}
            }
        },
    }
    RetainedStorage.verify_vm(vm, value, value["owner_id"])
    if wrong == "attachment":
        vm["metadata"]["labels"] = {}
    else:
        vm["spec"]["template"]["spec"]["volumes"][0]["dataVolume"]["name"] = (
            "legacy-job-rootdisk"
        )
    with pytest.raises(RuntimeError):
        RetainedStorage.verify_vm(vm, value, value["owner_id"])
