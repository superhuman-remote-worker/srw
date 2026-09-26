"""Final resource proof uses selected GETs and a bounded LimitRange LIST."""

import asyncio
from copy import deepcopy
import threading
from types import SimpleNamespace
from uuid import uuid4

from kubernetes.client.exceptions import ApiException
import pytest

from shared.vm_launcher_profile import default_launcher_profile
from shared.vm_resource_admission import ResourceAdmissionError
from shared import vm_resource_effect_node
from tests.test_vm_launcher_profile import installed_cr
from tests.test_vm_resource_placement import node as raw_node
from vm_controller.resource_inventory import ResourceInventoryCollector


def proof_case(*, retained=False):
    selected = raw_node()
    selected["metadata"]["labels"]["kubernetes.io/arch"] = "amd64"
    selected["status"]["allocatable"].update(
        {
            "ephemeral-storage": "100G",
            "devices.kubevirt.io/tun": "8",
            "devices.kubevirt.io/vhost-net": "8",
        }
    )
    pvc_uid, pv_uid, class_uid = (str(uuid4()) for _ in range(3))
    objects = {
        "kubevirt": installed_cr(),
        "storage_class": {
            "metadata": {"uid": class_uid, "name": "local"},
            "volumeBindingMode": "WaitForFirstConsumer",
            "allowedTopologies": [
                {
                    "matchLabelExpressions": [
                        {
                            "key": "zone",
                            "values": ["a"],
                        }
                    ]
                }
            ],
        },
        "pvc": {
            "metadata": {"uid": pvc_uid, "name": "root", "namespace": "workers"},
            "spec": {"storageClassName": "local", "volumeName": "retained"},
            "status": {"phase": "Bound"},
        },
        "pv": {
            "metadata": {"uid": pv_uid, "name": "retained"},
            "spec": {
                "claimRef": {"namespace": "workers", "uid": pvc_uid},
                "nodeAffinity": {
                    "required": {
                        "nodeSelectorTerms": [
                            {
                                "matchExpressions": [
                                    {
                                        "key": "zone",
                                        "operator": "In",
                                        "values": ["a"],
                                    }
                                ],
                            }
                        ]
                    }
                },
            },
        },
        "node": selected,
        "limitranges": [],
    }
    calls = []

    def get(kind):
        def read(**kwargs):
            calls.append((kind, kwargs))
            value = objects[kind]
            if isinstance(value, Exception):
                raise value
            return deepcopy(value)

        return read

    def no_list(**kwargs):
        raise AssertionError("final proof must not list cluster resources")

    def list_limits(**kwargs):
        calls.append(("limitranges", kwargs))
        return {
            "metadata": {"resourceVersion": "11", "continue": ""},
            "items": deepcopy(objects["limitranges"]),
        }

    collector = ResourceInventoryCollector(
        core=SimpleNamespace(
            read_node=get("node"),
            read_namespaced_persistent_volume_claim=get("pvc"),
            read_persistent_volume=get("pv"),
            list_node=no_list,
            list_pod_for_all_namespaces=no_list,
            list_namespaced_limit_range=list_limits,
        ),
        custom=SimpleNamespace(
            get_namespaced_custom_object=get("kubevirt"),
            list_namespaced_custom_object=no_list,
        ),
        storage=SimpleNamespace(
            read_storage_class=get("storage_class"), list_storage_class=no_list
        ),
        namespace="workers",
        cluster_id="test-cluster",
        controller_id=str(uuid4()),
        policy_digest="sha256:" + "a" * 64,
        label_keys=("kubernetes.io/hostname", "kubernetes.io/arch", "zone"),
        max_items=100,
        max_bytes=100000,
        request_timeout_seconds=1,
        collection_timeout_seconds=10,
        protocol=2,
        kubevirt_namespace="kubevirt",
        kubevirt_name="kubevirt",
    )
    resource = {
        "launcher_profile": default_launcher_profile(),
        "template_profile": {
            "selector": {"zone": "a"},
            "tolerations": [],
            "required_affinity": None,
            "storage_class": "local",
        },
    }
    row = {
        "controller_configuration": {"resource_admission": resource},
        "expected_pvc_uid": pvc_uid if retained else None,
    }
    grant = {
        "cluster_id": collector.cluster_id,
        "policy_digest": collector.policy_digest,
        "node_uid": selected["metadata"]["uid"],
        "node_name": "node-a",
        "vector": {
            "cpu_millicores": 1000,
            "memory_bytes": 1024**3,
            "ephemeral_storage_bytes": 100000000,
            "kvm_devices": 1,
            "tun_devices": 1,
            "vhost_net_devices": 1,
        },
        "headroom": {
            "cpu_millicores": 0,
            "memory_bytes": 0,
            "ephemeral_storage_bytes": 0,
            "kvm_devices": 0,
            "tun_devices": 0,
            "vhost_net_devices": 0,
        },
    }
    return SimpleNamespace(
        controller=SimpleNamespace(resource_inventory_collector=collector),
        row=row,
        grant=grant,
        objects=objects,
        calls=calls,
        pvc_name="root" if retained else None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("retained", [False, True])
async def test_final_proof_uses_selected_gets_with_node_last(retained):
    case = proof_case(retained=retained)
    await vm_resource_effect_node.targeted_resource_effect_node(
        case.controller,
        case.row,
        case.grant,
        pvc_name=case.pvc_name,
    )
    assert [kind for kind, _ in case.calls] == (
        ["kubevirt", "storage_class"]
        + (["pvc", "pv"] if retained else [])
        + ["limitranges", "node"]
    )
    assert all(
        kwargs["_request_timeout"] <= 1
        for kind, kwargs in case.calls
        if kind != "limitranges"
    )
    assert [
        kwargs["namespace"] for kind, kwargs in case.calls if kind == "limitranges"
    ] == ["workers"]
    assert case.calls[-1][1]["name"] == case.grant["node_name"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "node_uid",
        "node_ready",
        "class_name",
        "class_topology",
        "pvc_uid",
        "pv_claim_uid",
        "pv_topology",
        "profile",
        "unsettled_profile",
        "missing",
    ],
)
async def test_final_proof_refuses_actual_selected_change(change):
    case = proof_case(retained=True)
    if change == "node_uid":
        case.objects["node"]["metadata"]["uid"] = str(uuid4())
    elif change == "node_ready":
        case.objects["node"]["status"]["conditions"][0]["status"] = "False"
    elif change == "class_name":
        case.objects["storage_class"]["metadata"]["name"] = "other"
    elif change == "class_topology":
        case.objects["storage_class"]["allowedTopologies"][0]["matchLabelExpressions"][
            0
        ]["values"] = ["b"]
    elif change == "pvc_uid":
        case.objects["pvc"]["metadata"]["uid"] = str(uuid4())
    elif change == "pv_claim_uid":
        case.objects["pv"]["spec"]["claimRef"]["uid"] = str(uuid4())
    elif change == "pv_topology":
        case.objects["pv"]["spec"]["nodeAffinity"]["required"]["nodeSelectorTerms"][0][
            "matchExpressions"
        ][0]["values"] = ["b"]
    elif change == "profile":
        case.objects["kubevirt"]["spec"]["configuration"] = {
            "virtualMachineOptions": {"disableSerialConsoleLog": True},
        }
    elif change == "unsettled_profile":
        case.objects["kubevirt"]["status"]["observedGeneration"] -= 1
    else:
        case.objects["node"] = ApiException(status=404)
    with pytest.raises(ResourceAdmissionError, match="resource_node_changed"):
        await vm_resource_effect_node.targeted_resource_effect_node(
            case.controller,
            case.row,
            case.grant,
            pvc_name=case.pvc_name,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["kubevirt", "storage_class", "pvc", "pv", "node"])
async def test_final_proof_incomplete_read_is_unavailable(kind):
    case = proof_case(retained=True)
    case.objects[kind] = TimeoutError("private API timeout")
    with pytest.raises(ResourceAdmissionError, match="resource_inventory_unavailable"):
        await vm_resource_effect_node.targeted_resource_effect_node(
            case.controller,
            case.row,
            case.grant,
            pvc_name=case.pvc_name,
        )


@pytest.mark.asyncio
async def test_final_proof_refuses_limit_range_added_after_pregrant_inventory():
    case = proof_case()
    # The earlier full collection was complete; this namespace-scoped read is later.
    case.objects["limitranges"].append(
        {
            "metadata": {
                "uid": str(uuid4()),
                "name": "new-limit",
                "namespace": "workers",
            },
        }
    )
    with pytest.raises(ResourceAdmissionError, match="resource_node_changed"):
        await vm_resource_effect_node.targeted_resource_effect_node(
            case.controller,
            case.row,
            case.grant,
        )
    assert [kind for kind, _ in case.calls] == [
        "kubevirt",
        "storage_class",
        "limitranges",
    ]


@pytest.mark.asyncio
async def test_final_proof_refuses_normalization_that_finishes_after_deadline():
    case = proof_case()
    collector = case.controller.resource_inventory_collector
    collector.collection_timeout_seconds = 1
    original = collector._run_worker

    async def delayed(method):
        result = await original(method)
        if method.__name__ == "normalize":
            await asyncio.sleep(1.05)
        return result

    collector._run_worker = delayed
    with pytest.raises(ResourceAdmissionError, match="resource_inventory_unavailable"):
        await vm_resource_effect_node.targeted_resource_effect_node(
            case.controller,
            case.row,
            case.grant,
        )


@pytest.mark.asyncio
async def test_final_proof_cancellation_drains_selected_read():
    case = proof_case()
    entered, release, exited = (threading.Event() for _ in range(3))

    def blocked(**kwargs):
        entered.set()
        try:
            assert release.wait(timeout=2)
            return deepcopy(case.objects["kubevirt"])
        finally:
            exited.set()

    case.controller.resource_inventory_collector.custom.get_namespaced_custom_object = (
        blocked
    )
    task = asyncio.create_task(
        vm_resource_effect_node.targeted_resource_effect_node(
            case.controller,
            case.row,
            case.grant,
        )
    )
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert exited.is_set()
