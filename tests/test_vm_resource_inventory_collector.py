import asyncio
from copy import deepcopy
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from vm_controller.resource_inventory import ResourceInventoryCollector
from tests.test_vm_resource_placement import node
from tests.test_infrastructure_metering_pod_normalization import _pod


def page(items, *, rv="10", continuation=""):
    return {
        "metadata": {"resourceVersion": rv, "continue": continuation},
        "items": items,
    }


def fixture(*, max_items=100, max_bytes=100000):
    pages = {
        kind: [page([])]
        for kind in (
            "nodes",
            "pods",
            "vms",
            "vmis",
            "pvcs",
            "pvs",
            "storage_classes",
            "dvs",
        )
    }
    pages["nodes"] = [page([node()])]
    pages["pods"] = [page([_pod()])]
    calls = []

    def listing(kind):
        def get(**kwargs):
            calls.append((kind, kwargs))
            sequence = pages[kind]
            value = sequence.pop(0) if len(sequence) > 1 else sequence[0]
            if isinstance(value, Exception):
                raise value
            return deepcopy(value)

        return get

    collector = ResourceInventoryCollector(
        core=SimpleNamespace(
            list_node=listing("nodes"),
            list_pod_for_all_namespaces=listing("pods"),
            list_namespaced_persistent_volume_claim=listing("pvcs"),
            list_persistent_volume=listing("pvs"),
        ),
        custom=SimpleNamespace(
            list_namespaced_custom_object=lambda **kw: listing(
                {
                    "virtualmachines": "vms",
                    "virtualmachineinstances": "vmis",
                    "datavolumes": "dvs",
                }[kw["plural"]]
            )(**kw)
        ),
        storage=SimpleNamespace(list_storage_class=listing("storage_classes")),
        namespace="workers",
        cluster_id="test-cluster",
        controller_id=str(uuid4()),
        policy_digest="sha256:" + "a" * 64,
        label_keys=("kubernetes.io/hostname", "zone", "generation"),
        max_items=max_items,
        max_bytes=max_bytes,
        request_timeout_seconds=1,
        collection_timeout_seconds=10,
    )
    return collector, pages, calls


@pytest.mark.asyncio
async def test_collects_complete_scope_without_serializing_private_fields():
    collector, pages, calls = fixture()
    raw = pages["pods"][0]["items"][0]
    raw["metadata"]["annotations"] = {"private": "DO_NOT_EMIT"}
    raw["spec"]["containers"][0].update(
        image="DO_NOT_EMIT", env=[{"name": "SECRET", "value": "DO_NOT_EMIT"}]
    )
    raw["status"]["message"] = "DO_NOT_EMIT"
    pages["nodes"][0]["items"][0]["metadata"]["labels"]["private-label"] = "DO_NOT_EMIT"
    result = await collector.collect(1)
    assert result["complete"] is True
    assert result["pods"][0]["requests"] == {
        "cpu_millicores": 100,
        "memory_bytes": 1024**3,
        "kvm_devices": 0,
    }
    assert "DO_NOT_EMIT" not in json.dumps(result)
    assert {kind for kind, _ in calls} == set(result["resource_versions"])
    assert all("namespace" not in kwargs for kind, kwargs in calls if kind == "pods")
    assert all(kwargs["_request_timeout"] <= 1 for _, kwargs in calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault,reason",
    [
        ("repeat", "collection_incomplete"),
        ("rv", "collection_incomplete"),
        ("api", "collection_failed"),
        ("duplicate", "identity_unproven"),
        ("missing-node", "identity_unproven"),
    ],
)
async def test_pagination_or_identity_failure_invalidates_whole_snapshot(fault, reason):
    collector, pages, _ = fixture()
    if fault in {"repeat", "rv"}:
        pages["nodes"][0]["metadata"]["continue"] = "next"
        pages["nodes"].append(
            page([], rv="11" if fault == "rv" else "10", continuation="next")
        )
    elif fault == "api":
        pages["pods"] = [RuntimeError("DO_NOT_EMIT")]
    elif fault == "duplicate":
        pages["nodes"][0]["items"].append(deepcopy(pages["nodes"][0]["items"][0]))
    else:
        pages["nodes"][0]["items"] = []
    result = await collector.collect(1)
    assert result["complete"] is False and result["reason"] == reason
    assert all(result[kind] == [] for kind in result["resource_versions"])
    assert result["nodes"] == result["pods"] == []
    assert "DO_NOT_EMIT" not in json.dumps(result)


@pytest.mark.asyncio
async def test_item_limit_counts_all_pages_before_sanitization():
    collector, _, _ = fixture(max_items=1)
    result = await collector.collect(1)
    assert result["reason"] == "item_limit" and not result["complete"]


@pytest.mark.asyncio
async def test_valid_pagination_preserves_one_kind_resource_version():
    collector, pages, calls = fixture()
    pages["nodes"][0]["metadata"]["continue"] = "next"
    pages["nodes"].append(page([]))
    result = await collector.collect(1)
    assert result["complete"] is True
    assert [kwargs.get("_continue") for kind, kwargs in calls if kind == "nodes"] == [
        None,
        "next",
    ]


@pytest.mark.asyncio
async def test_cancellation_waits_for_bounded_synchronous_collection_call():
    import threading

    collector, _, _ = fixture()
    entered, released, finished = (
        threading.Event(),
        threading.Event(),
        threading.Event(),
    )

    def blocked(**kwargs):
        entered.set()
        released.wait(timeout=0.5)
        finished.set()
        return page([])

    collector.core.list_node = blocked
    task = asyncio.create_task(collector.collect(1))
    await asyncio.to_thread(entered.wait, 1)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    released.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert finished.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["vms", "vmis", "pvcs", "dvs"])
async def test_namespaced_list_never_relabels_foreign_objects(kind):
    collector, pages, _ = fixture()
    pages[kind] = [
        page(
            [
                {
                    "metadata": {
                        "uid": str(uuid4()),
                        "name": "foreign",
                        "namespace": "other",
                    }
                }
            ]
        )
    ]
    result = await collector.collect(1)
    assert not result["complete"] and result["reason"] == "identity_unproven"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation", ["foreign_group", "wrong_name", "wrong_uid", "exact"]
)
async def test_managed_pod_association_requires_exact_kubevirt_reference(mutation):
    collector, pages, _ = fixture()
    vmi = {
        "apiVersion": "kubevirt.io/v1",
        "kind": "VirtualMachineInstance",
        "metadata": {"uid": str(uuid4()), "name": "vmi-a", "namespace": "workers"},
        "status": {"phase": "Running", "nodeName": "node-a"},
    }
    pages["vmis"] = [page([vmi])]
    ref = {
        "apiVersion": "kubevirt.io/v1",
        "kind": "VirtualMachineInstance",
        "uid": vmi["metadata"]["uid"],
        "name": "vmi-a",
        "controller": True,
    }
    if mutation == "foreign_group":
        ref["apiVersion"] = "foreign.example/v1"
    elif mutation == "wrong_name":
        ref["name"] = "different"
    elif mutation == "wrong_uid":
        ref["uid"] = str(uuid4())
    pages["pods"][0]["items"][0]["metadata"]["ownerReferences"] = [ref]
    result = await collector.collect(1)
    if mutation == "exact":
        assert result["complete"] and result["pods"][0]["vmi_uid"] == ref["uid"]
    elif mutation == "foreign_group":
        assert result["complete"] and result["pods"][0]["vmi_uid"] is None
    else:
        assert not result["complete"]
