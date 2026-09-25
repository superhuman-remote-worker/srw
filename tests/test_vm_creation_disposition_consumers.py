"""Namespace consumer evidence must be readable and cover nested API references."""

import asyncio
from copy import deepcopy
import json
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from kubernetes.client import ApiClient

from vm_controller.creation_actuation import CreationUnproven
from vm_controller import creation_disposition_resources as resources
from vm_controller.creation_disposition_resources import require_no_consumers
from vm_controller.creation_disposition_sources import DispositionSources


def actuator(kind, member):
    def listing(items):
        return {"metadata": {"resourceVersion": "1"}, "items": items}

    return SimpleNamespace(
        namespace="test",
        read=AsyncMock(return_value=None),
        controller=SimpleNamespace(
            k8s_client=SimpleNamespace(
                list_namespaced_custom_object=lambda **kw: listing(
                    [member] if kw["plural"] == kind else []
                )
            ),
            core_api=SimpleNamespace(
                list_namespaced_pod=lambda **_: listing(
                    [member] if kind == "pods" else []
                )
            ),
        ),
    )


def member(spec):
    return {
        "metadata": {"name": "other", "namespace": "test", "uid": "uid"},
        "spec": spec,
    }


DISPOSITION = {
    "job_id": "job",
    "objects": {"rootdisk": {"name": "root"}, "cloud_init": {"name": "secret"}},
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "spec",
    [
        {"volumes": [{"name": "v", "persistentVolumeClaim": {"claimName": "root"}}]},
        {"volumes": [{"name": "v", "dataVolume": {"name": "root"}}]},
        {
            "volumes": [
                {
                    "name": "v",
                    "ephemeral": {"persistentVolumeClaim": {"claimName": "root"}},
                }
            ]
        },
        {"volumes": [{"name": "v", "memoryDump": {"claimName": "root"}}]},
        {"utilityVolumes": [{"name": "v", "claimName": "root"}]},
        {
            "volumes": [
                {
                    "name": "v",
                    "ephemeral": {
                        "volumeClaimTemplate": {
                            "spec": {
                                "dataSource": {
                                    "kind": "PersistentVolumeClaim",
                                    "name": "root",
                                }
                            }
                        }
                    },
                }
            ]
        },
        {
            "volumes": [
                {"name": "v", "cloudInitNoCloud": {"secretRef": {"name": "secret"}}}
            ]
        },
        {
            "volumes": [
                {
                    "name": "v",
                    "cloudInitConfigDrive": {
                        "networkDataSecretRef": {"name": "secret"}
                    },
                }
            ]
        },
        {"volumes": [{"name": "v", "secret": {"secretName": "secret"}}]},
        {
            "volumes": [
                {
                    "name": "v",
                    "projected": {"sources": [{"secret": {"name": "secret"}}]},
                }
            ]
        },
        {
            "containers": [
                {
                    "name": "c",
                    "env": [
                        {
                            "name": "V",
                            "valueFrom": {
                                "secretKeyRef": {"name": "secret", "key": "k"}
                            },
                        }
                    ],
                }
            ]
        },
        {"containers": [{"name": "c", "envFrom": [{"secretRef": {"name": "secret"}}]}]},
        {
            "volumes": [
                {"name": "v", "csi": {"nodePublishSecretRef": {"name": "secret"}}}
            ]
        },
    ],
)
async def test_reference_shapes_hold(spec):
    with pytest.raises(CreationUnproven):
        await require_no_consumers(actuator("pods", member(spec)), DISPOSITION)


@pytest.mark.asyncio
async def test_pending_hotplug_reference_holds():
    value = member({"template": {"spec": {}}})
    value["status"] = {
        "volumeRequests": [
            {
                "addVolumeOptions": {
                    "volumeSource": {"persistentVolumeClaim": {"claimName": "root"}}
                }
            }
        ]
    }
    with pytest.raises(CreationUnproven):
        await require_no_consumers(actuator("virtualmachines", value), DISPOSITION)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value", [("name", None), ("uid", ""), ("namespace", "foreign")]
)
async def test_unproven_member_identity_holds(field, value):
    item = member({})
    item["metadata"][field] = value
    with pytest.raises(CreationUnproven):
        await require_no_consumers(actuator("pods", item), DISPOSITION)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "spec",
    [
        None,
        [],
        {"volumes": {}},
        {"volumes": [None]},
        {"volumes": [{"name": "v", "persistentVolumeClaim": "unreadable"}]},
    ],
)
async def test_unreadable_member_spec_holds(spec):
    with pytest.raises(CreationUnproven):
        await require_no_consumers(actuator("pods", member(spec)), DISPOSITION)


@pytest.mark.asyncio
@pytest.mark.parametrize("spec", [{}, {"template": {}}, {"template": {"spec": None}}])
async def test_unreadable_vm_template_holds(spec):
    with pytest.raises(CreationUnproven):
        await require_no_consumers(
            actuator("virtualmachines", member(spec)), DISPOSITION
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("volumes", [None, [], [{"name": "unused", "emptyDir": {}}]])
async def test_genuine_unrelated_member_allows_absence(volumes):
    spec = {} if volumes is None else {"volumes": deepcopy(volumes)}
    await require_no_consumers(actuator("pods", member(spec)), DISPOSITION)


def _sdk_pod_actuator():
    pod = member(
        {
            "containers": [{"name": "app", "image": "synthetic.invalid/local"}],
            "volumes": [{"name": "unused", "emptyDir": {}}],
        }
    )
    wire = json.dumps({"metadata": {"resourceVersion": "1"}, "items": [pod]})
    sdk = ApiClient()
    target = actuator("pods", pod)
    target.controller.core_api.list_namespaced_pod = lambda **_: sdk.deserialize(
        SimpleNamespace(data=wire), "V1PodList"
    )
    return target


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stage", ("document", "_consumer_spec", "_root_reference", "_secret_reference")
)
@pytest.mark.parametrize("through_source", (False, True))
async def test_conversion_and_consumer_scans_leave_event_loop_responsive(
    monkeypatch, stage, through_source
):
    target = _sdk_pod_actuator()
    loop = asyncio.get_running_loop()
    loop_progress = threading.Event()
    original = getattr(resources, stage)
    checked = False

    def guarded(*args, **kwargs):
        nonlocal checked
        # Guard the SDK response for document; the earlier custom LISTs are dicts.
        if not checked and (stage != "document" or not isinstance(args[0], dict)):
            checked = True
            loop.call_soon_threadsafe(loop_progress.set)
            if not loop_progress.wait(timeout=1):
                raise RuntimeError(f"event loop blocked during {stage}")
        return original(*args, **kwargs)

    monkeypatch.setattr(resources, stage, guarded)
    if through_source:
        service = DispositionSources.__new__(DispositionSources)
        service.actuator, service.disposition = target, DISPOSITION
        await service.target_safe({"kind": "rootdisk_purged", "name": "root"})
    else:
        await require_no_consumers(target, DISPOSITION)
    assert checked and loop_progress.is_set()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ("list", "document", "_root_reference"))
@pytest.mark.parametrize("worker_fails", (False, True))
async def test_repeated_cancellation_drains_consumer_worker(
    monkeypatch, stage, worker_fails
):
    target = _sdk_pod_actuator()
    entered, release, exited = (threading.Event(), threading.Event(), threading.Event())
    effect = []
    if stage == "list":
        owner, name = target.controller.core_api, "list_namespaced_pod"
    else:
        owner, name = resources, stage
    original = getattr(owner, name)

    def blocked(*args, **kwargs):
        if (stage == "document" and isinstance(args[0], dict)) or entered.is_set():
            return original(*args, **kwargs)
        entered.set()
        try:
            if not release.wait(timeout=3):
                raise RuntimeError("consumer worker was not released")
            if worker_fails:
                raise RuntimeError("consumer worker failed")
            return original(*args, **kwargs)
        finally:
            exited.set()

    monkeypatch.setattr(owner, name, blocked)
    service = DispositionSources.__new__(DispositionSources)
    service.actuator, service.disposition = target, DISPOSITION

    async def guarded_effect():
        await service.target_safe({"kind": "rootdisk_purged", "name": "root"})
        effect.append("source CAS")

    task = asyncio.create_task(guarded_effect())
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        drain_started = asyncio.Event()
        asyncio.get_running_loop().call_soon(drain_started.set)
        await drain_started.wait()
        assert not task.done()
        task.cancel()
        second_cancel = asyncio.Event()
        asyncio.get_running_loop().call_soon(second_cancel.set)
        await second_cancel.wait()
        assert not task.done()
    finally:
        release.set()
        assert await asyncio.to_thread(exited.wait, 2)
    with pytest.raises(asyncio.CancelledError):
        await task
    assert effect == []


@pytest.mark.asyncio
@pytest.mark.parametrize("read", ("custom", "pods"))
@pytest.mark.parametrize("through_source", (False, True))
async def test_consumer_read_timeout_holds_and_has_request_budget(read, through_source):
    target = actuator("pods", member({}))
    calls = []

    def custom(**kwargs):
        calls.append(("custom", kwargs))
        if read == "custom":
            raise TimeoutError("SDK request timed out")
        return {"metadata": {"resourceVersion": "1"}, "items": []}

    def pods(**kwargs):
        calls.append(("pods", kwargs))
        if read == "pods":
            raise TimeoutError("SDK request timed out")
        return {"metadata": {"resourceVersion": "1"}, "items": []}

    target.controller.k8s_client.list_namespaced_custom_object = custom
    target.controller.core_api.list_namespaced_pod = pods
    effects = []
    service = DispositionSources.__new__(DispositionSources)
    service.actuator, service.disposition = target, DISPOSITION

    async def source_effect():
        await service.target_safe({"kind": "rootdisk_purged", "name": "root"})
        effects.append("source CAS")

    with pytest.raises(TimeoutError):
        if through_source:
            await source_effect()
        else:
            await require_no_consumers(target, DISPOSITION)
    assert calls
    assert all(kwargs["_request_timeout"] == 5 for _, kwargs in calls)
    assert effects == []


@pytest.mark.asyncio
async def test_all_consumer_reads_have_five_second_request_budget():
    target = actuator("pods", member({}))
    calls = []
    custom = target.controller.k8s_client.list_namespaced_custom_object
    pods = target.controller.core_api.list_namespaced_pod

    def listed(method, **kwargs):
        calls.append(kwargs)
        return method(**kwargs)

    target.controller.k8s_client.list_namespaced_custom_object = (
        lambda **kwargs: listed(custom, **kwargs)
    )
    target.controller.core_api.list_namespaced_pod = lambda **kwargs: listed(
        pods, **kwargs
    )
    await require_no_consumers(target, DISPOSITION)
    assert len(calls) == 3
    assert all(kwargs["_request_timeout"] == 5 for kwargs in calls)
