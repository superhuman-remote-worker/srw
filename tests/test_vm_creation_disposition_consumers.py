"""Namespace consumer evidence must be readable and cover nested API references."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from vm_controller.creation_actuation import CreationUnproven
from vm_controller.creation_disposition_resources import require_no_consumers


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
