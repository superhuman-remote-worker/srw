"""Disposed pins remain typed and compatible with every source deletion path."""

from copy import deepcopy
import json
from uuid import uuid4

import pytest

from tests.test_vm_creation_golden import (
    setup as _setup_fixture,
    golden as _golden_fixture,
)
from tests.test_vm_preparation_store import retiring_source
from vm_controller.creation_sources import GoldenSources, PINS, pins

setup, golden = _setup_fixture, _golden_fixture


def disposed(dv_uid, pvc_uid):
    job, generation, disposition = (str(uuid4()) for _ in range(3))
    root = "agent-vm-" + job + "-rootdisk"
    return {
        "version": 1,
        "state": "disposed",
        "job_id": job,
        "provision_generation": generation,
        "disposition_id": disposition,
        "dv_uid": dv_uid,
        "pvc_uid": pvc_uid,
        "rootdisk_name": root,
        "target": {
            "kind": "rootdisk_never_issued",
            "name": root,
            "namespace": "agent-vms",
        },
    }


@pytest.mark.asyncio
async def test_golden_delete_recognizes_exact_disposed_pin(golden):
    ctrl, api, _, _, name = golden
    dv = api.objects["DataVolume", name]
    pin = disposed(
        dv["metadata"]["uid"],
        api.objects["PersistentVolumeClaim", name]["metadata"]["uid"],
    )
    dv["metadata"].setdefault("annotations", {})[PINS] = json.dumps({str(uuid4()): pin})
    await GoldenSources(ctrl).delete(name, expected_uid=dv["metadata"]["uid"])
    assert len(api.deletions) == 1


@pytest.mark.asyncio
async def test_preparation_delete_recognizes_exact_disposed_pin():
    store, dv, owner, pvc_uid = retiring_source()
    dv["metadata"]["annotations"][PINS] = json.dumps(
        {str(uuid4()): disposed(dv["metadata"]["uid"], pvc_uid)}
    )
    assert not await store.delete_disk(
        "prepared-source",
        owner_uid=owner,
        pvc_uid=pvc_uid,
        dv_uid=dv["metadata"]["uid"],
    )
    assert store.call.await_count == 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("version", True),
        ("disposition_id", "missing"),
        ("dv_uid", str(uuid4())),
        ("rootdisk_name", "arbitrary"),
        ("target", {"done": True}),
    ],
)
def test_malformed_disposed_pin_never_unlocks_source(field, value):
    uid = str(uuid4())
    pin = disposed(uid, str(uuid4()))
    pin[field] = deepcopy(value)
    dv = {
        "metadata": {"uid": uid, "annotations": {PINS: json.dumps({str(uuid4()): pin})}}
    }
    with pytest.raises(ValueError):
        pins(dv)
