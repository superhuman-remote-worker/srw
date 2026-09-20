"""Storage retirement proves all identities before its first deletion."""

from types import SimpleNamespace as Obj
from unittest.mock import AsyncMock, Mock

import pytest
from vm_controller.preparation_store import (
    DISK_LABEL,
    PreparationConflict,
    PreparationStore,
)


@pytest.mark.asyncio
async def test_replaced_pvc_prevents_deletion_of_even_the_captured_datavolume():
    store = PreparationStore(Mock(), Mock(), "test", "local-path")
    store.unused = AsyncMock(return_value=True)
    store.dv = AsyncMock(
        return_value={
            "metadata": {"uid": "dv-original", "labels": {DISK_LABEL: "owner"}}
        }
    )
    store.pvc = AsyncMock(
        return_value=Obj(
            metadata=Obj(uid="pvc-replacement", labels={DISK_LABEL: "owner"})
        )
    )
    store.call = AsyncMock()
    with pytest.raises(PreparationConflict, match="PVC identity"):
        await store.delete_disk(
            "disk",
            owner_uid="owner",
            pvc_uid="pvc-original",
            dv_uid="dv-original",
            retire_import=True,
        )
    store.call.assert_not_awaited()


@pytest.mark.asyncio
async def test_external_disk_consumer_blocks_failed_import_retirement():
    store = PreparationStore(Mock(), Mock(), "test", "local-path")
    store.unused = AsyncMock(return_value=False)
    store.call = AsyncMock()
    assert not await store.delete_disk(
        "disk", owner_uid="owner", pvc_uid="pvc", dv_uid="dv", retire_import=True
    )
    store.unused.assert_awaited_once_with("disk", ignore_cdi_owner="pvc")
    store.call.assert_not_awaited()


def retiring_source():
    from uuid import uuid4

    store = PreparationStore(Mock(), Mock(), "test", "local-path")
    owner, dv_uid, pvc_uid = str(uuid4()), str(uuid4()), str(uuid4())
    dv = {
        "metadata": {
            "name": "prepared-source",
            "uid": dv_uid,
            "resourceVersion": "1",
            "labels": {DISK_LABEL: owner},
            "annotations": {},
        }
    }
    store.unused = AsyncMock(return_value=True)
    store.dv = AsyncMock(return_value=dv)
    store.pvc = AsyncMock(
        return_value=Obj(metadata=Obj(uid=pvc_uid, labels={DISK_LABEL: owner}))
    )
    store.call = AsyncMock()
    return store, dv, owner, pvc_uid


@pytest.mark.asyncio
async def test_active_creation_pin_blocks_prepared_source_dv_and_pvc_deletion():
    import json
    from uuid import uuid4
    from vm_controller.creation_sources import PINS

    store, dv, owner, pvc_uid = retiring_source()
    job = str(uuid4())
    dv["metadata"]["annotations"][PINS] = json.dumps(
        {
            str(uuid4()): {
                "state": "active",
                "job_id": job,
                "provision_generation": str(uuid4()),
                "dv_uid": dv["metadata"]["uid"],
                "pvc_uid": pvc_uid,
                "rootdisk_name": "agent-vm-" + job + "-rootdisk",
            }
        }
    )
    assert not await store.delete_disk(
        "prepared-source",
        owner_uid=owner,
        pvc_uid=pvc_uid,
        dv_uid=dv["metadata"]["uid"],
    )
    store.call.assert_not_awaited()


@pytest.mark.asyncio
async def test_prepared_source_delete_cas_loses_to_pin_and_never_deletes_pvc():
    from kubernetes.client.exceptions import ApiException

    store, dv, owner, pvc_uid = retiring_source()
    calls = []

    async def raced(method, **kwargs):
        calls.append(method)
        if method == store.custom.delete_namespaced_custom_object:
            assert kwargs["body"]["preconditions"] == {
                "uid": dv["metadata"]["uid"],
                "resourceVersion": "1",
            }
            dv["metadata"]["resourceVersion"] = "2"  # concurrent pin publication
            if kwargs["body"]["preconditions"].get("resourceVersion") != "2":
                raise ApiException(status=409)

    store.call = raced
    with pytest.raises(ApiException) as exc:
        await store.delete_disk(
            "prepared-source",
            owner_uid=owner,
            pvc_uid=pvc_uid,
            dv_uid=dv["metadata"]["uid"],
        )
    assert exc.value.status == 409
    assert calls == [store.custom.delete_namespaced_custom_object]


@pytest.mark.asyncio
async def test_prepared_source_delete_requires_observed_revision():
    store, dv, owner, pvc_uid = retiring_source()
    del dv["metadata"]["resourceVersion"]
    with pytest.raises(PreparationConflict):
        await store.delete_disk(
            "prepared-source",
            owner_uid=owner,
            pvc_uid=pvc_uid,
            dv_uid=dv["metadata"]["uid"],
        )
    store.call.assert_not_awaited()
