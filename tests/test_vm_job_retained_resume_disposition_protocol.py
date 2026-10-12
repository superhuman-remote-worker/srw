"""No-effect retained Resume keeps B's disk without inventing a C root effect."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from orchestrator.services.vm_creation_disposition_store import (
    VMCreationDispositionStore,
)
from orchestrator.services.vm_creation_retry_store import VMCreationRetryConflict
from vm_controller.creation_actuation import CreationUnproven
from vm_controller import creation_disposition_resources as resources


def _case():
    job_id, pvc_uid, dv_uid, request_id = (str(uuid4()) for _ in range(4))
    root = {
        "name": f"agent-vm-{job_id}-rootdisk",
        "namespace": "agent-vms",
        "uid": dv_uid,
        "pvc_uid": pvc_uid,
    }
    row = {
        "owner_kind": "job",
        "job_id": job_id,
        "request_id": request_id,
        "expected_pvc_uid": pvc_uid,
        "observed_pvc_uid": None,
        "canonical_request": {"workspace_storage": None},
        "cancellation_progress": {},
    }
    disposition = {
        "version": 1,
        "disposition_id": str(uuid4()),
        "job_id": job_id,
        "namespace": "agent-vms",
        "disk_policy": "retain",
        "objects": {},
        "effects": [],
    }
    return row, disposition, root


@pytest.mark.asyncio
async def test_no_effect_c_grants_only_exact_inherited_keep():
    row, disposition, root = _case()
    conn = SimpleNamespace(fetchval=AsyncMock(return_value=root))
    store = object.__new__(VMCreationDispositionStore)

    grant = await store._grant(conn, row, disposition, "rootdisk", create_child=True)

    assert grant == {
        "operation": "retain_inherited_rootdisk",
        "resource": root,
        "completion": {
            "version": 1,
            "disposition_id": disposition["disposition_id"],
            "kind": "rootdisk_retained",
            **root,
        },
    }
    assert disposition["objects"] == {}
    assert disposition["effects"] == []
    query, request_id = conn.fetchval.await_args.args
    assert "public.vm_job_retained_inherited_rootdisk" in query
    assert str(request_id) == row["request_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        lambda row, disposition, root: root.update(pvc_uid=str(uuid4())),
        lambda row, disposition, root: root.update(uid="invalid"),
        lambda row, disposition, root: root.update(extra=True),
        lambda row, disposition, root: root.pop("uid"),
        lambda row, disposition, root: root.clear(),
        lambda row, disposition, root: root.update(namespace="other"),
        lambda row, disposition, root: row.update(owner_kind="thread"),
        lambda row, disposition, root: disposition.update(
            disk_policy="purge_new_job_disk"
        ),
        lambda row, disposition, root: disposition["effects"].append(
            {"effect_kind": "rootdisk", "state": "observed"}
        ),
    ],
)
async def test_inherited_keep_refuses_changed_identity_or_wrong_lane(change):
    row, disposition, root = _case()
    change(row, disposition, root)
    conn = SimpleNamespace(fetchval=AsyncMock(return_value=root))
    store = object.__new__(VMCreationDispositionStore)
    with pytest.raises(VMCreationRetryConflict):
        await store._grant(conn, row, disposition, "rootdisk", create_child=False)


def _inherited_actuator(row, disposition, root, *, proof=None, pins=None):
    completion = {
        "version": 1,
        "disposition_id": disposition["disposition_id"],
        "kind": "rootdisk_retained",
        **root,
    }
    controller = SimpleNamespace(
        _qualify_cancel_retained_rootdisk=AsyncMock(
            return_value=proof
            or {
                "version": 1,
                "kind": "vm_retained_rootdisk_v1",
                "namespace": root["namespace"],
                "owner_kind": "job",
                "owner_id": row["job_id"],
                "pvc_name": root["name"],
                "pvc_uid": root["pvc_uid"],
                "dv_uid": root["uid"],
                "ownership": "standalone_dv",
                "deleting": False,
                "no_consumers": True,
            }
        ),
        _active_recovery_pins=AsyncMock(return_value=pins or []),
    )

    async def authority(_verb, *, stage, **_kwargs):
        if stage == "cloud_init":
            return {
                "operation": "confirm_absent",
                "completion": {"name": f"agent-vm-{row['job_id']}-cloudinit"},
            }
        return {
            "operation": "retain_inherited_rootdisk",
            "resource": root,
            "completion": completion,
        }

    actuator = SimpleNamespace(
        namespace=root["namespace"],
        controller=controller,
        authority=authority,
        read=AsyncMock(return_value=None),
    )
    return actuator, completion


@pytest.mark.asyncio
async def test_controller_qualifies_inherited_disk_before_record(monkeypatch):
    row, disposition, root = _case()
    scan = AsyncMock()
    monkeypatch.setattr(resources, "require_no_consumers", scan)
    actuator, completion = _inherited_actuator(row, disposition, root)
    operation = resources.DispositionResources(actuator, row, {}, disposition)
    operation.record = AsyncMock()

    await operation.run()

    assert actuator.controller._qualify_cancel_retained_rootdisk.await_count == 2
    for call in actuator.controller._qualify_cancel_retained_rootdisk.await_args_list:
        assert call.args == (row["job_id"], root["pvc_uid"])
    assert operation.record.await_args_list[-1].args == ("rootdisk", completion)
    assert disposition["objects"] == {}
    assert scan.await_count >= 1


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["consumer", "pin", "dv_uid"])
async def test_controller_holds_inherited_disk_on_consumer_pin_or_uid(
    monkeypatch, drift
):
    row, disposition, root = _case()
    monkeypatch.setattr(resources, "require_no_consumers", AsyncMock())
    pins = [{"pvc_uid": root["pvc_uid"]}] if drift == "pin" else []
    proof = None
    if drift == "dv_uid":
        proof = {"dv_uid": str(uuid4())}
    actuator, _completion = _inherited_actuator(
        row, disposition, root, proof=proof, pins=pins
    )
    if drift == "consumer":
        actuator.controller._qualify_cancel_retained_rootdisk.side_effect = (
            RuntimeError("creation_resource_in_use")
        )
    operation = resources.DispositionResources(actuator, row, {}, disposition)
    operation.record = AsyncMock()

    with pytest.raises(CreationUnproven):
        await operation.run()

    operation.record.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift", ["purge", "observed_root", "wrong_owner", "missing_owner"]
)
async def test_controller_rejects_inherited_grant_outside_no_effect_job_keep(
    monkeypatch, drift
):
    row, disposition, root = _case()
    monkeypatch.setattr(resources, "require_no_consumers", AsyncMock())
    if drift == "purge":
        disposition["disk_policy"] = "purge_new_job_disk"
    elif drift == "observed_root":
        disposition["objects"]["rootdisk"] = root
    elif drift == "missing_owner":
        row.pop("owner_kind")
    else:
        row["owner_kind"] = "thread"
    actuator, _completion = _inherited_actuator(row, disposition, root)
    operation = resources.DispositionResources(actuator, row, {}, disposition)
    operation.check = AsyncMock()
    operation.record = AsyncMock()

    with pytest.raises(CreationUnproven, match="grant_changed"):
        await operation.run()

    actuator.controller._qualify_cancel_retained_rootdisk.assert_not_awaited()
    assert not any(
        call.args[0] == "rootdisk" for call in operation.record.await_args_list
    )


@pytest.mark.asyncio
async def test_existing_observed_root_retain_path_stays_supported(monkeypatch):
    row, disposition, root = _case()
    disposition["objects"]["rootdisk"] = root
    monkeypatch.setattr(resources, "require_no_consumers", AsyncMock())
    actuator, completion = _inherited_actuator(row, disposition, root)
    original_authority = actuator.authority

    async def authority(verb, *, stage, **kwargs):
        grant = await original_authority(verb, stage=stage, **kwargs)
        if stage == "rootdisk":
            grant["operation"] = "retain_rootdisk"
        return grant

    actuator.authority = authority
    operation = resources.DispositionResources(actuator, row, {}, disposition)
    operation.check = AsyncMock()
    operation.record = AsyncMock()

    await operation.run()

    assert operation.record.await_args_list[-1].args == ("rootdisk", completion)
    actuator.controller._qualify_cancel_retained_rootdisk.assert_not_awaited()
